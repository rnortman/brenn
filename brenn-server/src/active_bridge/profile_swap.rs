//! Swapping a live conversation onto a different Claude account.
//!
//! An account is `CLAUDE_CODE_OAUTH_TOKEN`, fixed at spawn, so moving a
//! conversation to another account means replacing its CC process. The
//! conversation itself survives: its CC session id is persisted, and the
//! replacement resumes it.
//!
//! The swap runs only at an idle moment and holds `bridge.session` for its whole
//! duration, so a send that arrives mid-swap parks on the mutex and lands on the
//! new process rather than failing against a gap.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use brenn_cc::error::CcError;
use brenn_cc::session::{CcSession, InitAckInfo, ModelOption, SessionEvent};
use brenn_cc_profile::ResolvedProfile;
use brenn_obs::alerting::AlertDispatcher;
use brenn_obs::transcript::TranscriptWriter;
use brenn_ws_types::{CcState, ModelInfo, WsServerMessage};
use tracing::{error, info};

use super::ActiveBridge;
use super::cc_spawn_config::{
    CcSpawnInputs, build_cc_session_config, conversation_container_suffix,
};
use crate::model_cache::ModelCache;

/// How long the swap waits for the old process's `Died` acknowledgement before
/// giving the bridge to the ordinary death path. The reader emits `Died` on EOF
/// unconditionally, so a timeout means the event loop is not consuming.
const DIED_ACK_TIMEOUT: Duration = Duration::from_secs(30);

/// How often the wait for that acknowledgement looks for the ordinary death
/// path having claimed the same death instead.
const DIED_ACK_POLL: Duration = Duration::from_millis(100);

/// What a replacement spawn needs to know that the host does not already hold.
pub(in crate::active_bridge) struct RespawnRequest {
    pub conversation_id: i64,
    /// The account the replacement runs under.
    pub profile: ResolvedProfile,
    /// The model the conversation is on, when the bridge has asserted one. The
    /// replacement starts on it rather than reverting to the app default.
    pub model: Option<String>,
    /// The CC session id to `--resume`. `None` when the conversation has none.
    pub resume_session_id: Option<String>,
    /// Where the replacement's events go — the bridge's existing event loop.
    pub events: tokio::sync::mpsc::Sender<SessionEvent>,
}

/// Everything outside the bridge that a swap has to reach: the spawn itself and
/// the model cache the new account's model set lands in.
///
/// A trait because the two are the swap's only contact with the world, and a
/// test wants a swap it can run without a `claude` binary.
#[async_trait::async_trait]
pub(in crate::active_bridge) trait ProfileSwapHost: Send + Sync {
    /// Spawn a replacement CC process for this conversation.
    async fn respawn(&self, req: RespawnRequest) -> Result<(CcSession, InitAckInfo), CcError>;

    /// Record the models the replacement reported, returning them filtered to
    /// the app's allow-list — what the picker may show.
    async fn record_models(&self, models: &[ModelOption]) -> Vec<ModelInfo>;
}

/// The production host. Built in `spawn_new`, because two of its parts — the
/// transcript writer and the conversation-scoped alert dispatcher — belong to
/// the bridge and exist only there.
pub(in crate::active_bridge) struct ServerSwapHost {
    pub(in crate::active_bridge) models: ModelCache,
    /// The app this bridge runs. Held directly rather than looked up: the swap
    /// respawns the one app its bridge belongs to.
    pub(in crate::active_bridge) app_config: brenn_lib::config::AppConfig,
    pub(in crate::active_bridge) mcp_script_path: std::path::PathBuf,
    pub(in crate::active_bridge) transcript: Arc<TranscriptWriter>,
    pub(in crate::active_bridge) alert_dispatcher: AlertDispatcher,
    pub(in crate::active_bridge) user_tz: chrono_tz::Tz,
    pub(in crate::active_bridge) server_shutting_down: Arc<std::sync::atomic::AtomicBool>,
}

/// The `AppState`-owned half of [`ServerSwapHost`], carried on `SpawnContext`
/// so `spawn_new` can finish the host with the per-bridge half. Everything
/// else the host needs is already on `SpawnContext` in its own right.
pub struct SwapHostSeed {
    pub(crate) models: ModelCache,
}

#[async_trait::async_trait]
impl ProfileSwapHost for ServerSwapHost {
    async fn respawn(&self, req: RespawnRequest) -> Result<(CcSession, InitAckInfo), CcError> {
        let config = build_cc_session_config(CcSpawnInputs {
            app_config: &self.app_config,
            mcp_script_path: &self.mcp_script_path,
            model: req.model.unwrap_or_else(|| self.app_config.model.clone()),
            container_name_suffix: conversation_container_suffix(req.conversation_id),
            resume_session_id: req.resume_session_id,
            transcript: self.transcript.clone(),
            alert_dispatcher: self.alert_dispatcher.clone(),
            user_tz: self.user_tz,
            server_shutting_down: self.server_shutting_down.clone(),
            cc_profile: Some(req.profile),
        });
        CcSession::spawn(config, req.events).await
    }

    async fn record_models(&self, models: &[ModelOption]) -> Vec<ModelInfo> {
        self.models
            .record_and_filter(&self.app_config.slug, models)
            .await
    }
}

impl ActiveBridge {
    /// The profile name this bridge's CC process is running under, or `None`
    /// for an app that declared no `claude_profiles`.
    pub(in crate::active_bridge) fn cc_profile_name(&self) -> Option<String> {
        self.cc_profile
            .lock()
            .expect("cc_profile lock poisoned")
            .clone()
    }

    /// Take a swap's outstanding claim on the next `Died`, if there is one.
    ///
    /// The event loop calls this at every death: `Some` means this death is the
    /// swap's and nothing about it is a death to the conversation. Taking is
    /// what limits a claim to exactly one death.
    pub(in crate::active_bridge) fn take_swap_ack(
        &self,
    ) -> Option<tokio::sync::oneshot::Sender<()>> {
        self.swap_ack.lock().expect("swap_ack lock poisoned").take()
    }

    /// The account this bridge *should* be on, when that differs from the one it
    /// is on. `None` means nothing to do: no profiles, or already there.
    fn stale_target(&self) -> Option<ResolvedProfile> {
        let goals = self.cc_profiles.as_ref()?;
        // Compare names first: `resolve` allocates (it clones credentials),
        // and this check runs at every turn end of every profiled conversation.
        let name = goals.current(&self.app_slug)?;
        if self.cc_profile_name().as_deref() == Some(name.as_str()) {
            return None;
        }
        goals.resolve(&self.app_slug)
    }

    /// Swap this bridge's CC process onto its goal profile, if it is on the
    /// wrong one and this is a moment where that is allowed.
    ///
    /// Called from the two places the answer can change: the goal channel's
    /// drain task, and every turn end. Returns immediately in the common case;
    /// an actual swap runs as its own task so neither caller waits on a CC
    /// startup.
    ///
    /// A draining bridge is skipped on purpose — the drain kills the process
    /// anyway, and the next wake spawns under the goal. So is a bridge already
    /// mid-swap, and one whose server is going down: nothing may start a CC
    /// process, or a podman container, that the shutdown path has already
    /// walked past.
    pub fn reconsider_profile(self: &Arc<Self>) {
        if !self.cc_idle.load(Ordering::SeqCst)
            || self.drain_on_idle.load(Ordering::SeqCst)
            || self.swapping.load(Ordering::SeqCst)
            || self.server_shutting_down.load(Ordering::SeqCst)
        {
            return;
        }
        let Some(target) = self.stale_target() else {
            return;
        };
        let bridge = self.clone();
        drop(tokio::spawn(async move {
            swap_session(&bridge, target).await;
        }));
    }

    /// Test-only: pretend this bridge's process was spawned under `name`.
    #[cfg(test)]
    pub(in crate::active_bridge) fn set_cc_profile_for_test(&self, name: Option<&str>) {
        *self.cc_profile.lock().expect("cc_profile lock poisoned") = name.map(str::to_string);
    }
}

/// Replace the bridge's CC process with one running under `target`.
///
/// Holds `bridge.session` from the first re-check to the last broadcast. That is
/// the opposite of [`ActiveBridge::kill_session`]'s rule, and deliberately so:
/// every other holder of that mutex must see either the old live session or the
/// new one, never the gap between them, and the only thing that makes "a send
/// issued during the swap lands on the new process" true is that the sender is
/// parked on this mutex for the duration.
async fn swap_session(bridge: &Arc<ActiveBridge>, target: ResolvedProfile) {
    let mut guard = bridge.session.lock().await;

    // Re-check everything under the lock: the caller's read was outside it.
    if !bridge.cc_idle.load(Ordering::SeqCst)
        || bridge.drain_on_idle.load(Ordering::SeqCst)
        || bridge.swapping.load(Ordering::SeqCst)
        || bridge.server_shutting_down.load(Ordering::SeqCst)
    {
        return;
    }
    // Outstanding tool cards are safe across a swap: no CC-side state is held
    // open, and a tool answer arriving mid-swap or after parks on the session
    // lock and lands on the replacement process.
    match bridge.stale_target() {
        Some(t) if t.name == target.name => {}
        // Either no longer stale, or the goal moved again while this task was
        // queued; a later `reconsider_profile` handles the new target.
        _ => return,
    }
    if !guard.as_ref().is_some_and(|s| s.is_alive()) || bridge.died_handled() {
        // A dead session, or one whose death has already been accounted for, is
        // the death path's business, not the swap's.
        return;
    }

    let previous = bridge.cc_profile_name();
    info!(
        conversation_id = bridge.conversation_id,
        app_slug = %bridge.app_slug,
        from = ?previous,
        to = %target.name,
        "swapping CC session onto a new Claude profile"
    );
    bridge.broadcast(WsServerMessage::Status {
        state: CcState::Connecting,
    });
    // Cleared when this guard drops, so no exit from here on — including a
    // panic in a detached task — can leave the bridge marked as swapping.
    let _swapping = SwappingFlag::set(bridge);

    // Claim the retired process's death *before* tearing it down. The claim,
    // not the flag, is what tells the event loop that one `Died` is the swap's:
    // exactly one death can take it, so a replacement that dies a moment later
    // is an ordinary death no matter what the flag still says.
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
    *bridge.swap_ack.lock().expect("swap_ack lock poisoned") = Some(ack_tx);

    // Tear the old process down and wait for the event loop to acknowledge its
    // `Died`. A new process cannot exist until that acknowledgement, so a late
    // `Died` can never be misattributed to the new one.
    if let Some(old) = guard.take() {
        old.mark_shutting_down();
        drop(old);
    }
    match await_died_ack(bridge, ack_rx).await {
        DiedAck::Acked => {}
        DiedAck::DeathPathTookIt => {
            // The old process died on its own in the instant before the claim
            // went in, so the ordinary death arm consumed that `Died`: it has
            // alerted, marked the conversation Error and is tearing this bridge
            // down. One death path, and this is not it.
            drop(bridge.take_swap_ack());
            *bridge.cc_profile.lock().expect("cc_profile lock poisoned") = previous;
            info!(
                conversation_id = bridge.conversation_id,
                app_slug = %bridge.app_slug,
                "the old CC process died on its own as the swap began; abandoning the swap"
            );
            return;
        }
        DiedAck::TimedOut => {
            // The reader emits `Died` on EOF unconditionally, so nothing
            // consuming it within the timeout means the event loop is stuck.
            // The bridge holds no process and must not stay registered holding
            // one that never comes: hand it to the death path.
            hand_to_death_path(
                bridge,
                previous,
                CcError::ProcessDied { exit_status: None },
                guard,
                &format!(
                    "waited {DIED_ACK_TIMEOUT:?} for the retired CC process's Died \
                     acknowledgement and never got it"
                ),
            )
            .await;
            return;
        }
    }

    // The old process is gone and the lock is still held, but a SIGTERM may
    // have landed while we waited. Spawning now would put a fresh CC process —
    // and a fresh podman container — behind a shutdown path that has already
    // walked past this bridge, with nothing left to reap it.
    if bridge.server_shutting_down.load(Ordering::SeqCst) {
        info!(
            conversation_id = bridge.conversation_id,
            "server shutdown began during a profile swap; leaving the bridge without a process"
        );
        return;
    }

    // Set the name before the spawn: the new process's init frame can be
    // handled before `spawn` returns.
    *bridge.cc_profile.lock().expect("cc_profile lock poisoned") = Some(target.name.clone());

    let resume_session_id = {
        let conn = bridge.db.lock().await;
        brenn_db::conversation::get_conversation_opt(&conn, bridge.conversation_id)
            .and_then(|c| c.cc_session_id)
    };
    // The model the conversation is actually on. Without it the replacement's
    // `--model` flag would be the app default, silently discarding a
    // `model_override` the conversation was spawned with.
    let model = bridge.last_set_model.lock().await.clone();
    let host = bridge
        .swap_host
        .as_ref()
        .expect("BUG: a bridge that can be stale was spawned with a swap host");
    let spawned = host
        .respawn(RespawnRequest {
            conversation_id: bridge.conversation_id,
            profile: target.clone(),
            model,
            resume_session_id,
            events: bridge.cc_event_tx.clone(),
        })
        .await;

    let (session, ack) = match spawned {
        Ok(pair) => pair,
        Err(err) => {
            hand_to_death_path(
                bridge,
                previous,
                err,
                guard,
                &format!("the respawn onto profile {} failed", target.name),
            )
            .await;
            return;
        }
    };

    *guard = Some(session);
    // A resumed session is deliberately unseeded: whether `--resume` restores
    // the session's own model is unverified, so the first send's unconditional
    // `set_model` stays as the re-assertion.
    *bridge.last_set_model.lock().await = None;
    *bridge
        .spawn_instant
        .lock()
        .expect("spawn_instant lock poisoned") = Instant::now();

    // Which models are offered is a property of the account, which is exactly
    // what changed.
    let available_models = host.record_models(&ack.models).await;
    if !available_models.is_empty() {
        bridge.broadcast(WsServerMessage::ModelsAvailable { available_models });
    }
    bridge.broadcast(WsServerMessage::Status {
        state: CcState::Idle,
    });
    info!(
        conversation_id = bridge.conversation_id,
        profile = %target.name,
        "CC session swapped"
    );
}

/// Sets `bridge.swapping` for as long as it lives.
///
/// A flag left set has no way back: no new swap can start while it is set,
/// so a swap that exits without clearing it —
/// including by panicking in its detached task — would strand the conversation
/// for the life of the process. Clearing it in `Drop` is what makes every exit
/// path, named or not, clear it.
struct SwappingFlag<'a>(&'a std::sync::atomic::AtomicBool);

impl<'a> SwappingFlag<'a> {
    fn set(bridge: &'a ActiveBridge) -> Self {
        bridge.swapping.store(true, Ordering::SeqCst);
        Self(&bridge.swapping)
    }
}

impl Drop for SwappingFlag<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

/// How the wait for the retired process's `Died` ended.
enum DiedAck {
    /// The event loop took the swap's claim and rang it: the death is accounted
    /// for and the replacement may be spawned.
    Acked,
    /// The ordinary death arm handled that death instead — it beat the claim in
    /// — and now owns this bridge.
    DeathPathTookIt,
    /// Nothing consumed the death at all.
    TimedOut,
}

/// Wait for the event loop to account for the process the swap just retired.
///
/// Two ways it can be accounted for, because the process can die on its own an
/// instant before the swap claims it: the claim is rung (`Acked`), or the
/// ordinary death arm consumed the `Died` first and ran its clean-slate reset,
/// which is exactly what `died_handled` records. Polling for the second is what
/// keeps that race to a poll interval rather than the full timeout.
async fn await_died_ack(
    bridge: &ActiveBridge,
    ack_rx: tokio::sync::oneshot::Receiver<()>,
) -> DiedAck {
    let deadline = tokio::time::Instant::now() + DIED_ACK_TIMEOUT;
    tokio::pin!(ack_rx);
    loop {
        tokio::select! {
            acked = &mut ack_rx => {
                // `Err` is the sender dropped without ringing, which only
                // happens if the loop took the claim and then went away: the
                // process is gone either way and nobody else will report it.
                return if acked.is_ok() { DiedAck::Acked } else { DiedAck::DeathPathTookIt };
            }
            () = tokio::time::sleep(DIED_ACK_POLL) => {
                if bridge.died_handled() {
                    return DiedAck::DeathPathTookIt;
                }
                if tokio::time::Instant::now() >= deadline {
                    return DiedAck::TimedOut;
                }
            }
        }
    }
}

/// Give up on a swap that has already retired the old process, leaving the
/// bridge with none, and let the ordinary death path own what follows.
///
/// The bridge holds no process, and the loop ends on any `Died` that is not a
/// swap's — so the failure has to *produce* one, or the loop parks forever
/// holding this bridge, deregistered and invisible to the watchdog. The
/// ordinary death arm then alerts, marks the conversation Error and tears the
/// bridge down; the swap raises no alert of its own. Dropping the outstanding
/// claim first is what keeps that synthetic `Died` from being read as this
/// swap's own acknowledgement.
async fn hand_to_death_path(
    bridge: &Arc<ActiveBridge>,
    previous: Option<String>,
    err: CcError,
    guard: tokio::sync::MutexGuard<'_, Option<CcSession>>,
    what_happened: &str,
) {
    drop(bridge.take_swap_ack());
    *bridge.cc_profile.lock().expect("cc_profile lock poisoned") = previous;
    error!(
        conversation_id = bridge.conversation_id,
        app_slug = %bridge.app_slug,
        "abandoning a Claude profile swap: {what_happened} ({err})"
    );
    let events = bridge.cc_event_tx.clone();
    drop(guard);
    if events.send(SessionEvent::Died(err)).await.is_err() {
        error!(
            conversation_id = bridge.conversation_id,
            "CC event loop is gone; the abandoned swap has nothing to report to"
        );
    }
}

#[cfg(test)]
mod tests;
