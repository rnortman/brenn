//! Profile-swap tests: the staleness decision, the swap itself, and the two
//! ways it can end.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use brenn_cc::error::CcError;
use brenn_cc::session::{CcSession, InitAckInfo, ModelOption, OutgoingEnvelope};
use brenn_lib::config::{AppClaudeProfiles, ClaudeProfile, SecretString};
use brenn_obs::alerting::noop_alert_dispatcher;
use brenn_ws_types::{CcState, WsServerMessage};
use tokio::sync::{broadcast, mpsc, oneshot};

use super::*;
use crate::active_bridge::ActiveBridges;
use crate::active_bridge::cc_event_loop::cc_event_loop;
use crate::active_bridge::test_fixtures::TestBridgeConfig;
use crate::active_bridge::test_support::{drain_broadcast, make_bridge_no_loop};

/// The goal channel address for the tests that move a goal after boot.
const GOAL_ADDR: &str = "brenn:cc-profile.testapp";

/// A goal handle for one app whose allowed set is `allowed`, seeded (as
/// `ProfileGoal::new` always does) on the first entry.
fn goal_for(app: &str, allowed: &[&str]) -> Arc<brenn_cc_profile::ProfileGoal> {
    goal_handle(app, allowed, None)
}

/// The same, bound to a goal channel, so a test can publish a new goal into it
/// with `apply` the way the drain task does.
fn goal_on_channel(app: &str, allowed: &[&str], addr: &str) -> Arc<brenn_cc_profile::ProfileGoal> {
    goal_handle(app, allowed, Some(addr))
}

fn goal_handle(
    app: &str,
    allowed: &[&str],
    addr: Option<&str>,
) -> Arc<brenn_cc_profile::ProfileGoal> {
    let profiles: BTreeMap<String, ClaudeProfile> = allowed
        .iter()
        .map(|name| {
            (
                (*name).to_string(),
                ClaudeProfile {
                    token: SecretString::new(format!("token-for-{name}")),
                    expires: None,
                },
            )
        })
        .collect();
    let apps = BTreeMap::from([(
        app.to_string(),
        AppClaudeProfiles {
            allowed: allowed.iter().map(|s| (*s).to_string()).collect(),
            goal: addr.map(str::to_string),
        },
    )]);
    let (alerts, _h) = noop_alert_dispatcher();
    Arc::new(brenn_cc_profile::ProfileGoal::new(profiles, apps, alerts))
}

/// The swap's world, faked: one prepared spawn outcome, an optional gate that
/// holds `respawn` open, and a record of what was asked for.
struct FakeHost {
    outcome: Mutex<Option<Result<CcSession, CcError>>>,
    gate: Mutex<Option<oneshot::Receiver<()>>>,
    asked: Mutex<Vec<String>>,
    models: Mutex<Vec<ModelInfo>>,
}

impl FakeHost {
    fn spawning(session: CcSession) -> Arc<Self> {
        Arc::new(Self {
            outcome: Mutex::new(Some(Ok(session))),
            gate: Mutex::new(None),
            asked: Mutex::new(Vec::new()),
            models: Mutex::new(vec![ModelInfo {
                value: "sonnet".into(),
                display_name: "Sonnet".into(),
                description: "the new account's set".into(),
            }]),
        })
    }

    fn failing() -> Arc<Self> {
        Arc::new(Self {
            outcome: Mutex::new(Some(Err(CcError::SpawnFailed(std::io::Error::other(
                "no claude binary",
            ))))),
            gate: Mutex::new(None),
            asked: Mutex::new(Vec::new()),
            models: Mutex::new(Vec::new()),
        })
    }

    /// Each respawn as `<profile>/<model>`, in order.
    fn asked_for(&self) -> Vec<String> {
        self.asked.lock().expect("asked lock").clone()
    }
}

#[async_trait::async_trait]
impl ProfileSwapHost for FakeHost {
    async fn respawn(&self, req: RespawnRequest) -> Result<(CcSession, InitAckInfo), CcError> {
        self.asked.lock().expect("asked lock").push(format!(
            "{}/{}",
            req.profile.name,
            req.model.as_deref().unwrap_or("-")
        ));
        let gate = self.gate.lock().expect("gate lock").take();
        if let Some(gate) = gate {
            gate.await.expect("gate sender dropped");
        }
        let outcome = self
            .outcome
            .lock()
            .expect("outcome lock")
            .take()
            .expect("FakeHost was asked to respawn twice");
        outcome.map(|session| {
            (
                session,
                InitAckInfo {
                    models: vec![ModelOption {
                        value: "sonnet".into(),
                        display_name: "Sonnet".into(),
                        description: "the new account's set".into(),
                    }],
                },
            )
        })
    }

    async fn record_models(&self, _models: &[ModelOption]) -> Vec<ModelInfo> {
        self.models.lock().expect("models lock").clone()
    }
}

/// A bridge that is live on `running`, whose goal is the first entry of
/// `allowed`, with a running event loop and the fake host installed.
async fn swappable_bridge(
    host: Arc<dyn ProfileSwapHost>,
    allowed: &[&str],
    running: &str,
) -> (
    Arc<ActiveBridge>,
    mpsc::Sender<SessionEvent>,
    broadcast::Receiver<WsServerMessage>,
    mpsc::Receiver<OutgoingEnvelope>,
) {
    let (alerts, _h) = noop_alert_dispatcher();
    let (bridge, event_tx, event_rx, broadcast_rx, alerts, _ab) = make_bridge_no_loop(
        "testapp",
        alerts,
        TestBridgeConfig {
            cc_profiles: Some(goal_for("testapp", allowed)),
            swap_host: Some(host),
            ..Default::default()
        },
    )
    .await;
    bridge.set_cc_profile_for_test(Some(running));
    let outgoing = bridge.install_recording_session_for_test().await;
    drop(tokio::spawn(cc_event_loop(
        event_rx,
        bridge.clone(),
        alerts,
    )));
    (bridge, event_tx, broadcast_rx, outgoing)
}

/// The old process's `Died` arrives from its reader task in production. A
/// recording session has no reader, so the test plays that part: wait until the
/// swap has torn the old session down, then deliver the acknowledgement.
fn ack_the_swap_death(bridge: Arc<ActiveBridge>, event_tx: mpsc::Sender<SessionEvent>) {
    drop(tokio::spawn(async move {
        for _ in 0..200 {
            if bridge.swapping.load(Ordering::SeqCst) {
                event_tx
                    .send(SessionEvent::Died(CcError::ProcessDied {
                        exit_status: None,
                    }))
                    .await
                    .expect("event loop gone");
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("the swap never set `swapping`");
    }));
}

/// Wait until the fake host has been asked for a replacement.
async fn wait_for_respawn(host: &FakeHost) {
    for _ in 0..400 {
        if !host.asked_for().is_empty() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("the swap never reached respawn");
}

/// A swappable bridge of `app`, registered in `registry` and running its own
/// event loop. For the tests that need more than one bridge in one deployment.
///
/// `key` is the registry slot: each test bridge carries its own in-memory DB,
/// so two of them hold the same conversation id and would evict one another
/// from the registry under their own.
async fn registered_bridge(
    app: &str,
    key: i64,
    registry: &ActiveBridges,
    host: Arc<FakeHost>,
    allowed: &[&str],
) -> (Arc<ActiveBridge>, mpsc::Sender<SessionEvent>) {
    let (alerts, _h) = noop_alert_dispatcher();
    let (bridge, event_tx, event_rx, _broadcast_rx, alerts, _ab) = make_bridge_no_loop(
        app,
        alerts,
        TestBridgeConfig {
            active_bridges: Some(registry.clone()),
            cc_profiles: Some(goal_for(app, allowed)),
            swap_host: Some(host as Arc<dyn ProfileSwapHost>),
            ..Default::default()
        },
    )
    .await;
    bridge.set_cc_profile_for_test(Some("main"));
    bridge.install_recording_session_for_test().await;
    registry.insert(key, bridge.clone()).await;
    drop(tokio::spawn(cc_event_loop(
        event_rx,
        bridge.clone(),
        alerts,
    )));
    (bridge, event_tx)
}

/// Wait for a broadcast of the given shape, returning everything seen up to and
/// including it.
async fn broadcasts_until(
    rx: &mut broadcast::Receiver<WsServerMessage>,
    done: impl Fn(&WsServerMessage) -> bool,
) -> Vec<WsServerMessage> {
    let mut seen = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let msg = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting; saw {seen:?}"))
            .expect("broadcast channel closed");
        let stop = done(&msg);
        seen.push(msg);
        if stop {
            return seen;
        }
    }
}

#[tokio::test]
async fn swap_while_idle_replaces_the_session_and_reports_the_new_models() {
    let (replacement, mut new_outgoing) = CcSession::recording_for_test();
    let host = FakeHost::spawning(replacement);
    let (bridge, event_tx, mut broadcast_rx, _old_outgoing) =
        swappable_bridge(host.clone(), &["spare", "main"], "main").await;
    bridge.set_last_set_model_for_test(Some("opus")).await;

    ack_the_swap_death(bridge.clone(), event_tx.clone());
    bridge.reconsider_profile();

    let seen = broadcasts_until(&mut broadcast_rx, |m| {
        matches!(
            m,
            WsServerMessage::Status {
                state: CcState::Idle
            }
        )
    })
    .await;
    let states: Vec<&CcState> = seen
        .iter()
        .filter_map(|m| match m {
            WsServerMessage::Status { state } => Some(state),
            _ => None,
        })
        .collect();
    assert_eq!(
        states,
        vec![&CcState::Connecting, &CcState::Idle],
        "the swap reports Connecting for its duration, then Idle: {seen:?}"
    );
    assert!(
        seen.iter()
            .any(|m| matches!(m, WsServerMessage::ModelsAvailable { .. })),
        "the new account's model set must reach the picker: {seen:?}"
    );

    assert_eq!(
        host.asked_for(),
        vec!["spare/opus".to_string()],
        "the replacement starts on the model the conversation is on, not the app default"
    );
    assert_eq!(bridge.cc_profile_name().as_deref(), Some("spare"));
    assert_eq!(
        bridge.last_set_model_for_test().await,
        None,
        "a resumed session is left unseeded so the first send re-asserts the model"
    );

    bridge.send_message("after the swap").await.expect("send");
    let envelope = tokio::time::timeout(Duration::from_secs(2), new_outgoing.recv())
        .await
        .expect("no send reached the replacement")
        .expect("recording channel closed");
    assert!(format!("{:?}", envelope.msg).contains("after the swap"));
}

#[tokio::test]
async fn a_send_issued_during_the_swap_lands_on_the_new_session() {
    let (replacement, mut new_outgoing) = CcSession::recording_for_test();
    let host = FakeHost::spawning(replacement);
    let (gate_tx, gate_rx) = oneshot::channel();
    *host.gate.lock().expect("gate lock") = Some(gate_rx);
    let (bridge, event_tx, _broadcast_rx, _old_outgoing) =
        swappable_bridge(host.clone(), &["spare", "main"], "main").await;

    ack_the_swap_death(bridge.clone(), event_tx.clone());
    bridge.reconsider_profile();

    // Wait until the swap is inside `respawn`, then issue a send. It must park
    // on the session mutex rather than fail against the gap.
    for _ in 0..200 {
        if !host.asked_for().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        !host.asked_for().is_empty(),
        "the swap never reached respawn"
    );
    let sender = bridge.clone();
    let send = tokio::spawn(async move { sender.send_message("mid-swap").await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !send.is_finished(),
        "the send must wait for the new session"
    );

    gate_tx.send(()).expect("swap gone");
    tokio::time::timeout(Duration::from_secs(5), send)
        .await
        .expect("send never completed")
        .expect("send task panicked")
        .expect("the send must succeed against the new session");
    let envelope = tokio::time::timeout(Duration::from_secs(2), new_outgoing.recv())
        .await
        .expect("no send reached the replacement")
        .expect("recording channel closed");
    assert!(format!("{:?}", envelope.msg).contains("mid-swap"));
}

#[tokio::test]
async fn a_failed_respawn_becomes_an_ordinary_death() {
    let host = FakeHost::failing();
    let (bridge, event_tx, mut broadcast_rx, _outgoing) =
        swappable_bridge(host.clone(), &["spare", "main"], "main").await;
    let registry = bridge.active_bridges.clone();
    registry
        .insert(bridge.conversation_id, bridge.clone())
        .await;

    ack_the_swap_death(bridge.clone(), event_tx.clone());
    bridge.reconsider_profile();

    let seen = broadcasts_until(&mut broadcast_rx, |m| {
        matches!(
            m,
            WsServerMessage::Status {
                state: CcState::Error
            }
        )
    })
    .await;
    assert!(
        seen.iter()
            .any(|m| matches!(m, WsServerMessage::Error { .. })),
        "the ordinary death path reports the error: {seen:?}"
    );
    assert_eq!(
        bridge.cc_profile_name().as_deref(),
        Some("main"),
        "a failed swap leaves the profile it did not reach"
    );

    for _ in 0..200 {
        if registry.get(bridge.conversation_id).await.is_none() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        registry.get(bridge.conversation_id).await.is_none(),
        "a bridge holding no process must not linger registered"
    );
}

/// The states in which a stale bridge must attempt nothing. One value drives
/// both the setup and the label, so a case cannot end up testing its neighbour.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Quiet {
    MidTurn,
    Draining,
    Unprofiled,
    ShuttingDown,
    SessionAlreadyDead,
    DeathAlreadyHandled,
}

#[tokio::test]
async fn a_bridge_that_is_not_at_a_quiet_moment_does_not_swap() {
    for case in [
        Quiet::MidTurn,
        Quiet::Draining,
        Quiet::Unprofiled,
        Quiet::ShuttingDown,
        Quiet::SessionAlreadyDead,
        Quiet::DeathAlreadyHandled,
    ] {
        let label = format!("{case:?}");
        let (replacement, _rx) = CcSession::recording_for_test();
        let host = FakeHost::spawning(replacement);
        let allowed: &[&str] = if case == Quiet::Unprofiled {
            &[]
        } else {
            &["spare", "main"]
        };
        let (alerts, _h) = noop_alert_dispatcher();
        let (bridge, _event_tx, _event_rx, mut broadcast_rx, _alerts, _ab) = make_bridge_no_loop(
            "testapp",
            alerts,
            TestBridgeConfig {
                cc_profiles: (case != Quiet::Unprofiled).then(|| goal_for("testapp", allowed)),
                swap_host: Some(host.clone() as Arc<dyn ProfileSwapHost>),
                ..Default::default()
            },
        )
        .await;
        bridge.install_recording_session_for_test().await;
        if case != Quiet::Unprofiled {
            bridge.set_cc_profile_for_test(Some("main"));
        }
        match case {
            Quiet::MidTurn => bridge.cc_idle.store(false, Ordering::SeqCst),
            Quiet::Draining => bridge.drain_on_idle.store(true, Ordering::SeqCst),
            Quiet::ShuttingDown => bridge.server_shutting_down.store(true, Ordering::SeqCst),
            // Both of these are the death path's business: it owns a bridge
            // whose process is gone, and two owners would race the same reset.
            Quiet::SessionAlreadyDead => bridge
                .session
                .lock()
                .await
                .as_ref()
                .expect("a recording session is installed")
                .mark_dead_for_test(),
            Quiet::DeathAlreadyHandled => bridge.died_handled.store(true, Ordering::SeqCst),
            Quiet::Unprofiled => {}
        }

        bridge.reconsider_profile();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            host.asked_for().is_empty(),
            "{label}: no respawn may be attempted"
        );
        assert!(
            drain_broadcast(&mut broadcast_rx).is_empty(),
            "{label}: nothing is broadcast"
        );
    }
}

/// The production trigger for a goal that moved while the conversation was
/// busy: the turn end, and nothing else. Without the call in
/// `set_idle_and_drain` the agent keeps billing the old account until its
/// process dies for some unrelated reason, and nothing says so.
#[tokio::test]
async fn a_goal_that_moved_mid_turn_swaps_at_the_turn_end() {
    let (replacement, _new_outgoing) = CcSession::recording_for_test();
    let host = FakeHost::spawning(replacement);
    let (bridge, event_tx, mut broadcast_rx, _old_outgoing) =
        swappable_bridge(host.clone(), &["spare", "main"], "main").await;

    bridge.cc_idle.store(false, Ordering::SeqCst);
    bridge.reconsider_profile();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        host.asked_for().is_empty(),
        "a turn in flight is never interrupted"
    );

    ack_the_swap_death(bridge.clone(), event_tx.clone());
    crate::active_bridge::compaction::set_idle_and_drain(&bridge).await;

    let seen = broadcasts_until(&mut broadcast_rx, |m| {
        matches!(
            m,
            WsServerMessage::Status {
                state: CcState::Connecting
            }
        )
    })
    .await;
    assert!(
        seen.iter().any(|m| matches!(
            m,
            WsServerMessage::Status {
                state: CcState::Idle
            }
        )),
        "the turn end reports Idle before the swap reports Connecting: {seen:?}"
    );
    wait_for_respawn(&host).await;
    assert_eq!(host.asked_for(), vec!["spare/-".to_string()]);
}

/// The under-the-lock re-check earning its keep: the goal moved again while the
/// swap task sat in the run queue, so this swap installs nothing and leaves the
/// new target to the `reconsider_profile` that follows it.
#[tokio::test]
async fn a_goal_that_moves_again_before_the_lock_abandons_the_swap() {
    let (replacement, _rx) = CcSession::recording_for_test();
    let host = FakeHost::spawning(replacement);
    let goal = goal_on_channel("testapp", &["spare", "main", "third"], GOAL_ADDR);
    let (alerts, _h) = noop_alert_dispatcher();
    let (bridge, _event_tx, _event_rx, mut broadcast_rx, _alerts, _ab) = make_bridge_no_loop(
        "testapp",
        alerts,
        TestBridgeConfig {
            cc_profiles: Some(goal.clone()),
            swap_host: Some(host.clone() as Arc<dyn ProfileSwapHost>),
            ..Default::default()
        },
    )
    .await;
    bridge.install_recording_session_for_test().await;
    bridge.set_cc_profile_for_test(Some("main"));

    // Hold the session so the swap task parks exactly where the re-check is.
    let held = bridge.session.lock().await;
    bridge.reconsider_profile();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(goal.apply(GOAL_ADDR, "third"), vec!["testapp".to_string()]);
    drop(held);

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        host.asked_for().is_empty(),
        "the swap must not install the account the goal has already moved off"
    );
    assert!(drain_broadcast(&mut broadcast_rx).is_empty());
}

/// A SIGTERM that lands while the swap waits for its acknowledgement. The
/// retired process's death is still the swap's — it is not reclassified as an
/// intentional shutdown and reset — but no replacement may be started behind a
/// shutdown path that has already walked past this bridge.
#[tokio::test]
async fn a_shutdown_during_the_ack_wait_leaves_the_bridge_without_a_process() {
    let (replacement, _rx) = CcSession::recording_for_test();
    let host = FakeHost::spawning(replacement);
    let (bridge, event_tx, mut broadcast_rx, _old_outgoing) =
        swappable_bridge(host.clone(), &["spare", "main"], "main").await;

    let shutdown_then_ack = bridge.clone();
    let ack_tx = event_tx.clone();
    drop(tokio::spawn(async move {
        for _ in 0..200 {
            if shutdown_then_ack.swapping.load(Ordering::SeqCst) {
                shutdown_then_ack
                    .server_shutting_down
                    .store(true, Ordering::SeqCst);
                ack_tx
                    .send(SessionEvent::Died(CcError::ProcessDied {
                        exit_status: None,
                    }))
                    .await
                    .expect("event loop gone");
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("the swap never set `swapping`");
    }));
    bridge.reconsider_profile();

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        host.asked_for().is_empty(),
        "no CC process, and no podman container, may start during shutdown"
    );
    assert!(bridge.session.lock().await.is_none());
    let seen = drain_broadcast(&mut broadcast_rx);
    assert!(
        !seen.iter().any(|m| matches!(
            m,
            WsServerMessage::Status {
                state: CcState::Error
            }
        )),
        "the swap's own teardown is not a death to report: {seen:?}"
    );
    assert!(
        !bridge.died_handled.load(Ordering::SeqCst),
        "the swap's `Died` must not run the clean-slate reset"
    );
}

/// The replacement dies moments after it is installed. The swap's claim was
/// spent on the process it retired, so this is an ordinary death — anything
/// else leaves a registered bridge with a dead session that nothing reaps.
#[tokio::test]
async fn a_replacement_that_dies_right_after_the_swap_is_an_ordinary_death() {
    let (replacement, _new_outgoing) = CcSession::recording_for_test();
    let host = FakeHost::spawning(replacement);
    let (bridge, event_tx, mut broadcast_rx, _old_outgoing) =
        swappable_bridge(host.clone(), &["spare", "main"], "main").await;

    ack_the_swap_death(bridge.clone(), event_tx.clone());
    bridge.reconsider_profile();
    wait_for_respawn(&host).await;
    broadcasts_until(&mut broadcast_rx, |m| {
        matches!(
            m,
            WsServerMessage::Status {
                state: CcState::Idle
            }
        )
    })
    .await;

    event_tx
        .send(SessionEvent::Died(CcError::ProcessDied {
            exit_status: None,
        }))
        .await
        .expect("event loop gone");

    let seen = broadcasts_until(&mut broadcast_rx, |m| {
        matches!(
            m,
            WsServerMessage::Status {
                state: CcState::Error
            }
        )
    })
    .await;
    assert!(
        seen.iter()
            .any(|m| matches!(m, WsServerMessage::Error { .. })),
        "the replacement's death is reported like any other: {seen:?}"
    );
}

/// Nothing acknowledges the retired process's death. The bridge holds no
/// process, so it must not be left registered waiting for one that never comes:
/// the swap hands it to the ordinary death path instead of bricking it.
#[tokio::test(start_paused = true)]
async fn an_unacknowledged_teardown_hands_the_bridge_to_the_death_path() {
    let (replacement, _rx) = CcSession::recording_for_test();
    let host = FakeHost::spawning(replacement);
    let (bridge, _event_tx, mut broadcast_rx, _old_outgoing) =
        swappable_bridge(host.clone(), &["spare", "main"], "main").await;
    let registry = bridge.active_bridges.clone();
    registry
        .insert(bridge.conversation_id, bridge.clone())
        .await;

    // No `ack_the_swap_death` here: the acknowledgement never comes. Time is
    // paused, so the wait for it costs the test nothing but simulated seconds.
    bridge.reconsider_profile();

    for _ in 0..1000 {
        if registry.get(bridge.conversation_id).await.is_none() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let seen = drain_broadcast(&mut broadcast_rx);
    assert!(
        seen.iter()
            .any(|m| matches!(m, WsServerMessage::Error { .. })),
        "the ordinary death path reports the error: {seen:?}"
    );
    assert!(
        seen.iter().any(|m| matches!(
            m,
            WsServerMessage::Status {
                state: CcState::Error
            }
        )),
        "and marks the conversation errored for the tabs watching it: {seen:?}"
    );
    assert!(
        host.asked_for().is_empty(),
        "an unacknowledged teardown never reaches the respawn"
    );
    assert!(
        !bridge.swapping.load(Ordering::SeqCst),
        "a bridge left marked as swapping can never swap again"
    );
    assert_eq!(
        bridge.cc_profile_name().as_deref(),
        Some("main"),
        "an abandoned swap leaves the profile it did not reach"
    );
    for _ in 0..200 {
        if registry.get(bridge.conversation_id).await.is_none() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        registry.get(bridge.conversation_id).await.is_none(),
        "a bridge holding no process must not linger registered"
    );
}

/// The drain task's fan-out: the bridges of the app whose goal moved, and only
/// those. Waking every bridge in the deployment would swap conversations of
/// other users' apps that are on exactly the account their own goal names.
#[tokio::test]
async fn a_goal_change_reaches_only_the_bridges_of_that_app() {
    let registry = ActiveBridges::new();
    let (moved_replacement, _rx1) = CcSession::recording_for_test();
    let moved_host = FakeHost::spawning(moved_replacement);
    let (moved, moved_tx) = registered_bridge(
        "movedapp",
        1,
        &registry,
        moved_host.clone(),
        &["spare", "main"],
    )
    .await;
    let (other_replacement, _rx2) = CcSession::recording_for_test();
    let other_host = FakeHost::spawning(other_replacement);
    let (other, _other_tx) = registered_bridge(
        "otherapp",
        2,
        &registry,
        other_host.clone(),
        &["spare", "main"],
    )
    .await;

    ack_the_swap_death(moved.clone(), moved_tx);
    registry
        .reconsider_profiles(&["movedapp".to_string()])
        .await;

    wait_for_respawn(&moved_host).await;
    assert_eq!(moved_host.asked_for(), vec!["spare/-".to_string()]);
    assert!(
        other_host.asked_for().is_empty(),
        "an app whose goal did not move is left alone"
    );
    assert_eq!(other.cc_profile_name().as_deref(), Some("main"));
}
