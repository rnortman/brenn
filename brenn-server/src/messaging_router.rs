//! `WakeRouter` adapter implementing `brenn_lib::messaging::WakeRouter`
//! over `ActiveBridges` + `AppState`.
//!
//! `Messenger` lives in `brenn-lib` and must not depend on binary-crate
//! types; this adapter is the single bridge across that boundary. Held
//! on `AppState` indirectly via `Arc<dyn WakeRouter>` inside `Messenger`.

use std::collections::HashMap;
use std::sync::Arc;

use brenn_lib::messaging::store::{DeferredMessage, SurfaceFeedTarget};
use brenn_lib::messaging::{
    DeliveryShape, MessageEnvelope, ParticipantId, SubscriberEntryKind, WakeRouter,
};
use brenn_lib::obs::alerting::{AlertDispatcher, AlertSeverity};
use chrono_tz::Tz;
use tracing::{debug, warn};

use crate::active_bridge::ActiveBridges;
use crate::routes::surface::SubKey;
use crate::routes::surface::registry::{DeferredViewPush, DurableDelivery, SessionPush};
use crate::routes::surface::session::deferred_view_entries;
use crate::state::AppState;
use crate::system_message::render_event_drain;

/// Concrete `WakeRouter` impl. Closes over `ActiveBridges` + a clone of
/// the `AppState` so it can call `spawn_eager_wake`.
pub struct WakeRouterImpl {
    active_bridges: ActiveBridges,
    /// `AppState` is constructed after the router (the router is one of
    /// the fields on `AppState`). `set_state` runs immediately after
    /// `AppState` construction in `main.rs` — and crucially before the
    /// background tasks that may call `spawn_eager_wake`. By the time
    /// any caller hits the `spawn_eager_wake` path, `state` is `Some`.
    /// A `None` here at call time is an invariant violation; we panic
    /// (per CLAUDE.md "BETTER DEAD THAN WRONG" — never silently no-op on a
    /// structural invariant violation).
    state: tokio::sync::OnceCell<AppState>,
    /// Alert dispatcher for push-overflow alarms. Wired at
    /// construction by the binary crate bootstrap, which has access to the
    /// already-built `AlertDispatcher`. `None` when no alert dispatcher is
    /// configured (e.g. in tests that don't need alarm wiring).
    alert_dispatcher: Option<AlertDispatcher>,
    /// Every subscriber's declared delivery mechanism, keyed by its
    /// [`SubscriberEntryKind`]. Populated by [`Self::register_delivery_binding`]
    /// at bootstrap, before any publish path — one entry per configured app,
    /// WASM consumer, system component, and surface. `deliver` /
    /// `deliver_ingress` / `spawn_eager_wake` / `delivery_shape` resolve the
    /// binding by key and act on the binding variant, never on the identity
    /// prefix. A missing binding at dispatch time is a host-wiring invariant
    /// violation → panic.
    bindings: std::sync::RwLock<HashMap<SubscriberEntryKind, DeliveryBinding>>,
}

/// How a subscriber is woken and delivered to. Registered at boot behind the
/// subscriber's [`SubscriberEntryKind`]; the live dispatch path matches on the
/// variant rather than on the identity prefix.
pub(crate) enum DeliveryBinding {
    /// Off-loop task parked on a `Notify`; never delivered inline through the
    /// shared dispatch loop (WASM consumers, system subscribers). The off-loop
    /// dispatch task holds an `Arc` clone and awaits it; `spawn_eager_wake`
    /// calls `notify_one`.
    ParkedNotify(Arc<tokio::sync::Notify>),
    /// Deliver via the conversation's active bridge; wake via
    /// `state.spawn_eager_wake` (app subscribers).
    ConversationBridge,
    /// Claim-and-fan-out to attached, subscribed surface sessions.
    SurfaceSessions,
}

impl WakeRouterImpl {
    /// Build the adapter with `active_bridges` set and `state`
    /// uninitialized. Call [`Self::set_state`] once the `AppState`
    /// becomes available — must happen before any background task that
    /// can invoke `spawn_eager_wake`.
    pub fn new(active_bridges: ActiveBridges) -> Self {
        Self {
            active_bridges,
            state: tokio::sync::OnceCell::new(),
            alert_dispatcher: None,
            bindings: std::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Attach the alert dispatcher for push-overflow alarms. Called by the
    /// binary crate bootstrap after the dispatcher is built. Must be called
    /// before any publish path that uses `noise = Alarm`. Must not be called
    /// more than once — double-init is a structural bug; panics rather than
    /// silently replacing the dispatcher.
    pub fn set_alert_dispatcher(&mut self, dispatcher: AlertDispatcher) {
        assert!(
            self.alert_dispatcher.is_none(),
            "set_alert_dispatcher called twice — double init is a structural bug"
        );
        self.alert_dispatcher = Some(dispatcher);
    }

    /// Fill in the `AppState`. Idempotent: calling twice with different
    /// values panics.
    pub fn set_state(&self, state: AppState) {
        self.state
            .set(state)
            .map_err(|_| ())
            .expect("WakeRouterImpl state already set");
    }

    /// Register the delivery binding for one subscriber, keyed by its
    /// [`SubscriberEntryKind`]. Called at bootstrap for every configured app
    /// (`ConversationBridge`), WASM consumer / system component (`ParkedNotify`
    /// with the off-loop task's `Notify`), and surface (`SurfaceSessions`),
    /// before any publish path runs. Duplicate registration for the same key is
    /// a bootstrap wiring bug → panic.
    pub(crate) fn register_delivery_binding(
        &self,
        key: SubscriberEntryKind,
        binding: DeliveryBinding,
    ) {
        let mut map = self.bindings.write().expect("bindings RwLock poisoned");
        let prev = map.insert(key.clone(), binding);
        assert!(
            prev.is_none(),
            "register_delivery_binding called twice for {key:?} — bootstrap wiring bug"
        );
    }

    /// Whether a delivery binding is registered for `key`. Used by the boot
    /// cross-check to assert every directory subscriber has one before any
    /// publish can reach the dispatch path (a missing binding at dispatch time
    /// panics; the cross-check turns that into a named boot failure).
    pub(crate) fn has_delivery_binding(&self, key: &SubscriberEntryKind) -> bool {
        chat_conversation(key).is_some()
            || self
                .bindings
                .read()
                .expect("bindings RwLock poisoned")
                .contains_key(key)
    }

    /// Resolve a subscriber's delivery route from its registered binding,
    /// releasing the lock before the (async) delivery work runs. A missing
    /// binding is a host-wiring invariant violation → panic.
    fn delivery_route(&self, key: &SubscriberEntryKind) -> DeliveryRoute {
        let map = self.bindings.read().expect("bindings RwLock poisoned");
        match map.get(key) {
            Some(DeliveryBinding::ConversationBridge) => DeliveryRoute::ConversationBridge,
            Some(DeliveryBinding::SurfaceSessions) => DeliveryRoute::SurfaceSessions,
            Some(DeliveryBinding::ParkedNotify(_)) => DeliveryRoute::Parked,
            None => panic!(
                "no delivery binding registered for {key:?} — host-wiring invariant violated \
                 (every subscriber gets a binding at bootstrap)"
            ),
        }
    }
}

/// The conversation `key` is the chat subscription of, if it is one.
///
/// The one place this kind's delivery mechanism is decided, and the one kind
/// decided this way rather than from the registered binding map. Every other
/// subscriber is declared in config and bound at bootstrap, so a binding table
/// is what keeps a new kind from silently inheriting someone else's delivery
/// path. A chat subscription is minted at runtime, one per conversation, and has
/// exactly one mechanism by construction — its conversation's own bridge. A
/// per-conversation row in that table would carry no information and would have
/// to be torn down in step with the channels.
fn chat_conversation(key: &SubscriberEntryKind) -> Option<i64> {
    match key {
        SubscriberEntryKind::ChatConversation {
            conversation_id, ..
        } => Some(*conversation_id),
        _ => None,
    }
}

/// The retention position a surface delivery names, as `u64`.
///
/// Every message a surface subscription is delivered sits in its channel's
/// retention order: the store assigns positions from 1 and never re-uses one, so
/// a negative position means the message was delivered from outside retention,
/// which no path produces.
fn retention_position(retained_seq: i64) -> u64 {
    u64::try_from(retained_seq).unwrap_or_else(|_| {
        panic!(
            "surface durable delivery: retention position {retained_seq} is negative — a \
             delivered bus message is always in retention"
        )
    })
}

/// The delivery mechanism for a subscriber, resolved from its [`DeliveryBinding`]
/// without carrying the lock guard into async delivery.
enum DeliveryRoute {
    ConversationBridge,
    SurfaceSessions,
    Parked,
}

#[async_trait::async_trait]
impl WakeRouter for WakeRouterImpl {
    async fn deliver(
        &self,
        key: &SubscriberEntryKind,
        envelope: &Arc<MessageEnvelope>,
        retained_seq: i64,
    ) -> Result<bool, String> {
        // Row-less fan-out to attached, subscribed sessions. A surface holds no
        // server-side delivery state: the client's echoed cursor is the whole of
        // it, so this hands the envelope over and arbitrates nothing. The session
        // drops a copy its cursor already covers, and a session that missed one —
        // queue full, or the frame lost — resumes past its cursor and is served
        // the suffix. `try_send` (not awaited): a hung session must not stall the
        // shared fan-out task.
        //
        // Only surface subscribers reach here: `surface_feed_targets` is the one
        // caller's target set, and every other kind holds a position and is served
        // from it.
        let SubscriberEntryKind::Surface { slug, .. } = key else {
            panic!(
                "WakeRouter::deliver called for non-surface subscriber {key:?} — only a surface \
                 subscription takes the row-less live feed"
            );
        };
        let state = self
            .state
            .get()
            .expect("WakeRouter state must be set before any Surface deliver call");
        let retained_seq = retention_position(retained_seq);

        // The subscription this delivery belongs to: the principal the feed target
        // was resolved for, on the message's channel. `key` is the subscriber's
        // registration key, so its instance half is the principal — never
        // re-derived from the envelope.
        let sub = SubKey {
            instance: key.surface_subscriber_instance().to_owned(),
            channel: envelope.channel.clone(),
        };

        // 1. Sessions holding this exact subscription. Filtering on the whole
        //    subscription and not the channel is what keeps the delivery off a
        //    sibling instance's ports: siblings are separate principals with
        //    separate cursors, and this delivery is one principal's.
        let subscribed: Vec<_> = state
            .surface_registry
            .sessions(slug)
            .into_iter()
            .filter(|h| h.is_subscribed(&sub))
            .collect();

        // 2. None attached+subscribed → nothing was owed to a disconnected
        //    session; it resumes past its own cursor.
        if subscribed.is_empty() {
            return Ok(false);
        }

        // 3. Fan out to every subscribed session, each cloning the caller's
        //    refcount — the shared envelope is never deep-copied per session.
        let mut accepted = 0usize;
        let mut rejected = 0usize;
        for handle in &subscribed {
            let delivery = DurableDelivery {
                envelope: envelope.clone(),
                retained_seq,
                sub: sub.clone(),
            };
            if handle
                .push_tx
                .try_send(SessionPush::Durable(delivery))
                .is_ok()
            {
                accepted += 1;
            } else {
                rejected += 1;
            }
        }
        if rejected > 0 {
            debug!(
                slug = %slug,
                channel = %envelope.channel,
                retained_seq,
                rejected,
                accepted,
                "surface durable live delivery: some session queues full; those \
                 sessions are served the suffix by the drain nudge below"
            );
        }

        // 4. Per-delivery drain nudge on every subscribed session: serves the
        //    suffix above each session's cursor, which is what recovers a
        //    queue-full session. A spurious pass over a caught-up cursor is one
        //    indexed retention read per active channel.
        for handle in &subscribed {
            handle.drain_notify.notify_one();
        }

        // 5. ≥1 accepted → this delivery landed somewhere. Zero (every queue
        //    full/closed) → report it; the sessions catch up from their cursors
        //    either way.
        Ok(accepted >= 1)
    }

    async fn deliver_context(
        &self,
        key: &SubscriberEntryKind,
        envelope: &Arc<MessageEnvelope>,
        retained_seq: i64,
    ) {
        // Row-less deliver-if-attached fan-out for a fold-0 surface subscription.
        // No push row: a fold-0 subscription has no push window, so the message
        // reaches an attached session only here, live.
        let SubscriberEntryKind::Surface { slug, .. } = key else {
            // `resolve_context_targets` filters to Surface subscribers, so any
            // other kind here is a caller-side wiring bug.
            panic!(
                "deliver_context called for non-surface subscriber {key:?} — only fold-0 \
                 surface subscriptions take the row-less context feed"
            );
        };
        let state = self
            .state
            .get()
            .expect("WakeRouter state must be set before any deliver_context call");

        let sub = SubKey {
            instance: key.surface_subscriber_instance().to_owned(),
            channel: envelope.channel.clone(),
        };

        // Sessions holding this exact subscription, per-principal. None attached
        // → nothing owed to a disconnected session; its retained context arrives
        // at the next subscribe/resume.
        let subscribed: Vec<_> = state
            .surface_registry
            .sessions(slug)
            .into_iter()
            .filter(|h| h.is_subscribed(&sub))
            .collect();
        if subscribed.is_empty() {
            return;
        }

        for handle in &subscribed {
            let delivery = DurableDelivery {
                envelope: envelope.clone(),
                retained_seq: retention_position(retained_seq),
                sub: sub.clone(),
            };
            if handle
                .push_tx
                .try_send(SessionPush::Durable(delivery))
                .is_err()
            {
                // Full queue: a fold-0 subscription holds no cursor, so nothing
                // is owed and the loss is real — the wire's silence is the
                // contract, and recovery is the retained window at the next
                // subscribe/resume. A push-enabled subscription instead resumes
                // from its own position and re-reads what the drop skipped.
                warn!(
                    slug = %slug,
                    channel = %envelope.channel,
                    retained_seq,
                    "surface durable depth-0 context feed: session queue full; row-less \
                     delivery dropped (recovered at the next resume)"
                );
            }
        }
    }

    async fn push_surface_deferred_view(
        &self,
        slug: &str,
        instance: &str,
        channel: &str,
        view: &[DeferredMessage],
    ) {
        // The release sweep's only route to a page. It runs on the dispatcher
        // loop, which starts after `set_state`, so an unset state here is the
        // same wiring violation `deliver` reports.
        let state = self
            .state
            .get()
            .expect("WakeRouter state must be set before any deferred-view push");
        state.surface_registry.push_deferred_view(
            slug,
            &DeferredViewPush {
                channel: channel.to_string(),
                instance: instance.to_string(),
                entries: deferred_view_entries(view),
            },
        );
    }

    fn any_surface_session_attached(&self, slug: &str) -> bool {
        let Some(state) = self.state.get() else {
            // No state wired yet — no session can be attached.
            return false;
        };
        state.surface_registry.count(slug) > 0
    }

    fn any_surface_session_subscribed(&self, channel: &str, targets: &[SurfaceFeedTarget]) -> bool {
        let Some(state) = self.state.get() else {
            // No state wired yet — no session can be attached.
            return false;
        };
        targets.iter().any(|target| {
            let key = &target.kind;
            let SubscriberEntryKind::Surface { slug, .. } = key else {
                return false;
            };
            let sub = SubKey {
                instance: key.surface_subscriber_instance().to_owned(),
                channel: channel.to_owned(),
            };
            state
                .surface_registry
                .sessions(slug)
                .into_iter()
                .any(|h| h.is_subscribed(&sub))
        })
    }

    async fn deliver_ingress(
        &self,
        key: &SubscriberEntryKind,
        subscriber: &ParticipantId,
        event: &brenn_lib::messaging::ingress::Event,
    ) -> Result<bool, String> {
        match self.delivery_route(key) {
            DeliveryRoute::ConversationBridge => {
                let conversation_id = subscriber.as_conversation_id();
                let bridge = match self.active_bridges.get(conversation_id).await {
                    Some(b) => b,
                    None => return Ok(false),
                };
                let rendered =
                    render_event_drain(std::slice::from_ref(event)).unwrap_or_else(|| {
                        panic!(
                            "render_event_drain returned None for single-element ingress event \
                             (source={:?}, summary={:?}); format_event_batch contract violated",
                            event.source, event.summary
                        )
                    });
                match bridge.send_system_message(rendered, None).await {
                    Ok(()) => Ok(true),
                    Err(e) => Err(e),
                }
            }
            // Ingress is conversation-targeted by invariant: surfaces bind
            // brenn:/ephemeral: channels, parked subscribers (WASM/system) take
            // bus tool requests, and neither intersects the webhook:/mqtt:
            // channels ingress arrives on. A non-conversation target here is a
            // host-wiring invariant violation.
            DeliveryRoute::SurfaceSessions | DeliveryRoute::Parked => {
                panic!(
                    "WakeRouter::deliver_ingress called for non-conversation subscriber {key:?} — \
                     host-wiring invariant violated: ingress rows only target conversations"
                );
            }
        }
    }

    fn spawn_eager_wake(&self, key: &SubscriberEntryKind, subscriber: &ParticipantId) {
        // A chat subscription's wake is its conversation's spawn — the same
        // spawn an app subscriber's wake buys, subject to the same backoff. What
        // the woken bridge does with it differs (its adapter drains the command
        // channel), and that is the adapter's business, not the wake's.
        //
        // The one thing the wake does decide is the hold: a peer asked for this
        // conversation by name, so the bridge it buys is the bus door's to
        // account for, whether or not the adapter's drain finds anything left to
        // serve by the time it runs.
        if let Some(conversation_id) = chat_conversation(key) {
            let state = self
                .state
                .get()
                .expect("WakeRouter state must be set before any spawn_eager_wake call");
            state.spawn_chat_wake(conversation_id, Tz::UTC);
            return;
        }
        // Resolve the binding under the read lock; the wake work (notify / state
        // call) is sync so holding the guard across it is fine.
        let map = self.bindings.read().expect("bindings RwLock poisoned");
        match map.get(key) {
            // Notify the off-loop parked dispatch task (WASM consumer or system
            // component, e.g. the tool executor). The task holds an `Arc` clone;
            // `notify_one` sets its permit.
            Some(DeliveryBinding::ParkedNotify(notify)) => {
                notify.notify_one();
            }
            Some(DeliveryBinding::ConversationBridge) => {
                let conversation_id = subscriber.as_conversation_id();
                // `set_state` runs in main.rs before any task that can reach
                // this code path. A None here is a structural-invariant
                // violation; panic loudly rather than silently drop the wake.
                let state = self
                    .state
                    .get()
                    .expect("WakeRouter state must be set before any spawn_eager_wake call");
                // Autonomous wake — no browser-reported timezone available.
                // UTC is acceptable because every Graf tool requires a `today` param except for
                // those where a few hours' difference is not usually critical (e.g. query horizon).
                //
                // Bus-driven, so the spawn backoff applies: every trigger the
                // walk and the dispatcher have — kick, tick, deadline, urgency —
                // reaches a conversation through this one call.
                state.spawn_bus_wake(conversation_id, Tz::UTC);
            }
            // Nudge every attached session of this slug to run a drain pass. No
            // per-channel filter (the wake carries only the participant): the
            // session drains all its active durable channels. No sessions → no-op;
            // parked rows wait for the next attach.
            Some(DeliveryBinding::SurfaceSessions) => {
                let slug = key.slug();
                let state = self
                    .state
                    .get()
                    .expect("WakeRouter state must be set before any spawn_eager_wake call");
                for handle in state.surface_registry.sessions(slug) {
                    handle.drain_notify.notify_one();
                }
            }
            None => panic!(
                "spawn_eager_wake: no delivery binding registered for {key:?} — \
                 host-wiring invariant violated"
            ),
        }
    }

    /// A conversation that already has a live bridge is served from its position
    /// right here: it is awake, so the spawn-shaped wake would find its bridge
    /// running and return without delivering anything, and the backlog would
    /// wait for whatever the conversation did next. Every other binding's wake
    /// is its delivery trigger, so the default is the whole answer for them.
    ///
    /// Liveness is asked before the verdict, and that order is the rule:
    /// `spawn_permitted` prices a subprocess, and a bridge already running costs
    /// none, so a live conversation is served its whole owed backlog at any
    /// urgency. Only the spawn arm consults the verdict — a sleeping
    /// conversation whose backlog is below `wake_min` waits for its next natural
    /// drain or its deadline, which is what `wake_min` means.
    ///
    /// The delivery itself runs on its own task. The walk that calls this is
    /// awaited by the dispatcher loop, and serving a live bridge renders markdown
    /// and writes to a CC subprocess — inline, one stalled session would hold up
    /// releases, deadline wakes, and every other channel's wakes behind it.
    async fn wake_owed(
        &self,
        key: &SubscriberEntryKind,
        subscriber: &ParticipantId,
        spawn_permitted: bool,
    ) {
        // A live conversation's chat backlog costs a notify, so it is served
        // whatever the verdict says — the same rule the arm below applies, for
        // the same reason. The notify coalesces, so a burst of commands and a
        // pre-warm landing together cost one drain. Nothing is delivered from
        // here: the adapter owns the read, because acting on a command is not
        // the same as rendering one into a conversation.
        if let Some(conversation_id) = chat_conversation(key) {
            if let Some(bridge) = self.active_bridges.get(conversation_id).await {
                bridge.chat_commands.notify_one();
                return;
            }
            if spawn_permitted {
                self.spawn_eager_wake(key, subscriber);
            }
            return;
        }
        let conversation_bridge = matches!(
            self.bindings
                .read()
                .expect("bindings RwLock poisoned")
                .get(key),
            Some(DeliveryBinding::ConversationBridge)
        );
        if conversation_bridge
            && let Some(bridge) = self
                .active_bridges
                .get(subscriber.as_conversation_id())
                .await
        {
            let subscriber_key = subscriber.as_str().to_string();
            drop(tokio::spawn(async move {
                if let Err(e) = crate::active_bridge::deliver_conversation_backlog(&bridge).await {
                    // The bridge was live and the send failed: the position did
                    // not move, so the next walk finds the same backlog and
                    // tries again.
                    warn!(
                        subscriber = %subscriber_key,
                        "wake walk: delivery to a live bridge failed: {e}"
                    );
                }
            }));
            return;
        }
        if spawn_permitted {
            self.spawn_eager_wake(key, subscriber);
        }
    }

    fn delivery_shape(&self, key: &SubscriberEntryKind) -> DeliveryShape {
        // Inline, in the sense the shape means: it is served where it stands
        // rather than parked for an off-loop task, and its wake can cost a
        // subprocess, so the wake pass reads its urgency threshold.
        if chat_conversation(key).is_some() {
            return DeliveryShape::Inline;
        }
        let map = self.bindings.read().expect("bindings RwLock poisoned");
        match map.get(key) {
            Some(DeliveryBinding::ConversationBridge) | Some(DeliveryBinding::SurfaceSessions) => {
                DeliveryShape::Inline
            }
            Some(DeliveryBinding::ParkedNotify(_)) => DeliveryShape::ParkedWake,
            None => panic!(
                "delivery_shape: no delivery binding registered for {key:?} — \
                 host-wiring invariant violated"
            ),
        }
    }

    fn alarm(&self, channel: &str, subscriber: &ParticipantId, count: u64) {
        // Production bootstrap always calls `set_alert_dispatcher` before any publish;
        // tests that need the `alarm` path use mock WakeRouter implementations
        // (AlarmCountingRouter / FakeWakeRouter), not WakeRouterImpl. A None dispatcher
        // here means the production bootstrap wiring regressed — panic rather than
        // silently downgrading noise=Alarm channels to a log-only warning.
        let dispatcher = self.alert_dispatcher.as_ref().unwrap_or_else(|| {
            panic!(
                "WakeRouterImpl::alarm called but alert_dispatcher not set — \
                 call set_alert_dispatcher before any publish path can run"
            )
        });
        dispatcher.alert(
            AlertSeverity::Warning,
            "Push-depth overflow".to_string(),
            format!(
                "Channel {channel:?} subscriber {:?}: {count} message(s) passed the \
                 subscriber's position unread and are gone (noise = alarm).",
                subscriber.as_str()
            ),
        );
    }

    fn position_ahead_of_retention(
        &self,
        channel: &str,
        subscriber: &ParticipantId,
        position: u64,
        head: u64,
    ) {
        // Same wiring rule as `alarm`: production bootstrap sets the dispatcher
        // before the Messenger it hands to this router can run a boot reconcile.
        let dispatcher = self.alert_dispatcher.as_ref().unwrap_or_else(|| {
            panic!(
                "WakeRouterImpl::position_ahead_of_retention called but alert_dispatcher not set \
                 — call set_alert_dispatcher before the boot reconcile can run"
            )
        });
        dispatcher.alert(
            AlertSeverity::Warning,
            "Subscriber position ahead of retention".to_string(),
            format!(
                "Channel {channel:?} subscriber {:?}: position {position} stands above the \
                 channel's head {head}, which no append can produce — the database was restored \
                 under a position that outlived it. The position was reset to head; whatever the \
                 subscriber missed is uncountable.",
                subscriber.as_str()
            ),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The registration key a conversation row resolves to (its backing app).
    /// The tests below register a `ConversationBridge` binding under it before
    /// calling `deliver`/`spawn_eager_wake`, mirroring bootstrap wiring.
    fn conv_key() -> SubscriberEntryKind {
        SubscriberEntryKind::App("test-app".to_string())
    }

    /// The registration key for the `deskbar` surface's `protobar` instance —
    /// the principal these surface tests deliver to. Instance-grained, because
    /// the router now resolves both the route and the target subscription from
    /// this key.
    fn surface_key() -> SubscriberEntryKind {
        SubscriberEntryKind::Surface {
            slug: "deskbar".to_string(),
            instance: Some("protobar".to_string()),
        }
    }

    /// The push-enabled feed target for [`surface_key`], as the publish path
    /// resolves it.
    fn surface_feed_target() -> SurfaceFeedTarget {
        SurfaceFeedTarget {
            kind: surface_key(),
            subscriber: ParticipantId::for_surface_component("deskbar", "protobar"),
            push_enabled: true,
        }
    }

    /// `WakeRouterImpl::new` leaves `state` unset; the caller must
    /// invoke `set_state` before any background task that can reach
    /// `spawn_eager_wake`.
    #[test]
    fn new_leaves_state_unset() {
        let router = WakeRouterImpl::new(ActiveBridges::new());
        assert!(router.state.get().is_none());
    }

    /// **A pushed deferred view reaches every session of its surface, and only
    /// that surface.** The parked set belongs to the sub-identity every tab
    /// shares, so the sweep's push is a broadcast; a neighbouring surface's
    /// sessions are no part of it.
    #[tokio::test]
    async fn a_pushed_deferred_view_reaches_every_session_of_its_surface() {
        use brenn_lib::messaging::{ChannelScheme, Urgency};
        use chrono::{TimeZone, Utc};

        use crate::routes::surface::registry::{
            PUSH_QUEUE_FRAMES, SessionCaps, SurfaceSessionHandle,
        };

        let db = brenn_lib::db::init_db_memory();
        let state = crate::test_support::state::test_state(&db);
        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.set_state(state.clone());

        let attach = |slug: &str| {
            let (push_tx, push_rx) = tokio::sync::mpsc::channel(PUSH_QUEUE_FRAMES);
            let mut handle = SurfaceSessionHandle::for_test("dev");
            handle.push_tx = push_tx;
            let guard = state
                .surface_registry
                .try_register(slug, handle, SessionCaps::UNCAPPED)
                .expect("the test registry is uncapped");
            (guard, push_rx)
        };
        let (_deskbar_one, mut deskbar_one) = attach("deskbar");
        let (_deskbar_two, mut deskbar_two) = attach("deskbar");
        let (_kitchen, mut kitchen) = attach("kitchen");

        let release_at = Utc.timestamp_opt(1_800_000_060, 0).unwrap();
        let view = vec![DeferredMessage {
            release_at,
            envelope: Arc::new(MessageEnvelope {
                message_id: uuid::Uuid::nil(),
                source: "node".to_string(),
                channel: "brenn:sched".to_string(),
                sender: "surface:deskbar#protobar".to_string(),
                publish_ts: release_at,
                body: "wake me".to_string(),
                reply_to: None,
                delivery_deadline: None,
                deliver_after: Some(release_at),
                urgency: Urgency::Normal,
                envelope_type: ChannelScheme::Brenn,
            }),
        }];
        router
            .push_surface_deferred_view("deskbar", "protobar", "brenn:sched", &view)
            .await;

        for queue in [&mut deskbar_one, &mut deskbar_two] {
            let SessionPush::DeferredView(pushed) = queue.try_recv().expect("view pushed") else {
                panic!("expected a deferred view");
            };
            assert_eq!(pushed.channel, "brenn:sched");
            assert_eq!(pushed.instance, "protobar");
            assert_eq!(pushed.entries.len(), 1);
            assert_eq!(pushed.entries[0].body, "wake me");
            assert_eq!(
                pushed.entries[0].deliver_after, 1_800_000_060_000,
                "the release time travels as epoch milliseconds, the page's units"
            );
        }
        assert!(
            kitchen.try_recv().is_err(),
            "another surface's sessions hold no part of this sender's set"
        );
    }

    #[test]
    #[should_panic(expected = "WakeRouter state must be set")]
    fn spawn_eager_wake_panics_when_state_unset() {
        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.register_delivery_binding(conv_key(), DeliveryBinding::ConversationBridge);
        router.spawn_eager_wake(&conv_key(), &ParticipantId::for_conversation(42));
    }

    /// A live bridge whose conversation is owed one message on a channel its app
    /// subscribes to — what both delivery entry points (the dispatcher's
    /// `deliver` and the walk's `wake_owed`) are supposed to serve from.
    ///
    /// The bridge carries a recording CC session, so a send that reaches it
    /// succeeds and the render lands on the broadcast channel.
    async fn owed_conversation_bridge() -> (
        ActiveBridges,
        i64,
        tokio::sync::broadcast::Receiver<brenn_lib::ws_types::WsServerMessage>,
        tokio::sync::mpsc::Receiver<brenn_cc::session::OutgoingEnvelope>,
    ) {
        use brenn_lib::messaging::config::{Depth, NoiseLevel, ResolvedChannel, Sink};
        use brenn_lib::messaging::db::{insert_message, upsert_channels, utc_to_ns};
        use brenn_lib::messaging::query::NoopWakeRouter;
        use brenn_lib::messaging::{
            ChannelEntry, ChannelScheme, MessagingDirectory, MessagingGlobalConfig, Messenger,
            SubscriberEntry, Urgency, WakeMin, canonical_address,
        };

        let db = brenn_lib::db::init_db_memory();
        let channel = ChannelEntry {
            uuid: uuid::Uuid::new_v4(),
            address: canonical_address("wake-walk-channel"),
            description: None,
            resolved_channel: ResolvedChannel {
                send_rate: Default::default(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![SubscriberEntry {
                kind: conv_key(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: Some(WakeMin::Normal),
            }],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        };

        let (user_id, conversation_id) = {
            let conn = db.lock().await;
            let uid = brenn_lib::auth::user::create_user(&conn, "wake-user", "$argon2id$fake");
            let cid = brenn_lib::conversation::create_conversation(&conn, uid, "test-app", false);
            upsert_channels(&conn, std::slice::from_ref(&channel));
            (uid, cid)
        };

        let mut app = crate::bootstrap::messaging::test_fixtures::minimal_app_config(
            "test-app",
            None,
            vec![],
        );
        app.singleton = true;
        app.allowed_users = vec!["wake-user".to_string()];
        app.policy
            .grants
            .insert(brenn_lib::access::AppCapability::MessagingSubscribe);
        app.policy
            .acls
            .brenn_subscribe
            .push(brenn_lib::access::acl::ChannelMatcher::Prefix(String::new()));
        let mut apps = indexmap::IndexMap::new();
        apps.insert("test-app".to_string(), app);

        let messenger = Messenger::new(
            db.clone(),
            Arc::new(MessagingDirectory::with_entries(vec![channel.clone()])),
            Arc::from("test"),
            Arc::new(apps),
            Arc::new(NoopWakeRouter) as Arc<dyn brenn_lib::messaging::WakeRouter>,
            MessagingGlobalConfig::default(),
        );
        // Boot's attach, then one message the conversation has not seen.
        messenger.attach_conversation_subscribers().await;
        {
            let conn = db.lock().await;
            insert_message(
                &conn,
                channel.uuid,
                "test",
                "someone",
                "owed body",
                Urgency::Normal,
                ChannelScheme::Brenn,
                None,
                None,
                None,
                utc_to_ns(chrono::Utc::now()),
            );
        }

        let (broadcast_tx, broadcast_rx) = tokio::sync::broadcast::channel(64);
        let bridge = crate::active_bridge::ActiveBridge::inject_for_test_with_messenger(
            user_id,
            conversation_id,
            "test-app",
            db,
            broadcast_tx,
            messenger,
        );
        let cc_rx = bridge.install_recording_session_for_test().await;
        let active_bridges = ActiveBridges::new();
        active_bridges.insert(conversation_id, bridge).await;
        (active_bridges, conversation_id, broadcast_rx, cc_rx)
    }

    /// Wait for the conversation's render to land on the broadcast channel.
    async fn await_system_broadcast(
        rx: &mut tokio::sync::broadcast::Receiver<brenn_lib::ws_types::WsServerMessage>,
    ) -> bool {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            match tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv()).await {
                Ok(Ok(brenn_lib::ws_types::WsServerMessage::SystemMessageBroadcast { .. })) => {
                    return true;
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => return false,
                Err(_) => continue,
            }
        }
        false
    }

    /// The walk's override: a conversation whose bridge is already live is
    /// served here and now. The spawn-shaped wake would find the bridge running
    /// and deliver nothing, leaving the backlog until the conversation did
    /// something else — so a regression that falls through to it is exactly what
    /// this row catches. The delivery runs on its own task, so the assertion
    /// waits for the render rather than for `wake_owed` to return.
    ///
    /// Called with the spawn verdict **denied**, which is the liveness-before-
    /// urgency rule at its own seam: the verdict prices a subprocess, this arm
    /// spawns none, so a live bridge is served at any urgency.
    #[tokio::test]
    async fn wake_owed_serves_a_live_bridge_whatever_the_verdict() {
        let (active_bridges, conversation_id, mut broadcast_rx, _cc_rx) =
            owed_conversation_bridge().await;
        let router = WakeRouterImpl::new(active_bridges);
        router.register_delivery_binding(conv_key(), DeliveryBinding::ConversationBridge);

        router
            .wake_owed(
                &conv_key(),
                &ParticipantId::for_conversation(conversation_id),
                false,
            )
            .await;

        assert!(
            await_system_broadcast(&mut broadcast_rx).await,
            "the live bridge was served its backlog by the walk itself"
        );
    }

    /// With no live bridge there is nothing to serve, so the walk falls through
    /// to the ordinary eager wake. Test builds make the spawn a no-op, so the
    /// observable proof that the fall-through happened is that it demands the
    /// `AppState` every eager wake needs — the delivery arm never touches it.
    #[tokio::test]
    #[should_panic(expected = "WakeRouter state must be set")]
    async fn wake_owed_without_a_bridge_falls_through_to_the_eager_wake() {
        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.register_delivery_binding(conv_key(), DeliveryBinding::ConversationBridge);
        router
            .wake_owed(&conv_key(), &ParticipantId::for_conversation(42), true)
            .await;
    }

    /// The chat subscription for one conversation, as provisioning mints it.
    fn chat_key(conversation_id: i64) -> SubscriberEntryKind {
        SubscriberEntryKind::ChatConversation {
            app_slug: "test-app".to_string(),
            conversation_id,
        }
    }

    /// A chat conversation's binding is derived from its kind, not registered at
    /// bootstrap — there is no boot at which a per-conversation binding could be
    /// registered. The boot cross-check has to accept it and the wake pass has to
    /// price its wake as a subprocess.
    #[test]
    fn a_chat_conversation_needs_no_registered_binding() {
        let router = WakeRouterImpl::new(ActiveBridges::new());
        assert!(router.has_delivery_binding(&chat_key(7)));
        assert_eq!(router.delivery_shape(&chat_key(7)), DeliveryShape::Inline);
    }

    /// A live conversation's chat backlog is a notify to its adapter and nothing
    /// else. In particular it must not take the app-subscriber path, which
    /// renders what it finds into the conversation as a system message — a
    /// command is to be executed, not narrated.
    #[tokio::test]
    async fn wake_owed_rings_a_live_conversations_chat_drain_and_renders_nothing() {
        let (active_bridges, conversation_id, mut broadcast_rx, _cc_rx) =
            owed_conversation_bridge().await;
        let notify = Arc::clone(
            &active_bridges
                .get(conversation_id)
                .await
                .expect("the fixture's bridge is live")
                .chat_commands,
        );
        let router = WakeRouterImpl::new(active_bridges);

        router
            .wake_owed(
                &chat_key(conversation_id),
                &ParticipantId::for_conversation(conversation_id),
                false,
            )
            .await;

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(500), notify.notified())
                .await
                .is_ok(),
            "the adapter's drain was rung",
        );
        // Yield rounds rather than a wall-clock timeout: the app-subscriber arm
        // this must not take renders on a task of its own, so the negative half
        // has to give that task every chance to run — and on a loaded box a
        // render arriving after a 300 ms deadline would have read as a pass.
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert!(
            matches!(
                broadcast_rx.try_recv(),
                Err(tokio::sync::broadcast::error::TryRecvError::Empty)
            ),
            "and nothing was rendered into the conversation",
        );
    }

    /// Sleeping, the chat wake is the conversation's spawn — reached without any
    /// registered binding, which the panic message proves: a missing binding
    /// names itself, and this one asks for the `AppState` a spawn needs.
    #[tokio::test]
    #[should_panic(expected = "WakeRouter state must be set")]
    async fn a_chat_wake_with_no_bridge_buys_a_spawn() {
        let router = WakeRouterImpl::new(ActiveBridges::new());
        router
            .wake_owed(&chat_key(42), &ParticipantId::for_conversation(42), true)
            .await;
    }

    /// And below the threshold it buys nothing at all: the pass hands the verdict
    /// down, and a denied verdict on a sleeping conversation is where it stops.
    /// State is left unset, so any spawn attempt would panic.
    #[tokio::test]
    async fn a_denied_chat_wake_with_no_bridge_does_nothing() {
        let router = WakeRouterImpl::new(ActiveBridges::new());
        router
            .wake_owed(&chat_key(42), &ParticipantId::for_conversation(42), false)
            .await;
    }

    /// The other half of that rule: with no bridge to serve, a denied verdict is
    /// the whole answer. Proven by the absence of the panic its sibling above
    /// gets — reaching the spawn arm without an `AppState` cannot be silent.
    #[tokio::test]
    async fn wake_owed_without_a_bridge_respects_a_denied_verdict() {
        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.register_delivery_binding(conv_key(), DeliveryBinding::ConversationBridge);
        router
            .wake_owed(&conv_key(), &ParticipantId::for_conversation(42), false)
            .await;
    }

    /// `deliver` for a parked (`wasm:`) subscriber panics — reaching it is a
    /// host-wiring invariant violation (`dispatch_row` gates parked rows to
    /// `spawn_eager_wake` and never calls `deliver` for them).
    #[tokio::test]
    #[should_panic(expected = "WakeRouter::deliver called for non-surface subscriber")]
    async fn deliver_panics_for_wasm_subscriber() {
        use brenn_lib::messaging::{MessageEnvelope, Urgency};
        use chrono::Utc;
        use uuid::Uuid;
        let router = WakeRouterImpl::new(ActiveBridges::new());
        let key = SubscriberEntryKind::Wasm("my-consumer".to_string());
        router.register_delivery_binding(
            key.clone(),
            DeliveryBinding::ParkedNotify(Arc::new(tokio::sync::Notify::new())),
        );
        let env = MessageEnvelope {
            message_id: Uuid::new_v4(),
            source: "host".into(),
            channel: "brenn:ch".into(),
            sender: "alice".into(),
            publish_ts: Utc::now(),
            body: "hi".into(),
            reply_to: None,
            delivery_deadline: None,
            deliver_after: None,
            urgency: Urgency::Normal,
            envelope_type: brenn_lib::messaging::ChannelScheme::Brenn,
        };
        let _ = router.deliver(&key, &Arc::new(env), 1).await;
    }

    /// `deliver_ingress` for a parked (`wasm:`) subscriber panics — ingress rows
    /// are conversation-targeted by invariant.
    #[tokio::test]
    #[should_panic(expected = "WakeRouter::deliver_ingress called for non-conversation subscriber")]
    async fn deliver_ingress_panics_for_wasm_subscriber() {
        use brenn_lib::messaging::ingress::Event;
        let router = WakeRouterImpl::new(ActiveBridges::new());
        let key = SubscriberEntryKind::Wasm("my-consumer".to_string());
        router.register_delivery_binding(
            key.clone(),
            DeliveryBinding::ParkedNotify(Arc::new(tokio::sync::Notify::new())),
        );
        let event = Event {
            id: 1,
            conversation_id: 1,
            source: "src".into(),
            summary: "sum".into(),
            payload: "{}".into(),
            created_at: chrono::Utc::now(),
        };
        let _ = router
            .deliver_ingress(&key, &ParticipantId::for_wasm("my-consumer"), &event)
            .await;
    }

    /// `spawn_eager_wake` for a `wasm:` subscriber notifies the registered `Notify`.
    /// The off-loop dispatch task holds the `Arc` clone; `notify_one`
    /// sets the permit so the task's `notified().await` resolves immediately.
    #[test]
    fn spawn_eager_wake_notifies_wasm_subscriber() {
        let router = WakeRouterImpl::new(ActiveBridges::new());
        let notify = Arc::new(tokio::sync::Notify::new());
        let key = SubscriberEntryKind::Wasm("my-consumer".to_string());
        router.register_delivery_binding(
            key.clone(),
            DeliveryBinding::ParkedNotify(Arc::clone(&notify)),
        );

        router.spawn_eager_wake(&key, &ParticipantId::for_wasm("my-consumer"));

        // The Notify permit is set; a blocking poll resolves immediately.
        // We use try_recv-equivalent via the runtime: build a one-shot runtime
        // and assert the future completes without blocking.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        // notified() resolves immediately when a permit was set by notify_one.
        rt.block_on(async {
            tokio::time::timeout(std::time::Duration::from_millis(10), notify.notified())
                .await
                .expect("Notify::notified() should resolve immediately after notify_one");
        });
    }

    /// `spawn_eager_wake` for an unregistered `wasm:` slug panics — host-wiring
    /// invariant violation.
    #[test]
    #[should_panic(expected = "no delivery binding registered")]
    fn spawn_eager_wake_panics_for_unregistered_wasm_slug() {
        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.spawn_eager_wake(
            &SubscriberEntryKind::Wasm("not-registered".to_string()),
            &ParticipantId::for_wasm("not-registered"),
        );
    }

    /// Build a `Messenger` over `db` (empty directory), declare one `brenn:`
    /// channel, and commit one message onto it. Returns
    /// `(messenger, surface participant, retention position)`.
    async fn surface_push_fixture(
        db: &brenn_lib::db::Db,
        slug: &str,
        channel_addr: &str,
    ) -> (Arc<brenn_lib::messaging::Messenger>, ParticipantId, i64) {
        use brenn_lib::messaging::config::{
            ChannelConfigRaw, MessagingGlobalConfig, build_channel_entries,
        };
        use brenn_lib::messaging::db::{insert_message, upsert_channels, utc_to_ns};
        use brenn_lib::messaging::query::NoopWakeRouter;
        use brenn_lib::messaging::{ChannelScheme, MessagingDirectory, Messenger, Urgency};
        use chrono::Utc;
        use indexmap::IndexMap;
        use uuid::Uuid;

        let bare = channel_addr.strip_prefix("brenn:").expect("brenn: address");
        let raw = ChannelConfigRaw {
            send_rate: None,
            uuid: Some(Uuid::new_v4().to_string()),
            address: bare.to_string(),
            description: None,
            push_depth: None,
            retain_depth: None,
            standing_retain_depth: None,
            noise: None,
            sink: None,
            wake_min: None,
        };
        let entry = build_channel_entries(&[raw], &MessagingGlobalConfig::default())
            .pop()
            .expect("one channel entry");
        let participant = ParticipantId::for_surface(slug);
        let seq = {
            let conn = db.lock().await;
            upsert_channels(&conn, std::slice::from_ref(&entry));
            insert_message(
                &conn,
                entry.uuid,
                "test",
                "sender",
                "hello",
                Urgency::Normal,
                ChannelScheme::Brenn,
                None,
                None,
                None,
                utc_to_ns(Utc::now()),
            )
            .retained_seq
            .expect("a committed message holds a retention position")
        };
        let messenger = Messenger::new(
            db.clone(),
            Arc::new(MessagingDirectory::with_entries(vec![entry])),
            Arc::from("test"),
            Arc::new(IndexMap::new()),
            Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        );
        (messenger, participant, seq)
    }

    /// Whether the table holds no rows at all. A bus commit and the surface
    /// fan-out over it both write nothing per subscriber: what a channel owes is a
    /// position, and a surface does not even hold one.
    async fn no_pending_push_rows(db: &brenn_lib::db::Db) -> bool {
        let conn = db.lock().await;
        conn.query_row("SELECT COUNT(*) FROM messaging_pending_pushes", [], |row| {
            row.get::<_, i64>(0)
        })
        .expect("count pending pushes")
            == 0
    }

    /// A `MessageEnvelope` on `channel` (the only field the Surface `deliver` arm
    /// inspects — it filters sessions by it and clones the whole envelope).
    fn surface_envelope(channel: &str) -> brenn_lib::messaging::MessageEnvelope {
        use brenn_lib::messaging::{MessageEnvelope, Urgency};
        MessageEnvelope {
            message_id: uuid::Uuid::new_v4(),
            source: "host".into(),
            channel: channel.into(),
            sender: "alice".into(),
            publish_ts: chrono::Utc::now(),
            body: "hi".into(),
            reply_to: None,
            delivery_deadline: None,
            deliver_after: None,
            urgency: Urgency::Normal,
            envelope_type: brenn_lib::messaging::ChannelScheme::Brenn,
        }
    }

    /// The durable row inside a session push, panicking on any other variant —
    /// these tests drive the delivery fan-out, so anything else is a wiring bug.
    fn durable_push(push: SessionPush) -> DurableDelivery {
        match push {
            SessionPush::Durable(delivery) => delivery,
            SessionPush::DeferredView(view) => {
                panic!(
                    "expected a durable row, got a deferred view for {}",
                    view.channel
                )
            }
        }
    }

    /// Register a session handle for `slug` subscribed to `channel`, returning the
    /// guard (keep alive), the live-delivery receiver, and the drain notifier.
    fn register_surface_session(
        state: &AppState,
        slug: &str,
        channel: &str,
    ) -> (
        crate::routes::surface::registry::SurfaceSessionGuard,
        tokio::sync::mpsc::Receiver<SessionPush>,
        Arc<tokio::sync::Notify>,
    ) {
        use crate::routes::surface::registry::{
            PUSH_QUEUE_FRAMES, SessionCaps, SurfaceSessionHandle,
        };

        let (push_tx, push_rx) = tokio::sync::mpsc::channel(PUSH_QUEUE_FRAMES);
        let mut handle = SurfaceSessionHandle::for_test("dev");
        handle.push_tx = push_tx;
        handle
            .durable_subs
            .lock()
            .expect("durable_subs poisoned")
            .insert(SubKey {
                instance: "protobar".to_string(),
                channel: channel.to_string(),
            });
        let drain_notify = Arc::clone(&handle.drain_notify);
        let guard = state
            .surface_registry
            .try_register(slug, handle, SessionCaps::UNCAPPED)
            .expect("register");
        (guard, push_rx, drain_notify)
    }

    /// `deliver` for a `surface:` subscriber with an attached, subscribed session
    /// hands the envelope to that session's live queue and returns `Ok(true)`,
    /// writing nothing: the session's own cursor is its delivery state.
    #[tokio::test]
    async fn deliver_surface_fans_out_without_claiming() {
        let db = brenn_lib::db::init_db_memory();
        let channel = "brenn:durable-demo";
        let (messenger, participant, seq) = surface_push_fixture(&db, "deskbar", channel).await;

        let mut state = AppState::for_test(db.clone(), None);
        state.messenger = Some(messenger);
        let (_guard, mut rx, notify) = register_surface_session(&state, "deskbar", channel);

        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.set_state(state);
        router.register_delivery_binding(surface_key(), DeliveryBinding::SurfaceSessions);

        let _ = &participant;
        let result = router
            .deliver(&surface_key(), &Arc::new(surface_envelope(channel)), seq)
            .await;
        assert!(matches!(result, Ok(true)));

        // The row landed on the session's live queue with the wire seq.
        let delivered = durable_push(rx.try_recv().expect("live delivery enqueued"));
        assert_eq!(delivered.envelope.channel, channel);
        assert_eq!(delivered.retained_seq, seq as u64);
        // Per-delivery drain nudge fired.
        tokio::time::timeout(std::time::Duration::from_millis(10), notify.notified())
            .await
            .expect("drain nudge fired");

        assert!(
            no_pending_push_rows(&db).await,
            "a bus commit and its fan-out write no pending-push row"
        );
    }

    /// `deliver` for a `surface:` subscriber with no attached/subscribed session
    /// parks (`Ok(false)`) so the dispatcher wakes again for a later attach.
    #[tokio::test]
    async fn deliver_surface_no_session_parks() {
        let db = brenn_lib::db::init_db_memory();
        let channel = "brenn:durable-demo";
        let (messenger, participant, seq) = surface_push_fixture(&db, "deskbar", channel).await;

        let mut state = AppState::for_test(db.clone(), None);
        state.messenger = Some(messenger);
        // No session registered.

        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.set_state(state);
        router.register_delivery_binding(surface_key(), DeliveryBinding::SurfaceSessions);

        let _ = &participant;
        let result = router
            .deliver(&surface_key(), &Arc::new(surface_envelope(channel)), seq)
            .await;
        assert!(matches!(result, Ok(false)));
        assert!(no_pending_push_rows(&db).await);
    }

    /// When every subscribed session's live queue is unusable (here: receiver
    /// dropped, so `try_send` fails), `deliver` reports `Ok(false)` and writes
    /// nothing — the sessions catch up from their own cursors.
    #[tokio::test]
    async fn deliver_surface_all_queues_full_parks() {
        let db = brenn_lib::db::init_db_memory();
        let channel = "brenn:durable-demo";
        let (messenger, participant, seq) = surface_push_fixture(&db, "deskbar", channel).await;

        let mut state = AppState::for_test(db.clone(), None);
        state.messenger = Some(messenger);
        let (_guard, rx, _notify) = register_surface_session(&state, "deskbar", channel);
        // Close the session's live queue so every try_send is rejected.
        drop(rx);

        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.set_state(state);
        router.register_delivery_binding(surface_key(), DeliveryBinding::SurfaceSessions);

        let _ = &participant;
        let result = router
            .deliver(&surface_key(), &Arc::new(surface_envelope(channel)), seq)
            .await;
        assert!(matches!(result, Ok(false)));
        assert!(no_pending_push_rows(&db).await);
    }

    /// `deliver_context` (the durable depth-0 row-less feed) fans an envelope to
    /// an attached, subscribed session's live queue with **no** DB claim — it
    /// touches no `messaging_pending_pushes` row at all.
    #[tokio::test]
    async fn deliver_context_fans_out_row_less_with_no_claim() {
        let db = brenn_lib::db::init_db_memory();
        let channel = "brenn:durable-demo";
        // No push fixture and no messenger: the feed creates and claims no row.
        let state = AppState::for_test(db.clone(), None);
        let (_guard, mut rx, _notify) = register_surface_session(&state, "deskbar", channel);

        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.set_state(state);
        router.register_delivery_binding(surface_key(), DeliveryBinding::SurfaceSessions);

        router
            .deliver_context(&surface_key(), &Arc::new(surface_envelope(channel)), 7)
            .await;

        let delivered = durable_push(rx.try_recv().expect("row-less delivery enqueued"));
        assert_eq!(delivered.envelope.channel, channel);
        assert_eq!(delivered.retained_seq, 7);
    }

    /// `deliver_context` with no attached/subscribed session is a no-op — nothing
    /// is owed to a disconnected session (its context arrives at the next resume).
    #[tokio::test]
    async fn deliver_context_no_session_is_a_noop() {
        let db = brenn_lib::db::init_db_memory();
        let state = AppState::for_test(db.clone(), None);
        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.set_state(state);
        router.register_delivery_binding(surface_key(), DeliveryBinding::SurfaceSessions);
        // No session registered — completes without panic, delivers nothing.
        router
            .deliver_context(
                &surface_key(),
                &Arc::new(surface_envelope("brenn:durable-demo")),
                7,
            )
            .await;
    }

    /// `any_surface_session_subscribed` — the publish-time build-skip precheck —
    /// answers true for a subscribed attached session and false with none. The
    /// false branch is the cost saver: no envelope is built when no page is open.
    #[tokio::test]
    async fn any_surface_session_subscribed_true_with_subscriber_false_without() {
        let db = brenn_lib::db::init_db_memory();
        let channel = "brenn:durable-demo";
        let state = AppState::for_test(db.clone(), None);

        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.set_state(state.clone());
        assert!(
            !router.any_surface_session_subscribed(channel, &[surface_feed_target()]),
            "no session open — nothing to feed, skip the build"
        );

        let (_guard, _rx, _notify) = register_surface_session(&state, "deskbar", channel);
        assert!(
            router.any_surface_session_subscribed(channel, &[surface_feed_target()]),
            "the subscribed attached session is a feed target"
        );
        assert!(
            !router.any_surface_session_subscribed("brenn:other-channel", &[surface_feed_target()]),
            "subscribed to a different channel — not a target here"
        );
    }

    /// `any_surface_session_attached` — the release sweep's recompute-skip
    /// precheck — answers per slug and ignores what the session is subscribed to:
    /// the parked set belongs to the surface, and every session of it is told.
    #[tokio::test]
    async fn any_surface_session_attached_answers_per_slug() {
        let db = brenn_lib::db::init_db_memory();
        let state = AppState::for_test(db.clone(), None);

        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.set_state(state.clone());
        assert!(
            !router.any_surface_session_attached("deskbar"),
            "no session open — the sweep skips the view recompute"
        );

        // Subscribed to one channel; the answer is about the surface, not it.
        let (_guard, _rx, _notify) = register_surface_session(&state, "deskbar", "brenn:unrelated");
        assert!(router.any_surface_session_attached("deskbar"));
        assert!(
            !router.any_surface_session_attached("kitchen"),
            "another surface's sessions are no part of this one's answer"
        );
    }

    /// `deliver_context` onto a full/closed session queue drops the row-less
    /// delivery silently — there is no row to unclaim and nothing is owed;
    /// recovery is the retained window at the next resume.
    #[tokio::test]
    async fn deliver_context_full_queue_drops_silently() {
        let db = brenn_lib::db::init_db_memory();
        let channel = "brenn:durable-demo";
        let state = AppState::for_test(db.clone(), None);
        let (_guard, rx, _notify) = register_surface_session(&state, "deskbar", channel);
        // Close the live queue so every try_send is rejected.
        drop(rx);

        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.set_state(state);
        router.register_delivery_binding(surface_key(), DeliveryBinding::SurfaceSessions);

        // Dropped silently — no panic, no DB access.
        router
            .deliver_context(&surface_key(), &Arc::new(surface_envelope(channel)), 7)
            .await;
    }

    /// `deliver_context` for a non-surface key panics — only fold-0 surface
    /// subscriptions take the row-less feed (`resolve_context_targets` filters to
    /// them), so any other kind is a caller wiring bug.
    #[tokio::test]
    #[should_panic(expected = "deliver_context called for non-surface subscriber")]
    async fn deliver_context_panics_for_non_surface_key() {
        let router = WakeRouterImpl::new(ActiveBridges::new());
        router
            .deliver_context(
                &conv_key(),
                &Arc::new(surface_envelope("brenn:durable-demo")),
                1,
            )
            .await;
    }

    /// `deliver_ingress` for a `surface:` subscriber panics — surfaces are never
    /// ingress targets; reaching this arm is a host-wiring invariant
    /// violation. Mirrors the `wasm:` counterpart above (test-1).
    #[tokio::test]
    #[should_panic(expected = "WakeRouter::deliver_ingress called for non-conversation subscriber")]
    async fn deliver_ingress_panics_for_surface_subscriber() {
        use brenn_lib::messaging::ingress::Event;
        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.register_delivery_binding(surface_key(), DeliveryBinding::SurfaceSessions);
        let event = Event {
            id: 1,
            conversation_id: 1,
            source: "src".into(),
            summary: "sum".into(),
            payload: "{}".into(),
            created_at: chrono::Utc::now(),
        };
        let _ = router
            .deliver_ingress(
                &surface_key(),
                &ParticipantId::for_surface("deskbar"),
                &event,
            )
            .await;
    }

    /// `spawn_eager_wake` for a `surface:` subscriber nudges every attached
    /// session's `drain_notify` (no per-channel filter — the session drains all
    /// its active durable channels).
    #[tokio::test]
    async fn spawn_eager_wake_surface_notifies_attached_sessions() {
        let db = brenn_lib::db::init_db_memory();
        let channel = "brenn:durable-demo";
        // Messenger presence is irrelevant to spawn_eager_wake; leave it None.
        let state = AppState::for_test(db.clone(), None);
        let (_guard, _rx, notify) = register_surface_session(&state, "deskbar", channel);

        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.set_state(state);
        router.register_delivery_binding(surface_key(), DeliveryBinding::SurfaceSessions);

        router.spawn_eager_wake(&surface_key(), &ParticipantId::for_surface("deskbar"));

        tokio::time::timeout(std::time::Duration::from_millis(10), notify.notified())
            .await
            .expect("drain notifier fired for attached session");
    }

    /// Every bus-driven wake of a conversation goes through the **bus** spawn
    /// entrypoint, so the spawn backoff bounds it: with the backoff armed the
    /// router's wake reaches no spawn at all. Routing this arm to the
    /// user-initiated entrypoint instead would compile, deliver, and quietly
    /// restore the retry storm the backoff exists to damp.
    #[tokio::test]
    async fn the_conversation_arm_spawns_through_the_bus_entrypoint() {
        let state = AppState::for_test(brenn_lib::db::init_db_memory(), None);
        let spawns = Arc::clone(&state.wake_spawns);
        let backoff = state.spawn_backoff.clone();
        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.set_state(state);
        router.register_delivery_binding(conv_key(), DeliveryBinding::ConversationBridge);

        router.spawn_eager_wake(&conv_key(), &ParticipantId::for_conversation(42));
        assert_eq!(
            *spawns.lock().unwrap(),
            vec![(42, crate::state::BusHold::Unheld)],
            "an app subscriber's wake buys a bridge and decides nothing about its lifetime"
        );

        backoff.record_failure(42);
        router.spawn_eager_wake(&conv_key(), &ParticipantId::for_conversation(42));
        assert_eq!(
            spawns.lock().unwrap().len(),
            1,
            "an armed conversation's bus wake is declined before it reaches a spawn"
        );
    }

    /// A chat wake buys the same bridge through the same backoff, and leaves the
    /// bus door holding it: the peer asked for this conversation by name, and a
    /// start-up drain that finds nothing owed would otherwise leave the spawned
    /// bridge held by no door and re-asked about by no timer.
    #[tokio::test]
    async fn the_chat_arm_spawns_held_through_the_bus_entrypoint() {
        let state = AppState::for_test(brenn_lib::db::init_db_memory(), None);
        let spawns = Arc::clone(&state.wake_spawns);
        let backoff = state.spawn_backoff.clone();
        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.set_state(state);

        router.spawn_eager_wake(&chat_key(42), &ParticipantId::for_conversation(42));
        assert_eq!(
            *spawns.lock().unwrap(),
            vec![(42, crate::state::BusHold::Held)],
        );

        backoff.record_failure(42);
        router.spawn_eager_wake(&chat_key(42), &ParticipantId::for_conversation(42));
        assert_eq!(
            spawns.lock().unwrap().len(),
            1,
            "the hold does not exempt a chat wake from the backoff"
        );
    }

    /// `has_delivery_binding` is the boot cross-check's binding probe: false for
    /// an unregistered key, true once registered. A directory subscriber with no
    /// binding would fail the cross-check (rather than panicking later at dispatch).
    #[test]
    fn has_delivery_binding_reflects_registration() {
        let router = WakeRouterImpl::new(ActiveBridges::new());
        let key = SubscriberEntryKind::Wasm("my-consumer".to_string());
        assert!(
            !router.has_delivery_binding(&key),
            "unregistered key has no binding"
        );
        router.register_delivery_binding(
            key.clone(),
            DeliveryBinding::ParkedNotify(Arc::new(tokio::sync::Notify::new())),
        );
        assert!(
            router.has_delivery_binding(&key),
            "registered key has a binding"
        );
    }

    /// `delivery_shape` is the dispatcher's routing source of truth; assert each
    /// binding variant maps to its declared shape directly against the real
    /// router (the dispatcher tests substitute the brenn-lib mirror
    /// `default_delivery_shape`, so without this the two impls could diverge
    /// silently on the `ConversationBridge` / `ParkedNotify` arms).
    #[test]
    fn delivery_shape_maps_each_binding_variant() {
        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.register_delivery_binding(conv_key(), DeliveryBinding::ConversationBridge);
        router.register_delivery_binding(surface_key(), DeliveryBinding::SurfaceSessions);
        let parked_key = SubscriberEntryKind::System("tool-executor".to_string());
        router.register_delivery_binding(
            parked_key.clone(),
            DeliveryBinding::ParkedNotify(Arc::new(tokio::sync::Notify::new())),
        );
        assert!(matches!(
            router.delivery_shape(&conv_key()),
            DeliveryShape::Inline
        ));
        assert!(matches!(
            router.delivery_shape(&surface_key()),
            DeliveryShape::Inline
        ));
        assert!(matches!(
            router.delivery_shape(&parked_key),
            DeliveryShape::ParkedWake
        ));
    }

    /// `delivery_shape` on an unregistered key panics — same host-wiring
    /// invariant as the dispatch-path panics.
    #[test]
    #[should_panic(expected = "no delivery binding registered")]
    fn delivery_shape_panics_for_unregistered_key() {
        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.delivery_shape(&SubscriberEntryKind::Wasm("ghost".to_string()));
    }

    /// `register_delivery_binding` called twice for the same key panics
    /// (bootstrap wiring bug detection).
    #[test]
    #[should_panic(expected = "register_delivery_binding called twice")]
    fn register_delivery_binding_panics_on_duplicate_wasm() {
        let router = WakeRouterImpl::new(ActiveBridges::new());
        let n = Arc::new(tokio::sync::Notify::new());
        let key = SubscriberEntryKind::Wasm("my-consumer".to_string());
        router
            .register_delivery_binding(key.clone(), DeliveryBinding::ParkedNotify(Arc::clone(&n)));
        router.register_delivery_binding(key, DeliveryBinding::ParkedNotify(Arc::clone(&n)));
    }

    /// `deliver` for a parked (`system:`) subscriber panics — parked subscribers
    /// must never reach the shared dispatch loop deliver path (host-wiring
    /// invariant). Mirrors the `wasm:` counterpart.
    #[tokio::test]
    #[should_panic(expected = "WakeRouter::deliver called for non-surface subscriber")]
    async fn deliver_panics_for_system_subscriber() {
        let router = WakeRouterImpl::new(ActiveBridges::new());
        let key = SubscriberEntryKind::System("tool-executor".to_string());
        router.register_delivery_binding(
            key.clone(),
            DeliveryBinding::ParkedNotify(Arc::new(tokio::sync::Notify::new())),
        );
        let _ = router
            .deliver(&key, &Arc::new(surface_envelope("brenn:whatever")), 1)
            .await;
    }

    /// `deliver_ingress` for a parked (`system:`) subscriber panics — parked
    /// subscribers are never ingress targets (host-wiring invariant).
    #[tokio::test]
    #[should_panic(expected = "WakeRouter::deliver_ingress called for non-conversation subscriber")]
    async fn deliver_ingress_panics_for_system_subscriber() {
        use brenn_lib::messaging::ingress::Event;
        let router = WakeRouterImpl::new(ActiveBridges::new());
        let key = SubscriberEntryKind::System("tool-executor".to_string());
        router.register_delivery_binding(
            key.clone(),
            DeliveryBinding::ParkedNotify(Arc::new(tokio::sync::Notify::new())),
        );
        let event = Event {
            id: 1,
            conversation_id: 1,
            source: "src".into(),
            summary: "sum".into(),
            payload: "{}".into(),
            created_at: chrono::Utc::now(),
        };
        let _ = router
            .deliver_ingress(&key, &ParticipantId::for_system("tool-executor"), &event)
            .await;
    }

    /// `spawn_eager_wake` for a registered `system:` component fires its
    /// notifier (the substrate off-loop dispatch task's wake trigger).
    #[tokio::test]
    async fn spawn_eager_wake_system_fires_registered_notifier() {
        let router = WakeRouterImpl::new(ActiveBridges::new());
        let notify = Arc::new(tokio::sync::Notify::new());
        let key = SubscriberEntryKind::System("tool-executor".to_string());
        router.register_delivery_binding(
            key.clone(),
            DeliveryBinding::ParkedNotify(Arc::clone(&notify)),
        );

        router.spawn_eager_wake(&key, &ParticipantId::for_system("tool-executor"));

        tokio::time::timeout(std::time::Duration::from_millis(10), notify.notified())
            .await
            .expect("system notifier fired for registered component");
    }

    /// `spawn_eager_wake` for an unregistered `system:` component panics —
    /// host-wiring invariant violation.
    #[test]
    #[should_panic(expected = "no delivery binding registered")]
    fn spawn_eager_wake_panics_for_unregistered_system_component() {
        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.spawn_eager_wake(
            &SubscriberEntryKind::System("not-registered".to_string()),
            &ParticipantId::for_system("not-registered"),
        );
    }

    /// `register_delivery_binding` called twice for the same system key panics.
    #[test]
    #[should_panic(expected = "register_delivery_binding called twice")]
    fn register_delivery_binding_panics_on_duplicate_system() {
        let router = WakeRouterImpl::new(ActiveBridges::new());
        let n = Arc::new(tokio::sync::Notify::new());
        let key = SubscriberEntryKind::System("tool-executor".to_string());
        router
            .register_delivery_binding(key.clone(), DeliveryBinding::ParkedNotify(Arc::clone(&n)));
        router.register_delivery_binding(key, DeliveryBinding::ParkedNotify(Arc::clone(&n)));
    }
}
