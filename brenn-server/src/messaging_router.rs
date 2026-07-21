//! `WakeRouter` adapter implementing `brenn_lib::messaging::WakeRouter`
//! over `ActiveBridges` + `AppState`.
//!
//! `Messenger` lives in `brenn-lib` and must not depend on binary-crate
//! types; this adapter is the single bridge across that boundary. Held
//! on `AppState` indirectly via `Arc<dyn WakeRouter>` inside `Messenger`.

use std::collections::HashMap;
use std::sync::Arc;

use brenn_lib::messaging::{
    DeliveryShape, MessageEnvelope, ParticipantId, SubscriberEntryKind, WakeRouter,
};
use brenn_lib::obs::alerting::{AlertDispatcher, AlertSeverity};
use chrono_tz::Tz;
use tracing::{debug, warn};

use crate::active_bridge::ActiveBridges;
use crate::routes::surface::SubKey;
use crate::routes::surface::registry::DurableDelivery;
use crate::state::AppState;
use crate::system_message::{
    SystemMessageRender, render_event_drain, render_messages_received_single,
};

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
    /// Alert dispatcher for push-overflow alarms (design §2.8). Wired at
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
        self.bindings
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
        subscriber: &ParticipantId,
        envelope: &MessageEnvelope,
        // `push_id` (the claimed pending-push row) and `seq` (`messaging_messages.id`,
        // the durable row id the surface cursor's high-water is minted from) are
        // used only by the surface route; the conversation route ignores them.
        push_id: i64,
        seq: i64,
    ) -> Result<bool, String> {
        match self.delivery_route(key) {
            DeliveryRoute::ConversationBridge => {
                let conversation_id = subscriber.as_conversation_id();
                // Check for an active bridge first — render only when one exists.
                // This keeps the markdown render (pulldown-cmark) out of
                // the shared dispatch loop for sleeping targets, and confines any
                // malformed-envelope panic to the per-bridge path rather than the
                // loop that serves all conversations (correctness-2 / efficiency-1).
                let bridge = match self.active_bridges.get(conversation_id).await {
                    Some(b) => b,
                    None => return Ok(false),
                };
                // Render only after confirming a bridge is present.
                let rendered = render_messages_received_single(envelope);
                let render = SystemMessageRender {
                    text: rendered.text,
                    rendered_html: rendered.rendered_html,
                    category: rendered.category,
                    // Dual ToolUseSummary broadcast is drain-path only; the live
                    // messaging path never emitted it (only `drain_pending_events`
                    // does).
                    messaging_card_html: None,
                };
                match bridge.send_system_message(render, None).await {
                    Ok(()) => Ok(true),
                    Err(e) => Err(e),
                }
            }
            // Parked subscribers (WASM consumers, system components) are never
            // delivered to on the shared dispatch loop — dispatch_row routes their
            // rows to spawn_eager_wake and never calls deliver. Reaching this is a
            // host-wiring invariant violation.
            DeliveryRoute::Parked => {
                panic!(
                    "WakeRouter::deliver called for parked subscriber {key:?} — \
                     host-wiring invariant violated: parked subscribers must never \
                     reach the shared dispatch loop deliver path"
                );
            }
            // Claim-based at-most-once fan-out to attached, subscribed sessions.
            // The dispatcher fan-out (this arm) and each session's drain race for
            // the same rows; whoever atomically claims a push row sends it, the
            // other skips. `try_send` (not awaited): a hung session must not stall
            // the shared fan-out task, so partial acceptance is accepted — a slow
            // session's interior misses heal only where its own high-water has not
            // passed them (at-most-once; the planned Ack upgrade tightens this).
            DeliveryRoute::SurfaceSessions => {
                let slug = key.slug();
                let state = self
                    .state
                    .get()
                    .expect("WakeRouter state must be set before any Surface deliver call");

                // The subscription this row belongs to: the principal the push
                // row was resolved for, on the row's channel. `key` is the
                // subscriber's registration key, so its instance half is the
                // principal — never re-derived from the envelope.
                // Only `Surface` keys register `SurfaceSessions` (bootstrap
                // wiring); `surface_subscriber_instance` panics otherwise.
                let sub = SubKey {
                    instance: key.surface_subscriber_instance().to_owned(),
                    channel: envelope.channel.clone(),
                };

                // 1. Sessions holding this exact subscription. Filtering on the
                //    whole subscription and not the channel is what keeps the row
                //    off a sibling instance's ports: siblings are separate
                //    principals with separate windows, and this row is one
                //    principal's.
                let subscribed: Vec<_> = state
                    .surface_registry
                    .sessions(slug)
                    .into_iter()
                    .filter(|h| h.is_subscribed(&sub))
                    .collect();

                // 2. None attached+subscribed → park; dispatch_row eager-wakes per
                //    the row's flags, same as a sleeping conversation.
                if subscribed.is_empty() {
                    return Ok(false);
                }

                let messenger = state.messenger.as_ref().expect(
                    "WakeRouter Surface deliver: a Surface subscriber implies messaging is \
                     configured (boot invariant)",
                );

                // 3. Claim the row. Empty → a session drain already owns it and
                //    will send it; report delivered without re-sending.
                let claimed = {
                    let conn = messenger.db().lock().await;
                    brenn_lib::messaging::db::claim_pending_pushes(&conn, &[push_id])
                };
                if claimed.is_empty() {
                    return Ok(true);
                }

                // 4. Fan out the claimed row to every subscribed session. One
                //    `Arc<MessageEnvelope>` is built here and every session clones
                //    the refcount — the shared dispatch task never deep-clones the
                //    body per session.
                let shared_envelope = Arc::new(envelope.clone());
                let mut accepted = 0usize;
                let mut rejected = 0usize;
                for handle in &subscribed {
                    let delivery = DurableDelivery {
                        envelope: shared_envelope.clone(),
                        seq,
                        sub: sub.clone(),
                    };
                    if handle.durable_tx.try_send(delivery).is_ok() {
                        accepted += 1;
                    } else {
                        rejected += 1;
                    }
                }
                // Partial acceptance: sessions that were backpressured while others
                // took the row have a permanent interior gap (resume heals only
                // rows above a session's own high-water). Surface it — this is a
                // knowingly-lost message class, otherwise undiagnosable.
                if rejected > 0 && accepted > 0 {
                    warn!(
                        slug = %slug,
                        channel = %envelope.channel,
                        seq,
                        push_id,
                        rejected,
                        accepted,
                        "surface durable live delivery: some session queues full; those \
                         sessions may permanently miss this row"
                    );
                }

                // 5. Per-delivery drain nudge on every subscribed session: flushes
                //    below-`wake_min` parked rows on a healthy attached session
                //    (dispatch_row only eager-wakes on Ok(false)/Err) and recovers
                //    a queue-full session. Fires before the accept decision below
                //    (step 6) because that decision returns out of the arm. A
                //    spurious pass over an empty parked set is one indexed query
                //    per active channel.
                for handle in &subscribed {
                    handle.drain_notify.notify_one();
                }

                // 6. ≥1 accepted → delivered. Zero (every queue full/closed) →
                //    unclaim so the row re-parks and the sessions catch up by
                //    draining.
                if accepted >= 1 {
                    Ok(true)
                } else {
                    debug!(
                        slug = %slug,
                        channel = %envelope.channel,
                        seq,
                        push_id,
                        "surface durable live delivery: all session queues full; \
                         re-parking row for the next drain"
                    );
                    let conn = messenger.db().lock().await;
                    brenn_lib::messaging::db::unclaim_pending_pushes(&conn, &[push_id]);
                    Ok(false)
                }
            }
        }
    }

    async fn deliver_context(
        &self,
        key: &SubscriberEntryKind,
        envelope: &Arc<MessageEnvelope>,
        seq: i64,
    ) {
        // Row-less deliver-if-attached fan-out for a fold-0 surface subscription
        // (design §6). No push row, no claim: a fold-0 subscription has no push
        // window, so the message reaches an attached session only here, live.
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

        // Sessions holding this exact subscription (per-principal, like the
        // claimed-row fan-out). None attached → nothing owed to a disconnected
        // session; its retained context arrives at the next subscribe/resume.
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
                seq,
                sub: sub.clone(),
            };
            if handle.durable_tx.try_send(delivery).is_err() {
                // Full queue: a fold-0 feed has no row to re-park and nothing is
                // owed, so the loss is real and the wire's silence is the
                // contract — recovery is the retained window at the next
                // subscribe/resume. This is the deliberate divergence from the
                // claimed-row path (which unclaims a row for redelivery): there
                // is no row here.
                warn!(
                    slug = %slug,
                    channel = %envelope.channel,
                    seq,
                    "surface durable depth-0 context feed: session queue full; row-less \
                     delivery dropped (recovered at the next resume)"
                );
            }
        }
    }

    fn any_context_session_attached(&self, channel: &str, targets: &[SubscriberEntryKind]) -> bool {
        let Some(state) = self.state.get() else {
            // No state wired yet — no session can be attached.
            return false;
        };
        targets.iter().any(|key| {
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
                // Render via the unified timestamped batch formatter (design §2.10, R9).
                // All ingress — single or batched, live-inject or drain — renders through
                // render_event_drain (which calls format_event_batch with the event's
                // created_at timestamp). This is strictly better than the former
                // render_immediate_event: every event now gains a timestamp.
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
            // Ingress rows are conversation-targeted by invariant (submit_ingress
            // writes for_conversation; design §2.2). Surfaces bind only
            // brenn:/ephemeral: channels while ingress rows exist only on
            // webhook:/mqtt: channels, and parked subscribers (WASM/system) take
            // bus tool requests, not ingress — so any non-conversation ingress
            // target is a host-wiring invariant violation → panic.
            DeliveryRoute::SurfaceSessions | DeliveryRoute::Parked => {
                panic!(
                    "WakeRouter::deliver_ingress called for non-conversation subscriber {key:?} — \
                     host-wiring invariant violated: ingress rows only target conversations"
                );
            }
        }
    }

    fn spawn_eager_wake(&self, key: &SubscriberEntryKind, subscriber: &ParticipantId) {
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
                state.spawn_eager_wake(conversation_id, Tz::UTC);
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

    fn delivery_shape(&self, key: &SubscriberEntryKind) -> DeliveryShape {
        let map = self.bindings.read().expect("bindings RwLock poisoned");
        match map.get(key) {
            Some(DeliveryBinding::ConversationBridge) => DeliveryShape::Inline {
                marks_own_delivery: false,
            },
            Some(DeliveryBinding::SurfaceSessions) => DeliveryShape::Inline {
                marks_own_delivery: true,
            },
            Some(DeliveryBinding::ParkedNotify(_)) => DeliveryShape::ParkedWake,
            None => panic!(
                "delivery_shape: no delivery binding registered for {key:?} — \
                 host-wiring invariant violated"
            ),
        }
    }

    fn alarm(&self, channel: &str, subscriber: &ParticipantId) {
        // Fire an alert via `AlertDispatcher` (design §2.8).
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
                "Channel {channel:?} subscriber {:?}: push-depth overflow — \
                 oldest push-claim retired (noise = alarm).",
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

    /// `deliver` returns `Ok(false)` when the target conversation has
    /// no active bridge (the dispatcher maps this to "park" /
    /// eager-wake instead of error). No render work is performed because
    /// the bridge check returns None before the renderer is called.
    #[tokio::test]
    async fn deliver_returns_ok_false_when_no_bridge() {
        use brenn_lib::messaging::{MessageEnvelope, Urgency};
        use chrono::Utc;
        use uuid::Uuid;
        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.register_delivery_binding(conv_key(), DeliveryBinding::ConversationBridge);
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
        let result = router
            .deliver(
                &conv_key(),
                &ParticipantId::for_conversation(42),
                &env,
                1,
                1,
            )
            .await;
        assert!(matches!(result, Ok(false)));
    }

    /// `WakeRouterImpl::new` leaves `state` unset; the caller must
    /// invoke `set_state` before any background task runs that can
    /// reach `spawn_eager_wake` (review F1 ordering). Renamed from a
    /// previous misleading name that promised second-call coverage
    /// (review F28). We can't cheaply construct a real `AppState` in
    /// this unit test (~25 fields); the second-call panic is covered
    /// by `OnceCell::set`-returns-Err in std and `set_state`'s own
    /// `.expect(...)` line.
    #[test]
    fn new_leaves_state_unset() {
        let router = WakeRouterImpl::new(ActiveBridges::new());
        assert!(router.state.get().is_none());
    }

    /// After F2, `spawn_eager_wake` panics rather than warn-and-no-op
    /// when state is unset. Lock that contract: state-unset → panic.
    #[test]
    #[should_panic(expected = "WakeRouter state must be set")]
    fn spawn_eager_wake_panics_when_state_unset() {
        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.register_delivery_binding(conv_key(), DeliveryBinding::ConversationBridge);
        router.spawn_eager_wake(&conv_key(), &ParticipantId::for_conversation(42));
    }

    /// `render_messages_received_single` must produce a render whose `text`
    /// is byte-identical to `format_messaging_event_single` (singular
    /// `[Brenn message]` heading + JSON-object body, NOT the batch shape).
    /// This is the path-symmetry invariant: a future accidental switch to the
    /// batch renderer would silently change CC-facing content.
    #[test]
    fn render_messages_received_single_produces_correct_card() {
        use brenn_lib::messaging::{MessageEnvelope, Urgency};
        use brenn_lib::ws_types::SystemMessageCategory;
        use chrono::Utc;
        use uuid::Uuid;

        let env = MessageEnvelope {
            message_id: Uuid::new_v4(),
            source: "host".into(),
            channel: "brenn:ch".into(),
            sender: "alice".into(),
            publish_ts: Utc::now(),
            body: "hello world".into(),
            reply_to: None,
            delivery_deadline: None,
            deliver_after: None,
            urgency: Urgency::Normal,
            envelope_type: brenn_lib::messaging::ChannelScheme::Brenn,
        };

        let rendered = render_messages_received_single(&env);

        // text must be wrapped in <brenn-messages> with no preamble.
        assert!(
            rendered.text.starts_with("<brenn-messages>\n{"),
            "expected <brenn-messages> + JSON-object, got: {}",
            &rendered.text[..rendered.text.len().min(120)],
        );
        assert!(rendered.text.ends_with("\n</brenn-messages>"));
        // No preamble present.
        assert!(!rendered.text.contains("[Brenn message]"));
        // HTML wraps in the messages-received class.
        assert!(
            rendered
                .rendered_html
                .contains("brenn-system-messages-received"),
            "rendered_html must carry brenn-system-messages-received class: {}",
            rendered.rendered_html,
        );
        // Category tag.
        assert_eq!(rendered.category, SystemMessageCategory::MessagesReceived);
        // messaging_card_html must be None for the live path — drain path only.
        assert!(
            rendered.messaging_card_html.is_none(),
            "messaging_card_html must be None for the single-envelope renderer (live path only)",
        );
    }

    /// `WakeRouterImpl::deliver` with an active bridge in the registry returns
    /// `Ok(true)`. This exercises the recovery-then-dispatch path
    /// (`as_conversation_id` → `active_bridges.get` → `send_system_message`)
    /// that the render-symmetry test above does not reach, and guards AC#1
    /// (behavior-identical delivery) end-to-end through the adapter.
    ///
    /// The test bridge has no live CC session so `send_system_message` will fail
    /// to send to CC — but persistence + broadcast succeed first, and the
    /// function returns `Err(...)`. We therefore assert `Err` (not `Ok(true)`)
    /// and that it reached the bridge-send path (not the no-bridge `Ok(false)` path).
    #[tokio::test]
    async fn deliver_reaches_bridge_when_registered() {
        use brenn_lib::messaging::{MessageEnvelope, Urgency};
        use chrono::Utc;
        use uuid::Uuid;

        let db = brenn_lib::db::init_db_memory();
        let conversation_id = {
            let conn = db.lock().await;
            let uid = brenn_lib::auth::user::create_user(&conn, "testuser", "$argon2id$fake");
            brenn_lib::conversation::create_conversation(&conn, uid, "test-app", false)
        };

        let active_bridges = ActiveBridges::new();
        let (broadcast_tx, _broadcast_rx) = tokio::sync::broadcast::channel(16);
        let bridge = crate::active_bridge::ActiveBridge::inject_for_test(
            1,
            conversation_id,
            "test-app",
            db,
            broadcast_tx,
        );
        active_bridges.insert(conversation_id, bridge).await;

        let router = WakeRouterImpl::new(active_bridges);
        router.register_delivery_binding(conv_key(), DeliveryBinding::ConversationBridge);
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

        // The test bridge has no CC session so send_system_message returns Err
        // after persisting. The Err proves the code reached the bridge-found branch
        // rather than returning Ok(false) (no bridge). If deliver returned Ok(false),
        // the bridge lookup failed — the ParticipantId → conversation_id recovery
        // path is broken.
        let result = router
            .deliver(
                &conv_key(),
                &ParticipantId::for_conversation(conversation_id),
                &env,
                1,
                1,
            )
            .await;
        assert!(
            result.is_err(),
            "expected Err (bridge found but CC session absent), got Ok(false) \
             which would indicate the bridge was not found: {result:?}"
        );
    }

    /// `deliver` for a parked (`wasm:`) subscriber panics — reaching it is a
    /// host-wiring invariant violation (`dispatch_row` gates parked rows to
    /// `spawn_eager_wake` and never calls `deliver` for them).
    #[tokio::test]
    #[should_panic(expected = "WakeRouter::deliver called for parked subscriber")]
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
        let _ = router
            .deliver(&key, &ParticipantId::for_wasm("my-consumer"), &env, 1, 1)
            .await;
    }

    /// `deliver_ingress` for a parked (`wasm:`) subscriber panics — ingress rows
    /// are conversation-targeted by invariant (design §2.2).
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

    /// `spawn_eager_wake` for a `wasm:` subscriber notifies the registered `Notify`
    /// (design §2.2). The off-loop dispatch task holds the `Arc` clone; `notify_one`
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
    /// invariant violation (design §2.2).
    #[test]
    #[should_panic(expected = "no delivery binding registered")]
    fn spawn_eager_wake_panics_for_unregistered_wasm_slug() {
        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.spawn_eager_wake(
            &SubscriberEntryKind::Wasm("not-registered".to_string()),
            &ParticipantId::for_wasm("not-registered"),
        );
    }

    /// Build a `Messenger` over `db` (empty directory — the Surface `deliver` arm
    /// only reaches `db()` for the claim), declare one `brenn:` channel, and
    /// insert one Immediate pending-push row targeting `surface:<slug>` on it.
    /// Returns `(messenger, participant, push_id, seq)` where `push_id` is the
    /// real claimable row and `seq` is the message id.
    async fn surface_push_fixture(
        db: &brenn_lib::db::Db,
        slug: &str,
        channel_addr: &str,
    ) -> (
        Arc<brenn_lib::messaging::Messenger>,
        ParticipantId,
        i64,
        i64,
    ) {
        use brenn_lib::messaging::config::{
            ChannelConfigRaw, MessagingGlobalConfig, build_channel_entries,
        };
        use brenn_lib::messaging::db::{
            PendingPushInsert, insert_message_with_pushes, upsert_channels, utc_to_ns,
        };
        use brenn_lib::messaging::query::NoopWakeRouter;
        use brenn_lib::messaging::{ChannelScheme, MessagingDirectory, Messenger, Urgency};
        use chrono::Utc;
        use indexmap::IndexMap;
        use uuid::Uuid;

        let bare = channel_addr.strip_prefix("brenn:").expect("brenn: address");
        let raw = ChannelConfigRaw {
            uuid: Uuid::new_v4().to_string(),
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
        let (push_id, seq) = {
            let conn = db.lock().await;
            upsert_channels(&conn, std::slice::from_ref(&entry));
            let push = PendingPushInsert {
                target_subscriber: participant.clone(),
                target_app_slug: slug.to_string(),
                eager_wake: true,
                release_after: None,
                delivery_deadline: None,
            };
            let msg = insert_message_with_pushes(
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
                &[push],
            );
            (msg.push_ids[0], msg.id)
        };
        let messenger = Messenger::new(
            db.clone(),
            Arc::new(MessagingDirectory::with_entries(vec![entry])),
            Arc::from("test"),
            Arc::new(IndexMap::new()),
            Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        );
        (messenger, participant, push_id, seq)
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

    /// Register a session handle for `slug` subscribed to `channel`, returning the
    /// guard (keep alive), the live-delivery receiver, and the drain notifier.
    fn register_surface_session(
        state: &AppState,
        slug: &str,
        channel: &str,
    ) -> (
        crate::routes::surface::registry::SurfaceSessionGuard,
        tokio::sync::mpsc::Receiver<DurableDelivery>,
        Arc<tokio::sync::Notify>,
    ) {
        use std::collections::HashSet;
        use std::net::{IpAddr, Ipv4Addr};
        use std::sync::Mutex;

        use crate::routes::surface::registry::{
            DURABLE_QUEUE_FRAMES, SessionCaps, SurfaceSessionHandle,
        };

        let (durable_tx, durable_rx) = tokio::sync::mpsc::channel(DURABLE_QUEUE_FRAMES);
        let drain_notify = Arc::new(tokio::sync::Notify::new());
        let handle = SurfaceSessionHandle {
            session_id: uuid::Uuid::new_v4(),
            username: "dev".to_string(),
            client_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            connected_at: chrono::Utc::now(),
            durable_tx,
            durable_subs: Arc::new(Mutex::new(HashSet::from([SubKey {
                instance: "protobar".to_string(),
                channel: channel.to_string(),
            }]))),
            drain_notify: drain_notify.clone(),
        };
        let guard = state
            .surface_registry
            .try_register(slug, handle, SessionCaps::UNCAPPED)
            .expect("register");
        (guard, durable_rx, drain_notify)
    }

    /// `deliver` for a `surface:` subscriber with an attached, subscribed session
    /// claims the push row and hands it to that session's live queue, returning
    /// `Ok(true)`.
    #[tokio::test]
    async fn deliver_surface_fans_out_and_claims() {
        let db = brenn_lib::db::init_db_memory();
        let channel = "brenn:durable-demo";
        let (messenger, participant, push_id, seq) =
            surface_push_fixture(&db, "deskbar", channel).await;

        let mut state = AppState::for_test(db.clone(), None);
        state.messenger = Some(messenger);
        let (_guard, mut rx, notify) = register_surface_session(&state, "deskbar", channel);

        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.set_state(state);
        router.register_delivery_binding(surface_key(), DeliveryBinding::SurfaceSessions);

        let result = router
            .deliver(
                &surface_key(),
                &participant,
                &surface_envelope(channel),
                push_id,
                seq,
            )
            .await;
        assert!(matches!(result, Ok(true)));

        // The claimed row landed on the session's live queue with the wire seq.
        let delivered = rx.try_recv().expect("live delivery enqueued");
        assert_eq!(delivered.envelope.channel, channel);
        assert_eq!(delivered.seq, seq);
        // Per-delivery drain nudge fired.
        tokio::time::timeout(std::time::Duration::from_millis(10), notify.notified())
            .await
            .expect("drain nudge fired");

        // The push row is claimed (a re-claim finds nothing).
        let conn = db.lock().await;
        assert!(brenn_lib::messaging::db::claim_pending_pushes(&conn, &[push_id]).is_empty());
    }

    /// `deliver` for a `surface:` subscriber with no attached/subscribed session
    /// parks (`Ok(false)`) and leaves the row unclaimed for a later attach+drain.
    #[tokio::test]
    async fn deliver_surface_no_session_parks() {
        let db = brenn_lib::db::init_db_memory();
        let channel = "brenn:durable-demo";
        let (messenger, participant, push_id, seq) =
            surface_push_fixture(&db, "deskbar", channel).await;

        let mut state = AppState::for_test(db.clone(), None);
        state.messenger = Some(messenger);
        // No session registered.

        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.set_state(state);
        router.register_delivery_binding(surface_key(), DeliveryBinding::SurfaceSessions);

        let result = router
            .deliver(
                &surface_key(),
                &participant,
                &surface_envelope(channel),
                push_id,
                seq,
            )
            .await;
        assert!(matches!(result, Ok(false)));

        // Row stays unclaimed (claimable).
        let conn = db.lock().await;
        assert_eq!(
            brenn_lib::messaging::db::claim_pending_pushes(&conn, &[push_id]),
            vec![push_id]
        );
    }

    /// A row already claimed by a session drain → `deliver` returns `Ok(true)`
    /// without re-sending (the claimer owns the send).
    #[tokio::test]
    async fn deliver_surface_already_claimed_skips_send() {
        let db = brenn_lib::db::init_db_memory();
        let channel = "brenn:durable-demo";
        let (messenger, participant, push_id, seq) =
            surface_push_fixture(&db, "deskbar", channel).await;

        // Pre-claim the row, as a session drain would.
        {
            let conn = messenger.db().lock().await;
            assert_eq!(
                brenn_lib::messaging::db::claim_pending_pushes(&conn, &[push_id]),
                vec![push_id]
            );
        }

        let mut state = AppState::for_test(db.clone(), None);
        state.messenger = Some(messenger);
        let (_guard, mut rx, _notify) = register_surface_session(&state, "deskbar", channel);

        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.set_state(state);
        router.register_delivery_binding(surface_key(), DeliveryBinding::SurfaceSessions);

        let result = router
            .deliver(
                &surface_key(),
                &participant,
                &surface_envelope(channel),
                push_id,
                seq,
            )
            .await;
        assert!(matches!(result, Ok(true)));
        // Nothing was sent — the drain owns the row.
        assert!(rx.try_recv().is_err());
    }

    /// When every subscribed session's live queue is unusable (here: receiver
    /// dropped, so `try_send` fails), `deliver` unclaims the row and returns
    /// `Ok(false)` so it re-parks for a later drain — the "slow session, queue
    /// full" recovery path.
    #[tokio::test]
    async fn deliver_surface_all_queues_full_unclaims_and_parks() {
        let db = brenn_lib::db::init_db_memory();
        let channel = "brenn:durable-demo";
        let (messenger, participant, push_id, seq) =
            surface_push_fixture(&db, "deskbar", channel).await;

        let mut state = AppState::for_test(db.clone(), None);
        state.messenger = Some(messenger);
        let (_guard, rx, _notify) = register_surface_session(&state, "deskbar", channel);
        // Close the session's live queue so every try_send is rejected.
        drop(rx);

        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.set_state(state);
        router.register_delivery_binding(surface_key(), DeliveryBinding::SurfaceSessions);

        let result = router
            .deliver(
                &surface_key(),
                &participant,
                &surface_envelope(channel),
                push_id,
                seq,
            )
            .await;
        assert!(matches!(result, Ok(false)));

        // The row was unclaimed (re-parked): a fresh claim finds it again.
        let conn = db.lock().await;
        assert_eq!(
            brenn_lib::messaging::db::claim_pending_pushes(&conn, &[push_id]),
            vec![push_id]
        );
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

        let delivered = rx.try_recv().expect("row-less delivery enqueued");
        assert_eq!(delivered.envelope.channel, channel);
        assert_eq!(delivered.seq, 7);
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

    /// `any_context_session_attached` — the publish-time build-skip precheck —
    /// answers true for a subscribed attached session and false with none. The
    /// false branch is the cost saver: no envelope is built when no page is open.
    #[tokio::test]
    async fn any_context_session_attached_true_with_subscriber_false_without() {
        let db = brenn_lib::db::init_db_memory();
        let channel = "brenn:durable-demo";
        let state = AppState::for_test(db.clone(), None);

        let router = WakeRouterImpl::new(ActiveBridges::new());
        router.set_state(state.clone());
        assert!(
            !router.any_context_session_attached(channel, &[surface_key()]),
            "no session open — nothing to feed, skip the build"
        );

        let (_guard, _rx, _notify) = register_surface_session(&state, "deskbar", channel);
        assert!(
            router.any_context_session_attached(channel, &[surface_key()]),
            "the subscribed attached session is a feed target"
        );
        assert!(
            !router.any_context_session_attached("brenn:other-channel", &[surface_key()]),
            "subscribed to a different channel — not a target here"
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
            DeliveryShape::Inline {
                marks_own_delivery: false
            }
        ));
        assert!(matches!(
            router.delivery_shape(&surface_key()),
            DeliveryShape::Inline {
                marks_own_delivery: true
            }
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
    #[should_panic(expected = "WakeRouter::deliver called for parked subscriber")]
    async fn deliver_panics_for_system_subscriber() {
        let router = WakeRouterImpl::new(ActiveBridges::new());
        let key = SubscriberEntryKind::System("tool-executor".to_string());
        router.register_delivery_binding(
            key.clone(),
            DeliveryBinding::ParkedNotify(Arc::new(tokio::sync::Notify::new())),
        );
        let _ = router
            .deliver(
                &key,
                &ParticipantId::for_system("tool-executor"),
                &surface_envelope("brenn:whatever"),
                1,
                1,
            )
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

    /// END-TO-END regression: a `brenn:` message published to a channel whose
    /// only subscriber is a Wasm consumer must PARK the pending-push row (leave
    /// it pending so the off-loop dispatch task drains it), NOT deliver it
    /// through the shared dispatch loop.
    ///
    /// This wires the REAL `WakeRouterImpl` (not a mock) into a REAL `Messenger`
    /// and drives the real dispatch path (`dispatch_row`). The historical bug:
    /// `dispatch_pending_pushes` → `deliver_or_park` → `WakeRouter::deliver`
    /// with no Wasm gate. Post-fix: `dispatch_row` gates Wasm rows to
    /// `spawn_eager_wake`, never calling `deliver` for them (WASM panic invariant).
    /// Existing publish-path tests all used a mock router that returned `Ok(false)`
    /// for Wasm, so the panic was never caught before the fix.
    ///
    /// Post-fix the expected behavior is: the row stays pending (parked), an
    /// eager-wake fires to the registered notifier, and no panic occurs.
    #[tokio::test]
    async fn publish_to_wasm_subscriber_parks_does_not_deliver() {
        use brenn_lib::db::init_db_memory;
        use brenn_lib::messaging::config::{Depth, MessagingGlobalConfig};
        use brenn_lib::messaging::db::{
            PendingPushInsert, insert_message_with_pushes, upsert_channels, utc_to_ns,
        };
        use brenn_lib::messaging::testutils;
        use brenn_lib::messaging::{ChannelScheme, MessagingDirectory, Messenger, Urgency};
        use chrono::Utc;
        use indexmap::IndexMap;

        let slug = "demo-consumer";

        // Arrange: in-memory DB + one channel whose only subscriber is the Wasm
        // consumer. Noise Silent + Depth Unbounded so the alarm/alert path is never
        // reached.
        let db = init_db_memory();
        let entry = testutils::wasm_channel_entry(
            slug,
            "wasm-deliver-park-ch",
            Depth::Unbounded,
            Depth::Unbounded,
        );
        {
            let conn = db.lock().await;
            upsert_channels(&conn, std::slice::from_ref(&*entry));
        }
        let directory = Arc::new(MessagingDirectory::with_entries(vec![(*entry).clone()]));

        // The REAL router (not a mock). Register a ParkedNotify binding and keep a
        // clone so we can assert the eager-wake was fired after dispatch.
        let router = Arc::new(WakeRouterImpl::new(ActiveBridges::new()));
        let notify = Arc::new(tokio::sync::Notify::new());
        router.register_delivery_binding(
            SubscriberEntryKind::Wasm(slug.to_string()),
            DeliveryBinding::ParkedNotify(Arc::clone(&notify)),
        );

        let messenger = Messenger::new(
            db,
            directory,
            Arc::from("test"),
            Arc::new(IndexMap::new()),
            router as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        );

        let wasm_sub = ParticipantId::for_wasm(slug);

        // Insert a message + a single Immediate pending-push row targeting the
        // Wasm subscriber, then drive the real publish dispatch path.
        let message_id = {
            let ts_ns = utc_to_ns(Utc::now());
            let conn = messenger.db().lock().await;
            let push = PendingPushInsert {
                target_subscriber: wasm_sub.clone(),
                target_app_slug: String::new(),
                eager_wake: true,
                release_after: None,
                delivery_deadline: None,
            };
            let msg = insert_message_with_pushes(
                &conn,
                entry.uuid,
                "test",
                "test-sender",
                "hello wasm",
                Urgency::Normal,
                ChannelScheme::Brenn,
                None,
                None,
                None,
                ts_ns,
                &[push],
            );
            assert_eq!(msg.push_ids.len(), 1);
            msg.id
        };

        // Act: drive the real dispatch_row (dispatcher layer) with the real router.
        // This is the precise layer that was previously panicking for Wasm rows.
        let push_row = {
            let conn = messenger.db().lock().await;
            brenn_lib::messaging::db::load_all_dispatchable_pushes(&conn, Utc::now())
        };
        assert_eq!(push_row.len(), 1, "exactly one dispatchable row");
        let (ref row, deadline_expired) = push_row[0];
        let _ = message_id; // used only during setup
        brenn_lib::messaging::dispatcher::dispatch_row(
            messenger.router().as_ref(),
            row,
            deadline_expired,
            false,
        )
        .await;

        // Assert: the row is PARKED (still pending), not delivered.
        let rows = messenger.load_pending_pushes(&wasm_sub).await;
        assert_eq!(
            rows.len(),
            1,
            "Wasm pending-push row must remain parked (pending), not be delivered \
             through the shared dispatch loop"
        );

        // Assert: the eager-wake notifier was fired so the off-loop dispatch
        // task is woken promptly. The permit is set by notify_one.
        tokio::time::timeout(std::time::Duration::from_millis(10), notify.notified())
            .await
            .expect(
                "eager-wake Notify must be fired by dispatch_row for Wasm subscriber; \
                 notified() should resolve immediately after notify_one",
            );
    }
}
