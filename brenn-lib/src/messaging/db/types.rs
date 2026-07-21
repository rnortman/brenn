use super::{IngressOrBus, ParticipantId};

/// One pending-push row plus its typed payload, ready for the dispatch or
/// drain path.
///
/// `payload` is `IngressOrBus::Bus(MessageEnvelope)` for `kind='brenn'` rows
/// and `IngressOrBus::Ingress(Event)` for `kind='ingress'` rows.
///
/// **Invariant:** callers that are bus-only by construction (dispatch path,
/// deadline timer, deliver-after task) must call `payload.unwrap_bus()` to
/// enforce that ingress rows never reach bus-rendering logic.
///
/// `eager_wake` is the resolved per-subscriber wake decision computed at
/// insert time from `WakeMin::wakes(urgency)`. The DB column is
/// `messaging_pending_pushes.eager_wake INTEGER (0 or 1)`.
#[derive(Debug, Clone)]
pub struct PendingPushRow {
    pub push_id: i64,
    /// `messaging_messages.id` of the parent message (the FK
    /// `messaging_pending_pushes.message_id`). This is the globally monotone
    /// rowid the surface session mints a durable resume cursor's high-water from;
    /// the Conversation/ingress delivery paths ignore it. Because it is a single
    /// global counter, not per-channel, the cursor high-waters a surface client
    /// carries leak aggregate cross-channel publish volume/timing — accepted risk
    /// under the single-operator model, recorded in `docs/security-posture.md` §6.
    pub message_id: i64,
    pub payload: IngressOrBus,
    pub target_subscriber: ParticipantId,
    /// The app config slug this row was published to (`messaging_pending_pushes.
    /// target_app_slug`). For a `conversation:` (app-backed) target it names the
    /// backing app; for `wasm:`/`surface:`/`system:` targets it mirrors the
    /// subscriber's own slug/component. `registration_key` uses it to key a
    /// `Conversation` target to its `App(slug)` registration.
    pub target_app_slug: String,
    pub eager_wake: bool,
}
