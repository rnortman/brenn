use super::ParticipantId;
use crate::ingress::Event;

/// One pending-push row plus its decoded event, ready for the dispatch path.
///
/// Only the channel-less direct-to-participant ingress deliveries still ride
/// this table: what a subscriber is owed on a *channel* is its cursor position.
///
/// `eager_wake` is the resolved per-subscriber wake decision computed at
/// insert time from `WakeMin::wakes(urgency)`. The DB column is
/// `messaging_pending_pushes.eager_wake INTEGER (0 or 1)`.
#[derive(Debug, Clone)]
pub struct PendingPushRow {
    pub push_id: i64,
    /// `messaging_messages.id` of the parent message (the FK
    /// `messaging_pending_pushes.message_id`).
    pub message_id: i64,
    pub event: Event,
    pub target_subscriber: ParticipantId,
    /// The app config slug this row was published to (`messaging_pending_pushes.
    /// target_app_slug`). For a `conversation:` (app-backed) target it names the
    /// backing app. `registration_key` uses it to key a `Conversation` target to
    /// its `App(slug)` registration.
    pub target_app_slug: String,
    pub eager_wake: bool,
}
