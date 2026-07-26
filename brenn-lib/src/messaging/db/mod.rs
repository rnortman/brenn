//! Messaging DB operations.
//!
//! All schema is migrated by [`run_messaging_migrations`], which is invoked
//! from `crate::db::run_migrations`.
//!
//! NOTE on retention: bus messages are evicted by `bus_gc_evict_channel` when
//! channel depth exceeds `retain_depth`. Delivered ingress messages are reaped
//! by the ingress cleanup loop after the configured retention window (see
//! `delete_delivered_ingress_pushes_before`).

use super::ParticipantId;

mod shared;
pub(crate) use shared::parse_rfc3339;
pub use shared::{ns_to_utc, utc_to_ns};

mod envelope_column;
pub(crate) use envelope_column::EnvelopeTypeColumn;

mod types;
pub use types::PendingPushRow;

mod budget;
pub use budget::{
    BudgetDecrement, decrement_send_budget, read_send_budget, refund_send_budget, reset_send_budget,
};

mod bootstrap;
pub use bootstrap::{load_channels_by_uuids, prune_dropped_dynamic_subscriptions, upsert_channels};

mod dynamic;
pub use dynamic::{
    DynamicSubscriptionRow, delete_dynamic_subscription, insert_dynamic_subscription,
    load_dynamic_subscription_for, load_dynamic_subscriptions,
};

mod sender_check;
pub use sender_check::assert_senders_structured;

mod schema;
pub use schema::run_messaging_migrations;

mod store_identity;
pub use store_identity::{
    StoreIdentity, bump_incarnation, ensure_store_identity, read_store_identity,
};

mod ingress;
#[cfg(test)]
pub(crate) use ingress::LOAD_PENDING_INGRESS_FOR_DRAIN_SQL;
pub use ingress::{
    delete_delivered_ingress_pushes_before, insert_ingress_message, insert_ingress_message_raw,
    load_pending_ingress_for_drain, mark_stale_undelivered_ingress_repo_sync,
};

mod bus;
#[cfg(test)]
pub(crate) use bus::LOAD_DISPATCHABLE_INGRESS_SQL;
pub use bus::{
    BusGcEviction, EditFieldsApplied, InsertedMessage, MessageLookup, bus_gc_evict_channel,
    channel_last_retained_seq, channel_resume_epoch, channel_retained_count_after_seq,
    channel_retention_frontier, insert_message, insert_message_in_tx,
    list_pending_messages_for_sender, load_channel_messages_after_seq, load_channel_retained_tail,
    load_channel_retained_window_seq, load_dispatchable_ingress_pushes, load_envelope_by_uuid,
    lookup_message_for_authorship, mark_pending_pushes_delivered, retained_tail_floor_seq,
    update_parked_message, withdraw_parked_message,
};

mod cursors;
pub use cursors::{
    SubscriberCursorRow, all_subscriber_cursors, channel_subscriber_cursors,
    cursor_has_deliverable, delete_subscriber_cursor, deliverable_cursor_subscribers,
    ensure_subscriber_cursor, load_subscriber_cursor, retune_subscriber_cursor_depth,
    set_subscriber_cursor_position,
};

mod deferral;
pub use deferral::{
    DeferredLookup, DeferredRow, ReleasedRow, count_deferred, delete_deferred,
    earliest_channel_release, edit_deferred, list_deferred_for_sender, lookup_deferred,
    release_due_for_channel,
};

#[cfg(test)]
mod tests;
