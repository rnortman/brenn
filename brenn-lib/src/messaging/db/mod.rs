//! Messaging DB operations.
//!
//! All schema is migrated by [`run_messaging_migrations`], which is invoked
//! from `crate::db::run_migrations`.
//!
//! NOTE on retention: bus messages are evicted by `bus_gc_evict_channel` when
//! channel depth exceeds `retain_depth`; pending pushes are reaped by
//! `bus_gc_retire_pushes` when push depth exceeds `push_depth`. Delivered
//! ingress messages are reaped by the ingress cleanup loop after the configured
//! retention window (see `delete_delivered_ingress_pushes_before`).

use super::{IngressOrBus, ParticipantId};

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
pub use bootstrap::{
    load_channels_by_uuids, load_subscription_keys, mirror_dynamic_subscriptions,
    prune_dropped_dynamic_subscriptions, rebuild_subscriptions, upsert_channels,
};

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
pub use ingress::{
    delete_delivered_ingress_pushes_before, insert_ingress_message, insert_ingress_message_raw,
    load_pending_pushes_for_drain, mark_stale_undelivered_ingress_repo_sync,
};

mod bus;
#[cfg(test)]
pub(crate) use bus::LOAD_ALL_DISPATCHABLE_PUSHES_SQL;
pub use bus::{
    ChannelPushRow, EditFieldsApplied, EditUpdateResult, InsertedMessage, MessageLookup,
    PendingPushInsert, ReleasedPushRow, TentativePushRow, bus_gc_evict_channel,
    bus_gc_retire_pushes, cancel_pending_pushes_for_message, channel_deliverable_subscribers,
    channel_has_deliverable_for, channel_last_retained_seq, channel_resume_epoch,
    channel_retained_count_after_seq, claim_pending_pushes, confirm_pending_pushes,
    delete_pending_push_by_id, delete_pushes_for_subscriber, earliest_pending_deadline,
    insert_message_with_pushes, insert_message_with_pushes_in_tx, list_pending_messages_for_sender,
    load_all_dispatchable_pushes, load_channel_messages_after_seq, load_channel_retained_tail,
    load_channel_retained_window_seq, load_confirm_pending_pushes, load_envelope_by_uuid,
    load_pending_pushes_for_channel, load_push_window, load_pushes_by_ids,
    load_released_push_window_rows, lookup_message_for_authorship, mark_pending_pushes_delivered,
    min_owed_retained_seq, owed_push_positions, pending_push_exists,
    pending_pushes_outside_channels, seed_pending_pushes_for_messages, stamp_confirm_pending,
    unclaim_confirm_pending_pushes, unclaim_pending_pushes, update_message_and_pending_pushes,
    withdraw_parked_message,
};

mod deferral;
pub use deferral::{
    DeferredLookup, DeferredRow, ReleasedBatch, ReleasedRow, count_deferred, delete_deferred,
    earliest_channel_release, edit_deferred, list_deferred_for_sender, lookup_deferred,
    release_due_for_channel,
};

#[cfg(test)]
mod tests;
