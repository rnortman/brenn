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
pub use budget::{BudgetDecrement, decrement_send_budget, read_send_budget, reset_send_budget};

mod bootstrap;
pub use bootstrap::{
    load_channels_by_uuids, mirror_dynamic_subscriptions, prune_dropped_dynamic_subscriptions,
    rebuild_subscriptions, upsert_channels,
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
    EditFieldsApplied, EditUpdateResult, InsertedMessage, MessageLookup, PendingPushInsert,
    ReleasedPushRow, bus_gc_evict_channel, bus_gc_retire_pushes, cancel_pending_pushes_for_message,
    channel_max_message_id, channel_min_message_id, claim_pending_pushes, confirm_pending_pushes,
    delete_pending_push_by_id, earliest_pending_deadline, earliest_pending_release,
    insert_message_with_pushes, insert_message_with_pushes_in_tx, list_pending_messages_for_sender,
    load_all_dispatchable_pushes, load_channel_messages_after, load_confirm_pending_pushes,
    load_envelope_by_uuid, load_pending_pushes_for_channel, load_push_window, load_pushes_by_ids,
    load_released_push_window_rows, lookup_message_for_authorship, mark_pending_pushes_delivered,
    pending_push_exists, release_due_pushes, stamp_confirm_pending, unclaim_confirm_pending_pushes,
    unclaim_pending_pushes, update_message_and_pending_pushes,
};

#[cfg(test)]
mod tests;
