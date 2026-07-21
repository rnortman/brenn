//! Test helpers for building minimal in-memory `Messenger` fixtures with a single
//! WASM-subscriber channel.
//!
//! Available under `#[cfg(test)]` or when the `testutils` feature is enabled.
//! Pattern mirrors `run_deliver_after_pass` / `run_deadline_pass` in `dispatcher.rs`.

use std::collections::HashMap;
use std::sync::Arc;

use uuid::Uuid;

use super::{
    ChannelEntry, ChannelScheme, MessagingDirectory, Messenger, ParticipantId, SubscriberEntry,
    SubscriberEntryKind, SubscriberRegistration, Urgency, WakeEconomics, WakeMin, WakeRouter,
    canonical_address,
    config::{self, Depth, EphemeralChannelEntry, MessagingGlobalConfig, NoiseLevel},
    db::{self, PendingPushInsert, insert_message_with_pushes, upsert_channels},
    ephemeral_channel_uuid_from_name,
    query::NoopWakeRouter,
};
use crate::access::AppPolicy;

/// Build a subscriber-registration map from a `slug → policy` map for a single
/// kind, applying `wake` to every entry. The per-kind builders below
/// (`wasm_registrations`, `surface_registrations`, `system_registrations`) wrap
/// this so test call sites that previously installed a per-kind policy side map
/// (`with_wasm_policies` etc.) install the equivalent registrations through the
/// one `with_subscriber_registrations` installer.
fn registrations_for(
    policies: HashMap<String, AppPolicy>,
    to_kind: impl Fn(String) -> SubscriberEntryKind,
    wake: WakeEconomics,
) -> HashMap<SubscriberEntryKind, SubscriberRegistration> {
    policies
        .into_iter()
        .map(|(slug, policy)| {
            (
                to_kind(slug),
                SubscriberRegistration {
                    policy: Arc::new(policy),
                    wake,
                },
            )
        })
        .collect()
}

/// Registrations for WASM consumer subscribers (`Eager` wake), from a
/// `slug → policy` map.
pub fn wasm_registrations(
    policies: HashMap<String, AppPolicy>,
) -> HashMap<SubscriberEntryKind, SubscriberRegistration> {
    registrations_for(policies, SubscriberEntryKind::Wasm, WakeEconomics::Eager)
}

/// Registrations for surface subscribers (`Eager` wake) at the **kernel grain**,
/// from a `slug → policy` map. Component instances are separate principals and
/// register separately; see [`surface_component_registrations`].
pub fn surface_registrations(
    policies: HashMap<String, AppPolicy>,
) -> HashMap<SubscriberEntryKind, SubscriberRegistration> {
    registrations_for(
        policies,
        |slug| SubscriberEntryKind::Surface {
            slug,
            instance: None,
        },
        WakeEconomics::Eager,
    )
}

/// Registrations for one surface's component instances (`Eager` wake), all
/// carrying `policy` — authority is per-surface, so boot installs the surface's
/// own policy at every instance grain.
pub fn surface_component_registrations(
    slug: &str,
    instances: &[&str],
    policy: AppPolicy,
) -> HashMap<SubscriberEntryKind, SubscriberRegistration> {
    instances
        .iter()
        .map(|instance| {
            (
                SubscriberEntryKind::Surface {
                    slug: slug.to_string(),
                    instance: Some((*instance).to_string()),
                },
                SubscriberRegistration {
                    policy: std::sync::Arc::new(policy.clone()),
                    wake: WakeEconomics::Eager,
                },
            )
        })
        .collect()
}

/// Registrations for system-substrate subscribers (`Eager` wake), from a
/// `component → policy` map.
pub fn system_registrations(
    policies: HashMap<String, AppPolicy>,
) -> HashMap<SubscriberEntryKind, SubscriberRegistration> {
    registrations_for(policies, SubscriberEntryKind::System, WakeEconomics::Eager)
}

/// Build a single-WASM-subscriber `ChannelEntry`.
///
/// Channel-level `push_depth`, `retain_depth`, and `standing_retain_depth` are
/// fixed `Depth::Unbounded` to keep fixture construction simple; parameterize if
/// a test needs bounded channel depth. Only the *subscriber* depths vary and are
/// taken as parameters.
///
/// `noise = Silent`, `sink = Drop`, `transport_type = Brenn`, `mount = None`.
pub fn wasm_channel_entry(
    slug: &str,
    channel_name: &str,
    push_depth: Depth,
    retain_depth: Depth,
) -> Arc<ChannelEntry> {
    let channel_uuid = Uuid::new_v4();
    let channel_addr = canonical_address(channel_name);
    Arc::new(ChannelEntry {
        uuid: channel_uuid,
        address: channel_addr,
        description: None,
        resolved_channel: config::ResolvedChannel {
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise: config::NoiseLevel::Silent,
            sink: config::Sink::Drop,
            wake_min: WakeMin::Normal,
        },
        subscribers: vec![SubscriberEntry {
            kind: SubscriberEntryKind::Wasm(slug.to_string()),
            push_depth,
            retain_depth,
            noise: config::NoiseLevel::Silent,
            wake_min: None,
        }],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    })
}

/// Build a default `brenn:` `ChannelEntry` with the given subscribers.
///
/// Channel-level depths are `Depth::Unbounded`, `noise = Silent`, `sink = Drop`,
/// `transport_type = Brenn`, `mount = None`, `description = None`, and the uuid is
/// fresh. Pass `subscribers` (often `vec![]`) for the per-subscriber wiring a test
/// needs. Single home for the default `ChannelEntry` literal so a new field is one
/// edit rather than one per test module.
pub fn test_channel_entry(name: &str, subscribers: Vec<SubscriberEntry>) -> ChannelEntry {
    ChannelEntry {
        uuid: Uuid::new_v4(),
        address: canonical_address(name),
        description: None,
        resolved_channel: config::ResolvedChannel {
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise: config::NoiseLevel::Silent,
            sink: config::Sink::Drop,
            wake_min: WakeMin::Normal,
        },
        subscribers,
        transport_type: ChannelScheme::Brenn,
        mount: None,
    }
}

/// Build an `EphemeralChannelEntry` with the deterministic name-derived uuid.
pub fn ephemeral_channel_entry(
    name: &str,
    retain_depth: u64,
    capacity: u32,
) -> EphemeralChannelEntry {
    EphemeralChannelEntry {
        uuid: ephemeral_channel_uuid_from_name(name),
        name: name.to_string(),
        // The channel rung transparent to global: an ephemeral binding that
        // states no push_depth/noise inherits these, matching the pre-rung
        // behavior where the binding inherited straight from global.
        push_depth: Depth::Unbounded,
        retain_depth,
        noise: NoiseLevel::Silent,
        capacity,
    }
}

/// Build an in-memory `Messenger` with a single WASM-subscriber channel, using a
/// noop wake router.
///
/// Returns `(messenger, channel_entry, wasm_subscriber_id)`.
///
/// For callers that need `Depth::Unbounded` for both depths, use the terse wrapper
/// [`build_wasm_messenger_unbounded`].
pub async fn build_wasm_messenger(
    slug: &str,
    channel_name: &str,
    push_depth: Depth,
    retain_depth: Depth,
) -> (Arc<Messenger>, Arc<ChannelEntry>, ParticipantId) {
    let db = crate::db::init_db_memory();
    let entry = wasm_channel_entry(slug, channel_name, push_depth, retain_depth);
    {
        let conn = db.lock().await;
        upsert_channels(&conn, std::slice::from_ref(&*entry));
    }
    let directory = Arc::new(MessagingDirectory::with_entries(vec![(*entry).clone()]));
    let messenger = Messenger::new(
        db,
        directory,
        Arc::from("test"),
        Arc::new(indexmap::IndexMap::new()),
        Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    );
    let wasm_sub = ParticipantId::for_wasm(slug);
    (messenger, entry, wasm_sub)
}

/// Terse wrapper around [`build_wasm_messenger`] for the common `Unbounded`/`Unbounded` case.
pub async fn build_wasm_messenger_unbounded(
    slug: &str,
    channel_name: &str,
) -> (Arc<Messenger>, Arc<ChannelEntry>, ParticipantId) {
    build_wasm_messenger(slug, channel_name, Depth::Unbounded, Depth::Unbounded).await
}

/// Insert one push message row for `subscriber` on `channel` at an explicit
/// timestamp and return `(push_id, message_uuid)`.
///
/// `envelope_type` is caller-supplied because some channels use `Webhook` rather than `Brenn`.
/// `ts_ns` is the message timestamp in nanoseconds since the Unix epoch.
/// Use [`insert_wasm_push`] when the exact timestamp does not matter.
pub async fn insert_wasm_push_at(
    messenger: &Messenger,
    channel: &ChannelEntry,
    subscriber: &ParticipantId,
    body: &str,
    envelope_type: ChannelScheme,
    ts_ns: i64,
) -> (i64, Uuid) {
    let conn = messenger.db().lock().await;
    let push = PendingPushInsert {
        target_subscriber: subscriber.clone(),
        target_app_slug: String::new(),
        eager_wake: true,
        release_after: None,
        delivery_deadline: None,
    };
    let inserted = insert_message_with_pushes(
        &conn,
        channel.uuid,
        "test",
        "test-sender",
        body,
        Urgency::Normal,
        envelope_type,
        None,
        None,
        None,
        ts_ns,
        &[push],
    );
    assert_eq!(inserted.push_ids.len(), 1);
    (inserted.push_ids[0], inserted.uuid)
}

/// Insert one push message row for `subscriber` on `channel` at the current
/// wall-clock time and return `(push_id, message_uuid)`.
///
/// `envelope_type` is caller-supplied because some channels use `Webhook` rather than `Brenn`.
/// Use [`insert_wasm_push_at`] when the test needs to control the exact timestamp.
pub async fn insert_wasm_push(
    messenger: &Messenger,
    channel: &ChannelEntry,
    subscriber: &ParticipantId,
    body: &str,
    envelope_type: ChannelScheme,
) -> (i64, Uuid) {
    let ts_ns = db::utc_to_ns(chrono::Utc::now());
    insert_wasm_push_at(messenger, channel, subscriber, body, envelope_type, ts_ns).await
}

/// Directly increment the in-memory drop counter for `(channel, subscriber)` by
/// `amount`. Used in tests that need to simulate push-overflow without going through
/// the full publish path (which requires noise=Metered and an app config).
///
/// Delegates to `Messenger::inject_drop` (a `#[cfg(test)]` method) so drop-counter
/// mutation is cfg-gated and does not leak beyond test builds (quality-3).
pub fn inject_drop(messenger: &Messenger, channel: &str, subscriber: &ParticipantId, amount: u64) {
    messenger.inject_drop(channel, subscriber, amount);
}

/// Insert a retained-context message on `channel` with **no** push rows — the
/// message appears only in retained context (as returned by `clamp_and_fetch_context`
/// / `load_activation_snapshot`). Used to set up sampled-port fixtures where
/// `push_depth = Bounded(0)` so no push rows are created for a subscriber.
///
/// Returns the inserted `message_uuid`.
pub async fn insert_retain_only(
    messenger: &Messenger,
    channel: &ChannelEntry,
    body: &str,
    envelope_type: ChannelScheme,
) -> Uuid {
    let conn = messenger.db().lock().await;
    let ts_ns = db::utc_to_ns(chrono::Utc::now());
    let inserted = insert_message_with_pushes(
        &conn,
        channel.uuid,
        "test",
        "test-sender",
        body,
        Urgency::Low,
        envelope_type,
        None,
        None,
        None,
        ts_ns,
        &[], // no push rows — retained context only
    );
    inserted.uuid
}
