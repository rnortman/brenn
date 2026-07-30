//! Test helpers for building minimal in-memory `Messenger` fixtures with a single
//! WASM-subscriber channel.
//!
//! Available under `#[cfg(test)]` or when the `testutils` feature is enabled.

use std::collections::HashMap;
use std::sync::Arc;

use uuid::Uuid;

use super::{
    ChannelEntry, ChannelScheme, MessagingDirectory, Messenger, ParticipantId, SubscriberEntry,
    SubscriberEntryKind, SubscriberRegistration, Urgency, WakeEconomics, WakeMin, WakeRouter,
    canonical_address,
    config::{self, Depth, MessagingGlobalConfig, NoiseLevel},
    db::{self, insert_message, upsert_channels},
    ephemeral_channel_uuid_from_name,
    query::NoopWakeRouter,
};
use crate::access::AppPolicy;
use crate::db::Db;

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

/// An `AppPolicy` authorizing delivery on every bus scheme a fixture channel can
/// carry — `brenn:`, `ephemeral:`, and `local:` — each scoped by `matcher`.
///
/// The three schemes gate on three distinct transport grants, so a fixture whose
/// channels span them needs all three. `ChannelMatcher::Prefix(String::new())`
/// is the covering case; a matcher naming something else is the denied one.
pub fn bus_delivery_policy(matcher: crate::access::acl::ChannelMatcher) -> AppPolicy {
    let mut policy = AppPolicy::default();
    policy
        .grants
        .insert(crate::access::AppCapability::MessagingSubscribe);
    policy
        .grants
        .insert(crate::access::AppCapability::EphemeralSubscribe);
    policy
        .grants
        .insert(crate::access::AppCapability::LocalSubscribe);
    policy.acls.brenn_subscribe.push(matcher.clone());
    policy.acls.ephemeral_subscribe.push(matcher.clone());
    policy.acls.local_subscribe.push(matcher);
    policy
}

/// Registrations for every non-`App` subscriber on `entries`, under one policy
/// scoped by `matcher`.
///
/// Boot registers each such subscriber with the policy its ACL derives, and the
/// delivery-time gate reads that registration — so a fixture whose subscriber
/// has none is a subscriber the fail-closed gate denies. Deriving from the
/// channels' own subscriber lists means a case that adds a subscriber gets its
/// registration without editing a second list. `App` subscribers are excluded:
/// their policy comes from the apps map, not the registry.
pub fn bus_subscriber_registrations(
    entries: &[ChannelEntry],
    matcher: crate::access::acl::ChannelMatcher,
) -> HashMap<SubscriberEntryKind, SubscriberRegistration> {
    let policy = Arc::new(bus_delivery_policy(matcher));
    entries
        .iter()
        .flat_map(|entry| entry.subscribers.iter())
        .filter(|sub| !matches!(sub.kind, SubscriberEntryKind::App(_)))
        .map(|sub| {
            (
                sub.kind.clone(),
                SubscriberRegistration {
                    policy: Arc::clone(&policy),
                    wake: WakeEconomics::Eager,
                },
            )
        })
        .collect()
}

/// [`bus_subscriber_registrations`] at the covering scope — the ordinary case,
/// where a fixture's subscribers are all authorized for the channels they are
/// wired to.
pub fn covering_subscriber_registrations(
    entries: &[ChannelEntry],
) -> HashMap<SubscriberEntryKind, SubscriberRegistration> {
    bus_subscriber_registrations(
        entries,
        crate::access::acl::ChannelMatcher::Prefix(String::new()),
    )
}

/// A covering registration for one WASM consumer slug — for the fixture that
/// attaches its subscriber directly instead of declaring it on the channel, so
/// [`covering_subscriber_registrations`] would find nothing to register.
pub fn covering_wasm_registrations(
    slug: &str,
) -> HashMap<SubscriberEntryKind, SubscriberRegistration> {
    wasm_registrations(HashMap::from([(
        slug.to_string(),
        bus_delivery_policy(crate::access::acl::ChannelMatcher::Prefix(String::new())),
    )]))
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
            send_rate: Default::default(),
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

/// A single WASM-subscriber registration for a `ChannelEntry`: unbounded
/// depths, `noise = Silent`, no per-subscription `wake_min`.
pub fn wasm_subscriber_entry(slug: &str) -> SubscriberEntry {
    SubscriberEntry {
        kind: SubscriberEntryKind::Wasm(slug.to_string()),
        push_depth: Depth::Unbounded,
        retain_depth: Depth::Unbounded,
        noise: config::NoiseLevel::Silent,
        wake_min: None,
    }
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
            send_rate: Default::default(),
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

/// Build an `ephemeral:` `ChannelEntry` with the deterministic name-derived uuid.
///
/// The channel rung is transparent to global: an ephemeral binding that states
/// no push_depth/noise inherits these.
pub fn ephemeral_channel_entry(name: &str, retain_depth: u64) -> ChannelEntry {
    ChannelEntry {
        uuid: ephemeral_channel_uuid_from_name(name),
        address: format!("ephemeral:{name}"),
        description: None,
        resolved_channel: config::ResolvedChannel {
            send_rate: Default::default(),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Bounded(retain_depth),
            standing_retain_depth: Depth::Bounded(retain_depth),
            noise: NoiseLevel::Silent,
            sink: config::Sink::Drop,
            wake_min: WakeMin::Normal,
        },
        subscribers: vec![],
        transport_type: ChannelScheme::Ephemeral,
        mount: None,
    }
}

/// Build a `local:` `ChannelEntry` with the deterministic name-derived uuid —
/// non-durable like `ephemeral:`, and confined on top of it.
pub fn local_channel_entry(name: &str, retain_depth: u64) -> ChannelEntry {
    ChannelEntry {
        uuid: super::local_channel_uuid_from_name(name),
        address: format!("local:{name}"),
        transport_type: ChannelScheme::Local,
        ..ephemeral_channel_entry(name, retain_depth)
    }
}

/// Build an in-memory `Messenger` with a single WASM-subscriber channel, using a
/// noop wake router. The subscriber is attached at head, so every message a test
/// publishes afterwards is unseen to it.
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
    )
    .with_subscriber_registrations(covering_subscriber_registrations(std::slice::from_ref(
        &*entry,
    )));
    let wasm_sub = ParticipantId::for_wasm(slug);
    attach_wasm_port(&messenger, &entry, slug, &wasm_sub, push_depth).await;
    (messenger, entry, wasm_sub)
}

/// Give a WASM subscriber its position on `entry` — what a real boot does before
/// any message reaches the port. A sampled port is never delivered to and holds
/// none.
///
/// The stored depth is a cache the window read retunes from its own argument, so
/// a test that reads at a different depth than it attached at gets the depth it
/// asked for.
pub async fn attach_wasm_port(
    messenger: &Messenger,
    entry: &ChannelEntry,
    slug: &str,
    subscriber: &ParticipantId,
    push_depth: Depth,
) {
    if !push_depth.is_push_enabled() {
        return;
    }
    messenger
        .attach_subscriber(&entry.address, slug, subscriber, push_depth)
        .await;
}

/// A second `Messenger` over an existing database and channel — a host restart,
/// as observable from inside one process.
///
/// It attaches nothing: what makes this a restart rather than a fresh boot is
/// that the durable cursor row survives, so the returned messenger reads the
/// position the previous one left. A caller that wants the boot attach as well
/// calls [`attach_wasm_port`] on the result.
pub fn restart_wasm_messenger(db: Db, entry: &ChannelEntry) -> Arc<Messenger> {
    let directory = Arc::new(MessagingDirectory::with_entries(vec![entry.clone()]));
    Messenger::new(
        db,
        directory,
        Arc::from("test"),
        Arc::new(indexmap::IndexMap::new()),
        Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(covering_subscriber_registrations(std::slice::from_ref(
        entry,
    )))
}

/// Terse wrapper around [`build_wasm_messenger`] for the common `Unbounded`/`Unbounded` case.
pub async fn build_wasm_messenger_unbounded(
    slug: &str,
    channel_name: &str,
) -> (Arc<Messenger>, Arc<ChannelEntry>, ParticipantId) {
    build_wasm_messenger(slug, channel_name, Depth::Unbounded, Depth::Unbounded).await
}

/// Insert one bus message on `channel` at an explicit timestamp and return its
/// uuid.
///
/// `envelope_type` is caller-supplied because some channels use `Webhook` rather
/// than `Brenn`. `ts_ns` is the message timestamp in nanoseconds since the Unix
/// epoch. Use [`insert_bus_message`] when the exact timestamp does not matter.
///
/// Names no subscriber: a commit writes the message and nothing per-subscriber,
/// so who reads it is decided by each position at its own read.
pub async fn insert_bus_message_at(
    messenger: &Messenger,
    channel: &ChannelEntry,
    body: &str,
    envelope_type: ChannelScheme,
    ts_ns: i64,
) -> Uuid {
    let conn = messenger.db().lock().await;
    insert_message(
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
        None,
        ts_ns,
    )
    .uuid
}

/// Insert one bus message on `channel` at the current wall-clock time and return
/// its uuid.
///
/// `envelope_type` is caller-supplied because some channels use `Webhook` rather
/// than `Brenn`. Use [`insert_bus_message_at`] when the test needs to control the
/// exact timestamp.
pub async fn insert_bus_message(
    messenger: &Messenger,
    channel: &ChannelEntry,
    body: &str,
    envelope_type: ChannelScheme,
) -> Uuid {
    let ts_ns = db::utc_to_ns(chrono::Utc::now());
    insert_bus_message_at(messenger, channel, body, envelope_type, ts_ns).await
}

/// Everything `subscriber` is still owed, across every channel it holds a
/// position on, as `(channel address, envelope)` — oldest first within each
/// channel.
///
/// A pure read: it moves no position and charges nothing, so a case may call it
/// before and after a drain to see what the drain consumed. A channel the
/// subscriber holds no position on contributes nothing, which is what makes this
/// safe to call over the whole directory.
///
/// One side effect it does have: reading at `Depth::Unbounded` retunes the
/// cursor's stored `push_depth` to unbounded, because the caller of a window is
/// the authority on depth. The next production read retunes it back, but a case
/// asserting clamp or drop behaviour must not sample it between this call and
/// that read — pass through a real drain instead.
///
/// The unbounded read is not incidental and cannot be narrowed: the question
/// this answers is "everything still owed", which a window at the subscriber's
/// registered depth would clamp. Restoring the prior depth afterwards would need
/// a depth read the store trait does not expose.
pub async fn owed_everywhere(
    messenger: &Messenger,
    subscriber: &ParticipantId,
) -> Vec<(String, Arc<brenn_envelope::MessageEnvelope>)> {
    let mut owed = Vec::new();
    for entry in messenger.directory().list() {
        let store = messenger.store_for(&entry);
        if !store.has_deliverable(subscriber).await {
            continue;
        }
        let window = store
            .window(subscriber, Depth::Unbounded, Depth::Bounded(0))
            .await
            .unwrap_or_else(|| {
                panic!(
                    "owed_everywhere: {} answers has_deliverable for {} yet holds no position for it",
                    entry.address,
                    subscriber.as_str()
                )
            });
        for (_, envelope) in window.new_entries() {
            owed.push((entry.address.clone(), Arc::clone(envelope)));
        }
    }
    owed
}

/// Advance `subscriber` past everything it is currently owed on
/// `channel_address` — what a real consumer's drain does at its ack point.
///
/// For a case that needs a prior step's output out of the way before reading the
/// next one: the position is the only delivery state, so consuming means moving
/// it. It reads at `Depth::Unbounded` and so retunes the cursor's stored
/// `push_depth` exactly as [`owed_everywhere`] does — same caveat.
pub async fn consume_owed(
    messenger: &Messenger,
    channel_address: &str,
    subscriber: &ParticipantId,
) {
    let store = messenger.store_for_address(channel_address);
    let window = store
        .window(subscriber, Depth::Unbounded, Depth::Bounded(0))
        .await
        .unwrap_or_else(|| {
            panic!("consume_owed: {subscriber:?} holds no position on {channel_address}")
        });
    if let Some((through, seen_floor)) = window.advance_span() {
        store
            .advance(subscriber, through, seen_floor)
            .await
            .unwrap_or_else(|| {
                panic!(
                    "consume_owed: {subscriber:?} lost its position on {channel_address} between the read and the advance"
                )
            });
    }
}

/// Insert a low-urgency bus message on `channel` — the shape a sampled-port
/// fixture wants, where the message is context for every reader and wakes
/// nobody.
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
    insert_message(
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
        None,
        ts_ns,
    )
    .uuid
}
