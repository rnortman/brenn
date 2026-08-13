//! Shared scaffolding for WASM-subscriber receive tests across the MQTT and
//! webhook router suites: a single-`Wasm`-subscriber channel entry, a `Messenger`
//! carrying that subscriber's policy, the retained-message query both suites
//! assert on, and the activation read that answers what the consumer is actually
//! served. Keeping these in one place means a schema change or a new transport
//! edits one builder, not several near-identical copies.

use std::sync::Arc;

use brenn_db::Db;
use brenn_lib::access::AppPolicy;
use brenn_lib::messaging::{
    ChannelEntry, ChannelScheme, MessageEnvelope, MessagingDirectory, MessagingGlobalConfig,
    NoiseLevel, ParticipantId, ResolvedChannel, ResolvedSubscription, Sink, SubscriberEntry,
    SubscriberEntryKind, WakeMin, WasmInputPort,
};
use brenn_messaging::{Messenger, WakeRouter, config::Depth, query::NoopWakeRouter};
use brenn_messaging_store::db::upsert_channels;
use indexmap::IndexMap;
use rusqlite::OptionalExtension;
use uuid::Uuid;

/// A `ChannelEntry` whose sole subscriber is the WASM consumer `wasm_slug`, for
/// the given transport. `mount` is `Some` only for webhook channels.
pub fn wasm_subscriber_channel_entry(
    uuid: Uuid,
    address: &str,
    transport_type: ChannelScheme,
    mount: Option<String>,
    wasm_slug: &str,
) -> ChannelEntry {
    ChannelEntry {
        uuid,
        address: address.to_string(),
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
            kind: SubscriberEntryKind::Wasm(wasm_slug.to_string()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: None,
        }],
        transport_type,
        mount,
    }
}

/// Build a `Messenger` over `entries` whose `wasm_policies` maps `wasm_slug` to
/// `policy`, upserting the entries so the directory and DB agree. Callers build
/// `policy` through the real `build_wasm_policy` path so the test exercises the
/// production grant/ACL derivation.
pub fn messenger_with_wasm_policy(
    db: Db,
    entries: Vec<ChannelEntry>,
    origin: &str,
    wasm_slug: &str,
    policy: AppPolicy,
) -> Arc<Messenger> {
    {
        let conn = db.try_lock().expect("db lock for channel upsert");
        upsert_channels(&conn, &entries);
    }
    let directory = Arc::new(MessagingDirectory::with_entries(entries));
    let mut wasm_policies = std::collections::HashMap::new();
    wasm_policies.insert(wasm_slug.to_string(), policy);
    Messenger::new(
        db,
        directory,
        Arc::from(origin),
        Arc::new(IndexMap::new()),
        Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(brenn_messaging::testutils::wasm_registrations(
        wasm_policies,
    ))
}

/// Give the WASM consumer a position on `entry`, primed at head, and return the
/// `wasm:<slug>` identity that holds it.
///
/// A push-enabled read against a channel the subscriber holds no position on
/// panics, so a receive test that asks what its consumer was served has to
/// attach first — which is what boot does for every configured input port.
pub async fn attach_wasm_consumer(
    messenger: &Messenger,
    entry: &ChannelEntry,
    wasm_slug: &str,
) -> ParticipantId {
    let subscriber = ParticipantId::for_wasm(wasm_slug);
    brenn_messaging::testutils::attach_wasm_port(
        messenger,
        entry,
        wasm_slug,
        &subscriber,
        Depth::Unbounded,
    )
    .await;
    subscriber
}

/// What the production activation read would hand the guest for a WASM consumer
/// with one input port bound to `entry`: the new messages of that port's window,
/// oldest first.
///
/// The read is what enforces the delivery-time ACL gate, so this is the only
/// honest way to ask whether a subscriber is being served — a retention query
/// answers where the message is, not who may see it. Pure read: it moves no
/// position, so a case may ask twice.
pub async fn activation_new_messages(
    messenger: &Messenger,
    entry: &ChannelEntry,
    wasm_slug: &str,
) -> Vec<MessageEnvelope> {
    let subscriber = ParticipantId::for_wasm(wasm_slug);
    let inputs = vec![WasmInputPort {
        port: "in".to_string(),
        sub: ResolvedSubscription {
            channel_uuid: entry.uuid,
            channel_address: entry.address.clone(),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Bounded(0),
            noise: NoiseLevel::Silent,
            wake_min: WakeMin::Normal,
        },
        amplification_mt: 1_000,
    }];
    messenger
        .load_activation_snapshot(&subscriber, &inputs)
        .await
        .map(|snapshots| {
            snapshots
                .iter()
                .flat_map(|snapshot| snapshot.new_entries())
                .map(|(_, envelope)| envelope.clone())
                .collect()
        })
        .unwrap_or_default()
}

/// `(retained_count, envelope_type)` for the messages a transport ingress
/// committed onto `channel_address`.
///
/// A commit writes the message and nothing per-subscriber, so what a receive test
/// asserts is that the message reached the channel's retention — which is where
/// every subscriber's position reads it from. `envelope_type` is `None` when the
/// channel retains nothing.
pub async fn retained_on_channel(db: &Db, channel_address: &str) -> (i64, Option<String>) {
    let conn = db.lock().await;
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_messages m \
             JOIN messaging_channels c ON c.uuid = m.channel_uuid \
             WHERE c.address = ?1 AND m.retained_seq IS NOT NULL",
            [channel_address],
            |r| r.get(0),
        )
        .expect("retained-count query must succeed");
    let envelope_type: Option<String> = conn
        .query_row(
            "SELECT m.envelope_type FROM messaging_messages m \
             JOIN messaging_channels c ON c.uuid = m.channel_uuid \
             WHERE c.address = ?1 AND m.retained_seq IS NOT NULL \
             ORDER BY m.retained_seq DESC LIMIT 1",
            [channel_address],
            |r| r.get(0),
        )
        .optional()
        .expect("envelope-type query must succeed");
    (count, envelope_type)
}
