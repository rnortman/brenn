//! Shared scaffolding for WASM-subscriber receive tests across the MQTT and
//! webhook router suites: a single-`Wasm`-subscriber channel entry, a `Messenger`
//! carrying that subscriber's policy, and the pending-push row query both suites
//! assert on. Keeping these in one place means a `messaging_pending_pushes` schema
//! change or a new transport edits one builder, not several near-identical copies.

use std::sync::Arc;

use brenn_lib::access::AppPolicy;
use brenn_lib::db::Db;
use brenn_lib::messaging::{
    ChannelEntry, ChannelScheme, MessagingDirectory, Messenger, SubscriberEntry,
    SubscriberEntryKind, WakeMin, WakeRouter,
    config::{Depth, MessagingGlobalConfig, NoiseLevel, ResolvedChannel, Sink},
    db::upsert_channels,
    query::NoopWakeRouter,
};
use indexmap::IndexMap;
use rusqlite::OptionalExtension;
use uuid::Uuid;

/// A `ChannelEntry` whose sole subscriber is the WASM consumer `wasm_slug`, for
/// the given transport. `mount` is `Some` only for webhook channels.
pub(crate) fn wasm_subscriber_channel_entry(
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
pub(crate) fn messenger_with_wasm_policy(
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
    .with_subscriber_registrations(brenn_lib::messaging::testutils::wasm_registrations(
        wasm_policies,
    ))
}

/// `(push_row_count, envelope_type)` for the WASM consumer `wasm_slug`'s pending
/// pushes. `envelope_type` is `None` when no push row exists (the denied case).
pub(crate) async fn wasm_push_rows(db: &Db, wasm_slug: &str) -> (i64, Option<String>) {
    let target = format!("wasm:{wasm_slug}");
    let conn = db.lock().await;
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_pending_pushes WHERE target_subscriber = ?1",
            [&target],
            |r| r.get(0),
        )
        .expect("push-count query must succeed");
    let envelope_type: Option<String> = conn
        .query_row(
            "SELECT m.envelope_type FROM messaging_messages m \
             JOIN messaging_pending_pushes pp ON pp.message_id = m.id \
             WHERE pp.target_subscriber = ?1",
            [&target],
            |r| r.get(0),
        )
        .optional()
        .expect("envelope-type query must succeed");
    (count, envelope_type)
}
