use super::*;

mod death;
mod drain;
mod initialized;
mod rate_limit;
mod status;
mod streaming;

/// Test helper: insert a `kind='ingress'` message + push row into the
/// unified store, replacing `event_queue::enqueue_event` in drain tests.
/// Returns the push_id (used as the row identifier, analogous to old event id).
pub(super) fn enqueue_ingress(
    conn: &rusqlite::Connection,
    conversation_id: i64,
    source: &str,
    summary: &str,
    payload: &str,
) -> i64 {
    let subscriber = brenn_lib::messaging::ParticipantId::for_conversation(conversation_id);
    let ts_ns = brenn_messaging_store::db::utc_to_ns(chrono::Utc::now());
    let (_msg_id, push_id) = brenn_messaging_store::db::insert_ingress_message(
        conn,
        &subscriber,
        "test", // app_slug
        source,
        summary,
        payload,
        brenn_lib::messaging::Urgency::Normal,
        ts_ns,
    );
    push_id
}

/// Test helper: count undelivered ingress push rows for a conversation.
/// Replaces `event_queue::get_pending_events` in drain tests.
pub(super) fn pending_ingress_count(conn: &rusqlite::Connection, conversation_id: i64) -> i64 {
    let subscriber_str = format!("conversation:{conversation_id}");
    conn.query_row(
        "SELECT count(*) FROM messaging_pending_pushes pp \
             JOIN messaging_messages m ON pp.message_id = m.id \
             WHERE pp.target_subscriber = ?1 \
               AND m.envelope_type = 'ingress' \
               AND pp.delivered_at IS NULL",
        rusqlite::params![subscriber_str],
        |row| row.get::<_, i64>(0),
    )
    .expect("pending_ingress_count")
}

use brenn_obs::alerting::AlertDispatcher;

use crate::active_bridge::test_fixtures::TestBridgeConfig;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};

use super::super::ActiveBridges;

/// Build a bridge + unspawned event-loop inputs so a test can enqueue
/// events into the DB *before* `cc_event_loop` starts. Used to exercise
/// the drain-at-loop-start path (which only inspects events that are
/// already in the queue at that moment).
pub(super) async fn bridge_with_unspawned_event_loop(
    singleton: bool,
) -> (
    Arc<ActiveBridge>,
    mpsc::Sender<SessionEvent>,
    mpsc::Receiver<SessionEvent>,
    broadcast::Receiver<WsServerMessage>,
    AlertDispatcher,
    ActiveBridges,
) {
    let (alert_dispatcher, _handle) = brenn_obs::alerting::noop_alert_dispatcher();
    // The drain now reads exclusively from the unified messaging store, so
    // the bridge must have a messenger configured. Build the in-memory DB,
    // user, and conversation, then inject a minimal messenger (empty app map,
    // NoopWakeRouter) into the config before bridge construction.
    let db = crate::test_support::init_db_memory();
    let (user_id, conv_id) = {
        let conn = db.lock().await;
        let uid = brenn_db::auth::user::create_user(&conn, "testuser", "$argon2id$fake");
        let cid = brenn_db::conversation::create_conversation(&conn, uid, "test", false);
        (uid, cid)
    };
    let messenger = brenn_messaging::Messenger::new(
        db.clone(),
        Arc::new(brenn_lib::messaging::MessagingDirectory::with_entries(
            vec![],
        )),
        Arc::from("test-drain"),
        Arc::new(indexmap::IndexMap::new()),
        Arc::new(brenn_messaging::query::NoopWakeRouter) as Arc<dyn brenn_messaging::WakeRouter>,
        brenn_lib::messaging::MessagingGlobalConfig::default(),
    );
    let (broadcast_tx, broadcast_rx) = tokio::sync::broadcast::channel(64);
    let (event_tx, event_rx) = tokio::sync::mpsc::channel(64);
    let active_bridges = crate::active_bridge::ActiveBridges::new();
    let bridge = crate::active_bridge::ActiveBridge::inject_for_test_full(
        user_id,
        conv_id,
        "test",
        db,
        broadcast_tx,
        brenn_obs::alerting::noop_alert_dispatcher().0,
        TestBridgeConfig {
            singleton,
            messenger: Some(messenger),
            active_bridges: Some(active_bridges.clone()),
            ..Default::default()
        },
    );
    (
        bridge,
        event_tx,
        event_rx,
        broadcast_rx,
        alert_dispatcher,
        active_bridges,
    )
}

/// Build an `ActiveBridge` with a `Messenger` wired in, suitable for
/// testing the messaging-pushes branch of `drain_pending_events`.
///
/// Creates an in-memory DB with one user + conversation, upserts a single
/// `messaging_channels` row with a known UUID (`DRAIN_TEST_CHANNEL_UUID`),
/// constructs a `Messenger` using `NoopWakeRouter`, and returns the bridge
/// plus a broadcast receiver (capacity 64).
///
/// Does NOT install a recording CC session — callers that need
/// `send_system_message` to succeed call
/// `bridge.install_recording_session_for_test()` themselves.
///
/// Note: this helper is intentionally not migrated to `make_bridge_no_loop`
/// because it must run `upsert_channels` inside the same `db.lock()` block
/// as `create_conversation` (the channel UUID is the FK target for
/// `messaging_messages`; a UUID mismatch silently yields zero drained rows).
/// `make_bridge_no_loop` owns `init_db_memory` and does not expose the `db`
/// handle, so there is no way to thread the pre-`inject` DB work through it
/// without returning `db` (defeating the helper) or adding a closure hook
/// (a multi-mode escape hatch).
pub(super) async fn bridge_with_messenger_for_drain()
-> (Arc<ActiveBridge>, broadcast::Receiver<WsServerMessage>) {
    bridge_with_messenger_for_drain_at_ceiling(100).await
}

/// [`bridge_with_messenger_for_drain`] whose app sets the impetus pool's
/// ceiling, for the cases that turn on it — `0` in particular, which makes the
/// conversation attended-only.
pub(super) async fn bridge_with_messenger_for_drain_at_ceiling(
    send_budget: u32,
) -> (Arc<ActiveBridge>, broadcast::Receiver<WsServerMessage>) {
    let db = crate::test_support::init_db_memory();
    let (broadcast_tx, broadcast_rx) = broadcast::channel(64);

    // Single ChannelEntry binding shared between the DB upsert and the in-memory
    // MessagingDirectory — UUID mismatch between them would cause
    // load_pending_ingress_for_drain to silently return zero rows (FK JOIN miss).
    let channel_entry = brenn_lib::messaging::ChannelEntry {
        uuid: DRAIN_TEST_CHANNEL_UUID,
        address: brenn_lib::messaging::canonical_address("test-drain-channel"),
        description: None,
        resolved_channel: brenn_lib::messaging::config::ResolvedChannel {
            send_rate: Default::default(),
            push_depth: brenn_lib::messaging::config::Depth::Unbounded,
            retain_depth: brenn_lib::messaging::config::Depth::Unbounded,
            standing_retain_depth: brenn_lib::messaging::config::Depth::Unbounded,
            noise: brenn_lib::messaging::config::NoiseLevel::Silent,
            sink: brenn_lib::messaging::config::Sink::Drop,
            wake_min: brenn_lib::messaging::WakeMin::Normal,
        },
        subscribers: vec![brenn_lib::messaging::SubscriberEntry {
            kind: brenn_lib::messaging::SubscriberEntryKind::App("testapp".to_string()),
            push_depth: brenn_lib::messaging::config::Depth::Unbounded,
            retain_depth: brenn_lib::messaging::config::Depth::Unbounded,
            noise: brenn_lib::messaging::config::NoiseLevel::Silent,
            wake_min: Some(brenn_lib::messaging::WakeMin::Normal),
        }],
        transport_type: brenn_lib::messaging::ChannelScheme::Brenn,
        mount: None,
    };

    let (user_id, conv_id) = {
        let conn = db.lock().await;
        let uid = brenn_db::auth::user::create_user(&conn, "drain-test-user", "$argon2id$fake");
        let cid = conversation::create_conversation(&conn, uid, "testapp", false);
        // Upsert channel row — required FK for messaging_messages.
        brenn_messaging_store::db::upsert_channels(&conn, std::slice::from_ref(&channel_entry));
        (uid, cid)
    };

    let dir = brenn_lib::messaging::MessagingDirectory::with_entries(vec![channel_entry.clone()]);
    // The conversation's delivery state resolves through the app: the drain
    // reads the position held by `testapp`'s singleton conversation, so the
    // apps map must carry the app whose allowed user owns it.
    let mut apps = indexmap::IndexMap::new();
    apps.insert(
        "testapp".to_string(),
        drain_test_app_config("drain-test-user"),
    );
    let messenger = brenn_messaging::Messenger::new(
        db.clone(),
        Arc::new(dir),
        Arc::from("test-drain-source"),
        Arc::new(apps),
        Arc::new(brenn_messaging::query::NoopWakeRouter) as Arc<dyn brenn_messaging::WakeRouter>,
        brenn_lib::messaging::MessagingGlobalConfig::default(),
    );
    // Boot's attach: the conversation holds a position on every channel its app
    // subscribes to before anything is published.
    messenger.attach_conversation_subscribers().await;

    let bridge = ActiveBridge::inject_for_test_full(
        user_id,
        conv_id,
        "testapp",
        db,
        broadcast_tx,
        brenn_obs::alerting::noop_alert_dispatcher().0,
        TestBridgeConfig {
            messenger: Some(messenger),
            send_budget,
            ..Default::default()
        },
    );

    (bridge, broadcast_rx)
}

/// The app whose singleton conversation the drain fixtures deliver to:
/// `allowed_user` owns it, and the policy covers `test-drain-channel` so the
/// delivery-time ACL gate passes.
fn drain_test_app_config(allowed_user: &str) -> brenn_lib::config::AppConfig {
    let mut app = crate::test_support::app_config::minimal_app_config("testapp", None, vec![]);
    app.singleton = true;
    app.allowed_users = vec![allowed_user.to_string()];
    app.policy
        .grants
        .insert(brenn_envelope::grants::AppCapability::MessagingSubscribe);
    app.policy
        .acls
        .brenn_subscribe
        .push(brenn_lib::access::acl::ChannelMatcher::Prefix(
            "test-drain-channel".to_string(),
        ));
    app
}

/// Fixed channel UUID used by `bridge_with_messenger_for_drain` and
/// `seed_pending_push`. Both must use the same value to satisfy the FK
/// JOIN in `load_pending_ingress_for_drain`.
const DRAIN_TEST_CHANNEL_UUID: uuid::Uuid = uuid::Uuid::from_bytes([
    0x00, 0x00, 0x00, 0x00, 0xde, 0xad, 0xbe, 0xef, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
]);

/// Insert one `messaging_messages` row + one `messaging_pending_pushes` row
/// for `conversation_id` against `DRAIN_TEST_CHANNEL_UUID`.
///
/// Uses `Urgency::Normal` (eager wake) and no deadline/deliver_after.
/// Tests verify delivered state via `load_pending_ingress_for_drain` returning
/// empty or non-empty — the push row ID is not needed by callers.
///
/// Requires `bridge_with_messenger_for_drain` to have already upserted a
/// `messaging_channels` row with `DRAIN_TEST_CHANNEL_UUID`. A mismatch
/// produces an FK violation panic from `insert_message`.
pub(super) async fn seed_pending_push(bridge: &ActiveBridge, body: &str) {
    seed_pending_push_from(bridge, "test-sender", body).await;
}

/// [`seed_pending_push`] with the sender named, for the cases that turn on who
/// spoke — the drain filters a conversation's own utterances out of its batch.
pub(super) async fn seed_pending_push_from(bridge: &ActiveBridge, sender: &str, body: &str) {
    seed_push_row(bridge, sender, body, None).await;
}

/// [`seed_pending_push`] carrying user-interaction authority, for the cases that
/// turn on the redemption. Stamps the impetus column directly rather than going
/// through the publish gate, which owns the minting check in production.
pub(super) async fn seed_pending_push_with_impetus(bridge: &ActiveBridge, body: &str) {
    seed_push_row(
        bridge,
        "test-sender",
        body,
        Some(brenn_lib::messaging::Impetus::Replenish),
    )
    .await;
}

async fn seed_push_row(
    bridge: &ActiveBridge,
    sender: &str,
    body: &str,
    impetus: Option<brenn_lib::messaging::Impetus>,
) {
    let conn = bridge.db.lock().await;
    let now_ns = brenn_messaging_store::db::utc_to_ns(chrono::Utc::now());
    brenn_messaging_store::db::insert_message(
        &conn,
        DRAIN_TEST_CHANNEL_UUID,
        "test-drain-source",
        sender,
        body,
        brenn_lib::messaging::Urgency::Normal,
        brenn_lib::messaging::ChannelScheme::Brenn,
        None,
        None,
        None,
        impetus,
        now_ns,
    );
    // Verify the message entered retention: a FK violation (channel not seeded)
    // makes insert_message panic with a raw SQLite error. If it somehow succeeded
    // but landed unpositioned, catch that here with a clear diagnostic rather than
    // a confusing assertion failure later. What the conversation is *owed* is its
    // cursor position, which the caller's `attach_conversation_subscribers` set.
    let retained: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_messages \
             WHERE channel_uuid = ?1 AND retained_seq IS NOT NULL",
            rusqlite::params![DRAIN_TEST_CHANNEL_UUID.as_bytes().to_vec()],
            |row| row.get(0),
        )
        .expect("count retained messages on the drain channel");
    assert!(
        retained > 0,
        "seed_pending_push: the message did not enter retention — check \
         DRAIN_TEST_CHANNEL_UUID matches the upserted messaging_channels row"
    );
}
