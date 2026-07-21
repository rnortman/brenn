//! Shared test fixtures for the `routes::ws` module tree.
//!
//! This file is the body of `#[cfg(test)] mod testing;` declared in `mod.rs`.
//! All items are `pub(super)` so they are visible to every inline test mod
//! within `routes::ws::*` via `use super::testing::*;` or
//! `use super::super::testing::*;`.
//!
//! Key helpers: `test_ws_conn_with_resume_conv`, `test_ws_conn_with_channel`,
//! `seed_user_messages` (wraps `append_message` with common defaults).

use std::sync::Arc;

use brenn_lib::auth::user::create_user;
use brenn_lib::config::AppConfig;

use crate::test_support::app_config::default_test_app_config;
use brenn_lib::db::init_db_memory;
use brenn_lib::obs::alerting::noop_alert_dispatcher;
use brenn_lib::ws_types::{ViewportClass, WsServerMessage};
use indexmap::IndexMap;
use tokio::sync::{broadcast, mpsc};
use uuid::Uuid;

use crate::active_bridge::ActiveBridge;
use crate::state::{AppState, PendingUpload};

use super::connection::WsConnection;

/// Canonical test app slug — must match what `test_apps()` and helpers set up.
pub(super) const TEST_APP_SLUG: &str = "test";

/// Canonical test username — must match what `create_user` calls in helpers use.
pub(super) const TEST_USERNAME: &str = "testuser";

/// Localhost `IpAddr` — shared constant for all `handle_client_message` call sites in tests.
pub(super) const TEST_CLIENT_IP: std::net::IpAddr =
    std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);

/// Create a test device for `user_id` and return the device_id.
/// Used by test WsConnection builders to satisfy the device_id requirement.
pub(super) fn create_test_device(conn: &rusqlite::Connection, user_id: i64) -> i64 {
    let resolved = brenn_lib::auth::device::resolve_or_create_device(
        conn,
        None,
        user_id,
        "Mozilla/5.0 (X11; Linux x86_64) Chrome/125.0",
    );
    resolved.id
}

pub(super) fn test_apps() -> Arc<IndexMap<String, AppConfig>> {
    crate::test_support::app_config::test_apps()
}

/// Poll the DB until `SELECT COUNT(*) FROM {sql_where_clause}` reaches `min_count`,
/// or panic with a diagnostic after `max_wait_ms` ms.
///
/// `sql` must be a complete `SELECT COUNT(*) FROM …` query. The helper uses a
/// 50 ms sleep between attempts and surfaces a clear timeout message so failures
/// are not misread as logic regressions.
pub(super) async fn poll_until_db_count(
    db: &brenn_lib::db::Db,
    sql: &str,
    min_count: i64,
    max_wait_ms: u64,
) {
    let iters = max_wait_ms / 50;
    for _ in 0..iters {
        let count: i64 = db
            .lock()
            .await
            .query_row(sql, [], |row| row.get(0))
            .expect("poll_until_db_count: query failed");
        if count >= min_count {
            return;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
    // One last check before failing so we don't trip on a race between the
    // final sleep and the write landing.
    let final_count: i64 = db
        .lock()
        .await
        .query_row(sql, [], |row| row.get(0))
        .expect("poll_until_db_count: final query failed");
    assert!(
        final_count >= min_count,
        "poll_until_db_count: timed out after {max_wait_ms}ms — \
         expected count >= {min_count}, got {final_count}. \
         Query: {sql}"
    );
}

/// Collect all messages from the mpsc receiver with a short timeout.
/// Returns once the channel is idle (no message within timeout).
///
/// Prefer `collect_until` for tests where the final message is deterministic.
pub(super) async fn collect_messages(
    rx: &mut mpsc::Receiver<WsServerMessage>,
) -> Vec<WsServerMessage> {
    let mut msgs = Vec::new();
    while let Ok(Some(msg)) =
        tokio::time::timeout(std::time::Duration::from_millis(50), rx.recv()).await
    {
        msgs.push(msg);
    }
    msgs
}

/// Collect messages until a sentinel message is received (inclusive).
///
/// Blocks up to 2 seconds for each individual message. Returns all messages
/// collected up to and including the sentinel. Panics with a diagnostic if
/// the sentinel never arrives.
///
/// Use after `run_setup` calls where the last emitted message is deterministic
/// (e.g. `HistoryComplete`). This eliminates the 50 ms idle timeout from
/// `collect_messages` and makes collection instant once the final message arrives.
///
/// For tests that assert *absence* of a message type, collect until the sentinel
/// and then assert on the collected slice — the sentinel guarantees collection is
/// complete.
///
/// **Caller contract:** The sentinel must be structurally the last expected message
/// for the given test configuration. A follow-up `try_recv().is_err()` assertion is
/// only sound if this invariant holds. If production code adds a message after the
/// sentinel, `collect_until` stops early; the extra message bleeds into the next
/// collection and causes confusing failures in an unrelated call site.
pub(super) async fn collect_until<F>(
    rx: &mut mpsc::Receiver<WsServerMessage>,
    is_sentinel: F,
) -> Vec<WsServerMessage>
where
    F: Fn(&WsServerMessage) -> bool,
{
    let mut msgs = Vec::new();
    loop {
        let msg = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("collect_until timed out waiting for sentinel")
            .expect("channel closed before sentinel arrived");
        let done = is_sentinel(&msg);
        msgs.push(msg);
        if done {
            break;
        }
    }
    msgs
}

/// Standard two-model list for tests that need a non-empty model cache.
///
/// Returns `[{value: "sonnet", …}, {value: "opus", …}]`. Use with
/// `state.cached_models.write().await.insert(TEST_APP_SLUG, test_model_infos())`.
pub(super) fn test_model_infos() -> Vec<brenn_lib::ws_types::ModelInfo> {
    vec![
        brenn_lib::ws_types::ModelInfo {
            value: "sonnet".into(),
            display_name: "Sonnet".into(),
            description: "Fast".into(),
        },
        brenn_lib::ws_types::ModelInfo {
            value: "opus".into(),
            display_name: "Opus".into(),
            description: "Smart".into(),
        },
    ]
}

/// Returns true if `msg` is the CC-send-failure error emitted when the
/// injected test bridge has no CC session. Tests use this to distinguish
/// expected infrastructure noise from unexpected auth/routing errors.
pub(super) fn is_cc_send_failure_error(msg: &WsServerMessage) -> bool {
    matches!(msg, WsServerMessage::Error { message }
        if message == super::messaging::CC_SEND_FAILURE_MSG)
}

pub(super) fn test_apps_with_working_dir(
    working_dir: std::path::PathBuf,
) -> Arc<IndexMap<String, AppConfig>> {
    let mut apps = IndexMap::new();
    let mut cfg = default_test_app_config("test", "Test App");
    cfg.working_dir = working_dir;
    apps.insert("test".to_string(), cfg);
    Arc::new(apps)
}

// Builders below use AppState::for_test + create_test_device to minimise per-builder
// boilerplate. Adding a new AppState field only requires updating AppState::for_test.

/// Internal builder that consolidates `WsConnection` struct literal construction.
/// All test helpers use this builder; a new `WsConnection` field only requires
/// updating `build()` here.
///
/// Required fields: `user_id`, `username`, `app_slug`, `ws_tx`, `state`, `device_id`.
/// Optional overrides: `current_conversation_id`, `test_bridge`, `viewport_class`,
/// `viewer_only`, `broadcast_rx`.  Everything else defaults to the test-sensible values
/// (localhost, UTC, Wide, false, None, etc.).
pub(super) struct WsConnBuilder {
    pub user_id: i64,
    pub username: String,
    pub app_slug: String,
    pub current_conversation_id: Option<i64>,
    pub ws_tx: mpsc::Sender<WsServerMessage>,
    pub state: crate::state::AppState,
    pub device_id: i64,
    pub test_bridge: Option<std::sync::Arc<ActiveBridge>>,
    pub viewport_class: ViewportClass,
    pub viewer_only: bool,
    /// Non-None only for connections that are auto-attached to a shared bridge
    /// (e.g. viewer-side in multiuser tests).
    pub broadcast_rx: Option<broadcast::Receiver<WsServerMessage>>,
}

impl WsConnBuilder {
    /// Construct a builder pre-filled with test-sensible defaults for the optional
    /// fields (`viewport_class`, `viewer_only`, `broadcast_rx`, `test_bridge`,
    /// `current_conversation_id`).  Required fields are supplied as arguments.
    ///
    /// Use struct-update syntax to override individual optional fields:
    /// ```ignore
    /// WsConnBuilder::with_defaults(user_id, username, app_slug, ws_tx, state, device_id)
    ///     .current_conversation_id(Some(conv_id))
    ///     // ... then call .build()
    /// ```
    /// Or simply field-override before `.build()`:
    /// ```ignore
    /// WsConnBuilder { test_bridge: Some(b), ..WsConnBuilder::with_defaults(...) }.build()
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub(super) fn with_defaults(
        user_id: i64,
        username: String,
        app_slug: String,
        ws_tx: mpsc::Sender<WsServerMessage>,
        state: crate::state::AppState,
        device_id: i64,
    ) -> Self {
        Self {
            user_id,
            username,
            app_slug,
            ws_tx,
            state,
            device_id,
            current_conversation_id: None,
            test_bridge: None,
            viewport_class: ViewportClass::Wide,
            viewer_only: false,
            broadcast_rx: None,
        }
    }

    /// Build a `WsConnection` with the defaulted fields.
    ///
    /// NOTE: the production struct literal at `event_loop.rs:96` must be kept
    /// in sync with the defaults hard-coded here (`client_ip`, `timezone`,
    /// `history_sent`, `last_sent_seq`, `queued_responses`, `oldest_loaded_seq`).
    /// When adding a new `WsConnection` field, update both sites.
    pub(super) fn build(self) -> WsConnection {
        WsConnection {
            user_id: self.user_id,
            username: self.username,
            app_slug: self.app_slug,
            client_ip: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            current_conversation_id: self.current_conversation_id,
            broadcast_rx: self.broadcast_rx,
            ws_tx: self.ws_tx,
            bridge_notify_rx: self.state.bridge_notify_tx.subscribe(),
            state: self.state,
            timezone: chrono_tz::Tz::UTC,
            viewport_class: self.viewport_class,
            device_id: self.device_id,
            viewer_only: self.viewer_only,
            history_sent: false,
            last_sent_seq: None,
            queued_responses: Vec::new(),
            oldest_loaded_seq: None,
            client_error_bucket: super::connection::ClientErrorBucket::new(),
            test_bridge: self.test_bridge,
        }
    }
}

/// Create a test WsConnection with a real DB, mpsc channel for capturing
/// outgoing messages, and an injected test bridge that bypasses CC spawn.
///
/// Returns (connection, message_receiver, db, user_id, conversation_id).
/// The conversation is created in "completed" state with no title.
///
/// `conn.device_id` equals the `device_id` field of the returned `WsConnection`.
/// Tests that need to correlate DB rows by device (e.g. attribution canaries) can
/// read `conn.device_id` directly rather than spelling out `WsConnection` inline.
pub(super) async fn test_ws_conn_with_resume_conv() -> (
    WsConnection,
    mpsc::Receiver<WsServerMessage>,
    brenn_lib::db::Db,
    i64,
    i64,
) {
    test_ws_conn_with_resume_conv_and_apps(test_apps()).await
}

/// Like `test_ws_conn_with_resume_conv` but with a custom working_dir.
pub(super) async fn test_ws_conn_with_working_dir(
    working_dir: std::path::PathBuf,
) -> (
    WsConnection,
    mpsc::Receiver<WsServerMessage>,
    brenn_lib::db::Db,
    i64,
    i64,
) {
    test_ws_conn_with_resume_conv_and_apps(test_apps_with_working_dir(working_dir)).await
}

/// Create a test WsConnection with the bridge pre-inserted into
/// `state.active_bridges` (Case 1 dispatch path).
///
/// Unlike `test_ws_conn_with_resume_conv` — which puts the bridge in
/// `state.test_wake_bridge` (Case 2) — this helper satisfies tests that call
/// paths which look up the bridge via `state.active_bridges.get(conv_id)`.
///
/// Returns `(connection, message_receiver, db, user_id, conv_id)`.
/// The conversation is created in "completed" state with no title.
/// `conn.device_id` holds the created device id.
pub(super) async fn test_ws_conn_with_active_bridge() -> (
    WsConnection,
    mpsc::Receiver<WsServerMessage>,
    brenn_lib::db::Db,
    i64,
    i64,
) {
    test_ws_conn_with_active_bridge_and_apps(test_apps()).await
}

/// Like `test_ws_conn_with_active_bridge` but with a custom app config.
///
/// Pass a specific app registry (e.g., one with graf configured) to use when
/// tests need both the `active_bridges` dispatch path and a specific app
/// config (e.g., error injection tests that require a failing graf subprocess).
/// Use `test_apps()` to get the default app registry.
///
/// Returns `(connection, message_receiver, db, user_id, conv_id)`.
pub(super) async fn test_ws_conn_with_active_bridge_and_apps(
    apps: Arc<IndexMap<String, AppConfig>>,
) -> (
    WsConnection,
    mpsc::Receiver<WsServerMessage>,
    brenn_lib::db::Db,
    i64,
    i64,
) {
    let db = init_db_memory();
    let state = crate::state::AppState::for_test(db.clone(), Some(apps));

    let (ws_tx, ws_rx) = mpsc::channel(256);
    let (broadcast_tx, _) = broadcast::channel::<WsServerMessage>(64);

    // Create user, device, and a completed conversation with no title.
    let (user_id, conv_id, device_id) = {
        let conn = db.lock().await;
        let uid = create_user(&conn, TEST_USERNAME, "$argon2id$fake");
        let did = create_test_device(&conn, uid);
        let cid = brenn_lib::conversation::create_conversation(&conn, uid, "test", false);
        brenn_lib::conversation::complete_conversation(&conn, cid, None);
        (uid, cid, did)
    };

    let bridge = ActiveBridge::inject_for_test(user_id, conv_id, "test", db.clone(), broadcast_tx);

    // Register the bridge in active_bridges (Case 1 dispatch path).
    state.active_bridges.insert(conv_id, bridge.clone()).await;

    let conn = WsConnBuilder {
        current_conversation_id: Some(conv_id),
        ..WsConnBuilder::with_defaults(
            user_id,
            TEST_USERNAME.to_string(),
            TEST_APP_SLUG.to_string(),
            ws_tx,
            state,
            device_id,
        )
    }
    .build();

    (conn, ws_rx, db, user_id, conv_id)
}

pub(super) async fn test_ws_conn_with_resume_conv_and_apps(
    apps: Arc<IndexMap<String, AppConfig>>,
) -> (
    WsConnection,
    mpsc::Receiver<WsServerMessage>,
    brenn_lib::db::Db,
    i64,
    i64,
) {
    let db = init_db_memory();
    let state = crate::state::AppState::for_test(db.clone(), Some(apps));

    let (ws_tx, ws_rx) = mpsc::channel(256);
    let (broadcast_tx, _) = broadcast::channel::<WsServerMessage>(64);

    // Create user, device, and a completed conversation with no title.
    let (user_id, conv_id, device_id) = {
        let conn = db.lock().await;
        let uid = create_user(&conn, TEST_USERNAME, "$argon2id$fake");
        let did = create_test_device(&conn, uid);
        let cid = brenn_lib::conversation::create_conversation(&conn, uid, "test", false);
        brenn_lib::conversation::complete_conversation(&conn, cid, None);
        (uid, cid, did)
    };

    let test_bridge =
        ActiveBridge::inject_for_test(user_id, conv_id, "test", db.clone(), broadcast_tx);

    // Register the bridge for wake_conversation to find during handle_send_message.
    *state.test_wake_bridge.lock().await = Some(test_bridge.clone());

    let conn = WsConnBuilder {
        current_conversation_id: Some(conv_id),
        test_bridge: Some(test_bridge),
        ..WsConnBuilder::with_defaults(
            user_id,
            TEST_USERNAME.to_string(),
            TEST_APP_SLUG.to_string(),
            ws_tx,
            state,
            device_id,
        )
    }
    .build();

    (conn, ws_rx, db, user_id, conv_id)
}

/// Build a single-app registry with a `"graf"` integration pointing at `path`.
/// Shared by the graf-failure and structured-error fixture helpers.
fn test_apps_with_graf_at(path: &str) -> Arc<IndexMap<String, AppConfig>> {
    let mut apps = IndexMap::new();
    let mut cfg = default_test_app_config("test", "Test App");
    cfg.integrations.insert(
        "graf".to_string(),
        std::sync::Arc::new(brenn_graf::GrafIntegration::for_test(path)),
    );
    apps.insert("test".to_string(), cfg);
    Arc::new(apps)
}

/// Create a test app registry whose single "test" app has a graf integration
/// pointing to a stub script that exits non-zero with a valid structured error
/// envelope on stdout. Driving `handle_todo_done` with this registry causes
/// `DoneFailure::Structured` — the parsed `serde_json::Value` envelope is
/// forwarded to `inject_todo_error`, unlike the opaque (spawn-error) path.
///
/// Returns `(apps, _tmpdir)`. The caller **must keep `_tmpdir` alive** for the
/// duration of the test; dropping it deletes the script, converting any spawn
/// into a spawn error → `DoneFailure::Opaque`, silently defeating the test.
#[cfg(unix)]
pub(super) fn test_apps_with_structured_graf_error()
-> (Arc<IndexMap<String, AppConfig>>, tempfile::TempDir) {
    use std::os::unix::fs::PermissionsExt;

    let tmp = tempfile::TempDir::new().unwrap();
    let script_path = tmp.path().join("graf-stub");
    // Valid stale_anchor envelope — matches what parse_done_error_envelope
    // accepts (`error` key, `reason` key) plus a distinctive payload field
    // (`next_anchor_if_skip_past_true`) that cannot appear in an Opaque flat
    // string, used by the test assertion to prove the Structured arm was hit.
    let body = concat!(
        "echo '{\"error\":\"stale_anchor\",\"reason\":\"anchor shifted for test\",",
        "\"next_anchor_if_skip_past_true\":\"2026-04-25\"}'\n",
        "exit 1",
    );
    std::fs::write(&script_path, format!("#!/bin/sh\n{body}\n")).unwrap();
    std::fs::set_permissions(&script_path, std::fs::Permissions::from_mode(0o755)).unwrap();

    let apps = test_apps_with_graf_at(script_path.to_str().unwrap());
    (apps, tmp)
}

/// Create a test app registry whose single "test" app has a graf integration
/// pointing to a nonexistent binary. `send_todo_state` will spawn, fail to
/// exec, fall into the error branch, and emit an empty `TodoState` — enough
/// to verify that the call exists in `handle_switch_conversation`.
pub(super) fn test_apps_with_failing_graf() -> Arc<IndexMap<String, AppConfig>> {
    // Install a GrafIntegration whose command is guaranteed not to exist,
    // so `query_todos` fails fast and `send_todo_state` takes the error branch
    // (which still emits an empty TodoState).
    test_apps_with_graf_at("/nonexistent/graf-binary-for-testing")
}

/// Create apps map with a single-instance app.
pub(super) fn test_apps_single_instance() -> Arc<IndexMap<String, AppConfig>> {
    let mut apps = IndexMap::new();
    let mut cfg = default_test_app_config("test", "Test App");
    cfg.single_instance = true;
    apps.insert("test".to_string(), cfg);
    Arc::new(apps)
}

/// Create a WsConnection with given app config and user.
pub(super) async fn test_ws_conn_for_app(
    apps: Arc<IndexMap<String, AppConfig>>,
) -> (
    WsConnection,
    mpsc::Receiver<WsServerMessage>,
    brenn_lib::db::Db,
    i64,
) {
    let db = init_db_memory();
    let state = crate::state::AppState::for_test(db.clone(), Some(apps));

    let (ws_tx, ws_rx) = mpsc::channel(256);

    let (user_id, device_id) = {
        let conn = db.lock().await;
        let uid = create_user(&conn, TEST_USERNAME, "$argon2id$fake");
        let did = create_test_device(&conn, uid);
        (uid, did)
    };

    let conn = WsConnBuilder::with_defaults(
        user_id,
        TEST_USERNAME.to_string(),
        TEST_APP_SLUG.to_string(),
        ws_tx,
        state,
        device_id,
    )
    .build();

    (conn, ws_rx, db, user_id)
}

pub(super) fn test_apps_singleton() -> Arc<IndexMap<String, AppConfig>> {
    let mut apps = IndexMap::new();
    let mut cfg = default_test_app_config("test", "Test App");
    cfg.singleton = true;
    apps.insert("test".to_string(), cfg);
    Arc::new(apps)
}

pub(super) fn test_apps_multiuser() -> Arc<IndexMap<String, AppConfig>> {
    let mut apps = IndexMap::new();
    let mut cfg = default_test_app_config("test", "Test App");
    cfg.multiuser = true;
    cfg.prefix_username = true;
    apps.insert("test".to_string(), cfg);
    Arc::new(apps)
}

/// Create apps map with a single-instance multiuser app.
pub(super) fn test_apps_single_instance_multiuser() -> Arc<IndexMap<String, AppConfig>> {
    let mut apps = IndexMap::new();
    let mut cfg = default_test_app_config("test", "Test App");
    cfg.single_instance = true;
    cfg.multiuser = true;
    cfg.prefix_username = true;
    apps.insert("test".to_string(), cfg);
    Arc::new(apps)
}

pub(super) async fn test_multiuser_conn_for_privacy() -> (
    WsConnection,
    mpsc::Receiver<WsServerMessage>,
    brenn_lib::db::Db,
    i64,
    i64,
    i64,
) {
    let db = init_db_memory();
    let state = crate::state::AppState::for_test(db.clone(), Some(test_apps_multiuser()));

    let (ws_tx, ws_rx) = mpsc::channel(256);

    let (alice_id, bob_id, conv_id, device_id) = {
        let conn = db.lock().await;
        let alice = create_user(&conn, "alice", "$argon2id$fake");
        let bob = create_user(&conn, "bob", "$argon2id$fake");
        let cid = brenn_lib::conversation::create_conversation(&conn, alice, "test", true);
        let did = create_test_device(&conn, alice);
        (alice, bob, cid, did)
    };

    let conn = WsConnBuilder {
        current_conversation_id: Some(conv_id),
        ..WsConnBuilder::with_defaults(
            alice_id,
            "alice".to_string(),
            TEST_APP_SLUG.to_string(),
            ws_tx,
            state,
            device_id,
        )
    }
    .build();

    (conn, ws_rx, db, alice_id, bob_id, conv_id)
}

pub(super) async fn test_ws_conn_with_channel(
    capacity: usize,
) -> (
    WsConnection,
    mpsc::Receiver<WsServerMessage>,
    brenn_lib::db::Db,
    i64,
) {
    let db = init_db_memory();
    let state = crate::state::AppState::for_test(db.clone(), None);

    let (ws_tx, ws_rx) = mpsc::channel(capacity);
    let (user_id, device_id) = {
        let conn = db.lock().await;
        let uid = create_user(&conn, TEST_USERNAME, "$argon2id$fake");
        let did = create_test_device(&conn, uid);
        (uid, did)
    };
    let conn = WsConnBuilder::with_defaults(
        user_id,
        TEST_USERNAME.to_string(),
        TEST_APP_SLUG.to_string(),
        ws_tx,
        state,
        device_id,
    )
    .build();

    (conn, ws_rx, db, user_id)
}

/// Seed `n` synthetic user messages into `conv_id` using the common-case
/// defaults for `append_message` (Outgoing, "user" role, simple text JSON,
/// attributed to `user_id`, UTC timezone, no device).
///
/// Returns the seq of the last inserted row, or `None` if `n == 0`.
pub(super) fn seed_user_messages(
    conn: &rusqlite::Connection,
    conv_id: i64,
    user_id: i64,
    n: usize,
) -> Option<i64> {
    let mut last_seq = None;
    for i in 0..n {
        let (_id, seq) = brenn_lib::conversation::append_message(
            conn,
            conv_id,
            brenn_lib::conversation::MessageDirection::Outgoing,
            "user",
            None,
            None,
            &format!(r#"{{"type":"user","message":{{"role":"user","content":"msg {i}"}}}}"#),
            Some(user_id),
            Some("UTC"),
            None,
        );
        last_seq = Some(seq);
    }
    last_seq
}

/// Insert a fake pending upload into the state's registry.
pub(super) async fn inject_pending_upload(
    state: &AppState,
    upload_id: Uuid,
    user_id: i64,
    app_slug: &str,
    filename: &str,
) {
    let disk_filename = format!("{upload_id}_{filename}");
    state.pending_uploads.lock().await.insert(
        upload_id,
        PendingUpload {
            app_slug: app_slug.to_string(),
            filename: filename.to_string(),
            disk_filename,
            media_type: "image/jpeg".to_string(),
            size: 12345,
            uploaded_at: tokio::time::Instant::now(),
            uploader_user_id: user_id,
        },
    );
}

/// Create the attachment file on disk inside the given working directory.
pub(super) fn create_fake_attachment_file(
    working_dir: &std::path::Path,
    upload_id: &Uuid,
    filename: &str,
) {
    let dir = working_dir.join("attachments");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{upload_id}_{filename}"));
    std::fs::write(&path, b"fake jpeg content").unwrap();
}

pub(super) fn test_apps_persistent() -> Arc<IndexMap<String, AppConfig>> {
    let mut apps = IndexMap::new();
    let mut cfg = default_test_app_config("test", "Test App");
    cfg.persistent = true;
    apps.insert("test".to_string(), cfg);
    Arc::new(apps)
}

/// Helper: build a WsConnection for a persistent app with a conversation and bridge.
pub(super) async fn test_ws_conn_persistent() -> (
    WsConnection,
    mpsc::Receiver<WsServerMessage>,
    brenn_lib::db::Db,
    i64,
    i64,
) {
    test_ws_conn_with_resume_conv_and_apps(test_apps_persistent()).await
}

pub(super) fn fake_p256dh() -> String {
    // 0x04 + 64 bytes of 0x00 = 65 bytes
    let mut raw = vec![0x04u8];
    raw.extend_from_slice(&[0u8; 64]);
    use base64ct::{Base64UrlUnpadded, Encoding as _};
    Base64UrlUnpadded::encode_string(&raw)
}

/// Valid base64url-no-pad encoding of 16 zero bytes. ~22 chars.
pub(super) fn fake_auth() -> String {
    use base64ct::{Base64UrlUnpadded, Encoding as _};
    Base64UrlUnpadded::encode_string(&[0u8; 16])
}

/// Build an apps map with `pwa_push.enabled = true` on the "test" app.
pub(super) fn test_apps_with_pwa_push() -> Arc<IndexMap<String, AppConfig>> {
    let mut apps = IndexMap::new();
    let mut cfg = default_test_app_config("test", "Test App");
    cfg.pwa_push = Some(brenn_lib::pwa_push::config::AppPwaPushBlock {
        default_title: None,
    });
    // Push authorization is the `PwaPush` grant (the legacy
    // `[app.pwa_push].enabled` boolean was removed; §2.5.1); grant it so the gate
    // (pwa_push_enabled()) passes for this push-enabled fixture.
    cfg.policy
        .grants
        .insert(brenn_lib::access::AppCapability::PwaPush);
    apps.insert("test".to_string(), cfg);
    Arc::new(apps)
}

/// Build a WsConnection with pwa_push-enabled app and a generated VAPID keypair.
/// Returns `(conn, ws_rx, db, user_id, pwa_push_service)`.
pub(super) async fn test_ws_conn_with_pwa_push() -> (
    WsConnection,
    mpsc::Receiver<WsServerMessage>,
    brenn_lib::db::Db,
    i64,
    Arc<dyn brenn_lib::pwa_push::PwaPushSender>,
) {
    let dir = tempfile::tempdir().expect("tempdir");
    let keypair_path = dir.path().join("vapid.json");
    let vapid = brenn_lib::pwa_push::vapid::load_or_generate(&keypair_path);
    let resolved = brenn_lib::pwa_push::config::ResolvedPwaPushConfig {
        vapid,
        subject: "mailto:test@example.com".to_string(),
        // Use unenforced empty policy in tests so test endpoints like
        // "https://push.example.com/sub" pass without an allowlist.
        endpoint_policy: brenn_lib::pwa_push::endpoint_validator::EndpointPolicy::new(
            vec![],
            false,
        ),
    };

    let db = init_db_memory();
    let apps = test_apps_with_pwa_push();
    let (alert_dispatcher, _handle) = noop_alert_dispatcher();
    let pwa_push: Arc<dyn brenn_lib::pwa_push::PwaPushSender> =
        Arc::new(brenn_lib::pwa_push::PwaPushService::new(
            db.clone(),
            resolved,
            apps.clone(),
            brenn_lib::messaging::MessagingGlobalConfig::default(),
            std::sync::Arc::from("https://brenn.test"),
            alert_dispatcher,
        ));
    let mut state = crate::state::AppState::for_test(db.clone(), Some(apps));
    state.pwa_push = Some(pwa_push.clone());

    let (ws_tx, ws_rx) = mpsc::channel(256);
    let (broadcast_tx, _) = broadcast::channel::<WsServerMessage>(64);

    let (user_id, device_id) = {
        let conn = db.lock().await;
        let uid = create_user(&conn, TEST_USERNAME, "$argon2id$fake");
        let did = create_test_device(&conn, uid);
        (uid, did)
    };

    // Minimal bridge just to satisfy WsConnection (no CC needed for push tests).
    let conv_id = {
        let conn = db.lock().await;
        let cid = brenn_lib::conversation::create_conversation(&conn, user_id, "test", false);
        brenn_lib::conversation::complete_conversation(&conn, cid, None);
        cid
    };
    let test_bridge =
        ActiveBridge::inject_for_test(user_id, conv_id, "test", db.clone(), broadcast_tx);
    *state.test_wake_bridge.lock().await = Some(test_bridge.clone());

    let conn = WsConnBuilder {
        current_conversation_id: Some(conv_id),
        test_bridge: Some(test_bridge),
        ..WsConnBuilder::with_defaults(
            user_id,
            TEST_USERNAME.to_string(),
            TEST_APP_SLUG.to_string(),
            ws_tx,
            state,
            device_id,
        )
    }
    .build();

    (conn, ws_rx, db, user_id, pwa_push)
}

/// Canonical minimal `DebugViewportSnapshotData` for handler/dispatch tests.
///
/// All non-Option fields are set to plausible values; all Option fields are
/// `None`. This is the smallest valid struct the serde deserializer accepts.
/// Used by messaging.rs tests. `dispatch.rs` serializes `DebugViewportSnapshotData::default()`
/// via `serde_json::to_string` instead (no struct-literal duplication).
pub(super) fn minimal_debug_snapshot_data() -> Box<brenn_lib::ws_types::DebugViewportSnapshotData> {
    Box::new(brenn_lib::ws_types::DebugViewportSnapshotData {
        inner_width: 390.0,
        inner_height: 844.0,
        document_element_client_width: 390.0,
        document_element_client_height: 844.0,
        document_element_scroll_height: 844.0,
        scroll_x: 0.0,
        scroll_y: 0.0,
        scrolling_element_scroll_top: None,
        scrolling_element_scroll_left: None,
        device_pixel_ratio: 3.0,
        screen_width: 390.0,
        screen_height: 844.0,
        screen_orientation_type: None,
        display_mode_standalone: true,
        max_width_768: true,
        visual_viewport: None,
        input: None,
        input_bar: None,
        app_main: None,
        pane_layout: None,
        message_list: None,
        attachment_strip: None,
        chip_bar: None,
        presence_bar: None,
        steal_bar: None,
        status_bar: None,
        body: None,
        document_element: None,
        message_list_scroll_top: None,
        message_list_scroll_height: None,
        message_list_client_height: None,
        input_bottom_below_visual_fold: None,
        input_bottom_below_layout: None,
        html_height: None,
        body_height: None,
        body_overflow: None,
        input_bar_position: None,
        input_bar_flex_shrink: None,
        app_main_min_height: None,
        pane_layout_min_height: None,
        pane_layout_height: None,
        message_list_min_height: None,
        message_list_height: None,
        mobile_slot_content_min_height: None,
        app_main_height: None,
        app_topbar: None,
        app_header: None,
        app_layout: None,
        document_element_offset_height: None,
        safe_area_inset_top: None,
        safe_area_inset_right: None,
        safe_area_inset_bottom: None,
        safe_area_inset_left: None,
        probe_100vh_px: None,
        probe_100svh_px: None,
        probe_100lvh_px: None,
        probe_100dvh_px: None,
        probe_exception_units: None,
        screen_avail_height: 844.0,
        window_outer_height: 844.0,
        user_agent: "Mozilla/5.0 (test)".to_string(),
        ua_brands: None,
        ua_mobile: None,
        active_element_tag: None,
        active_element_id: None,
        visibility_state: "visible".to_string(),
        client_timestamp: "2026-06-06T00:00:00.000Z".to_string(),
        build_id: "test-build".to_string(),
    })
}

#[cfg(test)]
mod builder_tests {
    use std::net::{IpAddr, Ipv4Addr};

    use brenn_lib::auth::user::create_user;
    use brenn_lib::db::init_db_memory;
    use brenn_lib::ws_types::ViewportClass;
    use tokio::sync::mpsc;

    use super::{TEST_APP_SLUG, TEST_USERNAME, WsConnBuilder, create_test_device};

    #[tokio::test]
    async fn ws_conn_builder_defaults_are_test_sensible() {
        let db = init_db_memory();
        let state = crate::state::AppState::for_test(db.clone(), None);
        let (ws_tx, _ws_rx) = mpsc::channel(16);
        let (user_id, device_id) = {
            let conn = db.lock().await;
            let uid = create_user(&conn, TEST_USERNAME, "$argon2id$fake");
            let did = create_test_device(&conn, uid);
            (uid, did)
        };

        let mut conn = WsConnBuilder::with_defaults(
            user_id,
            TEST_USERNAME.to_string(),
            TEST_APP_SLUG.to_string(),
            ws_tx,
            state,
            device_id,
        )
        .build();

        assert_eq!(conn.client_ip, IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(conn.timezone, chrono_tz::Tz::UTC);
        assert_eq!(conn.viewport_class, ViewportClass::Wide);
        assert!(!conn.history_sent);
        assert!(conn.last_sent_seq.is_none());
        assert!(conn.queued_responses.is_empty());
        assert!(conn.oldest_loaded_seq.is_none());
        // bridge_notify_rx should be live (empty, not disconnected)
        assert_eq!(
            conn.bridge_notify_rx.try_recv().unwrap_err(),
            tokio::sync::broadcast::error::TryRecvError::Empty
        );
    }
}
