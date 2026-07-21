//! `submit_ingress` tests (design §"Tests: fanned into `publish/tests/` by
//! family", `publish/tests/ingress.rs`): the ingress publish path durably
//! inserts a pending push row and signals the dispatcher without any inline
//! deliver / eager wake (R1).
//!
//! These tests use the CountingRouter mock (deliver_returns controls outcome)
//! and a minimal in-memory DB with a user + conversation row.
//!
//! Production items are reached via `use super::super::*;` (directly from
//! `publish/mod.rs`); the cross-family shared fixtures (`CountingRouter`,
//! `test_app_config`) are declared `pub(super)` in `tests/mod.rs` and pulled in
//! by the named `use super::{…};` below. `build_ingress_messenger` and the
//! `count_pending_pushes` / `count_delivered_pushes` helpers are used only by
//! this family, so per design §"Tests: fanned…" they live here rather than in
//! the harness.

use super::super::*;
use super::{CountingRouter, test_app_config};
use crate::db::init_db_memory;
use crate::messaging::{MessagingDirectory, MessagingGlobalConfig, Urgency, WakeRouter};
use indexmap::IndexMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

async fn build_ingress_messenger(deliver_returns: u64) -> (Arc<Messenger>, Arc<CountingRouter>) {
    let db = init_db_memory();
    let conn = db.lock().await;
    conn.execute(
        "INSERT INTO users (id, username, password_hash, created_at) \
         VALUES (1, 'alice', 'h', '2024-01-01')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
         VALUES (1, 1, 'active', 'myapp', '2024-01-01', '2024-01-01')",
        [],
    )
    .unwrap();
    drop(conn);

    let directory = Arc::new(MessagingDirectory::new());
    let mut apps_raw: IndexMap<String, crate::config::AppConfig> = IndexMap::new();
    apps_raw.insert(
        "myapp".to_string(),
        test_app_config("myapp", None, vec!["alice".to_string()]),
    );
    let apps = Arc::new(apps_raw);
    let router = Arc::new(CountingRouter::default());
    router
        .deliver_returns
        .store(deliver_returns, Ordering::SeqCst);
    let messenger = Messenger::new(
        db,
        directory,
        Arc::from("test-source"),
        apps,
        router.clone() as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    );
    (messenger, router)
}

async fn count_pending_pushes(messenger: &Arc<Messenger>, conversation_id: i64) -> i64 {
    let conn = messenger.db.lock().await;
    conn.query_row(
        "SELECT COUNT(*) FROM messaging_pending_pushes pp
         JOIN messaging_messages m ON pp.message_id = m.id
         WHERE pp.target_subscriber = ?1
           AND pp.delivered_at IS NULL
           AND m.envelope_type = 'ingress'",
        rusqlite::params![
            crate::messaging::ParticipantId::for_conversation(conversation_id).as_str()
        ],
        |row| row.get(0),
    )
    .unwrap()
}

async fn count_delivered_pushes(messenger: &Arc<Messenger>, conversation_id: i64) -> i64 {
    let conn = messenger.db.lock().await;
    conn.query_row(
        "SELECT COUNT(*) FROM messaging_pending_pushes pp
         JOIN messaging_messages m ON pp.message_id = m.id
         WHERE pp.target_subscriber = ?1
           AND pp.delivered_at IS NOT NULL
           AND m.envelope_type = 'ingress'",
        rusqlite::params![
            crate::messaging::ParticipantId::for_conversation(conversation_id).as_str()
        ],
        |row| row.get(0),
    )
    .unwrap()
}

/// submit_ingress durably inserts the push row and signals the dispatcher
/// (R1). No inline deliver call is made — the router is never called.
#[tokio::test]
async fn submit_ingress_inserts_pending_row_and_signals_dispatcher() {
    let (m, router) = build_ingress_messenger(0).await;
    m.submit_ingress(
        1,
        "myapp",
        "mqtt:client:conn",
        "topic/x",
        r#"{"v":1}"#,
        Urgency::Normal,
    )
    .await;
    // Push row inserted and stays pending — dispatcher marks it delivered later.
    assert_eq!(
        count_pending_pushes(&m, 1).await,
        1,
        "pending row must exist"
    );
    assert_eq!(
        count_delivered_pushes(&m, 1).await,
        0,
        "must not be pre-marked delivered"
    );
    // No inline router calls — deliver and spawn_eager_wake are dispatcher's job.
    assert_eq!(
        router.deliveries.lock().await.len(),
        0,
        "submit_ingress must not call deliver inline"
    );
    assert_eq!(
        router.eager_wakes.load(Ordering::SeqCst),
        0,
        "submit_ingress must not call spawn_eager_wake inline"
    );
}

/// submit_ingress with None wake: same durable-insert + signal behavior.
#[tokio::test]
async fn submit_ingress_none_wake_inserts_pending_row() {
    let (m, router) = build_ingress_messenger(0).await;
    m.submit_ingress(1, "myapp", "webhook:x", "slug", "{}", Urgency::Low)
        .await;
    assert_eq!(count_pending_pushes(&m, 1).await, 1);
    assert_eq!(count_delivered_pushes(&m, 1).await, 0);
    assert_eq!(router.deliveries.lock().await.len(), 0);
    assert_eq!(router.eager_wakes.load(Ordering::SeqCst), 0);
}
