//! Bodies of every kind through the publish ladder and back out through the
//! query API: a non-text body and a typed text body, on a ring-backed channel
//! and on a durable channel whose store is reopened between the two. Both
//! channels are declared `non_text`.

use super::super::*;
use super::{CountingRouter, test_app_config};
use crate::config::{MessagingGlobalConfig, ResolvedMessagingConfig};
use crate::format::format_messaging_event_single;
use crate::query::MessageQuery;
use crate::store::RingStores;
use crate::testutils::{declared_non_text, ephemeral_channel_entry, test_channel_entry};
use crate::{MessagingDirectory, WakeRouter, canonical_address, db::upsert_channels};
use brenn_envelope::grants::AppCapability;
use brenn_envelope::{MessageBody, MessageEnvelope};
use brenn_lib::access::acl::ChannelMatcher;
use indexmap::IndexMap;
use std::path::Path;
use std::sync::Arc;

const APP: &str = "pub-app";
const CONVERSATION: i64 = 1;
const DURABLE: &str = "bodies";
const DURABLE_UUID: Uuid = Uuid::from_u128(0xb0d1e5);
const EPHEMERAL_NAME: &str = "bodies-eph";
/// Not valid UTF-8 anywhere: a lone continuation byte, a NUL, and a broken
/// two-byte sequence.
const BINARY: &[u8] = &[0xff, 0xfe, 0x00, 0x80, 0xc3, 0x28];
const BINARY_TYPE: &str = "application/octet-stream";
const MARKDOWN: &str = "# Heading\n\nA *walrus* sighting, not JSON.";
const MARKDOWN_TYPE: &str = "text/markdown";

fn binary_body() -> MessageBody {
    MessageBody::from_parts(Some(BINARY_TYPE), BINARY).expect("a well-formed binary body")
}

fn markdown_body() -> MessageBody {
    MessageBody::from_parts(Some(MARKDOWN_TYPE), MARKDOWN.as_bytes())
        .expect("a well-formed markdown body")
}

/// Publishes and reads on both schemes: `brenn:` through the default messaging
/// grants, `ephemeral:` through its own grant pair.
fn app() -> brenn_lib::config::AppConfig {
    let mut cfg = test_app_config(
        APP,
        Some(ResolvedMessagingConfig {
            send_budget: 100,
            subscriptions: vec![],
        }),
        vec!["bob".to_string()],
    );
    let policy = cfg.policy_mut();
    policy.grants.insert(AppCapability::EphemeralPublish);
    policy.grants.insert(AppCapability::EphemeralSubscribe);
    policy
        .acls
        .ephemeral_publish
        .push(ChannelMatcher::Prefix(String::new()));
    policy
        .acls
        .ephemeral_subscribe
        .push(ChannelMatcher::Prefix(String::new()));
    cfg
}

/// A messenger over one durable and one ring-backed channel. `path` names the
/// database file, or `None` for an in-memory one. The fixture rows are written
/// idempotently, so a second messenger over the same file sees the first's.
async fn messenger(path: Option<&Path>) -> Arc<Messenger> {
    let db = match path {
        Some(path) => crate::db::init_db(path),
        None => crate::db::init_db_memory(),
    };
    // A fixed uuid, so the reopened store finds the same channel row.
    let durable = declared_non_text(ChannelEntry {
        uuid: DURABLE_UUID,
        ..test_channel_entry(DURABLE, vec![])
    });
    let ephemeral = declared_non_text(ephemeral_channel_entry(EPHEMERAL_NAME, 8));
    {
        let conn = db.lock().await;
        conn.execute(
            "INSERT OR IGNORE INTO users (id, username, password_hash, created_at) \
             VALUES (1, 'bob', 'h', '2024-01-01')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO conversations \
             (id, user_id, status, app_slug, created_at, updated_at) \
             VALUES (1, 1, 'active', 'pub-app', '2024-01-01', '2024-01-01')",
            [],
        )
        .unwrap();
        upsert_channels(&conn, std::slice::from_ref(&durable));
    }
    let stores = Arc::new(RingStores::build(std::slice::from_ref(&ephemeral)));
    let mut apps = IndexMap::new();
    apps.insert(APP.to_string(), app());
    Messenger::new(
        db,
        Arc::new(MessagingDirectory::with_entries(vec![durable, ephemeral])),
        Arc::from("test-source"),
        Arc::new(apps),
        Arc::new(CountingRouter::default()) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_ring_stores(stores)
}

async fn publish(m: &Messenger, addr: &str, body: MessageBody) -> Uuid {
    match m
        .publish(
            PublishOrigin::Conversation { id: CONVERSATION },
            APP,
            addr,
            body,
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await
    {
        PublishResult::Ok { message_id, .. } => message_id,
        other => panic!("publish to {addr} refused: {other:?}"),
    }
}

async fn query(m: &Messenger, addr: &str, search: Option<&str>) -> Vec<MessageEnvelope> {
    m.query(&MessageQuery {
        channel: addr.to_string(),
        limit: 10,
        before: None,
        after: None,
        sender: None,
        search: search.map(str::to_string),
        calling_app_slug: APP.to_string(),
    })
    .await
    .unwrap_or_else(|e| panic!("query of {addr} failed: {e}"))
}

/// What a channel read back holds: the two published bodies, newest first,
/// each byte-exact with its content type; a search for a word of the markdown
/// finds it alone; and a conversation is shown the markdown as text with its
/// type and the binary body as a placeholder.
async fn assert_read_back(m: &Messenger, addr: &str, binary_id: Uuid, markdown_id: Uuid) {
    let all = query(m, addr, None).await;
    assert_eq!(
        all.iter().map(|e| e.message_id).collect::<Vec<_>>(),
        vec![markdown_id, binary_id],
        "both bodies, newest first, on {addr}"
    );
    let (markdown, binary) = (&all[0], &all[1]);

    assert_eq!(binary.body, binary_body());
    assert_eq!(binary.body.as_bytes(), BINARY);
    assert_eq!(binary.body.content_type(), Some(BINARY_TYPE));
    assert!(!binary.body.is_text());

    assert_eq!(markdown.body, markdown_body());
    assert_eq!(markdown.body.as_text(), Some(MARKDOWN));
    assert_eq!(markdown.body.content_type(), Some(MARKDOWN_TYPE));

    let found = query(m, addr, Some("walrus")).await;
    assert_eq!(
        found.iter().map(|e| e.message_id).collect::<Vec<_>>(),
        vec![markdown_id],
        "search finds the markdown body and never the binary one on {addr}"
    );

    let shown = format_messaging_event_single(markdown);
    assert!(
        shown.contains(r#""content_type":"text/markdown""#),
        "a typed text body is shown with its type: {shown}"
    );
    assert!(
        shown.contains("walrus"),
        "a typed text body is shown as its text: {shown}"
    );
    let shown = format_messaging_event_single(binary);
    assert!(
        shown.contains(
            r#""body":{"binary":true,"content_type":"application/octet-stream","size":6}"#
        ),
        "a binary body is shown as its placeholder: {shown}"
    );
}

#[tokio::test]
async fn bodies_of_every_kind_round_trip_through_a_ring_backed_channel() {
    let m = messenger(None).await;
    let addr = format!("ephemeral:{EPHEMERAL_NAME}");

    let binary_id = publish(&m, &addr, binary_body()).await;
    let markdown_id = publish(&m, &addr, markdown_body()).await;

    assert_read_back(&m, &addr, binary_id, markdown_id).await;
}

#[tokio::test]
async fn bodies_of_every_kind_round_trip_through_a_reopened_durable_store() {
    let dir = std::env::temp_dir().join(format!("brenn-body-kinds-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("create the test's database directory");
    let path = dir.join("messaging.db");
    let addr = canonical_address(DURABLE);

    let (binary_id, markdown_id) = {
        let m = messenger(Some(&path)).await;
        let binary_id = publish(&m, &addr, binary_body()).await;
        let markdown_id = publish(&m, &addr, markdown_body()).await;
        (binary_id, markdown_id)
    };

    let m = messenger(Some(&path)).await;
    assert_read_back(&m, &addr, binary_id, markdown_id).await;

    drop(m);
    std::fs::remove_dir_all(&dir).expect("remove the test's database directory");
}

/// The size gate measures a non-text body by its raw bytes, at the publish
/// ladder as at the gate itself.
#[tokio::test]
async fn an_over_cap_binary_body_is_refused_by_its_byte_count() {
    let m = messenger(None).await;
    let max = MessagingGlobalConfig::default().max_body_bytes;
    let at_cap = MessageBody::from_parts(Some(BINARY_TYPE), &vec![0xff; max]).expect("binary body");
    let over =
        MessageBody::from_parts(Some(BINARY_TYPE), &vec![0xff; max + 1]).expect("binary body");
    let addr = format!("ephemeral:{EPHEMERAL_NAME}");

    publish(&m, &addr, at_cap).await;
    let refused = m
        .publish(
            PublishOrigin::Conversation { id: CONVERSATION },
            APP,
            &addr,
            over,
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    assert!(
        matches!(refused, PublishResult::BodyTooLarge { len, max: cap } if len == max + 1 && cap == max),
        "expected BodyTooLarge, got {refused:?}"
    );
}
