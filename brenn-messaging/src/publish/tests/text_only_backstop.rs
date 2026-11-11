//! The store-entry backstop: every path that puts a `MessageBody` into a
//! channel's store asserts that a non-text body goes only to a channel declared
//! `non_text`. Each site is driven once onto a text-only channel, where it
//! panics, and once onto a `non_text` channel, where the body lands byte-exact.
//! Reaching the panic is a host bug: the doors in front of these sites refuse a
//! non-text body for a text-only channel.

use super::super::*;
use super::{CountingRouter, test_app_config};
use crate::config::{Depth, MessagingGlobalConfig, ResolvedMessagingConfig};
use crate::live::PrepaidEntry;
use crate::store::{DeferralOutcome, NewMessage, RingStores};
use crate::testutils::{declared_non_text, ephemeral_channel_entry, test_channel_entry};
use crate::{MessagingDirectory, WakeRouter, canonical_address, db::upsert_channels};
use brenn_envelope::MessageBody;
use brenn_envelope::grants::AppCapability;
use brenn_lib::access::AppPolicy;
use brenn_lib::access::acl::ChannelMatcher;
use chrono::Duration;
use indexmap::IndexMap;
use std::sync::Arc;

const APP: &str = "backstop-app";
const COMPONENT: &str = "backstop-proc";
const PLAIN: &str = "plain";
const FRAMES: &str = "frames";
const PLAIN_EPH: &str = "plain-eph";
const FRAMES_EPH: &str = "frames-eph";
const BINARY: &[u8] = &[0xff, 0x00, 0xd8, 0x80];
const BINARY_TYPE: &str = "image/jpeg";
const PANIC: &str = "is text-only and a host path is storing a non-text body";

fn binary_body() -> MessageBody {
    MessageBody::from_parts(Some(BINARY_TYPE), BINARY).expect("a well-formed binary body")
}

fn eph(name: &str) -> String {
    format!("ephemeral:{name}")
}

/// Publishes and reads on both schemes, every channel.
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
    policy
        .acls
        .ephemeral_publish
        .push(ChannelMatcher::Prefix(String::new()));
    cfg
}

/// A messenger over a text-only and a `non_text` channel on each substrate.
async fn messenger() -> Arc<Messenger> {
    let db = crate::db::init_db_memory();
    let durable = vec![
        test_channel_entry(PLAIN, vec![]),
        declared_non_text(test_channel_entry(FRAMES, vec![])),
    ];
    let rings = vec![
        ephemeral_channel_entry(PLAIN_EPH, 8),
        declared_non_text(ephemeral_channel_entry(FRAMES_EPH, 8)),
    ];
    {
        let conn = db.lock().await;
        conn.execute(
            "INSERT INTO users (id, username, password_hash, created_at) \
             VALUES (1, 'bob', 'h', '2024-01-01')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
             VALUES (1, 1, 'active', 'backstop-app', '2024-01-01', '2024-01-01')",
            [],
        )
        .unwrap();
        upsert_channels(&conn, &durable);
    }
    let stores = Arc::new(RingStores::build(&rings));
    let mut apps = IndexMap::new();
    apps.insert(APP.to_string(), app());
    Messenger::new(
        db,
        Arc::new(MessagingDirectory::with_entries(
            durable.into_iter().chain(rings).collect(),
        )),
        Arc::from("test-source"),
        Arc::new(apps),
        Arc::new(CountingRouter::default()) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_ring_stores(stores)
}

/// The newest retained body on `addr`.
async fn newest_body(m: &Messenger, addr: &str) -> MessageBody {
    let entry = m.directory.resolve(addr).expect("fixture channel");
    let tail = m.store_for(&entry).retained_tail(Depth::Bounded(1)).await;
    tail.first()
        .unwrap_or_else(|| panic!("{addr} retains nothing"))
        .body
        .clone()
}

async fn gated_publish(m: &Messenger, addr: &str, body: MessageBody) -> PublishResult {
    m.publish(
        PublishOrigin::Conversation { id: 1 },
        APP,
        addr,
        body,
        Urgency::Normal,
        None,
        None,
        None,
    )
    .await
}

async fn wasm_publish(m: &Messenger, addr: &str, body: MessageBody) {
    m.publish_from_wasm(
        COMPONENT,
        &[WasmPublish {
            channel_address: addr,
            body,
            urgency: Urgency::Normal,
            reply_to: None,
            deliver_after: None,
        }],
    )
    .await;
}

#[tokio::test]
#[should_panic(expected = "\"ephemeral:plain-eph\" is text-only")]
async fn a_gated_publish_of_a_non_text_body_to_a_text_only_ring_channel_panics() {
    let m = messenger().await;
    gated_publish(&m, &eph(PLAIN_EPH), binary_body()).await;
}

#[tokio::test]
#[should_panic(expected = "\"brenn:plain\" is text-only")]
async fn a_gated_publish_of_a_non_text_body_to_a_text_only_durable_channel_panics() {
    let m = messenger().await;
    gated_publish(&m, &canonical_address(PLAIN), binary_body()).await;
}

#[tokio::test]
async fn a_gated_publish_of_a_non_text_body_lands_on_a_non_text_channel() {
    let m = messenger().await;
    for addr in [canonical_address(FRAMES), eph(FRAMES_EPH)] {
        let result = gated_publish(&m, &addr, binary_body()).await;
        assert!(
            matches!(result, PublishResult::Ok { .. }),
            "{addr}: {result:?}"
        );
        assert_eq!(newest_body(&m, &addr).await, binary_body(), "{addr}");
    }
}

/// Text goes anywhere: typed text is text, and a text-only channel takes it.
#[tokio::test]
async fn a_typed_text_body_lands_on_a_text_only_channel() {
    let m = messenger().await;
    let markdown =
        MessageBody::from_parts(Some("text/markdown"), b"# fine").expect("typed text body");
    for addr in [canonical_address(PLAIN), eph(PLAIN_EPH)] {
        let result = gated_publish(&m, &addr, markdown.clone()).await;
        assert!(
            matches!(result, PublishResult::Ok { .. }),
            "{addr}: {result:?}"
        );
        assert_eq!(newest_body(&m, &addr).await, markdown, "{addr}");
    }
}

#[tokio::test]
#[should_panic(expected = "\"brenn:plain\" is text-only")]
async fn a_wasm_flush_of_a_non_text_body_to_a_text_only_durable_channel_panics() {
    let m = messenger().await;
    wasm_publish(&m, &canonical_address(PLAIN), binary_body()).await;
}

#[tokio::test]
#[should_panic(expected = "\"ephemeral:plain-eph\" is text-only")]
async fn a_wasm_flush_of_a_non_text_body_to_a_text_only_ring_channel_panics() {
    let m = messenger().await;
    wasm_publish(&m, &eph(PLAIN_EPH), binary_body()).await;
}

#[tokio::test]
async fn a_wasm_flush_of_a_non_text_body_lands_on_a_non_text_channel() {
    let m = messenger().await;
    for addr in [canonical_address(FRAMES), eph(FRAMES_EPH)] {
        wasm_publish(&m, &addr, binary_body()).await;
        assert_eq!(newest_body(&m, &addr).await, binary_body(), "{addr}");
    }
}

/// A flush that panics at the backstop lands nothing: the durable rows of the
/// batch are written in one transaction that the unwind rolls back.
#[tokio::test]
async fn a_wasm_flush_that_trips_the_backstop_lands_nothing() {
    let m = messenger().await;
    let frames = canonical_address(FRAMES);
    let plain = canonical_address(PLAIN);
    let flush = {
        let m = Arc::clone(&m);
        let (frames, plain) = (frames.clone(), plain.clone());
        tokio::spawn(async move {
            m.publish_from_wasm(
                COMPONENT,
                &[
                    WasmPublish {
                        channel_address: &frames,
                        body: binary_body(),
                        urgency: Urgency::Normal,
                        reply_to: None,
                        deliver_after: None,
                    },
                    WasmPublish {
                        channel_address: &plain,
                        body: binary_body(),
                        urgency: Urgency::Normal,
                        reply_to: None,
                        deliver_after: None,
                    },
                ],
            )
            .await;
        })
    };
    let err = flush.await.expect_err("the flush panics at the backstop");
    assert!(err.is_panic());
    for addr in [frames, plain] {
        let entry = m.directory.resolve(&addr).expect("fixture channel");
        assert!(
            m.store_for(&entry)
                .retained_tail(Depth::Unbounded)
                .await
                .is_empty(),
            "{addr} holds nothing from the panicked flush"
        );
    }
}

fn prepaid_policy() -> AppPolicy {
    let mut p = AppPolicy::with_grants(&[AppCapability::EphemeralPublish]);
    p.acls.ephemeral_publish = vec![ChannelMatcher::Prefix(String::new())];
    p
}

fn prepaid_entry() -> PrepaidEntry {
    PrepaidEntry {
        body: binary_body(),
        urgency: Urgency::Normal,
        publish_ts: Utc::now(),
    }
}

#[tokio::test]
#[should_panic(expected = "\"ephemeral:plain-eph\" is text-only")]
async fn a_prepaid_publish_of_a_non_text_body_to_a_text_only_channel_panics() {
    let m = messenger().await;
    let dest = m.resolve_prepaid(
        &ParticipantId::for_wasm(COMPONENT),
        &prepaid_policy(),
        &eph(PLAIN_EPH),
    );
    let _ = m.publish_prepaid(&dest, prepaid_entry()).await;
}

#[tokio::test]
#[should_panic(expected = "\"ephemeral:plain-eph\" is text-only")]
async fn a_prepaid_park_of_a_non_text_body_to_a_text_only_channel_panics() {
    let m = messenger().await;
    let dest = m.resolve_prepaid(
        &ParticipantId::for_wasm(COMPONENT),
        &prepaid_policy(),
        &eph(PLAIN_EPH),
    );
    let _ = m.park_prepaid(&dest, prepaid_entry(), Utc::now() + Duration::minutes(5));
}

#[tokio::test]
async fn a_prepaid_publish_and_park_of_a_non_text_body_land_on_a_non_text_channel() {
    let m = messenger().await;
    let sender = ParticipantId::for_wasm(COMPONENT);
    let addr = eph(FRAMES_EPH);
    let dest = m.resolve_prepaid(&sender, &prepaid_policy(), &addr);

    let appended = m.publish_prepaid(&dest, prepaid_entry()).await;
    assert_eq!(appended.retained.envelope.body, binary_body());
    assert_eq!(newest_body(&m, &addr).await, binary_body());

    m.park_prepaid(&dest, prepaid_entry(), Utc::now() + Duration::minutes(5))
        .expect("the deferred set has room");
    let parked = m.deferred_view_for_sender(&addr, sender.as_str()).await;
    assert_eq!(parked.len(), 1);
    assert_eq!(parked[0].envelope.body, binary_body());
}

/// Parks one text message from `COMPONENT` on `addr` and returns its uuid.
async fn park_text(m: &Messenger, addr: &str) -> Uuid {
    let entry = m.directory.resolve(addr).expect("fixture channel");
    let now = Utc::now();
    m.store_for(&entry)
        .park(
            NewMessage {
                source: "test-source".to_string(),
                sender: ParticipantId::for_wasm(COMPONENT).as_str().to_string(),
                body: "parked".into(),
                urgency: Urgency::Normal,
                envelope_type: entry.transport_type,
                reply_to: None,
                delivery_deadline: None,
                impetus: None,
                publish_ts_ns: now.timestamp_nanos_opt().unwrap(),
            },
            now + Duration::minutes(5),
        )
        .await
        .expect("the deferred set has room")
        .message_uuid
}

async fn edit_to_binary(m: &Messenger, addr: &str, uuid: Uuid) -> DeferralOutcome {
    m.edit_deferred_for_sender(
        addr,
        ParticipantId::for_wasm(COMPONENT).as_str(),
        uuid,
        Some(binary_body()),
        None,
        Utc::now(),
    )
    .await
}

#[tokio::test]
#[should_panic(expected = "\"ephemeral:plain-eph\" is text-only")]
async fn a_deferred_edit_to_a_non_text_body_on_a_text_only_ring_channel_panics() {
    let m = messenger().await;
    let addr = eph(PLAIN_EPH);
    let uuid = park_text(&m, &addr).await;
    edit_to_binary(&m, &addr, uuid).await;
}

#[tokio::test]
#[should_panic(expected = "\"brenn:plain\" is text-only")]
async fn a_deferred_edit_to_a_non_text_body_on_a_text_only_durable_channel_panics() {
    let m = messenger().await;
    let addr = canonical_address(PLAIN);
    let uuid = park_text(&m, &addr).await;
    edit_to_binary(&m, &addr, uuid).await;
}

#[tokio::test]
async fn a_deferred_edit_to_a_non_text_body_lands_on_a_non_text_channel() {
    let m = messenger().await;
    for addr in [canonical_address(FRAMES), eph(FRAMES_EPH)] {
        let uuid = park_text(&m, &addr).await;
        assert_eq!(
            edit_to_binary(&m, &addr, uuid).await,
            DeferralOutcome::Applied,
            "{addr}"
        );
        let parked = m
            .deferred_view_for_sender(&addr, ParticipantId::for_wasm(COMPONENT).as_str())
            .await;
        assert_eq!(parked.len(), 1, "{addr}");
        assert_eq!(parked[0].envelope.body, binary_body(), "{addr}");
    }
}

/// An edit that leaves the body alone asserts nothing about it.
#[tokio::test]
async fn a_deferred_edit_of_the_release_time_alone_passes_a_text_only_channel() {
    let m = messenger().await;
    let addr = eph(PLAIN_EPH);
    let uuid = park_text(&m, &addr).await;
    let outcome = m
        .edit_deferred_for_sender(
            &addr,
            ParticipantId::for_wasm(COMPONENT).as_str(),
            uuid,
            None,
            Some(Utc::now() + Duration::minutes(9)),
            Utc::now(),
        )
        .await;
    assert_eq!(outcome, DeferralOutcome::Applied);
}

#[test]
fn the_panic_text_is_the_one_these_tests_expect() {
    let entry = ephemeral_channel_entry(PLAIN_EPH, 8);
    let caught = std::panic::catch_unwind(|| entry.assert_may_hold(&binary_body()))
        .expect_err("a non-text body on a text-only entry panics");
    let text = caught
        .downcast_ref::<String>()
        .expect("a formatted panic message");
    assert!(text.contains(PANIC), "{text}");
    assert!(text.contains("image/jpeg, 4 bytes"), "{text}");
}
