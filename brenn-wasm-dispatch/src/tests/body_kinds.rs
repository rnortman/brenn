//! What the 0.1.0 activation hands a guest for each kind of body.
//!
//! The contract's `envelope-json` and `deferred-entry.payload` are strings. A
//! typed text body crosses as exactly its text; a non-text body reaching
//! either is a host invariant broken, and lowering panics naming the carrier.

use super::*;

use brenn_lib::messaging::MessageBody;

/// The `outcome` of each of this subscriber's quarantine rows.
async fn failure_outcomes(
    messenger: &brenn_messaging::Messenger,
    subscriber: &ParticipantId,
) -> Vec<String> {
    let conn = messenger.db().lock().await;
    let mut stmt = conn
        .prepare("SELECT outcome FROM messaging_wasm_consume_failures WHERE subscriber = ?1")
        .expect("the failure query prepares");
    let rows = stmt
        .query_map(rusqlite::params![subscriber.as_str()], |row| row.get(0))
        .expect("the failure query runs");
    rows.map(|r| r.expect("a failure row reads back")).collect()
}

fn binary_body() -> MessageBody {
    MessageBody::from_parts(Some("application/octet-stream"), &[0xff, 0x00, 0xfe])
        .expect("a typed binary body is constructible")
}

/// The demo component traps on a body that is exactly `__trap__`. A
/// `text/markdown` body with that text makes it trap, so the guest was handed
/// the bare text, not the type or a wrapped value. A guest that could not parse
/// its envelope would return an error instead, quarantined as `err`.
#[tokio::test]
async fn a_typed_text_body_reaches_the_guest_as_exactly_its_text() {
    let slug = "body-typed-text";
    let (messenger, channel, wasm_sub) =
        testutils::build_wasm_messenger(slug, "typed-ch", Depth::Unbounded, Depth::Unbounded).await;
    let body =
        MessageBody::from_parts(Some("text/markdown"), b"__trap__").expect("typed text body");
    testutils::insert_bus_body_from(
        &messenger,
        &channel,
        "test-sender",
        &body,
        ChannelScheme::Brenn,
        None,
        brenn_messaging_store::db::utc_to_ns(chrono::Utc::now()),
    )
    .await;

    let (cfg, _handle, _db) = build_cfg(
        slug,
        Arc::clone(&messenger),
        &channel,
        Depth::Unbounded,
        Depth::Unbounded,
    );
    drain_step(&cfg, &wasm_sub, MountDebt::Settled).await;

    assert_eq!(
        failure_outcomes(&messenger, &wasm_sub).await,
        vec!["trap".to_owned()],
        "the guest saw `__trap__` and trapped; the batch is quarantined"
    );
}

/// A non-text body in a consumer's window is lowered to `envelope-json`
/// nowhere: the drain panics before the guest runs.
#[tokio::test]
#[should_panic(expected = "0.1.0 envelope-json")]
async fn a_non_text_body_in_a_window_panics_at_lowering() {
    let slug = "body-binary-window";
    let (messenger, channel, wasm_sub) =
        testutils::build_wasm_messenger(slug, "binary-ch", Depth::Unbounded, Depth::Unbounded)
            .await;
    testutils::insert_bus_body_from(
        &messenger,
        &channel,
        "test-sender",
        &binary_body(),
        ChannelScheme::Brenn,
        None,
        brenn_messaging_store::db::utc_to_ns(chrono::Utc::now()),
    )
    .await;

    let (cfg, _handle, _db) = build_cfg(
        slug,
        Arc::clone(&messenger),
        &channel,
        Depth::Unbounded,
        Depth::Unbounded,
    );
    drain_step(&cfg, &wasm_sub, MountDebt::Settled).await;
}

/// A non-text body parked by this component on its output port panics where
/// the deferred view is lowered to `deferred-entry.payload`.
#[tokio::test]
#[should_panic(expected = "0.1.0 deferred-entry.payload")]
async fn a_non_text_parked_body_panics_at_deferred_lowering() {
    let (messenger, _in_entry, out_entry, wasm_sub, _out_sub, cfg, _handle, _db) =
        build_two_channel_setup("body-binary-parked", "body-binary-reader").await;
    let now = chrono::Utc::now();
    testutils::insert_bus_body_from(
        &messenger,
        &out_entry,
        wasm_sub.as_str(),
        &binary_body(),
        ChannelScheme::Brenn,
        Some(now + chrono::Duration::hours(1)),
        brenn_messaging_store::db::utc_to_ns(now),
    )
    .await;

    drain_step(&cfg, &wasm_sub, MountDebt::Owed).await;
}
