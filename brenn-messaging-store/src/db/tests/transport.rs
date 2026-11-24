//! The `transport` column: written by `insert_message_in_tx`, read back by every
//! decoder that rebuilds an envelope, carried into the eviction archive, and
//! refused on read when it does not parse.

use super::helpers::*;
use crate::db::*;
use brenn_lib::messaging::config::{Depth, Sink};
use brenn_lib::messaging::{
    ChannelEntry, ChannelScheme, MessageBody, MqttTransport, TransportMeta, Urgency,
    WebhookTransport,
};
use chrono::{DateTime, Duration, Utc};
use rusqlite::Connection;
use uuid::Uuid;

/// Bytes that are not UTF-8.
const NOT_UTF8: &[u8] = &[0xff, 0x00, 0xfe, 0x80, 0x41];

fn mqtt_meta() -> TransportMeta {
    TransportMeta::Mqtt(MqttTransport {
        client_slug: "broker".to_string(),
        topic: "cam/front".to_string(),
        qos: 1,
        received_at: DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
    })
}

fn webhook_meta() -> TransportMeta {
    TransportMeta::Webhook(WebhookTransport {
        headers: vec![
            ("authorization".to_string(), "[redacted]".to_string()),
            ("x-tag".to_string(), "a".to_string()),
        ],
        key_id: "k1".to_string(),
        client_ip: "192.0.2.7".to_string(),
        received_at: DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
        endpoint_slug: "scanner".to_string(),
    })
}

fn binary(bytes: &[u8]) -> MessageBody {
    MessageBody::from_parts(Some("application/octet-stream"), bytes).unwrap()
}

fn channel(conn: &Connection, address: &str, scheme: ChannelScheme) -> Uuid {
    let uuid = Uuid::new_v4();
    upsert_channels(
        conn,
        &[ChannelEntry {
            uuid,
            address: address.to_string(),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: scheme,
            mount: None,
        }],
    );
    uuid
}

fn insert(
    conn: &Connection,
    channel_uuid: Uuid,
    scheme: ChannelScheme,
    body: &MessageBody,
    deliver_after: Option<DateTime<Utc>>,
    transport: Option<&TransportMeta>,
) -> InsertedMessage {
    let tx = conn.unchecked_transaction().unwrap();
    let inserted = insert_message_in_tx(
        &tx,
        channel_uuid,
        "src",
        "alice",
        body,
        Urgency::Low,
        scheme,
        None,
        None,
        deliver_after,
        None,
        transport,
        &crate::blob::HeldAttachments::none(),
        utc_to_ns(Utc::now()),
    );
    tx.commit().unwrap();
    inserted
}

fn stored_transport(conn: &Connection, id: i64) -> Option<String> {
    conn.query_row(
        "SELECT transport FROM messaging_messages WHERE id = ?1",
        rusqlite::params![id],
        |r| r.get(0),
    )
    .unwrap()
}

/// A retained row with transport metadata reads back equal, body and all,
/// through the history query base, the retained window and the by-uuid load;
/// a row written without reads `None` through each.
#[test]
fn every_retained_read_decodes_transport() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let mqtt = channel(&conn, "mqtt:broker:cam/front", ChannelScheme::Mqtt);
    let webhook = channel(&conn, "webhook:scanner", ChannelScheme::Webhook);

    let cases = [
        (
            mqtt,
            ChannelScheme::Mqtt,
            binary(NOT_UTF8),
            Some(mqtt_meta()),
        ),
        (
            webhook,
            ChannelScheme::Webhook,
            MessageBody::from_parts(Some("application/json"), b"{\"a\":1}").unwrap(),
            Some(webhook_meta()),
        ),
        (
            mqtt,
            ChannelScheme::Mqtt,
            MessageBody::text("wrapped"),
            None,
        ),
    ];
    for (ch, scheme, body, meta) in &cases {
        let row = insert(&conn, *ch, *scheme, body, None, meta.as_ref());
        assert_eq!(
            stored_transport(&conn, row.id).is_some(),
            meta.is_some(),
            "NULL exactly when absent"
        );

        let sql = format!("{SELECT_ENVELOPE_BASE}WHERE m.uuid = ?1");
        let envelope = conn
            .query_row(&sql, [row.uuid.as_bytes().to_vec()], row_to_envelope)
            .unwrap();
        assert_eq!(&envelope.transport, meta, "history query base");
        assert_eq!(&envelope.body, body);

        let window = load_channel_retained_window_seq(&conn, *ch, Depth::Unbounded);
        let (seq, envelope) = window
            .iter()
            .find(|(_, e)| e.message_id == row.uuid)
            .unwrap();
        assert_eq!(Some(*seq), row.retained_seq, "retained window seq");
        assert_eq!(&envelope.transport, meta, "retained window");

        let tail = load_channel_retained_tail(&conn, *ch, Depth::Unbounded);
        let (id, envelope) = tail.iter().find(|(_, e)| e.message_id == row.uuid).unwrap();
        assert_eq!(*id, row.id, "retained tail rowid");
        assert_eq!(&envelope.transport, meta, "retained tail");

        let envelope = load_envelope_by_uuid(&conn, row.uuid).unwrap();
        assert_eq!(&envelope.transport, meta, "load by uuid");
        assert_eq!(&envelope.body, body);
    }
}

/// The parked reads decode the column at its place, ahead of their trailing
/// `m.id`. No transport adaptor parks a message — the table refuses a parked
/// non-`brenn` row — so the row here is a `brenn` one written directly.
#[test]
fn every_parked_read_decodes_transport_and_trailing_columns() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let ch = channel(&conn, "brenn:parked-transport", ChannelScheme::Brenn);
    let now = Utc::now();
    let meta = mqtt_meta();
    let body = binary(NOT_UTF8);

    let future = insert(
        &conn,
        ch,
        ChannelScheme::Brenn,
        &body,
        Some(now + Duration::hours(1)),
        Some(&meta),
    );
    let due = insert(
        &conn,
        ch,
        ChannelScheme::Brenn,
        &body,
        Some(now - Duration::seconds(1)),
        Some(&meta),
    );

    let pending = list_pending_messages_for_sender(&conn, "alice", Some(ch));
    for row in [&future, &due] {
        let envelope = pending.iter().find(|e| e.message_id == row.uuid).unwrap();
        assert_eq!(envelope.transport.as_ref(), Some(&meta), "pending list");
    }

    let deferred = list_deferred_for_sender(&conn, ch, "alice");
    for row in [&future, &due] {
        let entry = deferred
            .iter()
            .find(|d| d.envelope.message_id == row.uuid)
            .unwrap();
        assert_eq!(entry.message_id, row.id, "deferred view rowid");
        assert_eq!(entry.envelope.transport.as_ref(), Some(&meta));
    }

    let released = release_due_for_channel(&conn, ch, now);
    assert_eq!(released.len(), 1);
    assert_eq!(released[0].message_id, due.id, "due-release rowid");
    assert_eq!(released[0].envelope.transport.as_ref(), Some(&meta));
    assert_eq!(released[0].envelope.body, body);
}

/// The archive line of a raw message carries its transport metadata beside
/// the base64 of its stored bytes; a wrapped one carries no `transport` key.
#[test]
fn the_archive_line_carries_transport() {
    use base64ct::Encoding as _;

    let db = init_db_memory();
    let conn = db.blocking_lock();
    let address = "mqtt:broker:cam/front";
    let ch = channel(&conn, address, ChannelScheme::Mqtt);
    let meta = mqtt_meta();
    insert(
        &conn,
        ch,
        ChannelScheme::Mqtt,
        &binary(NOT_UTF8),
        None,
        Some(&meta),
    );
    insert(
        &conn,
        ch,
        ChannelScheme::Mqtt,
        &MessageBody::text("wrapped"),
        None,
        None,
    );
    insert(
        &conn,
        ch,
        ChannelScheme::Mqtt,
        &MessageBody::text("kept"),
        None,
        None,
    );

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let eviction = bus_gc_evict_channel(
        &conn,
        ch,
        address,
        ChannelScheme::Mqtt,
        1,
        Sink::Archive,
        Some(tmp.path()),
    );
    assert_eq!(eviction.messages_evicted, 2);

    let content = std::fs::read_to_string(tmp.path()).unwrap();
    let lines: Vec<serde_json::Value> = content
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["transport"], serde_json::to_value(&meta).unwrap());
    assert_eq!(lines[0]["envelope_type"], "mqtt");
    let decoded = base64ct::Base64::decode_vec(lines[0]["body_base64"].as_str().unwrap()).unwrap();
    assert_eq!(decoded, NOT_UTF8);
    assert!(lines[1].get("transport").is_none());
    assert_eq!(lines[1]["body"], "wrapped");
}

#[test]
#[should_panic(expected = "has a stored transport that does not parse")]
fn a_stored_transport_that_does_not_parse_panics_on_read() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let ch = channel(&conn, "mqtt:broker:t", ChannelScheme::Mqtt);
    let row = insert(
        &conn,
        ch,
        ChannelScheme::Mqtt,
        &MessageBody::text("x"),
        None,
        Some(&mqtt_meta()),
    );
    conn.execute(
        "UPDATE messaging_messages SET transport = '{\"coap\":{}}' WHERE id = ?1",
        rusqlite::params![row.id],
    )
    .unwrap();
    load_envelope_by_uuid(&conn, row.uuid);
}

// --- the newest retained row for an MQTT topic ---

fn mqtt_meta_on(topic: &str) -> TransportMeta {
    TransportMeta::Mqtt(MqttTransport {
        client_slug: "broker".to_string(),
        topic: topic.to_string(),
        qos: 1,
        received_at: DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
    })
}

fn wrapper_on(topic: &str, text: &str) -> MessageBody {
    MessageBody::text(
        serde_json::to_string(&brenn_lib::messaging::MqttEnvelope {
            client_slug: "broker".to_string(),
            topic: topic.to_string(),
            payload: brenn_lib::messaging::MqttPayloadBody::Text {
                text: text.to_string(),
            },
            received_at: DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
            qos: 1,
        })
        .unwrap(),
    )
}

fn newest_for(conn: &Connection, ch: Uuid, depth: Depth, topic: &str) -> Option<Uuid> {
    newest_retained_for_mqtt_topic(conn, ch, depth, topic).map(|e| e.message_id)
}

/// A raw row is found by the topic in its transport metadata, a wrapped row by
/// the topic in its body; the newest of several for one topic is returned, and
/// rows for other topics, a text row that is not JSON, and JSON without a
/// topic match nothing.
#[test]
fn the_newest_row_for_a_topic_is_found_in_either_shape() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let ch = channel(&conn, "mqtt:broker:cam/#", ChannelScheme::Mqtt);
    let mqtt = ChannelScheme::Mqtt;
    let unbounded = Depth::Unbounded;

    assert_eq!(newest_for(&conn, ch, unbounded, "cam/front"), None);

    let raw_meta = mqtt_meta_on("cam/front");
    let raw = insert(&conn, ch, mqtt, &binary(NOT_UTF8), None, Some(&raw_meta));
    assert_eq!(
        newest_for(&conn, ch, unbounded, "cam/front"),
        Some(raw.uuid)
    );
    let found = newest_retained_for_mqtt_topic(&conn, ch, unbounded, "cam/front").unwrap();
    assert_eq!(found.body, binary(NOT_UTF8));
    assert_eq!(found.transport, Some(raw_meta));

    let wrapped = insert(&conn, ch, mqtt, &wrapper_on("cam/front", "on"), None, None);
    assert_eq!(
        newest_for(&conn, ch, unbounded, "cam/front"),
        Some(wrapped.uuid)
    );

    insert(&conn, ch, mqtt, &wrapper_on("cam/back", "x"), None, None);
    let other_meta = mqtt_meta_on("cam/side");
    insert(&conn, ch, mqtt, &binary(NOT_UTF8), None, Some(&other_meta));
    insert(&conn, ch, mqtt, &MessageBody::text("cam/front"), None, None);
    insert(&conn, ch, mqtt, &MessageBody::text("{\"a\":1}"), None, None);
    assert_eq!(
        newest_for(&conn, ch, unbounded, "cam/front"),
        Some(wrapped.uuid),
        "newer rows for other topics or for none do not hide it"
    );
    assert_eq!(newest_for(&conn, ch, unbounded, "cam/nowhere"), None);
}

/// Rows on another channel are not read.
#[test]
fn the_newest_row_for_a_topic_is_read_on_its_channel_only() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let ch = channel(&conn, "mqtt:broker:cam/#", ChannelScheme::Mqtt);
    let other = channel(&conn, "mqtt:broker:cam/+", ChannelScheme::Mqtt);
    insert(
        &conn,
        other,
        ChannelScheme::Mqtt,
        &wrapper_on("cam/front", "on"),
        None,
        None,
    );
    assert_eq!(newest_for(&conn, ch, Depth::Unbounded, "cam/front"), None);
}

/// Only the newest `depth` retained rows are scanned: a topic whose newest row
/// lies further back is not found.
#[test]
fn a_row_past_the_depth_is_not_found() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let ch = channel(&conn, "mqtt:broker:cam/#", ChannelScheme::Mqtt);
    let mqtt = ChannelScheme::Mqtt;
    let front = insert(&conn, ch, mqtt, &wrapper_on("cam/front", "on"), None, None);
    insert(&conn, ch, mqtt, &wrapper_on("cam/back", "x"), None, None);

    assert_eq!(
        newest_for(&conn, ch, Depth::Bounded(2), "cam/front"),
        Some(front.uuid)
    );
    assert_eq!(newest_for(&conn, ch, Depth::Bounded(1), "cam/front"), None);
}

/// An unbounded or deeper channel is scanned to `RETAINED_REPLAY_SCAN_ROWS`
/// rows and no further.
#[test]
fn a_row_past_the_scan_bound_is_not_found() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let ch = channel(&conn, "mqtt:broker:cam/#", ChannelScheme::Mqtt);
    let mqtt = ChannelScheme::Mqtt;
    let front = insert(&conn, ch, mqtt, &wrapper_on("cam/front", "on"), None, None);
    let filler = wrapper_on("cam/back", "x");
    for _ in 1..RETAINED_REPLAY_SCAN_ROWS {
        insert(&conn, ch, mqtt, &filler, None, None);
    }
    for depth in [
        Depth::Unbounded,
        Depth::Bounded(RETAINED_REPLAY_SCAN_ROWS + 5),
    ] {
        assert_eq!(
            newest_for(&conn, ch, depth, "cam/front"),
            Some(front.uuid),
            "the bound's last row"
        );
    }

    insert(&conn, ch, mqtt, &filler, None, None);
    for depth in [
        Depth::Unbounded,
        Depth::Bounded(RETAINED_REPLAY_SCAN_ROWS + 5),
    ] {
        assert_eq!(newest_for(&conn, ch, depth, "cam/front"), None, "{depth:?}");
    }
}
