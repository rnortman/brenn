//! The envelope-row contract, pinned in the crate that owns it: the SELECT
//! constant's literal text, and the positional decode of a real row into a
//! `MessageEnvelope`.
//!
//! Every field the decoder reads gets a distinct sentinel, so a transposition
//! between two same-typed columns (`source`/`sender`/`body`/`channel`/
//! `reply_to` are all `String`) fails here rather than surfacing as a wrong
//! field a crate away.

use super::helpers::*;
use crate::db::*;
use brenn_lib::messaging::{ChannelEntry, ChannelScheme, Impetus, Urgency, canonical_address};
use chrono::{DateTime, Utc};
use uuid::Uuid;

#[test]
fn select_envelope_base_matches_prerefactor_literal() {
    // Golden: the base SELECT fragment, verbatim. Fails loudly if the constant
    // is edited without updating `row_to_envelope`'s positional contract.
    let golden = "SELECT m.uuid, m.channel_uuid, m.source, m.sender, m.body, m.urgency, \
                  m.reply_to_uuid, m.delivery_deadline, m.deliver_after, m.publish_ts_ns, \
                  c.address, rc.address, m.envelope_type, m.impetus \
           FROM messaging_messages m \
           JOIN messaging_channels c ON c.uuid = m.channel_uuid \
           LEFT JOIN messaging_channels rc ON rc.uuid = m.reply_to_uuid ";
    assert_eq!(SELECT_ENVELOPE_BASE, golden);
}

/// A channel row under `address`, upserted so the SELECT's JOINs resolve.
fn channel(conn: &rusqlite::Connection, address: &str) -> Uuid {
    let uuid = Uuid::new_v4();
    upsert_channels(
        conn,
        &[ChannelEntry {
            uuid,
            address: canonical_address(address),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );
    uuid
}

#[test]
fn select_envelope_base_decodes_every_column_into_its_field() {
    let db = init_db_memory();
    let conn = db.blocking_lock();

    let channel_uuid = channel(&conn, "row-channel");
    let reply_uuid = channel(&conn, "row-reply-channel");

    let deadline: DateTime<Utc> = "2026-03-04T05:06:07+00:00".parse().unwrap();
    let after: DateTime<Utc> = "2026-03-05T06:07:08+00:00".parse().unwrap();
    let publish_ts_ns = 1_772_000_000_000_000_000;

    let inserted = insert_message(
        &conn,
        channel_uuid,
        "row-source",
        "row-sender",
        "row-body",
        Urgency::High,
        ChannelScheme::Brenn,
        Some(reply_uuid),
        Some(deadline),
        Some(after),
        Some(Impetus::Replenish),
        publish_ts_ns,
    );

    let sql = format!("{SELECT_ENVELOPE_BASE}WHERE m.uuid = ?1");
    let mut stmt = conn.prepare(&sql).unwrap();
    let envelope = stmt
        .query_row([inserted.uuid.as_bytes().to_vec()], row_to_envelope)
        .unwrap();

    assert_eq!(envelope.message_id, inserted.uuid);
    assert_eq!(envelope.source, "row-source");
    assert_eq!(envelope.sender, "row-sender");
    assert_eq!(envelope.body, "row-body");
    assert_eq!(envelope.channel, canonical_address("row-channel"));
    assert_eq!(
        envelope.reply_to,
        Some(canonical_address("row-reply-channel"))
    );
    assert_eq!(envelope.urgency, Urgency::High);
    assert_eq!(envelope.envelope_type, ChannelScheme::Brenn);
    assert_eq!(envelope.impetus, Some(Impetus::Replenish));
    assert_eq!(envelope.delivery_deadline, Some(deadline));
    assert_eq!(envelope.deliver_after, Some(after));
    assert_eq!(envelope.publish_ts, ns_to_utc(publish_ts_ns));
}
