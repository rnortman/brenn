//! The envelope row contract: the SELECT that reads `messaging_messages` and
//! the decoder that turns one of its rows back into a [`MessageEnvelope`].
//!
//! Both halves live here because they are one contract — the column order below
//! is what the decoder indexes.

use rusqlite::Row;
use uuid::Uuid;

use brenn_lib::messaging::{ChannelScheme, Impetus, MessageEnvelope, Urgency};

use super::{ns_to_utc, parse_rfc3339};

/// The column list as a literal, so [`SELECT_ENVELOPE_BASE`] can be built from
/// it with `concat!`.
macro_rules! envelope_columns {
    () => {
        "m.uuid, m.channel_uuid, m.source, m.sender, m.body, m.urgency, \
         m.reply_to_uuid, m.delivery_deadline, m.deliver_after, m.publish_ts_ns, \
         c.address, rc.address, m.envelope_type, m.impetus, m.content_type, m.transport, \
         m.attachments"
    };
}

/// The FROM clause and its JOINs as a literal, for the same reason.
macro_rules! envelope_from_joins {
    () => {
        "FROM messaging_messages m \
         JOIN messaging_channels c ON c.uuid = m.channel_uuid \
         LEFT JOIN messaging_channels rc ON rc.uuid = m.reply_to_uuid"
    };
}

/// Columns 0–16 (m.uuid .. m.attachments) that [`row_to_envelope`] decodes, in
/// its order; do not reorder or add columns here without updating that
/// decoder and the byte-identity tests. A read that needs further columns
/// selects them after these, then [`ENVELOPE_FROM_JOINS`].
pub(crate) const ENVELOPE_COLUMNS: &str = envelope_columns!();

/// The FROM clause the columns of [`ENVELOPE_COLUMNS`] name: the mandatory
/// channel JOIN and the optional reply-to LEFT JOIN. No trailing space.
pub(crate) const ENVELOPE_FROM_JOINS: &str = envelope_from_joins!();

/// Common SELECT base for `messaging_messages` reads that feed
/// [`row_to_envelope`]: `SELECT` [`ENVELOPE_COLUMNS`] [`ENVELOPE_FROM_JOINS`].
///
/// Terminates with a trailing space **before** any FTS JOIN or `WHERE` clause
/// so callers can append either immediately.
pub const SELECT_ENVELOPE_BASE: &str = concat!(
    "SELECT ",
    envelope_columns!(),
    " ",
    envelope_from_joins!(),
    " "
);

/// Decode one SELECT row (columns 0–16) into a [`MessageEnvelope`].
///
/// Column layout expected:
/// - 0: m.uuid (bytes)  1: m.channel_uuid (bytes, unused)  2: m.source  3: m.sender
/// - 4: m.body (TEXT or BLOB)  5: m.urgency  6: m.reply_to_uuid (bytes, unused)
/// - 7: m.delivery_deadline  8: m.deliver_after  9: m.publish_ts_ns
/// - 10: c.address (channel address string)  11: rc.address (reply_to, nullable)
/// - 12: m.envelope_type  13: m.impetus (nullable)  14: m.content_type (nullable)
/// - 15: m.transport (nullable)  16: m.attachments (nullable)
///
/// A SELECT that carries further columns puts them after column 16.
pub fn row_to_envelope(row: &Row) -> rusqlite::Result<MessageEnvelope> {
    let msg_uuid_bytes: Vec<u8> = row.get(0)?;
    // col 1 (channel_uuid bytes) is selected but not used here — address is in col 10.
    let source: String = row.get(2)?;
    let sender: String = row.get(3)?;
    let urgency_str: String = row.get(5)?;
    // col 6 (reply_to_uuid bytes) is selected but not used here — address is in col 11.
    let delivery_deadline_s: Option<String> = row.get(7)?;
    let deliver_after_s: Option<String> = row.get(8)?;
    let publish_ts_ns: i64 = row.get(9)?;
    let channel: String = row.get(10)?;
    let reply_to: Option<String> = row.get(11)?;
    let envelope_type_str: String = row.get(12)?;
    let impetus_str: Option<String> = row.get(13)?;

    let message_id = Uuid::from_slice(&msg_uuid_bytes)
        .unwrap_or_else(|e| panic!("messaging: query row uuid malformed: {e}"));
    let body = super::read_body(row, 4, 14, message_id)?;
    let transport = super::read_transport(row, 15, message_id)?;
    let attachments = super::read_attachments(row, 16, message_id)?;
    let urgency = Urgency::parse(&urgency_str)
        .unwrap_or_else(|| panic!("messaging: invalid urgency {urgency_str:?}"));
    let delivery_deadline = delivery_deadline_s.and_then(|s| parse_rfc3339(&s));
    let deliver_after = deliver_after_s.and_then(|s| parse_rfc3339(&s));
    let envelope_type = ChannelScheme::parse(&envelope_type_str).unwrap_or_else(|| {
        panic!("messaging: unknown envelope_type {envelope_type_str:?} — host wrote every row")
    });
    let impetus = impetus_str.map(|s| {
        Impetus::parse(&s)
            .unwrap_or_else(|| panic!("messaging: unknown impetus {s:?} — host wrote every row"))
    });

    Ok(MessageEnvelope {
        message_id,
        source,
        channel,
        sender,
        publish_ts: ns_to_utc(publish_ts_ns),
        body,
        reply_to,
        delivery_deadline,
        deliver_after,
        impetus,
        urgency,
        envelope_type,
        transport,
        attachments,
    })
}
