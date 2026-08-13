//! Channel-scoped queries over parked (deferred) messages.
//!
//! A durable message is *parked* while its `deliver_after` lies in the future:
//! its row exists, but no retention read, replay, or dispatch may observe it.
//! The existing bus queries work at the whole-database grain (the dispatcher's
//! global release loop); these work at the single-channel grain a
//! `DbStore` owns.
//!
//! The message row's `deliver_after` decides *visibility*: a store's retention
//! read skips a row that still carries one, so a message becomes retained when
//! release clears the column, not when its instant passes. Release is a single
//! transaction that clears the column and allocates the retention sequence
//! together — nobody is named at that moment, and nobody needs to be. Who is
//! owed the message is whichever cursors are behind that sequence when the wake
//! walk next reads them.

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension};
use uuid::Uuid;

use super::shared::parse_rfc3339;
use brenn_db::format_ts_for_db;
use brenn_lib::messaging::MessageEnvelope;

/// A parked message on one channel, as its sender sees it.
#[derive(Debug)]
pub struct DeferredRow {
    pub message_id: i64,
    pub release_at: DateTime<Utc>,
    pub envelope: MessageEnvelope,
}

/// Identity and ownership of one parked message, without its body.
#[derive(Debug)]
pub struct DeferredLookup {
    pub message_id: i64,
    pub sender: String,
    pub release_at: DateTime<Utc>,
}

/// Columns 0-11 as `row_to_message_envelope` expects them, plus `m.id` at 12,
/// for the messages on one channel whose `deliver_after` stands in the `cmp`
/// relation to `?2`.
///
/// The two callers want opposite sides of the same instant — still parked
/// (`>`) and come due (`<=`) — and building both from one place is what keeps
/// them from drifting apart into a query that reports future messages as
/// released.
fn deferred_select(cmp: &str) -> String {
    format!(
        "SELECT m.uuid, m.source, m.sender, m.body, m.urgency,
                m.delivery_deadline, m.deliver_after, m.publish_ts_ns,
                c.address, rc.address, m.envelope_type, m.impetus, m.id
         FROM messaging_messages m
         JOIN messaging_channels c ON c.uuid = m.channel_uuid
         LEFT JOIN messaging_channels rc ON rc.uuid = m.reply_to_uuid
         WHERE m.channel_uuid = ?1
           AND m.deliver_after IS NOT NULL
           AND m.deliver_after {cmp} ?2"
    )
}

fn parse_release(s: &str) -> DateTime<Utc> {
    parse_rfc3339(s).unwrap_or_else(|| panic!("messaging: malformed deliver_after in db: {s:?}"))
}

/// Parked messages on `channel_uuid` published by `sender`, soonest release
/// first.
///
/// The sender filter is the whole authorization story for the callers that use
/// it: a component scoped to its own sender identity cannot name a message it
/// did not publish, because it never sees one.
pub fn list_deferred_for_sender(
    conn: &Connection,
    channel_uuid: Uuid,
    sender: &str,
    now: DateTime<Utc>,
) -> Vec<DeferredRow> {
    let sql = format!(
        "{} AND m.sender = ?3 ORDER BY m.deliver_after ASC, m.id ASC",
        deferred_select(">")
    );
    let mut stmt = conn
        .prepare(&sql)
        .expect("prepare list_deferred_for_sender");
    let rows = stmt
        .query_map(
            rusqlite::params![
                channel_uuid.as_bytes().to_vec(),
                format_ts_for_db(now),
                sender
            ],
            |row| {
                let deliver_after: String = row.get(6)?;
                Ok(DeferredRow {
                    message_id: row.get(12)?,
                    release_at: parse_release(&deliver_after),
                    envelope: super::bus::row_to_message_envelope(row)?,
                })
            },
        )
        .expect("query list_deferred_for_sender");
    rows.map(|r| r.expect("read deferred row")).collect()
}

/// Every sender holding at least one message still parked on `channel_uuid` at
/// `now`, once each, sorted.
///
/// The same `now` boundary [`list_deferred_for_sender`] applies, for the same
/// reason: a matured entry no release pass has taken is out of every view, so a
/// sender holding only those has nothing here either.
pub fn list_deferred_senders(
    conn: &Connection,
    channel_uuid: Uuid,
    now: DateTime<Utc>,
) -> Vec<String> {
    let mut stmt = conn
        .prepare(
            "SELECT DISTINCT sender FROM messaging_messages
             WHERE channel_uuid = ?1
               AND deliver_after IS NOT NULL
               AND deliver_after > ?2
             ORDER BY sender ASC",
        )
        .expect("prepare list_deferred_senders");
    let rows = stmt
        .query_map(
            rusqlite::params![channel_uuid.as_bytes().to_vec(), format_ts_for_db(now)],
            |row| row.get::<_, String>(0),
        )
        .expect("query list_deferred_senders");
    rows.map(|r| r.expect("read deferred sender")).collect()
}

/// How many messages on `channel_uuid` are unreleased, across all senders — the
/// quantity the channel-wide deferred cap bounds.
///
/// Deliberately clock-free: a message that has come due but that no release
/// pass has taken yet still occupies its slot, because it still holds the
/// resources the cap exists to bound. Counting by comparison against a `now`
/// instead would free the slot the instant the time passed, admitting parks a
/// ring-backed channel refuses — the class-shaped divergence the store
/// abstraction exists to remove.
pub fn count_deferred(conn: &Connection, channel_uuid: Uuid) -> u64 {
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_messages
             WHERE channel_uuid = ?1 AND deliver_after IS NOT NULL",
            rusqlite::params![channel_uuid.as_bytes().to_vec()],
            |row| row.get(0),
        )
        .expect("messaging: query count_deferred");
    u64::try_from(count).expect("messaging: negative deferred count")
}

/// The cap a park on `channel_uuid` is refused by, or `None` when the park is
/// admitted.
///
/// Every durable park asks here — the single publish, the WASM flush, the
/// surface batch flush — so one answer bounds them all. Call it on the same
/// connection or transaction guard the insert runs under: the count a park is
/// judged against is then the count it joins.
pub fn deferred_cap_refusal(
    conn: &Connection,
    channel_uuid: Uuid,
    cap: brenn_lib::messaging::config::Depth,
) -> Option<u64> {
    let brenn_lib::messaging::config::Depth::Bounded(cap) = cap else {
        return None;
    };
    (count_deferred(conn, channel_uuid) >= cap).then_some(cap)
}

/// Identity and owner of one parked message, or `None` when it is no longer
/// parked — released, cancelled, or never on this channel.
pub fn lookup_deferred(
    conn: &Connection,
    channel_uuid: Uuid,
    message_uuid: Uuid,
    now: DateTime<Utc>,
) -> Option<DeferredLookup> {
    conn.query_row(
        "SELECT id, sender, deliver_after FROM messaging_messages
         WHERE channel_uuid = ?1 AND uuid = ?2
           AND deliver_after IS NOT NULL AND deliver_after > ?3",
        rusqlite::params![
            channel_uuid.as_bytes().to_vec(),
            message_uuid.as_bytes().to_vec(),
            format_ts_for_db(now)
        ],
        |row| {
            let release: String = row.get(2)?;
            Ok(DeferredLookup {
                message_id: row.get(0)?,
                sender: row.get(1)?,
                release_at: parse_release(&release),
            })
        },
    )
    .optional()
    .expect("messaging: query lookup_deferred")
}

/// Delete a parked message and its undelivered push rows.
///
/// Cancelling a deferred message must erase it, not merely unschedule it: the
/// message row's `deliver_after` is what hides it from retention reads, so a
/// row left behind would surface as retained ambience the moment its time
/// passed — delivering the thing the sender cancelled.
///
/// Returns `false` if the message is no longer parked, which is the benign
/// race between the view a caller acted on and this call.
pub fn delete_deferred(
    conn: &Connection,
    channel_uuid: Uuid,
    message_id: i64,
    now: DateTime<Utc>,
) -> bool {
    let tx = conn
        .unchecked_transaction()
        .expect("messaging: begin delete_deferred tx");
    let now_str = format_ts_for_db(now);
    let still_parked: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM messaging_messages
             WHERE id = ?1 AND channel_uuid = ?2
               AND deliver_after IS NOT NULL AND deliver_after > ?3",
            rusqlite::params![message_id, channel_uuid.as_bytes().to_vec(), now_str],
            |row| row.get(0),
        )
        .expect("messaging: recheck parked in delete_deferred");
    if still_parked == 0 {
        tx.rollback().expect("messaging: rollback delete_deferred");
        return false;
    }
    tx.execute(
        "DELETE FROM messaging_messages WHERE id = ?1",
        rusqlite::params![message_id],
    )
    .expect("messaging: delete deferred message row");
    tx.commit().expect("messaging: commit delete_deferred");
    true
}

/// Replace a parked message's body, release time, or both.
///
/// Distinct from the general message edit path: a parked message has no
/// delivered copies by definition, so there is no delivered-check to fail and
/// no urgency recomputation to do. Returns `false` if the message is no longer
/// parked — the same benign race as [`delete_deferred`].
pub fn edit_deferred(
    conn: &Connection,
    channel_uuid: Uuid,
    message_id: i64,
    body: Option<&str>,
    release_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> bool {
    let tx = conn
        .unchecked_transaction()
        .expect("messaging: begin edit_deferred tx");
    let now_str = format_ts_for_db(now);
    let still_parked: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM messaging_messages
             WHERE id = ?1 AND channel_uuid = ?2
               AND deliver_after IS NOT NULL AND deliver_after > ?3",
            rusqlite::params![message_id, channel_uuid.as_bytes().to_vec(), now_str],
            |row| row.get(0),
        )
        .expect("messaging: recheck parked in edit_deferred");
    if still_parked == 0 {
        tx.rollback().expect("messaging: rollback edit_deferred");
        return false;
    }
    if let Some(body) = body {
        tx.execute(
            "UPDATE messaging_messages SET body = ?1 WHERE id = ?2",
            rusqlite::params![body, message_id],
        )
        .expect("messaging: update deferred body");
    }
    if let Some(release_at) = release_at {
        let release_str = format_ts_for_db(release_at);
        tx.execute(
            "UPDATE messaging_messages SET deliver_after = ?1 WHERE id = ?2",
            rusqlite::params![release_str, message_id],
        )
        .expect("messaging: update deferred release time");
    }
    tx.commit().expect("messaging: commit edit_deferred");
    true
}

/// Soonest release time among this channel's unreleased messages.
///
/// A time already in the past is reported like any other: a release loop that
/// waits on this deadline must be told about work it has not yet done, or an
/// entry that came due between its release pass and its next sleep is invisible
/// to both and stays parked until something unrelated disturbs the channel. On
/// a single-publisher timer channel, nothing unrelated ever comes.
pub fn earliest_channel_release(conn: &Connection, channel_uuid: Uuid) -> Option<DateTime<Utc>> {
    let result: Option<String> = conn
        .query_row(
            "SELECT MIN(deliver_after) FROM messaging_messages
             WHERE channel_uuid = ?1 AND deliver_after IS NOT NULL",
            rusqlite::params![channel_uuid.as_bytes().to_vec()],
            |row| row.get(0),
        )
        .expect("messaging: query earliest_channel_release");
    result.as_deref().map(parse_release)
}

/// One message that release moved from parked into retention.
#[derive(Debug)]
pub struct ReleasedRow {
    pub message_id: i64,
    /// The retention sequence assigned to this message as release moved it into
    /// retention — a late-released message is the newest retention entry.
    pub retained_seq: i64,
    pub envelope: MessageEnvelope,
}

/// Release this channel's messages that have come due, in release order.
///
/// Clearing the message row's `deliver_after` is what makes it retained:
/// retention reads treat a future `deliver_after` as "not here yet", so a
/// released message must stop carrying one. Who it reaches is nobody's record to
/// write — every subscriber reads the channel from its own position, and the
/// release is what puts the message where those positions can see it.
///
/// Scoped to one channel so a store releases only its own; the sweep that walks
/// every channel is one call of this per store.
pub fn release_due_for_channel(
    conn: &Connection,
    channel_uuid: Uuid,
    now: DateTime<Utc>,
) -> Vec<ReleasedRow> {
    let now_str = format_ts_for_db(now);
    let channel_bytes = channel_uuid.as_bytes().to_vec();
    let tx = conn
        .unchecked_transaction()
        .expect("messaging: begin release_due_for_channel tx");

    // The due messages in release order, awaiting the retention sequences that
    // complete them. A `ReleasedRow` is built only once its seq is known, so no
    // consumer can ever see one carrying a stand-in.
    let mut due: Vec<(i64, MessageEnvelope)> = Vec::new();
    {
        let sql = format!(
            "{} ORDER BY m.deliver_after ASC, m.id ASC",
            deferred_select("<=")
        );
        let mut stmt = tx.prepare(&sql).expect("prepare release_due_for_channel");
        let rows = stmt
            .query_map(rusqlite::params![channel_bytes, now_str], |row| {
                let mut envelope = super::bus::row_to_message_envelope(row)?;
                // The row still carries the release time this call is about to
                // clear; a released message is not a deferred one.
                envelope.deliver_after = None;
                Ok((row.get(12)?, envelope))
            })
            .expect("query release_due_for_channel");
        for r in rows {
            due.push(r.expect("read released message row"));
        }
    }

    // `due` is in release order (release_at asc, id tie); allocating retention
    // sequences in that order makes a late-released row the newest retention
    // entry.
    let mut released: Vec<ReleasedRow> = Vec::with_capacity(due.len());
    for (message_id, envelope) in due {
        let retained_seq = super::bus::allocate_retained_seq(&tx, channel_uuid);
        tx.execute(
            "UPDATE messaging_messages SET retained_seq = ?2, deliver_after = NULL WHERE id = ?1",
            rusqlite::params![message_id, retained_seq],
        )
        .expect("messaging: release message row");
        released.push(ReleasedRow {
            message_id,
            retained_seq,
            envelope,
        });
    }

    tx.commit()
        .expect("messaging: commit release_due_for_channel");
    released
}
