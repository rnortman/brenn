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
//! release clears the column, not when its instant passes. Delivery is decided
//! at the same moment: a parked message holds no push claim, and release mints
//! one per subscriber attached then. Both happen in one transaction, so
//! "released" is one state rather than two half-states that disagree.

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension};
use uuid::Uuid;

use super::super::MessageEnvelope;
use super::super::store::ReleaseTarget;
use super::shared::parse_rfc3339;
use crate::db::format_ts_for_db;

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

/// Columns 0-10 as `row_to_message_envelope` expects them, for the messages on
/// one channel whose `deliver_after` stands in the `cmp` relation to `?2`.
///
/// The two callers want opposite sides of the same instant — still parked
/// (`>`) and come due (`<=`) — and building both from one place is what keeps
/// them from drifting apart into a query that reports future messages as
/// released.
fn deferred_select(cmp: &str) -> String {
    format!(
        "SELECT m.uuid, m.source, m.sender, m.body, m.urgency,
                m.delivery_deadline, m.deliver_after, m.publish_ts_ns,
                c.address, rc.address, m.envelope_type, m.id
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
                    message_id: row.get(11)?,
                    release_at: parse_release(&deliver_after),
                    envelope: super::bus::row_to_message_envelope(row)?,
                })
            },
        )
        .expect("query list_deferred_for_sender");
    rows.map(|r| r.expect("read deferred row")).collect()
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
        "DELETE FROM messaging_pending_pushes WHERE message_id = ?1",
        rusqlite::params![message_id],
    )
    .expect("messaging: delete deferred push rows");
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

/// One message that release moved from parked into retention, with the push
/// rows that became deliverable with it.
#[derive(Debug)]
pub struct ReleasedRow {
    pub message_id: i64,
    /// The retention sequence assigned to this message as release moved it into
    /// retention — a late-released message is the newest retention entry.
    pub retained_seq: i64,
    pub envelope: MessageEnvelope,
    /// Empty when the message was parked with no delivery targets, or when its
    /// targets' rows were already delivered.
    pub push_ids: Vec<i64>,
}

/// What one release pass moved into retention, and the push claims it found
/// stale on the way.
#[derive(Debug)]
pub struct ReleasedBatch {
    pub released: Vec<ReleasedRow>,
    /// Ids of push rows that existed for a released message and were deleted:
    /// claims minted before the message was parked, which the release-time
    /// target set supersedes. The caller drops them from its push windows.
    pub stale_claims: Vec<i64>,
}

/// Release this channel's messages that have come due, in release order,
/// delivering each to `targets` — the subscribers attached now.
///
/// Two writes make a message released. Clearing the message row's
/// `deliver_after` is what makes it retained: retention reads treat a future
/// `deliver_after` as "not here yet", so a released message must stop carrying
/// one. Minting a push claim per target is what makes it deliverable. A parked
/// message holds no claim until this call, so the target set is resolved once,
/// here — a subscriber that attached while the message waited receives it, and
/// one that left is not owed it. Any claim already on a released message
/// predates the park (an edit re-parked a live message) and is deleted for the
/// same reason.
///
/// Every due message is reported, including one that reaches no target at all:
/// it enters retention like any other and carries no push ids.
///
/// Scoped to one channel so a store releases only its own; the sweep that walks
/// every channel is one call of this per store.
pub fn release_due_for_channel(
    conn: &Connection,
    channel_uuid: Uuid,
    now: DateTime<Utc>,
    targets: &[ReleaseTarget],
) -> ReleasedBatch {
    let now_str = format_ts_for_db(now);
    let channel_bytes = channel_uuid.as_bytes().to_vec();
    let tx = conn
        .unchecked_transaction()
        .expect("messaging: begin release_due_for_channel tx");

    // The due messages in release order, awaiting the retention sequences and
    // push ids that complete them. A `ReleasedRow` is built only once both are
    // known, so no consumer can ever see one carrying a stand-in seq.
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
                Ok((row.get(11)?, envelope))
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
    let mut stale_claims: Vec<i64> = Vec::new();
    for (message_id, envelope) in due {
        stale_claims.extend(stale_claims_for_message(&tx, message_id));
        let retained_seq = super::bus::allocate_retained_seq(&tx, channel_uuid);
        tx.execute(
            "UPDATE messaging_messages SET retained_seq = ?2, deliver_after = NULL WHERE id = ?1",
            rusqlite::params![message_id, retained_seq],
        )
        .expect("messaging: release message row");
        let push_ids = mint_release_claims(&tx, message_id, &envelope, targets);
        released.push(ReleasedRow {
            message_id,
            retained_seq,
            envelope,
            push_ids,
        });
    }

    tx.commit()
        .expect("messaging: commit release_due_for_channel");
    ReleasedBatch {
        released,
        stale_claims,
    }
}

/// Delete and report every undelivered claim on a message about to be released.
///
/// A parked message is minted claimless, so a claim here belongs to a live
/// message an edit later re-parked. The release-time target set is the
/// authority on who is owed the message, so the older claim is discarded rather
/// than reused: reusing it would deliver to a subscriber resolved under a
/// different attachment.
fn stale_claims_for_message(tx: &rusqlite::Transaction<'_>, message_id: i64) -> Vec<i64> {
    let mut stmt = tx
        .prepare_cached(
            "DELETE FROM messaging_pending_pushes
             WHERE message_id = ?1 AND delivered_at IS NULL
             RETURNING id",
        )
        .expect("prepare stale_claims_for_message");
    let rows = stmt
        .query_map(rusqlite::params![message_id], |row| row.get::<_, i64>(0))
        .expect("query stale_claims_for_message");
    rows.map(|r| r.expect("read stale claim id")).collect()
}

/// Mint one immediately-deliverable push claim per target for a just-released
/// message, in target order. Wake is decided per message: a target with a wake
/// threshold is woken only by a message whose urgency meets it.
fn mint_release_claims(
    tx: &rusqlite::Transaction<'_>,
    message_id: i64,
    envelope: &MessageEnvelope,
    targets: &[ReleaseTarget],
) -> Vec<i64> {
    let now = format_ts_for_db(Utc::now());
    let deadline = envelope.delivery_deadline.map(format_ts_for_db);
    let mut push_ids = Vec::with_capacity(targets.len());
    let mut stmt = tx
        .prepare_cached(
            "INSERT INTO messaging_pending_pushes
             (message_id, target_subscriber, target_app_slug, eager_wake,
              delivery_deadline, release_after, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6)",
        )
        .expect("prepare mint_release_claims");
    for target in targets {
        let eager_wake: i64 = if target.wakes(envelope.urgency) { 1 } else { 0 };
        stmt.execute(rusqlite::params![
            message_id,
            target.subscriber.as_str(),
            target.app_slug,
            eager_wake,
            deadline,
            now,
        ])
        .expect("messaging: mint release push claim");
        push_ids.push(tx.last_insert_rowid());
    }
    push_ids
}
