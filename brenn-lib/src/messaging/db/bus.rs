use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension};
use uuid::Uuid;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt as _;

use super::super::{ChannelScheme, IngressOrBus, MessageEnvelope, ParticipantId, Urgency};
use super::shared::{ns_to_utc, parse_rfc3339};
use super::types::PendingPushRow;
use crate::db::format_ts_for_db;

// ---------------------------------------------------------------------------
// Message + pending-push insert
// ---------------------------------------------------------------------------

/// One pending-push row to insert alongside a message.
#[derive(Debug)]
pub struct PendingPushInsert {
    pub target_subscriber: ParticipantId,
    pub target_app_slug: String,
    /// Resolved per-subscriber wake decision: `true` iff this subscriber should
    /// be eagerly woken when this push row is available. Computed at insert time
    /// from `WakeMin::wakes(urgency)`. Stored as `eager_wake INTEGER (0 or 1)`
    /// in `messaging_pending_pushes`.
    pub eager_wake: bool,
    /// `Some(deliver_after)` when the message is suppressed until that time;
    /// `None` otherwise.
    pub release_after: Option<DateTime<Utc>>,
    pub delivery_deadline: Option<DateTime<Utc>>,
}

/// Inserted message metadata returned to the caller.
#[derive(Debug)]
pub struct InsertedMessage {
    pub id: i64,
    pub uuid: Uuid,
    pub publish_ts_ns: i64,
    /// The retention sequence assigned to this message, or `None` when it was
    /// parked (a parked message holds no retention position until it releases).
    pub retained_seq: Option<i64>,
    /// Row ids of the `messaging_pending_pushes` rows inserted, in the same
    /// order as the `pending_pushes` slice passed to `insert_message_with_pushes`.
    /// Used by the publish path to populate the in-memory push-window deques.
    pub push_ids: Vec<i64>,
}

/// Insert a message row plus all its pending-push rows in a single
/// transaction. The caller has already validated channel/sender/budget.
#[allow(clippy::too_many_arguments)]
pub fn insert_message_with_pushes(
    conn: &Connection,
    channel_uuid: Uuid,
    source: &str,
    sender: &str,
    body: &str,
    urgency: Urgency,
    envelope_type: ChannelScheme,
    reply_to_uuid: Option<Uuid>,
    delivery_deadline: Option<DateTime<Utc>>,
    deliver_after: Option<DateTime<Utc>>,
    publish_ts_ns: i64,
    pending_pushes: &[PendingPushInsert],
) -> InsertedMessage {
    let tx = conn.unchecked_transaction().expect("messaging: begin tx");
    let result = insert_message_with_pushes_in_tx(
        &tx,
        channel_uuid,
        source,
        sender,
        body,
        urgency,
        envelope_type,
        reply_to_uuid,
        delivery_deadline,
        deliver_after,
        publish_ts_ns,
        pending_pushes,
    );
    tx.commit().expect("messaging: commit tx");
    result
}

/// Insert a message row plus all its pending-push rows under a
/// caller-owned `Transaction`. The caller is responsible for BEGIN/COMMIT
/// (or rollback on panic via the `Transaction` Drop guard).
///
/// Used by `publish_from_wasm` to batch multiple messages into one outer
/// transaction. All other callers use `insert_message_with_pushes`.
#[allow(clippy::too_many_arguments)]
pub fn insert_message_with_pushes_in_tx(
    tx: &rusqlite::Transaction<'_>,
    channel_uuid: Uuid,
    source: &str,
    sender: &str,
    body: &str,
    urgency: Urgency,
    envelope_type: ChannelScheme,
    reply_to_uuid: Option<Uuid>,
    delivery_deadline: Option<DateTime<Utc>>,
    deliver_after: Option<DateTime<Utc>>,
    publish_ts_ns: i64,
    pending_pushes: &[PendingPushInsert],
) -> InsertedMessage {
    let now = format_ts_for_db(Utc::now());
    let uuid = Uuid::new_v4();
    let uuid_bytes = uuid.as_bytes().to_vec();
    let channel_bytes = channel_uuid.as_bytes().to_vec();
    let reply_to_bytes = reply_to_uuid.map(|u| u.as_bytes().to_vec());
    let dd = delivery_deadline.map(format_ts_for_db);
    let da = deliver_after.map(format_ts_for_db);

    // A parked row holds no retention position, and every read that serves an
    // owed claim needs one. A claim written against it must therefore be held
    // (`release_after` set) rather than immediately deliverable: an owed,
    // unparked claim on a positionless message is a row no consumer can be
    // served, and the reads that meet it panic.
    assert!(
        deliver_after.is_none() || pending_pushes.iter().all(|p| p.release_after.is_some()),
        "messaging: deliverable push claims written for a parked message on channel \
         {channel_uuid}"
    );
    let retained_seq = match deliver_after {
        None => Some(allocate_retained_seq(tx, channel_uuid)),
        Some(_) => None,
    };

    tx.execute(
        "INSERT INTO messaging_messages
         (uuid, channel_uuid, source, sender, body, urgency, envelope_type,
          reply_to_uuid, delivery_deadline, deliver_after, publish_ts_ns, created_at,
          retained_seq)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        rusqlite::params![
            uuid_bytes,
            channel_bytes,
            source,
            sender,
            body,
            urgency.as_str(),
            envelope_type.as_str(),
            reply_to_bytes,
            dd,
            da,
            publish_ts_ns,
            now,
            retained_seq,
        ],
    )
    .expect("messaging: insert message");
    let message_id = tx.last_insert_rowid();

    let mut push_ids = Vec::with_capacity(pending_pushes.len());
    for push in pending_pushes {
        let release = push.release_after.map(format_ts_for_db);
        let deadline = push.delivery_deadline.map(format_ts_for_db);
        let eager_wake_int: i64 = if push.eager_wake { 1 } else { 0 };
        tx.execute(
            "INSERT INTO messaging_pending_pushes
             (message_id, target_subscriber, target_app_slug, eager_wake,
              delivery_deadline, release_after, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![
                message_id,
                push.target_subscriber.as_str(),
                push.target_app_slug,
                eager_wake_int,
                deadline,
                release,
                now,
            ],
        )
        .expect("messaging: insert pending push");
        push_ids.push(tx.last_insert_rowid());
    }

    InsertedMessage {
        id: message_id,
        uuid,
        publish_ts_ns,
        retained_seq,
        push_ids,
    }
}

/// Allocate the next dense retention sequence for a channel.
///
/// The channel row's `last_retained_seq` is the allocator and the persisted
/// high-water: bump it and return the new value. The DB is one mutex-guarded
/// connection, so allocation is serial by construction; the partial unique index
/// on `(channel_uuid, retained_seq)` is the better-dead-than-wrong backstop.
///
/// Takes a `&Connection` so an in-transaction caller (`&Transaction` derefs) and
/// an autocommit one share the one allocator statement.
pub fn allocate_retained_seq(conn: &Connection, channel_uuid: Uuid) -> i64 {
    allocate_retained_seqs(conn, channel_uuid, 1)
}

/// Allocate a block of `count` consecutive retention sequences, returning the
/// highest — the block is `highest - count + 1 ..= highest`.
///
/// One statement per block, for callers numbering a known set of messages at
/// once (the migration backfill). `count` of zero is a caller bug: there is no
/// sequence to return.
///
/// The channel row always exists — `messaging_messages.channel_uuid` is a
/// foreign key into `messaging_channels`, enforced on every DB this runs on — so
/// a missing row is a broken invariant and panics rather than inventing a seq.
pub fn allocate_retained_seqs(conn: &Connection, channel_uuid: Uuid, count: u64) -> i64 {
    assert!(count > 0, "messaging: allocate_retained_seqs: empty block");
    let count = i64::try_from(count).expect("messaging: retained seq block out of range");
    conn.query_row(
        "UPDATE messaging_channels SET last_retained_seq = last_retained_seq + ?2
         WHERE uuid = ?1 RETURNING last_retained_seq",
        rusqlite::params![channel_uuid.as_bytes().to_vec(), count],
        |row| row.get(0),
    )
    .unwrap_or_else(|e| {
        panic!("messaging: allocate_retained_seqs: channel {channel_uuid} has no row: {e}")
    })
}

/// Assign dense retention sequences to `message_ids` on one channel, in the
/// order given — the caller's order *is* the retention order.
pub fn assign_retained_seqs(conn: &Connection, channel_uuid: Uuid, message_ids: &[i64]) {
    if message_ids.is_empty() {
        return;
    }
    // The block ends at the returned high-water, so the first id takes the
    // lowest sequence in it.
    let highest = allocate_retained_seqs(conn, channel_uuid, message_ids.len() as u64);
    let mut seq = highest - message_ids.len() as i64;
    let mut assign = conn
        .prepare_cached("UPDATE messaging_messages SET retained_seq = ?2 WHERE id = ?1")
        .expect("messaging: prepare retained_seq assignment");
    for &id in message_ids {
        seq += 1;
        assign
            .execute(rusqlite::params![id, seq])
            .expect("messaging: assign retained_seq");
    }
}

/// Seed `messaging_pending_pushes` rows for a newly-created subscriber from a
/// set of already-retained messages, delivering the channel's retained tail as
/// NEW when a durable queue comes into existence (attach is a delivery point).
///
/// Each row is `eager_wake` so the substrate wakes the subscriber on the seeded
/// tail, and carries no deadline or release — the messages are already retained
/// and immediately observable. `message_ids` is oldest-first (the tail order).
///
/// The caller is responsible for only ever calling this for a genuinely new
/// queue (no pre-existing subscription registration): there is no
/// `(message_id, target_subscriber)` uniqueness constraint, so a second call for
/// the same subscriber would duplicate rows.
pub fn seed_pending_pushes_for_messages(
    conn: &Connection,
    message_ids: &[i64],
    subscriber: &ParticipantId,
    app_slug: &str,
) {
    let now = format_ts_for_db(Utc::now());
    for &message_id in message_ids {
        conn.execute(
            "INSERT INTO messaging_pending_pushes
             (message_id, target_subscriber, target_app_slug, eager_wake,
              delivery_deadline, release_after, created_at)
             VALUES (?1, ?2, ?3, 1, NULL, NULL, ?4)",
            rusqlite::params![message_id, subscriber.as_str(), app_slug, now],
        )
        .expect("messaging: seed pending push for new subscriber");
    }
}

// ---------------------------------------------------------------------------
// Bus GC
// ---------------------------------------------------------------------------

/// Evict bus message bodies past the channel's reap frontier,
/// handling both `drop` and `archive` sinks in a single transaction.
///
/// **Scope fence:** only rows with the given `channel_uuid` and
/// `envelope_type != 'ingress'` are touched. `ingress` rows have
/// `channel_uuid = NULL` and are GC'd by a separate path; the predicate
/// here matches all channel-associated transport types (`brenn`, `webhook`,
/// future `mqtt`, etc.) without needing to enumerate them.
///
/// Steps (all in one `unchecked_transaction` per design §2.5):
/// 1. Count rows for the channel; if `<= frontier`, returns `(0, 0)` immediately.
/// 2. For `archive` sink: SELECT eligible bodies (NOT IN top-frontier set) and
///    write each as a JSONL line to `archive_path`.
/// 3. Delete push rows for eligible messages first (satisfies FK constraint),
///    then delete message rows using the same NOT IN predicate (FTS triggers
///    fire on each row DELETE, keeping the FTS index consistent).
///
/// Returns `(messages_evicted, push_rows_retired)`.
///
/// # Panics
///
/// Panics (fail-fast) on any SQL error or (for `archive`) any file I/O error.
/// The body is **not** deleted if archiving fails — preserves no-data-loss.
pub fn bus_gc_evict_channel(
    conn: &Connection,
    channel_uuid: Uuid,
    channel_address: &str,
    channel_envelope_type: super::super::ChannelScheme,
    frontier: u64,
    sink: super::super::config::Sink,
    archive_path: Option<&std::path::Path>,
) -> (usize, usize) {
    use super::super::config::Sink;
    use std::io::Write as _;

    let channel_uuid_bytes = channel_uuid.as_bytes().to_vec();

    // Validate archive_path before opening the transaction so a misconfigured
    // sink panics with a clear message before any transaction state is created.
    // Config validation (build_channel_entries) guarantees this is Some when
    // sink == Archive; this assertion catches callers that bypass that path
    // (tests, future code) with an actionable message.
    if sink == Sink::Archive && archive_path.is_none() {
        panic!(
            "bus_gc_evict_channel: sink=Archive on channel {channel_address:?} \
             but archive_path is None — call set_archive_path at config load"
        );
    }

    // Open the transaction first, then count inside it so the guard and the
    // subsequent NOT-IN subqueries see a consistent snapshot. Previously the
    // COUNT was outside the transaction; under the Mutex<Connection> discipline
    // this was not a data-corruption risk (only one caller can hold conn at a
    // time), but the logical separation between the guard read and the transaction
    // was a latent TOCTOU if lock discipline ever changes.
    let tx = conn
        .unchecked_transaction()
        .expect("bus_gc_evict_channel: begin transaction");

    // Guard: if the channel has <= frontier retained rows, nothing is eligible.
    // Checked inside the transaction for snapshot consistency. Parked rows
    // (`retained_seq IS NULL`) are excluded here and from every eligible-set
    // predicate below — they have no retention position until release.
    let total_rows: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM messaging_messages
             WHERE channel_uuid = ?1 AND envelope_type != 'ingress'
               AND retained_seq IS NOT NULL",
            rusqlite::params![channel_uuid_bytes],
            |row| row.get(0),
        )
        .expect("bus_gc_evict_channel: count rows");
    if total_rows <= frontier as i64 {
        return (0, 0);
    }

    // Eligible = all retained rows except the `frontier` most-recent by
    // `retained_seq` (dense per-channel retention order, unique via the partial
    // index, so no tiebreaker is needed). Keying on `retained_seq` rather than
    // publish timestamp makes eviction oldest-retention-first, so a late-released
    // message (newest retention entry, old publish_ts) is never reaped.

    // Step 2 (archive): load eligible bodies and write to JSONL before deleting.
    if sink == Sink::Archive {
        // SAFETY: validated above before the transaction was opened.
        let path = archive_path
            .expect("bus_gc_evict_channel: archive_path is None despite validation (unreachable)");

        // `retained_seq` is dense and unique per channel (partial unique index),
        // so it is a total order — the keep-set is identical across all three
        // sub-query evaluations in this transaction (archive SELECT, push DELETE,
        // message DELETE) without any tiebreaker.
        let mut stmt = tx
            .prepare(
                "SELECT m.uuid, m.source, m.sender, m.body, m.urgency,
                        m.delivery_deadline, m.deliver_after, m.publish_ts_ns,
                        rc.address
                 FROM messaging_messages m
                 LEFT JOIN messaging_channels rc ON rc.uuid = m.reply_to_uuid
                 WHERE m.channel_uuid = ?1
                   AND m.envelope_type != 'ingress'
                   AND m.retained_seq IS NOT NULL
                   AND m.id NOT IN (
                       SELECT id FROM messaging_messages
                       WHERE channel_uuid = ?1 AND envelope_type != 'ingress'
                         AND retained_seq IS NOT NULL
                       ORDER BY retained_seq DESC
                       LIMIT ?2
                   )
                   AND m.id NOT IN (
                       SELECT message_id FROM messaging_pending_pushes
                       WHERE confirm_pending = 1
                   )
                 ORDER BY m.retained_seq ASC",
            )
            .expect("bus_gc_evict_channel: archive SELECT prepare");

        let envelopes: Vec<MessageEnvelope> = stmt
            .query_map(
                rusqlite::params![channel_uuid_bytes, frontier as i64],
                |row| {
                    let msg_uuid_bytes: Vec<u8> = row.get(0)?;
                    let source: String = row.get(1)?;
                    let sender: String = row.get(2)?;
                    let body: String = row.get(3)?;
                    let urgency_str: String = row.get(4)?;
                    let delivery_deadline_s: Option<String> = row.get(5)?;
                    let deliver_after_s: Option<String> = row.get(6)?;
                    let publish_ts_ns: i64 = row.get(7)?;
                    let reply_to: Option<String> = row.get(8)?;
                    Ok((
                        msg_uuid_bytes,
                        source,
                        sender,
                        body,
                        urgency_str,
                        delivery_deadline_s,
                        deliver_after_s,
                        publish_ts_ns,
                        reply_to,
                    ))
                },
            )
            .expect("bus_gc_evict_channel: archive SELECT query")
            .map(|r| {
                let (
                    msg_uuid_bytes,
                    source,
                    sender,
                    body,
                    urgency_str,
                    delivery_deadline_s,
                    deliver_after_s,
                    publish_ts_ns,
                    reply_to,
                ) = r.expect("bus_gc_evict_channel: archive SELECT row");
                let message_id = Uuid::from_slice(&msg_uuid_bytes)
                    .expect("bus_gc_evict_channel: malformed uuid in db");
                let urgency = Urgency::parse(&urgency_str).unwrap_or_else(|| {
                    panic!("bus_gc_evict_channel: invalid urgency: {urgency_str:?}")
                });
                let delivery_deadline = delivery_deadline_s.map(|s| {
                    parse_rfc3339(&s)
                        .unwrap_or_else(|| panic!("bus_gc_evict_channel: invalid rfc3339: {s:?}"))
                });
                let deliver_after = deliver_after_s.map(|s| {
                    parse_rfc3339(&s)
                        .unwrap_or_else(|| panic!("bus_gc_evict_channel: invalid rfc3339: {s:?}"))
                });
                MessageEnvelope {
                    message_id,
                    source,
                    channel: channel_address.to_string(),
                    sender,
                    publish_ts: ns_to_utc(publish_ts_ns),
                    body,
                    reply_to,
                    delivery_deadline,
                    deliver_after,
                    urgency,
                    envelope_type: channel_envelope_type,
                }
            })
            .collect();
        drop(stmt);

        if !envelopes.is_empty() {
            // Write to JSONL. Fail loudly on I/O error — body is NOT deleted.
            // mode(0o600): owner-only read/write, matching the VAPID key precedent
            // (pwa_push/vapid.rs:166). Archive bodies contain full MessageEnvelopes
            // (personal data); they must not be world/group-readable.
            // Note: on crash between write and SQLite commit, rows may appear twice
            // on the next GC pass (idempotent archiving; deduplication is out of scope).
            let mut opts = std::fs::OpenOptions::new();
            opts.create(true).append(true);
            #[cfg(unix)]
            opts.mode(0o600);
            let mut file = opts
                .open(path)
                .unwrap_or_else(|e| panic!("bus_gc_evict_channel: open archive {path:?}: {e}"));
            for envelope in &envelopes {
                let line = serde_json::to_string(envelope)
                    .expect("bus_gc_evict_channel: serialize envelope");
                writeln!(file, "{line}").unwrap_or_else(|e| {
                    panic!("bus_gc_evict_channel: write archive {path:?}: {e}")
                });
            }
            file.flush()
                .unwrap_or_else(|e| panic!("bus_gc_evict_channel: flush archive {path:?}: {e}"));
        }
    }

    // Step 3a: delete push rows for eligible messages FIRST (before message rows),
    // to satisfy the FK constraint on messaging_pending_pushes.message_id.
    // Same eligible set as the archive SELECT (retained_seq total order).
    // Tentative (`confirm_pending = 1`) push rows are excluded: they carry
    // below-water recovery evidence that the next resume's reconcile depends on.
    // Their message row is kept alongside them (the message DELETE below applies
    // the same exclusion) so the FK stays satisfied; both rejoin the reapable
    // universe once the flag clears at reconcile.
    let push_rows_retired = tx
        .execute(
            "DELETE FROM messaging_pending_pushes
             WHERE confirm_pending = 0
               AND message_id IN (
                 SELECT id FROM messaging_messages
                 WHERE channel_uuid = ?1
                   AND envelope_type != 'ingress'
                   AND retained_seq IS NOT NULL
                   AND id NOT IN (
                       SELECT id FROM messaging_messages
                       WHERE channel_uuid = ?1 AND envelope_type != 'ingress'
                         AND retained_seq IS NOT NULL
                       ORDER BY retained_seq DESC
                       LIMIT ?2
                   )
             )",
            rusqlite::params![channel_uuid_bytes, frontier as i64],
        )
        .expect("bus_gc_evict_channel: DELETE push rows for evicted messages");

    // Step 3b: delete eligible message rows (FTS trigger fires on each DELETE).
    // Snapshot is stable under the SQLite mutex. Parked rows (retained_seq NULL)
    // are excluded — a long-parked message is never reaped before it releases.
    let messages_deleted = tx
        .execute(
            "DELETE FROM messaging_messages
             WHERE channel_uuid = ?1
               AND envelope_type != 'ingress'
               AND retained_seq IS NOT NULL
               AND id NOT IN (
                   SELECT id FROM messaging_messages
                   WHERE channel_uuid = ?1 AND envelope_type != 'ingress'
                     AND retained_seq IS NOT NULL
                   ORDER BY retained_seq DESC
                   LIMIT ?2
               )
               AND id NOT IN (
                   SELECT message_id FROM messaging_pending_pushes
                   WHERE confirm_pending = 1
               )",
            rusqlite::params![channel_uuid_bytes, frontier as i64],
        )
        .expect("bus_gc_evict_channel: DELETE messages");

    tx.commit().expect("bus_gc_evict_channel: commit");
    (messages_deleted, push_rows_retired)
}

/// Backstop push-claim retirement for bounded-`push_depth` bus subscribers.
///
/// For each `(channel_uuid, app_slug)` subscription with a bounded `push_depth`
/// of `n`, deletes any `messaging_pending_pushes` rows beyond the `n` most-recent
/// by `retained_seq` (ordered via JOIN to `messaging_messages`). This is the
/// GC backstop — the primary retirement happens on the publish hot path
/// when the in-memory deque overflows; this catches anything the hot path missed.
///
/// **Scope fence:** only rows with `envelope_type != 'ingress'` are touched
/// (JOIN filters by `m.envelope_type != 'ingress'`). `ingress` rows have
/// `channel_uuid = NULL` and are never associated with a channel subscription;
/// the predicate here handles all channel-associated transport types.
///
/// Returns the total number of push rows deleted across all subscriptions.
///
/// # Panics
///
/// Panics on any SQL error (fail-fast per CLAUDE.md).
pub fn bus_gc_retire_pushes(
    conn: &Connection,
    channel_uuid: Uuid,
    app_slug: &str,
    push_depth: u64,
) -> usize {
    if push_depth == 0 {
        // Pull-only: no push rows ever created; nothing to retire.
        return 0;
    }

    let channel_uuid_bytes = channel_uuid.as_bytes().to_vec();

    // Keep the push_depth most-recent rows by `retained_seq` and delete the
    // rest. A late-released message has the oldest publish timestamp and the
    // newest retention position, so ranking on `publish_ts_ns` would retire its
    // freshly-released claim in preference to genuinely older ones. Every row in
    // this universe is released (`release_after IS NULL`), so every one carries
    // a `retained_seq`.
    //
    // Only count/retire rows with `release_after IS NULL` (non-parked rows).
    // Parked rows (deliver_after pushes awaiting release) are excluded from the
    // hot-path push-window deque and must be equally excluded here so the two
    // reapers operate on the same row universe.
    //
    // Tentative rows (`confirm_pending = 1`) are excluded the same way parked rows
    // are: they are a below-water delivery whose receipt is not yet acknowledged, so
    // retirement must not erase the recovery evidence the reconcile at the next
    // resume depends on. They clear back to 0 (confirmed) or NULL delivered_at
    // (unclaimed) at reconcile, rejoining the reapable universe then.
    conn.execute(
        "DELETE FROM messaging_pending_pushes
         WHERE id IN (
             SELECT pp.id
             FROM messaging_pending_pushes pp
             JOIN messaging_messages m ON m.id = pp.message_id
             WHERE pp.target_app_slug = ?1
               AND m.channel_uuid = ?2
               AND m.envelope_type != 'ingress'
               AND pp.release_after IS NULL
               AND pp.confirm_pending = 0
               AND pp.id NOT IN (
                   SELECT pp2.id
                   FROM messaging_pending_pushes pp2
                   JOIN messaging_messages m2 ON m2.id = pp2.message_id
                   WHERE pp2.target_app_slug = ?1
                     AND m2.channel_uuid = ?2
                     AND m2.envelope_type != 'ingress'
                     AND pp2.release_after IS NULL
                   ORDER BY m2.retained_seq DESC
                   LIMIT ?3
               )
         )",
        rusqlite::params![app_slug, channel_uuid_bytes, push_depth as i64],
    )
    .expect("bus_gc_retire_pushes: DELETE")
}

/// Delete a single `messaging_pending_pushes` row by its primary key.
///
/// Used by the publish hot path to retire the oldest push-claim when a
/// bounded-`push_depth` subscriber's window overflows (design §2.4). This is a
/// point delete (no scan); idempotent (deleting an already-deleted id affects
/// zero rows — the GC backstop may have already removed it).
///
/// # Panics
///
/// Panics on any SQL error (fail-fast per CLAUDE.md).
pub fn delete_pending_push_by_id(conn: &Connection, push_id: i64) {
    conn.execute(
        "DELETE FROM messaging_pending_pushes WHERE id = ?1",
        rusqlite::params![push_id],
    )
    .expect("delete_pending_push_by_id: DELETE");
}

/// Delete every `messaging_pending_pushes` row owned by `subscriber` on the
/// channel `channel_uuid` — undelivered, tentative, and parked alike. Returns
/// the number of rows removed.
///
/// # Panics
///
/// Panics on any SQL error.
pub fn delete_pushes_for_subscriber(
    conn: &Connection,
    channel_uuid: Uuid,
    subscriber: &ParticipantId,
) -> usize {
    conn.execute(
        "DELETE FROM messaging_pending_pushes
         WHERE target_subscriber = ?1
           AND message_id IN (
               SELECT id FROM messaging_messages WHERE channel_uuid = ?2
           )",
        rusqlite::params![subscriber.as_str(), channel_uuid.as_bytes().to_vec()],
    )
    .expect("delete_pushes_for_subscriber: DELETE")
}

// ---------------------------------------------------------------------------
// Push-window seed queries
// ---------------------------------------------------------------------------

/// Load all undelivered, non-parked push ids for a `(channel, subscriber)`
/// key, excluding a specific push id. Used to seed the in-memory push window
/// on first touch after boot.
///
/// `exclude_push_id`: the push id that was just inserted for the current
/// publish; excluded from the seed so the seed reflects pre-existing rows
/// rather than including the newly-inserted row (which is added separately
/// by `record_push_and_check_overflow` after the seed).
///
/// Returns push ids ordered oldest-first (`publish_ts_ns ASC, pp.id ASC`);
/// the caller `push_back`s each id and then truncates the deque front to
/// `push_depth`. Only `kind='brenn'` rows are returned (parked rows and
/// ingress rows are excluded).
///
/// # Panics
///
/// Panics on any SQL error (fail-fast per CLAUDE.md).
pub fn load_push_window(
    conn: &Connection,
    channel_uuid: Uuid,
    app_slug: &str,
    subscriber: &ParticipantId,
    exclude_push_id: i64,
) -> Vec<i64> {
    let channel_uuid_bytes = channel_uuid.as_bytes().to_vec();
    let mut stmt = conn
        .prepare(
            "SELECT pp.id
             FROM messaging_pending_pushes pp
             JOIN messaging_messages m ON m.id = pp.message_id
             WHERE m.channel_uuid = ?1
               AND pp.target_app_slug = ?2
               AND pp.target_subscriber = ?3
               AND pp.id != ?4
               AND pp.delivered_at IS NULL
               AND pp.release_after IS NULL
               AND m.envelope_type != 'ingress'
             ORDER BY m.publish_ts_ns ASC, pp.id ASC",
        )
        .expect("prepare load_push_window");
    let rows = stmt
        .query_map(
            rusqlite::params![
                channel_uuid_bytes,
                app_slug,
                subscriber.as_str(),
                exclude_push_id
            ],
            |row| row.get::<_, i64>(0),
        )
        .expect("query load_push_window");
    rows.map(|r| r.expect("read push window id")).collect()
}

/// Minimal row returned by `load_released_push_window_rows`: enough to
/// register a just-released parked push into its push window.
#[derive(Debug)]
pub struct ReleasedPushRow {
    pub push_id: i64,
    pub channel_address: String,
    pub channel_uuid: Uuid,
    pub target_app_slug: String,
    pub target_subscriber: ParticipantId,
}

/// Build a bare `?`-placeholder string for an IN clause of length `n`.
///
/// Returns `"?,?,?"` for `n=3`. Panics if `n == 0` — a caller with an empty
/// set must shape the clause itself rather than emit an empty `IN ()`.
fn build_bare_in_placeholders(n: usize) -> String {
    assert!(n > 0, "build_bare_in_placeholders: n must be > 0");
    std::iter::repeat_n("?", n).collect::<Vec<_>>().join(",")
}

/// Load the minimal columns needed to register released parked rows into their
/// push windows. Called by `register_released_pushes` immediately after a
/// release pass clears `release_after`.
///
/// Released parked rows are always `kind='brenn'` (parked rows are created only
/// on the bus publish path); the channel address is therefore always present.
///
/// # Panics
///
/// Panics on any SQL error.
pub fn load_released_push_window_rows(conn: &Connection, ids: &[i64]) -> Vec<ReleasedPushRow> {
    if ids.is_empty() {
        return vec![];
    }
    let sql = format!(
        "SELECT pp.id, c.address, c.uuid, pp.target_app_slug, pp.target_subscriber
         FROM messaging_pending_pushes pp
         JOIN messaging_messages m ON m.id = pp.message_id
         JOIN messaging_channels c ON c.uuid = m.channel_uuid
         WHERE pp.id IN ({}) AND pp.delivered_at IS NULL AND m.envelope_type != 'ingress'",
        build_bare_in_placeholders(ids.len()),
    );
    let ids_len = ids.len();
    let mut stmt = conn.prepare(&sql).unwrap_or_else(|e| {
        panic!("prepare load_released_push_window_rows (ids.len={ids_len}): {e}")
    });
    let params = rusqlite::params_from_iter(ids.iter());
    let rows = stmt
        .query_map(params, |row| {
            let push_id: i64 = row.get(0)?;
            let channel_address: String = row.get(1)?;
            let uuid_bytes: Vec<u8> = row.get(2)?;
            let target_app_slug: String = row.get(3)?;
            let target_subscriber_str: String = row.get(4)?;
            Ok((
                push_id,
                channel_address,
                uuid_bytes,
                target_app_slug,
                target_subscriber_str,
            ))
        })
        .unwrap_or_else(|e| {
            panic!("query load_released_push_window_rows (ids.len={ids_len}): {e}")
        });
    rows.map(|r| {
        let (push_id, channel_address, uuid_bytes, target_app_slug, target_subscriber_str) =
            r.expect("read released push window row");
        let channel_uuid = Uuid::from_slice(&uuid_bytes)
            .expect("load_released_push_window_rows: invalid uuid bytes");
        let target_subscriber = ParticipantId::from_stored(target_subscriber_str);
        ReleasedPushRow {
            push_id,
            channel_address,
            channel_uuid,
            target_app_slug,
            target_subscriber,
        }
    })
    .collect()
}

// ---------------------------------------------------------------------------
// Pending-push queries
// ---------------------------------------------------------------------------

/// Mark pending-push rows delivered. Idempotent.
///
/// A thin wrapper over [`claim_pending_pushes`] that discards the won ids: mark
/// and claim share the exact `delivered_at = now WHERE delivered_at IS NULL`
/// predicate, so the claim protocol's core statement has a single
/// implementation. The single `Mutex<Connection>` makes the claim atomic, so
/// the dispatcher's batch re-mark after a Surface `Ok(true)` is an idempotent
/// no-op against a row the session drain already claimed.
pub fn mark_pending_pushes_delivered(conn: &Connection, push_ids: &[i64]) {
    let _ = claim_pending_pushes(conn, push_ids);
}

/// Maximum ids bound into a single `IN (…)` batch. SQLite's compiled
/// `SQLITE_MAX_VARIABLE_NUMBER` is 32766 on modern builds; staying well under it
/// (claim also binds `now`) means an arbitrarily large parked backlog is claimed
/// across several statements instead of tripping a `prepare` error — the backlog
/// is bounded only by `push_depth`, which may be `Unbounded`.
const MAX_IN_CLAUSE_IDS: usize = 30000;

/// Atomically claim pending-push rows for delivery, returning the ids actually
/// claimed by this call. A row is claimed by stamping `delivered_at = now` while
/// it is still `delivered_at IS NULL`; the single `Mutex<Connection>` makes the
/// claim atomic across the two surface-delivery actors (the dispatcher fan-out
/// task and the session drain). An id already claimed by the other actor is
/// simply absent from the returned set — the caller then skips sending it.
///
/// Returns the claimed ids (a subset of `push_ids`). Idempotent double-claim of
/// the same id returns it at most once (to the first claimer). The id list is
/// batched under `MAX_IN_CLAUSE_IDS` so a large backlog never overflows the
/// SQLite bind-variable limit.
pub fn claim_pending_pushes(conn: &Connection, push_ids: &[i64]) -> Vec<i64> {
    if push_ids.is_empty() {
        return vec![];
    }
    let now = format_ts_for_db(Utc::now());
    let mut claimed = Vec::new();
    for chunk in push_ids.chunks(MAX_IN_CLAUSE_IDS) {
        let sql = format!(
            "UPDATE messaging_pending_pushes SET delivered_at = ?1
             WHERE id IN ({}) AND delivered_at IS NULL
             RETURNING id",
            build_bare_in_placeholders(chunk.len()),
        );
        let mut stmt = conn.prepare(&sql).expect("prepare claim_pending_pushes");
        // Params: now (?1) followed by the id list bound to the IN placeholders.
        let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::with_capacity(chunk.len() + 1);
        params.push(Box::new(now.clone()));
        for id in chunk {
            params.push(Box::new(*id));
        }
        let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| &**p as _).collect();
        let rows = stmt
            .query_map(rusqlite::params_from_iter(param_refs), |row| {
                row.get::<_, i64>(0)
            })
            .expect("query claim_pending_pushes");
        for r in rows {
            claimed.push(r.expect("read claimed push id"));
        }
    }
    claimed
}

/// Release a prior claim on pending-push rows by clearing `delivered_at`. Used
/// only by a claimer that failed to hand off any copy of the row (all target
/// session queues full or closed), so the row re-parks and is redelivered on
/// the next drain. Clears unconditionally over the given ids, batched under
/// `MAX_IN_CLAUSE_IDS`.
pub fn unclaim_pending_pushes(conn: &Connection, push_ids: &[i64]) {
    update_pushes_by_ids(
        conn,
        "delivered_at = NULL",
        push_ids,
        "unclaim_pending_pushes",
    );
}

/// Apply `SET <set_clause>` to `messaging_pending_pushes` rows matched by id,
/// batched under `MAX_IN_CLAUSE_IDS`. The shared skeleton behind every
/// SET-by-ids operation; `ctx` names the caller in the prepare/exec expects.
fn update_pushes_by_ids(conn: &Connection, set_clause: &str, push_ids: &[i64], ctx: &str) {
    if push_ids.is_empty() {
        return;
    }
    for chunk in push_ids.chunks(MAX_IN_CLAUSE_IDS) {
        let sql = format!(
            "UPDATE messaging_pending_pushes SET {set_clause} WHERE id IN ({})",
            build_bare_in_placeholders(chunk.len()),
        );
        let mut stmt = conn
            .prepare(&sql)
            .unwrap_or_else(|e| panic!("prepare {ctx}: {e}"));
        stmt.execute(rusqlite::params_from_iter(chunk.iter()))
            .unwrap_or_else(|e| panic!("exec {ctx}: {e}"));
    }
}

/// Stamp `confirm_pending = 1` on the claimed push row for `(subscriber,
/// message_id)` — the below-water ack channel's tentative mark.
/// Ordered before the socket write of a below-water durable row so a dead socket
/// after the write leaves recovery evidence the next resume's reconcile reads.
/// Scoped to a claimed row (`delivered_at IS NOT NULL`): only a row already on
/// its way to the wire is tentative, and `(subscriber, message_id)` identifies it
/// uniquely (one push row per subscriber per message). Returns the rows stamped.
pub fn stamp_confirm_pending(
    conn: &Connection,
    subscriber: &ParticipantId,
    message_id: i64,
) -> usize {
    conn.execute(
        "UPDATE messaging_pending_pushes SET confirm_pending = 1
         WHERE target_subscriber = ?1 AND message_id = ?2 AND delivered_at IS NOT NULL",
        rusqlite::params![subscriber.as_str(), message_id],
    )
    .expect("stamp_confirm_pending: UPDATE")
}

/// Whether a push row still exists for `(subscriber, message_id)`. Used to tell a
/// concurrently GC-evicted below-water row (row gone — an expected transient) from
/// a genuine invariant break (row present but unclaimed) when a stamp matched no
/// rows.
pub fn pending_push_exists(conn: &Connection, subscriber: &ParticipantId, message_id: i64) -> bool {
    conn.query_row(
        "SELECT 1 FROM messaging_pending_pushes
         WHERE target_subscriber = ?1 AND message_id = ?2
         LIMIT 1",
        rusqlite::params![subscriber.as_str(), message_id],
        |_| Ok(()),
    )
    .optional()
    .expect("pending_push_exists: query")
    .is_some()
}

/// Load the tentative (`confirm_pending = 1`) push rows for `(subscriber,
/// channel)`, returning `(push_id, message_id)` pairs. The reconcile at a durable
/// `Subscribe` reads these and, per the echoed cursor's confirm set, either
/// confirms (clears the flag) or unclaims each.
pub fn load_confirm_pending_pushes(
    conn: &Connection,
    subscriber: &ParticipantId,
    channel_uuid: Uuid,
) -> Vec<TentativePushRow> {
    let mut stmt = conn
        .prepare(
            "SELECT pp.id, pp.message_id, m.retained_seq
             FROM messaging_pending_pushes pp
             JOIN messaging_messages m ON pp.message_id = m.id
             WHERE pp.target_subscriber = ?1
               AND m.channel_uuid = ?2
               AND pp.confirm_pending = 1",
        )
        .expect("prepare load_confirm_pending_pushes");
    let rows = stmt
        .query_map(
            rusqlite::params![subscriber.as_str(), channel_uuid.as_bytes().to_vec()],
            |row| {
                Ok(TentativePushRow {
                    push_id: row.get(0)?,
                    message_id: row.get(1)?,
                    retained_seq: row.get(2)?,
                })
            },
        )
        .expect("query load_confirm_pending_pushes");
    rows.map(|r| r.expect("read confirm-pending push"))
        .collect()
}

/// One tentative (delivered, unconfirmed) push row, as
/// [`load_confirm_pending_pushes`] hands it over.
///
/// `retained_seq` is `None` only for a row whose message left retention order —
/// impossible for a delivered bus row. Callers may treat an absent position as
/// unresumable rather than matching it against a cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TentativePushRow {
    pub push_id: i64,
    pub message_id: i64,
    pub retained_seq: Option<i64>,
}

/// Confirm tentative push rows: clear `confirm_pending` back to 0, leaving them
/// delivered. Their receipt was proven by the client echoing a cursor whose
/// confirm set names them, so they age out via GC as any delivered row.
pub fn confirm_pending_pushes(conn: &Connection, push_ids: &[i64]) {
    update_pushes_by_ids(
        conn,
        "confirm_pending = 0",
        push_ids,
        "confirm_pending_pushes",
    );
}

/// Unclaim tentative push rows never acknowledged: clear both `confirm_pending`
/// and `delivered_at` so the same subscribe's parked claim redelivers them
/// exactly once. A below-water redelivery re-stamps and re-enters
/// the set; the recursion terminates at the first cursor the client echoes.
pub fn unclaim_confirm_pending_pushes(conn: &Connection, push_ids: &[i64]) {
    update_pushes_by_ids(
        conn,
        "delivered_at = NULL, confirm_pending = 0",
        push_ids,
        "unclaim_confirm_pending_pushes",
    );
}

/// Load undelivered, unparked bus pending-push rows for `(subscriber, channel)`
/// in `publish_ts_ns ASC, id ASC` order. The channel-scoped sibling of
/// `load_pending_pushes_for_drain`: same predicate
/// (`delivered_at IS NULL AND release_after IS NULL`) and ordering, plus a
/// `m.channel_uuid = ?` filter, bus rows only (inner join on
/// `messaging_channels`).
///
/// Ingress rows never reach this path (surfaces bind only `brenn:`/`ephemeral:`
/// channels, and ingress rows carry no such channel); a stray ingress row would
/// decode to a `Bus` payload only if it had a channel FK, which it does not, so
/// the inner join excludes it.
pub fn load_pending_pushes_for_channel(
    conn: &Connection,
    subscriber: &ParticipantId,
    channel_uuid: Uuid,
) -> Vec<ChannelPushRow> {
    // Cached: one call per input port on every WASM drain step.
    let mut stmt = conn
        .prepare_cached(
            "SELECT pp.id, pp.target_subscriber, pp.eager_wake,
                    m.uuid, m.source, m.sender, m.body,
                    m.urgency AS msg_urgency,
                    m.delivery_deadline, m.deliver_after, m.publish_ts_ns,
                    c.address, rc.address, m.envelope_type, pp.message_id,
                    pp.target_app_slug, m.retained_seq
             FROM messaging_pending_pushes pp
             JOIN messaging_messages m ON pp.message_id = m.id
             JOIN messaging_channels c ON c.uuid = m.channel_uuid
             LEFT JOIN messaging_channels rc ON rc.uuid = m.reply_to_uuid
             WHERE pp.target_subscriber = ?1
               AND m.channel_uuid = ?2
               AND pp.delivered_at IS NULL
               AND pp.release_after IS NULL
             ORDER BY m.publish_ts_ns ASC, m.id ASC",
        )
        .expect("prepare load_pending_pushes_for_channel");
    let rows = stmt
        .query_map(
            rusqlite::params![subscriber.as_str(), channel_uuid.as_bytes().to_vec()],
            row_to_pending_push,
        )
        .expect("query load_pending_pushes_for_channel");
    rows.map(|r| {
        let row = r.expect("read pending push for channel");
        let envelope = match row.payload {
            IngressOrBus::Bus(env) => env,
            IngressOrBus::Ingress(_) => panic!(
                "messaging: load_pending_pushes_for_channel got an ingress row (push {}) — \
                 bus-only query returned a non-bus payload",
                row.push_id
            ),
        };
        let retained_seq = row.retained_seq.unwrap_or_else(|| {
            panic!(
                "messaging: push {} is owed and unparked but its message {} holds no \
                 retention position",
                row.push_id, row.message_id
            )
        });
        ChannelPushRow {
            push_id: row.push_id,
            message_id: row.message_id,
            retained_seq,
            envelope,
        }
    })
    .collect()
}

/// The `messaging_pending_pushes` predicate for "owed and deliverable to this
/// subscriber on this channel", matching the one
/// [`load_pending_pushes_for_channel`] serves from. `?1` is the subscriber,
/// `?2` the channel uuid.
const OWED_ON_CHANNEL_WHERE: &str = "WHERE pp.target_subscriber = ?1
           AND m.channel_uuid = ?2
           AND pp.delivered_at IS NULL
           AND pp.release_after IS NULL";

/// Every claim `subscriber` is still owed on `channel_uuid` as
/// `(push_id, retained_seq)`, seq ascending.
pub fn owed_push_positions(
    conn: &Connection,
    subscriber: &ParticipantId,
    channel_uuid: Uuid,
) -> Vec<(i64, i64)> {
    // Cached: one call per advanced port on every WASM drain step.
    let mut stmt = conn
        .prepare_cached(&format!(
            "SELECT pp.id, m.retained_seq
             FROM messaging_pending_pushes pp
             JOIN messaging_messages m ON pp.message_id = m.id
             {OWED_ON_CHANNEL_WHERE}
             ORDER BY m.retained_seq ASC"
        ))
        .expect("prepare owed_push_positions");
    let rows = stmt
        .query_map(
            rusqlite::params![subscriber.as_str(), channel_uuid.as_bytes().to_vec()],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?)),
        )
        .expect("query owed_push_positions");
    rows.map(|r| {
        let (push_id, retained_seq) = r.expect("read owed push position");
        let retained_seq = retained_seq.unwrap_or_else(|| {
            panic!(
                "messaging: push {push_id} is owed and unparked but its message holds no \
                 retention position"
            )
        });
        (push_id, retained_seq)
    })
    .collect()
}

/// One owed, deliverable push row on one channel, as
/// [`load_pending_pushes_for_channel`] hands it over: the claim to settle, the
/// message's identity, its position in the channel's retention order, and the
/// payload. Two of the three ids are `i64`, so they travel named rather than
/// positionally.
#[derive(Debug, Clone)]
pub struct ChannelPushRow {
    /// `messaging_pending_pushes.id` — the claim.
    pub push_id: i64,
    /// `messaging_messages.id` — the message's identity, which the below-water
    /// ack evidence is written against.
    pub message_id: i64,
    /// `messaging_messages.retained_seq` — the message's position in this
    /// channel's retention order, which a resume cursor names.
    pub retained_seq: i64,
    pub envelope: MessageEnvelope,
}

/// Owed, deliverable pending-push rows held for `subscriber` on channels other
/// than `channels`.
///
/// Returns `(push_id, channel_uuid)`, with a `None` channel for a row that
/// carries no channel at all (an ingress row). Only owed, deliverable rows
/// are returned; a still-parked claim is never mistaken for residue.
pub fn pending_pushes_outside_channels(
    conn: &Connection,
    subscriber: &ParticipantId,
    channels: &[Uuid],
) -> Vec<(i64, Option<Uuid>)> {
    // No readable channel at all: every owed claim this subscriber holds is
    // residue, so the exclusion clause is simply absent.
    let exclusion = if channels.is_empty() {
        String::new()
    } else {
        format!(
            " AND (m.channel_uuid IS NULL OR m.channel_uuid NOT IN ({}))",
            build_bare_in_placeholders(channels.len())
        )
    };
    let sql = format!(
        "SELECT pp.id, m.channel_uuid
         FROM messaging_pending_pushes pp
         JOIN messaging_messages m ON pp.message_id = m.id
         WHERE pp.target_subscriber = ?
           AND pp.delivered_at IS NULL
           AND pp.release_after IS NULL{exclusion}
         ORDER BY pp.id ASC"
    );
    let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::with_capacity(channels.len() + 1);
    params.push(Box::new(subscriber.as_str().to_string()));
    for channel in channels {
        params.push(Box::new(channel.as_bytes().to_vec()));
    }
    let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p.as_ref()).collect();
    // Cached: this runs on every WASM drain step, and the SQL text is stable
    // per readable-set size.
    let mut stmt = conn
        .prepare_cached(&sql)
        .expect("prepare pending_pushes_outside_channels");
    let rows = stmt
        .query_map(param_refs.as_slice(), |row| {
            let push_id: i64 = row.get(0)?;
            let channel: Option<Vec<u8>> = row.get(1)?;
            Ok((push_id, channel))
        })
        .expect("query pending_pushes_outside_channels");
    rows.map(|r| {
        let (push_id, channel) = r.expect("read residue pending push");
        let channel = channel.map(|bytes| {
            Uuid::from_slice(&bytes)
                .unwrap_or_else(|e| panic!("messaging: residue push {push_id} channel uuid: {e}"))
        });
        (push_id, channel)
    })
    .collect()
}

/// Whether `subscriber` holds any owed, deliverable pending push on
/// `channel_uuid`: `delivered_at IS NULL AND release_after IS NULL`, the same
/// predicate `load_pending_pushes_for_channel` serves from. A row still parked
/// behind its `release_after` hold is owed to nobody yet.
///
/// Test-only. Production answers deliverability through cursor positions
/// (`cursor_has_deliverable`); this query answers through claim rows. Two
/// reachable answers to one question is one too many.
#[cfg(test)]
pub fn channel_has_deliverable_for(
    conn: &Connection,
    channel_uuid: Uuid,
    subscriber: &ParticipantId,
) -> bool {
    conn.query_row(
        "SELECT EXISTS(
             SELECT 1
             FROM messaging_pending_pushes pp
             JOIN messaging_messages m ON pp.message_id = m.id
             WHERE m.channel_uuid = ?1
               AND pp.target_subscriber = ?2
               AND pp.delivered_at IS NULL
               AND pp.release_after IS NULL)",
        rusqlite::params![channel_uuid.as_bytes().to_vec(), subscriber.as_str()],
        |row| row.get::<_, i64>(0),
    )
    .expect("query channel_has_deliverable_for")
        != 0
}

/// The retained suffix strictly after retention sequence `after_seq`, oldest
/// first (seq ascending), capped by `clamp` — the `Exact` branch's re-send set.
///
/// Keyed on `retained_seq`, the dense per-channel retention order: a parked row
/// carries no `retained_seq` and is absent, and a late-released row (its seq
/// assigned at release, above every trailing cursor) is present as the newest,
/// converging with the ring. Each row is returned with its seq so a consumer
/// mints its next cursor from this output alone.
pub fn load_channel_messages_after_seq(
    conn: &Connection,
    channel_uuid: Uuid,
    after_seq: i64,
    clamp: crate::messaging::config::Depth,
) -> Vec<(i64, MessageEnvelope)> {
    let sql = retained_window_sql("AND m.retained_seq > ?2", "?3");
    let mut stmt = conn
        .prepare(&sql)
        .expect("prepare load_channel_messages_after_seq");
    read_retained_rows(
        &mut stmt,
        rusqlite::params![
            channel_uuid.as_bytes().to_vec(),
            after_seq,
            depth_limit(clamp)
        ],
    )
    .into_iter()
    .map(|row| (row.seq, row.envelope))
    .collect()
}

/// The newest `limit` retained messages on `channel_uuid`, oldest first (seq
/// ascending), each with its retention sequence — the whole retained window as
/// the resume `Fresh`/`Gap` branches serve it.
pub fn load_channel_retained_window_seq(
    conn: &Connection,
    channel_uuid: Uuid,
    limit: crate::messaging::config::Depth,
) -> Vec<(i64, MessageEnvelope)> {
    let sql = retained_window_sql("", "?2");
    let mut stmt = conn
        .prepare(&sql)
        .expect("prepare load_channel_retained_window_seq");
    read_retained_rows(
        &mut stmt,
        rusqlite::params![channel_uuid.as_bytes().to_vec(), depth_limit(limit)],
    )
    .into_iter()
    .map(|row| (row.seq, row.envelope))
    .collect()
}

/// One row of a retained-window read: the message plus both keys callers select
/// on — its rowid identity and its retention position.
struct RetainedRow {
    id: i64,
    seq: i64,
    envelope: MessageEnvelope,
}

/// A retained-window row cap as SQLite spells it (`-1` is no limit).
fn depth_limit(depth: crate::messaging::config::Depth) -> i64 {
    match depth {
        crate::messaging::config::Depth::Unbounded => -1,
        crate::messaging::config::Depth::Bounded(n) => n as i64,
    }
}

/// The one retained-window query body, shared by every retention-order read.
///
/// Selects the 13 envelope columns `query::row_to_envelope` decodes (0-12) plus
/// `m.id` (13) and `m.retained_seq` (14), newest-first, for channel `?1`.
/// `where_tail` adds predicates after the retained-set predicate; `limit_param`
/// names the bind holding the row cap. One copy so a change to the retained-set
/// predicate cannot land on some reads and miss others.
fn retained_window_sql(where_tail: &str, limit_param: &str) -> String {
    format!(
        "SELECT m.uuid, m.channel_uuid, m.source, m.sender, m.body, m.urgency,
                m.reply_to_uuid, m.delivery_deadline, m.deliver_after, m.publish_ts_ns,
                c.address, rc.address, m.envelope_type, m.id, m.retained_seq
         FROM messaging_messages m
         JOIN messaging_channels c ON c.uuid = m.channel_uuid
         LEFT JOIN messaging_channels rc ON rc.uuid = m.reply_to_uuid
         WHERE m.channel_uuid = ?1 AND m.retained_seq IS NOT NULL {where_tail}
         ORDER BY m.retained_seq DESC LIMIT {limit_param}"
    )
}

/// Run a [`retained_window_sql`] statement, returning its rows oldest-first (the
/// query orders newest-first, so the result is reversed).
fn read_retained_rows(
    stmt: &mut rusqlite::Statement<'_>,
    params: &[&dyn rusqlite::ToSql],
) -> Vec<RetainedRow> {
    let rows = stmt
        .query_map(params, |row| {
            let envelope = crate::messaging::query::row_to_envelope(row)?;
            Ok(RetainedRow {
                id: row.get(13)?,
                seq: row.get(14)?,
                envelope,
            })
        })
        .expect("query retained-window rows");
    let mut out: Vec<RetainedRow> = rows.map(|r| r.expect("read retained-window row")).collect();
    out.reverse();
    out
}

/// The newest `limit` retained messages on `channel_uuid`, oldest first — the
/// channel's retained ambience as a `DbStore` serves it, returned as
/// `(message_id, envelope)` for callers that seed per-message push rows.
///
/// Ordered by `retained_seq` — the retention order — so a late-released message
/// (its seq assigned at release) is the newest tail entry, as it is on the ring.
/// A parked row carries no `retained_seq` and is absent, whatever the clock
/// says.
pub fn load_channel_retained_tail(
    conn: &Connection,
    channel_uuid: Uuid,
    limit: crate::messaging::config::Depth,
) -> Vec<(i64, MessageEnvelope)> {
    let sql = retained_window_sql("", "?2");
    let mut stmt = conn
        .prepare(&sql)
        .expect("prepare load_channel_retained_tail");
    read_retained_rows(
        &mut stmt,
        rusqlite::params![channel_uuid.as_bytes().to_vec(), depth_limit(limit)],
    )
    .into_iter()
    .map(|row| (row.id, row.envelope))
    .collect()
}

/// The highest retention sequence this channel ever assigned — its persisted
/// resume high-water, carried on the channel row and surviving eviction of every
/// retained message. `0` means nothing was ever retained.
///
/// This is what makes an empty retained window decidable: a cursor at this value
/// is up to date even when no rows remain, and one below it proves the client
/// once saw messages the store no longer holds.
pub fn channel_last_retained_seq(conn: &Connection, channel_uuid: Uuid) -> i64 {
    conn.query_row(
        "SELECT last_retained_seq FROM messaging_channels WHERE uuid = ?1",
        rusqlite::params![channel_uuid.as_bytes().to_vec()],
        |row| row.get::<_, i64>(0),
    )
    .unwrap_or_else(|e| panic!("messaging: channel {channel_uuid} has no row: {e}"))
}

/// The oldest retention sequence the channel still holds, or `None` when it
/// holds nothing — the boundary below which every message is gone.
///
/// Positions below this frontier name messages already evicted; their losses
/// were reported at eviction time and must not be charged again.
pub fn channel_retention_frontier(conn: &Connection, channel_uuid: Uuid) -> Option<i64> {
    // Cached: an advance that lost something asks on the drain path.
    let mut stmt = conn
        .prepare_cached(
            "SELECT MIN(retained_seq) FROM messaging_messages
             WHERE channel_uuid = ?1 AND retained_seq IS NOT NULL",
        )
        .expect("prepare channel_retention_frontier");
    stmt.query_row(rusqlite::params![channel_uuid.as_bytes().to_vec()], |row| {
        row.get::<_, Option<i64>>(0)
    })
    .expect("query channel_retention_frontier")
}

/// The oldest retention sequence among the newest `limit` retained messages on
/// `channel_uuid`, or `None` when the channel retains nothing — where a
/// retained-primed cursor starts.
///
/// Shares the retained-set predicate with [`load_channel_retained_tail`]: a
/// parked row carries no `retained_seq` and is not part of the tail a fresh
/// queue is owed.
pub fn retained_tail_floor_seq(
    conn: &Connection,
    channel_uuid: Uuid,
    limit: crate::messaging::config::Depth,
) -> Option<i64> {
    conn.query_row(
        "SELECT MIN(retained_seq) FROM (
             SELECT m.retained_seq FROM messaging_messages m
             WHERE m.channel_uuid = ?1 AND m.retained_seq IS NOT NULL
             ORDER BY m.retained_seq DESC LIMIT ?2
         )",
        rusqlite::params![channel_uuid.as_bytes().to_vec(), depth_limit(limit)],
        |row| row.get::<_, Option<i64>>(0),
    )
    .expect("query retained_tail_floor_seq")
}

/// The channel's persisted resume epoch — the identity of its numbering domain,
/// minted once with the channel row and dying only with it.
pub fn channel_resume_epoch(conn: &Connection, channel_uuid: Uuid) -> Uuid {
    let bytes: Vec<u8> = conn
        .query_row(
            "SELECT resume_epoch FROM messaging_channels WHERE uuid = ?1",
            rusqlite::params![channel_uuid.as_bytes().to_vec()],
            |row| row.get(0),
        )
        .unwrap_or_else(|e| panic!("messaging: channel {channel_uuid} has no row: {e}"));
    Uuid::from_slice(&bytes)
        .unwrap_or_else(|e| panic!("messaging: channel {channel_uuid} resume_epoch {bytes:?}: {e}"))
}

/// How many retained rows the channel still holds above `after_seq`, unclamped.
pub fn channel_retained_count_after_seq(
    conn: &Connection,
    channel_uuid: Uuid,
    after_seq: i64,
) -> i64 {
    conn.query_row(
        "SELECT COUNT(*) FROM messaging_messages
         WHERE channel_uuid = ?1 AND retained_seq IS NOT NULL AND retained_seq > ?2",
        rusqlite::params![channel_uuid.as_bytes().to_vec(), after_seq],
        |row| row.get(0),
    )
    .expect("messaging: query channel_retained_count_after_seq")
}

/// Earliest pending `delivery_deadline`, or `None` if no rows are
/// awaiting deadline-driven release.
pub fn earliest_pending_deadline(conn: &Connection) -> Option<DateTime<Utc>> {
    let result: Option<String> = conn
        .query_row(
            "SELECT MIN(delivery_deadline) FROM messaging_pending_pushes
             WHERE delivered_at IS NULL AND delivery_deadline IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .expect("messaging: query earliest_pending_deadline");
    // A non-NULL, non-parseable timestamp is a DB-invariant violation (schema bug or
    // corruption). Silent None would cause the dispatcher to sleep POLL_INTERVAL instead
    // of waking at the correct deadline — silent delivery delay with no on-call signal.
    // Panic per CLAUDE.md "fail fast on invariant violations".
    result.map(|s| {
        parse_rfc3339(&s).unwrap_or_else(|| {
            panic!("messaging: malformed delivery_deadline in messaging_pending_pushes: {s:?}")
        })
    })
}

/// Load a list of pending-push rows by id. Skips delivered rows and rows
/// not found.
pub fn load_pushes_by_ids(conn: &Connection, ids: &[i64]) -> Vec<PendingPushRow> {
    if ids.is_empty() {
        return vec![];
    }
    let sql = format!(
        "SELECT pp.id, pp.target_subscriber, pp.eager_wake,
                m.uuid, m.source, m.sender, m.body,
                m.urgency AS msg_urgency,
                m.delivery_deadline, m.deliver_after, m.publish_ts_ns,
                c.address, rc.address, m.envelope_type, pp.message_id,
                pp.target_app_slug, m.retained_seq
         FROM messaging_pending_pushes pp
         JOIN messaging_messages m ON pp.message_id = m.id
         JOIN messaging_channels c ON c.uuid = m.channel_uuid
         LEFT JOIN messaging_channels rc ON rc.uuid = m.reply_to_uuid
         WHERE pp.id IN ({}) AND pp.delivered_at IS NULL",
        build_bare_in_placeholders(ids.len()),
    );
    let mut stmt = conn.prepare(&sql).expect("prepare load_pushes_by_ids");
    let params = rusqlite::params_from_iter(ids.iter());
    let rows = stmt
        .query_map(params, row_to_pending_push)
        .expect("query load_pushes_by_ids");
    rows.map(|r| r.expect("read pending push by id")).collect()
}

/// SQL for the dispatcher's global scan. Shared between `load_all_dispatchable_pushes`
/// and its plan-assertion test so the test can never drift from the production query.
///
/// The first disjunction conjunct (`pp.eager_wake = 1 OR pp.delivery_deadline IS NOT NULL`)
/// is logically redundant with the second (the `<= ?1` bound is strictly narrower), so it
/// changes no rows. It is present verbatim to match the partial predicate of
/// `idx_messaging_pending_pushes_dispatchable`, letting SQLite qualify the partial index by
/// the identical-expression rule; the `<= ?1` term then applies as a residual filter.
pub(crate) const LOAD_ALL_DISPATCHABLE_PUSHES_SQL: &str = "SELECT pp.id, pp.target_subscriber, pp.eager_wake,
                    m.uuid, m.source, m.sender, m.body,
                    m.urgency AS msg_urgency,
                    m.delivery_deadline, m.deliver_after, m.publish_ts_ns,
                    c.address, rc.address, m.envelope_type,
                    m.ingress_source, m.ingress_summary,
                    (pp.delivery_deadline IS NOT NULL AND pp.delivery_deadline <= ?1) AS deadline_expired,
                    pp.message_id, pp.target_app_slug, m.retained_seq
             FROM messaging_pending_pushes pp
             JOIN messaging_messages m ON pp.message_id = m.id
             LEFT JOIN messaging_channels c ON c.uuid = m.channel_uuid
             LEFT JOIN messaging_channels rc ON rc.uuid = m.reply_to_uuid
             WHERE pp.delivered_at IS NULL
               AND pp.release_after IS NULL
               AND (pp.eager_wake = 1 OR pp.delivery_deadline IS NOT NULL)
               AND ( pp.eager_wake = 1
                  OR (pp.delivery_deadline IS NOT NULL AND pp.delivery_deadline <= ?1) )
             ORDER BY m.publish_ts_ns ASC";

/// Load all currently-dispatchable pending-push rows (both bus and ingress)
/// for the background dispatcher (design §2.3, D-b global scan).
///
/// Returns rows satisfying the dispatcher's working set predicate:
/// - `delivered_at IS NULL` — undelivered
/// - `release_after IS NULL` — not still suppressed (deferred-not-yet-released excluded)
/// - `pp.eager_wake = 1` OR (`delivery_deadline IS NOT NULL` AND `delivery_deadline <= now`)
///
/// The predicate deliberately excludes parked non-deadline rows (eager_wake=0, no deadline):
/// those are delivered by the startup/reconnect drain, not the dispatcher's wake action.
/// Loading them on every dispatcher poll would be pure waste.
///
/// The scan is index-backed by `idx_messaging_pending_pushes_dispatchable`, whose
/// partial predicate matches the first disjunction conjunct in the query verbatim so
/// SQLite's planner qualifies the partial index; the plan-assertion test
/// `load_all_dispatchable_pushes_uses_partial_index` guards against a silent regression
/// to a full scan.
///
/// Results are ordered by `m.publish_ts_ns ASC` to satisfy R10 (per-conversation
/// ordering within the dispatcher's fan-out groups).
///
/// The `deadline_expired` bool on each row tells `dispatch_row` whether to apply
/// the unconditional-wake deadline override (design §2.4).
pub fn load_all_dispatchable_pushes(
    conn: &Connection,
    now: DateTime<Utc>,
) -> Vec<(PendingPushRow, bool)> {
    let now_str = format_ts_for_db(now);
    let mut stmt = conn
        .prepare(LOAD_ALL_DISPATCHABLE_PUSHES_SQL)
        .expect("prepare load_all_dispatchable_pushes");
    let rows = stmt
        .query_map(rusqlite::params![now_str], row_to_dispatchable_push)
        .expect("query load_all_dispatchable_pushes");
    rows.map(|r| r.expect("read dispatchable push")).collect()
}

/// Row decoder for the dispatcher's global scan (columns 0-16, `LEFT JOIN` channel).
///
/// Column layout (matches `load_all_dispatchable_pushes` SELECT):
/// - 0: pp.id  1: pp.target_subscriber  2: pp.eager_wake (0 or 1)
/// - 3: m.uuid  4: m.source  5: m.sender  6: m.body  7: m.urgency (msg)
/// - 8: m.delivery_deadline  9: m.deliver_after  10: m.publish_ts_ns
/// - 11: c.address (NULL for ingress — LEFT JOIN)
/// - 12: rc.address (NULL when not a reply)
/// - 13: m.envelope_type
/// - 14: m.ingress_source (NULL for bus rows)
/// - 15: m.ingress_summary (NULL for bus rows)
/// - 16: deadline_expired (0 or 1)
/// - 17: pp.message_id
/// - 18: pp.target_app_slug
/// - 19: m.retained_seq (NULL for ingress and for a still-parked message)
///
/// Handles `brenn`, `webhook`, and `ingress` envelope types, matching the
/// drain-path decoder. Bus rows produce `IngressOrBus::Bus(MessageEnvelope)`;
/// ingress rows produce `IngressOrBus::Ingress(IngressEvent)`.
fn row_to_dispatchable_push(row: &rusqlite::Row) -> rusqlite::Result<(PendingPushRow, bool)> {
    use crate::messaging::ingress::Event as IngressEvent;

    let push_id: i64 = row.get(0)?;
    let target_subscriber_str: String = row.get(1)?;
    let target_subscriber = ParticipantId::from_stored(target_subscriber_str);
    let target_app_slug: String = row.get(18)?;
    let eager_wake: bool = row.get::<_, i64>(2)? != 0;
    let msg_uuid_bytes: Vec<u8> = row.get(3)?;
    // cols 4 (source) and 5 (sender) deferred — only used in the bus arm below.
    let body: String = row.get(6)?;
    let msg_urgency_str: String = row.get(7)?;
    let delivery_deadline_s: Option<String> = row.get(8)?;
    let deliver_after_s: Option<String> = row.get(9)?;
    let publish_ts_ns: i64 = row.get(10)?;
    // col 11: c.address (NULL for ingress — LEFT JOIN)
    // col 12: rc.address
    let envelope_type: String = row.get(13)?;
    // col 14: m.ingress_source (NULL for bus rows)
    // col 15: m.ingress_summary (NULL for bus rows)
    let deadline_expired: bool = row.get::<_, i64>(16)? != 0;
    let message_id: i64 = row.get(17)?;
    let retained_seq: Option<i64> = row.get(19)?;

    // Build IngressOrBus::Bus(MessageEnvelope) from the common row columns.
    // Shared by the `brenn` and `webhook` arms — structurally identical.
    // Takes `body` by value to avoid a move-into-closure conflict with the
    // ingress arm below (same pattern as `row_to_drain_push` in ingress.rs).
    let build_bus_envelope = |et: super::super::ChannelScheme,
                              body: String|
     -> rusqlite::Result<IngressOrBus> {
        let urgency = Urgency::parse(&msg_urgency_str).unwrap_or_else(|| {
            panic!("messaging: message for push {push_id} has invalid urgency {msg_urgency_str:?}")
        });
        let message_uuid = Uuid::from_slice(&msg_uuid_bytes).unwrap_or_else(|e| {
            panic!("messaging: message uuid for push {push_id} is malformed: {e}")
        });
        let delivery_deadline = delivery_deadline_s.map(|s| {
            parse_rfc3339(&s).unwrap_or_else(|| panic!("messaging: invalid rfc3339 in db: {s:?}"))
        });
        let deliver_after = deliver_after_s.map(|s| {
            parse_rfc3339(&s).unwrap_or_else(|| panic!("messaging: invalid rfc3339 in db: {s:?}"))
        });
        let publish_ts = ns_to_utc(publish_ts_ns);
        let source: String = row.get(4)?;
        let sender: String = row.get(5)?;
        let channel_address: String = row.get(11).unwrap_or_else(|e| {
            panic!(
                "messaging: push {push_id} is envelope_type={:?} but \
                     c.address is NULL (missing channel FK): {e}",
                et.as_str()
            )
        });
        let reply_to: Option<String> = row.get(12)?;
        Ok(IngressOrBus::Bus(MessageEnvelope {
            message_id: message_uuid,
            source,
            channel: channel_address,
            sender,
            publish_ts,
            body,
            reply_to,
            delivery_deadline,
            deliver_after,
            urgency,
            envelope_type: et,
        }))
    };

    use super::super::ChannelScheme;
    use super::EnvelopeTypeColumn;
    let payload = match EnvelopeTypeColumn::parse(&envelope_type) {
        // `brenn`, `webhook`, and `mqtt` rows are typed bus messages (non-NULL
        // channel_uuid) — route through the standard envelope renderer.
        Some(EnvelopeTypeColumn::Bus(
            scheme @ (ChannelScheme::Brenn | ChannelScheme::Webhook | ChannelScheme::Mqtt),
        )) => build_bus_envelope(scheme, body)?,
        Some(EnvelopeTypeColumn::Ingress) => {
            let ingress_source: Option<String> = row.get(14)?;
            let ingress_summary: Option<String> = row.get(15)?;
            let ingress_source = ingress_source.unwrap_or_else(|| {
                panic!(
                    "messaging: push {push_id} is envelope_type='ingress' \
                     but ingress_source IS NULL"
                )
            });
            let ingress_summary = ingress_summary.unwrap_or_else(|| {
                panic!(
                    "messaging: push {push_id} is envelope_type='ingress' \
                     but ingress_summary IS NULL"
                )
            });
            let created_at = ns_to_utc(publish_ts_ns);
            IngressOrBus::Ingress(IngressEvent {
                id: push_id,
                conversation_id: crate::messaging::ingress::SYNTHETIC_EVENT_ID,
                source: ingress_source,
                summary: ingress_summary,
                payload: body,
                created_at,
            })
        }
        // `ephemeral`/`local`/`pwa_push` are never persisted to
        // `messaging_messages`, and any other value is corruption. `local` rows
        // are doubly impossible: that traffic never leaves the page, so it
        // never reaches a DB write. The host wrote every row — panic.
        Some(EnvelopeTypeColumn::Bus(
            ChannelScheme::Ephemeral | ChannelScheme::Local | ChannelScheme::PwaPush,
        ))
        | None => {
            panic!(
                "messaging: push {push_id} has non-persistable envelope_type {envelope_type:?} \
             (ephemeral/local/pwa_push are never written; anything else is unrecognized) — \
             host wrote every row; this is a host-internal bug"
            )
        }
    };

    Ok((
        PendingPushRow {
            push_id,
            message_id,
            retained_seq,
            payload,
            target_subscriber,
            target_app_slug,
            eager_wake,
        },
        deadline_expired,
    ))
}

/// Row decoder for bus-only queries (columns 0-13, `INNER JOIN` channel).
///
/// Column layout (matches `load_pushes_by_ids` SELECT):
/// - 0: pp.id  1: pp.target_subscriber  2: pp.eager_wake (0 or 1)
/// - 3: m.uuid  4: m.source  5: m.sender  6: m.body  7: m.urgency (msg)
/// - 8: m.delivery_deadline  9: m.deliver_after  10: m.publish_ts_ns
/// - 11: c.address  12: rc.address  13: m.envelope_type
///
/// Used by `load_pushes_by_ids` and `load_pending_pushes_for_channel` — bus-only
/// by construction.
/// A `kind='ingress'` row reaching this path is an unexpected state (the
/// ingress path never sets `deliver_after`/`delivery_deadline` and never flows
/// through dispatch); the inner-join on `messaging_channels` means such a row
/// would silently fail to appear in the result (channel_uuid IS NULL → join miss).
/// That is acceptable for bus-only paths; the dispatcher uses `row_to_dispatchable_push`
/// instead.
///
/// Column 14 is `pp.message_id`; column 15 is `pp.target_app_slug`; column 16 is
/// `m.retained_seq` (NULL while the message is parked).
fn row_to_pending_push(row: &rusqlite::Row) -> rusqlite::Result<PendingPushRow> {
    let push_id: i64 = row.get(0)?;
    let target_subscriber_str: String = row.get(1)?;
    let target_subscriber = ParticipantId::from_stored(target_subscriber_str);
    let target_app_slug: String = row.get(15)?;
    let eager_wake: bool = row.get::<_, i64>(2)? != 0;
    let msg_uuid_bytes: Vec<u8> = row.get(3)?;
    let source: String = row.get(4)?;
    let sender: String = row.get(5)?;
    let body: String = row.get(6)?;
    let msg_urgency_str: String = row.get(7)?;
    let delivery_deadline_s: Option<String> = row.get(8)?;
    let deliver_after_s: Option<String> = row.get(9)?;
    let publish_ts_ns: i64 = row.get(10)?;
    // Resolved via INNER JOIN messaging_channels c ON c.uuid = m.channel_uuid.
    // Bus-only: channel_address is always non-NULL here.
    let channel_address: String = row.get(11)?;
    // Resolved via LEFT JOIN messaging_channels rc ON rc.uuid = m.reply_to_uuid;
    // NULL when not a reply.
    let reply_to: Option<String> = row.get(12)?;
    let envelope_type_str: String = row.get(13)?;
    let message_id: i64 = row.get(14)?;
    let retained_seq: Option<i64> = row.get(16)?;

    let urgency = Urgency::parse(&msg_urgency_str).unwrap_or_else(|| {
        panic!("messaging: message for push {push_id} has invalid urgency {msg_urgency_str:?}")
    });
    let envelope_type =
        super::super::ChannelScheme::parse(&envelope_type_str).unwrap_or_else(|| {
            panic!(
                "messaging: message for push {push_id} has unknown envelope_type \
                 {envelope_type_str:?} — host wrote every row"
            )
        });

    let message_uuid = Uuid::from_slice(&msg_uuid_bytes)
        .unwrap_or_else(|e| panic!("messaging: message uuid for push {push_id} is malformed: {e}"));

    let delivery_deadline = delivery_deadline_s.map(|s| {
        parse_rfc3339(&s).unwrap_or_else(|| panic!("messaging: invalid rfc3339 in db: {s:?}"))
    });
    let deliver_after = deliver_after_s.map(|s| {
        parse_rfc3339(&s).unwrap_or_else(|| panic!("messaging: invalid rfc3339 in db: {s:?}"))
    });
    let publish_ts = ns_to_utc(publish_ts_ns);

    let envelope = MessageEnvelope {
        message_id: message_uuid,
        source,
        channel: channel_address,
        sender,
        publish_ts,
        body,
        reply_to,
        delivery_deadline,
        deliver_after,
        urgency,
        envelope_type,
    };

    Ok(PendingPushRow {
        push_id,
        message_id,
        retained_seq,
        payload: IngressOrBus::Bus(envelope),
        target_subscriber,
        target_app_slug,
        eager_wake,
    })
}

// ---------------------------------------------------------------------------
// Cancel / edit / list-pending helpers
// ---------------------------------------------------------------------------

/// Per-message authorization data returned by `lookup_message_for_authorship`.
#[derive(Debug)]
pub struct MessageLookup {
    /// `messaging_messages.id` (integer rowid, not the UUID).
    pub message_id: i64,
    /// `messaging_messages.sender` DB value.
    pub sender: String,
    /// Pushes with `delivered_at IS NULL`.
    pub undelivered_count: u32,
    /// Pushes with `delivered_at IS NOT NULL` (genuine delivery, not cancel).
    pub delivered_count: u32,
    /// The message is still parked behind a `deliver_after`. A parked message
    /// carries no push rows — release mints them for the subscribers attached
    /// then — so it is pending despite `undelivered_count` being zero.
    pub parked: bool,
}

/// Look up a message by UUID for authorship / status checks.
/// Returns `None` if no row with that UUID exists.
pub fn lookup_message_for_authorship(conn: &Connection, uuid: Uuid) -> Option<MessageLookup> {
    let uuid_bytes = uuid.as_bytes().to_vec();
    conn.query_row(
        "SELECT m.id, m.sender,
                COUNT(CASE WHEN pp.id IS NOT NULL AND pp.delivered_at IS NULL THEN 1 END),
                COUNT(CASE WHEN pp.id IS NOT NULL AND pp.delivered_at IS NOT NULL THEN 1 END),
                m.deliver_after IS NOT NULL
         FROM messaging_messages m
         LEFT JOIN messaging_pending_pushes pp ON pp.message_id = m.id
         WHERE m.uuid = ?1
         GROUP BY m.id",
        rusqlite::params![uuid_bytes],
        |row| {
            Ok(MessageLookup {
                message_id: row.get(0)?,
                sender: row.get(1)?,
                undelivered_count: row.get::<_, i64>(2)? as u32,
                delivered_count: row.get::<_, i64>(3)? as u32,
                parked: row.get(4)?,
            })
        },
    )
    .optional()
    .expect("messaging: lookup_message_for_authorship")
}

/// Delete all undelivered pending-push rows for `message_id`. Returns the
/// count of rows deleted (0 if none were pending). Per design §2.2, cancel
/// DELETEs rather than marking delivered so `delivered_at IS NOT NULL`
/// retains its "bridge actually accepted" meaning.
///
/// `caller_sender` is re-checked here as defence in depth: the DELETE is
/// scoped to rows whose parent message row still carries the expected sender.
/// Protects against a future retention sweep or sender-rename feature that
/// could cause an in-flight cancel to touch a row whose ownership has changed.
pub fn cancel_pending_pushes_for_message(
    conn: &Connection,
    message_id: i64,
    caller_sender: &str,
) -> u32 {
    let affected = conn
        .execute(
            "DELETE FROM messaging_pending_pushes
             WHERE message_id = ?1
               AND delivered_at IS NULL
               AND EXISTS (
                   SELECT 1 FROM messaging_messages
                   WHERE id = ?1 AND sender = ?2
               )",
            rusqlite::params![message_id, caller_sender],
        )
        .expect("messaging: cancel_pending_pushes_for_message");
    affected as u32
}

/// Withdraw a parked message: delete the row itself, and any push claims still
/// attached to it.
///
/// Cancelling a parked message must erase it rather than unschedule it. Its
/// `deliver_after` is the only thing hiding it from retention reads, so a row
/// left behind would surface as ambience — and be minted claims — at its
/// release time, delivering exactly what the sender cancelled.
///
/// `caller_sender` is re-checked here as defence in depth, as in
/// [`cancel_pending_pushes_for_message`]. Returns the number of push claims
/// deleted with it, or `None` when the message is no longer parked (a release
/// pass took it between the caller's lookup and this call).
pub fn withdraw_parked_message(
    conn: &Connection,
    message_id: i64,
    caller_sender: &str,
) -> Option<u32> {
    let tx = conn
        .unchecked_transaction()
        .expect("messaging: begin withdraw_parked_message tx");
    let claims = tx
        .execute(
            "DELETE FROM messaging_pending_pushes
             WHERE message_id = ?1
               AND EXISTS (
                   SELECT 1 FROM messaging_messages
                   WHERE id = ?1 AND sender = ?2 AND deliver_after IS NOT NULL
               )",
            rusqlite::params![message_id, caller_sender],
        )
        .expect("messaging: withdraw parked push rows");
    let removed = tx
        .execute(
            "DELETE FROM messaging_messages
             WHERE id = ?1 AND sender = ?2 AND deliver_after IS NOT NULL",
            rusqlite::params![message_id, caller_sender],
        )
        .expect("messaging: withdraw parked message row");
    tx.commit()
        .expect("messaging: commit withdraw_parked_message");
    (removed > 0).then_some(claims as u32)
}

/// Resolved fields for an in-place message edit. `None` means "leave column
/// unchanged". For nullable columns the inner `Option` encodes the new value
/// (`Some(v)` to set, `None` to clear / write SQL NULL).
pub struct EditFieldsApplied<'a> {
    pub body: Option<&'a str>,
    pub reply_to_uuid: Option<Option<Uuid>>,
    pub deliver_after: Option<Option<DateTime<Utc>>>,
    pub delivery_deadline: Option<Option<DateTime<Utc>>>,
    pub urgency: Option<Urgency>,
}

/// Outcome of `update_message_and_pending_pushes`.
#[derive(Debug, PartialEq, Eq)]
pub enum EditUpdateResult {
    /// Edit applied; `affected_pushes` is the count of push rows updated.
    Ok { affected_pushes: u32 },
    /// At least one push already has `delivered_at IS NOT NULL`. Transaction
    /// was rolled back; no rows were changed.
    AnyDelivered,
    /// No undelivered push rows existed (cancelled, or zero-target broadcast).
    /// Commit happened but nothing changed.
    NoPendingPushes,
}

/// Atomically update the message row and its undelivered push rows.
///
/// Opens its own transaction. Re-checks "no delivered pushes" inside the
/// transaction per design §3.1 (A3 contract). FTS trigger fires automatically
/// when `body` changes (§2.6).
///
/// When `fields.urgency` is `Some`, this function recomputes the per-push
/// effective `eager_wake` inside the same transaction via a correlated UPDATE
/// against `messaging_subscriptions` (§3.5). The caller does not need a
/// second lock acquisition for recomputation.
///
/// `caller_sender` is pre-validated by the caller (`edit.rs:165-167`) before
/// reaching this function. This function asserts the invariant once at the top
/// of the transaction and panics on violation; push-row UPDATEs are unguarded
/// by design (the single up-front assert is the only sender check).
pub fn update_message_and_pending_pushes(
    conn: &Connection,
    message_id: i64,
    caller_sender: &str,
    fields: &EditFieldsApplied,
) -> EditUpdateResult {
    let tx = conn
        .unchecked_transaction()
        .expect("messaging: begin edit tx");

    // Invariant: the caller has already validated that caller_sender owns this
    // message. Assert once at the top; a mismatch or missing row is a programmer
    // bug — panic rather than silently no-op.
    // Defence-in-depth: re-read sender inside the edit transaction so the assert
    // fires under the same lock scope as the UPDATE. The caller's prior
    // lookup_message_for_authorship runs outside this transaction, so a
    // concurrent mutation (however unlikely) would be caught here.
    let stored: Option<(String, bool)> = tx
        .query_row(
            "SELECT sender, deliver_after IS NOT NULL FROM messaging_messages WHERE id = ?1",
            rusqlite::params![message_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .expect("messaging: sender fetch failed");
    let parked = match stored {
        None => panic!("messaging: edit row missing — message_id={message_id}"),
        Some((ref stored, _)) if stored != caller_sender => panic!(
            "messaging: edit sender mismatch — caller_sender={caller_sender:?} \
             stored={stored:?} message_id={message_id}"
        ),
        Some((_, parked)) => parked,
    };

    // A3: fail if any push has been delivered. Merged into one query using
    // FILTER (WHERE ...) expressions (SQLite ≥3.30) to halve index seeks.
    let (any_delivered, undelivered_count): (i64, i64) = tx
        .query_row(
            "SELECT COUNT(*) FILTER (WHERE delivered_at IS NOT NULL),
                    COUNT(*) FILTER (WHERE delivered_at IS NULL)
             FROM messaging_pending_pushes
             WHERE message_id = ?1",
            rusqlite::params![message_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("messaging: check delivered/undelivered counts");
    if any_delivered > 0 {
        tx.rollback().expect("messaging: rollback edit tx");
        return EditUpdateResult::AnyDelivered;
    }
    // A parked message is editable while holding no claims at all: it is owed to
    // nobody until release mints them.
    if undelivered_count == 0 && !parked {
        tx.commit().expect("messaging: commit edit tx (no pushes)");
        return EditUpdateResult::NoPendingPushes;
    }

    // Build and execute message UPDATE if any message-row field changes.
    // We always execute an UPDATE; if no fields were supplied the caller
    // should have checked NoFieldsProvided before reaching here.
    let mut set_clauses: Vec<&str> = Vec::new();
    let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

    if let Some(body) = fields.body {
        set_clauses.push("body = ?");
        params.push(Box::new(body.to_string()));
    }
    if let Some(reply_to) = &fields.reply_to_uuid {
        set_clauses.push("reply_to_uuid = ?");
        let bytes = reply_to.map(|u| u.as_bytes().to_vec());
        params.push(Box::new(bytes));
    }
    if let Some(da) = &fields.deliver_after {
        set_clauses.push("deliver_after = ?");
        params.push(Box::new(da.map(format_ts_for_db)));
        if da.is_some() {
            // Parking a message that already entered retention withdraws it from
            // retention: a parked message holds no position, and release assigns
            // it a fresh one above every entry made while it waited. Leaving the
            // old position behind would keep the message replayable during its
            // park, hand it a second position at release, and expose it to a GC
            // pass that only ever evicts positioned rows.
            set_clauses.push("retained_seq = NULL");
        }
    }
    if let Some(dd) = &fields.delivery_deadline {
        set_clauses.push("delivery_deadline = ?");
        params.push(Box::new(dd.map(format_ts_for_db)));
    }
    if let Some(urgency) = &fields.urgency {
        set_clauses.push("urgency = ?");
        params.push(Box::new(urgency.as_str().to_string()));
    }

    if !set_clauses.is_empty() {
        // Bind message_id after SET params.
        params.push(Box::new(message_id));
        let sql = format!(
            "UPDATE messaging_messages SET {} WHERE id = ?",
            set_clauses.join(", ")
        );
        let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| &**p as _).collect();
        let updated = tx
            .execute(&sql, rusqlite::params_from_iter(param_refs))
            .expect("messaging: update message row");
        // Belt-and-suspenders: the top-of-function assert confirmed the row exists.
        // Zero rows updated would mean the row vanished mid-transaction — an
        // impossible event (single-writer Tokio mutex) and thus an invariant violation.
        assert_eq!(
            updated, 1,
            "messaging: edit message row vanished mid-transaction — message_id={message_id}"
        );
    }

    // Update undelivered push rows for the fields that propagate to pushes.
    let mut push_set: Vec<&str> = Vec::new();
    let mut push_params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

    if let Some(da) = &fields.deliver_after {
        push_set.push("release_after = ?");
        push_params.push(Box::new(da.map(format_ts_for_db)));
    }
    if let Some(dd) = &fields.delivery_deadline {
        push_set.push("delivery_deadline = ?");
        push_params.push(Box::new(dd.map(format_ts_for_db)));
    }
    // Note: urgency is NOT included in push_set here — it is handled below
    // via a correlated UPDATE inside the same transaction (see §3.5 fix).

    if !push_set.is_empty() {
        // Bind message_id after SET params. Sender already asserted at top.
        push_params.push(Box::new(message_id));
        // Param count = push_set.len() (SET clauses) + 1 (message_id).
        assert_eq!(
            push_params.len(),
            push_set.len() + 1,
            "push_params/push_set mismatch: {push_set:?}"
        );
        let sql = format!(
            "UPDATE messaging_pending_pushes SET {} \
             WHERE message_id = ? AND delivered_at IS NULL",
            push_set.join(", ")
        );
        let param_refs: Vec<&dyn rusqlite::ToSql> = push_params.iter().map(|p| &**p as _).collect();
        tx.execute(&sql, rusqlite::params_from_iter(param_refs))
            .unwrap_or_else(|e| {
                panic!(
                    "messaging: update pending pushes failed \
                     (clauses={push_set:?}, param_count={}): {e}",
                    push_params.len()
                )
            });
    }

    // §3.5: if urgency was updated, recompute per-push effective eager_wake inside
    // this same transaction via a correlated UPDATE against messaging_subscriptions.
    // eager_wake = 1 iff:
    //   - subscriber is push-enabled (push_depth = 'unbounded' OR CAST > 0)
    //   - AND wake_min != 'never'
    //   - AND rank(urgency) >= rank(wake_min) (SQL CASE mapping)
    //   - AND (missing subscription row ⇒ COALESCE 0, preserving downgrade-to-0 default)
    // Rank mapping: very-low=0, low=1, normal=2, high=3 (matches WakeMin::wakes).
    // Sender already asserted at top.
    if let Some(new_urgency) = &fields.urgency {
        let urgency_rank: i64 = new_urgency.rank();
        tx.execute(
            "UPDATE messaging_pending_pushes AS p
             SET eager_wake = COALESCE(
                 (SELECT CASE
                      WHEN (s.push_depth = 'unbounded' OR CAST(s.push_depth AS INTEGER) > 0)
                       AND s.wake_min != 'never'
                       AND CASE s.wake_min
                               WHEN 'very-low' THEN 0
                               WHEN 'low'      THEN 1
                               WHEN 'normal'   THEN 2
                               WHEN 'high'     THEN 3
                           END <= ?1
                      THEN 1
                      ELSE 0
                  END
                  FROM messaging_subscriptions s
                  JOIN messaging_messages m ON m.channel_uuid = s.channel_uuid
                  WHERE m.id = p.message_id
                    AND s.app_slug = p.target_app_slug),
                 0
             )
             WHERE p.message_id = ?2 AND p.delivered_at IS NULL",
            rusqlite::params![urgency_rank, message_id],
        )
        .expect("messaging: recompute push eager_wake in edit tx");
    }

    tx.commit().expect("messaging: commit edit tx");
    EditUpdateResult::Ok {
        affected_pushes: undelivered_count as u32,
    }
}

/// Deserialize a `MessageEnvelope` from a row with columns:
/// 0:uuid, 1:source, 2:sender, 3:body, 4:urgency, 5:delivery_deadline,
/// 6:deliver_after, 7:publish_ts_ns, 8:channel_address, 9:reply_to, 10:envelope_type.
pub(super) fn row_to_message_envelope(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<MessageEnvelope> {
    let msg_uuid_bytes: Vec<u8> = row.get(0)?;
    let source: String = row.get(1)?;
    let sender: String = row.get(2)?;
    let body: String = row.get(3)?;
    let urgency_str: String = row.get(4)?;
    let delivery_deadline_s: Option<String> = row.get(5)?;
    let deliver_after_s: Option<String> = row.get(6)?;
    let publish_ts_ns: i64 = row.get(7)?;
    let channel_address: String = row.get(8)?;
    let reply_to: Option<String> = row.get(9)?;
    let envelope_type_str: String = row.get(10)?;

    let message_id = Uuid::from_slice(&msg_uuid_bytes)
        .unwrap_or_else(|e| panic!("messaging: row uuid malformed: {e}"));
    let urgency = Urgency::parse(&urgency_str)
        .unwrap_or_else(|| panic!("messaging: invalid urgency in row: {urgency_str:?}"));
    let delivery_deadline = delivery_deadline_s.map(|s| {
        parse_rfc3339(&s).unwrap_or_else(|| panic!("messaging: invalid rfc3339 in db: {s:?}"))
    });
    let deliver_after = deliver_after_s.map(|s| {
        parse_rfc3339(&s).unwrap_or_else(|| panic!("messaging: invalid rfc3339 in db: {s:?}"))
    });
    let envelope_type =
        super::super::ChannelScheme::parse(&envelope_type_str).unwrap_or_else(|| {
            panic!("messaging: unknown envelope_type {envelope_type_str:?} — host wrote every row")
        });

    Ok(MessageEnvelope {
        message_id,
        source,
        channel: channel_address,
        sender,
        publish_ts: ns_to_utc(publish_ts_ns),
        body,
        reply_to,
        delivery_deadline,
        deliver_after,
        urgency,
        envelope_type,
    })
}

/// Load pending messages for a sender. Returns `MessageEnvelope`s sorted
/// ascending by `deliver_after NULLS FIRST, publish_ts_ns ASC`.
///
/// Pending means undelivered: a message with an undelivered push claim, or one
/// still parked behind a `deliver_after` — a parked message holds no claim
/// (release mints them for the subscribers attached then) and is the case this
/// listing exists to make editable.
///
/// `channel_uuid_filter`: caller has already resolved the channel address →
/// UUID; an unresolvable address short-circuits before calling this function.
pub fn list_pending_messages_for_sender(
    conn: &Connection,
    sender: &str,
    channel_uuid_filter: Option<Uuid>,
) -> Vec<MessageEnvelope> {
    let mut sql = String::from(
        "SELECT m.uuid, m.source, m.sender, m.body, m.urgency,
                m.delivery_deadline, m.deliver_after, m.publish_ts_ns,
                c.address, rc.address, m.envelope_type
         FROM messaging_messages m
         JOIN messaging_channels c ON c.uuid = m.channel_uuid
         LEFT JOIN messaging_channels rc ON rc.uuid = m.reply_to_uuid
         WHERE (m.deliver_after IS NOT NULL
                OR EXISTS (SELECT 1 FROM messaging_pending_pushes pp
                           WHERE pp.message_id = m.id AND pp.delivered_at IS NULL))
           AND m.sender = ?
         ",
    );
    let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(sender.to_string())];

    if let Some(uuid) = channel_uuid_filter {
        sql.push_str("AND m.channel_uuid = ? ");
        params.push(Box::new(uuid.as_bytes().to_vec()));
    }

    sql.push_str("ORDER BY m.deliver_after ASC NULLS FIRST, m.publish_ts_ns ASC");

    let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| &**p as _).collect();
    let mut stmt = conn
        .prepare(&sql)
        .expect("prepare list_pending_messages_for_sender");
    let rows = stmt
        .query_map(
            rusqlite::params_from_iter(param_refs),
            row_to_message_envelope,
        )
        .expect("query list_pending_messages_for_sender");
    rows.map(|r| r.expect("read pending message row")).collect()
}

/// Load a single `MessageEnvelope` by message UUID. Returns `None` if the
/// message does not exist. Used by `Messenger::edit` to reload the envelope
/// after dispatch may have consumed the pending row.
pub fn load_envelope_by_uuid(conn: &Connection, uuid: Uuid) -> Option<MessageEnvelope> {
    let uuid_bytes = uuid.as_bytes().to_vec();
    conn.query_row(
        "SELECT m.uuid, m.source, m.sender, m.body, m.urgency,
                m.delivery_deadline, m.deliver_after, m.publish_ts_ns,
                c.address, rc.address, m.envelope_type
         FROM messaging_messages m
         JOIN messaging_channels c ON c.uuid = m.channel_uuid
         LEFT JOIN messaging_channels rc ON rc.uuid = m.reply_to_uuid
         WHERE m.uuid = ?1",
        rusqlite::params![uuid_bytes],
        row_to_message_envelope,
    )
    .optional()
    .expect("load_envelope_by_uuid")
}
