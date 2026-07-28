use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension};
use uuid::Uuid;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt as _;

use super::super::store::OverflowEvent;
use super::super::{ChannelScheme, Impetus, MessageEnvelope, ParticipantId, Urgency};
use super::cursors;
use super::shared::{ns_to_utc, parse_rfc3339};
use super::types::PendingPushRow;
use crate::db::format_ts_for_db;

// ---------------------------------------------------------------------------
// Message insert
// ---------------------------------------------------------------------------

/// Inserted message metadata returned to the caller.
#[derive(Debug)]
pub struct InsertedMessage {
    pub id: i64,
    pub uuid: Uuid,
    pub publish_ts_ns: i64,
    /// The retention sequence assigned to this message, or `None` when it was
    /// parked (a parked message holds no retention position until it releases).
    pub retained_seq: Option<i64>,
}

/// Insert a message row in a single transaction. The caller has already
/// validated channel/sender/budget.
#[allow(clippy::too_many_arguments)]
pub fn insert_message(
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
    impetus: Option<Impetus>,
    publish_ts_ns: i64,
) -> InsertedMessage {
    let tx = conn.unchecked_transaction().expect("messaging: begin tx");
    let result = insert_message_in_tx(
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
        impetus,
        publish_ts_ns,
    );
    tx.commit().expect("messaging: commit tx");
    result
}

/// Insert a message row under a caller-owned `Transaction`. The caller is
/// responsible for BEGIN/COMMIT (or rollback on panic via the `Transaction`
/// Drop guard).
///
/// Used by `publish_from_wasm` to batch multiple messages into one outer
/// transaction. All other callers use `insert_message`.
#[allow(clippy::too_many_arguments)]
pub fn insert_message_in_tx(
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
    impetus: Option<Impetus>,
    publish_ts_ns: i64,
) -> InsertedMessage {
    let now = format_ts_for_db(Utc::now());
    let uuid = Uuid::new_v4();
    let uuid_bytes = uuid.as_bytes().to_vec();
    let channel_bytes = channel_uuid.as_bytes().to_vec();
    let reply_to_bytes = reply_to_uuid.map(|u| u.as_bytes().to_vec());
    let dd = delivery_deadline.map(format_ts_for_db);
    let da = deliver_after.map(format_ts_for_db);

    let retained_seq = match deliver_after {
        None => Some(allocate_retained_seq(tx, channel_uuid)),
        Some(_) => None,
    };

    tx.execute(
        "INSERT INTO messaging_messages
         (uuid, channel_uuid, source, sender, body, urgency, envelope_type,
          reply_to_uuid, delivery_deadline, deliver_after, publish_ts_ns, created_at,
          retained_seq, impetus)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
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
            impetus.map(Impetus::as_str),
        ],
    )
    .expect("messaging: insert message");
    let message_id = tx.last_insert_rowid();

    InsertedMessage {
        id: message_id,
        uuid,
        publish_ts_ns,
        retained_seq,
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

// ---------------------------------------------------------------------------
// Bus GC
// ---------------------------------------------------------------------------

/// What one channel's eviction pass deleted, and whom it cost.
#[derive(Debug, Default)]
pub struct BusGcEviction {
    pub messages_evicted: usize,
    /// One entry per cursor the evicted span passed, carrying the seqs that
    /// subscriber will now never be served. Empty when every position was above
    /// the span, and always empty for a pass that deleted nothing.
    pub overflow: Vec<OverflowEvent>,
}

/// Evict bus message bodies past the channel's reap frontier,
/// handling both `drop` and `archive` sinks in a single transaction.
///
/// **Scope fence:** only rows with the given `channel_uuid` and
/// `envelope_type != 'ingress'` are touched. `ingress` rows have
/// `channel_uuid = NULL` and are GC'd by a separate path; the predicate
/// here matches all channel-associated transport types (`brenn`, `webhook`,
/// future `mqtt`, etc.) without needing to enumerate them.
///
/// Steps (all in one `unchecked_transaction`):
/// 1. Count rows for the channel; if `<= frontier`, returns immediately.
/// 2. For `archive` sink: SELECT eligible bodies (NOT IN top-frontier set) and
///    write each as a JSONL line to `archive_path`.
/// 3. Delete eligible message rows using the same NOT IN predicate (FTS triggers
///    fire on each row DELETE, keeping the FTS index consistent).
/// 4. Report, per subscriber cursor this pass's eviction outran, how much of
///    its unseen span went with the bodies.
///
/// The report is the durable half of the channel model's eviction accounting:
/// retention outran a position, so the loss is attributable the moment it
/// happens rather than waiting for a read that may never come. Both frontiers
/// are pass-local — the pass knows exactly the span it deleted — so a wedged
/// subscriber is reported for each pass's own span and never twice for the
/// same seq. The cursors themselves are untouched: a position left below the
/// frontier *is* the record of what it lost.
///
/// Both subtractions in the accounting — this one and the advance's — rest on
/// retention being dense, and nothing perforates it: every message the pass's
/// span covers is deleted, so the new frontier is exactly where the pass left
/// retention.
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
) -> BusGcEviction {
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
        return BusGcEviction::default();
    }

    // Captured before the deletes, so the span this pass is about to evict is
    // exactly `old_frontier .. new_frontier`.
    let old_frontier = retention_frontier_or_next(&tx, channel_uuid);
    let cursors = cursors::channel_subscriber_cursors(&tx, channel_uuid);

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
                        rc.address, m.impetus
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
                    let impetus_str: Option<String> = row.get(9)?;
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
                        impetus_str,
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
                    impetus_str,
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
                let impetus = impetus_str.map(|s| {
                    Impetus::parse(&s)
                        .unwrap_or_else(|| panic!("bus_gc_evict_channel: invalid impetus: {s:?}"))
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
                    impetus,
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

    // Step 3: delete eligible message rows (FTS trigger fires on each DELETE).
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
               )",
            rusqlite::params![channel_uuid_bytes, frontier as i64],
        )
        .expect("bus_gc_evict_channel: DELETE messages");

    // Step 4: report the evicted span against every position it outran. Read
    // after the deletes and inside the same transaction, so the frontier pair
    // brackets precisely what this pass removed.
    let new_frontier = retention_frontier_or_next(&tx, channel_uuid);
    let overflow = cursors
        .into_iter()
        .filter_map(|cursor| {
            let unreported_from = cursor.next_owed_seq.max(old_frontier);
            let dropped = new_frontier.saturating_sub(unreported_from);
            (dropped > 0).then(|| OverflowEvent {
                subscriber: cursor.subscriber,
                dropped: u64::try_from(dropped).expect("bus_gc_evict_channel: negative drop span"),
                app_slug: Some(cursor.app_slug),
            })
        })
        .collect();

    tx.commit().expect("bus_gc_evict_channel: commit");
    BusGcEviction {
        messages_evicted: messages_deleted,
        overflow,
    }
}

/// The channel's retention frontier, or the seq it will assign next when it
/// retains nothing — so an emptied channel's frontier still bounds the span a
/// pass evicted rather than reading as "no eviction happened".
fn retention_frontier_or_next(conn: &Connection, channel_uuid: Uuid) -> i64 {
    channel_retention_frontier(conn, channel_uuid)
        .unwrap_or_else(|| channel_last_retained_seq(conn, channel_uuid) + 1)
}

/// Build a bare `?`-placeholder string for an IN clause of length `n`.
///
/// Returns `"?,?,?"` for `n=3`. Panics if `n == 0` — a caller with an empty
/// set must shape the clause itself rather than emit an empty `IN ()`.
fn build_bare_in_placeholders(n: usize) -> String {
    assert!(n > 0, "build_bare_in_placeholders: n must be > 0");
    std::iter::repeat_n("?", n).collect::<Vec<_>>().join(",")
}

// ---------------------------------------------------------------------------
// Pending-push queries
// ---------------------------------------------------------------------------

/// Maximum ids bound into a single `IN (…)` batch. SQLite's compiled
/// `SQLITE_MAX_VARIABLE_NUMBER` is 32766 on modern builds; staying well under it
/// (the mark also binds `now`) means an arbitrarily large batch is retired
/// across several statements instead of tripping a `prepare` error.
const MAX_IN_CLAUSE_IDS: usize = 30000;

/// Mark pending-push rows delivered — `delivered_at = now` where it is still
/// `NULL`, so a row already marked keeps its first timestamp. Idempotent. The id
/// list is batched under [`MAX_IN_CLAUSE_IDS`] so a large backlog never overflows
/// the SQLite bind-variable limit.
pub fn mark_pending_pushes_delivered(conn: &Connection, push_ids: &[i64]) {
    if push_ids.is_empty() {
        return;
    }
    let now = format_ts_for_db(Utc::now());
    for chunk in push_ids.chunks(MAX_IN_CLAUSE_IDS) {
        let sql = format!(
            "UPDATE messaging_pending_pushes SET delivered_at = ?1
             WHERE id IN ({}) AND delivered_at IS NULL",
            build_bare_in_placeholders(chunk.len()),
        );
        let mut stmt = conn
            .prepare(&sql)
            .expect("prepare mark_pending_pushes_delivered");
        // Params: now (?1) followed by the id list bound to the IN placeholders.
        let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::with_capacity(chunk.len() + 1);
        params.push(Box::new(now.clone()));
        for id in chunk {
            params.push(Box::new(*id));
        }
        let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| &**p as _).collect();
        stmt.execute(rusqlite::params_from_iter(param_refs))
            .expect("exec mark_pending_pushes_delivered");
    }
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
/// Selects the 14 envelope columns `query::row_to_envelope` decodes (0-13) plus
/// `m.id` (14) and `m.retained_seq` (15), newest-first, for channel `?1`.
/// `where_tail` adds predicates after the retained-set predicate; `limit_param`
/// names the bind holding the row cap. One copy so a change to the retained-set
/// predicate cannot land on some reads and miss others.
fn retained_window_sql(where_tail: &str, limit_param: &str) -> String {
    format!(
        "SELECT m.uuid, m.channel_uuid, m.source, m.sender, m.body, m.urgency,
                m.reply_to_uuid, m.delivery_deadline, m.deliver_after, m.publish_ts_ns,
                c.address, rc.address, m.envelope_type, m.impetus, m.id, m.retained_seq
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
                id: row.get(14)?,
                seq: row.get(15)?,
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

/// Test-only: the retention position of a committed message, by uuid.
///
/// Panics if the row is missing or still parked — callers ask about a message
/// they just published unparked, so either answer is a broken invariant rather
/// than a case to handle. Reading by uuid rather than through
/// [`channel_last_retained_seq`] is direct evidence: the channel high-water is a
/// proxy that a misassigned position would still satisfy.
#[cfg(any(test, feature = "testutils"))]
pub fn message_retained_seq(conn: &Connection, message_id: Uuid) -> i64 {
    conn.query_row(
        "SELECT retained_seq FROM messaging_messages WHERE uuid = ?1",
        rusqlite::params![message_id.as_bytes().to_vec()],
        |row| row.get::<_, Option<i64>>(0),
    )
    .unwrap_or_else(|e| panic!("messaging: message {message_id} has no row: {e}"))
    .unwrap_or_else(|| panic!("messaging: message {message_id} holds no retention position"))
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

/// SQL for the dispatcher's ingress scan. Shared with its plan-assertion test so
/// the test can never drift from the production query.
///
/// `m.channel_uuid IS NULL` is the row-kind fence: what a subscriber is owed on a
/// *channel* is its cursor position, walked by the wake pass, so the only rows
/// this table still carries — and the only ones the dispatcher still dispatches
/// — are the channel-less direct-to-participant ingress deliveries.
pub(crate) const LOAD_DISPATCHABLE_INGRESS_SQL: &str =
    "SELECT pp.id, pp.target_subscriber, pp.eager_wake,
                    m.body, m.publish_ts_ns,
                    m.ingress_source, m.ingress_summary,
                    m.envelope_type, pp.message_id, pp.target_app_slug
             FROM messaging_pending_pushes pp
             JOIN messaging_messages m ON pp.message_id = m.id
             WHERE pp.delivered_at IS NULL
               AND pp.eager_wake = 1
               AND m.channel_uuid IS NULL
             ORDER BY m.publish_ts_ns ASC";

/// Load the dispatchable ingress rows for the background dispatcher: undelivered,
/// eager-wake, channel-less.
///
/// A non-eager ingress row is delivered by the startup/reconnect drain, not by
/// the dispatcher's wake action, so loading it on every poll would be pure waste.
///
/// The scan is index-backed by `idx_messaging_pending_pushes_ingress_dispatch`,
/// whose partial predicate matches this query's `pp` conjuncts verbatim so SQLite's
/// planner qualifies the partial index; the plan-assertion test
/// `load_dispatchable_ingress_pushes_uses_partial_index` guards against a silent
/// regression to a full scan.
///
/// Results are ordered by `m.publish_ts_ns ASC` so a subscriber's rows reach its
/// bridge in publish order within the dispatcher's fan-out group.
pub fn load_dispatchable_ingress_pushes(conn: &Connection) -> Vec<PendingPushRow> {
    let mut stmt = conn
        .prepare(LOAD_DISPATCHABLE_INGRESS_SQL)
        .expect("prepare load_dispatchable_ingress_pushes");
    let rows = stmt
        .query_map([], row_to_ingress_push)
        .expect("query load_dispatchable_ingress_pushes");
    rows.map(|r| r.expect("read dispatchable ingress push"))
        .collect()
}

/// Row decoder for the dispatcher's ingress scan.
///
/// Column layout (matches [`LOAD_DISPATCHABLE_INGRESS_SQL`]):
/// - 0: pp.id  1: pp.target_subscriber  2: pp.eager_wake (0 or 1)
/// - 3: m.body  4: m.publish_ts_ns
/// - 5: m.ingress_source  6: m.ingress_summary
/// - 7: m.envelope_type  8: pp.message_id  9: pp.target_app_slug
///
/// A row whose `envelope_type` is not `ingress` would mean the channel-less
/// predicate and the row kind disagree — a host bug, not a row to skip.
fn row_to_ingress_push(row: &rusqlite::Row) -> rusqlite::Result<PendingPushRow> {
    let push_id: i64 = row.get(0)?;
    let target_subscriber = ParticipantId::from_stored(row.get::<_, String>(1)?);
    let eager_wake: bool = row.get::<_, i64>(2)? != 0;
    let body: String = row.get(3)?;
    let publish_ts_ns: i64 = row.get(4)?;
    let ingress_source: Option<String> = row.get(5)?;
    let ingress_summary: Option<String> = row.get(6)?;
    let envelope_type: String = row.get(7)?;
    let message_id: i64 = row.get(8)?;
    let target_app_slug: String = row.get(9)?;

    assert!(
        matches!(
            super::EnvelopeTypeColumn::parse(&envelope_type),
            Some(super::EnvelopeTypeColumn::Ingress)
        ),
        "messaging: push {push_id} carries no channel but is envelope_type \
         {envelope_type:?} — host wrote every row"
    );
    let source = ingress_source.unwrap_or_else(|| {
        panic!("messaging: push {push_id} is envelope_type='ingress' but ingress_source IS NULL")
    });
    let summary = ingress_summary.unwrap_or_else(|| {
        panic!("messaging: push {push_id} is envelope_type='ingress' but ingress_summary IS NULL")
    });

    Ok(PendingPushRow {
        push_id,
        message_id,
        event: crate::messaging::ingress::Event {
            id: push_id,
            conversation_id: crate::messaging::ingress::SYNTHETIC_EVENT_ID,
            source,
            summary,
            payload: body,
            created_at: ns_to_utc(publish_ts_ns),
        },
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
    /// The message is still parked behind a `deliver_after`, and so is still the
    /// sender's to withdraw or edit. A message that has entered retention is
    /// past recall: every subscriber reads it from its own position, and no
    /// server-side record of who has read it exists to revoke.
    pub parked: bool,
}

/// Look up a message by UUID for authorship / status checks.
/// Returns `None` if no row with that UUID exists.
pub fn lookup_message_for_authorship(conn: &Connection, uuid: Uuid) -> Option<MessageLookup> {
    let uuid_bytes = uuid.as_bytes().to_vec();
    conn.query_row(
        "SELECT id, sender, deliver_after IS NOT NULL
         FROM messaging_messages
         WHERE uuid = ?1",
        rusqlite::params![uuid_bytes],
        |row| {
            Ok(MessageLookup {
                message_id: row.get(0)?,
                sender: row.get(1)?,
                parked: row.get(2)?,
            })
        },
    )
    .optional()
    .expect("messaging: lookup_message_for_authorship")
}

/// Withdraw a parked message: delete the row.
///
/// Cancelling a parked message must erase it rather than unschedule it. Its
/// `deliver_after` is the only thing hiding it from retention reads, so a row
/// left behind would enter retention at its release time, delivering exactly
/// what the sender cancelled.
///
/// `caller_sender` is re-checked here as defence in depth: the DELETE is scoped
/// to a row that still carries the expected sender, so a future sender-rename
/// feature cannot let an in-flight cancel withdraw someone else's message.
/// Returns `false` when the message is no longer parked (a release pass took it
/// between the caller's lookup and this call).
pub fn withdraw_parked_message(conn: &Connection, message_id: i64, caller_sender: &str) -> bool {
    conn.execute(
        "DELETE FROM messaging_messages
         WHERE id = ?1 AND sender = ?2 AND deliver_after IS NOT NULL",
        rusqlite::params![message_id, caller_sender],
    )
    .expect("messaging: withdraw parked message row")
        > 0
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

/// Atomically update a parked message's row.
///
/// Opens its own transaction. The FTS trigger fires automatically when `body`
/// changes.
///
/// `caller_sender` is pre-validated by the caller (`edit.rs`) before reaching
/// this function. This function re-reads the sender inside its own transaction
/// and asserts the invariant, so the check and the UPDATE share one lock scope;
/// a mismatch or a missing row is a programmer bug and panics.
pub fn update_parked_message(
    conn: &Connection,
    message_id: i64,
    caller_sender: &str,
    fields: &EditFieldsApplied,
) {
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
    let stored: Option<String> = tx
        .query_row(
            "SELECT sender FROM messaging_messages WHERE id = ?1",
            rusqlite::params![message_id],
            |r| r.get(0),
        )
        .optional()
        .expect("messaging: sender fetch failed");
    match stored {
        None => panic!("messaging: edit row missing — message_id={message_id}"),
        Some(ref stored) if stored != caller_sender => panic!(
            "messaging: edit sender mismatch — caller_sender={caller_sender:?} \
             stored={stored:?} message_id={message_id}"
        ),
        Some(_) => {}
    }

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

    tx.commit().expect("messaging: commit edit tx");
}

/// Deserialize a `MessageEnvelope` from a row with columns:
/// 0:uuid, 1:source, 2:sender, 3:body, 4:urgency, 5:delivery_deadline,
/// 6:deliver_after, 7:publish_ts_ns, 8:channel_address, 9:reply_to,
/// 10:envelope_type, 11:impetus.
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
    let impetus_str: Option<String> = row.get(11)?;

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
    let impetus = impetus_str.map(|s| {
        Impetus::parse(&s)
            .unwrap_or_else(|| panic!("messaging: unknown impetus {s:?} — host wrote every row"))
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
        impetus,
        urgency,
        envelope_type,
    })
}

/// Load a sender's still-pending messages: the ones parked behind a
/// `deliver_after`, sorted ascending by `deliver_after, publish_ts_ns`.
///
/// Pending means not yet in retention. A message that has entered retention is
/// past recall — every subscriber reads it from its own position — so the parked
/// set is exactly the set this listing exists to make cancellable and editable.
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
                c.address, rc.address, m.envelope_type, m.impetus
         FROM messaging_messages m
         JOIN messaging_channels c ON c.uuid = m.channel_uuid
         LEFT JOIN messaging_channels rc ON rc.uuid = m.reply_to_uuid
         WHERE m.deliver_after IS NOT NULL
           AND m.sender = ?
         ",
    );
    let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(sender.to_string())];

    if let Some(uuid) = channel_uuid_filter {
        sql.push_str("AND m.channel_uuid = ? ");
        params.push(Box::new(uuid.as_bytes().to_vec()));
    }

    sql.push_str("ORDER BY m.deliver_after ASC, m.publish_ts_ns ASC");

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
                c.address, rc.address, m.envelope_type, m.impetus
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
