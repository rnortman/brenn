//! Ingress message lifecycle DB operations.

use chrono::{DateTime, Utc};
use rusqlite::Connection;
use uuid::Uuid;

use crate::db::format_ts_for_db;
use crate::messaging::ingress::Event as IngressEvent;
use crate::messaging::{ParticipantId, Urgency};

use super::shared::ns_to_utc;

/// Insert a `kind='ingress'` message row plus exactly one pending-push row
/// keyed to `subscriber`, without wrapping in a new transaction. Callers
/// must supply a connection that is already within a transaction if
/// atomicity is required.
///
/// Prefer calling this inside an existing transaction (e.g. from
/// `repo_sync_cursor::upsert_and_enqueue`). For standalone, one-shot use
/// without an outer transaction, use `insert_ingress_message`.
///
/// The `urgency` parameter is the sender-intent level stored on the message row.
/// `eager_wake` for the single push row is resolved as `urgency >= Urgency::Normal`
/// (the default `wake_min` threshold). The ingress path has no `SubscriberEntry`
/// so `WakeMin` is not consulted; `Normal` as threshold is parity with old
/// `Immediate → eager_wake=1` / `None → eager_wake=0` via the §2.7 mapping.
#[allow(clippy::too_many_arguments)]
pub fn insert_ingress_message_raw(
    conn: &Connection,
    subscriber: &ParticipantId,
    app_slug: &str,
    source: &str,
    summary: &str,
    payload: &str,
    urgency: Urgency,
    publish_ts_ns: i64,
) -> (i64, i64) {
    let now = format_ts_for_db(Utc::now());
    let uuid = Uuid::new_v4();
    let uuid_bytes = uuid.as_bytes().to_vec();

    // Insert message row with envelope_type='ingress', channel_uuid=NULL.
    conn.execute(
        "INSERT INTO messaging_messages
         (uuid, channel_uuid, source, sender, body, urgency, reply_to_uuid,
          delivery_deadline, deliver_after, publish_ts_ns, created_at,
          envelope_type, ingress_source, ingress_summary)
         VALUES (?1, NULL, '', '', ?2, ?3, NULL,
                 NULL, NULL, ?4, ?5,
                 ?6, ?7, ?8)",
        rusqlite::params![
            uuid_bytes,
            payload,
            urgency.as_str(),
            publish_ts_ns,
            now,
            super::EnvelopeTypeColumn::Ingress.as_str(),
            source,
            summary,
        ],
    )
    .expect("messaging: insert ingress message");
    let message_id = conn.last_insert_rowid();

    // Insert exactly one pending-push row.
    // This path carries direct conversation-targeted ingress (repo-sync
    // notifications, automation error reports) with no subscription record, so
    // there is no per-subscriber wake_min to honour here; waking at Normal is
    // the intended behaviour for these direct rows.
    // Parity: Immediate→Normal (eager), None→Low (parked).
    let eager_wake: i64 = if urgency >= Urgency::Normal { 1 } else { 0 };
    conn.execute(
        "INSERT INTO messaging_pending_pushes
         (message_id, target_subscriber, target_app_slug, eager_wake,
          delivery_deadline, release_after, created_at)
         VALUES (?1, ?2, ?3, ?4, NULL, NULL, ?5)",
        rusqlite::params![message_id, subscriber.as_str(), app_slug, eager_wake, now,],
    )
    .expect("messaging: insert ingress pending push");
    let push_id = conn.last_insert_rowid();

    (message_id, push_id)
}

/// Insert a `kind='ingress'` message row plus exactly one pending-push row
/// keyed to `subscriber` in a single transaction. Returns `(message_id, push_id)`.
///
/// **No channel, no sender gate, no budget** — ingress is not subject to
/// those constraints (design §2.3).
///
/// Use this for standalone, one-shot inserts where no outer transaction exists.
/// When inserting inside an existing transaction, use `insert_ingress_message_raw`.
#[allow(clippy::too_many_arguments)]
pub fn insert_ingress_message(
    conn: &Connection,
    subscriber: &ParticipantId,
    app_slug: &str,
    source: &str,
    summary: &str,
    payload: &str,
    urgency: Urgency,
    publish_ts_ns: i64,
) -> (i64, i64) {
    let tx = conn
        .unchecked_transaction()
        .expect("messaging: begin ingress tx");
    let result = insert_ingress_message_raw(
        &tx,
        subscriber,
        app_slug,
        source,
        summary,
        payload,
        urgency,
        publish_ts_ns,
    );
    tx.commit().expect("messaging: commit ingress tx");
    result
}

/// Delete delivered ingress push rows older than `cutoff`, then orphan-reap
/// ingress message rows with no remaining push rows.
///
/// **Scope fence:** only ingress rows (`kind='ingress'`) are touched.
/// Bus rows (`kind='brenn'`) keep their current never-deleted behavior.
///
/// Returns `(pushes_deleted, messages_deleted)`.
pub fn delete_delivered_ingress_pushes_before(
    conn: &Connection,
    cutoff: DateTime<Utc>,
) -> (usize, usize) {
    let cutoff_str = format_ts_for_db(cutoff);
    // Step 1: delete delivered ingress push rows older than cutoff.
    let pushes_deleted = conn
        .execute(
            "DELETE FROM messaging_pending_pushes
             WHERE delivered_at IS NOT NULL
               AND delivered_at < ?1
               AND message_id IN (
                   SELECT id FROM messaging_messages WHERE envelope_type = 'ingress'
               )",
            rusqlite::params![cutoff_str],
        )
        .expect("messaging: delete_delivered_ingress_pushes_before (push step)");

    // Step 2: orphan-reap ingress message rows whose last push is now gone.
    // The kind='ingress' guard ensures bus message rows are never touched.
    // NOT EXISTS with the FK index on pp.message_id probes per-message rather
    // than materialising the full message_id set from messaging_pending_pushes
    // (which grows unboundedly with bus history); cost is tied to ingress volume.
    let messages_deleted = conn
        .execute(
            "DELETE FROM messaging_messages
             WHERE envelope_type = 'ingress'
               AND NOT EXISTS (
                   SELECT 1 FROM messaging_pending_pushes pp
                   WHERE pp.message_id = messaging_messages.id
               )",
            [],
        )
        .expect("messaging: delete_delivered_ingress_pushes_before (message step)");

    (pushes_deleted, messages_deleted)
}

/// Periodic janitor: mark stale undelivered `repo_sync:*` ingress push rows as
/// delivered so abandoned conversations don't accumulate orphaned rows.
///
/// Mirrors `event_queue::mark_stale_undelivered_repo_sync_events` but operates
/// on the unified ingress store. Uses `ParticipantId::as_conversation_id` to
/// validate `target_subscriber` shapes, then delegates to set-based SQL that
/// joins `conversations.updated_at` directly — no per-row SELECT under the lock.
///
/// Two-phase approach:
/// 1. Set-based UPDATE marking stale rows for conversations that exist but are
///    older than `staleness_days`. Single SQL statement, O(repo_sync ingress
///    volume) with the FK index.
/// 2. Warn + mark for orphaned pushes whose conversation row is absent (rare;
///    only occurs when a conversation is deleted while a push is in-flight).
///
/// Returns the number of push rows marked delivered.
///
/// # Panics
///
/// Panics (fail-fast) if any `repo_sync:*` ingress push has a non-conversation
/// `ParticipantId` — today all ingress is `conversation:<id>`, so this never fires;
/// if a future non-conversation subscriber owns a `repo_sync` push, it panics
/// rather than silently mis-handling (per CLAUDE.md fail-fast).
pub fn mark_stale_undelivered_ingress_repo_sync(conn: &Connection, staleness_days: u64) -> usize {
    use crate::messaging::ingress::{MAX_REPO_SYNC_STALENESS_DAYS, REPO_SYNC_SOURCE_PREFIX};
    assert!(
        staleness_days <= MAX_REPO_SYNC_STALENESS_DAYS,
        "staleness_days={staleness_days} exceeds safe arithmetic range"
    );
    // staleness_secs as TEXT for SQLite datetime arithmetic:
    //   unixepoch(updated_at) <= unixepoch('now') - staleness_secs
    let staleness_secs = staleness_days as i64 * 86_400_i64;
    let source_pattern = format!("{REPO_SYNC_SOURCE_PREFIX}%");
    let now_str = crate::db::format_ts_for_db(chrono::Utc::now());

    // Phase 1: set-based UPDATE for pushes whose conversation exists and is stale.
    // `CAST(substr(pp.target_subscriber, 14) AS INTEGER)` extracts the conv_id
    // from 'conversation:<id>' — the fail-fast panic below catches any non-matching
    // shape *before* production rows can reach this path. The substr offset (14) is
    // len("conversation:") + 1.
    let stale_count = conn
        .execute(
            "UPDATE messaging_pending_pushes
             SET delivered_at = ?1
             WHERE delivered_at IS NULL
               AND id IN (
                   SELECT pp.id
                   FROM messaging_pending_pushes pp
                   JOIN messaging_messages m ON pp.message_id = m.id
                   JOIN conversations c
                        ON c.id = CAST(substr(pp.target_subscriber, 14) AS INTEGER)
                   WHERE m.envelope_type = 'ingress'
                     AND m.ingress_source LIKE ?2
                     AND pp.delivered_at IS NULL
                     AND pp.target_subscriber LIKE 'conversation:%'
                     AND (unixepoch(?1) - unixepoch(c.updated_at)) > ?3
               )",
            rusqlite::params![now_str, source_pattern, staleness_secs],
        )
        .expect("mark_stale_undelivered_ingress_repo_sync: stale UPDATE");

    // Phase 2: handle orphaned pushes (conversation row absent). These cannot
    // be delivered or reaped, so we mark them delivered and warn.
    let mut orphan_stmt = conn
        .prepare(
            "SELECT pp.id, pp.target_subscriber \
             FROM messaging_pending_pushes pp \
             JOIN messaging_messages m ON pp.message_id = m.id \
             WHERE m.envelope_type = 'ingress' \
               AND m.ingress_source LIKE ?1 \
               AND pp.delivered_at IS NULL \
               AND pp.target_subscriber LIKE 'conversation:%' \
               AND NOT EXISTS (
                   SELECT 1 FROM conversations c
                   WHERE c.id = CAST(substr(pp.target_subscriber, 14) AS INTEGER)
               )",
        )
        .expect("mark_stale_undelivered_ingress_repo_sync: orphan prepare");

    let orphans: Vec<(i64, String)> = orphan_stmt
        .query_map(rusqlite::params![source_pattern], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .expect("mark_stale_undelivered_ingress_repo_sync: orphan query")
        .map(|r| r.expect("mark_stale_undelivered_ingress_repo_sync: orphan row"))
        .collect();
    drop(orphan_stmt);

    let orphan_count = orphans.len();
    for (push_id, subscriber_str) in &orphans {
        // Fail-fast: validate conversation: prefix (panics on unknown shapes).
        let _conv_id = ParticipantId::from_stored(subscriber_str.clone()).as_conversation_id();
        tracing::warn!(
            push_id,
            subscriber = subscriber_str,
            "mark_stale_undelivered_ingress_repo_sync: conversation absent; \
             marking push delivered so cleanup can reap it"
        );
        let now_str2 = crate::db::format_ts_for_db(chrono::Utc::now());
        conn.execute(
            "UPDATE messaging_pending_pushes SET delivered_at = ?1 WHERE id = ?2",
            rusqlite::params![now_str2, push_id],
        )
        .expect("mark_stale_undelivered_ingress_repo_sync: UPDATE (absent conv)");
    }

    stale_count + orphan_count
}

/// SQL for the drain read. Shared with its plan-assertion test so the test can
/// never drift from the production query.
///
/// The `pp` conjuncts match `idx_messaging_pending_pushes_ingress_undelivered`'s
/// partial predicate, so the planner can qualify that index rather than scanning
/// the table for one subscriber's rows.
pub(crate) const LOAD_PENDING_INGRESS_FOR_DRAIN_SQL: &str = "SELECT pp.id, m.body, m.publish_ts_ns,
                    m.ingress_source, m.ingress_summary, m.envelope_type
             FROM messaging_pending_pushes pp
             JOIN messaging_messages m ON pp.message_id = m.id
             WHERE pp.target_subscriber = ?1
               AND pp.delivered_at IS NULL
               AND m.channel_uuid IS NULL
             ORDER BY m.publish_ts_ns ASC, m.id ASC";

/// Undelivered, channel-less ingress rows for one subscriber, oldest publish
/// first.
///
/// What a participant is owed on a *channel* is its cursor position, read
/// through its window, so the drain reads only the channel-less rows here.
/// `m.channel_uuid IS NULL` is the row-kind fence; a row inside it whose
/// `envelope_type` is not `ingress` means the fence and the decoder disagree —
/// a host bug, not a row to skip.
pub fn load_pending_ingress_for_drain(
    conn: &Connection,
    subscriber: &ParticipantId,
) -> Vec<(i64, IngressEvent)> {
    let mut stmt = conn
        .prepare(LOAD_PENDING_INGRESS_FOR_DRAIN_SQL)
        .expect("prepare ingress drain read");
    let rows = stmt
        .query_map(rusqlite::params![subscriber.as_str()], |row| {
            let push_id: i64 = row.get(0)?;
            let payload: String = row.get(1)?;
            let publish_ts_ns: i64 = row.get(2)?;
            let source: Option<String> = row.get(3)?;
            let summary: Option<String> = row.get(4)?;
            let envelope_type: String = row.get(5)?;
            assert!(
                matches!(
                    super::EnvelopeTypeColumn::parse(&envelope_type),
                    Some(super::EnvelopeTypeColumn::Ingress)
                ),
                "messaging: push {push_id} carries no channel but is envelope_type \
                 {envelope_type:?} — host wrote every row"
            );
            Ok((
                push_id,
                IngressEvent {
                    id: push_id,
                    // Not used at drain time; the key is target_subscriber.
                    conversation_id: crate::messaging::ingress::SYNTHETIC_EVENT_ID,
                    source: source.unwrap_or_else(|| {
                        panic!(
                            "messaging: push {push_id} is envelope_type='ingress' but \
                             ingress_source IS NULL"
                        )
                    }),
                    summary: summary.unwrap_or_else(|| {
                        panic!(
                            "messaging: push {push_id} is envelope_type='ingress' but \
                             ingress_summary IS NULL"
                        )
                    }),
                    payload,
                    created_at: ns_to_utc(publish_ts_ns),
                },
            ))
        })
        .expect("query ingress drain read");
    rows.map(|r| r.expect("read pending ingress row")).collect()
}
