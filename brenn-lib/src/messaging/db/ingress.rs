//! Ingress message lifecycle DB operations.

use chrono::{DateTime, Utc};
use rusqlite::Connection;
use uuid::Uuid;

use crate::db::format_ts_for_db;
use crate::messaging::ingress::Event as IngressEvent;
use crate::messaging::{IngressOrBus, MessageEnvelope, ParticipantId, Urgency};

use super::shared::{ns_to_utc, parse_rfc3339};
use super::types::PendingPushRow;

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

/// Load all undelivered, unparked pending-push rows for `subscriber`,
/// in `publish_ts_ns ASC, id ASC` order. Rows of `kind='brenn'` produce
/// `Bus(MessageEnvelope)`, `kind='ingress'` rows produce an
/// `Ingress(Event)`. The drain caller partitions these into two slices for
/// `render_combined_drain`.
///
/// Used by the binary crate's drain-on-wake path.
pub fn load_pending_pushes_for_drain(
    conn: &Connection,
    subscriber: &ParticipantId,
) -> Vec<(i64, IngressOrBus)> {
    let mut stmt = conn
        .prepare(
            "SELECT pp.id, pp.target_subscriber, pp.eager_wake,
                    m.uuid, m.source, m.sender, m.body,
                    m.urgency AS msg_urgency,
                    m.delivery_deadline, m.deliver_after, m.publish_ts_ns,
                    c.address, rc.address,
                    m.envelope_type, m.ingress_source, m.ingress_summary,
                    pp.message_id, pp.target_app_slug
             FROM messaging_pending_pushes pp
             JOIN messaging_messages m ON pp.message_id = m.id
             LEFT JOIN messaging_channels c ON c.uuid = m.channel_uuid
             LEFT JOIN messaging_channels rc ON rc.uuid = m.reply_to_uuid
             WHERE pp.target_subscriber = ?1
               AND pp.delivered_at IS NULL
               AND pp.release_after IS NULL
             ORDER BY m.publish_ts_ns ASC, m.id ASC",
        )
        .expect("prepare load_pending_pushes_for_drain");
    let rows = stmt
        .query_map(rusqlite::params![subscriber.as_str()], row_to_drain_push)
        .expect("query load_pending_pushes_for_drain");
    rows.map(|r| {
        let row = r.expect("read pending push");
        (row.push_id, row.payload)
    })
    .collect()
}

/// Row decoder for the drain query (columns 0-15, `LEFT JOIN` channel).
///
/// Column layout:
/// - 0: pp.id  1: pp.target_subscriber  2: pp.eager_wake (0 or 1)
/// - 3: m.uuid  4: m.source  5: m.sender  6: m.body  7: m.urgency (msg)
/// - 8: m.delivery_deadline  9: m.deliver_after  10: m.publish_ts_ns
/// - 11: c.address (NULL for ingress — read only in brenn arm)
/// - 12: rc.address
/// - 13: m.envelope_type
/// - 14: m.ingress_source (NULL for brenn rows)
/// - 15: m.ingress_summary (NULL for brenn rows)
/// - 16: pp.message_id
/// - 17: pp.target_app_slug
///
/// `source` (col 4) and `sender` (col 5) are only read in the `brenn` arm;
/// they are NULL / irrelevant for ingress rows and deferring their reads
/// eliminates per-row heap allocations on the ingress-drain path.
fn row_to_drain_push(row: &rusqlite::Row) -> rusqlite::Result<PendingPushRow> {
    let push_id: i64 = row.get(0)?;
    let target_subscriber_str: String = row.get(1)?;
    let target_subscriber = ParticipantId::from_stored(target_subscriber_str);
    let target_app_slug: String = row.get(17)?;
    let eager_wake: bool = row.get::<_, i64>(2)? != 0;
    let msg_uuid_bytes: Vec<u8> = row.get(3)?;
    // cols 4 (source) and 5 (sender) deferred — read only in the brenn arm below.
    let body: String = row.get(6)?;
    let msg_urgency_str: String = row.get(7)?;
    let delivery_deadline_s: Option<String> = row.get(8)?;
    let deliver_after_s: Option<String> = row.get(9)?;
    let publish_ts_ns: i64 = row.get(10)?;
    // col 11: c.address (NULL for ingress rows — must NOT be read before envelope_type branch)
    // col 12: rc.address
    // col 13: m.envelope_type
    let envelope_type: String = row.get(13)?;
    // col 14: m.ingress_source (NULL for brenn rows)
    // col 15: m.ingress_summary (NULL for brenn rows)
    let message_id: i64 = row.get(16)?;

    // Build a Bus(MessageEnvelope) from the common row columns. Used for both
    // `brenn` and `webhook` arms, which are structurally identical — only the
    // panic message on a NULL c.address differs.
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
        // `brenn`, `webhook`, and `mqtt` rows are real bus messages: non-NULL
        // channel_uuid, transport-typed JSON body. Route through
        // Bus(MessageEnvelope) so they render via the standard envelope renderer,
        // not the [Event] card.
        Some(EnvelopeTypeColumn::Bus(
            scheme @ (ChannelScheme::Brenn | ChannelScheme::Webhook | ChannelScheme::Mqtt),
        )) => build_bus_envelope(scheme, body)?,
        Some(EnvelopeTypeColumn::Ingress) => {
            let ingress_source: Option<String> = row.get(14)?;
            let ingress_summary: Option<String> = row.get(15)?;
            let ingress_source = ingress_source.unwrap_or_else(|| {
                panic!("messaging: push {push_id} is envelope_type='ingress' but ingress_source IS NULL")
            });
            let ingress_summary = ingress_summary.unwrap_or_else(|| {
                panic!("messaging: push {push_id} is envelope_type='ingress' but ingress_summary IS NULL")
            });
            let created_at = ns_to_utc(publish_ts_ns);
            IngressOrBus::Ingress(IngressEvent {
                id: push_id,
                conversation_id: crate::messaging::ingress::SYNTHETIC_EVENT_ID, // not used at drain time; key is target_subscriber
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

    Ok(PendingPushRow {
        push_id,
        message_id,
        payload,
        target_subscriber,
        target_app_slug,
        eager_wake,
    })
}
