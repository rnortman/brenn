//! Per-subscriber cursor rows — where an attached, push-enabled subscriber
//! stands on one durable channel.
//!
//! A cursor is a bare position into the channel's dense retention order, and
//! nothing else: no delivery obligations, no drop ledger. It is created at
//! attach, moved only by an advance, and deleted at detach. A sampled
//! (`push_depth = 0`) subscriber is never delivered to and holds no row at all.
//!
//! The row is position state, not registration state. Who subscribes, at what
//! depths, with what noise rung lives in the registration directory; the two
//! caches carried here — `push_depth` and `app_slug` — have their authority
//! elsewhere (the caller's window argument, and the registration) and exist so
//! that eviction reporting can bound and attribute a lagging position without a
//! second lookup.

use rusqlite::{Connection, OptionalExtension};
use uuid::Uuid;

use crate::messaging::ParticipantId;
use crate::messaging::config::Depth;

use super::bootstrap::depth_to_sql;
use super::dynamic::depth_from_sql;

/// One `messaging_subscriber_cursors` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriberCursorRow {
    pub subscriber: ParticipantId,
    /// The subscriber's application name, cached for eviction reporting.
    pub app_slug: String,
    /// The depth this subscriber was last attached or read at. The caller is
    /// its authority and retunes it from its own argument.
    pub push_depth: Depth,
    /// The first sequence this subscriber has not seen.
    pub next_owed_seq: i64,
}

/// `subscriber`'s cursor on `channel_uuid`, or `None` when it holds none —
/// never attached, detached since, or sampled.
pub fn load_subscriber_cursor(
    conn: &Connection,
    channel_uuid: Uuid,
    subscriber: &ParticipantId,
) -> Option<SubscriberCursorRow> {
    let mut stmt = conn
        .prepare_cached(
            "SELECT app_slug, push_depth, next_owed_seq
             FROM messaging_subscriber_cursors
             WHERE channel_uuid = ?1 AND subscriber = ?2",
        )
        .expect("prepare load_subscriber_cursor");
    stmt.query_row(
        rusqlite::params![channel_uuid.as_bytes().to_vec(), subscriber.as_str()],
        |row| {
            Ok(SubscriberCursorRow {
                subscriber: subscriber.clone(),
                app_slug: row.get(0)?,
                push_depth: depth_from_sql(&row.get::<_, String>(1)?),
                next_owed_seq: row.get(2)?,
            })
        },
    )
    .optional()
    .expect("query load_subscriber_cursor")
}

/// Create `subscriber`'s cursor at `primed_next_owed_seq`, or retune the caches
/// on the one it already holds. Returns whether the row came into existence
/// here — the store's own Created/Existing determination.
///
/// An existing row keeps its position: the position is the subscriber's own and
/// only an advance moves it, so re-attaching neither re-delivers nor skips.
///
/// # Panics
///
/// If `push_depth` is zero. A sampled subscriber holds no position; the caller
/// deletes its row instead ([`delete_subscriber_cursor`]).
pub fn ensure_subscriber_cursor(
    conn: &Connection,
    channel_uuid: Uuid,
    subscriber: &ParticipantId,
    app_slug: &str,
    push_depth: Depth,
    primed_next_owed_seq: i64,
) -> bool {
    assert!(
        push_depth.is_push_enabled(),
        "messaging: cursor requested for sampled subscriber {} on channel {channel_uuid} — a \
         sampled subscriber holds no position",
        subscriber.as_str(),
    );
    if load_subscriber_cursor(conn, channel_uuid, subscriber).is_some() {
        conn.execute(
            "UPDATE messaging_subscriber_cursors
             SET app_slug = ?3, push_depth = ?4
             WHERE channel_uuid = ?1 AND subscriber = ?2",
            rusqlite::params![
                channel_uuid.as_bytes().to_vec(),
                subscriber.as_str(),
                app_slug,
                depth_to_sql(push_depth),
            ],
        )
        .expect("retune subscriber cursor");
        return false;
    }
    conn.execute(
        "INSERT INTO messaging_subscriber_cursors
             (channel_uuid, subscriber, app_slug, push_depth, next_owed_seq)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![
            channel_uuid.as_bytes().to_vec(),
            subscriber.as_str(),
            app_slug,
            depth_to_sql(push_depth),
            primed_next_owed_seq,
        ],
    )
    .expect("insert subscriber cursor");
    true
}

/// Move `subscriber`'s cursor to `next_owed_seq` — the only write that changes
/// a position.
///
/// # Panics
///
/// If the subscriber holds no cursor on this channel. A position moves only for
/// a subscriber that has one; advancing a queue that does not exist is a wiring
/// bug.
pub fn set_subscriber_cursor_position(
    conn: &Connection,
    channel_uuid: Uuid,
    subscriber: &ParticipantId,
    next_owed_seq: i64,
) {
    let moved = conn
        .execute(
            "UPDATE messaging_subscriber_cursors
             SET next_owed_seq = ?3
             WHERE channel_uuid = ?1 AND subscriber = ?2",
            rusqlite::params![
                channel_uuid.as_bytes().to_vec(),
                subscriber.as_str(),
                next_owed_seq,
            ],
        )
        .expect("move subscriber cursor");
    assert!(
        moved == 1,
        "messaging: {} holds no cursor on channel {channel_uuid} to move",
        subscriber.as_str()
    );
}

/// Retune the depth cache on `subscriber`'s cursor. Silent when it holds none:
/// the caller is the depth authority and a subscriber with no position has
/// nothing to retune.
pub fn retune_subscriber_cursor_depth(
    conn: &Connection,
    channel_uuid: Uuid,
    subscriber: &ParticipantId,
    push_depth: Depth,
) {
    conn.execute(
        "UPDATE messaging_subscriber_cursors
         SET push_depth = ?3
         WHERE channel_uuid = ?1 AND subscriber = ?2",
        rusqlite::params![
            channel_uuid.as_bytes().to_vec(),
            subscriber.as_str(),
            depth_to_sql(push_depth),
        ],
    )
    .expect("retune subscriber cursor depth");
}

/// Every subscriber on `channel_uuid` whose position trails a message retention
/// still holds.
///
/// A position above everything retained is caught up; one below a frontier that
/// has evicted past it is owed nothing that can still be served, and neither
/// lists here. A sampled subscriber holds no row and so never appears.
pub fn deliverable_cursor_subscribers(conn: &Connection, channel_uuid: Uuid) -> Vec<ParticipantId> {
    let mut stmt = conn
        .prepare_cached(
            "SELECT c.subscriber
             FROM messaging_subscriber_cursors c
             WHERE c.channel_uuid = ?1
               AND EXISTS (
                   SELECT 1 FROM messaging_messages m
                   WHERE m.channel_uuid = ?1
                     AND m.retained_seq IS NOT NULL
                     AND m.retained_seq >= c.next_owed_seq)",
        )
        .expect("prepare deliverable_cursor_subscribers");
    let rows = stmt
        .query_map(rusqlite::params![channel_uuid.as_bytes().to_vec()], |row| {
            row.get::<_, String>(0)
        })
        .expect("query deliverable_cursor_subscribers");
    rows.map(|r| ParticipantId::from_stored(r.expect("read deliverable cursor subscriber")))
        .collect()
}

/// Whether `subscriber`'s position trails a message retention still holds —
/// [`deliverable_cursor_subscribers`] scoped to one subscriber, and `false` for
/// one that holds no cursor at all.
pub fn cursor_has_deliverable(
    conn: &Connection,
    channel_uuid: Uuid,
    subscriber: &ParticipantId,
) -> bool {
    // Cached: one call per push-enabled input port on every WASM drain step.
    let mut stmt = conn
        .prepare_cached(
            "SELECT EXISTS (
                 SELECT 1
                 FROM messaging_subscriber_cursors c
                 JOIN messaging_messages m ON m.channel_uuid = c.channel_uuid
                 WHERE c.channel_uuid = ?1
                   AND c.subscriber = ?2
                   AND m.retained_seq IS NOT NULL
                   AND m.retained_seq >= c.next_owed_seq)",
        )
        .expect("prepare cursor_has_deliverable");
    stmt.query_row(
        rusqlite::params![channel_uuid.as_bytes().to_vec(), subscriber.as_str()],
        |row| row.get::<_, i64>(0),
    )
    .expect("query cursor_has_deliverable")
        != 0
}

/// Remove `subscriber`'s cursor from `channel_uuid`. Returns whether a row was
/// there to remove.
///
/// Idempotent, and the same call serves detach and the sampled-demotion rule: a
/// subscriber that lands at `push_depth = 0` is never delivered to again, so
/// keeping a position for it would leave eviction reporting charging a
/// subscriber nothing can reach.
pub fn delete_subscriber_cursor(
    conn: &Connection,
    channel_uuid: Uuid,
    subscriber: &ParticipantId,
) -> bool {
    conn.execute(
        "DELETE FROM messaging_subscriber_cursors
         WHERE channel_uuid = ?1 AND subscriber = ?2",
        rusqlite::params![channel_uuid.as_bytes().to_vec(), subscriber.as_str()],
    )
    .expect("delete subscriber cursor")
        > 0
}
