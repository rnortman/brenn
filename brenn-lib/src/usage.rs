//! Per-(device, user, app_slug) usage sessions and per-interaction events.
//!
//! A **usage session** is a span of real activity, bounded by a configurable
//! inactivity gap (default 30 minutes). Sessions are opened lazily on the first
//! recorded event and closed lazily when the next event for the same
//! (device, user, app_slug) tuple arrives after the gap has elapsed.
//!
//! Two tables:
//! - `usage_sessions` — one row per session, denormalized counters.
//! - `usage_events`   — one row per individual interaction.
//!
//! All public functions take `&Connection`, not `&Db`, mirroring
//! `cost_samples`. Callers that hold a `Db` do `let conn = db.lock().await;
//! usage::record_*(&conn, ...)`. The CLI opens a bare read-only connection and
//! calls `query_*` directly.
//!
//! # Time-skew note
//! `started_at`, `last_activity_at`, `ended_at` are wall-clock RFC3339 from
//! `chrono::Utc::now()`. A backwards system-clock jump could yield
//! `last_activity_at < started_at` for new events on resumed sessions; callers
//! that compute duration must use `max(0, ...)`.

use chrono::{DateTime, NaiveDate, Utc};
use rusqlite::{Connection, OptionalExtension, Transaction};
use tracing::warn;

// ---------------------------------------------------------------------------
// Event-type vocabulary
// ---------------------------------------------------------------------------

/// All valid values for `usage_events.event_type`.
///
/// The CHECK constraint in the `usage_events` DDL mirrors this list exactly.
/// Adding a new variant requires a table-rebuild migration (SQLite cannot ALTER
/// an existing CHECK constraint in place). That friction is intentional — every
/// new event type should be a deliberate decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    WsConnect,
    WsDisconnect,
    LlmTurn,
    SendMessage,
    StopRequest,
    TodoRefresh,
    TodoDone,
    TodoSchedule,
    TodoReorder,
    SwitchConversation,
    NewConversation,
    RequestCompaction,
    RunTarget,
    SetConversationPrivacy,
}

impl EventType {
    /// Returns the string stored in the DB (matches the CHECK constraint).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WsConnect => "ws_connect",
            Self::WsDisconnect => "ws_disconnect",
            Self::LlmTurn => "llm_turn",
            Self::SendMessage => "send_message",
            Self::StopRequest => "stop_request",
            Self::TodoRefresh => "todo_refresh",
            Self::TodoDone => "todo_done",
            Self::TodoSchedule => "todo_schedule",
            Self::TodoReorder => "todo_reorder",
            Self::SwitchConversation => "switch_conversation",
            Self::NewConversation => "new_conversation",
            Self::RequestCompaction => "request_compaction",
            Self::RunTarget => "run_target",
            Self::SetConversationPrivacy => "set_conversation_privacy",
        }
    }

    /// Parse from DB string. Returns `None` for unknown values.
    pub fn try_from_str(s: &str) -> Option<Self> {
        match s {
            "ws_connect" => Some(Self::WsConnect),
            "ws_disconnect" => Some(Self::WsDisconnect),
            "llm_turn" => Some(Self::LlmTurn),
            "send_message" => Some(Self::SendMessage),
            "stop_request" => Some(Self::StopRequest),
            "todo_refresh" => Some(Self::TodoRefresh),
            "todo_done" => Some(Self::TodoDone),
            "todo_schedule" => Some(Self::TodoSchedule),
            "todo_reorder" => Some(Self::TodoReorder),
            "switch_conversation" => Some(Self::SwitchConversation),
            "new_conversation" => Some(Self::NewConversation),
            "request_compaction" => Some(Self::RequestCompaction),
            "run_target" => Some(Self::RunTarget),
            "set_conversation_privacy" => Some(Self::SetConversationPrivacy),
            _ => None,
        }
    }

    /// Returns true for event types that increment `ui_interactions`.
    ///
    /// Excludes: `ws_connect`, `ws_disconnect` (session-boundary heartbeats),
    /// `llm_turn` (counted separately as `llm_turns`), `send_message`
    /// (pairs 1:1 with `llm_turn`, would double-count LLM work).
    pub fn counts_as_ui_interaction(self) -> bool {
        matches!(
            self,
            Self::StopRequest
                | Self::TodoRefresh
                | Self::TodoDone
                | Self::TodoSchedule
                | Self::TodoReorder
                | Self::SwitchConversation
                | Self::NewConversation
                | Self::RequestCompaction
                | Self::RunTarget
                | Self::SetConversationPrivacy
        )
    }
}

// ---------------------------------------------------------------------------
// Query types
// ---------------------------------------------------------------------------

/// Filter parameters for `query_sessions`.
#[derive(Debug, Default, Clone)]
pub struct SessionsFilter {
    /// Inclusive lower bound on `started_at`. Required.
    pub from: DateTime<Utc>,
    /// Exclusive upper bound on `started_at`. Required.
    pub to: DateTime<Utc>,
    /// Filter by `users.username` (exact match).
    pub user: Option<String>,
    /// Filter by device slug: `device_users.assigned_slug` if non-NULL,
    /// else `devices.guessed_slug`.
    pub device: Option<String>,
    /// Filter by `app_slug` (exact match).
    pub app: Option<String>,
}

/// Filter parameters for `query_events`.
#[derive(Debug, Default, Clone)]
pub struct EventsFilter {
    /// Inclusive lower bound on `created_at`. Required.
    pub from: DateTime<Utc>,
    /// Exclusive upper bound on `created_at`. Required.
    pub to: DateTime<Utc>,
    /// Filter by `users.username`.
    pub user: Option<String>,
    /// Filter by device slug.
    pub device: Option<String>,
    /// Filter by `app_slug`.
    pub app: Option<String>,
    /// Filter by `event_type`.
    pub event_type: Option<EventType>,
}

/// A session row as returned to the CLI / MCP tool.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SessionRow {
    pub session_id: i64,
    pub started_at: String,
    /// NULL for open sessions.
    pub ended_at: Option<String>,
    /// Seconds from `started_at` to `ended_at` (if closed) or to
    /// `last_activity_at` (if open). May be 0 if timestamps equal.
    pub duration_secs: i64,
    /// True when `ended_at IS NULL`.
    pub open: bool,
    /// `users.username`.
    pub user: String,
    /// `device_users.assigned_slug` if non-NULL, else `devices.guessed_slug`.
    pub device_slug: String,
    pub app_slug: String,
    pub llm_turns: i64,
    pub ui_interactions: i64,
    pub total_cost_usd: f64,
}

/// An event row as returned to the CLI / MCP tool.
#[derive(Debug, Clone, serde::Serialize)]
pub struct EventRow {
    pub event_id: i64,
    pub created_at: String,
    pub session_id: i64,
    pub user: String,
    pub device_slug: String,
    pub app_slug: String,
    pub event_type: String,
    pub conversation_id: Option<i64>,
    pub turn_cost_usd: Option<f64>,
}

// ---------------------------------------------------------------------------
// Schema migrations (called from db::run_migrations)
// ---------------------------------------------------------------------------

/// Create the `usage_sessions` and `usage_events` tables if they do not exist.
///
/// Called from `db::run_migrations`. Idempotent.
pub fn run_usage_migrations(conn: &Connection) {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS usage_sessions (
            id               INTEGER PRIMARY KEY,
            device_id        INTEGER NOT NULL REFERENCES devices(id),
            user_id          INTEGER NOT NULL REFERENCES users(id),
            app_slug         TEXT    NOT NULL,
            conversation_id  INTEGER REFERENCES conversations(id) ON DELETE SET NULL,
            started_at       TEXT    NOT NULL,
            last_activity_at TEXT    NOT NULL,
            ended_at         TEXT,
            llm_turns        INTEGER NOT NULL DEFAULT 0,
            ui_interactions  INTEGER NOT NULL DEFAULT 0,
            total_cost_usd   REAL    NOT NULL DEFAULT 0.0
        );

        -- Open-session lookup: UNIQUE partial index enforces the invariant
        -- that at most one open session exists per (device, user, app) tuple.
        CREATE UNIQUE INDEX IF NOT EXISTS idx_usage_sessions_open
            ON usage_sessions(device_id, user_id, app_slug)
            WHERE ended_at IS NULL;

        -- Time-window queries.
        CREATE INDEX IF NOT EXISTS idx_usage_sessions_started_at
            ON usage_sessions(started_at);

        -- Per-user time-window queries.
        CREATE INDEX IF NOT EXISTS idx_usage_sessions_user_started
            ON usage_sessions(user_id, started_at);

        CREATE TABLE IF NOT EXISTS usage_events (
            id              INTEGER PRIMARY KEY,
            session_id      INTEGER NOT NULL REFERENCES usage_sessions(id) ON DELETE CASCADE,
            device_id       INTEGER NOT NULL REFERENCES devices(id),
            user_id         INTEGER NOT NULL REFERENCES users(id),
            app_slug        TEXT    NOT NULL,
            event_type      TEXT    NOT NULL CHECK(event_type IN (
                                'ws_connect','ws_disconnect','llm_turn','send_message',
                                'stop_request','todo_refresh','todo_done','todo_schedule',
                                'todo_reorder','switch_conversation','new_conversation',
                                'request_compaction','run_target','set_conversation_privacy'
                            )),
            conversation_id INTEGER REFERENCES conversations(id) ON DELETE SET NULL,
            turn_cost_usd   REAL,
            created_at      TEXT    NOT NULL
        );

        -- Per-session event lookup.
        CREATE INDEX IF NOT EXISTS idx_usage_events_session_id
            ON usage_events(session_id);

        -- Time-window queries.
        CREATE INDEX IF NOT EXISTS idx_usage_events_created_at
            ON usage_events(created_at);

        -- Per-device timelines.
        CREATE INDEX IF NOT EXISTS idx_usage_events_device_created
            ON usage_events(device_id, created_at);
        ",
    )
    .expect("usage::run_usage_migrations failed");
}

// ---------------------------------------------------------------------------
// Internal: find-or-open-session
// ---------------------------------------------------------------------------

/// Core session find-or-open logic operating on an existing transaction.
///
/// Finds the open session for `(device_id, user_id, app_slug)`, lazily closing
/// it if the inactivity gap has elapsed, or inserts a new session. Does NOT
/// commit — the caller owns the transaction lifecycle.
///
/// If `conversation_id` is Some and the session's `conversation_id` is NULL,
/// the session row is updated with the supplied value (first non-NULL wins).
///
/// Returns the session_id to use for the new event.
#[allow(clippy::too_many_arguments)]
fn find_or_open_session_within_tx(
    tx: &Transaction<'_>,
    device_id: i64,
    user_id: i64,
    app_slug: &str,
    conversation_id: Option<i64>,
    now: &str,
    now_dt: DateTime<Utc>,
    gap_secs: u32,
) -> i64 {
    // 1. Look up open session.
    let open_row: Option<(i64, String, Option<i64>)> = tx
        .query_row(
            "SELECT id, last_activity_at, conversation_id
               FROM usage_sessions
              WHERE device_id = ?1 AND user_id = ?2 AND app_slug = ?3 AND ended_at IS NULL
              LIMIT 1",
            rusqlite::params![device_id, user_id, app_slug],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .expect("usage::find_or_open_session_within_tx: SELECT open session");

    if let Some((session_id, last_activity, existing_conv_id)) = open_row {
        // Parse timestamps to check gap.
        let last_dt = DateTime::parse_from_rfc3339(&last_activity)
            .unwrap_or_else(|e| {
                panic!(
                    "usage::find_or_open_session_within_tx: unparseable last_activity_at {:?}: {}",
                    last_activity, e
                )
            })
            .to_utc();
        let elapsed = (now_dt - last_dt).num_seconds();

        if elapsed <= i64::from(gap_secs) {
            // 2. Resume: update last_activity_at, maybe set conversation_id.
            if existing_conv_id.is_none() {
                if let Some(conv_id) = conversation_id {
                    tx.execute(
                        "UPDATE usage_sessions
                            SET last_activity_at = ?1, conversation_id = ?2
                          WHERE id = ?3",
                        rusqlite::params![now, conv_id, session_id],
                    )
                    .expect(
                        "usage::find_or_open_session_within_tx: UPDATE last_activity + conv_id",
                    );
                } else {
                    tx.execute(
                        "UPDATE usage_sessions SET last_activity_at = ?1 WHERE id = ?2",
                        rusqlite::params![now, session_id],
                    )
                    .expect("usage::find_or_open_session_within_tx: UPDATE last_activity_at");
                }
            } else {
                tx.execute(
                    "UPDATE usage_sessions SET last_activity_at = ?1 WHERE id = ?2",
                    rusqlite::params![now, session_id],
                )
                .expect("usage::find_or_open_session_within_tx: UPDATE last_activity_at");
            }
            return session_id;
        }

        // 3. Gap exceeded: close the old session.
        tx.execute(
            "UPDATE usage_sessions SET ended_at = last_activity_at WHERE id = ?1",
            rusqlite::params![session_id],
        )
        .expect("usage::find_or_open_session_within_tx: UPDATE ended_at (lazy-close)");
        // Fall through to insert a new session.
    }

    // 4. Insert new session.
    tx.execute(
        "INSERT INTO usage_sessions
             (device_id, user_id, app_slug, conversation_id, started_at, last_activity_at,
              ended_at, llm_turns, ui_interactions, total_cost_usd)
         VALUES (?1, ?2, ?3, ?4, ?5, ?5, NULL, 0, 0, 0.0)",
        rusqlite::params![device_id, user_id, app_slug, conversation_id, now],
    )
    .expect("usage::find_or_open_session_within_tx: INSERT new session");
    tx.last_insert_rowid()
}

/// Find the open session for (device_id, user_id, app_slug), or open a new one.
///
/// Wrapper around `find_or_open_session_within_tx` that manages its own
/// transaction. Used by callers that do **not** need the session-open and
/// event-INSERT to be atomic — specifically, callers that have no counter UPDATE
/// to keep in sync with the INSERT (`record_ws_connect`, `record_send_message`).
/// Both callers do execute a second statement after this returns, but outside
/// the transaction, which is intentional: there is no counter to keep consistent.
///
/// Callers that have a counter UPDATE (e.g. `record_stop_request`,
/// `record_ui_event`, `record_llm_turn`) MUST use `find_or_open_session_within_tx`
/// and manage their own outer transaction.
///
/// Returns the session_id to use for the new event.
#[allow(clippy::too_many_arguments)]
fn find_or_open_session(
    conn: &Connection,
    device_id: i64,
    user_id: i64,
    app_slug: &str,
    conversation_id: Option<i64>,
    now: &str,
    now_dt: DateTime<Utc>,
    gap_secs: u32,
) -> i64 {
    let tx = conn
        .unchecked_transaction()
        .expect("usage::find_or_open_session: begin transaction");
    let session_id = find_or_open_session_within_tx(
        &tx,
        device_id,
        user_id,
        app_slug,
        conversation_id,
        now,
        now_dt,
        gap_secs,
    );
    tx.commit().expect("usage::find_or_open_session: commit");
    session_id
}

// ---------------------------------------------------------------------------
// Recording: event insertion + counter updates
// ---------------------------------------------------------------------------

/// Record a `ws_connect` event.
///
/// Opens or resumes a session. Returns the session_id.
/// Does NOT increment any session counter — ws_connect/disconnect are
/// session-boundary heartbeats, not "interactions."
pub fn record_ws_connect(
    conn: &Connection,
    device_id: i64,
    user_id: i64,
    app_slug: &str,
    conversation_id: Option<i64>,
    gap_secs: u32,
) -> i64 {
    let now_dt = Utc::now();
    let now = crate::db::format_ts_for_db(now_dt);
    let session_id = find_or_open_session(
        conn,
        device_id,
        user_id,
        app_slug,
        conversation_id,
        &now,
        now_dt,
        gap_secs,
    );
    conn.execute(
        "INSERT INTO usage_events
             (session_id, device_id, user_id, app_slug, event_type, conversation_id,
              turn_cost_usd, created_at)
         VALUES (?1, ?2, ?3, ?4, 'ws_connect', ?5, NULL, ?6)",
        rusqlite::params![
            session_id,
            device_id,
            user_id,
            app_slug,
            conversation_id,
            now
        ],
    )
    .expect("usage::record_ws_connect: INSERT event");
    session_id
}

/// Record a `ws_disconnect` event.
///
/// Does NOT close the session (`ended_at` is NOT set — a user may reconnect
/// within the gap and continue the same session).
/// Does NOT increment any counter.
///
/// If no open session exists (e.g. after a server restart closed all sessions),
/// this is a no-op — we don't open an orphan session whose only event would be
/// the disconnect.
pub fn record_ws_disconnect(
    conn: &Connection,
    device_id: i64,
    user_id: i64,
    app_slug: &str,
    conversation_id: Option<i64>,
) {
    let now = crate::db::format_ts_for_db(Utc::now());
    // Single INSERT ... SELECT: inserts exactly one row when an open session
    // exists, zero rows when not (matching the prior early-return semantic).
    conn.execute(
        "INSERT INTO usage_events
             (session_id, device_id, user_id, app_slug, event_type, conversation_id,
              turn_cost_usd, created_at)
         SELECT id, device_id, user_id, app_slug, 'ws_disconnect', ?4, NULL, ?5
           FROM usage_sessions
          WHERE device_id = ?1 AND user_id = ?2 AND app_slug = ?3
            AND ended_at IS NULL
          LIMIT 1",
        rusqlite::params![device_id, user_id, app_slug, conversation_id, now],
    )
    .expect("usage::record_ws_disconnect: INSERT event");
}

/// Record a `send_message` event. Increments no session counters.
///
/// Keeping `send_message` distinct from `llm_turn` lets us detect "user
/// submitted but the turn never completed (interrupt, error)."
///
/// Returns the session_id.
pub fn record_send_message(
    conn: &Connection,
    device_id: i64,
    user_id: i64,
    app_slug: &str,
    conversation_id: Option<i64>,
    gap_secs: u32,
) -> i64 {
    let now_dt = Utc::now();
    let now = crate::db::format_ts_for_db(now_dt);
    let session_id = find_or_open_session(
        conn,
        device_id,
        user_id,
        app_slug,
        conversation_id,
        &now,
        now_dt,
        gap_secs,
    );
    // No transaction needed: single-statement INSERT, no counter to keep in sync.
    // (Unlike record_stop_request / record_ui_event / record_llm_turn, send_message
    // increments no session counter, so there is no INSERT+UPDATE pair to atomicize.)
    conn.execute(
        "INSERT INTO usage_events
             (session_id, device_id, user_id, app_slug, event_type, conversation_id,
              turn_cost_usd, created_at)
         VALUES (?1, ?2, ?3, ?4, 'send_message', ?5, NULL, ?6)",
        rusqlite::params![
            session_id,
            device_id,
            user_id,
            app_slug,
            conversation_id,
            now
        ],
    )
    .expect("usage::record_send_message: INSERT event");
    session_id
}

/// Record a `stop_request` event. Increments `ui_interactions`.
///
/// Returns the session_id.
pub fn record_stop_request(
    conn: &Connection,
    device_id: i64,
    user_id: i64,
    app_slug: &str,
    conversation_id: Option<i64>,
    gap_secs: u32,
) -> i64 {
    let now_dt = Utc::now();
    let now = crate::db::format_ts_for_db(now_dt);
    let tx = conn
        .unchecked_transaction()
        .expect("usage::record_stop_request: begin transaction");
    let session_id = find_or_open_session_within_tx(
        &tx,
        device_id,
        user_id,
        app_slug,
        conversation_id,
        &now,
        now_dt,
        gap_secs,
    );
    tx.execute(
        "INSERT INTO usage_events
             (session_id, device_id, user_id, app_slug, event_type, conversation_id,
              turn_cost_usd, created_at)
         VALUES (?1, ?2, ?3, ?4, 'stop_request', ?5, NULL, ?6)",
        rusqlite::params![
            session_id,
            device_id,
            user_id,
            app_slug,
            conversation_id,
            now
        ],
    )
    .expect("usage::record_stop_request: INSERT event");
    tx.execute(
        "UPDATE usage_sessions
            SET ui_interactions = ui_interactions + 1
          WHERE id = ?1",
        rusqlite::params![session_id],
    )
    .expect("usage::record_stop_request: UPDATE ui_interactions");
    tx.commit().expect("usage::record_stop_request: commit");
    session_id
}

/// Record a UI event (any `EventType` that `counts_as_ui_interaction()`).
///
/// # Panics
/// Panics if `event_type` is one of the types that have their own dedicated
/// recording function: `WsConnect`, `WsDisconnect`, `LlmTurn`, `SendMessage`,
/// `StopRequest`. Passing those here is a caller bug.
///
/// Returns the session_id.
pub fn record_ui_event(
    conn: &Connection,
    device_id: i64,
    user_id: i64,
    app_slug: &str,
    conversation_id: Option<i64>,
    event_type: EventType,
    gap_secs: u32,
) -> i64 {
    assert!(
        event_type.counts_as_ui_interaction(),
        "usage::record_ui_event called with non-UI event_type {:?}; use the dedicated function",
        event_type
    );

    let now_dt = Utc::now();
    let now = crate::db::format_ts_for_db(now_dt);
    let tx = conn
        .unchecked_transaction()
        .expect("usage::record_ui_event: begin transaction");
    let session_id = find_or_open_session_within_tx(
        &tx,
        device_id,
        user_id,
        app_slug,
        conversation_id,
        &now,
        now_dt,
        gap_secs,
    );
    tx.execute(
        "INSERT INTO usage_events
             (session_id, device_id, user_id, app_slug, event_type, conversation_id,
              turn_cost_usd, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7)",
        rusqlite::params![
            session_id,
            device_id,
            user_id,
            app_slug,
            event_type.as_str(),
            conversation_id,
            now
        ],
    )
    .expect("usage::record_ui_event: INSERT event");
    tx.execute(
        "UPDATE usage_sessions
            SET ui_interactions = ui_interactions + 1
          WHERE id = ?1",
        rusqlite::params![session_id],
    )
    .expect("usage::record_ui_event: UPDATE ui_interactions");
    tx.commit().expect("usage::record_ui_event: commit");
    session_id
}

/// Record an `llm_turn` event. Increments `llm_turns` and `total_cost_usd`.
///
/// `device_id` and `user_id` are the attributed sender (resolved by the
/// caller via `messages.sender_device_id` lookup — see design §2.3).
///
/// Returns the session_id.
pub fn record_llm_turn(
    conn: &Connection,
    device_id: i64,
    user_id: i64,
    app_slug: &str,
    conversation_id: Option<i64>,
    turn_cost_usd: f64,
    gap_secs: u32,
) -> i64 {
    let now_dt = Utc::now();
    let now = crate::db::format_ts_for_db(now_dt);
    // Single transaction covers session find-or-open + event INSERT + counter
    // UPDATE, eliminating the prior two-transaction window where a crash between
    // commits could leave last_activity_at advanced with no event row.
    let tx = conn
        .unchecked_transaction()
        .expect("usage::record_llm_turn: begin transaction");
    let session_id = find_or_open_session_within_tx(
        &tx,
        device_id,
        user_id,
        app_slug,
        conversation_id,
        &now,
        now_dt,
        gap_secs,
    );
    tx.execute(
        "INSERT INTO usage_events
             (session_id, device_id, user_id, app_slug, event_type, conversation_id,
              turn_cost_usd, created_at)
         VALUES (?1, ?2, ?3, ?4, 'llm_turn', ?5, ?6, ?7)",
        rusqlite::params![
            session_id,
            device_id,
            user_id,
            app_slug,
            conversation_id,
            turn_cost_usd,
            now
        ],
    )
    .expect("usage::record_llm_turn: INSERT event");
    tx.execute(
        "UPDATE usage_sessions
            SET llm_turns      = llm_turns + 1,
                total_cost_usd = total_cost_usd + ?1
          WHERE id = ?2",
        rusqlite::params![turn_cost_usd, session_id],
    )
    .expect("usage::record_llm_turn: UPDATE counters");
    tx.commit().expect("usage::record_llm_turn: commit");
    session_id
}

// ---------------------------------------------------------------------------
// Server-startup cleanup
// ---------------------------------------------------------------------------

/// Close all sessions that are still open (i.e. `ended_at IS NULL`).
///
/// Called once at server startup, immediately after `init_db`. Sets
/// `ended_at = last_activity_at` (which equals `started_at` for sessions
/// that never recorded a follow-up event).
///
/// Returns the number of rows closed.
pub fn close_open_sessions_on_startup(conn: &Connection) -> usize {
    conn.execute(
        "UPDATE usage_sessions SET ended_at = last_activity_at WHERE ended_at IS NULL",
        [],
    )
    .expect("usage::close_open_sessions_on_startup")
}

/// Delete usage events (and orphaned sessions) older than `before`.
///
/// Events with `created_at < before` are deleted first; then any
/// `usage_sessions` rows with no remaining events and whose `ended_at` is also
/// older than `before` are deleted. Called once at server startup to bound disk
/// growth, mirroring `cost_samples::prune_before`.
pub fn prune_usage_before(conn: &Connection, before: DateTime<Utc>) {
    let cutoff = crate::db::format_ts_for_db(before);
    let tx = conn
        .unchecked_transaction()
        .expect("usage::prune_usage_before: begin transaction");
    tx.execute("DELETE FROM usage_events WHERE created_at < ?1", [&cutoff])
        .expect("usage::prune_usage_before: delete events");
    // Use NOT EXISTS rather than NOT IN: NOT IN against a nullable column is a
    // SQL footgun — a single NULL in session_id would make NOT IN return NULL,
    // silently suppressing all session deletes. NOT EXISTS is NULL-safe by
    // construction.
    tx.execute(
        "DELETE FROM usage_sessions \
          WHERE ended_at IS NOT NULL AND ended_at < ?1 \
            AND NOT EXISTS (SELECT 1 FROM usage_events ue WHERE ue.session_id = usage_sessions.id)",
        [&cutoff],
    )
    .expect("usage::prune_usage_before: delete orphaned sessions");
    tx.commit().expect("usage::prune_usage_before: commit");
}

// ---------------------------------------------------------------------------
// Timestamp parsing (shared by CLI and MCP tool)
// ---------------------------------------------------------------------------

/// Parse an ISO-8601 timestamp or a bare `YYYY-MM-DD` date (UTC midnight).
///
/// Used by both `brenn-usage-obs` (CLI) and the `export_usage` MCP tool.
pub fn parse_ts_str(s: &str) -> Result<DateTime<Utc>, String> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.to_utc());
    }
    if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Ok(date
            .and_hms_opt(0, 0, 0)
            .expect("midnight always valid")
            .and_utc());
    }
    Err(format!(
        "invalid timestamp {s:?}: expected ISO-8601 (e.g. 2026-05-01T00:00:00Z) or YYYY-MM-DD"
    ))
}

// ---------------------------------------------------------------------------
// Query functions (CLI / MCP)
// ---------------------------------------------------------------------------

/// Query sessions whose `started_at` falls in `[filter.from, filter.to)`.
pub fn query_sessions(conn: &Connection, filter: &SessionsFilter) -> Vec<SessionRow> {
    let from = crate::db::format_ts_for_db(filter.from);
    let to = crate::db::format_ts_for_db(filter.to);

    // Build WHERE clauses dynamically for optional filters.
    let mut conditions = vec![
        "us.started_at >= ?1".to_string(),
        "us.started_at < ?2".to_string(),
    ];
    let mut extra_params: Vec<String> = vec![];
    let mut param_idx = 3usize;

    if let Some(ref user) = filter.user {
        conditions.push(format!("u.username = ?{param_idx}"));
        extra_params.push(user.clone());
        param_idx += 1;
    }
    if let Some(ref device) = filter.device {
        // Match assigned_slug or guessed_slug.
        conditions.push(format!(
            "(COALESCE(du.assigned_slug, d.guessed_slug) = ?{param_idx})"
        ));
        extra_params.push(device.clone());
        param_idx += 1;
    }
    if let Some(ref app) = filter.app {
        conditions.push(format!("us.app_slug = ?{param_idx}"));
        extra_params.push(app.clone());
    }

    let where_clause = conditions.join(" AND ");

    let sql = format!(
        "SELECT us.id,
                us.started_at,
                us.ended_at,
                us.last_activity_at,
                us.llm_turns,
                us.ui_interactions,
                us.total_cost_usd,
                u.username,
                COALESCE(du.assigned_slug, d.guessed_slug) AS device_slug,
                us.app_slug
           FROM usage_sessions us
           JOIN users u  ON u.id  = us.user_id
           JOIN devices d ON d.id = us.device_id
      LEFT JOIN device_users du ON du.device_id = us.device_id AND du.user_id = us.user_id
          WHERE {where_clause}
          ORDER BY us.started_at ASC"
    );

    let mut stmt = conn.prepare(&sql).expect("usage::query_sessions: prepare");

    // Bind all parameters: positional because rusqlite doesn't support named
    // params in runtime-built queries easily.
    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(from), Box::new(to)];
    for p in extra_params {
        params.push(Box::new(p));
    }
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|b| b.as_ref()).collect();

    stmt.query_map(param_refs.as_slice(), |row| {
        // Column order matches SELECT:
        // 0: us.id, 1: us.started_at, 2: us.ended_at, 3: us.last_activity_at,
        // 4: us.llm_turns, 5: us.ui_interactions, 6: us.total_cost_usd,
        // 7: u.username, 8: device_slug, 9: us.app_slug
        let session_id: i64 = row.get(0)?;
        let started_at: String = row.get(1)?;
        let ended_at: Option<String> = row.get(2)?;
        let last_activity_at: String = row.get(3)?;
        let open = ended_at.is_none();
        let duration_secs =
            compute_duration_secs(&started_at, ended_at.as_deref(), &last_activity_at);

        Ok(SessionRow {
            session_id,
            started_at,
            ended_at,
            duration_secs,
            open,
            llm_turns: row.get(4)?,
            ui_interactions: row.get(5)?,
            total_cost_usd: row.get(6)?,
            user: row.get(7)?,
            device_slug: row.get(8)?,
            app_slug: row.get(9)?,
        })
    })
    .expect("usage::query_sessions: query_map")
    .map(|r| r.expect("usage::query_sessions: row"))
    .collect()
}

fn compute_duration_secs(started_at: &str, ended_at: Option<&str>, last_activity_at: &str) -> i64 {
    let start = match DateTime::parse_from_rfc3339(started_at) {
        Ok(dt) => dt.to_utc(),
        Err(e) => panic!(
            "compute_duration_secs: started_at failed RFC 3339 parse: {:?} — error: {e}",
            started_at
        ),
    };
    let end_str = ended_at.unwrap_or(last_activity_at);
    let end = match DateTime::parse_from_rfc3339(end_str) {
        Ok(dt) => dt.to_utc(),
        Err(e) => panic!(
            "compute_duration_secs: end timestamp failed RFC 3339 parse: {:?} — error: {e}",
            end_str
        ),
    };
    (end - start).num_seconds().max(0)
}

/// Query events whose `created_at` falls in `[filter.from, filter.to)`.
pub fn query_events(conn: &Connection, filter: &EventsFilter) -> Vec<EventRow> {
    let from = crate::db::format_ts_for_db(filter.from);
    let to = crate::db::format_ts_for_db(filter.to);

    let mut conditions = vec![
        "ue.created_at >= ?1".to_string(),
        "ue.created_at < ?2".to_string(),
    ];
    let mut extra_params: Vec<String> = vec![];
    let mut param_idx = 3usize;

    if let Some(ref user) = filter.user {
        conditions.push(format!("u.username = ?{param_idx}"));
        extra_params.push(user.clone());
        param_idx += 1;
    }
    if let Some(ref device) = filter.device {
        conditions.push(format!(
            "(COALESCE(du.assigned_slug, d.guessed_slug) = ?{param_idx})"
        ));
        extra_params.push(device.clone());
        param_idx += 1;
    }
    if let Some(ref app) = filter.app {
        conditions.push(format!("ue.app_slug = ?{param_idx}"));
        extra_params.push(app.clone());
        param_idx += 1;
    }
    if let Some(et) = filter.event_type {
        conditions.push(format!("ue.event_type = ?{param_idx}"));
        extra_params.push(et.as_str().to_string());
    }

    let where_clause = conditions.join(" AND ");

    let sql = format!(
        "SELECT ue.id,
                ue.created_at,
                ue.session_id,
                u.username,
                COALESCE(du.assigned_slug, d.guessed_slug) AS device_slug,
                ue.app_slug,
                ue.event_type,
                ue.conversation_id,
                ue.turn_cost_usd
           FROM usage_events ue
           JOIN users u   ON u.id  = ue.user_id
           JOIN devices d ON d.id  = ue.device_id
      LEFT JOIN device_users du ON du.device_id = ue.device_id AND du.user_id = ue.user_id
          WHERE {where_clause}
          ORDER BY ue.id ASC"
    );

    let mut stmt = conn.prepare(&sql).expect("usage::query_events: prepare");
    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(from), Box::new(to)];
    for p in extra_params {
        params.push(Box::new(p));
    }
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|b| b.as_ref()).collect();

    stmt.query_map(param_refs.as_slice(), |row| {
        Ok(EventRow {
            event_id: row.get(0)?,
            created_at: row.get(1)?,
            session_id: row.get(2)?,
            user: row.get(3)?,
            device_slug: row.get(4)?,
            app_slug: row.get(5)?,
            event_type: row.get(6)?,
            conversation_id: row.get(7)?,
            turn_cost_usd: row.get(8)?,
        })
    })
    .expect("usage::query_events: query_map")
    .map(|r| r.expect("usage::query_events: row"))
    .collect()
}

/// Resolve `device_id` and `user_id` from the most-recent `messages.sender_device_id`
/// for a conversation. Returns `None` if no row exists or if `sender_device_id` is NULL.
///
/// Returns `None` for unattributable turns per design §3.2 — the caller drops
/// the usage event rather than misattributing it.
///
/// Used by `handle_turn_completed` to attribute `llm_turn` events.
///
pub fn resolve_sender_for_conversation(
    conn: &Connection,
    conversation_id: i64,
) -> Option<(i64, i64)> {
    let result: Option<(i64, i64)> = conn
        .query_row(
            "SELECT sender_device_id, sender_user_id
               FROM messages
              WHERE conversation_id = ?1
                AND sender_device_id IS NOT NULL
                AND sender_user_id IS NOT NULL
              ORDER BY id DESC
              LIMIT 1",
            rusqlite::params![conversation_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .expect("usage::resolve_sender_for_conversation: primary query");

    if result.is_none() {
        warn!(
            conversation_id,
            "llm_turn with no attributable device; dropping usage event"
        );
    }
    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth;
    use crate::conversation;
    use crate::db::init_db_memory;
    use chrono::{Datelike, Duration, Timelike};

    // ── Test helpers ──────────────────────────────────────────────────────────

    /// Seed users and a device. Returns (db, user_id, device_id, conv_id).
    ///
    /// A dummy user is inserted and deleted first so that alice's uid (2) differs
    /// from the device's did (1), making T1 sensitive to sender field swap bugs.
    async fn setup_db() -> (crate::db::Db, i64, i64, i64) {
        let db = init_db_memory();
        let (user_id, device_id, conv_id) = {
            let conn = db.lock().await;
            // Skew the users sequence so uid != did (uid=2, did=1).
            let dummy = auth::user::create_user(&conn, "_dummy", "$argon2id$fake");
            conn.execute("DELETE FROM users WHERE id = ?1", rusqlite::params![dummy])
                .unwrap();
            let uid = auth::user::create_user(&conn, "alice", "$argon2id$fake");
            // Insert a minimal device row.
            conn.execute(
                "INSERT INTO devices (token, guessed_slug, user_agent, last_seen_at, created_at)
                 VALUES ('aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899',
                         'chrome-linux', 'Mozilla/5.0', datetime('now'), datetime('now'))",
                [],
            )
            .unwrap();
            let did = conn.last_insert_rowid();
            conn.execute(
                "INSERT INTO device_users (device_id, user_id, first_seen_at, last_seen_at)
                 VALUES (?1, ?2, datetime('now'), datetime('now'))",
                rusqlite::params![did, uid],
            )
            .unwrap();
            let cid = conversation::create_conversation(&conn, uid, "test-app", false);
            (uid, did, cid)
        };
        (db, user_id, device_id, conv_id)
    }

    /// Insert a second user + device for multi-actor tests.
    async fn add_bob(db: &crate::db::Db) -> (i64, i64) {
        let conn = db.lock().await;
        let uid = auth::user::create_user(&conn, "bob", "$argon2id$fake");
        conn.execute(
            "INSERT INTO devices (token, guessed_slug, user_agent, last_seen_at, created_at)
             VALUES ('bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
                     'firefox-mac', 'Mozilla/5.0', datetime('now'), datetime('now'))",
            [],
        )
        .unwrap();
        let did = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO device_users (device_id, user_id, first_seen_at, last_seen_at)
             VALUES (?1, ?2, datetime('now'), datetime('now'))",
            rusqlite::params![did, uid],
        )
        .unwrap();
        (uid, did)
    }

    const GAP: u32 = 1800; // 30 minutes in seconds

    /// Return a wide query window: 2026-01-01 → 2099-01-01.
    ///
    /// Used by T3–T8 to avoid coupling test assertions to boundary semantics.
    fn wide_window() -> (DateTime<Utc>, DateTime<Utc>) {
        let from = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .to_utc();
        let to = DateTime::parse_from_rfc3339("2099-01-01T00:00:00Z")
            .unwrap()
            .to_utc();
        (from, to)
    }

    /// Insert a `usage_sessions` row and return its rowid.
    ///
    /// `last_activity_at` defaults to `started_at` when equal; pass a distinct
    /// value for open-session tests (T8). `ended_at = None` marks the session open.
    #[allow(clippy::too_many_arguments)]
    fn insert_test_session(
        conn: &Connection,
        device_id: i64,
        user_id: i64,
        app_slug: &str,
        conv_id: i64,
        started_at: &str,
        last_activity_at: &str,
        ended_at: Option<&str>,
    ) -> i64 {
        conn.execute(
            "INSERT INTO usage_sessions
                 (device_id, user_id, app_slug, conversation_id, started_at,
                  last_activity_at, ended_at, llm_turns, ui_interactions, total_cost_usd)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, 0, 0.0)",
            rusqlite::params![
                device_id,
                user_id,
                app_slug,
                conv_id,
                started_at,
                last_activity_at,
                ended_at
            ],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    // ── Unit tests ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn usage_session_starts_on_first_event() {
        let (db, uid, did, cid) = setup_db().await;
        let conn = db.lock().await;
        let session_id = record_ws_connect(&conn, did, uid, "test-app", Some(cid), GAP);

        // One open session row.
        let (started_at, ended_at, last_activity_at, llm_turns, ui_interactions): (
            String,
            Option<String>,
            String,
            i64,
            i64,
        ) = conn
            .query_row(
                "SELECT started_at, ended_at, last_activity_at, llm_turns, ui_interactions
               FROM usage_sessions WHERE id = ?1",
                rusqlite::params![session_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap();

        assert!(ended_at.is_none(), "session should be open");
        assert_eq!(
            started_at, last_activity_at,
            "started_at == last_activity_at on first event"
        );
        assert_eq!(llm_turns, 0);
        assert_eq!(ui_interactions, 0);

        // One ws_connect event row.
        let event_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM usage_events WHERE session_id = ?1",
                rusqlite::params![session_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(event_count, 1);
    }

    #[tokio::test]
    async fn usage_session_resumes_within_gap() {
        let (db, uid, did, cid) = setup_db().await;
        let conn = db.lock().await;

        // First event.
        let s1 = record_ws_connect(&conn, did, uid, "test-app", Some(cid), GAP);
        // Second event immediately after (well within gap).
        let s2 = record_ui_event(
            &conn,
            did,
            uid,
            "test-app",
            Some(cid),
            EventType::TodoDone,
            GAP,
        );

        assert_eq!(s1, s2, "same session should be reused within gap");

        let (ui_interactions,): (i64,) = conn
            .query_row(
                "SELECT ui_interactions FROM usage_sessions WHERE id = ?1",
                rusqlite::params![s1],
                |row| Ok((row.get(0)?,)),
            )
            .unwrap();
        assert_eq!(ui_interactions, 1);
    }

    #[tokio::test]
    async fn usage_session_closes_after_gap() {
        let (db, uid, did, cid) = setup_db().await;
        let conn = db.lock().await;

        // Manually insert an open session with last_activity_at in the past (>gap).
        let past = crate::db::format_ts_for_db(Utc::now() - Duration::hours(1));
        conn.execute(
            "INSERT INTO usage_sessions
                 (device_id, user_id, app_slug, conversation_id, started_at, last_activity_at,
                  ended_at, llm_turns, ui_interactions, total_cost_usd)
             VALUES (?1, ?2, 'test-app', ?3, ?4, ?4, NULL, 0, 0, 0.0)",
            rusqlite::params![did, uid, cid, past],
        )
        .unwrap();
        let old_session_id = conn.last_insert_rowid();

        // New event well after the gap.
        let new_session_id = record_ws_connect(&conn, did, uid, "test-app", Some(cid), GAP);

        assert_ne!(
            new_session_id, old_session_id,
            "new session should be opened after gap"
        );

        // Old session should be closed.
        let ended_at: Option<String> = conn
            .query_row(
                "SELECT ended_at FROM usage_sessions WHERE id = ?1",
                rusqlite::params![old_session_id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(ended_at.is_some(), "old session should be closed after gap");
    }

    #[tokio::test]
    async fn usage_session_per_device() {
        let (db, uid, did, cid) = setup_db().await;
        let (_, did2) = add_bob(&db).await;

        // NOTE: add_bob creates bob's user; we need a second device for alice too.
        // Use did2 for alice as a second device scenario — but add_bob made bob+did2.
        // For this test we just need two different device_ids for the SAME user.
        // Let's add alice's second device manually.
        let did_alice2 = {
            let conn = db.lock().await;
            conn.execute(
                "INSERT INTO devices (token, guessed_slug, user_agent, last_seen_at, created_at)
                 VALUES ('cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
                         'safari-ios', 'Mozilla/5.0', datetime('now'), datetime('now'))",
                [],
            )
            .unwrap();
            let d2 = conn.last_insert_rowid();
            conn.execute(
                "INSERT INTO device_users (device_id, user_id, first_seen_at, last_seen_at)
                 VALUES (?1, ?2, datetime('now'), datetime('now'))",
                rusqlite::params![d2, uid],
            )
            .unwrap();
            d2
        };

        let conn = db.lock().await;
        let s1 = record_ws_connect(&conn, did, uid, "test-app", Some(cid), GAP);
        let s2 = record_ws_connect(&conn, did_alice2, uid, "test-app", Some(cid), GAP);

        assert_ne!(s1, s2, "two devices should produce separate sessions");

        // Counters are independent.
        record_llm_turn(&conn, did, uid, "test-app", Some(cid), 0.10, GAP);
        let (turns1,): (i64,) = conn
            .query_row(
                "SELECT llm_turns FROM usage_sessions WHERE id = ?1",
                rusqlite::params![s1],
                |row| Ok((row.get(0)?,)),
            )
            .unwrap();
        let (turns2,): (i64,) = conn
            .query_row(
                "SELECT llm_turns FROM usage_sessions WHERE id = ?1",
                rusqlite::params![s2],
                |row| Ok((row.get(0)?,)),
            )
            .unwrap();
        assert_eq!(turns1, 1);
        assert_eq!(turns2, 0);
        let _ = did2; // suppress unused warning
    }

    #[tokio::test]
    async fn usage_session_per_user_on_shared_bridge() {
        let (db, uid_alice, did, cid) = setup_db().await;
        let (uid_bob, _) = add_bob(&db).await;

        // Bob needs device_users membership for did too (shared device scenario).
        {
            let conn = db.lock().await;
            conn.execute(
                "INSERT INTO device_users (device_id, user_id, first_seen_at, last_seen_at)
                 VALUES (?1, ?2, datetime('now'), datetime('now'))",
                rusqlite::params![did, uid_bob],
            )
            .unwrap();
        }

        let conn = db.lock().await;
        let s_alice = record_ws_connect(&conn, did, uid_alice, "test-app", Some(cid), GAP);
        let s_bob = record_ws_connect(&conn, did, uid_bob, "test-app", Some(cid), GAP);

        assert_ne!(
            s_alice, s_bob,
            "different users on same device produce separate sessions"
        );
    }

    #[tokio::test]
    async fn usage_llm_turn_increments_turns_and_cost() {
        let (db, uid, did, cid) = setup_db().await;
        let conn = db.lock().await;

        let sid = record_ws_connect(&conn, did, uid, "test-app", Some(cid), GAP);
        record_llm_turn(&conn, did, uid, "test-app", Some(cid), 0.05, GAP);
        record_llm_turn(&conn, did, uid, "test-app", Some(cid), 0.10, GAP);

        let (turns, cost): (i64, f64) = conn
            .query_row(
                "SELECT llm_turns, total_cost_usd FROM usage_sessions WHERE id = ?1",
                rusqlite::params![sid],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(turns, 2);
        assert!(
            (cost - 0.15).abs() < 1e-9,
            "cost should be 0.15, got {cost}"
        );
    }

    #[tokio::test]
    async fn usage_ui_event_increments_ui_interactions() {
        let (db, uid, did, cid) = setup_db().await;
        let conn = db.lock().await;

        let sid = record_ws_connect(&conn, did, uid, "test-app", Some(cid), GAP);
        record_ui_event(
            &conn,
            did,
            uid,
            "test-app",
            Some(cid),
            EventType::TodoDone,
            GAP,
        );
        record_ui_event(
            &conn,
            did,
            uid,
            "test-app",
            Some(cid),
            EventType::TodoSchedule,
            GAP,
        );
        record_stop_request(&conn, did, uid, "test-app", Some(cid), GAP);

        let (ui,): (i64,) = conn
            .query_row(
                "SELECT ui_interactions FROM usage_sessions WHERE id = ?1",
                rusqlite::params![sid],
                |row| Ok((row.get(0)?,)),
            )
            .unwrap();
        assert_eq!(ui, 3);
    }

    #[tokio::test]
    async fn usage_ws_connect_disconnect_does_not_increment_counters() {
        let (db, uid, did, cid) = setup_db().await;
        let conn = db.lock().await;

        let sid = record_ws_connect(&conn, did, uid, "test-app", Some(cid), GAP);
        record_ws_disconnect(&conn, did, uid, "test-app", Some(cid));

        let (llm, ui): (i64, i64) = conn
            .query_row(
                "SELECT llm_turns, ui_interactions FROM usage_sessions WHERE id = ?1",
                rusqlite::params![sid],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(llm, 0);
        assert_eq!(ui, 0);

        // Session remains open (ws_disconnect does NOT close).
        let ended_at: Option<String> = conn
            .query_row(
                "SELECT ended_at FROM usage_sessions WHERE id = ?1",
                rusqlite::params![sid],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            ended_at.is_none(),
            "ws_disconnect must not close the session"
        );
    }

    #[tokio::test]
    async fn usage_close_open_sessions_on_startup() {
        let (db, uid, did, cid) = setup_db().await;
        let conn = db.lock().await;

        let sid = record_ws_connect(&conn, did, uid, "test-app", Some(cid), GAP);

        // One open session.
        let ended_before: Option<String> = conn
            .query_row(
                "SELECT ended_at FROM usage_sessions WHERE id = ?1",
                rusqlite::params![sid],
                |row| row.get(0),
            )
            .unwrap();
        assert!(ended_before.is_none());

        let closed = close_open_sessions_on_startup(&conn);
        assert_eq!(closed, 1);

        let ended_after: Option<String> = conn
            .query_row(
                "SELECT ended_at FROM usage_sessions WHERE id = ?1",
                rusqlite::params![sid],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            ended_after.is_some(),
            "session should be closed after startup cleanup"
        );

        // Idempotent: second call closes 0 rows.
        let closed2 = close_open_sessions_on_startup(&conn);
        assert_eq!(closed2, 0);
    }

    #[tokio::test]
    async fn usage_zero_event_session() {
        let (db, uid, did, cid) = setup_db().await;
        let conn = db.lock().await;

        // Only ws_connect, then close on startup.
        let sid = record_ws_connect(&conn, did, uid, "test-app", Some(cid), GAP);
        close_open_sessions_on_startup(&conn);

        let (llm, ui, started, ended): (i64, i64, String, Option<String>) = conn.query_row(
            "SELECT llm_turns, ui_interactions, started_at, ended_at FROM usage_sessions WHERE id = ?1",
            rusqlite::params![sid],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        ).unwrap();
        assert_eq!(llm, 0);
        assert_eq!(ui, 0);
        assert!(ended.is_some(), "session should be closed");
        // For a ws_connect-only session, ended_at = last_activity_at = started_at.
        assert_eq!(
            ended.unwrap(),
            started,
            "ended_at should equal started_at for zero-event session"
        );
    }

    #[tokio::test]
    async fn usage_sessions_query_window() {
        let (db, uid, did, cid) = setup_db().await;
        let conn = db.lock().await;

        // Create three sessions with known started_at values.
        let t1 = "2026-01-01T00:00:00Z";
        let t2 = "2026-02-01T00:00:00Z";
        let t3 = "2026-03-01T00:00:00Z";

        for t in [t1, t2, t3] {
            conn.execute(
                "INSERT INTO usage_sessions
                     (device_id, user_id, app_slug, conversation_id, started_at,
                      last_activity_at, ended_at, llm_turns, ui_interactions, total_cost_usd)
                 VALUES (?1, ?2, 'test-app', ?3, ?4, ?4, ?4, 0, 0, 0.0)",
                rusqlite::params![did, uid, cid, t],
            )
            .unwrap();
        }

        let from = DateTime::parse_from_rfc3339("2026-01-15T00:00:00Z")
            .unwrap()
            .to_utc();
        let to = DateTime::parse_from_rfc3339("2026-03-01T00:00:00Z")
            .unwrap()
            .to_utc();

        let filter = SessionsFilter {
            from,
            to,
            ..Default::default()
        };
        let rows = query_sessions(&conn, &filter);

        assert_eq!(rows.len(), 1, "only t2 falls in [from, to)");
        assert_eq!(rows[0].started_at, t2);
    }

    #[tokio::test]
    async fn usage_events_query_filters() {
        let (db, uid_alice, did_alice, cid) = setup_db().await;
        let (uid_bob, did_bob) = add_bob(&db).await;

        // Bob needs device_users for did_alice app (shared device test).
        {
            let conn = db.lock().await;
            conn.execute(
                "INSERT INTO device_users (device_id, user_id, first_seen_at, last_seen_at)
                 VALUES (?1, ?2, datetime('now'), datetime('now'))",
                rusqlite::params![did_alice, uid_bob],
            )
            .unwrap();
        }

        let conn = db.lock().await;
        record_ui_event(
            &conn,
            did_alice,
            uid_alice,
            "test-app",
            Some(cid),
            EventType::TodoDone,
            GAP,
        );
        record_ui_event(
            &conn,
            did_bob,
            uid_bob,
            "test-app",
            Some(cid),
            EventType::TodoSchedule,
            GAP,
        );
        record_llm_turn(
            &conn,
            did_alice,
            uid_alice,
            "test-app",
            Some(cid),
            0.05,
            GAP,
        );

        let epoch_from = DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z")
            .unwrap()
            .to_utc();
        let epoch_to = DateTime::parse_from_rfc3339("2099-01-01T00:00:00Z")
            .unwrap()
            .to_utc();

        // Filter by event_type.
        let filter = EventsFilter {
            from: epoch_from,
            to: epoch_to,
            event_type: Some(EventType::TodoDone),
            ..Default::default()
        };
        let rows = query_events(&conn, &filter);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_type, "todo_done");

        // Filter by user (alice only).
        let filter = EventsFilter {
            from: epoch_from,
            to: epoch_to,
            user: Some("alice".to_string()),
            ..Default::default()
        };
        let rows = query_events(&conn, &filter);
        assert_eq!(rows.len(), 2, "alice has todo_done + llm_turn");
        assert!(rows.iter().all(|r| r.user == "alice"));
    }

    #[tokio::test]
    async fn usage_disconnect_after_startup_cleanup_is_noop() {
        // Open a session, close it via startup cleanup (simulating a prior server
        // run), then call record_ws_disconnect. Neither session count nor event
        // count must change — the INSERT...SELECT WHERE ended_at IS NULL guard
        // must prevent any row from being inserted when no open session exists.
        let (db, uid, did, cid) = setup_db().await;
        let conn = db.lock().await;
        record_ws_connect(&conn, did, uid, "test-app", Some(cid), GAP);
        close_open_sessions_on_startup(&conn);
        let sessions_before: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_sessions", [], |r| r.get(0))
            .unwrap();
        let events_before: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_events", [], |r| r.get(0))
            .unwrap();
        record_ws_disconnect(&conn, did, uid, "test-app", Some(cid));
        let sessions_after: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_sessions", [], |r| r.get(0))
            .unwrap();
        let events_after: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            sessions_before, sessions_after,
            "disconnect after startup cleanup must not alter session count"
        );
        assert_eq!(
            events_before, events_after,
            "disconnect after startup cleanup must not insert a spurious event row"
        );
    }

    #[tokio::test]
    async fn prune_usage_before_removes_old_events_and_sessions() {
        // Insert a closed session + event older than the cutoff; assert both
        // are gone after pruning.
        let (db, uid, did, _cid) = setup_db().await;
        let conn = db.lock().await;

        let old_ts = "2020-01-01T00:00:00Z";
        conn.execute(
            "INSERT INTO usage_sessions
                 (device_id, user_id, app_slug, conversation_id, started_at,
                  last_activity_at, ended_at, llm_turns, ui_interactions, total_cost_usd)
             VALUES (?1, ?2, 'test-app', NULL, ?3, ?3, ?3, 0, 0, 0.0)",
            rusqlite::params![did, uid, old_ts],
        )
        .unwrap();
        let sid = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO usage_events
                 (session_id, device_id, user_id, app_slug, event_type, conversation_id,
                  turn_cost_usd, created_at)
             VALUES (?1, ?2, ?3, 'test-app', 'ws_connect', NULL, NULL, ?4)",
            rusqlite::params![sid, did, uid, old_ts],
        )
        .unwrap();

        let cutoff = DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
            .unwrap()
            .to_utc();
        prune_usage_before(&conn, cutoff);

        let events: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_events", [], |r| r.get(0))
            .unwrap();
        let sessions: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_sessions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(events, 0, "old events must be pruned");
        assert_eq!(sessions, 0, "orphaned old session must be pruned");
    }

    #[tokio::test]
    async fn prune_usage_before_retains_session_with_surviving_event() {
        // Insert a closed session older than the cutoff, but with one event
        // newer than the cutoff. The session must survive because the NOT EXISTS
        // guard finds the surviving event.
        let (db, uid, did, _cid) = setup_db().await;
        let conn = db.lock().await;

        let old_ts = "2020-01-01T00:00:00Z";
        let new_ts = "2030-01-01T00:00:00Z";
        conn.execute(
            "INSERT INTO usage_sessions
                 (device_id, user_id, app_slug, conversation_id, started_at,
                  last_activity_at, ended_at, llm_turns, ui_interactions, total_cost_usd)
             VALUES (?1, ?2, 'test-app', NULL, ?3, ?3, ?3, 0, 0, 0.0)",
            rusqlite::params![did, uid, old_ts],
        )
        .unwrap();
        let sid = conn.last_insert_rowid();
        // Old event (will be pruned) + new event (survives).
        conn.execute(
            "INSERT INTO usage_events
                 (session_id, device_id, user_id, app_slug, event_type, conversation_id,
                  turn_cost_usd, created_at)
             VALUES (?1, ?2, ?3, 'test-app', 'ws_connect', NULL, NULL, ?4)",
            rusqlite::params![sid, did, uid, old_ts],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO usage_events
                 (session_id, device_id, user_id, app_slug, event_type, conversation_id,
                  turn_cost_usd, created_at)
             VALUES (?1, ?2, ?3, 'test-app', 'ws_disconnect', NULL, NULL, ?4)",
            rusqlite::params![sid, did, uid, new_ts],
        )
        .unwrap();

        let cutoff = DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
            .unwrap()
            .to_utc();
        prune_usage_before(&conn, cutoff);

        // The old event is gone; the new one and the session survive.
        let events: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_events", [], |r| r.get(0))
            .unwrap();
        let sessions: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_sessions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(events, 1, "only the new event must survive");
        assert_eq!(
            sessions, 1,
            "session with a surviving event must be retained"
        );
    }

    #[tokio::test]
    async fn usage_disconnect_without_open_session_is_noop() {
        // No record_ws_connect before this call; must not create a session.
        let (db, uid, did, cid) = setup_db().await;
        let conn = db.lock().await;
        record_ws_disconnect(&conn, did, uid, "test-app", Some(cid));
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_sessions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "orphan disconnect must not create a session");
    }

    #[tokio::test]
    async fn parse_ts_str_accepts_rfc3339() {
        let dt = parse_ts_str("2026-05-01T12:34:56Z").expect("valid RFC3339 must parse");
        assert_eq!(dt.year(), 2026);
        assert_eq!(dt.month(), 5);
        assert_eq!(dt.day(), 1);
    }

    #[tokio::test]
    async fn parse_ts_str_accepts_bare_date_as_utc_midnight() {
        let dt = parse_ts_str("2026-05-01").expect("bare YYYY-MM-DD must parse");
        assert_eq!(dt.year(), 2026);
        assert_eq!(dt.month(), 5);
        assert_eq!(dt.day(), 1);
        assert_eq!(dt.hour(), 0);
        assert_eq!(dt.minute(), 0);
        assert_eq!(dt.second(), 0);
    }

    #[tokio::test]
    async fn parse_ts_str_rejects_invalid() {
        assert!(parse_ts_str("not-a-date").is_err());
        assert!(parse_ts_str("2026-13-01").is_err());
    }

    #[tokio::test]
    async fn usage_llm_turn_event_and_counter_consistent() {
        // Regression guard: if the INSERT+UPDATE transaction were reverted to bare
        // execute() calls, this test still passes — but it establishes a baseline
        // that both event row and counter agree after a successful record_llm_turn.
        let (db, uid, did, cid) = setup_db().await;
        let conn = db.lock().await;
        let sid = record_ws_connect(&conn, did, uid, "test-app", Some(cid), GAP);
        record_llm_turn(&conn, did, uid, "test-app", Some(cid), 0.01, GAP);

        let (llm_turns,): (i64,) = conn
            .query_row(
                "SELECT llm_turns FROM usage_sessions WHERE id = ?1",
                rusqlite::params![sid],
                |r| Ok((r.get(0)?,)),
            )
            .unwrap();
        let event_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM usage_events WHERE session_id = ?1 AND event_type = 'llm_turn'",
                rusqlite::params![sid],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(llm_turns, 1);
        assert_eq!(
            event_count, 1,
            "event row count must match llm_turns counter"
        );
    }

    // ── T1–T8: coverage-gap tests ─────────────────────────────────────────────

    /// T1: resolve_sender_for_conversation returns Some when a message with sender
    /// fields exists.
    #[tokio::test]
    async fn resolve_sender_found() {
        let (db, uid, did, cid) = setup_db().await;
        let conn = db.lock().await;
        crate::conversation::append_message(
            &conn,
            cid,
            crate::conversation::MessageDirection::Outgoing,
            "user",
            None,
            None,
            "hello",
            Some(uid),
            None,
            Some(did),
        );
        let result = resolve_sender_for_conversation(&conn, cid);
        assert_eq!(result, Some((did, uid)));
    }

    /// T2: resolve_sender_for_conversation returns None when the conversation has
    /// no messages.
    #[tokio::test]
    async fn resolve_sender_none_when_no_messages() {
        let (db, _uid, _did, cid) = setup_db().await;
        let conn = db.lock().await;
        let result = resolve_sender_for_conversation(&conn, cid);
        assert!(
            result.is_none(),
            "expected None for conversation with no messages"
        );
    }

    /// T3: query_sessions device filter matches guessed_slug when no assigned_slug
    /// is set.
    #[tokio::test]
    async fn query_sessions_device_filter_guessed_slug() {
        let (db, uid, did, cid) = setup_db().await;
        let conn = db.lock().await;

        let t = "2026-06-01T00:00:00Z";
        insert_test_session(&conn, did, uid, "test-app", cid, t, t, Some(t));

        let (from, to) = wide_window();

        // Match on guessed_slug.
        let filter = SessionsFilter {
            from,
            to,
            device: Some("chrome-linux".to_string()),
            ..Default::default()
        };
        let rows = query_sessions(&conn, &filter);
        assert_eq!(rows.len(), 1, "expected one row for guessed_slug match");

        // Miss on nonexistent slug.
        let filter_miss = SessionsFilter {
            from,
            to,
            device: Some("nonexistent".to_string()),
            ..Default::default()
        };
        let rows_miss = query_sessions(&conn, &filter_miss);
        assert_eq!(rows_miss.len(), 0, "expected zero rows for unknown slug");
    }

    /// T4: query_sessions device filter uses COALESCE — assigned_slug takes
    /// precedence over guessed_slug.
    #[tokio::test]
    async fn query_sessions_device_filter_assigned_slug() {
        let (db, uid, did, cid) = setup_db().await;
        let conn = db.lock().await;

        // Set assigned_slug on Alice's device_users row.
        conn.execute(
            "UPDATE device_users SET assigned_slug = 'my-laptop'
             WHERE device_id = ?1 AND user_id = ?2",
            rusqlite::params![did, uid],
        )
        .unwrap();

        let t = "2026-06-01T00:00:00Z";
        insert_test_session(&conn, did, uid, "test-app", cid, t, t, Some(t));

        let (from, to) = wide_window();

        // assigned_slug matches.
        let filter = SessionsFilter {
            from,
            to,
            device: Some("my-laptop".to_string()),
            ..Default::default()
        };
        let rows = query_sessions(&conn, &filter);
        assert_eq!(rows.len(), 1, "assigned_slug should match");

        // guessed_slug should NOT match when assigned_slug is set (COALESCE
        // picks assigned_slug, so querying guessed_slug returns nothing).
        let filter_guessed = SessionsFilter {
            from,
            to,
            device: Some("chrome-linux".to_string()),
            ..Default::default()
        };
        let rows_guessed = query_sessions(&conn, &filter_guessed);
        assert_eq!(
            rows_guessed.len(),
            0,
            "guessed_slug must not match when assigned_slug is set"
        );
    }

    /// T5: query_events device filter.
    #[tokio::test]
    async fn query_events_device_filter() {
        let (db, uid, did, cid) = setup_db().await;
        let conn = db.lock().await;

        record_ui_event(
            &conn,
            did,
            uid,
            "test-app",
            Some(cid),
            EventType::TodoDone,
            GAP,
        );

        let (from, to) = wide_window();

        let filter = EventsFilter {
            from,
            to,
            device: Some("chrome-linux".to_string()),
            ..Default::default()
        };
        let rows = query_events(&conn, &filter);
        assert_eq!(rows.len(), 1, "expected todo_done event for chrome-linux");
        assert_eq!(rows[0].event_type, "todo_done");

        let filter_miss = EventsFilter {
            from,
            to,
            device: Some("nonexistent".to_string()),
            ..Default::default()
        };
        let rows_miss = query_events(&conn, &filter_miss);
        assert_eq!(rows_miss.len(), 0, "expected zero rows for unknown slug");
    }

    /// T6: query_sessions app filter.
    #[tokio::test]
    async fn query_sessions_app_filter() {
        let (db, uid, did, cid) = setup_db().await;
        let conn = db.lock().await;

        let t1 = "2026-06-01T00:00:00Z";
        let t2 = "2026-06-02T00:00:00Z";
        insert_test_session(&conn, did, uid, "test-app", cid, t1, t1, Some(t1));
        insert_test_session(&conn, did, uid, "other-app", cid, t2, t2, Some(t2));

        let (from, to) = wide_window();

        let filter = SessionsFilter {
            from,
            to,
            app: Some("test-app".to_string()),
            ..Default::default()
        };
        let rows = query_sessions(&conn, &filter);
        assert_eq!(
            rows.len(),
            1,
            "app filter should return only test-app session"
        );
        assert_eq!(rows[0].app_slug, "test-app");
    }

    /// T7: query_events app filter.
    #[tokio::test]
    async fn query_events_app_filter() {
        let (db, uid, did, cid) = setup_db().await;
        let conn = db.lock().await;

        record_ui_event(
            &conn,
            did,
            uid,
            "test-app",
            Some(cid),
            EventType::TodoDone,
            GAP,
        );
        record_ui_event(
            &conn,
            did,
            uid,
            "other-app",
            Some(cid),
            EventType::TodoSchedule,
            GAP,
        );

        let (from, to) = wide_window();

        let filter = EventsFilter {
            from,
            to,
            app: Some("test-app".to_string()),
            ..Default::default()
        };
        let rows = query_events(&conn, &filter);
        // Only todo_done event for test-app; todo_schedule was inserted for other-app and must be excluded.
        assert!(
            rows.iter().all(|r| r.app_slug == "test-app"),
            "all returned events must be for test-app"
        );
        assert!(
            rows.iter().any(|r| r.event_type == "todo_done"),
            "todo_done event must be present"
        );
        assert!(
            rows.iter().all(|r| r.event_type != "todo_schedule"),
            "other-app events must be excluded"
        );
    }

    /// T8: compute_duration_secs exercises the open-session path (ended_at = None).
    #[tokio::test]
    async fn compute_duration_secs_open_session() {
        let (db, uid, did, cid) = setup_db().await;
        let conn = db.lock().await;

        // Use a mid-window timestamp to avoid testing boundary semantics.
        let started_at = "2026-06-01T00:00:00Z";
        let last_activity_at = "2026-06-01T00:30:00Z";
        insert_test_session(
            &conn,
            did,
            uid,
            "test-app",
            cid,
            started_at,
            last_activity_at,
            None,
        );

        let (from, to) = wide_window();

        let filter = SessionsFilter {
            from,
            to,
            ..Default::default()
        };
        let rows = query_sessions(&conn, &filter);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].duration_secs, 1800,
            "open session: duration = last_activity_at - started_at = 1800s"
        );
        assert!(rows[0].open, "session must be reported as open");
    }
}
