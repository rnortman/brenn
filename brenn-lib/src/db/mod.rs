use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use rusqlite::Connection;
use tokio::sync::Mutex;

/// App-wide database handle. Single mutex-guarded connection.
/// Two users — there's no contention worth optimizing for.
pub type Db = Arc<Mutex<Connection>>;

/// Format a UTC timestamp for storage in SQLite.
///
/// Always emits RFC 3339 with the `+00:00` offset form (e.g.
/// `"2026-05-14T12:00:00+00:00"`), never the `Z` shorthand.
/// The distinction matters for `MAX(ts)` / `MIN(ts)` queries because `Z`
/// (0x5A) lex-compares *greater than* `+` (0x2B), so a `Z`-form row would
/// sort above a same-instant `+00:00`-form row.
pub fn format_ts_for_db(ts: DateTime<Utc>) -> String {
    // SecondsFormat::Secs + use_z=false: structurally enforces +00:00, never Z.
    ts.to_rfc3339_opts(chrono::SecondsFormat::Secs, false)
}

/// Open (or create) the SQLite database and run all migrations.
///
/// The database is opened in WAL mode for better concurrent read behavior.
/// Pragmas are set for safety: foreign keys enforced, busy timeout for WAL.
pub fn init_db(path: &Path) -> Db {
    let conn = Connection::open(path).expect("failed to open database");

    // WAL mode: better concurrency, crash safety.
    conn.pragma_update(None, "journal_mode", "WAL")
        .expect("failed to set WAL mode");
    // Enforce foreign key constraints (SQLite has them off by default).
    conn.pragma_update(None, "foreign_keys", "ON")
        .expect("failed to enable foreign keys");
    // Busy timeout: wait up to 5s if the DB is locked (WAL writer contention).
    conn.pragma_update(None, "busy_timeout", 5000)
        .expect("failed to set busy timeout");

    run_migrations(&conn);

    Arc::new(Mutex::new(conn))
}

/// Open an in-memory database for tests. Same pragmas and migrations as production.
/// Not gated behind #[cfg(test)] because the brenn binary crate's tests also use it.
pub fn init_db_memory() -> Db {
    let conn = Connection::open_in_memory().expect("failed to open in-memory database");

    conn.pragma_update(None, "foreign_keys", "ON")
        .expect("failed to enable foreign keys");

    run_migrations(&conn);

    Arc::new(Mutex::new(conn))
}

/// Run base (non-messaging) schema migrations. Idempotent — uses IF NOT EXISTS on all DDL.
///
/// Creates users, sessions, invite_codes, conversations, messages, and all other
/// core tables. Does NOT create messaging tables. Used by test helpers that need
/// the base schema without the messaging schema (e.g., migration tests that set
/// up a legacy messaging schema by hand).
pub(crate) fn run_base_migrations(conn: &Connection) {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            csrf_token TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS invite_codes (
            code TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            used_by INTEGER REFERENCES users(id),
            used_at TEXT
        );

        CREATE TABLE IF NOT EXISTS conversations (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            cc_session_id TEXT,
            title TEXT,
            model TEXT,
            cwd TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            total_cost_usd REAL,
            app_slug TEXT NOT NULL DEFAULT '',
            shared INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL REFERENCES conversations(id),
            seq INTEGER NOT NULL,
            direction TEXT NOT NULL,
            msg_type TEXT NOT NULL,
            cc_uuid TEXT,
            parent_tool_use_id TEXT,
            payload TEXT NOT NULL,
            created_at TEXT NOT NULL,
            sender_user_id INTEGER REFERENCES users(id),
            sender_tz TEXT,
            UNIQUE(conversation_id, seq)
        );

        CREATE INDEX IF NOT EXISTS idx_conversations_user_id ON conversations(user_id);
        CREATE INDEX IF NOT EXISTS idx_conversations_cc_session_id ON conversations(cc_session_id);
        CREATE INDEX IF NOT EXISTS idx_conversations_app_slug ON conversations(app_slug);
        CREATE INDEX IF NOT EXISTS idx_conversations_app_shared ON conversations(app_slug, shared);
        -- idx_messages_conversation_id covers (conversation_id, rowid) implicitly via
        -- SQLite's rowid alias for INTEGER PRIMARY KEY. It handles both the conversation
        -- filter and ORDER BY id DESC without a sort step, so idx_messages_conv_id
        -- (conversation_id, id) is structurally redundant. Dropped below.
        CREATE INDEX IF NOT EXISTS idx_messages_conversation_id ON messages(conversation_id);
        -- Composite index for bounded history replay queries (seam detection,
        -- replayable message counting, artifact cache, simplified pagination).
        CREATE INDEX IF NOT EXISTS idx_messages_conv_type_seq ON messages(conversation_id, msg_type, seq);

        CREATE TABLE IF NOT EXISTS message_attachments (
            upload_id TEXT PRIMARY KEY,
            message_id INTEGER NOT NULL REFERENCES messages(id),
            filename TEXT NOT NULL,
            media_type TEXT NOT NULL,
            size INTEGER NOT NULL,
            disk_filename TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_message_attachments_message_id
            ON message_attachments(message_id);

        CREATE TABLE IF NOT EXISTS app_models (
            app_slug TEXT PRIMARY KEY,
            models_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS approval_rules (
            id INTEGER PRIMARY KEY,
            app_slug TEXT NOT NULL,
            conversation_id INTEGER REFERENCES conversations(id),
            tool_name TEXT NOT NULL,
            pattern TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_approval_rules_app
            ON approval_rules(app_slug) WHERE conversation_id IS NULL;
        CREATE INDEX IF NOT EXISTS idx_approval_rules_conversation
            ON approval_rules(conversation_id) WHERE conversation_id IS NOT NULL;

        CREATE TABLE IF NOT EXISTS pending_tool_requests (
            request_id TEXT PRIMARY KEY,
            conversation_id INTEGER NOT NULL REFERENCES conversations(id),
            tool_name TEXT NOT NULL,
            tool_input TEXT NOT NULL,
            extra TEXT,
            status TEXT NOT NULL DEFAULT 'pending'
                CHECK(status IN ('pending', 'completed', 'denied')),
            result TEXT,
            delivered_to_cc INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            resolved_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_pending_tool_requests_conversation
            ON pending_tool_requests(conversation_id, status);

        -- Persisted `last_notified_head` cursor for repo_sync advance detection.
        -- See docs/designs/repo-sync-last-notified-head-loss-across-restart.md.
        -- Written in the same transaction as the corresponding event inserts
        -- so a crash mid-fan-out cannot drop the advance notification across
        -- restart. `updated_at` is forensics-only — no read path depends on
        -- it; keep it for post-hoc debugging of when a cursor last advanced.
        CREATE TABLE IF NOT EXISTS repo_sync_cursor (
            repo_slug  TEXT PRIMARY KEY,
            head       TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        -- Max context-window size keyed by model slug (including suffix).
        -- Seeded from result.modelUsage.contextWindow on each turn completion.
        -- Used at session start to supply an initial max_tokens before the
        -- first result frame arrives.
        CREATE TABLE IF NOT EXISTS model_window_cache (
            model_slug  TEXT PRIMARY KEY,
            max_tokens  INTEGER NOT NULL,
            cc_version  TEXT,
            updated_at  TEXT NOT NULL
        );

        -- Per-turn cost samples for the 24-hour rolling-window aggregate.
        -- Pruned eagerly on every turn completion (rows older than 24h deleted).
        -- `created_at` is Unix epoch seconds (INTEGER) for fast integer comparison.
        CREATE TABLE IF NOT EXISTS cost_samples (
            id              INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL REFERENCES conversations(id),
            turn_cost_usd   REAL NOT NULL,
            created_at      INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_cost_samples_created
            ON cost_samples(created_at);

        -- Persistent per-browser-profile device records.
        -- `token` is a 64-char hex CSPRNG value (32 bytes), globally unique.
        -- `guessed_slug` is computed once at creation from UA + platform; globally unique.
        CREATE TABLE IF NOT EXISTS devices (
            id              INTEGER PRIMARY KEY,
            token           TEXT NOT NULL UNIQUE,
            guessed_slug    TEXT NOT NULL,
            platform        TEXT,
            user_agent      TEXT,
            screen_width    INTEGER,
            screen_height   INTEGER,
            last_seen_at    TEXT NOT NULL,
            created_at      TEXT NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_devices_guessed_slug
            ON devices(guessed_slug);

        -- Many:many join between devices and users.
        -- `assigned_slug` is the LLM-assigned per-user human-friendly name.
        -- `slug_prompted_at` is the per-(user,device) rate-limit cursor for
        --   the unassigned-slug nudge.
        CREATE TABLE IF NOT EXISTS device_users (
            device_id        INTEGER NOT NULL REFERENCES devices(id),
            user_id          INTEGER NOT NULL REFERENCES users(id),
            assigned_slug    TEXT,
            first_seen_at    TEXT NOT NULL,
            last_seen_at     TEXT NOT NULL,
            slug_prompted_at TEXT,
            PRIMARY KEY (device_id, user_id)
        );
        CREATE INDEX IF NOT EXISTS idx_device_users_user
            ON device_users(user_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_device_users_user_slug
            ON device_users(user_id, assigned_slug)
            WHERE assigned_slug IS NOT NULL;
        ",
    )
    .expect("failed to run database migrations");

    // Drop the redundant composite index added before the rowid-alias equivalence was
    // recognized. idx_messages_conversation_id already covers (conversation_id, rowid)
    // via SQLite's INTEGER PRIMARY KEY rowid alias, so idx_messages_conv_id is dead weight.
    // IF EXISTS makes this idempotent — new DBs never had it; existing DBs drop it once.
    conn.execute_batch("DROP INDEX IF EXISTS idx_messages_conv_id;")
        .expect("drop idx_messages_conv_id");

    // Migrate cost_samples.created_at from TEXT to INTEGER if needed.
    //
    // This table was introduced with `created_at TEXT`; cycle 23 changed it to INTEGER.
    // `CREATE TABLE IF NOT EXISTS` is a no-op on existing databases — the column type on
    // disk stays TEXT. Fix: detect the old column type and rebuild the table.
    //
    // cost_samples holds only ephemeral 24-hour rolling-window data. Dropping the old
    // rows on upgrade is acceptable; losing at most 24h of cost history does not affect
    // correctness (the 24h aggregate starts fresh and self-heals within one day).
    if column_has_type(conn, "cost_samples", "created_at", "TEXT") {
        conn.execute_batch(
            "DROP TABLE IF EXISTS cost_samples;
             CREATE TABLE cost_samples (
                 id              INTEGER PRIMARY KEY,
                 conversation_id INTEGER NOT NULL REFERENCES conversations(id),
                 turn_cost_usd   REAL NOT NULL,
                 created_at      INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_cost_samples_created
                 ON cost_samples(created_at);",
        )
        .expect("migrate cost_samples.created_at TEXT→INTEGER");
    }

    // Add sender_device_id column to messages if not present.
    add_column_if_missing(
        conn,
        "messages",
        "sender_device_id",
        "INTEGER REFERENCES devices(id)",
    );

    // Add unenrolled_at column to devices if not present.
    // NULL means enrolled; a non-NULL ms-epoch integer records when the device
    // was unenrolled. Added as a nullable column so no backfill is needed —
    // all existing rows are implicitly enrolled (NULL).
    add_column_if_missing(conn, "devices", "unenrolled_at", "INTEGER");

    // Add timezone-override columns to device_users if not present.
    // `tz_override` is a nullable IANA zone name; NULL means no override (use browser TZ).
    // `tz_override_expires_at` is a nullable Unix epoch seconds value; NULL means no expiry.
    // Both default NULL — no backfill needed; existing rows have no override.
    add_column_if_missing(conn, "device_users", "tz_override", "TEXT");
    add_column_if_missing(conn, "device_users", "tz_override_expires_at", "INTEGER");

    // Active-devices view: the safe default for any query that should exclude
    // unenrolled devices. SQLite views are syntactic rewrites; the query planner
    // inlines `WHERE unenrolled_at IS NULL` at compile time, so there is no
    // runtime overhead relative to querying `devices` directly with the predicate.
    //
    // Rule: use `active_devices` for all read paths (listing, slug resolution,
    // visibility sets). Use `devices` directly only for admin/audit operations:
    // unenroll_device, assign_guessed_slug_in_tx, find_device_by_token, and
    // load_device / load_device_user (which are gated by auth middleware).
    conn.execute_batch(
        "CREATE VIEW IF NOT EXISTS active_devices AS \
         SELECT id, token, guessed_slug, platform, user_agent, \
                screen_width, screen_height, last_seen_at, created_at \
         FROM devices \
         WHERE unenrolled_at IS NULL;",
    )
    .expect("create active_devices view");
}

/// Run all migrations. Idempotent — uses IF NOT EXISTS on all DDL.
fn run_migrations(conn: &Connection) {
    run_base_migrations(conn);

    // Messaging tables. Idempotent — see `messaging::db::run_messaging_migrations`.
    crate::messaging::db::run_messaging_migrations(conn);

    // Usage observability tables (usage_sessions, usage_events).
    crate::usage::run_usage_migrations(conn);

    // PWA push subscription table.
    crate::pwa_push::db::run_pwa_push_migrations(conn);

    // Automation engine tables (automation_jobs, automation_fires,
    // automation_app_event_conversation).
    crate::automation::db::run_automation_migrations(conn);
}

/// Check whether a column exists in a table (via PRAGMA table_info).
pub(crate) fn column_exists(conn: &Connection, table: &str, column: &str) -> bool {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .expect("prepare PRAGMA table_info");
    stmt.query_map([], |row| row.get::<_, String>(1))
        .expect("query table_info")
        .any(|r| r.expect("read column name") == column)
}

/// Check whether a column in a table has the specified declared type (case-insensitive).
/// Returns false if the table or column does not exist.
fn column_has_type(conn: &Connection, table: &str, column: &str, col_type: &str) -> bool {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .expect("prepare PRAGMA table_info");
    stmt.query_map([], |row| {
        Ok((row.get::<_, String>(1)?, row.get::<_, String>(2)?))
    })
    .expect("query table_info")
    .any(|r| {
        let (name, ty) = r.expect("read column info");
        name == column && ty.eq_ignore_ascii_case(col_type)
    })
}

/// Add a column to a table if it does not already exist.
/// `col_def` is the column name + type + constraints (everything after the column name).
fn add_column_if_missing(conn: &Connection, table: &str, column: &str, col_def: &str) {
    if !column_exists(conn, table, column) {
        conn.execute_batch(&format!(
            "ALTER TABLE {table} ADD COLUMN {column} {col_def}"
        ))
        .unwrap_or_else(|e| panic!("add_column_if_missing {table}.{column}: {e}"));
    }
}

/// Save the available model list for an app (upsert).
pub fn save_app_models(conn: &Connection, app_slug: &str, models: &[crate::ws_types::ModelInfo]) {
    let json = serde_json::to_string(models).expect("serialize models");
    let now = format_ts_for_db(chrono::Utc::now());
    conn.execute(
        "INSERT INTO app_models (app_slug, models_json, updated_at) VALUES (?1, ?2, ?3)
         ON CONFLICT(app_slug) DO UPDATE SET models_json = ?2, updated_at = ?3",
        rusqlite::params![app_slug, json, now],
    )
    .expect("save_app_models");
}

/// Load the available model list for an app. Returns empty vec if none stored.
pub fn load_app_models(conn: &Connection, app_slug: &str) -> Vec<crate::ws_types::ModelInfo> {
    conn.query_row(
        "SELECT models_json FROM app_models WHERE app_slug = ?1",
        rusqlite::params![app_slug],
        |row| {
            let json: String = row.get(0)?;
            Ok(serde_json::from_str(&json).unwrap_or_default())
        },
    )
    .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Approval rules
// ---------------------------------------------------------------------------

/// Insert a new approval rule. Returns the new row id.
///
/// `conversation_id` is `None` for app-wide (permanent) rules,
/// `Some(id)` for conversation-scoped rules.
pub fn insert_approval_rule(
    conn: &Connection,
    app_slug: &str,
    conversation_id: Option<i64>,
    tool_name: &str,
    pattern: &str,
) -> i64 {
    let now = format_ts_for_db(chrono::Utc::now());
    conn.execute(
        "INSERT INTO approval_rules (app_slug, conversation_id, tool_name, pattern, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![app_slug, conversation_id, tool_name, pattern, now],
    )
    .expect("insert_approval_rule");
    conn.last_insert_rowid()
}

/// Load approval rules for a bridge: app-wide rules (conversation_id IS NULL)
/// plus conversation-specific rules for the given conversation.
pub fn load_approval_rules(
    conn: &Connection,
    app_slug: &str,
    conversation_id: i64,
) -> Vec<crate::approval_rules::ApprovalRuleRow> {
    let mut stmt = conn
        .prepare(
            "SELECT id, app_slug, conversation_id, tool_name, pattern
             FROM approval_rules
             WHERE app_slug = ?1 AND (conversation_id IS NULL OR conversation_id = ?2)
             ORDER BY id",
        )
        .expect("prepare load_approval_rules");
    let rows = stmt
        .query_map(rusqlite::params![app_slug, conversation_id], |row| {
            Ok(crate::approval_rules::ApprovalRuleRow {
                id: row.get(0)?,
                app_slug: row.get(1)?,
                conversation_id: row.get(2)?,
                tool_name: row.get(3)?,
                pattern: row.get(4)?,
            })
        })
        .expect("query load_approval_rules");
    rows.map(|r| r.expect("read approval_rules row")).collect()
}

/// Delete an approval rule by id.
pub fn delete_approval_rule(conn: &Connection, id: i64) {
    conn.execute(
        "DELETE FROM approval_rules WHERE id = ?1",
        rusqlite::params![id],
    )
    .expect("delete_approval_rule");
}

/// Load all cached model lists, keyed by app slug.
pub fn load_all_app_models(
    conn: &Connection,
) -> std::collections::HashMap<String, Vec<crate::ws_types::ModelInfo>> {
    let mut stmt = conn
        .prepare("SELECT app_slug, models_json FROM app_models")
        .expect("prepare load_all_app_models");
    let rows = stmt
        .query_map([], |row| {
            let slug: String = row.get(0)?;
            let json: String = row.get(1)?;
            let models: Vec<crate::ws_types::ModelInfo> =
                serde_json::from_str(&json).unwrap_or_default();
            Ok((slug, models))
        })
        .expect("query load_all_app_models");
    rows.filter_map(|r| r.ok()).collect()
}

// ---------------------------------------------------------------------------
// Pending tool requests (async interactive tools)
// ---------------------------------------------------------------------------

/// A pending interactive tool request stored in the database.
#[derive(Debug, Clone)]
pub struct PendingToolRequest {
    pub request_id: String,
    pub conversation_id: i64,
    pub tool_name: String,
    pub tool_input: String,
    pub extra: Option<String>,
    pub status: String,
    pub result: Option<String>,
    pub delivered_to_cc: bool,
    pub created_at: String,
    pub resolved_at: Option<String>,
}

/// Insert a new pending tool request. Panics on DB errors (fail-fast).
pub fn insert_pending_tool_request(
    conn: &Connection,
    request_id: &str,
    conversation_id: i64,
    tool_name: &str,
    tool_input: &str,
    extra: Option<&str>,
) {
    let now = format_ts_for_db(chrono::Utc::now());
    conn.execute(
        "INSERT INTO pending_tool_requests \
         (request_id, conversation_id, tool_name, tool_input, extra, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        rusqlite::params![
            request_id,
            conversation_id,
            tool_name,
            tool_input,
            extra,
            now
        ],
    )
    .expect("insert_pending_tool_request");
}

/// Atomically resolve a pending request. Returns true if a row was updated
/// (status was 'pending'), false if already resolved (race guard).
pub fn resolve_pending_tool_request(
    conn: &Connection,
    request_id: &str,
    new_status: &str,
    result: Option<&str>,
) -> bool {
    let now = format_ts_for_db(chrono::Utc::now());
    let rows = conn
        .execute(
            "UPDATE pending_tool_requests \
             SET status = ?1, result = ?2, resolved_at = ?3 \
             WHERE request_id = ?4 AND status = 'pending'",
            rusqlite::params![new_status, result, now, request_id],
        )
        .expect("resolve_pending_tool_request");
    rows > 0
}

/// Update the result and status of an already-claimed pending tool request.
/// Called after action execution to store the real result (the request was
/// claimed with a placeholder via `resolve_pending_tool_request`).
pub fn update_pending_tool_result(
    conn: &Connection,
    request_id: &str,
    new_status: &str,
    result: &str,
) {
    conn.execute(
        "UPDATE pending_tool_requests SET status = ?1, result = ?2 WHERE request_id = ?3",
        rusqlite::params![new_status, result, request_id],
    )
    .expect("update_pending_tool_result");
}

/// Load a single pending tool request by request_id. Returns None if not found.
/// Panics on unexpected DB errors (fail-fast).
pub fn get_pending_tool_request(conn: &Connection, request_id: &str) -> Option<PendingToolRequest> {
    match conn.query_row(
        "SELECT request_id, conversation_id, tool_name, tool_input, \
         extra, status, result, delivered_to_cc, created_at, resolved_at \
         FROM pending_tool_requests WHERE request_id = ?1",
        rusqlite::params![request_id],
        |row| Ok(row_to_pending_tool_request(row)),
    ) {
        Ok(row) => Some(row),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => panic!("get_pending_tool_request: {e}"),
    }
}

/// Load all pending tool requests for a conversation (status = 'pending'),
/// ordered by creation time.
pub fn get_pending_tool_requests_for_conversation(
    conn: &Connection,
    conversation_id: i64,
) -> Vec<PendingToolRequest> {
    let mut stmt = conn
        .prepare(
            "SELECT request_id, conversation_id, tool_name, tool_input, \
             extra, status, result, delivered_to_cc, created_at, resolved_at \
             FROM pending_tool_requests \
             WHERE conversation_id = ?1 AND status = 'pending' \
             ORDER BY created_at",
        )
        .expect("prepare get_pending_tool_requests_for_conversation");
    stmt.query_map(rusqlite::params![conversation_id], |row| {
        Ok(row_to_pending_tool_request(row))
    })
    .expect("query pending_tool_requests")
    .map(|r| r.expect("read pending_tool_request row"))
    .collect()
}

/// Load completed/denied requests that haven't been delivered to CC yet.
/// Used on CC resume to inject accumulated results.
pub fn get_undelivered_results(conn: &Connection, conversation_id: i64) -> Vec<PendingToolRequest> {
    let mut stmt = conn
        .prepare(
            "SELECT request_id, conversation_id, tool_name, tool_input, \
             extra, status, result, delivered_to_cc, created_at, resolved_at \
             FROM pending_tool_requests \
             WHERE conversation_id = ?1 AND status IN ('completed', 'denied') \
             AND delivered_to_cc = 0 \
             ORDER BY created_at",
        )
        .expect("prepare get_undelivered_results");
    stmt.query_map(rusqlite::params![conversation_id], |row| {
        Ok(row_to_pending_tool_request(row))
    })
    .expect("query undelivered results")
    .map(|r| r.expect("read undelivered result row"))
    .collect()
}

/// Mark a request as delivered to CC.
pub fn mark_delivered_to_cc(conn: &Connection, request_id: &str) {
    conn.execute(
        "UPDATE pending_tool_requests SET delivered_to_cc = 1 WHERE request_id = ?1",
        rusqlite::params![request_id],
    )
    .expect("mark_delivered_to_cc");
}

fn row_to_pending_tool_request(row: &rusqlite::Row) -> PendingToolRequest {
    PendingToolRequest {
        request_id: row.get(0).expect("request_id"),
        conversation_id: row.get(1).expect("conversation_id"),
        tool_name: row.get(2).expect("tool_name"),
        tool_input: row.get(3).expect("tool_input"),
        extra: row.get(4).expect("extra"),
        status: row.get(5).expect("status"),
        result: row.get(6).expect("result"),
        delivered_to_cc: row.get::<_, i64>(7).expect("delivered_to_cc") != 0,
        created_at: row.get(8).expect("created_at"),
        resolved_at: row.get(9).expect("resolved_at"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_ts_for_db_never_emits_z() {
        use chrono::TimeZone;
        let ts = chrono::Utc.with_ymd_and_hms(2026, 1, 15, 12, 0, 0).unwrap();
        let s = format_ts_for_db(ts);
        assert!(
            !s.ends_with('Z'),
            "format_ts_for_db must never emit Z-form; got: {s}"
        );
        assert!(
            s.ends_with("+00:00"),
            "format_ts_for_db must end with +00:00; got: {s}"
        );
    }

    #[test]
    fn migrations_run_cleanly() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        // Verify tables exist by querying them.
        conn.execute(
            "SELECT id, username, password_hash, created_at FROM users WHERE 0",
            [],
        )
        .expect("users table should exist");
        conn.execute(
            "SELECT token, user_id, created_at, expires_at, last_seen_at, csrf_token FROM sessions WHERE 0",
            [],
        )
        .expect("sessions table should exist");
        conn.execute(
            "SELECT code, created_at, used_by, used_at FROM invite_codes WHERE 0",
            [],
        )
        .expect("invite_codes table should exist");
        conn.execute(
            "SELECT id, user_id, cc_session_id, title, model, cwd, status, created_at, updated_at, total_cost_usd, app_slug, shared FROM conversations WHERE 0",
            [],
        )
        .expect("conversations table should exist");
        conn.execute(
            "SELECT id, conversation_id, seq, direction, msg_type, cc_uuid, parent_tool_use_id, payload, created_at, sender_user_id, sender_tz FROM messages WHERE 0",
            [],
        )
        .expect("messages table should exist");
        conn.execute(
            "SELECT upload_id, message_id, filename, media_type, size, disk_filename FROM message_attachments WHERE 0",
            [],
        )
        .expect("message_attachments table should exist");
        conn.execute(
            "SELECT id, app_slug, conversation_id, tool_name, pattern, created_at FROM approval_rules WHERE 0",
            [],
        )
        .expect("approval_rules table should exist");
        conn.execute(
            "SELECT request_id, conversation_id, tool_name, tool_input, \
             extra, status, result, delivered_to_cc, created_at, resolved_at \
             FROM pending_tool_requests WHERE 0",
            [],
        )
        .expect("pending_tool_requests table should exist");
        conn.execute(
            "SELECT id, envelope_type, ingress_source, ingress_summary FROM messaging_messages WHERE 0",
            [],
        )
        .expect("messaging_messages table with ingress columns should exist");
        conn.execute(
            "SELECT repo_slug, head, updated_at FROM repo_sync_cursor WHERE 0",
            [],
        )
        .expect("repo_sync_cursor table should exist");
        conn.execute(
            "SELECT id, token, guessed_slug, platform, user_agent, \
             screen_width, screen_height, last_seen_at, created_at FROM devices WHERE 0",
            [],
        )
        .expect("devices table should exist");
        conn.execute(
            "SELECT device_id, user_id, assigned_slug, first_seen_at, \
             last_seen_at, slug_prompted_at FROM device_users WHERE 0",
            [],
        )
        .expect("device_users table should exist");
        assert!(
            column_exists(&conn, "messages", "sender_device_id"),
            "messages.sender_device_id column should exist"
        );
        assert!(
            column_exists(&conn, "devices", "unenrolled_at"),
            "devices.unenrolled_at column should exist"
        );
        assert!(
            column_exists(&conn, "device_users", "tz_override"),
            "device_users.tz_override column should exist"
        );
        assert!(
            column_exists(&conn, "device_users", "tz_override_expires_at"),
            "device_users.tz_override_expires_at column should exist"
        );
    }

    #[test]
    fn approval_rules_crud() {
        let db = init_db_memory();
        let conn = db.blocking_lock();

        // Insert a conversation row first (FK target).
        conn.execute(
            "INSERT INTO users (id, username, password_hash, created_at) VALUES (1, 'test', 'hash', '2024-01-01')",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO conversations (id, user_id, status, created_at, updated_at) VALUES (1, 1, 'active', '2024-01-01', '2024-01-01')",
            [],
        ).unwrap();

        // Insert app-wide rule.
        let id1 = insert_approval_rule(&conn, "pfin", None, "Bash", "git status\\b.*");
        assert!(id1 > 0);

        // Insert conversation-scoped rule.
        let id2 = insert_approval_rule(&conn, "pfin", Some(1), "Bash", "cargo test\\b.*");
        assert!(id2 > id1);

        // Load rules for this conversation — should get both.
        let rules = load_approval_rules(&conn, "pfin", 1);
        assert_eq!(rules.len(), 2);
        assert!(rules[0].conversation_id.is_none());
        assert_eq!(rules[1].conversation_id, Some(1));

        // Load rules for a different conversation — should only get the app-wide rule.
        let rules = load_approval_rules(&conn, "pfin", 999);
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].pattern, "git status\\b.*");

        // Delete.
        delete_approval_rule(&conn, id2);
        let rules = load_approval_rules(&conn, "pfin", 1);
        assert_eq!(rules.len(), 1);
    }

    #[test]
    fn migrations_are_idempotent() {
        let conn = Connection::open_in_memory().expect("open");
        conn.pragma_update(None, "foreign_keys", "ON").expect("fk");
        run_migrations(&conn);
        run_migrations(&conn); // Second run should not fail.
    }

    /// Verify that run_migrations correctly rebuilds cost_samples when the
    /// existing table has the old `created_at TEXT` column type.
    ///
    /// Simulates the upgrade path on a pre-cycle-23 database: start with a
    /// fresh DB (gets INTEGER), then manually tear the table back to TEXT to
    /// reproduce the old schema, run migrations again, and assert that the
    /// column is restored to INTEGER and insert/sum_since work correctly.
    #[test]
    fn cost_samples_created_at_migration_from_text() {
        use crate::cost_samples;

        let conn = Connection::open_in_memory().expect("open");
        conn.pragma_update(None, "foreign_keys", "ON").expect("fk");

        // First pass: fresh database gets the correct INTEGER column.
        run_migrations(&conn);
        assert!(
            column_has_type(&conn, "cost_samples", "created_at", "INTEGER"),
            "fresh DB: created_at must be INTEGER after first migration"
        );

        // Simulate a pre-cycle-23 upgrade: drop and recreate cost_samples with
        // the old TEXT column and a stale RFC3339 row.
        conn.pragma_update(None, "foreign_keys", "OFF")
            .expect("fk off");
        conn.execute_batch(
            "DROP TABLE IF EXISTS idx_cost_samples_created;
             DROP INDEX IF EXISTS idx_cost_samples_created;
             DROP TABLE IF EXISTS cost_samples;
             CREATE TABLE cost_samples (
                 id              INTEGER PRIMARY KEY,
                 conversation_id INTEGER NOT NULL,
                 turn_cost_usd   REAL NOT NULL,
                 created_at      TEXT NOT NULL
             );
             INSERT INTO cost_samples (conversation_id, turn_cost_usd, created_at)
                 VALUES (1, 0.99, '2026-05-15T03:25:14+00:00');",
        )
        .expect("recreate cost_samples with old TEXT schema");
        conn.pragma_update(None, "foreign_keys", "ON")
            .expect("fk on");

        // Verify we now have the old TEXT column and a stale row.
        assert!(
            column_has_type(&conn, "cost_samples", "created_at", "TEXT"),
            "pre-migration pass 2: created_at should be TEXT"
        );
        let pre_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM cost_samples", [], |r| r.get(0))
            .expect("count");
        assert_eq!(
            pre_count, 1,
            "stale RFC3339 row should exist before migration"
        );

        // Second run_migrations must detect and rebuild the table.
        run_migrations(&conn);

        // Old TEXT column must be gone; INTEGER column must be present.
        assert!(
            !column_has_type(&conn, "cost_samples", "created_at", "TEXT"),
            "post-migration: created_at TEXT column must not exist"
        );
        assert!(
            column_has_type(&conn, "cost_samples", "created_at", "INTEGER"),
            "post-migration: created_at INTEGER column must exist"
        );

        // Old TEXT row was dropped (acceptable for ephemeral 24h data).
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM cost_samples", [], |r| r.get(0))
            .expect("count");
        assert_eq!(count, 0, "old TEXT rows must be dropped during migration");

        // insert and sum_since must work correctly with epoch integers on the rebuilt table.
        // Seed a user + conversation so the FK constraint on cost_samples is satisfied.
        let conv_id = {
            let uid = crate::auth::user::create_user(&conn, "mig-test", "$argon2id$fake");
            crate::conversation::create_conversation(&conn, uid, "mig-test", false)
        };
        let cutoff = chrono::Utc::now() - chrono::Duration::hours(24);
        cost_samples::insert(&conn, conv_id, 0.42);
        let total = cost_samples::sum_since(&conn, cutoff);
        assert!(
            (total - 0.42).abs() < 1e-9,
            "sum_since must return the inserted value after migration; got {total}"
        );
    }

    #[test]
    fn pending_tool_request_claim_then_update() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        conn.execute(
            "INSERT INTO users (id, username, password_hash, created_at) VALUES (1, 'test', 'hash', '2024-01-01')",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO conversations (id, user_id, status, created_at, updated_at) VALUES (1, 1, 'active', '2024-01-01', '2024-01-01')",
            [],
        ).unwrap();

        insert_pending_tool_request(&conn, "req-1", 1, "TestTool", "{}", None);

        // Claim it (resolve with placeholder).
        assert!(resolve_pending_tool_request(
            &conn,
            "req-1",
            "completed",
            Some("{}")
        ));

        // Second claim fails (already resolved).
        assert!(!resolve_pending_tool_request(
            &conn,
            "req-1",
            "completed",
            Some("{}")
        ));

        // Update with real result.
        update_pending_tool_result(&conn, "req-1", "completed", r#"{"status":"ok"}"#);

        let req = get_pending_tool_request(&conn, "req-1").unwrap();
        assert_eq!(req.status, "completed");
        assert_eq!(req.result.as_deref(), Some(r#"{"status":"ok"}"#));
    }

    #[test]
    fn pending_tool_request_denied_status() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        conn.execute(
            "INSERT INTO users (id, username, password_hash, created_at) VALUES (1, 'test', 'hash', '2024-01-01')",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO conversations (id, user_id, status, created_at, updated_at) VALUES (1, 1, 'active', '2024-01-01', '2024-01-01')",
            [],
        ).unwrap();

        insert_pending_tool_request(&conn, "req-d", 1, "TestTool", "{}", None);
        assert!(resolve_pending_tool_request(
            &conn,
            "req-d",
            "denied",
            Some(r#"{"reason":"no"}"#)
        ));

        let req = get_pending_tool_request(&conn, "req-d").unwrap();
        assert_eq!(req.status, "denied");
        assert!(!req.delivered_to_cc);
    }

    #[test]
    fn undelivered_results_only_returns_undelivered() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        conn.execute(
            "INSERT INTO users (id, username, password_hash, created_at) VALUES (1, 'test', 'hash', '2024-01-01')",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO conversations (id, user_id, status, created_at, updated_at) VALUES (1, 1, 'active', '2024-01-01', '2024-01-01')",
            [],
        ).unwrap();

        // Two requests: one delivered, one not.
        insert_pending_tool_request(&conn, "req-a", 1, "T", "{}", None);
        insert_pending_tool_request(&conn, "req-b", 1, "T", "{}", None);

        resolve_pending_tool_request(&conn, "req-a", "completed", Some("{}"));
        resolve_pending_tool_request(&conn, "req-b", "completed", Some("{}"));

        mark_delivered_to_cc(&conn, "req-a");

        let undelivered = get_undelivered_results(&conn, 1);
        assert_eq!(undelivered.len(), 1);
        assert_eq!(undelivered[0].request_id, "req-b");
    }

    #[test]
    fn pending_requests_excludes_resolved() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        conn.execute(
            "INSERT INTO users (id, username, password_hash, created_at) VALUES (1, 'test', 'hash', '2024-01-01')",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO conversations (id, user_id, status, created_at, updated_at) VALUES (1, 1, 'active', '2024-01-01', '2024-01-01')",
            [],
        ).unwrap();

        insert_pending_tool_request(&conn, "req-p", 1, "T", "{}", None);
        insert_pending_tool_request(&conn, "req-c", 1, "T", "{}", None);

        resolve_pending_tool_request(&conn, "req-c", "completed", Some("{}"));

        let pending = get_pending_tool_requests_for_conversation(&conn, 1);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].request_id, "req-p");
    }
}
