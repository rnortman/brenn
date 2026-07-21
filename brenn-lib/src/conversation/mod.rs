use std::fmt;

use chrono::Utc;
use rusqlite::{Connection, OptionalExtension};

use crate::db::format_ts_for_db;

/// Generate a SELECT query with the conversation column list.
/// Keeps the column list in one place without runtime `format!()` allocations.
macro_rules! select_conversations {
    ($tail:expr) => {
        concat!(
            "SELECT id, user_id, cc_session_id, title, model, cwd, status, \
             created_at, updated_at, total_cost_usd, app_slug, shared ",
            $tail
        )
    };
}

/// Generate a SELECT query with the message column list.
macro_rules! select_messages {
    ($tail:expr) => {
        concat!(
            "SELECT id, conversation_id, seq, direction, msg_type, cc_uuid, \
             parent_tool_use_id, payload, created_at, sender_user_id, sender_tz ",
            $tail
        )
    };
}

/// Status of a conversation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversationStatus {
    Active,
    Completed,
    Error,
}

impl fmt::Display for ConversationStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Completed => write!(f, "completed"),
            Self::Error => write!(f, "error"),
        }
    }
}

impl ConversationStatus {
    fn from_db(s: &str) -> Self {
        match s {
            "active" => Self::Active,
            "completed" => Self::Completed,
            "error" => Self::Error,
            other => panic!("invalid conversation status in database: {other:?}"),
        }
    }
}

/// Direction of a message relative to Brenn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageDirection {
    /// CC -> Brenn
    Incoming,
    /// Brenn -> CC
    Outgoing,
}

impl fmt::Display for MessageDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Incoming => write!(f, "incoming"),
            Self::Outgoing => write!(f, "outgoing"),
        }
    }
}

impl MessageDirection {
    fn from_db(s: &str) -> Self {
        match s {
            "incoming" => Self::Incoming,
            "outgoing" => Self::Outgoing,
            other => panic!("invalid message direction in database: {other:?}"),
        }
    }
}

/// A conversation between a user and CC.
#[derive(Debug, Clone)]
pub struct Conversation {
    pub id: i64,
    pub user_id: i64,
    pub cc_session_id: Option<String>,
    pub title: Option<String>,
    pub model: Option<String>,
    pub cwd: Option<String>,
    pub status: ConversationStatus,
    pub created_at: String,
    pub updated_at: String,
    pub total_cost_usd: Option<f64>,
    /// App slug this conversation belongs to. Empty string for pre-multi-app conversations.
    pub app_slug: String,
    /// Whether this conversation is shared (visible to all app users) or private (owner only).
    pub shared: bool,
}

/// Pre-joined conversation row for building sidebar summaries.
///
/// Includes message count and optional owner username so the caller
/// doesn't need per-row queries (avoids N+1).
#[derive(Debug, Clone)]
pub struct ConversationListRow {
    pub id: i64,
    pub user_id: i64,
    pub title: Option<String>,
    pub model: Option<String>,
    pub status: ConversationStatus,
    pub updated_at: String,
    pub shared: bool,
    pub message_count: i64,
    pub owner_username: Option<String>,
}

/// A stored message within a conversation.
#[derive(Debug, Clone)]
pub struct Message {
    pub id: i64,
    pub conversation_id: i64,
    pub seq: i64,
    pub direction: MessageDirection,
    pub msg_type: String,
    pub cc_uuid: Option<String>,
    pub parent_tool_use_id: Option<String>,
    pub payload: String,
    pub created_at: String,
    /// User who sent this message (for user messages). None for assistant/system messages.
    pub sender_user_id: Option<i64>,
    /// IANA timezone of the sender at send time (e.g. "America/New_York").
    pub sender_tz: Option<String>,
}

// ---------------------------------------------------------------------------
// Conversation CRUD
// ---------------------------------------------------------------------------

/// Create a new conversation for a user within an app. Returns the conversation id.
/// `shared` controls visibility: true = all app users can see/participate, false = owner only.
pub fn create_conversation(conn: &Connection, user_id: i64, app_slug: &str, shared: bool) -> i64 {
    let now = format_ts_for_db(Utc::now());
    conn.execute(
        "INSERT INTO conversations (user_id, app_slug, shared, status, created_at, updated_at) \
         VALUES (?1, ?2, ?3, 'active', ?4, ?5)",
        (user_id, app_slug, shared, &now, &now),
    )
    .expect("failed to insert conversation");
    conn.last_insert_rowid()
}

/// Set the CC session_id on a conversation (after CC sends system/init).
pub fn set_cc_session_id(conn: &Connection, conversation_id: i64, cc_session_id: &str) {
    let now = format_ts_for_db(Utc::now());
    let rows = conn
        .execute(
            "UPDATE conversations SET cc_session_id = ?1, updated_at = ?2 WHERE id = ?3",
            (cc_session_id, &now, conversation_id),
        )
        .expect("failed to set cc_session_id");
    assert!(rows == 1, "conversation {conversation_id} not found");
}

/// Set or update the conversation title.
pub fn set_title(conn: &Connection, conversation_id: i64, title: &str) {
    let now = format_ts_for_db(Utc::now());
    let rows = conn
        .execute(
            "UPDATE conversations SET title = ?1, updated_at = ?2 WHERE id = ?3",
            (title, &now, conversation_id),
        )
        .expect("failed to set title");
    assert!(rows == 1, "conversation {conversation_id} not found");
}

/// Update metadata from CC's system/init message (model, cwd).
pub fn set_init_metadata(conn: &Connection, conversation_id: i64, model: &str, cwd: &str) {
    let now = format_ts_for_db(Utc::now());
    let rows = conn
        .execute(
            "UPDATE conversations SET model = ?1, cwd = ?2, updated_at = ?3 WHERE id = ?4",
            (model, cwd, &now, conversation_id),
        )
        .expect("failed to set init metadata");
    assert!(rows == 1, "conversation {conversation_id} not found");
}

/// Mark a conversation as completed with optional cost info (from CC result message).
pub fn complete_conversation(conn: &Connection, conversation_id: i64, total_cost_usd: Option<f64>) {
    let now = format_ts_for_db(Utc::now());
    let rows = conn
        .execute(
            "UPDATE conversations SET status = 'completed', total_cost_usd = ?1, updated_at = ?2 WHERE id = ?3",
            (total_cost_usd, &now, conversation_id),
        )
        .expect("failed to complete conversation");
    assert!(rows == 1, "conversation {conversation_id} not found");
}

/// Update the cumulative cost for a conversation (replace, not accumulate).
/// Called on each turn completion — CC's result.total_cost_usd is cumulative.
pub fn set_cost(conn: &Connection, conversation_id: i64, total_cost_usd: Option<f64>) {
    let now = format_ts_for_db(Utc::now());
    let rows = conn
        .execute(
            "UPDATE conversations SET total_cost_usd = ?1, updated_at = ?2 WHERE id = ?3",
            (total_cost_usd, &now, conversation_id),
        )
        .expect("failed to set conversation cost");
    assert!(rows == 1, "conversation {conversation_id} not found");
}

/// Read the cumulative cost for a conversation.
///
/// Returns `None` for a fresh conversation where no cost has been recorded yet.
/// Used at bridge construction to seed `last_total_cost_usd`.
pub fn get_total_cost_usd(conn: &Connection, conversation_id: i64) -> Option<f64> {
    conn.query_row(
        "SELECT total_cost_usd FROM conversations WHERE id = ?1",
        rusqlite::params![conversation_id],
        |row| row.get::<_, Option<f64>>(0),
    )
    .expect("get_total_cost_usd: conversation not found")
}

/// Update a conversation's shared flag (privacy toggle).
pub fn set_conversation_shared(conn: &Connection, conversation_id: i64, shared: bool) {
    let now = format_ts_for_db(Utc::now());
    let rows = conn
        .execute(
            "UPDATE conversations SET shared = ?1, updated_at = ?2 WHERE id = ?3",
            (shared as i32, &now, conversation_id),
        )
        .expect("failed to update conversation shared flag");
    assert!(rows == 1, "conversation {conversation_id} not found");
}

/// Mark a conversation as errored.
pub fn error_conversation(conn: &Connection, conversation_id: i64) {
    let now = format_ts_for_db(Utc::now());
    let rows = conn
        .execute(
            "UPDATE conversations SET status = 'error', updated_at = ?1 WHERE id = ?2",
            (&now, conversation_id),
        )
        .expect("failed to error conversation");
    assert!(rows == 1, "conversation {conversation_id} not found");
}

/// Reactivate a completed or errored conversation (for resume).
pub fn reactivate_conversation(conn: &Connection, conversation_id: i64) {
    let now = format_ts_for_db(Utc::now());
    let rows = conn
        .execute(
            "UPDATE conversations SET status = 'active', updated_at = ?1 WHERE id = ?2",
            (&now, conversation_id),
        )
        .expect("failed to reactivate conversation");
    assert!(rows == 1, "conversation {conversation_id} not found");
}

/// Get a conversation by id. Panics if not found.
pub fn get_conversation(conn: &Connection, conversation_id: i64) -> Conversation {
    get_conversation_opt(conn, conversation_id)
        .unwrap_or_else(|| panic!("conversation {conversation_id} not found"))
}

/// Get a conversation by id, if it exists.
pub fn get_conversation_opt(conn: &Connection, conversation_id: i64) -> Option<Conversation> {
    conn.query_row(
        select_conversations!("FROM conversations WHERE id = ?1"),
        [conversation_id],
        |row| Ok(row_to_conversation(row)),
    )
    .optional()
    .expect("failed to query conversation")
}

/// List conversations for a user within a specific app, newest first.
pub fn list_conversations(conn: &Connection, user_id: i64, app_slug: &str) -> Vec<Conversation> {
    let mut stmt = conn
        .prepare(select_conversations!(
            "FROM conversations WHERE user_id = ?1 AND app_slug = ?2 ORDER BY created_at DESC, id DESC"
        ))
        .expect("failed to prepare list_conversations");
    stmt.query_map((user_id, app_slug), |row| Ok(row_to_conversation(row)))
        .expect("failed to query conversations")
        .map(|r| r.expect("failed to read conversation row"))
        .collect()
}

/// Return just the `updated_at` string for a conversation. Cheaper than
/// `get_conversation` when the caller only needs the activity timestamp
/// (e.g. drain-time staleness checks). Panics if the row doesn't exist —
/// same contract as `get_conversation`.
pub fn get_updated_at(conn: &Connection, conversation_id: i64) -> String {
    conn.query_row(
        "SELECT updated_at FROM conversations WHERE id = ?1",
        (conversation_id,),
        |row| row.get::<_, String>(0),
    )
    .unwrap_or_else(|e| panic!("get_updated_at: conversation {conversation_id} not found: {e}"))
}

/// Test helper: backdate a conversation's `updated_at` to the given
/// RFC3339 timestamp. Used by repo-sync drain staleness tests to simulate
/// an idle-too-long conversation without sleeping. Not part of the main
/// API — production code must never backdate activity timestamps.
#[cfg(any(test, feature = "testutils"))]
pub fn set_updated_at_for_test(conn: &Connection, conversation_id: i64, updated_at: &str) {
    conn.execute(
        "UPDATE conversations SET updated_at = ?1 WHERE id = ?2",
        (updated_at, conversation_id),
    )
    .expect("set_updated_at_for_test");
}

/// Return the ids of every conversation whose `app_slug` is in `app_slugs`.
/// No status filter — all statuses (active, completed, error) are returned.
/// Empty `app_slugs` short-circuits to an empty result.
///
/// Used by the repo-sync manager for consumer fan-out (see
/// `docs/designs/repo-sync.md` — "Consumer lookup").
///
/// **Why no status filter:** Brenn's conversation lifecycle cycles
/// `active → completed → active → completed` across every turn —
/// `completed` is the default idle state between uses, not "the user
/// archived this". Singleton PAs live in `completed` between
/// interactions; filtering them out would miss exactly the conversations
/// repo-sync is trying to notify. `error` is similar — a user may
/// resume after fixing the underlying issue. Staleness filtering at
/// drain time (see `event_queue::split_stale_repo_sync`) catches
/// truly-abandoned conversations; the DB-level filter here stays broad.
///
/// TODO(event-cleanup-undelivered): events enqueued to an abandoned
/// non-singleton conversation that never wakes again are never marked
/// delivered. A periodic janitor mirroring the drain-time staleness
/// rule (mark delivered without inject for rows whose conversation's
/// `updated_at` is older than the cap) would close the leak.
pub fn conversation_ids_for_apps(conn: &Connection, app_slugs: &[String]) -> Vec<i64> {
    if app_slugs.is_empty() {
        return Vec::new();
    }
    let placeholders: Vec<String> = (1..=app_slugs.len()).map(|i| format!("?{i}")).collect();
    let sql = format!(
        "SELECT id FROM conversations WHERE app_slug IN ({})",
        placeholders.join(", "),
    );
    let mut stmt = conn
        .prepare(&sql)
        .expect("failed to prepare conversation_ids_for_apps");
    let params: Vec<&dyn rusqlite::types::ToSql> = app_slugs
        .iter()
        .map(|s| s as &dyn rusqlite::types::ToSql)
        .collect();
    stmt.query_map(params.as_slice(), |row| row.get::<_, i64>(0))
        .expect("failed to query conversation_ids_for_apps")
        .map(|r| r.expect("failed to read id row"))
        .collect()
}

/// Like `conversation_ids_for_apps` but also returns each conversation's `app_slug`.
/// Returns `Vec<(id, app_slug)>` for all conversations whose app_slug is in `app_slugs`.
pub fn conversation_ids_and_slugs_for_apps(
    conn: &Connection,
    app_slugs: &[String],
) -> Vec<(i64, String)> {
    if app_slugs.is_empty() {
        return Vec::new();
    }
    let placeholders: Vec<String> = (1..=app_slugs.len()).map(|i| format!("?{i}")).collect();
    let sql = format!(
        "SELECT id, app_slug FROM conversations WHERE app_slug IN ({})",
        placeholders.join(", "),
    );
    let mut stmt = conn
        .prepare(&sql)
        .expect("failed to prepare conversation_ids_and_slugs_for_apps");
    let params: Vec<&dyn rusqlite::types::ToSql> = app_slugs
        .iter()
        .map(|s| s as &dyn rusqlite::types::ToSql)
        .collect();
    stmt.query_map(params.as_slice(), |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    })
    .expect("failed to query conversation_ids_and_slugs_for_apps")
    .map(|r| r.expect("failed to read id/app_slug row"))
    .collect()
}

/// List conversations visible to a user in a multiuser app, newest first.
/// Includes: all of the user's own conversations + other users' shared conversations.
pub fn list_conversations_multiuser(
    conn: &Connection,
    user_id: i64,
    app_slug: &str,
) -> Vec<Conversation> {
    let mut stmt = conn
        .prepare(select_conversations!(
            "FROM conversations WHERE app_slug = ?1 AND (user_id = ?2 OR shared = 1) \
                 ORDER BY created_at DESC, id DESC"
        ))
        .expect("failed to prepare list_conversations_multiuser");
    stmt.query_map((app_slug, user_id), |row| Ok(row_to_conversation(row)))
        .expect("failed to query conversations")
        .map(|r| r.expect("failed to read conversation row"))
        .collect()
}

/// List conversation summaries for a user within an app, with message counts.
///
/// Single query with a correlated subquery — no N+1.
pub fn list_conversation_summaries(
    conn: &Connection,
    user_id: i64,
    app_slug: &str,
) -> Vec<ConversationListRow> {
    let mut stmt = conn
        .prepare(
            "SELECT c.id, c.user_id, c.title, c.model, c.status, c.updated_at, c.shared, \
                    (SELECT COUNT(*) FROM messages WHERE conversation_id = c.id) AS message_count \
             FROM conversations c \
             WHERE c.user_id = ?1 AND c.app_slug = ?2 \
             ORDER BY c.created_at DESC, c.id DESC",
        )
        .expect("failed to prepare list_conversation_summaries");
    stmt.query_map((user_id, app_slug), |row| {
        Ok(ConversationListRow {
            id: row.get(0)?,
            user_id: row.get(1)?,
            title: row.get(2)?,
            model: row.get(3)?,
            status: ConversationStatus::from_db(&row.get::<_, String>(4)?),
            updated_at: row.get(5)?,
            shared: row.get(6)?,
            message_count: row.get(7)?,
            owner_username: None,
        })
    })
    .expect("failed to query conversation summaries")
    .map(|r| r.expect("failed to read conversation summary row"))
    .collect()
}

/// List conversation summaries visible to a user in a multiuser app, with message counts and owner usernames.
///
/// Single query with correlated subquery + LEFT JOIN — no N+1.
pub fn list_conversation_summaries_multiuser(
    conn: &Connection,
    user_id: i64,
    app_slug: &str,
) -> Vec<ConversationListRow> {
    let mut stmt = conn
        .prepare(
            "SELECT c.id, c.user_id, c.title, c.model, c.status, c.updated_at, c.shared, \
                    (SELECT COUNT(*) FROM messages WHERE conversation_id = c.id) AS message_count, \
                    u.username AS owner_username \
             FROM conversations c \
             LEFT JOIN users u ON u.id = c.user_id \
             WHERE c.app_slug = ?1 AND (c.user_id = ?2 OR c.shared = 1) \
             ORDER BY c.created_at DESC, c.id DESC",
        )
        .expect("failed to prepare list_conversation_summaries_multiuser");
    stmt.query_map((app_slug, user_id), |row| {
        Ok(ConversationListRow {
            id: row.get(0)?,
            user_id: row.get(1)?,
            title: row.get(2)?,
            model: row.get(3)?,
            status: ConversationStatus::from_db(&row.get::<_, String>(4)?),
            updated_at: row.get(5)?,
            shared: row.get(6)?,
            message_count: row.get(7)?,
            owner_username: row.get(8)?,
        })
    })
    .expect("failed to query conversation summaries (multiuser)")
    .map(|r| r.expect("failed to read conversation summary row"))
    .collect()
}

/// Check if a user can access a conversation, given the app config.
///
/// Rules:
/// 1. Owner can always access their own conversations.
/// 2. In a multiuser app, any user can access shared conversations.
/// 3. Otherwise, access denied.
pub fn can_access_conversation(user_id: i64, conversation: &Conversation, multiuser: bool) -> bool {
    if conversation.user_id == user_id {
        return true;
    }
    multiuser && conversation.shared
}

/// Look up the username for a user id. Returns None if not found.
pub fn get_username(conn: &Connection, user_id: i64) -> Option<String> {
    conn.query_row(
        "SELECT username FROM users WHERE id = ?1",
        [user_id],
        |row| row.get(0),
    )
    .optional()
    .expect("failed to query username")
}

/// Find the active conversation for a user within a specific app.
/// Returns the most recently created active conversation if multiple exist.
///
/// When `multiuser` is true, also returns shared conversations from other users.
/// This ensures connecting users in multiuser apps see shared conversation history
/// even when no active bridge exists (e.g., after server restart).
pub fn get_active_conversation(
    conn: &Connection,
    user_id: i64,
    app_slug: &str,
    multiuser: bool,
) -> Option<Conversation> {
    if multiuser {
        conn.query_row(
            select_conversations!("FROM conversations WHERE app_slug = ?1 AND (user_id = ?2 OR shared = 1) AND status = 'active' ORDER BY created_at DESC, id DESC LIMIT 1"),
            (app_slug, user_id),
            |row| Ok(row_to_conversation(row)),
        )
        .optional()
        .expect("failed to query active conversation (multiuser)")
    } else {
        conn.query_row(
            select_conversations!("FROM conversations WHERE user_id = ?1 AND app_slug = ?2 AND status = 'active' ORDER BY created_at DESC, id DESC LIMIT 1"),
            (user_id, app_slug),
            |row| Ok(row_to_conversation(row)),
        )
        .optional()
        .expect("failed to query active conversation")
    }
}

/// Find the most recent empty (zero-message) active conversation for a user/app.
/// Used to reuse a conversation created by "New Conversation" before any message was sent.
pub fn find_empty_conversation(conn: &Connection, user_id: i64, app_slug: &str) -> Option<i64> {
    conn.query_row(
        "SELECT c.id FROM conversations c \
         WHERE c.user_id = ?1 AND c.app_slug = ?2 AND c.status = 'active' \
           AND NOT EXISTS (SELECT 1 FROM messages m WHERE m.conversation_id = c.id) \
         ORDER BY c.created_at DESC, c.id DESC LIMIT 1",
        (user_id, app_slug),
        |row| row.get(0),
    )
    .optional()
    .expect("failed to query empty conversation")
}

/// Find or create the singleton conversation for a user within an app.
///
/// Singleton apps have exactly one conversation per user. This function finds it
/// regardless of status (Active, Completed, or Error) and creates it if it doesn't
/// exist. The conversation is always private (shared=false).
pub fn get_or_create_singleton_conversation(
    conn: &Connection,
    user_id: i64,
    app_slug: &str,
) -> Conversation {
    // Find existing — any status.
    if let Some(conv) = conn
        .query_row(
            select_conversations!(
                "FROM conversations WHERE user_id = ?1 AND app_slug = ?2 \
                 ORDER BY created_at DESC, id DESC LIMIT 1"
            ),
            (user_id, app_slug),
            |row| Ok(row_to_conversation(row)),
        )
        .optional()
        .expect("failed to query singleton conversation")
    {
        return conv;
    }

    // No conversation exists — create one.
    let id = create_conversation(conn, user_id, app_slug, false);
    get_conversation(conn, id)
}

/// Resolve user + app slug → singleton conversation id.
///
/// For event sources (cron, Discord) that know the user but not the conversation.
/// Returns `None` if no conversation exists for this user/app.
#[allow(dead_code)] // Called by future event sources.
pub fn get_singleton_conversation_id(
    conn: &Connection,
    user_id: i64,
    app_slug: &str,
) -> Option<i64> {
    conn.query_row(
        "SELECT id FROM conversations WHERE user_id = ?1 AND app_slug = ?2 \
         ORDER BY created_at DESC, id DESC LIMIT 1",
        (user_id, app_slug),
        |row| row.get(0),
    )
    .optional()
    .expect("failed to query singleton conversation id")
}

/// Find a conversation by its CC session_id (for --resume correlation).
pub fn get_conversation_by_cc_session(
    conn: &Connection,
    cc_session_id: &str,
) -> Option<Conversation> {
    conn.query_row(
        select_conversations!("FROM conversations WHERE cc_session_id = ?1"),
        [cc_session_id],
        |row| Ok(row_to_conversation(row)),
    )
    .optional()
    .expect("failed to query conversation by cc_session_id")
}

fn row_to_conversation(row: &rusqlite::Row<'_>) -> Conversation {
    let status_str: String = row.get(6).expect("failed to read status");
    let shared_int: i64 = row.get(11).expect("failed to read shared");
    Conversation {
        id: row.get(0).expect("failed to read id"),
        user_id: row.get(1).expect("failed to read user_id"),
        cc_session_id: row.get(2).expect("failed to read cc_session_id"),
        title: row.get(3).expect("failed to read title"),
        model: row.get(4).expect("failed to read model"),
        cwd: row.get(5).expect("failed to read cwd"),
        status: ConversationStatus::from_db(&status_str),
        created_at: row.get(7).expect("failed to read created_at"),
        updated_at: row.get(8).expect("failed to read updated_at"),
        total_cost_usd: row.get(9).expect("failed to read total_cost_usd"),
        app_slug: row.get(10).expect("failed to read app_slug"),
        shared: shared_int != 0,
    }
}

// ---------------------------------------------------------------------------
// Message CRUD
// ---------------------------------------------------------------------------

/// Append a message row and return `(id, seq)` — both from the inserted row.
///
/// `seq` is assigned atomically by the INSERT (`SELECT COALESCE(MAX(seq) + 1, 0)`)
/// and recovered via a follow-up SELECT on the same `&Connection`. There is no
/// race because `Connection` is single-threaded and the caller holds the
/// `MutexGuard` around the shared connection.
///
/// `sender_user_id` and `sender_tz` identify who sent a user message and their
/// timezone. `sender_device_id` identifies the originating device (populated
/// only for outgoing user messages; `None` for assistant/system messages).
/// All three are `None` for assistant/system messages.
#[allow(clippy::too_many_arguments)]
pub fn append_message(
    conn: &Connection,
    conversation_id: i64,
    direction: MessageDirection,
    msg_type: &str,
    cc_uuid: Option<&str>,
    parent_tool_use_id: Option<&str>,
    payload: &str,
    sender_user_id: Option<i64>,
    sender_tz: Option<&str>,
    sender_device_id: Option<i64>,
) -> (i64, i64) {
    let now = format_ts_for_db(Utc::now());
    conn.query_row(
        "INSERT INTO messages (conversation_id, seq, direction, msg_type, cc_uuid, parent_tool_use_id, payload, created_at, sender_user_id, sender_tz, sender_device_id) \
         SELECT ?1, COALESCE(MAX(seq) + 1, 0), ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10 FROM messages WHERE conversation_id = ?1 \
         RETURNING id, seq",
        (
            conversation_id,
            &direction.to_string(),
            msg_type,
            cc_uuid,
            parent_tool_use_id,
            payload,
            &now,
            sender_user_id,
            sender_tz,
            sender_device_id,
        ),
        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
    )
    .expect("failed to insert message")
}

/// Get all messages for a conversation, ordered by seq.
pub fn get_messages(conn: &Connection, conversation_id: i64) -> Vec<Message> {
    let mut stmt = conn
        .prepare(select_messages!(
            "FROM messages WHERE conversation_id = ?1 ORDER BY seq"
        ))
        .expect("failed to prepare get_messages");
    stmt.query_map([conversation_id], |row| Ok(row_to_message(row)))
        .expect("failed to query messages")
        .map(|r| r.expect("failed to read message row"))
        .collect()
}

/// Get messages starting from a given seq (inclusive). For catch-up after reconnect.
pub fn get_messages_from(conn: &Connection, conversation_id: i64, from_seq: i64) -> Vec<Message> {
    let mut stmt = conn
        .prepare(select_messages!(
            "FROM messages WHERE conversation_id = ?1 AND seq >= ?2 ORDER BY seq"
        ))
        .expect("failed to prepare get_messages_from");
    stmt.query_map((conversation_id, from_seq), |row| Ok(row_to_message(row)))
        .expect("failed to query messages from seq")
        .map(|r| r.expect("failed to read message row"))
        .collect()
}

/// Get the latest N messages for a conversation, returned in seq order (ascending).
pub fn get_recent_messages(conn: &Connection, conversation_id: i64, limit: i64) -> Vec<Message> {
    let mut stmt = conn
        .prepare(
            select_messages!(
             "FROM (SELECT * FROM messages WHERE conversation_id = ?1 ORDER BY seq DESC LIMIT ?2) ORDER BY seq ASC"
            ),
        )
        .expect("failed to prepare get_recent_messages");
    stmt.query_map((conversation_id, limit), |row| Ok(row_to_message(row)))
        .expect("failed to query recent messages")
        .map(|r| r.expect("failed to read message row"))
        .collect()
}

/// Get the maximum seq number for a conversation, or None if the conversation has no messages.
pub fn get_max_seq(conn: &Connection, conversation_id: i64) -> Option<i64> {
    conn.query_row(
        "SELECT MAX(seq) FROM messages WHERE conversation_id = ?1",
        [conversation_id],
        |row| row.get(0),
    )
    .expect("failed to query max seq")
}

/// Count messages in a conversation.
pub fn count_messages(conn: &Connection, conversation_id: i64) -> i64 {
    conn.query_row(
        "SELECT COUNT(*) FROM messages WHERE conversation_id = ?1",
        [conversation_id],
        |row| row.get(0),
    )
    .expect("failed to count messages")
}

/// SQL filter for message types that `build_history` would replay.
/// Must stay in sync with the match arms in `build_history`.
const REPLAYABLE_FILTER: &str = "\
    ((msg_type = 'user' AND direction = 'outgoing') \
    OR msg_type = 'assistant' \
    OR msg_type = 'tool_summary' \
    OR msg_type = 'artifact_display' \
    OR msg_type = 'target_result')";

/// Count replayable messages in a conversation (the types `build_history` emits).
pub fn count_replayable_messages(conn: &Connection, conversation_id: i64) -> i64 {
    let sql =
        format!("SELECT COUNT(*) FROM messages WHERE conversation_id = ?1 AND {REPLAYABLE_FILTER}");
    conn.query_row(&sql, [conversation_id], |row| row.get(0))
        .expect("failed to count replayable messages")
}

/// Find the seq of the Nth replayable message from the start (0-indexed offset).
/// Returns `None` if the conversation has fewer than `offset+1` replayable messages.
pub fn nth_replayable_seq(conn: &Connection, conversation_id: i64, offset: usize) -> Option<i64> {
    let sql = format!(
        "SELECT seq FROM messages WHERE conversation_id = ?1 AND {REPLAYABLE_FILTER} \
         ORDER BY seq ASC LIMIT 1 OFFSET ?2"
    );
    conn.query_row(&sql, (conversation_id, offset as i64), |row| row.get(0))
        .ok()
}

/// Find the most recent `compact_boundary` at or before a given seq.
/// Returns `None` if no compaction boundary exists before the cutoff.
pub fn latest_compact_boundary_before(
    conn: &Connection,
    conversation_id: i64,
    max_seq: i64,
) -> Option<i64> {
    conn.query_row(
        "SELECT seq FROM messages WHERE conversation_id = ?1 \
         AND msg_type = 'compact_boundary' AND seq <= ?2 \
         ORDER BY seq DESC LIMIT 1",
        (conversation_id, max_seq),
        |row| row.get(0),
    )
    .ok()
}

/// Get only artifact-type messages for a conversation (for building the
/// artifact cache without loading all messages).
pub fn get_artifact_messages(conn: &Connection, conversation_id: i64) -> Vec<Message> {
    let mut stmt = conn
        .prepare(select_messages!(
            "FROM messages WHERE conversation_id = ?1 AND msg_type = 'artifact' ORDER BY seq"
        ))
        .expect("failed to prepare get_artifact_messages");
    stmt.query_map([conversation_id], |row| Ok(row_to_message(row)))
        .expect("failed to query artifact messages")
        .map(|r| r.expect("failed to read message row"))
        .collect()
}

/// SQL filter for simplified history types (user text + assistant text only).
const SIMPLIFIED_FILTER: &str = "\
    ((msg_type = 'user' AND direction = 'outgoing') \
    OR msg_type = 'assistant')";

/// Get a page of simplified messages before a given seq, for backward pagination.
/// Returns messages in ascending seq order (oldest first).
/// Fetches `limit + 1` rows to detect `has_more`.
pub fn get_simplified_page(
    conn: &Connection,
    conversation_id: i64,
    before_seq: i64,
    limit: usize,
) -> Vec<Message> {
    let sql = format!(
        "{} FROM messages WHERE conversation_id = ?1 AND seq < ?2 AND {SIMPLIFIED_FILTER} \
         ORDER BY seq DESC LIMIT ?3",
        select_messages!("")
    );
    let mut stmt = conn
        .prepare(&sql)
        .expect("failed to prepare get_simplified_page");
    let mut msgs: Vec<Message> = stmt
        .query_map((conversation_id, before_seq, (limit + 1) as i64), |row| {
            Ok(row_to_message(row))
        })
        .expect("failed to query simplified page")
        .map(|r| r.expect("failed to read message row"))
        .collect();
    msgs.reverse(); // Return in ascending seq order.
    msgs
}

/// Attachment metadata stored in the message_attachments table.
#[derive(Debug, Clone)]
pub struct StoredAttachment {
    pub upload_id: String,
    pub message_id: i64,
    pub filename: String,
    pub media_type: String,
    pub size: u64,
    pub disk_filename: String,
}

/// Insert attachment rows for a message. Called within the same DB lock as
/// `append_message` to avoid a second lock acquisition.
pub fn insert_attachments(conn: &Connection, attachments: &[StoredAttachment]) {
    for att in attachments {
        conn.execute(
            "INSERT INTO message_attachments (upload_id, message_id, filename, media_type, size, disk_filename) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            (
                &att.upload_id,
                att.message_id,
                &att.filename,
                &att.media_type,
                att.size as i64,
                &att.disk_filename,
            ),
        )
        .expect("failed to insert message_attachment");
    }
}

/// Get all attachments for a set of message IDs in a conversation.
/// Returns a map from message_id to Vec<AttachmentMeta> (using ws_types).
pub fn get_attachments_for_conversation(
    conn: &Connection,
    conversation_id: i64,
) -> std::collections::HashMap<i64, Vec<crate::ws_types::AttachmentMeta>> {
    use std::collections::HashMap;

    let mut stmt = conn
        .prepare(
            "SELECT ma.upload_id, ma.message_id, ma.filename, ma.media_type, ma.size \
             FROM message_attachments ma \
             INNER JOIN messages m ON m.id = ma.message_id \
             WHERE m.conversation_id = ?1",
        )
        .expect("failed to prepare get_attachments_for_conversation");

    let mut map: HashMap<i64, Vec<crate::ws_types::AttachmentMeta>> = HashMap::new();
    let rows = stmt
        .query_map([conversation_id], |row| {
            let message_id: i64 = row.get(1)?;
            let meta = crate::ws_types::AttachmentMeta {
                upload_id: row.get(0)?,
                filename: row.get(2)?,
                media_type: row.get(3)?,
                size: row.get::<_, i64>(4)? as u64,
            };
            Ok((message_id, meta))
        })
        .expect("failed to query attachments");

    for row in rows {
        let (message_id, meta) = row.expect("failed to read attachment row");
        map.entry(message_id).or_default().push(meta);
    }
    map
}

/// Get attachments for a specific set of message IDs.
///
/// Returns a map from message_id to Vec<AttachmentMeta>. Scoped to the provided IDs
/// (direct `message_id IN (...)` lookup — no JOIN needed).
///
/// # Panics
///
/// Panics if the query cannot be prepared or executed (data-integrity violation).
pub fn get_attachments_for_messages(
    conn: &Connection,
    message_ids: &[i64],
) -> std::collections::HashMap<i64, Vec<crate::ws_types::AttachmentMeta>> {
    use std::collections::HashMap;

    if message_ids.is_empty() {
        return HashMap::new();
    }

    let placeholders: Vec<String> = (1..=message_ids.len()).map(|i| format!("?{i}")).collect();
    let sql = format!(
        "SELECT upload_id, message_id, filename, media_type, size \
         FROM message_attachments \
         WHERE message_id IN ({})",
        placeholders.join(", "),
    );

    let mut stmt = conn
        .prepare(&sql)
        .expect("failed to prepare get_attachments_for_messages");

    let params: Vec<&dyn rusqlite::types::ToSql> = message_ids
        .iter()
        .map(|id| id as &dyn rusqlite::types::ToSql)
        .collect();

    let mut map: HashMap<i64, Vec<crate::ws_types::AttachmentMeta>> = HashMap::new();
    let rows = stmt
        .query_map(params.as_slice(), |row| {
            let message_id: i64 = row.get(1)?;
            let meta = crate::ws_types::AttachmentMeta {
                upload_id: row.get(0)?,
                filename: row.get(2)?,
                media_type: row.get(3)?,
                size: row.get::<_, i64>(4)? as u64,
            };
            Ok((message_id, meta))
        })
        .expect("failed to query attachments for messages");

    for row in rows {
        let (message_id, meta) = row.expect("failed to read attachment row");
        map.entry(message_id).or_default().push(meta);
    }
    map
}

/// Check whether a given upload_id exists in message_attachments.
pub fn attachment_exists(conn: &Connection, upload_id: &str) -> bool {
    conn.query_row(
        "SELECT 1 FROM message_attachments WHERE upload_id = ?1",
        [upload_id],
        |_| Ok(()),
    )
    .optional()
    .expect("failed to check attachment existence")
    .is_some()
}

fn row_to_message(row: &rusqlite::Row<'_>) -> Message {
    let direction_str: String = row.get(3).expect("failed to read direction");
    Message {
        id: row.get(0).expect("failed to read id"),
        conversation_id: row.get(1).expect("failed to read conversation_id"),
        seq: row.get(2).expect("failed to read seq"),
        direction: MessageDirection::from_db(&direction_str),
        msg_type: row.get(4).expect("failed to read msg_type"),
        cc_uuid: row.get(5).expect("failed to read cc_uuid"),
        parent_tool_use_id: row.get(6).expect("failed to read parent_tool_use_id"),
        payload: row.get(7).expect("failed to read payload"),
        created_at: row.get(8).expect("failed to read created_at"),
        sender_user_id: row.get(9).expect("failed to read sender_user_id"),
        sender_tz: row.get(10).expect("failed to read sender_tz"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::user::create_user;
    use crate::db::init_db_memory;

    fn setup_user(conn: &Connection) -> i64 {
        create_user(conn, "testuser", "$argon2id$fake-hash")
    }

    #[test]
    fn create_conversation_returns_valid_id() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);

        let conv_id = create_conversation(&conn, user_id, "test", false);
        assert!(conv_id > 0);

        let conv = get_conversation(&conn, conv_id);
        assert_eq!(conv.user_id, user_id);
        assert_eq!(conv.status, ConversationStatus::Active);
        assert!(conv.cc_session_id.is_none());
        assert!(conv.title.is_none());
        assert!(conv.model.is_none());
        assert!(conv.cwd.is_none());
        assert!(conv.total_cost_usd.is_none());
        assert!(!conv.created_at.is_empty());
        assert!(!conv.updated_at.is_empty());
    }

    #[test]
    fn set_cc_session_id_updates_conversation() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);
        let conv_id = create_conversation(&conn, user_id, "test", false);

        set_cc_session_id(&conn, conv_id, "cc-uuid-abc123");

        let conv = get_conversation(&conn, conv_id);
        assert_eq!(conv.cc_session_id.as_deref(), Some("cc-uuid-abc123"));
    }

    #[test]
    fn set_init_metadata_updates_model_and_cwd() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);
        let conv_id = create_conversation(&conn, user_id, "test", false);

        set_init_metadata(
            &conn,
            conv_id,
            "claude-opus-4-20250514",
            "/home/user/project",
        );

        let conv = get_conversation(&conn, conv_id);
        assert_eq!(conv.model.as_deref(), Some("claude-opus-4-20250514"));
        assert_eq!(conv.cwd.as_deref(), Some("/home/user/project"));
    }

    #[test]
    fn complete_conversation_sets_status_and_cost() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);
        let conv_id = create_conversation(&conn, user_id, "test", false);
        let created = get_conversation(&conn, conv_id).updated_at;

        complete_conversation(&conn, conv_id, Some(0.025));

        let conv = get_conversation(&conn, conv_id);
        assert_eq!(conv.status, ConversationStatus::Completed);
        assert_eq!(conv.total_cost_usd, Some(0.025));
        assert!(conv.updated_at >= created);
    }

    #[test]
    fn set_cost_updates_cost_without_changing_status() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);
        let conv_id = create_conversation(&conn, user_id, "test", false);

        // First turn cost.
        set_cost(&conn, conv_id, Some(0.01));
        let conv = get_conversation(&conn, conv_id);
        assert_eq!(conv.total_cost_usd, Some(0.01));
        assert_eq!(conv.status, ConversationStatus::Active); // Status unchanged.

        // Second turn cost (cumulative — replaces, not accumulates).
        set_cost(&conn, conv_id, Some(0.035));
        let conv = get_conversation(&conn, conv_id);
        assert_eq!(conv.total_cost_usd, Some(0.035));
        assert_eq!(conv.status, ConversationStatus::Active);
    }

    /// `get_total_cost_usd` returns `None` for a fresh conversation (test-4).
    #[test]
    fn get_total_cost_usd_returns_none_for_fresh_conversation() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);
        let conv_id = create_conversation(&conn, user_id, "test", false);
        // A newly created conversation has no cost recorded.
        assert_eq!(get_total_cost_usd(&conn, conv_id), None);
    }

    /// `get_total_cost_usd` returns the previously persisted value (test-4).
    #[test]
    fn get_total_cost_usd_returns_set_value() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);
        let conv_id = create_conversation(&conn, user_id, "test", false);
        set_cost(&conn, conv_id, Some(0.42));
        let cost = get_total_cost_usd(&conn, conv_id);
        assert!(
            cost.is_some(),
            "get_total_cost_usd must return Some after set_cost(Some(...))"
        );
        assert!(
            (cost.unwrap() - 0.42).abs() < 1e-9,
            "get_total_cost_usd must return the value written by set_cost"
        );
    }

    #[test]
    fn error_conversation_sets_status() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);
        let conv_id = create_conversation(&conn, user_id, "test", false);

        error_conversation(&conn, conv_id);

        let conv = get_conversation(&conn, conv_id);
        assert_eq!(conv.status, ConversationStatus::Error);
    }

    #[test]
    fn set_title_updates_conversation() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);
        let conv_id = create_conversation(&conn, user_id, "test", false);

        set_title(&conn, conv_id, "My first conversation");

        let conv = get_conversation(&conn, conv_id);
        assert_eq!(conv.title.as_deref(), Some("My first conversation"));
    }

    #[test]
    fn list_conversations_newest_first_and_user_isolated() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user1 = setup_user(&conn);
        let user2 = create_user(&conn, "other", "$argon2id$fake-hash");

        let conv1 = create_conversation(&conn, user1, "test", false);
        let conv2 = create_conversation(&conn, user1, "test", false);
        let _conv3 = create_conversation(&conn, user2, "test", false);

        let list = list_conversations(&conn, user1, "test");
        assert_eq!(list.len(), 2);
        // Newest first.
        assert_eq!(list[0].id, conv2);
        assert_eq!(list[1].id, conv1);

        // user2 only sees their conversation.
        let list2 = list_conversations(&conn, user2, "test");
        assert_eq!(list2.len(), 1);
    }

    #[test]
    fn conversations_isolated_between_apps() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);

        // Create conversations in two different apps.
        let pfin_conv1 = create_conversation(&conn, user_id, "pfin", false);
        let pfin_conv2 = create_conversation(&conn, user_id, "pfin", false);
        let graf_conv = create_conversation(&conn, user_id, "graf", false);

        // list_conversations only returns conversations for the requested app.
        let pfin_list = list_conversations(&conn, user_id, "pfin");
        assert_eq!(pfin_list.len(), 2);
        assert_eq!(pfin_list[0].id, pfin_conv2); // newest first
        assert_eq!(pfin_list[1].id, pfin_conv1);

        let graf_list = list_conversations(&conn, user_id, "graf");
        assert_eq!(graf_list.len(), 1);
        assert_eq!(graf_list[0].id, graf_conv);

        // An app with no conversations returns empty.
        let empty = list_conversations(&conn, user_id, "nonexistent");
        assert!(empty.is_empty());

        // get_active_conversation is also app-scoped.
        let pfin_active = get_active_conversation(&conn, user_id, "pfin", false);
        assert_eq!(pfin_active.unwrap().id, pfin_conv2);

        let graf_active = get_active_conversation(&conn, user_id, "graf", false);
        assert_eq!(graf_active.unwrap().id, graf_conv);

        assert!(get_active_conversation(&conn, user_id, "nonexistent", false).is_none());

        // app_slug is stored on the conversation.
        let conv = get_conversation(&conn, pfin_conv1);
        assert_eq!(conv.app_slug, "pfin");
        let conv = get_conversation(&conn, graf_conv);
        assert_eq!(conv.app_slug, "graf");
    }

    #[test]
    fn get_active_conversation_returns_active_only() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);

        // No conversations yet.
        assert!(get_active_conversation(&conn, user_id, "test", false).is_none());

        let conv_id = create_conversation(&conn, user_id, "test", false);
        let active = get_active_conversation(&conn, user_id, "test", false);
        assert_eq!(active.unwrap().id, conv_id);

        // Complete it — no more active.
        complete_conversation(&conn, conv_id, None);
        assert!(get_active_conversation(&conn, user_id, "test", false).is_none());
    }

    // -----------------------------------------------------------------------
    // get_or_create_singleton_conversation
    // -----------------------------------------------------------------------

    #[test]
    fn singleton_creates_on_first_call() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);

        let conv = get_or_create_singleton_conversation(&conn, user_id, "pa");
        assert_eq!(conv.user_id, user_id);
        assert_eq!(conv.app_slug, "pa");
        assert_eq!(conv.status, ConversationStatus::Active);
        assert!(!conv.shared);
    }

    #[test]
    fn singleton_returns_same_conversation() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);

        let first = get_or_create_singleton_conversation(&conn, user_id, "pa");
        let second = get_or_create_singleton_conversation(&conn, user_id, "pa");
        assert_eq!(first.id, second.id);
    }

    #[test]
    fn singleton_returns_completed_conversation() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);

        let conv = get_or_create_singleton_conversation(&conn, user_id, "pa");
        complete_conversation(&conn, conv.id, None);

        // Still returns the same conversation even though it's Completed.
        let again = get_or_create_singleton_conversation(&conn, user_id, "pa");
        assert_eq!(again.id, conv.id);
        assert_eq!(again.status, ConversationStatus::Completed);
    }

    #[test]
    fn singleton_returns_errored_conversation() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);

        let conv = get_or_create_singleton_conversation(&conn, user_id, "pa");
        error_conversation(&conn, conv.id);

        let again = get_or_create_singleton_conversation(&conn, user_id, "pa");
        assert_eq!(again.id, conv.id);
        assert_eq!(again.status, ConversationStatus::Error);
    }

    #[test]
    fn singleton_per_user_isolation() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let alice = crate::auth::user::create_user(&conn, "alice", "$argon2id$fake");
        let bob = crate::auth::user::create_user(&conn, "bob", "$argon2id$fake");

        let alice_conv = get_or_create_singleton_conversation(&conn, alice, "pa");
        let bob_conv = get_or_create_singleton_conversation(&conn, bob, "pa");
        assert_ne!(alice_conv.id, bob_conv.id);
    }

    // -----------------------------------------------------------------------
    // get_singleton_conversation_id
    // -----------------------------------------------------------------------

    #[test]
    fn singleton_conversation_id_found() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);
        let conv = get_or_create_singleton_conversation(&conn, user_id, "assistant");

        let found = get_singleton_conversation_id(&conn, user_id, "assistant");
        assert_eq!(found, Some(conv.id));
    }

    #[test]
    fn singleton_conversation_id_none_for_missing() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);

        assert_eq!(
            get_singleton_conversation_id(&conn, user_id, "assistant"),
            None
        );
    }

    #[test]
    fn singleton_conversation_id_per_app_isolation() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);

        let pa = get_or_create_singleton_conversation(&conn, user_id, "assistant");
        let pfin = get_or_create_singleton_conversation(&conn, user_id, "pfin");

        assert_eq!(
            get_singleton_conversation_id(&conn, user_id, "assistant"),
            Some(pa.id)
        );
        assert_eq!(
            get_singleton_conversation_id(&conn, user_id, "pfin"),
            Some(pfin.id)
        );
        assert_ne!(pa.id, pfin.id);
    }

    #[test]
    fn singleton_conversation_id_per_user_isolation() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let alice = crate::auth::user::create_user(&conn, "alice", "$argon2id$fake");
        let bob = crate::auth::user::create_user(&conn, "bob", "$argon2id$fake");

        get_or_create_singleton_conversation(&conn, alice, "assistant");
        // Bob has no conversation.
        assert_eq!(get_singleton_conversation_id(&conn, bob, "assistant"), None);
    }

    #[test]
    fn get_conversation_opt_returns_none_for_missing() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        assert!(get_conversation_opt(&conn, 999).is_none());
    }

    #[test]
    #[should_panic(expected = "conversation 999 not found")]
    fn get_conversation_panics_for_missing() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        get_conversation(&conn, 999);
    }

    #[test]
    fn lookup_by_cc_session_id() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);
        let conv_id = create_conversation(&conn, user_id, "test", false);
        set_cc_session_id(&conn, conv_id, "cc-session-xyz");

        let found = get_conversation_by_cc_session(&conn, "cc-session-xyz");
        assert_eq!(found.unwrap().id, conv_id);

        assert!(get_conversation_by_cc_session(&conn, "nonexistent").is_none());
    }

    // -----------------------------------------------------------------------
    // Message tests
    // -----------------------------------------------------------------------

    #[test]
    fn append_and_get_messages() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);
        let conv_id = create_conversation(&conn, user_id, "test", false);

        let (msg1_id, _) = append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            r#"{"type":"user","message":{"role":"user","content":"hello"}}"#,
            None,
            None,
            None,
        );
        let (msg2_id, _) = append_message(
            &conn,
            conv_id,
            MessageDirection::Incoming,
            "assistant",
            Some("uuid-1"),
            None,
            r#"{"type":"assistant","message":{"role":"assistant","content":[]}}"#,
            None,
            None,
            None,
        );

        assert!(msg1_id > 0);
        assert!(msg2_id > msg1_id);

        let messages = get_messages(&conn, conv_id);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].seq, 0);
        assert_eq!(messages[0].direction, MessageDirection::Outgoing);
        assert_eq!(messages[0].msg_type, "user");
        assert!(messages[0].cc_uuid.is_none());
        assert_eq!(messages[1].seq, 1);
        assert_eq!(messages[1].direction, MessageDirection::Incoming);
        assert_eq!(messages[1].msg_type, "assistant");
        assert_eq!(messages[1].cc_uuid.as_deref(), Some("uuid-1"));
    }

    /// `append_message` returns `(id, seq)` where `seq` matches the stored row.
    #[test]
    fn append_message_returns_seq() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);
        let conv_id = create_conversation(&conn, user_id, "test", false);

        let (id1, seq1) = append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            "{}",
            None,
            None,
            None,
        );
        let (id2, seq2) = append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            "{}",
            None,
            None,
            None,
        );

        // Verify returned (id, seq) match what's stored.
        let msgs = get_messages(&conn, conv_id);
        assert_eq!(msgs[0].id, id1);
        assert_eq!(msgs[0].seq, seq1);
        assert_eq!(msgs[1].id, id2);
        assert_eq!(msgs[1].seq, seq2);

        // Seqs are monotonically increasing starting at 0.
        assert_eq!(seq1, 0);
        assert_eq!(seq2, 1);
    }

    #[test]
    fn append_message_with_parent_tool_use_id() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);
        let conv_id = create_conversation(&conn, user_id, "test", false);

        let _ = append_message(
            &conn,
            conv_id,
            MessageDirection::Incoming,
            "user",
            Some("uuid-result"),
            Some("toolu_abc123"),
            r#"{"type":"user","tool_use_result":{}}"#,
            None,
            None,
            None,
        );

        let messages = get_messages(&conn, conv_id);
        assert_eq!(messages.len(), 1);
        assert_eq!(
            messages[0].parent_tool_use_id.as_deref(),
            Some("toolu_abc123")
        );
    }

    #[test]
    fn get_messages_from_seq() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);
        let conv_id = create_conversation(&conn, user_id, "test", false);

        for i in 0..5 {
            let _ = append_message(
                &conn,
                conv_id,
                MessageDirection::Outgoing,
                "user",
                None,
                None,
                &format!(r#"{{"seq":{i}}}"#),
                None,
                None,
                None,
            );
        }

        let from_2 = get_messages_from(&conn, conv_id, 2);
        assert_eq!(from_2.len(), 3);
        assert_eq!(from_2[0].seq, 2);
        assert_eq!(from_2[1].seq, 3);
        assert_eq!(from_2[2].seq, 4);
    }

    #[test]
    fn get_max_seq_empty_conversation() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);
        let conv_id = create_conversation(&conn, user_id, "test", false);

        assert_eq!(get_max_seq(&conn, conv_id), None);
    }

    #[test]
    fn get_max_seq_with_messages() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);
        let conv_id = create_conversation(&conn, user_id, "test", false);

        for _ in 0..5 {
            let _ = append_message(
                &conn,
                conv_id,
                MessageDirection::Outgoing,
                "user",
                None,
                None,
                "{}",
                None,
                None,
                None,
            );
        }

        assert_eq!(get_max_seq(&conn, conv_id), Some(4));
    }

    #[test]
    fn get_recent_messages_returns_last_n_in_order() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);
        let conv_id = create_conversation(&conn, user_id, "test", false);

        for i in 0..10 {
            let _ = append_message(
                &conn,
                conv_id,
                MessageDirection::Outgoing,
                "user",
                None,
                None,
                &format!(r#"{{"seq":{i}}}"#),
                None,
                None,
                None,
            );
        }

        let recent = get_recent_messages(&conn, conv_id, 3);
        assert_eq!(recent.len(), 3);
        // Should be the last 3, in ascending order.
        assert_eq!(recent[0].seq, 7);
        assert_eq!(recent[1].seq, 8);
        assert_eq!(recent[2].seq, 9);
    }

    #[test]
    fn count_messages_correct() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);
        let conv_id = create_conversation(&conn, user_id, "test", false);

        assert_eq!(count_messages(&conn, conv_id), 0);

        let _ = append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            "{}",
            None,
            None,
            None,
        );
        let _ = append_message(
            &conn,
            conv_id,
            MessageDirection::Incoming,
            "assistant",
            None,
            None,
            "{}",
            None,
            None,
            None,
        );

        assert_eq!(count_messages(&conn, conv_id), 2);
    }

    #[test]
    fn messages_isolated_between_conversations() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);
        let conv1 = create_conversation(&conn, user_id, "test", false);
        let conv2 = create_conversation(&conn, user_id, "test", false);

        let _ = append_message(
            &conn,
            conv1,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            "{}",
            None,
            None,
            None,
        );
        let _ = append_message(
            &conn,
            conv1,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            "{}",
            None,
            None,
            None,
        );
        let _ = append_message(
            &conn,
            conv2,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            "{}",
            None,
            None,
            None,
        );

        assert_eq!(count_messages(&conn, conv1), 2);
        assert_eq!(count_messages(&conn, conv2), 1);

        // Seq numbering is independent per conversation.
        let msgs1 = get_messages(&conn, conv1);
        let msgs2 = get_messages(&conn, conv2);
        assert_eq!(msgs1[0].seq, 0);
        assert_eq!(msgs1[1].seq, 1);
        assert_eq!(msgs2[0].seq, 0);
    }

    #[test]
    fn conversations_tied_to_user_not_auth_session() {
        // Conversations reference user_id, not the auth session token.
        // Auth session deletion (logout/expiry) cannot cascade to conversations
        // because there's no FK relationship between conversations and sessions.
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);
        let conv_id = create_conversation(&conn, user_id, "test", false);

        let _ = append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            r#"{"msg":"hello"}"#,
            None,
            None,
            None,
        );

        // Create and then delete an auth session — conversations unaffected.
        let (token, _csrf) = crate::auth::session::create_session(&conn, user_id);
        crate::auth::session::delete_session(&conn, &token);

        let convs = list_conversations(&conn, user_id, "test");
        assert_eq!(convs.len(), 1);
        assert_eq!(count_messages(&conn, conv_id), 1);
    }

    // -----------------------------------------------------------------------
    // can_access_conversation
    // -----------------------------------------------------------------------

    fn make_conversation(user_id: i64, shared: bool) -> Conversation {
        Conversation {
            id: 1,
            user_id,
            cc_session_id: None,
            title: None,
            model: None,
            cwd: None,
            status: ConversationStatus::Active,
            created_at: String::new(),
            updated_at: String::new(),
            total_cost_usd: None,
            app_slug: "test".to_string(),
            shared,
        }
    }

    #[test]
    fn can_access_owner_always() {
        // Owner can always access, regardless of shared/multiuser.
        let conv = make_conversation(1, false);
        assert!(can_access_conversation(1, &conv, false));
        assert!(can_access_conversation(1, &conv, true));
        let shared_conv = make_conversation(1, true);
        assert!(can_access_conversation(1, &shared_conv, false));
        assert!(can_access_conversation(1, &shared_conv, true));
    }

    #[test]
    fn can_access_non_owner_non_multiuser_denied() {
        // Non-owner can never access in non-multiuser mode.
        let conv = make_conversation(1, false);
        assert!(!can_access_conversation(2, &conv, false));
        let shared_conv = make_conversation(1, true);
        assert!(!can_access_conversation(2, &shared_conv, false));
    }

    #[test]
    fn can_access_multiuser_shared_allowed() {
        // Non-owner can access a shared conversation in multiuser mode.
        let conv = make_conversation(1, true);
        assert!(can_access_conversation(2, &conv, true));
    }

    #[test]
    fn can_access_multiuser_private_denied() {
        // Non-owner cannot access a private conversation even in multiuser mode.
        let conv = make_conversation(1, false);
        assert!(!can_access_conversation(2, &conv, true));
    }

    // -----------------------------------------------------------------------
    // list_conversations_multiuser
    // -----------------------------------------------------------------------

    #[test]
    fn multiuser_list_includes_shared_from_others() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let alice = crate::auth::user::create_user(&conn, "alice", "$argon2id$fake");
        let bob = crate::auth::user::create_user(&conn, "bob", "$argon2id$fake");

        // Alice creates a shared conversation and a private one.
        let _alice_shared = create_conversation(&conn, alice, "app", true);
        let _alice_private = create_conversation(&conn, alice, "app", false);

        // Bob creates a shared conversation.
        let _bob_shared = create_conversation(&conn, bob, "app", true);

        // Bob should see: his own shared + Alice's shared = 2.
        let bob_convs = list_conversations_multiuser(&conn, bob, "app");
        assert_eq!(bob_convs.len(), 2);

        // Alice should see: her own (both) + Bob's shared = 3.
        let alice_convs = list_conversations_multiuser(&conn, alice, "app");
        assert_eq!(alice_convs.len(), 3);
    }

    #[test]
    fn multiuser_list_excludes_private_from_others() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let alice = crate::auth::user::create_user(&conn, "alice", "$argon2id$fake");
        let bob = crate::auth::user::create_user(&conn, "bob", "$argon2id$fake");

        // Alice creates a private conversation only.
        create_conversation(&conn, alice, "app", false);

        // Bob should see nothing.
        let bob_convs = list_conversations_multiuser(&conn, bob, "app");
        assert!(bob_convs.is_empty());
    }

    // -----------------------------------------------------------------------
    // get_active_conversation multiuser
    // -----------------------------------------------------------------------

    #[test]
    fn get_active_conversation_multiuser_returns_shared() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let alice = crate::auth::user::create_user(&conn, "alice", "$argon2id$fake");
        let bob = crate::auth::user::create_user(&conn, "bob", "$argon2id$fake");

        // Alice creates a shared conversation.
        let alice_conv = create_conversation(&conn, alice, "app", true);

        // Bob can find it with multiuser=true.
        let active = get_active_conversation(&conn, bob, "app", true);
        assert_eq!(active.unwrap().id, alice_conv);

        // Bob cannot find it with multiuser=false.
        assert!(get_active_conversation(&conn, bob, "app", false).is_none());
    }

    #[test]
    fn get_active_conversation_multiuser_excludes_private() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let alice = crate::auth::user::create_user(&conn, "alice", "$argon2id$fake");
        let bob = crate::auth::user::create_user(&conn, "bob", "$argon2id$fake");

        // Alice creates a private conversation.
        create_conversation(&conn, alice, "app", false);

        // Bob can't see it even with multiuser=true.
        assert!(get_active_conversation(&conn, bob, "app", true).is_none());
    }

    // -----------------------------------------------------------------------
    // get_username
    // -----------------------------------------------------------------------

    #[test]
    fn get_username_returns_name() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let uid = crate::auth::user::create_user(&conn, "alice", "$argon2id$fake");
        assert_eq!(get_username(&conn, uid), Some("alice".to_string()));
        assert_eq!(get_username(&conn, 9999), None);
    }

    #[test]
    fn set_conversation_shared_round_trip() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);

        // Create private, toggle to shared, toggle back.
        let conv_id = create_conversation(&conn, user_id, "test", false);
        assert!(!get_conversation(&conn, conv_id).shared);

        set_conversation_shared(&conn, conv_id, true);
        assert!(get_conversation(&conn, conv_id).shared);

        set_conversation_shared(&conn, conv_id, false);
        assert!(!get_conversation(&conn, conv_id).shared);
    }

    #[test]
    fn can_access_conversation_with_privacy_toggle() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let alice = crate::auth::user::create_user(&conn, "alice", "$argon2id$fake");
        let bob = crate::auth::user::create_user(&conn, "bob", "$argon2id$fake");

        // Shared conversation: bob can access in multiuser mode.
        let conv_id = create_conversation(&conn, alice, "test", true);
        let conv = get_conversation(&conn, conv_id);
        assert!(can_access_conversation(bob, &conv, true));

        // Toggle to private: bob loses access.
        set_conversation_shared(&conn, conv_id, false);
        let conv = get_conversation(&conn, conv_id);
        assert!(!can_access_conversation(bob, &conv, true));

        // Toggle back to shared: bob regains access.
        set_conversation_shared(&conn, conv_id, true);
        let conv = get_conversation(&conn, conv_id);
        assert!(can_access_conversation(bob, &conv, true));

        // Alice always has access regardless.
        set_conversation_shared(&conn, conv_id, false);
        let conv = get_conversation(&conn, conv_id);
        assert!(can_access_conversation(alice, &conv, true));
        assert!(can_access_conversation(alice, &conv, false));
    }

    // -----------------------------------------------------------------------
    // list_conversation_summaries
    // -----------------------------------------------------------------------

    #[test]
    fn summaries_include_message_counts() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);

        let conv1 = create_conversation(&conn, user_id, "test", false);
        let conv2 = create_conversation(&conn, user_id, "test", false);

        // Add 3 messages to conv1, 0 to conv2.
        for _ in 0..3 {
            let _ = append_message(
                &conn,
                conv1,
                MessageDirection::Outgoing,
                "user",
                None,
                None,
                "{}",
                None,
                None,
                None,
            );
        }

        let rows = list_conversation_summaries(&conn, user_id, "test");
        assert_eq!(rows.len(), 2);
        // Newest first → conv2 first.
        assert_eq!(rows[0].id, conv2);
        assert_eq!(rows[0].message_count, 0);
        assert_eq!(rows[1].id, conv1);
        assert_eq!(rows[1].message_count, 3);
        // Single-user: owner_username is always None.
        assert!(rows[0].owner_username.is_none());
        assert!(rows[1].owner_username.is_none());
    }

    #[test]
    fn summaries_user_isolated() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let alice = setup_user(&conn);
        let bob = create_user(&conn, "bob", "$argon2id$fake-hash");

        create_conversation(&conn, alice, "test", false);
        create_conversation(&conn, bob, "test", false);

        let alice_rows = list_conversation_summaries(&conn, alice, "test");
        let bob_rows = list_conversation_summaries(&conn, bob, "test");
        assert_eq!(alice_rows.len(), 1);
        assert_eq!(bob_rows.len(), 1);
        assert_ne!(alice_rows[0].id, bob_rows[0].id);
    }

    #[test]
    fn summaries_multiuser_includes_shared_with_username() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let alice = create_user(&conn, "alice", "$argon2id$fake-hash");
        let bob = create_user(&conn, "bob", "$argon2id$fake-hash");

        // Alice: shared, Bob: shared, Alice: private.
        create_conversation(&conn, alice, "test", true);
        create_conversation(&conn, bob, "test", true);
        create_conversation(&conn, alice, "test", false);

        // Bob sees his own + Alice's shared (not Alice's private).
        let rows = list_conversation_summaries_multiuser(&conn, bob, "test");
        assert_eq!(rows.len(), 2);

        // The row owned by Alice should have her username.
        let alice_row = rows.iter().find(|r| r.user_id == alice).unwrap();
        assert_eq!(alice_row.owner_username.as_deref(), Some("alice"));

        // The row owned by Bob should also have his username (LEFT JOIN provides it).
        let bob_row = rows.iter().find(|r| r.user_id == bob).unwrap();
        assert_eq!(bob_row.owner_username.as_deref(), Some("bob"));
    }

    #[test]
    fn summaries_multiuser_excludes_others_private() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let alice = create_user(&conn, "alice", "$argon2id$fake-hash");
        let bob = create_user(&conn, "bob", "$argon2id$fake-hash");

        // Alice: private only.
        create_conversation(&conn, alice, "test", false);

        let rows = list_conversation_summaries_multiuser(&conn, bob, "test");
        assert!(rows.is_empty());
    }

    // -----------------------------------------------------------------------
    // find_empty_conversation tests
    // -----------------------------------------------------------------------

    #[test]
    fn find_empty_conversation_returns_none_when_no_conversations() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let uid = setup_user(&conn);

        assert!(find_empty_conversation(&conn, uid, "test").is_none());
    }

    #[test]
    fn find_empty_conversation_returns_empty_active_conversation() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let uid = setup_user(&conn);

        let cid = create_conversation(&conn, uid, "test", false);

        assert_eq!(find_empty_conversation(&conn, uid, "test"), Some(cid));
    }

    #[test]
    fn find_empty_conversation_ignores_conversation_with_messages() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let uid = setup_user(&conn);

        let cid = create_conversation(&conn, uid, "test", false);
        let _ = append_message(
            &conn,
            cid,
            MessageDirection::Outgoing,
            "text",
            None,
            None,
            "hello",
            Some(uid),
            None,
            None,
        );

        assert!(find_empty_conversation(&conn, uid, "test").is_none());
    }

    #[test]
    fn find_empty_conversation_ignores_completed_conversation() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let uid = setup_user(&conn);

        let cid = create_conversation(&conn, uid, "test", false);
        complete_conversation(&conn, cid, None);

        assert!(
            find_empty_conversation(&conn, uid, "test").is_none(),
            "completed conversations should not be reused"
        );
    }

    #[test]
    fn find_empty_conversation_isolates_by_user_and_app() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let alice = create_user(&conn, "alice", "$argon2id$fake-hash");
        let bob = create_user(&conn, "bob", "$argon2id$fake-hash");

        // Alice has an empty conversation in "app_a".
        let alice_conv = create_conversation(&conn, alice, "app_a", false);
        // Bob has an empty conversation in "app_b".
        let bob_conv = create_conversation(&conn, bob, "app_b", false);

        // Bob can't see Alice's conversation.
        assert!(find_empty_conversation(&conn, bob, "app_a").is_none());
        // Alice can't see Bob's conversation.
        assert!(find_empty_conversation(&conn, alice, "app_b").is_none());
        // Each sees their own.
        assert_eq!(
            find_empty_conversation(&conn, alice, "app_a"),
            Some(alice_conv)
        );
        assert_eq!(find_empty_conversation(&conn, bob, "app_b"), Some(bob_conv));
    }

    #[test]
    fn find_empty_conversation_returns_most_recent() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let uid = setup_user(&conn);

        let _old = create_conversation(&conn, uid, "test", false);
        let new = create_conversation(&conn, uid, "test", false);

        assert_eq!(
            find_empty_conversation(&conn, uid, "test"),
            Some(new),
            "should return the most recent empty conversation"
        );
    }

    // ------------------------------------------------------------------
    // conversation_ids_for_apps (repo-sync consumer lookup)
    // ------------------------------------------------------------------

    #[test]
    fn conversation_ids_for_apps_empty_list_short_circuits() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let uid = setup_user(&conn);
        let _c = create_conversation(&conn, uid, "appa", false);
        // Empty slug list → always empty, regardless of DB content.
        let ids = conversation_ids_for_apps(&conn, &[]);
        assert!(ids.is_empty());
    }

    #[test]
    fn conversation_ids_for_apps_ignores_status_and_still_filters_slug() {
        // Regression: Brenn's conversation lifecycle cycles active↔completed
        // across every turn — singleton PAs are in `completed` between uses.
        // The repo-sync consumer lookup MUST return them or else no agents
        // ever get notified. Staleness filter at drain time handles true
        // abandonment separately.
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let uid = setup_user(&conn);

        let c_a_active = create_conversation(&conn, uid, "appa", false);
        let c_b_active = create_conversation(&conn, uid, "appb", false);
        let c_c_active = create_conversation(&conn, uid, "appc", false);

        // Completed — must STILL appear.
        let c_a_done = create_conversation(&conn, uid, "appa", false);
        complete_conversation(&conn, c_a_done, Some(0.01));

        // Errored — must STILL appear (user may resume after fixing).
        let c_b_err = create_conversation(&conn, uid, "appb", false);
        error_conversation(&conn, c_b_err);

        let mut ids = conversation_ids_for_apps(&conn, &["appa".to_string(), "appb".to_string()]);
        ids.sort();
        let mut expected = vec![c_a_active, c_a_done, c_b_active, c_b_err];
        expected.sort();
        assert_eq!(ids, expected);

        // Querying appc returns only the c_c_active row.
        let ids_c = conversation_ids_for_apps(&conn, &["appc".to_string()]);
        assert_eq!(ids_c, vec![c_c_active]);
    }

    #[test]
    fn conversation_ids_for_apps_unknown_slug_returns_empty() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let uid = setup_user(&conn);
        let _c = create_conversation(&conn, uid, "real-app", false);
        let ids = conversation_ids_for_apps(&conn, &["ghost".to_string()]);
        assert!(ids.is_empty());
    }
}
