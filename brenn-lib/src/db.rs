//! Migration composition for this crate's slice: `brenn_db`'s tables plus the
//! ones the subsystems in this crate own.
//!
//! `brenn_db` owns the base tables and the push-subscription table and
//! `brenn_usage_db` the usage-accounting tables. That is the whole slice at or
//! below `brenn-lib` — a caller that runs it can call anything in this crate or
//! below. The messaging tables belong to `brenn-messaging`, a layer up, and are
//! composed there.

use std::path::Path;

use brenn_db::Db;
use rusqlite::Connection;

/// Open (or create) the SQLite database and run this slice's migrations.
pub fn init_db(path: &Path) -> Db {
    let conn = brenn_db::open_connection(path);
    run_slice_migrations(&conn);
    brenn_db::into_db(conn)
}

/// Open an in-memory database carrying this slice, for tests of this crate and
/// the crates between it and the server.
pub fn init_db_memory() -> Db {
    let conn = brenn_db::open_connection_memory();
    run_slice_migrations(&conn);
    brenn_db::into_db(conn)
}

/// Run this slice's migrations: every set owned at or below this crate.
/// Idempotent — uses IF NOT EXISTS on all DDL.
///
/// A composition, not an own set — the `run_*_migrations` name is reserved for
/// the crate that owns the tables.
pub fn run_slice_migrations(conn: &Connection) {
    // Base tables and push subscriptions.
    brenn_db::run_migrations(conn);

    // Usage observability tables (usage_sessions, usage_events).
    brenn_usage_db::run_usage_migrations(conn);
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_db::column_exists;

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
        conn.execute("SELECT 1 FROM usage_sessions WHERE 0", [])
            .expect("usage_sessions table should exist");
        conn.execute("SELECT 1 FROM usage_events WHERE 0", [])
            .expect("usage_events table should exist");
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
    fn migrations_are_idempotent() {
        let conn = Connection::open_in_memory().expect("open");
        conn.pragma_update(None, "foreign_keys", "ON").expect("fk");
        run_slice_migrations(&conn);
        run_slice_migrations(&conn); // Second run should not fail.
    }
}
