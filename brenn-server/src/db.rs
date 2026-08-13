//! Migration composition for the server: every DDL set in the server's
//! dependency closure, run on one connection.
//!
//! It sits here rather than in `bootstrap` because this crate's own tests open
//! through it as well as the composition root does; one function is what keeps
//! the test slice and the production slice from drifting.

use std::path::Path;

use brenn_db::Db;
use rusqlite::Connection;

/// Run every migration set the server's closure owns. Idempotent — every set
/// is `IF NOT EXISTS` throughout, and no set's foreign keys point outside
/// itself and the base tables, so "base first, then any order" holds.
pub fn run_server_slice_migrations(conn: &Connection) {
    // Base tables, push subscriptions, messaging, usage.
    brenn_messaging_store::db::run_slice_migrations(conn);

    // Automation jobs, fires, and the per-app event-conversation mapping.
    brenn_automation::run_automation_migrations(conn);
}

/// Open (or create) the server's database and run every set it needs.
pub fn init_db(path: &Path) -> Db {
    let conn = brenn_db::open_connection(path);
    run_server_slice_migrations(&conn);
    brenn_db::into_db(conn)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// At least one table from every DDL set the server slice runs — not the
    /// slice's whole ~38-table schema. That is the invariant: a set registered
    /// at `run_server_slice_migrations` owes this list a name of its own, and then a
    /// set dropped from the composition, or a newly extracted crate whose set
    /// nobody registered, fails `server_migrations_run_cleanly` rather than at
    /// a runtime table touch. A set with no name here is invisible to this
    /// test.
    const SERVER_SLICE_TABLES: &[&str] = &[
        "users",
        "sessions",
        "invite_codes",
        "conversations",
        "messages",
        "message_attachments",
        "approval_rules",
        "pending_tool_requests",
        "messaging_messages",
        "repo_sync_cursor",
        "usage_sessions",
        "usage_events",
        "devices",
        "device_users",
        "pwa_push_subscriptions",
        "automation_jobs",
        "automation_fires",
        "automation_app_event_conversation",
    ];

    #[test]
    fn server_migrations_run_cleanly() {
        let db = crate::test_support::init_db_memory();
        let conn = db.blocking_lock();
        for table in SERVER_SLICE_TABLES {
            conn.execute(&format!("SELECT 1 FROM {table} WHERE 0"), [])
                .unwrap_or_else(|e| panic!("{table} should exist after the server slice ran: {e}"));
        }
    }

    #[test]
    fn server_migrations_are_idempotent() {
        let conn = brenn_db::open_connection_memory();
        run_server_slice_migrations(&conn);
        run_server_slice_migrations(&conn); // Second run must not fail.
    }
}
