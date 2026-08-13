//! Migration composition for this crate's slice: the `brenn-messaging-store`
//! slice below plus the one set this crate owns
//! ([`crate::repo_sync_cursor::run_repo_sync_cursor_migrations`]).
//!
//! A caller that runs [`run_slice_migrations`] can call anything in this crate
//! or below; a caller that wires a subsystem above this one adds that
//! subsystem's set to its own composition.
//!
//! Not to be confused with `crate::db`, which is `brenn-messaging-store`'s
//! module bound at this crate's root: its `init_db_memory` carries the store
//! slice only, without this crate's cursor table.

use brenn_db::Db;
use rusqlite::Connection;

/// Open an in-memory database carrying this crate's slice, for tests of this
/// crate and the crates between it and the server.
pub fn init_db_memory() -> Db {
    let conn = brenn_db::open_connection_memory();
    run_slice_migrations(&conn);
    brenn_db::into_db(conn)
}

/// Run the slice at or below this crate: the store's composition plus the
/// repo-sync cursor set. Idempotent — every set uses IF NOT EXISTS.
///
/// A composition, not an own set — the `run_*_migrations` name is reserved for
/// the crate that owns the tables.
pub fn run_slice_migrations(conn: &Connection) {
    brenn_messaging_store::db::run_slice_migrations(conn);
    crate::repo_sync_cursor::run_repo_sync_cursor_migrations(conn);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slice_migrations_are_idempotent() {
        let conn = brenn_db::open_connection_memory();
        run_slice_migrations(&conn);
        run_slice_migrations(&conn);
        // One probe per set in the composition: the store's below, this
        // crate's own above.
        conn.execute("SELECT 1 FROM messaging_messages WHERE 0", [])
            .expect("store slice ran below this crate's set");
        conn.execute("SELECT 1 FROM repo_sync_cursor WHERE 0", [])
            .expect("repo_sync_cursor exists after the slice ran");
    }
}
