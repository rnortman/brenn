use std::fmt;

use chrono::Utc;
use rusqlite::Connection;

use crate::db::format_ts_for_db;

/// A user record from the database.
#[derive(Debug, Clone)]
pub struct User {
    pub id: i64,
    pub username: String,
}

/// Error returned when a username is already taken.
#[derive(Debug)]
pub struct UserAlreadyExists;

impl fmt::Display for UserAlreadyExists {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "username already exists")
    }
}

/// Attempt to create a user. Returns `Ok(user_id)` on success, or
/// `Err(UserAlreadyExists)` if the username violates the UNIQUE constraint.
/// Panics on unexpected database errors (per project convention).
pub fn try_create_user(
    conn: &Connection,
    username: &str,
    password_hash: &str,
) -> Result<i64, UserAlreadyExists> {
    let now = format_ts_for_db(Utc::now());
    match conn.execute(
        "INSERT INTO users (username, password_hash, created_at) VALUES (?1, ?2, ?3)",
        (username, password_hash, &now),
    ) {
        Ok(_) => Ok(conn.last_insert_rowid()),
        Err(ref e) if e.sqlite_error_code() == Some(rusqlite::ErrorCode::ConstraintViolation) => {
            Err(UserAlreadyExists)
        }
        Err(e) => panic!("failed to create user: {e}"),
    }
}

/// Create a new user. Returns the user's row ID.
/// Panics if the username is already taken. Prefer `try_create_user` when
/// the caller needs to handle duplicates gracefully.
pub fn create_user(conn: &Connection, username: &str, password_hash: &str) -> i64 {
    try_create_user(conn, username, password_hash)
        .expect("failed to insert user (duplicate username?)")
}

/// Look up a user by username. Returns None if not found.
pub fn get_user_by_username(conn: &Connection, username: &str) -> Option<User> {
    match conn.query_row(
        "SELECT id, username FROM users WHERE username = ?1",
        [username],
        |row| {
            Ok(User {
                id: row.get(0)?,
                username: row.get(1)?,
            })
        },
    ) {
        Ok(user) => Some(user),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => panic!("unexpected database error in get_user_by_username: {e}"),
    }
}

/// Look up a user by username, case-insensitively.
///
/// When multiple rows would match (possible with BINARY collation schema), returns
/// the row with the lowest `id` (oldest registration) for determinism. Returns `None`
/// if no row matches.
///
/// Used by the pwa_push publish pipeline to resolve an addressed username to the
/// DB-canonical casing, preventing `pwa_push:Alice` and `pwa_push:alice` from
/// creating two distinct `messaging_channels` rows for the same logical user.
pub fn get_user_by_username_nocase(conn: &Connection, username: &str) -> Option<User> {
    match conn.query_row(
        "SELECT id, username FROM users WHERE username = ?1 COLLATE NOCASE ORDER BY id LIMIT 1",
        [username],
        |row| {
            Ok(User {
                id: row.get(0)?,
                username: row.get(1)?,
            })
        },
    ) {
        Ok(user) => Some(user),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => panic!("unexpected database error in get_user_by_username_nocase: {e}"),
    }
}

/// Look up a user's ID and password hash by username, for authentication.
/// Returns None if the user doesn't exist.
pub fn get_user_credentials(conn: &Connection, username: &str) -> Option<(i64, String)> {
    match conn.query_row(
        "SELECT id, password_hash FROM users WHERE username = ?1",
        [username],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ) {
        Ok(creds) => Some(creds),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => panic!("unexpected database error in get_user_credentials: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db_memory;

    #[test]
    fn create_and_lookup_user() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let id = create_user(&conn, "alice", "$argon2id$fake-hash");
        assert!(id > 0);

        let user = get_user_by_username(&conn, "alice").expect("user should exist");
        assert_eq!(user.id, id);
        assert_eq!(user.username, "alice");
    }

    #[test]
    fn lookup_nonexistent_user_returns_none() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        assert!(get_user_by_username(&conn, "nobody").is_none());
    }

    #[test]
    #[should_panic(expected = "duplicate username")]
    fn duplicate_username_panics() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        create_user(&conn, "alice", "hash1");
        create_user(&conn, "alice", "hash2"); // Should panic.
    }

    #[test]
    fn try_create_duplicate_returns_error() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let id = try_create_user(&conn, "bob", "hash1").expect("first create should succeed");
        assert!(id > 0);
        assert!(try_create_user(&conn, "bob", "hash2").is_err());
    }
}
