use chrono::{Duration, Utc};
use rand::RngExt;
use rusqlite::Connection;

use super::user::User;
use crate::db::format_ts_for_db;

/// Session duration: 90 days. Sessions use a sliding window — this resets on
/// every successful validation, so active sessions never expire.
const SESSION_DURATION_DAYS: i64 = 90;

/// A validated session with its associated user.
#[derive(Debug, Clone)]
pub struct Session {
    pub token: String,
    pub user: User,
    pub csrf_token: String,
}

/// Create a new session for a user. Returns the session token and CSRF token.
///
/// The session token is 32 bytes of CSPRNG, hex-encoded (64 chars).
/// The CSRF token is 16 bytes of CSPRNG, hex-encoded (32 chars).
pub fn create_session(conn: &Connection, user_id: i64) -> (String, String) {
    let token = generate_token(32);
    let csrf_token = generate_token(16);
    let now = Utc::now();
    let expires_at = now + Duration::days(SESSION_DURATION_DAYS);

    conn.execute(
        "INSERT INTO sessions (token, user_id, created_at, expires_at, last_seen_at, csrf_token) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        (
            &token,
            user_id,
            &format_ts_for_db(now),
            &format_ts_for_db(expires_at),
            &format_ts_for_db(now),
            &csrf_token,
        ),
    )
    .expect("failed to insert session");

    (token, csrf_token)
}

/// Validate a session token. Returns the session (with user) if valid and not expired.
/// On success, updates `last_seen_at` and extends `expires_at` (sliding window).
pub fn validate_session(conn: &Connection, token: &str) -> Option<Session> {
    let now = format_ts_for_db(Utc::now());

    // Look up session + user in one query.
    let result = conn.query_row(
        "SELECT s.token, s.csrf_token, s.expires_at, u.id, u.username \
         FROM sessions s \
         JOIN users u ON s.user_id = u.id \
         WHERE s.token = ?1",
        [token],
        |row| {
            let expires_at: String = row.get(2)?;
            Ok((
                Session {
                    token: row.get(0)?,
                    csrf_token: row.get(1)?,
                    user: User {
                        id: row.get(3)?,
                        username: row.get(4)?,
                    },
                },
                expires_at,
            ))
        },
    );

    match result {
        Ok((session, expires_at)) => {
            // Check expiry.
            if now > expires_at {
                // Expired — clean it up.
                delete_session(conn, &session.token);
                return None;
            }
            // Sliding window: extend expiration on every successful validation.
            let new_expires = format_ts_for_db(Utc::now() + Duration::days(SESSION_DURATION_DAYS));
            conn.execute(
                "UPDATE sessions SET last_seen_at = ?1, expires_at = ?2 WHERE token = ?3",
                (&now, &new_expires, &session.token),
            )
            .expect("failed to update session");
            Some(session)
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => panic!("unexpected database error in validate_session: {e}"),
    }
}

/// Delete a session by token.
pub fn delete_session(conn: &Connection, token: &str) {
    conn.execute("DELETE FROM sessions WHERE token = ?1", [token])
        .expect("failed to delete session");
}

/// Delete all expired sessions. Call periodically for hygiene.
pub fn cleanup_expired_sessions(conn: &Connection) -> usize {
    let now = format_ts_for_db(Utc::now());
    conn.execute("DELETE FROM sessions WHERE expires_at < ?1", [&now])
        .expect("failed to clean up expired sessions")
}

/// Generate a cryptographically random hex token of the given byte length.
fn generate_token(byte_len: usize) -> String {
    let mut bytes = vec![0u8; byte_len];
    rand::rng().fill(&mut bytes[..]);
    hex::encode(bytes)
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
    fn create_and_validate_session() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);

        let (token, csrf_token) = create_session(&conn, user_id);
        assert_eq!(token.len(), 64); // 32 bytes hex
        assert_eq!(csrf_token.len(), 32); // 16 bytes hex

        let session = validate_session(&conn, &token).expect("session should be valid");
        assert_eq!(session.token, token);
        assert_eq!(session.csrf_token, csrf_token);
        assert_eq!(session.user.id, user_id);
        assert_eq!(session.user.username, "testuser");
    }

    #[test]
    fn validate_nonexistent_session_returns_none() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        assert!(validate_session(&conn, "nonexistent-token").is_none());
    }

    #[test]
    fn delete_session_invalidates_it() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);
        let (token, _) = create_session(&conn, user_id);

        delete_session(&conn, &token);
        assert!(validate_session(&conn, &token).is_none());
    }

    #[test]
    fn expired_session_is_rejected_and_cleaned() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);

        // Insert a session that's already expired.
        let token = generate_token(32);
        let csrf_token = generate_token(16);
        let past = Utc::now() - Duration::days(91);
        conn.execute(
            "INSERT INTO sessions (token, user_id, created_at, expires_at, last_seen_at, csrf_token) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            (
                &token,
                user_id,
                &format_ts_for_db(past),
                &format_ts_for_db(past), // expires_at in the past
                &format_ts_for_db(past),
                &csrf_token,
            ),
        )
        .unwrap();

        // Validation should reject and clean up.
        assert!(validate_session(&conn, &token).is_none());
        // Session row should be gone.
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE token = ?1",
                [&token],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn cleanup_expired_sessions_removes_old() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);

        // One valid session.
        let (valid_token, _) = create_session(&conn, user_id);

        // One expired session.
        let expired_token = generate_token(32);
        let past = Utc::now() - Duration::days(91);
        conn.execute(
            "INSERT INTO sessions (token, user_id, created_at, expires_at, last_seen_at, csrf_token) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            (
                &expired_token,
                user_id,
                &format_ts_for_db(past),
                &format_ts_for_db(past),
                &format_ts_for_db(past),
                "csrf",
            ),
        )
        .unwrap();

        let removed = cleanup_expired_sessions(&conn);
        assert_eq!(removed, 1);
        assert!(validate_session(&conn, &valid_token).is_some());
    }

    #[test]
    fn validate_session_extends_expiration() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);

        // Create a session manually with expires_at only 10 days from now,
        // simulating a session created 80 days ago that's almost expired.
        let token = generate_token(32);
        let csrf_token = generate_token(16);
        let created = Utc::now() - Duration::days(80);
        let old_expires = Utc::now() + Duration::days(10);
        conn.execute(
            "INSERT INTO sessions (token, user_id, created_at, expires_at, last_seen_at, csrf_token) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            (
                &token,
                user_id,
                &format_ts_for_db(created),
                &format_ts_for_db(old_expires),
                &format_ts_for_db(created),
                &csrf_token,
            ),
        )
        .unwrap();

        // Validate — this should extend expires_at back to 90 days from now.
        assert!(validate_session(&conn, &token).is_some());

        let updated_expires: String = conn
            .query_row(
                "SELECT expires_at FROM sessions WHERE token = ?1",
                [&token],
                |row| row.get(0),
            )
            .unwrap();

        // The new expiration should be ~90 days out, far beyond the old 10 days.
        // Check it's at least 80 days out (generous margin for clock granularity).
        let updated = chrono::DateTime::parse_from_rfc3339(&updated_expires).unwrap();
        let min_expected = Utc::now() + Duration::days(80);
        assert!(
            updated > min_expected,
            "expected expires_at to be extended to ~90 days out, got {updated}"
        );
    }
}
