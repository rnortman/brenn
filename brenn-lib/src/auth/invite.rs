use chrono::Utc;
use rand::RngExt;
use rusqlite::Connection;

use crate::db::format_ts_for_db;

/// Create a new invite code. Returns the code string (16 bytes, hex-encoded = 32 chars).
///
/// This is called from a CLI helper or directly in dev, not from a web UI.
pub fn create_invite_code(conn: &Connection) -> String {
    let mut bytes = [0u8; 16];
    rand::rng().fill(&mut bytes);
    let code = hex::encode(bytes);
    let now = format_ts_for_db(Utc::now());

    conn.execute(
        "INSERT INTO invite_codes (code, created_at) VALUES (?1, ?2)",
        (&code, &now),
    )
    .expect("failed to insert invite code");

    code
}

/// Check if an invite code is valid (exists and unused).
pub fn validate_invite_code(conn: &Connection, code: &str) -> bool {
    match conn.query_row(
        "SELECT 1 FROM invite_codes WHERE code = ?1 AND used_by IS NULL",
        [code],
        |_| Ok(()),
    ) {
        Ok(()) => true,
        Err(rusqlite::Error::QueryReturnedNoRows) => false,
        Err(e) => panic!("unexpected database error in validate_invite_code: {e}"),
    }
}

/// Mark an invite code as used by a user. Panics if the code doesn't exist or is already used.
pub fn use_invite_code(conn: &Connection, code: &str, user_id: i64) {
    let now = format_ts_for_db(Utc::now());
    let updated = conn
        .execute(
            "UPDATE invite_codes SET used_by = ?1, used_at = ?2 WHERE code = ?3 AND used_by IS NULL",
            (user_id, &now, code),
        )
        .expect("failed to update invite code");
    assert_eq!(updated, 1, "invite code not found or already used: {code}");
}

/// Check if any unused invite codes exist.
pub fn has_unused_invite_codes(conn: &Connection) -> bool {
    match conn.query_row(
        "SELECT 1 FROM invite_codes WHERE used_by IS NULL LIMIT 1",
        [],
        |_| Ok(()),
    ) {
        Ok(()) => true,
        Err(rusqlite::Error::QueryReturnedNoRows) => false,
        Err(e) => panic!("unexpected database error in has_unused_invite_codes: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::user::create_user;
    use crate::db::init_db_memory;

    /// Create a test user and return its ID (needed for FK constraints).
    fn setup_user(conn: &Connection) -> i64 {
        create_user(conn, "testuser", "$argon2id$fake-hash")
    }

    #[test]
    fn create_and_validate_invite_code() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let code = create_invite_code(&conn);
        assert_eq!(code.len(), 32); // 16 bytes hex
        assert!(validate_invite_code(&conn, &code));
    }

    #[test]
    fn use_invite_code_marks_as_used() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);
        let code = create_invite_code(&conn);

        use_invite_code(&conn, &code, user_id);
        assert!(!validate_invite_code(&conn, &code));
    }

    #[test]
    #[should_panic(expected = "invite code not found or already used")]
    fn double_use_panics() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);
        let code = create_invite_code(&conn);

        use_invite_code(&conn, &code, user_id);
        use_invite_code(&conn, &code, user_id); // Should panic — already used.
    }

    #[test]
    fn validate_nonexistent_code_returns_false() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        assert!(!validate_invite_code(&conn, "nonexistent"));
    }

    #[test]
    fn has_unused_invite_codes_works() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn);

        assert!(!has_unused_invite_codes(&conn));

        let code = create_invite_code(&conn);
        assert!(has_unused_invite_codes(&conn));

        use_invite_code(&conn, &code, user_id);
        assert!(!has_unused_invite_codes(&conn));
    }
}
