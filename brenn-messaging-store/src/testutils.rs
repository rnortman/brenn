//! Test helpers over this crate's tables, for its own suites and the crates
//! above it.
//!
//! Available under `#[cfg(test)]` or when the `testutils` feature is enabled.

/// Seed the FK rows (user id 1, conversation `conversation_id`) required by
/// tests that insert into tables referencing `users`/`conversations`.
pub fn ensure_user_and_conv(conn: &rusqlite::Connection, conversation_id: i64) {
    conn.execute(
        "INSERT OR IGNORE INTO users (id, username, password_hash, created_at) \
         VALUES (1, 'u', 'h', '2024-01-01')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT OR IGNORE INTO conversations (id, user_id, status, created_at, updated_at) \
         VALUES (?1, 1, 'active', '2024-01-01', '2024-01-01')",
        [conversation_id],
    )
    .unwrap();
}
