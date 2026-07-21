use crate::db::format_ts_for_db;
use chrono::Utc;
use rusqlite::Connection;

// ---------------------------------------------------------------------------
// Send budget
// ---------------------------------------------------------------------------

/// Reset the per-conversation send budget unconditionally (upsert).
pub fn reset_send_budget(conn: &Connection, conversation_id: i64, budget: u32) {
    let now = format_ts_for_db(Utc::now());
    conn.execute(
        "INSERT INTO messaging_send_budget (conversation_id, remaining, last_reset_at)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(conversation_id) DO UPDATE
         SET remaining = excluded.remaining, last_reset_at = excluded.last_reset_at",
        rusqlite::params![conversation_id, budget, now],
    )
    .expect("messaging: reset_send_budget");
}

/// Outcome of a budget decrement attempt.
#[derive(Debug)]
pub enum BudgetDecrement {
    /// Decrement succeeded; this many remain afterwards.
    Ok { remaining: u32 },
    /// Budget was zero before this call. No decrement happened.
    Exhausted,
}

/// Decrement-or-deny the conversation's budget. Initializes the row to
/// `default_budget` on first call.
///
/// All work happens under the caller's lock — must be invoked while the
/// caller holds `db.lock().await`.
pub fn decrement_send_budget(
    conn: &Connection,
    conversation_id: i64,
    default_budget: u32,
) -> BudgetDecrement {
    let now = format_ts_for_db(Utc::now());
    // Ensure the row exists with the default budget.
    conn.execute(
        "INSERT OR IGNORE INTO messaging_send_budget (conversation_id, remaining, last_reset_at)
         VALUES (?1, ?2, ?3)",
        rusqlite::params![conversation_id, default_budget, now],
    )
    .expect("messaging: ensure budget row");

    // Atomic decrement with predicate.
    let updated = conn
        .execute(
            "UPDATE messaging_send_budget SET remaining = remaining - 1
             WHERE conversation_id = ?1 AND remaining > 0",
            rusqlite::params![conversation_id],
        )
        .expect("messaging: decrement budget");
    if updated == 0 {
        return BudgetDecrement::Exhausted;
    }

    let remaining: i64 = conn
        .query_row(
            "SELECT remaining FROM messaging_send_budget WHERE conversation_id = ?1",
            rusqlite::params![conversation_id],
            |row| row.get(0),
        )
        .expect("messaging: read remaining budget");
    BudgetDecrement::Ok {
        remaining: remaining.max(0) as u32,
    }
}

/// Read the current send-budget remaining for a conversation. Returns
/// `None` if no row exists yet. Test/debug helper.
pub fn read_send_budget(conn: &Connection, conversation_id: i64) -> Option<u32> {
    conn.query_row(
        "SELECT remaining FROM messaging_send_budget WHERE conversation_id = ?1",
        rusqlite::params![conversation_id],
        |row| row.get::<_, i64>(0),
    )
    .ok()
    .map(|n| n.max(0) as u32)
}
