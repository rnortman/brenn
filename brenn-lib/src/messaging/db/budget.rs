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

/// Give back a unit drawn for a publish that then committed nothing — the
/// compensating half of [`decrement_send_budget`], for the refusals that can
/// only be discovered after the draw.
///
/// Clamped at `default_budget`: a refund racing a reset must not mint budget
/// above the ceiling the reset just set. A missing row (nothing was ever drawn)
/// updates nothing.
///
/// All work happens under the caller's lock — must be invoked while the caller
/// holds `db.lock().await`.
pub fn refund_send_budget(conn: &Connection, conversation_id: i64, default_budget: u32) {
    conn.execute(
        "UPDATE messaging_send_budget SET remaining = remaining + 1
         WHERE conversation_id = ?1 AND remaining < ?2",
        rusqlite::params![conversation_id, default_budget],
    )
    .expect("messaging: refund send budget");
}

/// Read the current send-budget remaining for a conversation. `None` means no
/// row exists yet — nothing has ever been drawn.
///
/// Errors panic rather than resolving to `None`: a silent "untouched" on a
/// corrupt read would let a turn-provoking injection through.
pub fn read_send_budget(conn: &Connection, conversation_id: i64) -> Option<u32> {
    match conn.query_row(
        "SELECT remaining FROM messaging_send_budget WHERE conversation_id = ?1",
        rusqlite::params![conversation_id],
        |row| row.get::<_, i64>(0),
    ) {
        Ok(remaining) => Some(remaining.max(0) as u32),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => panic!("messaging: read_send_budget: {e}"),
    }
}
