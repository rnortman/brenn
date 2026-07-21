//! Per-turn cost samples for the 24-hour rolling cost window.
//!
//! Each row records the per-turn cost delta (`current_total - prev_total`)
//! from a single CC turn completion. Old rows are pruned eagerly on each turn.
//! The `sum_since` query is process-wide (no `WHERE conversation_id`) so the
//! 24h aggregate spans all conversations served by this brenn instance.
//!
//! `created_at` is stored as Unix epoch seconds (INTEGER) for fast integer
//! comparison, removing the dependency on the RFC3339 `+00:00` text format.

use chrono::{DateTime, Utc};
use rusqlite::Connection;

/// Insert a per-turn cost sample.
///
/// `turn_cost_usd` is the delta between consecutive `result.total_cost_usd`
/// values. Zero-valued samples are not inserted (caller responsibility).
pub fn insert(conn: &Connection, conversation_id: i64, turn_cost_usd: f64) {
    let now_secs = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO cost_samples (conversation_id, turn_cost_usd, created_at)
         VALUES (?1, ?2, ?3)",
        rusqlite::params![conversation_id, turn_cost_usd, now_secs],
    )
    .expect("cost_samples::insert");
}

/// Sum all cost samples with `created_at >= since` across all conversations.
///
/// Returns 0.0 if no matching rows exist.
pub fn sum_since(conn: &Connection, since: DateTime<Utc>) -> f64 {
    let since_secs = since.timestamp();
    conn.query_row(
        "SELECT COALESCE(SUM(turn_cost_usd), 0.0)
         FROM cost_samples
         WHERE created_at >= ?1",
        rusqlite::params![since_secs],
        |row| row.get::<_, f64>(0),
    )
    .expect("cost_samples::sum_since")
}

/// Insert a cost sample with an explicit timestamp. Available for tests that
/// need to simulate backdated rows (e.g. 24h window boundary tests).
#[cfg(any(test, feature = "testutils"))]
pub fn insert_at(conn: &Connection, conversation_id: i64, turn_cost_usd: f64, at: DateTime<Utc>) {
    let at_secs = at.timestamp();
    conn.execute(
        "INSERT INTO cost_samples (conversation_id, turn_cost_usd, created_at)
         VALUES (?1, ?2, ?3)",
        rusqlite::params![conversation_id, turn_cost_usd, at_secs],
    )
    .expect("cost_samples::insert_at");
}

/// Delete all samples with `created_at < before`.
///
/// Called eagerly on each turn completion with a 24h cutoff to prevent
/// unbounded table growth.
pub fn prune_before(conn: &Connection, before: DateTime<Utc>) {
    let before_secs = before.timestamp();
    conn.execute(
        "DELETE FROM cost_samples WHERE created_at < ?1",
        rusqlite::params![before_secs],
    )
    .expect("cost_samples::prune_before");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth;
    use crate::conversation;
    use crate::db::init_db_memory;
    use chrono::Duration;

    async fn setup_conv() -> (crate::db::Db, i64) {
        let db = init_db_memory();
        let conv_id = {
            let conn = db.lock().await;
            let uid = auth::user::create_user(&conn, "testuser", "$argon2id$fake");
            conversation::create_conversation(&conn, uid, "test", false)
        };
        (db, conv_id)
    }

    #[tokio::test]
    async fn cost_samples_sum_since_empty() {
        let (db, _conv_id) = setup_conv().await;
        let conn = db.lock().await;
        let cutoff = Utc::now() - Duration::hours(24);
        assert_eq!(sum_since(&conn, cutoff), 0.0);
    }

    #[tokio::test]
    async fn cost_samples_sum_since_basic() {
        let (db, conv_id) = setup_conv().await;
        let conn = db.lock().await;
        insert(&conn, conv_id, 0.10);
        insert(&conn, conv_id, 0.05);
        let cutoff = Utc::now() - Duration::hours(24);
        let total = sum_since(&conn, cutoff);
        assert!(
            (total - 0.15).abs() < 1e-9,
            "sum should be 0.15, got {total}"
        );
    }

    #[tokio::test]
    async fn cost_samples_prune_before() {
        let (db, conv_id) = setup_conv().await;
        let conn = db.lock().await;
        // Insert a sample, then prune everything before "now + 1s" (i.e., all rows).
        insert(&conn, conv_id, 0.50);
        let future = Utc::now() + Duration::seconds(1);
        prune_before(&conn, future);
        let cutoff = Utc::now() - Duration::hours(48);
        assert_eq!(
            sum_since(&conn, cutoff),
            0.0,
            "all rows should have been pruned"
        );
    }

    /// `sum_since` excludes rows older than the cutoff (test-5).
    ///
    /// Verifies the `WHERE created_at >= ?1` predicate in `sum_since` — a bug
    /// in the comparison direction or epoch conversion would only be caught here
    /// at the library level, not just in the integration test.
    #[tokio::test]
    async fn cost_samples_sum_since_excludes_old() {
        let (db, conv_id) = setup_conv().await;
        let conn = db.lock().await;
        let now = Utc::now();
        // Insert one row inside the 24h window (12h ago) and one outside (25h ago).
        insert_at(&conn, conv_id, 0.30, now - Duration::hours(12));
        insert_at(&conn, conv_id, 0.50, now - Duration::hours(25));
        let cutoff = now - Duration::hours(24);
        let total = sum_since(&conn, cutoff);
        assert!(
            (total - 0.30).abs() < 1e-9,
            "only the 12h-old row should be in the 24h window; got {total}"
        );
    }

    #[tokio::test]
    async fn cost_samples_prune_before_leaves_recent() {
        let (db, conv_id) = setup_conv().await;
        let conn = db.lock().await;
        insert(&conn, conv_id, 0.30);
        // Prune everything before an hour ago — the just-inserted row is recent,
        // so it survives.
        let cutoff = Utc::now() - Duration::hours(1);
        prune_before(&conn, cutoff);
        let sum = sum_since(&conn, Utc::now() - Duration::hours(24));
        assert!((sum - 0.30).abs() < 1e-9, "recent row should survive prune");
    }
}
