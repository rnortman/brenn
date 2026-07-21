//! Cache of max context-window sizes keyed by model slug.
//!
//! Populated from `result.modelUsage[*].contextWindow` on each turn completion.
//! Persists across bridge restarts so the first `ContextUsage` broadcast after
//! reconnect carries the right denominator even before any `result` frame arrives.

use rusqlite::Connection;

/// Look up the cached max context window for a model slug.
///
/// Returns `(max_tokens, cc_version, updated_at_rfc3339)` or `None` if no
/// entry exists for `slug`. Panics on any DB error other than a cache miss
/// (i.e., `QueryReturnedNoRows`) — consistent with CLAUDE.md's "fail fast
/// on errors" posture.
pub fn get(conn: &Connection, slug: &str) -> Option<(u64, Option<String>, String)> {
    match conn.query_row(
        "SELECT max_tokens, cc_version, updated_at
         FROM model_window_cache
         WHERE model_slug = ?1",
        rusqlite::params![slug],
        |row| {
            let max_tokens: i64 = row.get(0)?;
            let cc_version: Option<String> = row.get(1)?;
            let updated_at: String = row.get(2)?;
            Ok((max_tokens as u64, cc_version, updated_at))
        },
    ) {
        Ok(row) => Some(row),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => panic!("model_window_cache::get DB error for slug {slug:?}: {e}"),
    }
}

/// Insert or replace the max context-window size for a model slug.
///
/// `cc_version` is the CC version string from the most recent `system/init`
/// that observed this slug. May be `None` if the version was unavailable.
pub fn upsert(conn: &Connection, slug: &str, max_tokens: u64, cc_version: Option<&str>) {
    let now = crate::db::format_ts_for_db(chrono::Utc::now());
    conn.execute(
        "INSERT INTO model_window_cache (model_slug, max_tokens, cc_version, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(model_slug) DO UPDATE
             SET max_tokens = ?2, cc_version = ?3, updated_at = ?4",
        rusqlite::params![slug, max_tokens as i64, cc_version, now],
    )
    .expect("model_window_cache::upsert");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db_memory;

    #[tokio::test]
    async fn model_window_cache_round_trip() {
        let db = init_db_memory();
        let conn = db.lock().await;
        upsert(&conn, "claude-opus-4-7[1m]", 1_000_000, Some("2.1.123"));
        let result = get(&conn, "claude-opus-4-7[1m]").expect("should be cached");
        assert_eq!(result.0, 1_000_000);
        assert_eq!(result.1.as_deref(), Some("2.1.123"));
    }

    #[tokio::test]
    async fn model_window_cache_overwrites() {
        let db = init_db_memory();
        let conn = db.lock().await;
        upsert(&conn, "claude-sonnet-4-6", 200_000, Some("2.1.100"));
        upsert(&conn, "claude-sonnet-4-6", 300_000, Some("2.1.123"));
        let result = get(&conn, "claude-sonnet-4-6").expect("should be cached");
        assert_eq!(result.0, 300_000, "second upsert should overwrite");
        assert_eq!(result.1.as_deref(), Some("2.1.123"));
    }

    #[tokio::test]
    async fn model_window_cache_missing_returns_none() {
        let db = init_db_memory();
        let conn = db.lock().await;
        assert!(get(&conn, "never-inserted").is_none());
    }

    #[tokio::test]
    async fn model_window_cache_null_cc_version() {
        let db = init_db_memory();
        let conn = db.lock().await;
        upsert(&conn, "some-model", 200_000, None);
        let result = get(&conn, "some-model").expect("cached");
        assert_eq!(result.0, 200_000);
        assert!(result.1.is_none());
    }
}
