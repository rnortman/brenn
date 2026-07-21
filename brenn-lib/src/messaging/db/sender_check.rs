use rusqlite::Connection;

use super::super::identity::ParticipantId;

// ---------------------------------------------------------------------------
// Startup sender-invariant check
// ---------------------------------------------------------------------------

/// Assert that every `envelope_type='brenn'` row in `messaging_messages` has a
/// structured `sender` value (one of the `app:`, `conversation:`, or `wasm:`
/// schemes as defined by [`ParticipantId::is_structured`]).
///
/// Called unconditionally at startup, before `build_messaging` / `build_pwa_push`,
/// so that any pre-migration row is caught with a clear, actionable panic before
/// any messaging machinery touches the table.
///
/// On success (all senders structured, or no brenn rows) returns silently.
/// On failure panics with the offending row details (up to 50) and remediation
/// instructions.
///
/// `envelope_type='ingress'` rows (`sender=''`) are excluded by the query
/// predicate and are never checked.
///
/// # Design notes
///
/// - Predicate applied in Rust, not SQL: using `ParticipantId::is_structured` as
///   the single source of truth makes drift structurally impossible — a future
///   prefix added to `is_structured` is automatically accepted.
/// - `DISTINCT sender` first, row detail second: the distinct-sender set is tiny
///   (a handful of participants per personal deployment); the detail query runs
///   only on the failure path.
/// - No transaction needed: read-only; single statements suffice.
pub fn assert_senders_structured(conn: &Connection) {
    // Step 1: collect the distinct sender values for brenn-type messages.
    let mut stmt = conn
        .prepare("SELECT DISTINCT sender FROM messaging_messages WHERE envelope_type = 'brenn'")
        .expect("assert_senders_structured: prepare DISTINCT sender query");

    let distinct_senders: Vec<String> = stmt
        .query_map([], |row| row.get(0))
        .expect("assert_senders_structured: query distinct senders")
        .map(|r| r.expect("assert_senders_structured: read sender row"))
        .collect();
    drop(stmt);

    // Step 2: filter through is_structured in Rust (the single source of truth).
    let unstructured: Vec<&str> = distinct_senders
        .iter()
        .filter(|s| !ParticipantId::is_structured(s))
        .map(|s| s.as_str())
        .collect();

    if unstructured.is_empty() {
        return;
    }

    // Step 3: gather row details for the offending senders (capped at 50).
    // Build a parameterized IN clause to avoid any SQL LIKE prefix list.
    let placeholders: String = unstructured
        .iter()
        .enumerate()
        .map(|(i, _)| format!("?{}", i + 1))
        .collect::<Vec<_>>()
        .join(", ");

    let detail_sql = format!(
        "SELECT id, sender, source FROM messaging_messages \
         WHERE envelope_type = 'brenn' AND sender IN ({placeholders}) \
         ORDER BY id ASC \
         LIMIT 51"
    );

    struct OffendingRow {
        id: i64,
        sender: String,
        source: String,
    }

    let mut detail_stmt = conn
        .prepare(&detail_sql)
        .expect("assert_senders_structured: prepare detail query");

    let rows: Vec<OffendingRow> = detail_stmt
        .query_map(rusqlite::params_from_iter(unstructured.iter()), |row| {
            Ok(OffendingRow {
                id: row.get(0)?,
                sender: row.get(1)?,
                source: row.get(2)?,
            })
        })
        .expect("assert_senders_structured: query offending rows")
        .map(|r| r.expect("assert_senders_structured: read offending row"))
        .collect();
    drop(detail_stmt);

    let capped = rows.len().min(50);
    let detail_lines: String = rows[..capped]
        .iter()
        .map(|r| {
            format!(
                "  row id={}: sender={:?} source={:?}",
                r.id, r.sender, r.source
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let suffix = if rows.len() > 50 {
        String::from("\n  (first 50 rows shown; more exist)")
    } else {
        String::new()
    };

    // `unstructured.len()` is the exact count of distinct legacy sender values —
    // already computed above and always accurate regardless of the LIMIT on the
    // detail query.
    let distinct_count = unstructured.len();

    panic!(
        "assert_senders_structured: messaging_messages contains {distinct_count} distinct \
         legacy (non-structured) sender value(s). The DB was not fully migrated before this \
         release was deployed. Operator cleanup required: rewrite the affected rows to \
         structured `app:<slug>@<origin>` identities. Do NOT boot a prior release that \
         includes `migrate_sender_values` as a fix — those releases panic before migrating \
         whenever any `wasm:` sender exists on a brenn row, which is the exact state that \
         exists on any DB where a WASM consumer has published.\n\
         Offending rows:\n{detail_lines}{suffix}",
    );
}
