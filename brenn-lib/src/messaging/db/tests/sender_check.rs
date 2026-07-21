use rusqlite::Connection;
use uuid::Uuid;

use crate::db::init_db_memory;
use crate::messaging::db::assert_senders_structured;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Insert a bare messaging_messages row. Uses NULL channel_uuid (ingress-style)
/// to avoid channel FK requirements.
fn insert_msg(conn: &Connection, sender: &str, envelope_type: &str, source: &str) -> i64 {
    let uuid = Uuid::new_v4();
    let uuid_bytes = uuid.as_bytes().to_vec();
    conn.execute(
        "INSERT INTO messaging_messages \
         (uuid, channel_uuid, source, sender, body, urgency, publish_ts_ns, created_at, envelope_type) \
         VALUES (?1, NULL, ?2, ?3, 'body', 'low', 1, '2024-01-01', ?4)",
        rusqlite::params![uuid_bytes, source, sender, envelope_type],
    )
    .expect("insert_msg");
    conn.last_insert_rowid()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Empty DB — no rows — passes silently.
#[test]
fn empty_db_passes() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    assert_senders_structured(&conn); // must not panic
}

/// All three structured identity kinds are accepted.
/// The `wasm:` case is the prod-incident regression test (row id=540 in prod,
/// `sender="wasm:consume-demo-alice"`).
#[test]
fn all_structured_kinds_pass() {
    let db = init_db_memory();
    let conn = db.blocking_lock();

    insert_msg(&conn, "app:my-app@https://brenn.example", "brenn", "src");
    insert_msg(&conn, "conversation:42", "brenn", "src");
    // Prod-incident regression: wasm: sender on a brenn row must NOT panic.
    insert_msg(
        &conn,
        "wasm:consume-demo-alice",
        "brenn",
        "consume-demo-alice",
    );

    assert_senders_structured(&conn); // must not panic
}

/// The wasm: prefix passes — explicit regression pin for prod row id=540.
#[test]
fn wasm_sender_passes_prod_regression_row_540() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    insert_msg(
        &conn,
        "wasm:consume-demo-alice",
        "brenn",
        "consume-demo-alice",
    );
    assert_senders_structured(&conn); // must not panic
}

/// Legacy bare-slug sender panics.
#[test]
#[should_panic(expected = "legacy (non-structured) sender value(s)")]
fn legacy_bare_slug_sender_panics() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    insert_msg(&conn, "pa-alice", "brenn", "https://brenn.example");
    assert_senders_structured(&conn);
}

/// Legacy display-name sender panics — this previously would have been
/// rule-2-mappable under `migrate_sender_values` (source="phonebuddy" is a slug),
/// but is now unconditionally fatal. Pins the behavior change.
#[test]
#[should_panic(expected = "legacy (non-structured) sender value(s)")]
fn legacy_display_name_sender_panics() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    // source="phonebuddy" was mappable under old migration rule 2 — check still panics.
    insert_msg(&conn, "Phone Buddy", "brenn", "phonebuddy");
    assert_senders_structured(&conn);
}

/// Panic message contains the correct row id, sender, and source values.
#[test]
fn panic_message_contains_row_detail() {
    use std::panic;

    let db = init_db_memory();
    let conn = db.blocking_lock();
    let id = insert_msg(&conn, "bad-sender", "brenn", "src-value");

    // Capture the panic payload to inspect the message.
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        assert_senders_structured(&conn);
    }));

    let err = result.expect_err("assert_senders_structured must panic for legacy sender");
    let msg = err
        .downcast_ref::<String>()
        .map(|s| s.as_str())
        .or_else(|| err.downcast_ref::<&str>().copied())
        .expect("panic payload must be a string");

    assert!(
        msg.contains("bad-sender"),
        "panic message must contain the legacy sender value; got: {msg}"
    );
    assert!(
        msg.contains("src-value"),
        "panic message must contain the source value; got: {msg}"
    );
    // The exact row id must appear — not just the "row id=" prefix.
    assert!(
        msg.contains(&format!("row id={id}")),
        "panic message must contain the correct row id ({id}); got: {msg}"
    );
}

/// Ingress rows (envelope_type='ingress', sender='') are ignored; check passes.
#[test]
fn ingress_rows_ignored() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    insert_msg(&conn, "", "ingress", "");
    assert_senders_structured(&conn); // must not panic
}

/// Mixed: some structured rows plus one legacy row — check still panics.
#[test]
#[should_panic(expected = "legacy (non-structured) sender value(s)")]
fn mixed_structured_and_legacy_panics() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    insert_msg(&conn, "app:my-app@https://brenn.example", "brenn", "src");
    insert_msg(&conn, "wasm:my-consumer", "brenn", "src");
    // One legacy row in the mix is enough to fail.
    insert_msg(&conn, "old-slug", "brenn", "src");
    assert_senders_structured(&conn);
}

/// Webhook rows (envelope_type='webhook') with non-structured senders are ignored;
/// `publish_transport_ingress` stamps `sender=key_id` on webhook rows, so any real
/// deployment will have them.
#[test]
fn webhook_rows_ignored() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    insert_msg(&conn, "some-key-id", "webhook", "src");
    assert_senders_structured(&conn); // must not panic
}

/// Multiple distinct unstructured senders — exercises the multi-placeholder IN clause.
/// Both sender values must appear in the panic message.
#[test]
fn multiple_distinct_unstructured_senders_panic() {
    use std::panic;

    let db = init_db_memory();
    let conn = db.blocking_lock();
    insert_msg(&conn, "old-slug-a", "brenn", "src");
    insert_msg(&conn, "old-slug-b", "brenn", "src");

    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        assert_senders_structured(&conn);
    }));

    let err = result.expect_err("must panic with multiple legacy senders");
    let msg = err
        .downcast_ref::<String>()
        .map(|s| s.as_str())
        .or_else(|| err.downcast_ref::<&str>().copied())
        .expect("panic payload must be a string");

    assert!(
        msg.contains("old-slug-a"),
        "panic message must contain first legacy sender; got: {msg}"
    );
    assert!(
        msg.contains("old-slug-b"),
        "panic message must contain second legacy sender; got: {msg}"
    );
}

/// More than 50 offending rows — the detail display caps at 50 and the overflow
/// suffix appears. The distinct count in the message reflects the actual number of
/// distinct legacy sender values, not the fetch-capped row count.
#[test]
fn overflow_rows_shows_capped_detail_and_suffix() {
    use std::panic;

    let db = init_db_memory();
    let conn = db.blocking_lock();

    // Insert 52 rows each with a unique legacy sender to force the LIMIT 51 path.
    for i in 0..52 {
        insert_msg(&conn, &format!("legacy-sender-{i:03}"), "brenn", "src");
    }

    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        assert_senders_structured(&conn);
    }));

    let err = result.expect_err("must panic with >50 legacy senders");
    let msg = err
        .downcast_ref::<String>()
        .map(|s| s.as_str())
        .or_else(|| err.downcast_ref::<&str>().copied())
        .expect("panic payload must be a string");

    // Distinct count must say 52, not 51.
    assert!(
        msg.contains("52 distinct"),
        "panic message must report 52 distinct legacy senders; got: {msg}"
    );
    // Overflow suffix must appear.
    assert!(
        msg.contains("more exist"),
        "panic message must note that more rows exist beyond the 50 shown; got: {msg}"
    );
    // Exactly 50 detail lines shown (each starts with "  row id=").
    let detail_line_count = msg.lines().filter(|l| l.contains("row id=")).count();
    assert_eq!(
        detail_line_count, 50,
        "panic message must show exactly 50 detail lines; got {detail_line_count}"
    );
}
