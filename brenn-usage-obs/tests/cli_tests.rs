//! Integration tests for `brenn-usage-obs` CLI.
//!
//! These tests seed a temporary SQLite DB, call the CLI library functions
//! directly (not via subprocess), and assert on the output.

use std::io::BufWriter;

use brenn_lib::usage::{
    EventType, close_open_sessions_on_startup, record_llm_turn, record_ui_event, record_ws_connect,
};
use brenn_lib::usage_export::{write_events_json, write_sessions_csv};
use brenn_lib::{
    db::init_db_memory,
    usage::{EventsFilter, SessionsFilter, query_events, query_sessions},
};
use tempfile::NamedTempFile;

/// Seed a fresh in-memory DB with user+device rows and return
/// `(conn, user_id, device_id, conv_id)`.
async fn seed_db() -> (brenn_lib::db::Db, i64, i64, i64) {
    let db = init_db_memory();
    let (uid, did, cid) = {
        let conn = db.lock().await;
        let uid = brenn_lib::auth::user::create_user(&conn, "alice", "$argon2id$fake");
        conn.execute(
            "INSERT INTO devices (token, guessed_slug, user_agent, last_seen_at, created_at)
             VALUES ('aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899',
                     'chrome-linux', 'Mozilla/5.0', datetime('now'), datetime('now'))",
            [],
        )
        .unwrap();
        let did = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO device_users (device_id, user_id, first_seen_at, last_seen_at)
             VALUES (?1, ?2, datetime('now'), datetime('now'))",
            rusqlite::params![did, uid],
        )
        .unwrap();
        let cid = brenn_lib::conversation::create_conversation(&conn, uid, "test-app", false);
        (uid, did, cid)
    };
    (db, uid, did, cid)
}

const GAP: u32 = 1800;

// ---------------------------------------------------------------------------
// sessions_csv_window
// ---------------------------------------------------------------------------

/// Seed three sessions at known timestamps; query the middle window; assert
/// only the middle session appears in the CSV output.
#[tokio::test]
async fn sessions_csv_window() {
    let (db, uid, did, _cid) = seed_db().await;
    let conn = db.lock().await;

    // Insert sessions at three known times.
    let t1 = "2026-01-01T00:00:00Z";
    let t2 = "2026-02-01T00:00:00Z";
    let t3 = "2026-03-01T00:00:00Z";
    for t in [t1, t2, t3] {
        conn.execute(
            "INSERT INTO usage_sessions
                 (device_id, user_id, app_slug, conversation_id, started_at,
                  last_activity_at, ended_at, llm_turns, ui_interactions, total_cost_usd)
             VALUES (?1, ?2, 'test-app', NULL, ?3, ?3, ?3, 0, 0, 0.0)",
            rusqlite::params![did, uid, t],
        )
        .unwrap();
    }

    let from = chrono::DateTime::parse_from_rfc3339("2026-01-15T00:00:00Z")
        .unwrap()
        .to_utc();
    let to = chrono::DateTime::parse_from_rfc3339("2026-03-01T00:00:00Z")
        .unwrap()
        .to_utc();

    let filter = SessionsFilter {
        from,
        to,
        ..Default::default()
    };
    let rows = query_sessions(&conn, &filter);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].started_at, t2);

    // Write to a Vec<u8> and parse the CSV back.
    let mut buf = Vec::new();
    let written = write_sessions_csv(BufWriter::new(&mut buf), &rows).unwrap();
    assert_eq!(written, 1);

    let csv_text = String::from_utf8(buf).unwrap();
    let mut rdr = csv::Reader::from_reader(csv_text.as_bytes());
    let headers = rdr.headers().unwrap().clone();
    assert!(headers.iter().any(|h| h == "session_id"));
    assert!(headers.iter().any(|h| h == "started_at"));
    assert!(headers.iter().any(|h| h == "llm_turns"));

    let records: Vec<csv::StringRecord> = rdr.records().map(|r| r.unwrap()).collect();
    assert_eq!(records.len(), 1);
    let started_at_col = headers.iter().position(|h| h == "started_at").unwrap();
    assert_eq!(&records[0][started_at_col], t2);
}

// ---------------------------------------------------------------------------
// events_json_filters
// ---------------------------------------------------------------------------

/// Seed events for two users; query only `todo_done` events; assert correct
/// JSON output.
#[tokio::test]
async fn events_json_filters() {
    let (db, uid, did, cid) = seed_db().await;
    let conn = db.lock().await;

    record_ui_event(
        &conn,
        did,
        uid,
        "test-app",
        Some(cid),
        EventType::TodoDone,
        GAP,
    );
    record_ui_event(
        &conn,
        did,
        uid,
        "test-app",
        Some(cid),
        EventType::TodoSchedule,
        GAP,
    );
    record_llm_turn(&conn, did, uid, "test-app", Some(cid), 0.05, GAP);

    let epoch_from = chrono::DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z")
        .unwrap()
        .to_utc();
    let epoch_to = chrono::DateTime::parse_from_rfc3339("2099-01-01T00:00:00Z")
        .unwrap()
        .to_utc();

    let filter = EventsFilter {
        from: epoch_from,
        to: epoch_to,
        event_type: Some(EventType::TodoDone),
        ..Default::default()
    };
    let rows = query_events(&conn, &filter);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].event_type, "todo_done");

    // Write JSON.
    let mut buf = Vec::new();
    let written = write_events_json(BufWriter::new(&mut buf), &rows).unwrap();
    assert_eq!(written, 1);

    let parsed: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    let arr = parsed.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["event_type"], "todo_done");
    assert_eq!(arr[0]["user"], "alice");
    assert_eq!(arr[0]["app_slug"], "test-app");
}

// ---------------------------------------------------------------------------
// output_file_written
// ---------------------------------------------------------------------------

/// Write sessions to a temp file; assert file contents match in-memory write.
#[tokio::test]
async fn output_file_written() {
    let (db, uid, did, cid) = seed_db().await;
    let conn = db.lock().await;

    record_ws_connect(&conn, did, uid, "test-app", Some(cid), GAP);
    record_llm_turn(&conn, did, uid, "test-app", Some(cid), 0.10, GAP);
    close_open_sessions_on_startup(&conn);

    let epoch_from = chrono::DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z")
        .unwrap()
        .to_utc();
    let epoch_to = chrono::DateTime::parse_from_rfc3339("2099-01-01T00:00:00Z")
        .unwrap()
        .to_utc();

    let filter = SessionsFilter {
        from: epoch_from,
        to: epoch_to,
        ..Default::default()
    };
    let rows = query_sessions(&conn, &filter);
    assert_eq!(rows.len(), 1);

    // Write to a temp file.
    let tmp = NamedTempFile::new().unwrap();
    {
        let f = std::fs::File::create(tmp.path()).unwrap();
        write_sessions_csv(BufWriter::new(f), &rows).unwrap();
    }

    // Write to memory buffer for comparison.
    let mut buf = Vec::new();
    write_sessions_csv(BufWriter::new(&mut buf), &rows).unwrap();

    let file_contents = std::fs::read(tmp.path()).unwrap();
    assert_eq!(
        file_contents, buf,
        "file output should match in-memory output"
    );

    // Sanity: contains at least the header and one data row.
    let text = String::from_utf8(buf).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert!(lines.len() >= 2, "expected header + 1 data row");
    assert!(lines[0].contains("session_id"));
    assert!(lines[1].contains("test-app"));
}

// ---------------------------------------------------------------------------
// parse_ts helper test
// ---------------------------------------------------------------------------

/// Verify `parse_ts` accepts both ISO-8601 and bare date formats.
#[test]
fn parse_ts_accepts_iso8601_and_bare_date() {
    use brenn_usage_obs::parse_ts;

    let full = parse_ts("2026-05-01T12:00:00Z").unwrap();
    assert_eq!(
        full,
        chrono::DateTime::parse_from_rfc3339("2026-05-01T12:00:00Z")
            .unwrap()
            .to_utc()
    );

    let bare = parse_ts("2026-05-01").unwrap();
    assert_eq!(
        bare,
        chrono::DateTime::parse_from_rfc3339("2026-05-01T00:00:00Z")
            .unwrap()
            .to_utc()
    );

    let err = parse_ts("not-a-date");
    assert!(err.is_err());
}
