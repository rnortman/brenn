//! CSV and JSON export writers for usage sessions and events.
//!
//! Both the `brenn-usage-obs` CLI and the `mcp__brenn__ExportUsage` MCP tool
//! call these functions. They write to any `impl Write`, so callers can point
//! them at stdout, a file, or a `Vec<u8>` for tests.

use std::io::{self, Write};

use crate::usage::{EventRow, SessionRow};

// ---------------------------------------------------------------------------
// CSV writers
// ---------------------------------------------------------------------------

/// Write a CSV header + rows for sessions to `writer`.
///
/// Column order matches the CLI/MCP spec:
/// `session_id, started_at, ended_at, duration_secs, open, user, device_slug,
///  app_slug, llm_turns, ui_interactions, total_cost_usd`
///
/// Returns the number of data rows written.
pub fn write_sessions_csv(writer: impl Write, rows: &[SessionRow]) -> Result<usize, csv::Error> {
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(true)
        .from_writer(writer);

    wtr.write_record([
        "session_id",
        "started_at",
        "ended_at",
        "duration_secs",
        "open",
        "user",
        "device_slug",
        "app_slug",
        "llm_turns",
        "ui_interactions",
        "total_cost_usd",
    ])?;

    for row in rows {
        wtr.write_record([
            row.session_id.to_string(),
            row.started_at.clone(),
            row.ended_at.clone().unwrap_or_default(),
            row.duration_secs.to_string(),
            row.open.to_string(),
            row.user.clone(),
            row.device_slug.clone(),
            row.app_slug.clone(),
            row.llm_turns.to_string(),
            row.ui_interactions.to_string(),
            row.total_cost_usd.to_string(),
        ])?;
    }
    wtr.flush()?;
    Ok(rows.len())
}

/// Write a CSV header + rows for events to `writer`.
///
/// Column order:
/// `event_id, created_at, session_id, user, device_slug, app_slug, event_type,
///  conversation_id, turn_cost_usd`
///
/// Returns the number of data rows written.
pub fn write_events_csv(writer: impl Write, rows: &[EventRow]) -> Result<usize, csv::Error> {
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(true)
        .from_writer(writer);

    wtr.write_record([
        "event_id",
        "created_at",
        "session_id",
        "user",
        "device_slug",
        "app_slug",
        "event_type",
        "conversation_id",
        "turn_cost_usd",
    ])?;

    for row in rows {
        wtr.write_record([
            row.event_id.to_string(),
            row.created_at.clone(),
            row.session_id.to_string(),
            row.user.clone(),
            row.device_slug.clone(),
            row.app_slug.clone(),
            row.event_type.clone(),
            row.conversation_id
                .map(|id| id.to_string())
                .unwrap_or_default(),
            row.turn_cost_usd.map(|c| c.to_string()).unwrap_or_default(),
        ])?;
    }
    wtr.flush()?;
    Ok(rows.len())
}

// ---------------------------------------------------------------------------
// JSON writers
// ---------------------------------------------------------------------------

/// Write a JSON array of session rows to `writer`.
///
/// Writes the entire array; callers that need streaming should split the
/// `rows` slice themselves and call this per-chunk.
///
/// Returns the number of rows written.
pub fn write_sessions_json(
    mut writer: impl Write,
    rows: &[SessionRow],
) -> Result<usize, io::Error> {
    serde_json::to_writer(&mut writer, rows).map_err(io::Error::other)?;
    writer.write_all(b"\n")?;
    Ok(rows.len())
}

/// Write a JSON array of event rows to `writer`.
///
/// Returns the number of rows written.
pub fn write_events_json(mut writer: impl Write, rows: &[EventRow]) -> Result<usize, io::Error> {
    serde_json::to_writer(&mut writer, rows).map_err(io::Error::other)?;
    writer.write_all(b"\n")?;
    Ok(rows.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_event_row(
        event_id: i64,
        conversation_id: Option<i64>,
        turn_cost_usd: Option<f64>,
    ) -> EventRow {
        EventRow {
            event_id,
            created_at: "2026-03-01T10:00:00Z".to_string(),
            session_id: 100,
            user: "alice".to_string(),
            device_slug: "chrome-linux".to_string(),
            app_slug: "test-app".to_string(),
            event_type: "ws_connect".to_string(),
            conversation_id,
            turn_cost_usd,
        }
    }

    #[test]
    fn write_events_csv_headers_and_fields() {
        let row_some = make_event_row(1, Some(42), Some(0.05));
        let row_none = make_event_row(2, None, None);
        let rows = vec![row_some, row_none];

        let mut buf = Vec::new();
        let n = write_events_csv(&mut buf, &rows).expect("write_events_csv");
        assert_eq!(n, 2);

        let text = String::from_utf8(buf).expect("utf8");
        let mut rdr = csv::Reader::from_reader(text.as_bytes());

        // Verify headers.
        let headers = rdr.headers().expect("headers").clone();
        let expected_headers = [
            "event_id",
            "created_at",
            "session_id",
            "user",
            "device_slug",
            "app_slug",
            "event_type",
            "conversation_id",
            "turn_cost_usd",
        ];
        assert_eq!(headers.len(), expected_headers.len());
        for (i, h) in expected_headers.iter().enumerate() {
            assert_eq!(&headers[i], *h, "header mismatch at index {i}");
        }

        // Verify data rows.
        let records: Vec<csv::StringRecord> = rdr.records().map(|r| r.expect("record")).collect();
        assert_eq!(records.len(), 2);

        // Derive column indices from the already-verified header order so that a
        // column-order change breaks both the header check and these assertions at
        // a shared source rather than silently diverging.
        let col = |name: &str| {
            expected_headers
                .iter()
                .position(|h| *h == name)
                .unwrap_or_else(|| panic!("header '{name}' not found"))
        };
        let event_id_col = col("event_id");
        let conversation_id_col = col("conversation_id");
        let turn_cost_usd_col = col("turn_cost_usd");

        // Row 0: Some values.
        assert_eq!(&records[0][event_id_col], "1");
        assert_eq!(&records[0][conversation_id_col], "42");
        assert_eq!(&records[0][turn_cost_usd_col], "0.05");

        // Row 1: None values produce empty strings.
        assert_eq!(&records[1][event_id_col], "2");
        assert_eq!(&records[1][conversation_id_col], "");
        assert_eq!(&records[1][turn_cost_usd_col], "");
    }

    #[test]
    fn write_sessions_json_round_trip() {
        let row = SessionRow {
            session_id: 7,
            started_at: "2026-03-01T09:00:00Z".to_string(),
            ended_at: None,
            duration_secs: 1800,
            open: true,
            user: "alice".to_string(),
            device_slug: "chrome-linux".to_string(),
            app_slug: "test-app".to_string(),
            llm_turns: 3,
            ui_interactions: 5,
            total_cost_usd: 1.23,
        };

        let mut buf = Vec::new();
        let n = write_sessions_json(&mut buf, &[row]).expect("write_sessions_json");
        assert_eq!(n, 1);

        let text = String::from_utf8(buf).expect("utf8");
        let values: Vec<serde_json::Value> = serde_json::from_str(&text).expect("parse json");
        assert_eq!(values.len(), 1);

        let v = &values[0];
        assert_eq!(v["session_id"], serde_json::json!(7));
        assert_eq!(v["started_at"], serde_json::json!("2026-03-01T09:00:00Z"));
        assert_eq!(v["ended_at"], serde_json::Value::Null);
        assert_eq!(v["duration_secs"], serde_json::json!(1800));
        assert_eq!(v["open"], serde_json::json!(true));
        assert_eq!(v["user"], serde_json::json!("alice"));
        assert_eq!(v["device_slug"], serde_json::json!("chrome-linux"));
        assert_eq!(v["app_slug"], serde_json::json!("test-app"));
        assert_eq!(v["llm_turns"], serde_json::json!(3));
        assert_eq!(v["ui_interactions"], serde_json::json!(5));
        assert_eq!(v["total_cost_usd"], serde_json::json!(1.23));
    }

    #[test]
    fn write_sessions_csv_headers_and_fields() {
        let row_closed = SessionRow {
            session_id: 1,
            started_at: "2026-03-01T09:00:00Z".to_string(),
            ended_at: Some("2026-03-01T09:30:00Z".to_string()),
            duration_secs: 1800,
            open: false,
            user: "alice".to_string(),
            device_slug: "chrome-linux".to_string(),
            app_slug: "test-app".to_string(),
            llm_turns: 2,
            ui_interactions: 4,
            total_cost_usd: 0.50,
        };
        let row_open = SessionRow {
            session_id: 2,
            started_at: "2026-03-02T10:00:00Z".to_string(),
            ended_at: None,
            duration_secs: 600,
            open: true,
            user: "bob".to_string(),
            device_slug: "firefox-mac".to_string(),
            app_slug: "other-app".to_string(),
            llm_turns: 0,
            ui_interactions: 1,
            total_cost_usd: 0.0,
        };
        let rows = vec![row_closed, row_open];

        let mut buf = Vec::new();
        let n = write_sessions_csv(&mut buf, &rows).expect("write_sessions_csv");
        assert_eq!(n, 2);

        let text = String::from_utf8(buf).expect("utf8");
        let mut rdr = csv::Reader::from_reader(text.as_bytes());

        // Verify headers.
        let headers = rdr.headers().expect("headers").clone();
        let expected_headers = [
            "session_id",
            "started_at",
            "ended_at",
            "duration_secs",
            "open",
            "user",
            "device_slug",
            "app_slug",
            "llm_turns",
            "ui_interactions",
            "total_cost_usd",
        ];
        assert_eq!(headers.len(), expected_headers.len());
        for (i, h) in expected_headers.iter().enumerate() {
            assert_eq!(&headers[i], *h, "header mismatch at index {i}");
        }

        // Derive column indices from the verified header order.
        let col = |name: &str| {
            expected_headers
                .iter()
                .position(|h| *h == name)
                .unwrap_or_else(|| panic!("header '{name}' not found"))
        };
        let ended_at_col = col("ended_at");
        let open_col = col("open");

        let records: Vec<csv::StringRecord> = rdr.records().map(|r| r.expect("record")).collect();
        assert_eq!(records.len(), 2);

        // Row 0: closed session — ended_at present, open = false.
        assert_eq!(&records[0][ended_at_col], "2026-03-01T09:30:00Z");
        assert_eq!(&records[0][open_col], "false");

        // Row 1: open session — ended_at = None → empty string, open = true.
        assert_eq!(&records[1][ended_at_col], "");
        assert_eq!(&records[1][open_col], "true");
    }

    #[test]
    fn write_events_json_round_trip() {
        let row_some = make_event_row(10, Some(99), Some(0.12));
        let row_none = make_event_row(11, None, None);
        let rows = vec![row_some, row_none];

        let mut buf = Vec::new();
        let n = write_events_json(&mut buf, &rows).expect("write_events_json");
        assert_eq!(n, 2);

        let text = String::from_utf8(buf).expect("utf8");
        let values: Vec<serde_json::Value> = serde_json::from_str(&text).expect("parse json");
        assert_eq!(values.len(), 2);

        // Row 0: Some fields are present.
        assert_eq!(values[0]["event_id"], serde_json::json!(10));
        assert_eq!(
            values[0]["created_at"],
            serde_json::json!("2026-03-01T10:00:00Z")
        );
        assert_eq!(values[0]["session_id"], serde_json::json!(100));
        assert_eq!(values[0]["user"], serde_json::json!("alice"));
        assert_eq!(values[0]["device_slug"], serde_json::json!("chrome-linux"));
        assert_eq!(values[0]["app_slug"], serde_json::json!("test-app"));
        assert_eq!(values[0]["event_type"], serde_json::json!("ws_connect"));
        assert_eq!(values[0]["conversation_id"], serde_json::json!(99));
        assert_eq!(values[0]["turn_cost_usd"], serde_json::json!(0.12));

        // Row 1: None fields serialize as null.
        assert_eq!(values[1]["event_id"], serde_json::json!(11));
        assert_eq!(values[1]["conversation_id"], serde_json::Value::Null);
        assert_eq!(values[1]["turn_cost_usd"], serde_json::Value::Null);
    }
}
