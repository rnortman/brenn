use std::io;

use serde::Serialize;

use crate::format::format_start_time;
use crate::report::{Report, ReportType, Row};

/// A flat, serializable row object for JSON output. Field names match CSV
/// column names exactly. `session_id` and `project` are empty strings in
/// aggregate mode. `start_time` is present only in session-mode rows.
#[derive(Serialize)]
struct FlatRow<'a> {
    session_id: &'a str,
    project: &'a str,
    role: &'a str,
    agent_type: &'a str,
    agent_id: &'a str,
    model: &'a str,
    input_tokens: u64,
    cache_write_5m_tokens: u64,
    cache_write_1h_tokens: u64,
    cache_read_tokens: u64,
    output_tokens: u64,
    total_tokens: u64,
    cost_usd: Option<f64>,
    entry_count: u64,
    /// RFC-3339 UTC string, or `null` if no records. Present only in session
    /// mode; absent (not serialized) in aggregate mode.
    #[serde(skip_serializing_if = "Option::is_none")]
    start_time: Option<String>,
}

impl<'a> FlatRow<'a> {
    /// Build a flat row for serialization.
    ///
    /// `session_id` and `project` are empty strings in aggregate mode.
    /// `start_time` is `None` in aggregate mode; the `skip_serializing_if`
    /// annotation omits the field entirely from aggregate JSON output.
    fn from_row(
        row: &'a Row,
        session_id: &'a str,
        project: &'a str,
        start_time: Option<String>,
    ) -> Self {
        Self {
            session_id,
            project,
            role: row.role.as_str(),
            agent_type: row.agent_type.as_deref().unwrap_or(""),
            agent_id: row.agent_id.as_deref().unwrap_or(""),
            model: row.model.as_deref().unwrap_or(""),
            input_tokens: row.input_tokens,
            cache_write_5m_tokens: row.cache_write_5m_tokens,
            cache_write_1h_tokens: row.cache_write_1h_tokens,
            cache_read_tokens: row.cache_read_tokens,
            output_tokens: row.output_tokens,
            total_tokens: row.total_tokens,
            cost_usd: row.cost_usd,
            entry_count: row.entry_count,
            start_time,
        }
    }
}

pub fn write(report: &Report, writer: &mut dyn io::Write) -> io::Result<()> {
    let flat: Vec<FlatRow<'_>> = match report.report_type {
        ReportType::Session => report
            .sessions
            .iter()
            .flat_map(|sr| {
                sr.rows.iter().map(|row| {
                    let start_time = row.start_time.map(format_start_time);
                    FlatRow::from_row(row, &sr.session_id, &sr.project, start_time)
                })
            })
            .collect(),
        ReportType::Aggregate => report
            .aggregate_rows
            .iter()
            .map(|row| FlatRow::from_row(row, "", "", None))
            .collect(),
    };
    serde_json::to_writer_pretty(writer, &flat).map_err(io::Error::other)?;
    Ok(())
}
