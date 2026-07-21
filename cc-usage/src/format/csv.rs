use std::io;

use crate::format::{cost_str, format_start_time_opt};
use crate::report::{Report, ReportType, Row};

const SESSION_HEADER: &[&str] = &[
    "session_id",
    "project",
    "role",
    "agent_type",
    "agent_id",
    "model",
    "input_tokens",
    "cache_write_5m_tokens",
    "cache_write_1h_tokens",
    "cache_read_tokens",
    "output_tokens",
    "total_tokens",
    "cost_usd",
    "entry_count",
    "start_time",
];

// Aggregate header is session header minus the two identifier columns.
// Expressed as a slice so any future column additions to SESSION_HEADER
// automatically propagate here; a mismatch would shift column positions
// and be caught by tests.
const AGGREGATE_HEADER: &[&str] = {
    // `&SESSION_HEADER[2..]` is not yet stable in const context; use the
    // equivalent literal. Keep in sync with SESSION_HEADER[2..].
    &[
        "role",
        "agent_type",
        "agent_id",
        "model",
        "input_tokens",
        "cache_write_5m_tokens",
        "cache_write_1h_tokens",
        "cache_read_tokens",
        "output_tokens",
        "total_tokens",
        "cost_usd",
        "entry_count",
    ]
};

pub fn write(report: &Report, writer: &mut dyn io::Write) -> io::Result<()> {
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(writer);
    match report.report_type {
        ReportType::Session => {
            wtr.write_record(SESSION_HEADER).map_err(io::Error::other)?;
            for sr in &report.sessions {
                for row in &sr.rows {
                    write_session_row(&mut wtr, &sr.session_id, &sr.project, row).map_err(|e| {
                        io::Error::other(format!(
                            "csv write failed for session '{}': {e}",
                            sr.session_id
                        ))
                    })?;
                }
            }
        }
        ReportType::Aggregate => {
            wtr.write_record(AGGREGATE_HEADER)
                .map_err(io::Error::other)?;
            for (idx, row) in report.aggregate_rows.iter().enumerate() {
                write_aggregate_row(&mut wtr, row).map_err(|e| {
                    io::Error::other(format!("csv write failed for aggregate row {idx}: {e}"))
                })?;
            }
        }
    }
    wtr.flush().map_err(io::Error::other)?;
    Ok(())
}

fn write_session_row<W: io::Write>(
    wtr: &mut csv::Writer<W>,
    session_id: &str,
    project: &str,
    row: &Row,
) -> io::Result<()> {
    let cost = cost_str(row.cost_usd);
    let start_time = format_start_time_opt(row.start_time);
    wtr.write_record([
        session_id,
        project,
        row.role.as_str(),
        row.agent_type.as_deref().unwrap_or(""),
        row.agent_id.as_deref().unwrap_or(""),
        row.model.as_deref().unwrap_or(""),
        &row.input_tokens.to_string(),
        &row.cache_write_5m_tokens.to_string(),
        &row.cache_write_1h_tokens.to_string(),
        &row.cache_read_tokens.to_string(),
        &row.output_tokens.to_string(),
        &row.total_tokens.to_string(),
        &cost,
        &row.entry_count.to_string(),
        &start_time,
    ])
    .map_err(io::Error::other)
}

fn write_aggregate_row<W: io::Write>(wtr: &mut csv::Writer<W>, row: &Row) -> io::Result<()> {
    let cost = cost_str(row.cost_usd);
    wtr.write_record([
        row.role.as_str(),
        row.agent_type.as_deref().unwrap_or(""),
        row.agent_id.as_deref().unwrap_or(""),
        row.model.as_deref().unwrap_or(""),
        &row.input_tokens.to_string(),
        &row.cache_write_5m_tokens.to_string(),
        &row.cache_write_1h_tokens.to_string(),
        &row.cache_read_tokens.to_string(),
        &row.output_tokens.to_string(),
        &row.total_tokens.to_string(),
        &cost,
        &row.entry_count.to_string(),
    ])
    .map_err(io::Error::other)
}
