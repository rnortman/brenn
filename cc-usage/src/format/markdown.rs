use std::io;

use crate::format::{cost_str, format_start_time_opt};
use crate::report::{Report, ReportType, Row};

pub fn write(report: &Report, writer: &mut dyn io::Write) -> io::Result<()> {
    match report.report_type {
        ReportType::Session => {
            for sr in &report.sessions {
                writeln!(writer, "## Session {}", sr.session_id)?;
                writeln!(writer, "Project: {}", sr.project)?;
                writeln!(writer)?;
                write_table(writer, &sr.rows, true)?;
                writeln!(writer)?;
            }
        }
        ReportType::Aggregate => {
            let window_str = format_window(report);
            writeln!(writer, "## Aggregate {window_str}")?;
            writeln!(writer)?;
            write_table(writer, &report.aggregate_rows, false)?;
            writeln!(writer)?;
        }
    }

    if !report.warnings.is_empty() {
        writeln!(writer, "### Warnings")?;
        for w in &report.warnings {
            writeln!(writer, "- {}", w.message)?;
        }
    }

    Ok(())
}

fn format_window(report: &Report) -> String {
    match (report.window.from, report.window.to) {
        (Some(from), Some(to)) => {
            format!("{} to {}", from.format("%Y-%m-%d"), to.format("%Y-%m-%d"))
        }
        (Some(from), None) => format!("from {}", from.format("%Y-%m-%d")),
        (None, Some(to)) => format!("to {}", to.format("%Y-%m-%d")),
        (None, None) => "(all time)".to_string(),
    }
}

fn write_table(writer: &mut dyn io::Write, rows: &[Row], session_mode: bool) -> io::Result<()> {
    if session_mode {
        writeln!(
            writer,
            "| Role | Agent Type | Agent ID | Model | Input | CW 5m | CW 1h | C Read | Output | Total | Cost USD | Entries | Start Time |"
        )?;
        writeln!(
            writer,
            "|------|-----------|----------|-------|------:|------:|------:|-------:|-------:|------:|---------:|--------:|------------|"
        )?;
    } else {
        writeln!(
            writer,
            "| Role | Agent Type | Agent ID | Model | Input | CW 5m | CW 1h | C Read | Output | Total | Cost USD | Entries |"
        )?;
        writeln!(
            writer,
            "|------|-----------|----------|-------|------:|------:|------:|-------:|-------:|------:|---------:|--------:|"
        )?;
    }

    for row in rows {
        let role = row.role.as_str();
        let agent_type = row.agent_type.as_deref().unwrap_or("-");
        let agent_id = row.agent_id.as_deref().unwrap_or("-");
        let model = row.model.as_deref().unwrap_or("-");
        let cost = cost_str(row.cost_usd);

        if session_mode {
            let start_time = format_start_time_opt(row.start_time);
            writeln!(
                writer,
                "| {role} | {agent_type} | {agent_id} | {model} | {} | {} | {} | {} | {} | {} | {cost} | {} | {start_time} |",
                fmt_num(row.input_tokens),
                fmt_num(row.cache_write_5m_tokens),
                fmt_num(row.cache_write_1h_tokens),
                fmt_num(row.cache_read_tokens),
                fmt_num(row.output_tokens),
                fmt_num(row.total_tokens),
                fmt_num(row.entry_count),
            )?;
        } else {
            writeln!(
                writer,
                "| {role} | {agent_type} | {agent_id} | {model} | {} | {} | {} | {} | {} | {} | {cost} | {} |",
                fmt_num(row.input_tokens),
                fmt_num(row.cache_write_5m_tokens),
                fmt_num(row.cache_write_1h_tokens),
                fmt_num(row.cache_read_tokens),
                fmt_num(row.output_tokens),
                fmt_num(row.total_tokens),
                fmt_num(row.entry_count),
            )?;
        }
    }

    Ok(())
}

/// Format a number with thousands separators.
fn fmt_num(n: u64) -> String {
    let s = n.to_string();
    let chars: Vec<char> = s.chars().collect();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    let len = chars.len();
    for (i, c) in chars.iter().enumerate() {
        if i > 0 && (len - i).is_multiple_of(3) {
            result.push(',');
        }
        result.push(*c);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fmt_num_basic() {
        assert_eq!(fmt_num(0), "0");
        assert_eq!(fmt_num(999), "999");
        assert_eq!(fmt_num(1000), "1,000");
        assert_eq!(fmt_num(1_234_567), "1,234,567");
        assert_eq!(fmt_num(u64::MAX), "18,446,744,073,709,551,615");
    }
}
