use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::warnings::Warning;

/// The canonical output of a usage analysis run.
#[derive(Debug, Serialize, Deserialize)]
pub struct Report {
    pub schema_version: u32,
    pub report_type: ReportType,
    pub generated_at: DateTime<Utc>,
    pub window: Window,
    pub price_table_fingerprint: String,
    /// Populated for Session report; empty for Aggregate.
    pub sessions: Vec<SessionReport>,
    /// Populated for Aggregate report; empty for Session.
    pub aggregate_rows: Vec<Row>,
    pub warnings: Vec<Warning>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportType {
    Session,
    Aggregate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Window {
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SessionReport {
    pub session_id: String,
    pub project: String,
    pub rows: Vec<Row>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    pub role: RowRole,
    /// `Some(name)` for Parent/Subagent/SubagentInvocation rows.
    /// `"untyped"` for parent without agent-setting.
    /// `"unknown"` for orphan subagent.
    /// `None` for SessionTotal.
    pub agent_type: Option<String>,
    /// `Some(id)` only for SubagentInvocation rows.
    pub agent_id: Option<String>,
    /// `Some(name)` when message.model is present.
    pub model: Option<String>,
    pub input_tokens: u64,
    pub cache_write_5m_tokens: u64,
    pub cache_write_1h_tokens: u64,
    pub cache_read_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
    /// `None` when model is unknown/unpriced.
    pub cost_usd: Option<f64>,
    pub entry_count: u64,
    /// Earliest timestamp of any usage record in this row's bucket.
    /// `None` for rows with zero records (e.g. degenerate session total).
    pub start_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RowRole {
    Parent,
    Subagent,
    SubagentInvocation,
    SessionTotal,
}

impl RowRole {
    /// Stable lowercase string for display in formatters. Matches the
    /// `serde(rename_all = "snake_case")` form used in JSON output.
    pub fn as_str(&self) -> &'static str {
        match self {
            RowRole::Parent => "parent",
            RowRole::Subagent => "subagent",
            RowRole::SubagentInvocation => "subagent_invocation",
            RowRole::SessionTotal => "session_total",
        }
    }
}
