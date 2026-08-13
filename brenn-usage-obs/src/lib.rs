//! Library portion of `brenn-usage-obs` — shared helpers callable from tests.

use chrono::{DateTime, Utc};

/// Parse an ISO-8601 timestamp or a bare `YYYY-MM-DD` date (UTC midnight).
pub fn parse_ts(s: &str) -> Result<DateTime<Utc>, Box<dyn std::error::Error>> {
    brenn_usage_db::parse_ts_str(s).map_err(|e| e.into())
}
