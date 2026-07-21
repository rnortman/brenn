pub mod csv;
pub mod json;
pub mod markdown;

use chrono::{DateTime, SecondsFormat, Utc};

/// Format an optional cost value as a 4-decimal-place string, or empty string
/// if absent.
pub(super) fn cost_str(cost_usd: Option<f64>) -> String {
    match cost_usd {
        Some(c) => format!("{c:.4}"),
        None => String::new(),
    }
}

/// Format a timestamp as an RFC-3339 UTC string.
pub(super) fn format_start_time(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// Format an optional timestamp as an RFC-3339 UTC string, or empty string if
/// absent.
pub(super) fn format_start_time_opt(t: Option<DateTime<Utc>>) -> String {
    t.map(format_start_time).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn cost_str_none_is_empty() {
        assert_eq!(cost_str(None), "");
    }

    #[test]
    fn cost_str_rounds_to_four_places() {
        assert_eq!(cost_str(Some(1.234_56)), "1.2346");
    }

    #[test]
    fn format_start_time_opt_none_is_empty() {
        assert_eq!(format_start_time_opt(None), "");
    }

    #[test]
    fn format_start_time_opt_some_is_rfc3339() {
        let t = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        assert_eq!(format_start_time_opt(Some(t)), "2024-01-15T10:30:00Z");
    }
}
