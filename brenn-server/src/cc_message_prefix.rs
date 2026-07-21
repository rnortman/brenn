//! CC-message prefix builder.
//!
//! Builds the `[username@device YYYY-MM-DD HH:MM Tz] <text>` prefix that
//! `persist_and_send` and `system_message::render_user_compaction_request`
//! both apply to user-attributed text before sending it on CC's stdin.
//! The prefix is gated by per-app config flags (`prefix_username`,
//! `prefix_timestamp`, `prefix_device`); when all are false the function
//! returns the input text unchanged.
//!
//! Lives in its own module so the system-message renderer can call it
//! without depending on `routes::ws` (and without the temporary `_pub`
//! wrapper that would have been needed otherwise).

use chrono::Timelike;

/// Build the text sent to CC, optionally prefixed with username, device slug,
/// and/or timestamp.
///
/// Format rules:
/// - All three flags off → return `text` unchanged.
/// - Identity token (when `prefix_username` or `prefix_device` is on):
///   - both on → `username@device_slug`
///   - only `prefix_username` → `username`
///   - only `prefix_device` → `@device_slug`
/// - Bracket assembly: `[{identity} {timestamp}] text`, `[{identity}] text`,
///   or `[{timestamp}] text` as appropriate.
/// - `prefix_device = true` with `device_slug = None` emits `unknown` as slug.
pub(crate) fn build_cc_message_text(
    text: &str,
    username: &str,
    device_slug: Option<&str>,
    local_now: &chrono::DateTime<chrono_tz::Tz>,
    prefix_username: bool,
    prefix_timestamp: bool,
    prefix_device: bool,
) -> String {
    if !prefix_username && !prefix_timestamp && !prefix_device {
        return text.to_string();
    }

    let mut prefix = String::from("[");

    let has_identity = prefix_username || prefix_device;
    if has_identity {
        if prefix_username {
            prefix.push_str(username);
        }
        if prefix_device {
            let slug = device_slug.unwrap_or("unknown");
            prefix.push('@');
            prefix.push_str(slug);
        }
    }

    if prefix_timestamp {
        if has_identity {
            prefix.push(' ');
        }
        prefix.push_str(&format!(
            "{} {:02}:{:02} {}",
            local_now.format("%Y-%m-%d"),
            local_now.hour(),
            local_now.minute(),
            local_now.timezone(),
        ));
    }

    prefix.push_str("] ");
    prefix.push_str(text);
    prefix
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_datetime() -> chrono::DateTime<chrono_tz::Tz> {
        use chrono::TimeZone;
        let utc = chrono::Utc.with_ymd_and_hms(2026, 3, 26, 5, 32, 0).unwrap();
        utc.with_timezone(&chrono_tz::Asia::Tokyo) // 14:32 JST
    }

    #[test]
    fn cc_message_no_prefix() {
        let dt = make_test_datetime();
        let result = build_cc_message_text("hello", "alice", None, &dt, false, false, false);
        assert_eq!(result, "hello");
    }

    #[test]
    fn cc_message_username_only() {
        let dt = make_test_datetime();
        let result = build_cc_message_text("hello", "alice", None, &dt, true, false, false);
        assert_eq!(result, "[alice] hello");
    }

    #[test]
    fn cc_message_timestamp_only() {
        let dt = make_test_datetime();
        let result = build_cc_message_text("hello", "alice", None, &dt, false, true, false);
        assert_eq!(result, "[2026-03-26 14:32 Asia/Tokyo] hello");
    }

    #[test]
    fn cc_message_both() {
        let dt = make_test_datetime();
        let result = build_cc_message_text("hello", "alice", None, &dt, true, true, false);
        assert_eq!(result, "[alice 2026-03-26 14:32 Asia/Tokyo] hello");
    }

    #[test]
    fn cc_message_username_and_device() {
        let dt = make_test_datetime();
        let result = build_cc_message_text("hi", "alice", Some("laptop"), &dt, true, false, true);
        assert_eq!(result, "[alice@laptop] hi");
    }

    #[test]
    fn cc_message_device_only() {
        let dt = make_test_datetime();
        let result = build_cc_message_text("hi", "alice", Some("laptop"), &dt, false, false, true);
        assert_eq!(result, "[@laptop] hi");
    }

    #[test]
    fn cc_message_all_three() {
        let dt = make_test_datetime();
        let result = build_cc_message_text("hi", "alice", Some("laptop"), &dt, true, true, true);
        assert_eq!(result, "[alice@laptop 2026-03-26 14:32 Asia/Tokyo] hi");
    }

    #[test]
    fn cc_message_device_unknown_when_none() {
        let dt = make_test_datetime();
        let result = build_cc_message_text("hi", "alice", None, &dt, true, false, true);
        assert_eq!(result, "[alice@unknown] hi");
    }
}
