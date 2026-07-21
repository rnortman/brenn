/// Compare two version strings of the form `X.Y.Z` numerically.
///
/// Returns `true` when `v >= minimum`. Non-numeric components and versions
/// with more or fewer parts than three are not supported (CC versions are
/// always plain `major.minor.patch`). Malformed input panics — the caller
/// already did the version-floor check so a malformed string here is a bug.
///
/// ```
/// use brenn_lib::util::version_at_least;
/// assert!(version_at_least("2.1.123", "2.1.123"));
/// assert!(version_at_least("2.1.124", "2.1.123"));
/// assert!(!version_at_least("2.1.122", "2.1.123"));
/// assert!(version_at_least("3.0.0", "2.1.123"));
/// assert!(!version_at_least("2.0.999", "2.1.123"));
/// ```
pub fn version_at_least(v: &str, minimum: &str) -> bool {
    fn parse(s: &str) -> (u64, u64, u64) {
        let parts: Vec<&str> = s.split('.').collect();
        assert!(parts.len() == 3, "version string must be X.Y.Z, got {s:?}");
        let major: u64 = parts[0]
            .parse()
            .unwrap_or_else(|_| panic!("non-numeric major version in {s:?}"));
        let minor: u64 = parts[1]
            .parse()
            .unwrap_or_else(|_| panic!("non-numeric minor version in {s:?}"));
        let patch: u64 = parts[2]
            .parse()
            .unwrap_or_else(|_| panic!("non-numeric patch version in {s:?}"));
        (major, minor, patch)
    }
    let (vm, vn, vp) = parse(v);
    let (mm, mn, mp) = parse(minimum);
    (vm, vn, vp) >= (mm, mn, mp)
}

/// Escape a string for safe inclusion in HTML content or attribute values.
/// Prevents XSS from user-controlled strings rendered into templates.
pub fn html_escape(s: &str) -> String {
    let mut escaped = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#x27;"),
            _ => escaped.push(c),
        }
    }
    escaped
}

/// Maximum byte length for graf subprocess error strings stored in the DB or
/// injected into CC context. Shared by all call sites that cap graf output.
pub const GRAF_ERROR_MAX_BYTES: usize = 4096;

/// Truncate `text` to at most `max_bytes` bytes (UTF-8 safe) and append a marker.
///
/// If `text.len() <= max_bytes`, returns an owned copy of `text` unchanged.
/// Otherwise, slices at the largest UTF-8 char boundary at or before `max_bytes`
/// and appends `"…\n\n[truncated, {original_len} bytes total]"`.
///
/// The retained prefix is at most `max_bytes` bytes; the marker suffix is additive,
/// so the total output may exceed `max_bytes` by the length of the marker (~30 bytes).
///
/// ```
/// use brenn_lib::util::truncate_with_marker;
/// let short = "hello";
/// assert_eq!(truncate_with_marker(short, 100), "hello");
/// let long = "abcde";
/// let out = truncate_with_marker(long, 3);
/// assert!(out.starts_with("abc"));
/// assert!(out.contains("[truncated, 5 bytes total]"));
/// ```
pub fn truncate_with_marker(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_string();
    }
    let boundary = text.floor_char_boundary(max_bytes);
    let prefix = &text[..boundary];
    let original_len = text.len();
    format!("{prefix}…\n\n[truncated, {original_len} bytes total]")
}

/// Serialize a JSON value for safe embedding in a `<script type="application/json">` tag.
///
/// The only dangerous sequence in a script tag's text content is `</` which could
/// prematurely close the tag. We escape `</` → `<\/` which is valid in JSON string
/// context and prevents the browser from seeing a closing tag.
pub fn json_for_script_tag(value: &serde_json::Value) -> String {
    let json = serde_json::to_string(value).unwrap();
    json.replace("</", "<\\/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_at_least_equal() {
        assert!(version_at_least("2.1.123", "2.1.123"));
    }

    #[test]
    fn version_at_least_greater_patch() {
        assert!(version_at_least("2.1.124", "2.1.123"));
    }

    #[test]
    fn version_at_least_less_patch() {
        assert!(!version_at_least("2.1.122", "2.1.123"));
    }

    #[test]
    fn version_at_least_greater_major() {
        assert!(version_at_least("3.0.0", "2.1.123"));
    }

    #[test]
    fn version_at_least_less_minor() {
        assert!(!version_at_least("2.0.999", "2.1.123"));
    }

    #[test]
    fn html_escape_special_chars() {
        assert_eq!(html_escape("<>&\"'"), "&lt;&gt;&amp;&quot;&#x27;");
    }

    #[test]
    fn truncate_below_cap_returns_identical() {
        assert_eq!(truncate_with_marker("hello", 100), "hello");
    }

    #[test]
    fn truncate_exactly_at_cap_returns_identical() {
        let s = "hello";
        assert_eq!(truncate_with_marker(s, s.len()), "hello");
    }

    #[test]
    fn truncate_above_cap_truncates_with_marker() {
        let s = "abcdefgh";
        let out = truncate_with_marker(s, 3);
        assert!(out.starts_with("abc"), "output: {out:?}");
        assert!(
            out.contains("[truncated, 8 bytes total]"),
            "output: {out:?}"
        );
    }

    #[test]
    fn truncate_multibyte_utf8_no_split() {
        // Each Japanese char is 3 bytes. With max_bytes=5, floor_char_boundary(5) = 3.
        let s = "日本語テスト"; // 18 bytes total
        let out = truncate_with_marker(s, 5);
        assert!(
            std::str::from_utf8(out.as_bytes()).is_ok(),
            "not valid UTF-8"
        );
        // Prefix must be exactly "日" (3 bytes, since 5 floors to 3)
        assert!(out.starts_with("日"), "output: {out:?}");
        assert!(
            !out.starts_with("日本"),
            "should not include second char: {out:?}"
        );
        assert!(
            out.contains("[truncated, 18 bytes total]"),
            "output: {out:?}"
        );
    }

    #[test]
    fn truncate_marker_format_exact() {
        let s = "abcde"; // 5 bytes
        let out = truncate_with_marker(s, 3);
        // Should be "abc" + "…\n\n[truncated, 5 bytes total]"
        assert_eq!(out, "abc…\n\n[truncated, 5 bytes total]");
    }

    #[test]
    fn truncate_zero_cap_marker_only() {
        let out = truncate_with_marker("abc", 0);
        assert_eq!(out, "…\n\n[truncated, 3 bytes total]");
    }

    #[test]
    fn truncate_empty_string_any_cap_returns_empty() {
        assert_eq!(truncate_with_marker("", 0), "");
        assert_eq!(truncate_with_marker("", 10), "");
    }

    #[test]
    fn json_for_script_tag_escapes_closing_script() {
        let val = serde_json::json!({"text": "</script>"});
        let safe = json_for_script_tag(&val);
        assert!(
            !safe.contains("</script>"),
            "should escape closing tag: {safe}"
        );
        assert!(
            safe.contains("<\\/script>"),
            "should have escaped form: {safe}"
        );
    }
}
