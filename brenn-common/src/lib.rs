//! Leaf micro-crate for tiny shared items needed on both sides of the
//! `brenn-wasm` / `brenn-lib` split (neither crate depends on the other, and no
//! other crate is shared between them).
//!
//! **Charter: zero dependencies.** This crate holds only constants and
//! dependency-free pure functions. Anything that needs a dependency does not
//! belong here — put it in a crate that can carry the weight. The zero-deps rule
//! is what keeps this from drifting into a dumping ground.

/// Appended when [`sanitize_untrusted_str`] drops input. `"…"` (3 bytes) plus
/// `"(truncated)"` (11 bytes) = 14 bytes.
pub const TRUNCATION_MARKER: &str = "…(truncated)";

/// SQLite default page size. The store sets no `page_size` pragma, so SQLite
/// always uses its default of 4096. Used by both the store layer (`brenn-wasm`)
/// and config page-count arithmetic (`brenn-lib`).
pub const PAGE_SIZE: u64 = 4096;

/// Default cap (bytes) for an untrusted string rendered into a host log field
/// or alert body via [`sanitize_untrusted_str`]. Untrusted here means CC
/// output, WASM guest output, or broker-derived text (posture B3): cap it so a
/// hostile input cannot bloat the log stream that feeds alerting. Output is
/// bounded at `MAX_LOGGED_UNTRUSTED_BYTES + TRUNCATION_MARKER.len()` per field.
/// Signal paths with a documented reason to diverge declare their own cap (see
/// `brenn-wasm`'s `PROCESSOR_MAX_*` and `surface-proto`'s alert caps) — this
/// constant is the default posture, not a mandate.
pub const MAX_LOGGED_UNTRUSTED_BYTES: usize = 256;

/// Sanitize an untrusted string (browser input, WASM guest output, subprocess
/// output — hostile until proven boring) before logging, alerting, or
/// persisting.
///
/// Streams the input a char at a time, applying [`char::escape_debug`] to each
/// (escaping control characters to prevent log-line injection, forged
/// `key=value` pairs, and ANSI-escape injection into host log/alert pipelines),
/// and **bounds the output**: at most `max_bytes` bytes of escaped content, plus
/// [`TRUNCATION_MARKER`] iff any input char was dropped.
///
/// # Contract
///
/// - **Output bound:** the returned string is at most
///   `max_bytes + TRUNCATION_MARKER.len()` bytes. The marker rides outside the
///   `max_bytes` budget, so the named `*_BYTES` caps at call sites mean what they
///   say, modulo the fixed 14-byte marker.
/// - **Escape-unit atomicity:** an escape sequence is never split. The char
///   whose escaped form would overflow the budget is rolled back whole, so the
///   output never ends in a partial escape like `\u{`.
/// - **Marker iff dropped:** the marker is appended exactly when at least one
///   input char did not fit. `max_bytes == 0` with non-empty input returns the
///   bare marker (signals "there was something here" rather than a silent `""`).
/// - **Identity for clean input:** input whose escaped form fits within
///   `max_bytes` is returned exactly as `escape_debug` would produce it, with no
///   marker — the overwhelmingly common case.
///
/// # Deliberate strengthening
///
/// Per-char [`char::escape_debug`] escapes *all* grapheme-extend (combining)
/// characters, whereas `str::escape_debug` escapes only a leading one. Mid-string
/// combining characters in guest/browser text therefore render as `\u{...}` here.
/// For a sanitizer feeding log lines and pager titles, escaping
/// Unicode-spoofing-capable characters everywhere is the safer behavior.
///
/// # Marker forgery
///
/// Input may contain a literal `…(truncated)` (`…` is printable and passes
/// `escape_debug` untouched), so the marker is not a trustworthy signal — these
/// are display-only sinks and this is accepted.
pub fn sanitize_untrusted_str(raw: &str, max_bytes: usize) -> String {
    // Clean input (the common case) escapes to ≈`raw.len()` bytes; the cap keeps a
    // hostile giant input from pre-allocating megabytes.
    let mut out = String::with_capacity(raw.len().min(max_bytes) + TRUNCATION_MARKER.len());
    for c in raw.chars() {
        let start = out.len();
        out.extend(c.escape_debug());
        if out.len() > max_bytes {
            out.truncate(start);
            out.push_str(TRUNCATION_MARKER);
            return out;
        }
    }
    out
}

#[cfg(test)]
mod sanitize_tests {
    use super::{TRUNCATION_MARKER, sanitize_untrusted_str};

    #[test]
    fn empty_string() {
        assert_eq!(sanitize_untrusted_str("", 100), "");
    }

    #[test]
    fn below_cap_no_escape_needed() {
        assert_eq!(sanitize_untrusted_str("hello", 100), "hello");
    }

    #[test]
    fn exact_cap_no_truncation() {
        let s = "a".repeat(10);
        assert_eq!(sanitize_untrusted_str(&s, 10), s);
    }

    #[test]
    fn one_over_cap_ascii_boundary() {
        // 11 bytes, cap 10 — the 11th 'a' overflows and is rolled back, marker appended.
        let s = "a".repeat(11);
        assert_eq!(
            sanitize_untrusted_str(&s, 10),
            format!("{}{TRUNCATION_MARKER}", "a".repeat(10))
        );
    }

    #[test]
    fn multi_byte_char_straddles_cap() {
        // 9 ASCII 'a's + 'é' (printable, 2 bytes) = 11 bytes escaped. Cap 10: the 'é'
        // overflows and is rolled back whole (never split), marker appended.
        let mut s = "a".repeat(9);
        s.push('é');
        assert_eq!(
            sanitize_untrusted_str(&s, 10),
            format!("{}{TRUNCATION_MARKER}", "a".repeat(9))
        );
    }

    #[test]
    fn multi_byte_char_at_offset_zero() {
        // 4-byte printable char at offset 0, cap 3 — overflows immediately, bare marker.
        let s = "𐍈rest"; // U+10348, 4 bytes
        assert_eq!(sanitize_untrusted_str(s, 3), TRUNCATION_MARKER);
    }

    #[test]
    fn max_bytes_zero() {
        // Non-empty input, cap 0 — bare marker, not a silent "".
        assert_eq!(sanitize_untrusted_str("hello", 0), TRUNCATION_MARKER);
    }

    #[test]
    fn control_chars_escaped() {
        // '\n', '\x1b', '\x00' must be escape_debug'd — no raw control chars.
        let result = sanitize_untrusted_str("a\nb\x1b[31mc\x00d", 100);
        assert!(!result.contains('\n'), "raw newline must be escaped");
        assert!(!result.contains('\x1b'), "raw ESC must be escaped");
        assert!(!result.contains('\x00'), "raw NUL must be escaped");
        assert!(result.contains(r"\n"), r"'\n' escape sequence must appear");
        assert!(
            result.contains(r"\u{1b}"),
            "ESC escape sequence must appear"
        );
        assert!(result.contains(r"\0"), "NUL escape sequence must appear");
    }

    #[test]
    fn pure_control_char_string() {
        // 4096 bytes of '\x01' — each escapes to `\u{1}` (6 bytes), ~6× expansion.
        // The load-bearing assertion: output stays bounded (before the fix this
        // produced ~20 KB), and no raw control char survives.
        let s = "\x01".repeat(4096);
        let result = sanitize_untrusted_str(&s, 4096);
        assert!(
            !result.contains('\x01'),
            "all control chars must be escaped"
        );
        assert!(
            result.len() <= 4096 + TRUNCATION_MARKER.len(),
            "output must be bounded to max_bytes + marker, got {}",
            result.len()
        );
    }

    #[test]
    fn marker_present_iff_truncated() {
        // Fits exactly: no marker.
        assert!(!sanitize_untrusted_str("abcde", 5).contains(TRUNCATION_MARKER));
        // One over: marker present.
        assert!(sanitize_untrusted_str("abcdef", 5).ends_with(TRUNCATION_MARKER));
    }

    #[test]
    fn escape_unit_is_atomic() {
        // "ab" then '\x1b' (escapes to `\u{1b}`, 6 bytes). Cap 4 lands inside the
        // escape sequence: it must be rolled back whole, never left as a partial `\u{`.
        let result = sanitize_untrusted_str("ab\x1b", 4);
        assert_eq!(result, format!("ab{TRUNCATION_MARKER}"));
        assert!(
            !result.contains(r"\u{"),
            "partial escape must not survive: {result}"
        );
    }

    #[test]
    fn printable_multibyte_counted_at_byte_length() {
        // 'é' is printable (passes escape_debug untouched) and 2 bytes. Cap 10 fits
        // exactly 5 of them (10 bytes); the 6th overflows.
        let s = "é".repeat(6);
        assert_eq!(
            sanitize_untrusted_str(&s, 10),
            format!("{}{TRUNCATION_MARKER}", "é".repeat(5))
        );
    }
}
