use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

/// Constant-time equality check for two byte slices.
///
/// Returns `true` iff both slices have the same length and identical contents.
/// `subtle::ConstantTimeEq` answers `false` on a length mismatch without
/// comparing contents, so no byte position leaks through timing; the length
/// itself does. For fixed-width inputs (digests, MACs) that is nothing: both
/// sides are the same width by construction. For variable-length secrets it is
/// an oracle on the stored side's length — use [`eq_secret`] there, with the
/// caveat documented on it.
///
/// The single constant-time comparison for the crate: a timing-hygiene audit
/// or a `subtle` API migration has one site to visit.
pub fn ct_eq_bytes(a: &[u8], b: &[u8]) -> bool {
    a.ct_eq(b).into()
}

/// SHA-256 of `bytes`, the digest form secrets are compared in.
pub fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut out = [0u8; 32];
    out.copy_from_slice(&Sha256::digest(bytes));
    out
}

/// Constant-time equality for two variable-length secrets, compared as digests.
///
/// Hashes both sides and compares the two 32-byte results with [`ct_eq_bytes`],
/// which removes the exact-length short-circuit: a prober can no longer find
/// the stored secret's length by varying the presented one until the comparison
/// switches from an early return to a full byte loop.
///
/// **What it does not buy.** Because both sides are hashed at compare time, a
/// residual timing difference remains whenever the two inputs occupy a
/// different number of SHA-256 blocks (64-byte classes) — the compression
/// function runs once more for the longer input. That narrows the leak from an
/// exact length to a block class; it does not remove it. Full independence
/// requires pre-hashing the stored side once, so that only the presented side's
/// length is in play — which is what
/// [`RemoteToken`](crate::messaging::remote::RemoteToken) does.
pub fn eq_secret(a: &[u8], b: &[u8]) -> bool {
    ct_eq_bytes(&sha256(a), &sha256(b))
}

/// Compute HMAC-SHA256 over `data` using `key`. Returns 32 output bytes.
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

/// Compute HMAC-SHA256 over multiple data slices fed in sequence, using `key`.
/// Equivalent to `hmac_sha256(key, &parts.concat())` but without allocating a
/// temporary buffer. Returns 32 output bytes.
pub fn hmac_sha256_parts(key: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC can take key of any size");
    for part in parts {
        mac.update(part);
    }
    mac.finalize().into_bytes().into()
}

/// Compute HMAC-SHA256 over `data` using `key` and return the result as a
/// lowercase hex string.
pub fn hmac_sha256_hex(key: &[u8], data: &[u8]) -> String {
    hex::encode(hmac_sha256(key, data))
}

/// Verify a multi-part HMAC-SHA256 signature in constant time.
///
/// Returns `true` iff `sig` is exactly 32 bytes and matches
/// `HMAC-SHA256(key, parts[0] || parts[1] || … || parts[N-1])`.
///
/// The length check is deliberately non-constant-time (length is not secret).
/// The 32-byte comparison is constant-time via [`ct_eq_bytes`]. This is the
/// single constant-time HMAC gate: every HMAC verification in the tree,
/// including [`hmac_sha256_verify`], routes through here.
pub fn hmac_sha256_parts_verify(key: &[u8], parts: &[&[u8]], sig: &[u8]) -> bool {
    if sig.len() != 32 {
        return false;
    }
    let expected = hmac_sha256_parts(key, parts);
    ct_eq_bytes(expected.as_slice(), sig)
}

/// Verify a raw HMAC-SHA256 signature in constant time.
///
/// Returns `true` iff `sig` is exactly 32 bytes and matches `HMAC-SHA256(key,
/// data)`. The length short-circuit is deliberately non-constant-time (length
/// is not secret); the 32-byte comparison is constant-time.
pub fn hmac_sha256_verify(key: &[u8], data: &[u8], sig: &[u8]) -> bool {
    hmac_sha256_parts_verify(key, &[data], sig)
}

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

/// Maximum number of commit oneline entries reported in a `repo_sync:pulled`
/// event and preserved through collapse/merge.
pub const ONELINE_CAP: usize = 10;

/// Apply the [`ONELINE_CAP`] truncation rule in place. When `lines.len()`
/// exceeds the cap, keep the first `ONELINE_CAP - 1` entries and replace the
/// tail with a single `"... N more (older)"` sentinel.
///
/// Shared by the producer of a commit-oneline list (the managed-clone pull) and
/// the consumer that collapses several pulls into one event, so both sides
/// bound the list the same way.
pub fn cap_oneline(lines: &mut Vec<String>) {
    if lines.len() > ONELINE_CAP {
        let extra = lines.len() - (ONELINE_CAP - 1);
        lines.truncate(ONELINE_CAP - 1);
        lines.push(format!("... {extra} more (older)"));
    }
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
    fn cap_oneline_truncates_at_cap() {
        let mut lines: Vec<String> = (0..=ONELINE_CAP).map(|i| format!("commit {i}")).collect();
        assert_eq!(lines.len(), ONELINE_CAP + 1);
        cap_oneline(&mut lines);
        assert_eq!(lines.len(), ONELINE_CAP);
        assert_eq!(
            lines[ONELINE_CAP - 2],
            format!("commit {}", ONELINE_CAP - 2)
        );
        assert_eq!(lines.last().unwrap(), "... 2 more (older)");
    }

    #[test]
    fn cap_oneline_sentinel_counts_every_dropped_entry() {
        let mut lines: Vec<String> = (0..25).map(|i| format!("commit {i}")).collect();
        cap_oneline(&mut lines);
        assert_eq!(lines.len(), ONELINE_CAP);
        let kept: Vec<String> = (0..ONELINE_CAP - 1)
            .map(|i| format!("commit {i}"))
            .collect();
        assert_eq!(lines[..ONELINE_CAP - 1], kept[..]);
        // 25 entries in, 9 kept: the sentinel accounts for the other 16.
        assert_eq!(lines.last().unwrap(), "... 16 more (older)");
    }

    #[test]
    fn cap_oneline_at_exactly_cap_is_noop() {
        let mut lines: Vec<String> = (0..ONELINE_CAP).map(|i| format!("commit {i}")).collect();
        let before = lines.clone();
        cap_oneline(&mut lines);
        assert_eq!(lines, before);
    }

    #[test]
    fn ct_eq_bytes_agrees_with_plain_equality() {
        assert!(ct_eq_bytes(b"abc", b"abc"));
        assert!(!ct_eq_bytes(b"abc", b"abd"));
        assert!(!ct_eq_bytes(b"abc", b"abcd"));
        assert!(ct_eq_bytes(b"", b""));
    }

    #[test]
    fn eq_secret_matches_equal_inputs_of_any_length() {
        for secret in [
            "".to_string(),
            "x".to_string(),
            "a".repeat(55),
            "a".repeat(64),
            "a".repeat(200),
        ] {
            assert!(eq_secret(secret.as_bytes(), secret.as_bytes()));
            assert!(!eq_secret(
                secret.as_bytes(),
                format!("{secret}x").as_bytes()
            ));
        }
    }

    #[test]
    fn eq_secret_rejects_a_prefix_of_the_secret() {
        // The case a length-comparing implementation would answer without
        // comparing contents: a strict prefix of the stored secret.
        assert!(!eq_secret(b"s3cret-token", b"s3cret"));
        assert!(!eq_secret(b"s3cret", b"s3cret-token"));
    }

    #[test]
    fn sha256_is_the_known_digest_of_the_empty_string() {
        assert_eq!(
            hex::encode(sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn hmac_sha256_hex_produces_lowercase_hex_of_hmac() {
        let key = b"test-key";
        let data = b"test-data";
        let raw = hmac_sha256(key, data);
        let expected_hex = hex::encode(raw);
        assert_eq!(hmac_sha256_hex(key, data), expected_hex);
        let h = hmac_sha256_hex(key, data);
        assert_eq!(h.len(), 64);
        assert!(
            h.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase())
        );
    }

    #[test]
    fn hmac_sha256_verify_correct_returns_true() {
        let key = b"verify-key";
        let data = b"verify-data";
        let sig = hmac_sha256(key, data);
        assert!(hmac_sha256_verify(key, data, &sig));
    }

    #[test]
    fn hmac_sha256_verify_wrong_key_returns_false() {
        let key = b"verify-key";
        let wrong_key = b"wrong-key!!";
        let data = b"verify-data";
        let sig = hmac_sha256(key, data);
        assert!(!hmac_sha256_verify(wrong_key, data, &sig));
    }

    #[test]
    fn hmac_sha256_verify_wrong_length_returns_false() {
        let key = b"verify-key";
        let data = b"verify-data";
        let short_sig = &hmac_sha256(key, data)[..16];
        assert!(!hmac_sha256_verify(key, data, short_sig));
        let long_sig = [hmac_sha256(key, data).as_slice(), b"\x00"].concat();
        assert!(!hmac_sha256_verify(key, data, &long_sig));
    }

    #[test]
    fn hmac_sha256_parts_equivalent_to_concat() {
        let key = b"test-key-for-parts-equivalence";
        let a: &[u8] = b"prefix-data/";
        let b: &[u8] = b"middle-chunk";
        let c: &[u8] = b":suffix";
        let parts_result = hmac_sha256_parts(key, &[a, b, c]);
        let concat: Vec<u8> = [a, b, c].concat();
        let concat_result = hmac_sha256(key, &concat);
        assert_eq!(
            parts_result, concat_result,
            "hmac_sha256_parts must be equivalent to hmac_sha256 on concatenated input"
        );
    }

    /// `hmac_sha256_parts_verify(key, &[data], sig)` must agree with
    /// `hmac_sha256_verify(key, data, sig)` for both matching and non-matching
    /// signatures. Pins the forwarding invariant.
    #[test]
    fn hmac_sha256_parts_verify_single_part_equivalent() {
        let key = b"gate-key";
        let data = b"gate-data";
        let sig_match = hmac_sha256(key, data);
        let sig_wrong = hmac_sha256(b"other-key", data);

        assert!(
            hmac_sha256_parts_verify(key, &[data], &sig_match),
            "parts_verify(single part) must return true on match"
        );
        assert!(
            hmac_sha256_verify(key, data, &sig_match),
            "hmac_sha256_verify must return true on match"
        );

        assert!(
            !hmac_sha256_parts_verify(key, &[data], &sig_wrong),
            "parts_verify(single part) must return false on mismatch"
        );
        assert!(
            !hmac_sha256_verify(key, data, &sig_wrong),
            "hmac_sha256_verify must return false on mismatch"
        );
    }

    /// `hmac_sha256_parts_verify` with multiple parts must match the
    /// equivalent concatenation, reject a wrong key, and enforce the 32-byte
    /// length guard directly at the primitive level.
    #[test]
    fn hmac_sha256_parts_verify_multi_part() {
        let key = b"multi-key";
        let a: &[u8] = b"part-a";
        let b_part: &[u8] = b"part-b";
        let c: &[u8] = b"part-c";

        let correct_sig = hmac_sha256_parts(key, &[a, b_part, c]);
        assert!(
            hmac_sha256_parts_verify(key, &[a, b_part, c], &correct_sig),
            "must return true on multi-part match"
        );

        let wrong_sig = hmac_sha256_parts(b"wrong-key", &[a, b_part, c]);
        assert!(
            !hmac_sha256_parts_verify(key, &[a, b_part, c], &wrong_sig),
            "must return false with wrong key"
        );

        let short_sig = &correct_sig[..16];
        assert!(
            !hmac_sha256_parts_verify(key, &[a, b_part, c], short_sig),
            "must return false for sig shorter than 32 bytes"
        );

        let mut long_sig = correct_sig.to_vec();
        long_sig.push(0x00);
        assert!(
            !hmac_sha256_parts_verify(key, &[a, b_part, c], &long_sig),
            "must return false for sig longer than 32 bytes"
        );
    }

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
