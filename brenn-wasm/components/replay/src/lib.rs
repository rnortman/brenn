// Phonebuddy envelope replay-protection component.
//
// Implements `check(input: check-input) -> result<_, replay-error>` for
// phonebuddy envelope replay protection. Algorithm: envelope parse/validate,
// ±5-minute skew check (pre-transaction), then monotonicity + nonce-TTL-eviction
// + N-cap-abuse-signal inside a single BEGIN IMMEDIATE transaction.
//
// See design §2.2, §5.2, §5.3 for full specification.

use bindings::brenn::replay::store;
use bindings::brenn::replay::types::{CheckInput, ReplayError};
use bindings::Guest;
use brenn_cal::{days_from_epoch, days_in_month};

#[allow(dead_code, clippy::all)]
mod bindings;

// ±5-minute skew window, in milliseconds. This IS the nonce TTL.
// Both incoming-envelope check (step 2) and stored-nonce expiry (step 5) use
// this single constant. Rationale: design §2.2 constants, notes-design-user.md directive 3.
const SKEW_WINDOW_MS: i64 = 5 * 60 * 1000;

// Grace period added on top of SKEW_WINDOW_MS when computing the `last` namespace prune cutoff.
// A `last` row is functionally dead once older than SKEW_WINDOW_MS; 60 s of grace ensures rows
// written by a just-accepted envelope survive long enough for any in-flight envelope from the
// same client_id to land while monotonicity is still meaningful. Must be small and positive.
const LAST_GRACE_MS: i64 = 60 * 1000; // 60 s

// Amortization factor for the `last` namespace prune scan.
// Prune runs when `received_at_ms.rem_euclid(PRUNE_GATE_MODULUS) == 0`,
// i.e. on approximately 1/PRUNE_GATE_MODULUS of accepted checks.
const PRUNE_GATE_MODULUS: i64 = 64;

// Abuse-signal cap on non-expired stored nonces per client_id.
// Hitting this cap means one client_id produced 1024 valid envelopes within 5 minutes —
// far above legitimate phonebuddy scale. Failing closed on cap-hit; surfaced to fail2ban
// via the TooManyRequests typed variant.
const NONCE_CAP_N: usize = 1024;

// ── Envelope parsing ──────────────────────────────────────────────────────────

/// Validated, parsed phonebuddy envelope fields.
#[derive(Debug)]
struct Envelope {
    client_id: String,
    sent_at: String,
    nonce: String,
    sent_at_ms: i64,
}

/// Typed subset of the phonebuddy envelope: only the three fields we need.
/// Unknown fields (kind, schema_version, seq, payload, …) are silently ignored.
#[derive(serde::Deserialize)]
struct EnvelopeRaw {
    client_id: String,
    sent_at: String,
    nonce: String,
}

/// Parse and validate the phonebuddy envelope from body bytes.
/// Validation rules from phonebuddy/protocol/src/envelope.rs:50-108.
/// Returns `Err(ReplayError::MalformedInput)` on any parse or validation failure.
fn parse_envelope(body: &[u8]) -> Result<Envelope, ReplayError> {
    // serde_json::from_slice handles UTF-8 validation internally; no pre-scan needed.
    // Only client_id, sent_at, nonce are deserialized; unused fields are skipped.
    let raw: EnvelopeRaw = serde_json::from_slice(body)
        .map_err(|e| ReplayError::MalformedInput(format!("json parse: {e}")))?;

    let EnvelopeRaw {
        client_id,
        sent_at,
        nonce,
    } = raw;

    // Validate per envelope.rs constraints.
    validate_client_id(&client_id)?;
    validate_sent_at(&sent_at)?;
    validate_nonce(&nonce)?;

    let sent_at_ms = parse_sent_at_ms(&sent_at).ok_or_else(|| {
        ReplayError::MalformedInput(
            "sent_at is not a valid ms-precision UTC RFC3339 timestamp".into(),
        )
    })?;

    Ok(Envelope {
        client_id,
        sent_at,
        nonce,
        sent_at_ms,
    })
}

// Regex pattern for client_id: ^[A-Za-z0-9._\-]{1,64}$
// Plus: must not be ".", "..", or start with "." (filesystem-safe rules from envelope.rs).
fn validate_client_id(s: &str) -> Result<(), ReplayError> {
    if s.is_empty() || s.len() > 64 {
        return Err(ReplayError::MalformedInput(
            "client_id length must be 1-64".into(),
        ));
    }
    for b in s.bytes() {
        if !matches!(b, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'-') {
            return Err(ReplayError::MalformedInput(
                "client_id contains invalid character".into(),
            ));
        }
    }
    check_filesystem_safe(s, "client_id")
}

// Regex pattern for nonce: ^[A-Za-z0-9._\-]{8,64}$
// Plus filesystem-safe rules.
fn validate_nonce(s: &str) -> Result<(), ReplayError> {
    if s.len() < 8 || s.len() > 64 {
        return Err(ReplayError::MalformedInput(
            "nonce length must be 8-64".into(),
        ));
    }
    for b in s.bytes() {
        if !matches!(b, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'-') {
            return Err(ReplayError::MalformedInput(
                "nonce contains invalid character".into(),
            ));
        }
    }
    check_filesystem_safe(s, "nonce")
}

// Regex pattern for sent_at: ^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$
fn validate_sent_at(s: &str) -> Result<(), ReplayError> {
    // Length: "2026-05-17T12:34:56.789Z" = 24 chars
    if s.len() != 24 {
        return Err(ReplayError::MalformedInput("sent_at wrong length".into()));
    }
    let b = s.as_bytes();
    // Digit positions: 0-3, 5-6, 8-9, 11-12, 14-15, 17-18, 20-22
    // Separator positions: 4='-', 7='-', 10='T', 13=':', 16=':', 19='.', 23='Z'
    if !b[0..4].iter().all(|c| c.is_ascii_digit())
        || b[4] != b'-'
        || !b[5..7].iter().all(|c| c.is_ascii_digit())
        || b[7] != b'-'
        || !b[8..10].iter().all(|c| c.is_ascii_digit())
        || b[10] != b'T'
        || !b[11..13].iter().all(|c| c.is_ascii_digit())
        || b[13] != b':'
        || !b[14..16].iter().all(|c| c.is_ascii_digit())
        || b[16] != b':'
        || !b[17..19].iter().all(|c| c.is_ascii_digit())
        || b[19] != b'.'
        || !b[20..23].iter().all(|c| c.is_ascii_digit())
        || b[23] != b'Z'
    {
        return Err(ReplayError::MalformedInput(
            "sent_at does not match expected format".into(),
        ));
    }
    Ok(())
}

fn check_filesystem_safe(s: &str, field: &str) -> Result<(), ReplayError> {
    if s == "." || s == ".." || s.starts_with('.') {
        return Err(ReplayError::MalformedInput(format!(
            "{field} fails filesystem-safe check"
        )));
    }
    Ok(())
}

// ── sent_at → i64 ms-since-epoch ─────────────────────────────────────────────

/// Parse a fixed-width RFC3339 ms-precision UTC timestamp string into milliseconds
/// since Unix epoch. Returns None if any field is out of range.
///
/// Input guaranteed to be exactly `\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z`
/// by the caller (validate_sent_at + extract). Bespoke parser to stay WASI-free
/// (chrono pulls std, which imports wasi:* on wasm32-wasip2).
///
/// Edge cases covered by unit tests:
/// - 1970-01-01T00:00:00.000Z → 0
/// - year-2000 century-leap (2000-02-29 valid)
/// - 1900-02-29 invalid (century-non-leap)
/// - 2024-02-29 valid (4-year-leap)
/// - 2025-02-29 invalid (non-leap)
/// - 2024-12-31T23:59:59.999Z is one ms before 2025-01-01T00:00:00.000Z
pub fn parse_sent_at_ms(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    let year = parse_u16_4(&b[0..4])?;
    let month = parse_u8_2(&b[5..7])?;
    let day = parse_u8_2(&b[8..10])?;
    let hour = parse_u8_2(&b[11..13])?;
    let min = parse_u8_2(&b[14..16])?;
    let sec = parse_u8_2(&b[17..19])?;
    let ms = parse_u16_3(&b[20..23])?;

    // Validate field ranges.
    if month == 0 || month > 12 {
        return None;
    }
    if day == 0 {
        return None;
    }
    if hour > 23 || min > 59 || sec > 59 {
        return None;
    }

    let days_in_m = days_in_month(year, month)?;
    if day > days_in_m {
        return None;
    }

    // Days since 1970-01-01 (total function; no ? needed).
    let days = days_from_epoch(year, month, day);

    let total_ms = days * 86_400_000i64
        + hour as i64 * 3_600_000i64
        + min as i64 * 60_000i64
        + sec as i64 * 1_000i64
        + ms as i64;

    Some(total_ms)
}

fn parse_u8_2(b: &[u8]) -> Option<u32> {
    if b.len() != 2 {
        return None;
    }
    let hi = b[0].wrapping_sub(b'0');
    let lo = b[1].wrapping_sub(b'0');
    if hi > 9 || lo > 9 {
        return None;
    }
    Some(hi as u32 * 10 + lo as u32)
}

fn parse_u16_3(b: &[u8]) -> Option<u32> {
    if b.len() != 3 {
        return None;
    }
    let d0 = b[0].wrapping_sub(b'0');
    let d1 = b[1].wrapping_sub(b'0');
    let d2 = b[2].wrapping_sub(b'0');
    if d0 > 9 || d1 > 9 || d2 > 9 {
        return None;
    }
    Some(d0 as u32 * 100 + d1 as u32 * 10 + d2 as u32)
}

fn parse_u16_4(b: &[u8]) -> Option<u32> {
    if b.len() != 4 {
        return None;
    }
    let d0 = b[0].wrapping_sub(b'0');
    let d1 = b[1].wrapping_sub(b'0');
    let d2 = b[2].wrapping_sub(b'0');
    let d3 = b[3].wrapping_sub(b'0');
    if d0 > 9 || d1 > 9 || d2 > 9 || d3 > 9 {
        return None;
    }
    Some(d0 as u32 * 1000 + d1 as u32 * 100 + d2 as u32 * 10 + d3 as u32)
}

// ── KV encoding helpers ───────────────────────────────────────────────────────

fn last_ns() -> &'static str {
    "phonebuddy-replay/last"
}

/// Prune the `last` namespace: delete any row whose stored `sent_at` value
/// compares older than `received_at_ms - SKEW_WINDOW_MS - LAST_GRACE_MS`.
///
/// Must run inside the existing write transaction, after the monotonicity read
/// and before the `last` put for the current envelope. See design §2.3–§2.4.
fn prune_last_ns(tx: &store::Transaction, received_at_ms: i64) {
    let cutoff_ms_i64 = received_at_ms - SKEW_WINDOW_MS - LAST_GRACE_MS;
    // Negative cutoff implies received_at < SKEW_WINDOW_MS + LAST_GRACE_MS (~6 min past epoch).
    // The host derives received_at from SystemTime (u64 ms cast to i64 at check site); a negative
    // value here indicates either a host bug or a u64 value > i64::MAX (year 292,277,026 AD —
    // not reachable in practice). Either way, fail-fast with the actual value for diagnosis.
    assert!(
        cutoff_ms_i64 >= 0,
        "prune_last_ns: negative cutoff ({cutoff_ms_i64}) — received_at_ms ({received_at_ms}) \
         is before epoch + SKEW_WINDOW_MS + LAST_GRACE_MS; host bug or year > 292M AD (u64 > i64::MAX)"
    );

    // No >= 4096 panic guard here: a pre-seeded oversized `last` namespace must drain
    // incrementally, not DoS. See design §2.3 step 4 and requirements §Behavior constraint 6.
    //
    // limit=0 means "no limit" per host contract (clamped to MAX_SCAN_LIMIT=4096 by the host).
    // See store.rs: the component receives no signal when the result is truncated at 4096,
    // so the namespace drains incrementally across calls rather than in one pass when it
    // exceeds 4096 rows.
    let pairs = tx
        .scan(last_ns(), &[], None, 0)
        .unwrap_or_else(|e| panic!("store::scan({}) failed: {e}", last_ns()));

    if pairs.is_empty() {
        return;
    }

    let cutoff_str = brenn_cal::ms_to_sent_at(cutoff_ms_i64 as u64);

    let mut to_delete: Vec<Vec<u8>> = Vec::new();
    for (key, val) in &pairs {
        assert!(
            val.len() == 24,
            "store integrity violation: last namespace value has wrong length {} (expected 24) \
             for key={key:?}, val={val:?}",
            val.len()
        );
        // Lex order = chronological order for canonical 24-byte UTC RFC3339 (design §2.3 step 5).
        if val.as_slice() < cutoff_str.as_bytes() {
            to_delete.push(key.clone());
        }
    }

    for key in &to_delete {
        tx.delete(last_ns(), key)
            .unwrap_or_else(|e| panic!("store::delete({}, key={key:?}) failed: {e}", last_ns()));
    }
}

fn nonce_ns(client_id: &str) -> String {
    format!("phonebuddy-replay/nonce/{client_id}")
}

/// Encode a nonce key: 8-byte big-endian received_at_ms || b':' || nonce bytes.
/// Lexicographic order = chronological order on received_at_ms (design §5.2.3).
///
/// `received_at_ms` is stored as `u64` for correct big-endian lex ordering.
/// The expiry comparison in `check` casts the stored value back to `i64`; for
/// values > `i64::MAX` (year 292,277,026 AD) the cast wraps and the entry is
/// classified as expired. This is not reachable in practice. The `CheckInput`
/// type's `received_at: u64` field is set by the host from `SystemTime`, which
/// itself is bounded by the OS clock (typically a 64-bit signed seconds value;
/// ms overflow requires year ≫ 9999).
fn nonce_key(received_at_ms: u64, nonce: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(9 + nonce.len());
    k.extend_from_slice(&received_at_ms.to_be_bytes());
    k.push(b':');
    k.extend_from_slice(nonce.as_bytes());
    k
}

/// Extract the received_at_ms prefix from a nonce key. Panics if key < 8 bytes
/// (keys we wrote always have the full prefix; truncation would indicate a bug).
fn nonce_key_received_at(key: &[u8]) -> u64 {
    let key_hex: String = key.iter().map(|b| format!("{b:02x}")).collect();
    assert!(
        key.len() >= 8,
        "nonce key too short (len={}, bytes=0x{}) — store integrity violation",
        key.len(),
        key_hex
    );
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&key[..8]);
    u64::from_be_bytes(arr)
}

/// Extract the nonce suffix from a nonce key (bytes after the 9-byte prefix: 8 + b':').
fn nonce_key_nonce(key: &[u8]) -> &[u8] {
    let key_hex: String = key.iter().map(|b| format!("{b:02x}")).collect();
    assert!(
        key.len() >= 9,
        "nonce key too short for nonce extraction (len={}, bytes=0x{}) — store integrity violation",
        key.len(),
        key_hex
    );
    &key[9..]
}

// ── Main algorithm ────────────────────────────────────────────────────────────

struct Component;

impl Guest for Component {
    fn check(input: CheckInput) -> Result<(), ReplayError> {
        let env = parse_envelope(&input.body)?;

        // Skew check is pre-transaction: fast reject without acquiring the write lock.
        let received_at_ms = input.received_at as i64;
        let skew = received_at_ms - env.sent_at_ms;
        if skew.abs() > SKEW_WINDOW_MS {
            return Err(ReplayError::TimestampOutOfWindow);
        }

        // BEGIN IMMEDIATE acquires the write lock upfront, preventing SQLITE_BUSY
        // on the subsequent writes inside a read-then-write transaction.
        let tx = store::begin()
            .unwrap_or_else(|e| panic!("store::begin failed for client_id={}: {e}", env.client_id));

        let last_key = env.client_id.as_bytes().to_vec();
        let last_val = tx.get(last_ns(), &last_key).unwrap_or_else(|e| {
            panic!(
                "store::get({}, client_id={}) failed: {e}",
                last_ns(),
                env.client_id
            )
        });

        if let Some(last_bytes) = last_val {
            // Compare sent_at strings lexicographically.
            // Both are canonical fixed-width forms; lex order = chronological order.
            // Phonebuddy rule: sent_at must be strictly greater than last_sent_at
            // (equal is rejected, matching phonebuddy replay.rs:213 `<=`).
            let last_str = core::str::from_utf8(&last_bytes).unwrap_or_else(|_| {
                panic!("non-utf8 last_sent_at in store — store integrity violation")
            });
            if env.sent_at.as_str() <= last_str {
                tx.rollback();
                return Err(ReplayError::MonotonicityViolation);
            }
        }

        // Prune stale `last` rows. Runs after the monotonicity read (which uses unpruned state)
        // and before the `last` put (so the current envelope's row cannot be pruned by this call).
        //
        // Note: monotonicity-violation envelopes roll back *before* reaching this point, so prune
        // does not run on that path. Stale rows from clients that only ever trigger monotonicity
        // violations are not collected until a successful accept arrives. This is an accepted
        // limitation — any successful accept from any client_id resumes pruning the full namespace.
        //
        // Amortize the O(N) `last` namespace scan: prune on ~1/64 of accepted checks.
        // Predicate is a pure function of received_at_ms — deterministic across replay.
        // Silent-client rows linger up to ~64× longer; they are functionally inert
        // (skew check rejects any envelope old enough for its `last` row to be deletable).
        if received_at_ms.rem_euclid(PRUNE_GATE_MODULUS) == 0 {
            prune_last_ns(&tx, received_at_ms);
        }

        let ns = nonce_ns(&env.client_id);
        // Scan all entries for this client (no start/end bounds; namespace isolates the client).
        let pairs = tx.scan(&ns, &[], None, 0).unwrap_or_else(|e| {
            panic!(
                "store::scan(ns={}, client_id={}) failed: {e}",
                ns, env.client_id
            )
        });

        // If the scan returned the full MAX_SCAN_LIMIT (4096), the namespace is
        // larger than the algorithm can make sound decisions on. Trap (fail-fast).
        // This indicates either a bug (cap-hit not failing closed) or misconfiguration.
        // Design §2.11.
        if pairs.len() >= 4096 {
            panic!(
                "nonce namespace for client_id={} exceeded MAX_SCAN_LIMIT=4096 (got {}) — \
                 store integrity violation; cap-hit guard may have failed",
                env.client_id,
                pairs.len()
            );
        }

        let received_at_u64 = input.received_at;
        let mut to_delete: Vec<Vec<u8>> = Vec::new();
        let mut non_expired_count: usize = 0;

        for (key, _val) in &pairs {
            let entry_received_at = nonce_key_received_at(key);
            // Use absolute-value distance for NTP step-back safety (design §3.8).
            let age_abs = (received_at_ms - entry_received_at as i64).unsigned_abs();
            if age_abs > SKEW_WINDOW_MS as u64 {
                // Expired — queue for deletion.
                to_delete.push(key.clone());
            } else {
                // Non-expired — check for duplicate nonce.
                let entry_nonce = nonce_key_nonce(key);
                if entry_nonce == env.nonce.as_bytes() {
                    tx.rollback();
                    return Err(ReplayError::Duplicate);
                }
                non_expired_count += 1;
            }
        }

        // Cap check: if non-expired count >= NONCE_CAP_N, fail closed.
        if non_expired_count >= NONCE_CAP_N {
            tx.rollback();
            return Err(ReplayError::TooManyRequests);
        }

        for key in &to_delete {
            let key_hex: String = key.iter().map(|b| format!("{b:02x}")).collect();
            tx.delete(&ns, key).unwrap_or_else(|e| {
                panic!(
                    "store::delete(ns={}, key=0x{}, client_id={}) failed: {e}",
                    ns, key_hex, env.client_id
                )
            });
        }

        match tx.put(last_ns(), &last_key, env.sent_at.as_bytes()) {
            Ok(()) => {}
            Err(store::StoreError::QuotaExceeded) => {
                // Host-enforced per-store size cap reached. Roll back and fail
                // closed as TooManyRequests (design §2.D, §4.3).
                tx.rollback();
                return Err(ReplayError::TooManyRequests);
            }
            Err(e) => panic!("store::put({}, client_id={}) failed: {e}", last_ns(), env.client_id),
        }

        let new_nonce_key = nonce_key(received_at_u64, &env.nonce);
        match tx.put(&ns, &new_nonce_key, &[]) {
            Ok(()) => {}
            Err(store::StoreError::QuotaExceeded) => {
                // Host-enforced per-store size cap reached. Roll back and fail
                // closed as TooManyRequests (design §2.D, §4.3).
                tx.rollback();
                return Err(ReplayError::TooManyRequests);
            }
            Err(e) => panic!(
                "store::put(ns={}, client_id={}, nonce={}) failed: {e}",
                ns, env.client_id, env.nonce
            ),
        }

        tx.commit().unwrap_or_else(|e| {
            panic!(
                "store::commit failed for client_id={}, nonce={}: {e}",
                env.client_id, env.nonce
            )
        });

        Ok(())
    }
}

bindings::export!(Component with_types_in bindings);

// ── Unit tests (native, no WASM roundtrip) ────────────────────────────────────
// parse_sent_at_ms edge cases from design §4.2.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_zero() {
        assert_eq!(parse_sent_at_ms("1970-01-01T00:00:00.000Z"), Some(0));
    }

    #[test]
    fn year_2000_century_leap_feb29() {
        // 2000 is divisible by 400 → leap year.
        assert!(parse_sent_at_ms("2000-02-29T00:00:00.000Z").is_some());
    }

    #[test]
    fn year_1900_century_non_leap_feb29_invalid() {
        // 1900 is divisible by 100 but not 400 → not a leap year.
        assert!(parse_sent_at_ms("1900-02-29T00:00:00.000Z").is_none());
    }

    #[test]
    fn year_2024_leap_feb29() {
        // 2024 is divisible by 4 but not 100 → leap year.
        assert!(parse_sent_at_ms("2024-02-29T00:00:00.000Z").is_some());
    }

    #[test]
    fn year_2025_non_leap_feb29_invalid() {
        assert!(parse_sent_at_ms("2025-02-29T00:00:00.000Z").is_none());
    }

    #[test]
    fn year_end_boundary_2024() {
        // 2024-12-31T23:59:59.999Z must be exactly 1 ms before 2025-01-01T00:00:00.000Z.
        let end_of_year = parse_sent_at_ms("2024-12-31T23:59:59.999Z").unwrap();
        let start_of_next = parse_sent_at_ms("2025-01-01T00:00:00.000Z").unwrap();
        assert_eq!(start_of_next - end_of_year, 1);
    }

    #[test]
    fn invalid_month_13() {
        assert!(parse_sent_at_ms("2024-13-01T00:00:00.000Z").is_none());
    }

    #[test]
    fn invalid_day_zero() {
        assert!(parse_sent_at_ms("2024-01-00T00:00:00.000Z").is_none());
    }

    #[test]
    fn invalid_hour_24() {
        assert!(parse_sent_at_ms("2024-01-01T24:00:00.000Z").is_none());
    }

    #[test]
    fn validate_client_id_dot_prefix_rejected() {
        assert!(validate_client_id(".hidden").is_err());
    }

    #[test]
    fn validate_client_id_dot_rejected() {
        assert!(validate_client_id(".").is_err());
    }

    #[test]
    fn validate_client_id_dotdot_rejected() {
        assert!(validate_client_id("..").is_err());
    }

    #[test]
    fn validate_nonce_too_short() {
        assert!(validate_nonce("short").is_err()); // <8 chars
    }

    #[test]
    fn validate_sent_at_wrong_format() {
        assert!(validate_sent_at("2024-01-01T00:00:00Z").is_err()); // missing ms
    }

    #[test]
    fn validate_sent_at_correct() {
        assert!(validate_sent_at("2024-01-01T00:00:00.000Z").is_ok());
    }

    // days_from_epoch correctness — Hinnant formula must be correct for pre-epoch
    // dates (negative results) and a date several years before epoch.
    #[test]
    fn days_from_epoch_pre_epoch_one_day() {
        // 1969-12-31 is one day before epoch.
        assert_eq!(days_from_epoch(1969, 12, 31), -1);
    }

    #[test]
    fn days_from_epoch_pre_epoch_several_years() {
        // 1960-01-01: from 1970-01-01, that is 10 years back.
        // 1960-1969 span includes leap years 1960, 1964, 1968 → 3 leap years.
        // Days = -(7*365 + 3*366) = -(2555 + 1098) = -3653.
        assert_eq!(days_from_epoch(1960, 1, 1), -3653);
    }

    #[test]
    fn days_from_epoch_epoch() {
        assert_eq!(days_from_epoch(1970, 1, 1), 0);
    }

    // parse_envelope: valid JSON non-object (e.g. array) must return MalformedInput.
    #[test]
    fn parse_envelope_json_array_returns_malformed_input() {
        let result = parse_envelope(b"[]");
        assert!(
            matches!(result, Err(ReplayError::MalformedInput(_))),
            "expected MalformedInput for JSON array, got {result:?}"
        );
    }

    // parse_envelope: JSON object missing a required field must return MalformedInput.
    #[test]
    fn parse_envelope_missing_field_returns_malformed_input() {
        let result = parse_envelope(br#"{"client_id":"x","sent_at":"2024-01-01T00:00:00.000Z"}"#);
        assert!(
            matches!(result, Err(ReplayError::MalformedInput(_))),
            "expected MalformedInput for missing nonce, got {result:?}"
        );
    }

    #[test]
    fn last_ns_prune_gate_predicate() {
        // Gate open on multiples of PRUNE_GATE_MODULUS (including 0).
        assert_eq!((0i64).rem_euclid(PRUNE_GATE_MODULUS), 0);
        assert_eq!(PRUNE_GATE_MODULUS.rem_euclid(PRUNE_GATE_MODULUS), 0);
        assert_eq!((PRUNE_GATE_MODULUS * 12345).rem_euclid(PRUNE_GATE_MODULUS), 0);
        // Gate closed on non-multiples.
        assert_ne!((1i64).rem_euclid(PRUNE_GATE_MODULUS), 0);
        assert_ne!((PRUNE_GATE_MODULUS - 1).rem_euclid(PRUNE_GATE_MODULUS), 0);
        assert_ne!((PRUNE_GATE_MODULUS * 12345 + 17).rem_euclid(PRUNE_GATE_MODULUS), 0);
        // Negative-domain well-defined (rem_euclid, not %): predicate stays a pure function.
        assert_eq!((-PRUNE_GATE_MODULUS).rem_euclid(PRUNE_GATE_MODULUS), 0);
        assert_ne!((-1i64).rem_euclid(PRUNE_GATE_MODULUS), 0);
    }
}
