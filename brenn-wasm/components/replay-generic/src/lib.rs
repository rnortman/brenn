// Generic body-agnostic replay-protection component.
//
// Implements `check(input: CheckInput) -> result<_, replay-error>` for
// any `hmac-timestamped-body` + plain-text endpoint. The dedup identity is
// derived from the `x-brenn-push-signature` header, not the body — so the
// component works on arbitrary (non-JSON) bodies.
//
// Algorithm:
//   1. Extract dedup key from x-brenn-push-signature header.
//   2. BEGIN IMMEDIATE transaction.
//   3. Scan dedup namespace; expire stale entries; check for duplicate signature.
//   4. Cap check (TooManyRequests on overflow).
//   5. Insert new entry; commit.
//
// See design §3 for full specification.

use bindings::brenn::replay::config;
use bindings::brenn::replay::store;
use bindings::brenn::replay::types::{CheckInput, ReplayError};
use bindings::Guest;

#[allow(dead_code, clippy::all)]
mod bindings;

/// Read the dedup protection window from the host-injected config key `brenn.max-skew-secs`.
///
/// Why 2×: the signature layer accepts a timestamp `t` whenever
/// `|server_now - t| <= max_skew_secs`. A captured `(t, body, sig)` triple is therefore
/// signature-valid across a *received_at* band of width 2 × max_skew_secs. The dedup entry
/// is keyed on `received_at` (server time), so the memory window must span the full
/// 2 × max_skew_secs to prevent the entry from expiring while the triple is still
/// signature-valid. An `unreachable` trap on the first request means this component was
/// paired with a skew-less scheme (i.e. `brenn.max-skew-secs` was not injected by the host).
fn read_window_ms() -> u64 {
    let skew_secs: u64 = config::get("brenn.max-skew-secs")
        .unwrap_or_else(|| {
            panic!(
                "brenn.max-skew-secs not set — replay-generic requires a timestamped signature scheme"
            )
        })
        .parse()
        .unwrap_or_else(|e| panic!("brenn.max-skew-secs is not a valid u64: {e}"));
    skew_secs
        .checked_mul(2)
        .and_then(|v| v.checked_mul(1000))
        .unwrap_or_else(|| panic!("brenn.max-skew-secs overflow computing 2 * skew * 1000"))
}

/// Maximum non-expired dedup entries allowed before returning TooManyRequests.
/// Must be strictly less than the host scan trap (4096) so cap fires as 429 (not 500).
/// Value matches the existing replay component (NONCE_CAP_N = 1024, lib.rs:38).
const CAP: usize = 1024;

/// The header name carrying the per-request dedup identity.
/// This is the HMAC signature header as configured in the endpoint TOML (§4).
/// The signature layer also uses this header for auth; a request missing it is
/// rejected with 401 before the component runs. The guard below is therefore
/// a defensive check against an impossible host state (design §3.2 step 1).
const SIG_HEADER: &str = "x-brenn-push-signature";

/// Dedup namespace. Per-component fixed string; the store file is already
/// per-endpoint (unique store_path per endpoint config), so no further
/// namespacing by endpoint_slug is required.
const NS: &str = "replay-generic/sigs";

/// Encode a dedup-entry key: 8-byte big-endian received_at_ms || signature bytes.
/// Lexicographic order = chronological order on received_at_ms (time-sorted scan).
fn entry_key(received_at_ms: u64, sig_bytes: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(8 + sig_bytes.len());
    k.extend_from_slice(&received_at_ms.to_be_bytes());
    k.extend_from_slice(sig_bytes);
    k
}

/// Extract the received_at_ms prefix from an entry key.
/// Panics if the key is shorter than 8 bytes — store integrity violation.
fn key_received_at(key: &[u8]) -> u64 {
    assert!(
        key.len() >= 8,
        "entry key too short (len={}) — store integrity violation",
        key.len()
    );
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&key[..8]);
    u64::from_be_bytes(arr)
}

/// Extract the signature suffix from an entry key (bytes after the 8-byte prefix).
fn key_sig(key: &[u8]) -> &[u8] {
    assert!(
        key.len() >= 8,
        "entry key too short for sig extraction (len={}) — store integrity violation",
        key.len()
    );
    &key[8..]
}

struct Component;

impl Guest for Component {
    fn check(input: CheckInput) -> Result<(), ReplayError> {
        // Read the dedup window from config. Panics (anonymous trap) if the key is
        // absent or unparseable — means this component was paired with a skew-less scheme.
        // config::get is a host import; the map is process-lifetime-stable, but the guest
        // has no persistent globals between calls (fresh instantiation each check), so we
        // read it here each invocation rather than caching.
        let window_ms = read_window_ms();

        // Step 1: Extract dedup identity from x-brenn-push-signature header.
        // The body is intentionally ignored (B2: body-agnostic).
        // errhandling-1: distinguish absent header from empty-valued header.
        let sig_value_raw = match input.headers.iter().find(|h| h.name == SIG_HEADER) {
            None => {
                // Defensive guard: this path is unreachable in practice because the
                // signature layer rejects requests lacking this header before the
                // component runs. See design §3.2 step 1.
                return Err(ReplayError::MalformedInput(format!(
                    "absent {SIG_HEADER} header"
                )));
            }
            Some(h) if h.value.is_empty() => {
                return Err(ReplayError::MalformedInput(format!(
                    "empty {SIG_HEADER} header value"
                )));
            }
            Some(h) => &h.value,
        };

        // Normalize to lowercase so hex-case variants of the same MAC map to
        // the same dedup key. The signature layer accepts both upper- and
        // lower-case hex (hex::decode is case-insensitive); without normalization
        // a re-cased signature would verify but yield a distinct dedup key,
        // allowing replay within the skew window. (security-1)
        let sig_normalized = sig_value_raw.to_ascii_lowercase();
        let sig_bytes = sig_normalized.as_bytes();
        let received_at_ms = input.received_at;

        // Step 2: BEGIN IMMEDIATE — acquires write lock upfront (no SQLITE_BUSY on writes).
        let tx = store::begin().unwrap_or_else(|e| panic!("store::begin failed: {e}"));

        // Step 3: Scan dedup namespace; expire stale entries; check for duplicate.
        // TODO(replay-generic-bounded-scan): the design calls for a bounded range
        // scan starting at (received_at_ms - window_ms).to_be_bytes() to avoid
        // re-reading unconditionally-expired entries. The current unbounded scan
        // matches the existing replay component's pattern and is bounded by CAP=1024,
        // so the cost is acceptable at expected push volumes; fix if push rates grow.
        let pairs = tx
            .scan(NS, &[], None, 0)
            .unwrap_or_else(|e| panic!("store::scan({NS}) failed: {e}"));

        // Host scan trap guard: if the namespace exceeds MAX_SCAN_LIMIT (4096),
        // entries are inconsistent. This should never fire if CAP < 4096.
        if pairs.len() >= 4096 {
            panic!(
                "dedup namespace exceeded MAX_SCAN_LIMIT=4096 (got {}) — \
                 store integrity violation; CAP guard may have failed",
                pairs.len()
            );
        }

        let mut to_delete: Vec<Vec<u8>> = Vec::new();
        let mut non_expired_count: usize = 0;

        for (key, _val) in &pairs {
            let entry_ms = key_received_at(key);
            // Age distance: absolute value for NTP step-back safety
            // (both operands are u64; abs_diff has identical unsigned semantics).
            let age_abs = received_at_ms.abs_diff(entry_ms);

            if age_abs > window_ms {
                // Expired — queue for deletion.
                to_delete.push(key.clone());
            } else {
                // Non-expired — check for duplicate signature.
                if key_sig(key) == sig_bytes {
                    // rollback() returns () per generated bindings (bindings.rs:974) —
                    // not a Result, so no error to handle. (errhandling-2 verified)
                    tx.rollback();
                    return Err(ReplayError::Duplicate);
                }
                non_expired_count += 1;
            }
        }

        // Step 4: Cap check — fail closed before inserting.
        if non_expired_count >= CAP {
            // rollback() returns () per generated bindings (bindings.rs:974). (errhandling-2)
            tx.rollback();
            return Err(ReplayError::TooManyRequests);
        }

        // Prune expired entries.
        for key in &to_delete {
            tx.delete(NS, key)
                .unwrap_or_else(|e| panic!("store::delete({NS}, key={key:?}) failed: {e}"));
        }

        // Step 5: Insert new entry (empty value — key encodes all needed info).
        let new_key = entry_key(received_at_ms, sig_bytes);
        match tx.put(NS, &new_key, &[]) {
            Ok(()) => {}
            Err(store::StoreError::QuotaExceeded) => {
                // Host-enforced per-store size cap reached. Roll back and fail
                // closed as TooManyRequests (design §2.D, §4.3).
                tx.rollback();
                return Err(ReplayError::TooManyRequests);
            }
            Err(e) => panic!("store::put({NS}) failed: {e}"),
        }

        tx.commit()
            .unwrap_or_else(|e| panic!("store::commit failed: {e}"));

        Ok(())
    }
}

bindings::export!(Component with_types_in bindings);

// ── Unit tests (native, no WASM roundtrip) ────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_key_encodes_ms_big_endian_then_sig() {
        let ms: u64 = 0x0102030405060708;
        let sig = b"v1=abcdef";
        let key = entry_key(ms, sig);
        assert_eq!(&key[..8], &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
        assert_eq!(&key[8..], sig);
    }

    #[test]
    fn key_received_at_round_trips() {
        let ms: u64 = 1_749_200_000_000;
        let key = entry_key(ms, b"sig");
        assert_eq!(key_received_at(&key), ms);
    }

    #[test]
    fn key_sig_round_trips() {
        let sig = b"v1=deadbeef";
        let key = entry_key(0, sig);
        assert_eq!(key_sig(&key), sig);
    }

    #[test]
    fn key_ordering_is_chronological() {
        // Earlier ms → lexicographically smaller key (big-endian prefix).
        let k1 = entry_key(1_000, b"sig");
        let k2 = entry_key(2_000, b"sig");
        assert!(k1 < k2, "earlier received_at must produce smaller key");
    }

    #[test]
    fn cap_below_scan_trap() {
        // CAP must be strictly less than the host scan trap (4096).
        assert!(CAP < 4096, "CAP must be < 4096 to ensure 429 fires before 500");
    }
}
