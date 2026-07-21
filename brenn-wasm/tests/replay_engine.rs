// Integration tests for the iter-3 replay protection component.
//
// All tests load `brenn_replay.wasm` (the production component) via
// `ReplayComponent::load` and drive `check_raw_for_testing` directly.
// No HTTP, no axum, no tokio. Covers design §4.2 ACs.
//
// received_at is supplied as a fixed u64 ms value; no wall-clock dependency.

mod common;

use brenn_cal::ms_to_sent_at;
use brenn_wasm::{CheckInput, LAST_GRACE_MS, PRUNE_GATE_MODULUS, ReplayComponent, ReplayError};
use tempfile::NamedTempFile;

const REPLAY_ARTIFACT_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/target/components/brenn_replay.wasm"
);

// ±5-minute skew window in milliseconds, matching the component constant.
const SKEW_WINDOW_MS: u64 = 5 * 60 * 1000;

fn replay_artifact() -> std::path::PathBuf {
    std::path::PathBuf::from(REPLAY_ARTIFACT_PATH)
}

fn open_component() -> (NamedTempFile, ReplayComponent) {
    common::open_component(&replay_artifact())
}

/// Build a minimal valid phonebuddy envelope body as JSON bytes.
/// All three required fields (client_id, sent_at, nonce) must be present
/// and valid per envelope.rs constraints.
fn envelope(client_id: &str, sent_at: &str, nonce: &str) -> Vec<u8> {
    format!(
        r#"{{"schema_version":"1","kind":"test","client_id":"{client_id}","sent_at":"{sent_at}","nonce":"{nonce}","seq":1,"payload":{{}}}}"#
    )
    .into_bytes()
}

/// Build a CheckInput with a valid envelope where received_at == sent_at_ms
/// (zero skew). Monotonicity is assumed to be the caller's responsibility.
fn input_with_timestamp(
    client_id: &str,
    sent_at_ms: u64,
    nonce: &str,
    received_at: u64,
) -> CheckInput {
    let sent_at = ms_to_sent_at(sent_at_ms);
    CheckInput {
        headers: vec![],
        body: envelope(client_id, &sent_at, nonce),
        received_at,
        key_id: "primary".to_string(),
        endpoint_slug: "phonebuddy".to_string(),
    }
}

/// Build a CheckInput with sent_at == received_at (no skew) for the simple cases.
fn simple_input(client_id: &str, ts_ms: u64, nonce: &str) -> CheckInput {
    input_with_timestamp(client_id, ts_ms, nonce, ts_ms)
}

// ── AC-accept ──────────────────────────────────────────────────────────────────

#[test]
fn fresh_envelope_accepts() {
    let (_db, component) = open_component();
    let t: u64 = 1_748_000_000_000; // arbitrary fixed ms (2025-ish)
    let result = component
        .check_raw_for_testing(&simple_input("client1", t, "nonce00001"))
        .expect("no wasmtime error");
    assert!(
        result.is_ok(),
        "fresh envelope must be accepted: {result:?}"
    );
}

// ── AC-duplicate ───────────────────────────────────────────────────────────────

#[test]
fn duplicate_rejected() {
    let (_db, component) = open_component();
    let t: u64 = 1_748_000_000_000;
    let t2 = t + 1; // slightly later for monotonicity
    let sent_at2 = ms_to_sent_at(t2);

    // First accept.
    let r1 = component
        .check_raw_for_testing(&simple_input("client1", t, "nonce00001"))
        .expect("no wasmtime error");
    assert!(r1.is_ok(), "first accept must succeed: {r1:?}");

    // Replay: same nonce, advanced sent_at so monotonicity passes, received_at close enough.
    let replay_input = CheckInput {
        headers: vec![],
        body: envelope("client1", &sent_at2, "nonce00001"),
        received_at: t2,
        key_id: "primary".to_string(),
        endpoint_slug: "phonebuddy".to_string(),
    };
    let r2 = component
        .check_raw_for_testing(&replay_input)
        .expect("no wasmtime error");
    assert!(
        matches!(r2, Err(ReplayError::Duplicate)),
        "duplicate nonce must be rejected: {r2:?}"
    );
}

// ── AC-skew-past ───────────────────────────────────────────────────────────────

#[test]
fn sent_at_too_old_rejected() {
    let (_db, component) = open_component();
    let received_at: u64 = 1_748_000_000_000;
    // sent_at is 1 ms outside the ±5-minute window (too old).
    let sent_at_ms = received_at - SKEW_WINDOW_MS - 1;
    let input = input_with_timestamp("client1", sent_at_ms, "nonce00001", received_at);
    let result = component
        .check_raw_for_testing(&input)
        .expect("no wasmtime error");
    assert!(
        matches!(result, Err(ReplayError::TimestampOutOfWindow)),
        "sent_at too old must be rejected: {result:?}"
    );
}

#[test]
fn sent_at_within_skew_accepted() {
    // sent_at exactly 1 ms inside the ±5-minute window (boundary-inclusive).
    let (_db, component) = open_component();
    let received_at: u64 = 1_748_000_000_000;
    let sent_at_ms = received_at - SKEW_WINDOW_MS + 1;
    let input = input_with_timestamp("client1", sent_at_ms, "nonce00001", received_at);
    let result = component
        .check_raw_for_testing(&input)
        .expect("no wasmtime error");
    assert!(
        result.is_ok(),
        "sent_at within skew window must be accepted: {result:?}"
    );
}

// ── AC-skew-future ─────────────────────────────────────────────────────────────

#[test]
fn sent_at_too_new_rejected() {
    let (_db, component) = open_component();
    let received_at: u64 = 1_748_000_000_000;
    // sent_at is 1 ms outside the ±5-minute window (too new / future).
    let sent_at_ms = received_at + SKEW_WINDOW_MS + 1;
    let input = input_with_timestamp("client1", sent_at_ms, "nonce00001", received_at);
    let result = component
        .check_raw_for_testing(&input)
        .expect("no wasmtime error");
    assert!(
        matches!(result, Err(ReplayError::TimestampOutOfWindow)),
        "sent_at too new must be rejected: {result:?}"
    );
}

// ── AC-monotonicity ────────────────────────────────────────────────────────────

#[test]
fn monotonicity_lower_rejected() {
    let (_db, component) = open_component();
    let t1: u64 = 1_748_000_001_000;
    let t0: u64 = 1_748_000_000_000; // earlier

    // Accept at t1.
    let r1 = component
        .check_raw_for_testing(&simple_input("client1", t1, "nonce00001"))
        .expect("no wasmtime error");
    assert!(r1.is_ok(), "first accept must succeed: {r1:?}");

    // Try to accept at t0 (earlier than t1) — monotonicity violation.
    // received_at must be close to sent_at so skew passes: use t0 as received_at.
    // But sent_at t0 < stored last_sent_at t1 → monotonicity violation.
    let input = input_with_timestamp("client1", t0, "nonce00002", t0);
    let result = component
        .check_raw_for_testing(&input)
        .expect("no wasmtime error");
    assert!(
        matches!(result, Err(ReplayError::MonotonicityViolation)),
        "lower sent_at must be rejected: {result:?}"
    );
}

#[test]
fn monotonicity_equal_rejected() {
    let (_db, component) = open_component();
    let t: u64 = 1_748_000_000_000;

    // First accept at t.
    let r1 = component
        .check_raw_for_testing(&simple_input("client1", t, "nonce00001"))
        .expect("no wasmtime error");
    assert!(r1.is_ok(), "first accept must succeed: {r1:?}");

    // Second accept with same sent_at (equal, not strictly greater) — monotonicity violation.
    // Different nonce so it won't be a duplicate rejection.
    let input = simple_input("client1", t, "nonce00002");
    let result = component
        .check_raw_for_testing(&input)
        .expect("no wasmtime error");
    assert!(
        matches!(result, Err(ReplayError::MonotonicityViolation)),
        "equal sent_at must be rejected: {result:?}"
    );
}

// ── AC-malformed-envelope ──────────────────────────────────────────────────────

#[test]
fn malformed_envelope_returns_malformed_input() {
    let (_db, component) = open_component();
    let t: u64 = 1_748_000_000_000;
    // Missing required fields — not valid JSON envelope.
    let bad_input = CheckInput {
        headers: vec![],
        body: b"not json at all".to_vec(),
        received_at: t,
        key_id: "primary".to_string(),
        endpoint_slug: "phonebuddy".to_string(),
    };
    let result = component
        .check_raw_for_testing(&bad_input)
        .expect("no wasmtime error");
    assert!(
        matches!(result, Err(ReplayError::MalformedInput(_))),
        "malformed envelope must return MalformedInput: {result:?}"
    );
}

// ── AC-client-independence ─────────────────────────────────────────────────────

#[test]
fn two_clients_share_nonce_both_accepted() {
    let (_db, component) = open_component();
    let t: u64 = 1_748_000_000_000;
    // Both clients use the same nonce string — they must be independent.
    let r1 = component
        .check_raw_for_testing(&simple_input("client1", t, "shared00001"))
        .expect("no wasmtime error");
    assert!(r1.is_ok(), "client1 must be accepted: {r1:?}");

    let r2 = component
        .check_raw_for_testing(&simple_input("client2", t, "shared00001"))
        .expect("no wasmtime error");
    assert!(
        r2.is_ok(),
        "client2 with same nonce must be accepted (client-independence): {r2:?}"
    );
}

// ── AC-nonce-cap-fail-closed ───────────────────────────────────────────────────

#[test]
fn nonce_cap_hit_fails_closed() {
    let (_db, component) = open_component();
    // Use a base time well-within the current epoch so ms_to_sent_at yields valid dates.
    let base_ms: u64 = 1_748_000_000_000;

    // Insert 1024 nonces, all within the 5-minute window.
    // Each accept: received_at = base_ms + i ms, sent_at = same (zero skew, strictly monotonic).
    for i in 0u64..1024 {
        let ts = base_ms + i;
        let nonce = format!("capnonce{i:016}");
        let result = component
            .check_raw_for_testing(&simple_input("capper", ts, &nonce))
            .expect("no wasmtime error");
        assert!(result.is_ok(), "accept {i} must succeed: {result:?}");
    }

    // 1025th: received_at and sent_at still inside the window (1024 ms after base).
    // All prior entries have age ≤ 1024 ms < 5 min → still non-expired → cap = 1024 → fail.
    let ts_cap = base_ms + 1024;
    let cap_input = simple_input("capper", ts_cap, "capnonce_new00000");
    // NOTE: sent_at ts_cap must be strictly > last accepted sent_at (base_ms + 1023).
    // ts_cap = base_ms + 1024 > base_ms + 1023 → monotonicity passes.
    let result = component
        .check_raw_for_testing(&cap_input)
        .expect("no wasmtime error");
    assert!(
        matches!(result, Err(ReplayError::TooManyRequests)),
        "1025th non-expired request must return TooManyRequests: {result:?}"
    );

    // The cap-hit nonce must NOT have been inserted — store must still hold exactly 1024 entries.
    // Key layout: 1-byte kind tag + 8-byte big-endian sent_at_ms || nonce bytes (prefix_len=9).
    let nonce_ns = format!("phonebuddy-replay/nonce/{}", "capper");
    common::assert_scan_count_and_absent(
        component.kv_store_for_testing(),
        &nonce_ns,
        1024,
        9,
        b"capnonce_new00000",
    );
}

// ── AC-ttl-eviction ────────────────────────────────────────────────────────────

#[test]
fn ttl_eviction_after_skew_window() {
    let (_db, component) = open_component();
    let t1: u64 = 1_748_000_000_000;

    // Accept #1 at t1.
    let r1 = component
        .check_raw_for_testing(&simple_input("evict_client", t1, "nonce00001"))
        .expect("no wasmtime error");
    assert!(r1.is_ok(), "first accept must succeed: {r1:?}");

    // Accept #2 at t2 = t1 + SKEW_WINDOW_MS + 1 (entry 1 is now expired).
    // sent_at = t2 (strictly > t1, monotonicity passes, within skew of received_at).
    let t2 = t1 + SKEW_WINDOW_MS + 1;
    let r2 = component
        .check_raw_for_testing(&simple_input("evict_client", t2, "nonce00002"))
        .expect("no wasmtime error");
    assert!(
        r2.is_ok(),
        "second accept after TTL expiry must succeed: {r2:?}"
    );

    // Verify that the expired entry was actually deleted from the store.
    // After accept #2, exactly one nonce entry must remain: nonce00002.
    // The delete loop is directly pinned here (not just indirectly via cap-reset).
    let nonce_ns = format!("phonebuddy-replay/nonce/{}", "evict_client");
    let entries = component.kv_store_for_testing().scan_for_testing(&nonce_ns);
    assert_eq!(
        entries.len(),
        1,
        "nonce namespace must hold exactly 1 entry after TTL eviction; got {}: {entries:?}",
        entries.len()
    );
    let nonce_suffix = b"nonce00002";
    let has_nonce00002 = entries
        .iter()
        .any(|(k, _)| k.len() >= 9 && &k[k.len() - nonce_suffix.len()..] == nonce_suffix);
    assert!(
        has_nonce00002,
        "the surviving entry must be nonce00002; got {entries:?}"
    );
}

// ── AC-ttl-eviction-resets-cap ─────────────────────────────────────────────────

#[test]
fn ttl_eviction_then_cap_reset() {
    let (_db, component) = open_component();
    let base_ms: u64 = 1_748_000_000_000;

    // Fill cap to 1024 within the 5-minute window.
    for i in 0u64..1024 {
        let ts = base_ms + i;
        let nonce = format!("capnonce{i:016}");
        let result = component
            .check_raw_for_testing(&simple_input("reset_client", ts, &nonce))
            .expect("no wasmtime error");
        assert!(result.is_ok(), "accept {i} must succeed: {result:?}");
    }

    // Advance time past the skew window so all 1024 prior entries are TTL-expired.
    // Also ensure sent_at is strictly greater than the last accepted sent_at.
    let t_later = base_ms + SKEW_WINDOW_MS + 2000;
    let fresh_result = component
        .check_raw_for_testing(&simple_input("reset_client", t_later, "freshafter1111111"))
        .expect("no wasmtime error");
    assert!(
        fresh_result.is_ok(),
        "accept after TTL expiry must succeed (capacity reset by eviction): {fresh_result:?}"
    );
}

// ── AC-last-ns-prune ──────────────────────────────────────────────────────────

#[test]
fn last_ns_stale_row_pruned() {
    // AC1: stale `last` row for client_id_A is pruned when client_id_B's envelope
    // arrives at t1 + SKEW_WINDOW_MS + LAST_GRACE_MS + 64 (a prune-gate-open timestamp).
    let (_db, component) = open_component();
    let t1: u64 = 1_748_000_000_000;

    // Accept client_id_A at t1.
    let r1 = component
        .check_raw_for_testing(&simple_input("client-prune-A", t1, "nonce00001"))
        .expect("no wasmtime error");
    assert!(r1.is_ok(), "first accept must succeed: {r1:?}");

    // Accept client_id_B at t2 = t1 + SKEW_WINDOW_MS + LAST_GRACE_MS + PRUNE_GATE_MODULUS.
    // sent_at == received_at (zero skew), so skew check passes.
    // +PRUNE_GATE_MODULUS ensures t2 is a multiple of PRUNE_GATE_MODULUS, so the amortized prune
    // gate (rem_euclid(PRUNE_GATE_MODULUS)==0) opens.
    let t2 = t1 + SKEW_WINDOW_MS + LAST_GRACE_MS as u64 + PRUNE_GATE_MODULUS as u64;
    let r2 = component
        .check_raw_for_testing(&simple_input("client-prune-B", t2, "nonce00002"))
        .expect("no wasmtime error");
    assert!(r2.is_ok(), "second accept must succeed: {r2:?}");

    // After B's check, A's row must have been pruned (its sent_at is older than the cutoff).
    let last_entries = component
        .kv_store_for_testing()
        .scan_for_testing("phonebuddy-replay/last");
    let has_a = last_entries.iter().any(|(k, _)| k == b"client-prune-A");
    let has_b = last_entries.iter().any(|(k, _)| k == b"client-prune-B");
    assert!(
        !has_a,
        "stale row for client-prune-A must have been pruned; entries={last_entries:?}"
    );
    assert!(
        has_b,
        "fresh row for client-prune-B must be present; entries={last_entries:?}"
    );
}

#[test]
fn last_ns_boundary_row_retained() {
    // AC1 boundary: a row whose sent_at is exactly at the prune cutoff must NOT be pruned
    // (strict `<` comparison; equal is retained).
    //
    // Setup: accept client_A at t1. Then accept client_B at received_at = t1 + SKEW_WINDOW_MS + LAST_GRACE_MS.
    // The prune cutoff for client_B's call is received_at - SKEW_WINDOW_MS - LAST_GRACE_MS = t1.
    // client_A's stored sent_at == ms_to_sent_at(t1), and cutoff_str == ms_to_sent_at(t1).
    // Since val < cutoff_str is strict, client_A's row must survive (equal is not pruned).
    let (_db, component) = open_component();
    let t1: u64 = 1_748_000_000_000;

    // Accept client_A at t1.
    let r1 = component
        .check_raw_for_testing(&simple_input("client-boundary-A", t1, "nonce-bnd-001"))
        .expect("no wasmtime error");
    assert!(r1.is_ok(), "first accept must succeed: {r1:?}");

    // Accept client_B at exactly t1 + SKEW_WINDOW_MS + LAST_GRACE_MS.
    // Prune cutoff = t2 - SKEW_WINDOW_MS - LAST_GRACE_MS = t1. client_A's sent_at == t1 == cutoff.
    let t2 = t1 + SKEW_WINDOW_MS + LAST_GRACE_MS as u64;
    let r2 = component
        .check_raw_for_testing(&simple_input("client-boundary-B", t2, "nonce-bnd-002"))
        .expect("no wasmtime error");
    assert!(r2.is_ok(), "second accept must succeed: {r2:?}");

    // client_A's row is exactly at the cutoff; strict `<` means it must NOT be pruned.
    let last_entries = component
        .kv_store_for_testing()
        .scan_for_testing("phonebuddy-replay/last");
    let has_a = last_entries.iter().any(|(k, _)| k == b"client-boundary-A");
    assert!(
        has_a,
        "row exactly at cutoff must be retained (strict < comparison); entries={last_entries:?}"
    );
}

#[test]
fn last_ns_fresh_row_retained() {
    // AC2: a fresh `last` row (well within SKEW_WINDOW_MS) is NOT pruned.
    let (_db, component) = open_component();
    let t1: u64 = 1_748_000_000_000;

    // Accept client_id_A at t1.
    let r1 = component
        .check_raw_for_testing(&simple_input("client-fresh-A", t1, "nonce00001"))
        .expect("no wasmtime error");
    assert!(r1.is_ok(), "first accept must succeed: {r1:?}");

    // Accept client_id_B at t2 = t1 + PRUNE_GATE_MODULUS.
    // t2 is a multiple of PRUNE_GATE_MODULUS so the prune gate opens, ensuring prune actually
    // runs on this call. Cutoff = t2 - SKEW_WINDOW_MS - LAST_GRACE_MS = 1_747_999_640_064 < t1,
    // so A's row is not stale and must survive. Both rows are fresh; both must be retained.
    let t2 = t1 + PRUNE_GATE_MODULUS as u64;
    let r2 = component
        .check_raw_for_testing(&simple_input("client-fresh-B", t2, "nonce00002"))
        .expect("no wasmtime error");
    assert!(r2.is_ok(), "second accept must succeed: {r2:?}");

    // Both rows must still be present.
    let last_entries = component
        .kv_store_for_testing()
        .scan_for_testing("phonebuddy-replay/last");
    let has_a = last_entries.iter().any(|(k, _)| k == b"client-fresh-A");
    let has_b = last_entries.iter().any(|(k, _)| k == b"client-fresh-B");
    assert!(
        has_a,
        "fresh row for client-fresh-A must be retained; entries={last_entries:?}"
    );
    assert!(
        has_b,
        "fresh row for client-fresh-B must be retained; entries={last_entries:?}"
    );
}

#[test]
fn last_ns_self_row_present_after_accept() {
    // AC3: the current envelope's own `last` row must survive (self-skip by placement — prune
    // runs before the put, so the new row cannot be seen by prune).
    let (_db, component) = open_component();
    let t1: u64 = 1_748_000_000_000;

    let r1 = component
        .check_raw_for_testing(&simple_input("client-self", t1, "nonce00001"))
        .expect("no wasmtime error");
    assert!(r1.is_ok(), "accept must succeed: {r1:?}");

    let last_entries = component
        .kv_store_for_testing()
        .scan_for_testing("phonebuddy-replay/last");
    let self_row = last_entries.iter().find(|(k, _)| k == b"client-self");
    assert!(
        self_row.is_some(),
        "self row for client-self must be present after accept; entries={last_entries:?}"
    );
    let (_, val) = self_row.unwrap();
    let expected_val = ms_to_sent_at(t1);
    assert_eq!(
        val.as_slice(),
        expected_val.as_bytes(),
        "self row value must equal the sent_at of the accepted envelope"
    );
}

#[test]
fn last_ns_nonce_cap_hit_rolls_back_prune() {
    // AC5: prune deletes inside a tx that later fails (cap-hit) are rolled back.
    //
    // Strategy:
    // - Seed X and Y at t_old = t_now - 500_000 ms (expired from prune's perspective at t_now,
    //   but NOT expired during the cap-fill loop whose envelopes sit at t_now - SKEW_WINDOW_MS + 1).
    // - Fill nonce cap (1024 entries) for client_id_Z within a 5-minute window ending at t_now.
    // - Assert X and Y still present (cap-fill prunes did not remove them).
    // - Attempt the 1025th Z envelope (cap-hit) at t_now; its prune cutoff is
    //   t_now - SKEW_WINDOW_MS - LAST_GRACE_MS, which covers t_old → prune would delete X and Y.
    //   But the cap-hit causes rollback → X and Y must survive.
    let (_db, component) = open_component();

    // t_now is an arbitrary base well into the epoch.
    let t_now: u64 = 1_748_000_000_000;
    // t_old is 500_000 ms before t_now. This is:
    //   - < t_now - SKEW_WINDOW_MS - LAST_GRACE_MS (= t_now - 360_000), so expired at t_now.
    //   - > t_now - 2*SKEW_WINDOW_MS - LAST_GRACE_MS (= t_now - 660_000), so NOT expired at
    //     the cap-fill calls (which use received_at ≥ t_now - SKEW_WINDOW_MS + 1 = t_now - 299_999).
    //     Cap-fill prune cutoff = t_now - 299_999 - SKEW_WINDOW_MS - LAST_GRACE_MS
    //                           = t_now - 299_999 - 300_000 - 60_000 = t_now - 659_999.
    //     t_old = t_now - 500_000 > t_now - 659_999 → not expired during cap fill.
    //   The amortized gate fires on ~1/PRUNE_GATE_MODULUS of cap-fill calls; for the ~16 gate-open
    //   iterations the cutoff is still below t_old, so X and Y are not pruned.
    let t_old = t_now - 500_000;

    // Seed X and Y.
    let rx = component
        .check_raw_for_testing(&simple_input("client-prune-X", t_old, "nonce-x-01"))
        .expect("no wasmtime error");
    assert!(rx.is_ok(), "seed X must succeed: {rx:?}");
    let ry = component
        .check_raw_for_testing(&simple_input("client-prune-Y", t_old + 1, "nonce-y-01"))
        .expect("no wasmtime error");
    assert!(ry.is_ok(), "seed Y must succeed: {ry:?}");

    // Fill nonce cap for client_id_Z: 1024 envelopes, all strictly monotonic,
    // received_at = sent_at within the 5-minute window ending at t_now.
    // Use t_now - SKEW_WINDOW_MS + 1 as the base so all entries are non-expired at t_now.
    let cap_base = t_now - SKEW_WINDOW_MS + 1;
    for i in 0u64..1024 {
        let ts = cap_base + i;
        let nonce = format!("capnonce-Z-{i:016}");
        let result = component
            .check_raw_for_testing(&simple_input("client-cap-Z", ts, &nonce))
            .expect("no wasmtime error");
        assert!(result.is_ok(), "cap fill {i} must succeed: {result:?}");
    }

    // Mid-test assertion: X and Y must still be present (cap-fill prunes didn't remove them).
    let mid_entries = component
        .kv_store_for_testing()
        .scan_for_testing("phonebuddy-replay/last");
    let mid_has_x = mid_entries.iter().any(|(k, _)| k == b"client-prune-X");
    let mid_has_y = mid_entries.iter().any(|(k, _)| k == b"client-prune-Y");
    assert!(
        mid_has_x,
        "X must still be present before cap-hit attempt (not pruned by cap-fill); entries={mid_entries:?}"
    );
    assert!(
        mid_has_y,
        "Y must still be present before cap-hit attempt (not pruned by cap-fill); entries={mid_entries:?}"
    );

    // 1025th Z envelope at t_now: triggers cap-hit; prune would delete X and Y but rollback reverses it.
    let cap_hit_ts = t_now;
    let cap_hit_input = simple_input("client-cap-Z", cap_hit_ts, "capnonce-Z-overshoot-01");
    let cap_result = component
        .check_raw_for_testing(&cap_hit_input)
        .expect("no wasmtime error");
    assert!(
        matches!(cap_result, Err(ReplayError::TooManyRequests)),
        "1025th Z envelope must return TooManyRequests (cap-hit): {cap_result:?}"
    );

    // X and Y must still be present (prune rollback).
    let post_entries = component
        .kv_store_for_testing()
        .scan_for_testing("phonebuddy-replay/last");
    let post_has_x = post_entries.iter().any(|(k, _)| k == b"client-prune-X");
    let post_has_y = post_entries.iter().any(|(k, _)| k == b"client-prune-Y");
    assert!(
        post_has_x,
        "X must survive cap-hit rollback; entries={post_entries:?}"
    );
    assert!(
        post_has_y,
        "Y must survive cap-hit rollback; entries={post_entries:?}"
    );

    // Nonce namespace for client_id_Z must still have exactly 1024 entries (not 1025):
    // the cap-hit tx rolled back the nonce put as well as the prune deletes.
    let z_nonce_ns = "phonebuddy-replay/nonce/client-cap-Z";
    let z_nonces = component
        .kv_store_for_testing()
        .scan_for_testing(z_nonce_ns);
    assert_eq!(
        z_nonces.len(),
        1024,
        "nonce namespace for client-cap-Z must still have 1024 entries after cap-hit rollback; \
         got {}",
        z_nonces.len()
    );
}

#[test]
fn last_ns_bad_value_traps() {
    // AC: a non-24-byte value in the `last` namespace causes a wasm trap on the next accepted check.
    //
    // Seeds a malformed row directly via put_for_testing (skipping the component to avoid the
    // well-formed write path), then drives check_raw_for_testing which hits prune_last_ns and
    // asserts store integrity. The outer Err from check_raw_for_testing is the wasmtime trap.
    let (_db, component) = open_component();

    // Seed a malformed `last` row: value is 9 bytes, not 24.
    component.kv_store_for_testing().put_for_testing(
        "phonebuddy-replay/last",
        b"client-bad",
        b"not-24-bytes",
    );

    // Advance time past prune cutoff so the prune scan will encounter the malformed row.
    // received_at must be a multiple of PRUNE_GATE_MODULUS so the amortized prune gate
    // (rem_euclid(PRUNE_GATE_MODULUS)==0) opens.
    let t: u64 =
        1_748_000_000_000 + SKEW_WINDOW_MS + LAST_GRACE_MS as u64 + PRUNE_GATE_MODULUS as u64;
    let result = component.check_raw_for_testing(&simple_input("client-trigger", t, "trapnonce01"));

    // The assert inside prune_last_ns fires on the malformed value → wasmtime trap → outer Err.
    assert!(
        result.is_err(),
        "bad last-ns value must produce a wasmtime trap (outer Err); got: {result:?}"
    );
}

#[test]
fn last_ns_large_pre_seeded_no_panic() {
    // AC6: a `last` namespace with >= 4096 rows does not panic; subsequent calls drain expired rows.
    //
    // Seeded via direct KV writes (put_for_testing) to avoid the O(N²) cost of running prune
    // inside 4097 wasm check calls. The production check/prune path is exercised by the single
    // post-seed call below, which is what AC6 actually verifies.
    let (_db, component) = open_component();
    let t_old: u64 = 1_748_000_000_000;
    let t_old_str = ms_to_sent_at(t_old);

    // Seed 4097 rows directly into the `last` namespace. client_ids are lex-sorted
    // as "pre0000000000000000" .. "pre0000000000004096", all with value = ms_to_sent_at(t_old).
    let kv = component.kv_store_for_testing();
    for i in 0u64..4097 {
        let client_id = format!("pre{i:016}");
        kv.put_for_testing(
            "phonebuddy-replay/last",
            client_id.as_bytes(),
            t_old_str.as_bytes(),
        );
    }

    // Advance to t_now > t_old + SKEW_WINDOW_MS + LAST_GRACE_MS so all seeded rows are expired.
    // +PRUNE_GATE_MODULUS ensures t_now is a multiple of PRUNE_GATE_MODULUS so the amortized prune
    // gate (rem_euclid(PRUNE_GATE_MODULUS)==0) opens.
    let t_now = t_old + SKEW_WINDOW_MS + LAST_GRACE_MS as u64 + PRUNE_GATE_MODULUS as u64;

    // One more envelope from a fresh client at t_now — must not panic, and must be accepted.
    // This call's prune scans the first 4096 of the 4097 seeded rows (all expired), deletes them,
    // then inserts the new row for "client-post-seed".
    let result = component
        .check_raw_for_testing(&simple_input("client-post-seed", t_now, "postseed00001"))
        .expect("no wasmtime error");
    assert!(
        result.is_ok(),
        "post-seed envelope at t_now must be accepted: {result:?}"
    );

    // Seeded rows "pre0000000000000000" .. "pre0000000000004095" sort before "pre0000000000004096"
    // (the 4097th seeded row) lex-wise, so the scan covers the first 4096 of the 4097 seeded rows.
    // All 4096 are expired → deleted. "pre0000000000004096" survives (beyond the 4096-row scan cap).
    // "client-post-seed" is added. Final last namespace: exactly 2 rows.
    let last_entries = component
        .kv_store_for_testing()
        .scan_for_testing("phonebuddy-replay/last");
    let seeded_count = last_entries
        .iter()
        .filter(|(k, _)| k.starts_with(b"pre"))
        .count();
    assert!(
        seeded_count == 1,
        "exactly 1 seeded row must survive (pre0000000000004096, beyond scan cap); \
         got seeded_count={seeded_count}, entries.len()={}",
        last_entries.len()
    );
}

// ── AC-restart-durability ──────────────────────────────────────────────────────

#[test]
fn restart_persists_state() {
    let db = NamedTempFile::new().unwrap();
    let path = db.path().to_path_buf();
    let t: u64 = 1_748_000_000_000;
    let t2 = t + 1;

    // First component instance: accept a request.
    {
        let component = ReplayComponent::load(
            "phonebuddy",
            &replay_artifact(),
            &path,
            brenn_wasm::store::DEFAULT_MAX_PAGE_COUNT,
            std::collections::HashMap::new(),
        );
        let r = component
            .check_raw_for_testing(&simple_input("persist_client", t, "nonce00001"))
            .expect("no wasmtime error");
        assert!(r.is_ok(), "initial accept must succeed: {r:?}");
        // component dropped here
    }

    // Second component instance: load the same store path.
    {
        let component = ReplayComponent::load(
            "phonebuddy",
            &replay_artifact(),
            &path,
            brenn_wasm::store::DEFAULT_MAX_PAGE_COUNT,
            std::collections::HashMap::new(),
        );
        // Try to replay the same nonce at a slightly later time (monotonicity passes).
        let replay_input = CheckInput {
            headers: vec![],
            body: envelope("persist_client", &ms_to_sent_at(t2), "nonce00001"),
            received_at: t2,
            key_id: "primary".to_string(),
            endpoint_slug: "phonebuddy".to_string(),
        };
        let r2 = component
            .check_raw_for_testing(&replay_input)
            .expect("no wasmtime error");
        assert!(
            matches!(r2, Err(ReplayError::Duplicate)),
            "nonce replayed after restart must be rejected: {r2:?}"
        );
    }
}
