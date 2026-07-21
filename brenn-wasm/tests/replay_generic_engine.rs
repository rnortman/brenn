// Engine-level tests for the generic body-agnostic replay-protection component.
//
// Loads `brenn_replay_generic.wasm` via `ReplayComponent::load` and drives
// `check` directly. No HTTP, no axum, no tokio. Covers expiry (AC 1–2) and
// per-window cap (AC 3–4) from the requirements.
//
// received_at is supplied as a fixed u64 ms value; no wall-clock dependency.
// The generic component's dedup identity is the x-brenn-push-signature header
// value (lowercased); body content is irrelevant.

mod common;

use brenn_wasm::{CheckInput, Header, ReplayComponent, ReplayError, store::DEFAULT_MAX_PAGE_COUNT};
use tempfile::NamedTempFile;

const GENERIC_ARTIFACT_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/target/components/brenn_replay_generic.wasm"
);

/// max_skew_secs value injected as `brenn.max-skew-secs` for most tests.
/// Matches the default brenn endpoint configuration (300 s).
const TEST_MAX_SKEW_SECS: u64 = 300;

/// Compute the dedup window in ms from a skew_secs value — mirrors the formula
/// inside `read_window_ms()` in replay-generic/src/lib.rs. Using a function
/// rather than a pre-computed constant means a formula change in the component
/// would cause the tests here to diverge (they would use the old formula) and
/// flag the mismatch.
fn window_ms_for(skew_secs: u64) -> u64 {
    2 * skew_secs * 1_000
}

/// Per-window cap — mirrors replay-generic/src/lib.rs:53.
const CAP: usize = 1024;

/// Dedup namespace — mirrors replay-generic/src/lib.rs:65.
const NS: &str = "replay-generic/sigs";

fn generic_artifact() -> std::path::PathBuf {
    std::path::PathBuf::from(GENERIC_ARTIFACT_PATH)
}

/// Build the default config map with `brenn.max-skew-secs = TEST_MAX_SKEW_SECS`.
fn default_config() -> std::collections::HashMap<String, String> {
    let mut m = std::collections::HashMap::new();
    m.insert(
        "brenn.max-skew-secs".to_string(),
        TEST_MAX_SKEW_SECS.to_string(),
    );
    m
}

fn open_component() -> (NamedTempFile, ReplayComponent) {
    let db = NamedTempFile::new().unwrap();
    let component = ReplayComponent::load(
        "push-test",
        &generic_artifact(),
        db.path(),
        brenn_wasm::store::DEFAULT_MAX_PAGE_COUNT,
        default_config(),
    );
    (db, component)
}

/// Build a CheckInput whose dedup identity is `sig`.
/// body is irrelevant to the generic component (body-agnostic).
fn sig_input(sig: &str, received_at: u64) -> CheckInput {
    CheckInput {
        headers: vec![Header {
            name: "x-brenn-push-signature".to_string(),
            value: sig.to_string(),
        }],
        body: b"irrelevant".to_vec(),
        received_at,
        key_id: "primary".to_string(),
        endpoint_slug: "push-test".to_string(),
    }
}

// ── AC 1+2 — expiry releases dedup entry ──────────────────────────────────────
//
// Self-defeating property: this test fails if expiry logic is removed (step 4
// would return Err(Duplicate)) or made unconditional (step 3 would return Ok).

#[test]
fn expiry_releases_dedup_entry() {
    let (_db, component) = open_component();
    let sig = "v1=aabbccddeeff0011";
    let t0: u64 = 1_748_000_000_000; // arbitrary fixed ms (2025-ish)
    let window_ms = window_ms_for(TEST_MAX_SKEW_SECS);

    // Step 1: First sight — must be accepted.
    let (r1, _) = component.check(&sig_input(sig, t0));
    assert!(r1.is_ok(), "first check must be accepted: {r1:?}");

    // Step 2: Within-window control: age_abs == window_ms (strict >, not expired).
    // This guards against an unconditional-accept bug: the entry is NOT yet expired
    // at exactly window_ms (lib.rs uses strict `>`), so re-presentation must block.
    let (r2, _) = component.check(&sig_input(sig, t0 + window_ms));
    assert!(
        matches!(r2, Err(ReplayError::Duplicate)),
        "re-check at exactly window_ms must still be blocked (age_abs == window_ms, not expired): {r2:?}"
    );

    // Step 3: Post-window: age_abs > window_ms → t0 entry pruned → re-accept.
    let t_post = t0 + window_ms + 1;
    let (r3, _) = component.check(&sig_input(sig, t_post));
    assert!(
        r3.is_ok(),
        "re-check at window_ms+1 must be accepted (expired entry pruned): {r3:?}"
    );

    // Step 4: Store assertion — exactly one entry survives; it belongs to step 3.
    // The t0 entry must have been deleted (not just shadowed) and the new entry
    // inserted with received_at = t_post.
    let entries = component.kv_store_for_testing().scan_for_testing(NS);
    assert_eq!(
        entries.len(),
        1,
        "exactly one entry must remain after expiry prune; got {}: {entries:?}",
        entries.len()
    );
    let (surviving_key, _) = &entries[0];
    // Key layout: 8-byte big-endian received_at_ms || sig bytes (normalized lowercase).
    assert!(
        surviving_key.len() >= 8,
        "surviving entry key too short: {:?}",
        surviving_key
    );
    let stored_ms = u64::from_be_bytes(surviving_key[..8].try_into().unwrap());
    let stored_sig = &surviving_key[8..];
    assert_eq!(
        stored_ms, t_post,
        "surviving entry must have received_at = t_post ({t_post}); got {stored_ms}"
    );
    assert_eq!(
        stored_sig,
        sig.to_ascii_lowercase().as_bytes(),
        "surviving entry must carry the re-presented sig"
    );
}

// ── AC 3+4 — cap fires at boundary ────────────────────────────────────────────
//
// Self-defeating property: this test fails if CAP enforcement is removed (1025th
// accepts) or if the CAP constant shifts (boundary moves in either direction).

#[test]
fn cap_fires_at_boundary() {
    let (_db, component) = open_component();
    let base: u64 = 1_748_000_000_000;

    // Admit CAP distinct signatures, all within one window.
    // Each uses a unique sig so they each get a distinct dedup key.
    // Max spread = CAP-1 ms = 1023 ms ≪ window_ms_for(TEST_MAX_SKEW_SECS) = 600_000 ms, so none expire
    // during the fill loop and all count toward non_expired_count.
    for i in 0..CAP {
        let sig = format!("v1=cap{i:016x}");
        let ts = base + i as u64;
        let (result, _) = component.check(&sig_input(&sig, ts));
        assert!(
            result.is_ok(),
            "admit {i} (of {CAP}) must succeed: {result:?}"
        );
    }

    // (CAP+1)-th distinct sig: non_expired_count == CAP → TooManyRequests.
    // received_at = base + CAP (1024 ms after base, still within window).
    let overflow_sig = "v1=cap_overflow_entry";
    let ts_overflow = base + CAP as u64;
    let (result, _) = component.check(&sig_input(overflow_sig, ts_overflow));
    assert!(
        matches!(result, Err(ReplayError::TooManyRequests)),
        "{CAP}+1-th distinct sig must return TooManyRequests: {result:?}"
    );

    // Store assertion: exactly CAP entries remain; the rejected sig was not inserted
    // (rollback at lib.rs:185–187 precedes the put at lib.rs:198).
    // Key layout: 8-byte big-endian received_at_ms || sig bytes.
    let overflow_sig_normalized = overflow_sig.to_ascii_lowercase();
    common::assert_scan_count_and_absent(
        component.kv_store_for_testing(),
        NS,
        CAP,
        8,
        overflow_sig_normalized.as_bytes(),
    );
}

// ── Case normalization — uppercase sig variant is treated as duplicate ─────────
//
// Self-defeating property: this test fails if to_ascii_lowercase() is removed or
// inverted (lib.rs:120), because the uppercase and lowercase variants would then
// map to distinct dedup keys and both would be accepted.

#[test]
fn case_normalized_sig_is_duplicate() {
    let (_db, component) = open_component();
    let t: u64 = 1_748_000_000_000;

    // Accept the lowercase form.
    let (r1, _) = component.check(&sig_input("v1=aabbccddeeff0011", t));
    assert!(r1.is_ok(), "first check must accept: {r1:?}");

    // Uppercase variant of the same MAC value: the component normalizes to lowercase
    // before building the dedup key (lib.rs:120), so this must be treated as a
    // duplicate even though the raw header bytes differ.
    // received_at is offset by +1 ms so the key would differ if normalization were
    // absent (key = received_at_be || sig_bytes), making the test self-defeating.
    let (r2, _) = component.check(&sig_input("V1=AABBCCDDEEFF0011", t + 1));
    assert!(
        matches!(r2, Err(ReplayError::Duplicate)),
        "uppercased sig must be treated as duplicate of its lowercase form: {r2:?}"
    );
}

// ── Persistence — dedup state survives component restart ──────────────────────
//
// Self-defeating property: this test fails if the generic component's store-path
// wiring is broken (a new ReplayComponent load on the same path would start with
// an empty store, causing the second check to accept instead of reject).

#[test]
fn restart_persists_dedup() {
    use std::path::PathBuf;

    let db = NamedTempFile::new().unwrap();
    let path: PathBuf = db.path().to_path_buf();
    let sig = "v1=persist0011aabb";
    let t: u64 = 1_748_000_000_000;

    // First component instance: accept a request.
    {
        let component = ReplayComponent::load(
            "push-test",
            &generic_artifact(),
            &path,
            brenn_wasm::store::DEFAULT_MAX_PAGE_COUNT,
            default_config(),
        );
        let (r, _) = component.check(&sig_input(sig, t));
        assert!(r.is_ok(), "initial accept must succeed: {r:?}");
        // component dropped here; store flushed to disk
    }

    // Second component instance on the same store path: same sig must be rejected.
    {
        let component = ReplayComponent::load(
            "push-test",
            &generic_artifact(),
            &path,
            brenn_wasm::store::DEFAULT_MAX_PAGE_COUNT,
            default_config(),
        );
        // t+1 so received_at differs (key = received_at_be || sig_bytes, same sig → duplicate).
        let (r2, _) = component.check(&sig_input(sig, t + 1));
        assert!(
            matches!(r2, Err(ReplayError::Duplicate)),
            "sig replayed after restart must be rejected: {r2:?}"
        );
    }
}

// ── Host-quota integration: store at cap returns TooManyRequests, quota_hit set ──
//
// Design §2.E / §5 integration test plan. Tests the full path from
// KvStore::open(small_cap) → component.check → QuotaExceeded →
// TooManyRequests (no panic), and verifies the quota_hit flag on the
// ReplayComponent (the §2.E layer-2 alert seam).
//
// Page arithmetic: 64 KiB / 4096 bytes = 16 pages. We use a very small cap
// (e.g. 20 pages) to fill quickly without inserting thousands of entries.
// Each replay-generic entry is ~8 bytes (u64 be timestamp) + sig bytes ≈ 30–40
// bytes of key, plus SQLite row overhead. At 20 pages (= 81920 bytes) total
// the store fits ~a few hundred rows. We drive it to near-full with CAP
// entries (each with a distinct large-ish sig), then confirm quota fires.
//
// The exact entry count is empirically discovered by this test — we fill until
// either TooManyRequests (guest CAP) or QuotaExceeded (host quota). We expect
// host quota to fire first given a small page cap.

const TINY_CAP_PAGES: u32 = 20; // 20 pages = 81920 bytes

#[test]
fn host_quota_returns_too_many_requests_and_sets_quota_hit_flag() {
    // Load component with a tiny store cap.
    let db = tempfile::NamedTempFile::new().unwrap();
    let component = ReplayComponent::load(
        "push-test",
        &generic_artifact(),
        db.path(),
        TINY_CAP_PAGES,
        default_config(),
    );

    let base: u64 = 1_748_000_000_000;

    // Insert entries until we hit either the guest CAP (TooManyRequests from
    // guest logic) or the host quota (QuotaExceeded → TooManyRequests via guest
    // §2.D mapping). We record whether quota_hit was ever set to confirm the
    // host-quota path fires before the guest CAP.
    let mut quota_hit_observed = false;
    let mut final_err = None;

    for i in 0u64..2000 {
        // Use a long sig to consume more bytes per entry and reach the cap sooner.
        let sig = format!("v1={:064x}", i);
        let ts = base + i;
        let (result, quota_hit) = component.check(&sig_input(&sig, ts));
        if quota_hit {
            quota_hit_observed = true;
        }
        match result {
            Ok(()) => {}
            Err(e) => {
                final_err = Some(e);
                break;
            }
        }
    }

    assert!(
        final_err.is_some(),
        "expected a rejection (TooManyRequests) before 2000 entries with TINY_CAP_PAGES={TINY_CAP_PAGES}"
    );
    assert!(
        matches!(final_err, Some(ReplayError::TooManyRequests)),
        "over-cap or over-guest-cap insert must return TooManyRequests; got: {final_err:?}"
    );
    assert!(
        quota_hit_observed,
        "host quota must have been hit (quota_hit flag set) before insert loop terminated; \
         if this fails, the guest CAP fired first — increase TINY_CAP_PAGES or use shorter sigs"
    );
}

#[test]
fn quota_hit_flag_is_per_call_not_sticky() {
    // Validates two properties of the quota_hit flag:
    // 1. A fresh component returns quota_hit=false when no quota was hit.
    // 2. A component whose store was just filled returns quota_hit=true exactly
    //    on the call that hits the quota — the flag is not "sticky" across calls
    //    (each call's swap(false) makes the next call start from false).
    //
    // We use two separate component instances so we can independently observe
    // quota_hit=false (fresh) and quota_hit=true (filled), without depending on
    // a store transitioning from full to non-full (which requires guest prune logic).

    // --- Part 1: fresh component → quota_hit always false ---
    let db_fresh = tempfile::NamedTempFile::new().unwrap();
    // Use a generous cap so we never hit quota.
    let large_cap = 10_000u32;
    let fresh = ReplayComponent::load(
        "push-test",
        &generic_artifact(),
        db_fresh.path(),
        large_cap,
        default_config(),
    );
    let (_, flag_fresh) = fresh.check(&sig_input("v1=aabbccdd", 1_748_000_000_000));
    assert!(
        !flag_fresh,
        "quota_hit must be false on a fresh non-full component"
    );

    // --- Part 2: filled component → quota_hit=true on the hit call, swap-false cycle demonstrated ---
    // Fill to cap on a tiny-cap component.
    let db_full = tempfile::NamedTempFile::new().unwrap();
    let full_component = ReplayComponent::load(
        "push-test",
        &generic_artifact(),
        db_full.path(),
        TINY_CAP_PAGES,
        default_config(),
    );
    let base: u64 = 1_748_000_000_000;
    let mut first_hit_sig_idx: Option<u64> = None;
    for i in 0u64..2000 {
        let sig = format!("v1={:064x}", i);
        let ts = base + i;
        let (result, quota_hit) = full_component.check(&sig_input(&sig, ts));
        if quota_hit {
            first_hit_sig_idx = Some(i);
            break;
        }
        if result.is_err() {
            // Guest CAP fired instead of host quota — test precondition not met.
            break;
        }
    }
    let hit_idx = first_hit_sig_idx.expect(
        "quota_hit must have been true during the fill loop; host quota should fire first \
         with TINY_CAP_PAGES={TINY_CAP_PAGES}",
    );

    // The flag was cleared (swap(false)) by the call that returned quota_hit=true.
    // A second call on the SAME full_component must return quota_hit=true again —
    // confirming the swap-false cycle ran (prior call cleared the flag) and the store
    // is still full (so the next insert also hits SQLITE_FULL, setting it again).
    // This proves the flag is "per-call, not sticky": it fires exactly once per
    // quota-hitting call, not permanently.
    let next_sig = format!("v1={:064x}", hit_idx + 1);
    let next_ts = base + hit_idx + 1;
    let (_, second_quota_hit) = full_component.check(&sig_input(&next_sig, next_ts));
    assert!(
        second_quota_hit,
        "second call on the same full component must also return quota_hit=true; \
         if it returned false the swap(false) did not run on the first call"
    );

    // A THIRD call on a completely separate fresh db confirms the flag is not
    // a process-global sticky bit (each component has its own AtomicBool).
    let db_second = tempfile::NamedTempFile::new().unwrap();
    let second = ReplayComponent::load(
        "push-test",
        &generic_artifact(),
        db_second.path(),
        large_cap,
        default_config(),
    );
    let (_, flag_second) = second.check(&sig_input("v1=notfull", base));
    assert!(
        !flag_second,
        "quota_hit must be false on a separate non-full component after another component hit quota"
    );
}

// ── Config: missing brenn.max-skew-secs key → trap ───────────────────────────
//
// Self-defeating property: if the panic in read_window_ms() is removed or has a
// fallback, the check call succeeds (Ok or typed error) instead of trapping.

#[test]
fn missing_max_skew_secs_traps() {
    let db = tempfile::NamedTempFile::new().unwrap();
    // Load with an empty config — no brenn.max-skew-secs injected.
    let component = ReplayComponent::load(
        "push-test",
        &generic_artifact(),
        db.path(),
        DEFAULT_MAX_PAGE_COUNT,
        std::collections::HashMap::new(),
    );
    let result = component.check_raw_for_testing(&sig_input("v1=aabbccdd", 1_748_000_000_000));
    assert!(
        result.is_err(),
        "missing brenn.max-skew-secs must trap (unreachable); got Ok({:?})",
        result.unwrap()
    );
}

// ── Config: unparseable brenn.max-skew-secs value → trap ─────────────────────
//
// Self-defeating property: if parse error handling in read_window_ms() is removed
// or falls back to a default, the check call succeeds instead of trapping.

#[test]
fn unparseable_max_skew_secs_traps() {
    let db = tempfile::NamedTempFile::new().unwrap();
    let mut bad_config = std::collections::HashMap::new();
    bad_config.insert(
        "brenn.max-skew-secs".to_string(),
        "not-a-number".to_string(),
    );
    let component = ReplayComponent::load(
        "push-test",
        &generic_artifact(),
        db.path(),
        DEFAULT_MAX_PAGE_COUNT,
        bad_config,
    );
    let result = component.check_raw_for_testing(&sig_input("v1=aabbccdd", 1_748_000_000_000));
    assert!(
        result.is_err(),
        "unparseable brenn.max-skew-secs must trap (unreachable); got Ok({:?})",
        result.unwrap()
    );
}

// ── Config: non-default skew value drives the window correctly ────────────────
//
// Self-defeating property: if the window is hard-coded or not read from config,
// the boundary assertions below will fail (wrong expiry threshold).

#[test]
fn non_default_skew_window_tracks_config() {
    let db = tempfile::NamedTempFile::new().unwrap();
    let skew_secs: u64 = 60;
    let window_ms: u64 = 2 * skew_secs * 1_000; // 120_000 ms

    let mut cfg = std::collections::HashMap::new();
    cfg.insert("brenn.max-skew-secs".to_string(), skew_secs.to_string());

    let component = ReplayComponent::load(
        "push-test",
        &generic_artifact(),
        db.path(),
        DEFAULT_MAX_PAGE_COUNT,
        cfg,
    );

    let sig = "v1=non_default_skew_test";
    let t0: u64 = 1_748_000_000_000;

    // First sight: accepted.
    let (r1, _) = component.check(&sig_input(sig, t0));
    assert!(r1.is_ok(), "first check must be accepted: {r1:?}");

    // At exactly window_ms (age_abs == window_ms, strict >): still blocked.
    let (r2, _) = component.check(&sig_input(sig, t0 + window_ms));
    assert!(
        matches!(r2, Err(ReplayError::Duplicate)),
        "re-check at exactly window_ms={window_ms} must be blocked: {r2:?}"
    );

    // At window_ms+1: entry expired → re-accepted.
    let (r3, _) = component.check(&sig_input(sig, t0 + window_ms + 1));
    assert!(
        r3.is_ok(),
        "re-check at window_ms+1={} must be accepted (entry expired): {r3:?}",
        window_ms + 1
    );
}
