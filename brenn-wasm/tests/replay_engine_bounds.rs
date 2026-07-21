// Resource-bound tests for the replay-check WASM world (H1046, design §4).
//
// The replay world runs an operator-supplied guest on every inbound webhook POST.
// These tests verify the in-engine bounds installed by H1046 — fuel, epoch deadline,
// and the store memory cap — actually trap a pathological guest instead of letting it
// hang, and that a guest trapping mid-transaction does not wedge the shared store.
//
// They mirror the processor world's bound tests in `consume_engine.rs`. The production
// `brenn_replay.wasm` cannot spin/grow on demand, so the trap-driving tests use the
// `replay-fault-test` fixture (sentinel ops keyed on the `x-brenn-fault-test` header).

mod common;

use brenn_cal::ms_to_sent_at;
use brenn_wasm::{CheckInput, Header, REPLAY_FUEL, ReplayComponent};
use std::collections::HashMap;
use tempfile::NamedTempFile;

const FAULT_ARTIFACT_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/target/components/brenn_replay_fault_test.wasm"
);

const REPLAY_ARTIFACT_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/target/components/brenn_replay.wasm"
);

fn fault_artifact() -> std::path::PathBuf {
    std::path::PathBuf::from(FAULT_ARTIFACT_PATH)
}

fn replay_artifact() -> std::path::PathBuf {
    std::path::PathBuf::from(REPLAY_ARTIFACT_PATH)
}

fn open_fault() -> (NamedTempFile, ReplayComponent) {
    common::open_component(&fault_artifact())
}

/// Build a CheckInput with the `x-brenn-fault-test` header set to `op`.
fn fault_input(op: &str) -> CheckInput {
    CheckInput {
        headers: vec![Header {
            name: "x-brenn-fault-test".to_string(),
            value: op.to_string(),
        }],
        body: vec![],
        received_at: 0,
        key_id: String::new(),
        endpoint_slug: "test".to_string(),
    }
}

/// A no-op input for the fault fixture (absent fault header → Ok(())).
fn noop_input() -> CheckInput {
    CheckInput {
        headers: vec![],
        body: vec![],
        received_at: 0,
        key_id: String::new(),
        endpoint_slug: "test".to_string(),
    }
}

/// Build a minimal valid phonebuddy envelope body for the production component.
fn envelope(client_id: &str, sent_at: &str, nonce: &str) -> Vec<u8> {
    format!(
        r#"{{"schema_version":"1","kind":"test","client_id":"{client_id}","sent_at":"{sent_at}","nonce":"{nonce}","seq":1,"payload":{{}}}}"#
    )
    .into_bytes()
}

fn simple_input(client_id: &str, ts_ms: u64, nonce: &str) -> CheckInput {
    CheckInput {
        headers: vec![],
        body: envelope(client_id, &ms_to_sent_at(ts_ms), nonce),
        received_at: ts_ms,
        key_id: "primary".to_string(),
        endpoint_slug: "phonebuddy".to_string(),
    }
}

// ── Fuel exhaustion ───────────────────────────────────────────────────────────

/// A fuel-exhausting replay guest traps (outer Err) rather than hanging.
///
/// SPIN with a generous epoch deadline and the production `REPLAY_FUEL`: fuel runs out
/// first and the guest traps. If the fuel bound were not installed (revert §2.2/§2.4)
/// this call would never return — the test would hang rather than fail, which is the
/// exact behavior H1046 fixes. Mirrors `fuel_exhausting_guest_traps_not_panics`
/// (`consume_engine.rs:619`).
#[test]
fn fuel_exhausting_replay_guest_traps_not_hangs() {
    let (_db, component) = open_fault();
    // High epoch deadline so the epoch cannot trip before fuel is exhausted.
    let result = component.check_with_limits(&fault_input("SPIN"), u64::MAX, REPLAY_FUEL);
    assert!(
        result.is_err(),
        "fuel-exhausting SPIN must trap (outer Err), got Ok({:?})",
        result.unwrap()
    );
    let err = format!("{:#}", result.unwrap_err());
    assert!(
        err.contains("all fuel consumed"),
        "fuel-trap message must contain the wasmtime fuel-exhaustion phrase; got: {err}"
    );
}

// ── Epoch deadline ────────────────────────────────────────────────────────────

/// A spinning replay guest that does not exhaust fuel still traps when the epoch
/// deadline fires. SPIN with a 1-tick deadline and effectively-unlimited fuel; the
/// per-component epoch ticker thread (100 ms interval) trips the deadline. Mirrors
/// `epoch_deadline_spins_guest_traps` (`consume_engine.rs:870`).
///
/// The error message is pinned to the wasmtime epoch-interrupt phrase so the test
/// provably exercises the *epoch* path, not fuel exhaustion landing at a different
/// budget. `u64::MAX / 2` (~9.2e18) fuel cannot be consumed by the SPIN loop within
/// the ~100 ms it takes the ticker to advance the 1-tick deadline, so the epoch fires
/// first; the assert makes that guarantee a regression gate (reverting
/// `epoch_interruption(true)` or dropping the ticker would change the trap cause and
/// fail this assert).
#[test]
fn epoch_deadline_spins_replay_guest_traps() {
    let (_db, component) = open_fault();
    let result = component.check_with_limits(
        &fault_input("SPIN"),
        1,            // epoch_deadline: 1 tick — trips within ~100 ms
        u64::MAX / 2, // fuel: effectively unlimited so fuel does NOT trip first
    );
    assert!(
        result.is_err(),
        "spinning guest must trap on epoch deadline (outer Err), got Ok({:?})",
        result.unwrap()
    );
    let err = format!("{:#}", result.unwrap_err());
    assert!(
        err.contains("interrupt"),
        "epoch-trap message must contain the wasmtime epoch-interrupt phrase \
         (`wasm trap: interrupt`), not a fuel-exhaustion phrase; got: {err}"
    );
}

// ── Memory cap ────────────────────────────────────────────────────────────────

/// A replay guest that grows past the 16 MiB memory cap traps deterministically.
///
/// GROW allocates 1 MiB chunks in a loop; with `trap_on_grow_failure(true)` the host
/// raises a trap inside `memory.grow` when the guest crosses `REPLAY_MAX_MEMORY_BYTES`.
/// The message is pinned to the grow-failure root cause so reverting
/// `trap_on_grow_failure` would fail this test (regression gate). Mirrors
/// `memory_cap_excess_traps_deterministically` (`consume_engine.rs:685`).
#[test]
fn replay_memory_cap_excess_traps_deterministically() {
    let (_db, component) = open_fault();
    let result = component.check_raw_for_testing(&fault_input("GROW"));
    assert!(
        result.is_err(),
        "memory-growing GROW must trap (outer Err), got Ok({:?})",
        result.unwrap()
    );
    let err = format!("{:#}", result.unwrap_err());
    assert!(
        err.contains("forcing trap when growing memory"),
        "trap message must contain the limiter grow-failure root-cause phrase; got: {err}"
    );
}

// ── Mid-transaction trap → leaked-tx rollback ─────────────────────────────────

/// A guest that traps mid-transaction (here via a resource-exhaustion trap, not the
/// `HostTransaction::drop`-detected leak) leaves no active transaction on the shared
/// store, and a subsequent check on the same component succeeds. Generalizes the
/// existing `store_round_trip.rs` leak test to the new fuel/epoch trap causes
/// (design §2.5.1 / §4).
#[test]
fn replay_trap_mid_transaction_rolls_back() {
    let (_db, component) = open_fault();

    // LEAK_TX_THEN_TRAP begins a transaction then spins; a 1-tick epoch deadline traps
    // it mid-transaction. The WIT drop destructor does not run after a trap, so the
    // host's leaked-tx cleanup in run_check/check_with_limits must roll back.
    let result = component.check_with_limits(&fault_input("LEAK_TX_THEN_TRAP"), 1, u64::MAX / 2);
    assert!(
        result.is_err(),
        "mid-transaction SPIN must trap (outer Err), got Ok({:?})",
        result.unwrap()
    );

    // The store must not be wedged: tx_active cleared by the leaked-tx cleanup.
    assert!(
        !component.kv_store_for_testing().is_tx_active(),
        "tx_active must be cleared after a mid-transaction trap (leaked-tx cleanup ran)"
    );

    // A second normal check on the same component must succeed — the store is usable.
    let result2 = component
        .check_raw_for_testing(&noop_input())
        .expect("no wasmtime error on a normal check after the leaked-tx rollback");
    assert!(
        result2.is_ok(),
        "normal check after mid-transaction trap must succeed (store not wedged); got {result2:?}"
    );
}

// ── Instantiation-time memory cap ─────────────────────────────────────────────

/// The store memory limiter applies at **instantiation time**, not only to a later
/// `memory.grow`. An over-cap initial-memory module fails inside `replay_pre.instantiate`
/// and surfaces as an outer `Err`.
///
/// Design §3's edge-case table claims "Guest declares huge initial memory
/// (instantiation-time) → make_store limits apply to instantiate too → instantiation fails
/// → outer Err". This test enforces that claim for the replay world: it drives the fault
/// fixture with a 1-byte memory cap (via the `check_with_memory_limit` accessor), so the
/// fixture's own initial linear-memory allocation cannot fit and instantiation fails. If
/// the limiter were installed incorrectly (e.g. not before `instantiate`), the module would
/// instantiate and this test would fail. Replay analogue of the processor world's
/// instantiation-time memory-limit coverage (`handle_with_memory_limit`).
#[test]
fn replay_instantiation_time_memory_cap_traps() {
    let (_db, component) = open_fault();
    // 1-byte memory cap: far below the fixture's initial linear memory, so the limit must
    // fire during instantiation rather than at a later memory.grow.
    let result = component.check_with_memory_limit(&noop_input(), 1);
    assert!(
        result.is_err(),
        "an over-cap initial-memory module must fail at instantiation (outer Err), got Ok({:?})",
        result.unwrap()
    );
}

// ── Honest-workload regression ────────────────────────────────────────────────

/// The production replay component runs an honest accept within all installed budgets:
/// the new fuel/epoch/memory bounds do not spuriously trap the real workload. This is
/// the replay analogue of `full_size_window_does_not_spuriously_trap_demo`
/// (`consume_engine.rs:636`) — coverage that the budgets are not too tight.
#[test]
fn honest_replay_check_within_budget_unaffected() {
    let db = NamedTempFile::new().unwrap();
    let component = ReplayComponent::load(
        "phonebuddy",
        &replay_artifact(),
        db.path(),
        brenn_wasm::store::DEFAULT_MAX_PAGE_COUNT,
        HashMap::new(),
    );
    let result = component
        .check_raw_for_testing(&simple_input(
            "budget_client",
            1_748_000_000_000,
            "nonce00001",
        ))
        .expect("no wasmtime error on an honest check within the production budget");
    assert!(
        result.is_ok(),
        "honest replay check must be accepted within the production budget (no spurious trap); \
         got {result:?}"
    );
}

/// The honest *worst case* — a near-full nonce window forcing the production component's
/// cap-scan / prune walk — still runs within the `REPLAY_FUEL` budget.
///
/// `honest_replay_check_within_budget_unaffected` covers only the cheapest path (an empty
/// store, a single insert, no dedup scan). The `REPLAY_FUEL` doc comment justifies raising
/// the budget from 50M to 320M on a measured ≈ 58.6M-instruction honest *worst* case (a
/// scan/prune at the high end of the nonce window). This test exercises that high-fuel
/// honest path: it fills the window with many distinct nonces from a single client (so they
/// land in the same namespace and accumulate), then asserts a final honest check still
/// accepts within budget — coverage that the bound is not too tight for the real worst case,
/// not only the cheapest case. Replay analogue of
/// `full_size_window_does_not_spuriously_trap_demo` (`consume_engine.rs:636`).
#[test]
fn honest_replay_check_near_full_window_within_budget() {
    let db = NamedTempFile::new().unwrap();
    let component = ReplayComponent::load(
        "phonebuddy",
        &replay_artifact(),
        db.path(),
        brenn_wasm::store::DEFAULT_MAX_PAGE_COUNT,
        HashMap::new(),
    );

    // Fill the nonce window for one client with strictly-increasing timestamps (the
    // production component enforces per-client `sent_at` monotonicity) spaced 1 ms apart, so
    // all entries land within the same ±5-minute skew/TTL window and accumulate in one
    // namespace — the dedup scan then walks a near-full window. `NONCE_CAP_N` is 1024 (the
    // window size cited in the REPLAY_FUEL rationale); hitting it is a `TooManyRequests`
    // *reject*, so we fill to one below the cap (1023) and let the final accepted insert be
    // #1024 — the worst-case scan/insert at the high end of the window. ~1024 ms of spread is
    // far inside the 5-minute window, so none of the nonces expire before the final check.
    let base_ts = 1_748_000_000_000u64;
    for i in 0..1023u64 {
        let nonce = format!("nonce{i:05}");
        let result = component
            .check_raw_for_testing(&simple_input("full_window_client", base_ts + i, &nonce))
            .expect("no wasmtime error while filling the nonce window");
        assert!(
            result.is_ok(),
            "honest window-fill check #{i} must be accepted within budget (no spurious trap); \
             got {result:?}"
        );
    }

    // The final accepted insert (#1024 of NONCE_CAP_N) against the near-full window, at a
    // strictly-later timestamp: this drives the worst-case scan/insert path at the high end of
    // the window and must still accept within REPLAY_FUEL (not trap, not TooManyRequests).
    let result = component
        .check_raw_for_testing(&simple_input(
            "full_window_client",
            base_ts + 1023,
            "noncefinal",
        ))
        .expect("no wasmtime error on the worst-case honest check against a full window");
    assert!(
        result.is_ok(),
        "honest worst-case check against a near-full nonce window must accept within \
         REPLAY_FUEL (no spurious trap); got {result:?}"
    );
}

/// The production `check` entry point *panics* (rather than hanging or returning Ok) when a
/// resource-exhaustion trap fires.
///
/// All other tests use `check_raw_for_testing` / `check_with_limits`, which surface the trap
/// as an outer `Err`. This test exercises the real production dispatch path: `check`'s
/// `unwrap_or_else(|e| panic!(...))` (lib.rs) maps the trap to a panic, which the route layer
/// re-raises into a 500 (design §2.5). A SPIN at the production `REPLAY_FUEL` exhausts fuel
/// and traps; `check` must panic. If the panic mapping were removed or made conditional, this
/// test would fail. `expected` pins the production panic phrase so the panic is the trap-map,
/// not some unrelated panic.
#[test]
#[should_panic(expected = "WASM runtime error (guest trap or instantiation)")]
fn check_production_path_panics_on_resource_trap() {
    let (_db, component) = open_fault();
    // Production fuel, generous epoch: fuel exhausts and traps; `check` maps it to a panic.
    let _ = component.check(&fault_input("SPIN"));
}
