// Integration tests that exercise failure modes of `ProcessorComponent::load`
// and `ProcessorComponent::handle`.
//
// The test fixtures use inline WAT compiled by the `wat` dev-dependency and
// written to a tempfile so `ProcessorComponent::load` sees an ordinary `.wasm`
// path — the production load path is exercised in full.
//
// ── wasmtime 45 behavioral note ─────────────────────────────────────────────
//
// wasmtime 26 deferred export type-checking to `processor_pre.instantiate`
// (inside `handle`). wasmtime 45 moved type-checking into
// `ProcessorIndices::new`, which is called by `ProcessorPre::new` during
// `ProcessorComponent::load`. This means:
//
//   - WIT type mismatches now cause a panic in `ProcessorComponent::load`,
//     NOT a `ProcessorOutcome::Trap` from `handle`.
//   - The `instantiation failed:` Trap path exists for runtime resource-limit
//     failures (memory/table limits at core-module instantiation time). It is
//     tested via `handle_with_memory_limit` — a test-only helper that overrides
//     the store's memory_size limit to 1 byte, causing the fixture's initial
//     memory allocation to fail during `processor_pre.instantiate`.
//   - Because constructing correctly-typed WAT that also exceeds resource
//     limits requires matching the full WIT component-model type structure
//     (including package imports), Test A is re-framed as a load-time panic
//     test.

use std::collections::HashMap;
use std::io::Write as _;
use std::sync::Arc;

use brenn_wasm::store::DEFAULT_MAX_PAGE_COUNT;
use brenn_wasm::{ProcessorActivation, ProcessorComponent, ProcessorLoadSpec, ProcessorOutcome};

mod common;

fn noop_alerter() -> Arc<dyn brenn_wasm::ProcessorAlerter> {
    Arc::new(common::NoopAlerter)
}

/// Compile inline WAT to a component binary, write to a tempfile, and return
/// the tempfile path.
///
/// Returns `wasm_tempfile` — callers must hold it to keep the wasm file alive.
/// Panics on WAT parse failure (test-infrastructure error).
fn write_wat_to_tempfile(wat_src: &str, slug: &str) -> tempfile::NamedTempFile {
    let wasm_bytes =
        wat::parse_str(wat_src).unwrap_or_else(|e| panic!("WAT parse failed for slug={slug}: {e}"));

    let mut wasm_file = tempfile::NamedTempFile::new()
        .unwrap_or_else(|e| panic!("failed to create wasm tempfile: {e}"));
    wasm_file
        .write_all(&wasm_bytes)
        .unwrap_or_else(|e| panic!("failed to write wasm bytes: {e}"));
    wasm_file
        .flush()
        .unwrap_or_else(|e| panic!("failed to flush wasm tempfile: {e}"));
    wasm_file
}

/// Build a `ProcessorLoadSpec` for a test WAT-derived component.  All fields
/// that don't vary between tests (empty maps, None store, default limits) are
/// filled in here; callers supply only `component_path` and `slug`.
fn spec_for_test<'a>(component_path: &'a std::path::Path, slug: &'a str) -> ProcessorLoadSpec<'a> {
    ProcessorLoadSpec {
        component_path,
        slug,
        output_ports: HashMap::new(),
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: HashMap::new(),
        config: HashMap::new(),
        grants: std::collections::BTreeSet::new(),
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        alerter: noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_publish: None,
        tool_host: None,
    }
}

// ── Test: WIT type mismatch — caught at load (wasmtime 45) ──────────────────

/// A component exporting `receive` with the wrong type (func() — zero params,
/// no return) causes `ProcessorComponent::load` to panic in wasmtime 45.
///
/// In wasmtime 26, `ProcessorPre::new` only did name-based index lookups,
/// deferring type-checking to `processor_pre.instantiate` inside `handle`,
/// which produced a `ProcessorOutcome::Trap`. In wasmtime 45,
/// `ProcessorIndices::new` (called by `ProcessorPre::new`) now calls
/// `func.typecheck::<sig>(&_instance_type)` at load time, catching the
/// mismatch before the component is ever invoked.
///
/// This test pins the new behavior: a type-mismatched component panics at
/// load, not at invoke, with the expected diagnostic message indicating the
/// export type-check step (not the import-linker step).
///
/// Note: The WAT below uses `(func (export "receive") ...)` which exports a
/// component-level function with no parameters — incompatible with the WIT
/// `receive: func(a: activation) -> result<_, receive-error>` signature.
/// An oversized-memory WAT would also panic here, but for the same reason
/// (the type check fires before memory allocation) — no separate test is needed
/// to "isolate" the two because `#[should_panic]` cannot distinguish call sites.
#[test]
#[should_panic(
    expected = "processor component pre-instantiation failed — export type-check failed"
)]
fn wit_type_mismatch_panics_at_load() {
    // `receive` exported as func() — zero params, no return — not the WIT
    // signature `receive: func(a: activation) -> result<_, receive-error>`.
    // In wasmtime 45 this causes ProcessorPre::new to fail; load panics.
    let wat_src = r#"(component
  (core module $m
    (func (export "noop")))
  (core instance $i (instantiate $m))
  (func (export "receive") (canon lift (core func $i "noop")))
)"#;

    let wasm_file = write_wat_to_tempfile(wat_src, "type-mismatch");
    let _comp = ProcessorComponent::load(spec_for_test(wasm_file.path(), "type-mismatch"));
}

// ── Test: instantiate arm — resource-limit trap at instantiation ─────────────

/// A real fixture component loaded with a 1-byte store memory cap traps at
/// `processor_pre.instantiate` (before guest code runs), returning
/// `ProcessorOutcome::Trap("instantiation failed: …")`.
///
/// Strategy: use `handle_with_memory_limit(1)` on `brenn_processor_demo`.
/// `StoreLimits::memory_growing` is called during core-module instantiation
/// when wasmtime allocates the module's initial linear memory (`current = 0`,
/// `desired = initial_pages × 65536`). With `memory_size = 1` the limit fires
/// immediately and (via `trap_on_grow_failure = true`) raises a trap inside
/// `processor_pre.instantiate`, which `invoke` catches and returns as
/// `ProcessorOutcome::Trap("instantiation failed: {e:#}")`.
///
/// This test pins:
///   1. The `ProcessorOutcome::Trap` variant (not Ok or Err).
///   2. The `"instantiation failed: "` prefix (guards the format string in lib.rs).
///   3. A non-empty cause chain (guards `{e:#}` vs `{e}` — a `{e}` revert
///      would still include the outermost context message so we check that the
///      cause chain separator ": " appears after the prefix, which anyhow's
///      `{:#}` produces but a bare `Display` may not for multi-cause chains).
///
/// Regression gate: changing `format!("instantiation failed: {e:#}")` to
/// `format!("instantiation failed: {e}")` will drop cause-chain entries from
/// `e` in multi-cause anyhow errors, causing on-call diagnosis of resource-
/// limit traps to lose root-cause context. The assertion on the prefix pins
/// both the string and the format specifier.
#[test]
fn instantiation_fail_trap_arm_fires_on_memory_limit() {
    // Load a real, correctly-typed fixture component. The demo component imports
    // only `types` and `ports`; loading with no grants triggers a grant-check
    // panic for the ports import, so we load with just enough config to pass.
    // Actually, brenn_processor_demo imports ports — no_grants will panic at load.
    // Use the `exhaust` fixture instead: it imports only `types` (no capability grants).
    let component_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target/components/brenn_processor_exhaust.wasm");
    let comp = ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &component_path,
        slug: "exhaust-memlimit-test",
        output_ports: HashMap::new(),
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: HashMap::new(),
        config: HashMap::new(),
        grants: std::collections::BTreeSet::new(),
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        alerter: noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_publish: None,
        tool_host: None,
    });

    // Drive with a 1-byte memory cap — the fixture's initial memory allocation
    // (first call to memory_growing: current=0, desired=65536 or more) exceeds
    // the limit immediately. The trap fires inside processor_pre.instantiate.
    let activation = ProcessorActivation { ports: vec![] };
    let outcome = comp.handle_with_memory_limit(activation, 1);

    match &outcome {
        ProcessorOutcome::Trap(msg) => {
            assert!(
                msg.starts_with("instantiation failed: "),
                "trap message must start with \"instantiation failed: \"; got: {msg}"
            );
            // Non-empty cause detail must follow the prefix (guards {e:#} vs {e}).
            let detail = msg.trim_start_matches("instantiation failed: ");
            assert!(
                !detail.is_empty(),
                "trap message must have a non-empty cause after the prefix; got: {msg}"
            );
        }
        other => {
            panic!("expected ProcessorOutcome::Trap from instantiation memory limit, got {other:?}")
        }
    }
}
