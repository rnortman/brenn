// The core-wasm feature envelope the guest engines accept from guest code.
//
// wasmtime's on-by-default proposal set moves with its releases, so without a
// host-side pin the set of instructions an out-of-tree component may contain
// changes underneath the sandbox at every engine upgrade. These tests assert the
// envelope on the engines a loaded `ProcessorComponent` / `ReplayComponent`
// actually compile with, not on a re-derived `Config`, so dropping
// `pin_guest_feature_envelope` from either load path fails here.
//
// The accepted half of the matrix compiles *and runs* each probe: a proposal that
// validates but cannot execute in this build is not in the envelope in any sense
// a component author could rely on.

use std::collections::HashMap;

use brenn_wasm::{ProcessorComponent, ProcessorLoadSpec, store::DEFAULT_MAX_PAGE_COUNT};
use wasmtime::{Engine, Instance, Module, Store};

mod common;

// ── Accepted probes: each exports `run` with no params and an i32 result ──────

/// Baseline: nothing outside the WebAssembly 1.0 core. Guards every rejection
/// below from passing for some unrelated reason (a broken engine fails this too).
const PLAIN_MODULE: &str = r#"(module
  (func (export "run") (result i32) (i32.const 7))
)"#;

/// Tail calls: `return_call` to a same-signature callee.
const TAIL_CALL_MODULE: &str = r#"(module
  (func $callee (result i32) (i32.const 7))
  (func (export "run") (result i32) (return_call $callee))
)"#;

/// Extended const expressions: arithmetic in a global's initializer, which the
/// MVP constant-expression grammar does not admit.
const EXTENDED_CONST_MODULE: &str = r#"(module
  (global $g i32 (i32.add (i32.const 3) (i32.const 4)))
  (func (export "run") (result i32) (global.get $g))
)"#;

/// Bulk memory: `memory.copy` moves a data-segment byte to a second address.
const BULK_MEMORY_MODULE: &str = r#"(module
  (memory 1)
  (data (i32.const 0) "\07\00\00\00")
  (func (export "run") (result i32)
    (memory.copy (i32.const 16) (i32.const 0) (i32.const 4))
    (i32.load (i32.const 16)))
)"#;

/// Fixed-width SIMD: splat and extract a lane. Deterministic per spec, unlike
/// the relaxed-simd probe below.
const SIMD_MODULE: &str = r#"(module
  (func (export "run") (result i32)
    (i32x4.extract_lane 0 (i32x4.splat (i32.const 7))))
)"#;

/// Reference types: a `funcref` table written and read through `table.set`/`get`.
const REFERENCE_TYPES_MODULE: &str = r#"(module
  (table $t 1 funcref)
  (func $seven (result i32) (i32.const 7))
  (elem declare func $seven)
  (type $sig (func (result i32)))
  (func (export "run") (result i32)
    (table.set $t (i32.const 0) (ref.func $seven))
    (call_indirect $t (type $sig) (i32.const 0)))
)"#;

/// Multi-value: a function returning two results, consumed by its caller.
const MULTI_VALUE_MODULE: &str = r#"(module
  (func $pair (result i32 i32) (i32.const 3) (i32.const 4))
  (func (export "run") (result i32) (call $pair) (i32.add))
)"#;

/// Non-trapping float-to-int: `i32.trunc_sat_f32_s`, which saturates where the MVP
/// conversion traps.
const SATURATING_FLOAT_TO_INT_MODULE: &str = r#"(module
  (func (export "run") (result i32) (i32.trunc_sat_f32_s (f32.const 7.5)))
)"#;

/// Mutable globals: an exported global declared `mut`, which the MVP forbids exporting.
const MUTABLE_GLOBAL_MODULE: &str = r#"(module
  (global $g (export "g") (mut i32) (i32.const 7))
  (func (export "run") (result i32) (global.get $g))
)"#;

/// Every accepted probe returns this, so the assert proves execution reached the
/// end of the function rather than merely that instantiation succeeded.
const ACCEPTED_RESULT: i32 = 7;

// ── Rejected probes ──────────────────────────────────────────────────────────

/// Typed function references: `ref.func` producing a typed (non-`funcref`) value
/// consumed by `call_ref`. The `elem declare` is what makes the reference legal to
/// take; without it the module is rejected for an unrelated reason.
const CALL_REF_MODULE: &str = r#"(module
  (type $t (func))
  (func $f)
  (elem declare func $f)
  (func (export "run") (call_ref $t (ref.func $f)))
)"#;

/// GC proposal: a `struct` type definition in the type section.
const STRUCT_TYPE_MODULE: &str = r#"(module (type $s (struct (field i32))))"#;

/// Exception handling: a tag definition plus `throw`.
const EXCEPTIONS_MODULE: &str = r#"(module
  (tag $e)
  (func (export "run") (throw $e))
)"#;

/// Threads/atomics: a shared memory and an atomic load.
const THREADS_MODULE: &str = r#"(module
  (memory 1 1 shared)
  (func (export "run") (result i32) (i32.atomic.load (i32.const 0)))
)"#;

/// Multi-memory: two linear memories in one module.
const MULTI_MEMORY_MODULE: &str = r#"(module
  (memory 1)
  (memory 1)
)"#;

/// Memory64: an `i64`-indexed linear memory.
const MEMORY64_MODULE: &str = r#"(module (memory i64 1))"#;

/// Relaxed SIMD: `f32x4.relaxed_madd`, whose result is implementation-dependent
/// by specification — the reason it is outside the envelope.
const RELAXED_SIMD_MODULE: &str = r#"(module
  (func (export "run") (result v128)
    (f32x4.relaxed_madd
      (v128.const i32x4 0 0 0 0)
      (v128.const i32x4 0 0 0 0)
      (v128.const i32x4 0 0 0 0)))
)"#;

/// Every proposal outside the envelope, with the phrase its rejection must name.
/// Ordered as the allow-list's "out" list: GC cluster, threads, then the three
/// proposals the allow-list narrowed away relative to wasmtime's own defaults.
const REJECTED: &[(&str, &str, &str)] = &[
    (
        "function references",
        CALL_REF_MODULE,
        "function references support is not enabled",
    ),
    ("gc", STRUCT_TYPE_MODULE, "gc"),
    ("exceptions", EXCEPTIONS_MODULE, "exceptions"),
    ("threads", THREADS_MODULE, "threads"),
    ("multi-memory", MULTI_MEMORY_MODULE, "multiple memories"),
    ("memory64", MEMORY64_MODULE, "memory64"),
    (
        "relaxed simd",
        RELAXED_SIMD_MODULE,
        "relaxed SIMD support is not enabled",
    ),
];

// ── Harness ──────────────────────────────────────────────────────────────────

/// A loaded processor whose engine is the one under test. The demo fixture needs
/// no grants beyond its output port, and this test never runs it.
fn loaded_processor() -> ProcessorComponent {
    let mut ports = HashMap::new();
    ports.insert("out".to_string(), common::out_spec("brenn:test-out"));
    ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &common::artifact_path("brenn_processor_demo"),
        slug: "feature-envelope",
        output_ports: ports,
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: HashMap::new(),
        grants: [brenn_wasm::Capability::Ports].into_iter().collect(),
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        config: HashMap::new(),
        alerter: common::noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_publish: None,
        tool_host: None,
    })
}

fn assemble(wat: &str) -> Vec<u8> {
    wat::parse_str(wat).expect("fixture assembles")
}

/// Instantiate `wat` on `engine` and call its `run` export.
///
/// Both guest engines are built with fuel consumption and epoch interruption on,
/// so a bare store would trap before executing anything; the budgets here are
/// large enough that neither bound can fire inside a handful of instructions.
fn run_probe(engine: &Engine, label: &str, wat: &str) -> i32 {
    let module = Module::new(engine, assemble(wat))
        .unwrap_or_else(|e| panic!("{label} must be inside the guest feature envelope: {e:#}"));
    let mut store = Store::new(engine, ());
    store
        .set_fuel(10_000_000)
        .expect("fuel is enabled on the guest engines");
    store.set_epoch_deadline(1_000_000);
    let instance = Instance::new(&mut store, &module, &[])
        .unwrap_or_else(|e| panic!("{label} probe must instantiate: {e:#}"));
    let run = instance
        .get_typed_func::<(), i32>(&mut store, "run")
        .unwrap_or_else(|e| panic!("{label} probe must export `run`: {e:#}"));
    run.call(&mut store, ())
        .unwrap_or_else(|e| panic!("{label} probe must run to completion: {e:#}"))
}

/// Compile `wat` on `engine`, requiring rejection, and return the diagnostic.
fn rejection_diagnostic(engine: &Engine, label: &str, engine_label: &str, wat: &str) -> String {
    let err = Module::new(engine, assemble(wat)).expect_err(&format!(
        "{label} must be outside the {engine_label} feature envelope, but the engine accepted it"
    ));
    format!("{err:#}")
}

/// Assert every rejected probe is refused by `engine`, with the proposal named.
fn assert_rejections(engine: &Engine, engine_label: &str) {
    for (label, wat, phrase) in REJECTED {
        let diag = rejection_diagnostic(engine, label, engine_label, wat);
        assert!(
            diag.contains(phrase),
            "{label} rejection on the {engine_label} engine must name the proposal \
             (looked for {phrase:?}); got: {diag}"
        );
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

/// Each denied proposal is rejected by the production processor engine, and the
/// rejection names the proposal — so a future wasmtime that renames a feature or
/// moves it out of the disabled set fails here rather than silently widening the
/// sandbox.
#[test]
fn denied_proposals_are_rejected_by_the_processor_engine() {
    let comp = loaded_processor();
    assert_rejections(comp.engine(), "processor");
}

/// The replay engine runs guest code on external webhook input, so it carries the
/// same envelope. A spot check of one denied proposal proves the pin is installed
/// on that engine family too.
#[test]
fn denied_proposal_is_rejected_by_the_replay_engine() {
    let (_db, comp) = common::open_component(&common::replay_artifact());
    let diag = rejection_diagnostic(
        comp.engine(),
        "function references",
        "replay",
        CALL_REF_MODULE,
    );
    assert!(
        diag.contains("function references support is not enabled"),
        "the replay rejection must name the proposal; got: {diag}"
    );
}

/// Everything inside the envelope compiles, instantiates and executes on the
/// production processor engine.
///
/// Validation acceptance alone would be a weaker claim than the envelope makes: a
/// proposal wasmtime validates but cannot compile in this build (no cargo feature,
/// no cranelift support) would pass a validation-only probe while no real guest
/// could use it.
///
/// Every flag the allow-list names is probed here except `FLOATS`, `SIGN_EXTENSION`
/// and `COMPONENT_MODEL`, which need no probe because dropping any of them stops the
/// in-tree artifacts loading at all. The rest — including the three that no in-tree
/// artifact happens to use — are covered so that *narrowing* the envelope fails a
/// test, not just widening it: a silent narrowing rejects an out-of-tree guest that
/// stable Rust emits by default.
#[test]
fn accepted_proposals_execute_on_the_processor_engine() {
    let comp = loaded_processor();
    let engine = comp.engine();

    for (label, wat) in [
        ("baseline", PLAIN_MODULE),
        ("tail call", TAIL_CALL_MODULE),
        ("extended const", EXTENDED_CONST_MODULE),
        ("bulk memory", BULK_MEMORY_MODULE),
        ("simd", SIMD_MODULE),
        ("reference types", REFERENCE_TYPES_MODULE),
        ("multi value", MULTI_VALUE_MODULE),
        ("saturating float to int", SATURATING_FLOAT_TO_INT_MODULE),
        ("mutable global", MUTABLE_GLOBAL_MODULE),
    ] {
        assert_eq!(
            run_probe(engine, label, wat),
            ACCEPTED_RESULT,
            "{label} probe must run and return {ACCEPTED_RESULT}"
        );
    }
}
