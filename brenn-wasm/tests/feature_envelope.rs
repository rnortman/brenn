// The core-wasm feature envelope a processor engine accepts from guest code.
//
// wasmtime's on-by-default proposal set moves with its releases, so without a
// host-side pin the set of instructions an out-of-tree component may contain
// changes underneath the sandbox at every engine upgrade. These tests assert the
// envelope on the engine a loaded `ProcessorComponent` actually compiles with,
// not on a re-derived `Config`, so dropping `pin_guest_feature_envelope` from the
// load path fails here.

use std::collections::HashMap;

use brenn_wasm::{ProcessorComponent, ProcessorLoadSpec, store::DEFAULT_MAX_PAGE_COUNT};
use wasmtime::Module;

mod common;

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

/// Baseline: nothing outside the envelope. Guards the four rejections above from
/// passing for some unrelated reason (a broken engine would fail this too).
const PLAIN_MODULE: &str = r#"(module (func (export "run")))"#;

/// A loaded processor whose engine is the one under test. The demo fixture needs
/// no grants beyond its output port, and this test never runs it.
fn loaded_processor() -> ProcessorComponent {
    let mut ports = HashMap::new();
    ports.insert("out".to_string(), common::out_spec("brenn:test-out"));
    ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target/components/brenn_processor_demo.wasm"),
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

/// Each denied proposal is rejected by the production engine, and the rejection
/// names the proposal — so a future wasmtime that renames a feature or moves it
/// out of the disabled set fails here rather than silently widening the sandbox.
#[test]
fn denied_proposals_are_rejected_by_the_processor_engine() {
    let comp = loaded_processor();
    let engine = comp.engine();

    for (label, wat, phrase) in [
        (
            "function references",
            CALL_REF_MODULE,
            "function references support is not enabled",
        ),
        ("gc", STRUCT_TYPE_MODULE, "gc"),
        ("exceptions", EXCEPTIONS_MODULE, "exceptions"),
        ("threads", THREADS_MODULE, "threads"),
    ] {
        let err = Module::new(engine, assemble(wat)).expect_err(&format!(
            "{label} must be outside the guest feature envelope, but the engine accepted it"
        ));
        let diag = format!("{err:#}");
        assert!(
            diag.contains(phrase),
            "{label} rejection must name the proposal (looked for {phrase:?}); got: {diag}"
        );
    }
}

/// The envelope is not so narrow that ordinary guest code stops compiling.
#[test]
fn plain_module_compiles_on_the_processor_engine() {
    let comp = loaded_processor();
    Module::new(comp.engine(), assemble(PLAIN_MODULE))
        .expect("a module using no gated proposal must compile");
}
