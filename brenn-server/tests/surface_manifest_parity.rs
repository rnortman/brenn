//! Parity pin: the shell emitter that writes real processor manifests and
//! `processor_component_imports` must extract the *same* import profile from the
//! same artifact.
//!
//! Two independent implementations of one load-bearing normalization
//! (fully-qualified name, `@version` stripped) drift silently otherwise: a change
//! to the emitter's `sed` pipeline, or a `wasm-tools` output-format change, would
//! ship a differently-shaped profile that no test built on the Rust twin would
//! notice until a deploy or page-load failure. This asserts the emitted `imports`
//! equals what the twin reads out of the very component the emitted manifest was
//! built from.
//!
//! Its own suite rather than a unit test in the crate: the input is the whole
//! assembled surface asset tree, which is downstream of every surface crate, the
//! wasm-bindgen CLI and the transpiler. Declared on the crate's unit-test target
//! it would rerun all ~2,500 of those tests on any surface edit; here the wide
//! input closure belongs to the one test that needs it.
//!
//! The transpiled tree is build output, not a checked-in fixture, so `make test`
//! takes `surface-transpile` as a prerequisite. Its absence is a hard failure
//! naming that command — a skipped parity test reports green while asserting
//! nothing, which is the failure mode this pin exists to close.

use std::path::Path;

#[test]
fn emitted_processor_manifest_imports_match_the_in_process_extractor() {
    let kind = "processor-transplant";
    let tree = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../surface/dist/processor")
        .join(kind);
    let manifest_path = tree.join("manifest.json");
    assert!(
        manifest_path.exists(),
        "no transpiled tree at {} — build it with `make surface-transpile`",
        tree.display()
    );

    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&manifest_path).expect("read the emitted manifest"))
            .expect("the emitted manifest parses as JSON");
    let emitted: Vec<String> = manifest["imports"]
        .as_array()
        .expect("the emitted manifest has an imports array")
        .iter()
        .map(|v| {
            v.as_str()
                .expect("every emitted import is a string")
                .to_string()
        })
        .collect();

    let component = tree.join(format!("{kind}.component.wasm"));
    let extracted = brenn_wasm::processor_component_imports(&component);

    assert_eq!(
        emitted,
        extracted,
        "the build's manifest emitter and `processor_component_imports` disagree \
         about {}'s import profile — one of the two normalizations changed; they \
         must stay identical",
        component.display(),
    );
}
