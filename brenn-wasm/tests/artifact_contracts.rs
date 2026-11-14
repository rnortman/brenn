// Which `brenn:processor` contract each in-tree artifact is built against, read
// off the artifact's own imports rather than asserted in prose.
//
// Every component built on the guest SDK, and every surface kind `//surface:dist`
// serves, is a `brenn:processor@0.2.0` artifact; the raw string-contract
// fixtures that keep the hosts' 0.1.0 path covered are `@0.1.0` ones. Every
// `brenn:processor` package the deploy manifest ships is checked the same way,
// off its own imports, whether or not a list here names it.

use std::path::PathBuf;

use brenn_envelope::grants::ProcessorContract;
use wasmtime::component::Component;
use wasmtime::{Config, Engine};

/// Components built on the guest SDK, by package name.
const SDK_COMPONENTS: &[&str] = &[
    "conversation-recycler",
    "git-forge-parser",
    "git-sync-consumer",
    "google-calendar-read",
    "google-calendar-write",
    "google-gmail-compose",
    "google-gmail-organize",
    "google-gmail-read",
    "google-maps-tools",
    "processor-blobs-test",
    "processor-call-test",
    "processor-config",
    "processor-demo",
    "processor-dual",
    "processor-log",
    "processor-mqtt-test",
    "processor-multiport",
    "processor-provider-test",
    "processor-sdk-bytes-test",
    "processor-store-rt",
    "processor-transplant",
];

/// The processor kinds `//surface:dist` serves, by kind name.
/// `processor-transplant` is served there too and is listed above.
const SURFACE_KINDS: &[&str] = &["chrome", "echo-stub", "meeting", "mode-clock", "protobar"];

/// The raw fixtures that stay on the string contract.
const STRING_FIXTURES: &[&str] = &[
    "processor-exhaust",
    "processor-mem-exhaust",
    "processor-page-test",
    "processor-tool-test",
];

fn artifact(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target/components")
        .join(format!("brenn_{}.wasm", name.replace('-', "_")))
}

fn engine() -> Engine {
    Engine::new(&Config::new()).expect("wasmtime engine")
}

/// The artifact's import names.
fn imports(engine: &Engine, name: &str) -> Vec<String> {
    let path = artifact(name);
    let component = Component::from_file(engine, &path)
        .unwrap_or_else(|e| panic!("the artifact {}: {e}", path.display()));
    component
        .component_type()
        .imports(engine)
        .map(|(import, _)| import.to_string())
        .collect()
}

fn contract(engine: &Engine, name: &str) -> ProcessorContract {
    let names = imports(engine, name);
    brenn_wasm::detect_contract(names.iter().map(String::as_str))
}

#[test]
fn every_sdk_component_and_served_kind_is_a_0_2_artifact() {
    let engine = engine();
    let wrong: Vec<(&str, ProcessorContract)> = SDK_COMPONENTS
        .iter()
        .chain(SURFACE_KINDS)
        .map(|name| (*name, contract(&engine, name)))
        .filter(|(_, contract)| *contract != ProcessorContract::V0_2)
        .collect();
    assert!(
        wrong.is_empty(),
        "built against a contract other than brenn:processor@0.2.0: {wrong:?}"
    );
}

#[test]
fn the_string_contract_fixtures_are_0_1_artifacts() {
    let engine = engine();
    let wrong: Vec<(&str, ProcessorContract)> = STRING_FIXTURES
        .iter()
        .map(|name| (*name, contract(&engine, name)))
        .filter(|(_, contract)| *contract != ProcessorContract::V0_1)
        .collect();
    assert!(
        wrong.is_empty(),
        "built against a contract other than brenn:processor@0.1.0: {wrong:?}"
    );
}

/// The deploy manifest's packages, as its readers parse it: one name per
/// line, blank lines and `#` comments skipped.
fn deployed() -> Vec<String> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("deployed-components.txt");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("the deploy manifest {}: {e}", path.display()));
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_owned)
        .collect()
}

#[test]
fn every_deployed_processor_package_is_a_0_2_artifact() {
    let engine = engine();
    let mut processors = 0;
    let mut wrong = Vec::new();
    for name in deployed() {
        let names = imports(&engine, &name);
        if !names
            .iter()
            .any(|import| import.starts_with("brenn:processor/"))
        {
            continue;
        }
        processors += 1;
        let contract = brenn_wasm::detect_contract(names.iter().map(String::as_str));
        if contract != ProcessorContract::V0_2 {
            wrong.push((name, contract));
        }
    }
    assert!(processors > 0, "the manifest names no processor package");
    assert!(
        wrong.is_empty(),
        "deployed packages built against a contract other than brenn:processor@0.2.0: {wrong:?}"
    );
}
