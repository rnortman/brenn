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
    "processor-blob-caller-test",
    "processor-blob-provider-test",
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

/// The members `brenn:processor/blobs@0.3.0` had before it gained the
/// tool-call forms. An artifact that names only these, and
/// [`BLOBS_PUBLISH_ERROR_ALIAS`], imports no function added since; no
/// function of the interface was removed or renamed, and the one
/// pre-existing type that changed is `publish-error`.
const BLOBS_MEMBERS_BEFORE_TOOL_CALLS: &[&str] = &[
    "blob",
    "blob-limits",
    "create",
    "create-error",
    "gap",
    "limits",
    "open",
    "open-error",
    "publish",
    "publish-error",
    "publish-options",
    "read-error",
    "seal-error",
    "urgency",
    "write-error",
    "writer",
    "[method]blob.size",
    "[method]blob.content-type",
    "[method]blob.read",
    "[method]writer.write",
    "[method]writer.seal",
    "[resource-drop]blob",
    "[resource-drop]writer",
    "body",
];

/// The one member the wrapping `publish-error` adds to an artifact that
/// imports `publish`: the alias of `ports.publish-error` it `use`s, exported
/// by the instance under its local name.
const BLOBS_PUBLISH_ERROR_ALIAS: &str = "ports-publish-error";

/// The member names an artifact imports from the interface `interface`.
fn instance_members(engine: &Engine, name: &str, interface: &str) -> Option<Vec<String>> {
    let path = artifact(name);
    let component = Component::from_file(engine, &path)
        .unwrap_or_else(|e| panic!("the artifact {}: {e}", path.display()));
    let ty = component.component_type();
    let (_, item) = ty
        .imports(engine)
        .find(|(import, _)| *import == interface)?;
    let wasmtime::component::types::ComponentItem::ComponentInstance(instance) = item.ty else {
        panic!("{name} imports {interface} as something other than an instance");
    };
    Some(
        instance
            .exports(engine)
            .map(|(member, _)| member.to_string())
            .collect(),
    )
}

/// `processor-blobs-test` calls none of the tool-call forms, so it imports
/// none of them: no function was added to its import, removed or renamed,
/// and its import of `publish` reaches `ports.publish-error` through the one
/// pre-existing type that changed. The existing suites load and run this
/// artifact. The provider fixture imports no `blobs` at all.
#[test]
fn the_blobs_fixture_imports_only_what_the_interface_had_before_tool_calls() {
    let engine = engine();
    let members = instance_members(
        &engine,
        "processor-blobs-test",
        "brenn:processor/blobs@0.3.0",
    )
    .expect("processor-blobs-test imports blobs");
    let added: Vec<&String> = members
        .iter()
        .filter(|m| {
            !BLOBS_MEMBERS_BEFORE_TOOL_CALLS.contains(&m.as_str())
                && *m != BLOBS_PUBLISH_ERROR_ALIAS
        })
        .collect();
    assert!(
        added.is_empty(),
        "processor-blobs-test imports blobs members it did not before: {added:?}"
    );
    assert!(members.iter().any(|m| m == "publish"), "{members:?}");
    assert!(
        members.iter().any(|m| m == BLOBS_PUBLISH_ERROR_ALIAS),
        "{members:?}"
    );
    for interface in [
        "brenn:processor/tools@0.1.0",
        "brenn:processor/provides@0.1.0",
    ] {
        assert_eq!(
            instance_members(&engine, "processor-blobs-test", interface),
            None,
            "processor-blobs-test imports {interface}"
        );
    }
    assert_eq!(
        instance_members(
            &engine,
            "processor-provider-test",
            "brenn:processor/blobs@0.3.0"
        ),
        None
    );
}

/// Whether `member` names a function rather than a type: a freestanding
/// function, a resource method, or a resource's drop.
fn is_function(engine: &Engine, name: &str, interface: &str, member: &str) -> bool {
    let path = artifact(name);
    let component = Component::from_file(engine, &path)
        .unwrap_or_else(|e| panic!("the artifact {}: {e}", path.display()));
    let ty = component.component_type();
    let (_, item) = ty
        .imports(engine)
        .find(|(import, _)| *import == interface)
        .unwrap_or_else(|| panic!("{name} does not import {interface}"));
    let wasmtime::component::types::ComponentItem::ComponentInstance(instance) = item.ty else {
        panic!("{name} imports {interface} as something other than an instance");
    };
    let (_, member_item) = instance
        .exports(engine)
        .find(|(m, _)| *m == member)
        .unwrap_or_else(|| panic!("{name}'s import of {interface} has no member {member}"));
    matches!(
        member_item.ty,
        wasmtime::component::types::ComponentItem::ComponentFunc(_)
    )
}

/// The caller fixture makes every tool call through `blobs.call-async` and
/// every publish through `blobs.publish`, and its class requires `tools` and
/// `ports`: it imports both interfaces for the error types those functions
/// reach, and no function of either. Its `grant_parity_test` holds the same
/// artifact to its specification, so a component whose only tool calls and
/// publishes carry attachments passes the build check.
#[test]
fn the_blob_caller_imports_tools_and_ports_for_their_types_alone() {
    let engine = engine();
    for interface in ["brenn:processor/tools@0.1.0", "brenn:processor/ports@0.2.0"] {
        let members = instance_members(&engine, "processor-blob-caller-test", interface)
            .unwrap_or_else(|| panic!("processor-blob-caller-test does not import {interface}"));
        let functions: Vec<&String> = members
            .iter()
            .filter(|m| is_function(&engine, "processor-blob-caller-test", interface, m))
            .collect();
        assert!(
            functions.is_empty(),
            "processor-blob-caller-test calls {interface} functions: {functions:?}"
        );
    }
    let blobs = instance_members(
        &engine,
        "processor-blob-caller-test",
        "brenn:processor/blobs@0.3.0",
    )
    .expect("processor-blob-caller-test imports blobs");
    for function in ["call-async", "publish"] {
        assert!(blobs.iter().any(|m| m == function), "{blobs:?}");
    }
}

/// The provider fixture completes through `blobs.complete` and nests through
/// `blobs.call-async-for`, and imports `tools` and `provides` for their types.
#[test]
fn the_blob_provider_imports_tools_and_provides() {
    let engine = engine();
    for interface in [
        "brenn:processor/tools@0.1.0",
        "brenn:processor/provides@0.1.0",
    ] {
        assert!(
            instance_members(&engine, "processor-blob-provider-test", interface).is_some(),
            "processor-blob-provider-test does not import {interface}"
        );
    }
    let blobs = instance_members(
        &engine,
        "processor-blob-provider-test",
        "brenn:processor/blobs@0.3.0",
    )
    .expect("processor-blob-provider-test imports blobs");
    for function in ["complete", "call-async-for"] {
        assert!(blobs.iter().any(|m| m == function), "{blobs:?}");
    }
}
