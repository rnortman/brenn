// Integration tests for the grant-check mechanism (§8 items 1-10).
//
// Tests cover:
//   1. Ungranted known capability → named panic at load.
//   2. Unrecognized import → named panic at load.
//   3. Multiple violations listed in one panic.
//   4. Subset success: component imports subset of granted capabilities → loads.
//   5. Degenerate empty grants — load: minimal no-import WAT passes the grant check.
//   6. Degenerate empty grants — invoke: component importing only `types` invokes ok.
//   7. Superset: granted ⊃ component imports → loads.
//   8. Drift guard: every fixture import satisfies is_types_import or from_import_name.
//   9. Map round-trip + semver unit tests.
//  10. Invariant assert: store_path/Store grant mismatch panics in load.

use std::collections::BTreeSet;
use std::io::Write as _;
use std::sync::Arc;

use brenn_wasm::{
    Capability, ProcessorActivation, ProcessorComponent, ProcessorLoadSpec,
    store::DEFAULT_MAX_PAGE_COUNT,
};

mod common;

fn noop_alerter() -> Arc<dyn brenn_wasm::ProcessorAlerter> {
    common::noop_alerter()
}

fn component_path(name: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target/components")
        .join(format!("{name}.wasm"))
}

/// Compile inline WAT to a `.wasm` tempfile.
fn wat_to_tempfile(wat_src: &str) -> tempfile::NamedTempFile {
    let wasm_bytes = wat::parse_str(wat_src).unwrap_or_else(|e| panic!("WAT parse failed: {e}"));
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(&wasm_bytes).unwrap();
    f.flush().unwrap();
    f
}

// ── Item 1: ungranted known capability → named panic ─────────────────────────

/// Loading brenn_processor_demo (imports types + ports) with grants = {} panics
/// with a message naming the ungranted capability ("ports").
#[test]
#[should_panic(expected = "requires ungranted capability \"ports\"")]
fn ungranted_known_capability_panics() {
    ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &component_path("brenn_processor_demo"),
        slug: "demo-no-grants",
        output_ports: std::collections::HashMap::new(),
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: std::collections::HashMap::new(),
        config: std::collections::HashMap::new(),
        grants: BTreeSet::new(),
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        alerter: noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_publish: None,
        tool_host: None,
    });
}

// ── Item 2: unrecognized import → named panic ─────────────────────────────────

/// A component importing a bogus interface triggers the "unrecognized interface"
/// branch of enforce_grants, naming the import in the panic message.
#[test]
#[should_panic(expected = "imports unrecognized interface")]
fn unrecognized_import_panics() {
    // A minimal component with only the bogus import; enforce_grants fires before
    // instantiate_pre, naming the import in the panic message.
    let bogus_wat = r#"(component
  (import "bogus:vendor/thing@0.1.0" (instance))
)"#;
    let wasm_file = wat_to_tempfile(bogus_wat);
    ProcessorComponent::load(ProcessorLoadSpec {
        component_path: wasm_file.path(),
        slug: "bogus-import",
        output_ports: std::collections::HashMap::new(),
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: std::collections::HashMap::new(),
        config: std::collections::HashMap::new(),
        grants: BTreeSet::new(),
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        alerter: noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_publish: None,
        tool_host: None,
    });
}

// ── Item 3: multiple violations listed ───────────────────────────────────────

/// brenn_processor_log imports types + log + alert; loading with grants = {} triggers
/// two named violations in one panic.
#[test]
#[should_panic(expected = "grant check failed")]
fn multiple_violations_listed() {
    ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &component_path("brenn_processor_log"),
        slug: "log-no-grants",
        output_ports: std::collections::HashMap::new(),
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: std::collections::HashMap::new(),
        config: std::collections::HashMap::new(),
        grants: BTreeSet::new(),
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        alerter: noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_publish: None,
        tool_host: None,
    });
}

/// Verify the panic from loading log fixture with no grants names BOTH log and alert.
/// Uses std::panic::catch_unwind to inspect the panic message.
#[test]
fn multiple_violations_names_both_capabilities() {
    let result = std::panic::catch_unwind(|| {
        ProcessorComponent::load(ProcessorLoadSpec {
            component_path: &component_path("brenn_processor_log"),
            slug: "log-no-grants-2",
            output_ports: std::collections::HashMap::new(),
            input_amplification_mt: common::amp_in(),
            mqtt_sinks: std::collections::HashMap::new(),
            config: std::collections::HashMap::new(),
            grants: BTreeSet::new(),
            store_path: None,
            max_page_count: DEFAULT_MAX_PAGE_COUNT,
            max_payload_bytes: 1024 * 1024,
            alerter: noop_alerter(),
            output_acl: common::allow_all(),
            mqtt_publish: None,
            tool_host: None,
        });
    });
    let err = result.expect_err("must panic");
    let msg = err
        .downcast_ref::<String>()
        .map(|s| s.as_str())
        .or_else(|| err.downcast_ref::<&str>().copied())
        .unwrap_or("<non-string panic>");
    assert!(
        msg.contains("\"log\""),
        "panic must mention capability \"log\"; got: {msg}"
    );
    assert!(
        msg.contains("\"alert\""),
        "panic must mention capability \"alert\"; got: {msg}"
    );
}

// ── Item 4: subset success ───────────────────────────────────────────────────

/// brenn_processor_log with grants = {log, alert} loads and invokes successfully.
#[test]
fn subset_grants_loads_and_invokes() {
    let grants = [Capability::Log, Capability::Alert].into_iter().collect();
    let comp = ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &component_path("brenn_processor_log"),
        slug: "log-subset",
        output_ports: std::collections::HashMap::new(),
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: std::collections::HashMap::new(),
        config: std::collections::HashMap::new(),
        grants,
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        alerter: noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_publish: None,
        tool_host: None,
    });
    // An empty activation (no new envelopes) returns Ok from the log fixture.
    let outcome = comp.handle(ProcessorActivation { ports: vec![] });
    assert!(
        matches!(outcome, brenn_wasm::ProcessorOutcome::Ok(_)),
        "subset-granted component must invoke ok; got {outcome:?}"
    );
}

// ── Item 5: degenerate empty grants — load only ──────────────────────────────

/// A minimal WAT component with no imports (not even types) passes the grant check
/// with grants = {} and loads successfully.
#[test]
fn degenerate_empty_grants_load_succeeds() {
    // Minimal component: no imports, exports a no-arg noop as `receive`.
    // The export type won't match the real WIT signature — that's fine; we only
    // care that enforce_grants passes. instantiate_pre / ProcessorPre::new may fail
    // (deferred type error), but load itself passes the grant check.
    let wat_src = r#"(component
  (core module $m
    (func (export "receive") (result i32) i32.const 0))
  (core instance $i (instantiate $m))
  (func (export "receive") (result s32) (canon lift (core func $i "receive")))
)"#;
    let wasm_file = wat_to_tempfile(wat_src);
    // Load must not panic in enforce_grants (the grant check passes: no imports).
    // The component may trap on invoke due to type mismatch — that's out of scope here.
    let _result = std::panic::catch_unwind(|| {
        ProcessorComponent::load(ProcessorLoadSpec {
            component_path: wasm_file.path(),
            slug: "noop-wat",
            output_ports: std::collections::HashMap::new(),
            input_amplification_mt: common::amp_in(),
            mqtt_sinks: std::collections::HashMap::new(),
            config: std::collections::HashMap::new(),
            grants: BTreeSet::new(),
            store_path: None,
            max_page_count: DEFAULT_MAX_PAGE_COUNT,
            max_payload_bytes: 1024 * 1024,
            alerter: noop_alerter(),
            output_acl: common::allow_all(),
            mqtt_publish: None,
            tool_host: None,
        })
    });
    // The grant check must pass: if it panicked, the message must NOT be a grant violation.
    if let Err(ref e) = _result {
        let msg = e
            .downcast_ref::<String>()
            .map(|s| s.as_str())
            .or_else(|| e.downcast_ref::<&str>().copied())
            .unwrap_or("<non-string panic>");
        assert!(
            !msg.contains("requires ungranted capability")
                && !msg.contains("unrecognized interface"),
            "panic must not be a grant violation for a no-import component; got: {msg}"
        );
    }
}

// ── Item 6: degenerate empty grants — invoke succeeds ───────────────────────

/// brenn_processor_exhaust imports only `types` (no capability interfaces).
/// With grants = {} and an empty activation (no new envelopes), invoke succeeds.
#[test]
fn degenerate_empty_grants_invoke_succeeds() {
    // processor-exhaust imports only types; with no new envelopes it returns Ok.
    let comp = ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &component_path("brenn_processor_exhaust"),
        slug: "exhaust-empty-grants",
        output_ports: std::collections::HashMap::new(),
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: std::collections::HashMap::new(),
        config: std::collections::HashMap::new(),
        grants: BTreeSet::new(),
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        alerter: noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_publish: None,
        tool_host: None,
    });
    // Empty activation: no new envelopes → exhaust returns Ok (no spin).
    let outcome = comp.handle(ProcessorActivation { ports: vec![] });
    assert!(
        matches!(outcome, brenn_wasm::ProcessorOutcome::Ok(_)),
        "zero-grant component with empty activation must invoke ok; got {outcome:?}"
    );
}

// ── Item 7: superset grants ──────────────────────────────────────────────────

/// Loading demo (imports types + ports) with all grants is fine.
/// Linker holds extra definitions that the component never imports; loads ok.
#[test]
fn superset_grants_loads() {
    let grants: BTreeSet<Capability> = Capability::ALL.iter().copied().collect();
    let db = tempfile::NamedTempFile::new().unwrap();
    let _comp = ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &component_path("brenn_processor_demo"),
        slug: "demo-all-grants",
        output_ports: std::collections::HashMap::new(),
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: std::collections::HashMap::new(),
        config: std::collections::HashMap::new(),
        grants,
        store_path: Some(db.path()),
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        alerter: noop_alerter(),
        output_acl: common::allow_all(),
        // ALL grants includes Mqtt; the load invariant requires a callback then.
        mqtt_publish: Some(common::ok_mqtt_publish()),
        // ALL grants includes Tools; the load invariant requires a seam then.
        tool_host: Some(common::noop_tool_host()),
    });
}

// ── Item 8: drift guard ──────────────────────────────────────────────────────

/// For every built fixture component, every import name either satisfies
/// `is_types_import` or resolves via `Capability::from_import_name`.
///
/// This test pins the artifact import strings against the Capability table;
/// a drift between the built components and the table surfaces here first.
///
/// semver_compat_match algorithm (lib.rs `semver_compat_match`) was re-diffed
/// against wasmtime-environ-45.0.1/src/component/names.rs:293-320
/// (`alternate_lookup_key`); rules are unchanged vs. wasmtime-26.
/// Re-diff required at the next major wasmtime bump.
#[test]
fn drift_guard_all_fixture_imports_recognized() {
    use wasmtime::{Config, Engine, component::Component};

    let mut cfg = Config::new();
    cfg.consume_fuel(true);
    cfg.epoch_interruption(true);
    let engine = Engine::new(&cfg)
        .unwrap_or_else(|e| panic!("failed to create engine for drift guard: {e}"));

    let fixtures = [
        "brenn_processor_demo",
        "brenn_processor_log",
        "brenn_processor_config",
        "brenn_processor_exhaust",
        "brenn_processor_mem_exhaust",
        "brenn_processor_dual",
        "brenn_processor_multiport",
        "brenn_processor_store_rt",
    ];

    for fixture in &fixtures {
        let path = component_path(fixture);
        let component = Component::from_file(&engine, &path)
            .unwrap_or_else(|e| panic!("drift guard: failed to load {fixture}: {e}"));
        let ct = component.component_type();
        for (name, _item) in ct.imports(&engine) {
            let recognized =
                brenn_wasm::is_types_import(name) || Capability::from_import_name(name).is_some();
            assert!(
                recognized,
                "drift guard: fixture {fixture:?} has unrecognized import {name:?} — \
                 update Capability::import_name table or WIT"
            );
        }
    }
}

// ── Item 9: map round-trip + semver unit tests ───────────────────────────────

/// grant_name / import_name / from_import_name round-trip over ALL capabilities.
#[test]
fn capability_map_round_trip() {
    for cap in &Capability::ALL {
        // from_import_name must recover the capability from its own import_name.
        let recovered = Capability::from_import_name(cap.import_name()).unwrap_or_else(|| {
            panic!(
                "from_import_name failed for {}: {:?}",
                cap.grant_name(),
                cap.import_name()
            )
        });
        assert_eq!(
            *cap,
            recovered,
            "round-trip mismatch for capability {:?}",
            cap.grant_name()
        );
    }
}

/// Semver rule: @0.1.1 resolves (same major.minor, 0.x rule).
#[test]
fn semver_compat_patch_bump_resolves() {
    // Simulate a component built against 0.1.1 on a 0.1.0 host.
    let name_101 = "brenn:processor/ports@0.1.1";
    let result = Capability::from_import_name(name_101);
    assert_eq!(
        result,
        Some(Capability::Ports),
        "0.1.1 must resolve to Ports against 0.1.0 host (same major.minor)"
    );
}

/// Semver rule: @0.2.0 does NOT resolve (different minor under 0.x).
#[test]
fn semver_incompat_minor_bump_does_not_resolve() {
    let name_020 = "brenn:processor/ports@0.2.0";
    let result = Capability::from_import_name(name_020);
    assert_eq!(
        result, None,
        "0.2.0 must not resolve against 0.1.0 host (different minor under 0.x)"
    );
}

/// Semver rule: @1.0.0 does NOT resolve (different major).
#[test]
fn semver_incompat_major_bump_does_not_resolve() {
    let name_100 = "brenn:processor/ports@1.0.0";
    let result = Capability::from_import_name(name_100);
    assert_eq!(
        result, None,
        "1.0.0 must not resolve against 0.1.0 host (different major)"
    );
}

/// is_types_import correctly identifies the types interface with a patch bump.
#[test]
fn is_types_import_patch_bump() {
    assert!(
        brenn_wasm::is_types_import("brenn:processor/types@0.1.1"),
        "types@0.1.1 must be recognized as types import (same major.minor)"
    );
    assert!(
        !brenn_wasm::is_types_import("brenn:processor/types@0.2.0"),
        "types@0.2.0 must not be recognized as types import (different minor)"
    );
}

/// Semver rule: @0.0.z exact — same patch resolves, different patch does not.
#[test]
fn semver_exact_00z_same_patch_resolves() {
    // Host capability at @0.0.1; guest also at @0.0.1 → exact match.
    // We construct a synthetic name using the ports interface name at 0.0.1.
    assert!(
        brenn_wasm::semver_compat_match(
            "brenn:processor/ports@0.0.1",
            "brenn:processor/ports@0.0.1"
        ),
        "0.0.1 must match 0.0.1 exactly (0.0.z rule)"
    );
}

/// Semver rule: @0.0.z exact — different patch does NOT resolve.
#[test]
fn semver_exact_00z_different_patch_does_not_resolve() {
    assert!(
        !brenn_wasm::semver_compat_match(
            "brenn:processor/ports@0.0.2",
            "brenn:processor/ports@0.0.1"
        ),
        "0.0.2 must not match 0.0.1 (0.0.z exact rule)"
    );
}

/// Semver rule: >=1.x — same major with different minor resolves.
#[test]
fn semver_ge1_same_major_different_minor_resolves() {
    assert!(
        brenn_wasm::semver_compat_match(
            "brenn:processor/ports@1.1.0",
            "brenn:processor/ports@1.0.0"
        ),
        "1.1.0 must resolve against 1.0.0 host (same major ≥1, minor difference allowed)"
    );
}

/// Semver rule: build metadata is stripped — @0.1.1+abc resolves against @0.1.0 host.
#[test]
fn semver_build_metadata_stripped_and_resolves() {
    let result = Capability::from_import_name("brenn:processor/ports@0.1.1+fork.1");
    assert_eq!(
        result,
        Some(Capability::Ports),
        "0.1.1+fork.1 must resolve to Ports against 0.1.0 host (build metadata ignored, same major.minor)"
    );
}

/// Semver rule: prerelease versions do NOT resolve (explicitly rejected).
#[test]
fn semver_prerelease_does_not_resolve() {
    let result = Capability::from_import_name("brenn:processor/ports@0.1.1-rc.1");
    assert_eq!(
        result, None,
        "0.1.1-rc.1 must not resolve (prerelease versions are incompatible per wasmtime rule)"
    );
}

/// The `Tools` capability round-trips through the grant/import name table and is
/// present in `ALL` — pinning the additive `tools` interface against the table so
/// a future drift between the map arms surfaces here.
#[test]
fn tools_capability_maps_and_is_in_all() {
    assert_eq!(Capability::Tools.grant_name(), "tools");
    assert_eq!(
        Capability::Tools.import_name(),
        "brenn:processor/tools@0.1.0"
    );
    assert_eq!(
        Capability::from_import_name("brenn:processor/tools@0.1.0"),
        Some(Capability::Tools),
        "the canonical tools import must resolve to Capability::Tools"
    );
    assert!(
        Capability::ALL.contains(&Capability::Tools),
        "Tools must be in Capability::ALL so enforce_grants/from_import_name see it"
    );
}

/// The tool-host seam and the `Tools` grant must agree at load — a `Tools` grant
/// without a seam is an out-of-tree wiring bug the invariant assert names.
#[test]
#[should_panic(
    expected = "tool_host seam (None) and Tools grant (granted) must both be set or both absent"
)]
fn tools_grant_without_seam_panics() {
    let grants: BTreeSet<Capability> = [Capability::Tools].into_iter().collect();
    ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &component_path("brenn_processor_exhaust"),
        slug: "tools-no-seam",
        output_ports: std::collections::HashMap::new(),
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: std::collections::HashMap::new(),
        config: std::collections::HashMap::new(),
        grants,
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        alerter: noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_publish: None,
        tool_host: None,
    });
}

// ── Item 10: invariant assert ────────────────────────────────────────────────

/// load with store_path = Some but no Store grant panics (invariant violated).
/// (The config layer prevents this; load defends against out-of-tree callers.)
/// The assert! in load fires before enforce_grants, naming the mismatch.
#[test]
#[should_panic(
    expected = "store_path (Some) and Store grant (not granted) must both be set or both absent"
)]
fn load_store_path_without_store_grant_panics() {
    let db = tempfile::NamedTempFile::new().unwrap();
    ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &component_path("brenn_processor_store_rt"),
        slug: "store-rt-no-grant",
        output_ports: std::collections::HashMap::new(),
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: std::collections::HashMap::new(),
        config: std::collections::HashMap::new(),
        grants: BTreeSet::new(), // Store not granted
        store_path: Some(db.path()),
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        alerter: noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_publish: None,
        tool_host: None,
    });
}

/// load with Store grant but store_path = None panics (invariant violated).
/// (The config layer prevents this; load defends against out-of-tree callers.)
///
/// This is the inverse of `load_store_path_without_store_grant_panics`: an
/// out-of-tree host that grants Store but forgets to supply the path gets an
/// assertion failure at load, not a runtime panic inside a host impl.
#[test]
#[should_panic(
    expected = "store_path (None) and Store grant (granted) must both be set or both absent"
)]
fn load_store_grant_without_store_path_panics() {
    ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &component_path("brenn_processor_store_rt"),
        slug: "store-rt-no-path",
        output_ports: std::collections::HashMap::new(),
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: std::collections::HashMap::new(),
        config: std::collections::HashMap::new(),
        grants: [Capability::Store].into_iter().collect(),
        store_path: None, // Store granted but no path
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        alerter: noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_publish: None,
        tool_host: None,
    });
}
