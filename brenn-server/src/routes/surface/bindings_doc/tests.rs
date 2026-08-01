use std::collections::BTreeMap;

use brenn_lib::access::{AppCapability, AppPolicy};
use brenn_lib::messaging::config::{ResolvedLocalChannel, ResolvedSurface};
use brenn_surface_schema::{LOCAL_THEME_CHANNEL, LogLevel, reserved_local_channel};

use super::*;
use crate::test_support::surface::SurfaceFixture;

const PREFIX: &str = "surface";

fn params() -> BindingsDocParams<'static> {
    BindingsDocParams {
        prefix: PREFIX,
        status_interval_secs: 60,
        error_report: Some(("brenn:surface-errors", LogLevel::Warn)),
    }
}

/// A surface with one chrome component, a durable input, a local output, and a
/// headless processor carrying a config map — enough shape that every document
/// section is non-empty.
fn bar() -> ResolvedSurface {
    let mut config = BTreeMap::new();
    config.insert("zeta".to_string(), "1".to_string());
    config.insert("alpha".to_string(), "2".to_string());
    let mut surface = SurfaceFixture::new("bar", "chrome")
        .processor("mode", "mode-clock", config)
        .subscribe("brenn:bar-a", "chrome", "content")
        .output(LOCAL_THEME_CHANNEL, "mode", "theme")
        .build();
    surface.local_channels.push(ResolvedLocalChannel {
        address: LOCAL_THEME_CHANNEL.to_string(),
        ring_depth: reserved_local_channel(LOCAL_THEME_CHANNEL)
            .expect("the theme plane is reserved")
            .ring_depth,
    });
    surface
}

/// `doc_noise` is a four-arm hand-written match between two same-named,
/// same-ordered enums: transposing `Alarm` and `Fatal` compiles clean and ships
/// an overflow that should toast as one that kills the instance. Each rung is
/// pinned to its own.
#[test]
fn doc_noise_maps_every_rung_to_its_own() {
    use brenn_lib::messaging::config::NoiseLevel as N;
    assert_eq!(doc_noise(N::Silent), DocNoiseLevel::Silent);
    assert_eq!(doc_noise(N::Metered), DocNoiseLevel::Metered);
    assert_eq!(doc_noise(N::Alarm), DocNoiseLevel::Alarm);
    assert_eq!(doc_noise(N::Fatal), DocNoiseLevel::Fatal);
}

#[test]
fn document_carries_every_resolved_section() {
    let doc = build_bindings_document(&bar(), &params());
    assert_eq!(doc.v, BINDINGS_DOCUMENT_VERSION);
    assert_eq!(doc.chrome_instance, "chrome");
    assert_eq!(doc.components.len(), 2);
    let mode = doc
        .components
        .iter()
        .find(|c| c.instance == "mode")
        .expect("the processor instance reaches the document");
    assert_eq!(mode.kind, "mode-clock");
    assert_eq!(mode.config.get("alpha").map(String::as_str), Some("2"));
    assert_eq!(doc.subscriptions.len(), 1);
    assert_eq!(doc.subscriptions[0].channel, "brenn:bar-a");
    assert_eq!(doc.outputs.len(), 1);
    assert_eq!(doc.outputs[0].channel, LOCAL_THEME_CHANNEL);
    assert_eq!(doc.local_channels.len(), 1);
    assert!(doc.validate().is_ok(), "the builder writes valid documents");
}

#[test]
fn platform_section_names_derived_addresses_and_the_error_pair() {
    let doc = build_bindings_document(&bar(), &params());
    assert_eq!(
        doc.platform.geometry_channel,
        "brenn:surface.surface.bar.geometry"
    );
    assert_eq!(
        doc.platform.status_channel,
        "brenn:surface.surface.bar.status"
    );
    assert_eq!(doc.platform.status_interval_secs, 60);
    assert_eq!(
        doc.platform.error_channel.as_deref(),
        Some("brenn:surface-errors")
    );
    assert_eq!(doc.platform.error_report_floor, Some(LogLevel::Warn));
}

/// No configured error channel ⇒ neither half is set.
#[test]
fn an_unconfigured_error_channel_clears_both_halves() {
    let doc = build_bindings_document(
        &bar(),
        &BindingsDocParams {
            error_report: None,
            ..params()
        },
    );
    assert_eq!(doc.platform.error_channel, None);
    assert_eq!(doc.platform.error_report_floor, None);
    assert!(doc.validate().is_ok());
}

#[test]
fn takeover_grant_reaches_the_platform_section() {
    let without = build_bindings_document(&bar(), &params());
    assert!(!without.platform.takeover_granted);

    let mut policy = AppPolicy::default();
    policy.grants.insert(AppCapability::SurfaceTakeover);
    let mut granted = bar();
    granted.policy = policy;
    let with = build_bindings_document(&granted, &params());
    assert!(with.platform.takeover_granted);
}

/// Determinism contract: same config, same bytes — consumers may use byte
/// equality to detect changes. Built from scratch twice rather than cloned, so a
/// field serialized from an unordered collection would flake here.
#[test]
fn documents_are_byte_stable_across_rebuilds() {
    let first = build_bindings_documents(std::slice::from_ref(&bar()), &params());
    let second = build_bindings_documents(std::slice::from_ref(&bar()), &params());
    assert_eq!(first, second);
}

#[test]
fn changed_config_changes_the_body() {
    let before = build_bindings_documents(std::slice::from_ref(&bar()), &params());
    let mut changed = bar();
    changed.subscriptions[0].push_depth += 1;
    let after = build_bindings_documents(std::slice::from_ref(&changed), &params());
    assert_ne!(before[0].1, after[0].1);
    assert_eq!(before[0].0, after[0].0, "the address is not what changed");
}

#[test]
fn documents_are_addressed_to_each_surfaces_config_channel() {
    let surfaces = vec![bar(), SurfaceFixture::new("dev-stub", "echo-stub").build()];
    let docs = build_bindings_documents(&surfaces, &params());
    let addresses: Vec<&str> = docs.iter().map(|(a, _)| a.as_str()).collect();
    assert_eq!(
        addresses,
        vec![
            "ephemeral:surface.surface.bar.bindings",
            "ephemeral:surface.surface.dev-stub.bindings",
        ],
        "one document per surface, in declaration order"
    );
}

/// Round-trip through the wire form the kernel will read: the builder's output
/// parses back to an equal document.
#[test]
fn a_published_body_parses_back() {
    let docs = build_bindings_documents(std::slice::from_ref(&bar()), &params());
    let parsed = BindingsDocument::parse(&docs[0].1).expect("the builder writes readable bodies");
    assert_eq!(parsed, build_bindings_document(&bar(), &params()));
}

/// A local binding whose router entry was lost is unresolvable wiring — the
/// builder refuses rather than publishing a document no surface can apply.
#[test]
#[should_panic(expected = "does not validate")]
fn build_panics_on_a_document_that_fails_schema_validation() {
    let mut broken = bar();
    broken.local_channels.clear();
    build_bindings_documents(std::slice::from_ref(&broken), &params());
}
