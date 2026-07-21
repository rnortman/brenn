use std::path::Path;

use brenn_lib::access::acl::ChannelMatcher;
use brenn_lib::access::{AppCapability, AppPolicy};
use brenn_lib::config::SurfaceDescriptionConfig;
use brenn_lib::messaging::MessagingDirectory;
use brenn_lib::messaging::config::{
    ChannelConfigRaw, Depth, MessagingGlobalConfig, ResolvedComponent, ResolvedSurface,
    SurfaceBinding, SurfaceOutput, SurfaceSendBudget, build_channel_entries,
};

use super::*;
use crate::routes::surface::SingleWriterPrincipals;
use crate::routes::surface::test_fixtures::{directory_with, surface_outputting_to};

const PREFIX: &str = "surface";

/// A resolved surface: slug/skin, `(instance, kind)` components, one content
/// subscription per instance (`brenn:{slug}-{instance}` on port `messages`). All
/// ACL/policy/limit fields are inert defaults.
fn surface(slug: &str, skin: &str, components: &[(&str, &str)]) -> ResolvedSurface {
    let subscriptions: Vec<SurfaceBinding> = components
        .iter()
        .map(|(instance, _)| SurfaceBinding {
            channel_address: format!("brenn:{slug}-{instance}"),
            instance: (*instance).to_string(),
            port: "messages".to_string(),
            push_depth: 8,
            retain_depth: 0,
            noise: brenn_lib::messaging::config::NoiseLevel::Silent,
        })
        .collect();
    ResolvedSurface {
        slug: slug.to_string(),
        skin: skin.to_string(),
        components: components
            .iter()
            .map(|(instance, kind)| ResolvedComponent {
                instance: (*instance).to_string(),
                kind: (*kind).to_string(),
                abi: brenn_surface_proto::Abi::Dom,
                send_budget: SurfaceSendBudget::default(),
                parked_batch_depth: 8,
                config: Default::default(),
                chrome: false,
            })
            .collect(),
        subscriptions,
        durable_subscriptions: vec![],
        local_channels: vec![],
        outputs: vec![],
        policy: AppPolicy::default(),
        allowed_users: vec![],
        publish_burst: 60,
        publish_per_sec: 1,
    }
}

fn multi_surface_config() -> Vec<ResolvedSurface> {
    vec![
        surface(
            "bar",
            "bench",
            &[("p1", "protobar"), ("mode", "mode-clock")],
        ),
        surface("dev-stub", "bench", &[("dev", "echo-stub")]),
    ]
}

/// A directory declaring exactly the given bare channels, each at the given
/// standing retain depth, built the same way boot does.
fn directory_with_channels(bares: &[String], standing: Depth) -> MessagingDirectory {
    let raw: Vec<ChannelConfigRaw> = bares
        .iter()
        .enumerate()
        .map(|(i, b)| ChannelConfigRaw {
            uuid: format!("00000000-0000-4000-8000-{i:012x}"),
            address: b.clone(),
            description: None,
            push_depth: None,
            retain_depth: None,
            standing_retain_depth: Some(standing),
            noise: None,
            sink: None,
            wake_min: None,
        })
        .collect();
    let entries = build_channel_entries(&raw, &MessagingGlobalConfig::default());
    MessagingDirectory::with_entries(entries)
}

/// The per-surface runtime (geometry + status) bare channel names for `surfaces`.
fn runtime_bares(surfaces: &[ResolvedSurface]) -> Vec<String> {
    surfaces
        .iter()
        .flat_map(|s| {
            [
                format!("surface.surface.{}.geometry", s.slug),
                format!("surface.surface.{}.status", s.slug),
            ]
        })
        .collect()
}

/// A directory declaring the full derived set for `surfaces`: the boot-published
/// channels at `standing_retain_depth = 1` (retain default), and the runtime
/// geometry/status channels at `retain_depth = 1` AND `standing_retain_depth = 1`
/// (the stricter runtime rule). `runtime_retain` lets a test declare the runtime
/// channels with a non-conforming retain depth to exercise the bounded-retention
/// panic.
fn full_directory(
    surfaces: &[ResolvedSurface],
    runtime_retain: Option<Depth>,
) -> MessagingDirectory {
    let mut raw: Vec<ChannelConfigRaw> = Vec::new();
    let mut uuid = 0u64;
    let mut push = |address: String, retain: Option<Depth>, standing: Depth, uuid: &mut u64| {
        raw.push(ChannelConfigRaw {
            uuid: format!("00000000-0000-4000-8000-{uuid:012x}"),
            address,
            description: None,
            push_depth: None,
            retain_depth: retain,
            standing_retain_depth: Some(standing),
            noise: None,
            sink: None,
            wake_min: None,
        });
        *uuid += 1;
    };
    for bare in boot_published_bare_channels(PREFIX, surfaces) {
        push(bare, None, Depth::Bounded(1), &mut uuid);
    }
    for bare in runtime_bares(surfaces) {
        push(bare, runtime_retain, Depth::Bounded(1), &mut uuid);
    }
    let entries = build_channel_entries(&raw, &MessagingGlobalConfig::default());
    MessagingDirectory::with_entries(entries)
}

fn on_config() -> SurfaceDescriptionConfig {
    SurfaceDescriptionConfig {
        prefix: PREFIX.to_string(),
        status_interval_secs: 60,
    }
}

// ── Derived addresses ──────────────────────────────────────────────────────

#[test]
fn boot_published_channels_is_the_exact_derived_set() {
    let channels = boot_published_channels(PREFIX, &multi_surface_config());
    // index + 2 surface helps + (3 kinds * 2) = 9.
    assert!(channels.contains(&"brenn:surface.index".to_string()));
    assert!(channels.contains(&"brenn:surface.surface.bar.help".to_string()));
    assert!(channels.contains(&"brenn:surface.surface.dev-stub.help".to_string()));
    for kind in ["protobar", "mode-clock", "echo-stub"] {
        assert!(channels.contains(&format!("brenn:surface.kind.{kind}.help")));
        assert!(channels.contains(&format!("brenn:surface.kind.{kind}.schema")));
    }
    assert_eq!(channels.len(), 9);
}

#[test]
fn segment_structure_prevents_collisions() {
    // A kind literally named "layout" lives under `kind.layout.*`; a slug named
    // "kind" lives under `surface.kind.help`, never colliding with `kind.*`.
    let surfaces = vec![surface("kind", "bench", &[("w", "layout")])];
    let channels = boot_published_channels(PREFIX, &surfaces);
    assert!(channels.contains(&"brenn:surface.surface.kind.help".to_string()));
    assert!(channels.contains(&"brenn:surface.kind.layout.help".to_string()));
    assert!(channels.contains(&"brenn:surface.kind.layout.schema".to_string()));
    // All derived names are distinct.
    let mut sorted = channels.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), channels.len(), "no two derived names collide");
}

#[test]
fn doc_addresses_match_boot_published_set() {
    // The publish set and the validated set cannot drift: build_description_docs
    // keys its bodies with the same per-address helpers boot_published_channels
    // uses. (No sidecars needed — a missing-sidecar dir just yields stubs.)
    let surfaces = multi_surface_config();
    let docs = build_description_docs(PREFIX, "b", &surfaces, Path::new("/nonexistent-dist"));
    let mut doc_addrs: Vec<String> = docs.iter().map(|(a, _)| a.clone()).collect();
    let mut expected = boot_published_channels(PREFIX, &surfaces);
    doc_addrs.sort();
    expected.sort();
    assert_eq!(doc_addrs, expected);
}

// ── Document builders ──────────────────────────────────────────────────────

#[test]
fn index_lists_every_surface_and_kind_address() {
    let surfaces = multi_surface_config();
    let docs = build_description_docs(PREFIX, "build-xyz", &surfaces, Path::new("/nonexistent"));
    let (_, index) = docs
        .iter()
        .find(|(a, _)| a == "brenn:surface.index")
        .expect("index present");
    assert!(index.contains("build-xyz"));
    assert!(index.contains("brenn:surface.surface.bar.help"));
    assert!(index.contains("brenn:surface.surface.dev-stub.help"));
    assert!(index.contains("brenn:surface.kind.protobar.help"));
    assert!(index.contains("brenn:surface.kind.protobar.schema"));
}

#[test]
fn surface_help_lists_instances_channels_and_pointers() {
    let surfaces = multi_surface_config();
    let docs = build_description_docs(PREFIX, "b", &surfaces, Path::new("/nonexistent"));
    let (_, help) = docs
        .iter()
        .find(|(a, _)| a == "brenn:surface.surface.bar.help")
        .expect("bar help present");
    // Identity + instance table with kind help links.
    assert!(help.contains("Surface `bar`"));
    assert!(help.contains("`p1`"));
    assert!(help.contains("brenn:surface.kind.protobar.help"));
    // Content channel table.
    assert!(help.contains("brenn:bar-p1"));
    // Geometry + status pointers.
    assert!(help.contains("brenn:surface.surface.bar.geometry"));
    assert!(help.contains("brenn:surface.surface.bar.status"));
}

#[test]
fn kind_help_embeds_sidecar_verbatim_under_header() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("brenn_protobar.help.md"),
        "PROTOBAR PAYLOAD DOCS: publish text or a JSON object.",
    )
    .unwrap();
    let surfaces = multi_surface_config();
    let help = build_kind_help("protobar", &surfaces, dir.path(), "2026-07-13T00:00:00Z");
    // Generated header names the kind, module, and mounting surface/channel.
    assert!(help.contains("Component kind `protobar`"));
    assert!(help.contains("brenn_protobar.js"));
    assert!(help.contains("surface `bar`"));
    assert!(help.contains("brenn:bar-p1"));
    // Sidecar body verbatim.
    assert!(help.contains("PROTOBAR PAYLOAD DOCS: publish text or a JSON object."));
}

#[test]
fn kind_help_missing_sidecar_produces_stub() {
    let dir = tempfile::tempdir().unwrap();
    let surfaces = multi_surface_config();
    let help = build_kind_help("protobar", &surfaces, dir.path(), "2026-07-13T00:00:00Z");
    assert!(help.contains("Component kind `protobar`"));
    assert!(help.contains("ships no documentation"));
}

#[test]
fn kind_schema_carries_verbatim_json_or_null() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("brenn_protobar.schema.json"),
        r#"{"type":"object","properties":{"text":{"type":"string"}}}"#,
    )
    .unwrap();
    // With a sidecar: schema embedded verbatim.
    let body = build_kind_schema("protobar", dir.path(), "2026-07-13T00:00:00Z");
    let doc: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(doc["v"], 1);
    assert_eq!(doc["kind"], "protobar");
    assert_eq!(doc["schema"]["type"], "object");
    assert_eq!(doc["schema"]["properties"]["text"]["type"], "string");
    assert!(doc["ts"].is_string());

    // No sidecar (echo-stub): schema is JSON null, channel still uniform.
    let body = build_kind_schema("echo-stub", dir.path(), "2026-07-13T00:00:00Z");
    let doc: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(doc["kind"], "echo-stub");
    assert_eq!(doc["schema"], serde_json::Value::Null);
}

#[test]
#[should_panic(expected = "is not valid JSON")]
fn kind_schema_malformed_sidecar_panics() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("brenn_protobar.schema.json"),
        "{ this is not json",
    )
    .unwrap();
    let _ = build_kind_schema("protobar", dir.path(), "2026-07-13T00:00:00Z");
}

// ── §9 dimensions vocabulary ─────────────────────────────────────────────────

#[test]
fn kind_schema_dimensions_null_when_absent() {
    let dir = tempfile::tempdir().unwrap();
    let body = build_kind_schema("protobar", dir.path(), "2026-07-13T00:00:00Z");
    let doc: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(
        doc.as_object().unwrap().contains_key("dimensions"),
        "dimensions is an always-present field"
    );
    assert_eq!(doc["dimensions"], serde_json::Value::Null);
}

#[test]
fn kind_schema_dimensions_published_validated() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("brenn_protobar.dimensions.json"),
        r#"{"v":1,"min_width":320,"max_width":1280,"min_height":200}"#,
    )
    .unwrap();
    let body = build_kind_schema("protobar", dir.path(), "2026-07-13T00:00:00Z");
    let doc: serde_json::Value = serde_json::from_str(&body).unwrap();
    let dims = &doc["dimensions"];
    assert_eq!(dims["v"], 1);
    assert_eq!(dims["min_width"], 320);
    assert_eq!(dims["max_width"], 1280);
    assert_eq!(dims["min_height"], 200);
    // Absent axis bound is not serialized (skip_serializing_if).
    assert!(dims.as_object().unwrap().get("max_height").is_none());
}

#[test]
#[should_panic(expected = "not a valid dimensions document")]
fn kind_schema_dimensions_malformed_panics() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("brenn_protobar.dimensions.json"),
        "{ not json",
    )
    .unwrap();
    let _ = build_kind_schema("protobar", dir.path(), "2026-07-13T00:00:00Z");
}

#[test]
#[should_panic(expected = "not a valid dimensions document")]
fn kind_schema_dimensions_unknown_field_panics() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("brenn_protobar.dimensions.json"),
        r#"{"v":1,"min_width":320,"depth":10}"#,
    )
    .unwrap();
    let _ = build_kind_schema("protobar", dir.path(), "2026-07-13T00:00:00Z");
}

#[test]
#[should_panic(expected = "sets no bound")]
fn kind_schema_dimensions_empty_panics() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("brenn_protobar.dimensions.json"),
        r#"{"v":1}"#,
    )
    .unwrap();
    let _ = build_kind_schema("protobar", dir.path(), "2026-07-13T00:00:00Z");
}

#[test]
#[should_panic(expected = "inverts an axis")]
fn kind_schema_dimensions_min_gt_max_panics() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("brenn_protobar.dimensions.json"),
        r#"{"v":1,"min_width":1280,"max_width":320}"#,
    )
    .unwrap();
    let _ = build_kind_schema("protobar", dir.path(), "2026-07-13T00:00:00Z");
}

/// The height axis is the second conjunct of the inversion check; without this
/// the conjunct could be deleted or transposed with the suite still green.
#[test]
#[should_panic(expected = "inverts an axis")]
fn kind_schema_dimensions_min_gt_max_height_panics() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("brenn_protobar.dimensions.json"),
        r#"{"v":1,"min_height":800,"max_height":200}"#,
    )
    .unwrap();
    let _ = build_kind_schema("protobar", dir.path(), "2026-07-13T00:00:00Z");
}

/// `min == max` is a legitimate fixed-size kind: the boundary is `lo <= hi`, and
/// an off-by-one to `lo < hi` would silently reject it.
#[test]
fn kind_schema_dimensions_fixed_axis_accepted() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("brenn_protobar.dimensions.json"),
        r#"{"v":1,"min_width":640,"max_width":640}"#,
    )
    .unwrap();
    let body = build_kind_schema("protobar", dir.path(), "2026-07-13T00:00:00Z");
    let doc: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(doc["dimensions"]["min_width"], 640);
    assert_eq!(doc["dimensions"]["max_width"], 640);
}

/// Version gating on a deploy-time artifact must not decay: a sidecar at any
/// other `v` is refused rather than read under the wrong schema.
#[test]
#[should_panic(expected = "only v = ")]
fn kind_schema_dimensions_wrong_version_panics() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("brenn_protobar.dimensions.json"),
        r#"{"v":2,"min_width":320}"#,
    )
    .unwrap();
    let _ = build_kind_schema("protobar", dir.path(), "2026-07-13T00:00:00Z");
}

// ── §9 instance-config channel reservation ───────────────────────────────────

#[test]
fn instance_config_grammar_is_pinned() {
    assert_eq!(
        instance_config_bare(PREFIX, "bar", "p1"),
        "surface.surface.bar.instance.p1.config"
    );
    assert_eq!(
        instance_config_channel(PREFIX, "bar", "p1"),
        "brenn:surface.surface.bar.instance.p1.config"
    );
}

#[test]
fn instance_config_address_collides_with_no_boot_published_address() {
    // The reserved name must not equal any boot-published address, even for
    // overlapping slug/name/kind strings (a kind and an instance both "p1", a
    // slug "instance", etc.).
    let surfaces = vec![
        surface("bar", "bench", &[("p1", "p1"), ("instance", "config")]),
        surface("instance", "bench", &[("config", "bar")]),
    ];
    let boot: std::collections::BTreeSet<String> = boot_published_channels(PREFIX, &surfaces)
        .into_iter()
        .collect();
    for s in &surfaces {
        for c in &s.components {
            let reserved = instance_config_channel(PREFIX, &s.slug, &c.instance);
            assert!(
                !boot.contains(&reserved),
                "reserved instance-config address {reserved} collides with a boot-published address"
            );
        }
    }
}

// ── Publisher spec ─────────────────────────────────────────────────────────

#[test]
fn surface_help_spec_has_exact_publish_acl_per_channel_and_no_subscriptions() {
    let surfaces = multi_surface_config();
    let bares = boot_published_bare_channels(PREFIX, &surfaces);
    let spec = surface_help_spec(&bares);
    assert_eq!(spec.component, SURFACE_HELP_COMPONENT);
    assert!(
        spec.subscriptions.is_empty(),
        "publish-only: no subscriptions"
    );
    assert!(spec.policy.grants.has(AppCapability::MessagingPublish));
    assert_eq!(
        spec.policy.acls.brenn_publish.len(),
        bares.len(),
        "one exact matcher per boot-published channel"
    );
    for bare in &bares {
        assert!(
            spec.policy
                .acls
                .brenn_publish
                .contains(&ChannelMatcher::Exact(bare.clone())),
            "spec must grant exact publish on {bare}"
        );
    }
    // Bare names carry no scheme prefix.
    assert!(bares.iter().all(|b| !b.contains(':')));
}

// ── Boot validation ────────────────────────────────────────────────────────

#[test]
fn validate_passes_for_valid_config() {
    let surfaces = multi_surface_config();
    let dir = full_directory(&surfaces, Some(Depth::Bounded(1)));
    validate_surface_description(
        &on_config(),
        &surfaces,
        Some(&dir),
        SingleWriterPrincipals {
            surfaces: &surfaces,
            ..Default::default()
        },
    );
}

#[test]
#[should_panic(expected = "written forever")]
fn validate_panics_on_unbounded_runtime_retain_depth() {
    // A geometry/status channel with a default (unbounded) retain_depth is
    // unbounded DB growth by design — a boot error.
    let surfaces = multi_surface_config();
    let dir = full_directory(&surfaces, None);
    validate_surface_description(
        &on_config(),
        &surfaces,
        Some(&dir),
        SingleWriterPrincipals {
            surfaces: &surfaces,
            ..Default::default()
        },
    );
}

#[test]
#[should_panic(expected = "single-writer")]
fn validate_panics_on_foreign_writer_of_runtime_channel() {
    // A foreign surface whose output binding targets another surface's status
    // channel breaks the runtime single-writer premise.
    let surfaces = multi_surface_config();
    let dir = full_directory(&surfaces, Some(Depth::Bounded(1)));
    let foreign = surface_outputting_to("brenn:surface.surface.bar.status");
    let principals: Vec<ResolvedSurface> = surfaces
        .iter()
        .cloned()
        .chain(std::iter::once(foreign))
        .collect();
    validate_surface_description(
        &on_config(),
        &surfaces,
        Some(&dir),
        SingleWriterPrincipals {
            surfaces: &principals,
            ..Default::default()
        },
    );
}

/// The sub-identity rule: a component of the **owning** surface is a foreign
/// writer to that surface's own runtime channel. The owning surface's exemption
/// covers its kernel identity (`surface:bar`, the geometry/status grant); its
/// components publish under `surface:bar#<instance>`, a different principal, and the
/// only way one can reach a channel is a bound output port — so the binding is
/// rejected even though the surface itself is the sanctioned writer.
#[test]
#[should_panic(expected = "single-writer")]
fn validate_panics_on_owning_surfaces_own_component_writing_runtime_channel() {
    let mut surfaces = multi_surface_config();
    // `bar` binds one of its own components' output ports at its own status
    // channel — the exact case the owner-exclusion in the policy sweep must not
    // be read as blessing.
    surfaces[0].outputs.push(SurfaceOutput {
        channel_address: "brenn:surface.surface.bar.status".to_string(),
        instance: "p1".to_string(),
        port: "out".to_string(),
        default_urgency: Urgency::Normal,
        budget: brenn_budget::SinkBudget {
            fill_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
            capacity_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
        },
    });
    let dir = full_directory(&surfaces, Some(Depth::Bounded(1)));
    let principals = surfaces.clone();
    validate_surface_description(
        &on_config(),
        &surfaces,
        Some(&dir),
        SingleWriterPrincipals {
            surfaces: &principals,
            ..Default::default()
        },
    );
}

#[test]
fn validate_accepts_owning_surface_geometry_status_grant() {
    // The owning surface's injected geometry/status grant is the sanctioned
    // single-writer coverage — it must not trip the sweep.
    let mut surfaces = multi_surface_config();
    for surface in &mut surfaces {
        surface
            .policy
            .grants
            .insert(AppCapability::MessagingPublish);
        surface
            .policy
            .acls
            .brenn_publish
            .push(ChannelMatcher::Exact(format!(
                "surface.surface.{}.geometry",
                surface.slug
            )));
        surface
            .policy
            .acls
            .brenn_publish
            .push(ChannelMatcher::Exact(format!(
                "surface.surface.{}.status",
                surface.slug
            )));
    }
    let dir = full_directory(&surfaces, Some(Depth::Bounded(1)));
    validate_surface_description(
        &on_config(),
        &surfaces,
        Some(&dir),
        SingleWriterPrincipals {
            surfaces: &surfaces,
            ..Default::default()
        },
    );
}

#[test]
#[should_panic(expected = "no matching [[channel]] declaration")]
fn validate_panics_and_aggregates_missing_declarations() {
    let surfaces = multi_surface_config();
    let mut bares = boot_published_bare_channels(PREFIX, &surfaces);
    // Drop two declarations; the panic must name both.
    bares.retain(|b| b != "surface.index" && b != "surface.kind.echo-stub.schema");
    let dir = directory_with_channels(&bares, Depth::Bounded(1));
    validate_surface_description(
        &on_config(),
        &surfaces,
        Some(&dir),
        SingleWriterPrincipals::default(),
    );
}

#[test]
#[should_panic(expected = "standing_retain_depth")]
fn validate_panics_on_zero_standing_retain_depth() {
    let surfaces = multi_surface_config();
    let bares = boot_published_bare_channels(PREFIX, &surfaces);
    let dir = directory_with_channels(&bares, Depth::Bounded(0));
    validate_surface_description(
        &on_config(),
        &surfaces,
        Some(&dir),
        SingleWriterPrincipals::default(),
    );
}

#[test]
#[should_panic(expected = "well-formed bare-name segment")]
fn validate_panics_on_malformed_prefix() {
    let config = SurfaceDescriptionConfig {
        prefix: "bad/prefix".to_string(),
        status_interval_secs: 60,
    };
    validate_surface_description(&config, &[], None, SingleWriterPrincipals::default());
}

#[test]
#[should_panic(expected = "out of range 5..=3600")]
fn validate_panics_on_status_interval_below_floor() {
    let config = SurfaceDescriptionConfig {
        prefix: PREFIX.to_string(),
        status_interval_secs: 4,
    };
    validate_surface_description(&config, &[], None, SingleWriterPrincipals::default());
}

#[test]
#[should_panic(expected = "out of range 5..=3600")]
fn validate_panics_on_status_interval_above_ceiling() {
    let config = SurfaceDescriptionConfig {
        prefix: PREFIX.to_string(),
        status_interval_secs: 3601,
    };
    validate_surface_description(&config, &[], None, SingleWriterPrincipals::default());
}

/// No messaging configured ⇒ no directory and no surfaces, so there are no
/// derived channels to validate and the sweep is a no-op. The parameter checks
/// still run (see the malformed-prefix and interval tests, which pass `None`).
#[test]
fn validate_noop_when_messaging_absent() {
    validate_surface_description(&on_config(), &[], None, SingleWriterPrincipals::default());
}

#[test]
#[should_panic(expected = "single-writer")]
fn validate_panics_on_foreign_writer() {
    // No described surfaces ⇒ derived set is just the index. A foreign
    // surface whose output binding targets it breaks single-writer.
    let bares: Vec<String> = boot_published_bare_channels(PREFIX, &[]);
    let dir = directory_with_channels(&bares, Depth::Bounded(1));
    let foreign = surface_outputting_to("brenn:surface.index");
    validate_surface_description(
        &on_config(),
        &[],
        Some(&dir),
        SingleWriterPrincipals {
            surfaces: std::slice::from_ref(&foreign),
            ..Default::default()
        },
    );
}

#[test]
fn validate_no_surfaces_still_validates_index() {
    // Feature on, zero surfaces: the index doc is still derived, declared, and
    // validated (the discovery entry point must exist).
    let bares = boot_published_bare_channels(PREFIX, &[]);
    assert_eq!(bares.len(), 1);
    let dir = directory_with_channels(&bares, Depth::Bounded(1));
    validate_surface_description(
        &on_config(),
        &[],
        Some(&dir),
        SingleWriterPrincipals::default(),
    );
}

// A sanity check that `directory_with` still resolves a single channel (used by
// the shared single-writer suites) so this module's fixtures stay compatible.
#[test]
fn directory_with_resolves_single_channel() {
    let dir = directory_with("surface.index");
    assert!(dir.resolve("brenn:surface.index").is_some());
}
