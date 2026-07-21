//! Surface self-description: the boot-published help/schema/index topology that
//! replaces the single-blob catalog.
//!
//! One configured `prefix` (`[surface_description]`) roots a family of retained
//! durable channels whose addresses are derived by convention (never templated
//! by consumers): an `index` entry point, a markdown `help` doc per surface, and
//! a markdown `help` + JSON `schema` pair per component kind. Every document is
//! built at boot
//! from the same resolved surfaces that produce `Welcome` bindings (no drift)
//! and published once under the reserved single-writer `system:surface-help`
//! identity. Readers (LLM conversations) pull the latest retained value via the
//! auto-approved `MessageChannelGet` tool; the shell never consumes these.
//!
//! Per-kind documentation is sourced from **sidecar files** shipped next to the
//! component module in the surface assets directory
//! (`brenn_<kind>.help.md` / `brenn_<kind>.schema.json`), so an out-of-tree kind
//! is documented with zero in-tree code change. A missing help sidecar produces
//! a generated stub (boot warning); a malformed schema sidecar is a boot panic.
//!
//! The runtime-published per-surface `geometry` and `status` channels share this
//! prefix; their addresses appear as pointers in the per-surface help doc, but
//! their declaration/validation and the frames that feed them are provisioned
//! with their writers elsewhere.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::Path;

use brenn_lib::config::SurfaceDescriptionConfig;
use brenn_lib::messaging::config::{Depth, ResolvedSurface};
use brenn_lib::messaging::gates::well_formed_name;
use brenn_lib::messaging::system::SystemParticipantSpec;
use brenn_lib::messaging::{
    ChannelScheme, MessagingDirectory, Messenger, PublishResult, Urgency, is_unreserved_char,
};
use brenn_surface_contract::module_artifact;
use serde::Deserialize;
use serde_json::json;

use super::{ExpectedWriter, SingleWriterPrincipals, assert_channel_single_writer};

/// System-participant component name of the boot description publisher; its
/// identity is `system:surface-help`.
pub const SURFACE_HELP_COMPONENT: &str = "surface-help";

/// Body-schema version stamped on every document (`v: 1`).
const SCHEMA_VERSION: u32 = 1;

// ── Derived addresses ──────────────────────────────────────────────────────
//
// Every address is `brenn:<prefix>.<family>...<leaf>`. Disjoint family segments
// (`index`, `surface.`, `kind.`) plus fixed leaf tokens mean two
// derivations collide only when their slug/kind strings are equal, which config
// resolution already forbids.

// The bare (scheme-less) name is the one derivation of each address; the
// `..._channel` wrappers add the `brenn:` scheme at the edge. Bare names are what
// the publisher spec's `brenn_publish` ACL and the per-surface grant injector
// consume, so both sides derive from the same helper and cannot drift.

/// `<prefix>.index`.
pub fn index_bare(prefix: &str) -> String {
    format!("{prefix}.index")
}

/// `<prefix>.surface.<slug>.help`.
pub fn surface_help_bare(prefix: &str, slug: &str) -> String {
    format!("{prefix}.surface.{slug}.help")
}

/// `<prefix>.surface.<slug>.geometry` — runtime-published.
pub fn surface_geometry_bare(prefix: &str, slug: &str) -> String {
    format!("{prefix}.surface.{slug}.geometry")
}

/// `<prefix>.surface.<slug>.status` — runtime-published.
pub fn surface_status_bare(prefix: &str, slug: &str) -> String {
    format!("{prefix}.surface.{slug}.status")
}

/// `<prefix>.kind.<kind>.help`.
pub fn kind_help_bare(prefix: &str, kind: &str) -> String {
    format!("{prefix}.kind.{kind}.help")
}

/// `<prefix>.kind.<kind>.schema`.
pub fn kind_schema_bare(prefix: &str, kind: &str) -> String {
    format!("{prefix}.kind.{kind}.schema")
}

/// `<prefix>.surface.<slug>.instance.<name>.config` — **reserved, unbuilt.**
///
/// The name §9 pins for future runtime per-instance config delivery. Nothing
/// publishes it, no `[[channel]]` declares it, and no machinery special-cases
/// it; declaring a binding on this address today takes the ordinary
/// unknown-channel path. This builder exists so the grammar is fixed (pinned by
/// test) and cannot collide with any boot-published address for overlapping
/// slug/name values.
#[allow(
    dead_code,
    reason = "reserved-unbuilt name (§9); no production caller by design"
)]
pub fn instance_config_bare(prefix: &str, slug: &str, name: &str) -> String {
    format!("{prefix}.surface.{slug}.instance.{name}.config")
}

/// Add the `brenn:` scheme to a derived bare name.
fn with_scheme(bare: String) -> String {
    format!("brenn:{bare}")
}

/// `brenn:<prefix>.index`.
pub fn index_channel(prefix: &str) -> String {
    with_scheme(index_bare(prefix))
}

/// `brenn:<prefix>.surface.<slug>.help`.
pub fn surface_help_channel(prefix: &str, slug: &str) -> String {
    with_scheme(surface_help_bare(prefix, slug))
}

/// `brenn:<prefix>.surface.<slug>.geometry` — runtime-published; here only as a
/// help-doc pointer.
pub fn surface_geometry_channel(prefix: &str, slug: &str) -> String {
    with_scheme(surface_geometry_bare(prefix, slug))
}

/// `brenn:<prefix>.surface.<slug>.status` — runtime-published; here only as a
/// help-doc pointer.
pub fn surface_status_channel(prefix: &str, slug: &str) -> String {
    with_scheme(surface_status_bare(prefix, slug))
}

/// `brenn:<prefix>.kind.<kind>.help`.
pub fn kind_help_channel(prefix: &str, kind: &str) -> String {
    with_scheme(kind_help_bare(prefix, kind))
}

/// `brenn:<prefix>.kind.<kind>.schema`.
pub fn kind_schema_channel(prefix: &str, kind: &str) -> String {
    with_scheme(kind_schema_bare(prefix, kind))
}

/// `brenn:<prefix>.surface.<slug>.instance.<name>.config` — **reserved,
/// unbuilt** (see [`instance_config_bare`]).
#[allow(
    dead_code,
    reason = "reserved-unbuilt name (§9); no production caller by design"
)]
pub fn instance_config_channel(prefix: &str, slug: &str, name: &str) -> String {
    with_scheme(instance_config_bare(prefix, slug, name))
}

/// Distinct component kinds appearing across `surfaces`, sorted for a stable
/// derived-channel set and stable documents.
fn distinct_kinds(surfaces: &[ResolvedSurface]) -> BTreeSet<String> {
    surfaces
        .iter()
        .flat_map(|s| s.components.iter().map(|c| c.kind.clone()))
        .collect()
}

/// The full set of **boot-published** channel addresses derived from `prefix`
/// and the resolved surfaces: the index, one help doc per surface, and a
/// help+schema pair per kind. The geometry/status channels
/// are runtime-published and are not in this set.
///
/// This is the single source of truth for both boot validation (each must
/// resolve to a declared `[[channel]]`) and the publish loop; the document
/// builder keys its bodies with the same per-address helpers, so the two cannot
/// drift (pinned by `doc_addresses_match_boot_published_set`).
pub fn boot_published_channels(prefix: &str, surfaces: &[ResolvedSurface]) -> Vec<String> {
    let mut channels = vec![index_channel(prefix)];
    for surface in surfaces {
        channels.push(surface_help_channel(prefix, &surface.slug));
    }
    for kind in distinct_kinds(surfaces) {
        channels.push(kind_help_channel(prefix, &kind));
        channels.push(kind_schema_channel(prefix, &kind));
    }
    channels
}

/// The boot-published channels as bare names, for the publisher spec's
/// exact-match `brenn_publish` ACL. Mirrors [`boot_published_channels`] exactly,
/// deriving each bare name from the same per-address helper (no scheme round-trip)
/// so the ACL matchers and the published addresses cannot drift.
pub fn boot_published_bare_channels(prefix: &str, surfaces: &[ResolvedSurface]) -> Vec<String> {
    let mut channels = vec![index_bare(prefix)];
    for surface in surfaces {
        channels.push(surface_help_bare(prefix, &surface.slug));
    }
    for kind in distinct_kinds(surfaces) {
        channels.push(kind_help_bare(prefix, &kind));
        channels.push(kind_schema_bare(prefix, &kind));
    }
    channels
}

/// The `system:surface-help` participant spec: publish-only, granted exactly
/// `MessagingPublish` + one exact-match `brenn_publish` ACL per derived
/// boot-published channel, no subscriptions. Code-built; no config produces it.
pub fn surface_help_spec(bare_channels: &[String]) -> SystemParticipantSpec {
    SystemParticipantSpec::publish_only(SURFACE_HELP_COMPONENT, bare_channels)
}

// ── Document builders ──────────────────────────────────────────────────────

/// Build every boot-published document as `(address, body)` pairs, in publish
/// order. Reads the per-kind sidecar files under `surface_dist_dir`: a missing
/// `.help.md` warns and yields a generated stub; a missing `.schema.json` yields
/// `schema: null`; a malformed `.schema.json` panics (a shipped machine-readable
/// artifact that is not valid JSON is operator/vendor error, worse to publish
/// than to fail fast).
pub fn build_description_docs(
    prefix: &str,
    build_id: &str,
    surfaces: &[ResolvedSurface],
    surface_dist_dir: &Path,
) -> Vec<(String, String)> {
    let ts = chrono::Utc::now().to_rfc3339();
    let mut docs = Vec::new();

    docs.push((
        index_channel(prefix),
        build_index(prefix, build_id, surfaces, &ts),
    ));

    for surface in surfaces {
        docs.push((
            surface_help_channel(prefix, &surface.slug),
            build_surface_help(prefix, build_id, surface, &ts),
        ));
    }

    for kind in distinct_kinds(surfaces) {
        docs.push((
            kind_help_channel(prefix, &kind),
            build_kind_help(&kind, surfaces, surface_dist_dir, &ts),
        ));
        docs.push((
            kind_schema_channel(prefix, &kind),
            build_kind_schema(&kind, surface_dist_dir, &ts),
        ));
    }

    docs
}

/// The index (markdown): the one address a reader memorizes. Lists every surface
/// (slug → help address), every kind (kind → help + schema addresses), the
/// prefix, the build id, and the generation timestamp.
fn build_index(prefix: &str, build_id: &str, surfaces: &[ResolvedSurface], ts: &str) -> String {
    let mut md = String::new();
    let _ = writeln!(md, "# Surface topology index");
    let _ = writeln!(md);
    let _ = writeln!(md, "- prefix: `{prefix}`");
    let _ = writeln!(md, "- build: `{build_id}`");
    let _ = writeln!(md, "- generated: {ts}");
    let _ = writeln!(md);
    let _ = writeln!(md, "## Surfaces");
    let _ = writeln!(md);
    if surfaces.is_empty() {
        let _ = writeln!(md, "_none configured_");
    } else {
        for surface in surfaces {
            let _ = writeln!(
                md,
                "- `{}` — help: `{}`",
                surface.slug,
                surface_help_channel(prefix, &surface.slug),
            );
        }
    }
    let _ = writeln!(md);
    let _ = writeln!(md, "## Component kinds");
    let _ = writeln!(md);
    let kinds = distinct_kinds(surfaces);
    if kinds.is_empty() {
        let _ = writeln!(md, "_none configured_");
    } else {
        for kind in &kinds {
            let _ = writeln!(
                md,
                "- `{}` — help: `{}`, schema: `{}`",
                kind,
                kind_help_channel(prefix, kind),
                kind_schema_channel(prefix, kind),
            );
        }
    }
    md
}

/// One surface's help doc (markdown): identity, the instance table (each linking
/// its kind's help channel), the content-channel table (where the LLM publishes
/// content), and the geometry/status channel pointers with one-line semantics.
fn build_surface_help(prefix: &str, build_id: &str, surface: &ResolvedSurface, ts: &str) -> String {
    let mut md = String::new();
    let _ = writeln!(md, "# Surface `{}`", surface.slug);
    let _ = writeln!(md);
    let _ = writeln!(md, "- skin: `{}`", surface.skin);
    let _ = writeln!(md, "- build: `{build_id}`");
    let _ = writeln!(md, "- generated: {ts}");
    let _ = writeln!(md);

    let _ = writeln!(md, "## Instances");
    let _ = writeln!(md);
    let _ = writeln!(md, "| instance | kind | kind help channel |");
    let _ = writeln!(md, "|---|---|---|");
    for comp in &surface.components {
        let _ = writeln!(
            md,
            "| `{}` | `{}` | `{}` |",
            comp.instance,
            comp.kind,
            kind_help_channel(prefix, &comp.kind),
        );
    }
    let _ = writeln!(md);

    let _ = writeln!(md, "## Content channels");
    let _ = writeln!(md);
    let _ = writeln!(
        md,
        "Channels the LLM publishes content to, per instance/port:"
    );
    let _ = writeln!(md);
    let _ = writeln!(md, "| channel | instance | port |");
    let _ = writeln!(md, "|---|---|---|");
    for binding in &surface.subscriptions {
        let _ = writeln!(
            md,
            "| `{}` | `{}` | `{}` |",
            binding.channel_address, binding.instance, binding.port,
        );
    }
    let _ = writeln!(md);

    let _ = writeln!(md, "## Geometry and status");
    let _ = writeln!(md);
    let _ = writeln!(
        md,
        "- geometry (latest viewport size, JSON, latest-wins): `{}`",
        surface_geometry_channel(prefix, &surface.slug),
    );
    let _ = writeln!(
        md,
        "- status (latest health/mount snapshot, JSON, latest-wins): `{}`",
        surface_status_channel(prefix, &surface.slug),
    );
    let _ = writeln!(
        md,
        "\nFull geometry/status field schemas ride the same pattern as the kind and layout schema channels."
    );
    md
}

/// One kind's help doc (markdown): a generated header (kind, module filename,
/// which surfaces mount it and on which content channels) followed by the kind's
/// sidecar markdown verbatim, or a stub when no `.help.md` ships.
fn build_kind_help(
    kind: &str,
    surfaces: &[ResolvedSurface],
    surface_dist_dir: &Path,
    ts: &str,
) -> String {
    let module = module_artifact(kind);
    let mut md = String::new();
    let _ = writeln!(md, "# Component kind `{kind}`");
    let _ = writeln!(md);
    let _ = writeln!(md, "- module: `{module}`");
    let _ = writeln!(md, "- generated: {ts}");
    let _ = writeln!(md);
    let _ = writeln!(md, "## Mounted by");
    let _ = writeln!(md);
    let mut any = false;
    for surface in surfaces {
        for comp in &surface.components {
            if comp.kind != kind {
                continue;
            }
            any = true;
            let channels: Vec<&str> = surface
                .subscriptions
                .iter()
                .filter(|b| b.instance == comp.instance)
                .map(|b| b.channel_address.as_str())
                .collect();
            let channels = if channels.is_empty() {
                "(no content channel)".to_string()
            } else {
                channels
                    .iter()
                    .map(|c| format!("`{c}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            let _ = writeln!(
                md,
                "- surface `{}`, instance `{}` — content channel(s): {}",
                surface.slug, comp.instance, channels,
            );
        }
    }
    if !any {
        let _ = writeln!(md, "_no surface currently mounts this kind_");
    }
    let _ = writeln!(md);
    let _ = writeln!(md, "## Documentation");
    let _ = writeln!(md);
    match read_help_sidecar(kind, surface_dist_dir) {
        Some(doc) => {
            let _ = writeln!(md, "{doc}");
        }
        None => {
            let _ = writeln!(
                md,
                "This component ships no documentation (`{}` not found next to `{module}`). \
                 Consult the component's own documentation for its payload format.",
                help_sidecar_filename(kind),
            );
        }
    }
    md
}

/// A kind's optional viewport-size preferences, authored in the
/// `brenn_<kind>.dimensions.json` sidecar. All fields are CSS px; every field is
/// optional, but at least one must be present, and where both bounds of an axis
/// are given `min <= max`. Consumed by chrome later (layout validation, LLM
/// hints) without a contract change — this phase defines, validates, and
/// publishes the vocabulary only.
#[derive(Debug, Clone, Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Dimensions {
    /// Schema version (`1`).
    v: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_width: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_width: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_height: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_height: Option<u32>,
}

/// One kind's schema doc (JSON): `{v, kind, schema, dimensions, ts}`, where
/// `schema` is the verbatim `.schema.json` sidecar (or `null` when the kind ships
/// none) and `dimensions` is the validated `.dimensions.json` sidecar (or `null`
/// when absent). The channel always exists so the topology is uniform; `null` is
/// the machine-readable "not shipped".
fn build_kind_schema(kind: &str, surface_dist_dir: &Path, ts: &str) -> String {
    let schema = read_schema_sidecar(kind, surface_dist_dir);
    let dimensions = read_dimensions_sidecar(kind, surface_dist_dir);
    let body = json!({
        "v": SCHEMA_VERSION,
        "kind": kind,
        "schema": schema,
        "dimensions": dimensions,
        "ts": ts,
    });
    serde_json::to_string(&body).expect("kind schema document serializes to JSON")
}

// ── Sidecar files ──────────────────────────────────────────────────────────

/// `brenn_<kind>.help.md` — the help sidecar filename for `kind` (module base +
/// `.help.md`), following the frozen module naming convention.
fn help_sidecar_filename(kind: &str) -> String {
    format!("{}.help.md", sidecar_stem(kind))
}

/// `brenn_<kind>.schema.json` — the schema sidecar filename for `kind`.
fn schema_sidecar_filename(kind: &str) -> String {
    format!("{}.schema.json", sidecar_stem(kind))
}

/// `brenn_<kind>.dimensions.json` — the dimensions sidecar filename for `kind`.
fn dimensions_sidecar_filename(kind: &str) -> String {
    format!("{}.dimensions.json", sidecar_stem(kind))
}

/// The `brenn_<kind_underscored>` stem shared by the module and its sidecars,
/// derived from the frozen `module_artifact` convention rather than
/// re-implementing the hyphen→underscore mapping.
fn sidecar_stem(kind: &str) -> String {
    let js = module_artifact(kind);
    js.strip_suffix(".js")
        .unwrap_or_else(|| {
            panic!("module_artifact({kind:?}) = {js:?} lacks a .js suffix — host bug")
        })
        .to_string()
}

/// Read the kind's markdown help sidecar (any valid UTF-8 accepted). `None` +
/// boot warning when absent — undocumented is valid config, but the remedy is
/// now in the component author's hands (ship the file), not an in-tree edit.
fn read_help_sidecar(kind: &str, surface_dist_dir: &Path) -> Option<String> {
    let path = surface_dist_dir.join(help_sidecar_filename(kind));
    match std::fs::read_to_string(&path) {
        Ok(doc) => Some(doc),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            tracing::warn!(
                kind,
                path = %path.display(),
                "boot: component kind ships no help sidecar; publishing a generated stub. \
                 Ship a .help.md next to the module to document it."
            );
            None
        }
        Err(err) => panic!(
            "boot: reading help sidecar {} failed ({err}) — the file exists but is unreadable; \
             refusing to start (fail-fast on invalid deploy).",
            path.display(),
        ),
    }
}

/// Read and parse the kind's JSON schema sidecar. `None` when absent (schemas
/// are optional, no warning). A present-but-unparseable sidecar is a boot panic:
/// publishing malformed bytes to a machine-consumer channel is worse than
/// failing fast.
fn read_schema_sidecar(kind: &str, surface_dist_dir: &Path) -> Option<serde_json::Value> {
    let path = surface_dist_dir.join(schema_sidecar_filename(kind));
    match std::fs::read_to_string(&path) {
        Ok(text) => Some(serde_json::from_str(&text).unwrap_or_else(|err| {
            panic!(
                "boot: schema sidecar {} is not valid JSON ({err}) — a shipped machine-readable \
                 schema must parse; refusing to start (fail-fast on invalid deploy).",
                path.display(),
            )
        })),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(err) => panic!(
            "boot: reading schema sidecar {} failed ({err}) — the file exists but is unreadable; \
             refusing to start (fail-fast on invalid deploy).",
            path.display(),
        ),
    }
}

/// Read, parse, and validate the kind's dimensions sidecar. `None` when absent
/// (dimensions are optional, no warning). A present sidecar that is unparseable,
/// carries an unexpected/missing field, declares no bound, or inverts an axis
/// (`min > max`) is a boot panic — the same fail-fast posture as the schema
/// sidecar, since a shipped invalid layout constraint is worse to publish than to
/// refuse. Returns the validated value re-serialized so the published payload
/// carries exactly the accepted shape.
fn read_dimensions_sidecar(kind: &str, surface_dist_dir: &Path) -> Option<serde_json::Value> {
    let path = surface_dist_dir.join(dimensions_sidecar_filename(kind));
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return None,
        Err(err) => panic!(
            "boot: reading dimensions sidecar {} failed ({err}) — the file exists but is \
             unreadable; refusing to start (fail-fast on invalid deploy).",
            path.display(),
        ),
    };
    let dims: Dimensions = serde_json::from_str(&text).unwrap_or_else(|err| {
        panic!(
            "boot: dimensions sidecar {} is not a valid dimensions document ({err}) — expected \
             {{ v, min_width?, max_width?, min_height?, max_height? }} with integer CSS px; \
             refusing to start (fail-fast on invalid deploy).",
            path.display(),
        )
    });
    assert!(
        dims.v == SCHEMA_VERSION,
        "boot: dimensions sidecar {} declares v = {} but only v = {SCHEMA_VERSION} is understood; \
         refusing to start (fail-fast on invalid deploy).",
        path.display(),
        dims.v,
    );
    assert!(
        dims.min_width.is_some()
            || dims.max_width.is_some()
            || dims.min_height.is_some()
            || dims.max_height.is_some(),
        "boot: dimensions sidecar {} sets no bound — at least one of min_width/max_width/\
         min_height/max_height must be present, else omit the sidecar entirely. Refusing to start \
         (fail-fast on invalid deploy).",
        path.display(),
    );
    let axis_ok = |min: Option<u32>, max: Option<u32>| match (min, max) {
        (Some(lo), Some(hi)) => lo <= hi,
        _ => true,
    };
    assert!(
        axis_ok(dims.min_width, dims.max_width) && axis_ok(dims.min_height, dims.max_height),
        "boot: dimensions sidecar {} inverts an axis (min > max) — width {:?}..={:?}, height \
         {:?}..={:?}; refusing to start (fail-fast on invalid deploy).",
        path.display(),
        dims.min_width,
        dims.max_width,
        dims.min_height,
        dims.max_height,
    );
    Some(serde_json::to_value(&dims).expect("validated dimensions serialize to JSON"))
}

// ── Boot validation ────────────────────────────────────────────────────────

/// Boot-time validation of the derived self-description topology. Every failure
/// is operator config, never attacker-reachable, so each is a boot panic (house
/// fail-fast policy):
///
/// - `prefix` must be a non-empty well-formed bare-name segment;
/// - `status_interval_secs` must be in `5..=3600`;
/// - every derived channel — the boot-published help/schema/index set plus the
///   per-surface runtime geometry/status pair — must resolve to a declared
///   `[[channel]]`; missing declarations are aggregated into one panic naming
///   *all* of them, so the operator fixes the config in one pass;
/// - boot-published channels need `standing_retain_depth >= 1` (a non-subscriber
///   pull is clamped to it, so 0 would retain nothing); geometry/status channels
///   need both `retain_depth` and `standing_retain_depth` `Bounded(n >= 1)` —
///   they are written forever, so unbounded retention is unbounded DB growth; and
/// - each channel is single-writer: `system:surface-help` for the boot-published
///   set, the owning surface for its geometry/status pair.
pub fn validate_surface_description(
    config: &SurfaceDescriptionConfig,
    surfaces: &[ResolvedSurface],
    directory: Option<&MessagingDirectory>,
    principals: SingleWriterPrincipals,
) {
    let prefix = config.prefix.as_str();

    assert!(
        !prefix.is_empty() && prefix.chars().all(is_unreserved_char),
        "boot: [surface_description] prefix {prefix:?} is not a well-formed bare-name segment \
         (allowed: A-Za-z0-9._~-, non-empty) — it roots every derived channel address. Refusing \
         to start (fail-fast on invalid config)."
    );
    assert!(
        (5..=3600).contains(&config.status_interval_secs),
        "boot: [surface_description] status_interval_secs = {} is out of range 5..=3600 — the \
         status channel is a heartbeat, not a meter (never high-frequency data on durable \
         channels). Refusing to start (fail-fast on invalid config).",
        config.status_interval_secs,
    );

    // No messaging configured ⇒ no directory, no surfaces (a `[[surface]]` block
    // forces messaging on), hence no derived channels to validate. The parameter
    // checks above still run: a malformed prefix or cadence is a config error
    // whether or not anything consumes them.
    let Some(directory) = directory else {
        return;
    };

    let SingleWriterPrincipals {
        app_policies,
        wasm_consumers,
        surfaces: principal_surfaces,
        system_participants,
    } = principals;

    let mut missing = Vec::new();

    // Boot-published channels: one write per boot, single-writer under
    // `system:surface-help`, `standing_retain_depth >= 1` (a non-subscriber pull
    // is clamped to it).
    for channel in &boot_published_channels(prefix, surfaces) {
        let Some(bare) = resolve_derived_bare(directory, channel, &mut missing) else {
            continue;
        };
        assert_standing_retain_at_least_one(channel, directory);
        assert_channel_single_writer(
            channel,
            bare,
            ExpectedWriter::System(SURFACE_HELP_COMPONENT),
            app_policies,
            wasm_consumers,
            principal_surfaces,
            system_participants,
        );
    }

    // Runtime-published channels: geometry + status per surface, written forever
    // (a heartbeat every interval, a geometry update per resize) by the owning
    // surface. Both the channel-level `retain_depth` and `standing_retain_depth`
    // must be `Bounded(n >= 1)` — unbounded retention here is unbounded database
    // growth by design, so it is a boot error, not a recommendation.
    for surface in surfaces {
        for channel in [
            surface_geometry_channel(prefix, &surface.slug),
            surface_status_channel(prefix, &surface.slug),
        ] {
            let Some(bare) = resolve_derived_bare(directory, &channel, &mut missing) else {
                continue;
            };
            assert_runtime_retention_bounded(&channel, directory);
            assert_channel_single_writer(
                &channel,
                bare,
                ExpectedWriter::Surface(&surface.slug),
                app_policies,
                wasm_consumers,
                principal_surfaces,
                system_participants,
            );
        }
    }

    assert!(
        missing.is_empty(),
        "boot: [surface_description] derives {} channel(s) with no matching [[channel]] \
         declaration: {missing:?} — every derived address must resolve to an explicit channel; \
         no implicit channel is created. Declare each (address = the bare name; help/schema/index \
         need standing_retain_depth >= 1, geometry/status need bounded retain_depth AND \
         standing_retain_depth >= 1). Refusing to start (fail-fast on invalid config).",
        missing.len(),
    );
}

/// Scheme-strip a derived channel address and confirm it resolves to a declared
/// `[[channel]]`. Records an unresolved address in `missing` and returns `None`
/// so the caller skips it (the aggregate missing-declaration panic fires once at
/// the end). A derived address that is not a well-formed `brenn:` name is a host
/// bug (its segments come from validated slugs/kinds) — panic.
fn resolve_derived_bare<'a>(
    directory: &MessagingDirectory,
    channel: &'a str,
    missing: &mut Vec<String>,
) -> Option<&'a str> {
    let bare = well_formed_name(channel, ChannelScheme::Brenn).unwrap_or_else(|| {
        panic!(
            "boot: derived surface-description channel {channel:?} is not a well-formed brenn: \
             address — a slug or kind carries a character outside the bare-name charset. Refusing \
             to start (fail-fast on invalid config)."
        )
    });
    if directory.resolve(channel).is_none() {
        missing.push(channel.to_string());
        return None;
    }
    Some(bare)
}

/// A `Depth` that retains at least one row (`Bounded(n >= 1)` or `Unbounded`).
fn depth_retains_at_least_one(depth: Depth) -> bool {
    matches!(depth, Depth::Unbounded) || matches!(depth, Depth::Bounded(n) if n >= 1)
}

/// Boot-assert a boot-published channel's `standing_retain_depth` retains at
/// least one row. The caller has already confirmed the channel resolves.
fn assert_standing_retain_at_least_one(channel: &str, directory: &MessagingDirectory) {
    let entry = directory
        .resolve(channel)
        .expect("caller confirmed the channel resolves");
    assert!(
        depth_retains_at_least_one(entry.resolved_channel.standing_retain_depth),
        "boot: derived surface-description channel {channel:?} resolves to standing_retain_depth \
         = {:?} — a non-subscriber pull is clamped to standing_retain_depth, so 0 would retain \
         nothing and every read would return nothing. Set standing_retain_depth >= 1. Refusing to \
         start (fail-fast on invalid config).",
        entry.resolved_channel.standing_retain_depth,
    );
}

/// Boot-assert a runtime geometry/status channel's retention is bounded on both
/// axes: `retain_depth` AND `standing_retain_depth` must be `Bounded(n >= 1)`.
/// These channels receive writes forever, so unbounded retention is unbounded
/// database growth by design — a boot error. The caller has already confirmed the
/// channel resolves.
fn assert_runtime_retention_bounded(channel: &str, directory: &MessagingDirectory) {
    let entry = directory
        .resolve(channel)
        .expect("caller confirmed the channel resolves");
    let rc = &entry.resolved_channel;
    let bounded = |d: Depth| matches!(d, Depth::Bounded(n) if n >= 1);
    assert!(
        bounded(rc.retain_depth) && bounded(rc.standing_retain_depth),
        "boot: runtime surface-description channel {channel:?} resolves to retain_depth = {:?}, \
         standing_retain_depth = {:?} — geometry/status channels are written forever (a heartbeat \
         every interval, a geometry update per resize), so both must be Bounded(n >= 1); \
         unbounded retention is unbounded database growth by design. Set bounded depths >= 1. \
         Refusing to start (fail-fast on invalid config).",
        rc.retain_depth,
        rc.standing_retain_depth,
    );
}

// ── Boot publish ───────────────────────────────────────────────────────────

/// Publish every boot document under the `system:surface-help` identity, once at
/// boot after the messenger is built.
///
/// # Panics
///
/// A publish that does not return `Ok` panics rather than starting with a
/// missing/stale document while configured to have one. Most non-`Ok` arms are
/// host bugs made unreachable by the code-built policy and boot-validated
/// channels; `BodyTooLarge` is the exception — a per-kind help doc can be large
/// while `max_body_bytes` is operator-set — so it gets a config-flavored message
/// naming the channel, the size, and the remedy.
pub async fn publish_description(messenger: &Messenger, docs: &[(String, String)]) {
    for (address, body) in docs {
        let result = messenger
            .publish_from_system(SURFACE_HELP_COMPONENT, address, body, Urgency::Normal, None)
            .await;
        match result {
            PublishResult::Ok { .. } => {}
            PublishResult::BodyTooLarge { len, max } => panic!(
                "boot: surface-description publish to {address:?} rejected — the document is {len} \
                 bytes but [messaging] max_body_bytes is {max}. Per-kind help docs grow with their \
                 sidecar content; raise max_body_bytes above {len} (or shrink the sidecar). \
                 Refusing to start (fail-fast on invalid config)."
            ),
            other => panic!(
                "boot: surface-description publish to {address:?} did not succeed ({other:?}) — the \
                 reserved system publisher's policy and the boot-validated channels make this \
                 unreachable, so a failure is a host bug. Refusing to start."
            ),
        }
    }
}

#[cfg(test)]
mod tests;
