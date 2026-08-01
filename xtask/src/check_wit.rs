// `xtask check-wit`: WASI-free gate, bindings-drift gate, world-equivalence gate.

use crate::discover::{Kind, discover_units};
use crate::world_sig::{ItemSignature, WorldSignature, world_signature};
use std::path::{Path, PathBuf};
use std::process::Command;
use wasmparser::{Encoding, Parser, Payload};
use wit_component::{DecodedWasm, decode};
use wit_parser::{Resolve, TypeOwner, WorldId, WorldItem, WorldKey};

/// Run all WIT gates over all applicable units. Returns true if all pass.
pub fn run_check_wit(repo_root: &Path) -> bool {
    let units = discover_units(repo_root);
    let mut ok = true;

    // Artifact base dir: brenn-wasm/target/components/ — the final output dir where
    // the Makefile copies the .wasm files (via the `cp` in wasm_component_rule /
    // wasm_guest_component_rule). This is distinct from WASM_COMPONENTS_TARGET
    // (brenn-wasm/components/target/) which is the cargo build dir. The WASI-free
    // check runs on the copied final artifacts, matching the Makefile posture.
    let artifact_dir = repo_root
        .join("brenn-wasm")
        .join("target")
        .join("components");

    // One ephemeral scratch dir outside the repo for all bindings regeneration.
    // Generating into scratch (never a crate's src/) keeps this lane tree-read-only,
    // so concurrent discovery walks in other lanes cannot race a vanishing file.
    // The TempDir self-deletes when this sweep ends.
    let scratch = tempfile::TempDir::new()
        .unwrap_or_else(|e| panic!("xtask check-wit: failed to create scratch tempdir: {e}"));
    let scratch_root = scratch_root_outside_repo(scratch.path(), repo_root);

    // The bindings-drift gate shells out to whatever `wit-bindgen` is on PATH. The
    // Makefile preflights guard component *rebuilds* only, so an up-to-date tree reaches
    // this lane with nothing having checked the binary: a wrong-version generator would
    // report spurious drift, and its fix-it hint would then plant wrong-generator bindings
    // that the gate afterwards accepts. Assert once, before any unit runs. Unconditional —
    // Family A units always exist in-tree, so the lane always needs the binary.
    let wit_bindgen_pin = makefile_pin(repo_root, WIT_BINDGEN_PIN_VAR);
    assert_wit_bindgen_version(&wit_bindgen_pin);

    for (unit_index, unit) in units.iter().enumerate() {
        match unit.kind {
            Kind::WasmComponent | Kind::WasmGuest => {
                let artifact = LoadedArtifact::load(&unit.dir, &artifact_dir);
                // WASI-free gate.
                if !check_wasi_free(&artifact) {
                    ok = false;
                }
                // Bindings-drift gate: WasmComponent only (Family A has committed bindings.rs).
                if unit.kind == Kind::WasmComponent
                    && !check_bindings_drift(&unit.dir, &scratch_root, unit_index, &wit_bindgen_pin)
                {
                    ok = false;
                }
                // World-equivalence gate: both families. The two are built by two
                // different generator invocations (the pinned CLI for Family A, the
                // `wit_bindgen::generate!` macro in the brenn-guest SDK for Family B),
                // so a world-moving generator change on either path must fail here.
                let (wit_path, world_name) = wit_source_for_unit(&unit.kind, &unit.dir, repo_root);
                if !check_world_equivalence(&wit_path, &world_name, &artifact) {
                    ok = false;
                }
            }
            Kind::WasmSdk | Kind::RootWorkspace => {
                // No WIT gates for these.
            }
        }
    }

    ok
}

/// Canonicalize the scratch dir and assert it does not resolve inside the repo, returning
/// the canonical scratch path. Both sides are canonicalized before the prefix test: bare
/// `starts_with` is lexical, and a symlinked or relative TMPDIR can resolve inside the repo
/// without lexically matching it. A scratch dir inside the repo would re-open the vanishing-
/// file race, so this fails fast. Canonicalization failure on either side is itself
/// unexpected and panics.
fn scratch_root_outside_repo(scratch: &Path, repo_root: &Path) -> PathBuf {
    let scratch_root = scratch.canonicalize().unwrap_or_else(|e| {
        panic!("xtask check-wit: failed to canonicalize scratch dir {scratch:?}: {e}")
    });
    let repo_canon = repo_root.canonicalize().unwrap_or_else(|e| {
        panic!("xtask check-wit: failed to canonicalize repo root {repo_root:?}: {e}")
    });
    assert!(
        !scratch_root.starts_with(&repo_canon),
        "xtask check-wit: scratch dir {scratch_root:?} resolves inside the repo {repo_canon:?}. \
         Point TMPDIR at a location outside the repository."
    );
    scratch_root
}

/// A built artifact, read from disk and decoded once for every gate that inspects it.
///
/// Both artifact gates need the same decoded world, and the decode is the expensive
/// step in this lane; loading per gate would read the file twice and decode it twice
/// per unit, and each new artifact gate would add another pair.
struct LoadedArtifact {
    path: PathBuf,
    /// `Err` = the bytes do not decode as a component. Held rather than resolved so
    /// every gate fails closed on it with its own diagnostic.
    world: Result<(Resolve, WorldId), String>,
}

impl LoadedArtifact {
    /// A missing or unreadable artifact is broken build state rather than a gate outcome,
    /// so this panics (run `make wasm-components` first).
    fn load(crate_dir: &Path, artifact_dir: &Path) -> Self {
        // Derive artifact name from crate package name.
        let path = artifact_dir.join(artifact_name_for(crate_dir));

        assert!(
            path.exists(),
            "xtask check-wit: artifact {path:?} not found. \
             Run `make wasm-components` first to build all WASM artifacts."
        );

        // Existence was just asserted; a read failure now is unexpected → fail fast.
        let bytes = std::fs::read(&path)
            .unwrap_or_else(|e| panic!("xtask check-wit: failed to read {path:?}: {e}"));

        let world = decode_component_world(&bytes);
        Self { path, world }
    }

    /// The decoded world, or the reason the bytes are not a component.
    fn world(&self) -> Result<&(Resolve, WorldId), &str> {
        self.world.as_ref().map_err(String::as_str)
    }
}

/// WASI-free gate: assert the artifact's decoded world imports no `wasi:`-namespaced
/// package. Fails closed on anything that did not decode as a component.
fn check_wasi_free(artifact: &LoadedArtifact) -> bool {
    let path = &artifact.path;
    match artifact.world() {
        Err(msg) => {
            eprintln!(
                "xtask check-wit [wasi-free FAIL]: {path:?} did not decode as a \
                 component with the locked wit-component version: {msg}"
            );
            false
        }
        Ok((resolve, world_id)) => {
            let offenders = wasi_imports(resolve, *world_id);
            if offenders.is_empty() {
                true
            } else {
                eprintln!("xtask check-wit [wasi-free FAIL]: {path:?} imports wasi:*:");
                for name in &offenders {
                    eprintln!("  {name}");
                }
                false
            }
        }
    }
}

/// The fully-qualified names (sorted, deduped) of all imports in a decoded world that
/// resolve to a `wasi:`-namespaced package.
fn wasi_imports(resolve: &Resolve, world_id: WorldId) -> Vec<String> {
    let mut offenders = Vec::new();
    for (key, item) in &resolve.worlds[world_id].imports {
        match (key, item) {
            // A named-key interface carries no package (inline interface), so in practice
            // only the ID-keyed case can be wasi; the helper guards both uniformly.
            (WorldKey::Interface(id), _) | (WorldKey::Name(_), WorldItem::Interface { id, .. }) => {
                if let Some(name) = wasi_interface_name(resolve, *id) {
                    offenders.push(name);
                }
            }
            (WorldKey::Name(name), WorldItem::Type(type_id)) => {
                // A type-only `use` confers no capability by itself, but it still means the
                // world depends on wasi definitions; flag it conservatively so any wasi
                // reference trips the gate, reporting the owning interface + type name.
                // Deliberately untested: producing a world-level WorldItem::Type with an
                // interface owner needs a full component encode (ComponentEncoder + the
                // dummy-module feature), not a `wat` text fixture — not worth the plumbing
                // for one conservative flag-only branch.
                if let TypeOwner::Interface(owner) = resolve.types[*type_id].owner
                    && let Some(owner_id) = wasi_interface_name(resolve, owner)
                {
                    offenders.push(format!("{owner_id} (type {name})"));
                }
            }
            (WorldKey::Name(_), WorldItem::Function(_)) => {
                // Plain kebab-named function import: no package namespace, not WASI.
            }
        }
    }

    offenders.sort();
    offenders.dedup();
    offenders
}

/// Decode a component binary into its `Resolve` plus the id of the world it carries.
///
/// `Err(msg)` means the bytes do not decode as a component: a malformed artifact, a core
/// wasm module, a WIT-package encoding, or a component newer than the locked decoder
/// supports. Every such case fails its caller's gate closed rather than passing vacuously.
fn decode_component_world(component_bytes: &[u8]) -> Result<(Resolve, WorldId), String> {
    // Encoding pre-check: wit_component::decode accepts a plain core module and
    // synthesizes an empty world, which would let a core module (potentially
    // importing wasi_snapshot_preview1) pass a gate. Require the binary to
    // announce itself as a component in its header before decoding.
    match Parser::new(0).parse(component_bytes, true) {
        Ok(wasmparser::Chunk::Parsed {
            payload:
                Payload::Version {
                    encoding: Encoding::Component,
                    ..
                },
            ..
        }) => {}
        Ok(_) => return Err("artifact is not a component binary".to_string()),
        Err(e) => return Err(format!("artifact header did not parse: {e}")),
    }

    // `decode` can hit an `unreachable!()` inside wit-parser on some valid-but-unusual
    // component binaries (e.g. an ID-form function import). Contain that as a fail-closed
    // Err so the artifact is still named and the per-unit sweep continues, matching this
    // function's Err contract instead of aborting the whole run without attribution.
    let decoded =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| decode(component_bytes)))
            .map_err(|_| "wit-component decoder panicked on this artifact".to_string())?
            .map_err(|e| format!("component decode failed: {e}"))?;
    match decoded {
        DecodedWasm::Component(resolve, world) => Ok((resolve, world)),
        DecodedWasm::WitPackage(..) => {
            Err("artifact is a WIT package encoding, not a component".to_string())
        }
    }
}

/// If the interface belongs to a package whose namespace is exactly `wasi`, return its
/// reportable name (synthesizing a fallback for an unnamed interface); otherwise `None`.
fn wasi_interface_name(resolve: &Resolve, id: wit_parser::InterfaceId) -> Option<String> {
    interface_is_wasi(resolve, id).then(|| {
        resolve
            .id_of(id)
            .unwrap_or_else(|| format!("wasi:<unnamed-interface#{}>", id.index()))
    })
}

/// True if the interface belongs to a package whose namespace is exactly `wasi`.
fn interface_is_wasi(resolve: &Resolve, id: wit_parser::InterfaceId) -> bool {
    resolve.interfaces[id]
        .package
        .is_some_and(|pkg| resolve.packages[pkg].name.namespace == "wasi")
}

/// Derive the WIT file path and world name for a raw-bindings crate from its Cargo.toml.
///
/// Reads `package.metadata.component.target.path` (the same TOML key that cargo-component
/// used to locate the WIT file). Returns (absolute_wit_path, world_name) where world_name
/// is derived as the WIT filename stem (e.g. "processor.wit" → "processor").
///
/// Panics if the metadata is absent or unparseable — all WasmComponent crates must have it
/// (it is how `discover.rs` classifies them as WasmComponent in the first place).
fn wit_path_for_crate(crate_dir: &Path) -> (PathBuf, String) {
    let cargo_toml = crate_dir.join("Cargo.toml");
    let content = std::fs::read_to_string(&cargo_toml)
        .unwrap_or_else(|e| panic!("xtask check-wit: failed to read {cargo_toml:?}: {e}"));
    let parsed: toml::Value = toml::from_str(&content)
        .unwrap_or_else(|e| panic!("xtask check-wit: failed to parse {cargo_toml:?}: {e}"));

    let wit_path_str = parsed
        .get("package")
        .and_then(|p| p.get("metadata"))
        .and_then(|m| m.get("component"))
        .and_then(|c| c.get("target"))
        .and_then(|t| t.get("path"))
        .and_then(|p| p.as_str())
        .unwrap_or_else(|| {
            panic!(
                "xtask check-wit: no package.metadata.component.target.path in {cargo_toml:?}. \
                 All WasmComponent crates must have this field."
            )
        });

    // Resolve relative to crate_dir (the path in Cargo.toml is relative to the crate).
    let wit_path = crate_dir.join(wit_path_str);
    let wit_path = wit_path.canonicalize().unwrap_or_else(|e| {
        panic!("xtask check-wit: failed to canonicalize WIT path {wit_path:?}: {e}")
    });

    // Derive world name from WIT filename stem (e.g. "processor.wit" → "processor").
    let world_name = wit_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or_else(|| panic!("xtask check-wit: could not derive world name from {wit_path:?}"))
        .to_owned();

    (wit_path, world_name)
}

/// The WIT source and world name a unit's artifact is compared against, by family.
///
/// Family A (`WasmComponent`) names its WIT file in Cargo metadata, so the mapping is
/// read from there. Family B (`WasmGuest`) does not: its world comes from the
/// `wit_bindgen::generate!` invocation in the brenn-guest SDK, which every Family B
/// crate consumes, so the source is the same file and world for all of them and is
/// stated here. `guest_sdk_world_is_processor` keeps that statement honest.
fn wit_source_for_unit(kind: &Kind, crate_dir: &Path, repo_root: &Path) -> (PathBuf, String) {
    match kind {
        Kind::WasmComponent => wit_path_for_crate(crate_dir),
        Kind::WasmGuest => {
            let wit_path = repo_root
                .join("brenn-wasm")
                .join("wit")
                .join(GUEST_SDK_WIT_FILE);
            let wit_path = wit_path.canonicalize().unwrap_or_else(|e| {
                panic!("xtask check-wit: failed to canonicalize WIT path {wit_path:?}: {e}")
            });
            guest_sdk_world_is_processor(repo_root);
            (wit_path, GUEST_SDK_WORLD.to_string())
        }
        Kind::WasmSdk | Kind::RootWorkspace => {
            panic!("xtask check-wit: {kind:?} units carry no artifact to compare")
        }
    }
}

/// The WIT file and world the brenn-guest SDK generates its bindings from.
const GUEST_SDK_WIT_FILE: &str = "processor.wit";
const GUEST_SDK_WORLD: &str = "processor";

/// Liveness guard for the hard-coded Family B mapping above: the SDK's `generate!`
/// invocation must still name that file and world. If someone repoints or splits it,
/// every guest artifact would otherwise be silently compared against the wrong source
/// world — or, worse, against one it still happens to satisfy.
fn guest_sdk_world_is_processor(repo_root: &Path) {
    let sdk_bindings = repo_root
        .join("brenn-wasm")
        .join("guest")
        .join("src")
        .join("bindings.rs");
    let content = std::fs::read_to_string(&sdk_bindings)
        .unwrap_or_else(|e| panic!("xtask check-wit: failed to read {sdk_bindings:?}: {e}"));
    for needle in [
        &format!("world: \"{GUEST_SDK_WORLD}\""),
        &format!("path: \"../wit/{GUEST_SDK_WIT_FILE}\""),
    ] {
        assert!(
            content.contains(needle.as_str()),
            "xtask check-wit: {sdk_bindings:?} no longer contains `{needle}`. Every WasmGuest \
             artifact's world comes from this `generate!` invocation and the world-equivalence \
             gate hard-codes it; update `wit_source_for_unit` to match."
        );
    }
}

/// The Makefile variable holding the pinned `wit-bindgen-cli` version.
const WIT_BINDGEN_PIN_VAR: &str = "WIT_BINDGEN_CLI_VERSION";

/// The value of a `NAME := value` line in the repo-root `Makefile`.
///
/// The Makefile is the single home of the guest-build toolchain pins; the public CI
/// workflow and the private CD pipeline read those same lines by grep, and this is the
/// third reader. Parsing rather than embedding a copy means a bump is one edit and this
/// lane's assert and install hints move with it.
///
/// Panics when the line is missing, appears more than once, carries an empty value, or
/// departs from the exact `NAME := value` shape: a pin that cannot be resolved, or that
/// this parser and the two seds would resolve differently, is broken build state rather
/// than a gate outcome. The shape check is the strictest of the three readers on purpose —
/// `sed -n 's/^NAME := //p'` takes everything after exactly one space, so an aligned or
/// trailing-space assignment hands CI and CD a version with whitespace in it while a
/// trimming parser sees nothing wrong.
fn makefile_pin(repo_root: &Path, name: &str) -> String {
    let makefile = repo_root.join("Makefile");
    let content = std::fs::read_to_string(&makefile)
        .unwrap_or_else(|e| panic!("xtask check-wit: failed to read {makefile:?}: {e}"));

    let prefix = format!("{name} :=");
    let raw: Vec<&str> = content
        .lines()
        .filter_map(|line| line.strip_prefix(prefix.as_str()))
        .collect();

    assert_eq!(
        raw.len(),
        1,
        "xtask check-wit: expected exactly one `{name} := <value>` line in {makefile:?}, \
         found {}. That line is the pin's only home and CI, CD and xtask all read it; \
         keep the `NAME := value` shape.",
        raw.len()
    );
    assert!(
        !raw[0].trim().is_empty(),
        "xtask check-wit: `{name} :=` in {makefile:?} has an empty value. \
         CI, CD and xtask all read that line for the pin."
    );

    let value = raw[0].strip_prefix(' ').unwrap_or_else(|| {
        panic!(
            "xtask check-wit: `{name} :=` in {makefile:?} is not followed by a single space \
             (found {:?}). CI and CD read the line with `sed -n 's/^{name} := //p'`, which \
             matches nothing without that space; keep the `NAME := value` shape.",
            raw[0]
        )
    });
    assert_eq!(
        value,
        value.trim(),
        "xtask check-wit: the value on `{name} :=` in {makefile:?} carries surrounding \
         whitespace. CI and CD read the line with `sed -n 's/^{name} := //p'` and would \
         install version {value:?}; keep the `NAME := value` shape."
    );

    value.to_string()
}

/// The version an installed tool reports: the second whitespace-separated field of the
/// first `--version` line (`wit-bindgen-cli 1.2.3` → `1.2.3`). Mirrors the Makefile
/// preflight's `awk 'NR==1{print $2}'` so both readers of the same output agree.
fn version_field(output: &str) -> Option<&str> {
    output.lines().next()?.split_whitespace().nth(1)
}

/// The exact command that installs the pinned generator.
fn wit_bindgen_install_hint(pin: &str) -> String {
    format!("Install with: cargo install --locked wit-bindgen-cli --version {pin}")
}

/// The verdict on `wit-bindgen --version` output, separated from the process spawn so
/// both the decision and the operator-facing message are exercisable without a PATH shim.
///
/// `Err` carries the whole message, install command included: this gate compares generator
/// output against committed bytes, so a wrong generator makes both its verdict and its
/// remediation wrong, and the remediation is the part that has to name the pin.
fn check_reported_version(version_output: &str, pin: &str) -> Result<(), String> {
    let hint = wit_bindgen_install_hint(pin);
    match version_field(version_output) {
        None => Err(format!(
            "xtask check-wit: could not read a version from `wit-bindgen --version` \
             output {:?}. {hint}",
            version_output.trim()
        )),
        Some(have) if have == pin => Ok(()),
        Some(have) => Err(format!(
            "xtask check-wit: wit-bindgen-cli {have} is on PATH but the pin is {pin}. {hint}"
        )),
    }
}

/// Assert the `wit-bindgen` on PATH reports exactly the pinned version.
///
/// Absence, a failed or unparseable `--version`, and a mismatch all panic with the
/// install command.
fn assert_wit_bindgen_version(pin: &str) {
    let hint = wit_bindgen_install_hint(pin);

    let output = Command::new("wit-bindgen")
        .arg("--version")
        .output()
        .unwrap_or_else(|e| {
            panic!("xtask check-wit: failed to run `wit-bindgen --version`: {e}. {hint}")
        });
    assert!(
        output.status.success(),
        "xtask check-wit: `wit-bindgen --version` exited {}: {}. {hint}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    check_reported_version(&stdout, pin).unwrap_or_else(|msg| panic!("{msg}"));
}

/// Bindings-drift gate: regenerate the crate's bindings into an ephemeral scratch dir
/// via the pinned `wit-bindgen-cli` and byte-compare against the committed
/// `src/bindings.rs`.
/// Never writes into the crate — the working tree is untouched, so this gate is safe to
/// run concurrently with lanes that walk the tree.
///
/// Both sides of the comparison are produced by the pinned generator, so this detects
/// a hand-edited or stale `bindings.rs` and nothing else: when the pin itself moves,
/// committed and regenerated bindings move together and the gate passes by construction.
/// Whether the world the artifact carries still matches its WIT source is the separate
/// question `check_world_equivalence` answers.
fn check_bindings_drift(
    crate_dir: &Path,
    scratch_root: &Path,
    unit_index: usize,
    wit_bindgen_pin: &str,
) -> bool {
    let bindings_path = crate_dir.join("src").join("bindings.rs");
    assert!(
        bindings_path.exists(),
        "xtask check-wit: bindings.rs not found at {bindings_path:?} for WasmComponent crate. \
         Expected a committed bindings.rs (Family A). If this crate was reclassified, \
         update its kind in xtask/lint-allowlist.toml."
    );

    // Parse WIT path and world name from crate Cargo.toml.
    let (wit_path, world_name) = wit_path_for_crate(crate_dir);

    let original = std::fs::read(&bindings_path)
        .unwrap_or_else(|e| panic!("xtask check-wit: failed to read {bindings_path:?}: {e}"));

    // Per-crate scratch subdir keyed on the crate's unique discovery index (with the
    // basename appended for readability). The index guarantees disjoint output paths even
    // when two crates share a basename, so a future parallelized per-crate loop cannot make
    // one crate read another's regenerated bytes.
    let crate_name = crate_dir
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or_else(|| {
            panic!("xtask check-wit: crate dir {crate_dir:?} has no valid file name")
        });
    let out_dir = scratch_root.join(format!("{unit_index}-{crate_name}"));
    std::fs::create_dir_all(&out_dir).unwrap_or_else(|e| {
        panic!("xtask check-wit: failed to create scratch dir {out_dir:?}: {e}")
    });
    let world_named_path = out_dir.join(format!("{world_name}.rs"));

    let wit_path_str = wit_path
        .to_str()
        .unwrap_or_else(|| panic!("xtask check-wit: WIT path {wit_path:?} is not valid UTF-8"));
    let out_dir_str = out_dir
        .to_str()
        .unwrap_or_else(|| panic!("xtask check-wit: scratch dir {out_dir:?} is not valid UTF-8"));

    let output = Command::new("wit-bindgen")
        .args([
            "rust",
            wit_path_str,
            "--runtime-path",
            "wit_bindgen_rt",
            "--out-dir",
            out_dir_str,
        ])
        .output()
        .unwrap_or_else(|e| {
            let hint = wit_bindgen_install_hint(wit_bindgen_pin);
            panic!(
                "xtask check-wit: failed to run `wit-bindgen rust` for {crate_dir:?}: {e}. {hint}"
            )
        });

    if !output.status.success() {
        eprintln!(
            "xtask check-wit [bindings-drift FAIL]: `wit-bindgen rust` failed for {crate_dir:?}"
        );
        eprintln!(
            "  wit-bindgen stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        return false;
    }

    // A read failure after a successful wit-bindgen exit is unexpected → fail fast,
    // naming the crate (lane-attributed via run_jobs).
    let regenerated = std::fs::read(&world_named_path).unwrap_or_else(|e| {
        panic!(
            "xtask check-wit: failed to read regenerated bindings {world_named_path:?} \
             for {crate_dir:?} after a successful wit-bindgen run: {e}"
        )
    });

    let drift = original != regenerated;
    if drift {
        // Remediation targets the crate's real src/, not the scratch out_dir used above.
        let src_dir = crate_dir.join("src");
        let src_dir_str = src_dir
            .to_str()
            .unwrap_or_else(|| panic!("xtask check-wit: src dir {src_dir:?} is not valid UTF-8"));
        eprintln!(
            "xtask check-wit [bindings-drift FAIL]: {bindings_path:?} is stale. \
             Regenerate with: wit-bindgen rust {wit_path_str} --runtime-path wit_bindgen_rt \
             --out-dir {src_dir_str} && mv {src_dir_str}/{world_name}.rs {src_dir_str}/bindings.rs"
        );
    }

    !drift
}

/// World-equivalence gate: assert the world embedded in the built artifact is still the
/// world the crate's WIT source declares.
///
/// An artifact conforms to its source world iff:
///
/// 1. **Imports are a subset with shape equality.** Every import the artifact carries
///    appears in the source world under the same name, and — for an imported interface —
///    every member it carries appears among that interface's source members with a
///    structurally identical shape. Omission is legal at both granularities, because
///    componentization elides at both: `brenn_replay.wasm` carries no
///    `brenn:replay/config` import at all, and `brenn_replay_fault_test.wasm` carries the
///    `store` import with only the `begin` function of the six-method resource the source
///    declares. Addition is not legal at either: an import or member nothing declared is
///    a host obligation no one agreed to.
/// 2. **Exports are equal.** The artifact's exports are exactly the source world's
///    exports, with structurally identical shapes, member for member. No omission — a
///    missing export breaks the host's call surface — and no addition, which is contract
///    noise an in-tree build gate has no reason to admit. Out-of-tree artifacts never pass
///    through this gate, so the strictness costs nothing external.
/// 3. **Comparison is structural**, over decoded `wit_parser` data (see `world_sig`),
///    never over printed WIT — so doc-stripping and item reordering, both of which the
///    encoder does, are invisible by construction rather than special-cased.
///
/// This is the coverage `check_bindings_drift` cannot have: both sides of that comparison
/// are produced by the same pinned generator, so a generator or encoder change that moves
/// the guest-visible world passes it by construction. Here the source side is the
/// hand-written WIT, so such a change fails.
///
/// Runs on every built artifact of both families — the two are generated by two different
/// wit-bindgen invocations (pinned CLI vs. the SDK's `generate!` macro), and the deployed
/// components are Family B, so covering only Family A would leave the shipped guests'
/// world unguarded. `wit_source_for_unit` supplies each family's source.
///
/// Fails closed on anything that does not decode as a component, mirroring `wasi_imports`.
fn check_world_equivalence(wit_path: &Path, world_name: &str, artifact: &LoadedArtifact) -> bool {
    let source = source_world_signature(wit_path, world_name);

    match world_conformance(artifact.world(), &source) {
        Ok(()) => true,
        Err(problems) => {
            let artifact_path = &artifact.path;
            eprintln!(
                "xtask check-wit [world-equivalence FAIL]: {artifact_path:?} no longer carries \
                 the world `{world_name}` declared by {wit_path:?}:"
            );
            for problem in &problems {
                eprintln!("  {problem}");
            }
            false
        }
    }
}

/// Parse the in-tree WIT source and reduce the named world to its signature.
///
/// Every failure here is a broken repo state, not a gate outcome: the WIT file is
/// hand-written, in-tree, and read by the build itself, so a parse error or a missing
/// world means something is wrong with the checkout rather than with an artifact.
fn source_world_signature(wit_path: &Path, world_name: &str) -> WorldSignature {
    let mut resolve = Resolve::new();
    let pkg = resolve.push_file(wit_path).unwrap_or_else(|e| {
        panic!("xtask check-wit: failed to parse WIT source {wit_path:?}: {e:?}")
    });
    let world = resolve
        .select_world(&[pkg], Some(world_name))
        .unwrap_or_else(|e| {
            panic!("xtask check-wit: no world `{world_name}` in {wit_path:?}: {e:?}")
        });
    world_signature(&resolve, world)
}

/// Compare a component artifact's embedded world against a source world signature under
/// the conformance rules documented on `check_world_equivalence`. `Err` carries every
/// problem found, so one run names all of them rather than the first; bytes that never
/// decoded as a component are themselves the single problem, so the gate fails closed.
fn world_conformance(
    world: Result<&(Resolve, WorldId), &str>,
    source: &WorldSignature,
) -> Result<(), Vec<String>> {
    let (resolve, world_id) = world.map_err(|msg| vec![msg.to_string()])?;
    signatures_conform(source, &world_signature(resolve, *world_id))
}

/// The conformance rules themselves, over two already-computed signatures.
fn signatures_conform(
    source: &WorldSignature,
    artifact: &WorldSignature,
) -> Result<(), Vec<String>> {
    let mut problems = Vec::new();

    // Imports: subset with shape equality, at both item and interface-member granularity.
    for (name, shape) in &artifact.imports {
        match source.imports.get(name) {
            None => problems.push(format!(
                "artifact imports `{name}`, which the source world does not declare"
            )),
            Some(expected) => compare_item(
                &format!("import `{name}`"),
                expected,
                shape,
                MemberRule::Subset,
                &mut problems,
            ),
        }
    }

    // Exports: equal key sets with shape equality, members included.
    for (name, shape) in &artifact.exports {
        match source.exports.get(name) {
            None => problems.push(format!(
                "artifact exports `{name}`, which the source world does not declare"
            )),
            Some(expected) => compare_item(
                &format!("export `{name}`"),
                expected,
                shape,
                MemberRule::Equal,
                &mut problems,
            ),
        }
    }
    for name in source.exports.keys() {
        if !artifact.exports.contains_key(name) {
            problems.push(format!(
                "source world exports `{name}`, which the artifact does not carry"
            ));
        }
    }

    if problems.is_empty() {
        Ok(())
    } else {
        Err(problems)
    }
}

/// Whether an interface's members must match exactly or may be a subset of the source's.
enum MemberRule {
    Subset,
    Equal,
}

/// Compare one artifact item against its source counterpart, appending every difference.
fn compare_item(
    what: &str,
    source: &ItemSignature,
    artifact: &ItemSignature,
    rule: MemberRule,
    problems: &mut Vec<String>,
) {
    match (source, artifact) {
        (ItemSignature::Interface(source_members), ItemSignature::Interface(artifact_members)) => {
            for (member, shape) in artifact_members {
                match source_members.get(member) {
                    None => problems.push(format!(
                        "{what} carries `{member}`, which the source interface does not declare"
                    )),
                    Some(expected) if expected != shape => problems.push(format!(
                        "{what} member `{member}` has a different shape:\n    \
                         source:   {expected}\n    artifact: {shape}"
                    )),
                    Some(_) => {}
                }
            }
            if let MemberRule::Equal = rule {
                for member in source_members.keys() {
                    if !artifact_members.contains_key(member) {
                        problems.push(format!(
                            "{what} does not carry `{member}`, which the source declares"
                        ));
                    }
                }
            }
        }
        (ItemSignature::Item(expected), ItemSignature::Item(shape)) if expected == shape => {}
        (expected, shape) => problems.push(format!(
            "{what} has a different shape:\n    source:   {}\n    artifact: {}",
            expected.describe(),
            shape.describe()
        )),
    }
}

/// Derive the WASM artifact filename for a crate directory.
/// Uses the Cargo.toml package name, converting hyphens to underscores and appending .wasm.
fn artifact_name_for(crate_dir: &Path) -> String {
    let cargo_toml = crate_dir.join("Cargo.toml");
    let content = std::fs::read_to_string(&cargo_toml)
        .unwrap_or_else(|e| panic!("xtask check-wit: failed to read {cargo_toml:?}: {e}"));
    let parsed: toml::Value = toml::from_str(&content)
        .unwrap_or_else(|e| panic!("xtask check-wit: failed to parse {cargo_toml:?}: {e}"));
    let name = parsed
        .get("package")
        .and_then(|p| p.get("name"))
        .and_then(|n| n.as_str())
        .unwrap_or_else(|| panic!("xtask check-wit: no package.name in {cargo_toml:?}"));
    format!("{}.wasm", name.replace('-', "_"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// Happy path: `wit_path_for_crate` correctly parses the WIT path and derives
    /// the world name from the filename stem.
    #[test]
    fn wit_path_for_crate_happy_path() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();

        // Create a minimal WIT file so canonicalize() succeeds.
        let wit_dir = root.join("wit");
        fs::create_dir_all(&wit_dir).unwrap();
        let wit_file = wit_dir.join("processor.wit");
        fs::write(&wit_file, "package example:processor;\nworld processor {}").unwrap();

        // Write a Cargo.toml with [package.metadata.component.target] path pointing to the WIT file.
        // The path is relative to the crate dir (root in this test).
        fs::write(
            root.join("Cargo.toml"),
            r#"[package]
name = "my-processor"
version = "0.1.0"
edition = "2021"

[package.metadata.component.target]
path = "wit/processor.wit"
"#,
        )
        .unwrap();

        let (returned_path, world_name) = wit_path_for_crate(root);

        assert_eq!(
            returned_path,
            wit_file.canonicalize().unwrap(),
            "returned path should be the canonical path of the WIT file"
        );
        assert_eq!(
            world_name, "processor",
            "world name should be derived from WIT filename stem"
        );
    }

    /// `wit_path_for_crate` must panic when the Cargo.toml lacks
    /// `[package.metadata.component.target] path`.
    #[test]
    #[should_panic(expected = "no package.metadata.component.target.path")]
    fn wit_path_for_crate_missing_metadata_panics() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();

        // Cargo.toml with no component metadata at all.
        fs::write(
            root.join("Cargo.toml"),
            r#"[package]
name = "no-wit-metadata"
version = "0.1.0"
edition = "2021"
"#,
        )
        .unwrap();

        wit_path_for_crate(root);
    }

    /// Write a Makefile fixture into a fresh tempdir and return the dir.
    ///
    /// Fixture bodies carry deliberately fictional versions: the parser is indifferent to
    /// the value, and a real pin value here would be one more hand-maintained copy of the
    /// number this module exists to stop copying.
    fn makefile_fixture(body: &str) -> tempfile::TempDir {
        let tmp = tempfile::tempdir().expect("tempdir");
        fs::write(tmp.path().join("Makefile"), body).unwrap();
        tmp
    }

    #[test]
    fn makefile_pin_reads_the_pinned_value() {
        let tmp = makefile_fixture(
            "# comment\nWASM_TOOLS_VERSION := 4.5.6\nWIT_BINDGEN_CLI_VERSION := 1.2.3\n\nall:\n\t@true\n",
        );
        assert_eq!(makefile_pin(tmp.path(), "WIT_BINDGEN_CLI_VERSION"), "1.2.3");
        assert_eq!(makefile_pin(tmp.path(), "WASM_TOOLS_VERSION"), "4.5.6");
    }

    /// A pin line that was renamed or deleted must fail loudly rather than leave the
    /// version assert and the install hints guessing.
    #[test]
    #[should_panic(expected = "expected exactly one `WIT_BINDGEN_CLI_VERSION := <value>` line")]
    fn makefile_pin_missing_line_panics() {
        let tmp = makefile_fixture("WASM_TOOLS_VERSION := 4.5.6\n");
        makefile_pin(tmp.path(), "WIT_BINDGEN_CLI_VERSION");
    }

    /// Two definitions means the greps in CI and CD and this parser can disagree about
    /// which one wins; refuse rather than pick.
    #[test]
    #[should_panic(expected = "found 2")]
    fn makefile_pin_duplicate_line_panics() {
        let tmp = makefile_fixture(
            "WIT_BINDGEN_CLI_VERSION := 1.2.3\nWIT_BINDGEN_CLI_VERSION := 1.2.4\n",
        );
        makefile_pin(tmp.path(), "WIT_BINDGEN_CLI_VERSION");
    }

    #[test]
    #[should_panic(expected = "has an empty value")]
    fn makefile_pin_empty_value_panics() {
        let tmp = makefile_fixture("WIT_BINDGEN_CLI_VERSION :=\n");
        makefile_pin(tmp.path(), "WIT_BINDGEN_CLI_VERSION");
    }

    /// Aligning the two assignments is the natural cosmetic edit, and `sed -n
    /// 's/^NAME := //p'` would hand CI and CD a version with a leading space. Reject the
    /// shape here rather than let the pipeline install `" 1.2.3"`.
    #[test]
    #[should_panic(expected = "carries surrounding whitespace")]
    fn makefile_pin_extra_leading_space_panics() {
        let tmp = makefile_fixture("WIT_BINDGEN_CLI_VERSION :=  1.2.3\n");
        makefile_pin(tmp.path(), "WIT_BINDGEN_CLI_VERSION");
    }

    /// Same hazard from the other end: the seds keep a trailing space, a trimming parser
    /// does not.
    #[test]
    #[should_panic(expected = "carries surrounding whitespace")]
    fn makefile_pin_trailing_space_panics() {
        let tmp = makefile_fixture("WIT_BINDGEN_CLI_VERSION := 1.2.3   \n");
        makefile_pin(tmp.path(), "WIT_BINDGEN_CLI_VERSION");
    }

    #[test]
    #[should_panic(expected = "is not followed by a single space")]
    fn makefile_pin_missing_space_panics() {
        let tmp = makefile_fixture("WIT_BINDGEN_CLI_VERSION :=1.2.3\n");
        makefile_pin(tmp.path(), "WIT_BINDGEN_CLI_VERSION");
    }

    /// The match is anchored at start of line, so parking the previous pin as a comment
    /// during a bump neither resolves nor counts toward the duplicate check — the same
    /// reading the two seds' `^` anchors give.
    #[test]
    fn makefile_pin_ignores_commented_out_pins() {
        let tmp = makefile_fixture(
            "# WIT_BINDGEN_CLI_VERSION := 9.9.9\nWIT_BINDGEN_CLI_VERSION := 1.2.3\n",
        );
        assert_eq!(makefile_pin(tmp.path(), "WIT_BINDGEN_CLI_VERSION"), "1.2.3");
    }

    /// Liveness against the real Makefile: both pin lines still carry the `NAME := value`
    /// shape their three readers depend on. xtask consumes only the wit-bindgen pin, but
    /// CI and CD read the wasm-tools one from the same contract, so both are pinned here.
    #[test]
    fn makefile_pin_resolves_both_repo_pins() {
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("xtask/ has a parent");
        for name in ["WIT_BINDGEN_CLI_VERSION", "WASM_TOOLS_VERSION"] {
            let value = makefile_pin(repo_root, name);
            assert!(
                value.starts_with(|c: char| c.is_ascii_digit()),
                "{name} resolved to {value:?}, which is not a version"
            );
        }
    }

    #[test]
    fn version_field_takes_the_second_field_of_the_first_line() {
        assert_eq!(version_field("wit-bindgen-cli 1.2.3\n"), Some("1.2.3"));
        assert_eq!(version_field("wasm-tools 4.5.6"), Some("4.5.6"));
        // Space-separated build metadata is a later field and is dropped.
        assert_eq!(
            version_field("wit-bindgen-cli 1.2.3 (abc1234)\nsecond line 9.9.9"),
            Some("1.2.3")
        );
        // A suffix glued to the version token stays attached.
        assert_eq!(
            version_field("wit-bindgen-cli 1.2.3+abc\n"),
            Some("1.2.3+abc")
        );
        // Whitespace runs collapse and tabs count as separators, matching the awk the
        // Makefile preflight reads the same output with.
        assert_eq!(version_field("wit-bindgen-cli\t1.2.3\n"), Some("1.2.3"));
        assert_eq!(version_field("wit-bindgen-cli  1.2.3\n"), Some("1.2.3"));
        assert_eq!(version_field("single-token"), None);
        assert_eq!(version_field(""), None);
    }

    /// The generator assert's decision and its remediation, without a PATH shim. The
    /// install command is the substantive assertion: a stale or missing one sends the
    /// operator to install a version every other preflight then rejects.
    #[test]
    fn check_reported_version_accepts_only_the_pin() {
        assert_eq!(
            check_reported_version("wit-bindgen-cli 1.2.3\n", "1.2.3"),
            Ok(())
        );

        let msg = check_reported_version("wit-bindgen-cli 1.2.2\n", "1.2.3")
            .expect_err("a mismatched version is rejected");
        assert!(msg.contains("1.2.2"), "{msg}");
        assert!(msg.contains("1.2.3"), "{msg}");
        assert!(
            msg.contains("cargo install --locked wit-bindgen-cli --version 1.2.3"),
            "{msg}"
        );

        let msg = check_reported_version("unparseable\n", "1.2.3")
            .expect_err("unreadable output is rejected");
        assert!(msg.contains("unparseable"), "{msg}");
        assert!(
            msg.contains("cargo install --locked wit-bindgen-cli --version 1.2.3"),
            "{msg}"
        );
    }

    // Component-model text fixtures, assembled to binary at test time via `wat`.
    // No committed .wasm binaries; hermetic (no wasm-tools on PATH).
    const WASI_COMPONENT: &str = r#"(component (import "wasi:cli/environment@0.2.0" (instance)))"#;
    const CLEAN_COMPONENT: &str = r#"(component (import "brenn:x/y@0.1.0" (instance)))"#;
    const EMPTY_COMPONENT: &str = r#"(component)"#;
    const CORE_MODULE: &str = r#"(module)"#;
    const CORE_MODULE_WASI: &str = r#"(module (import "wasi_snapshot_preview1" "fd_write" (func (param i32 i32 i32 i32) (result i32))))"#;
    // Namespace that shares the `wasi` prefix but is not exactly `wasi`: must NOT be flagged
    // (guards the exact-match contract against a loosening to `starts_with`/`contains`).
    const WASI_LIKE_COMPONENT: &str = r#"(component (import "wasi-like:x/y@0.1.0" (instance)))"#;
    // Two distinct wasi imports: exercises the offender loop past its first hit plus the
    // sort/dedup step with more than one element.
    const TWO_WASI_COMPONENT: &str = r#"(component (import "wasi:cli/environment@0.2.0" (instance)) (import "wasi:clocks/wall-clock@0.2.0" (instance)))"#;
    // Valid component whose import is an ID-form function (not an instance): wit-parser's
    // decoder hits `unreachable!()` on this shape, which `wasi_imports` must contain as Err.
    const WASI_FUNC_COMPONENT: &str = r#"(component (import "wasi:x/y@0.2.0" (func)))"#;

    fn assemble(text: &str) -> Vec<u8> {
        wat::parse_str(text).expect("fixture assembles")
    }

    /// Decode then scan — the pairing `check_wasi_free` performs — so byte-level
    /// fixtures still exercise both the decode failure modes and the import scan.
    fn wasi_imports_of(bytes: &[u8]) -> Result<Vec<String>, String> {
        let (resolve, world_id) = decode_component_world(bytes)?;
        Ok(wasi_imports(&resolve, world_id))
    }

    /// Decode then compare — the pairing `check_world_equivalence` performs.
    fn world_conformance_of(bytes: &[u8], source: &WorldSignature) -> Result<(), Vec<String>> {
        let decoded = decode_component_world(bytes);
        world_conformance(decoded.as_ref().map_err(String::as_str), source)
    }

    #[test]
    fn wasi_imports_flags_wasi_package() {
        let offenders = wasi_imports_of(&assemble(WASI_COMPONENT)).expect("decodes");
        assert_eq!(offenders.len(), 1, "exactly one wasi import: {offenders:?}");
        assert!(
            offenders[0].contains("wasi:cli/environment"),
            "offender names the wasi interface: {offenders:?}"
        );
    }

    #[test]
    fn wasi_imports_clean_package_is_empty() {
        let offenders = wasi_imports_of(&assemble(CLEAN_COMPONENT)).expect("decodes");
        assert!(offenders.is_empty(), "no wasi imports: {offenders:?}");
    }

    #[test]
    fn wasi_imports_wasi_like_namespace_not_flagged() {
        // Exact-match contract: `wasi-like` shares the prefix but is not `wasi`.
        let offenders = wasi_imports_of(&assemble(WASI_LIKE_COMPONENT)).expect("decodes");
        assert!(
            offenders.is_empty(),
            "wasi-prefixed but non-wasi namespace must not be flagged: {offenders:?}"
        );
    }

    #[test]
    fn wasi_imports_reports_all_distinct_wasi_imports() {
        let offenders = wasi_imports_of(&assemble(TWO_WASI_COMPONENT)).expect("decodes");
        assert_eq!(
            offenders.len(),
            2,
            "both wasi imports reported: {offenders:?}"
        );
        assert!(
            offenders.iter().any(|o| o.contains("wasi:cli/environment")),
            "first wasi import present: {offenders:?}"
        );
        assert!(
            offenders
                .iter()
                .any(|o| o.contains("wasi:clocks/wall-clock")),
            "second wasi import present: {offenders:?}"
        );
        // Sorted output is deterministic for stable CI reporting.
        let mut sorted = offenders.clone();
        sorted.sort();
        assert_eq!(
            offenders, sorted,
            "offenders returned sorted: {offenders:?}"
        );
    }

    #[test]
    fn wasi_imports_wit_package_is_err() {
        // A WIT-package-encoded binary has a component header (so it passes the encoding
        // pre-check) but decodes to `DecodedWasm::WitPackage`, not a component. It must
        // fail the gate rather than pass vacuously. Built via `wit_component::encode`
        // since `wat` only assembles components/core modules, not WIT packages.
        let mut resolve = Resolve::new();
        let pkg = resolve
            .push_str("test.wit", "package a:b@0.1.0;\ninterface i {}\n")
            .expect("parse wit");
        let bytes = wit_component::encode(&resolve, pkg).expect("encode wit package");
        assert!(
            wasi_imports_of(&bytes).is_err(),
            "a WIT-package encoding must be rejected, not decoded as an empty component"
        );
    }

    #[test]
    fn wasi_imports_contains_decoder_panic() {
        // An ID-form function import makes wit-parser's decoder panic; `wasi_imports`
        // must convert that to a fail-closed Err rather than aborting the sweep.
        assert!(
            wasi_imports_of(&assemble(WASI_FUNC_COMPONENT)).is_err(),
            "a decoder panic must be contained as Err"
        );
    }

    #[test]
    fn wasi_imports_empty_component_is_empty() {
        let offenders = wasi_imports_of(&assemble(EMPTY_COMPONENT)).expect("decodes");
        assert!(offenders.is_empty(), "no imports: {offenders:?}");
    }

    #[test]
    fn wasi_imports_garbage_is_err() {
        assert!(wasi_imports_of(b"not a wasm binary at all").is_err());
    }

    #[test]
    fn wasi_imports_core_module_is_err() {
        // decode() alone accepts a core module and synthesizes an empty world; the
        // encoding pre-check must reject it so the gate fails closed.
        assert!(
            wasi_imports_of(&assemble(CORE_MODULE)).is_err(),
            "core module must be rejected, not pass vacuously"
        );
    }

    #[test]
    fn wasi_imports_core_module_with_wasi_is_err() {
        // The fail-open case the pre-check exists to close: a core module importing
        // wasi_snapshot_preview1 must never read as Ok(empty).
        assert!(
            wasi_imports_of(&assemble(CORE_MODULE_WASI)).is_err(),
            "core wasm importing wasi preview1 must be rejected"
        );
    }

    /// Build a temp crate dir (Cargo.toml with package.name) and an artifact dir
    /// containing `<name>.wasm` with the given bytes. Returns the `TempDir`; the crate
    /// dir is `<tmp>/crate` and the artifact dir is `<tmp>/artifacts`. The caller keeps
    /// the `TempDir` alive.
    fn temp_crate_with_artifact(name: &str, artifact_bytes: &[u8]) -> tempfile::TempDir {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        let crate_dir = root.join("crate");
        let artifact_dir = root.join("artifacts");
        fs::create_dir_all(&crate_dir).unwrap();
        fs::create_dir_all(&artifact_dir).unwrap();
        fs::write(
            crate_dir.join("Cargo.toml"),
            format!("[package]\nname = \"{name}\"\nversion = \"0.1.0\"\nedition = \"2021\"\n"),
        )
        .unwrap();
        let artifact_name = format!("{}.wasm", name.replace('-', "_"));
        fs::write(artifact_dir.join(artifact_name), artifact_bytes).unwrap();
        tmp
    }

    #[test]
    fn check_wasi_free_fails_on_wasi_artifact() {
        let tmp = temp_crate_with_artifact("wasi-comp", &assemble(WASI_COMPONENT));
        assert!(!check_wasi_free(&LoadedArtifact::load(
            &tmp.path().join("crate"),
            &tmp.path().join("artifacts")
        )));
    }

    #[test]
    fn check_wasi_free_passes_on_clean_artifact() {
        let tmp = temp_crate_with_artifact("clean-comp", &assemble(CLEAN_COMPONENT));
        assert!(check_wasi_free(&LoadedArtifact::load(
            &tmp.path().join("crate"),
            &tmp.path().join("artifacts")
        )));
    }

    #[test]
    #[should_panic(expected = "not found")]
    fn loading_an_absent_artifact_panics() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        let crate_dir = root.join("crate");
        fs::create_dir_all(&crate_dir).unwrap();
        fs::write(
            crate_dir.join("Cargo.toml"),
            "[package]\nname = \"absent-comp\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        )
        .unwrap();
        // artifacts dir exists but contains no matching .wasm.
        let artifact_dir = root.join("artifacts");
        fs::create_dir_all(&artifact_dir).unwrap();
        LoadedArtifact::load(&crate_dir, &artifact_dir);
    }

    /// A repo root carrying the two files the Family B mapping reads: the SDK's
    /// `generate!` invocation and the WIT file it names.
    fn temp_repo_with_guest_sdk(generate_invocation: &str) -> tempfile::TempDir {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        let wit_dir = root.join("brenn-wasm").join("wit");
        let sdk_src = root.join("brenn-wasm").join("guest").join("src");
        fs::create_dir_all(&wit_dir).unwrap();
        fs::create_dir_all(&sdk_src).unwrap();
        fs::write(
            wit_dir.join("processor.wit"),
            "package test:processor@0.1.0;\nworld processor {}\n",
        )
        .unwrap();
        fs::write(sdk_src.join("bindings.rs"), generate_invocation).unwrap();
        tmp
    }

    const REAL_SDK_GENERATE: &str = "wit_bindgen::generate!({\n    world: \"processor\",\n    \
         path: \"../wit/processor.wit\",\n});\n";

    /// Family B units carry no WIT path in Cargo metadata, so the mapping is the SDK's
    /// own world — the same file and world for every guest crate.
    #[test]
    fn wit_source_for_unit_maps_guest_units_to_the_sdk_world() {
        let tmp = temp_repo_with_guest_sdk(REAL_SDK_GENERATE);
        let root = tmp.path();
        // The crate dir is never consulted for a guest unit; pass the repo root itself.
        let (wit_path, world) = wit_source_for_unit(&Kind::WasmGuest, root, root);
        assert_eq!(
            wit_path,
            root.join("brenn-wasm")
                .join("wit")
                .join("processor.wit")
                .canonicalize()
                .unwrap()
        );
        assert_eq!(world, "processor");
    }

    /// The liveness guard on that hard-coded mapping: an SDK repointed at another world
    /// or file must stop the gate rather than silently compare guest artifacts against a
    /// world nothing generates them from.
    #[test]
    #[should_panic(expected = "no longer contains")]
    fn wit_source_for_unit_panics_when_the_guest_sdk_repoints() {
        let tmp = temp_repo_with_guest_sdk(
            "wit_bindgen::generate!({\n    world: \"something-else\",\n    \
             path: \"../wit/processor.wit\",\n});\n",
        );
        wit_source_for_unit(&Kind::WasmGuest, tmp.path(), tmp.path());
    }

    /// Write a crate dir containing just a Cargo.toml with the given contents.
    fn temp_crate_with_cargo(cargo_toml: &str) -> tempfile::TempDir {
        let tmp = tempfile::tempdir().expect("tempdir");
        fs::write(tmp.path().join("Cargo.toml"), cargo_toml).unwrap();
        tmp
    }

    /// The hyphen→underscore transform is applied to the package name.
    #[test]
    fn artifact_name_for_transforms_hyphens() {
        let tmp = temp_crate_with_cargo(
            "[package]\nname = \"my-cool-crate\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        );
        assert_eq!(artifact_name_for(tmp.path()), "my_cool_crate.wasm");
    }

    #[test]
    #[should_panic(expected = "xtask check-wit: failed to read")]
    fn artifact_name_for_missing_cargo_panics() {
        // Empty crate dir: no Cargo.toml at all.
        let tmp = tempfile::tempdir().expect("tempdir");
        artifact_name_for(tmp.path());
    }

    #[test]
    #[should_panic(expected = "xtask check-wit: failed to parse")]
    fn artifact_name_for_malformed_toml_panics() {
        let tmp = temp_crate_with_cargo("not = = valid [[[");
        artifact_name_for(tmp.path());
    }

    #[test]
    #[should_panic(expected = "no package.name in")]
    fn artifact_name_for_missing_name_panics() {
        // Valid TOML, but no [package] name.
        let tmp = temp_crate_with_cargo("[other]\nkey = \"value\"\n");
        artifact_name_for(tmp.path());
    }

    /// A scratch dir disjoint from the repo passes and returns its canonical path.
    #[test]
    fn scratch_root_outside_repo_accepts_disjoint_dir() {
        let repo = tempfile::tempdir().expect("repo tempdir");
        let scratch = tempfile::tempdir().expect("scratch tempdir");
        let got = scratch_root_outside_repo(scratch.path(), repo.path());
        assert_eq!(got, scratch.path().canonicalize().unwrap());
    }

    /// A scratch dir physically inside the repo trips the guard.
    #[test]
    #[should_panic(expected = "resolves inside the repo")]
    fn scratch_root_outside_repo_rejects_dir_inside_repo() {
        let repo = tempfile::tempdir().expect("repo tempdir");
        let inside = repo.path().join("scratch");
        fs::create_dir_all(&inside).unwrap();
        scratch_root_outside_repo(&inside, repo.path());
    }

    /// A symlink that lives outside the repo (lexically disjoint) but resolves inside it
    /// must still trip the guard. A lexical `starts_with` without canonicalizing first
    /// would miss this and fail open — the exact regression the canonicalize-first order
    /// exists to prevent.
    #[cfg(unix)]
    #[test]
    #[should_panic(expected = "resolves inside the repo")]
    fn scratch_root_outside_repo_rejects_symlink_into_repo() {
        let repo = tempfile::tempdir().expect("repo tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let real_inside = repo.path().join("real-scratch");
        fs::create_dir_all(&real_inside).unwrap();
        let link = outside.path().join("link");
        std::os::unix::fs::symlink(&real_inside, &link).unwrap();
        scratch_root_outside_repo(&link, repo.path());
    }

    /// Sorted (name, bytes) of every entry directly under `dir`. Directory entries carry
    /// empty bytes; used to prove a check left a directory untouched.
    fn dir_snapshot(dir: &Path) -> Vec<(String, Vec<u8>)> {
        let mut entries: Vec<(String, Vec<u8>)> = fs::read_dir(dir)
            .unwrap()
            .map(|e| {
                let e = e.unwrap();
                let name = e.file_name().to_string_lossy().into_owned();
                let bytes = if e.path().is_file() {
                    fs::read(e.path()).unwrap()
                } else {
                    Vec::new()
                };
                (name, bytes)
            })
            .collect();
        entries.sort();
        entries
    }

    // World-equivalence fixtures. The "artifact side" of these tests is a WIT text too:
    // what the gate compares is two decoded worlds, and a mutated WIT is exactly how a
    // moved guest-visible world presents. The real artifacts are exercised by the live
    // sweep in every `make check`.
    const SOURCE_WIT: &str = r#"
package test:app@0.1.0;

interface types {
    record item { id: u32, label: string }
    variant fault { bad-input(string), refused }
}

interface store {
    resource handle {
        read: func(key: string) -> option<string>;
        write: func(key: string, value: string);
    }
    open: func() -> handle;
}

interface config {
    get: func(key: string) -> option<string>;
}

world app {
    use types.{item, fault};
    import store;
    import config;
    export accept: func(i: item) -> result<_, fault>;
}
"#;

    /// Parse one WIT text and reduce the named world to its signature.
    fn signature_of(wit: &str, world: &str) -> WorldSignature {
        let mut resolve = Resolve::new();
        let pkg = resolve.push_str("fixture.wit", wit).expect("parse wit");
        let id = resolve
            .select_world(&[pkg], Some(world))
            .expect("select world");
        world_signature(&resolve, id)
    }

    /// Conformance of a mutated "artifact" world against the unmutated source.
    fn conformance(artifact_wit: &str) -> Result<(), Vec<String>> {
        conformance_of(SOURCE_WIT, artifact_wit)
    }

    /// Conformance of one WIT text's `app` world against another's.
    fn conformance_of(source_wit: &str, artifact_wit: &str) -> Result<(), Vec<String>> {
        signatures_conform(
            &signature_of(source_wit, "app"),
            &signature_of(artifact_wit, "app"),
        )
    }

    /// A world that *exports* an interface. Neither in-tree world does today (both export
    /// a bare func), so without this fixture the export rule's member-equality arm — the
    /// strictness the gate's doc comment advertises — is never entered by any test.
    const EXPORTING_WIT: &str = r#"package test:iface@0.1.0;

interface ops {
    ping: func();
    pong: func(n: u32) -> u32;
}

world app {
    export ops;
}
"#;

    /// The unmutated pair conforms — the "passes today" direction.
    #[test]
    fn world_conformance_accepts_the_unmutated_world() {
        assert_eq!(conformance(SOURCE_WIT), Ok(()));
    }

    /// Whole-import elision is legal: `brenn_replay.wasm` really does drop a declared
    /// import it never calls.
    #[test]
    fn world_conformance_accepts_an_elided_import() {
        let elided = SOURCE_WIT.replace("    import config;\n", "");
        assert_ne!(elided, SOURCE_WIT, "the fixture edit applied");
        assert_eq!(conformance(&elided), Ok(()));
    }

    /// Member-level elision is legal too, and is what the in-tree artifacts actually do:
    /// an imported interface arrives carrying only the members the guest reaches.
    #[test]
    fn world_conformance_accepts_an_elided_interface_member() {
        let elided = SOURCE_WIT.replace("        write: func(key: string, value: string);\n", "");
        assert_ne!(elided, SOURCE_WIT, "the fixture edit applied");
        assert_eq!(conformance(&elided), Ok(()));
    }

    /// An import the source world does not declare is an unagreed host obligation.
    #[test]
    fn world_conformance_rejects_an_added_import() {
        let added = SOURCE_WIT.replace(
            "world app {",
            "interface extra { ping: func(); }\n\nworld app {\n    import extra;",
        );
        let problems = conformance(&added).expect_err("added import must fail");
        assert!(
            problems.iter().any(|p| p.contains("test:app/extra@0.1.0")),
            "{problems:?}"
        );
    }

    /// A renamed function inside an imported interface: the artifact carries a member the
    /// source interface does not declare. This is the generator-drift class the whole gate
    /// exists for — `check_bindings_drift` cannot see it.
    #[test]
    fn world_conformance_rejects_a_renamed_import_function() {
        let renamed = SOURCE_WIT.replace(
            "    open: func() -> handle;",
            "    open-store: func() -> handle;",
        );
        let problems = conformance(&renamed).expect_err("renamed import fn must fail");
        assert!(
            problems
                .iter()
                .any(|p| p.contains("func open-store") && p.contains("does not declare")),
            "{problems:?}"
        );
    }

    /// A changed member shape is caught even though the name is untouched.
    #[test]
    fn world_conformance_rejects_a_changed_member_shape() {
        let widened = SOURCE_WIT.replace(
            "record item { id: u32, label: string }",
            "record item { id: u64, label: string }",
        );
        let problems = conformance(&widened).expect_err("changed shape must fail");
        assert!(
            problems.iter().any(|p| p.contains("different shape")),
            "{problems:?}"
        );
    }

    /// Exports may not be omitted: a missing one breaks the host's call surface.
    #[test]
    fn world_conformance_rejects_a_removed_export() {
        let removed = SOURCE_WIT.replace(
            "    export accept: func(i: item) -> result<_, fault>;\n",
            "",
        );
        assert_ne!(removed, SOURCE_WIT, "the fixture edit applied");
        let problems = conformance(&removed).expect_err("removed export must fail");
        assert!(
            problems
                .iter()
                .any(|p| p.contains("`accept`") && p.contains("does not carry")),
            "{problems:?}"
        );
    }

    /// Exports may not be added either.
    #[test]
    fn world_conformance_rejects_an_added_export() {
        let added = SOURCE_WIT.replace(
            "    export accept:",
            "    export extra-hook: func();\n    export accept:",
        );
        let problems = conformance(&added).expect_err("added export must fail");
        assert!(
            problems.iter().any(|p| p.contains("`extra-hook`")),
            "{problems:?}"
        );
    }

    /// A changed export signature is caught.
    #[test]
    fn world_conformance_rejects_a_changed_export_signature() {
        let changed = SOURCE_WIT.replace(
            "export accept: func(i: item) -> result<_, fault>;",
            "export accept: func(i: item) -> result<u32, fault>;",
        );
        let problems = conformance(&changed).expect_err("changed export must fail");
        assert!(
            problems
                .iter()
                .any(|p| p.contains("export `accept`") && p.contains("different shape")),
            "{problems:?}"
        );
    }

    /// An exported interface conforms to itself — the guard that the three mutation tests
    /// below fail for their mutation and not for the fixture's shape.
    #[test]
    fn world_conformance_accepts_the_unmutated_exported_interface() {
        assert_eq!(conformance_of(EXPORTING_WIT, EXPORTING_WIT), Ok(()));
    }

    /// Export members may not be elided, unlike import members: the host calls every one.
    #[test]
    fn world_conformance_rejects_an_elided_export_interface_member() {
        let elided = EXPORTING_WIT.replace("    pong: func(n: u32) -> u32;\n", "");
        assert_ne!(elided, EXPORTING_WIT, "the fixture edit applied");
        let problems =
            conformance_of(EXPORTING_WIT, &elided).expect_err("elided export member must fail");
        assert!(
            problems
                .iter()
                .any(|p| p.contains("func pong") && p.contains("does not carry")),
            "{problems:?}"
        );
    }

    /// Nor added: an export nothing declared is contract noise, same as at item level.
    #[test]
    fn world_conformance_rejects_an_added_export_interface_member() {
        let added =
            EXPORTING_WIT.replace("    ping: func();", "    ping: func();\n    peek: func();");
        assert_ne!(added, EXPORTING_WIT, "the fixture edit applied");
        let problems =
            conformance_of(EXPORTING_WIT, &added).expect_err("added export member must fail");
        assert!(
            problems
                .iter()
                .any(|p| p.contains("func peek") && p.contains("does not declare")),
            "{problems:?}"
        );
    }

    /// A reshaped export member is caught even though the member set is unchanged — the
    /// case a transposed `MemberRule` would still catch, so it is asserted separately
    /// from the two above.
    #[test]
    fn world_conformance_rejects_a_changed_export_interface_member_shape() {
        let widened =
            EXPORTING_WIT.replace("pong: func(n: u32) -> u32;", "pong: func(n: u64) -> u32;");
        assert_ne!(widened, EXPORTING_WIT, "the fixture edit applied");
        let problems =
            conformance_of(EXPORTING_WIT, &widened).expect_err("changed export member must fail");
        assert!(
            problems.iter().any(|p| p.contains("different shape")),
            "{problems:?}"
        );
    }

    /// Fail closed on bytes that are not a component: a core module, a WIT package, a
    /// binary the decoder panics on, and garbage all fail rather than comparing an empty
    /// world against the source.
    #[test]
    fn world_conformance_fails_closed_on_non_components() {
        let source = signature_of(SOURCE_WIT, "app");
        // A WIT-package encoding carries a component header, so it passes the encoding
        // pre-check and is rejected only by the decode branch. `wat` cannot assemble one.
        let mut resolve = Resolve::new();
        let pkg = resolve
            .push_str("test.wit", "package a:b@0.1.0;\ninterface i {}\n")
            .expect("parse wit");
        let wit_package = wit_component::encode(&resolve, pkg).expect("encode wit package");
        for (label, bytes) in [
            ("core module", assemble(CORE_MODULE)),
            ("wit package", wit_package),
            ("decoder panic", assemble(WASI_FUNC_COMPONENT)),
            ("garbage", b"not a wasm binary at all".to_vec()),
        ] {
            assert!(
                world_conformance_of(&bytes, &source).is_err(),
                "{label} must fail the gate closed"
            );
        }
    }

    /// An actual component binary whose world is empty still fails: the source world's
    /// export is missing. This is the decode path end to end, not just the rule table.
    #[test]
    fn world_conformance_rejects_a_component_without_the_export() {
        let source = signature_of(SOURCE_WIT, "app");
        let problems = world_conformance_of(&assemble(EMPTY_COMPONENT), &source)
            .expect_err("an export-less component must fail");
        assert!(
            problems.iter().any(|p| p.contains("`accept`")),
            "{problems:?}"
        );
    }

    /// The tree-read-only invariant: `check_bindings_drift` must never write into the
    /// crate's `src/`. It generates into the passed scratch dir and byte-compares; a
    /// regression pointing `--out-dir` back at `src/` (reintroducing the vanishing-file
    /// mutation this gate was rewritten to remove) would add or modify entries under
    /// `src/`. Robust whether or not `wit-bindgen` is on PATH: no code path writes to
    /// `src/`, so even a wit-bindgen-absent spawn panic leaves the tree untouched.
    #[test]
    fn check_bindings_drift_leaves_crate_src_untouched() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let crate_dir = tmp.path().join("crate");
        let src_dir = crate_dir.join("src");
        let wit_dir = crate_dir.join("wit");
        fs::create_dir_all(&src_dir).unwrap();
        fs::create_dir_all(&wit_dir).unwrap();
        fs::write(
            wit_dir.join("thing.wit"),
            "package example:thing;\nworld thing {}\n",
        )
        .unwrap();
        fs::write(
            crate_dir.join("Cargo.toml"),
            "[package]\nname = \"thing\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n\
             [package.metadata.component.target]\npath = \"wit/thing.wit\"\n",
        )
        .unwrap();
        // Committed bindings.rs with sentinel bytes; the gate may report drift, but src/
        // must not be mutated regardless of the outcome.
        let bindings = src_dir.join("bindings.rs");
        let sentinel = b"// committed bindings sentinel\n";
        fs::write(&bindings, sentinel).unwrap();

        let before = dir_snapshot(&src_dir);

        let scratch = tempfile::tempdir().expect("scratch tempdir");
        // Tolerate a wit-bindgen-absent panic; either way, assert src/ is untouched.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            check_bindings_drift(&crate_dir, scratch.path(), 0, "0.0.0")
        }));

        let after = dir_snapshot(&src_dir);
        assert_eq!(
            before, after,
            "check_bindings_drift must not add or modify entries under src/"
        );
        assert_eq!(
            fs::read(&bindings).unwrap(),
            sentinel,
            "committed bindings.rs must be byte-identical after the check"
        );
    }
}
