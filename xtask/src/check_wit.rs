// `xtask check-wit`: WASI-free gate + bindings-drift gate.
// See design §5.

use crate::discover::{Kind, discover_units};
use std::path::{Path, PathBuf};
use std::process::Command;
use wasmparser::{Encoding, Parser, Payload};
use wit_component::{DecodedWasm, decode};
use wit_parser::{Resolve, TypeOwner, WorldItem, WorldKey};

/// Run both WIT gates over all applicable units. Returns true if all pass.
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

    for (unit_index, unit) in units.iter().enumerate() {
        match unit.kind {
            Kind::WasmComponent | Kind::WasmGuest => {
                // WASI-free gate.
                if !check_wasi_free(&unit.dir, &artifact_dir) {
                    ok = false;
                }
                // Bindings-drift gate: WasmComponent only (Family A has committed bindings.rs).
                if unit.kind == Kind::WasmComponent
                    && !check_bindings_drift(&unit.dir, &scratch_root, unit_index)
                {
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

/// WASI-free gate: structurally decode the component artifact and assert it imports
/// no `wasi:`-namespaced package. Panics if the artifact doesn't exist (run
/// `make wasm-components` first).
fn check_wasi_free(crate_dir: &Path, artifact_dir: &Path) -> bool {
    // Derive artifact name from crate package name.
    let artifact_name = artifact_name_for(crate_dir);
    let artifact_path = artifact_dir.join(&artifact_name);

    assert!(
        artifact_path.exists(),
        "xtask check-wit: artifact {artifact_path:?} not found. \
         Run `make wasm-components` first to build all WASM artifacts."
    );

    // Existence was just asserted; a read failure now is unexpected → fail fast.
    let bytes = std::fs::read(&artifact_path)
        .unwrap_or_else(|e| panic!("xtask check-wit: failed to read {artifact_path:?}: {e}"));

    match wasi_imports(&bytes) {
        Err(msg) => {
            eprintln!(
                "xtask check-wit [wasi-free FAIL]: {artifact_path:?} did not decode as a \
                 component with the locked wit-component version: {msg}"
            );
            false
        }
        Ok(offenders) if !offenders.is_empty() => {
            eprintln!("xtask check-wit [wasi-free FAIL]: {artifact_path:?} imports wasi:*:");
            for name in &offenders {
                eprintln!("  {name}");
            }
            false
        }
        Ok(_) => true,
    }
}

/// Decode a component binary and return the fully-qualified names (sorted, deduped)
/// of all world imports that resolve to a `wasi:`-namespaced package.
///
/// `Err(msg)` means the bytes do not decode as a component: a malformed artifact, a
/// core wasm module, a WIT-package encoding, or a component newer than the locked
/// decoder supports. Every such case fails the gate closed rather than passing
/// vacuously.
fn wasi_imports(component_bytes: &[u8]) -> Result<Vec<String>, String> {
    // Encoding pre-check: wit_component::decode accepts a plain core module and
    // synthesizes an empty world, which would let a core module (potentially
    // importing wasi_snapshot_preview1) pass the gate. Require the binary to
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
    let (resolve, world_id) = match decoded {
        DecodedWasm::Component(r, w) => (r, w),
        DecodedWasm::WitPackage(..) => {
            return Err("artifact is a WIT package encoding, not a component".to_string());
        }
    };

    let mut offenders = Vec::new();
    for (key, item) in &resolve.worlds[world_id].imports {
        match (key, item) {
            // A named-key interface carries no package (inline interface), so in practice
            // only the ID-keyed case can be wasi; the helper guards both uniformly.
            (WorldKey::Interface(id), _) | (WorldKey::Name(_), WorldItem::Interface { id, .. }) => {
                if let Some(name) = wasi_interface_name(&resolve, *id) {
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
                    && let Some(owner_id) = wasi_interface_name(&resolve, owner)
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
    Ok(offenders)
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

/// Bindings-drift gate: regenerate the crate's bindings into an ephemeral scratch dir
/// via `wit-bindgen-cli` 0.58 and byte-compare against the committed `src/bindings.rs`.
/// Never writes into the crate — the working tree is untouched, so this gate is safe to
/// run concurrently with lanes that walk the tree.
fn check_bindings_drift(crate_dir: &Path, scratch_root: &Path, unit_index: usize) -> bool {
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
            panic!(
                "xtask check-wit: failed to run `wit-bindgen rust` for {crate_dir:?}: {e}. \
                 Install with: cargo install --locked wit-bindgen-cli --version 0.58.0"
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

    #[test]
    fn wasi_imports_flags_wasi_package() {
        let offenders = wasi_imports(&assemble(WASI_COMPONENT)).expect("decodes");
        assert_eq!(offenders.len(), 1, "exactly one wasi import: {offenders:?}");
        assert!(
            offenders[0].contains("wasi:cli/environment"),
            "offender names the wasi interface: {offenders:?}"
        );
    }

    #[test]
    fn wasi_imports_clean_package_is_empty() {
        let offenders = wasi_imports(&assemble(CLEAN_COMPONENT)).expect("decodes");
        assert!(offenders.is_empty(), "no wasi imports: {offenders:?}");
    }

    #[test]
    fn wasi_imports_wasi_like_namespace_not_flagged() {
        // Exact-match contract: `wasi-like` shares the prefix but is not `wasi`.
        let offenders = wasi_imports(&assemble(WASI_LIKE_COMPONENT)).expect("decodes");
        assert!(
            offenders.is_empty(),
            "wasi-prefixed but non-wasi namespace must not be flagged: {offenders:?}"
        );
    }

    #[test]
    fn wasi_imports_reports_all_distinct_wasi_imports() {
        let offenders = wasi_imports(&assemble(TWO_WASI_COMPONENT)).expect("decodes");
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
            wasi_imports(&bytes).is_err(),
            "a WIT-package encoding must be rejected, not decoded as an empty component"
        );
    }

    #[test]
    fn wasi_imports_contains_decoder_panic() {
        // An ID-form function import makes wit-parser's decoder panic; `wasi_imports`
        // must convert that to a fail-closed Err rather than aborting the sweep.
        assert!(
            wasi_imports(&assemble(WASI_FUNC_COMPONENT)).is_err(),
            "a decoder panic must be contained as Err"
        );
    }

    #[test]
    fn wasi_imports_empty_component_is_empty() {
        let offenders = wasi_imports(&assemble(EMPTY_COMPONENT)).expect("decodes");
        assert!(offenders.is_empty(), "no imports: {offenders:?}");
    }

    #[test]
    fn wasi_imports_garbage_is_err() {
        assert!(wasi_imports(b"not a wasm binary at all").is_err());
    }

    #[test]
    fn wasi_imports_core_module_is_err() {
        // decode() alone accepts a core module and synthesizes an empty world; the
        // encoding pre-check must reject it so the gate fails closed.
        assert!(
            wasi_imports(&assemble(CORE_MODULE)).is_err(),
            "core module must be rejected, not pass vacuously"
        );
    }

    #[test]
    fn wasi_imports_core_module_with_wasi_is_err() {
        // The fail-open case the pre-check exists to close: a core module importing
        // wasi_snapshot_preview1 must never read as Ok(empty).
        assert!(
            wasi_imports(&assemble(CORE_MODULE_WASI)).is_err(),
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
        assert!(!check_wasi_free(
            &tmp.path().join("crate"),
            &tmp.path().join("artifacts")
        ));
    }

    #[test]
    fn check_wasi_free_passes_on_clean_artifact() {
        let tmp = temp_crate_with_artifact("clean-comp", &assemble(CLEAN_COMPONENT));
        assert!(check_wasi_free(
            &tmp.path().join("crate"),
            &tmp.path().join("artifacts")
        ));
    }

    #[test]
    #[should_panic(expected = "not found")]
    fn check_wasi_free_panics_on_absent_artifact() {
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
        check_wasi_free(&crate_dir, &artifact_dir);
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
            check_bindings_drift(&crate_dir, scratch.path(), 0)
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
