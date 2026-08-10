// Discovery core: walk the repo tree, find every Cargo workspace, classify each.
// Shared by all subcommands.

use std::path::{Path, PathBuf};

/// Classification of a discovered Rust unit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    /// The root workspace (brenn/Cargo.toml) and all its members.
    RootWorkspace,
    /// A virtual workspace of wasm32-unknown-unknown crates: the brenn-guest SDK
    /// and the component crates built against it or against raw WIT bindings.
    WasmWorkspace,
}

impl Kind {
    /// Canonical string form used in the allowlist TOML.
    pub fn as_str(&self) -> &'static str {
        match self {
            Kind::RootWorkspace => "root-workspace",
            Kind::WasmWorkspace => "wasm-workspace",
        }
    }

    /// Parse from the allowlist TOML string.
    pub fn from_str(s: &str) -> Option<Kind> {
        match s {
            "root-workspace" => Some(Kind::RootWorkspace),
            "wasm-workspace" => Some(Kind::WasmWorkspace),
            _ => None,
        }
    }
}

/// A discovered Rust unit: a directory containing a standalone [workspace] Cargo.toml,
/// or the repo root workspace.
#[derive(Debug, Clone)]
pub struct Unit {
    /// Absolute path to the crate directory (contains Cargo.toml).
    pub dir: PathBuf,
    pub kind: Kind,
}

/// Walk `repo_root`, find every Cargo.toml, classify each into a Unit.
///
/// Panics if:
/// - repo_root/Cargo.toml doesn't exist or has no [workspace] table
/// - any Cargo.toml with [workspace] doesn't classify into a known kind
/// - any Cargo.toml without [workspace] is not a member of its nearest enclosing
///   workspace (orphan)
///
/// Returns one RootWorkspace unit plus one unit per standalone workspace.
pub fn discover_units(repo_root: &Path) -> Vec<Unit> {
    assert!(
        repo_root.join("Cargo.toml").exists(),
        "discover_units: repo root Cargo.toml not found at {:?}",
        repo_root.join("Cargo.toml")
    );

    let mut manifests: Vec<PathBuf> = Vec::new();
    let mut stack = vec![repo_root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        // Read dir entries; panic on I/O errors (better dead than wrong).
        let entries = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("Failed to read directory {dir:?}: {e}"));
        for entry in entries {
            let entry =
                entry.unwrap_or_else(|e| panic!("Failed to read dir entry in {dir:?}: {e}"));
            let path = entry.path();
            let file_name = entry.file_name();
            let name = file_name.to_string_lossy();

            // Exclude target/ and .git/ by path component name — must be component-name
            // match, NOT a root-only starts_with check, to catch nested build dirs like
            // brenn-wasm/components/target/.
            // Also exclude all hidden directories (starting with '.') — these include
            // tooling-internal dirs like .claude/worktrees/ that may contain Cargo.toml
            // files from worktree checkouts and should never be classified as components.
            if name == "target" || name.starts_with('.') {
                continue;
            }

            // Never follow a directory symlink. The guest workspace reaches the
            // first-party crates it shares with the host through one, and the
            // crates on the far side are already walked at their real paths.
            let meta = std::fs::symlink_metadata(&path)
                .unwrap_or_else(|e| panic!("discover_units: failed to stat {path:?}: {e}"));
            if !meta.is_dir() {
                continue;
            }
            if path.join("Cargo.toml").exists() && path != repo_root {
                let rel = path.strip_prefix(repo_root).unwrap_or_else(|_| {
                    panic!("Path {path:?} is not under repo root {repo_root:?}")
                });
                manifests.push(rel.join("Cargo.toml"));
            }
            stack.push(path);
        }
    }

    units_from_manifests(repo_root, &manifests, "discover_units")
}

/// Classify every `Cargo.toml` in `files` (repo-root-relative) into a Unit.
///
/// Same rules and same panics as `discover_units`, over a caller-supplied file
/// set instead of a tree walk: a manifest is either the root, a standalone
/// workspace, or a member of the nearest workspace above it. Which manifests
/// exist is then a declared input rather than whatever the filesystem happened
/// to hold.
pub fn discover_units_from_files(repo_root: &Path, files: &[PathBuf]) -> Vec<Unit> {
    let mut manifests: Vec<PathBuf> = files
        .iter()
        .filter(|rel| rel.file_name().is_some_and(|n| n == "Cargo.toml"))
        .filter(|rel| rel.parent().is_some_and(|d| d != Path::new("")))
        .cloned()
        .collect();
    manifests.sort();

    units_from_manifests(repo_root, &manifests, "discover_units_from_files")
}

/// A workspace root and the members it declares, both repo-root-relative
/// (the root workspace's dir is the empty path).
struct Workspace {
    dir: PathBuf,
    members: Vec<PathBuf>,
}

/// Classify a set of repo-root-relative `Cargo.toml` paths into Units.
///
/// The root workspace is always a unit. Every other manifest either carries its
/// own `[workspace]` table — a standalone workspace, classified — or must be a
/// member of the nearest workspace above it. A manifest that is neither is an
/// orphan and panics: no lint lane would ever reach it.
fn units_from_manifests(repo_root: &Path, manifests: &[PathBuf], caller: &str) -> Vec<Unit> {
    let root_cargo = repo_root.join("Cargo.toml");
    let root_toml = read_manifest(&root_cargo);
    assert!(
        root_toml.get("workspace").is_some(),
        "{caller}: {root_cargo:?} has no [workspace] table — wrong repo root?"
    );

    let mut units = vec![Unit {
        dir: repo_root.to_path_buf(),
        kind: Kind::RootWorkspace,
    }];
    let mut workspaces = vec![Workspace {
        dir: PathBuf::new(),
        members: collect_workspace_members(&root_toml),
    }];

    // Pass 1: every workspace root, so pass 2 resolves membership whatever order
    // the manifests arrive in.
    let mut plain: Vec<PathBuf> = Vec::new();
    for rel in manifests {
        let dir_rel = rel.parent().expect("filtered by caller").to_path_buf();
        let cargo_toml = repo_root.join(rel);
        let parsed = read_manifest(&cargo_toml);
        if parsed.get("workspace").is_some() {
            units.push(Unit {
                dir: repo_root.join(&dir_rel),
                kind: classify(&parsed, &cargo_toml),
            });
            workspaces.push(Workspace {
                dir: dir_rel,
                members: collect_workspace_members(&parsed),
            });
        } else {
            plain.push(dir_rel);
        }
    }

    // Pass 2: membership. Nearest enclosing workspace wins, matching cargo.
    for dir_rel in plain {
        let ws = workspaces
            .iter()
            .filter(|ws| dir_rel.starts_with(&ws.dir))
            .max_by_key(|ws| ws.dir.components().count())
            .unwrap_or_else(|| panic!("{caller}: no workspace encloses {dir_rel:?}"));
        let member_rel = dir_rel
            .strip_prefix(&ws.dir)
            .expect("filtered by starts_with");
        assert!(
            ws.members.iter().any(|m| m == member_rel),
            "{caller}: {dir_rel:?} has no [workspace] table and is not a member of the workspace \
             at {:?} — orphan crate? Add it to that workspace's members or give it its own \
             [workspace] table.",
            ws.dir,
        );
    }

    units
}

/// Which generator owns a wasm crate's WIT bindings. Orthogonal to `Kind`:
/// `Kind` is the lint unit (a whole workspace), `Family` is the per-crate WIT
/// provenance the check-wit gates dispatch on.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Family {
    /// Committed `bindings.rs` from the pinned wit-bindgen CLI; names its WIT
    /// file in `[package.metadata.component]`.
    Raw,
    /// Bindings come from the brenn-guest SDK's `generate!` invocation.
    Guest,
    /// The brenn-guest SDK itself. Builds no component artifact.
    Sdk,
}

/// A member crate of a wasm workspace.
#[derive(Debug, Clone)]
pub struct WasmCrate {
    /// Absolute path to the crate directory.
    pub dir: PathBuf,
    pub family: Family,
}

/// Every member crate of every discovered wasm workspace, with its family.
///
/// The member list comes from the workspace manifest, so a crate that exists on
/// disk but is not a member is invisible here — and `discover_units` has already
/// panicked on it as an orphan.
pub fn wasm_crates(repo_root: &Path) -> Vec<WasmCrate> {
    let mut crates = Vec::new();
    for unit in discover_units(repo_root) {
        if unit.kind != Kind::WasmWorkspace {
            continue;
        }
        let ws_manifest = read_manifest(&unit.dir.join("Cargo.toml"));
        for member in collect_workspace_members(&ws_manifest) {
            let dir = unit.dir.join(member);
            let manifest = read_manifest(&dir.join("Cargo.toml"));
            crates.push(WasmCrate {
                family: classify_family(&manifest, &dir),
                dir,
            });
        }
    }
    crates
}

/// Classify one wasm workspace member into a `Family`. Panics on unclassifiable.
fn classify_family(manifest: &toml::Value, crate_dir: &Path) -> Family {
    let package = manifest.get("package");
    let pkg_name = package.and_then(|p| p.get("name")).and_then(|n| n.as_str());
    if pkg_name == Some("brenn-guest") {
        return Family::Sdk;
    }
    if package
        .and_then(|p| p.get("metadata"))
        .and_then(|m| m.get("component"))
        .is_some()
    {
        return Family::Raw;
    }
    let has_guest_dep = manifest
        .get("dependencies")
        .and_then(|d| d.as_table())
        .is_some_and(|deps| deps.contains_key("brenn-guest"));
    if has_guest_dep {
        return Family::Guest;
    }
    panic!(
        "discover: unclassifiable wasm workspace member at {crate_dir:?}.\n\
         Does not match any known family:\n\
         - Sdk (package.name == \"brenn-guest\"): no\n\
         - Raw ([package.metadata.component]): no\n\
         - Guest (brenn-guest in [dependencies]): no\n\
         Add a classification rule in xtask/src/discover.rs or re-examine this crate's structure."
    );
}

/// Read and parse a Cargo.toml. Panics on unreadable or malformed input.
fn read_manifest(path: &Path) -> toml::Value {
    let content =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("Failed to read {path:?}: {e}"));
    toml::from_str(&content).unwrap_or_else(|e| panic!("Failed to parse {path:?}: {e}"))
}

/// Classify a directory as a standalone workspace unit by parsing its Cargo.toml.
///
/// Returns `None` if the dir has no `Cargo.toml` or no `[workspace]` table (i.e. it is not a
/// standalone workspace). Panics on unclassifiable standalone workspaces (same as `discover_units`).
/// Used by `lint_one` for out-of-tree paths not in the brenn discovery set.
pub fn classify_dir(dir: &Path) -> Option<Kind> {
    let cargo_toml = dir.join("Cargo.toml");
    if !cargo_toml.exists() {
        return None;
    }
    let parsed = read_manifest(&cargo_toml);
    parsed.get("workspace")?;
    Some(classify(&parsed, &cargo_toml))
}

/// Classify a standalone workspace's Cargo.toml into a Kind.
/// Panics on unclassifiable.
fn classify(toml: &toml::Value, cargo_toml_path: &Path) -> Kind {
    // WasmWorkspace: a virtual workspace (no [package] of its own) with members.
    // That is the shape of the wasm32 guest workspace, and the only standalone
    // workspace this tree has.
    let is_virtual = toml.get("package").is_none();
    let has_members = !collect_workspace_members(toml).is_empty();
    if is_virtual && has_members {
        return Kind::WasmWorkspace;
    }

    panic!(
        "discover_units: unclassifiable standalone [workspace] at {cargo_toml_path:?}.\n\
         Does not match any known kind:\n\
         - WasmWorkspace (virtual [workspace] with members): {}.\n\
         A single-crate [workspace] opt-out is no longer a supported shape — the crate \
         belongs in a workspace that a lint lane covers. Add it to one, or add a \
         classification rule in xtask/src/discover.rs.",
        if is_virtual {
            "no members"
        } else {
            "has its own [package]"
        }
    );
}

/// Extract the list of member paths (relative to workspace root) from a parsed workspace Cargo.toml.
fn collect_workspace_members(toml: &toml::Value) -> Vec<PathBuf> {
    toml.get("workspace")
        .and_then(|w| w.get("members"))
        .and_then(|m| m.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .map(PathBuf::from)
                .collect()
        })
        .unwrap_or_else(|| {
            // Memberless workspace (e.g. a standalone crate that just has [workspace] to opt out).
            // That's fine — it has no members to enumerate.
            Vec::new()
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// The repo root, from CARGO_MANIFEST_DIR (xtask/).
    ///
    /// Under a build system that hands the variable over workspace-relative,
    /// xtask/'s parent is the empty path; the tree is then rooted at the
    /// working directory the test starts in.
    fn repo_root() -> PathBuf {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        match manifest_dir.parent() {
            Some(parent) if parent.as_os_str().is_empty() => PathBuf::from("."),
            Some(parent) => parent.to_path_buf(),
            None => panic!("xtask/ has no parent"),
        }
    }

    /// Assert discovery finds the root workspace and the one standalone wasm
    /// workspace, and that every other manifest in the tree resolves to a
    /// member of one of them.
    #[test]
    fn known_tree_classification() {
        let repo_root = repo_root();
        let repo_root = repo_root.as_path();

        let units = discover_units(repo_root);

        let root_units: Vec<_> = units
            .iter()
            .filter(|u| u.kind == Kind::RootWorkspace)
            .collect();
        assert_eq!(root_units.len(), 1, "Expected exactly 1 RootWorkspace");

        let standalone: Vec<_> = units
            .iter()
            .filter(|u| u.kind != Kind::RootWorkspace)
            .collect();
        assert_eq!(
            standalone.len(),
            1,
            "Expected 1 standalone unit; got {}: {:?}",
            standalone.len(),
            standalone.iter().map(|u| &u.dir).collect::<Vec<_>>()
        );
        assert_eq!(standalone[0].dir, repo_root.join("brenn-wasm/components"));
        assert_eq!(standalone[0].kind, Kind::WasmWorkspace);
    }

    /// Nested target/ directories must be excluded by component-name, not by
    /// root-only prefix match. A Cargo.toml inside a/b/target/c/ must be skipped.
    #[test]
    fn nested_target_excluded() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();

        // Root workspace
        fs::write(
            root.join("Cargo.toml"),
            r#"[workspace]
members = []
resolver = "2"
"#,
        )
        .unwrap();

        // Legit standalone workspace (not in target/)
        let legit = root.join("my-crate");
        fs::create_dir_all(&legit).unwrap();
        fs::write(
            legit.join("Cargo.toml"),
            r#"[workspace]
members = ["a"]
resolver = "2"
"#,
        )
        .unwrap();

        // A Cargo.toml nested inside a target/ component — must be skipped.
        let in_target = root
            .join("subdir")
            .join("target")
            .join("vendor")
            .join("fake-crate");
        fs::create_dir_all(&in_target).unwrap();
        fs::write(
            in_target.join("Cargo.toml"),
            r#"[workspace]
members = ["nope"]
resolver = "2"
"#,
        )
        .unwrap();

        let units = discover_units(root);
        let dirs: Vec<_> = units.iter().map(|u| u.dir.clone()).collect();
        assert!(
            !dirs.iter().any(|d| d.to_string_lossy().contains("target")),
            "target/ nested Cargo.toml should be excluded; found: {dirs:?}"
        );
        assert!(dirs.contains(&legit), "legit crate should be discovered");

        // Also assert the legit crate classifies correctly (not just that it's present).
        let legit_kind = units.iter().find(|u| u.dir == legit).map(|u| &u.kind);
        assert_eq!(
            legit_kind,
            Some(&Kind::WasmWorkspace),
            "a virtual standalone workspace should be WasmWorkspace; got: {legit_kind:?}"
        );
    }

    /// Hidden directories (starting with '.') must be excluded — they may contain worktree
    /// checkouts (.claude/worktrees/) with their own Cargo.toml files that must never be
    /// classified as components. A sibling non-hidden workspace must still be discovered.
    #[test]
    fn hidden_dir_excluded() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();

        // Root workspace.
        fs::write(
            root.join("Cargo.toml"),
            r#"[workspace]
members = []
resolver = "2"
"#,
        )
        .unwrap();

        // A legit standalone workspace (not hidden).
        let legit = root.join("real-crate");
        fs::create_dir_all(&legit).unwrap();
        fs::write(
            legit.join("Cargo.toml"),
            r#"[workspace]
members = ["a"]
resolver = "2"
"#,
        )
        .unwrap();

        // A Cargo.toml inside a hidden dir (e.g. .claude/worktrees/...) — must be skipped.
        let in_hidden = root.join(".claude").join("worktrees").join("branch-x");
        fs::create_dir_all(&in_hidden).unwrap();
        fs::write(
            in_hidden.join("Cargo.toml"),
            r#"[workspace]
members = ["a"]
resolver = "2"
"#,
        )
        .unwrap();

        let units = discover_units(root);
        let dirs: Vec<_> = units.iter().map(|u| u.dir.clone()).collect();

        // The hidden-dir workspace must not appear.
        assert!(
            !dirs.iter().any(|d| d.to_string_lossy().contains(".claude")),
            "hidden dir Cargo.toml should be excluded; found: {dirs:?}"
        );
        // The sibling non-hidden workspace must still be found.
        assert!(
            dirs.contains(&legit),
            "legit crate should still be discovered"
        );
    }

    /// An unclassifiable standalone workspace panics.
    #[test]
    #[should_panic(expected = "unclassifiable standalone")]
    fn unclassifiable_panics() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();

        fs::write(
            root.join("Cargo.toml"),
            r#"[workspace]
members = []
resolver = "2"
"#,
        )
        .unwrap();

        // A single-crate [workspace] opt-out — unclassifiable, triggers panic.
        let weird = root.join("weird-crate");
        fs::create_dir_all(&weird).unwrap();
        fs::write(
            weird.join("Cargo.toml"),
            r#"[workspace]
[package]
name = "opts-out-of-everything"
version = "0.1.0"
edition = "2021"
"#,
        )
        .unwrap();

        discover_units(root);
    }

    /// A Cargo.toml without [workspace] that is NOT a root member → orphan → panic.
    #[test]
    #[should_panic(expected = "orphan crate")]
    fn orphan_crate_panics() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();

        fs::write(
            root.join("Cargo.toml"),
            r#"[workspace]
members = []
resolver = "2"
"#,
        )
        .unwrap();

        // A subdirectory with a Cargo.toml but no [workspace] and NOT in members.
        let orphan = root.join("orphan");
        fs::create_dir_all(&orphan).unwrap();
        fs::write(
            orphan.join("Cargo.toml"),
            r#"[package]
name = "orphan"
version = "0.1.0"
edition = "2021"
"#,
        )
        .unwrap();

        discover_units(root);
    }

    /// Every wasm workspace member, by family. The family is the sole dispatch
    /// key of the bindings-drift gate and of which WIT world a component's
    /// artifact is compared against, so a member that silently changed family
    /// would switch its gate off and compare it to the wrong world, both green.
    #[test]
    fn known_wasm_crate_families() {
        let repo_root = repo_root();
        let components = repo_root.join("brenn-wasm/components");

        let mut observed: Vec<(String, Family)> = wasm_crates(&repo_root)
            .into_iter()
            .map(|c| {
                let rel = c
                    .dir
                    .strip_prefix(&components)
                    .unwrap_or_else(|_| panic!("{:?} is not under {components:?}", c.dir));
                (rel.to_string_lossy().into_owned(), c.family)
            })
            .collect();
        observed.sort();

        let mut expected: Vec<(String, Family)> = [
            ("guest", Family::Sdk),
            ("processor-exhaust", Family::Raw),
            ("processor-mem-exhaust", Family::Raw),
            ("processor-mqtt-test", Family::Raw),
            ("processor-tool-test", Family::Raw),
            ("replay", Family::Raw),
            ("replay-fault-test", Family::Raw),
            ("replay-generic", Family::Raw),
            ("git-forge-parser", Family::Guest),
            ("git-sync-consumer", Family::Guest),
            ("processor-config", Family::Guest),
            ("processor-demo", Family::Guest),
            ("processor-dual", Family::Guest),
            ("processor-log", Family::Guest),
            ("processor-multiport", Family::Guest),
            ("processor-store-rt", Family::Guest),
            ("processor-transplant", Family::Guest),
        ]
        .into_iter()
        .map(|(name, family)| (name.to_string(), family))
        .collect();
        expected.sort();

        assert_eq!(observed, expected);
    }

    fn manifest(text: &str) -> toml::Value {
        toml::from_str(text).expect("fixture manifest parses")
    }

    #[test]
    fn each_family_arm_is_recognized() {
        let dir = Path::new("brenn-wasm/components/x");
        assert_eq!(
            classify_family(&manifest("[package]\nname = \"brenn-guest\"\n"), dir),
            Family::Sdk
        );
        assert_eq!(
            classify_family(
                &manifest(
                    "[package]\nname = \"replay\"\n\n[package.metadata.component]\n\
                     target = { path = \"../../wit/replay.wit\" }\n"
                ),
                dir
            ),
            Family::Raw
        );
        assert_eq!(
            classify_family(
                &manifest(
                    "[package]\nname = \"demo\"\n\n[dependencies]\n\
                     brenn-guest = { path = \"../guest\" }\n"
                ),
                dir
            ),
            Family::Guest
        );
    }

    /// The SDK arm wins over the raw arm: `brenn-guest` itself would otherwise
    /// have to be kept free of component metadata forever.
    #[test]
    fn the_sdk_arm_is_checked_before_the_raw_arm() {
        assert_eq!(
            classify_family(
                &manifest(
                    "[package]\nname = \"brenn-guest\"\n\n[package.metadata.component]\n\
                     target = { path = \"x.wit\" }\n"
                ),
                Path::new("brenn-wasm/components/guest")
            ),
            Family::Sdk
        );
    }

    #[test]
    #[should_panic(expected = "unclassifiable wasm workspace member")]
    fn a_member_matching_no_family_panics() {
        classify_family(
            &manifest("[package]\nname = \"stray\"\n\n[dependencies]\nserde = \"1\"\n"),
            Path::new("brenn-wasm/components/stray"),
        );
    }

    /// A root workspace listing `root_members`, a nested standalone virtual
    /// workspace listing `nested_members`, and a plain manifest at each of
    /// `plain`.
    fn nested_tree(root: &Path, root_members: &str, nested_members: &str, plain: &[&str]) {
        fs::write(
            root.join("Cargo.toml"),
            format!("[workspace]\nmembers = [{root_members}]\nresolver = \"3\"\n"),
        )
        .unwrap();
        fs::create_dir_all(root.join("nested")).unwrap();
        fs::write(
            root.join("nested/Cargo.toml"),
            format!("[workspace]\nmembers = [{nested_members}]\nresolver = \"3\"\n"),
        )
        .unwrap();
        for rel in plain {
            let dir = root.join(rel);
            fs::create_dir_all(&dir).unwrap();
            fs::write(
                dir.join("Cargo.toml"),
                "[package]\nname = \"c\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
            )
            .unwrap();
        }
    }

    fn file_list(rels: &[&str]) -> Vec<PathBuf> {
        rels.iter().map(PathBuf::from).collect()
    }

    /// Membership resolves against the *nearest* enclosing workspace. `host` is
    /// a root member and `nested/guest` a member of `nested`; neither is listed
    /// by the other workspace, so a resolver that reached for the root — or for
    /// any workspace but the nearest — would call one of them an orphan.
    #[test]
    fn a_member_resolves_to_the_nearest_enclosing_workspace() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        nested_tree(root, "\"host\"", "\"guest\"", &["host", "nested/guest"]);

        let units = discover_units_from_files(
            root,
            &file_list(&[
                "Cargo.toml",
                "host/Cargo.toml",
                "nested/Cargo.toml",
                "nested/guest/Cargo.toml",
            ]),
        );

        let dirs: Vec<PathBuf> = units.iter().map(|u| u.dir.clone()).collect();
        assert_eq!(dirs, vec![root.to_path_buf(), root.join("nested")]);
        assert_eq!(units[1].kind, Kind::WasmWorkspace);
    }

    /// The nearest workspace is the *only* one consulted: a manifest the root
    /// lists but its own enclosing workspace does not is still an orphan, and
    /// the lint lane that covers `nested` would never reach it.
    #[test]
    #[should_panic(expected = "orphan crate")]
    fn the_root_cannot_adopt_a_member_of_a_nested_workspace() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        nested_tree(
            root,
            "\"nested/guest\"",
            "\"other\"",
            &["nested/guest", "nested/other"],
        );

        discover_units_from_files(
            root,
            &file_list(&[
                "Cargo.toml",
                "nested/Cargo.toml",
                "nested/guest/Cargo.toml",
                "nested/other/Cargo.toml",
            ]),
        );
    }

    /// The root manifest is the root workspace, never a member of itself. It is
    /// dropped from the manifest list rather than falling into pass 2, where
    /// its empty relative dir would resolve against the root's own member list.
    #[test]
    fn the_root_manifest_is_not_treated_as_a_member() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        nested_tree(root, "\"host\"", "\"guest\"", &[]);

        let units = discover_units_from_files(root, &file_list(&["Cargo.toml"]));
        assert_eq!(units.len(), 1);
        assert_eq!(units[0].kind, Kind::RootWorkspace);
    }

    /// TOML-parse (not grep) handles `[ workspace ]` with padded brackets and leading whitespace.
    /// A naive grep for `[workspace]` would miss this variant.
    #[test]
    fn space_padded_workspace_header() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();

        // Root workspace with space-padded bracket (unusual but TOML-valid).
        fs::write(
            root.join("Cargo.toml"),
            "[ workspace ]\nmembers = []\nresolver = \"2\"\n",
        )
        .unwrap();

        // Standalone workspace using the same space-padded syntax.
        let crate_dir = root.join("my-crate");
        fs::create_dir_all(&crate_dir).unwrap();
        fs::write(
            crate_dir.join("Cargo.toml"),
            "[ workspace ]\nmembers = [\"a\"]\nresolver = \"2\"\n",
        )
        .unwrap();

        // Discovery must find the standalone crate (TOML parse handles spaces; grep would not).
        let units = discover_units(root);
        let standalone: Vec<_> = units
            .iter()
            .filter(|u| u.kind != Kind::RootWorkspace)
            .collect();
        assert_eq!(
            standalone.len(),
            1,
            "Expected 1 standalone unit; got: {:?}",
            standalone.iter().map(|u| &u.dir).collect::<Vec<_>>()
        );
        assert_eq!(
            standalone[0].kind,
            Kind::WasmWorkspace,
            "Space-padded virtual workspace should be WasmWorkspace"
        );
    }
}
