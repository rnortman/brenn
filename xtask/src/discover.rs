// Discovery core: walk the repo tree, find every Cargo workspace, classify each.
// Shared by all subcommands. See design §2.3.

use std::path::{Path, PathBuf};

/// Classification of a discovered Rust unit. See design §2.3 table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    /// The root workspace (brenn/Cargo.toml). Covers all 12 root members.
    RootWorkspace,
    /// Standalone WASM component with [package.metadata.component] (Family A: raw bindings).
    WasmComponent,
    /// Standalone WASM component using brenn-guest, no [package.metadata.component] (Family B).
    WasmGuest,
    /// The brenn-guest SDK rlib (package name == "brenn-guest").
    WasmSdk,
}

impl Kind {
    /// Canonical string form used in the allowlist TOML.
    pub fn as_str(&self) -> &'static str {
        match self {
            Kind::RootWorkspace => "root-workspace",
            Kind::WasmComponent => "wasm-component",
            Kind::WasmGuest => "wasm-guest",
            Kind::WasmSdk => "wasm-sdk",
        }
    }

    /// Parse from the allowlist TOML string.
    pub fn from_str(s: &str) -> Option<Kind> {
        match s {
            "root-workspace" => Some(Kind::RootWorkspace),
            "wasm-component" => Some(Kind::WasmComponent),
            "wasm-guest" => Some(Kind::WasmGuest),
            "wasm-sdk" => Some(Kind::WasmSdk),
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
/// - any Cargo.toml without [workspace] is not a root-workspace member (orphan)
///
/// Returns one RootWorkspace unit plus one unit per standalone workspace.
pub fn discover_units(repo_root: &Path) -> Vec<Unit> {
    // Assert the root is actually a workspace.
    let root_cargo = repo_root.join("Cargo.toml");
    assert!(
        root_cargo.exists(),
        "discover_units: repo root Cargo.toml not found at {root_cargo:?}"
    );
    let root_content = std::fs::read_to_string(&root_cargo)
        .unwrap_or_else(|e| panic!("Failed to read {root_cargo:?}: {e}"));
    let root_toml: toml::Value = toml::from_str(&root_content)
        .unwrap_or_else(|e| panic!("Failed to parse {root_cargo:?}: {e}"));
    assert!(
        root_toml.get("workspace").is_some(),
        "discover_units: {root_cargo:?} has no [workspace] table — wrong repo root?"
    );

    // Collect the root workspace's members (relative paths from root).
    let root_members: Vec<PathBuf> = collect_workspace_members(&root_toml);

    let mut units = Vec::new();
    units.push(Unit {
        dir: repo_root.to_path_buf(),
        kind: Kind::RootWorkspace,
    });

    // Walk the tree for standalone workspaces.
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
            // brenn-wasm/components/target/. See design §2.3 (nested target/ warning).
            // Also exclude all hidden directories (starting with '.') — these include
            // tooling-internal dirs like .claude/worktrees/ that may contain Cargo.toml
            // files from worktree checkouts and should never be classified as components.
            if name == "target" || name.starts_with('.') {
                continue;
            }

            let meta = std::fs::metadata(&path)
                .unwrap_or_else(|e| panic!("discover_units: failed to stat {path:?}: {e}"));
            if meta.is_dir() {
                let cargo_toml = path.join("Cargo.toml");
                if cargo_toml.exists() {
                    // Check if this is the root (already handled).
                    if path == repo_root {
                        continue;
                    }
                    // Parse it.
                    let content = std::fs::read_to_string(&cargo_toml)
                        .unwrap_or_else(|e| panic!("Failed to read {cargo_toml:?}: {e}"));
                    let parsed: toml::Value = toml::from_str(&content)
                        .unwrap_or_else(|e| panic!("Failed to parse {cargo_toml:?}: {e}"));

                    if parsed.get("workspace").is_some() {
                        // Standalone workspace — classify it.
                        let kind = classify(&path, &parsed, &cargo_toml);
                        units.push(Unit {
                            dir: path.clone(),
                            kind,
                        });
                        // Don't recurse into standalone workspaces' subdirectories
                        // looking for more Cargo.toml — their source subdirs aren't
                        // themselves workspaces. But we do need to recurse to find
                        // sibling standalone workspaces. Push children that don't have
                        // Cargo.toml at top level (they won't trigger this arm).
                        // Actually: we do need to recurse into the dir's children to
                        // find further-nested standalone workspaces (e.g. brenn-wasm/guest
                        // lives inside brenn-wasm/ which has its own Cargo.toml).
                        // Push the path for continued recursion.
                        stack.push(path);
                    } else {
                        // No [workspace] — check if it's a root workspace member.
                        let rel = path.strip_prefix(repo_root).unwrap_or_else(|_| {
                            panic!("Path {path:?} is not under repo root {repo_root:?}")
                        });
                        let is_member = root_members.iter().any(|m| m == rel);
                        if !is_member {
                            panic!(
                                "discover_units: {cargo_toml:?} has no [workspace] table and is \
                                 not a root workspace member — orphan crate? Add it to the root \
                                 workspace or give it its own [workspace] table."
                            );
                        }
                        // It's a root workspace member — covered by RootWorkspace. Recurse.
                        stack.push(path);
                    }
                } else {
                    // No Cargo.toml here — recurse into children.
                    stack.push(path);
                }
            }
            // Files are ignored; only dirs are recursed.
        }
    }

    units
}

/// Classify a directory as a standalone workspace unit by parsing its Cargo.toml.
///
/// Returns `None` if the dir has no `Cargo.toml` or no `[workspace]` table (i.e. it is not a
/// standalone workspace). Panics on unclassifiable standalone workspaces (same as `discover_units`).
/// Used by `lint_one` for out-of-tree paths that are not in the brenn discovery set (design §2.2 R5).
pub fn classify_dir(dir: &Path) -> Option<Kind> {
    let cargo_toml = dir.join("Cargo.toml");
    if !cargo_toml.exists() {
        return None;
    }
    let content = std::fs::read_to_string(&cargo_toml)
        .unwrap_or_else(|e| panic!("Failed to read {cargo_toml:?}: {e}"));
    let parsed: toml::Value =
        toml::from_str(&content).unwrap_or_else(|e| panic!("Failed to parse {cargo_toml:?}: {e}"));
    parsed.get("workspace")?;
    Some(classify(dir, &parsed, &cargo_toml))
}

/// Classify a standalone workspace's Cargo.toml into a Kind.
/// Panics on unclassifiable. See design §2.3 priority order.
fn classify(_dir: &Path, toml: &toml::Value, cargo_toml_path: &Path) -> Kind {
    let pkg_name = toml
        .get("package")
        .and_then(|p| p.get("name"))
        .and_then(|n| n.as_str());

    // WasmSdk: package name == "brenn-guest"
    if pkg_name == Some("brenn-guest") {
        return Kind::WasmSdk;
    }

    // WasmComponent (Family A): has [package.metadata.component]
    if toml
        .get("package")
        .and_then(|p| p.get("metadata"))
        .and_then(|m| m.get("component"))
        .is_some()
    {
        return Kind::WasmComponent;
    }

    // WasmGuest (Family B): has a brenn-guest path dependency, no [package.metadata.component]
    let has_brenn_guest_dep = toml
        .get("dependencies")
        .and_then(|d| d.as_table())
        .map(|deps| deps.contains_key("brenn-guest"))
        .unwrap_or(false);
    if has_brenn_guest_dep {
        return Kind::WasmGuest;
    }

    // Check if it's the root (dir == repo root) — already handled by caller, but defensive.
    // For any OTHER standalone workspace that matches no rule above: hard panic.
    panic!(
        "discover_units: unclassifiable standalone [workspace] at {cargo_toml_path:?}.\n\
         Does not match any known kind:\n\
         - WasmSdk (package.name == \"brenn-guest\"): no\n\
         - WasmComponent ([package.metadata.component]): no\n\
         - WasmGuest (brenn-guest in [dependencies]): no\n\
         Add a classification rule in xtask/src/discover.rs or re-examine this crate's structure."
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

    /// Compute the repo root from CARGO_MANIFEST_DIR (xtask/), then assert discovery
    /// classifies the known fourteen standalone crates and the root correctly.
    #[test]
    fn known_tree_classification() {
        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let repo_root = manifest_dir.parent().expect("xtask/ has no parent");

        let units = discover_units(repo_root);

        // Should have exactly 1 RootWorkspace + 17 standalone units.
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
            17,
            "Expected 17 standalone units; got {}: {:?}",
            standalone.len(),
            standalone.iter().map(|u| &u.dir).collect::<Vec<_>>()
        );

        // Check known kinds by relative path.
        let kind_of = |rel: &str| -> Option<&Kind> {
            let abs = repo_root.join(rel);
            units.iter().find(|u| u.dir == abs).map(|u| &u.kind)
        };

        assert_eq!(
            kind_of("brenn-wasm/guest"),
            Some(&Kind::WasmSdk),
            "brenn-wasm/guest should be WasmSdk"
        );

        // WasmComponent (raw-bindings): replay, replay-fault-test, replay-generic,
        // processor-exhaust, processor-mem-exhaust, processor-mqtt-test,
        // processor-tool-test
        for name in &[
            "replay",
            "replay-fault-test",
            "replay-generic",
            "processor-exhaust",
            "processor-mem-exhaust",
            "processor-mqtt-test",
            "processor-tool-test",
        ] {
            let rel = format!("brenn-wasm/components/{name}");
            assert_eq!(
                kind_of(&rel),
                Some(&Kind::WasmComponent),
                "{rel} should be WasmComponent"
            );
        }

        // WasmGuest (brenn-guest dep): processor-demo, processor-dual, processor-store-rt,
        // processor-log, processor-multiport, processor-config, git-forge-parser,
        // git-sync-consumer
        for name in &[
            "processor-demo",
            "processor-dual",
            "processor-store-rt",
            "processor-log",
            "processor-multiport",
            "processor-config",
            "git-forge-parser",
            "git-sync-consumer",
        ] {
            let rel = format!("brenn-wasm/components/{name}");
            assert_eq!(
                kind_of(&rel),
                Some(&Kind::WasmGuest),
                "{rel} should be WasmGuest"
            );
        }
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
[package]
name = "brenn-guest"
version = "0.1.0"
edition = "2021"
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
[package]
name = "should-not-appear"
version = "0.1.0"
edition = "2021"
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
            Some(&Kind::WasmSdk),
            "legit crate with name=brenn-guest should be WasmSdk; got: {legit_kind:?}"
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
[package]
name = "brenn-guest"
version = "0.1.0"
edition = "2021"
"#,
        )
        .unwrap();

        // A Cargo.toml inside a hidden dir (e.g. .claude/worktrees/...) — must be skipped.
        let in_hidden = root.join(".claude").join("worktrees").join("branch-x");
        fs::create_dir_all(&in_hidden).unwrap();
        fs::write(
            in_hidden.join("Cargo.toml"),
            r#"[workspace]
[package]
name = "brenn-guest"
version = "0.1.0"
edition = "2021"
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

        // A standalone workspace that matches no classification rule.
        let weird = root.join("weird-crate");
        fs::create_dir_all(&weird).unwrap();
        fs::write(
            weird.join("Cargo.toml"),
            r#"[workspace]
[package]
name = "definitely-not-brenn-guest"
version = "0.1.0"
edition = "2021"

[dependencies]
# no brenn-guest, no [package.metadata.component]
serde = "1"
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

    /// TOML-parse (not grep) handles `[ workspace ]` with padded brackets and leading whitespace.
    /// A naive grep for `[workspace]` would miss this variant. This test proves the toml crate
    /// handles it correctly, making the property a test rather than just a comment (design §9).
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

        // Standalone crate using the same space-padded syntax.
        let crate_dir = root.join("my-crate");
        fs::create_dir_all(&crate_dir).unwrap();
        fs::write(
            crate_dir.join("Cargo.toml"),
            "[ workspace ]\n[ package ]\nname = \"brenn-guest\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
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
            Kind::WasmSdk,
            "Space-padded workspace crate should be WasmSdk"
        );
    }
}
