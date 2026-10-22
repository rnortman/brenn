//! Workspace guard: the Cargo workspaces in the tree are exactly the declared
//! ones.
//!
//! `xtask deny` runs cargo-deny once per workspace from a written-down list
//! ([`crate::deny::WORKSPACES`]), because a workspace resolves its own
//! dependency graph out of its own `Cargo.lock` and a run in one sees nothing
//! of another. A list is the right shape — a second workspace is a deliberate
//! act — but a list with no backstop stops asserting anything the moment an
//! unlisted workspace appears: the advisory gate would print its unit count
//! and pass over dependencies nothing ever looked at.
//!
//! This guard is that backstop. It observes which tracked `Cargo.toml`s carry a
//! top-level `[workspace]` table and which directories hold a tracked
//! `Cargo.lock`, and requires the workspaces to equal the declared ones, in
//! both directions — an undeclared workspace and a declared one that no longer
//! exists are both drift. A `Cargo.lock` with no workspace manifest beside it
//! is a dependency graph `cargo deny` cannot run on (a manifest-less
//! crate_universe hub resolves one), and is drift too.

use std::path::{Path, PathBuf};

use crate::deny::WORKSPACES;

/// The repo-root-relative directory of a manifest, as `WORKSPACES` spells it:
/// the empty string for the root workspace.
fn workspace_dir(rel: &Path) -> String {
    rel.parent()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// Whether a manifest declares a workspace. Parsed rather than grepped: a
/// `workspace = true` inheritance key inside `[dependencies]` is not a
/// workspace root, and a `[workspace]` line inside a string would be.
fn declares_workspace(text: &str, rel: &Path) -> bool {
    let value: toml::Value = toml::from_str(text).unwrap_or_else(|e| {
        panic!(
            "workspace guard: {} does not parse as TOML: {e}",
            rel.display()
        )
    });
    value.get("workspace").is_some()
}

/// What the tree holds: repo-root-relative directories, sorted, the root as
/// the empty string.
#[derive(Debug, PartialEq)]
struct Observed {
    /// Directories whose tracked `Cargo.toml` declares a workspace.
    manifests: Vec<String>,
    /// Directories holding a tracked `Cargo.lock`.
    lockfiles: Vec<String>,
}

fn violations_from(observed: &Observed) -> Vec<String> {
    let mut found = Vec::new();
    for dir in &observed.manifests {
        if !WORKSPACES.contains(&dir.as_str()) {
            found.push(format!(
                "{}: a Cargo workspace that xtask/src/deny.rs does not list. Every \
                 workspace resolves its own dependency graph, so one outside \
                 WORKSPACES is outside `xtask deny` — add it there, or make this \
                 manifest a member of an existing workspace.",
                display_dir(dir)
            ));
        }
    }
    for declared in WORKSPACES {
        if !observed.manifests.iter().any(|dir| dir == declared) {
            found.push(format!(
                "{}: listed in xtask/src/deny.rs WORKSPACES but no tracked Cargo.toml \
                 there declares a workspace. `xtask deny` would run cargo-deny in a \
                 directory that is not a workspace root — drop the entry or fix the \
                 path.",
                display_dir(declared)
            ));
        }
    }
    for dir in &observed.lockfiles {
        if !observed.manifests.contains(dir) && !WORKSPACES.contains(&dir.as_str()) {
            found.push(format!(
                "{}: a tracked Cargo.lock outside xtask/src/deny.rs WORKSPACES. A \
                 dependency graph resolves here — a manifest-less crate_universe \
                 hub makes one too — and `xtask deny` never audits it. Give it a \
                 manifest with a `[workspace]` table and list it there.",
                display_dir(dir)
            ));
        }
    }
    found
}

/// `WORKSPACES` spells the root as the empty string, which reads as nothing in
/// a message.
fn display_dir(dir: &str) -> &str {
    if dir.is_empty() { "<repo root>" } else { dir }
}

/// Reads each `Cargo.toml`; a `Cargo.lock` counts by its file name alone.
fn collect_workspaces(root: &Path, files: &[PathBuf]) -> Observed {
    let named = |name: &'static str| {
        files
            .iter()
            .filter(move |rel| rel.file_name().is_some_and(|n| n == name))
    };
    let mut manifests: Vec<String> = named("Cargo.toml")
        .filter(|rel| {
            let text = std::fs::read_to_string(root.join(rel))
                .unwrap_or_else(|e| panic!("workspace guard: cannot read {rel:?}: {e}"));
            declares_workspace(&text, rel)
        })
        .map(|rel| workspace_dir(rel))
        .collect();
    manifests.sort();
    let mut lockfiles: Vec<String> = named("Cargo.lock").map(|rel| workspace_dir(rel)).collect();
    lockfiles.sort();
    Observed {
        manifests,
        lockfiles,
    }
}

/// True if the tree's Cargo workspaces are exactly the declared ones.
pub fn run_workspace_guard(root: &Path, files: &[PathBuf]) -> bool {
    let found = violations_from(&collect_workspaces(root, files));
    if found.is_empty() {
        return true;
    }
    eprintln!("workspace guard: Cargo workspaces are not as declared:");
    for line in &found {
        eprintln!("  {line}");
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dirs(paths: &[&str]) -> Vec<String> {
        paths.iter().map(|s| (*s).to_string()).collect()
    }

    fn baseline() -> Vec<String> {
        let mut v = dirs(&WORKSPACES);
        v.sort();
        v
    }

    /// Workspace manifests only, no lockfiles.
    fn manifests_only(manifests: Vec<String>) -> Observed {
        Observed {
            manifests,
            lockfiles: Vec::new(),
        }
    }

    /// A control for the matching logic, not for the declared list: the
    /// observation is built *from* `WORKSPACES`, so it says only that an exact
    /// match yields nothing. Whether the tree matches is the guard's job.
    #[test]
    fn observed_equal_to_the_declared_list_yields_no_violations() {
        assert!(violations_from(&manifests_only(baseline())).is_empty());
    }

    /// A declared workspace with its lockfile beside the manifest is the normal
    /// shape; the lockfile adds nothing to report.
    #[test]
    fn declared_workspaces_with_lockfiles_yield_no_violations() {
        let observed = Observed {
            manifests: baseline(),
            lockfiles: baseline(),
        };
        assert!(violations_from(&observed).is_empty());
    }

    #[test]
    fn an_undeclared_workspace_fails() {
        let mut observed = baseline();
        observed.push("tools/scratch".to_string());
        let out = violations_from(&manifests_only(observed));
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(
            out[0].starts_with("tools/scratch: a Cargo workspace"),
            "{}",
            out[0]
        );
        assert!(out[0].contains("xtask deny"), "{}", out[0]);
    }

    #[test]
    fn a_declared_workspace_that_vanished_fails() {
        let observed: Vec<String> = baseline()
            .into_iter()
            .filter(|d| d != "brenn-wasm/components")
            .collect();
        let out = violations_from(&manifests_only(observed));
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(
            out[0].starts_with("brenn-wasm/components: listed in"),
            "{}",
            out[0]
        );
    }

    /// The root workspace's empty path must read as something in a message.
    #[test]
    fn the_root_workspace_is_named_in_messages() {
        let observed: Vec<String> = baseline().into_iter().filter(|d| !d.is_empty()).collect();
        let out = violations_from(&manifests_only(observed));
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].starts_with("<repo root>: listed in"), "{}", out[0]);
    }

    /// A lockfile with no workspace manifest and no `WORKSPACES` entry is a
    /// graph the gate never sees: one violation, the lockfile one.
    #[test]
    fn an_undeclared_lockfile_without_a_manifest_fails() {
        let observed = Observed {
            manifests: baseline(),
            lockfiles: dirs(&["tools/hub"]),
        };
        let out = violations_from(&observed);
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(
            out[0].starts_with("tools/hub: a tracked Cargo.lock outside"),
            "{}",
            out[0]
        );
    }

    /// A declared directory holding only a lockfile is reported once, as a
    /// declared workspace with no manifest: `cargo deny` cannot run there.
    #[test]
    fn a_declared_lockfile_without_a_manifest_fails_as_vanished() {
        let observed = Observed {
            manifests: baseline()
                .into_iter()
                .filter(|d| d != "tools/scrub")
                .collect(),
            lockfiles: dirs(&["tools/scrub"]),
        };
        let out = violations_from(&observed);
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].starts_with("tools/scrub: listed in"), "{}", out[0]);
    }

    /// Detection is by top-level table, not by substring: a member manifest
    /// inheriting a dependency with `workspace = true` is not a workspace root.
    #[test]
    fn dependency_inheritance_is_not_a_workspace() {
        let rel = Path::new("crate-a/Cargo.toml");
        assert!(!declares_workspace(
            "[package]\nname = \"a\"\n\n[dependencies]\nserde = { workspace = true }\n",
            rel
        ));
        assert!(declares_workspace(
            "[workspace]\nmembers = [\"crate-a\"]\n",
            rel
        ));
        assert!(declares_workspace(
            "[package]\nname = \"a\"\n\n[workspace]\n",
            rel
        ));
    }

    /// The collector half: only `Cargo.toml` files are read, at any depth; a
    /// `Cargo.lock` is observed by name; the reported directories are
    /// repo-root-relative with the root as the empty string.
    #[test]
    fn the_collector_reports_workspace_dirs_relative_to_the_root() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path();
        let nested = root.join("guests");
        std::fs::create_dir_all(&nested).unwrap();

        std::fs::write(root.join("Cargo.toml"), "[workspace]\nmembers = []\n").unwrap();
        std::fs::write(nested.join("Cargo.toml"), "[workspace]\nmembers = []\n").unwrap();
        std::fs::write(root.join("member.toml"), "[workspace]\nmembers = []\n").unwrap();

        let files = [
            "Cargo.toml",
            "Cargo.lock",
            "guests/Cargo.toml",
            "hub/Cargo.lock",
            "member.toml",
        ]
        .map(PathBuf::from);
        assert_eq!(
            collect_workspaces(root, &files),
            Observed {
                manifests: dirs(&["", "guests"]),
                lockfiles: dirs(&["", "hub"]),
            }
        );
    }
}
