// `xtask guard`: workspace coverage guard (subsumes design.md's integration test).
// Discovery + allowlist completeness (Assertion A) + config verification (Assertion B).
// See design §6.

use crate::discover::{Kind, Unit, discover_units};
use crate::policy::lint_command_for;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// Allowlist entry: a standalone workspace dir (relative to repo root) and its kind.
#[derive(Debug, Clone)]
pub struct AllowlistEntry {
    pub dir: PathBuf, // repo-root-relative
    pub kind: Kind,
}

/// Parse the allowlist TOML file. Panics on missing file or malformed TOML.
/// Never returns an empty set on a missing file (fail-closed). See design §6.5.
pub fn load_allowlist(repo_root: &Path) -> Vec<AllowlistEntry> {
    let allowlist_path = repo_root.join("xtask").join("lint-allowlist.toml");
    assert!(
        allowlist_path.exists(),
        "xtask guard: allowlist not found at {allowlist_path:?}. \
         Never treat a missing allowlist as empty — that would silently allow all workspaces. \
         Create xtask/lint-allowlist.toml with at least one entry."
    );
    let content = std::fs::read_to_string(&allowlist_path)
        .unwrap_or_else(|e| panic!("xtask guard: failed to read {allowlist_path:?}: {e}"));
    let parsed: toml::Value = toml::from_str(&content)
        .unwrap_or_else(|e| panic!("xtask guard: malformed TOML in {allowlist_path:?}: {e}"));

    let workspaces = parsed
        .get("workspaces")
        .unwrap_or_else(|| panic!("xtask guard: {allowlist_path:?} has no [workspaces] table"))
        .as_table()
        .unwrap_or_else(|| {
            panic!("xtask guard: [workspaces] in {allowlist_path:?} is not a table")
        });

    let mut entries = Vec::new();
    for (dir_str, entry_val) in workspaces {
        let kind_str = entry_val
            .get("kind")
            .unwrap_or_else(|| {
                panic!("xtask guard: allowlist entry {dir_str:?} missing `kind` field")
            })
            .as_str()
            .unwrap_or_else(|| {
                panic!("xtask guard: allowlist entry {dir_str:?} `kind` is not a string")
            });
        let kind = Kind::from_str(kind_str).unwrap_or_else(|| {
            panic!(
                "xtask guard: allowlist entry {dir_str:?} has unknown kind {:?}. \
                 Valid kinds: root-workspace, wasm-component, wasm-guest, wasm-sdk.",
                kind_str
            )
        });
        entries.push(AllowlistEntry {
            dir: PathBuf::from(dir_str),
            kind,
        });
    }
    entries
}

/// Run the guard. Returns true on success (both assertions pass).
pub fn run_guard(repo_root: &Path) -> bool {
    let units = discover_units(repo_root);
    let allowlist = load_allowlist(repo_root);
    let mut ok = true;

    // Separate out standalone units (exclude RootWorkspace from the set diff).
    let discovered_standalone: HashMap<PathBuf, &Unit> = units
        .iter()
        .filter(|u| u.kind != crate::discover::Kind::RootWorkspace)
        .map(|u| {
            let rel = u.dir.strip_prefix(repo_root).unwrap_or_else(|_| {
                panic!("Unit dir {:?} not under repo root {:?}", u.dir, repo_root)
            });
            (rel.to_path_buf(), u)
        })
        .collect();

    let allowlisted: HashMap<&PathBuf, &AllowlistEntry> =
        allowlist.iter().map(|e| (&e.dir, e)).collect();

    // --- Assertion A: allowlist completeness ---
    let discovered_set: HashSet<&PathBuf> = discovered_standalone.keys().collect();
    let allowlisted_set: HashSet<&PathBuf> = allowlisted.keys().copied().collect();

    // Discovered but not allowlisted → new workspace not yet reviewed.
    let not_allowlisted: Vec<_> = {
        let mut v: Vec<_> = discovered_set
            .difference(&allowlisted_set)
            .copied()
            .collect();
        v.sort();
        v
    };
    if !not_allowlisted.is_empty() {
        eprintln!(
            "\nxtask guard [Assertion A FAIL]: discovered standalone workspaces NOT in the allowlist:"
        );
        eprintln!("  (These are invisible to root `cargo clippy` and have NO lint coverage.)");
        for path in &not_allowlisted {
            eprintln!("  {}", path.display());
        }
        eprintln!(
            "\n  Add these to xtask/lint-allowlist.toml with the correct `kind`.\n\
               Run `cargo xtask lint <dir>` to discover each crate's kind.\n\
               See the allowlist header for the standards every entry must satisfy."
        );
        ok = false;
    }

    // Allowlisted but not discovered → stale entry.
    let stale: Vec<_> = {
        let mut v: Vec<_> = allowlisted_set
            .difference(&discovered_set)
            .copied()
            .collect();
        v.sort();
        v
    };
    if !stale.is_empty() {
        eprintln!(
            "\nxtask guard [Assertion A FAIL]: allowlist entries for non-existent workspaces:"
        );
        eprintln!("  (Stale entries must be removed — they can mask a re-added crate.)");
        for path in &stale {
            eprintln!("  {}", path.display());
        }
        eprintln!("\n  Remove these entries from xtask/lint-allowlist.toml.");
        ok = false;
    }

    // --- Assertion B: configuration verification ---
    // For each allowlisted entry that exists in discovery:
    // B.1: the recorded kind matches the discovered kind.
    // B.2: lint_command_for(kind) contains "clippy" and "-D warnings".
    let mut b_failures: Vec<String> = Vec::new();

    for (dir, entry) in &allowlisted {
        let Some(unit) = discovered_standalone.get(*dir) else {
            // Already reported as stale in Assertion A.
            continue;
        };

        // B.1: kind must match.
        if unit.kind != entry.kind {
            b_failures.push(format!(
                "  {}: allowlist says kind={:?} but discover_units classified it as kind={:?}. \
                 Update the allowlist entry to match the classifier, or fix the classifier.",
                dir.display(),
                entry.kind.as_str(),
                unit.kind.as_str(),
            ));
            // Skip B.2 when kinds disagree — the B.2 command would be for the wrong kind,
            // producing a misleading diagnostic alongside the B.1 message.
            continue;
        }

        // B.2: the policy command for this kind must contain "clippy" and "-D warnings".
        // This is structural: the command string in policy.rs is what's verified, not a copy.
        let (prog, args) = lint_command_for(&entry.kind);
        let full: Vec<&str> = std::iter::once(prog).chain(args.iter().copied()).collect();
        let cmd_str = full.join(" ");
        if !cmd_str.contains("clippy") || !cmd_str.contains("-D warnings") {
            b_failures.push(format!(
                "  {}: lint_command_for({:?}) = {:?} — does not contain 'clippy' and '-D warnings'. \
                 Fix xtask/src/policy.rs.",
                dir.display(),
                entry.kind.as_str(),
                cmd_str,
            ));
        }
    }

    if !b_failures.is_empty() {
        eprintln!("\nxtask guard [Assertion B FAIL]: configuration verification failures:");
        for msg in &b_failures {
            eprintln!("{msg}");
        }
        ok = false;
    }

    if ok {
        println!(
            "xtask guard: PASSED — {} standalone workspaces discovered and allowlisted; \
             all kinds verified.",
            discovered_standalone.len()
        );
    }

    ok
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn make_root(tmp: &Path) {
        fs::write(
            tmp.join("Cargo.toml"),
            "[workspace]\nmembers = []\nresolver = \"2\"\n",
        )
        .unwrap();
    }

    fn make_sdk_crate(tmp: &Path, rel: &str) {
        let dir = tmp.join(rel);
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("Cargo.toml"),
            "[workspace]\n[package]\nname = \"brenn-guest\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        ).unwrap();
    }

    fn write_allowlist(tmp: &Path, entries: &[(&str, &str)]) {
        let mut content = String::from("# allowlist\n[workspaces]\n");
        for (dir, kind) in entries {
            content.push_str(&format!("[workspaces.\"{dir}\"]\nkind = \"{kind}\"\n"));
        }
        write_raw_allowlist(tmp, &content);
    }

    /// Extra discovered (not in allowlist) → Assertion A fails.
    #[test]
    fn assertion_a_extra_discovered() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        make_root(root);
        make_sdk_crate(root, "my-sdk");
        // Allowlist is empty (no entries).
        write_allowlist(root, &[]);

        let ok = run_guard(root);
        assert!(
            !ok,
            "Guard should fail when a discovered workspace is missing from the allowlist"
        );
    }

    /// Extra allowlisted (not discovered) → Assertion A fails.
    #[test]
    fn assertion_a_stale_allowlist() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        make_root(root);
        // No actual crates — but allowlist has a stale entry.
        write_allowlist(root, &[("nonexistent-crate", "wasm-sdk")]);

        let ok = run_guard(root);
        assert!(!ok, "Guard should fail on stale allowlist entries");
    }

    /// Matching discovered and allowlisted → passes.
    #[test]
    fn assertion_a_passes_when_equal() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        make_root(root);
        make_sdk_crate(root, "my-sdk");
        write_allowlist(root, &[("my-sdk", "wasm-sdk")]);

        let ok = run_guard(root);
        assert!(ok, "Guard should pass when discovered == allowlisted");
    }

    /// Allowlist entry with mismatched kind → Assertion B fails.
    #[test]
    fn assertion_b_mismatched_kind() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        make_root(root);
        make_sdk_crate(root, "my-sdk");
        // Allowlist says wasm-component, but classifier says wasm-sdk.
        write_allowlist(root, &[("my-sdk", "wasm-component")]);

        let ok = run_guard(root);
        assert!(
            !ok,
            "Guard should fail when allowlist kind mismatches classifier"
        );
    }

    /// Write raw bytes to `xtask/lint-allowlist.toml` under `tmp` (bypasses the
    /// valid-TOML `write_allowlist` helper so panic paths are reachable).
    fn write_raw_allowlist(tmp: &Path, content: &str) {
        let xtask_dir = tmp.join("xtask");
        fs::create_dir_all(&xtask_dir).unwrap();
        fs::write(xtask_dir.join("lint-allowlist.toml"), content).unwrap();
    }

    /// Missing allowlist file must fail closed, not be treated as empty.
    #[test]
    #[should_panic(expected = "allowlist not found at")]
    fn load_allowlist_missing_file_panics() {
        let tmp = tempfile::tempdir().unwrap();
        // No xtask/lint-allowlist.toml written.
        load_allowlist(tmp.path());
    }

    #[test]
    #[should_panic(expected = "malformed TOML in")]
    fn load_allowlist_malformed_toml_panics() {
        let tmp = tempfile::tempdir().unwrap();
        write_raw_allowlist(tmp.path(), "this is = = not valid toml [[[");
        load_allowlist(tmp.path());
    }

    #[test]
    #[should_panic(expected = "has no [workspaces] table")]
    fn load_allowlist_missing_workspaces_table_panics() {
        let tmp = tempfile::tempdir().unwrap();
        write_raw_allowlist(
            tmp.path(),
            "# valid TOML, but no workspaces table\nother = 1\n",
        );
        load_allowlist(tmp.path());
    }

    #[test]
    #[should_panic(expected = "is not a table")]
    fn load_allowlist_workspaces_not_a_table_panics() {
        let tmp = tempfile::tempdir().unwrap();
        write_raw_allowlist(tmp.path(), "workspaces = 42\n");
        load_allowlist(tmp.path());
    }

    #[test]
    #[should_panic(expected = "missing `kind` field")]
    fn load_allowlist_entry_missing_kind_panics() {
        let tmp = tempfile::tempdir().unwrap();
        write_raw_allowlist(tmp.path(), "[workspaces.\"my-crate\"]\nother = \"x\"\n");
        load_allowlist(tmp.path());
    }

    #[test]
    #[should_panic(expected = "`kind` is not a string")]
    fn load_allowlist_kind_not_a_string_panics() {
        let tmp = tempfile::tempdir().unwrap();
        write_raw_allowlist(tmp.path(), "[workspaces.\"my-crate\"]\nkind = 7\n");
        load_allowlist(tmp.path());
    }

    #[test]
    #[should_panic(expected = "has unknown kind")]
    fn load_allowlist_unknown_kind_panics() {
        let tmp = tempfile::tempdir().unwrap();
        write_raw_allowlist(tmp.path(), "[workspaces.\"my-crate\"]\nkind = \"bogus\"\n");
        load_allowlist(tmp.path());
    }

    /// A valid two-entry allowlist parses into the expected entries (order-independent).
    #[test]
    fn load_allowlist_happy_path_parses_entries() {
        let tmp = tempfile::tempdir().unwrap();
        write_allowlist(
            tmp.path(),
            &[("my-sdk", "wasm-sdk"), ("my-comp", "wasm-component")],
        );

        let mut entries = load_allowlist(tmp.path());
        entries.sort_by(|a, b| a.dir.cmp(&b.dir));

        assert_eq!(entries.len(), 2, "two entries parsed: {entries:?}");
        assert_eq!(entries[0].dir, PathBuf::from("my-comp"));
        assert_eq!(entries[0].kind, Kind::WasmComponent);
        assert_eq!(entries[1].dir, PathBuf::from("my-sdk"));
        assert_eq!(entries[1].kind, Kind::WasmSdk);
    }
}
