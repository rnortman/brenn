// `xtask lint`: lint every Rust unit in the repo (or a single specified unit).
// Aggregates failures across all units, then exits non-zero if any fail.
// See design §4.

use crate::discover::{Kind, Unit, classify_dir, discover_units};
use crate::policy::{lint_command_for, wasm_components_target};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Lint all units discovered from `repo_root`. Returns true on success (all pass).
pub fn lint_all(repo_root: &Path) -> bool {
    lint_matching(repo_root, |_| true)
}

/// True for the root workspace lane. Exact complement of `is_wasm` over `Kind`, so
/// `lint_root` and `lint_wasm` partition the discovered units with no overlap and
/// no gap.
fn is_root(k: &Kind) -> bool {
    *k == Kind::RootWorkspace
}

/// True for the standalone WASM lane (everything that is not the root workspace).
fn is_wasm(k: &Kind) -> bool {
    *k != Kind::RootWorkspace
}

/// Lint only the root workspace. Uses the root `target/` dir, disjoint from the
/// shared WASM target dir, so it is safe to run concurrently with `lint_wasm`.
pub fn lint_root(repo_root: &Path) -> bool {
    lint_matching(repo_root, is_root)
}

/// Lint only the standalone WASM units. These share `WASM_COMPONENTS_TARGET`, which
/// cargo serializes via its build-dir lock, and only read the tree (including the
/// committed `bindings.rs` files), so this lane runs concurrently with the others.
pub fn lint_wasm(repo_root: &Path) -> bool {
    lint_matching(repo_root, is_wasm)
}

/// Lint every discovered unit whose kind satisfies `keep`. Aggregates failures.
fn lint_matching(repo_root: &Path, keep: impl Fn(&Kind) -> bool) -> bool {
    let units = discover_units(repo_root);
    let wasm_target = wasm_components_target(repo_root);
    let mut failures: Vec<PathBuf> = Vec::new();

    for unit in units.iter().filter(|u| keep(&u.kind)) {
        if !lint_unit(unit, &wasm_target) {
            failures.push(unit.dir.clone());
        }
    }

    if !failures.is_empty() {
        eprintln!("\nxtask lint: FAILED — the following units had clippy warnings/errors:");
        for f in &failures {
            eprintln!("  {f:?}");
        }
        eprintln!("\n{} unit(s) failed.", failures.len());
        false
    } else {
        println!("xtask lint: all units passed.");
        true
    }
}

/// Lint a single unit by path. Returns true on success.
///
/// Supports both in-tree paths (looked up from `discover_units`) and out-of-tree paths
/// (classified directly from the target dir's own Cargo.toml). Design §2.2 R5.
pub fn lint_one(repo_root: &Path, target_dir: &Path) -> bool {
    // Canonicalize the target to handle ./dir, symlinks, and non-lexical forms.
    // Fall back to the non-canonical form if canonicalize fails (e.g. path doesn't exist yet).
    let abs_target = if target_dir.is_absolute() {
        target_dir.to_path_buf()
    } else {
        repo_root.join(target_dir)
    };
    let canonical_target =
        std::fs::canonicalize(&abs_target).unwrap_or_else(|_| abs_target.clone());

    // First try to find the unit in the brenn discovery set (fast path for in-tree dirs).
    let units = discover_units(repo_root);
    let wasm_target = wasm_components_target(repo_root);

    if let Some(unit) = units.iter().find(|u| {
        let canonical_unit = std::fs::canonicalize(&u.dir).unwrap_or_else(|_| u.dir.clone());
        canonical_unit == canonical_target
    }) {
        return lint_unit(unit, &wasm_target);
    }

    // Out-of-tree path (design §2.2 R5): classify the dir directly from its own Cargo.toml,
    // then lint it with the same policy. `repo_root` is still used for wasm_components_target.
    let kind = classify_dir(&canonical_target).unwrap_or_else(|| {
        panic!(
            "xtask lint: {abs_target:?} is not a Rust workspace directory (no Cargo.toml with \
             [workspace] found). Cannot classify or lint it."
        )
    });
    let synthetic_unit = Unit {
        dir: canonical_target.clone(),
        kind,
    };
    lint_unit(&synthetic_unit, &wasm_target)
}

/// Lint a single unit. Returns true if clippy passes.
fn lint_unit(unit: &Unit, wasm_components_target: &Path) -> bool {
    assert_clippy_available(unit);

    let (prog, args) = lint_command_for(&unit.kind);
    println!(
        "xtask lint: [{kind}] {dir}",
        kind = unit.kind.as_str(),
        dir = unit.dir.display()
    );

    let mut cmd = Command::new(prog);
    cmd.args(&args);
    cmd.current_dir(&unit.dir);

    // For WASM kinds, set CARGO_TARGET_DIR to the shared component target dir.
    match unit.kind {
        Kind::WasmComponent | Kind::WasmGuest | Kind::WasmSdk => {
            cmd.env("CARGO_TARGET_DIR", wasm_components_target);
        }
        Kind::RootWorkspace => {}
    }

    let status = cmd.status().unwrap_or_else(|e| {
        panic!(
            "xtask lint: failed to spawn `{prog} {}` in {:?}: {e}",
            args.join(" "),
            unit.dir,
        )
    });

    status.success()
}

/// Assert that clippy is available for the unit's toolchain.
/// Panics with an actionable remediation if absent. See design §4.2.
///
/// The clippy-absent panic paths are deliberately untested: the probe command is a
/// hard-coded `Command::new("cargo")` and forcing clippy-absence would make rustup
/// attempt toolchain resolution (network-dependent, non-hermetic). The channel-reading
/// helper `read_toolchain_channel` is covered by unit tests.
fn assert_clippy_available(unit: &Unit) {
    // Only assert for non-root units (root toolchain is asserted to have clippy
    // via rust-toolchain.toml; if it somehow doesn't, cargo clippy itself will fail).
    if unit.kind == Kind::RootWorkspace {
        return;
    }

    // Probe clippy availability by running `cargo clippy --version` in the unit's dir.
    let output = Command::new("cargo")
        .args(["clippy", "--version"])
        .current_dir(&unit.dir)
        .output()
        .unwrap_or_else(|e| {
            panic!(
                "xtask lint: failed to probe clippy availability in {:?}: {e}",
                unit.dir
            )
        });

    if !output.status.success() {
        let has_toolchain_file = unit.dir.join("rust-toolchain.toml").exists();
        let stderr = String::from_utf8_lossy(&output.stderr);
        if has_toolchain_file {
            // Read the channel for the error message.
            let channel = read_toolchain_channel(&unit.dir);
            panic!(
                "clippy not available for {:?} (toolchain {channel}). \
                 Add components=[\"clippy\"] to {:?}/rust-toolchain.toml.\n\
                 cargo stderr: {stderr}",
                unit.dir, unit.dir,
            );
        } else {
            let channel = read_toolchain_channel(&unit.dir);
            panic!(
                "clippy not available for {:?} (inherits root toolchain {channel}). \
                 Root rust-toolchain.toml already lists clippy; if this fires, the root \
                 toolchain lost its clippy component — restore components=[\"clippy\"] in \
                 rust-toolchain.toml, or create {:?}/rust-toolchain.toml with \
                 components=[\"clippy\"].\n\
                 cargo stderr: {stderr}",
                unit.dir, unit.dir,
            );
        }
    }
}

fn read_toolchain_channel(dir: &Path) -> String {
    // Try the dir's own rust-toolchain.toml; if absent, caller's context (inherits root).
    let local = dir.join("rust-toolchain.toml");
    let content = if local.exists() {
        std::fs::read_to_string(&local).ok()
    } else {
        None
    };
    content
        .as_deref()
        .and_then(|s| {
            let parsed: toml::Value = toml::from_str(s).ok()?;
            parsed
                .get("toolchain")?
                .get("channel")?
                .as_str()
                .map(str::to_string)
        })
        .unwrap_or_else(|| "<unknown>".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn write_toolchain(dir: &Path, content: &str) {
        fs::write(dir.join("rust-toolchain.toml"), content).unwrap();
    }

    /// The root/wasm lint lanes must partition every Kind: exactly one predicate
    /// holds per variant, so no unit is linted twice or skipped.
    #[test]
    fn root_wasm_lanes_partition_every_kind() {
        for k in [
            Kind::RootWorkspace,
            Kind::WasmComponent,
            Kind::WasmGuest,
            Kind::WasmSdk,
        ] {
            assert_ne!(
                is_root(&k),
                is_wasm(&k),
                "each Kind must fall in exactly one lint lane"
            );
        }
        assert!(is_root(&Kind::RootWorkspace));
        assert!(is_wasm(&Kind::WasmComponent));
        assert!(is_wasm(&Kind::WasmGuest));
        assert!(is_wasm(&Kind::WasmSdk));
    }

    /// No rust-toolchain.toml in the dir → "<unknown>".
    #[test]
    fn read_toolchain_channel_no_file() {
        let tmp = tempfile::tempdir().expect("tempdir");
        assert_eq!(read_toolchain_channel(tmp.path()), "<unknown>");
    }

    /// File present but no `toolchain.channel` key → "<unknown>".
    #[test]
    fn read_toolchain_channel_no_channel_key() {
        let tmp = tempfile::tempdir().expect("tempdir");
        write_toolchain(tmp.path(), "[toolchain]\ncomponents = [\"clippy\"]\n");
        assert_eq!(read_toolchain_channel(tmp.path()), "<unknown>");
    }

    /// File with a channel → the channel string.
    #[test]
    fn read_toolchain_channel_reads_channel() {
        let tmp = tempfile::tempdir().expect("tempdir");
        write_toolchain(tmp.path(), "[toolchain]\nchannel = \"1.85.0\"\n");
        assert_eq!(read_toolchain_channel(tmp.path()), "1.85.0");
    }

    /// Malformed TOML → "<unknown>" (the `.ok()?` swallows the parse error).
    #[test]
    fn read_toolchain_channel_malformed_toml() {
        let tmp = tempfile::tempdir().expect("tempdir");
        write_toolchain(tmp.path(), "not = = valid [[[");
        assert_eq!(read_toolchain_channel(tmp.path()), "<unknown>");
    }

    /// Minimal root workspace so `discover_units` succeeds without extra units.
    fn make_root(root: &Path) {
        fs::write(
            root.join("Cargo.toml"),
            "[workspace]\nmembers = []\nresolver = \"2\"\n",
        )
        .unwrap();
    }

    /// A dir with no Cargo.toml (absolute path) is unclassifiable → fail-closed panic.
    #[test]
    #[should_panic(expected = "is not a Rust workspace directory")]
    fn lint_one_unknown_dir_absolute_panics() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        make_root(root);
        let target = root.join("not-a-unit");
        fs::create_dir_all(&target).unwrap();
        lint_one(root, &target);
    }

    /// Same, but via a relative path (exercises the `repo_root.join` normalization branch).
    #[test]
    #[should_panic(expected = "is not a Rust workspace directory")]
    fn lint_one_unknown_dir_relative_panics() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        make_root(root);
        fs::create_dir_all(root.join("not-a-unit")).unwrap();
        lint_one(root, Path::new("not-a-unit"));
    }

    /// A dir with a Cargo.toml but no `[workspace]` is also unclassifiable. It must live
    /// outside the repo root: inside it, `discover_units`' walk would panic with "orphan
    /// crate" (the wrong panic) before `lint_one` reaches `classify_dir`.
    #[test]
    #[should_panic(expected = "is not a Rust workspace directory")]
    fn lint_one_no_workspace_dir_panics() {
        let root_tmp = tempfile::tempdir().expect("tempdir");
        make_root(root_tmp.path());

        let target_tmp = tempfile::tempdir().expect("tempdir");
        fs::write(
            target_tmp.path().join("Cargo.toml"),
            "[package]\nname = \"lonely\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        )
        .unwrap();

        lint_one(root_tmp.path(), target_tmp.path());
    }
}
