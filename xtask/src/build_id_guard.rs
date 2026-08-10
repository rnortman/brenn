//! Build-id leaf guard: only the `brenn` bin crate may name the build-id
//! environment variable.
//!
//! The value changes on every release build, so any crate that bakes it in is
//! recompiled on every release build, and so is everything downstream of it.
//! Keeping the reference in the thin leaf binary costs one crate's rebuild;
//! a reference in `brenn-lib` would cost the whole graph's, with no compile
//! error and no failing test to say so — only a build that quietly got slow
//! again. A `rustc_env` entry in another crate's `BUILD.bazel` reintroduces it
//! exactly as effectively as an `env!` in that crate's source, so both are in
//! scope.

use std::path::{Path, PathBuf};

/// Split so this file does not match its own scan. Shared with the sync guard,
/// which derives the Bazel stamp key from it rather than restating it.
pub const TOKEN: &str = concat!("BRENN_", "BUILD_ID");

/// The one crate dir permitted to name it: the bin crate, whose whole job is to
/// bake the value in.
const LEAF_DIR: &str = "brenn";

/// Extensions carrying references that reach a compile: Rust source, the cargo
/// manifests that could set the variable for one, and the Bazel files that can
/// do the same through `rustc_env` or `build_setting` plumbing (`BUILD.bazel`
/// and `MODULE.bazel` carry the `bazel` extension; macros carry `bzl`).
const EXTENSIONS: &[&str] = &["rs", "toml", "bazel", "bzl"];

fn is_scanned(rel: &Path) -> bool {
    if rel
        .components()
        .next()
        .is_some_and(|c| c.as_os_str() == LEAF_DIR)
    {
        return false;
    }
    rel.extension()
        .is_some_and(|e| EXTENSIONS.contains(&e.to_string_lossy().as_ref()))
}

fn violations_from(scanned: &[(PathBuf, String)]) -> Vec<String> {
    let mut found = Vec::new();
    for (rel, text) in scanned {
        for (i, line) in text.lines().enumerate() {
            if line.contains(TOKEN) {
                found.push(format!(
                    "{}:{}: names {TOKEN}. Only the `{LEAF_DIR}` bin crate may — every other \
                     crate that does is recompiled on every stamped release build, and so is \
                     everything that depends on it. Pass the value down as a parameter.",
                    rel.display(),
                    i + 1,
                ));
            }
        }
    }
    found
}

/// True if no crate outside `brenn/` names the build-id environment variable.
pub fn run_build_id_guard(root: &Path, files: &[PathBuf]) -> bool {
    let scanned: Vec<(PathBuf, String)> = files
        .iter()
        .filter(|rel| is_scanned(rel))
        .filter_map(|rel| match std::fs::read_to_string(root.join(rel)) {
            Ok(text) => Some((rel.clone(), text)),
            // Non-UTF8 is not source. Any other read failure is a broken scan.
            Err(e) if e.kind() == std::io::ErrorKind::InvalidData => None,
            Err(e) => panic!("build-id guard: cannot read {rel:?}: {e}"),
        })
        .collect();

    if scanned.len() < crate::file_set::MIN_SCANNED_FILES {
        eprintln!(
            "build-id guard: scanned only {} files — the file set is broken, and a vacuous \
             guard asserts nothing",
            scanned.len()
        );
        return false;
    }

    let found = violations_from(&scanned);
    if found.is_empty() {
        return true;
    }
    eprintln!("build-id guard: the build id has leaked out of the leaf crate:");
    for line in &found {
        eprintln!("  {line}");
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scanned(pairs: &[(&str, &str)]) -> Vec<(PathBuf, String)> {
        pairs
            .iter()
            .map(|(p, t)| (PathBuf::from(*p), (*t).to_string()))
            .collect()
    }

    #[test]
    fn a_reference_outside_the_leaf_is_reported_with_its_line() {
        let out = violations_from(&scanned(&[(
            "brenn-lib/src/x.rs",
            &format!("fn f() {{}}\nlet v = env!(\"{TOKEN}\");\n"),
        )]));
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(
            out[0].starts_with("brenn-lib/src/x.rs:2: names"),
            "{}",
            out[0]
        );
    }

    #[test]
    fn source_without_the_token_is_clean() {
        let out = violations_from(&scanned(&[("brenn-lib/src/x.rs", "let v = 1;\n")]));
        assert!(out.is_empty(), "{out:?}");
    }

    #[test]
    fn the_leaf_crate_is_out_of_scope_and_other_extensions_are_not_scanned() {
        assert!(!is_scanned(Path::new("brenn/src/build_info.rs")));
        assert!(!is_scanned(Path::new("brenn/BUILD.bazel")));
        assert!(!is_scanned(Path::new("Makefile")));
        assert!(!is_scanned(Path::new("bazel/workspace_status.sh")));
        assert!(is_scanned(Path::new("brenn-lib/src/x.rs")));
        assert!(is_scanned(Path::new("brenn-server/Cargo.toml")));
        // A sibling whose name merely starts with the leaf's is in scope.
        assert!(is_scanned(Path::new("brenn-cli/src/main.rs")));
    }

    #[test]
    fn bazel_files_outside_the_leaf_are_scanned() {
        assert!(is_scanned(Path::new("brenn-lib/BUILD.bazel")));
        assert!(is_scanned(Path::new("MODULE.bazel")));
        assert!(is_scanned(Path::new("bazel/wasm/defs.bzl")));
    }

    /// The anti-vacuity floor is what protects the guard from a collapsed file
    /// set, and a collapsed file set has no other symptom — a vacuous guard
    /// passes. So the floor itself has to be exercised.
    #[test]
    fn a_collapsed_file_set_fails_rather_than_passing_over_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::write(root.join("a.rs"), "fn a() {}\n").unwrap();
        std::fs::write(root.join("b.rs"), "fn b() {}\n").unwrap();
        assert!(!run_build_id_guard(
            root,
            &[PathBuf::from("a.rs"), PathBuf::from("b.rs")]
        ));
    }

    #[test]
    fn a_rustc_env_entry_in_a_build_file_is_reported() {
        let out = violations_from(&scanned(&[(
            "brenn-lib/BUILD.bazel",
            &format!("rust_library(\n    rustc_env = {{\"{TOKEN}\": \"x\"}},\n)\n"),
        )]));
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(
            out[0].starts_with("brenn-lib/BUILD.bazel:2: names"),
            "{}",
            out[0]
        );
    }
}
