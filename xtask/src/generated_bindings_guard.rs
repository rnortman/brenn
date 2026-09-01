//! Generated-bindings guard: no guest crate carries a `src/bindings.rs` in the
//! source tree.
//!
//! A guest crate under `brenn-wasm/components/` gets its WIT bindings one of two
//! ways. The brenn-guest SDK writes its own copy by hand and commits it, and the
//! world-liveness gate reads that copy. Every other crate takes a generated one,
//! substituted into the source list at the same path — so a file sitting there
//! is dropped from the compile and nothing else notices it. It sits in the tree
//! looking authoritative: scanned by the comment scrub, diffed in review, and
//! answering "which copy is real" wrongly for whoever reads it next.
//!
//! The `.gitignore` line each crate carries is a convention, and a convention is
//! what a new crate is added without. This is the assertion.
//!
//! **The file set is whatever listing the guard is handed, and the rule is
//! deliberately the broader one.** The Makefile path feeds `git ls-files`; the
//! Bazel path feeds `//:policy_manifest`, a `native.glob(["**"])` that does not
//! consult `.gitignore` and therefore sees untracked files too. A gitignored
//! `src/bindings.rs` materialized into a crate — a stray `cargo component
//! build`, a merge leaving a deleted file on disk — is a violation under the
//! Bazel path, on purpose: the ambiguity the guard exists to kill is a file
//! being *there*, not a file being tracked.
//!
//! Only the crate-root path is checked. A deeper `.../src/bindings.rs` is a
//! hand-written module that nothing substitutes, so it is not this guard's
//! business.

use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};

/// The guest workspace, whose crates take generated bindings.
const GUEST_TREE: &str = "brenn-wasm/components";

/// The one crate whose `src/bindings.rs` is hand-written and committed.
const HAND_WRITTEN: &str = "guest";

/// Whether `rel` is `<GUEST_TREE>/<crate>/src/bindings.rs` for some crate other
/// than the SDK's — exactly one crate segment, nothing deeper.
fn is_guest_bindings_path(rel: &Path) -> bool {
    let Ok(tail) = rel.strip_prefix(GUEST_TREE) else {
        return false;
    };
    let parts: Vec<Component<'_>> = tail.components().collect();
    if parts.len() != 3 {
        return false;
    }
    let names: Vec<&str> = parts
        .iter()
        .filter_map(|c| match c {
            Component::Normal(s) => s.to_str(),
            _ => None,
        })
        .collect();
    if names.len() != 3 {
        return false;
    }
    names[0] != HAND_WRITTEN && names[1] == "src" && names[2] == "bindings.rs"
}

/// `src/bindings.rs` paths under the guest tree, minus the SDK's.
fn violations(files: &[PathBuf]) -> BTreeSet<PathBuf> {
    files
        .iter()
        .filter(|rel| is_guest_bindings_path(rel))
        .cloned()
        .collect()
}

/// The failure text for the given violations.
fn report(found: &BTreeSet<PathBuf>) -> String {
    let mut out = String::from(
        "generated-bindings guard: guest crates take generated WIT bindings, so these \
         source-tree copies must not exist:\n",
    );
    for rel in found {
        out.push_str(&format!("  {}\n", rel.display()));
    }
    out.push_str(
        "A generated `src/bindings.rs` is substituted over that path — by \
         `wasm_guest_cdylib`'s `shared_bindings`, or by the crate's own `wit_bindgen_rust` \
         — so the copy in the tree is not what compiles. A crate with no bindings \
         generation at all should be taking the brenn-guest SDK instead of hand-rolling \
         a bindings module. Either way: delete the file, and add its path to .gitignore \
         if it is not there already.\n",
    );
    out
}

/// Returns `true` when no guest crate carries a generated `src/bindings.rs`.
pub fn run_generated_bindings_guard(files: &[PathBuf]) -> bool {
    let found = violations(files);
    if found.is_empty() {
        return true;
    }
    eprint!("{}", report(&found));
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn files(paths: &[&str]) -> Vec<PathBuf> {
        paths.iter().map(PathBuf::from).collect()
    }

    #[test]
    fn a_tree_with_no_committed_bindings_passes() {
        assert!(run_generated_bindings_guard(&files(&[
            "brenn-wasm/components/replay/src/lib.rs",
            "brenn-wasm/components/replay/BUILD.bazel",
        ])));
    }

    #[test]
    fn the_sdks_hand_written_copy_is_exempt() {
        assert!(run_generated_bindings_guard(&files(&[
            "brenn-wasm/components/guest/src/bindings.rs",
        ])));
    }

    #[test]
    fn a_committed_copy_in_any_other_guest_crate_fails() {
        assert!(!run_generated_bindings_guard(&files(&[
            "brenn-wasm/components/guest/src/bindings.rs",
            "brenn-wasm/components/replay/src/bindings.rs",
        ])));
    }

    #[test]
    fn a_bindings_module_outside_the_guest_tree_is_not_this_guards_business() {
        assert!(run_generated_bindings_guard(&files(&[
            "brenn-lib/src/bindings.rs",
        ])));
    }

    #[test]
    fn a_gitignored_path_present_in_the_file_set_is_still_a_violation() {
        // The Bazel path's manifest globs the tree without consulting
        // `.gitignore`, and every shared-bindings crate's path is gitignored.
        assert!(!run_generated_bindings_guard(&files(&[
            "brenn-wasm/components/processor-exhaust/src/bindings.rs",
        ])));
    }

    #[test]
    fn a_nested_bindings_module_is_not_a_violation() {
        assert!(run_generated_bindings_guard(&files(&[
            "brenn-wasm/components/replay/src/inner/src/bindings.rs",
            "brenn-wasm/components/replay/src/nested/bindings.rs",
        ])));
    }

    #[test]
    fn the_report_names_the_path_and_the_fix() {
        let found = violations(&files(&["brenn-wasm/components/replay/src/bindings.rs"]));
        let text = report(&found);
        assert!(text.contains("brenn-wasm/components/replay/src/bindings.rs"));
        assert!(text.contains("shared_bindings"));
        assert!(text.contains(".gitignore"));
    }
}
