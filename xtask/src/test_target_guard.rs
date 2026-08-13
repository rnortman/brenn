//! Test-target guard: a crate whose sources carry `#[test]` functions declares
//! a Bazel target that runs them.
//!
//! `rust_test(crate = ":x")` is the target that compiles a crate's in-crate
//! tests into a runnable binary. Nothing else in the build notices its absence:
//! the sources are still in a package (`policy_parity` green), the crate is
//! still a workspace member (`xtask deny` and the workspace guard green), and
//! `bazel test //...` simply never learns the tests exist. Moving a module into
//! a new crate and forgetting that one target takes the whole suite dark with
//! every gate still passing.
//!
//! Scope is the in-crate suite: `.rs` files under a package's `src/`, against
//! the `rust_library`/`rust_binary` targets that package declares. Integration
//! tests under `tests/` are separate `rust_test` targets over their own `srcs`,
//! and a package declaring no Rust crate at all — the wasm guests, built by the
//! component rules — is outside what this guard can say anything about.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// Directories holding a `BUILD.bazel`, i.e. Bazel packages.
fn package_dirs(files: &[PathBuf]) -> BTreeSet<PathBuf> {
    files
        .iter()
        .filter(|rel| rel.file_name().is_some_and(|n| n == "BUILD.bazel"))
        .map(|rel| rel.parent().unwrap_or_else(|| Path::new("")).to_path_buf())
        .collect()
}

/// The package a file belongs to: the nearest ancestor directory that is one.
/// Nearest, because a subpackage owns its own files.
fn owning_package<'a>(packages: &'a BTreeSet<PathBuf>, rel: &Path) -> Option<&'a Path> {
    rel.ancestors()
        .skip(1)
        .find_map(|dir| packages.get(dir).map(PathBuf::as_path))
}

/// The closing paren matching the one at `open`, skipping quoted strings and
/// `#` comments so a paren inside either does not move the depth.
pub fn matching_paren(text: &str, open: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut quote: Option<u8> = None;
    let mut i = open;
    while i < bytes.len() {
        let b = bytes[i];
        match quote {
            Some(q) => {
                if b == b'\\' {
                    i += 2;
                    continue;
                }
                if b == q {
                    quote = None;
                }
            }
            None => match b {
                b'"' | b'\'' => quote = Some(b),
                b'#' => {
                    while i < bytes.len() && bytes[i] != b'\n' {
                        i += 1;
                    }
                    continue;
                }
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(i);
                    }
                }
                _ => {}
            },
        }
        i += 1;
    }
    None
}

/// The argument text of every call to `rule` in a BUILD file. Whole-identifier
/// matching, so `rust_test(` does not also report `native_rust_test(`.
pub fn call_blocks<'a>(text: &'a str, rule: &str) -> Vec<&'a str> {
    let needle = format!("{rule}(");
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut from = 0usize;
    while let Some(hit) = text[from..].find(&needle) {
        let start = from + hit;
        from = start + needle.len();
        if start > 0 {
            let prev = bytes[start - 1];
            if prev == b'_' || prev.is_ascii_alphanumeric() {
                continue;
            }
        }
        let open = start + needle.len() - 1;
        if let Some(end) = matching_paren(text, open) {
            out.push(&text[open + 1..end]);
            from = end + 1;
        }
    }
    out
}

/// The string value of `key = "..."` inside a call's argument text.
pub fn attr_string(block: &str, key: &str) -> Option<String> {
    for (i, _) in block.match_indices(key) {
        if i > 0 {
            let prev = block.as_bytes()[i - 1];
            if prev == b'_' || prev.is_ascii_alphanumeric() {
                continue;
            }
        }
        let rest = block[i + key.len()..].trim_start();
        let Some(rest) = rest.strip_prefix('=') else {
            continue;
        };
        let rest = rest.trim_start();
        let Some(rest) = rest.strip_prefix('"') else {
            continue;
        };
        if let Some(end) = rest.find('"') {
            return Some(rest[..end].to_string());
        }
    }
    None
}

/// The Rust crates a BUILD file declares, by target name.
fn declared_crates(build: &str) -> BTreeSet<String> {
    ["rust_library", "rust_binary"]
        .iter()
        .flat_map(|rule| call_blocks(build, rule))
        .filter_map(|block| attr_string(block, "name"))
        .collect()
}

/// Whether the file declares a `rust_test` over one of its own crates.
/// `rust_doc_test` carries the same `crate` attribute and runs no `#[test]`,
/// which is why the rule is read per call and not by grepping for `crate =`.
fn runs_in_crate_tests(build: &str, crates: &BTreeSet<String>) -> bool {
    call_blocks(build, "rust_test")
        .iter()
        .filter_map(|block| attr_string(block, "crate"))
        .any(|label| {
            label
                .strip_prefix(':')
                .is_some_and(|name| crates.contains(name))
        })
}

/// Whether a source file defines test functions. The attribute's last path
/// segment is what decides, so `#[tokio::test(flavor = "multi_thread")]` counts
/// and `#[cfg(test)]` does not.
fn has_test_fn(src: &str) -> bool {
    src.match_indices("#[").any(|(i, _)| {
        let rest = &src[i + 2..];
        let end = rest.find([']', '(', '\n']).unwrap_or(rest.len());
        rest[..end].trim().rsplit("::").next() == Some("test")
    })
}

/// Packages whose `src/` carries tests, mapped to the crates they declare.
/// A package declaring no Rust crate is not in the map at all.
fn packages_with_in_crate_tests(
    root: &Path,
    files: &[PathBuf],
) -> BTreeMap<PathBuf, BTreeSet<String>> {
    let packages = package_dirs(files);
    let mut builds: BTreeMap<PathBuf, BTreeSet<String>> = BTreeMap::new();
    let mut found: BTreeMap<PathBuf, BTreeSet<String>> = BTreeMap::new();
    for rel in files {
        if rel.extension().is_none_or(|e| e != "rs") {
            continue;
        }
        let Some(pkg) = owning_package(&packages, rel) else {
            continue;
        };
        if !rel
            .strip_prefix(pkg)
            .is_ok_and(|inner| inner.starts_with("src"))
        {
            continue;
        }
        let crates = builds.entry(pkg.to_path_buf()).or_insert_with(|| {
            let build = pkg.join("BUILD.bazel");
            let text = std::fs::read_to_string(root.join(&build))
                .unwrap_or_else(|e| panic!("test-target guard: cannot read {build:?}: {e}"));
            declared_crates(&text)
        });
        if crates.is_empty() || found.contains_key(pkg) {
            continue;
        }
        let text = std::fs::read_to_string(root.join(rel))
            .unwrap_or_else(|e| panic!("test-target guard: cannot read {rel:?}: {e}"));
        if has_test_fn(&text) {
            found.insert(pkg.to_path_buf(), crates.clone());
        }
    }
    found
}

fn violation(pkg: &Path, crates: &BTreeSet<String>) -> String {
    let names: Vec<&str> = crates.iter().map(String::as_str).collect();
    format!(
        "{}: sources under src/ define #[test] functions, but its BUILD.bazel declares no \
         rust_test(crate = \":<crate>\") over any of [{}]. Those tests compile into the crate \
         and run nowhere — `bazel test //...` cannot see a suite no target names. Add the \
         rust_test target beside the library, or delete the tests.",
        pkg.display(),
        names.join(", ")
    )
}

fn violations_from(packages: &BTreeMap<PathBuf, BTreeSet<String>>, root: &Path) -> Vec<String> {
    let mut found = Vec::new();
    for (pkg, crates) in packages {
        let build = pkg.join("BUILD.bazel");
        let text = std::fs::read_to_string(root.join(&build))
            .unwrap_or_else(|e| panic!("test-target guard: cannot read {build:?}: {e}"));
        if !runs_in_crate_tests(&text, crates) {
            found.push(violation(pkg, crates));
        }
    }
    found
}

/// True if every crate carrying in-crate tests has a target that runs them.
pub fn run_test_target_guard(root: &Path, files: &[PathBuf]) -> bool {
    let found = violations_from(&packages_with_in_crate_tests(root, files), root);
    if found.is_empty() {
        return true;
    }
    eprintln!("test-target guard: in-crate tests with no target to run them:");
    for line in &found {
        eprintln!("  {line}");
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A package: its BUILD file and one source file, written under `root`.
    fn package(root: &Path, dir: &str, build: &str, src: &str) -> Vec<PathBuf> {
        let pkg = root.join(dir);
        std::fs::create_dir_all(pkg.join("src")).unwrap();
        std::fs::write(pkg.join("BUILD.bazel"), build).unwrap();
        std::fs::write(pkg.join("src/lib.rs"), src).unwrap();
        vec![
            PathBuf::from(dir).join("BUILD.bazel"),
            PathBuf::from(dir).join("src/lib.rs"),
        ]
    }

    const LIB: &str =
        "rust_library(\n    name = \"crate-a\",\n    srcs = glob([\"src/**/*.rs\"]),\n)\n";
    const DOC_TEST: &str =
        "rust_doc_test(\n    name = \"crate-a_doc_test\",\n    crate = \":crate-a\",\n)\n";
    const CRATE_TEST: &str =
        "rust_test(\n    name = \"crate-a_test\",\n    crate = \":crate-a\",\n)\n";
    const WITH_TESTS: &str =
        "pub fn f() {}\n\n#[cfg(test)]\nmod tests {\n    #[test]\n    fn t() {}\n}\n";

    #[test]
    fn a_crate_with_tests_and_a_test_target_passes() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let files = package(
            root,
            "crate-a",
            &format!("{LIB}{CRATE_TEST}{DOC_TEST}"),
            WITH_TESTS,
        );
        assert!(violations_from(&packages_with_in_crate_tests(root, &files), root).is_empty());
    }

    /// The failure this guard exists for: the library and its doc test survive
    /// a move, the `rust_test` does not.
    #[test]
    fn a_crate_with_tests_and_only_a_doc_test_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let files = package(root, "crate-a", &format!("{LIB}{DOC_TEST}"), WITH_TESTS);
        let out = violations_from(&packages_with_in_crate_tests(root, &files), root);
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(
            out[0].starts_with("crate-a: sources under src/"),
            "{}",
            out[0]
        );
        assert!(out[0].contains("crate-a"), "{}", out[0]);
    }

    #[test]
    fn a_test_target_naming_a_foreign_crate_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let build = format!(
            "{LIB}rust_test(\n    name = \"x\",\n    crate = \"//other:other\",\n)\n{DOC_TEST}"
        );
        let files = package(root, "crate-a", &build, WITH_TESTS);
        assert_eq!(
            violations_from(&packages_with_in_crate_tests(root, &files), root).len(),
            1
        );
    }

    #[test]
    fn a_crate_with_no_test_functions_needs_no_test_target() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let files = package(
            root,
            "crate-a",
            &format!("{LIB}{DOC_TEST}"),
            "pub fn f() {}\n\n#[cfg(test)]\nmod tests {\n    use super::*;\n}\n",
        );
        assert!(packages_with_in_crate_tests(root, &files).is_empty());
    }

    #[test]
    fn a_package_declaring_no_rust_crate_is_out_of_scope() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let files = package(
            root,
            "guest",
            "wasm_guest_cdylib(\n    name = \"g\",\n)\n",
            WITH_TESTS,
        );
        assert!(packages_with_in_crate_tests(root, &files).is_empty());
    }

    #[test]
    fn a_subpackage_owns_its_own_sources() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let mut files = package(
            root,
            "outer",
            &format!("{LIB}{CRATE_TEST}"),
            "pub fn f() {}\n",
        );
        files.extend(package(
            root,
            "outer/inner",
            "rust_library(\n    name = \"inner\",\n)\n",
            WITH_TESTS,
        ));
        let found = packages_with_in_crate_tests(root, &files);
        assert_eq!(
            found.keys().collect::<Vec<_>>(),
            [&PathBuf::from("outer/inner")]
        );
    }

    #[test]
    fn test_attributes_are_recognized_by_their_last_path_segment() {
        assert!(has_test_fn("    #[test]\n    fn t() {}\n"));
        assert!(has_test_fn("#[tokio::test(flavor = \"multi_thread\")]\n"));
        assert!(!has_test_fn("#[cfg(test)]\nmod tests {}\n"));
        assert!(!has_test_fn("#[cfg_attr(test, derive(Debug))]\n"));
        assert!(!has_test_fn("#[wasm_bindgen_test]\n"));
        assert!(!has_test_fn("pub fn test() {}\n"));
    }

    /// Blocks are read by paren matching, so a rule call spanning lines, nested
    /// calls in its arguments, and a `#` comment holding a paren all parse.
    #[test]
    fn call_blocks_read_whole_calls() {
        let text = "rust_test(\n    name = \"a\",\n    # a ) in a comment\n    deps = f(g(1)),\n    crate = \":x\",\n)\nrust_test(name = \"b\", crate = \":y\")\n";
        let blocks = call_blocks(text, "rust_test");
        assert_eq!(blocks.len(), 2, "{blocks:?}");
        assert_eq!(attr_string(blocks[0], "crate").as_deref(), Some(":x"));
        assert_eq!(attr_string(blocks[1], "crate").as_deref(), Some(":y"));
        assert_eq!(attr_string(blocks[1], "name").as_deref(), Some("b"));
    }

    #[test]
    fn a_longer_rule_name_is_not_this_rule() {
        assert!(call_blocks("native_rust_test(\n    src = \":x\",\n)\n", "rust_test").is_empty());
        assert!(!runs_in_crate_tests(
            "rust_doc_test(\n    crate = \":crate-a\",\n)\n",
            &BTreeSet::from(["crate-a".to_string()])
        ));
    }
}
