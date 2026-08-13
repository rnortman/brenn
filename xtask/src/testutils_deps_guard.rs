//! Testutils-dep guard: a label reached only from `testutils`-gated code is
//! named through `testutils_deps([...])` and nowhere else in the same target's
//! `deps`.
//!
//! `testutils_deps` returns a `select()` that yields its labels only when the
//! feature is on, so a configuration that clears the feature drops the build
//! edge instead of carrying it into the release graph. Naming the same label
//! unconditionally beside the wrapper restores the edge in every configuration
//! and nothing reddens: both spellings build, and the only symptom is the
//! dependents rebuilding on an edit they no longer read.
//!
//! Scope is textual, over `BUILD.bazel` files: the `deps` attribute of every
//! `rust_library`, `rust_binary` and `rust_test` call. A `testutils_deps` call
//! anywhere else in such a file is itself reported, because a wrapper this
//! guard cannot read is a wrapper it cannot hold to the rule.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::test_target_guard::{attr_string, call_blocks};

/// Rules whose `deps` this guard reads.
const RULES: [&str; 3] = ["rust_library", "rust_binary", "rust_test"];

/// The text with `#` comments removed, so a quote or a paren inside one cannot
/// move the scanner's state.
fn strip_comments(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut quote: Option<char> = None;
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        match quote {
            Some(q) => {
                out.push(c);
                if c == '\\' {
                    if let Some(next) = chars.next() {
                        out.push(next);
                    }
                } else if c == q {
                    quote = None;
                }
            }
            None => match c {
                '"' | '\'' => {
                    quote = Some(c);
                    out.push(c);
                }
                '#' => {
                    for c in chars.by_ref() {
                        if c == '\n' {
                            out.push('\n');
                            break;
                        }
                    }
                }
                _ => out.push(c),
            },
        }
    }
    out
}

/// The expression text of `key = <expr>` inside a call's argument text: from
/// after the `=` to the comma that closes the attribute at depth zero.
fn attr_expr<'a>(block: &'a str, key: &str) -> Option<&'a str> {
    let bytes = block.as_bytes();
    for (i, _) in block.match_indices(key) {
        if i > 0 {
            let prev = bytes[i - 1];
            if prev == b'_' || prev.is_ascii_alphanumeric() {
                continue;
            }
        }
        let after = i + key.len();
        let rest = block[after..].trim_start();
        let Some(rest) = rest.strip_prefix('=') else {
            continue;
        };
        // `==` is not an assignment; nothing in a BUILD file should have one,
        // but reading it as one would take the wrong span.
        if rest.starts_with('=') {
            continue;
        }
        let start = block.len() - rest.len();
        return Some(&block[start..start + expr_len(rest)]);
    }
    None
}

/// The length of the expression starting at `text`, ending at the first comma
/// at bracket depth zero (or at the end of the argument text).
fn expr_len(text: &str) -> usize {
    let bytes = text.as_bytes();
    let mut depth = 0i32;
    let mut quote: Option<u8> = None;
    let mut i = 0usize;
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
                b'(' | b'[' | b'{' => depth += 1,
                b')' | b']' | b'}' => {
                    if depth == 0 {
                        return i;
                    }
                    depth -= 1;
                }
                b',' if depth == 0 => return i,
                _ => {}
            },
        }
        i += 1;
    }
    bytes.len()
}

/// Every string literal naming a Bazel label (`//pkg:target` or `:target`).
fn labels(text: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut rest = text;
    while let Some(open) = rest.find('"') {
        let after = &rest[open + 1..];
        let Some(end) = after.find('"') else {
            break;
        };
        let value = &after[..end];
        if value.starts_with("//") || value.starts_with(':') || value.starts_with('@') {
            out.insert(value.to_string());
        }
        rest = &after[end + 1..];
    }
    out
}

/// The text with every `testutils_deps(...)` call — arguments included —
/// removed, leaving what the target names unconditionally.
fn without_wrapper_calls(text: &str) -> String {
    let needle = "testutils_deps(";
    let mut out = String::new();
    let mut rest = text;
    while let Some(hit) = rest.find(needle) {
        out.push_str(&rest[..hit]);
        let open = hit + needle.len() - 1;
        match crate::test_target_guard::matching_paren(rest, open) {
            Some(end) => rest = &rest[end + 1..],
            None => return out,
        }
    }
    out.push_str(rest);
    out
}

/// One target's verdict: the labels named both ways, and how many wrapper calls
/// its `deps` accounted for.
struct TargetRead {
    name: String,
    both_ways: BTreeSet<String>,
    wrapper_calls: usize,
}

fn read_targets(build: &str) -> Vec<TargetRead> {
    let mut out = Vec::new();
    for rule in RULES {
        for block in call_blocks(build, rule) {
            let Some(deps) = attr_expr(block, "deps") else {
                continue;
            };
            let gated: BTreeSet<String> = call_blocks(deps, "testutils_deps")
                .iter()
                .flat_map(|args| labels(args))
                .collect();
            if gated.is_empty() && !deps.contains("testutils_deps(") {
                continue;
            }
            let unconditional = labels(&without_wrapper_calls(deps));
            out.push(TargetRead {
                name: attr_string(block, "name").unwrap_or_else(|| "<unnamed>".to_string()),
                both_ways: gated.intersection(&unconditional).cloned().collect(),
                wrapper_calls: deps.matches("testutils_deps(").count(),
            });
        }
    }
    out
}

fn violations_in(build: &str, path: &Path) -> Vec<String> {
    let text = strip_comments(build);
    let targets = read_targets(&text);
    let mut found = Vec::new();
    let mut accounted = 0usize;
    for target in &targets {
        accounted += target.wrapper_calls;
        for label in &target.both_ways {
            found.push(format!(
                "{}: target {:?} names {label} inside testutils_deps([...]) and again in its \
                 unconditional deps. The wrapper's select() drops the edge when the feature is \
                 off; the second spelling puts it back in every configuration, silently. Name it \
                 once, inside the wrapper.",
                path.display(),
                target.name
            ));
        }
    }
    let total = text.matches("testutils_deps(").count();
    if total > accounted {
        found.push(format!(
            "{}: {} testutils_deps([...]) call(s) sit outside the deps attribute of a \
             {} target, where this guard cannot hold them to the rule. Move the wrapper into \
             a deps expression.",
            path.display(),
            total - accounted,
            RULES.join("/")
        ));
    }
    found
}

/// True if no target names a testutils-gated label unconditionally as well.
pub fn run_testutils_deps_guard(root: &Path, files: &[PathBuf]) -> bool {
    let mut found = Vec::new();
    for rel in files {
        if rel.file_name().is_none_or(|n| n != "BUILD.bazel") {
            continue;
        }
        let text = std::fs::read_to_string(root.join(rel))
            .unwrap_or_else(|e| panic!("testutils-dep guard: cannot read {rel:?}: {e}"));
        if !text.contains("testutils_deps(") {
            continue;
        }
        found.extend(violations_in(&text, rel));
    }
    if found.is_empty() {
        return true;
    }
    eprintln!("testutils-dep guard: feature-gated build edges named unconditionally:");
    for line in &found {
        eprintln!("  {line}");
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    const GATED: &str = r#"rust_library(
    name = "crate-a",
    deps = all_crate_deps(normal = True) + testutils_deps([
        "//attach/client",
    ]) + [
        "//brenn-lib",
    ],
    edition = "2024",
)
"#;

    #[test]
    fn a_label_named_only_through_the_wrapper_passes() {
        assert!(violations_in(GATED, Path::new("a/BUILD.bazel")).is_empty());
    }

    /// The regression this guard exists for: the label comes back to the
    /// unconditional list and every configuration builds green.
    #[test]
    fn the_same_label_named_both_ways_fails() {
        let build = GATED.replace(
            "\"//brenn-lib\",",
            "\"//attach/client\",\n        \"//brenn-lib\",",
        );
        let out = violations_in(&build, Path::new("a/BUILD.bazel"));
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].contains("//attach/client"), "{}", out[0]);
        assert!(out[0].contains("\"crate-a\""), "{}", out[0]);
    }

    #[test]
    fn each_rule_is_read_separately() {
        let build = format!("{GATED}{}", GATED.replace("rust_library", "rust_test"));
        assert!(violations_in(&build, Path::new("a/BUILD.bazel")).is_empty());
        let broken = format!(
            "{GATED}{}",
            GATED
                .replace("rust_library", "rust_test")
                .replace("\"//brenn-lib\",", "\"//attach/client\",")
        );
        let out = violations_in(&broken, Path::new("a/BUILD.bazel"));
        assert_eq!(out.len(), 1, "{out:?}");
    }

    #[test]
    fn a_wrapper_outside_a_deps_attribute_is_reported() {
        let build = "MY_DEPS = testutils_deps([\"//attach/client\"])\n\nrust_library(\n    name = \"crate-a\",\n    deps = MY_DEPS,\n)\n";
        let out = violations_in(build, Path::new("a/BUILD.bazel"));
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].contains("outside the deps attribute"), "{}", out[0]);
    }

    #[test]
    fn a_label_in_data_is_not_a_deps_edge() {
        let build = GATED.replace(
            "    edition = \"2024\",",
            "    data = [\"//attach/client\"],\n    edition = \"2024\",",
        );
        assert!(violations_in(&build, Path::new("a/BUILD.bazel")).is_empty());
    }

    #[test]
    fn proc_macro_deps_is_not_deps() {
        let build = "rust_library(\n    name = \"crate-a\",\n    proc_macro_deps = [\"//p\"],\n    deps = testutils_deps([\"//attach/client\"]),\n)\n";
        assert!(violations_in(build, Path::new("a/BUILD.bazel")).is_empty());
    }

    #[test]
    fn a_commented_out_label_does_not_count() {
        let build = GATED.replace(
            "    edition = \"2024\",",
            "    # deps once named \"//attach/client\" here\n    edition = \"2024\",",
        );
        assert!(violations_in(&build, Path::new("a/BUILD.bazel")).is_empty());
    }

    #[test]
    fn attr_expr_spans_the_whole_expression() {
        let block = "\n    name = \"a\",\n    deps = f(1) + [\n        \"//x\",\n    ] + g(2),\n    size = \"small\",\n";
        let deps = attr_expr(block, "deps").expect("deps");
        assert!(deps.trim().starts_with("f(1)"), "{deps}");
        assert!(deps.trim().ends_with("g(2)"), "{deps}");
        assert_eq!(labels(deps), BTreeSet::from(["//x".to_string()]));
    }

    /// The real tree: the guard's rule holds at HEAD, so a failure here is a
    /// regression and not a fixture drifting.
    #[test]
    fn the_repo_is_clean() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace root")
            .to_path_buf();
        let build = root.join("brenn-server/BUILD.bazel");
        let text = std::fs::read_to_string(&build).expect("brenn-server BUILD.bazel");
        assert!(text.contains("testutils_deps("), "call site moved");
        assert!(
            violations_in(&text, Path::new("brenn-server/BUILD.bazel")).is_empty(),
            "brenn-server/BUILD.bazel violates the testutils-dep rule"
        );
    }
}
