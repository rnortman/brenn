//! Removal guard: condemned vocabulary must stay absent from live source.
//!
//! Each entry names a token whose absence is a contract, plus the scope the
//! absence holds over. Scopes are directories under the repo root; ADR docs and
//! design artifacts are outside every scope — a token may legitimately appear in
//! prose describing why it was removed.
//!
//! A token whose scope is the whole tree carries `&["."]`. A token that
//! survives legitimately somewhere carries the narrower scope in which it is
//! condemned, with the survivor named in `why`.
//!
//! Test sources are scanned like any other source. An entry sets
//! `tests_exempt` only when tests legitimately construct wire values whose
//! names survive server-side; unconditional condemnations hold in tests too,
//! because a test still naming a removed concept keeps it alive in the suite's
//! vocabulary.
//!
//! Matching is on **identifier boundaries**, not raw substrings: `is_layout`
//! does not match `is_layout_root`. A newly named symbol that merely contains a
//! condemned token is not a reintroduction of the condemned concept, and a
//! guard that fails on one erodes by rename.

use std::path::{Path, PathBuf};

/// One condemned token and the scope its absence is asserted over.
struct Condemned {
    token: &'static str,
    /// Repo-root-relative directories the token must not appear in.
    scopes: &'static [&'static str],
    /// Test sources are outside this entry's scope.
    tests_exempt: bool,
    /// What the absence means, and where the token legitimately survives.
    why: &'static str,
}

const CONDEMNED: &[Condemned] = &[
    Condemned {
        token: "PORT_MESSAGE",
        scopes: &["."],
        tests_exempt: false,
        why: "per-message dialect event; replaced by activation delivery",
    },
    Condemned {
        token: "PORT_DROPS",
        scopes: &["."],
        tests_exempt: false,
        why: "per-message dialect event; drops ride the activation window",
    },
    Condemned {
        token: "PORT_GAP",
        scopes: &["."],
        tests_exempt: false,
        why: "per-message dialect event; gaps ride SubscribeResult",
    },
    Condemned {
        token: "GapReason",
        scopes: &["surface/component-support", "surface/components"],
        tests_exempt: true,
        why: "gap classification never reaches the component seam; survives \
              in brenn-lib's resume layer, in surface/proto as the wire \
              encoding the kernel re-resumes on, and in surface/contract's \
              prose saying exactly that",
    },
    Condemned {
        token: "SetBanner",
        scopes: &["surface/kernel", "surface/proto", "frontend"],
        tests_exempt: true,
        why: "shell-side rendering of application state; survives only as \
              chrome's own internal ChromeAction, which is a component's \
              private vocabulary",
    },
    Condemned {
        token: "LayoutBinding",
        scopes: &["."],
        tests_exempt: false,
        why: "layout special-casing; the layout is an ordinary brenn: binding \
              on the chrome instance",
    },
    Condemned {
        token: "validate_surface_slugs_disjoint",
        scopes: &["."],
        tests_exempt: false,
        why: "kernel-grain subscription validation; grain is per-instance",
    },
    Condemned {
        token: "is_layout",
        scopes: &["."],
        tests_exempt: false,
        why: "layout-by-grain inference",
    },
    Condemned {
        token: "COMPONENT_THEME",
        scopes: &["."],
        tests_exempt: false,
        why: "v0 theme seam event; theme rides the local:brenn/theme plane",
    },
    Condemned {
        token: "COMPONENT_TAKEOVER",
        scopes: &["."],
        tests_exempt: false,
        why: "v0 takeover seam events; takeover rides its own plane",
    },
    Condemned {
        token: "recovers_by_replay",
        scopes: &["."],
        tests_exempt: false,
        why: "class-split predicate; overflow is drop-oldest on every class",
    },
    Condemned {
        token: "surface/shell",
        scopes: &["."],
        tests_exempt: false,
        why: "crate split into surface/kernel and surface/chrome",
    },
];

/// Extensions worth scanning. Source, config, and build glue — a stale crate
/// path survives longest in CI workflows and shell scripts.
const EXTENSIONS: &[&str] = &[
    "rs", "ts", "js", "wit", "toml", "html", "css", "yml", "yaml", "json", "sh",
];

/// Extensionless file names worth scanning.
const FILENAMES: &[&str] = &["Makefile"];

/// Repo-root-relative path prefixes excluded from every scope. Build output and
/// untracked state never reach the scan (the file list is `git ls-files`); these
/// are tracked paths whose content legitimately names condemned vocabulary.
const EXCLUDED: &[&str] = &["docs", "xtask/src/removal_guard.rs"];

/// True when the path is test source. Anchored on path components, not raw
/// substrings: `latests.rs` is live source, and so is `src/protests/mod.rs`.
fn is_test_source(rel: &Path) -> bool {
    let name = rel.file_name().map(|n| n.to_string_lossy().into_owned());
    let dir_hit = rel.components().any(|c| {
        let s = c.as_os_str().to_string_lossy();
        s == "tests" || s == "test_support" || s == "test-fixtures" || s == "e2e"
    });
    let name_hit = name.as_deref().is_some_and(|n| {
        n == "tests.rs"
            || n.ends_with("_tests.rs")
            || n.ends_with(".test.ts")
            || n.ends_with(".spec.ts")
    });
    dir_hit || name_hit
}

fn is_excluded(rel: &Path) -> bool {
    let s = rel.to_string_lossy();
    EXCLUDED
        .iter()
        .any(|p| s == *p || s.starts_with(&format!("{p}/")))
}

fn is_scannable(rel: &Path) -> bool {
    if is_excluded(rel) {
        return false;
    }
    let by_ext = rel
        .extension()
        .is_some_and(|e| EXTENSIONS.contains(&e.to_string_lossy().as_ref()));
    let by_name = rel
        .file_name()
        .is_some_and(|n| FILENAMES.contains(&n.to_string_lossy().as_ref()));
    by_ext || by_name
}

/// Tracked source files worth scanning, repo-root-relative.
///
/// The file set is git's, so build output, untracked scratch files, and
/// anything `.gitignore` covers are outside the scan by construction.
fn collect(root: &Path) -> Vec<PathBuf> {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["ls-files", "-z"])
        .output()
        .unwrap_or_else(|e| panic!("removal guard: cannot run git ls-files: {e}"));
    assert!(
        out.status.success(),
        "removal guard: git ls-files failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let listing = String::from_utf8(out.stdout)
        .unwrap_or_else(|e| panic!("removal guard: git ls-files output is not UTF-8: {e}"));
    listing
        .split('\0')
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .filter(|rel| is_scannable(rel))
        .collect()
}

fn is_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// True when `token` occurs in `line` delimited by non-identifier characters.
fn contains_token(line: &str, token: &str) -> bool {
    let mut from = 0;
    while let Some(off) = line[from..].find(token) {
        let start = from + off;
        let end = start + token.len();
        let before_ok = line[..start]
            .chars()
            .next_back()
            .is_none_or(|c| !is_ident_char(c) || !token.starts_with(|t: char| is_ident_char(t)));
        let after_ok = line[end..]
            .chars()
            .next()
            .is_none_or(|c| !is_ident_char(c) || !token.ends_with(is_ident_char));
        if before_ok && after_ok {
            return true;
        }
        from = end;
    }
    false
}

/// Scan one file's text. Pure: no I/O, no tree walk — the matching half of the
/// guard, testable with synthetic inputs.
fn scan_text(rel: &Path, text: &str) -> Vec<String> {
    let mut found = Vec::new();
    let is_test = is_test_source(rel);
    for entry in CONDEMNED {
        if is_test && entry.tests_exempt {
            continue;
        }
        let in_scope = entry.scopes.iter().any(|s| *s == "." || rel.starts_with(s));
        if !in_scope {
            continue;
        }
        for (i, line) in text.lines().enumerate() {
            if contains_token(line, entry.token) {
                found.push(format!(
                    "{}:{}: condemned `{}` ({})",
                    rel.display(),
                    i + 1,
                    entry.token,
                    entry.why
                ));
            }
        }
    }
    found
}

/// Scan the tree; return one line per surviving occurrence.
pub fn violations(root: &Path) -> Vec<String> {
    let files = collect(root);
    let mut found = Vec::new();
    for rel in &files {
        let text = match std::fs::read_to_string(root.join(rel)) {
            Ok(t) => t,
            // Non-UTF8 source is not source we condemn vocabulary in. Every
            // other read failure is a broken scan, not a skippable file.
            Err(e) if e.kind() == std::io::ErrorKind::InvalidData => continue,
            Err(e) => panic!("removal guard: cannot read {rel:?}: {e}"),
        };
        found.extend(scan_text(rel, &text));
    }
    found
}

/// Run the guard as a check lane. Prints violations; returns pass/fail.
///
/// This is a lane of `xtask check`, not a `#[cfg(test)]` assertion: its input is
/// the whole tracked tree, which is not part of any test binary's input closure,
/// so the test runner's binary-hash pass cache would replay a stale pass for
/// exactly the edits the guard exists to catch.
pub fn run_removal_guard(root: &Path) -> bool {
    let found = violations(root);
    let files = collect(root);
    if files.len() < 200 {
        // A guard that walks an empty file set passes vacuously forever.
        eprintln!(
            "removal guard: scanned only {} files — the walk or the exclusion \
             list is broken, and a vacuous guard asserts nothing",
            files.len()
        );
        return false;
    }
    if found.is_empty() {
        return true;
    }
    eprintln!("removal guard: condemned vocabulary survives in live source:");
    for line in &found {
        eprintln!("  {line}");
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> PathBuf {
        PathBuf::from(s)
    }

    #[test]
    fn tree_wide_token_is_reported_from_any_path() {
        let out = scan_text(&p("brenn-lib/src/x.rs"), "let a = 1;\nuse PORT_GAP;\n");
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].starts_with("brenn-lib/src/x.rs:2: condemned `PORT_GAP`"));
    }

    #[test]
    fn scoped_token_is_not_reported_outside_its_scope() {
        assert!(scan_text(&p("brenn-lib/src/x.rs"), "GapReason::Overflow").is_empty());
        assert_eq!(
            scan_text(&p("surface/components/src/x.rs"), "GapReason::Overflow").len(),
            1
        );
    }

    #[test]
    fn each_occurrence_line_is_reported() {
        let out = scan_text(&p("frontend/src/a.ts"), "PORT_DROPS\nx\nPORT_DROPS\n");
        assert_eq!(out.len(), 2, "{out:?}");
        assert!(out[0].contains(":1:"));
        assert!(out[1].contains(":3:"));
    }

    #[test]
    fn matching_is_on_identifier_boundaries() {
        assert!(scan_text(&p("surface/kernel/src/a.rs"), "fn is_layout_root()").is_empty());
        assert_eq!(
            scan_text(&p("surface/kernel/src/a.rs"), "fn is_layout()").len(),
            1
        );
        // Path-shaped tokens still match inside ordinary punctuation.
        assert_eq!(
            scan_text(&p("Makefile"), "\tcargo build -p surface/shell").len(),
            1
        );
    }

    #[test]
    fn test_sources_are_scanned_except_for_exempt_entries() {
        // Unconditional condemnation holds in tests.
        assert_eq!(
            scan_text(&p("e2e/tests/bar.spec.ts"), "PORT_MESSAGE").len(),
            1
        );
        // Exempt entry: tests may construct the surviving wire value.
        assert!(scan_text(&p("surface/components/src/tests.rs"), "GapReason").is_empty());
    }

    #[test]
    fn test_source_classification_is_component_anchored() {
        assert!(is_test_source(&p("surface/kernel/src/tests.rs")));
        assert!(is_test_source(&p("surface/kernel/src/core/core_tests.rs")));
        assert!(is_test_source(&p("e2e/tests/bar.spec.ts")));
        assert!(!is_test_source(&p("surface/kernel/src/latests.rs")));
        assert!(!is_test_source(&p("brenn-lib/src/protests/mod.rs")));
    }

    #[test]
    fn condemned_vocabulary_is_absent_from_live_source() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("xtask has a parent directory")
            .to_path_buf();
        let found = violations(&root);
        assert!(
            found.is_empty(),
            "removal guard: condemned vocabulary survives in live source:\n{}",
            found.join("\n")
        );
    }
}
