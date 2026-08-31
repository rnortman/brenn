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
        scopes: &["surface/components"],
        tests_exempt: true,
        why: "gap classification never reaches the component seam; survives \
              in brenn-lib's resume layer, in surface/schema as the wire \
              encoding the kernel re-resumes on, and in surface/contract's \
              prose saying exactly that",
    },
    Condemned {
        token: "SetBanner",
        scopes: &["surface/kernel", "surface/schema", "frontend"],
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

/// The scannable subset of the caller's file set, repo-root-relative.
fn collect(files: &[PathBuf]) -> Vec<PathBuf> {
    files
        .iter()
        .filter(|rel| is_scannable(rel))
        .cloned()
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

fn violations(root: &Path, files: &[PathBuf]) -> Vec<String> {
    let mut found = Vec::new();
    for rel in files {
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

/// True if no condemned vocabulary survives in the given files.
pub fn run_removal_guard(root: &Path, files: &[PathBuf]) -> bool {
    let files = collect(files);
    let found = violations(root, &files);
    if files.len() < crate::file_set::MIN_SCANNED_FILES {
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

    /// The anti-vacuity floor is the only thing standing between a collapsed
    /// file set and a green guard, and a collapsed file set produces no other
    /// signal. An inverted comparison here would disable the protection
    /// silently, so the floor is exercised directly.
    #[test]
    fn a_collapsed_file_set_fails_rather_than_passing_over_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::write(root.join("a.rs"), "fn a() {}\n").unwrap();
        std::fs::write(root.join("b.ts"), "export const b = 1;\n").unwrap();
        assert!(!run_removal_guard(root, &[p("a.rs"), p("b.ts")]));
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
        assert!(is_test_source(&p(
            "brenn-server/src/routes/surface/ws_tests.rs"
        )));
        assert!(is_test_source(&p("e2e/tests/bar.spec.ts")));
        assert!(!is_test_source(&p("surface/kernel/src/latests.rs")));
        assert!(!is_test_source(&p("brenn-lib/src/protests/mod.rs")));
    }
}
