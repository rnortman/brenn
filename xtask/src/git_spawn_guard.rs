//! Git-spawn guard: raw `git` subprocess sites in Rust source are allowlisted.
//!
//! A bare git `Command` with only `current_dir(repo)` does not target `repo`:
//! git's own environment (`GIT_DIR`, `GIT_INDEX_FILE`, `GIT_WORK_TREE`, …)
//! overrides the working directory, so a spawn written that way resolves
//! whatever repo the ambient environment names. Test fixtures written that way
//! are isolated only by luck. The `git-fixture` crate is the one place git is
//! spawned hermetically for tests; production code that must spawn git
//! directly is named here, one entry per file, with the exact number of sites.
//!
//! Counts are per file and exact. That is deliberate: a changed count is a
//! conscious decision about a subprocess boundary, and a guard that tolerates
//! drift stops asserting anything. Adding a site to an already-listed file
//! trips the gate exactly like adding a new file does.

use std::path::{Path, PathBuf};

/// The byte pattern a raw git spawn leaves in source. The suffix form catches
/// `Command`, `StdCommand`, and any other alias of the type. Assembled from
/// fragments so this file does not match its own pattern.
const PATTERN: &str = concat!("::new(", "\"git\")");

/// One file permitted to spawn git directly, and how many times.
struct Allowed {
    /// Repo-root-relative path.
    path: &'static str,
    count: usize,
    /// Why these spawns are not fixture spawns.
    why: &'static str,
}

const ALLOWLIST: &[Allowed] = &[
    Allowed {
        path: "scrub/src/git.rs",
        count: 4,
        why: "scrub's production git interrogation; staged mode depends on \
              inheriting the hook's GIT_INDEX_FILE",
    },
    Allowed {
        path: "brenn-git/src/subprocess.rs",
        count: 1,
        why: "production git subprocess; never runs under a git hook",
    },
    Allowed {
        path: "xtask/src/file_set.rs",
        count: 1,
        why: "the one tracked-file enumeration every policy guard takes its \
              file set from",
    },
    Allowed {
        path: "git-fixture/src/lib.rs",
        count: 6,
        why: "the hermetic fixture crate itself: its spawns (including the \
              failure-tolerating one), its own tests, and the pattern quoted \
              in its module docs",
    },
];

/// Occurrences of `PATTERN` in `text`, counted over overlapping-free matches.
fn count_pattern(text: &str) -> usize {
    text.match_indices(PATTERN).count()
}

fn violations_from(observed: &[(PathBuf, usize)]) -> Vec<String> {
    let mut found = Vec::new();
    for (rel, count) in observed {
        let rel_str = rel.to_string_lossy();
        match ALLOWLIST.iter().find(|a| a.path == rel_str) {
            // Unlisted files are only interesting once they spawn git.
            None if *count == 0 => {}
            None => found.push(format!(
                "{rel_str}: {count} raw git spawn(s). Test code must spawn git \
                 through the `git-fixture` crate, which strips GIT_* so the \
                 spawn actually targets its fixture. A deliberate production \
                 spawn needs an entry in xtask/src/git_spawn_guard.rs."
            )),
            // A listed file is compared at any count, zero included: a count
            // that fell to zero is an entry that has stopped asserting
            // anything, which is as much a drift as a count that rose.
            Some(entry) if entry.count != *count => found.push(format!(
                "{rel_str}: {count} raw git spawn(s), allowlisted for {} ({}). \
                 Confirm the change is deliberate, then update the count in \
                 xtask/src/git_spawn_guard.rs.",
                entry.count, entry.why
            )),
            Some(_) => {}
        }
    }
    for entry in ALLOWLIST {
        let seen = observed
            .iter()
            .any(|(rel, _)| rel.to_string_lossy() == entry.path);
        if !seen {
            found.push(format!(
                "{}: allowlisted for {} raw git spawn(s) but the file is not a \
                 tracked *.rs file. A guard whose allowlist names nothing \
                 asserts nothing — drop the entry or fix the path.",
                entry.path, entry.count
            ));
        }
    }
    found
}

/// Not coupled to `removal_guard`'s file scope: each guard defines its own
/// scan filter independently.
fn collect_counts(root: &Path, files: &[PathBuf]) -> Vec<(PathBuf, usize)> {
    files
        .iter()
        .filter(|rel| rel.extension().is_some_and(|e| e == "rs"))
        .cloned()
        .map(|rel| {
            let bytes = std::fs::read(root.join(&rel))
                .unwrap_or_else(|e| panic!("git-spawn guard: cannot read {rel:?}: {e}"));
            // Lossy rather than a UTF-8 requirement: a file that is valid
            // except for one stray byte must still be scanned, or a spawn site
            // in its valid regions would count as zero.
            let n = count_pattern(&String::from_utf8_lossy(&bytes));
            (rel, n)
        })
        .collect()
}

fn violations(root: &Path, files: &[PathBuf]) -> Vec<String> {
    violations_from(&collect_counts(root, files))
}

/// True if every raw git spawn site in the tree is allowlisted at its exact
/// count.
pub fn run_git_spawn_guard(root: &Path, files: &[PathBuf]) -> bool {
    let found = violations(root, files);
    if found.is_empty() {
        return true;
    }
    eprintln!("git-spawn guard: raw git spawn sites are not as declared:");
    for line in &found {
        eprintln!("  {line}");
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs(pairs: &[(&str, usize)]) -> Vec<(PathBuf, usize)> {
        pairs.iter().map(|(p, n)| (PathBuf::from(*p), *n)).collect()
    }

    /// Every allowlist entry present at its declared count, plus ordinary
    /// spawn-free source.
    fn baseline() -> Vec<(&'static str, usize)> {
        let mut v: Vec<(&'static str, usize)> =
            ALLOWLIST.iter().map(|a| (a.path, a.count)).collect();
        v.push(("brenn-lib/src/lib.rs", 0));
        v
    }

    /// A control for the matching logic, not for the declared counts: the
    /// observation is built *from* `ALLOWLIST`, so it says only that an exact
    /// match yields nothing. Whether the tree matches is the check lane's job.
    #[test]
    fn observed_equal_to_the_allowlist_yields_no_violations() {
        assert!(violations_from(&obs(&baseline())).is_empty());
    }

    #[test]
    fn a_new_spawn_site_in_an_unlisted_file_fails() {
        let mut b = baseline();
        b.push(("brenn-server/src/hooks.rs", 1));
        let out = violations_from(&obs(&b));
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].starts_with("brenn-server/src/hooks.rs: 1 raw git spawn"));
        assert!(out[0].contains("git-fixture"), "{}", out[0]);
    }

    #[test]
    fn count_drift_in_an_allowlisted_file_fails() {
        let mut b = baseline();
        let entry = b
            .iter_mut()
            .find(|(p, _)| *p == "scrub/src/git.rs")
            .expect("scrub/src/git.rs is allowlisted");
        entry.1 += 1;
        let out = violations_from(&obs(&b));
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(
            out[0].contains("scrub/src/git.rs: 5 raw git spawn"),
            "{}",
            out[0]
        );
    }

    /// An allowlist entry naming a file that no longer exists would let the
    /// gate pass vacuously for that path.
    fn without(path: &str) -> Vec<(&'static str, usize)> {
        baseline().into_iter().filter(|(p, _)| *p != path).collect()
    }

    #[test]
    fn a_stale_allowlist_entry_fails() {
        let out = violations_from(&obs(&without("git-fixture/src/lib.rs")));
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].starts_with("git-fixture/src/lib.rs: allowlisted for 6"));
    }

    #[test]
    fn a_count_that_fell_to_zero_fails() {
        let mut b = baseline();
        let entry = b
            .iter_mut()
            .find(|(p, _)| *p == "xtask/src/file_set.rs")
            .expect("xtask/src/file_set.rs is allowlisted");
        entry.1 = 0;
        let out = violations_from(&obs(&b));
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(
            out[0].contains("xtask/src/file_set.rs: 0 raw git spawn"),
            "{}",
            out[0]
        );
    }

    #[test]
    fn the_pattern_is_counted_per_occurrence_not_per_line() {
        let line = format!(
            "{} {}",
            concat!("Command", "::new(", "\"git\")"),
            concat!("StdCommand", "::new(", "\"git\")")
        );
        assert_eq!(count_pattern(&line), 2);
        assert_eq!(count_pattern("Command::new(\"gitleaks\")"), 0);
    }

    /// The collector half: every Rust file in the set is scanned at any depth,
    /// the extension filter holds, and the counts come from file bytes. A
    /// filter that quietly stopped matching nested paths would leave a guard
    /// that scans almost nothing and passes.
    #[test]
    fn the_collector_scans_every_rust_file_in_the_set() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path();
        let nested = root.join("crate-a").join("src");
        std::fs::create_dir_all(&nested).unwrap();

        let spawn = concat!("Command", "::new(", "\"git\")");
        std::fs::write(nested.join("deep.rs"), format!("fn f() {{ {spawn}; }}\n")).unwrap();
        std::fs::write(root.join("top.rs"), "fn g() {}\n").unwrap();
        // A decoy the extension filter must exclude even though it matches.
        std::fs::write(root.join("notes.txt"), spawn).unwrap();

        let files = ["crate-a/src/deep.rs", "top.rs", "notes.txt"].map(PathBuf::from);
        let mut observed = collect_counts(root, &files);
        observed.sort();
        assert_eq!(
            observed,
            vec![
                (PathBuf::from("crate-a/src/deep.rs"), 1),
                (PathBuf::from("top.rs"), 0),
            ]
        );
    }
}
