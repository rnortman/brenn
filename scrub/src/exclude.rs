//! Invocation-scoped path exclusion for tree scans.
//!
//! Exclusion drops files before they are mirrored, so an excluded path is
//! never scanned and never reported. It is deliberately not expressible in the
//! shared rule config: a `paths` allowlist there would apply to every consumer
//! of that config, including the layers whose whole job is to see everything.

use std::path::{Component, Path, PathBuf};

/// Repo-relative prefixes to skip, in the order given on the command line.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Exclusions {
    prefixes: Vec<PathBuf>,
}

impl Exclusions {
    pub fn new(prefixes: Vec<PathBuf>) -> Exclusions {
        Exclusions { prefixes }
    }

    /// Prefixes as given, for reporting.
    pub fn as_strings(&self) -> Vec<String> {
        self.prefixes
            .iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect()
    }

    /// Split `tracked` into kept and per-prefix excluded counts.
    ///
    /// A prefix matching nothing is fatal: a silently inert exclusion reads as
    /// a narrower scan than it is, and this is the command a green tree is
    /// declared on.
    ///
    /// A file counts against *every* prefix covering it, not just the first.
    /// Attributing it to one would leave overlapping prefixes
    /// (`docs/adr` plus `docs/adr/2026`, or a duplicate) at zero and trip the
    /// zero-match check on a scan whose scope is perfectly correct.
    pub fn partition(&self, tracked: Vec<PathBuf>) -> (Vec<PathBuf>, Vec<(String, usize)>) {
        let mut counts = vec![0usize; self.prefixes.len()];
        let mut kept = Vec::new();
        for rel in tracked {
            let mut excluded = false;
            for (i, prefix) in self.prefixes.iter().enumerate() {
                if under(&rel, prefix) {
                    counts[i] += 1;
                    excluded = true;
                }
            }
            if !excluded {
                kept.push(rel);
            }
        }
        let mut report = Vec::new();
        for (prefix, count) in self.prefixes.iter().zip(counts) {
            assert!(
                count > 0,
                "--exclude {} matched no tracked files in scope; \
                 an exclusion that skips nothing is a typo, not a scan scope",
                prefix.display()
            );
            report.push((prefix.to_string_lossy().into_owned(), count));
        }
        (kept, report)
    }
}

/// Whether `rel` lies under `prefix`. Matching is on whole path components, so
/// `docs/adr` covers `docs/adr/**` and the file `docs/adr` itself, but not
/// `docs/adr-notes.md`.
fn under(rel: &Path, prefix: &Path) -> bool {
    let mut r = rel.components();
    for p in prefix.components() {
        if !matches!(p, Component::Normal(_)) {
            return false;
        }
        if r.next() != Some(p) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ex(prefixes: &[&str]) -> Exclusions {
        Exclusions::new(prefixes.iter().map(PathBuf::from).collect())
    }

    fn tracked(paths: &[&str]) -> Vec<PathBuf> {
        paths.iter().map(PathBuf::from).collect()
    }

    #[test]
    fn matching_is_on_component_boundaries() {
        let p = Path::new("docs/adr");
        assert!(under(Path::new("docs/adr/2026/07/x.md"), p));
        assert!(under(Path::new("docs/adr"), p));
        assert!(!under(Path::new("docs/adr-notes.md"), p));
        assert!(!under(Path::new("other/docs/adr/x.md"), p));
    }

    #[test]
    fn a_single_file_prefix_excludes_just_that_file() {
        let p = Path::new("brenn-prod.toml");
        assert!(under(Path::new("brenn-prod.toml"), p));
        assert!(!under(Path::new("brenn-prod.toml.bak"), p));
    }

    #[test]
    fn partition_keeps_the_rest_and_counts_each_prefix() {
        let e = ex(&["docs/adr", "brenn-prod.toml"]);
        let (kept, report) = e.partition(tracked(&[
            "src/a.rs",
            "docs/adr/one.md",
            "docs/adr/two.md",
            "brenn-prod.toml",
            "docs/adr-notes.md",
        ]));
        assert_eq!(
            kept,
            tracked(&["src/a.rs", "docs/adr-notes.md"]),
            "only files under an excluded prefix are dropped"
        );
        assert_eq!(
            report,
            vec![
                ("docs/adr".to_string(), 2),
                ("brenn-prod.toml".to_string(), 1)
            ]
        );
    }

    #[test]
    fn no_exclusions_keeps_everything() {
        let e = Exclusions::default();
        assert!(e.as_strings().is_empty());
        let (kept, report) = e.partition(tracked(&["a.rs", "b.rs"]));
        assert_eq!(kept, tracked(&["a.rs", "b.rs"]));
        assert!(report.is_empty());
    }

    /// A file counts against every covering prefix. Attributing it only to the
    /// first left the narrower one at zero, so a correct scope died with a
    /// "typo" diagnosis it did not earn.
    #[test]
    fn nested_prefixes_are_both_credited_rather_than_read_as_a_typo() {
        let e = ex(&["docs/adr", "docs/adr/2026"]);
        let (kept, report) = e.partition(tracked(&[
            "src/a.rs",
            "docs/adr/2026/one.md",
            "docs/adr/2025/two.md",
        ]));
        assert_eq!(kept, tracked(&["src/a.rs"]));
        assert_eq!(
            report,
            vec![
                ("docs/adr".to_string(), 2),
                ("docs/adr/2026".to_string(), 1)
            ]
        );
    }

    /// A double-pasted flag is a shell-history accident, not a scope error.
    #[test]
    fn a_duplicated_prefix_is_not_fatal() {
        let e = ex(&["docs/adr", "docs/adr"]);
        let (kept, report) = e.partition(tracked(&["src/a.rs", "docs/adr/one.md"]));
        assert_eq!(kept, tracked(&["src/a.rs"]));
        assert_eq!(
            report.iter().map(|(_, c)| *c).collect::<Vec<_>>(),
            vec![1, 1]
        );
    }

    #[test]
    #[should_panic(expected = "matched no tracked files in scope")]
    fn a_prefix_matching_nothing_is_fatal() {
        ex(&["docs/adr", "typo/path"]).partition(tracked(&["docs/adr/one.md"]));
    }

    /// The zero-match check is judged against the scoped set, so an exclusion
    /// that is inert within a positional scope is fatal even though it would
    /// match elsewhere in the repo.
    #[test]
    #[should_panic(expected = "matched no tracked files in scope")]
    fn inert_within_a_narrower_scope_is_still_fatal() {
        ex(&["docs/adr"]).partition(tracked(&["src/a.rs"]));
    }
}
