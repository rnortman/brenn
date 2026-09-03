//! A list of install roots, scanned for what makes it not a set of distinct
//! directories holding distinct names.
//!
//! The module roots and the components roots are both "one directory per
//! installed release" lists, and both are refused for the same three shapes: a
//! root that cannot be listed, one directory named twice, one name present
//! under two roots. What counts as an entry differs — a `*.brenn` file, a
//! package directory — and so does what the caller does with a fault, so the
//! scan is generic over the first and hands back the second as data.

use std::collections::BTreeMap;
use std::fs::DirEntry;
use std::path::{Path, PathBuf};

/// One thing wrong with a root list.
#[derive(Debug)]
pub enum RootListFault<'a> {
    /// A root that cannot be listed: absent, a plain file, or unreadable.
    Unreadable {
        root: &'a Path,
        error: std::io::Error,
    },
    /// Two entries in the list that canonicalize to one directory.
    SameDirectory { first: &'a Path, second: &'a Path },
    /// One name held by more than one root, in list order.
    Duplicate { name: String, roots: Vec<&'a Path> },
}

impl RootListFault<'_> {
    /// The fault as a sentence, with `flag` the option that named the roots
    /// (`--modules`) and `thing` what an entry is (`packaged module`).
    pub fn describe(&self, flag: &str, thing: &str) -> String {
        match self {
            Self::Unreadable { root, error } => {
                format!(
                    "{flag} {}: not a readable directory: {error}",
                    root.display()
                )
            }
            Self::SameDirectory { first, second } => format!(
                "{flag} {} and {flag} {} name the same directory: every {flag} root is a \
                 distinct release's",
                first.display(),
                second.display()
            ),
            Self::Duplicate { name, roots } => format!(
                "{thing} `{name}` is installed under more than one {flag} root: {}. It ships \
                 with exactly one release; two copies mean a stale install or two bundles \
                 claiming one name. Remove or rename one",
                display_list(roots)
            ),
        }
    }
}

/// Scan `roots` for the faults above.
///
/// `is_entry` says what an entry is named, or `None` for a directory entry that
/// is not one. Roots are compared after canonicalization, so `a` and `a/` are
/// one directory and refused as such rather than as a duplicate of everything
/// in them. Unreadable and same-directory faults come first, in list order; the
/// duplicates follow, sorted by name. A root that can be listed but not
/// canonicalized, or whose listing fails midway, is not an operator's mistake
/// and panics naming `flag`.
pub fn scan_roots<'a>(
    flag: &str,
    roots: &'a [PathBuf],
    is_entry: impl Fn(&DirEntry) -> Option<String>,
) -> Vec<RootListFault<'a>> {
    scan_roots_in(flag, roots, None, is_entry).0
}

/// [`scan_roots`], with the entries read from `subdir` under each root and the
/// name → holders map handed back.
///
/// `subdir` is for a root whose entries are one level in: a surface root holds
/// its kinds under `processor/`, and the root itself carries other installed
/// files beside that directory. A root without the subdirectory contributes no
/// entries and is not a fault — a release may ship a root and no entry of this
/// kind — but the root itself must still be a listable directory.
///
/// The map holds every name, single-held ones included, so a caller that needs
/// "which root owns this name" builds it from the same walk that refused the
/// collisions.
pub fn scan_roots_in<'a>(
    flag: &str,
    roots: &'a [PathBuf],
    subdir: Option<&str>,
    is_entry: impl Fn(&DirEntry) -> Option<String>,
) -> (Vec<RootListFault<'a>>, BTreeMap<String, Vec<&'a Path>>) {
    let mut faults = Vec::new();
    let mut canonical: Vec<(PathBuf, &Path)> = Vec::new();
    let mut holders: BTreeMap<String, Vec<&Path>> = BTreeMap::new();
    for root in roots {
        if let Err(error) = std::fs::read_dir(root) {
            faults.push(RootListFault::Unreadable { root, error });
            continue;
        }
        let resolved = root.canonicalize().unwrap_or_else(|error| {
            panic!(
                "{flag} {}: listed but not canonicalizable: {error}",
                root.display()
            )
        });
        if let Some((_, first)) = canonical.iter().find(|(path, _)| *path == resolved) {
            faults.push(RootListFault::SameDirectory {
                first,
                second: root,
            });
            continue;
        }
        canonical.push((resolved, root));
        let listed = match subdir {
            None => root.clone(),
            Some(subdir) => {
                let inner = root.join(subdir);
                if !inner.is_dir() {
                    continue;
                }
                inner
            }
        };
        let entries = std::fs::read_dir(&listed).unwrap_or_else(|error| {
            panic!("{flag} {}: cannot be listed: {error}", listed.display())
        });
        for entry in entries {
            let entry = entry.unwrap_or_else(|error| {
                panic!(
                    "{flag} {}: listing failed midway: {error}",
                    listed.display()
                )
            });
            if let Some(name) = is_entry(&entry) {
                holders.entry(name).or_default().push(root);
            }
        }
    }
    faults.extend(
        holders
            .iter()
            .filter(|(_, roots)| roots.len() > 1)
            .map(|(name, roots)| RootListFault::Duplicate {
                name: name.clone(),
                roots: roots.clone(),
            }),
    );
    (faults, holders)
}

/// Paths joined with `, ` for a message.
pub fn display_list<P: AsRef<Path>>(paths: impl IntoIterator<Item = P>) -> String {
    paths
        .into_iter()
        .map(|path| path.as_ref().display().to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Entry names of plain files, whatever their extension.
    fn files(entry: &DirEntry) -> Option<String> {
        entry
            .path()
            .is_file()
            .then(|| entry.file_name().to_str().map(str::to_string))
            .flatten()
    }

    /// A fresh directory under the test's scratch space holding `files`, each
    /// empty. Named per test and per process, so tests sharing a process do not
    /// see one another's trees.
    fn root_with(test: &str, files: &[&str]) -> PathBuf {
        static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let scratch =
            std::env::var("TEST_TMPDIR").map_or_else(|_| std::env::temp_dir(), PathBuf::from);
        let dir = scratch.join(format!(
            "roots-{test}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        for file in files {
            std::fs::write(dir.join(file), b"").unwrap();
        }
        dir
    }

    #[test]
    fn disjoint_roots_have_no_faults() {
        let a = root_with("disjoint", &["x", "y"]);
        let b = root_with("disjoint", &["z"]);
        let roots = [a, b];
        assert!(scan_roots("--f", &roots, files).is_empty());
    }

    #[test]
    fn a_name_under_two_roots_is_a_duplicate_naming_both_in_list_order() {
        let a = root_with("dup", &["x", "y"]);
        let b = root_with("dup", &["y", "x"]);
        let roots = [a.clone(), b.clone()];
        let faults = scan_roots("--f", &roots, files);
        let described: Vec<String> = faults.iter().map(|f| f.describe("--f", "thing")).collect();
        assert_eq!(
            described,
            [
                format!(
                    "thing `x` is installed under more than one --f root: {}, {}. It ships with \
                     exactly one release; two copies mean a stale install or two bundles \
                     claiming one name. Remove or rename one",
                    a.display(),
                    b.display()
                ),
                format!(
                    "thing `y` is installed under more than one --f root: {}, {}. It ships with \
                     exactly one release; two copies mean a stale install or two bundles \
                     claiming one name. Remove or rename one",
                    a.display(),
                    b.display()
                ),
            ]
        );
    }

    #[test]
    fn the_same_directory_twice_is_one_fault_and_not_a_duplicate_of_its_contents() {
        let a = root_with("same", &["x"]);
        let mut trailing = a.as_os_str().to_os_string();
        trailing.push("/");
        let roots = [a.clone(), PathBuf::from(trailing.clone())];
        let faults = scan_roots("--f", &roots, files);
        assert_eq!(faults.len(), 1, "{faults:?}");
        assert_eq!(
            faults[0].describe("--f", "thing"),
            format!(
                "--f {} and --f {} name the same directory: every --f root is a distinct \
                 release's",
                a.display(),
                PathBuf::from(trailing).display()
            )
        );
    }

    #[test]
    fn an_unreadable_root_is_reported_and_the_others_still_scanned() {
        let a = root_with("unreadable", &["x"]);
        let b = root_with("unreadable", &["x"]);
        let missing = a.join("no-such-root");
        let roots = [a, missing.clone(), b];
        let faults = scan_roots("--f", &roots, files);
        assert_eq!(faults.len(), 2, "{faults:?}");
        assert!(
            faults[0].describe("--f", "thing").starts_with(&format!(
                "--f {}: not a readable directory: ",
                missing.display()
            )),
            "{faults:?}"
        );
        assert!(matches!(&faults[1], RootListFault::Duplicate { name, .. } if name == "x"));
    }

    #[test]
    fn a_root_without_the_subdirectory_contributes_no_entries_and_is_not_a_fault() {
        let a = root_with("subdir-absent", &[]);
        let b = root_with("subdir-absent", &[]);
        std::fs::create_dir(b.join("inner")).unwrap();
        std::fs::write(b.join("inner").join("x"), b"").unwrap();
        let roots = [a.clone(), b.clone()];
        let (faults, holders) = scan_roots_in("--f", &roots, Some("inner"), files);
        assert!(faults.is_empty(), "{faults:?}");
        assert_eq!(holders.len(), 1);
        assert_eq!(holders["x"], vec![b.as_path()]);
    }

    #[test]
    fn entries_under_the_subdirectory_of_two_roots_collide_and_name_the_roots() {
        let a = root_with("subdir-dup", &[]);
        let b = root_with("subdir-dup", &[]);
        for root in [&a, &b] {
            std::fs::create_dir(root.join("inner")).unwrap();
            std::fs::write(root.join("inner").join("x"), b"").unwrap();
        }
        let roots = [a.clone(), b.clone()];
        let (faults, holders) = scan_roots_in("--f", &roots, Some("inner"), files);
        assert_eq!(faults.len(), 1, "{faults:?}");
        assert!(
            matches!(&faults[0], RootListFault::Duplicate { name, roots }
                if name == "x" && *roots == vec![a.as_path(), b.as_path()]),
            "{faults:?}"
        );
        assert_eq!(holders["x"], vec![a.as_path(), b.as_path()]);
    }

    #[test]
    fn what_is_not_an_entry_is_not_counted() {
        let a = root_with("predicate", &["x"]);
        let b = root_with("predicate", &[]);
        std::fs::create_dir(b.join("x")).unwrap();
        let roots = [a, b];
        assert!(scan_roots("--f", &roots, files).is_empty());
    }
}
