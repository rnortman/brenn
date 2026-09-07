//! A list of install roots, scanned for what makes it not a set of distinct
//! directories holding distinct names.
//!
//! The module roots and the components roots are both "one directory per
//! installed release" lists, and both are refused for the same three shapes: a
//! root that cannot be listed, one directory named twice, one name present
//! under two roots. What counts as an entry differs — a `*.brenn` file, a
//! package directory — and so does what the caller does with a fault, so the
//! scan is generic over the first and hands back the second as data.
//!
//! A root list also carries how it was named, because that is what a refusal
//! has to tell the operator to go and fix: a flag on a workstation invocation,
//! or a mount a host was declared with.

use std::collections::BTreeMap;
use std::fs::DirEntry;
use std::ops::Deref;
use std::path::{Path, PathBuf};

/// How a root list was named, for the messages that name a root.
///
/// A host learns every root from its declared mounts, so a refusal there names
/// the mount and not a path the operator never wrote. The config tools keep the
/// flag form, where the flag is the remedy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RootSource {
    /// Roots named one per flag on the command line.
    Flag(String),
    /// Roots derived from the declared mounts: one `<mount>/<tree>` per mount
    /// offering `tree`, with the mount each root came from.
    Mounts {
        tree: String,
        of: BTreeMap<PathBuf, String>,
    },
}

impl RootSource {
    /// Roots named by `flag`, once per root.
    pub fn flag(flag: impl Into<String>) -> Self {
        Self::Flag(flag.into())
    }

    /// What one root is, as a message calls it: `--modules /srv/x`, or
    /// ``mount `brenn` (/srv/x)``.
    pub fn locate(&self, root: &Path) -> String {
        match self {
            Self::Flag(flag) => format!("{flag} {}", root.display()),
            Self::Mounts { of, .. } => match of.get(root) {
                Some(mount) => format!("mount `{mount}` ({})", root.display()),
                None => root.display().to_string(),
            },
        }
    }

    /// What one root is called where a list of them reads better as names:
    /// the path under a flag, the mount name under mounts.
    pub fn name(&self, root: &Path) -> String {
        match self {
            Self::Flag(_) => root.display().to_string(),
            Self::Mounts { of, .. } => match of.get(root) {
                Some(mount) => mount.clone(),
                None => root.display().to_string(),
            },
        }
    }

    /// The unit a root is one of: `--modules root`, or `mount`.
    pub fn unit(&self) -> String {
        match self {
            Self::Flag(flag) => format!("{flag} root"),
            Self::Mounts { .. } => "mount".to_string(),
        }
    }

    /// The list as a whole, as a message calls it: `the --modules roots`, or
    /// ``the declared mounts' `surface/` trees``.
    pub fn all(&self) -> String {
        match self {
            Self::Flag(flag) => format!("the {flag} roots"),
            Self::Mounts { tree, .. } => format!("the declared mounts' `{tree}/` trees"),
        }
    }

    /// `root`s joined with `, `, each as [`RootSource::name`] gives it.
    pub fn names<'a>(&self, roots: impl IntoIterator<Item = &'a &'a Path>) -> String {
        roots
            .into_iter()
            .map(|root| self.name(root))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// A root list and how it was named.
///
/// One type for all three lists — modules, components, surface — because the
/// scan and every refusal it produces are the same for each. Dereferences to
/// the paths, so a caller that only resolves against them says nothing about
/// where they came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootList {
    paths: Vec<PathBuf>,
    source: RootSource,
}

impl RootList {
    /// A list named one root per `flag`.
    pub fn flag(flag: impl Into<String>, paths: Vec<PathBuf>) -> Self {
        Self {
            paths,
            source: RootSource::flag(flag),
        }
    }

    /// A list derived from the declared mounts: `mounts` is the mount name and
    /// the `<mount>/<tree>` path it contributes, in declaration order.
    pub fn mounts(tree: impl Into<String>, mounts: Vec<(String, PathBuf)>) -> Self {
        let tree = tree.into();
        let paths = mounts.iter().map(|(_, path)| path.clone()).collect();
        let of = mounts
            .into_iter()
            .map(|(mount, path)| (path, mount))
            .collect();
        Self {
            paths,
            source: RootSource::Mounts { tree, of },
        }
    }

    /// How this list was named.
    pub fn source(&self) -> &RootSource {
        &self.source
    }

    /// The paths, for a caller handing them on as a plain list.
    pub fn paths(&self) -> &[PathBuf] {
        &self.paths
    }
}

impl Default for RootList {
    /// No roots, named the workstation way: what a document compiled with no
    /// packaged imports is checked against.
    fn default() -> Self {
        Self::flag(MODULES_FLAG, Vec::new())
    }
}

impl Deref for RootList {
    type Target = [PathBuf];

    fn deref(&self) -> &[PathBuf] {
        &self.paths
    }
}

impl From<Vec<PathBuf>> for RootList {
    /// The workstation form: a list of module roots as `--modules` named them.
    fn from(paths: Vec<PathBuf>) -> Self {
        Self::flag(MODULES_FLAG, paths)
    }
}

/// The option that names the module roots on a workstation invocation, as
/// diagnostics spell it.
pub const MODULES_FLAG: &str = "--modules";

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
    /// The fault as a sentence, with `source` how the roots were named and
    /// `thing` what an entry is (`packaged module`).
    pub fn describe(&self, source: &RootSource, thing: &str) -> String {
        match self {
            Self::Unreadable { root, error } => {
                format!("{}: not a readable directory: {error}", source.locate(root))
            }
            Self::SameDirectory { first, second } => format!(
                "{} and {} name the same directory: every {} is a distinct release's",
                source.locate(first),
                source.locate(second),
                source.unit(),
            ),
            Self::Duplicate { name, roots } => format!(
                "{thing} `{name}` is installed under more than one {}: {}. It ships \
                 with exactly one release; two copies mean a stale install or two bundles \
                 claiming one name. Remove or rename one",
                source.unit(),
                source.names(roots.iter()),
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
/// and panics naming the root.
pub fn scan_roots<'a>(
    roots: &'a RootList,
    is_entry: impl Fn(&DirEntry) -> Option<String>,
) -> Vec<RootListFault<'a>> {
    scan_roots_in(roots, None, is_entry).0
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
    roots: &'a RootList,
    subdir: Option<&str>,
    is_entry: impl Fn(&DirEntry) -> Option<String>,
) -> (Vec<RootListFault<'a>>, BTreeMap<String, Vec<&'a Path>>) {
    let mut faults = Vec::new();
    let mut canonical: Vec<(PathBuf, &Path)> = Vec::new();
    let mut holders: BTreeMap<String, Vec<&Path>> = BTreeMap::new();
    let source = roots.source();
    for root in roots.iter() {
        if let Err(error) = std::fs::read_dir(root) {
            faults.push(RootListFault::Unreadable { root, error });
            continue;
        }
        let resolved = root.canonicalize().unwrap_or_else(|error| {
            panic!(
                "{}: listed but not canonicalizable: {error}",
                source.locate(root)
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
            panic!(
                "{}: cannot be listed ({}): {error}",
                source.locate(root),
                listed.display()
            )
        });
        for entry in entries {
            let entry = entry.unwrap_or_else(|error| {
                panic!(
                    "{}: listing failed midway ({}): {error}",
                    source.locate(root),
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
        let roots = RootList::flag("--f", vec![a, b]);
        assert!(scan_roots(&roots, files).is_empty());
    }

    #[test]
    fn a_name_under_two_roots_is_a_duplicate_naming_both_in_list_order() {
        let a = root_with("dup", &["x", "y"]);
        let b = root_with("dup", &["y", "x"]);
        let roots = RootList::flag("--f", vec![a.clone(), b.clone()]);
        let faults = scan_roots(&roots, files);
        let described: Vec<String> = faults
            .iter()
            .map(|f| f.describe(roots.source(), "thing"))
            .collect();
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
    fn a_duplicate_under_mounts_names_the_mounts_and_not_a_flag() {
        let a = root_with("mount-dup", &["x"]);
        let b = root_with("mount-dup", &["x"]);
        let roots = RootList::mounts(
            "modules",
            vec![
                ("brenn".to_string(), a.clone()),
                ("demo".to_string(), b.clone()),
            ],
        );
        let faults = scan_roots(&roots, files);
        assert_eq!(faults.len(), 1, "{faults:?}");
        assert_eq!(
            faults[0].describe(roots.source(), "packaged module"),
            "packaged module `x` is installed under more than one mount: brenn, demo. It \
             ships with exactly one release; two copies mean a stale install or two bundles \
             claiming one name. Remove or rename one"
        );
    }

    #[test]
    fn an_unreadable_root_under_mounts_names_the_mount_and_the_path() {
        let a = root_with("mount-unreadable", &[]);
        let missing = a.join("no-such-tree");
        let roots = RootList::mounts("components", vec![("brenn".to_string(), missing.clone())]);
        let faults = scan_roots(&roots, files);
        assert_eq!(faults.len(), 1, "{faults:?}");
        assert!(
            faults[0]
                .describe(roots.source(), "component package")
                .starts_with(&format!(
                    "mount `brenn` ({}): not a readable directory: ",
                    missing.display()
                )),
            "{faults:?}"
        );
    }

    #[test]
    fn the_same_directory_twice_is_one_fault_and_not_a_duplicate_of_its_contents() {
        let a = root_with("same", &["x"]);
        let mut trailing = a.as_os_str().to_os_string();
        trailing.push("/");
        let roots = RootList::flag("--f", vec![a.clone(), PathBuf::from(trailing.clone())]);
        let faults = scan_roots(&roots, files);
        assert_eq!(faults.len(), 1, "{faults:?}");
        assert_eq!(
            faults[0].describe(roots.source(), "thing"),
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
        let roots = RootList::flag("--f", vec![a, missing.clone(), b]);
        let faults = scan_roots(&roots, files);
        assert_eq!(faults.len(), 2, "{faults:?}");
        assert!(
            faults[0]
                .describe(roots.source(), "thing")
                .starts_with(&format!(
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
        let roots = RootList::flag("--f", vec![a.clone(), b.clone()]);
        let (faults, holders) = scan_roots_in(&roots, Some("inner"), files);
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
        let roots = RootList::flag("--f", vec![a.clone(), b.clone()]);
        let (faults, holders) = scan_roots_in(&roots, Some("inner"), files);
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
        let roots = RootList::flag("--f", vec![a, b]);
        assert!(scan_roots(&roots, files).is_empty());
    }
}
