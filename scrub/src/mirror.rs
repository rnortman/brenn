//! Staging scan input in a temp directory at repo-relative paths.
//!
//! gitleaks has no files-from input, so scoping a scan to a chosen set of
//! files means mirroring them into a directory and scanning that. Paths are
//! preserved rather than flattened: extensions drive path allowlists,
//! same-basename files must not collide, and directory-shaped allowlists stay
//! correct.

use std::path::{Component, Path, PathBuf};

pub struct Mirror {
    dir: tempfile::TempDir,
}

impl Mirror {
    pub fn new() -> Mirror {
        Mirror {
            dir: tempfile::Builder::new()
                .prefix("brenn-scrub-mirror")
                .tempdir()
                .expect("cannot create mirror temp dir"),
        }
    }

    pub fn root(&self) -> &Path {
        self.dir.path()
    }

    /// Resolve a repo-relative path inside the mirror, refusing anything that
    /// would escape it.
    fn target(&self, rel: &Path) -> PathBuf {
        assert!(
            rel.is_relative(),
            "mirror path must be relative: {}",
            rel.display()
        );
        assert!(
            !rel.components().any(|c| matches!(c, Component::ParentDir)),
            "mirror path must not contain '..': {}",
            rel.display()
        );
        let target = self.dir.path().join(rel);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)
                .unwrap_or_else(|e| panic!("cannot create {}: {e}", parent.display()));
        }
        target
    }

    pub fn write(&self, rel: &Path, content: &[u8]) {
        let target = self.target(rel);
        std::fs::write(&target, content)
            .unwrap_or_else(|e| panic!("cannot write {}: {e}", target.display()));
    }

    /// Hardlink when the filesystem allows it, copy otherwise. Content is
    /// identical either way; the link is purely to keep large trees cheap.
    pub fn link_or_copy(&self, rel: &Path, src: &Path) {
        let target = self.target(rel);
        if std::fs::hard_link(src, &target).is_ok() {
            return;
        }
        std::fs::copy(src, &target).unwrap_or_else(|e| {
            panic!("cannot copy {} to {}: {e}", src.display(), target.display())
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_basename_files_do_not_collide() {
        let m = Mirror::new();
        m.write(Path::new("brenn/src/mod.rs"), b"alpha");
        m.write(Path::new("surface/kernel/src/mod.rs"), b"beta");

        assert_eq!(
            std::fs::read(m.root().join("brenn/src/mod.rs")).unwrap(),
            b"alpha"
        );
        assert_eq!(
            std::fs::read(m.root().join("surface/kernel/src/mod.rs")).unwrap(),
            b"beta"
        );
    }

    #[test]
    fn extension_is_preserved_so_path_allowlists_apply() {
        let m = Mirror::new();
        m.write(Path::new("docs/notes.md"), b"x");
        m.write(Path::new("src/a.rs"), b"x");
        assert!(m.root().join("docs/notes.md").exists());
        assert!(m.root().join("src/a.rs").exists());
    }

    #[test]
    #[should_panic(expected = "must be relative")]
    fn absolute_mirror_path_panics() {
        Mirror::new().write(Path::new("/etc/passwd"), b"x");
    }

    #[test]
    #[should_panic(expected = "must not contain '..'")]
    fn parent_dir_escape_panics() {
        Mirror::new().write(Path::new("a/../../outside.rs"), b"x");
    }

    #[test]
    fn link_or_copy_reproduces_content() {
        let src_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("orig.rs");
        std::fs::write(&src, b"contents").unwrap();

        let m = Mirror::new();
        m.link_or_copy(Path::new("nested/orig.rs"), &src);
        assert_eq!(
            std::fs::read(m.root().join("nested/orig.rs")).unwrap(),
            b"contents"
        );
    }
}
