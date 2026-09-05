//! What a compile read, and the one hash that names the whole of it.
//!
//! A document is a tree of files: the root, the tree modules it reaches by
//! `use`, and the packaged modules it reaches by `use @<name>::…`. Each file's
//! own bytes are hashed where it is parsed ([`crate::source_sha256`]); this
//! module is the level above — the ordered list of those hashes, and the single
//! identity derived from it.

use std::path::PathBuf;

/// One file a compile read, and the hash of the bytes it read.
///
/// `path` is the file's place *within* the document rather than on the machine:
/// the root's basename, a tree module's path under the root's directory, and a
/// packaged module's name under whichever module root held it, sigil kept. Two
/// checkouts of one deployment therefore describe their files identically, and
/// naming module roots in a different order describes them identically too.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceFile {
    pub path: PathBuf,
    pub source_sha256: String,
}

/// The identity of a whole document: SHA-256 over every file's place and hash,
/// in the order the compile read them.
///
/// One line per file, `<path>\0<sha256>\n`, so neither half of an entry can run
/// into the next. Equal hashes mean the same bytes were read from the same
/// places in the same order.
pub fn document_sha256(files: &[SourceFile]) -> String {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    for file in files {
        hasher.update(file.path.to_string_lossy().as_bytes());
        hasher.update(b"\0");
        hasher.update(file.source_sha256.as_bytes());
        hasher.update(b"\n");
    }
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str, hash: &str) -> SourceFile {
        SourceFile {
            path: PathBuf::from(path),
            source_sha256: hash.to_string(),
        }
    }

    #[test]
    fn the_hash_is_over_places_and_hashes_in_order() {
        let one = vec![file("main.brenn", "aa"), file("@widget.brenn", "bb")];
        let swapped = vec![file("@widget.brenn", "bb"), file("main.brenn", "aa")];
        // A golden hash, so the framing itself — `<place>\0<sha256>\n` per
        // file, nothing between the lines — cannot drift unobserved.
        assert_eq!(
            document_sha256(&one),
            "21d3d551ef84a57b18bb8f618450e6258c97f66d37787ad70c276fe40280ff4e"
        );
        assert_ne!(document_sha256(&one), document_sha256(&swapped));
    }

    #[test]
    fn a_field_boundary_cannot_be_forged() {
        // Without the separators, `("ab", "c")` and `("a", "bc")` would hash
        // equal, and a renamed module could impersonate an edited one.
        let split_one = vec![file("ab", "c")];
        let split_two = vec![file("a", "bc")];
        assert_ne!(document_sha256(&split_one), document_sha256(&split_two));
    }

    #[test]
    fn no_files_is_a_hash_of_its_own() {
        assert_eq!(
            document_sha256(&[]),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }
}
