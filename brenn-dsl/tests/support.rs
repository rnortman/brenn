//! Shared fixture loading for the corpus suites.

// Each suite uses a subset of these.
#![allow(dead_code)]

use std::path::PathBuf;

use brenn_dsl::model::File;
use brenn_dsl::parse_str;

/// The directory the corpus fixtures live in.
///
/// `CARGO_MANIFEST_DIR` is workspace-relative here and a test starts in the
/// runfiles root, so this resolves as long as the fixtures are declared
/// runfiles of the target.
pub fn corpus_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("corpus")
}

/// The text of one corpus fixture.
pub fn corpus_text(name: &str) -> String {
    let path = corpus_dir().join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

/// One corpus fixture, parsed. A fixture that does not parse is a broken test
/// input, so it panics rather than returning an error the caller might absorb.
pub fn corpus_file(name: &str) -> File {
    parse_str(&corpus_text(name), name).unwrap_or_else(|error| panic!("{error}"))
}
