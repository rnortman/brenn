//! The scheme-classification invariant, mechanically.
//!
//! `resolved::scheme::split_spellable` is this crate's only entry into scheme
//! classification: it filters `pwa_push:`, and that filter is what makes the
//! `PwaPush` arms of the passes' matches unreachable. A pass calling
//! `ChannelScheme::split` directly would admit an address the language cannot
//! write, turning one of those `unreachable!` arms into a panic on operator
//! input. The invariant is stated in prose on the shim; prose cannot hold it.

use std::fs;
use std::path::{Path, PathBuf};

/// The crate's hand-written sources, declared runfiles of this suite.
fn src_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src")
}

fn rust_files(dir: &Path, found: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("the declared source tree") {
        let path = entry.expect("a readable directory entry").path();
        if path.is_dir() {
            rust_files(&path, found);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            found.push(path);
        }
    }
}

#[test]
fn no_pass_reaches_scheme_classification_directly() {
    let mut files = Vec::new();
    rust_files(&src_dir(), &mut files);
    assert!(
        files.len() > 5,
        "only {} sources found: the scan is looking in the wrong place",
        files.len()
    );

    let mut offenders = Vec::new();
    for path in files {
        let text = fs::read_to_string(&path).expect("a readable source file");
        for (number, line) in text.lines().enumerate() {
            // Comments name the call to say why nothing may make it.
            if line.trim_start().starts_with("//") {
                continue;
            }
            if line.contains("ChannelScheme::split") {
                offenders.push(format!("{}:{}", path.display(), number + 1));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "these sites classify a scheme without filtering `pwa_push:`; call \
         `resolved::scheme::split_spellable` instead: {offenders:?}"
    );
}
