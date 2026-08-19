//! Shared fixture loading for the corpus suites.

// Each suite uses a subset of these.
#![allow(dead_code)]

use std::path::PathBuf;

use brenn_dsl::diag::Diagnostic;
use brenn_dsl::model::File;
use brenn_dsl::resolved::ResolvedConfig;
use brenn_dsl::{parse_str, resolve_files};

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

// ── compiling a document in a test ───────────────────────────────────────────
//
// One contract for every resolver suite: a tree is spelled inline as
// `(module key, source)` pairs with `""` for the root, goes through the I/O-free
// core, and comes back as either the config or the messages it was refused with.

/// Resolve a tree of modules; the entry keyed `""` is the root.
pub fn compile_tree(modules: &[(&str, &str)]) -> Result<ResolvedConfig, Vec<Diagnostic>> {
    let files = modules
        .iter()
        .map(|(key, source)| {
            let filename = if key.is_empty() { "main" } else { key };
            let file = parse_str(source, &format!("{filename}.brenn"))
                .unwrap_or_else(|error| panic!("{error}"));
            ((*key).to_string(), file)
        })
        .collect();
    resolve_files(files, "").map(|output| output.config)
}

/// Resolve a one-file document, named as the root module.
pub fn compile(source: &str) -> Result<ResolvedConfig, Vec<Diagnostic>> {
    compile_tree(&[("", source)])
}

/// The config a tree that is expected to resolve produces.
pub fn resolved_tree(modules: &[(&str, &str)]) -> ResolvedConfig {
    compile_tree(modules).unwrap_or_else(|errors| {
        panic!(
            "{:?}",
            errors
                .iter()
                .map(|error| error.message.as_str())
                .collect::<Vec<_>>()
        )
    })
}

/// The config a one-file document that is expected to resolve produces.
pub fn resolved(source: &str) -> ResolvedConfig {
    resolved_tree(&[("", source)])
}

/// The messages a tree is refused with.
pub fn refusals_tree(modules: &[(&str, &str)]) -> Vec<String> {
    match compile_tree(modules) {
        Ok(_) => panic!("expected a refusal"),
        Err(errors) => errors.into_iter().map(|error| error.message).collect(),
    }
}

/// The messages a one-file document is refused with.
pub fn refusals(source: &str) -> Vec<String> {
    refusals_tree(&[("", source)])
}

/// The one message a tree is refused with.
pub fn refusal_tree(modules: &[(&str, &str)]) -> String {
    let mut messages = refusals_tree(modules);
    assert_eq!(messages.len(), 1, "{messages:?}");
    messages.pop().expect("one message")
}

/// The one message a one-file document is refused with.
pub fn refusal(source: &str) -> String {
    refusal_tree(&[("", source)])
}
