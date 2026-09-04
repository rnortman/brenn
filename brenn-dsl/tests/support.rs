//! Shared fixture loading for the corpus suites.

// Each suite uses a subset of these; the macro re-export is unused wherever the
// suite writes no packaged fence.
#![allow(dead_code, unused_imports)]

use std::path::PathBuf;

use brenn_dsl::derived::DerivedConfig;
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

// ── channel fixtures ─────────────────────────────────────────────────────────
//
// One spelling of a well-formed channel block for every suite that needs one, so
// a presence rule added to the model is answered in one place rather than in
// whichever copy the author remembered.

/// A disk-backed declaration with the three depths every one of them states.
pub fn durable(handle: &str, address: &str) -> String {
    format!(
        "channel {handle} at \"{address}\" {{\n    push_depth = 4;\n    \
         retain_depth = 16;\n    standing_retain_depth = 64;\n}}\n"
    )
}

/// A declaration on a scheme whose retention is its retained window alone.
pub fn nondurable(handle: &str, address: &str) -> String {
    format!(
        "channel {handle} at \"{address}\" {{\n    push_depth = 4;\n    retain_depth = 16;\n}}\n"
    )
}

/// A tuning block with the three depths a system-minted family's block states.
pub fn tuning(address: &str) -> String {
    format!(
        "channel at \"{address}\" {{\n    push_depth = 4;\n    retain_depth = 16;\n    \
         standing_retain_depth = 64;\n}}\n"
    )
}

/// A tuning block over a family rather than one address.
pub fn prefix_tuning(address: &str) -> String {
    format!(
        "channel at prefix \"{address}\" {{\n    push_depth = 4;\n    retain_depth = 16;\n    \
         standing_retain_depth = 64;\n}}\n"
    )
}

// ── what a stage produced, or what it refused ────────────────────────────────
//
// Stage-agnostic: a resolve and a derive both hand back either the model or the
// diagnostics, so the "one message" discipline and the panic formatting are
// written once and every wrapper below is one call.

/// What a stage produced, with its refusals as the panic message.
fn value_of<T>(result: Result<T, Vec<Diagnostic>>) -> T {
    result.unwrap_or_else(|errors| panic!("{:?}", messages(&errors)))
}

/// The messages a stage refused a document with.
fn refusals_of<T>(result: Result<T, Vec<Diagnostic>>) -> Vec<String> {
    match result {
        Ok(_) => panic!("expected a refusal"),
        Err(errors) => errors.into_iter().map(|error| error.message).collect(),
    }
}

/// The one message a stage refused a document with.
fn refusal_of<T>(result: Result<T, Vec<Diagnostic>>) -> String {
    let mut messages = refusals_of(result);
    assert_eq!(messages.len(), 1, "{messages:?}");
    messages.pop().expect("one message")
}

// ── compiling a document in a test ───────────────────────────────────────────
//
// One contract for every resolver suite: a tree is spelled inline as
// `(module key, source)` pairs with `""` for the root, goes through the I/O-free
// core, and comes back as either the config or the messages it was refused with.

/// The fence a fixture writes around the class declarations that have to live
/// in a packaged module.
///
/// A top-level instance's class is declared in an installed component package,
/// and most suites here are about authority or expansion rather than about
/// module structure. The line is written twice, opening and closing; what sits
/// between the two becomes the packaged module every fixture in the file
/// shares, and the document keeps the import in its place.
///
/// The substitution is line-for-line in both halves —
/// blank lines stand in for the text that moved — so every span a suite asserts
/// reads the same line and column it reads in the fixture as written; [`at`] is
/// what knows about the one line the document gains.
macro_rules! packaged {
    () => {
        brenn_dsl::packaged_fence!()
    };
}
pub(crate) use packaged;

/// The fence as a value, for the fixtures that build their text with `format!`.
pub const PACKAGED: &str = packaged!();

/// The module key the fenced half is loaded under.
const PACKAGED_KEY: &str = concat!("@", brenn_dsl::packaged_module!());

/// Resolve a tree of modules; the entry keyed `""` is the root.
pub fn compile_tree(modules: &[(&str, &str)]) -> Result<ResolvedConfig, Vec<Diagnostic>> {
    let mut files: Vec<(String, File)> = Vec::new();
    for (key, source) in modules {
        let filename = if key.is_empty() { "main" } else { key };
        match brenn_dsl::fixture_text::split_packaged(source) {
            Some((module, document)) => {
                assert!(
                    key.is_empty(),
                    "only the root fixture splits into a packaged module"
                );
                files.push((
                    PACKAGED_KEY.to_string(),
                    parsed(&module, &format!("{PACKAGED_KEY}.brenn")),
                ));
                files.push(((*key).to_string(), parsed(&document, "main.brenn")));
            }
            None => files.push((
                (*key).to_string(),
                parsed(source, &format!("{filename}.brenn")),
            )),
        }
    }
    resolve_files(files, "")
}

/// One fixture source, parsed. A fixture that does not parse is a broken test
/// input.
fn parsed(source: &str, name: &str) -> File {
    parse_str(source, name).unwrap_or_else(|error| panic!("{error}"))
}

/// Resolve a one-file document, named as the root module.
pub fn compile(source: &str) -> Result<ResolvedConfig, Vec<Diagnostic>> {
    compile_tree(&[("", source)])
}

/// The config a tree that is expected to resolve produces.
pub fn resolved_tree(modules: &[(&str, &str)]) -> ResolvedConfig {
    value_of(compile_tree(modules))
}

/// The config a one-file document that is expected to resolve produces.
pub fn resolved(source: &str) -> ResolvedConfig {
    resolved_tree(&[("", source)])
}

/// The messages a tree is refused with.
pub fn refusals_tree(modules: &[(&str, &str)]) -> Vec<String> {
    refusals_of(compile_tree(modules))
}

/// The messages a one-file document is refused with.
pub fn refusals(source: &str) -> Vec<String> {
    refusals_tree(&[("", source)])
}

/// The one message a tree is refused with.
pub fn refusal_tree(modules: &[(&str, &str)]) -> String {
    refusal_of(compile_tree(modules))
}

/// The one message a one-file document is refused with.
pub fn refusal(source: &str) -> String {
    refusal_tree(&[("", source)])
}

// ── deriving a document in a test ────────────────────────────────────────────
//
// The same contract one pass further on: a tree that resolves goes through
// derivation, and a suite asserts either what came out or the messages it was
// refused with. A resolve refusal reaches these too — a document that does not
// resolve has nothing to derive, and a suite here asserting a derivation message
// would report the resolve one instead of quietly passing.

/// Derive a tree of modules; the entry keyed `""` is the root.
pub fn derive_tree(modules: &[(&str, &str)]) -> Result<DerivedConfig, Vec<Diagnostic>> {
    compile_tree(modules).and_then(brenn_dsl::derive::derive)
}

/// The derived config a tree that is expected to compile produces.
pub fn derived_tree(modules: &[(&str, &str)]) -> DerivedConfig {
    value_of(derive_tree(modules))
}

/// The derived config a one-file document that is expected to compile produces.
pub fn derived(source: &str) -> DerivedConfig {
    derived_tree(&[("", source)])
}

/// The messages a tree is refused with in derivation.
pub fn derive_refusals_tree(modules: &[(&str, &str)]) -> Vec<String> {
    refusals_of(derive_tree(modules))
}

/// The messages a one-file document is refused with in derivation.
pub fn derive_refusals(source: &str) -> Vec<String> {
    derive_refusals_tree(&[("", source)])
}

/// The one message a one-file document is refused with in derivation.
pub fn derive_refusal(source: &str) -> String {
    refusal_of(derive_tree(&[("", source)]))
}

/// The one-based line and column of `needle` in `source`.
///
/// A span assertion reads as "the kind word" rather than as a pair of numbers,
/// and a fixture that grows a line does not have to be recounted.
pub fn at(source: &str, needle: &str) -> Option<(i64, i64)> {
    let index = source.find(needle).expect("the token is in the fixture");
    let line = source[..index].matches('\n').count() + 1;
    let column = index - source[..index].rfind('\n').map_or(0, |start| start + 1) + 1;
    // The document half carries the import as an extra first line; the packaged
    // half does not, so a token inside a fence keeps the fixture's numbering.
    let shift = usize::from(source.contains(PACKAGED) && !packaged_at(source, index));
    Some(((line + shift) as i64, column as i64))
}

/// Is the byte at `index` inside one of the fixture's fenced regions?
fn packaged_at(source: &str, index: usize) -> bool {
    let mut offset = 0;
    for (line, half) in brenn_dsl::fixture_text::fenced(source) {
        if (offset..offset + line.len()).contains(&index) {
            return half.unwrap_or(false);
        }
        offset += line.len();
    }
    false
}

/// The diagnostics a one-file document is refused with in resolution, whole —
/// for the assertions that read a span or a `related` list rather than a
/// message.
pub fn resolve_errors(source: &str) -> Vec<Diagnostic> {
    compile(source).expect_err("a refusal")
}

/// The diagnostics a one-file document is refused with in derivation, whole.
pub fn derive_errors(source: &str) -> Vec<Diagnostic> {
    derive_errors_tree(&[("", source)])
}

/// The diagnostics a tree is refused with in derivation, whole — for the
/// assertions that read a span in a module other than the root.
pub fn derive_errors_tree(modules: &[(&str, &str)]) -> Vec<Diagnostic> {
    derive_tree(modules).expect_err("a refusal")
}

/// Just the messages, for a panic that has to say what went wrong.
pub fn messages(errors: &[Diagnostic]) -> Vec<&str> {
    errors.iter().map(|error| error.message.as_str()).collect()
}
