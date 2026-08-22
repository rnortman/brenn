//! The Brenn configuration language: parse a `.brenn` file into the semantic
//! model.
//!
//! `cst`, `parser`, `unparser` and `de` are generated from `grammar/brenn.fltkg`
//! and its two sidecars at build time; nothing generated is tracked, so the
//! grammar is the only place a syntax change is made. `model` is hand-written
//! and is the typed surface everything downstream reads.
//!
//! One file at a time here; `resolve` follows imports from a root file and
//! produces the `resolved` model, which `derive` then turns into the `derived`
//! one.

use std::path::Path;

pub mod cst;
// `de`'s per-rule entry points are the re-entry surface for held nodes; the
// ones no caller has reached for yet are dead code under `-D warnings`.
#[allow(dead_code)]
mod de;
/// The pass after resolution: authority, identity and the channel model made
/// concrete and checked.
pub mod derive;
/// The far side of derivation: what a document comes to.
pub mod derived;
pub mod diag;
pub mod model;
// `parser` and `unparser` are public because the formatter binary names their
// types from outside this crate; `cst` is public because their signatures are
// stated in `cst` types.
pub mod parser;
/// From a root document to a resolved one: imports, references, expansion.
pub mod resolve;
/// The far side of resolution: what a document means once references, strings,
/// classes and assemblies are gone.
pub mod resolved;
pub mod unparser;

use fltk_serde_core::ParseToTargetError;

use diag::Diagnostic;

pub use resolve::{compile, resolve_files};

/// The position type every diagnostic and every resolved value carries.
///
/// Re-exported because it is part of this crate's public surface: a consumer of
/// [`diag::Diagnostic`] or of a [`resolved::RVal`] reads and constructs spans,
/// and the crate that owns the type is this crate's own dependency.
pub use fltk_cst_core::Span;

/// How deep a document may nest before the parse is refused.
///
/// The generated parser is recursive descent and its own default is sized for an
/// ~8 MiB stack. A Rust spawned thread gets 2 MiB and a tokio worker no more,
/// and nothing says a `.brenn` file is parsed on the main thread — at the
/// default, nesting deep enough to matter overflows the stack before the limit
/// trips. Sized for the smaller stack, so the answer to pathological input is a
/// diagnostic rather than an abort. Real documents nest an order of magnitude
/// less than this.
const MAX_DEPTH: u32 = 250;

/// Parse and deserialize one document.
///
/// `filename` is what diagnostics name; it need not exist on disk.
pub fn parse_str(src: &str, filename: &str) -> Result<model::File, Diagnostic> {
    parse_bounded(src, filename).map_err(|error| Diagnostic::from_parse_error(error, filename))
}

/// The generated `de::from_str`, with the depth limit this crate sets.
///
/// Written out here rather than called because the generated entry point takes
/// the parser's default limit and offers no way to lower it.
fn parse_bounded(src: &str, filename: &str) -> Result<model::File, ParseToTargetError> {
    let mut parser = parser::Parser::new(src, Some(filename), false);
    parser.set_max_depth(MAX_DEPTH);
    let parsed = parser.apply__parse_file(0);
    // A depth-rejected parse can still come back as `Some` holding a wrong tree.
    if parser.depth_exceeded() {
        return Err(ParseToTargetError::Parse(parser.error_message()));
    }
    let Some(parsed) = parsed else {
        return Err(ParseToTargetError::Parse(parser.error_message()));
    };
    // The whole input has to be consumed; `pos` counts characters, not bytes.
    if parsed.pos != src.chars().count() as i64 {
        return Err(ParseToTargetError::Parse(parser.error_message()));
    }
    Ok(de::from_file_cst(&parsed.result)?)
}

/// Read a file and parse it.
pub fn parse_file(path: &Path) -> Result<model::File, Diagnostic> {
    let filename = path.display().to_string();
    let src = std::fs::read_to_string(path)
        .map_err(|error| Diagnostic::unpositioned(error.to_string(), &filename))?;
    parse_str(&src, &filename)
}
