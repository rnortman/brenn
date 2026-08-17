//! Diagnostics: one shape for every way a `.brenn` file can fail to become a
//! model value.

use std::fmt;

use fltk_cst_core::Span;
use fltk_serde_core::{DeserializeError, ParseToTargetError};

/// A failure, with the file it happened in and — where the front end could
/// position it — where.
///
/// `related` carries secondary locations with their own explanations: the other
/// definition of a duplicated key, and whatever later stages add.
#[derive(Debug)]
pub struct Diagnostic {
    pub message: String,
    pub file: String,
    pub span: Span,
    pub related: Vec<(String, Span)>,
}

impl Diagnostic {
    /// A diagnostic with no position — an I/O failure, or anything else that
    /// happened before there was source text to point at.
    pub fn unpositioned(message: impl Into<String>, file: impl Into<String>) -> Self {
        Diagnostic {
            message: message.into(),
            file: file.into(),
            span: Span::unknown(),
            related: Vec::new(),
        }
    }

    /// A diagnostic at a span that names its own file.
    ///
    /// Everything raised after the parse works from held CST nodes rather than
    /// source text, so the filename comes from the span the node carries. Every
    /// such span came out of a parse that was handed a filename, so a span
    /// without one is a broken tree rather than bad input.
    pub fn at(message: impl Into<String>, span: Span) -> Self {
        let file = span
            .filename_inner()
            .expect("a parsed span carries its filename")
            .to_string();
        Diagnostic {
            message: message.into(),
            file,
            span,
            related: Vec::new(),
        }
    }

    /// Carry over a deserialize failure raised by re-entering the bridge with a
    /// held node, where no caller passed a filename in.
    pub fn from_deserialize_error(error: DeserializeError) -> Self {
        let file = error
            .span
            .filename_inner()
            .expect("a parsed span carries its filename")
            .to_string();
        Diagnostic {
            message: error.message,
            file,
            span: error.span,
            related: error.related,
        }
    }

    /// Carry over what the front end reported.
    ///
    /// The parse arm has already rendered position into its own message, so it
    /// arrives unpositioned here; the deserialize arm carries a span and its
    /// related locations.
    pub fn from_parse_error(error: ParseToTargetError, file: impl Into<String>) -> Self {
        match error {
            ParseToTargetError::Parse(message) => Diagnostic {
                message,
                file: file.into(),
                span: Span::unknown(),
                related: Vec::new(),
            },
            ParseToTargetError::Deserialize(error) => Diagnostic {
                message: error.message,
                file: file.into(),
                span: error.span,
                related: error.related,
            },
        }
    }

    /// `line:column` of the span's start, one-based, when the span resolves one.
    pub fn line_col(&self) -> Option<(i64, i64)> {
        self.span
            .line_col_inner()
            .map(|position| (position.line + 1, position.col + 1))
    }
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.line_col() {
            Some((line, column)) => write!(f, "{}:{line}:{column}: {}", self.file, self.message),
            None => write!(f, "{}: {}", self.file, self.message),
        }
    }
}

impl std::error::Error for Diagnostic {}
