//! Diagnostics: one shape for every way a `.brenn` file can fail to become a
//! model value.

use std::collections::HashMap;
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
        Self::span_line_col(&self.span)
    }

    /// `line:column` of a span's start, one-based.
    pub fn span_line_col(span: &Span) -> Option<(i64, i64)> {
        span.line_col_inner()
            .map(|position| (position.line + 1, position.col + 1))
    }

    /// `file:line:col: note` for one entry of [`Diagnostic::related`].
    ///
    /// Degrades gracefully: each of filename and position is omitted from the
    /// rendering when absent rather than asserted, because this is the reporting
    /// path — a worse prefix beats no report.
    pub fn related_line(note: &str, span: &Span) -> String {
        match (span.filename_inner(), Self::span_line_col(span)) {
            (Some(file), Some((line, column))) => format!("{file}:{line}:{column}: {note}"),
            (Some(file), None) => format!("{file}: {note}"),
            (None, Some((line, column))) => format!("{line}:{column}: {note}"),
            (None, None) => note.to_string(),
        }
    }
}

/// A set, as a diagnostic that expects one of its members lists it:
/// ``​`a`, `b` or `c` ``.
///
/// The one home for the enumerated-set idiom: every refusal that names the
/// legal values — schemes, matcher kinds, segment boundaries, grant words —
/// reads the same way, and how a set reads is decided once.
pub fn or_list(items: impl IntoIterator<Item = impl fmt::Display>) -> String {
    let quoted: Vec<String> = items.into_iter().map(|item| format!("`{item}`")).collect();
    match quoted.split_last() {
        Some((last, rest)) if !rest.is_empty() => format!("{} or {last}", rest.join(", ")),
        Some((last, _)) => last.clone(),
        None => String::new(),
    }
}

/// A diagnostic that cites a second location.
///
/// The one home for the shape every pass reaches for when a refusal has another
/// site to point at, so a change to how a related location reads lands once.
pub fn two_site(
    message: impl Into<String>,
    span: Span,
    related: impl Into<String>,
    related_span: Span,
) -> Diagnostic {
    let mut diagnostic = Diagnostic::at(message, span);
    diagnostic.related.push((related.into(), related_span));
    diagnostic
}

/// Every key seen once, and a diagnostic per repeat citing the site that holds
/// it. Returns what was kept, so a caller can go on asking who holds a key.
///
/// The one collision engine: identities, addresses, tuning keys and whatever the
/// next whole-document rule keys on all read the same way, so a fix to how
/// repeats are reported lands once.
///
/// `collide` is handed the key, the repeat and its site, then the value that
/// holds the key and its site.
pub fn check_unique<'a, K, V>(
    items: impl Iterator<Item = (K, V, &'a Span)>,
    collide: impl Fn(&K, &V, &Span, &V, &Span) -> Diagnostic,
    errors: &mut Vec<Diagnostic>,
) -> HashMap<K, (V, &'a Span)>
where
    K: Eq + std::hash::Hash,
{
    let mut held: HashMap<K, (V, &'a Span)> = HashMap::new();
    for (key, value, span) in items {
        match held.get(&key) {
            Some((prior, prior_span)) => {
                errors.push(collide(&key, &value, span, prior, prior_span));
            }
            None => {
                held.insert(key, (value, span));
            }
        }
    }
    held
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A span that came out of a parse: it names its file and its position.
    fn parsed_span() -> Span {
        let file = crate::parse_str("acl subscribe [exact \"brenn:alice.cmd\"];\n", "main.brenn")
            .expect("a top-level acl parses");
        let mut errors = crate::resolve_files(vec![(String::new(), file)], "")
            .expect_err("a top-level acl is refused at resolve");
        errors.pop().expect("one refusal").span
    }

    #[test]
    fn a_positioned_related_line_carries_file_line_and_column() {
        assert_eq!(
            Diagnostic::related_line("declared here", &parsed_span()),
            "main.brenn:1:5: declared here"
        );
    }

    #[test]
    fn an_unpositioned_related_line_is_the_note_alone() {
        assert_eq!(
            Diagnostic::related_line("declared here", &Span::unknown()),
            "declared here"
        );
    }

    #[test]
    fn a_set_of_several_reads_as_a_comma_list_ending_in_or() {
        assert_eq!(or_list(["a", "b", "c"]), "`a`, `b` or `c`");
        assert_eq!(or_list(["a", "b"]), "`a` or `b`");
    }

    #[test]
    fn a_set_of_one_is_that_one_and_no_or() {
        assert_eq!(or_list(["a"]), "`a`");
    }

    #[test]
    fn a_set_of_none_names_nothing() {
        assert_eq!(or_list(Vec::<&str>::new()), "");
    }
}
