//! A bus message body: bytes with an optional content type.
//!
//! A body is either *text* (valid UTF-8, untyped or carrying a text content
//! type) or *non-text* (arbitrary bytes, always carrying a content type).
//! [`is_text_content_type`] is the one rule that decides which; a body's kind
//! is fixed by it when the body is constructed and never guessed from the
//! bytes.

use std::fmt;
use std::sync::Arc;

/// Longest content type [`MessageBody::from_parts`] accepts, in bytes.
pub const MAX_CONTENT_TYPE_LEN: usize = 255;

/// A message body: UTF-8 text, or bytes with a content type.
///
/// Invariants: a text body is valid UTF-8 and its content type, if any, is a
/// text type; a non-text body always has a content type, and it is not a text
/// type. Every content type held is well-formed and is returned verbatim as it
/// was given. Cloning shares the bytes; it never copies them.
#[derive(Clone, PartialEq, Eq)]
pub struct MessageBody(Repr);

#[derive(Clone, PartialEq, Eq)]
enum Repr {
    Text {
        text: Arc<str>,
        content_type: Option<Arc<str>>,
    },
    Binary {
        bytes: Arc<[u8]>,
        content_type: Arc<str>,
    },
}

/// Why [`MessageBody::from_parts`] refused its input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BodyError {
    /// The content type is not a well-formed media type.
    InvalidContentType {
        content_type: String,
        reason: &'static str,
    },
    /// A text body (untyped, or typed with a text type) is not valid UTF-8.
    NotUtf8 { content_type: Option<String> },
}

impl fmt::Display for BodyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BodyError::InvalidContentType {
                content_type,
                reason,
            } => write!(f, "invalid content type {content_type:?}: {reason}"),
            BodyError::NotUtf8 { content_type: None } => {
                f.write_str("untyped body is not valid UTF-8")
            }
            BodyError::NotUtf8 {
                content_type: Some(ct),
            } => write!(f, "body typed {ct:?} is not valid UTF-8"),
        }
    }
}

impl std::error::Error for BodyError {}

impl MessageBody {
    /// Untyped UTF-8 text. Infallible.
    pub fn text(text: impl Into<Arc<str>>) -> Self {
        MessageBody(Repr::Text {
            text: text.into(),
            content_type: None,
        })
    }

    /// A body from a content type and bytes. Bytes or a content type from
    /// outside the host must enter through this, so its checks always apply.
    /// The bytes are copied once, into the body's shared allocation.
    ///
    /// Refuses a malformed content type, and a text body (untyped, or typed
    /// with a text type) whose bytes are not valid UTF-8.
    pub fn from_parts(content_type: Option<&str>, bytes: &[u8]) -> Result<Self, BodyError> {
        let Some(ct) = content_type else {
            let text = std::str::from_utf8(bytes)
                .map_err(|_| BodyError::NotUtf8 { content_type: None })?;
            return Ok(Self::text(text));
        };
        let media = parse_media_type(ct).map_err(|reason| BodyError::InvalidContentType {
            content_type: ct.to_owned(),
            reason,
        })?;
        if media.is_text() {
            let text = std::str::from_utf8(bytes).map_err(|_| BodyError::NotUtf8 {
                content_type: Some(ct.to_owned()),
            })?;
            Ok(MessageBody(Repr::Text {
                text: text.into(),
                content_type: Some(ct.into()),
            }))
        } else {
            Ok(MessageBody(Repr::Binary {
                bytes: bytes.into(),
                content_type: ct.into(),
            }))
        }
    }

    /// The body's text; `Some` iff the body is text.
    pub fn as_text(&self) -> Option<&str> {
        match &self.0 {
            Repr::Text { text, .. } => Some(text),
            Repr::Binary { .. } => None,
        }
    }

    /// The body's raw bytes, for either kind.
    pub fn as_bytes(&self) -> &[u8] {
        match &self.0 {
            Repr::Text { text, .. } => text.as_bytes(),
            Repr::Binary { bytes, .. } => bytes,
        }
    }

    /// The content type, verbatim as given; `None` for untyped text.
    pub fn content_type(&self) -> Option<&str> {
        match &self.0 {
            Repr::Text { content_type, .. } => content_type.as_deref(),
            Repr::Binary { content_type, .. } => Some(content_type),
        }
    }

    /// Whether the body is text, as decided by [`is_text_content_type`].
    pub fn is_text(&self) -> bool {
        matches!(self.0, Repr::Text { .. })
    }

    /// The raw byte count.
    pub fn len(&self) -> usize {
        self.as_bytes().len()
    }

    /// Whether the body has no bytes.
    pub fn is_empty(&self) -> bool {
        self.as_bytes().is_empty()
    }

    /// The body described by its metadata, never its bytes: displays as
    /// `<content type>, <n> bytes`, or `untyped text, <n> bytes`.
    pub fn summary(&self) -> BodySummary<'_> {
        BodySummary(self)
    }
}

/// A body's content type and size, for a log line or a refusal; see
/// [`MessageBody::summary`].
pub struct BodySummary<'a>(&'a MessageBody);

impl fmt::Display for BodySummary<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.content_type() {
            Some(ct) => write!(f, "{ct}, {} bytes", self.0.len()),
            None => write!(f, "untyped text, {} bytes", self.0.len()),
        }
    }
}

impl From<&str> for MessageBody {
    fn from(text: &str) -> Self {
        Self::text(text)
    }
}

impl From<String> for MessageBody {
    fn from(text: String) -> Self {
        Self::text(text)
    }
}

// A non-text body prints its content type and length, never its bytes, so a
// log line cannot carry them. Untyped text prints as a `String` does.
impl fmt::Debug for MessageBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Repr::Text {
                text,
                content_type: None,
            } => fmt::Debug::fmt(&**text, f),
            Repr::Text {
                text,
                content_type: Some(ct),
            } => f
                .debug_struct("TextBody")
                .field("content_type", &&**ct)
                .field("text", &&**text)
                .finish(),
            Repr::Binary {
                bytes,
                content_type,
            } => f
                .debug_struct("BinaryBody")
                .field("content_type", &&**content_type)
                .field("len", &bytes.len())
                .finish(),
        }
    }
}

/// Whether a body with this content type is text: the type is absent, or it
/// names a text type (`text/*`, `application/json` or any `+json` subtype)
/// with no `charset` parameter other than `utf-8` or `us-ascii`.
///
/// A content type that is not well-formed is not text; [`MessageBody`] never
/// holds one.
///
/// A durable row's storage class records this rule's answer at the time the
/// row was written, and reading a row checks the two agree. A change to this
/// rule therefore ships with a migration of the stored rows it reclassifies.
pub fn is_text_content_type(content_type: Option<&str>) -> bool {
    match content_type {
        None => true,
        Some(ct) => parse_media_type(ct).is_ok_and(|m| m.is_text()),
    }
}

/// A well-formed content type, borrowed from the string it was parsed from.
struct MediaType<'a> {
    type_: &'a str,
    subtype: &'a str,
    /// Parameter names and values; a quoted value keeps its quotes.
    params: Vec<(&'a str, &'a str)>,
}

impl MediaType<'_> {
    fn is_text(&self) -> bool {
        let text_type = self.type_.eq_ignore_ascii_case("text")
            || (self.type_.eq_ignore_ascii_case("application")
                && self.subtype.eq_ignore_ascii_case("json"))
            || ends_with_ignore_ascii_case(self.subtype, "+json");
        text_type
            && self
                .params
                .iter()
                .filter(|(name, _)| name.eq_ignore_ascii_case("charset"))
                .all(|(_, value)| {
                    let value = unquote(value);
                    value.eq_ignore_ascii_case("utf-8") || value.eq_ignore_ascii_case("us-ascii")
                })
    }
}

fn ends_with_ignore_ascii_case(s: &str, suffix: &str) -> bool {
    s.len() >= suffix.len()
        && s.as_bytes()[s.len() - suffix.len()..].eq_ignore_ascii_case(suffix.as_bytes())
}

/// RFC 9110 `tchar`.
fn is_tchar(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)
}

/// The length of the token at the start of `s`.
fn token_len(s: &[u8]) -> usize {
    s.iter().take_while(|b| is_tchar(**b)).count()
}

/// The length of the RFC 9110 quoted-string at the start of `s`, or `None`
/// if `s` does not start with a complete one.
fn quoted_string_len(s: &[u8]) -> Option<usize> {
    if s.first() != Some(&b'"') {
        return None;
    }
    let mut i = 1;
    while i < s.len() {
        match s[i] {
            b'"' => return Some(i + 1),
            b'\\' => {
                let escaped = *s.get(i + 1)?;
                if !(escaped == b'\t' || (b' '..=b'~').contains(&escaped)) {
                    return None;
                }
                i += 2;
            }
            b'\t' | b' ' | 0x21 | 0x23..=0x5b | 0x5d..=0x7e => i += 1,
            _ => return None,
        }
    }
    None
}

/// A parameter value without its quotes and quoted-pair escapes, for
/// comparison. Values compared by the text rule are short ASCII words, so an
/// escaped value never equals one and is returned as given.
fn unquote(value: &str) -> &str {
    match value.strip_prefix('"').and_then(|v| v.strip_suffix('"')) {
        Some(inner) if !inner.contains('\\') => inner,
        _ => value,
    }
}

/// Parse `type "/" subtype *( *SP ";" *SP token "=" ( token / quoted-string ) )`
/// over ASCII, at most [`MAX_CONTENT_TYPE_LEN`] bytes.
fn parse_media_type(ct: &str) -> Result<MediaType<'_>, &'static str> {
    if ct.len() > MAX_CONTENT_TYPE_LEN {
        return Err("longer than 255 bytes");
    }
    if !ct.is_ascii() {
        return Err("not ASCII");
    }
    let b = ct.as_bytes();

    let type_len = token_len(b);
    if type_len == 0 {
        return Err("missing type");
    }
    if b.get(type_len) != Some(&b'/') {
        return Err("expected '/' after the type");
    }
    let sub_start = type_len + 1;
    let sub_len = token_len(&b[sub_start..]);
    if sub_len == 0 {
        return Err("missing subtype");
    }
    let mut i = sub_start + sub_len;
    let mut params = Vec::new();

    let skip_spaces = |mut i: usize| {
        while b.get(i) == Some(&b' ') {
            i += 1;
        }
        i
    };

    while i < b.len() {
        i = skip_spaces(i);
        if b.get(i) != Some(&b';') {
            return Err("expected ';' before a parameter");
        }
        i = skip_spaces(i + 1);
        let name_len = token_len(&b[i..]);
        if name_len == 0 {
            return Err("missing parameter name");
        }
        let name = &ct[i..i + name_len];
        i += name_len;
        if b.get(i) != Some(&b'=') {
            return Err("expected '=' after a parameter name");
        }
        i += 1;
        let value_len = match quoted_string_len(&b[i..]) {
            Some(n) => n,
            None if b.get(i) == Some(&b'"') => return Err("malformed quoted-string"),
            None => token_len(&b[i..]),
        };
        if value_len == 0 {
            return Err("missing parameter value");
        }
        params.push((name, &ct[i..i + value_len]));
        i += value_len;
    }

    Ok(MediaType {
        type_: &ct[..type_len],
        subtype: &ct[sub_start..sub_start + sub_len],
        params,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn invalid_reason(ct: &str) -> &'static str {
        match MessageBody::from_parts(Some(ct), &[]) {
            Err(BodyError::InvalidContentType {
                content_type,
                reason,
            }) => {
                assert_eq!(content_type, ct);
                reason
            }
            other => panic!("{ct:?} accepted or refused for another reason: {other:?}"),
        }
    }

    #[test]
    fn text_round_trips() {
        let body = MessageBody::text("hello");
        assert_eq!(body.as_text(), Some("hello"));
        assert_eq!(body.as_bytes(), b"hello");
        assert_eq!(body.content_type(), None);
        assert!(body.is_text());
        assert_eq!(body.len(), 5);
        assert!(!body.is_empty());
        assert_eq!(MessageBody::from("hello"), body);
        assert_eq!(MessageBody::from(String::from("hello")), body);
    }

    #[test]
    fn empty_text_is_untyped_text() {
        let body = MessageBody::text("");
        assert!(body.is_text());
        assert!(body.is_empty());
        assert_eq!(body.as_text(), Some(""));
    }

    #[test]
    fn from_parts_accepts_untyped_utf8_as_untyped_text() {
        let body = MessageBody::from_parts(None, "héllo".as_bytes()).unwrap();
        assert_eq!(body, MessageBody::text("héllo"));
    }

    #[test]
    fn from_parts_accepts_typed_text_and_keeps_the_type_verbatim() {
        let body = MessageBody::from_parts(Some("Text/Markdown"), b"# hi").unwrap();
        assert!(body.is_text());
        assert_eq!(body.as_text(), Some("# hi"));
        assert_eq!(body.content_type(), Some("Text/Markdown"));
        assert_ne!(body, MessageBody::text("# hi"));
    }

    #[test]
    fn from_parts_accepts_typed_binary() {
        let bytes = vec![0xff, 0x00, 0xd8, 0x80];
        let body = MessageBody::from_parts(Some("image/jpeg"), &bytes).unwrap();
        assert!(!body.is_text());
        assert_eq!(body.as_text(), None);
        assert_eq!(body.as_bytes(), &bytes[..]);
        assert_eq!(body.content_type(), Some("image/jpeg"));
        assert_eq!(body.len(), 4);
    }

    #[test]
    fn non_text_kind_follows_the_type_not_the_bytes() {
        let body = MessageBody::from_parts(Some("application/octet-stream"), b"plain").unwrap();
        assert!(!body.is_text());
        assert_eq!(body.as_bytes(), b"plain");
    }

    #[test]
    fn zero_byte_binary_body() {
        let body = MessageBody::from_parts(Some("application/octet-stream"), &[]).unwrap();
        assert!(!body.is_text());
        assert!(body.is_empty());
    }

    #[test]
    fn from_parts_refuses_untyped_non_utf8() {
        assert_eq!(
            MessageBody::from_parts(None, &[0xff, 0xfe]),
            Err(BodyError::NotUtf8 { content_type: None })
        );
    }

    #[test]
    fn from_parts_refuses_text_type_over_invalid_utf8() {
        assert_eq!(
            MessageBody::from_parts(Some("text/plain"), &[0xc3]),
            Err(BodyError::NotUtf8 {
                content_type: Some("text/plain".into())
            })
        );
    }

    #[test]
    fn non_utf8_charset_is_carried_as_bytes() {
        let body =
            MessageBody::from_parts(Some("text/plain; charset=iso-8859-1"), &[0xe9]).unwrap();
        assert!(!body.is_text());
        assert_eq!(body.as_bytes(), &[0xe9]);
    }

    #[test]
    fn from_parts_refuses_malformed_content_types() {
        for ct in [
            "",
            "text",
            "text/",
            "/plain",
            " text/plain",
            "text/plain ",
            "text /plain",
            "text/pla in",
            "text/plain;",
            "text/plain; charset",
            "text/plain; charset=",
            "text/plain; =utf-8",
            "text/plain; charset=\"utf-8",
            "text/plain charset=utf-8",
            "text/plain; charset=utf-8;",
            "text/plaín",
            "text/plain\n",
            "text/plain; note=\"a\u{1}b\"",
        ] {
            invalid_reason(ct);
        }
    }

    #[test]
    fn from_parts_accepts_well_formed_parameters() {
        for ct in [
            "text/plain;charset=utf-8",
            "text/plain ; charset=utf-8",
            "text/plain;  charset=utf-8 ;format=flowed",
            "application/x-test; note=\"<b> \\\" ;=\"",
            "application/vnd.example+json; profile=\"https://alice.example.com/p\"",
        ] {
            MessageBody::from_parts(Some(ct), b"{}")
                .unwrap_or_else(|e| panic!("{ct:?} refused: {e}"));
        }
    }

    #[test]
    fn content_type_length_limit_is_255_bytes() {
        let at_limit = format!("application/{}", "x".repeat(MAX_CONTENT_TYPE_LEN - 12));
        assert_eq!(at_limit.len(), MAX_CONTENT_TYPE_LEN);
        MessageBody::from_parts(Some(&at_limit), &[]).unwrap();
        let over = format!("{at_limit}x");
        assert_eq!(invalid_reason(&over), "longer than 255 bytes");
    }

    #[test]
    fn text_rule_table() {
        let cases: &[(Option<&str>, bool)] = &[
            (None, true),
            (Some("text/plain"), true),
            (Some("text/markdown"), true),
            (Some("TEXT/Markdown"), true),
            (Some("text/csv"), true),
            (Some("application/json"), true),
            (Some("Application/JSON"), true),
            (Some("application/ld+json"), true),
            (Some("application/LD+JSON"), true),
            (Some("text/plain; charset=utf-8"), true),
            (Some("text/plain; charset=\"UTF-8\""), true),
            (Some("text/plain; CHARSET=US-ASCII"), true),
            (Some("text/plain; format=flowed"), true),
            (Some("text/plain; charset=iso-8859-1"), false),
            (Some("text/plain; charset=utf-8; charset=latin1"), false),
            (Some("application/json; charset=utf-16"), false),
            (Some("application/octet-stream"), false),
            (Some("image/jpeg"), false),
            (Some("application/xml"), false),
            (Some("application/jsonx"), false),
            (Some("application/json-seq"), false),
            (Some("text"), false),
        ];
        for (ct, expected) in cases {
            assert_eq!(is_text_content_type(*ct), *expected, "{ct:?}");
        }
    }

    #[test]
    fn body_kind_agrees_with_the_text_rule() {
        for ct in [
            "text/markdown",
            "application/ld+json",
            "image/png",
            "text/plain; charset=iso-8859-1",
        ] {
            let body = MessageBody::from_parts(Some(ct), b"ok").unwrap();
            assert_eq!(body.is_text(), is_text_content_type(Some(ct)), "{ct}");
        }
    }

    #[test]
    fn clone_shares_the_allocation() {
        let text = MessageBody::text("shared text");
        let text_clone = text.clone();
        assert_eq!(text.as_bytes().as_ptr(), text_clone.as_bytes().as_ptr());

        let binary = MessageBody::from_parts(Some("image/png"), &[1, 2, 3]).unwrap();
        let binary_clone = binary.clone();
        assert_eq!(binary.as_bytes().as_ptr(), binary_clone.as_bytes().as_ptr());
    }

    #[test]
    fn equality_compares_kind_bytes_and_type_verbatim() {
        let a = MessageBody::from_parts(Some("text/markdown"), b"x").unwrap();
        let b = MessageBody::from_parts(Some("text/markdown"), b"x").unwrap();
        let c = MessageBody::from_parts(Some("TEXT/markdown"), b"x").unwrap();
        let d = MessageBody::from_parts(Some("application/octet-stream"), b"x").unwrap();
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_ne!(a, d);
        assert_ne!(d, MessageBody::text("x"));
    }

    #[test]
    fn debug_of_untyped_text_matches_string() {
        let s = "a \"quoted\" line\n";
        assert_eq!(format!("{:?}", MessageBody::text(s)), format!("{s:?}"));
    }

    #[test]
    fn debug_of_typed_text_shows_type_and_text() {
        let body = MessageBody::from_parts(Some("text/markdown"), b"# hi").unwrap();
        let out = format!("{body:?}");
        assert!(out.contains("text/markdown"), "{out}");
        assert!(out.contains("# hi"), "{out}");
    }

    #[test]
    fn debug_of_non_text_body_omits_its_bytes() {
        let body =
            MessageBody::from_parts(Some("application/octet-stream"), b"SECRET-PAYLOAD").unwrap();
        let out = format!("{body:?}");
        assert!(!out.contains("SECRET"), "{out}");
        assert!(!out.contains("83"), "{out}");
        assert!(out.contains("application/octet-stream"), "{out}");
        assert!(out.contains("14"), "{out}");
    }

    #[test]
    fn summary_names_type_and_size_never_the_bytes() {
        let binary = MessageBody::from_parts(Some("application/octet-stream"), b"SECRET").unwrap();
        assert_eq!(
            binary.summary().to_string(),
            "application/octet-stream, 6 bytes"
        );
        let typed = MessageBody::from_parts(Some("text/markdown"), b"# hi").unwrap();
        assert_eq!(typed.summary().to_string(), "text/markdown, 4 bytes");
        assert_eq!(
            MessageBody::text("hi").summary().to_string(),
            "untyped text, 2 bytes"
        );
    }

    #[test]
    fn body_error_display_names_the_cause() {
        let err = MessageBody::from_parts(Some("text/"), &[]).unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid content type \"text/\": missing subtype"
        );
        let err = MessageBody::from_parts(Some("text/csv"), &[0xff]).unwrap_err();
        assert_eq!(
            err.to_string(),
            "body typed \"text/csv\" is not valid UTF-8"
        );
    }
}
