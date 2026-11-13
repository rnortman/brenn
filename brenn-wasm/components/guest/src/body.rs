// Message bodies as a component reads and writes them: bytes with an optional
// content type, owned (`Body`) or borrowed for the host (`BodyRef`), and one
// window entry (`Message`) pairing a body with its envelope header.

use brenn_envelope::{EnvelopeHeader, MessageBody, MessageEnvelope, is_text_content_type};

use crate::Error;

/// How a diagnostic names the absent content type, as
/// [`MessageBody::summary`] does.
pub(crate) const UNTYPED_TEXT: &str = "untyped text";

/// A body as it crossed the boundary: an optional content type and bytes.
///
/// This is the host's copy, held once. [`MessageBody`] — what
/// [`MessageEnvelope::body`] is — is a validated, shared copy made from it by
/// [`Message::envelope`]; a component handling a large body reads it here and
/// never builds the envelope.
///
/// A body with no content type is untyped text. A typed body is text when
/// [`brenn_envelope::is_text_content_type`] says its type is, and bytes
/// otherwise.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Body {
    content_type: Option<String>,
    bytes: Vec<u8>,
}

impl Body {
    /// An untyped text body.
    pub fn text(text: impl Into<String>) -> Body {
        Body {
            content_type: None,
            bytes: text.into().into_bytes(),
        }
    }

    /// A body of the given content type: `application/octet-stream` for bytes,
    /// `text/markdown` for typed text, and so on.
    pub fn typed(content_type: impl Into<String>, bytes: Vec<u8>) -> Body {
        Body {
            content_type: Some(content_type.into()),
            bytes,
        }
    }

    /// The content type, or `None` for untyped text.
    pub fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }

    /// The bytes, whatever the type.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The bytes, moved out.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Whether the content type is a text type (untyped included).
    pub fn is_text(&self) -> bool {
        is_text_content_type(self.content_type())
    }

    /// The body's text.
    ///
    /// `Err(MalformedEnvelope)`, naming the content type and length, when the
    /// type is not a text type or the bytes are not UTF-8. There is no lossy
    /// reading: a body that is not text is not made into one.
    pub fn as_text(&self) -> Result<&str, Error> {
        if !self.is_text() {
            return Err(Error::malformed(format!(
                "body is not text: {}",
                self.describe()
            )));
        }
        core::str::from_utf8(&self.bytes).map_err(|e| {
            Error::malformed(format!("text body is not UTF-8 ({}): {e}", self.describe()))
        })
    }

    /// The body's text deserialized as JSON into `T`.
    ///
    /// `Err(MalformedEnvelope)` when the body is not text (as
    /// [`Body::as_text`]) or does not deserialize.
    pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T, Error> {
        serde_json::from_str(self.as_text()?)
            .map_err(|e| Error::malformed(format!("body JSON ({}): {e}", self.describe())))
    }

    /// `<content type>, <n> bytes`, the content type `untyped text` when there
    /// is none — for a diagnostic, never the bytes themselves. Spelled as
    /// [`MessageBody::summary`] displays a body.
    pub(crate) fn describe(&self) -> String {
        BodyRef::from(self).describe()
    }

    pub(crate) fn from_parts(content_type: Option<String>, bytes: Vec<u8>) -> Body {
        Body {
            content_type,
            bytes,
        }
    }

    pub(crate) fn into_parts(self) -> (Option<String>, Vec<u8>) {
        (self.content_type, self.bytes)
    }
}

impl From<String> for Body {
    fn from(text: String) -> Body {
        Body::text(text)
    }
}

impl From<&str> for Body {
    fn from(text: &str) -> Body {
        Body::text(text)
    }
}

/// A borrowed body to hand the host: a publish, a parked or edited message, a
/// call's request.
///
/// A `&str` or `&String` converts to an untyped text body, which is what every
/// text publish puts on the bus.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BodyRef<'a> {
    content_type: Option<&'a str>,
    bytes: &'a [u8],
}

impl<'a> BodyRef<'a> {
    /// An untyped text body.
    pub fn text(text: &'a str) -> BodyRef<'a> {
        BodyRef {
            content_type: None,
            bytes: text.as_bytes(),
        }
    }

    /// A body of the given content type. A non-text type needs a channel
    /// declared `non_text`; publishing one anywhere else is
    /// [`Error::TextOnlyChannel`].
    pub fn typed(content_type: &'a str, bytes: &'a [u8]) -> BodyRef<'a> {
        BodyRef {
            content_type: Some(content_type),
            bytes,
        }
    }

    pub(crate) fn from_parts(content_type: Option<&'a str>, bytes: &'a [u8]) -> BodyRef<'a> {
        BodyRef {
            content_type,
            bytes,
        }
    }

    /// The content type, or `None` for untyped text.
    pub fn content_type(&self) -> Option<&'a str> {
        self.content_type
    }

    /// The bytes.
    pub fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }

    pub(crate) fn describe(&self) -> String {
        format!(
            "{}, {} bytes",
            self.content_type.unwrap_or(UNTYPED_TEXT),
            self.bytes.len()
        )
    }
}

impl<'a> From<&'a str> for BodyRef<'a> {
    fn from(text: &'a str) -> BodyRef<'a> {
        BodyRef::text(text)
    }
}

impl<'a> From<&'a String> for BodyRef<'a> {
    fn from(text: &'a String) -> BodyRef<'a> {
        BodyRef::text(text)
    }
}

impl<'a> From<&'a Body> for BodyRef<'a> {
    fn from(body: &'a Body) -> BodyRef<'a> {
        BodyRef {
            content_type: body.content_type.as_deref(),
            bytes: &body.bytes,
        }
    }
}

/// One window entry: an envelope's header and its body, as handed over.
///
/// The header is an open JSON object — every envelope field but the body —
/// and is parsed only when asked for, by [`Message::envelope`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    header_json: String,
    body: Body,
}

impl Message {
    pub(crate) fn new(header_json: String, body: Body) -> Message {
        Message { header_json, body }
    }

    /// The envelope header as JSON text: every envelope field but the body.
    /// The object is open; a reader ignores keys it does not know.
    pub fn header_json(&self) -> &str {
        &self.header_json
    }

    /// The body.
    pub fn body(&self) -> &Body {
        &self.body
    }

    pub(crate) fn into_body(self) -> Body {
        self.body
    }

    /// The whole envelope, parsed: the header read as an
    /// [`EnvelopeHeader`], the body copied into a [`MessageBody`].
    ///
    /// `Err(MalformedEnvelope)` when the header does not read or the body does
    /// not build (a malformed content type, or a text-typed body that is not
    /// UTF-8). This copies the body; a component handling a large one reads
    /// [`Message::body`] instead.
    pub fn envelope(&self) -> Result<MessageEnvelope, Error> {
        let header: EnvelopeHeader = serde_json::from_str(&self.header_json)
            .map_err(|e| Error::malformed(format!("envelope header JSON: {e}")))?;
        let body = MessageBody::from_parts(self.body.content_type(), self.body.as_bytes())
            .map_err(|e| {
                Error::malformed(format!("envelope body ({}): {e}", self.body.describe()))
            })?;
        Ok(MessageEnvelope::from_header(header, body))
    }
}
