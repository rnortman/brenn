//! How a frame reaches the websocket: the message it is written as, the
//! per-version [`Codec`] that writes and reads it, and the frozen `Hello` shell
//! both ends read before any version is in force.
//!
//! The frames of the crate root are the in-memory vocabulary; a frame becomes
//! bytes only through a `Codec`, and the version that codec was built for
//! decides how. Each version's encoding lives in its own module here.

mod v4;
mod v5;

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{
    BatchDeferredOp, BatchEntry, BatchIllegal, ClientFrame, MAX_BATCH_OPS, MessageBody,
    SUPPORTED_VERSIONS, ServerFrame, VersionRange,
};

/// One websocket message, as either end writes or reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireMessage {
    Text(String),
    Binary(Vec<u8>),
}

impl WireMessage {
    /// The message's length in bytes, which is what a frame cap is compared
    /// with.
    pub fn len(&self) -> usize {
        match self {
            Self::Text(text) => text.len(),
            Self::Binary(bytes) => bytes.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Why a message is not a frame of the negotiated version.
///
/// `rule` is a short fixed name for the rule broken, fit for a server's
/// violation detail. `detail` is the parser's own message, which may quote
/// the offending input: for a client's diagnostics of its trusted peer only,
/// never echoed by a server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError {
    rule: &'static str,
    detail: Option<String>,
}

impl DecodeError {
    fn new(rule: &'static str) -> Self {
        Self { rule, detail: None }
    }

    fn with_detail(rule: &'static str, detail: impl fmt::Display) -> Self {
        Self {
            rule,
            detail: Some(detail.to_string()),
        }
    }

    pub fn rule(&self) -> &'static str {
        self.rule
    }

    pub fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }
}

/// The rule alone, or `rule: detail`.
impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.detail {
            Some(detail) => write!(f, "{}: {detail}", self.rule),
            None => f.write_str(self.rule),
        }
    }
}

impl std::error::Error for DecodeError {}

/// The encoding rules of one negotiated version: how each frame is written and
/// read, the batch law, and the frame cap derived from it.
///
/// **Encoding is infallible** and panics on a frame the version cannot write:
/// by the time one reaches an encoder it is a host or embedder bug.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Codec {
    version: Version,
}

/// The versions this build has a codec for. Every version-dependent method
/// matches on it exhaustively, so a version added here does not compile until
/// each method answers for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Version {
    V4,
    V5,
}

impl Codec {
    /// The codec of `version`.
    ///
    /// # Panics
    ///
    /// On a version outside [`SUPPORTED_VERSIONS`]: [`negotiate`](crate::negotiate)
    /// between two ranges inside it yields no other.
    pub fn negotiated(version: u32) -> Codec {
        assert!(
            SUPPORTED_VERSIONS.contains(version),
            "attach codec: version {version} is outside this build's {}..={}",
            SUPPORTED_VERSIONS.min,
            SUPPORTED_VERSIONS.max
        );
        let version = match version {
            4 => Version::V4,
            5 => Version::V5,
            // A supported version with no codec: caught by any test that
            // builds the codec of every supported version.
            other => panic!("attach codec: no codec for supported version {other}"),
        };
        Codec { version }
    }

    pub fn version(self) -> u32 {
        match self.version {
            Version::V4 => 4,
            Version::V5 => 5,
        }
    }

    /// Whether this version can write a body that is not text.
    pub(crate) fn carries_non_text_bodies(self) -> bool {
        match self.version {
            Version::V4 => v4::CARRIES_NON_TEXT_BODIES,
            Version::V5 => v5::CARRIES_NON_TEXT_BODIES,
        }
    }

    pub fn encode_client(self, frame: &ClientFrame) -> WireMessage {
        match self.version {
            Version::V4 => v4::encode_client(frame),
            Version::V5 => v5::encode_client(frame),
        }
    }

    pub fn encode_server(self, frame: &ServerFrame) -> WireMessage {
        match self.version {
            Version::V4 => v4::encode_server(frame),
            Version::V5 => v5::encode_server(frame),
        }
    }

    pub fn decode_client(self, msg: WireMessage) -> Result<ClientFrame, DecodeError> {
        match self.version {
            Version::V4 => v4::decode_client(msg),
            Version::V5 => v5::decode_client(msg),
        }
    }

    pub fn decode_server(self, msg: WireMessage) -> Result<ServerFrame, DecodeError> {
        match self.version {
            Version::V4 => v4::decode_server(msg),
            Version::V5 => v5::decode_server(msg),
        }
    }

    /// Whether this version can write `body` in a client frame.
    ///
    /// Every body under v5. Under v4, untyped text only: a server writes a
    /// typed text body to a v4 peer as its text, but a client has no way to
    /// state the type, so a typed body from a client would lose it. An
    /// embedder asks this before it hands
    /// the planes a body; a body this answers false for panics at the encoder.
    pub fn carries(self, body: &MessageBody) -> bool {
        match self.version {
            Version::V4 => v4::carries(body),
            Version::V5 => v5::carries(body),
        }
    }

    /// What a batch's two lists charge against the body cap.
    ///
    /// A batch is charged as a whole rather than per entry because it travels as
    /// one frame: what has to be bounded is the frame, and one budget spent
    /// across the operations bounds it whatever the mix. The charge includes
    /// every variable-length string an operation carries — its channel address
    /// as well as its body — because an address is client-composed and has no
    /// length of its own, so a rule that ignored it would bound nothing.
    pub fn batch_charged_bytes(
        self,
        publishes: &[BatchEntry],
        deferred_ops: &[BatchDeferredOp],
    ) -> usize {
        match self.version {
            Version::V4 => v4::batch_charged_bytes(publishes, deferred_ops),
            Version::V5 => v5::batch_charged_bytes(publishes, deferred_ops),
        }
    }

    /// Whether these two lists compose a legal [`ClientFrame::PublishBatch`] on
    /// an attachment whose body cap is `max_body_bytes`: at most
    /// [`MAX_BATCH_OPS`] operations, and at most `max_body_bytes` charged bytes.
    ///
    /// Protocol law, enforced at both ends of the same wire: the client checks it
    /// before composing the frame — an illegal batch is an embedder bug, since
    /// the buffer-time gates exist to prevent one — and the server checks it
    /// after decoding, where an illegal-but-decodable batch is a protocol
    /// violation. The two clauses are what
    /// [`max_client_frame_bytes`](Codec::max_client_frame_bytes) is derived
    /// from, so a legal batch is by construction a frame the peer will read.
    pub fn check_batch_legality(
        self,
        publishes: &[BatchEntry],
        deferred_ops: &[BatchDeferredOp],
        max_body_bytes: usize,
    ) -> Result<(), BatchIllegal> {
        match self.version {
            Version::V4 => v4::check_batch_legality(publishes, deferred_ops, max_body_bytes),
            Version::V5 => v5::check_batch_legality(publishes, deferred_ops, max_body_bytes),
        }
    }

    /// The largest client message this version lets through on an attachment
    /// whose body cap is `max_body_bytes` — derived, not a fixed constant,
    /// because the body cap is operator config and a fixed frame cap could
    /// contradict a legal config.
    ///
    /// Covers the worst legal frame of either publishing shape, because the
    /// batch law gives a batch the same byte budget a single
    /// [`ClientFrame::Publish`] gets. A server advertises it as
    /// [`Welcome::max_frame_bytes`](ServerFrame::Welcome).
    ///
    /// # Panics
    ///
    /// On a `max_body_bytes` large enough to overflow the derivation: a config
    /// contradiction, refused rather than answered with a wrong cap.
    pub fn max_client_frame_bytes(self, max_body_bytes: usize) -> usize {
        match self.version {
            Version::V4 => v4::max_client_frame_bytes(max_body_bytes),
            Version::V5 => v5::max_client_frame_bytes(max_body_bytes),
        }
    }

    /// The encoded size of the [`ClientFrame::PublishBatch`] these lists
    /// compose under `attribution`, measured against the widest correlation,
    /// so the answer never understates the frame that goes out. For a client's
    /// own frame-cap check.
    pub fn batch_frame_bytes(
        self,
        attribution: Option<&str>,
        publishes: &[BatchEntry],
        deferred_ops: &[BatchDeferredOp],
    ) -> usize {
        match self.version {
            Version::V4 => v4::batch_frame_bytes(attribution, publishes, deferred_ops),
            Version::V5 => v5::batch_frame_bytes(attribution, publishes, deferred_ops),
        }
    }
}

/// The batch law's two clauses, over a version's own count and charge.
fn check_law(ops: usize, bytes: usize, max_body_bytes: usize) -> Result<(), BatchIllegal> {
    if ops > MAX_BATCH_OPS {
        return Err(BatchIllegal::TooManyOps {
            ops,
            cap: MAX_BATCH_OPS,
        });
    }
    if bytes > max_body_bytes {
        return Err(BatchIllegal::TooManyBytes {
            bytes,
            cap: max_body_bytes,
        });
    }
    Ok(())
}

/// The largest [`Codec::max_client_frame_bytes`] over `versions`: what a route
/// sets as its websocket read cap at the upgrade, before a version is agreed.
/// A session then holds each message to its own version's cap.
///
/// # Panics
///
/// On an empty range or one reaching outside [`SUPPORTED_VERSIONS`]: a route
/// serves versions this build speaks.
pub fn max_transport_frame_bytes(versions: VersionRange, max_body_bytes: usize) -> usize {
    assert!(
        SUPPORTED_VERSIONS.contains_range(versions),
        "attach: served range {}..={} is empty or outside this build's",
        versions.min,
        versions.max
    );
    (versions.min..=versions.max)
        .map(|version| Codec::negotiated(version).max_client_frame_bytes(max_body_bytes))
        .max()
        .expect("a non-empty range")
}

// ---------------------------------------------------------------------------
// The frozen Hello shell
// ---------------------------------------------------------------------------

/// The one frame both ends read before a version is in force, in its frozen
/// shape: `{"type":"Hello","versions":{"min":..,"max":..},"ident":".."}`, a
/// text message in every version.
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum HelloShell {
    Hello {
        versions: VersionRange,
        ident: String,
    },
}

/// A text message's `type` tag, every other key ignored.
#[derive(Deserialize)]
struct TypeTag {
    #[serde(rename = "type")]
    tag: String,
}

/// What reading a message as the peer's `Hello` found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HelloRead {
    Hello {
        versions: VersionRange,
        ident: String,
    },
    /// A message that is not a `Hello`: a text message whose `type` tag names
    /// another kind, or a binary message. `kind` is the tag, or `binary
    /// frame`; peer-supplied, so for a client's diagnostics only.
    NotHello { kind: String },
    /// A text message with no readable `type` tag, or a `Hello` tag over a
    /// body that is not the frozen shell. `detail` is the parser's message,
    /// for a client's diagnostics only.
    Unparseable { detail: String },
}

fn encode_hello(versions: VersionRange, ident: &str) -> WireMessage {
    WireMessage::Text(
        serde_json::to_string(&HelloShell::Hello {
            versions,
            ident: ident.to_string(),
        })
        .expect("attach: the Hello shell serializes to JSON"),
    )
}

/// The attacher's `Hello`, as a text message.
pub fn encode_client_hello(versions: VersionRange, ident: &str) -> WireMessage {
    encode_hello(versions, ident)
}

/// The server's `Hello`, as a text message.
pub fn encode_server_hello(versions: VersionRange, ident: &str) -> WireMessage {
    encode_hello(versions, ident)
}

/// Classified by the `type` tag first, so a well-formed frame of another kind
/// is told apart from one that is not a frame at all.
fn read_hello(msg: &WireMessage) -> HelloRead {
    let text = match msg {
        WireMessage::Text(text) => text,
        WireMessage::Binary(_) => {
            return HelloRead::NotHello {
                kind: "binary frame".to_string(),
            };
        }
    };
    let tag = match serde_json::from_str::<TypeTag>(text) {
        Ok(TypeTag { tag }) => tag,
        Err(err) => {
            return HelloRead::Unparseable {
                detail: err.to_string(),
            };
        }
    };
    if tag != "Hello" {
        return HelloRead::NotHello { kind: tag };
    }
    match serde_json::from_str::<HelloShell>(text) {
        Ok(HelloShell::Hello { versions, ident }) => HelloRead::Hello { versions, ident },
        Err(err) => HelloRead::Unparseable {
            detail: err.to_string(),
        },
    }
}

/// Read the attacher's first message as its `Hello`.
pub fn read_client_hello(msg: &WireMessage) -> HelloRead {
    read_hello(msg)
}

/// Read the server's first message as its `Hello`.
pub fn read_server_hello(msg: &WireMessage) -> HelloRead {
    read_hello(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    const HELLO_TEXT: &str =
        r#"{"type":"Hello","versions":{"min":4,"max":5},"ident":"attach-client/0.1.0"}"#;

    fn text(s: &str) -> WireMessage {
        WireMessage::Text(s.to_string())
    }

    /// The shell writes the bytes a v4 `Hello` frame is written as.
    #[test]
    fn the_hello_shell_is_the_v4_hello_byte_for_byte() {
        let range = VersionRange { min: 4, max: 5 };
        assert_eq!(
            encode_client_hello(range, "attach-client/0.1.0"),
            text(HELLO_TEXT)
        );
        assert_eq!(
            encode_server_hello(range, "attach-client/0.1.0"),
            text(HELLO_TEXT)
        );
        assert_eq!(
            encode_client_hello(range, "attach-client/0.1.0"),
            Codec::negotiated(4).encode_client(&ClientFrame::Hello {
                versions: range,
                ident: "attach-client/0.1.0".to_string(),
            })
        );
    }

    /// The shell reads the same before a version is in force and under every
    /// codec.
    #[test]
    fn the_hello_shell_reads_the_same_before_and_under_every_codec() {
        let expected = HelloRead::Hello {
            versions: VersionRange { min: 4, max: 5 },
            ident: "attach-client/0.1.0".to_string(),
        };
        assert_eq!(read_client_hello(&text(HELLO_TEXT)), expected);
        assert_eq!(read_server_hello(&text(HELLO_TEXT)), expected);
        for version in SUPPORTED_VERSIONS.min..=SUPPORTED_VERSIONS.max {
            let codec = Codec::negotiated(version);
            assert_eq!(
                codec.decode_client(text(HELLO_TEXT)).unwrap(),
                ClientFrame::Hello {
                    versions: VersionRange { min: 4, max: 5 },
                    ident: "attach-client/0.1.0".to_string(),
                }
            );
            assert_eq!(
                codec.decode_server(text(HELLO_TEXT)).unwrap(),
                ServerFrame::Hello {
                    versions: VersionRange { min: 4, max: 5 },
                    ident: "attach-client/0.1.0".to_string(),
                }
            );
        }
    }

    #[test]
    fn another_kind_is_not_a_hello_whatever_its_fields() {
        assert_eq!(
            read_client_hello(&text(r#"{"type":"Subscribe","nonsense":true}"#)),
            HelloRead::NotHello {
                kind: "Subscribe".to_string()
            }
        );
        assert_eq!(
            read_server_hello(&text(r#"{"type":"Heartbeat"}"#)),
            HelloRead::NotHello {
                kind: "Heartbeat".to_string()
            }
        );
        assert_eq!(
            read_client_hello(&WireMessage::Binary(HELLO_TEXT.as_bytes().to_vec())),
            HelloRead::NotHello {
                kind: "binary frame".to_string()
            }
        );
    }

    #[test]
    fn a_message_with_no_tag_or_a_malformed_hello_is_unparseable() {
        for bad in [
            "not json",
            r#"["Hello"]"#,
            r#"{"versions":{"min":4,"max":4},"ident":"x"}"#,
            r#"{"type":7}"#,
            r#"{"type":"Hello","versions":"not-a-range","ident":"x"}"#,
            r#"{"type":"Hello","versions":{"min":4,"max":4}}"#,
            r#"{"type":"Hello","versions":{"min":4,"max":4},"ident":"x","extra":1}"#,
        ] {
            assert!(
                matches!(read_client_hello(&text(bad)), HelloRead::Unparseable { .. }),
                "{bad}"
            );
        }
        let HelloRead::Unparseable { detail } =
            read_server_hello(&text(r#"{"type":"Hello","versions":"x","ident":"y"}"#))
        else {
            panic!("a malformed Hello is unparseable");
        };
        assert!(detail.contains("VersionRange"), "{detail}");
    }

    #[test]
    fn every_supported_version_has_a_codec() {
        for version in SUPPORTED_VERSIONS.min..=SUPPORTED_VERSIONS.max {
            assert_eq!(Codec::negotiated(version).version(), version);
        }
    }

    #[test]
    #[should_panic(expected = "outside this build's")]
    fn a_codec_for_an_unspoken_version_panics() {
        Codec::negotiated(SUPPORTED_VERSIONS.max + 1);
    }

    #[test]
    fn the_transport_cap_is_the_largest_over_the_served_range() {
        let body = 64 * 1024;
        let expected = (SUPPORTED_VERSIONS.min..=SUPPORTED_VERSIONS.max)
            .map(|v| Codec::negotiated(v).max_client_frame_bytes(body))
            .max()
            .unwrap();
        assert_eq!(
            max_transport_frame_bytes(SUPPORTED_VERSIONS, body),
            expected
        );
        assert_eq!(
            max_transport_frame_bytes(VersionRange::exactly(4), body),
            Codec::negotiated(4).max_client_frame_bytes(body)
        );
    }

    #[test]
    #[should_panic(expected = "empty or outside")]
    fn an_empty_served_range_has_no_transport_cap() {
        max_transport_frame_bytes(VersionRange { min: 5, max: 4 }, 1024);
    }

    #[test]
    fn a_decode_error_renders_its_rule_then_its_detail() {
        assert_eq!(DecodeError::new("binary frame").to_string(), "binary frame");
        assert_eq!(
            DecodeError::with_detail("unparseable client frame", "eof").to_string(),
            "unparseable client frame: eof"
        );
    }
}
