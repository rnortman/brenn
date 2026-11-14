//! Named references to blobs, carried beside a message's body.
//!
//! A blob is an immutable byte object held by the host's blob store; a message
//! names the blobs it carries in an [`Attachments`] map from
//! [`AttachmentName`] to [`BlobRef`]. These are pure data, compiled for the
//! host, the WASM guests and the page alike: nothing here can read or create a
//! blob, and outside the host a reference is metadata.
//!
//! The rules on names and on the number of attachments are enforced where a
//! value is constructed ([`AttachmentName::new`], and the host's held
//! attachment set). `Deserialize` does not re-check them: the host wrote every
//! value a reader sees, and a reader that validated would refuse a later host
//! whose rules are wider.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

/// The most attachments one message may carry.
///
/// A worst-case reference is about 400 bytes serialized, so a full set adds
/// about 13 KiB to a message header.
pub const MAX_ATTACHMENTS_PER_MESSAGE: usize = 32;

/// The longest attachment name, in bytes.
pub const MAX_ATTACHMENT_NAME_LEN: usize = 64;

/// A blob's identity: opaque text minted by the host.
///
/// Readers must not parse it or rely on its form; the host may change how it
/// mints ids without changing this contract. Holding an id grants nothing
/// outside the host.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BlobId(String);

impl BlobId {
    /// Wrap an id the host's blob store minted, or one read back from a value
    /// the host wrote. Performs no check.
    pub fn new(id: String) -> Self {
        BlobId(id)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A reference to a sealed blob: its id, its size in bytes and its content
/// type, as the host's store recorded them at seal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobRef {
    pub id: BlobId,
    pub size: u64,
    pub content_type: String,
}

/// The name a message gives one of its attachments.
///
/// 1 to [`MAX_ATTACHMENT_NAME_LEN`] bytes; the first is ASCII alphanumeric,
/// the rest ASCII alphanumeric or one of `.`, `_`, `-`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AttachmentName(String);

/// Why a string is not an [`AttachmentName`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttachmentNameError {
    Empty,
    TooLong {
        len: usize,
    },
    /// The byte at `index` is not allowed there.
    InvalidByte {
        index: usize,
        byte: u8,
    },
}

impl fmt::Display for AttachmentNameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AttachmentNameError::Empty => f.write_str("attachment name is empty"),
            AttachmentNameError::TooLong { len } => write!(
                f,
                "attachment name is {len} bytes, longer than {MAX_ATTACHMENT_NAME_LEN}"
            ),
            AttachmentNameError::InvalidByte { index, byte } => write!(
                f,
                "attachment name has byte {byte:#04x} at index {index}; \
                 names are [A-Za-z0-9] then [A-Za-z0-9._-]"
            ),
        }
    }
}

impl std::error::Error for AttachmentNameError {}

impl AttachmentName {
    pub fn new(name: impl Into<String>) -> Result<Self, AttachmentNameError> {
        let name = name.into();
        let bytes = name.as_bytes();
        if bytes.is_empty() {
            return Err(AttachmentNameError::Empty);
        }
        if bytes.len() > MAX_ATTACHMENT_NAME_LEN {
            return Err(AttachmentNameError::TooLong { len: bytes.len() });
        }
        for (index, &byte) in bytes.iter().enumerate() {
            let allowed =
                byte.is_ascii_alphanumeric() || (index > 0 && matches!(byte, b'.' | b'_' | b'-'));
            if !allowed {
                return Err(AttachmentNameError::InvalidByte { index, byte });
            }
        }
        Ok(AttachmentName(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AttachmentName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A message's attachments, ordered by name so every serialization is
/// deterministic. Empty on a message that carries none.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Attachments(BTreeMap<AttachmentName, BlobRef>);

impl Attachments {
    pub fn new() -> Self {
        Attachments(BTreeMap::new())
    }

    /// Wrap a map the host built or read back. Performs no check on the
    /// number of entries; the host's held attachment set enforces it.
    pub fn from_map(map: BTreeMap<AttachmentName, BlobRef>) -> Self {
        Attachments(map)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn get(&self, name: &AttachmentName) -> Option<&BlobRef> {
        self.0.get(name)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&AttachmentName, &BlobRef)> {
        self.0.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(s: &str) -> AttachmentName {
        AttachmentName::new(s).unwrap()
    }

    #[test]
    fn attachment_name_accepts_the_grammar() {
        for ok in ["a", "Z", "0", "photo", "IMG_0001.jpg", "a-b.c_d", "9-"] {
            assert_eq!(AttachmentName::new(ok).unwrap().as_str(), ok);
        }
        let longest = "x".repeat(MAX_ATTACHMENT_NAME_LEN);
        assert!(AttachmentName::new(longest).is_ok());
    }

    #[test]
    fn attachment_name_refuses_outside_the_grammar() {
        assert_eq!(AttachmentName::new(""), Err(AttachmentNameError::Empty));
        assert_eq!(
            AttachmentName::new("x".repeat(MAX_ATTACHMENT_NAME_LEN + 1)),
            Err(AttachmentNameError::TooLong {
                len: MAX_ATTACHMENT_NAME_LEN + 1
            })
        );
        for (bad, index, byte) in [
            (".hidden", 0, b'.'),
            ("_x", 0, b'_'),
            ("-x", 0, b'-'),
            ("a b", 1, b' '),
            ("a/b", 1, b'/'),
            ("a:b", 1, b':'),
        ] {
            assert_eq!(
                AttachmentName::new(bad),
                Err(AttachmentNameError::InvalidByte { index, byte }),
                "{bad:?}"
            );
        }
        // A non-ASCII name is refused at its first non-ASCII byte.
        assert_eq!(
            AttachmentName::new("aé"),
            Err(AttachmentNameError::InvalidByte {
                index: 1,
                byte: 0xc3
            })
        );
    }

    #[test]
    fn attachments_serialize_in_name_order() {
        let r = |id: &str, size, ct: &str| BlobRef {
            id: BlobId::new(id.to_owned()),
            size,
            content_type: ct.to_owned(),
        };
        let mut map = BTreeMap::new();
        map.insert(name("zeta"), r("bb", 2, "text/plain"));
        map.insert(name("alpha"), r("aa", 102400, "image/jpeg"));
        let attachments = Attachments::from_map(map);
        let json = serde_json::to_string(&attachments).unwrap();
        assert_eq!(
            json,
            r#"{"alpha":{"id":"aa","size":102400,"content_type":"image/jpeg"},"zeta":{"id":"bb","size":2,"content_type":"text/plain"}}"#
        );
        let back: Attachments = serde_json::from_str(&json).unwrap();
        assert_eq!(back, attachments);
    }

    #[test]
    fn deserialize_does_not_recheck_the_name_grammar() {
        // A later host may widen the grammar; a reader keeps what it is given.
        let json = r#"{"a name with spaces":{"id":"x","size":0,"content_type":"a/b"}}"#;
        let back: Attachments = serde_json::from_str(json).unwrap();
        assert_eq!(back.len(), 1);
        assert_eq!(back.iter().next().unwrap().0.as_str(), "a name with spaces");
    }
}
