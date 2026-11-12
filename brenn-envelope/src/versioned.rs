//! The versioned JSON body codec every body family in this crate shares.
//!
//! A versioned body is a JSON object carrying an integer `v` beside the
//! family's own fields. [`encode`] stamps the family's version; [`decode`]
//! refuses anything that is not a JSON object carrying exactly that version,
//! then hands the whole object to the target type. Unknown fields are ignored,
//! so a family adds fields without moving its version.
//!
//! Each family wraps the two functions with its own version constant
//! ([`chat::encode`](crate::chat::encode), [`agent::encode`](crate::agent::encode)).

use serde::Serialize;
use serde::de::DeserializeOwned;

/// Why a versioned body could not be decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionedDecodeError {
    /// The body is not parseable JSON.
    NotJson(String),
    /// The body parsed but is not a JSON object.
    NotAnObject,
    /// No `v` field, or its value is not a non-negative integer.
    MissingVersion,
    /// The `v` field names a version this build does not implement.
    UnsupportedVersion(u64),
    /// The body is a versioned JSON object but does not match the target type
    /// (unknown `type`, missing or ill-typed field).
    Payload(String),
}

impl core::fmt::Display for VersionedDecodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::NotJson(e) => write!(f, "body is not JSON: {e}"),
            Self::NotAnObject => write!(f, "body is not a JSON object"),
            Self::MissingVersion => write!(f, "body has no integer `v` field"),
            Self::UnsupportedVersion(v) => {
                write!(
                    f,
                    "body declares version {v}, which this build does not implement"
                )
            }
            Self::Payload(e) => write!(f, "body does not match the message schema: {e}"),
        }
    }
}

impl std::error::Error for VersionedDecodeError {}

/// Serialize `body`, stamping `"v": version`.
///
/// # Panics
///
/// If `body` does not serialize to a JSON object. Every body type of every
/// family does; anything else is a programming error.
pub fn encode<T: Serialize>(version: u32, body: &T) -> String {
    #[derive(Serialize)]
    struct Versioned<'a, T> {
        v: u32,
        #[serde(flatten)]
        body: &'a T,
    }

    serde_json::to_string(&Versioned { v: version, body })
        .expect("a versioned body serializes to a JSON object")
}

/// Parse `body`, requiring a `v` field equal to `version`.
pub fn decode<T: DeserializeOwned>(version: u32, body: &str) -> Result<T, VersionedDecodeError> {
    let value: serde_json::Value =
        serde_json::from_str(body).map_err(|e| VersionedDecodeError::NotJson(e.to_string()))?;
    let object = value.as_object().ok_or(VersionedDecodeError::NotAnObject)?;
    let got = object
        .get("v")
        .and_then(serde_json::Value::as_u64)
        .ok_or(VersionedDecodeError::MissingVersion)?;
    if got != u64::from(version) {
        return Err(VersionedDecodeError::UnsupportedVersion(got));
    }
    serde_json::from_value(value).map_err(|e| VersionedDecodeError::Payload(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use serde_json::{Value, json};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Body {
        n: u32,
    }

    #[test]
    fn encode_stamps_the_version_beside_the_fields() {
        let value: Value = serde_json::from_str(&encode(7, &Body { n: 3 })).unwrap();
        assert_eq!(value, json!({"v": 7, "n": 3}));
    }

    #[test]
    fn a_body_round_trips_and_ignores_unknown_fields() {
        assert_eq!(
            decode::<Body>(7, &encode(7, &Body { n: 3 })),
            Ok(Body { n: 3 })
        );
        assert_eq!(
            decode::<Body>(7, r#"{"v":7,"n":3,"added_later":true}"#),
            Ok(Body { n: 3 })
        );
    }

    #[test]
    fn framing_and_version_failures_are_distinguished() {
        assert!(matches!(
            decode::<Body>(1, "not json"),
            Err(VersionedDecodeError::NotJson(_))
        ));
        assert_eq!(
            decode::<Body>(1, "[1,2,3]"),
            Err(VersionedDecodeError::NotAnObject)
        );
        assert_eq!(
            decode::<Body>(1, r#"{"n":3}"#),
            Err(VersionedDecodeError::MissingVersion)
        );
        assert_eq!(
            decode::<Body>(1, r#"{"v":"1","n":3}"#),
            Err(VersionedDecodeError::MissingVersion)
        );
        assert_eq!(
            decode::<Body>(1, r#"{"v":2,"n":3}"#),
            Err(VersionedDecodeError::UnsupportedVersion(2))
        );
        assert!(matches!(
            decode::<Body>(1, r#"{"v":1}"#),
            Err(VersionedDecodeError::Payload(_))
        ));
    }
}
