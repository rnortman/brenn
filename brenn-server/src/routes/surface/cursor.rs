//! Server-side minting and parsing of the opaque [`Cursor`] a `Deliver` carries
//! and a `Subscribe` echoes.
//!
//! [`Cursor`] is opaque to the kernel: the client stores and echoes it verbatim,
//! never interpreting it. All interpretation lives here, on the server. One
//! shape serves both ring and durable classes: a store cursor `(epoch, seq)`
//! wrapped in a session-owned envelope (`incarnation`) the store cannot see.
//!
//! The wire encoding is a JSON string wrapped into a [`Cursor`] via serde. The
//! kernel never sees inside it, so the encoding can grow server-side with no wire
//! change — the opacity is what keeps future cursor state additive.

use brenn_lib::messaging::store::ResumeCursor;
use brenn_surface_proto::Cursor;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// A parsed cursor's meaning.
///
/// `resume` is the store's own resume position — the numbering domain that
/// assigned the position and the position itself, carried as the store's type
/// rather than re-assembled field by field at each subscribe. Every channel
/// shares one shape because a store answers a resume against those two fields
/// alone.
///
/// `incarnation` is the session's envelope around the store position: the
/// store's boot counter at mint time, which catches positions minted under a
/// boot the current store never counted (e.g. after a backup restore).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorState {
    pub incarnation: i64,
    pub resume: ResumeCursor,
}

/// The internal serde shape of a cursor's inner JSON string. Private: only this
/// module builds or reads it, and the kernel never sees it.
///
/// Field names are terse (`i`, `e`, `s`) to match the rest of the wire — this
/// cursor rides every `Deliver`, and it is opaque, so there is no readability
/// cost to pay for the bytes.
#[derive(Debug, Serialize, Deserialize)]
struct Wire {
    #[serde(rename = "i")]
    incarnation: i64,
    #[serde(rename = "e")]
    epoch: Uuid,
    #[serde(rename = "s")]
    seq: u64,
}

/// Wrap an internal [`Wire`] into an opaque [`Cursor`] via the sanctioned serde
/// round-trip: serialize to a JSON string, then build the newtype from a
/// `Value::String`. The `Cursor` newtype has no constructor, so this round-trip
/// is the only way to mint one.
fn wrap(wire: &Wire) -> Cursor {
    let inner = serde_json::to_string(wire).expect("cursor Wire serialization is infallible");
    serde_json::from_value(Value::String(inner))
        .expect("a JSON string always deserializes into a transparent Cursor newtype")
}

/// Mint a cursor from the store's boot incarnation and the subscription's store
/// position.
pub fn mint(incarnation: i64, resume: ResumeCursor) -> Cursor {
    wrap(&Wire {
        incarnation,
        epoch: resume.epoch,
        seq: resume.seq,
    })
}

/// Parse an echoed [`Cursor`] back to its [`CursorState`]. `Err(reason)` when the
/// cursor is unparseable — a conforming client cannot produce one, so the caller
/// treats it as a protocol violation. The `reason` names *why* (malformed JSON,
/// missing fields, wrong field types) so the violation log line that feeds
/// fail2ban carries a cause, not just a category.
pub fn parse(cursor: &Cursor) -> Result<CursorState, String> {
    // The sanctioned read: a `Cursor` serializes transparently to a JSON string.
    let inner = match serde_json::to_value(cursor) {
        Ok(Value::String(s)) => s,
        other => {
            return Err(format!(
                "cursor did not serialize to a JSON string: {other:?}"
            ));
        }
    };
    match serde_json::from_str::<Wire>(&inner) {
        Ok(Wire {
            incarnation,
            epoch,
            seq,
        }) => Ok(CursorState {
            incarnation,
            resume: ResumeCursor { epoch, seq },
        }),
        Err(e) => Err(format!("malformed cursor encoding: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mint_parse_round_trips() {
        let epoch = Uuid::from_u128(0x1234);
        for (inc, seq) in [(0i64, 0u64), (1, 1), (7, 42), (i64::MAX, u64::MAX)] {
            let c = mint(inc, ResumeCursor { epoch, seq });
            assert_eq!(
                parse(&c),
                Ok(CursorState {
                    incarnation: inc,
                    resume: ResumeCursor { epoch, seq },
                })
            );
        }
    }

    /// One shape for both classes, named concretely: the encoding is the store
    /// position (`e`, `s`) plus the session envelope (`i`). Asserting the key set
    /// rather than comparing two mints against each other is what makes this fail
    /// on a re-split enum, a re-introduced class tag, or a renamed field — all of
    /// which two mints from one `mint` would carry identically.
    #[test]
    fn ring_and_durable_positions_share_one_encoding() {
        // The key *set*, not its order — the object's iteration order is serde's
        // business, the field names are the contract.
        let shape = |c: &Cursor| match serde_json::to_value(c) {
            Ok(Value::String(s)) => {
                let mut keys = serde_json::from_str::<serde_json::Value>(&s)
                    .expect("inner cursor JSON")
                    .as_object()
                    .expect("cursor object")
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>();
                keys.sort();
                keys
            }
            other => panic!("expected string cursor, got {other:?}"),
        };
        let ring = mint(
            3,
            ResumeCursor {
                epoch: Uuid::from_u128(0xabcd),
                seq: 9,
            },
        );
        assert_eq!(shape(&ring), vec!["e", "i", "s"]);
        let durable = mint(
            3,
            ResumeCursor {
                epoch: Uuid::from_u128(0xbeef),
                seq: 9,
            },
        );
        assert_eq!(shape(&durable), vec!["e", "i", "s"]);
    }

    #[test]
    fn garbage_cursor_parses_to_err() {
        // A cursor whose inner string is not the cursor encoding at all.
        let bogus: Cursor = serde_json::from_value(Value::String("not-a-cursor".into())).unwrap();
        assert!(parse(&bogus).is_err());
        // A cursor whose inner string is JSON but the wrong shape.
        let wrong: Cursor = serde_json::from_value(Value::String(r#"{"c":"Z"}"#.into())).unwrap();
        assert!(parse(&wrong).is_err());
    }

    #[test]
    fn cursor_serializes_transparently_as_a_string() {
        let c = mint(
            3,
            ResumeCursor {
                epoch: Uuid::from_u128(0x1234),
                seq: 7,
            },
        );
        assert!(matches!(serde_json::to_value(&c), Ok(Value::String(_))));
    }
}
