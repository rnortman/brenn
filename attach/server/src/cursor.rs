//! Server-side minting and parsing of the opaque [`Cursor`] a `Deliver` carries
//! and a `Subscribe` echoes.
//!
//! [`Cursor`] is opaque to the attacher: it stores and echoes the token
//! verbatim, never interpreting it. All interpretation lives here, on the
//! server. One shape serves every channel class: a store cursor `(epoch, seq)`
//! wrapped in a session-owned envelope (`incarnation` and the channel the cursor
//! was minted on) the store cannot see.
//!
//! The channel is part of the encoding because a store position means nothing
//! outside the channel that assigned it, and the store cannot tell: every ring
//! store in the process shares one epoch, so a position minted on one ephemeral
//! channel resolves as a position in another's numbering. Carrying the channel
//! makes that answerable here, before the store ever sees the position.
//!
//! The wire encoding is a JSON string wrapped into a [`Cursor`] via serde. The
//! attacher never sees inside it, so the encoding can grow server-side with no
//! wire change — the opacity is what keeps future cursor state additive.

use brenn_attach_proto::Cursor;
use brenn_messaging_store::store::ResumeCursor;
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
///
/// `channel` is the address the position was minted on, so a cursor presented
/// for a different channel is answerable without asking the store — which cannot
/// tell, since every ring store in the process numbers under one shared epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorState {
    pub incarnation: i64,
    pub channel: String,
    pub resume: ResumeCursor,
}

/// The internal serde shape of a cursor's inner JSON string. Private: only this
/// module builds or reads it, and the attacher never sees it.
///
/// Field names are terse (`i`, `c`, `e`, `s`) to match the rest of the wire —
/// this cursor rides every `Deliver`, and it is opaque, so there is no
/// readability cost to pay for the bytes.
///
/// `c` is the channel address verbatim rather than a hash of it: a client holds
/// a cursor across restarts and upgrades, and a hash whose function changed
/// between builds would turn every held cursor into a mismatch.
#[derive(Debug, Serialize, Deserialize)]
struct Wire {
    #[serde(rename = "i")]
    incarnation: i64,
    #[serde(rename = "c")]
    channel: String,
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

/// Mint a cursor from the store's boot incarnation, the channel the position
/// belongs to, and the subscription's store position.
pub fn mint(incarnation: i64, channel: &str, resume: ResumeCursor) -> Cursor {
    wrap(&Wire {
        incarnation,
        channel: channel.to_string(),
        epoch: resume.epoch,
        seq: resume.seq,
    })
}

/// Parse an echoed [`Cursor`] back to its [`CursorState`]. `Err(reason)` when the
/// cursor is unparseable — a conforming attacher cannot produce one, so the
/// caller treats it as a protocol violation. The `reason` names *why* (malformed
/// JSON, missing fields, wrong field types) so the violation log line that feeds
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
            channel,
            epoch,
            seq,
        }) => Ok(CursorState {
            incarnation,
            channel,
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
            let c = mint(inc, "brenn:room", ResumeCursor { epoch, seq });
            assert_eq!(
                parse(&c),
                Ok(CursorState {
                    incarnation: inc,
                    channel: "brenn:room".to_string(),
                    resume: ResumeCursor { epoch, seq },
                })
            );
        }
    }

    /// Two channels' cursors are distinguishable even when their positions and
    /// epochs are identical — which is the ephemeral case, where every ring store
    /// numbers under one shared epoch and the store itself cannot tell them
    /// apart.
    #[test]
    fn cursors_for_two_channels_at_one_position_name_their_own_channel() {
        let epoch = Uuid::from_u128(0x5150);
        let resume = ResumeCursor { epoch, seq: 4 };
        let first = mint(3, "ephemeral:one", resume);
        let second = mint(3, "ephemeral:two", resume);
        assert_ne!(first, second);
        assert_eq!(parse(&first).unwrap().channel, "ephemeral:one");
        assert_eq!(parse(&second).unwrap().channel, "ephemeral:two");
    }

    /// One shape for both classes, named concretely: the encoding is the store
    /// position (`e`, `s`) plus the session envelope (`i`, `c`). Asserting the key set
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
            "ephemeral:room",
            ResumeCursor {
                epoch: Uuid::from_u128(0xabcd),
                seq: 9,
            },
        );
        assert_eq!(shape(&ring), vec!["c", "e", "i", "s"]);
        let durable = mint(
            3,
            "brenn:room",
            ResumeCursor {
                epoch: Uuid::from_u128(0xbeef),
                seq: 9,
            },
        );
        assert_eq!(shape(&durable), vec!["c", "e", "i", "s"]);
    }

    #[test]
    fn garbage_cursor_parses_to_err() {
        let bogus: Cursor = serde_json::from_value(Value::String("not-a-cursor".into())).unwrap();
        assert!(parse(&bogus).is_err());
        let wrong: Cursor = serde_json::from_value(Value::String(r#"{"c":"Z"}"#.into())).unwrap();
        assert!(parse(&wrong).is_err());
    }

    #[test]
    fn cursor_serializes_transparently_as_a_string() {
        let c = mint(
            3,
            "brenn:room",
            ResumeCursor {
                epoch: Uuid::from_u128(0x1234),
                seq: 7,
            },
        );
        assert!(matches!(serde_json::to_value(&c), Ok(Value::String(_))));
    }
}
