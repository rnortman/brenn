//! Server-side minting and parsing of the opaque [`Cursor`] a `Deliver` carries
//! and a `Subscribe` echoes.
//!
//! [`Cursor`] is opaque to the kernel: the client stores and echoes it verbatim,
//! never interpreting it. All interpretation lives here, on the server. One
//! shape serves both ring and durable classes: a store cursor `(epoch, seq)`
//! wrapped in a session-owned envelope (`incarnation` + below-water ack confirm
//! set) the store cannot see.
//!
//! The wire encoding is a JSON string wrapped into a [`Cursor`] via serde. The
//! kernel never sees inside it, so the encoding can grow server-side with no wire
//! change — the opacity is what keeps future cursor state additive.

use brenn_surface_proto::Cursor;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// A parsed cursor's meaning.
///
/// `epoch` + `seq` are the store cursor: the numbering domain that assigned the
/// position and the position itself. Both classes share one shape because a
/// store answers a resume against these two fields alone.
///
/// `incarnation` and `confirm` are the session's envelope around the store
/// cursor. `incarnation` is the store's boot counter at mint time — it catches
/// cursors minted under a boot the current store never counted (e.g. after a
/// backup restore). `confirm` is the below-water ack confirm set: the message
/// ids delivered below the high-water up to this frame, empty in the common
/// case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorState {
    pub incarnation: i64,
    pub epoch: Uuid,
    pub seq: u64,
    pub confirm: Vec<i64>,
}

/// The internal serde shape of a cursor's inner JSON string. Private: only this
/// module builds or reads it, and the kernel never sees it.
///
/// Field names are terse (`i`, `e`, `s`, `cf`) to match the rest of the wire —
/// this cursor rides every `Deliver`, and it is opaque, so there is no
/// readability cost to pay for the bytes.
#[derive(Debug, Serialize, Deserialize)]
struct Wire {
    #[serde(rename = "i")]
    incarnation: i64,
    #[serde(rename = "e")]
    epoch: Uuid,
    #[serde(rename = "s")]
    seq: u64,
    /// The below-water ack confirm set. `default`/skip-if-empty so the common
    /// (empty) case adds no bytes.
    #[serde(rename = "cf", default, skip_serializing_if = "Vec::is_empty")]
    confirm: Vec<i64>,
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

/// Mint a cursor from the store's boot incarnation, the delivered row's
/// `(epoch, seq)` store position, and the subscription's current below-water
/// confirm set (empty in the common case, always empty on a ring-backed
/// subscription).
pub fn mint(incarnation: i64, epoch: Uuid, seq: u64, confirm: Vec<i64>) -> Cursor {
    wrap(&Wire {
        incarnation,
        epoch,
        seq,
        confirm,
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
            confirm,
        }) => Ok(CursorState {
            incarnation,
            epoch,
            seq,
            confirm,
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
        for (inc, seq, confirm) in [
            (0i64, 0u64, vec![]),
            (1, 1, vec![]),
            (7, 42, vec![3i64, 5, 41]),
            (i64::MAX, u64::MAX, vec![i64::MAX]),
        ] {
            let c = mint(inc, epoch, seq, confirm.clone());
            assert_eq!(
                parse(&c),
                Ok(CursorState {
                    incarnation: inc,
                    epoch,
                    seq,
                    confirm,
                })
            );
        }
    }

    /// One shape for both classes, named concretely: the encoding is the store
    /// position (`e`, `s`) plus the session envelope (`i`, and `cf` only when the
    /// confirm set is non-empty). Asserting the key set rather than comparing two
    /// mints against each other is what makes this fail on a re-split enum, a
    /// re-introduced class tag, or a renamed field — all of which two mints from
    /// one `mint` would carry identically.
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
        // A ring-backed subscription has no below-water ack channel, so its
        // confirm set is always empty.
        let ring = mint(3, Uuid::from_u128(0xabcd), 9, vec![]);
        assert_eq!(shape(&ring), vec!["e", "i", "s"]);
        // A durable one carries the same position fields plus its evidence.
        let durable = mint(3, Uuid::from_u128(0xbeef), 9, vec![41]);
        assert_eq!(shape(&durable), vec!["cf", "e", "i", "s"]);
    }

    #[test]
    fn empty_confirm_set_adds_no_bytes() {
        let c = mint(3, Uuid::from_u128(0x1234), 7, vec![]);
        let inner = match serde_json::to_value(&c) {
            Ok(Value::String(s)) => s,
            other => panic!("expected string cursor, got {other:?}"),
        };
        assert!(
            !inner.contains("cf"),
            "empty confirm set must not be serialized: {inner}"
        );
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
        let c = mint(3, Uuid::from_u128(0x1234), 7, vec![1, 2]);
        assert!(matches!(serde_json::to_value(&c), Ok(Value::String(_))));
    }
}
