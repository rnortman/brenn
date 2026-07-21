//! Server-side minting and parsing of the opaque [`Cursor`] a `Deliver` carries
//! and a `Subscribe` echoes.
//!
//! [`Cursor`] is opaque to the kernel: the client stores and echoes it verbatim,
//! never interpreting it. All interpretation lives here, on the server, where the
//! delivery class genuinely is the question. A durable cursor carries the
//! subscription's high-water rowid; an ephemeral cursor carries the delivered
//! row's `(bus epoch, ring seq)`.
//!
//! The wire encoding is a JSON string wrapped into a [`Cursor`] via serde. The
//! kernel never sees inside it, so the encoding can grow server-side with no wire
//! change — the opacity is what keeps future cursor state additive.

use brenn_surface_proto::Cursor;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// A parsed cursor's meaning, one variant per wire class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CursorState {
    /// A durable subscription's position, anchored to a store incarnation: the
    /// store's `generation` UUID and `incarnation` counter (the durable epoch),
    /// plus the `high_water` = max `messaging_messages.id` presented at the resume
    /// anchor or delivered this connection. The store identity is what lets the
    /// server catch a cursor minted against a store that was replaced, wiped, or
    /// restored from backup (the three stale-store arms). `confirm` is the
    /// below-water ack channel's confirm set: the message ids of the
    /// below-water rows delivered up to the frame this cursor was minted for,
    /// empty in the common case (no below-water send).
    Durable {
        generation: Uuid,
        incarnation: i64,
        high_water: i64,
        confirm: Vec<i64>,
    },
    /// An ephemeral subscription's position: the delivered row's `(bus epoch,
    /// ring seq)`.
    Ephemeral { epoch: Uuid, seq: u64 },
}

/// The internal serde shape of a cursor's inner JSON string. Private: only this
/// module builds or reads it, and the kernel never sees it.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "c")]
enum Wire {
    /// Durable: store generation + incarnation + high-water rowid + confirm set.
    /// Field names are terse (`g`, `i`, `hw`) to match the rest of the wire —
    /// this cursor rides every durable `Deliver`, and it is opaque, so there is no
    /// readability cost to pay for the bytes.
    D {
        #[serde(rename = "g")]
        generation: Uuid,
        #[serde(rename = "i")]
        incarnation: i64,
        hw: i64,
        /// The below-water ack confirm set. `default`/skip-if-empty so the common
        /// (empty) case adds no bytes and an older cursor without the field still
        /// parses.
        #[serde(rename = "cf", default, skip_serializing_if = "Vec::is_empty")]
        confirm: Vec<i64>,
    },
    /// Ephemeral: bus epoch + ring seq.
    E { epoch: Uuid, seq: u64 },
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

/// Mint a durable cursor from the store's identity, a subscription's high-water
/// rowid, and its current below-water confirm set (empty in the common
/// case).
pub fn mint_durable(
    generation: Uuid,
    incarnation: i64,
    high_water: i64,
    confirm: Vec<i64>,
) -> Cursor {
    wrap(&Wire::D {
        generation,
        incarnation,
        hw: high_water,
        confirm,
    })
}

/// Mint an ephemeral cursor from the delivered row's bus epoch and ring seq.
pub fn mint_ephemeral(epoch: Uuid, seq: u64) -> Cursor {
    wrap(&Wire::E { epoch, seq })
}

/// Parse an echoed [`Cursor`] back to its [`CursorState`]. `Err(reason)` when the
/// cursor is unparseable — a conforming client cannot produce one, so the caller
/// treats it as a protocol violation. The `reason` names *why* (malformed JSON,
/// unknown tag, wrong field types) so the violation log line that feeds fail2ban
/// carries a cause, not just a category.
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
        Ok(Wire::D {
            generation,
            incarnation,
            hw,
            confirm,
        }) => Ok(CursorState::Durable {
            generation,
            incarnation,
            high_water: hw,
            confirm,
        }),
        Ok(Wire::E { epoch, seq }) => Ok(CursorState::Ephemeral { epoch, seq }),
        Err(e) => Err(format!("malformed cursor encoding: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_mint_parse_round_trips() {
        let generation = Uuid::from_u128(0x1234);
        for (inc, hw, confirm) in [
            (0i64, 0i64, vec![]),
            (1, 1, vec![]),
            (7, 42, vec![3i64, 5, 41]),
            (i64::MAX, i64::MAX, vec![i64::MAX]),
        ] {
            let c = mint_durable(generation, inc, hw, confirm.clone());
            assert_eq!(
                parse(&c),
                Ok(CursorState::Durable {
                    generation,
                    incarnation: inc,
                    high_water: hw,
                    confirm,
                })
            );
        }
    }

    #[test]
    fn empty_confirm_set_adds_no_bytes_and_older_cursor_still_parses() {
        // The common (empty confirm) case serializes without the field...
        let c = mint_durable(Uuid::from_u128(0x1234), 3, 7, vec![]);
        let inner = match serde_json::to_value(&c) {
            Ok(Value::String(s)) => s,
            other => panic!("expected string cursor, got {other:?}"),
        };
        assert!(
            !inner.contains("cf"),
            "empty confirm set must not be serialized: {inner}"
        );
        // ...and a cursor minted before the field existed still parses to an empty set.
        let legacy: Cursor = serde_json::from_value(Value::String(
            r#"{"c":"D","g":"00000000-0000-0000-0000-000000001234","i":3,"hw":7}"#.into(),
        ))
        .unwrap();
        assert_eq!(
            parse(&legacy),
            Ok(CursorState::Durable {
                generation: Uuid::from_u128(0x1234),
                incarnation: 3,
                high_water: 7,
                confirm: vec![],
            })
        );
    }

    #[test]
    fn ephemeral_mint_parse_round_trips() {
        let epoch = Uuid::from_u128(0xabcd);
        for seq in [0u64, 1, 999, u64::MAX] {
            let c = mint_ephemeral(epoch, seq);
            assert_eq!(parse(&c), Ok(CursorState::Ephemeral { epoch, seq }));
        }
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
        let c = mint_durable(Uuid::from_u128(0x1234), 3, 7, vec![1, 2]);
        assert!(matches!(serde_json::to_value(&c), Ok(Value::String(_))));
    }
}
