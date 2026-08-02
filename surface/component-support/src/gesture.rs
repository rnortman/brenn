//! The gesture dialect: what a sync-call activation's reply says about the
//! browser event that caused it.
//!
//! A sync reply is an opaque string to the kernel — a private dialect between a
//! component's activation entry and its own gesture wiring. This is the one the
//! SDK speaks, and the only thing it can say is whether the browser's default
//! action should be suppressed: `{"cancel":true}` or `{"cancel":false}`.
//!
//! DOM-free and host-tested, so the two halves of the dialect are pinned against
//! each other under the ordinary test sweep rather than only in a browser.

/// The one key the dialect has: whether the wiring should call
/// `preventDefault()` on the originating event.
const CANCEL_KEY: &str = "cancel";

/// The gesture dialect reply for a sync activation: `Some` always, so it drops
/// straight into an entry's `Ok(gesture_reply(true))`.
///
/// A gesture entry that does not care about the browser's default action answers
/// `Ok(None)` instead — no reply at all — which the wiring reads as "do not
/// cancel". This exists for the entry that has decided.
pub fn gesture_reply(cancel: bool) -> Option<String> {
    Some(format!("{{\"{CANCEL_KEY}\":{cancel}}}"))
}

/// Read a gesture reply: does the entry want the browser's default suppressed?
///
/// `None` — the entry answered nothing — is not a cancellation. A `Some` that is
/// not the dialect is a component talking to itself in two languages, which no
/// caller can act on, so it is returned as an error for the wiring to fault on
/// rather than silently read as "do not cancel".
///
/// The reading half is the gesture wiring's, so it exists on the browser target
/// and under test — where it is pinned against the writing half — and nowhere
/// else. The writing half is ungated: an entry composes its reply in the
/// host-tested logic a component's state machine lives in.
#[cfg(any(target_arch = "wasm32", test))]
pub(crate) fn reply_cancels(reply: Option<&str>) -> Result<bool, String> {
    let Some(reply) = reply else {
        return Ok(false);
    };
    let parsed: serde_json::Value = serde_json::from_str(reply).map_err(|err| err.to_string())?;
    parsed
        .get(CANCEL_KEY)
        .and_then(|value| value.as_bool())
        .ok_or_else(|| format!("no boolean {CANCEL_KEY} in {reply:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reply_round_trips_through_the_dialect() {
        // The two halves are written independently — an entry composes, a wiring
        // parses — and nothing else pins that they agree.
        for cancel in [true, false] {
            let reply = gesture_reply(cancel).expect("the dialect always answers");
            assert_eq!(reply_cancels(Some(&reply)), Ok(cancel));
        }
    }

    #[test]
    fn the_dialect_is_the_shape_the_contract_documents() {
        // A component may hand-roll the reply instead of calling the SDK, so the
        // literal spelling is the contract, not an implementation detail.
        assert_eq!(
            gesture_reply(true).as_deref(),
            Some(r#"{"cancel":true}"#),
            "the reply is a one-field object spelled exactly this way"
        );
    }

    #[test]
    fn no_reply_is_not_a_cancellation() {
        // The common gesture — an ack, a dismiss — answers nothing at all, and the
        // browser's default must proceed for it.
        assert_eq!(reply_cancels(None), Ok(false));
    }

    #[test]
    fn a_reply_outside_the_dialect_is_an_error_rather_than_a_false() {
        // Reading gibberish as "do not cancel" would turn a component bug into a
        // gesture that silently stopped working.
        assert!(reply_cancels(Some("not json")).is_err());
        assert!(reply_cancels(Some(r#"{"cancelled":true}"#)).is_err());
    }
}
