//! The canonical [`GateRefusal`] → guest-error classification, for both hosts.
//!
//! [`GateRefusal`] says which check refused a call. It does not say what the
//! guest is told, and that is a separate decision: the same verdict answers
//! differently depending on which call the guest made. An oversize body is an
//! invalid payload when it was *published* and a quota refusal when it was an
//! *edit*, because an edit body is charged against the activation's byte
//! aggregate like any other body. An unrepresentable release time is an invalid
//! payload on the publish path and gets its own answer on the control-op path,
//! because collapsing it into a quota refusal would tell the guest to retry
//! later — and no later is any better.
//!
//! There is deliberately no `NotPermitted` kind. That verdict is the wiring and
//! allowlist answer — an unbound port, an ACL that does not cover the channel —
//! and it is produced by a host reading host-owned maps, outside the gate
//! entirely. A kind for it here would invite a host to route it through this
//! classification and lose the distinction.

use crate::GateRefusal;

/// What a guest is told about a refused call, in the vocabulary both hosts'
/// error types share.
///
/// The variants are the intersection of the WIT `publish-error`/`defer-error`
/// pair and their surface-contract twins, minus `not-permitted` and
/// `out-of-range` — the two answers no gate verdict can produce.
///
/// TODO(budget-refusal-per-path): one enum over two paths leaves each host an
/// `unreachable!` arm per path, and always builds the `InvalidPayload` detail
/// even for the host that drops it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefusalKind {
    /// The call's argument is wrong, with the detail a guest needs to fix it.
    InvalidPayload(String),
    /// A control op's release time is not a representable timestamp. Distinct
    /// from a quota refusal precisely so the guest does not retry.
    InvalidDeliverAfter,
    /// A budget is spent: a per-activation cap or a sink's token bucket.
    QuotaExceeded,
}

/// The classification a guest sees on the publish path.
///
/// An oversize body and an impossible release time are facts about the
/// argument; everything else is a budget.
pub fn publish_refusal_kind(refusal: GateRefusal) -> RefusalKind {
    match refusal {
        GateRefusal::BodyTooLarge { len, max } => {
            RefusalKind::InvalidPayload(format!("payload {len} bytes exceeds max {max}"))
        }
        GateRefusal::UnrepresentableDeliverAfter { ms } => RefusalKind::InvalidPayload(format!(
            "deliver_after {ms} ms is not a representable timestamp"
        )),
        GateRefusal::CallCap { .. }
        | GateRefusal::SinkExhausted
        | GateRefusal::EntryCap { .. }
        | GateRefusal::ByteCap { .. }
        | GateRefusal::OpCap { .. } => RefusalKind::QuotaExceeded,
    }
}

/// The classification a guest sees on the control-op path.
///
/// An impossible release time answers for itself. An oversize edit body is a
/// quota refusal, because the body is charged against the activation's
/// aggregate like any other.
pub fn defer_refusal_kind(refusal: GateRefusal) -> RefusalKind {
    match refusal {
        GateRefusal::UnrepresentableDeliverAfter { .. } => RefusalKind::InvalidDeliverAfter,
        GateRefusal::CallCap { .. }
        | GateRefusal::BodyTooLarge { .. }
        | GateRefusal::SinkExhausted
        | GateRefusal::EntryCap { .. }
        | GateRefusal::ByteCap { .. }
        | GateRefusal::OpCap { .. } => RefusalKind::QuotaExceeded,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_published_body_over_the_cap_is_the_guests_fault_and_says_the_numbers() {
        assert_eq!(
            publish_refusal_kind(GateRefusal::BodyTooLarge { len: 9, max: 8 }),
            RefusalKind::InvalidPayload("payload 9 bytes exceeds max 8".to_string())
        );
    }

    #[test]
    fn an_impossible_release_time_is_a_bad_publish_but_its_own_defer_answer() {
        assert_eq!(
            publish_refusal_kind(GateRefusal::UnrepresentableDeliverAfter { ms: 42 }),
            RefusalKind::InvalidPayload(
                "deliver_after 42 ms is not a representable timestamp".to_string()
            )
        );
        assert_eq!(
            defer_refusal_kind(GateRefusal::UnrepresentableDeliverAfter { ms: 42 }),
            RefusalKind::InvalidDeliverAfter
        );
    }

    #[test]
    fn an_edit_body_over_the_cap_is_a_quota_refusal() {
        assert_eq!(
            defer_refusal_kind(GateRefusal::BodyTooLarge { len: 9, max: 8 }),
            RefusalKind::QuotaExceeded
        );
    }

    #[test]
    fn every_counter_and_bucket_refusal_is_quota_on_both_paths() {
        for refusal in [
            GateRefusal::CallCap { cap: 4 },
            GateRefusal::SinkExhausted,
            GateRefusal::EntryCap { cap: 2 },
            GateRefusal::ByteCap { cap: 16 },
            GateRefusal::OpCap { cap: 3 },
        ] {
            assert_eq!(publish_refusal_kind(refusal), RefusalKind::QuotaExceeded);
            assert_eq!(defer_refusal_kind(refusal), RefusalKind::QuotaExceeded);
        }
    }

    #[test]
    fn the_publish_path_never_answers_invalid_deliver_after() {
        // That answer exists to stop a guest retrying an op. A publish carrying
        // an impossible release time is told its argument is wrong instead, with
        // the number in it, because there is no op to reissue.
        for refusal in [
            GateRefusal::CallCap { cap: 4 },
            GateRefusal::BodyTooLarge { len: 9, max: 8 },
            GateRefusal::UnrepresentableDeliverAfter { ms: u64::MAX },
            GateRefusal::SinkExhausted,
            GateRefusal::EntryCap { cap: 2 },
            GateRefusal::ByteCap { cap: 16 },
            GateRefusal::OpCap { cap: 3 },
        ] {
            assert_ne!(
                publish_refusal_kind(refusal),
                RefusalKind::InvalidDeliverAfter
            );
        }
    }
}
