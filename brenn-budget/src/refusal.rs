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

use std::fmt;

use crate::GateRefusal;

/// What is wrong with a published argument, with the numbers a guest needs to
/// fix it. `Display` is the diagnostic a host hands a guest whose error
/// vocabulary carries one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadFault {
    /// The body exceeds the per-message cap.
    BodyTooLarge { len: usize, max: usize },
    /// The release time is not a representable timestamp.
    UnrepresentableDeliverAfter { ms: u64 },
}

impl fmt::Display for PayloadFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PayloadFault::BodyTooLarge { len, max } => {
                write!(f, "payload {len} bytes exceeds max {max}")
            }
            PayloadFault::UnrepresentableDeliverAfter { ms } => {
                write!(f, "deliver_after {ms} ms is not a representable timestamp")
            }
        }
    }
}

/// What a guest is told about a refused publish or peer call: the half both
/// hosts' publish-error and call-error types share, minus `not-permitted`, the
/// one answer no gate verdict can produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishRefusalKind {
    /// The call's argument is wrong; the detail says how. This includes an
    /// impossible release time: a publish has no op to reissue, so the guest is
    /// told its argument is wrong, with the number in it, rather than being
    /// given the control-op path's do-not-retry answer.
    InvalidPayload(PayloadFault),
    /// A budget is spent: a per-activation cap or a sink's token bucket.
    QuotaExceeded,
}

/// What a guest is told about a refused deferred-message control op: the half
/// both hosts' defer-error types share, minus `not-permitted` and
/// `out-of-range`, the two answers no gate verdict can produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeferRefusalKind {
    /// The op's release time is not a representable timestamp. Distinct from a
    /// quota refusal precisely so the guest does not retry.
    InvalidDeliverAfter,
    /// A budget is spent: a per-activation cap or a sink's token bucket.
    QuotaExceeded,
}

/// The classification a guest sees on the publish path.
///
/// An oversize body and an impossible release time are facts about the
/// argument; everything else is a budget.
pub fn publish_refusal_kind(refusal: GateRefusal) -> PublishRefusalKind {
    match refusal {
        GateRefusal::BodyTooLarge { len, max } => {
            PublishRefusalKind::InvalidPayload(PayloadFault::BodyTooLarge { len, max })
        }
        GateRefusal::UnrepresentableDeliverAfter { ms } => {
            PublishRefusalKind::InvalidPayload(PayloadFault::UnrepresentableDeliverAfter { ms })
        }
        GateRefusal::CallCap { .. }
        | GateRefusal::SinkExhausted
        | GateRefusal::EntryCap { .. }
        | GateRefusal::ByteCap { .. }
        | GateRefusal::OpCap { .. } => PublishRefusalKind::QuotaExceeded,
    }
}

/// The classification a guest sees on the control-op path.
///
/// An impossible release time answers for itself. An oversize edit body is a
/// quota refusal, because the body is charged against the activation's
/// aggregate like any other.
pub fn defer_refusal_kind(refusal: GateRefusal) -> DeferRefusalKind {
    match refusal {
        GateRefusal::UnrepresentableDeliverAfter { .. } => DeferRefusalKind::InvalidDeliverAfter,
        GateRefusal::CallCap { .. }
        | GateRefusal::BodyTooLarge { .. }
        | GateRefusal::SinkExhausted
        | GateRefusal::EntryCap { .. }
        | GateRefusal::ByteCap { .. }
        | GateRefusal::OpCap { .. } => DeferRefusalKind::QuotaExceeded,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_published_body_over_the_cap_is_the_guests_fault_and_says_the_numbers() {
        let kind = publish_refusal_kind(GateRefusal::BodyTooLarge { len: 9, max: 8 });
        let fault = PayloadFault::BodyTooLarge { len: 9, max: 8 };
        assert_eq!(kind, PublishRefusalKind::InvalidPayload(fault));
        assert_eq!(fault.to_string(), "payload 9 bytes exceeds max 8");
    }

    #[test]
    fn an_impossible_release_time_is_a_bad_publish_but_its_own_defer_answer() {
        let fault = PayloadFault::UnrepresentableDeliverAfter { ms: 42 };
        assert_eq!(
            publish_refusal_kind(GateRefusal::UnrepresentableDeliverAfter { ms: 42 }),
            PublishRefusalKind::InvalidPayload(fault)
        );
        assert_eq!(
            fault.to_string(),
            "deliver_after 42 ms is not a representable timestamp"
        );
        assert_eq!(
            defer_refusal_kind(GateRefusal::UnrepresentableDeliverAfter { ms: 42 }),
            DeferRefusalKind::InvalidDeliverAfter
        );
    }

    #[test]
    fn an_edit_body_over_the_cap_is_a_quota_refusal() {
        assert_eq!(
            defer_refusal_kind(GateRefusal::BodyTooLarge { len: 9, max: 8 }),
            DeferRefusalKind::QuotaExceeded
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
            assert_eq!(
                publish_refusal_kind(refusal),
                PublishRefusalKind::QuotaExceeded
            );
            assert_eq!(defer_refusal_kind(refusal), DeferRefusalKind::QuotaExceeded);
        }
    }
}
