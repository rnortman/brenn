//! Publish-budget arithmetic and per-activation caps.
//!
//! One activation gets one budget. This crate holds the numbers and the
//! arithmetic that bound it, for every host that mints activations: the
//! wasmtime host on the backend and the kernel in the page. Both run the same
//! model, so a component's publish budget means the same thing on either
//! hosting — the same vocabulary, the same clamp, the same grant.
//!
//! Two layers, kept apart:
//!
//! - **Per-sink token buckets** ([`SinkBudget`], [`seed_sink_budget`],
//!   [`grant_input_mt`]) — millitokens, per output binding, refilled per
//!   activation. This is the budget a component feels: exhausting it returns
//!   `quota-exceeded` from the publish call.
//! - **Per-activation caps** ([`MAX_PUBLISHES_PER_ACTIVATION`] and friends) —
//!   flat ceilings on one activation's buffer, independent of any bucket. They
//!   bound the host's own memory and log volume against a hostile or broken
//!   guest, which is why they are constants and not operator knobs.
//!
//! Config parsing lives elsewhere: the `f64` knobs an operator writes are
//! resolved to the millitokens below by whoever reads the config. This crate
//! sees only resolved integers.

#![forbid(unsafe_code)]

/// Millitokens charged per publish: the token-bucket fixed-point scale.
///
/// One publish costs `MILLITOKENS_PER_PUBLISH` millitokens, and every bucket is
/// integer millitokens, so enforcement never depends on float drift. Operator
/// knobs are `f64` tokens; they resolve to this scale once, at boot.
pub const MILLITOKENS_PER_PUBLISH: u64 = 1000;

/// Maximum number of publishes buffered per activation.
pub const MAX_PUBLISHES_PER_ACTIVATION: usize = 256;

/// Maximum total publish bytes buffered per activation (4 MiB).
pub const MAX_PUBLISH_BYTES_PER_ACTIVATION: usize = 4 * 1024 * 1024;

/// Maximum total publish calls (successful + failed) per activation.
///
/// Counts every call — including `not-permitted` and `invalid-payload`
/// rejections — to bound log-flood DoS from a hostile or buggy component that
/// repeatedly publishes to unbound ports or with oversized payloads. A cap on
/// successes alone would leave the rejection path free.
pub const MAX_PUBLISH_CALLS_PER_ACTIVATION: usize = 512;

/// Resolved token-bucket parameters for one egress sink, in millitokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinkBudget {
    /// Bucket fill added per activation.
    pub fill_mt: u64,
    /// Maximum tokens carried over between activations; the clamp is applied at
    /// the start of the next activation, by [`seed_sink_budget`].
    pub capacity_mt: u64,
}

/// Sum the input-driven token grant (millitokens) for one activation.
///
/// `grant = Σ over ports: amplification_mt × new_envelope_count`, over
/// `(amplification_mt, new_count)` pairs — one per windowed input port,
/// counting only new (unprocessed) envelopes, never retained context. The grant
/// is shared: it seeds every sink of the activation, so a component that
/// republishes what it consumes stays solvent at 1:1 without an operator
/// raising any knob.
///
/// Pairs rather than windows: the two hosts carry different activation types
/// (neither crate can see the other's), and the arithmetic never needed more
/// than the two numbers.
///
/// Saturating throughout — a grant that would overflow `u64` is
/// `u64::MAX`-worth of permission, which is the same practical answer.
pub fn grant_input_mt(ports: impl IntoIterator<Item = (u64, u64)>) -> u64 {
    ports
        .into_iter()
        .map(|(amplification_mt, new_count)| amplification_mt.saturating_mul(new_count))
        .fold(0u64, u64::saturating_add)
}

/// Seed one sink's activation budget from its persistent carryover: clamp carry
/// to capacity, then add the per-activation fill and the shared input grant.
///
/// The clamp lands here, at the start of the next activation, rather than at the
/// end of the previous one: `capacity_mt` bounds what an idle component
/// accumulates, and only the seeding host knows an activation is starting.
pub fn seed_sink_budget(carry_mt: u64, budget: SinkBudget, grant_input_mt: u64) -> u64 {
    carry_mt
        .min(budget.capacity_mt)
        .saturating_add(budget.fill_mt)
        .saturating_add(grant_input_mt)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The constants are the numbers the wasmtime host enforced before they
    /// moved here. Pinned literally: the move is exactly the kind of edit that
    /// renumbers a cap silently, and every consumer now reads these.
    #[test]
    fn constants_are_the_backend_numbers() {
        assert_eq!(MILLITOKENS_PER_PUBLISH, 1000);
        assert_eq!(MAX_PUBLISHES_PER_ACTIVATION, 256);
        assert_eq!(MAX_PUBLISH_BYTES_PER_ACTIVATION, 4 * 1024 * 1024);
        assert_eq!(MAX_PUBLISH_CALLS_PER_ACTIVATION, 512);
    }

    /// `grant_input_mt` sums `amplification × new_count` across ports.
    #[test]
    fn grant_sums_across_ports() {
        assert_eq!(grant_input_mt([(1000, 3), (1000, 2)]), 3000 + 2000);
        assert_eq!(grant_input_mt([(3000, 1)]), 3000);
    }

    /// An activation with no new envelopes — all-context windows — grants
    /// nothing: the grant pays for work delivered, and context is not new work.
    #[test]
    fn grant_is_zero_without_new_envelopes() {
        assert_eq!(grant_input_mt([(1000, 0), (5000, 0)]), 0);
        assert_eq!(grant_input_mt([]), 0);
    }

    /// Both folds saturate rather than wrap: a hostile amplification × count
    /// must not wrap to a small grant.
    #[test]
    fn grant_saturates() {
        assert_eq!(grant_input_mt([(u64::MAX, 2)]), u64::MAX);
        assert_eq!(grant_input_mt([(u64::MAX, 1), (u64::MAX, 1)]), u64::MAX);
    }

    /// `seed_sink_budget` clamps carry to capacity, then adds fill and grant.
    #[test]
    fn seed_clamps_carry_then_adds() {
        let budget = SinkBudget {
            fill_mt: 1000,
            capacity_mt: 2000,
        };
        // Carry above capacity clamps to capacity: 2000 + 1000 fill + 3000 grant.
        assert_eq!(seed_sink_budget(5000, budget, 3000), 6000);
        // Carry below capacity survives whole: 500 + 1000 fill + no grant.
        assert_eq!(seed_sink_budget(500, budget, 0), 1500);
        // Saturating, not wrapping.
        assert_eq!(seed_sink_budget(u64::MAX, budget, u64::MAX), u64::MAX);
    }

    /// A zero-fill sink is purely input-driven: it publishes only what its
    /// inputs grant it. The operator states this with `publish_per_activation =
    /// 0`, and it must not be confused with a sink that cannot publish at all.
    #[test]
    fn zero_fill_sink_lives_on_the_grant() {
        let budget = SinkBudget {
            fill_mt: 0,
            capacity_mt: 0,
        };
        assert_eq!(seed_sink_budget(0, budget, 2000), 2000);
        assert_eq!(seed_sink_budget(0, budget, 0), 0);
    }
}
