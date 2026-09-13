//! The scenarios, each a [`MountSpec`] paired with a `run` over any [`Host`].
//!
//! Every one of them is a scheduling-or-lifetime behaviour, which is the class
//! the direct-drive transplant suites structurally cannot see: a component whose
//! only input is a self-tick (scenarios 3 and 8), and a component that keeps
//! state in linear memory (scenario 7), are each hosted identically or not at
//! all, and nothing that drives the entry directly can tell which.

use brenn_activation::sync::SyncAnswer;
use brenn_envelope::ChannelScheme;

use crate::{
    Host, InputBinding, MountSpec, Report, TrapDisposition, assert_fresh_memory, await_reports,
    port, quiesce, settle,
};

/// A push-enabled input binding with room for a small batch.
fn pushed(port: &'static str) -> InputBinding {
    InputBinding {
        port,
        push_depth: 4,
        retain_depth: 8,
    }
}

/// A sampled binding: no position, context only, never a wake.
fn sampled(port: &'static str) -> InputBinding {
    InputBinding {
        port,
        push_depth: 0,
        retain_depth: 4,
    }
}

/// Every bound input port is present in the report, and every output port has a
/// deferred window. A host that presents a partial activation fails here before
/// any scenario's own assertion runs.
fn assert_shape(report: &Report, spec: &MountSpec) {
    for binding in &spec.inputs {
        assert!(
            report.port(binding.port).is_some(),
            "bound input port {:?} is absent from the report",
            binding.port,
        );
    }
    if spec.tick.is_some() {
        assert!(report.port(port::TICK).is_some(), "bound io port is absent");
        assert!(
            report.deferred(port::TICK).is_some(),
            "bound io port has no deferred window",
        );
    }
    for out in [port::OUT, port::REPORT] {
        assert!(
            report.deferred(out).is_some(),
            "bound output port {out:?} has no deferred window",
        );
    }
    assert!(report.now_set, "every activation carries the host's clock");
}

/// 1. Mount over empty channels: the activation is owed unconditionally, so it
///    happens with nothing to deliver.
pub mod mount_over_empty_channels {
    use super::*;

    pub fn spec() -> MountSpec {
        MountSpec {
            inputs: vec![pushed(port::IN), sampled(port::SAMPLED)],
            tick: None,
            tick_ms: None,
        }
    }

    pub async fn run<H: Host>(host: &mut H, spec: &MountSpec) {
        host.mount().await;
        let reports = settle(host, 1).await;
        assert_eq!(reports.len(), 1, "a mount is owed exactly one activation");
        let report = &reports[0];
        assert_shape(report, spec);
        for window in &report.ports {
            assert_eq!(
                (window.context_len, window.new_len, window.dropped),
                (0, 0, 0),
                "port {:?} over a channel that never carried anything",
                window.port,
            );
        }
        for window in &report.deferred {
            assert!(
                window.payloads.is_empty(),
                "port {:?} has nothing parked at mount",
                window.port,
            );
        }
        assert_fresh_memory(&reports);
    }
}

/// 2. Mount over channels with history: a fresh position is primed behind the
///    retained tail, so the tail arrives as new in the mount activation — capped
///    at the binding's push depth — and nothing follows it.
pub mod mount_over_history {
    use super::*;

    /// Three publishes against a push depth of two, so the cap is observable.
    pub const PUBLISHED: usize = 3;
    pub const PUSH_DEPTH: u32 = 2;

    pub fn spec() -> MountSpec {
        MountSpec {
            inputs: vec![InputBinding {
                port: port::IN,
                push_depth: PUSH_DEPTH,
                retain_depth: 8,
            }],
            tick: None,
            tick_ms: None,
        }
    }

    pub async fn run<H: Host>(host: &mut H, spec: &MountSpec) {
        for i in 0..PUBLISHED {
            host.publish(port::IN, &format!("history-{i}")).await;
        }
        host.mount().await;
        let reports = settle(host, 1).await;
        assert_eq!(reports.len(), 1, "the history arrives in the mount, once");
        assert_shape(&reports[0], spec);
        let window = reports[0].port(port::IN).expect("the bound input");
        assert_eq!(
            window.new_len, PUSH_DEPTH as usize,
            "the primed position sits at most push-depth behind the tail",
        );
        assert_fresh_memory(&reports);
    }
}

/// 3. The self-tick chain: the mount activation is the first tick of a chain the
///    component could not otherwise start, and each release re-arms exactly one
///    successor.
pub mod self_tick_chain {
    use super::*;

    /// Short enough that a handful of releases fit inside the wait bound, long
    /// enough that a loaded runner is not racing the report drain.
    pub const TICK_MS: u64 = 80;
    /// Releases to observe past the mount. Three is enough to show the chain
    /// sustaining itself rather than firing once.
    pub const RELEASES: usize = 3;

    /// The wiring, with the `io tick` port bound in the scheme under test.
    pub fn spec_in(scheme: ChannelScheme) -> MountSpec {
        MountSpec {
            inputs: vec![pushed(port::IN)],
            tick: Some(scheme),
            tick_ms: Some(TICK_MS),
        }
    }

    pub async fn run<H: Host>(host: &mut H, spec: &MountSpec) {
        host.mount().await;
        let reports = await_reports(host, 1 + RELEASES).await;
        assert_shape(&reports[0], spec);
        assert!(
            reports[0]
                .deferred(port::TICK)
                .expect("the io port's deferred window")
                .payloads
                .is_empty(),
            "nothing is parked yet when the mount activation is reported",
        );
        assert_eq!(
            reports[0].port(port::TICK).expect("the io port").new_len,
            0,
            "the mount activation is owed, not triggered by a tick",
        );
        for (i, report) in reports[1..].iter().enumerate() {
            assert_shape(report, spec);
            assert_eq!(
                report.port(port::TICK).expect("the io port").new_len,
                1,
                "release {i} delivers exactly the tick that came due",
            );
            assert!(
                report
                    .deferred(port::TICK)
                    .expect("the io port's deferred window")
                    .payloads
                    .is_empty(),
                "release {i} reports its window after the release and before the \
                 re-arm, so nothing is standing",
            );
        }
        assert_fresh_memory(&reports);
    }
}

/// 4. Sampled-only wiring: a `push_depth = 0` port holds no position, so it
///    cannot wake its owner — but the mount debt is not a port, and is owed
///    anyway.
pub mod sampled_only_wiring {
    use super::*;

    pub fn spec() -> MountSpec {
        MountSpec {
            inputs: vec![sampled(port::SAMPLED)],
            tick: None,
            tick_ms: None,
        }
    }

    pub async fn run<H: Host>(host: &mut H, spec: &MountSpec) {
        host.mount().await;
        let mounted = settle(host, 1).await;
        assert_eq!(
            mounted.len(),
            1,
            "a consumer whose only input is sampled still gets its mount",
        );
        assert_shape(&mounted[0], spec);

        host.publish(port::SAMPLED, "sampled-body").await;
        assert!(
            settle(host, 0).await.is_empty(),
            "traffic on a sampled port is context, never a wake",
        );
        assert_fresh_memory(&mounted);
    }
}

/// 5. Err consumes: a failed activation discards its buffer, and the messages it
///    carried are gone from the cursor all the same.
pub mod err_consumes {
    use super::*;

    pub fn spec() -> MountSpec {
        MountSpec {
            inputs: vec![pushed(port::IN)],
            tick: None,
            tick_ms: None,
        }
    }

    pub async fn run<H: Host>(host: &mut H, _spec: &MountSpec) {
        host.mount().await;
        assert_eq!(settle(host, 1).await.len(), 1, "the mount activation");

        host.publish(port::IN, "__err__").await;
        assert!(
            settle(host, 0).await.is_empty(),
            "an err activation flushes nothing, so it publishes no report",
        );

        host.publish(port::IN, "after-err").await;
        let reports = settle(host, 1).await;
        assert_eq!(reports.len(), 1, "the instance keeps being activated");
        let window = reports[0].port(port::IN).expect("the bound input");
        assert_eq!(
            (window.context_len, window.new_len),
            (1, 1),
            "the err body was consumed — it is context now, not redelivered",
        );
        assert_fresh_memory(&reports);
    }
}

/// 6. Trap: the one deliberate host difference in error handling, asserted per
///    host rather than papered over.
pub mod trap_disposition {
    use super::*;

    pub fn spec() -> MountSpec {
        MountSpec {
            inputs: vec![pushed(port::IN)],
            tick: None,
            tick_ms: None,
        }
    }

    pub async fn run<H: Host>(host: &mut H, _spec: &MountSpec) {
        host.mount().await;
        assert_eq!(settle(host, 1).await.len(), 1, "the mount activation");

        host.publish(port::IN, "__trap__").await;
        assert!(
            settle(host, 0).await.is_empty(),
            "a trapped activation flushes nothing",
        );

        host.publish(port::IN, "after-trap").await;
        // What is expected of this host is what it is waited for: the
        // quarantining host owes a report, the terminal one owes none.
        let want = match host.trap_disposition() {
            TrapDisposition::Quarantine => 1,
            TrapDisposition::Terminal => 0,
        };
        let after = settle(host, want).await;
        match host.trap_disposition() {
            TrapDisposition::Quarantine => {
                assert_eq!(
                    after.len(),
                    1,
                    "the backend quarantines the activation and keeps delivering",
                );
                assert_fresh_memory(&after);
            }
            TrapDisposition::Terminal => assert!(
                after.is_empty(),
                "the surface takes a trapped instance terminal",
            ),
        }
    }
}

/// 7. State does not survive: linear memory is activation-scoped, reduced to a
///    counter.
pub mod state_does_not_survive {
    use super::*;

    pub fn spec() -> MountSpec {
        MountSpec {
            inputs: vec![pushed(port::IN)],
            tick: None,
            tick_ms: None,
        }
    }

    pub async fn run<H: Host>(host: &mut H, _spec: &MountSpec) {
        host.mount().await;
        assert_eq!(settle(host, 1).await.len(), 1, "the mount activation");

        let mut seen = Vec::new();
        for i in 0..2 {
            host.publish(port::IN, &format!("body-{i}")).await;
            let reports = settle(host, 1).await;
            assert_eq!(reports.len(), 1, "one publish, one activation");
            seen.extend(reports);
        }
        assert_fresh_memory(&seen);
    }
}

/// 8. Remount: a new mount is owed its own activation, and a parked tick is
///    still there when it arrives — the store outlives the registration on every
///    scheme — so a conforming component does not double-arm.
pub mod remount {
    use super::*;

    /// Far enough out that the tick is still parked when the remount lands.
    pub const TICK_MS: u64 = 60_000;

    /// The wiring, with the `io tick` port bound in the scheme under test.
    pub fn spec_in(scheme: ChannelScheme) -> MountSpec {
        MountSpec {
            inputs: vec![pushed(port::IN)],
            tick: Some(scheme),
            tick_ms: Some(TICK_MS),
        }
    }

    pub async fn run<H: Host>(host: &mut H, spec: &MountSpec) {
        host.mount().await;
        let first = settle(host, 1).await;
        assert_eq!(first.len(), 1, "the first mount activation");
        assert!(
            first[0]
                .deferred(port::TICK)
                .expect("the io port's deferred window")
                .payloads
                .is_empty(),
            "nothing is parked before the probe arms its chain",
        );

        host.remount().await;
        let second = settle(host, 1).await;
        assert_eq!(second.len(), 1, "a remount is a mount and is owed one too");
        assert_shape(&second[0], spec);
        assert_eq!(
            second[0]
                .deferred(port::TICK)
                .expect("the io port's deferred window")
                .payloads
                .len(),
            1,
            "a parked tick survives the remount, and the probe re-arms only \
             into an empty window — so it is still exactly one",
        );
        assert_fresh_memory(&first);
        assert_fresh_memory(&second);
    }
}

/// 9. A tick that came due while the instance was out of service is still in
///    the deferred window at the next mount, carrying its past instant.
///
/// "Parked" means "no release pass has taken it", not "release time in the
/// future", and this is the scenario that pins it: without the rule the window
/// at that mount is empty, the probe re-arms into it, and the chain runs at
/// twice its cadence until the extra tick drains — silently, on both hosts.
///
/// The entry is shown and is not actionable: the cancel/edit authority cutoff
/// is still the release instant. That half is pinned in the stores' parity
/// suite, where an op's refusal is observable; here the question is what the
/// component is shown.
pub mod due_tick_is_shown_at_mount {
    use super::*;
    use std::time::Duration;

    /// Long enough that a loaded runner cannot reach the next step before the
    /// tick is due, short enough that the outage is not most of the suite's
    /// runtime.
    pub const TICK_MS: u64 = 1_000;
    /// Spent out of service with no release pass run, so the tick comes due
    /// with nobody there to take it.
    pub const OUTAGE: Duration = Duration::from_millis(1_300);

    /// The wiring, with the `io tick` port bound in the scheme under test.
    pub fn spec_in(scheme: ChannelScheme) -> MountSpec {
        MountSpec {
            inputs: vec![pushed(port::IN)],
            tick: Some(scheme),
            tick_ms: Some(TICK_MS),
        }
    }

    fn tick_window(report: &Report) -> &crate::DeferredReport {
        report
            .deferred(port::TICK)
            .expect("the io port's deferred window")
    }

    pub async fn run<H: Host>(host: &mut H, spec: &MountSpec) {
        host.mount().await;
        let first = await_reports(host, 1).await;
        assert_eq!(first.len(), 1, "the mount activation");
        assert!(
            tick_window(&first[0]).payloads.is_empty(),
            "nothing is parked when the mount activation is reported; the probe \
             arms its chain after",
        );

        // Out of service across the tick's instant, with no release pass run —
        // neither during the outage nor by the drains that read the reports.
        // Held first, then settled: a report is visible as soon as its publish
        // lands, and the park that follows it in the same flush is what this
        // scenario is about, so the outage must not start until the flush is
        // done.
        host.hold_releases(true);
        settle(host, 0).await;
        host.remount_after(OUTAGE).await;
        let second = await_reports(host, 1).await;
        assert_eq!(second.len(), 1, "a remount is a mount and is owed one too");
        assert_shape(&second[0], spec);
        let window = tick_window(&second[0]);
        assert_eq!(
            window.payloads.len(),
            1,
            "the tick nobody released is still parked, and the probe re-arms \
             only into an empty window — so it is exactly one",
        );
        assert_eq!(
            window.due,
            vec![true],
            "and it is shown with the instant that has already passed",
        );

        host.hold_releases(false);
        let third = await_reports(host, 1).await;
        assert_eq!(
            third[0].port(port::TICK).expect("the io port").new_len,
            1,
            "one release, one tick delivered",
        );

        // And exactly one tick stands afterwards: the probe re-armed from a
        // window the release had emptied. Held and settled again for the same
        // reason: the re-arm is the tail of the flush whose report was just
        // read.
        host.hold_releases(true);
        settle(host, 0).await;
        host.remount().await;
        let fourth = await_reports(host, 1).await;
        assert_eq!(
            tick_window(&fourth[0]).payloads.len(),
            1,
            "exactly one tick stands after the release — the chain re-armed \
             once, not twice and not never",
        );
        // Whether that tick is due by now is the runner's speed, not the
        // host's behaviour, so it is not asserted here: the due case is what
        // the second mount above pins.

        assert_fresh_memory(&first);
        assert_fresh_memory(&second);
        assert_fresh_memory(&third);
        assert_fresh_memory(&fourth);
    }
}

/// 10. A sync call is one ordinary activation plus a reply.
///
/// The two halves of the facility's definition, asserted together: the
/// activation the request causes is composed exactly as an async one is — every
/// bound input windowed, every output's deferred window present, the clock
/// handed over — with one fabricated window carrying the request and nothing
/// else, and the buffer it built is flushed *before* the caller is answered.
///
/// Flush-before-reply is read the only way a caller can read it: the report is
/// on its channel by the time the answer is in hand, without the scenario
/// having driven the host in between.
pub mod a_sync_call_is_an_activation_plus_a_reply {
    use super::*;

    pub fn spec() -> MountSpec {
        MountSpec {
            inputs: vec![pushed(port::IN), sampled(port::SAMPLED)],
            tick: None,
            tick_ms: None,
        }
    }

    pub async fn run<H: Host>(host: &mut H, spec: &MountSpec) {
        host.mount().await;
        assert_eq!(settle(host, 1).await.len(), 1, "the mount activation");

        let answer = host.sync_call(port::ASK, "__reply__").await;
        // The probe's reply names the port it was asked on, that port read back
        // as an item, what the generated classifier makes of the cause — the
        // declared port here, a refusal on a mount — the request body, whether
        // the request is attributed to the
        // target's own bare name — the same answer on both hosts, where a
        // participant-vocabulary spelling would differ — and the ports the
        // activation *delivered*, the whole window list minus the request's own.
        assert_eq!(
            answer,
            SyncAnswer::Ok(Some(format!(
                "replied:{}:mount=false:classified={}:request=__reply__:\
                 bare-identity=true:delivered=[{},{}]",
                port::ASK,
                port::ASK,
                port::IN,
                port::SAMPLED,
            ))),
            "a sync call is answered with the reply its callee returned",
        );

        let reports = host.drain().await;
        assert_eq!(
            reports.len(),
            1,
            "the request caused exactly one activation, and its buffer was \
             flushed before the answer came back",
        );
        let report = &reports[0];
        assert_shape(report, spec);
        let request = report
            .port(port::ASK)
            .expect("the request's own window is in the activation");
        assert_eq!(
            (request.context_len, request.new_len, request.dropped),
            (0, 1, 0),
            "the sync window holds exactly the one live request as new: a sync \
             port has no retention and no position, so nothing is context and \
             nothing can have been passed unserved",
        );
        for window in &report.ports {
            if window.port != port::ASK {
                assert_eq!(
                    (window.context_len, window.new_len),
                    (0, 0),
                    "port {:?} was windowed over a channel that never carried \
                     anything — the request does not stand in for delivery",
                    window.port,
                );
            }
        }
        assert_fresh_memory(&reports);

        assert!(
            settle(host, 0).await.is_empty(),
            "a sync call is the whole activation it caused; nothing follows it",
        );
    }
}

/// 11. A sync call consumes queued input: it is an ordinary activation, so the
///     messages waiting on the instance's bound ports are delivered by it and
///     are gone from the cursor afterwards.
pub mod a_sync_call_consumes_queued_input {
    use super::*;

    pub fn spec() -> MountSpec {
        MountSpec {
            inputs: vec![pushed(port::IN)],
            tick: None,
            tick_ms: None,
        }
    }

    pub async fn run<H: Host>(host: &mut H, _spec: &MountSpec) {
        host.mount().await;
        assert_eq!(settle(host, 1).await.len(), 1, "the mount activation");

        // Published and *not* drained: the queued message must still be waiting
        // when the request arrives, or the scenario proves nothing. The pause is
        // what makes "not drained" true — a host still holding a wake from the
        // mount's own settling would spend it on this publish.
        quiesce().await;
        host.publish(port::IN, "queued").await;
        let answer = host.sync_call(port::ASK, "__reply__").await;
        assert!(
            matches!(answer, SyncAnswer::Ok(Some(_))),
            "the request was answered: {answer:?}",
        );

        let reports = host.drain().await;
        assert_eq!(reports.len(), 1, "one request, one activation");
        let window = reports[0].port(port::IN).expect("the bound input");
        assert_eq!(
            (window.context_len, window.new_len),
            (0, 1),
            "the queued message is delivered as new by the request's own \
             activation",
        );
        assert_fresh_memory(&reports);

        assert!(
            settle(host, 0).await.is_empty(),
            "and it was consumed there — no async activation follows for it",
        );
    }
}

/// 13. A reply to an activation that asked nothing is a trap of the callee.
///
/// The instruction arrives on an ordinary input port, so the activation it
/// causes is async and the reply it returns is unasked-for. The trap is the
/// *host's*, not the probe's: the probe answers unconditionally, which is what
/// leaves the rule to the host to enforce.
pub mod a_reply_to_an_async_activation_is_a_trap {
    use super::*;

    pub fn spec() -> MountSpec {
        MountSpec {
            inputs: vec![pushed(port::IN)],
            tick: None,
            tick_ms: None,
        }
    }

    pub async fn run<H: Host>(host: &mut H, _spec: &MountSpec) {
        host.mount().await;
        assert_eq!(settle(host, 1).await.len(), 1, "the mount activation");

        host.publish(port::IN, "__answer__:1").await;
        assert!(
            settle(host, 0).await.is_empty(),
            "the activation trapped, so its buffer — the report included — was \
             discarded and never reached the channel",
        );

        // The disposition is the one deliberate host difference, asserted per
        // host exactly as scenario 6 does: what is expected is what is waited
        // for.
        host.publish(port::IN, "after-reply").await;
        let want = match host.trap_disposition() {
            TrapDisposition::Quarantine => 1,
            TrapDisposition::Terminal => 0,
        };
        let after = settle(host, want).await;
        match host.trap_disposition() {
            TrapDisposition::Quarantine => {
                assert_eq!(after.len(), 1, "the backend quarantines and carries on");
                assert_fresh_memory(&after);
            }
            TrapDisposition::Terminal => assert!(
                after.is_empty(),
                "the surface takes a trapped instance terminal",
            ),
        }
    }
}

/// 14. An oversize reply is a trap of the callee.
///
/// A reply is bounded by the same cap as a publish body, so a callee cannot hand
/// a caller more than it could have published. The control is the other half of
/// the argument: the two replies differ by one byte, so the trap on the second
/// can be nothing but the cap.
pub mod an_oversize_reply_is_a_trap {
    use super::*;

    use crate::BODY_CAP;

    pub fn spec() -> MountSpec {
        MountSpec {
            inputs: vec![pushed(port::IN)],
            tick: None,
            tick_ms: None,
        }
    }

    pub async fn run<H: Host>(host: &mut H, _spec: &MountSpec) {
        host.mount().await;
        assert_eq!(settle(host, 1).await.len(), 1, "the mount activation");

        // The cap is inclusive, and this control runs first: on a host whose
        // trap disposition is terminal there is no instance left after the
        // oversize call.
        let at_cap = host
            .sync_call(port::ASK, &format!("__answer__:{BODY_CAP}"))
            .await;
        assert_eq!(
            at_cap,
            SyncAnswer::Ok(Some("x".repeat(BODY_CAP as usize))),
            "a reply of exactly the cap is admissible",
        );
        assert_eq!(
            host.drain().await.len(),
            1,
            "and its activation's buffer flushed",
        );

        let over = host
            .sync_call(port::ASK, &format!("__answer__:{}", BODY_CAP + 1))
            .await;
        assert_eq!(
            over,
            SyncAnswer::Trap,
            "one byte over the cap is a trap of the callee, not a truncation \
             and not an answer given in its place",
        );
        assert!(
            settle(host, 0).await.is_empty(),
            "a trapped activation flushes nothing, so its report never reached \
             the channel",
        );
    }
}
