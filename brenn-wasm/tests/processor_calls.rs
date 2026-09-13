// The epoch deadline across a peer call: a caller stopped inside `calls.call` is
// not charged for the wait.
//
// The mechanism is a store-level deadline callback that gives back whatever the
// activation spent blocked, and its failure is the quietest one this host has —
// a `Continue` arm that never converges is an activation with no time bound at
// all, holding a blocking thread, which the pacer does not cover because it
// delays and does not kill. Nothing else drives it: every other suite's caller
// answers instantly, so only the `Interrupt` arm is reached.
//
// Uses the `processor-call-test` fixture, the one component that imports
// `brenn:processor/calls`: on a sync-call activation carrying `call:<rest>` it
// asks its peer `<rest>` and answers `via:<the peer's reply>`.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use brenn_activation::sync::SyncAnswer;
use brenn_wasm::{
    ComponentGrant, ProcessorActivation, ProcessorCallTarget, ProcessorComponent,
    ProcessorLoadSpec, ProcessorOutcome, ProcessorPortWindow, SyncCallerFn,
};

mod common;

/// One epoch tick is 100 ms, so one tick of deadline is a budget any sleeping
/// peer overruns.
const ONE_TICK: u64 = 1;
/// High enough that the fuel cap cannot be what ends an activation here.
const AMPLE_FUEL: u64 = u64::MAX / 2;
/// Comfortably past the one-tick deadline, and short enough to keep the suite
/// quick.
const PEER_DELAY: Duration = Duration::from_millis(350);

fn component_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target/components/brenn_processor_call_test.wasm")
}

fn request_envelope(body: &str) -> String {
    format!(
        r#"{{"message_id":"00000000-0000-0000-0000-000000000001","source":"caller","channel":"local:brenn/sync/answer","sender":"caller","publish_ts":"2026-01-01T00:00:00Z","body":"{body}","urgency":"normal","envelope_type":"local"}}"#
    )
}

/// The one shape this suite drives: a sync-call activation on `answer` whose
/// request tells the fixture to ask its peer.
fn sync_activation(body: &str) -> ProcessorActivation {
    ProcessorActivation {
        ports: vec![ProcessorPortWindow {
            port: "answer".to_string(),
            envelopes: vec![request_envelope(body)],
            new_from: 0,
            dropped: 0,
        }],
        deferred: vec![],
        now: None,
        sync: Some("answer".to_string()),
    }
}

/// The fixture with its `ask` port wired to a peer that sleeps `delay` before
/// answering. The output port is declared and unbound, so the guest's publish is
/// dropped rather than refused and the case is about the deadline alone.
fn component_with_slow_peer(delay: Duration) -> ProcessorComponent {
    let caller: SyncCallerFn = Arc::new(move |_slug: &str, _port: &str, body: String, _chain| {
        std::thread::sleep(delay);
        SyncAnswer::Ok(Some(format!("answer:{body}")))
    });
    let mut calls = HashMap::new();
    calls.insert(
        "ask".to_string(),
        ProcessorCallTarget {
            target_slug: "peer".to_string(),
            target_port: "answer".to_string(),
        },
    );
    ProcessorComponent::load(ProcessorLoadSpec {
        declared_out_ports: BTreeSet::from(["out".to_string()]),
        input_amplification_mt: common::amp_in(),
        grants: [ComponentGrant::Ports, ComponentGrant::Calls]
            .into_iter()
            .collect(),
        alerter: common::noop_alerter(),
        output_acl: common::allow_all(),
        declared_call_ports: BTreeSet::from(["ask".to_string()]),
        calls,
        sync_caller: Some(caller),
        ..ProcessorLoadSpec::minimal(&component_path(), "caller")
    })
}

/// The wait is given back: an activation whose peer takes three deadlines to
/// answer still returns ok, because the bound is on the caller's *own* running
/// time and it has spent almost none.
#[test]
fn a_caller_blocked_past_the_deadline_does_not_trap_when_its_peer_answers() {
    let comp = component_with_slow_peer(PEER_DELAY);
    let outcome = comp.handle_with_limits(sync_activation("call:leaf"), ONE_TICK, AMPLE_FUEL);
    match outcome {
        ProcessorOutcome::Ok { reply, .. } => assert_eq!(
            reply,
            Some("via:answer:leaf".to_string()),
            "the caller answered with what its peer replied"
        ),
        other => panic!(
            "a caller charged only for its own time must finish, got {other:?} after a \
             {PEER_DELAY:?} wait against a {ONE_TICK}-tick deadline"
        ),
    }
}

/// The other half of the same rule: the deadline still ends an activation that
/// spends the time itself. Without this the case above would pass on a store
/// with no bound at all.
#[test]
fn a_caller_that_spends_its_own_budget_still_traps() {
    let comp = component_with_slow_peer(Duration::ZERO);
    // `handle_with_limits` arms zero ticks: the budget is spent before the guest
    // starts, so the first deadline the engine raises is the last.
    let outcome = comp.handle_with_limits(sync_activation("call:leaf"), 0, AMPLE_FUEL);
    assert!(
        matches!(outcome, ProcessorOutcome::Trap(_)),
        "an activation with no time left must trap, got {outcome:?}"
    );
}
