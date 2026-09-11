// The surface kinds' DOM transcripts, wasmtime half.
//
// The artifacts the page loads transpiled — `brenn_echo_stub.wasm`,
// `brenn_meeting.wasm`, `brenn_chrome.wasm` — driven through scripted
// activation sequences against `brenn-page-harness`, the recording
// implementation of the page's imports, and reduced to an ordered transcript of
// what each asked the page to do.
//
// Why this suite exists: every other test of a page-hosted kind's DOM behaviour
// is a `wasm_bindgen_test` in a browser suite nothing in CI runs
// (TODO(surface-wasm-test-in-ci)). This one runs where the rest of the Rust
// tree runs, so a migrated UI kind's actual element calls, gesture handling and
// publish taxonomy are executable coverage rather than inspection.
//
// What is left here is this suite's own: which three artifacts it drives, the
// grant profile each is linked with, their port and marker vocabularies, and
// the scripted fixtures built out of the harness's activation constructors.

use std::collections::BTreeMap;

use brenn_chrome::spec::port::{SURFACE_STATE, TOAST};
use brenn_chrome::wire::{
    CONTROL_PLANE_VERSION, InstanceState, SurfaceStateBody, SurfaceStateInstance, ToastBody,
    ToastSeverity, ToastSource,
};
use brenn_envelope::grants::ComponentGrant::{Alert, Config, Dom, Log, PageDom, Ports};
use brenn_envelope::testutils::NOW_MS;
use brenn_meeting::logic::{AckTarget, SNOOZE_SECS, dismiss_body, snooze_body};
use brenn_page_harness::{
    Harness, Listener, Page, ROOT, delivery_on, gesture, gesture_at, mount, ports, types,
};
use chrono::{DateTime, Duration, Utc};

mod common;

/// The one input port echo-stub's specification binds.
const ECHO_IN_PORT: &str = "messages";

/// The one output port it publishes on.
const ECHO_OUT_PORT: &str = "out";

/// The `io` port its state rides between activations.
const ECHO_STATE_PORT: &str = "state";

/// Echo-stub's sync port names, its own vocabulary — not bound in the
/// specification.
const SEND: &str = "send";
const SEND_CUSTOM: &str = "send-custom";
const PANIC: &str = "panic";

/// The delivered body that makes echo-stub fail *after* its state has been
/// stored.
const FAIL_AFTER_STORE: &str = "__fail-after-store__";

/// The component's scrollback cap. A copy of its own private constant; the test
/// that uses it asserts the observable bound rather than the number.
const MAX_SCROLLBACK_ENTRIES: usize = 100;

/// meeting's two bound ports, and the two sync ports its buttons ask for.
const AGENDA_PORT: &str = "agenda";
const ACKS_PORT: &str = "acks";
const DISMISS: &str = "dismiss";
const SNOOZE: &str = "snooze";

/// The `io` port meeting's state rides between activations, and the `out` port
/// it announces a takeover transition on.
const MEETING_STATE_PORT: &str = "state";
const MEETING_TAKEOVER_PORT: &str = "takeover";

/// The instance chrome runs as in this fixture, and the one other instance
/// already registered on the page it arranges.
const CHROME_INSTANCE: &str = "chrome";
const SIBLING_INSTANCE: &str = "panel-1";

/// The sync port chrome's one delegated toast listener asks for. Its own
/// vocabulary, bound to nothing in the specification.
const TOAST_DISMISS: &str = "toast-dismiss";

/// The `io` port chrome's state rides between activations.
const CHROME_STATE_PORT: &str = "state";

/// echo-stub, linked with exactly what its specification requires.
fn echo_stub() -> Harness {
    Harness::new(
        &common::artifact_path("brenn_echo_stub"),
        Page::new(),
        &[Ports, Log, Dom],
    )
    .retaining_state(ECHO_STATE_PORT)
}

/// echo-stub, mounted: every test past the mount transcript is about what an
/// activation *after* the mount does.
fn echo_stub_mounted() -> Harness {
    Harness::mount(echo_stub())
}

/// mode-clock's bound ports: the retained config snapshot, the boundary wake it
/// parks for itself, and the port its state rides.
const MODE_TICK_PORT: &str = "tick";
const MODE_STATE_PORT: &str = "state";
const MODE_THEME_PORT: &str = "theme";

/// mode-clock, the one headless kind: it holds `dom` for the zone offset and
/// draws nothing.
fn mode_clock() -> Harness {
    Harness::new(
        &common::artifact_path("brenn_mode_clock"),
        Page::new(),
        &[Ports, Log, Dom],
    )
    .retaining_state(MODE_STATE_PORT)
}

/// protobar's bound ports, and the markers on the two elements it builds.
const PROTOBAR_IN_PORT: &str = "messages";
const PROTOBAR_STATE_PORT: &str = "state";
const PROTOBAR_MESSAGE_MARKER: &str = "data-protobar-message";
const PROTOBAR_STATUS_MARKER: &str = "data-protobar-status";

/// protobar: the kind whose whole state — priority slots, view handles and the
/// render it diffs against — rides the state port.
fn protobar() -> Harness {
    Harness::new(
        &common::artifact_path("brenn_protobar"),
        Page::new(),
        &[Ports, Log, Dom],
    )
    .retaining_state(PROTOBAR_STATE_PORT)
}

fn meeting_unmounted() -> Harness {
    Harness::new(
        &common::artifact_path("brenn_meeting"),
        Page::new(),
        &[Ports, Log, Dom],
    )
    .retaining_state(MEETING_STATE_PORT)
}

fn meeting() -> Harness {
    Harness::mount(meeting_unmounted())
}

/// chrome, the one kind holding page authority.
fn chrome_unmounted() -> Harness {
    Harness::new(
        &common::artifact_path("brenn_chrome"),
        Page::with_page_authority(CHROME_INSTANCE, SIBLING_INSTANCE),
        &[Ports, Log, Dom, PageDom],
    )
    .retaining_state(CHROME_STATE_PORT)
}

fn chrome() -> Harness {
    Harness::mount(chrome_unmounted())
}

/// An ordinary delivery on echo-stub's one bound input port.
fn delivery(context: &[&str], new: &[&str], dropped: u32) -> types::Activation {
    delivery_on(ECHO_IN_PORT, context, new, dropped)
}

/// The whole of what the mount activation asks the page for, in order.
///
/// Regenerate deliberately, never to make this pass: this is the one executable
/// statement in CI of what a migrated UI kind draws.
const MOUNT_TRANSCRIPT: &[&str] = &[
    "dom.root -> n1",
    "dom.create-element(div) -> n2",
    "dom.set-attribute(n2, data-echo-status, \"\")",
    "dom.set-text(n2, \"awaiting data\")",
    "dom.create-element(div) -> n3",
    "dom.set-attribute(n3, data-echo-scrollback, \"\")",
    "dom.create-element(button) -> n4",
    "dom.set-attribute(n4, data-echo-send, \"\")",
    "dom.set-text(n4, \"send\")",
    "dom.create-element(input) -> n5",
    "dom.set-attribute(n5, data-echo-input, \"\")",
    "dom.set-attribute(n5, type, \"text\")",
    "dom.create-element(button) -> n6",
    "dom.set-attribute(n6, data-echo-send-custom, \"\")",
    "dom.set-text(n6, \"send custom\")",
    "dom.create-element(button) -> n7",
    "dom.set-attribute(n7, data-echo-panic, \"\")",
    "dom.set-text(n7, \"panic\")",
    "dom.append(n1, n2)",
    "dom.append(n1, n3)",
    "dom.append(n1, n4)",
    "dom.append(n1, n5)",
    "dom.append(n1, n6)",
    "dom.append(n1, n7)",
    "dom.listen(n4, click, send)",
    "dom.listen(n6, click, send-custom)",
    "dom.listen(n7, click, panic)",
    "dom.set-text(n2, \"sent: 0  drops: 0\")",
    // The last thing every activation does: the state the next one reads back.
    // The handles in it are the elements built above, which the mount owns and
    // the memory does not.
    "ports.publish(state, \"{\\\"view\\\":{\\\"status\\\":2,\\\"scrollback\\\":3,\\\"input\\\":5},\\\"entries\\\":[],\\\"drops\\\":0,\\\"sent\\\":0}\")",
];

#[test]
fn the_mount_activation_builds_the_view_and_wires_its_gestures() {
    let mut harness = echo_stub();
    harness.call(mount());
    assert_eq!(harness.transcript(), MOUNT_TRANSCRIPT);

    let root_children = harness.page().children(ROOT);
    assert_eq!(root_children.len(), 6, "{root_children:?}");
    assert_eq!(
        harness.page().listeners,
        vec![
            Listener {
                node: 4,
                event: "click".to_string(),
                port: SEND.to_string()
            },
            Listener {
                node: 6,
                event: "click".to_string(),
                port: SEND_CUSTOM.to_string()
            },
            Listener {
                node: 7,
                event: "click".to_string(),
                port: PANIC.to_string()
            },
        ]
    );
    assert!(
        harness.page().published_on(ECHO_OUT_PORT).is_empty(),
        "mounting publishes nothing but its own state"
    );
}

/// The retained-state port is where a kind's state lives between activations,
/// and the counters here prove the round trip: linear memory holds nothing
/// across calls, so a `sent` that advances can only have come back off the
/// port.
#[test]
fn the_state_port_carries_the_counters_between_activations() {
    let mut harness = echo_stub_mounted();
    let send = harness.page().marked_child(ROOT, "data-echo-send");
    harness.call(gesture(SEND, send));
    let after_first = state_writes(harness.transcript());
    harness.call(gesture(SEND, send));
    let after_second = state_writes(harness.transcript());

    assert_eq!(after_first.len(), 1, "{after_first:?}");
    assert_eq!(after_second.len(), 1, "{after_second:?}");
    assert!(after_first[0].contains("\\\"sent\\\":1"), "{after_first:?}");
    assert!(
        after_second[0].contains("\\\"sent\\\":2"),
        "the second press counted from the first press's state: {after_second:?}"
    );
}

/// An unchanged state costs no message and no budget: the helper compares what
/// it is asked to store with what it read and publishes only a difference.
#[test]
fn an_activation_that_changes_nothing_writes_no_state() {
    let mut harness = echo_stub_mounted();
    harness.call(delivery(&["seen before"], &[], 0));
    let writes = state_writes(harness.transcript());
    assert!(
        writes.is_empty(),
        "nothing about this activation changed the state: {writes:?}"
    );
}

/// A state body that is not this kind's shape is a version skew across a
/// deploy, not a host fault. echo-stub cannot go on without its view handles,
/// so it refuses the activation rather than rebuilding a second view over the
/// first one.
#[test]
fn a_state_body_this_kind_cannot_read_refuses_the_activation() {
    let mut harness = echo_stub().seeding_state("{\"view\":\"not a view\"}");
    let refusal = harness.call_expecting_a_refusal(mount());
    let refusal = format!("{refusal:?}");
    assert!(
        refusal.contains("retained state on port \\\"state\\\" is unreadable"),
        "{refusal}"
    );
}

/// The state publishes in a transcript, in order.
fn state_writes(transcript: Vec<String>) -> Vec<String> {
    transcript
        .into_iter()
        .filter(|line| line.starts_with("ports.publish(state,"))
        .collect()
}

/// A kind whose `io state` port is not bound sees no window on it. That is a
/// deployment fault and not a first mount, and the two must not look alike: a
/// cell that started from a default instead would make an instance whose state
/// never persists behave exactly like one that has none yet.
#[test]
fn an_unbound_state_port_refuses_the_activation_by_name() {
    let mut harness = Harness::new(
        &common::artifact_path("brenn_echo_stub"),
        Page::new(),
        &[Ports, Log, Dom],
    );
    let refusal = harness.call_expecting_a_refusal(mount());
    let refusal = format!("{refusal:?}");
    assert!(refusal.contains("no window for port"), "{refusal}");
    assert!(refusal.contains("state"), "{refusal}");
    assert!(refusal.contains("the port is not bound"), "{refusal}");
}

/// The state write is the SDK's publish, not the component's, and a host that
/// refuses it has lost this activation's state. echo-stub's state carries its
/// view handles, so it cannot go on from the body it read: the activation
/// fails, whichever refusal it was. Neither is a trap — a state body's size is
/// driven by what the component was sent, so trapping on the per-message cap
/// would make a large message a way to kill an instance.
#[test]
fn a_refused_state_write_fails_an_activation_whose_state_holds_handles() {
    for refusal in [
        ports::PublishError::QuotaExceeded,
        ports::PublishError::InvalidPayload("payload 99 bytes exceeds max 8".to_string()),
    ] {
        let mut harness = echo_stub_mounted();
        harness
            .page()
            .publish_answer_on
            .insert(ECHO_STATE_PORT.to_string(), refusal.clone());
        let send = harness.page().marked_child(ROOT, "data-echo-send");
        let error = harness.call_expecting_a_refusal(gesture(SEND, send));
        let error = format!("{error:?}");
        assert!(
            error.contains("retained state publish on port \\\"state\\\""),
            "{refusal:?}: {error}"
        );
        assert!(
            error.contains("bytes) was refused"),
            "the diagnostic names the body's size: {error}"
        );
    }
}

/// The other half of that decision: a kind whose state is a recomputable
/// summary loses a recomputation and nothing else, so a refused write is
/// reported and the activation stands — which is what keeps its self-tick
/// chain, parked in the same activation, alive.
#[test]
fn a_refused_state_write_is_reported_where_the_state_is_recomputable() {
    let mut harness = mode_clock();
    harness.page().publish_answer_on.insert(
        MODE_STATE_PORT.to_string(),
        ports::PublishError::QuotaExceeded,
    );
    harness.call(mount());

    let transcript = harness.transcript();
    assert!(
        transcript
            .iter()
            .any(|line| line.starts_with("log.error(") && line.contains("state")),
        "the refusal is reported, not swallowed: {transcript:?}"
    );
    assert_eq!(
        harness.page().published_on(MODE_THEME_PORT).len(),
        1,
        "the recompute this activation did still reached the shell"
    );
    assert_eq!(
        harness
            .page()
            .parked
            .iter()
            .filter(|(port, _, _)| port == MODE_TICK_PORT)
            .count(),
        1,
        "and the chain that causes the next activation is still armed"
    );
}

/// A page discards a failed activation's whole publish buffer, so the harness
/// must too: a suite that kept the state body a failing activation wrote would
/// model a lifetime the browser does not have, in tree and out of it.
#[test]
fn a_failed_activation_leaves_the_last_committed_state_body() {
    let mut harness = echo_stub_mounted();
    harness.call(delivery(&[], &["first"], 1));
    harness.transcript();

    // This one renders, counts its drops and writes its state, and only then
    // fails.
    harness.call_expecting_a_refusal(delivery(&[], &[FAIL_AFTER_STORE], 4));
    assert_eq!(
        state_writes(harness.transcript()).len(),
        1,
        "the failing activation did publish a state body"
    );

    harness.call(delivery(&[], &["third"], 0));
    let status = harness.page().marked_child(ROOT, "data-echo-status");
    assert_eq!(
        harness.page().text_of(status),
        "sent: 0  drops: 1",
        "the failed activation's four drops went with its publish buffer"
    );
}

/// The body a state-cell hands the component has no window on the state port:
/// the port is the SDK's own mechanism and a component folding through its
/// delivered windows must not meet its own state as traffic. echo-stub says so
/// by refusing such a window, so every one of its scripts above pins the rule.
#[test]
fn the_state_window_is_taken_off_the_activation_the_component_sees() {
    let mut harness = echo_stub_mounted();
    harness.call(delivery(&[], &["one message"], 0));
    let scrollback = harness.page().marked_child(ROOT, "data-echo-scrollback");
    assert_eq!(
        harness.page().children(scrollback).len(),
        1,
        "the state envelope is not a delivery"
    );
}

#[test]
fn a_delivery_renders_its_new_envelopes_and_sums_the_drops() {
    let mut harness = echo_stub_mounted();
    harness.call(delivery(&["seen before"], &["first", "second"], 3));

    let transcript = harness.transcript();
    assert_eq!(
        transcript
            .iter()
            .filter(|line| line.starts_with("dom.create-element"))
            .count(),
        2,
        "only the new envelopes are rendered, never the retained context: {transcript:?}"
    );
    assert!(
        transcript.iter().any(|line| line.contains("first")),
        "{transcript:?}"
    );
    assert!(
        !transcript.iter().any(|line| line.contains("seen before")),
        "the context is what this instance already scrolled back: {transcript:?}"
    );

    let scrollback = harness.page().marked_child(ROOT, "data-echo-scrollback");
    assert_eq!(harness.page().children(scrollback).len(), 2);
    let status = harness.page().marked_child(ROOT, "data-echo-status");
    assert_eq!(harness.page().text_of(status), "sent: 0  drops: 3");
}

#[test]
fn a_press_publishes_a_numbered_message_and_the_status_counts_it() {
    let mut harness = echo_stub_mounted();
    let send = harness.page().marked_child(ROOT, "data-echo-send");
    harness.call(gesture(SEND, send));
    harness.call(gesture(SEND, send));

    assert_eq!(
        harness.page().published_on(ECHO_OUT_PORT),
        ["echo-stub message #1", "echo-stub message #2"],
        "the counter advances only where the publish happened"
    );
    let status = harness.page().marked_child(ROOT, "data-echo-status");
    assert_eq!(harness.page().text_of(status), "sent: 2  drops: 0");
}

#[test]
fn the_custom_send_publishes_the_field_as_it_stood_at_press_time() {
    let mut harness = echo_stub_mounted();
    let input = harness.page().marked_child(ROOT, "data-echo-input");
    // The sync call runs on the press's own event stack, so the component reads
    // the field inside `receive` rather than being handed its content.
    harness.page().element(input).value = "# typed markdown".to_string();
    let send_custom = harness.page().marked_child(ROOT, "data-echo-send-custom");
    harness.call(gesture(SEND_CUSTOM, send_custom));

    let transcript = harness.transcript();
    assert!(
        transcript.contains(&format!("dom.value(n{input}) -> \"# typed markdown\"")),
        "{transcript:?}"
    );
    assert_eq!(
        harness.page().published_on(ECHO_OUT_PORT),
        ["# typed markdown"],
        "the field's text is published verbatim, not JSON-encoded into a string"
    );
}

#[test]
fn a_quota_refusal_is_logged_and_leaves_the_counter_where_it_was() {
    let mut harness = echo_stub_mounted();
    harness.page().publish_answer = Some(ports::PublishError::QuotaExceeded);
    let send = harness.page().marked_child(ROOT, "data-echo-send");
    harness.call(gesture(SEND, send));

    let transcript = harness.transcript();
    assert!(
        transcript.iter().any(|line| line.starts_with("log.error(")),
        "the one transient refusal is reported, not swallowed: {transcript:?}"
    );
    assert!(harness.page().published_on(ECHO_OUT_PORT).is_empty());
    let status = harness.page().marked_child(ROOT, "data-echo-status");
    assert_eq!(
        harness.page().text_of(status),
        "sent: 0  drops: 0",
        "the status line must never claim a message the bus did not see"
    );

    // And the instance is still live: quota is transient, so the next press,
    // accepted, counts.
    harness.page().publish_answer = None;
    harness.call(gesture(SEND, send));
    assert_eq!(
        harness.page().published_on(ECHO_OUT_PORT),
        ["echo-stub message #1"]
    );
}

#[test]
fn a_structural_refusal_takes_the_instance_down() {
    // No later press repairs an unbound port or an oversize body, so the first
    // one is the detection and the instance takes its error card.
    let mut harness = echo_stub_mounted();
    harness.page().publish_answer = Some(ports::PublishError::NotPermitted);
    let send = harness.page().marked_child(ROOT, "data-echo-send");
    harness.call_expecting_a_trap(gesture(SEND, send));
    assert!(
        harness.page().published_on(ECHO_OUT_PORT).is_empty(),
        "the refused publish reached nothing"
    );
}

#[test]
fn the_panic_gesture_traps_the_instance() {
    // The fixture's third button exists to exercise the error-card path from a
    // real component; this is that path, executable.
    let mut harness = echo_stub_mounted();
    let panic_button = harness.page().marked_child(ROOT, "data-echo-panic");
    harness.call_expecting_a_trap(gesture(PANIC, panic_button));
}

#[test]
fn a_gesture_on_a_port_the_component_wired_nothing_to_is_refused_not_trapped() {
    // A sync port the component does not know is a wiring bug, not a memory
    // one: it answers err, keeps running, and the page keeps its instance.
    let mut harness = echo_stub_mounted();
    let refusal = harness.call_expecting_a_refusal(gesture("no-such-port", ROOT));
    match refusal {
        types::ReceiveError::ProcessingFailed(why) => {
            assert!(why.contains("no-such-port"), "{why}")
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn the_scrollback_cap_detaches_the_oldest_entries() {
    // The capability offers no traversal and no child count, so the component
    // keeps its own deque of entry handles and detaches from the front. Nothing
    // has executed that until now.
    let mut harness = echo_stub_mounted();
    let overflow = 2;
    let bodies: Vec<String> = (0..MAX_SCROLLBACK_ENTRIES + overflow)
        .map(|i| format!("message {i}"))
        .collect();
    let new: Vec<&str> = bodies.iter().map(String::as_str).collect();
    harness.call(delivery(&[], &new, 0));

    let scrollback = harness.page().marked_child(ROOT, "data-echo-scrollback");
    let children = harness.page().children(scrollback);
    assert_eq!(
        children.len(),
        MAX_SCROLLBACK_ENTRIES,
        "the cap bounds what the page renders"
    );
    assert!(
        harness.page().text_of(children[0]).contains("message 2"),
        "the oldest entries are the ones detached"
    );
    assert!(
        harness
            .page()
            .text_of(children[MAX_SCROLLBACK_ENTRIES - 1])
            .contains(&format!(
                "message {}",
                MAX_SCROLLBACK_ENTRIES + overflow - 1
            )),
        "the newest entry stays"
    );
    let transcript = harness.transcript();
    assert_eq!(
        transcript
            .iter()
            .filter(|line| line.starts_with("dom.remove("))
            .count(),
        overflow,
        "exactly the overflow is detached, one call each"
    );
}

// ---------------------------------------------------------------------------
// meeting: the press dispatch, which lives entirely in the glue
// ---------------------------------------------------------------------------

/// The instant every meeting fixture is anchored on.
fn now() -> DateTime<Utc> {
    DateTime::from_timestamp_millis(NOW_MS as i64).expect("the fixture clock is representable")
}

/// The one occurrence the agenda fixtures carry: a minute out, which is inside
/// the default takeover window and so the phase that shows the buttons.
fn occurrence() -> AckTarget {
    AckTarget {
        meeting_id: "m1".to_string(),
        start: now() + Duration::seconds(60),
    }
}

/// An agenda snapshot carrying that occurrence, or none at all.
fn agenda(carries_a_meeting: bool) -> String {
    let target = occurrence();
    let meetings = if carries_a_meeting {
        vec![serde_json::json!({
            "id": target.meeting_id,
            "start": target.start.to_rfc3339(),
            "title": "Standup",
        })]
    } else {
        vec![]
    };
    serde_json::json!({ "v": 1, "meetings": meetings }).to_string()
}

/// meeting, mounted and holding the escalating occurrence on screen.
fn escalated_meeting() -> Harness {
    let mut harness = meeting();
    harness.call(delivery_on(AGENDA_PORT, &[], &[&agenda(true)], 0));
    harness.transcript();
    harness
}

/// The handle of the panel button carrying `marker`.
fn meeting_button(harness: &mut Harness, marker: &str) -> u64 {
    harness.page().marked_descendant(ROOT, marker)
}

#[test]
fn the_dismiss_press_acks_the_occurrence_that_is_on_screen() {
    // The dispatch under test is the glue's: sync port name → action → body.
    // A swapped arm publishes a permanent dismissal where the user asked for a
    // five-minute snooze, on every device, and nothing else tests it.
    let mut harness = escalated_meeting();
    let dismiss = meeting_button(&mut harness, "data-meeting-dismiss");
    harness.call(gesture(DISMISS, dismiss));

    assert_eq!(
        harness.page().published_on(ACKS_PORT),
        [dismiss_body(&occurrence())],
        "the ack names the occurrence the panel was showing"
    );
}

#[test]
fn the_snooze_press_acks_the_same_occurrence_until_the_snooze_deadline() {
    let mut harness = escalated_meeting();
    let snooze = meeting_button(&mut harness, "data-meeting-snooze");
    harness.call(gesture(SNOOZE, snooze));

    assert_eq!(
        harness.page().published_on(ACKS_PORT),
        [snooze_body(
            &occurrence(),
            now() + Duration::seconds(SNOOZE_SECS)
        )],
        "the deadline is the button's own, measured from the activation clock"
    );
}

#[test]
fn a_press_with_nothing_on_screen_acks_nothing_and_does_not_trap() {
    // The buttons are hidden outside the escalated phases, so this is only
    // reachable through a programmatic click — which must cost the instance
    // nothing, not take its error card.
    let mut harness = meeting();
    let dismiss = meeting_button(&mut harness, "data-meeting-dismiss");
    harness.call(delivery_on(AGENDA_PORT, &[], &[&agenda(false)], 0));
    harness.call(gesture(DISMISS, dismiss));

    assert!(harness.page().published_on(ACKS_PORT).is_empty());
    let label = meeting_button(&mut harness, "data-meeting-label");
    assert_eq!(harness.page().text_of(label), "NO MEETINGS");
}

#[test]
fn a_quota_refused_ack_still_takes_the_meeting_off_this_device() {
    // The user dismissed the meeting. A refused publish means the other devices
    // keep escalating, not that this one should.
    //
    // The refusal is the ack port's alone: a refused *state* write is a
    // different loss with a different answer, pinned on its own below.
    let mut harness = escalated_meeting();
    harness
        .page()
        .publish_answer_on
        .insert(ACKS_PORT.to_string(), ports::PublishError::QuotaExceeded);
    let dismiss = meeting_button(&mut harness, "data-meeting-dismiss");
    harness.call(gesture(DISMISS, dismiss));

    assert!(harness.page().published_on(ACKS_PORT).is_empty());
    let transcript = harness.transcript();
    assert!(
        transcript.iter().any(|line| line.starts_with("log.error(")),
        "the refusal is reported, not swallowed: {transcript:?}"
    );
    let label = meeting_button(&mut harness, "data-meeting-label");
    assert_eq!(
        harness.page().text_of(label),
        "NO MEETINGS",
        "the local ack applies whatever the bus said"
    );
}

/// The takeover announcement rides the state port: it goes out on a transition
/// and not on every recompute, which is only true if the flag the last
/// activation set is the flag this one reads.
#[test]
fn the_takeover_announcement_is_made_once_across_activations() {
    let mut harness = escalated_meeting();
    let after_first = harness.page().published_on(MEETING_TAKEOVER_PORT).len();
    harness.call(delivery_on(AGENDA_PORT, &[&agenda(true)], &[], 0));
    let after_second = harness.page().published_on(MEETING_TAKEOVER_PORT).len();

    assert_eq!(after_first, 1, "the escalation announced the takeover once");
    assert_eq!(
        after_second, after_first,
        "the second recompute saw the announcement the first one recorded"
    );
}

/// A state body that is not meeting's shape is a version skew across a deploy.
/// Starting from a default would lose the view handles and build a second panel
/// over the first, so the activation is refused instead.
#[test]
fn a_state_body_meeting_cannot_read_refuses_the_activation() {
    let mut harness = meeting_unmounted().seeding_state("{\"view\":\"not a view\"}");
    let refusal = harness.call_expecting_a_refusal(mount());
    let refusal = format!("{refusal:?}");
    assert!(
        refusal.contains("retained state on port \\\"state\\\" is unreadable"),
        "{refusal}"
    );
}

#[test]
fn a_press_on_a_port_meeting_wired_nothing_to_is_refused_not_trapped() {
    let mut harness = escalated_meeting();
    let refusal = harness.call_expecting_a_refusal(gesture("no-such-port", ROOT));
    match refusal {
        types::ReceiveError::ProcessingFailed(why) => {
            assert!(why.contains("no-such-port"), "{why}")
        }
        other => panic!("{other:?}"),
    }
}

// ---------------------------------------------------------------------------
// chrome: page authority, which no other kind holds
// ---------------------------------------------------------------------------

/// A surface-state roster naming chrome and its one sibling, both mounted.
fn roster() -> String {
    let row = |instance: &str, kind: &str| SurfaceStateInstance {
        instance: instance.to_string(),
        kind: kind.to_string(),
        state: InstanceState::Mounted,
        reason: None,
    };
    serde_json::to_string(&SurfaceStateBody {
        v: CONTROL_PLANE_VERSION,
        instances: vec![
            row(CHROME_INSTANCE, "chrome"),
            row(SIBLING_INSTANCE, "protobar"),
        ],
    })
    .expect("the roster serializes")
}

#[test]
fn chrome_arranges_its_sibling_and_leaves_its_own_wrapper_alone() {
    // Chrome learns which row is its own by element identity — the wrapper its
    // host element hangs under is the one `page-dom` returns for its row and
    // for no other. If that identification never matches, chrome arranges its
    // own wrapper into a layout slot and the shell renders inside a panel.
    let mut harness = chrome();
    harness.call(delivery_on(SURFACE_STATE, &[], &[&roster()], 0));

    let page_root = harness
        .page()
        .page_root
        .expect("the fixture has a page root");
    let sibling = harness.page().wrapper_of(SIBLING_INSTANCE);
    let section = harness
        .page()
        .element(sibling)
        .parent
        .expect("the sibling's wrapper was adopted somewhere");
    assert_eq!(
        harness
            .page()
            .element(section)
            .attributes
            .get("data-instance"),
        Some(&SIBLING_INSTANCE.to_string()),
        "the wrapper hangs under its own instance's section"
    );

    let own = harness.page().wrapper_of(CHROME_INSTANCE);
    assert_eq!(
        harness.page().element(own).parent,
        Some(page_root),
        "chrome's own wrapper stays where the kernel put it"
    );
}

#[test]
fn a_click_on_a_toast_dismisses_that_toast_and_a_click_on_the_gap_dismisses_none() {
    // One delegated listener covers every toast and the gaps between them, so
    // which toast (if any) a press dismisses is decided by retargeting the
    // gesture's `target` through chrome's own handle bookkeeping.
    let mut harness = chrome();
    let toast_body = serde_json::to_string(&ToastBody {
        v: CONTROL_PLANE_VERSION,
        severity: ToastSeverity::Warning,
        text: "disk nearly full".to_string(),
        source: ToastSource::Kernel,
    })
    .expect("the toast serializes");
    harness.call(delivery_on(TOAST, &[], &[&toast_body], 0));

    let page_root = harness
        .page()
        .page_root
        .expect("the fixture has a page root");
    let container = harness
        .page()
        .marked_descendant(page_root, "data-surface-toasts");
    let toast = harness.page().children(container)[0];
    assert_eq!(harness.page().text_of(toast), "disk nearly full");

    harness.call(gesture_at(TOAST_DISMISS, container, container));
    assert_eq!(
        harness.page().children(container).len(),
        1,
        "a click that landed on no toast dismisses nothing"
    );

    harness.call(gesture_at(TOAST_DISMISS, container, toast));
    assert!(
        harness.page().children(container).is_empty(),
        "the toast the click landed on is gone"
    );
}

/// A state body that is not chrome's shape is a version skew across a deploy.
/// Starting from a default would lose the page furniture and build a second
/// banner over the first, so the activation is refused instead.
#[test]
fn a_state_body_chrome_cannot_read_refuses_the_activation() {
    let mut harness = chrome_unmounted().seeding_state("{\"view\":\"not a view\"}");
    let refusal = harness.call_expecting_a_refusal(mount());
    let refusal = format!("{refusal:?}");
    assert!(
        refusal.contains("retained state on port \\\"state\\\" is unreadable"),
        "{refusal}"
    );
}

// protobar: the kind whose view handles, priority slots and last render all
// ride one state body

/// The mount activation builds the bar's two elements and draws an empty
/// status line — nothing has been delivered yet.
#[test]
fn the_protobar_mount_activation_builds_its_two_elements() {
    let mut harness = protobar();
    harness.call(mount());

    let message = harness.page().marked_child(ROOT, PROTOBAR_MESSAGE_MARKER);
    let status = harness.page().marked_child(ROOT, PROTOBAR_STATUS_MARKER);
    assert_eq!(harness.page().text_of(message), "");
    assert_eq!(harness.page().text_of(status), "");
}

/// Everything protobar carries between activations is in one state body: the
/// two view handles it writes through, the slot holding the live message, the
/// drop counter, and the render it diffs the next one against. A second
/// delivery that writes into the *same* elements and sums its drops onto the
/// first one's can only have read all four back off the port.
#[test]
fn the_protobar_state_port_carries_its_view_slots_and_counters() {
    let mut harness = protobar();
    harness.call(mount());
    let message = harness.page().marked_child(ROOT, PROTOBAR_MESSAGE_MARKER);
    let status = harness.page().marked_child(ROOT, PROTOBAR_STATUS_MARKER);

    harness.call(delivery_on(PROTOBAR_IN_PORT, &[], &["first message"], 2));
    assert_eq!(harness.page().text_of(status), "dropped: 2");
    let rendered = harness.page().children(message);
    assert_eq!(rendered.len(), 1, "the message subtree is one paragraph");

    harness.call(delivery_on(PROTOBAR_IN_PORT, &[], &[], 3));
    assert_eq!(
        harness.page().text_of(status),
        "dropped: 5",
        "the second activation counted from the first one's state"
    );
    assert_eq!(
        harness.page().children(message),
        rendered,
        "nothing about the displayed message changed, so the render it diffs \
         against came back off the port and the subtree was left alone"
    );
    assert_eq!(
        harness.page().marked_child(ROOT, PROTOBAR_MESSAGE_MARKER),
        message,
        "and the handle it wrote through is the one the mount built"
    );
}

/// A state body that is not protobar's shape is a version skew across a
/// deploy. Starting from a default would lose the view handles and build a
/// second bar over the first, so the activation is refused instead.
#[test]
fn a_state_body_protobar_cannot_read_refuses_the_activation() {
    let mut harness = protobar().seeding_state("{\"view\":\"not a view\"}");
    let refusal = harness.call_expecting_a_refusal(mount());
    let refusal = format!("{refusal:?}");
    assert!(
        refusal.contains("retained state on port \\\"state\\\" is unreadable"),
        "{refusal}"
    );
}

// ---------------------------------------------------------------------------
// The harness itself: what it links, and what it records
// ---------------------------------------------------------------------------

/// A headless processor, driven on the page host. Nothing about `processor-log`
/// or `processor-config` is page-shaped: they are the proof that one artifact
/// runs wherever its import profile is satisfied, and that the page can satisfy
/// a profile with no `dom` in it.
fn headless(
    artifact: &str,
    grants: &[brenn_envelope::grants::ComponentGrant],
    page: Page,
) -> Harness {
    Harness::new(&common::artifact_path(artifact), page, grants)
}

/// The directive port both headless fixtures read, and the one they answer on.
const DIRECTIVE_PORT: &str = "in";
const DIRECTIVE_OUT: &str = "out";

#[test]
fn a_grant_the_caller_did_not_name_is_not_linked() {
    // Deny-by-default, the production host's rule: the linker holds exactly the
    // profile the specification requires, so an artifact that acquired an
    // import nobody granted fails at instantiation rather than at boot.
    //
    // Instantiation is inside the activation (linear memory is
    // activation-scoped), so mounting is not what refuses; the first
    // activation is.
    let refused = std::panic::catch_unwind(|| {
        let mut harness = Harness::new(
            &common::artifact_path("brenn_echo_stub"),
            Page::new(),
            &[Ports, Log],
        );
        harness.call(brenn_page_harness::mount());
    });
    assert!(
        refused.is_err(),
        "echo-stub imports `dom`, which nothing here linked"
    );
}

#[test]
fn the_config_impl_answers_out_of_the_map_the_page_was_built_with() {
    let config = BTreeMap::from([("test-key".to_string(), "carrots".to_string())]);
    let mut harness = headless(
        "brenn_processor_config",
        &[Ports, Config],
        Page::with_config(config),
    );
    harness.call(delivery_on(
        DIRECTIVE_PORT,
        &[],
        &[r#"{"cmd":"get","key":"test-key"}"#],
        0,
    ));
    assert_eq!(harness.page().published_on(DIRECTIVE_OUT), ["carrots"]);

    harness.call(delivery_on(
        DIRECTIVE_PORT,
        &[],
        &[r#"{"cmd":"get","key":"no-such-key"}"#],
        0,
    ));
    assert_eq!(
        harness.page().published_on(DIRECTIVE_OUT),
        ["carrots", "absent"],
        "a key the map does not hold reads as absent, not as an error"
    );
}

#[test]
fn the_alert_impl_records_what_the_component_paged_about() {
    let mut harness = headless("brenn_processor_log", &[Ports, Log, Alert], Page::new());
    harness.call(delivery_on(
        DIRECTIVE_PORT,
        &[],
        &[r#"{"cmd":"alert","severity":"critical","title":"disk","body":"nearly full"}"#],
        0,
    ));

    let alerts = &harness.page().alerts;
    assert_eq!(alerts.len(), 1, "{alerts:?}");
    assert_eq!(alerts[0].severity, "critical");
    assert_eq!(alerts[0].title, "disk");
    assert_eq!(alerts[0].body, "nearly full");
    assert!(
        harness
            .transcript()
            .iter()
            .any(|line| line.starts_with("alert.alert(critical,")),
        "the alert is in the transcript beside the DOM calls"
    );
}

// mode-clock: the headless kind, and the one whose whole observable behaviour
// is a decision carried from one activation to the next

/// The first recompute always dispatches — that is what converges the shell off
/// the page's initial stamp — and it parks the boundary that will cause the
/// next one.
#[test]
fn the_mount_recompute_dispatches_a_theme_and_parks_the_next_boundary() {
    let mut harness = mode_clock();
    harness.call(mount());

    assert_eq!(harness.page().published_on(MODE_THEME_PORT).len(), 1);
    let parked: Vec<&str> = harness
        .page()
        .parked
        .iter()
        .filter(|(port, _, _)| port == MODE_TICK_PORT)
        .map(|(_, body, _)| body.as_str())
        .collect();
    assert_eq!(parked, ["{}"], "the boundary wake is the message");
}

/// mode-clock's state is a recomputable summary — the theme is a function of
/// the clock and the config — so a body it cannot read is reported and the
/// activation starts over from the default rather than failing. The store that
/// ends it overwrites the bad body, which is what stops the skew repeating on
/// every activation forever.
#[test]
fn a_state_body_mode_clock_cannot_read_starts_from_the_default() {
    let mut harness = mode_clock().seeding_state("{\"config\":\"nonsense\"}");
    harness.call(mount());

    let transcript = harness.transcript();
    assert!(
        transcript
            .iter()
            .any(|line| line.starts_with("log.error(") && line.contains("unreadable")),
        "the skew is reported, not swallowed: {transcript:?}"
    );
    assert_eq!(
        state_writes(transcript).len(),
        1,
        "the default-derived state overwrote the body it could not read"
    );
    assert_eq!(
        harness.page().published_on(MODE_THEME_PORT).len(),
        1,
        "and the recompute the default asked for still happened"
    );

    // The overwrite is what the next activation reads: it converges instead of
    // reporting the same skew again.
    harness.call(delivery_on(MODE_TICK_PORT, &[], &["{}"], 0));
    let transcript = harness.transcript();
    assert!(
        !transcript.iter().any(|line| line.starts_with("log.error(")),
        "{transcript:?}"
    );
}

/// The dispatch-on-change decision lives in the state machine, and the state
/// machine lives on the `state` port: a second recompute at the same instant
/// re-dispatches nothing only if the first one's decision came back.
#[test]
fn a_second_recompute_at_the_same_instant_dispatches_nothing() {
    let mut harness = mode_clock();
    harness.call(mount());
    harness.call(delivery_on(MODE_TICK_PORT, &[], &["{}"], 0));

    assert_eq!(
        harness.page().published_on(MODE_THEME_PORT).len(),
        1,
        "the theme is unchanged, so the second activation had nothing to say"
    );
}
