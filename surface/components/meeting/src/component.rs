//! The meeting component — browser target only.
//!
//! Registers `<brenn-meeting>` via the optional component-support helpers,
//! installs the module panic hook, and wires the `agenda` and `acks` ports to
//! the DOM-free [`crate::logic::MeetingState`]. It renders a semantic panel
//! (label / title / big countdown / sub-line + a Dismiss/Snooze button row) with
//! a `data-state` hook the skins dress, dispatches `brenn-takeover-request` /
//! `-release` as the phase crosses the takeover threshold, and publishes
//! dismiss/snooze acks on the `acks` output port.
//!
//! Every text run reaches the DOM via `set_text_content` — never `innerHTML`,
//! never an anchor — so a meeting title is inert text regardless of content.
//!
//! The phase is a pure function of the wall clock, so the panel recomputes on
//! every activation from that activation's own clock reading — never trusting
//! elapsed time. The next boundary is a deferred self-publish on the `tick`
//! in/out port, re-parked from each recompute; near a meeting the recommended
//! interval is 1 s for a smooth countdown, coarser otherwise.
//!
//! Every publish this component makes — the ack, the takeover transition, the
//! next boundary — is made from inside `on_activation`. A button press causes
//! one of its own: the wiring encodes which button and which occurrence, and the
//! entry decides and publishes.

use std::cell::RefCell;
use std::rc::Rc;

use brenn_surface_component_support::{
    Activation, Publisher, append, boot, claim_initialized, component_log, create_button,
    create_div, document, publish_or_fault, read_now_utc, register_component, repark_tick,
    wire_gesture,
};
use brenn_surface_schema::{CONTROL_PLANE_VERSION, LogLevel, TakeoverAction, TakeoverBody};
use chrono::{DateTime, Utc};
use wasm_bindgen::prelude::wasm_bindgen;
use web_sys::HtmlElement;

use crate::logic::dismiss_body;
use crate::logic::{
    AckAction, AckKind, AckTarget, MeetingState, Recompute, SNOOZE_SECS, WarningLevel,
    ack_request_body, parse_ack_request, snooze_body,
};
use crate::spec::port::{ACKS, TAKEOVER, TICK};

/// This component's kind — its config `kind`, its element-tag stem
/// (`brenn-<kind>`), and the `component` field of its panic events.
const KIND: &str = "meeting";

/// The sync port a Dismiss/Snooze press arrives on. Not `acks`: that is a bound
/// input port, and the kernel refuses a sync port that collides with one.
const ACK_PORT: &str = "ack";

/// A page-lifetime closure that recomputes the panel as of an instant, renders
/// it, and records the occurrence now on screen. It publishes nothing and
/// schedules nothing — both are the activation's, where the buffered
/// [`Publisher`] lives — and hands the recompute back so its caller can decide
/// from the same view it drew.
type Render = Rc<dyn Fn(DateTime<Utc>) -> Recompute>;

/// The panel's semantic child elements, updated in place on each recompute.
struct Panel {
    label: HtmlElement,
    title: HtmlElement,
    countdown: HtmlElement,
    subline: HtmlElement,
    actions: HtmlElement,
}

/// The loader's entry, called once after this module's `default` init with the
/// instance this module record was loaded for. The whole boot sequence lives
/// here rather than in `#[wasm_bindgen(start)]`: the panic hook's subject and the
/// element's tag are both this instance's, and neither exists until the bind.
#[wasm_bindgen]
pub fn brenn_bind_instance(instance: String) {
    boot(&instance);
    // This instance's state and its recompute closure, captured by both the
    // connected closure and the activation entry: one module record backs one
    // instance, so these are that instance's and nobody else's. The ticker needs
    // the built panel, so it is made on connect and published here.
    let state = Rc::new(RefCell::new(MeetingState::new()));
    let wiring: Rc<RefCell<Option<Wiring>>> = Rc::new(RefCell::new(None));
    register_component(
        KIND,
        {
            let state = Rc::clone(&state);
            let wiring = Rc::clone(&wiring);
            move |host| on_connected(host, &state, &wiring)
        },
        {
            let state = Rc::clone(&state);
            let wiring = Rc::clone(&wiring);
            move |activation: &Activation, publisher: &mut Publisher| {
                on_activation(activation, &state, &wiring, publisher);
                Ok(None)
            }
        },
    );
}

/// What an activation needs from the built element: the host to log against, the
/// render closure to run, and the takeover state last announced. `None` only
/// before `connectedCallback` builds them: `register_component` hands the kernel
/// the entry after `on_connected` returns, so every activation finds them.
struct Wiring {
    host: HtmlElement,
    render: Render,
    /// The takeover state of the last announcement, so request/release goes out
    /// on a transition rather than on every recompute.
    last_takeover: RefCell<bool>,
}

/// Build the panel and wire its buttons, invoked from the element's
/// `connectedCallback` with the host element as `this`.
fn on_connected(
    host: HtmlElement,
    state: &Rc<RefCell<MeetingState>>,
    wiring: &Rc<RefCell<Option<Wiring>>>,
) {
    // Build exactly once per element: `connectedCallback` fires on every
    // insertion, so a re-insertion must not duplicate the UI, listeners, or
    // timers.
    if !claim_initialized(&host, KIND) {
        return;
    }

    let doc = document();

    // Stable, kind-identifying marker on the host so skins can anchor on the
    // meeting element itself: the kernel names the element `brenn-meeting--<inst>`
    // (per-instance tag), so a bare `brenn-meeting` type selector never matches.
    // The host cannot anchor on the wrapper's `data-kind` instead, because that
    // wrapper may hold a kernel error card rather than this component. All meeting
    // skin rules in surface.css, bench.css, and foundry.css descend from this
    // attribute: removing it silently unstyles the component (the bug this marker
    // fixes), with no error raised.
    host.set_attribute("data-meeting-root", "")
        .expect("set data-meeting-root marker on the host");

    let label = create_div(&doc, "data-meeting-label");
    let title = create_div(&doc, "data-meeting-title");
    let countdown = create_div(&doc, "data-meeting-countdown");
    let subline = create_div(&doc, "data-meeting-subline");
    let actions = create_div(&doc, "data-meeting-actions");
    let dismiss = create_button(&doc, "data-meeting-dismiss", "Dismiss");
    let snooze = create_button(&doc, "data-meeting-snooze", "Snooze 5 min");
    append(&actions, &dismiss);
    append(&actions, &snooze);
    for child in [&label, &title, &countdown, &subline, &actions] {
        append(&host, child);
    }

    let panel = Rc::new(Panel {
        label,
        title,
        countdown,
        subline,
        actions,
    });
    // The active meeting's occurrence from the last render, so a button press
    // targets the meeting currently on screen.
    let active: Rc<RefCell<Option<AckTarget>>> = Rc::new(RefCell::new(None));

    let render = make_renderer(
        host.clone(),
        Rc::clone(&panel),
        Rc::clone(state),
        Rc::clone(&active),
    );
    // Render the initial idle state before any delivery. A render is the one side
    // effect connect-time code may have; nothing is published and nothing is
    // scheduled here, and the first of both is the mount activation's.
    render(read_now_utc());

    wire_action_button(&dismiss, &host, &active, AckKind::Dismiss);
    wire_action_button(&snooze, &host, &active, AckKind::Snooze);

    *wiring.borrow_mut() = Some(Wiring {
        host,
        render,
        last_takeover: RefCell::new(false),
    });
}

/// Act on a button press if this activation is one, feed each delivered window to
/// the pure state machine, then recompute once — not once per message —
/// announcing any takeover transition and parking the next boundary. Errors are
/// logged; none stop the panel.
fn on_activation(
    activation: &Activation,
    state: &Rc<RefCell<MeetingState>>,
    wiring: &Rc<RefCell<Option<Wiring>>>,
    publisher: &mut Publisher,
) {
    let wiring = wiring.borrow();
    let wiring = wiring
        .as_ref()
        .expect("on_connected builds the panel before the entry is registered");
    let now_ms = activation
        .now
        .expect("the surface kernel stamps every activation with its wall clock");
    let now = DateTime::from_timestamp_millis(now_ms as i64)
        .expect("an activation's wall clock is a representable instant");

    if let Some((_, press)) = activation.sync_request() {
        on_press(&press.body, state, wiring, publisher, now);
    }
    for window in activation.delivered_windows() {
        // The tick's payload is irrelevant — the wake is the message.
        if window.port == TICK {
            continue;
        }
        let notes = state
            .borrow_mut()
            .on_window(window, now)
            .expect("an activation window satisfies the meeting contract");
        for note in notes {
            let level = match note.level {
                WarningLevel::Warn => LogLevel::Warn,
                WarningLevel::Error => LogLevel::Error,
            };
            component_log(&wiring.host, level, &note.message);
        }
    }

    // Once per activation, not once per message: the render is a pure function of
    // the folded state and the clock.
    let view = (wiring.render)(now);
    announce_takeover(wiring, publisher, view.want_takeover);
    // Every activation re-aims the boundary: the recommended interval tightens to
    // a second near a meeting and relaxes away from one, so the wake that just
    // fired is rarely the wake now wanted.
    let release_at = now_ms + u64::from(view.next_tick_secs) * 1_000;
    repark_tick(activation, publisher, &wiring.host, TICK, Some(release_at));
}

/// Publish the ack one button press asked for and apply it locally at once
/// (responsive; the echo and other devices converge on the idempotent ack).
///
/// The occurrence comes from the press rather than from the panel's current
/// state: the user acted on what they saw. A press that named no occurrence
/// reached a hidden button through a programmatic click and acks nothing.
fn on_press(
    press: &str,
    state: &Rc<RefCell<MeetingState>>,
    wiring: &Wiring,
    publisher: &mut Publisher,
    now: DateTime<Utc>,
) {
    let parsed = parse_ack_request(press).unwrap_or_else(|reason| {
        panic!("meeting's own gesture wiring produced an unreadable press: {reason}")
    });
    let Some((kind, target)) = parsed else {
        return;
    };
    let (body, action) = match kind {
        AckKind::Dismiss => (dismiss_body(&target), AckAction::Dismiss),
        AckKind::Snooze => {
            let until = now + chrono::Duration::seconds(SNOOZE_SECS);
            (snooze_body(&target, until), AckAction::Snooze { until })
        }
    };
    // The local transition happens whatever the bus says: the user dismissed the
    // meeting, and a quota-refused publish means the other devices keep escalating,
    // not that this one should.
    publish_or_fault(publisher, &wiring.host, ACKS, &body);
    state.borrow_mut().apply_local_ack(&target, action, now);
}

/// Publish a takeover request/release when the desired state changed, and record
/// it as announced only once the kernel took the publish — a refused announcement
/// leaves the transition pending for the next recompute to retry.
fn announce_takeover(wiring: &Wiring, publisher: &mut Publisher, want_takeover: bool) {
    if want_takeover == *wiring.last_takeover.borrow() {
        return;
    }
    let action = if want_takeover {
        TakeoverAction::Request
    } else {
        TakeoverAction::Release
    };
    // The router overwrites `instance` with meeting's authenticated identity, so
    // the empty value here is never trusted on the wire.
    let body = serde_json::to_string(&TakeoverBody {
        v: CONTROL_PLANE_VERSION,
        action,
        instance: String::new(),
    })
    .expect("a TakeoverBody serializes to JSON");
    if publish_or_fault(publisher, &wiring.host, TAKEOVER, &body) {
        *wiring.last_takeover.borrow_mut() = want_takeover;
    }
}

/// Wire a Dismiss/Snooze button to a sync-call activation on the `ack` port.
///
/// The press body names the button and the occurrence the panel was showing when
/// it happened; every decision the press implies — which ack, what snooze
/// deadline, whether to publish at all — is the entry's.
fn wire_action_button(
    button: &HtmlElement,
    host: &HtmlElement,
    active: &Rc<RefCell<Option<AckTarget>>>,
    kind: AckKind,
) {
    let active = Rc::clone(active);
    wire_gesture(host, button.as_ref(), "click", ACK_PORT, move |_event| {
        ack_request_body(kind, active.borrow().as_ref())
    });
}

/// Build the page-lifetime render closure: it recomputes as of the instant it is
/// handed, writes the panel, records the occurrence now on screen for a press to
/// name, and hands the recompute back. It publishes nothing and schedules
/// nothing — both live in the activation, where the buffered `Publisher` does.
fn make_renderer(
    host: HtmlElement,
    panel: Rc<Panel>,
    state: Rc<RefCell<MeetingState>>,
    active: Rc<RefCell<Option<AckTarget>>>,
) -> Render {
    Rc::new(move |now| {
        let view = state.borrow().recompute(now);
        render(&host, &panel, &view);
        *active.borrow_mut() = view.active.clone();
        view
    })
}

/// Write the panel from `view`: the `data-state` hook on the host, each text
/// slot, and the button row's visibility.
fn render(host: &HtmlElement, panel: &Panel, view: &Recompute) {
    host.set_attribute("data-state", view.state.as_wire_str())
        .expect("set data-state attribute");
    panel.label.set_text_content(Some(&view.label));
    panel.title.set_text_content(Some(&view.title));
    panel.countdown.set_text_content(Some(&view.countdown));
    panel.subline.set_text_content(Some(&view.subline));
    // Hide the buttons outside takeover+ phases so they are neither shown nor
    // clickable until escalation; the skins additionally dress them.
    panel.actions.set_hidden(!view.show_buttons);
}

/// Browser-DOM tests for the connect path, run under wasm-bindgen-test via
/// the browser test runner. wasm32-only, matching the crate's wasm-gated glue.
#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use super::*;
    use brenn_surface_contract::{PublishError, publish_status_str};
    use brenn_surface_test_fixtures::browser::{
        activation_json, answer_publishes_with, mount, record_ops, take_recorded,
    };
    use wasm_bindgen::JsCast;
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

    wasm_bindgen_test_configure!(run_in_browser);

    /// A fresh, unattached host element to drive `on_connected` against, standing
    /// in for the kernel's per-instance `<brenn-meeting--…>` custom element.
    fn fresh_host() -> HtmlElement {
        document()
            .create_element("div")
            .expect("document creates a div")
            .dyn_into::<HtmlElement>()
            .expect("created div is an HtmlElement")
    }

    /// The connect path must stamp `data-meeting-root` on the host — the anchor
    /// every meeting skin rule descends from. A refactor dropping the stamp
    /// silently reproduces the unstyled-panel bug this marker fixes; this test
    /// makes that failure loud.
    #[wasm_bindgen_test]
    fn on_connected_stamps_root_marker_and_child_hooks() {
        let host = fresh_host();
        let state = Rc::new(RefCell::new(MeetingState::new()));
        let wiring: Rc<RefCell<Option<Wiring>>> = Rc::new(RefCell::new(None));

        on_connected(host.clone(), &state, &wiring);

        assert!(
            host.has_attribute("data-meeting-root"),
            "host must carry the data-meeting-root skin anchor after connect"
        );
        // The idle render runs on connect, so the state hook is present too.
        assert!(
            host.has_attribute("data-state"),
            "host must carry the data-state hook after the initial render"
        );
        // Every text slot and the action row the skins dress must exist under the
        // host, plus both buttons inside the action row.
        for hook in [
            "[data-meeting-label]",
            "[data-meeting-title]",
            "[data-meeting-countdown]",
            "[data-meeting-subline]",
            "[data-meeting-actions]",
            "[data-meeting-actions] [data-meeting-dismiss]",
            "[data-meeting-actions] [data-meeting-snooze]",
        ] {
            assert!(
                host.query_selector(hook)
                    .expect("query_selector runs on the host")
                    .is_some(),
                "connect must build the {hook} child hook"
            );
        }
    }

    /// `connectedCallback` fires on every insertion; a re-connect must not rebuild
    /// the panel. The second call bails on the init marker, leaving exactly one set
    /// of child hooks.
    #[wasm_bindgen_test]
    fn on_connected_is_idempotent() {
        let host = fresh_host();
        let state = Rc::new(RefCell::new(MeetingState::new()));
        let wiring: Rc<RefCell<Option<Wiring>>> = Rc::new(RefCell::new(None));

        on_connected(host.clone(), &state, &wiring);
        on_connected(host.clone(), &state, &wiring);

        let labels = host
            .query_selector_all("[data-meeting-label]")
            .expect("query_selector_all runs on the host");
        assert_eq!(
            labels.length(),
            1,
            "a re-connect must not duplicate the panel"
        );
    }

    /// The instance this test binary binds its one module record to.
    const TEST_INSTANCE: &str = "wbt-meeting";

    /// The occurrence every activation below is about.
    const MEETING_START: &str = "2126-07-08T12:00:00Z";

    /// A one-meeting agenda snapshot body for the occurrence `id`, all of them
    /// starting at [`MEETING_START`].
    fn agenda_body(id: &str) -> String {
        serde_json::json!({
            "v": 1,
            "meetings": [{ "id": id, "start": MEETING_START, "title": "Design" }],
        })
        .to_string()
    }

    /// `MEETING_START` minus `secs`, in epoch milliseconds.
    fn before_start_ms(secs: i64) -> u64 {
        (MEETING_START
            .parse::<DateTime<Utc>>()
            .unwrap()
            .timestamp_millis()
            - secs * 1_000) as u64
    }

    /// The mount activation publishes nothing and parks exactly one boundary
    /// wake — connect-time code that published or scheduled would show up here
    /// as an extra op, and a mount that parked nothing would leave a panel that
    /// never counts down again — and a Dismiss press then publishes the ack and
    /// suppresses it locally at once, from inside the activation the press
    /// caused.
    ///
    /// One test because one module record binds one instance: a second mount in
    /// this binary is the double-bind the SDK panics on.
    ///
    /// The second half pins the critical invariant: the press names an occurrence,
    /// and every decision it implies is made where the buffered publisher lives.
    /// A press whose ack never reached the bus, or one that acked whatever
    /// happens to be on screen when the entry runs, both read the same from the
    /// DOM.
    ///
    /// The last half pins the refusal arm: a takeover announcement the kernel did
    /// not take stays pending, so the next recompute re-announces it. Recording
    /// the transition regardless would wedge the panel out of takeover for the
    /// page's life after one transient quota, with one log line as the only trace.
    #[wasm_bindgen_test]
    fn the_panel_mounts_quiet_and_a_press_publishes_its_ack() {
        // Installed before the mount: connect-time code that published, parked or
        // asked for a sync activation lands here, where silence is the assertion.
        let ops = record_ops();
        let (entry, host) = mount(KIND, TEST_INSTANCE, brenn_bind_instance);
        assert_eq!(
            ops.length(),
            0,
            "connect-time code renders and wires only, and reaches no kernel seam"
        );
        let now_ms = before_start_ms(30);

        // The mount activation: nothing delivered, nothing to say.
        entry
            .call1(
                &wasm_bindgen::JsValue::NULL,
                &wasm_bindgen::JsValue::from_str(&activation_json(&[], None, now_ms)),
            )
            .expect("the entry returns ok");
        let mount_ops = take_recorded(&ops);
        let [wake] = &mount_ops[..] else {
            panic!(
                "an idle mount wants no takeover and no ack — only its next boundary: {mount_ops:?}"
            )
        };
        assert_eq!(
            (
                wake[0].as_str(),
                wake[1].as_str(),
                wake[2].as_str(),
                wake[3].as_str()
            ),
            ("defer", "publish", TICK, "{}"),
        );
        assert!(
            wake[4].parse::<u64>().expect("a decimal release instant") > now_ms,
            "the boundary is aimed into the activation's future: {wake:?}"
        );

        // An escalating meeting.
        entry
            .call1(
                &wasm_bindgen::JsValue::NULL,
                &wasm_bindgen::JsValue::from_str(&activation_json(
                    &[("agenda", &agenda_body("m1"))],
                    None,
                    now_ms,
                )),
            )
            .expect("the entry returns ok");
        assert_eq!(
            host.get_attribute("data-state").as_deref(),
            Some("critical"),
            "30 s out is inside the critical rung"
        );
        let agenda_ops = take_recorded(&ops);
        assert!(
            agenda_ops
                .iter()
                .any(|row| (row[0].as_str(), row[2].as_str()) == ("publish", TAKEOVER)),
            "an escalating meeting requests the takeover overlay: {agenda_ops:?}"
        );

        // The Dismiss press against the on-screen meeting.
        let press = ack_request_body(
            AckKind::Dismiss,
            Some(&AckTarget {
                meeting_id: "m1".to_string(),
                start: MEETING_START.parse().expect("the fixture start parses"),
            }),
        );
        entry
            .call1(
                &wasm_bindgen::JsValue::NULL,
                &wasm_bindgen::JsValue::from_str(&activation_json(
                    &[(ACK_PORT, &press)],
                    Some(ACK_PORT),
                    now_ms,
                )),
            )
            .expect("the entry returns ok");

        let press_ops = take_recorded(&ops);
        let [ack, takeover, wake] = &press_ops[..] else {
            panic!("expected an ack, a takeover release and a wake: {press_ops:?}")
        };
        assert_eq!((ack[0].as_str(), ack[2].as_str()), ("publish", ACKS));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&ack[3]).expect("the ack body is JSON")["action"],
            "dismiss"
        );
        assert_eq!(
            (takeover[0].as_str(), takeover[2].as_str()),
            ("publish", TAKEOVER),
            "the dismissal drops the panel out of takeover, which must be released"
        );
        assert_eq!((wake[0].as_str(), wake[2].as_str()), ("defer", TICK));
        assert_eq!(
            host.get_attribute("data-state").as_deref(),
            Some("idle"),
            "the local ack applies at once, without waiting for the echo"
        );

        // A second occurrence escalates while the kernel is refusing publishes:
        // the announcement is attempted, refused, reported, and left pending.
        answer_publishes_with(&ops, publish_status_str(Err(PublishError::QuotaExceeded)));
        entry
            .call1(
                &wasm_bindgen::JsValue::NULL,
                &wasm_bindgen::JsValue::from_str(&activation_json(
                    &[("agenda", &agenda_body("m2"))],
                    None,
                    now_ms,
                )),
            )
            .expect("a refused publish is an answer, not a failed activation");
        let refused_ops = take_recorded(&ops);
        assert_eq!(
            refused_ops
                .iter()
                .filter(|row| (row[0].as_str(), row[2].as_str()) == ("publish", TAKEOVER))
                .count(),
            1,
            "the transition is announced once: {refused_ops:?}"
        );
        assert!(
            refused_ops.iter().any(|row| row[0] == "log"),
            "the refusal is reported to the operator: {refused_ops:?}"
        );

        // The next recompute re-announces the same transition, because nothing
        // recorded it as announced.
        answer_publishes_with(&ops, publish_status_str(Ok(())));
        entry
            .call1(
                &wasm_bindgen::JsValue::NULL,
                &wasm_bindgen::JsValue::from_str(&activation_json(&[(TICK, "{}")], None, now_ms)),
            )
            .expect("the entry returns ok");
        let retry_ops = take_recorded(&ops);
        let takeover = retry_ops
            .iter()
            .find(|row| (row[0].as_str(), row[2].as_str()) == ("publish", TAKEOVER))
            .unwrap_or_else(|| {
                panic!("a refused transition is retried, not dropped: {retry_ops:?}")
            });
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&takeover[3])
                .expect("the takeover body is JSON")["action"],
            "request"
        );

        // And once taken, it is not re-announced.
        entry
            .call1(
                &wasm_bindgen::JsValue::NULL,
                &wasm_bindgen::JsValue::from_str(&activation_json(&[(TICK, "{}")], None, now_ms)),
            )
            .expect("the entry returns ok");
        let settled_ops = take_recorded(&ops);
        assert!(
            !settled_ops
                .iter()
                .any(|row| (row[0].as_str(), row[2].as_str()) == ("publish", TAKEOVER)),
            "a recorded transition is announced once: {settled_ops:?}"
        );
    }
}
