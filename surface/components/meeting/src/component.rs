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
//! The phase is a pure function of the wall clock, so the glue reads the browser
//! clock and recomputes on every delivery and every scheduled boundary — never
//! trusting elapsed time. A single `setTimeout` is kept alive and rescheduled
//! (clamped to a ~15 min ceiling so a suspend/resume or DST step self-corrects
//! within one interval); near a meeting the recommended interval is 1 s for a
//! smooth countdown, coarser otherwise.

use std::cell::RefCell;
use std::rc::Rc;

use brenn_surface_component_support::{
    Activation, MAX_WAKEUP_MS, PersistentTimer, Publisher, add_listener, append, boot,
    claim_initialized, clamp_timeout_ms, component_log, create_button, create_div, document,
    publish, read_now_utc, register_component,
};
use brenn_surface_proto::{CONTROL_PLANE_VERSION, LogLevel, TakeoverAction, TakeoverBody};
use wasm_bindgen::prelude::wasm_bindgen;
use web_sys::HtmlElement;

use crate::logic::dismiss_body;
use crate::logic::{
    AckAction, AckTarget, IngestOutcome, MeetingState, Recompute, SNOOZE_SECS, WarningLevel,
    snooze_body,
};

/// This component's kind — its config `kind`, its element-tag stem
/// (`brenn-<kind>`), and the `component` field of its panic events.
const KIND: &str = "meeting";

/// The ack output port — must match a `[[surface.output]] port` binding.
const ACKS_PORT: &str = "acks";

/// The output port meeting publishes takeover request/release on; must match the
/// `[[surface.output]]` binding onto `local:brenn/takeover`.
const TAKEOVER_PORT: &str = "takeover";

/// A page-lifetime closure that reads the clock, renders, dispatches takeover
/// transitions, and reschedules the boundary timer.
type Ticker = Rc<dyn Fn()>;

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
            move |activation: &Activation, _publisher: &mut Publisher| {
                on_activation(activation, &state, &wiring);
                Ok(())
            }
        },
    );
}

/// What an activation needs from the built element: the host to log against and
/// the recompute closure to run. `None` until `connectedCallback` builds them.
struct Wiring {
    host: HtmlElement,
    tick: Ticker,
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

    let tick = make_ticker(
        host.clone(),
        Rc::clone(&panel),
        Rc::clone(state),
        Rc::clone(&active),
    );
    // Render the initial idle state before any delivery.
    tick();

    wire_action_button(&dismiss, &host, state, &tick, &active, ActionKind::Dismiss);
    wire_action_button(&snooze, &host, state, &tick, &active, ActionKind::Snooze);

    *wiring.borrow_mut() = Some(Wiring { host, tick });
}

/// Feed each activation's new messages to the pure state machine, then recompute
/// once — not once per message. Agenda snapshots and acks arrive on the same
/// activation, distinguished by the window's port exactly where the dialect used
/// the event's `port` field. A malformed body (or an invalid per-meeting
/// override) is a publisher fault: log it and carry on.
///
/// A nonzero `dropped` means a snapshot or a dismiss/snooze ack was lost (a
/// device offline past the channel's queue bound). The pure state machine
/// reconverges on the next retained delivery, so there is nothing to recover
/// locally — but the loss must not be silent, or two devices can diverge (a
/// meeting dismissed elsewhere keeps escalating here) with no evidence in the
/// operator log.
fn on_activation(
    activation: &Activation,
    state: &Rc<RefCell<MeetingState>>,
    wiring: &Rc<RefCell<Option<Wiring>>>,
) {
    let wiring = wiring.borrow();
    // No panel yet, so no ticker and nothing to log against. The activation is
    // still consumed; its messages remain visible as context in a later window
    // while retention covers them, and the first render runs on connect.
    let Some(wiring) = wiring.as_ref() else {
        return;
    };
    let now = read_now_utc();
    for window in &activation.ports {
        if window.dropped > 0 {
            component_log(
                &wiring.host,
                LogLevel::Warn,
                &format!(
                    "meeting port {:?} dropped {} message(s); agenda/ack state may lag \
                     until the next delivery",
                    window.port, window.dropped
                ),
            );
        }
        for envelope in window.new_envelopes() {
            let envelope_json =
                serde_json::to_string(envelope).expect("a MessageEnvelope serializes to JSON");
            let outcome = state
                .borrow_mut()
                .on_message(&window.port, &envelope_json, now)
                .expect("an activation window satisfies the meeting contract");
            match outcome {
                IngestOutcome::Accepted { warnings } => {
                    for warning in warnings {
                        let level = match warning.level {
                            WarningLevel::Warn => LogLevel::Warn,
                            WarningLevel::Error => LogLevel::Error,
                        };
                        component_log(&wiring.host, level, &warning.message);
                    }
                }
                IngestOutcome::Malformed(report) => {
                    component_log(
                        &wiring.host,
                        LogLevel::Error,
                        &report.log_message("meeting body"),
                    );
                }
            }
        }
    }
    // Once per activation, not once per message: the render is a pure function of
    // the folded state and the clock.
    (wiring.tick)();
}

/// Which ack a button publishes.
#[derive(Clone, Copy)]
enum ActionKind {
    Dismiss,
    Snooze,
}

/// Wire a Dismiss/Snooze button: on click, if a meeting is active, publish its
/// ack on the `acks` port and transition locally immediately (responsive; the
/// echo and other devices converge on the idempotent ack), then re-render.
fn wire_action_button(
    button: &HtmlElement,
    host: &HtmlElement,
    state: &Rc<RefCell<MeetingState>>,
    tick: &Ticker,
    active: &Rc<RefCell<Option<AckTarget>>>,
    kind: ActionKind,
) {
    let host = host.clone();
    let state = Rc::clone(state);
    let tick = Rc::clone(tick);
    let active = Rc::clone(active);
    add_listener(button.as_ref(), "click", move |_event| {
        let Some(target) = active.borrow().clone() else {
            return;
        };
        let now = read_now_utc();
        let (body, action) = match kind {
            ActionKind::Dismiss => (dismiss_body(&target), AckAction::Dismiss),
            ActionKind::Snooze => {
                let until = now + chrono::Duration::seconds(SNOOZE_SECS);
                (snooze_body(&target, until), AckAction::Snooze { until })
            }
        };
        publish(&host, ACKS_PORT, &body);
        state.borrow_mut().apply_local_ack(&target, action, now);
        tick();
    });
}

/// Build the page-lifetime render/dispatch/reschedule closure. The boundary timer
/// is a [`PersistentTimer`] (one fire closure, reused): each tick renders,
/// publishes a takeover request/release on the `takeover` output port only when
/// the desired takeover state changed, and reschedules the timer with a clamped
/// delay.
fn make_ticker(
    host: HtmlElement,
    panel: Rc<Panel>,
    state: Rc<RefCell<MeetingState>>,
    active: Rc<RefCell<Option<AckTarget>>>,
) -> Ticker {
    let timer = Rc::new(PersistentTimer::new());
    // The last takeover state we dispatched, so we emit request/release only on a
    // transition rather than every tick.
    let last_takeover: Rc<RefCell<bool>> = Rc::new(RefCell::new(false));

    let ticker: Ticker = {
        let timer = Rc::clone(&timer);
        Rc::new(move || {
            let now = read_now_utc();
            let view = state.borrow().recompute(now);
            render(&host, &panel, &view);
            *active.borrow_mut() = view.active.clone();

            let prev = *last_takeover.borrow();
            if view.want_takeover != prev {
                let action = if view.want_takeover {
                    TakeoverAction::Request
                } else {
                    TakeoverAction::Release
                };
                // The router overwrites `instance` with meeting's authenticated
                // identity, so the empty value here is never trusted on the wire.
                let body = serde_json::to_string(&TakeoverBody {
                    v: CONTROL_PLANE_VERSION,
                    action,
                    instance: String::new(),
                })
                .expect("a TakeoverBody serializes to JSON");
                publish(&host, TAKEOVER_PORT, &body);
                *last_takeover.borrow_mut() = view.want_takeover;
            }

            let target = now + chrono::Duration::seconds(i64::from(view.next_tick_secs));
            let delay = clamp_timeout_ms(now, target, Some(MAX_WAKEUP_MS));
            timer.reschedule(delay);
        })
    };
    timer.set_callback(Rc::clone(&ticker));
    ticker
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
/// `make surface-wasm-test`. wasm32-only, matching the crate's wasm-gated glue.
#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use super::*;
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
}
