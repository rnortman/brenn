//! The meeting component — page-hosted target only.
//!
//! Builds its panel in the mount activation and wires the `agenda` and `acks`
//! ports to the DOM-free [`crate::logic::MeetingState`]. It renders a semantic
//! panel (label / title / big countdown / sub-line + a Dismiss/Snooze button
//! row) with a `data-state` hook the skins dress, publishes a takeover
//! request/release on the `takeover` port as the phase crosses the threshold,
//! and publishes dismiss/snooze acks on the `acks` port.
//!
//! Every text run reaches the page through [`dom::set_text`], which writes inert
//! text and never parses markup — so a meeting title is inert regardless of
//! content.
//!
//! The phase is a pure function of the wall clock, so the panel recomputes on
//! every activation from that activation's own clock reading — never trusting
//! elapsed time. The next boundary is a deferred self-publish on the `tick`
//! in/out port, re-parked from each recompute; near a meeting the recommended
//! interval is 1 s for a smooth countdown, coarser otherwise.
//!
//! Each button is its own sync port. The press names no occurrence: the sync
//! call runs on the press's own event stack, so the occurrence this component
//! recorded at its last render *is* what the user saw when they pressed.

use std::cell::RefCell;

use brenn_guest::{Activation, Error, Processor, dom, log, publish, repark};
use chrono::{DateTime, Utc};

use crate::logic::{
    AckAction, AckTarget, ActivationWindow, MeetingState, Recompute, SNOOZE_SECS, TakeoverAction,
    TakeoverBody, WarningLevel, dismiss_body, snooze_body,
};
use crate::spec::{
    InPort,
    port::{ACKS, TICK},
};

/// The body of a boundary wake. The tick's payload is irrelevant — the wake is
/// the message — but every body on this bus is JSON.
const TICK_BODY: &str = "{}";

/// The sync port the Dismiss button's press arrives on. Not `acks`: that is a
/// bound input port, and the kernel refuses a sync port that collides with one.
const DISMISS_PORT: dom::SyncPort = dom::SyncPort("dismiss");

/// The sync port the Snooze button's press arrives on.
const SNOOZE_PORT: dom::SyncPort = dom::SyncPort("snooze");

/// The marker attribute on this instance's host element. Every meeting rule in
/// `surface.css` and both skins descends from it, so dropping the stamp
/// silently unstyles the panel with no error raised.
const ROOT_MARKER: &str = "data-meeting-root";

/// The styling hook carrying the rendered escalation state.
const STATE_ATTRIBUTE: &str = "data-state";

/// The attribute that hides the button row outside the escalated phases, so the
/// buttons are neither shown nor clickable until then.
const HIDDEN_ATTRIBUTE: &str = "hidden";

impl crate::spec::TakeoverPayload for TakeoverBody {}

// One instantiation backs one instance for the page's lifetime, so the state
// machine, the view handles and the announced-takeover flag are ordinary
// interior-mutable module state. That is what lets two declarations of this one
// kind each keep their own agenda.
thread_local! {
    static PANEL: RefCell<Panel> = RefCell::new(Panel::new());
}

struct Panel {
    state: MeetingState,
    /// The elements an activation writes into, built by the mount activation.
    view: Option<View>,
    /// The active meeting's occurrence from the last render, so a press acks the
    /// meeting that was on screen.
    active: Option<AckTarget>,
    /// The takeover state of the last announcement the kernel took, so
    /// request/release goes out on a transition rather than on every recompute.
    last_takeover: bool,
}

/// The panel's semantic child elements, updated in place on each recompute.
struct View {
    root: dom::Node,
    label: dom::Node,
    title: dom::Node,
    countdown: dom::Node,
    subline: dom::Node,
    actions: dom::Node,
}

impl Panel {
    fn new() -> Panel {
        Panel {
            state: MeetingState::new(),
            view: None,
            active: None,
            last_takeover: false,
        }
    }

    /// The view, which every activation after the mount one has.
    fn view(&self) -> &View {
        self.view
            .as_ref()
            .expect("the mount activation builds the view before any other call")
    }
}

struct MeetingComponent;

impl Processor for MeetingComponent {
    fn receive(activation: Activation) -> Result<Option<String>, Error> {
        PANEL.with(|panel| on_activation(&activation, &mut panel.borrow_mut()))?;
        // Mount is answered with nothing, and neither button's default action is
        // one this component cancels.
        Ok(None)
    }
}

brenn_guest::export_processor!(MeetingComponent);

/// Build the panel when this is the mount call, act on a button press when this
/// is one, feed each delivered window to the pure state machine, then recompute
/// once — not once per message — announcing any takeover transition and parking
/// the next boundary.
fn on_activation(activation: &Activation, panel: &mut Panel) -> Result<(), Error> {
    let now_ms = activation
        .now()
        .ok_or_else(|| Error::failed("meeting: the host stamped no wall clock"))?;
    let now = DateTime::from_timestamp_millis(now_ms as i64).ok_or_else(|| {
        Error::failed(format!("meeting: {now_ms} is not a representable instant"))
    })?;

    if activation.sync_is(dom::MOUNT) {
        panel.view = Some(build_view());
    } else if let Some(port) = activation.sync() {
        on_gesture(port, panel, now)?;
    }
    for window in activation.delivered_windows() {
        // The tick's payload is irrelevant — the wake is the message.
        if InPort::of(window)? == InPort::Tick {
            continue;
        }
        let notes = panel
            .state
            .on_window(
                ActivationWindow {
                    port: window.port(),
                    new_raw: window.new_raw(),
                    dropped: u64::from(window.dropped()),
                },
                now,
            )
            .map_err(|violation| {
                Error::failed(format!("meeting: {violation:?} on an activation window"))
            })?;
        for note in notes {
            match note.level {
                WarningLevel::Warn => log::warn(note.message),
                WarningLevel::Error => log::error(note.message),
            }
        }
    }

    // Once per activation, not once per message: the render is a pure function
    // of the folded state and the clock.
    let view = panel.state.recompute(now);
    render(panel.view(), &view);
    panel.active = view.active.clone();
    announce_takeover(panel, view.want_takeover);
    // Every activation re-aims the boundary: the recommended interval tightens
    // to a second near a meeting and relaxes away from one, so the wake that
    // just fired is rarely the wake now wanted.
    let release_at = now_ms + u64::from(view.next_tick_secs) * 1_000;
    repark(activation, TICK, TICK_BODY, Some(release_at));
    Ok(())
}

/// Publish the ack the pressed button asked for.
fn on_gesture(port: &str, panel: &mut Panel, now: DateTime<Utc>) -> Result<(), Error> {
    let action = if port == DISMISS_PORT {
        AckAction::Dismiss
    } else if port == SNOOZE_PORT {
        AckAction::Snooze {
            until: now + chrono::Duration::seconds(SNOOZE_SECS),
        }
    } else {
        return Err(Error::failed(format!(
            "meeting wired no gesture to sync port {port:?}"
        )));
    };
    // A press with nothing on screen acks nothing: the buttons are hidden
    // outside the escalated phases, so this is only reachable through a
    // programmatic click.
    let Some(target) = panel.active.clone() else {
        return Ok(());
    };
    let body = match action {
        AckAction::Dismiss => dismiss_body(&target),
        AckAction::Snooze { until } => snooze_body(&target, until),
    };
    // The local transition happens whatever the bus says: the user dismissed the
    // meeting, and a refused publish means the other devices keep escalating,
    // not that this one should.
    if let Err(err) = publish(ACKS, &body) {
        assert!(
            err.is_quota(),
            "meeting: the ack publish on {ACKS:?} was refused: {err:?}"
        );
        log::error(format!("ack publish on {ACKS:?} refused: quota exceeded"));
    }
    panel.state.apply_local_ack(&target, action, now);
    Ok(())
}

/// Publish a takeover request/release when the desired state changed, and record
/// it as announced only once the kernel took the publish — a quota-refused
/// announcement leaves the transition pending for the next recompute to retry.
fn announce_takeover(panel: &mut Panel, want_takeover: bool) {
    if want_takeover == panel.last_takeover {
        return;
    }
    let action = if want_takeover {
        TakeoverAction::Request
    } else {
        TakeoverAction::Release
    };
    // The refusal taxonomy, not `?`: failing the activation would discard the
    // whole buffer, including the repark below, and stop the panel's clock. A
    // structural refusal is a deployment fault and traps.
    match crate::spec::takeover().publish(&TakeoverBody::of(action)) {
        Ok(()) => panel.last_takeover = want_takeover,
        Err(err) => {
            assert!(
                err.is_quota(),
                "meeting: the takeover announcement was refused: {err:?}"
            );
            log::error("takeover announcement refused: quota exceeded");
        }
    }
}

/// Build the panel under this instance's host element and wire its two buttons.
///
/// Called once, from the mount activation. Listeners are the kernel's and are
/// page-lifetime: each press arrives as a sync-call activation on its port.
fn build_view() -> View {
    let root = dom::root();
    dom::set_attribute(root, ROOT_MARKER, "");

    let label = dom::marked("div", "data-meeting-label");
    let title = dom::marked("div", "data-meeting-title");
    let countdown = dom::marked("div", "data-meeting-countdown");
    let subline = dom::marked("div", "data-meeting-subline");
    let actions = dom::marked("div", "data-meeting-actions");
    let dismiss = button("data-meeting-dismiss", "Dismiss");
    let snooze = button("data-meeting-snooze", "Snooze 5 min");
    dom::append(actions, dismiss);
    dom::append(actions, snooze);
    for child in [label, title, countdown, subline, actions] {
        dom::append(root, child);
    }

    dom::listen(dismiss, "click", DISMISS_PORT);
    dom::listen(snooze, "click", SNOOZE_PORT);

    View {
        root,
        label,
        title,
        countdown,
        subline,
        actions,
    }
}

fn button(marker: &str, label: &str) -> dom::Node {
    let node = dom::marked("button", marker);
    dom::set_text(node, label);
    node
}

/// Write the panel from `view`: the `data-state` hook on the host, each text
/// slot, and the button row's visibility.
fn render(panel: &View, view: &Recompute) {
    dom::set_attribute(panel.root, STATE_ATTRIBUTE, view.state.as_wire_str());
    dom::set_text(panel.label, &view.label);
    dom::set_text(panel.title, &view.title);
    dom::set_text(panel.countdown, &view.countdown);
    dom::set_text(panel.subline, &view.subline);
    dom::set_attribute_present(panel.actions, HIDDEN_ATTRIBUTE, !view.show_buttons);
}
