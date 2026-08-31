//! The mode-clock component — page-hosted target only.
//!
//! Wires the `config` port to the DOM-free [`crate::logic::ModeClock`]. It renders
//! no UI (headless: it holds an element and draws nothing in it); its whole output
//! is a theme body it publishes on its `theme` output port, bound to the reserved
//! `local:brenn/theme` plane, which chrome turns into a `data-theme` write on
//! `<body>`.
//!
//! The theme is a pure function of the wall clock and the current config, so it is
//! recomputed on every config delivery and every boundary — never trusting elapsed
//! time, and never parking a wake further out than the horizon a local-minute
//! count can be trusted as an elapsed-millisecond one. The boundary wake is a
//! deferred self-publish on the `tick` in/out port: each recompute parks the next
//! one and cancels whatever it replaces, both buffered with the activation, so an
//! entry that errs neither drops the standing tick nor schedules a new one.
//!
//! Every recompute happens inside an activation and nowhere else, because
//! [`ModeClock::tick`] *consumes* the dispatch-on-change decision: a call made
//! outside one would swallow a theme change that then has no publisher to go out
//! through. The first one is the mount activation, whose recompute always
//! dispatches — which is what converges the shell off the page's initial dark
//! stamp.

use std::cell::RefCell;

use brenn_guest::{Activation, Error, Processor, dom, log, repark};

use crate::logic::{ConfigNote, ConfigWindow, ModeClock, ThemeBody};
use crate::spec::{InPort, port::TICK};

/// The body of a boundary wake. The tick's payload is irrelevant — the wake is
/// the message — but every body on this bus is JSON.
const TICK_BODY: &str = "{}";

const MINUTES_PER_DAY: i64 = 24 * 60;
const MS_PER_MINUTE: i64 = 60_000;

impl crate::spec::ThemePayload for ThemeBody {}

// One instantiation backs one instance for the page's lifetime, so the state
// machine is ordinary interior-mutable module state rather than anything handed
// across a seam.
thread_local! {
    static CLOCK: RefCell<ModeClock> = RefCell::new(ModeClock::new());
}

struct ModeClockComponent;

impl Processor for ModeClockComponent {
    fn receive(activation: Activation) -> Result<Option<String>, Error> {
        CLOCK.with(|clock| on_activation(&activation, &mut clock.borrow_mut()))?;
        // Mount is a sync-call activation, and the mount call is answered with
        // nothing; no other activation this component sees is one at all.
        Ok(None)
    }
}

brenn_guest::export_processor!(ModeClockComponent);

/// The activation's own instant as minutes since local midnight.
///
/// Schedule membership is judged in browser-local time: the schedule expresses the
/// user's local day, and there is no server tick to consult. The instant comes from
/// the activation rather than from a clock read of this glue's — the page is asked
/// only for the zone.
fn local_minutes(now_ms: u64) -> u16 {
    let offset = i64::from(dom::utc_offset_minutes(now_ms));
    let utc_minutes = (now_ms / MS_PER_MINUTE as u64) as i64;
    (utc_minutes + offset).rem_euclid(MINUTES_PER_DAY) as u16
}

/// Feed each activation's newest config message to the pure state machine, then
/// recompute once — not once per message — and park the next boundary wake.
///
/// Every note the fold returns is an operator error: a malformed body is a
/// publisher fault, a window of several new configs is a `push_depth` fault. Both
/// are logged; neither stops the theme.
fn on_activation(activation: &Activation, clock: &mut ModeClock) -> Result<(), Error> {
    for window in activation.delivered_windows() {
        // The tick's payload is irrelevant — the wake is the message.
        if InPort::of(window)? == InPort::Tick {
            continue;
        }
        let notes = clock
            .on_config_window(ConfigWindow {
                port: window.port(),
                new_raw: window.new_raw(),
            })
            .map_err(|violation| {
                Error::failed(format!("mode-clock: {violation:?} on an activation window"))
            })?;
        for note in notes {
            match note {
                ConfigNote::Misconfigured(message) => log::error(message),
                ConfigNote::Malformed(report) => {
                    log::error(report.log_message("mode-clock config"))
                }
            }
        }
    }
    let now_ms = activation
        .now()
        .ok_or_else(|| Error::failed("mode-clock: the host stamped no wall clock"))?;
    // Once per activation, not once per message: the theme is a pure function of
    // the clock and the *effective* config, so only the last one matters.
    let plan = clock.tick(local_minutes(now_ms));
    if let Some(theme) = plan.dispatch {
        // The refusal taxonomy, not `?`: failing the activation here would
        // discard the whole buffer — including the repark below — and the
        // dispatch decision was already consumed by `tick`, so the theme change
        // would be lost with nothing left to carry it. A structural refusal is a
        // deployment fault and traps; a quota refusal is the transient one, and
        // the next boundary or config delivery recomputes.
        if let Err(err) = crate::spec::theme().publish(&ThemeBody::of(theme)) {
            assert!(
                err.is_quota(),
                "mode-clock: the theme publish was refused: {err:?}"
            );
            log::error("theme publish refused: quota exceeded");
        }
    }
    // A fixed dark/light mode has no next boundary, so it parks nothing — the
    // chain resumes from the config activation that puts the clock back in auto.
    repark(activation, TICK, TICK_BODY, plan.release_at(now_ms));
    Ok(())
}
