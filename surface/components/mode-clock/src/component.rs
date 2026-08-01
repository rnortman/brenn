//! The mode-clock component — browser target only.
//!
//! Registers `<brenn-mode-clock>` via the optional component-support helpers,
//! installs the module panic hook, and wires the `config` port to the DOM-free
//! [`crate::logic::ModeClock`]. It renders no UI (headless: mounted but never
//! assigned a layout slot); its whole output is a `ThemeBody` it publishes on its
//! `theme` output port, bound to the reserved `local:brenn/theme` plane, which
//! chrome turns into a `data-theme` write on `<body>`.
//!
//! The theme is a pure function of the wall clock and the current config, so the
//! glue reads the browser clock and recomputes on every config delivery and
//! every scheduled boundary — never trusting elapsed time. A single `setTimeout`
//! is kept alive and rescheduled (clamped to a ~15 min ceiling so a
//! suspend/resume or DST step self-corrects within one interval).

use std::cell::RefCell;
use std::rc::Rc;

use brenn_surface_component_support::{
    Activation, MAX_WAKEUP_MS, PersistentTimer, Publisher, boot, claim_initialized,
    clamp_timeout_ms, component_log, publish, read_now_utc, register_component,
};
use brenn_surface_schema::{CONTROL_PLANE_VERSION, LogLevel, ThemeBody};
use wasm_bindgen::prelude::wasm_bindgen;
use web_sys::HtmlElement;

use crate::logic::{ConfigNote, ModeClock};

/// This component's kind — its config `kind`, its element-tag stem
/// (`brenn-<kind>`), and the `component` field of its panic events.
const KIND: &str = "mode-clock";

/// The theme output port — must match a `[[surface.output]] port` binding to the
/// reserved `local:brenn/theme` plane chrome consumes.
const THEME_PORT: &str = "theme";

/// A page-lifetime closure that reads the clock, dispatches on change, and
/// reschedules the boundary timer.
type Ticker = Rc<dyn Fn()>;

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
    // the host element, so it is built on connect and published here.
    let state = Rc::new(RefCell::new(ModeClock::new()));
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

/// The current local wall time as minutes since local midnight. Schedule
/// membership is judged in browser-local time: the schedule expresses the user's
/// local day, and there is no server tick to consult.
fn read_now_local_minutes() -> u16 {
    let date = js_sys::Date::new_0();
    (date.get_hours() as u16) * 60 + (date.get_minutes() as u16)
}

/// Build the recompute closure and run the first recompute, invoked from the
/// element's `connectedCallback` with the host element as `this`. No child DOM
/// is built — the component is headless.
fn on_connected(
    host: HtmlElement,
    state: &Rc<RefCell<ModeClock>>,
    wiring: &Rc<RefCell<Option<Wiring>>>,
) {
    // Run exactly once per element: `connectedCallback` fires on every insertion,
    // so a re-insertion must not double-build the ticker or its timer.
    if !claim_initialized(&host, KIND) {
        return;
    }

    let tick = make_ticker(host.clone(), Rc::clone(state));
    // Compute and dispatch from the default config before any delivery, so the
    // shell converges from the page's initial dark stamp.
    tick();
    *wiring.borrow_mut() = Some(Wiring { host, tick });
}

/// Feed each activation's newest config message to the pure state machine, then
/// recompute once — not once per message. Every note the fold returns is an
/// operator error: a malformed body is a publisher fault, a window of several
/// new configs is a `push_depth` fault. Both are logged; neither stops the
/// theme.
fn on_activation(
    activation: &Activation,
    state: &Rc<RefCell<ModeClock>>,
    wiring: &Rc<RefCell<Option<Wiring>>>,
) {
    let wiring = wiring.borrow();
    // No element yet, so no ticker and nothing to log against. The activation is
    // still consumed; its config remains visible as context in a later window
    // while retention covers it, and the first recompute runs on connect.
    let Some(wiring) = wiring.as_ref() else {
        return;
    };
    for window in &activation.ports {
        let notes = state
            .borrow_mut()
            .on_config_window(window)
            .expect("an activation window satisfies the mode-clock contract");
        for note in notes {
            let message = match note {
                ConfigNote::Misconfigured(message) => message,
                ConfigNote::Malformed(report) => report.log_message("mode-clock config"),
            };
            component_log(&wiring.host, LogLevel::Error, &message);
        }
    }
    // Once per activation, not once per message: the theme is a pure function of
    // the clock and the *effective* config, so only the last one matters.
    (wiring.tick)();
}

/// Build the page-lifetime recompute/reschedule closure. The boundary timer is a
/// [`PersistentTimer`] (one fire closure, reused): each recompute dispatches on a
/// theme change and then, in auto mode, reschedules the timer with a clamped
/// delay, or cancels it in a fixed dark/light mode.
fn make_ticker(host: HtmlElement, state: Rc<RefCell<ModeClock>>) -> Ticker {
    let timer = Rc::new(PersistentTimer::new());
    let ticker: Ticker = {
        let timer = Rc::clone(&timer);
        Rc::new(move || {
            let now_utc = read_now_utc();
            let plan = state.borrow_mut().tick(read_now_local_minutes());
            if let Some(theme) = plan.dispatch {
                let body = serde_json::to_string(&ThemeBody {
                    v: CONTROL_PLANE_VERSION,
                    theme: theme.as_wire_str().to_string(),
                })
                .expect("a ThemeBody serializes to JSON");
                publish(&host, THEME_PORT, &body);
            }
            match plan.next_in_minutes {
                Some(minutes) => {
                    // The boundary delta is whole minutes from the floored local
                    // minute; subtract the current second-of-minute so the timer
                    // targets the boundary instant itself rather than up to 59 s
                    // past it. Second-of-minute is identical in UTC and local
                    // (zone offsets are whole minutes), so it comes off `now_utc`.
                    let secs_into_minute = now_utc.timestamp().rem_euclid(60);
                    let target = now_utc + chrono::Duration::minutes(i64::from(minutes))
                        - chrono::Duration::seconds(secs_into_minute);
                    let delay = clamp_timeout_ms(now_utc, target, Some(MAX_WAKEUP_MS));
                    timer.reschedule(delay);
                }
                None => timer.cancel(),
            }
        })
    };
    timer.set_callback(Rc::clone(&ticker));
    ticker
}
