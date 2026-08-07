//! The mode-clock component — browser target only.
//!
//! Registers `<brenn-mode-clock>` via the optional component-support helpers,
//! installs the module panic hook, and wires the `config` port to the DOM-free
//! [`crate::logic::ModeClock`]. It renders no UI (headless: mounted but never
//! assigned a layout slot); its whole output is a `ThemeBody` it publishes on its
//! `theme` output port, bound to the reserved `local:brenn/theme` plane, which
//! chrome turns into a `data-theme` write on `<body>`.
//!
//! The theme is a pure function of the wall clock and the current config, so it is
//! recomputed on every config delivery and every boundary — never trusting elapsed
//! time, and never parking a wake further out than the horizon a local-minute
//! count can be trusted as an elapsed-millisecond one. The boundary wake is a
//! deferred self-publish on the `tick` in/out port:
//! each recompute parks the next one and cancels whatever it replaces, both
//! buffered with the activation, so an entry that errs neither drops the standing
//! tick nor schedules a new one.
//!
//! Every recompute happens inside an activation and nowhere else, because
//! [`ModeClock::tick`] *consumes* the dispatch-on-change decision: a call made
//! outside one would swallow a theme change that then has no publisher to go out
//! through. The first one is the guaranteed mount activation, whose recompute
//! always dispatches — which is what converges the shell off the page's initial
//! dark stamp.

use std::cell::RefCell;
use std::rc::Rc;

use brenn_surface_component_support::{
    Activation, Publisher, boot, claim_initialized, component_log, register_component, repark_tick,
};
use brenn_surface_schema::{CONTROL_PLANE_VERSION, LogLevel, ThemeBody};
use wasm_bindgen::JsValue;
use wasm_bindgen::prelude::wasm_bindgen;
use web_sys::HtmlElement;

use crate::logic::{ConfigNote, ModeClock, THEME_PORT};

/// This component's kind — its config `kind`, its element-tag stem
/// (`brenn-<kind>`), and the `component` field of its panic events.
const KIND: &str = "mode-clock";

/// The boundary-wake port — must match a `[[surface.io_port]] port` declaration.
const TICK_PORT: &str = "tick";

/// The loader's entry, called once after this module's `default` init with the
/// instance this module record was loaded for. The whole boot sequence lives
/// here rather than in `#[wasm_bindgen(start)]`: the panic hook's subject and the
/// element's tag are both this instance's, and neither exists until the bind.
#[wasm_bindgen]
pub fn brenn_bind_instance(instance: String) {
    boot(&instance);
    // This instance's state and its host element, captured by both the connected
    // closure and the activation entry: one module record backs one instance, so
    // these are that instance's and nobody else's.
    let state = Rc::new(RefCell::new(ModeClock::new()));
    let host: Rc<RefCell<Option<HtmlElement>>> = Rc::new(RefCell::new(None));
    register_component(
        KIND,
        {
            let host = Rc::clone(&host);
            move |element| on_connected(element, &host)
        },
        {
            let state = Rc::clone(&state);
            let host = Rc::clone(&host);
            move |activation: &Activation, publisher: &mut Publisher| {
                on_activation(activation, &state, &host, publisher);
                Ok(None)
            }
        },
    );
}

/// The activation's own instant as minutes since local midnight.
///
/// Schedule membership is judged in browser-local time: the schedule expresses the
/// user's local day, and there is no server tick to consult. The instant comes from
/// the activation rather than from a clock read of this glue's — the browser is
/// asked only for the zone.
fn local_minutes(now_ms: u64) -> u16 {
    let date = js_sys::Date::new(&JsValue::from_f64(now_ms as f64));
    (date.get_hours() as u16) * 60 + (date.get_minutes() as u16)
}

/// Record the host element, invoked from the element's `connectedCallback` with
/// it as `this`. No child DOM is built — the component is headless — and nothing
/// is computed: the first recompute is the mount activation's.
fn on_connected(element: HtmlElement, host: &Rc<RefCell<Option<HtmlElement>>>) {
    // Run exactly once per element: `connectedCallback` fires on every insertion,
    // so a re-insertion must not replace the element an activation logs against.
    if !claim_initialized(&element, KIND) {
        return;
    }
    *host.borrow_mut() = Some(element);
}

/// Feed each activation's newest config message to the pure state machine, then
/// recompute once — not once per message — and park the next boundary wake.
///
/// Every note the fold returns is an operator error: a malformed body is a
/// publisher fault, a window of several new configs is a `push_depth` fault. Both
/// are logged; neither stops the theme.
fn on_activation(
    activation: &Activation,
    state: &Rc<RefCell<ModeClock>>,
    host: &Rc<RefCell<Option<HtmlElement>>>,
    publisher: &mut Publisher,
) {
    let host = host.borrow();
    // No element yet, so nothing to log against. The activation is still consumed;
    // its config remains visible as context in a later window while retention
    // covers it.
    let Some(host) = host.as_ref() else {
        return;
    };
    for window in &activation.ports {
        // The tick's payload is irrelevant — the wake is the message.
        if window.port == TICK_PORT {
            continue;
        }
        let notes = state
            .borrow_mut()
            .on_config_window(window)
            .expect("an activation window satisfies the mode-clock contract");
        for note in notes {
            let message = match note {
                ConfigNote::Misconfigured(message) => message,
                ConfigNote::Malformed(report) => report.log_message("mode-clock config"),
            };
            component_log(host, LogLevel::Error, &message);
        }
    }
    let now_ms = activation
        .now
        .expect("the surface kernel stamps every activation with its wall clock");
    // Once per activation, not once per message: the theme is a pure function of
    // the clock and the *effective* config, so only the last one matters.
    let plan = state.borrow_mut().tick(local_minutes(now_ms));
    if let Some(theme) = plan.dispatch {
        let body = serde_json::to_string(&ThemeBody {
            v: CONTROL_PLANE_VERSION,
            theme: theme.as_wire_str().to_string(),
        })
        .expect("a ThemeBody serializes to JSON");
        if let Err(err) = publisher.publish(THEME_PORT, &body) {
            component_log(
                host,
                LogLevel::Error,
                &format!("theme publish refused: {err:?}"),
            );
        }
    }
    // A fixed dark/light mode has no next boundary, so it parks nothing — the
    // chain resumes from the config activation that puts the clock back in auto.
    repark_tick(
        activation,
        publisher,
        host,
        TICK_PORT,
        plan.release_at(now_ms),
    );
}

// Browser-level tests for the activation glue: the DOM-free half is covered
// natively in `logic.rs`, and everything below the entry — the publisher, the
// element, the clock conversion — exists only in a browser. Run via
// `make surface-wasm-test`.
#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use super::*;

    use brenn_surface_test_fixtures::browser::{activation_json, mount, record_ops, recorded};
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

    wasm_bindgen_test_configure!(run_in_browser);

    /// The instance this test binary binds its one module record to.
    const TEST_INSTANCE: &str = "wbt-mode-clock";

    /// The activation that bootstraps the chain: the initial theme goes out
    /// through the buffered publisher, and exactly one boundary wake is parked
    /// behind it.
    ///
    /// Losing either is silent and total — the shell stays on the page's initial
    /// dark stamp, or the clock never wakes again — and nothing else pins them.
    ///
    /// The activation carries a `tick` window the port loop must skip: its body is
    /// not a config document, so a component that folded it would log a third op.
    #[wasm_bindgen_test]
    fn the_mount_activation_dispatches_the_theme_and_parks_one_boundary_wake() {
        // Installed before the mount: a connect-time publish, park or sync request
        // lands here, where silence is the assertion.
        let ops = record_ops();
        let (entry, _host) = mount(KIND, TEST_INSTANCE, brenn_bind_instance);
        assert_eq!(
            ops.length(),
            0,
            "connect-time code sets up only, and reaches no kernel seam"
        );
        let now_ms = 1_770_000_123_456;

        entry
            .call1(
                &JsValue::NULL,
                &JsValue::from_str(&activation_json(&[(TICK_PORT, "{}")], None, now_ms)),
            )
            .expect("the entry returns ok");

        let recorded = recorded(&ops);
        assert_eq!(
            recorded.len(),
            2,
            "the theme and the wake, and nothing else — a tick window folded as a \
             config document would log a third: {recorded:?}"
        );

        let [seam, _, port, body, _] = &recorded[0][..] else {
            panic!("{recorded:?}")
        };
        assert_eq!((seam.as_str(), port.as_str()), ("publish", THEME_PORT));
        let theme: ThemeBody = serde_json::from_str(body).expect("the theme body is a ThemeBody");
        assert_eq!(theme.v, CONTROL_PLANE_VERSION);
        assert!(
            ["dark", "light"].contains(&theme.theme.as_str()),
            "the first recompute always dispatches a theme: {theme:?}"
        );

        let [seam, defer_op, port, _, deliver_after] = &recorded[1][..] else {
            panic!("{recorded:?}")
        };
        assert_eq!(
            (seam.as_str(), defer_op.as_str(), port.as_str()),
            ("defer", "publish", TICK_PORT),
            "the wake is parked on the in/out tick port, after the theme"
        );
        let deliver_after: u64 = deliver_after.parse().expect("a decimal release instant");
        assert!(
            deliver_after > now_ms && deliver_after <= now_ms + 15 * 60_000,
            "the wake is future and inside the horizon a local-minute count is \
             trusted over: {deliver_after} against {now_ms}"
        );
    }
}
