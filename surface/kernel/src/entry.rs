//! wasm-bindgen entry point and kernel-facing handle.
//!
//! Browser target only. [`start`] is the bootstrap's single entry into the
//! kernel: it installs the panic hook, reads the page's three surface metas,
//! builds a page and the front door onto it, spawns the [`SurfaceRunner`] over a
//! [`WebSysConnector`] and the kernel's event loop, and returns a
//! [`KernelHandle`] the bootstrap holds for its post-kernel error path.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use futures_util::StreamExt;
use uuid::Uuid;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

use brenn_attach_client::conn::ConnConfig;

use crate::WebSysConnector;
use crate::contract::{
    PublishError, SyncStatus, defer_status_str, element_name_for_instance, publish_status_str,
};
use crate::front::{self, EventStream, SurfaceHandle};
use crate::page::SurfacePage;
use crate::runner::SurfaceRunner;
use crate::schema::{LogLevel, STALE_BUILD_CLOSE_CODE};
use crate::session::Event;

use crate::dom;
use crate::logic::{
    ConnectIndicatorState, DeferIntent, KernelAction, KernelCore, SyncIntent,
    malformed_registration, route_component_alert, route_component_log, route_defer_intent,
    route_processor_alert, route_processor_log, route_publish_intent, route_sync_intent,
    sync_refused, unbuffered_defer_refused, unbuffered_publish_refused,
};
use crate::sync_door::{SyncAnswer, SyncDoor};

const INITIAL_BACKOFF: Duration = Duration::from_secs(3);
const MAX_BACKOFF: Duration = Duration::from_secs(60);
/// Handshake timeout, covering transport-open through `Welcome`-received.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// Multiple of the peer's advertised `heartbeat_secs` of inbound silence that
/// marks the attachment dead.
const LIVENESS_MULTIPLIER: u32 = 3;

/// Bring the kernel online and hand the bootstrap a handle to it.
///
/// Installs the kernel's panic hook (which dispatches `brenn-surface-reload` so
/// the bootstrap's capped reload heals a kernel death), reads `surface-slug`,
/// `brenn-build-id` and `surface-config-channel` from the page metas, derives
/// the connect URL from `window.location`, builds the page and its front door,
/// renders the initial `Connecting` connect indicator, and `spawn_local`s the
/// [`SurfaceRunner`] and the kernel's event loop. The event loop folds each
/// [`Event`] through [`KernelCore`] and applies the resulting actions to the DOM.
///
/// The three metas are the page's whole boot identity: which surface this is,
/// which served-asset build it was rendered by, and the channel its wiring is
/// retained on. Everything else the kernel runs on arrives as the bindings
/// document on that third address.
#[wasm_bindgen]
pub fn start() -> KernelHandle {
    // The panic hook must be installed before anything can panic: a panic during
    // start() itself or the first synchronous stretch of a spawned task must land
    // on the bootstrap's already-installed reload listener.
    std::panic::set_hook(Box::new(|info| {
        dom::report_panic(&info.to_string());
    }));

    // Stamp the page-uptime origin as early as possible so a status report's
    // uptime measures from kernel start.
    dom::mark_page_start();

    let window = web_sys::window().expect("surface kernel: no window");
    let document = window.document().expect("surface kernel: no document");

    let slug = meta_content(&document, "surface-slug");
    let build_id = meta_content(&document, "brenn-build-id");
    let config_channel = meta_content(&document, "surface-config-channel");

    let location = window.location();
    let protocol = location
        .protocol()
        .expect("surface kernel: location has no protocol");
    let host = location
        .host()
        .expect("surface kernel: location has no host");

    let config = ConnConfig {
        url: crate::logic::connect_url(&protocol, &host, &slug, &build_id),
        // The build id doubles as the `Hello` ident: the peer logs it, and it is
        // the one string that identifies which assets this page was served.
        ident: build_id,
        initial_backoff: INITIAL_BACKOFF,
        max_backoff: MAX_BACKOFF,
        connect_timeout: CONNECT_TIMEOUT,
        liveness_multiplier: LIVENESS_MULTIPLIER,
        // Seeded from per-page entropy so a fleet reconnecting in lockstep after
        // a deploy restart decorrelates its reconnects.
        backoff_jitter_seed: crate::entropy::seed(),
        terminal_close_code: Some(STALE_BUILD_CLOSE_CODE),
    };
    // The page's store epoch, minted here for the same reason as the jitter seed:
    // nothing below this edge reads entropy. `Uuid::new_v4` reads the platform
    // CSPRNG (`crypto.getRandomValues`), which is what a page-lifetime identity
    // should be — not the deliberately non-cryptographic backoff seed source.
    let page = SurfacePage::new(config_channel, Uuid::new_v4());
    let (handle, events, channels) = front::new();
    let handle = Rc::new(handle);
    let runner = SurfaceRunner::new(page, config, WebSysConnector::new(), channels);
    // Taken before the run is spawned, and held for the page's life: a gesture
    // cannot wait for the loop, so its activation runs through this instead.
    let door = Rc::new(runner.sync_door());

    // The DOM-free decision core is shared: `run_event_loop` folds control-plane
    // events through it, and the delegated alert listener reads its
    // `alert_granted()` flag to gate a `brenn-alert` forward. Both touches are
    // short synchronous borrows on the single-threaded page, never overlapping
    // (the event loop's borrow is released before any DOM effect that could
    // re-enter via a component event).
    let core = Rc::new(RefCell::new(KernelCore::new()));

    install_listeners(&handle, &core, &door);

    // Render the pre-chrome connect indicator before any attachment: the one
    // thing the kernel itself draws, removed the moment chrome first mounts (or,
    // on a chrome-less surface, once the page is first configured).
    dom::render_connect_indicator(ConnectIndicatorState::Connecting);

    // Publish the kernel's host seam for headless processor instances. The DOM
    // seam is delegated events on `#surface-root`; a processor has no element, so
    // its imports are direct calls from the bootstrap loader's shims into the
    // free functions below, which read this cell. Set before the driver runs so a
    // loader that instantiates the moment the kernel is up finds it populated.
    PROCESSOR_HOST.with(|cell| {
        *cell.borrow_mut() = Some(ProcessorHost {
            core: Rc::clone(&core),
            handle: Rc::clone(&handle),
        });
    });

    spawn_local(async move {
        // The browser's run hands nothing back: the sync door holds the page cell
        // for the document's life, and the run only returns once the platform half
        // is gone anyway.
        runner.run().await;
    });
    spawn_local(run_event_loop(Rc::clone(&core), events, Rc::clone(&handle)));

    KernelHandle { handle }
}

/// Install the kernel's six delegated DOM listeners on `#surface-root` plus the
/// window-level component-panic listener.
///
/// Split out of [`start`] so the browser suite installs exactly the wiring the
/// real page runs on: a component that dispatches a registration or a publish is
/// heard only if these are in place, so a test that stood them up by hand would
/// be testing its own stand-in.
fn install_listeners(
    handle: &Rc<SurfaceHandle>,
    core: &Rc<RefCell<KernelCore>>,
    door: &Rc<SyncDoor>,
) {
    // Route component publish intents. The delegated `#surface-root` listener
    // hands each `brenn-port-publish` event's (retargeted host) tag and its
    // untrusted `{ port, body }` detail to the DOM-free `route_publish_intent`,
    // which decides route-vs-drop against the mounted-element registry; the
    // single resulting action is applied to the DOM. A misrouted or malformed
    // publish becomes a `Report`, never a bus message — and is refused on the
    // detail, so every publish the kernel hears is answered.
    //
    // Well-formed publishes then fork on one question the kernel alone can answer:
    // is this instance the one whose activation entry is on the stack? Activations
    // are serialized per instance and synchronous on the one JS thread, so exactly
    // one instance can be mid-activation — if it is this one, the publish belongs
    // in that activation's buffer (quota-checked inline, flushed only if the entry
    // returns ok, answered synchronously on the detail). Anything else is refused:
    // a publish outside an activation has no flush boundary to belong to, and the
    // component is told so on the same detail.
    {
        let handle = Rc::clone(handle);
        dom::install_publish_listener(move |instance, target_tag, port, body, urgency, detail| {
            let action = route_publish_intent(instance, target_tag, port, body, urgency);
            let KernelAction::Publish {
                instance,
                port,
                body,
                urgency,
            } = &action
            else {
                // Misrouted or malformed: a breadcrumb, never a message. Answered
                // all the same — the SDK reads a *missing* status as a page whose
                // kernel listener is absent, so a publish the kernel heard and
                // dropped owes the dispatcher a refusal rather than silence.
                dom::apply_actions(std::slice::from_ref(&action), &handle);
                dom::set_publish_status(detail, Err(PublishError::NotPermitted));
                return;
            };
            match handle.try_buffered_publish(instance, port, body, *urgency) {
                Some(status) => {
                    dom::count_publish(instance);
                    dom::set_publish_status(detail, status);
                }
                None => {
                    dom::set_publish_status(detail, Err(PublishError::NotPermitted));
                    dom::apply_actions(
                        std::slice::from_ref(&unbuffered_publish_refused(instance, port)),
                        &handle,
                    );
                }
            }
        });
    }

    // Route component deferred-message ops. A misrouted or malformed one becomes a
    // `Report`, never a schedule. There is deliberately no immediate path: a
    // schedule staged outside the flush boundary would survive the very activation
    // that failed to stage it.
    {
        let handle = Rc::clone(handle);
        dom::install_defer_listener(move |instance, target_tag, detail, js_detail| {
            let intent = match route_defer_intent(instance, target_tag, detail) {
                Ok(intent) => intent,
                Err(drop) => {
                    dom::apply_actions(std::slice::from_ref(&drop), &handle);
                    return;
                }
            };
            let answer = match &intent {
                DeferIntent::Publish {
                    instance,
                    port,
                    body,
                    deliver_after,
                } => handle
                    .try_buffered_publish_deferred(instance, port, body, *deliver_after)
                    .map(|status| {
                        // A refused publish is not a publish this component made.
                        if status.is_ok() {
                            dom::count_publish(instance);
                        }
                        publish_status_str(status)
                    }),
                DeferIntent::Cancel {
                    instance,
                    port,
                    index,
                } => handle
                    .try_buffered_defer_cancel(instance, port, *index)
                    .map(defer_status_str),
                DeferIntent::Edit {
                    instance,
                    port,
                    index,
                    body,
                    deliver_after,
                } => handle
                    .try_buffered_defer_edit(instance, port, *index, body.clone(), *deliver_after)
                    .map(defer_status_str),
            };
            match answer {
                Some(status) => dom::set_defer_status(js_detail, status),
                None => dom::apply_actions(
                    std::slice::from_ref(&unbuffered_defer_refused(&intent)),
                    &handle,
                ),
            }
        });
    }

    // Route component sync-call requests. Every path writes a status onto the
    // detail, including the malformed one: the SDK reads a missing status as a
    // broken page rather than an outcome.
    {
        let handle = Rc::clone(handle);
        let door = Rc::clone(door);
        dom::install_sync_listener(move |instance, target_tag, port, body, detail| {
            let intent = match route_sync_intent(instance, target_tag, port, body) {
                Ok(intent) => intent,
                Err(drop) => {
                    dom::apply_actions(std::slice::from_ref(&drop), &handle);
                    dom::set_sync_status(detail, SyncStatus::Refused, None, None);
                    return;
                }
            };
            let SyncIntent {
                instance,
                port,
                body,
            } = intent;
            let answer = door.request(&instance, &port, body);
            if let SyncAnswer::Refused(refusal) = &answer {
                dom::apply_actions(
                    std::slice::from_ref(&sync_refused(&instance, &port, refusal)),
                    &handle,
                );
            }
            let (reply, error) = match &answer {
                SyncAnswer::Ok(reply) => (reply.as_deref(), None),
                SyncAnswer::Err(err) => (None, Some(err.message.as_str())),
                SyncAnswer::Trap | SyncAnswer::Refused(_) => (None, None),
            };
            dom::set_sync_status(detail, answer.status(), reply, error);
        });
    }

    // Route component activation registrations. The delegated `#surface-root`
    // listener hands each `brenn-activation-register` event's `entry` function and
    // its resolved instance to the core's gate, which admits exactly one
    // registration per mounted instance; only an admitted one reaches the client
    // core, whose own `RegisterActivation` bound panics on a duplicate or unknown
    // instance. That bound is the backstop for a *kernel* bug and must never be
    // reached by a *component* bug — a component's double registration is a
    // contained fault report, not a dead page.
    {
        let handle = Rc::clone(handle);
        let core = Rc::clone(core);
        dom::install_activation_register_listener(move |instance, target_tag, entry| {
            // Checked before the gate, deliberately: the gate *consumes* the
            // instance's one registration, and spending it on a detail carrying no
            // entry would lock a component out of ever registering a real one.
            let Some(entry) = entry else {
                dom::apply_actions(
                    std::slice::from_ref(&malformed_registration(instance, target_tag)),
                    &handle,
                );
                return;
            };
            let (admitted, actions) = core
                .borrow_mut()
                .on_activation_register(instance, target_tag);
            dom::apply_actions(&actions, &handle);
            let Some(instance) = admitted else { return };
            handle.register_activation(&instance, dom::wrap_activation_entry(&instance, entry));
        });
    }

    // Route component log intents. The delegated `#surface-root` listener hands
    // each `brenn-log` event's (retargeted host) tag and its untrusted
    // `{ level, message }` detail to the DOM-free `route_component_log`, which
    // resolves the mounted component, stamps `source = "component:<kind>"`, and
    // emits a `Log` frame; a misrouted or malformed log becomes a `Report`,
    // never a mis-attributed server log line.
    {
        let handle = Rc::clone(handle);
        dom::install_log_listener(move |instance, target_tag, level, message| {
            let action = route_component_log(instance, target_tag, level, message);
            dom::apply_actions(std::slice::from_ref(&action), &handle);
        });
    }

    // Route component alert intents. The delegated `#surface-root` listener hands
    // each `brenn-alert` event's (retargeted host) tag and its untrusted
    // `{ severity, title, body }` detail to the DOM-free `route_component_alert`,
    // which — only on an alert-granted surface (`KernelCore::alert_granted`) —
    // emits an `Alert` frame; an ungranted surface yields a `log(warn)`
    // suppression breadcrumb, and a misrouted or malformed alert a drop-report. A
    // conforming kernel never sends an ungranted `Alert` (the server kills on one).
    {
        let handle = Rc::clone(handle);
        let core = Rc::clone(core);
        dom::install_alert_listener(move |instance, target_tag, severity, title, body| {
            let action = route_component_alert(
                instance,
                target_tag,
                severity,
                title,
                body,
                core.borrow().alert_granted(),
            );
            dom::apply_actions(std::slice::from_ref(&action), &handle);
        });
    }

    // Route component-module panics. A component's panic hook dispatches
    // `brenn-component-panic { component, message }` on `window`; the DOM-free
    // core turns the (untrusted) detail into an error-card + report for the named
    // mounted component, or a drop-and-report for an unattributable one. On an
    // alert-granted surface an attributed panic additionally pages — the one
    // client-side event that does. The borrow is a short synchronous borrow
    // released before any DOM effect, so it never overlaps the event loop's fold.
    {
        let handle = Rc::clone(handle);
        let core = Rc::clone(core);
        dom::install_component_panic_listener(move |kind, message| {
            let actions = core
                .borrow_mut()
                .on_component_panic(kind, message, dom::is_mounted);
            dom::apply_actions(&actions, &handle);
        });
    }
}

/// The kernel state the processor host entry points act on, published by
/// [`start`] for the page's lifetime.
struct ProcessorHost {
    core: Rc<RefCell<KernelCore>>,
    handle: Rc<SurfaceHandle>,
}

thread_local! {
    /// `None` until [`start`] runs. A processor host call before that is a
    /// bootstrap-ordering bug — the loader cannot have a module to call from
    /// until the kernel handed it one — so the accessor panics rather than
    /// silently dropping a component's publish.
    static PROCESSOR_HOST: RefCell<Option<ProcessorHost>> = const { RefCell::new(None) };
}

/// Run `f` against the published processor host.
fn with_processor_host<R>(what: &str, f: impl FnOnce(&ProcessorHost) -> R) -> R {
    PROCESSOR_HOST.with(|cell| {
        let borrow = cell.borrow();
        let host = borrow.as_ref().unwrap_or_else(|| {
            panic!("surface kernel: {what} before start() published the processor host")
        });
        f(host)
    })
}

/// A processor instance's `ports.publish` / `ports.publish-with-urgency` import.
///
/// `instance` comes from the loader's own closure over the manifest entry it
/// instantiated the module for — never from the component, exactly as the DOM
/// path's instance comes from the executor's element registry rather than the
/// event detail.
///
/// A processor only ever publishes from inside its own `receive`, so the buffered
/// path always takes it: the publish joins that activation's buffer, is
/// quota-checked inline, and flushes only if the entry returns ok. The answer is
/// the WIT `publish-error` string the guest lifts, or the empty string for ok. A
/// `None` from `try_buffered_publish` means no activation of this instance is in
/// flight — a component publishing outside `receive`, which its world gives it no
/// way to do — so it is refused, exactly as the DOM seam refuses one.
#[wasm_bindgen]
pub fn brenn_processor_publish(
    instance: &str,
    port: &str,
    body: &str,
    urgency: Option<String>,
) -> String {
    let urgency = match urgency {
        Some(raw) => match crate::Urgency::parse(&raw) {
            Some(urgency) => Some(urgency),
            // The guest's WIT enum lifts to a fixed string set, so an
            // unrecognized value is transpile-glue drift, not a component typo.
            None => return "invalid-payload".to_string(),
        },
        None => None,
    };
    with_processor_host("processor publish", |host| {
        match host
            .handle
            .try_buffered_publish(instance, port, body, urgency)
        {
            Some(Ok(())) => {
                dom::count_publish(instance);
                String::new()
            }
            Some(Err(err)) => crate::logic::publish_error_str(err),
            // TODO(surface-wasm-test-in-ci): this None arm (absent host slot →
            // "not-permitted") depends on the live wasm host slot and can only
            // be pinned by the browser test runner, unlike the variant map,
            // which is natively tested in `logic`.
            None => "not-permitted".to_string(),
        }
    })
}

/// A processor instance's `ports.publish-deferred` import. `deliver_after` is
/// epoch milliseconds UTC, the instant the message becomes observable; a value at
/// or before the flush's own clock reading publishes immediately, exactly like
/// `publish`.
///
/// Buffered on the same terms as [`brenn_processor_publish`], for the same reason:
/// a schedule an activation staged and then failed on must not exist. The answer
/// is the WIT `publish-error` string, or the empty string for ok — deferral adds no
/// error vocabulary to the publish family.
#[wasm_bindgen]
pub fn brenn_processor_publish_deferred(
    instance: &str,
    port: &str,
    body: &str,
    deliver_after: u64,
) -> String {
    with_processor_host("processor deferred publish", |host| {
        match host
            .handle
            .try_buffered_publish_deferred(instance, port, body, deliver_after)
        {
            Some(Ok(())) => {
                dom::count_publish(instance);
                String::new()
            }
            Some(Err(err)) => crate::logic::publish_error_str(err),
            None => "not-permitted".to_string(),
        }
    })
}

/// A processor instance's `ports.defer-cancel` import: unpark one message this
/// instance already scheduled on `port`'s channel, named by its `index` into the
/// deferred window this activation was handed.
///
/// Buffered like a publish, so an err or trap cancels nothing. The answer is the
/// WIT `defer-error` string, or the empty string for ok. Note what ok does *not*
/// promise: the named message may release between this call and the flush, which
/// the WIT rules a benign race the host logs and counts — the component has
/// already returned by then, so it is not a refusal.
#[wasm_bindgen]
pub fn brenn_processor_defer_cancel(instance: &str, port: &str, index: u32) -> String {
    with_processor_host("processor defer cancel", |host| {
        match host.handle.try_buffered_defer_cancel(instance, port, index) {
            Some(Ok(())) => String::new(),
            Some(Err(err)) => crate::logic::defer_error_str(err),
            None => "not-permitted".to_string(),
        }
    })
}

/// A processor instance's `ports.defer-edit` import: rewrite one message this
/// instance already scheduled on `port`'s channel — its body, its release time, or
/// both. An absent argument leaves that half alone.
///
/// Same buffered semantics, index resolution, and race treatment as
/// [`brenn_processor_defer_cancel`].
#[wasm_bindgen]
pub fn brenn_processor_defer_edit(
    instance: &str,
    port: &str,
    index: u32,
    body: Option<String>,
    deliver_after: Option<u64>,
) -> String {
    with_processor_host("processor defer edit", |host| {
        match host
            .handle
            .try_buffered_defer_edit(instance, port, index, body, deliver_after)
        {
            Some(Ok(())) => String::new(),
            Some(Err(err)) => crate::logic::defer_error_str(err),
            None => "not-permitted".to_string(),
        }
    })
}

/// A processor instance's `log.*` import: one component log line, attributed to
/// the instance, on the same plane a `dom` component's `brenn-log` reaches.
#[wasm_bindgen]
pub fn brenn_processor_log(instance: &str, level: &str, message: &str) {
    let action = route_processor_log(instance, level, message);
    with_processor_host("processor log", |host| {
        dom::apply_actions(std::slice::from_ref(&action), &host.handle);
    });
}

/// A processor instance's `alert.*` import. Gated on the surface's alert grant
/// exactly as the DOM path is: boot proved the grant for a kind that imports
/// `alert`, and this is the runtime half of that same gate — a conforming kernel
/// never emits an ungranted `Alert`.
#[wasm_bindgen]
pub fn brenn_processor_alert(instance: &str, severity: &str, title: &str, body: &str) {
    with_processor_host("processor alert", |host| {
        let granted = host.core.borrow().alert_granted();
        let action = route_processor_alert(instance, severity, title, body, granted);
        dom::apply_actions(std::slice::from_ref(&action), &host.handle);
    });
}

/// A processor instance's `config.get` import. Answers from the map the
/// instance's own component entry carries; a miss is `None`, which is the
/// import's own `option<string>`.
#[wasm_bindgen]
pub fn brenn_processor_config_get(instance: &str, key: &str) -> Option<String> {
    with_processor_host("processor config get", |host| {
        host.core.borrow().processor_config_get(instance, key)
    })
}

/// Register a headless processor instance's `receive` export with the kernel.
///
/// The tail is `handle.register_activation` — the DOM path's tail, unchanged — but
/// the admission ahead of it is [`KernelCore::on_processor_register`], because the
/// DOM gate resolves its instance from a mounted element and a processor has
/// none. Returns whether the registration was admitted, so the loader can tell a
/// refusal from success without reading kernel state.
#[wasm_bindgen]
pub fn brenn_processor_register(instance: &str, entry: js_sys::Function) -> bool {
    with_processor_host("processor register", |host| {
        let (admitted, actions) = host.core.borrow_mut().on_processor_register(instance);
        dom::apply_actions(&actions, &host.handle);
        if admitted {
            host.handle
                .register_activation(instance, dom::wrap_activation_entry(instance, entry));
        }
        admitted
    })
}

/// Report that a processor instance could not be brought up — module import,
/// `instantiate`, or registration failure in the bootstrap loader.
///
/// A headless instance has no wrapper, so there is no error card to render: the
/// `failed` status row and its `surface-state` publish are the observable, plus
/// the death report. One instance's failure is one instance's; its siblings have
/// their own instantiation and their own linear memory.
#[wasm_bindgen]
pub fn brenn_processor_load_failed(instance: &str, detail: &str) {
    with_processor_host("processor load failure", |host| {
        let actions = host
            .core
            .borrow_mut()
            .on_processor_load_failed(instance, detail);
        dom::apply_actions(&actions, &host.handle);
    });
}

/// Drain the page's event stream, folding each [`Event`] through the DOM-free
/// core and applying the emitted actions. The `is_element_defined` predicate the
/// core consults on first connect asks the live `customElements` registry whether
/// the component's element is registered; a missing registration error-cards that
/// mount (per the core's mount plan).
async fn run_event_loop(
    core: Rc<RefCell<KernelCore>>,
    mut events: EventStream,
    handle: Rc<SurfaceHandle>,
) {
    let registry = web_sys::window()
        .expect("surface kernel: no window")
        .custom_elements();
    // The telemetry listeners (resize + status tick) are page-lifetime and
    // installed once, off the first bindings document's cadence. Installing after
    // the fold means the core is already running on that document when the
    // startup viewport read fires.
    let mut telemetry_installed = false;
    while let Some(event) = events.next().await {
        // Borrow the shared core only for the synchronous fold; the borrow is
        // released before any effect below, so a component event that re-enters
        // the core (e.g. the alert listener reading `alert_granted`) never
        // overlaps this mutable borrow.
        let actions = core.borrow_mut().on_event(&event, |kind, instance| {
            !registry
                .get(&element_name_for_instance(kind, instance))
                .is_undefined()
        });
        // Walk the actions in the order the core emitted them. Every one is a
        // web-sys effect the DOM executor runs; the core emits no task-spawn
        // action.
        for action in &actions {
            dom::apply_action(action, &handle);
        }
        if !telemetry_installed && let Event::Connected { bindings, .. } = &event {
            telemetry_installed = true;
            install_telemetry(
                bindings.platform.status_interval_secs,
                Rc::clone(&core),
                Rc::clone(&handle),
            );
        }
    }
}

/// Install the surface-description telemetry observers: a debounced `window`
/// resize listener that folds each viewport reading through
/// [`KernelCore::on_viewport_changed`], and a periodic status-tick timer that
/// folds through [`KernelCore::on_status_tick`]. Both apply the core's emitted
/// actions to the DOM (the resulting `SendGeometry`/`SendStatus` reaches the
/// client's best-effort telemetry channel). Called once per page.
fn install_telemetry(
    status_interval_secs: u32,
    core: Rc<RefCell<KernelCore>>,
    handle: Rc<SurfaceHandle>,
) {
    {
        let core = Rc::clone(&core);
        let handle = Rc::clone(&handle);
        dom::install_resize_listener(move |width, height, device_pixel_ratio| {
            let actions = core
                .borrow_mut()
                .on_viewport_changed(width, height, device_pixel_ratio);
            dom::apply_actions(&actions, &handle);
        });
    }
    {
        let core = Rc::clone(&core);
        let handle = Rc::clone(&handle);
        dom::install_status_timer(status_interval_secs, move || {
            let actions = core.borrow_mut().on_status_tick();
            dom::apply_actions(&actions, &handle);
        });
    }
}

/// Read a required `<meta name="…" content="…">` from the page. The served page
/// always carries both surface metas; an absent one means a broken deploy,
/// so this panics (house policy — our own page, never attacker-reachable).
fn meta_content(document: &web_sys::Document, name: &str) -> String {
    document
        .query_selector(&format!("meta[name=\"{name}\"]"))
        .expect("surface kernel: meta selector is valid")
        .and_then(|el| el.get_attribute("content"))
        .unwrap_or_else(|| panic!("surface kernel: missing <meta name=\"{name}\">"))
}

/// The bootstrap's handle to the running kernel. Its one method is the post-kernel
/// error path: the bootstrap forwards a caught global error here, which delegates
/// to the client's best-effort leveled `log` at `Error` — every bootstrap-caught
/// global error (uncaught error, unhandled rejection, kernel panic) is error-level
/// and never pages.
#[wasm_bindgen]
pub struct KernelHandle {
    handle: Rc<SurfaceHandle>,
}

#[wasm_bindgen]
impl KernelHandle {
    /// Forward a bootstrap-caught global error at `Error` level: write the
    /// browser-console copy, then hand it to [`SurfaceHandle::report`], which
    /// publishes it to the surface's error channel when the configured floor
    /// admits `Error` (best-effort; console-only otherwise or when down).
    ///
    /// No report subject: a global error is caught at the window, which attests
    /// nothing about which component's code raised it. `source` is the
    /// bootstrap's untrusted best guess and stays body detail; guessing a
    /// subject from it would attribute the error — and its budget draw — to a
    /// component on no evidence. The report carries the bare surface identity.
    pub fn log_error(&self, message: &str, source: &str) {
        web_sys::console::error_1(&wasm_bindgen::JsValue::from_str(message));
        self.handle.report(LogLevel::Error, source, message, None);
    }
}

// Browser-level integration tests for the entry wiring. Run via
// wasm-bindgen-test under a headless WebDriver browser; the whole module
// is wasm32-only, so the host `cargo test` sweep never compiles them. They drive
// `run_event_loop` directly (not `start()`) over a scripted fake connector.
// Isolation: each test starts from `fresh_root` and uses a unique `wbt-*`
// component kind (custom-element registrations and `MOUNTED` entries are
// page-lifetime and cannot be removed).
#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::Cell;
    use std::collections::VecDeque;
    use std::time::Duration;

    use brenn_attach_client::conn::AttachmentFacts;
    use brenn_attach_proto::{AlertSeverity, ClientFrame};
    use brenn_surface_component_support as sdk;
    use brenn_surface_schema::bindings::BindingsDocument;

    use crate::contract::{
        ACTIVATION_REGISTER, ACTIVATION_SYNC, ENTRY_REPLY_FIELD, PORT_PUBLISH, SURFACE_READY,
        SURFACE_RELOAD, SYNC_ERROR_FIELD, SYNC_REPLY_FIELD, SYNC_STATUS_FIELD,
    };
    use crate::test_support::bindings as fixtures;
    use crate::test_support::frames;
    use crate::test_support::pages;
    use crate::{TransportConnection, TransportConnector, TransportError, TransportEvent};
    use futures_channel::mpsc;
    use js_sys::{Object, Promise, Reflect};
    use wasm_bindgen_futures::JsFuture;
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};
    use web_sys::{CustomEvent, CustomEventInit, Element, HtmlElement};

    use crate::wasm_test_util::{
        capture_console_warn, define_test_element, doc, fresh_root, str_field, watch_window,
    };

    wasm_bindgen_test_configure!(run_in_browser);

    /// A recorder for what the kernel delivers to a component, keyed by instance:
    /// each entry is `(instance, activation_json)`.
    type ActivationSink = Rc<RefCell<Vec<(String, String)>>>;

    /// Define `instance`'s instance-scoped element so it upgrades on insertion.
    /// Its `connectedCallback` records the host once (dedup across reparents) and,
    /// on first connect, dispatches `ACTIVATION_REGISTER` carrying a JS entry that
    /// records every activation JSON the kernel later invokes it with, tagged with
    /// the element's own `data-instance`.
    ///
    /// This is the activation-era stand-in for the old dialect: the kernel's
    /// `run_event_loop` wires the dispatched entry through
    /// `handle.register_activation`, so a `Deliver` reaches it exactly as it would
    /// a real component built on the component-support SDK.
    fn define_recording_element(
        instance: &str,
        kind: &str,
        hosts: Rc<RefCell<Vec<HtmlElement>>>,
        sink: ActivationSink,
    ) {
        define_test_element(
            &element_name_for_instance(kind, instance),
            move |host: HtmlElement| {
                if hosts
                    .borrow()
                    .iter()
                    .any(|h| h.is_same_node(Some(host.as_ref())))
                {
                    return;
                }
                hosts.borrow_mut().push(host.clone());
                let inst = host.get_attribute("data-instance").unwrap_or_default();
                let sink = Rc::clone(&sink);
                let entry = Closure::<dyn FnMut(JsValue) -> JsValue>::new(move |json: JsValue| {
                    sink.borrow_mut()
                        .push((inst.clone(), json.as_string().unwrap_or_default()));
                    JsValue::UNDEFINED
                });
                let detail = Object::new();
                Reflect::set(&detail, &JsValue::from_str("entry"), entry.as_ref())
                    .expect("set entry on the registration detail");
                let init = CustomEventInit::new();
                init.set_detail(&detail);
                init.set_bubbles(true);
                init.set_composed(true);
                let event = CustomEvent::new_with_event_init_dict(ACTIVATION_REGISTER, &init)
                    .expect("construct the registration event");
                host.dispatch_event(&event)
                    .expect("dispatch the registration event");
                entry.forget();
            },
        );
    }

    /// The bodies delivered as *new* to `instance`, across every recorded
    /// activation, decoded from the activation JSON.
    fn new_bodies_for(sink: &ActivationSink, instance: &str) -> Vec<String> {
        let mut bodies = Vec::new();
        for (inst, json) in sink.borrow().iter() {
            if inst != instance {
                continue;
            }
            let activation: crate::contract::Activation =
                serde_json::from_str(json).expect("recorded activation JSON decodes");
            for window in &activation.ports {
                for env in &window.envelopes[window.new_from as usize..] {
                    bodies.push(env.body.clone());
                }
            }
        }
        bodies
    }

    // ── fake connector ────────────────────────────────────────────────────

    type EventTx = mpsc::UnboundedSender<TransportEvent>;
    type EventRx = mpsc::UnboundedReceiver<TransportEvent>;

    /// Scripts the connect sequence and captures the frames the driver sent. One
    /// connection (its scripted inbound `TransportEvent` stream) is queued per
    /// expected connect; a connect past the script errors (a retryable outcome,
    /// never a panic), which the terminal-leg reconnect relies on staying quiet.
    #[derive(Clone)]
    struct FakeControls {
        conns: Rc<RefCell<VecDeque<EventRx>>>,
        sent: Rc<RefCell<Vec<String>>>,
        connect_count: Rc<Cell<usize>>,
    }

    impl FakeControls {
        fn new() -> Self {
            Self {
                conns: Rc::new(RefCell::new(VecDeque::new())),
                sent: Rc::new(RefCell::new(Vec::new())),
                connect_count: Rc::new(Cell::new(0)),
            }
        }

        /// Queue the next connect to succeed; returns the sender that pushes
        /// inbound transport events into that connection.
        fn add_connection(&self) -> EventTx {
            let (tx, rx) = mpsc::unbounded();
            self.conns.borrow_mut().push_back(rx);
            tx
        }

        fn connector(&self) -> FakeConnector {
            FakeConnector { ctrl: self.clone() }
        }

        fn connect_count(&self) -> usize {
            self.connect_count.get()
        }

        fn sent(&self) -> Vec<String> {
            self.sent.borrow().clone()
        }
    }

    struct FakeConnector {
        ctrl: FakeControls,
    }

    impl TransportConnector for FakeConnector {
        type Conn = FakeConnection;

        async fn connect(&mut self, _url: &str) -> Result<FakeConnection, TransportError> {
            self.ctrl
                .connect_count
                .set(self.ctrl.connect_count.get() + 1);
            match self.ctrl.conns.borrow_mut().pop_front() {
                Some(incoming) => Ok(FakeConnection {
                    incoming,
                    sent: Rc::clone(&self.ctrl.sent),
                }),
                None => Err(TransportError::new("fake connector: script exhausted")),
            }
        }
    }

    struct FakeConnection {
        incoming: EventRx,
        sent: Rc<RefCell<Vec<String>>>,
    }

    impl TransportConnection for FakeConnection {
        async fn send_text(&mut self, text: String) -> Result<(), TransportError> {
            self.sent.borrow_mut().push(text);
            Ok(())
        }

        async fn next_event(&mut self) -> TransportEvent {
            match self.incoming.next().await {
                Some(event) => event,
                // The test dropped the sender: model it as a peer close.
                None => TransportEvent::Closed {
                    code: None,
                    reason: String::new(),
                },
            }
        }

        async fn close(&mut self) {}
    }

    // ── async + DOM helpers ───────────────────────────────────────────────

    /// Resolve after `ms` via `setTimeout`, yielding to the microtask queue so
    /// the spawned driver and event-loop tasks make progress between polls.
    async fn sleep_ms(ms: i32) {
        let promise = Promise::new(&mut |resolve, _reject| {
            web_sys::window()
                .expect("window")
                .set_timeout_with_callback_and_timeout_and_arguments_0(&resolve, ms)
                .expect("setTimeout");
        });
        JsFuture::from(promise).await.expect("timeout resolves");
    }

    /// Poll `pred` every few ms until it holds; panic (naming `desc`, so a
    /// timeout points at the failed condition) after the bound rather than hang.
    async fn wait_until(desc: &str, mut pred: impl FnMut() -> bool) {
        for _ in 0..2_000 {
            if pred() {
                return;
            }
            sleep_ms(5).await;
        }
        panic!("wait_until({desc}): condition never held within the bound");
    }

    /// The kernel-owned wrapper element for `instance`, if present, so a test can
    /// observe a component's card without a DOM-executor accessor.
    fn instance_wrapper(instance: &str) -> Option<Element> {
        doc().get_element_by_id(&crate::dom::wrapper_id(instance))
    }

    /// The text of `instance`'s error card, if its wrapper currently holds one.
    fn error_card_text(instance: &str) -> Option<String> {
        instance_wrapper(instance)?
            .query_selector("[data-surface-error]")
            .expect("query_selector")
            .and_then(|card| card.text_content())
    }

    /// Whether a parsed `ClientFrame` matching `pred` is in the sent transcript.
    fn sent_has(ctrl: &FakeControls, pred: impl Fn(&ClientFrame) -> bool) -> bool {
        ctrl.sent()
            .iter()
            .filter_map(|f| serde_json::from_str::<ClientFrame>(f).ok())
            .any(|f| pred(&f))
    }

    /// A connection config with near-instant backoff so the terminal leg's one
    /// scripted reconnect resolves promptly under the poll loop.
    fn config() -> ConnConfig {
        ConnConfig {
            url: "ws://localhost/surface/wbt/ws?build=wbt-build".into(),
            ident: "wbt-build".into(),
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(10),
            connect_timeout: CONNECT_TIMEOUT,
            liveness_multiplier: LIVENESS_MULTIPLIER,
            backoff_jitter_seed: 0,
            terminal_close_code: Some(STALE_BUILD_CLOSE_CODE),
        }
    }

    /// The channel this page's wiring is retained on, and the epoch its own
    /// stores are stamped with — the two boot facts `start()` reads off the page.
    const CONFIG_CHANNEL: &str = "ephemeral:site.surface.wbt.bindings";
    const EPOCH: Uuid = Uuid::from_u128(0x000b_0117);

    /// Build a page, its front door and the kernel's event loop over `ctrl`'s
    /// scripted connector — `start()`'s wiring, minus the DOM listeners each test
    /// installs for itself. Hands back the handle the executor publishes through.
    fn spawn_kernel(ctrl: &FakeControls) -> (Rc<SurfaceHandle>, Rc<RefCell<KernelCore>>) {
        let page = SurfacePage::new(CONFIG_CHANNEL.to_string(), EPOCH);
        let (handle, events, channels) = front::new();
        let handle = Rc::new(handle);
        let runner = SurfaceRunner::new(page, config(), ctrl.connector(), channels);
        let door = Rc::new(runner.sync_door());
        let core = Rc::new(RefCell::new(KernelCore::new()));
        install_listeners(&handle, &core, &door);
        spawn_local(async move {
            runner.run().await;
        });
        spawn_local(run_event_loop(Rc::clone(&core), events, Rc::clone(&handle)));
        (handle, core)
    }

    /// Script the peer's whole opening on `server`: `Hello`, `Welcome`, the config
    /// channel's own `SubscribeResult`, and `document` retained on it. Queued in
    /// one go — the page's `Subscribe` is composed synchronously in the `Welcome`
    /// turn, so the acknowledgement behind it is never early.
    fn open(server: &EventTx, document: &BindingsDocument, facts: AttachmentFacts) {
        for text in [
            frames::server_hello(),
            frames::welcome(facts),
            frames::subscribe_result(CONFIG_CHANNEL, 1),
            frames::deliver(CONFIG_CHANNEL, &document.to_body()),
        ] {
            server
                .unbounded_send(TransportEvent::Text(text))
                .expect("script the opening");
        }
    }

    /// A `dom` component of `instance`'s own kind — these tests give each instance
    /// a kind of its own, because a custom-element registration is page-lifetime
    /// across the whole wasm test binary.
    fn component(instance: &str) -> brenn_surface_schema::ComponentEntry {
        fixtures::component_of_kind(instance, instance)
    }

    /// Where this page's error reports go when its document declares a channel.
    const ERRORS: &str = "brenn:site.surface.wbt.errors";

    /// A page and its front door with no kernel event loop over it — for the test
    /// that drives the DOM executor by hand and only needs to know when the page
    /// became usable. The flag is set at the first `Connected`.
    fn spawn_page(ctrl: &FakeControls) -> (Rc<SurfaceHandle>, Rc<Cell<bool>>) {
        let page = SurfacePage::new(CONFIG_CHANNEL.to_string(), EPOCH);
        let (handle, mut events, channels) = front::new();
        let runner = SurfaceRunner::new(page, config(), ctrl.connector(), channels);
        spawn_local(async move {
            runner.run().await;
        });
        let configured = Rc::new(Cell::new(false));
        {
            let configured = Rc::clone(&configured);
            spawn_local(async move {
                while let Some(event) = events.next().await {
                    if matches!(event, Event::Connected { .. }) {
                        configured.set(true);
                    }
                }
            });
        }
        (Rc::new(handle), configured)
    }

    /// The empty wiring with error reports switched on at `warn` — the platform
    /// section's optional pair, which decides whether the log path publishes at
    /// all and where.
    fn reporting_doc() -> BindingsDocument {
        let mut document = fixtures::doc(vec![], vec![], vec![], vec![]);
        document.platform.error_channel = Some(ERRORS.to_string());
        document.platform.error_report_floor = Some(crate::schema::LogLevel::Warn);
        document
    }

    /// Declare and define `instance` with an element that does nothing: a mounted
    /// target a test can resolve and dispatch at, registering no entry.
    fn inert(instance: &str) -> brenn_surface_schema::ComponentEntry {
        define_test_element(&element_name_for_instance(instance, instance), |_| {});
        component(instance)
    }

    /// Declare and define `instance` as this document's chrome singleton.
    ///
    /// Every bindings document names one — a chromeless surface is not a document
    /// the schema admits — and a chrome whose element never registers reloads the
    /// page instead of mounting anything, so a test that wants to observe a mount
    /// has to give chrome a real element. It does nothing but exist.
    fn chrome(instance: &str) -> brenn_surface_schema::ComponentEntry {
        inert(instance)
    }

    // ── the sync seam ─────────────────────────────────────────────────────

    /// Define `instance`'s element for the sync seam: as
    /// [`define_recording_element`], except its entry publishes `body` on `port`
    /// before returning, so a test can watch a sync activation's buffer reach the
    /// wire.
    ///
    /// It publishes on **sync activations only**, which is what makes the frame
    /// attributable. The guaranteed mount activation runs this same entry, and an
    /// outbox holds one flush in flight at a time — so an entry that published on
    /// both would put the mount's batch on the wire and park the gesture's behind
    /// an acknowledgement no scripted peer here sends.
    fn define_sync_element(
        instance: &str,
        hosts: Rc<RefCell<Vec<HtmlElement>>>,
        sink: ActivationSink,
        port: &'static str,
        body: &'static str,
    ) {
        define_test_element(
            &element_name_for_instance(instance, instance),
            move |host: HtmlElement| {
                if hosts
                    .borrow()
                    .iter()
                    .any(|h| h.is_same_node(Some(host.as_ref())))
                {
                    return;
                }
                hosts.borrow_mut().push(host.clone());
                let inst = host.get_attribute("data-instance").unwrap_or_default();
                let sink = Rc::clone(&sink);
                let publisher = host.clone();
                let entry = Closure::<dyn FnMut(JsValue) -> JsValue>::new(move |json: JsValue| {
                    let json = json.as_string().unwrap_or_default();
                    let activation: crate::contract::Activation =
                        serde_json::from_str(&json).expect("the activation JSON decodes");
                    sink.borrow_mut().push((inst.clone(), json));
                    if activation.sync.is_some() {
                        dispatch_detail(
                            &publisher,
                            PORT_PUBLISH,
                            &[("port", port), ("body", body)],
                        );
                    }
                    JsValue::UNDEFINED
                });
                let detail = Object::new();
                Reflect::set(&detail, &JsValue::from_str("entry"), entry.as_ref())
                    .expect("set entry on the registration detail");
                let init = CustomEventInit::new();
                init.set_detail(&detail);
                init.set_bubbles(true);
                init.set_composed(true);
                let event = CustomEvent::new_with_event_init_dict(ACTIVATION_REGISTER, &init)
                    .expect("construct the registration event");
                host.dispatch_event(&event)
                    .expect("dispatch the registration event");
                entry.forget();
            },
        );
    }

    /// The body of an entry that answers by the sync port it was activated on —
    /// one of every shape the call convention admits, plus a re-entrant request.
    ///
    /// Written in JS because two of the four cannot be written in Rust: a thrown
    /// exception is what a trap *is* at this boundary, and the re-entrant case has
    /// to dispatch a second request from inside the first and read the answer back
    /// off the same detail object. The inner answer rides out on the outer reply —
    /// legal, because the activation carrying it is itself sync.
    const ANSWERING_ENTRY: &str = "\
        const port = JSON.parse(json).sync;\n\
        if (port === 'reply') { return { __REPLY_KEY__: '{\"cancel\":true}' }; }\n\
        if (port === 'fail') { return 'the entry declined'; }\n\
        if (port === 'boom') { throw new Error('the gesture blew up'); }\n\
        if (port === 'nested') {\n\
          const detail = { port: 'reply', body: '{}' };\n\
          document.querySelector('__TAG__').dispatchEvent(\n\
            new CustomEvent('__SYNC_EVENT__', { detail, bubbles: true, composed: true }));\n\
          return { __REPLY_KEY__: String(detail.__STATUS_KEY__) };\n\
        }\n\
        return undefined;";

    /// Define `instance`'s element with [`ANSWERING_ENTRY`] as its entry, so a test
    /// can drive every answer the door can write.
    fn define_answering_element(instance: &str, hosts: Rc<RefCell<Vec<HtmlElement>>>) {
        let tag = element_name_for_instance(instance, instance);
        let source = ANSWERING_ENTRY
            .replace("__TAG__", &tag)
            .replace("__SYNC_EVENT__", ACTIVATION_SYNC)
            .replace("__REPLY_KEY__", ENTRY_REPLY_FIELD)
            .replace("__STATUS_KEY__", SYNC_STATUS_FIELD);
        define_test_element(&tag, move |host: HtmlElement| {
            if hosts
                .borrow()
                .iter()
                .any(|h| h.is_same_node(Some(host.as_ref())))
            {
                return;
            }
            hosts.borrow_mut().push(host.clone());
            let entry = js_sys::Function::new_with_args("json", &source);
            let detail = Object::new();
            Reflect::set(&detail, &JsValue::from_str("entry"), &entry)
                .expect("set entry on the registration detail");
            let init = CustomEventInit::new();
            init.set_detail(&detail);
            init.set_bubbles(true);
            init.set_composed(true);
            let event = CustomEvent::new_with_event_init_dict(ACTIVATION_REGISTER, &init)
                .expect("construct the registration event");
            host.dispatch_event(&event)
                .expect("dispatch the registration event");
        });
    }

    /// Request a sync activation on `port` and hand back the detail the kernel
    /// answered on, retrying while the answer is `refused`.
    ///
    /// The retry is for the registration turn alone: it crosses the control
    /// channel, and a request that beats it is refused. A component would never
    /// retry a refusal — it is a bug — but how many turns a mount took is not what
    /// these tests are about.
    async fn admitted_request(host: &HtmlElement, port: &'static str) -> JsValue {
        let answered: Rc<RefCell<Option<JsValue>>> = Rc::new(RefCell::new(None));
        let seen = Rc::clone(&answered);
        wait_until("the request is admitted", move || {
            let detail = dispatch_detail(
                host.as_ref(),
                ACTIVATION_SYNC,
                &[("port", port), ("body", "{}")],
            );
            if str_field(&detail, SYNC_STATUS_FIELD).as_deref() == Some("refused") {
                return false;
            }
            *seen.borrow_mut() = Some(detail);
            true
        })
        .await;
        answered.borrow_mut().take().expect("the wait held")
    }

    /// Every answer the door writes, driven through one live instance: the reply
    /// on ok, the entry's own account on err, the re-entrancy refusal an entry
    /// earns by dispatching at itself, and the trap that ends the instance.
    ///
    /// Each is a production branch no other test reaches — the harness elsewhere
    /// returns `undefined` unconditionally, so without this test every answer but
    /// `status = ok, reply absent` is unexercised. The reply is the whole reason
    /// the class exists: it is what a gesture wiring reads to decide
    /// `preventDefault`.
    #[wasm_bindgen_test]
    async fn the_door_writes_every_answer_an_entry_can_earn() {
        const INST: &str = "wbt-sync-answers";
        const CHROME: &str = "wbt-sync-answers-chrome";
        fresh_root();

        let hosts: Rc<RefCell<Vec<HtmlElement>>> = Rc::new(RefCell::new(Vec::new()));
        define_answering_element(INST, Rc::clone(&hosts));

        let ctrl = FakeControls::new();
        let server = ctrl.add_connection();
        let _kernel = spawn_kernel(&ctrl);

        let mut document = fixtures::doc(
            vec![component(INST), chrome(CHROME)],
            vec![],
            vec![],
            vec![],
        );
        document.chrome_instance = CHROME.to_string();
        open(&server, &document, pages::facts());
        wait_until("the component mounts", || !hosts.borrow().is_empty()).await;
        let host = hosts.borrow()[0].clone();

        let detail = admitted_request(&host, "reply").await;
        assert_eq!(str_field(&detail, SYNC_STATUS_FIELD).as_deref(), Some("ok"));
        assert_eq!(
            str_field(&detail, SYNC_REPLY_FIELD).as_deref(),
            Some("{\"cancel\":true}"),
            "the entry's reply reaches the requester on the detail it dispatched"
        );

        let detail = dispatch_detail(
            host.as_ref(),
            ACTIVATION_SYNC,
            &[("port", "fail"), ("body", "{}")],
        );
        assert_eq!(
            str_field(&detail, SYNC_STATUS_FIELD).as_deref(),
            Some("err")
        );
        assert_eq!(
            str_field(&detail, SYNC_ERROR_FIELD).as_deref(),
            Some("the entry declined"),
            "an err carries the entry's own account, which is the requester's only \
             window onto what it said"
        );
        assert!(
            str_field(&detail, SYNC_REPLY_FIELD).is_none(),
            "an err answers no reply"
        );

        // The entry dispatches at its own host from inside its activation. An
        // entry is on the stack, so the door refuses — and the outer activation,
        // being sync itself, may carry the inner answer out on its reply.
        let detail = dispatch_detail(
            host.as_ref(),
            ACTIVATION_SYNC,
            &[("port", "nested"), ("body", "{}")],
        );
        assert_eq!(str_field(&detail, SYNC_STATUS_FIELD).as_deref(), Some("ok"));
        assert_eq!(
            str_field(&detail, SYNC_REPLY_FIELD).as_deref(),
            Some("refused"),
            "a request from inside an activation finds the page borrowed"
        );

        let detail = dispatch_detail(
            host.as_ref(),
            ACTIVATION_SYNC,
            &[("port", "boom"), ("body", "{}")],
        );
        assert_eq!(
            str_field(&detail, SYNC_STATUS_FIELD).as_deref(),
            Some("trap"),
            "the requesting closure survives the dispatch and has to be told to stop"
        );

        // Terminal, and answered as such — the kill was folded inside the same
        // dispatch: the next request finds no instance the page still activates.
        let detail = dispatch_detail(
            host.as_ref(),
            ACTIVATION_SYNC,
            &[("port", "reply"), ("body", "{}")],
        );
        assert_eq!(
            str_field(&detail, SYNC_STATUS_FIELD).as_deref(),
            Some("refused")
        );
    }

    /// Dispatch a cancelable, bubbling `click` at `target` and answer whether the
    /// browser's default action survived it — `false` when a listener called
    /// `preventDefault`. Built in JS because the answer is `dispatchEvent`'s own
    /// return value, which is the only witness that the suppression really took.
    fn click_survived(target: &HtmlElement) -> bool {
        js_sys::Function::new_with_args(
            "el",
            "return el.dispatchEvent(new MouseEvent('click', { cancelable: true, bubbles: true }));",
        )
        .call1(&JsValue::NULL, target.as_ref())
        .expect("dispatch a click")
        .as_bool()
        .expect("dispatchEvent answers a boolean")
    }

    /// The two halves of the sync seam against each other rather than each against
    /// a stand-in of the other: a real `wire_gesture` wiring on a real SDK-built
    /// component, the real kernel listener and door, and the browser's own
    /// `dispatchEvent` return as the witness.
    ///
    /// This is the payoff the whole sync class exists for. Everything under it —
    /// the request's detail keys, the reply's key, the dialect the wiring reads —
    /// is spelled independently on the two sides, and a drift in any of them stops
    /// every gesture in the product from cancelling while both suites stay green.
    #[wasm_bindgen_test]
    async fn a_real_gesture_wiring_cancels_the_browsers_default_action() {
        const INST: &str = "wbt-sync-gesture";
        const CHROME: &str = "wbt-sync-gesture-chrome";
        fresh_root();

        // What the entry was handed, one entry per activation: the sync port name
        // and the request body the wiring encoded.
        type Handed = Rc<RefCell<Vec<(Option<String>, Option<String>)>>>;
        let seen: Handed = Rc::new(RefCell::new(Vec::new()));
        sdk::bind_instance(INST);
        sdk::register_component(
            INST,
            |host| {
                sdk::wire_gesture(&host, host.as_ref(), "click", "gesture", |_event| {
                    "{\"click\":1}".to_string()
                });
            },
            {
                let seen = Rc::clone(&seen);
                move |activation: &crate::contract::Activation, _publisher: &mut sdk::Publisher| {
                    let port = activation.sync.clone();
                    let body = port.as_deref().map(|port| {
                        activation
                            .ports
                            .iter()
                            .find(|window| window.port == port)
                            .expect("the named sync port is one of the windows")
                            .envelopes[0]
                            .body
                            .clone()
                    });
                    seen.borrow_mut().push((port.clone(), body));
                    match port {
                        Some(_) => Ok(sdk::gesture_reply(true)),
                        None => Ok(None),
                    }
                }
            },
        );

        let ctrl = FakeControls::new();
        let server = ctrl.add_connection();
        let _kernel = spawn_kernel(&ctrl);

        let mut document = fixtures::doc(
            vec![component(INST), chrome(CHROME)],
            vec![],
            vec![],
            vec![],
        );
        document.chrome_instance = CHROME.to_string();
        open(&server, &document, pages::facts());

        let tag = element_name_for_instance(INST, INST);
        wait_until("the component mounts", || {
            doc()
                .query_selector(&tag)
                .expect("query_selector")
                .is_some()
        })
        .await;
        let host: HtmlElement = doc()
            .query_selector(&tag)
            .expect("query_selector")
            .expect("the wait held")
            .dyn_into()
            .expect("the component's host is an HtmlElement");

        // The mount activation is the registration having landed. Clicking before
        // it would be refused, and the SDK faults on a refusal rather than
        // retrying — a component never gets a second chance at one.
        wait_until("the mount activation runs", || !seen.borrow().is_empty()).await;

        assert!(
            !click_survived(&host),
            "the entry's cancel reply reaches the wiring and suppresses the default action"
        );
        assert_eq!(
            seen.borrow().last().expect("the click activated the entry"),
            &(
                Some("gesture".to_string()),
                Some("{\"click\":1}".to_string())
            ),
            "the wiring's encoded body reaches the entry through the request window"
        );
    }

    /// Dispatch a contract event with a string-valued detail on `target`, the way
    /// a component's SDK does, and hand the detail back — the kernel writes its
    /// synchronous answer onto that same object.
    fn dispatch_detail(
        target: &web_sys::EventTarget,
        name: &str,
        fields: &[(&str, &str)],
    ) -> JsValue {
        let detail = Object::new();
        for (key, value) in fields {
            Reflect::set(&detail, &JsValue::from_str(key), &JsValue::from_str(value))
                .expect("set a detail field");
        }
        let init = CustomEventInit::new();
        init.set_detail(&detail);
        init.set_bubbles(true);
        init.set_composed(true);
        let event =
            CustomEvent::new_with_event_init_dict(name, &init).expect("construct a contract event");
        target.dispatch_event(&event).expect("dispatch");
        detail.into()
    }

    /// The last activation the sink recorded for `instance`, decoded.
    fn last_activation(sink: &ActivationSink, instance: &str) -> crate::contract::Activation {
        let json = sink
            .borrow()
            .iter()
            .rev()
            .find(|(inst, _)| inst == instance)
            .map(|(_, json)| json.clone())
            .expect("the instance was activated at least once");
        serde_json::from_str(&json).expect("recorded activation JSON decodes")
    }

    /// The whole seam in one dispatch: the kernel resolves the instance from the
    /// retargeted target, assembles a sync activation around the minted request,
    /// invokes the entry and writes the answer — all before `dispatchEvent`
    /// returns. That the answer is readable on the next line *is* the testable
    /// proxy for same-task execution, which is what gesture-token liveness reduces
    /// to.
    ///
    /// The effects come after, through the loop: the entry's buffered publish
    /// reaches the fake transport only once the run drains what the door queued.
    #[wasm_bindgen_test]
    async fn a_sync_request_runs_a_whole_activation_before_the_dispatch_returns() {
        const INST: &str = "wbt-sync-ok";
        const CHROME: &str = "wbt-sync-ok-chrome";
        const OUT: &str = "ephemeral:demo.sync-ok";
        fresh_root();

        let hosts: Rc<RefCell<Vec<HtmlElement>>> = Rc::new(RefCell::new(Vec::new()));
        let activations: ActivationSink = Rc::new(RefCell::new(Vec::new()));
        define_sync_element(
            INST,
            Rc::clone(&hosts),
            Rc::clone(&activations),
            "out",
            "from the gesture",
        );

        let ctrl = FakeControls::new();
        let server = ctrl.add_connection();
        let _kernel = spawn_kernel(&ctrl);

        let mut document = fixtures::doc(
            vec![component(INST), chrome(CHROME)],
            vec![],
            vec![fixtures::output(INST, "out", OUT)],
            vec![],
        );
        document.chrome_instance = CHROME.to_string();
        open(&server, &document, pages::facts());
        wait_until("the component mounts", || !hosts.borrow().is_empty()).await;

        // The registration crosses the control channel and is answered by a turn,
        // so a request dispatched before that turn lands is refused. A component
        // would never retry a refusal — it is a bug — but a test has to reach the
        // state the seam is about, and how many turns the mount took is not it.
        let host = hosts.borrow()[0].clone();
        wait_until("the request is admitted", || {
            let detail = dispatch_detail(
                host.as_ref(),
                ACTIVATION_SYNC,
                &[("port", "gesture"), ("body", "{\"click\":1}")],
            );
            // Read on the line after the dispatch: the whole activation is over.
            str_field(&detail, SYNC_STATUS_FIELD).as_deref() == Some("ok")
        })
        .await;

        let activation = last_activation(&activations, INST);
        assert_eq!(activation.sync.as_deref(), Some("gesture"));
        let window = activation
            .ports
            .iter()
            .find(|w| w.port == "gesture")
            .expect("the sync port is windowed like any other");
        assert_eq!(window.new_from, 0);
        assert_eq!(window.dropped, 0);
        let [request] = &window.envelopes[..] else {
            panic!("a sync window is exactly the one live request: {window:?}");
        };
        assert_eq!(request.body, "{\"click\":1}");
        assert_eq!(request.channel, crate::contract::sync_channel("gesture"));
        assert_eq!(request.sender, INST);

        let batch = || {
            ctrl.sent()
                .iter()
                .filter_map(|f| serde_json::from_str::<ClientFrame>(f).ok())
                .find(|f| matches!(f, ClientFrame::PublishBatch { .. }))
        };
        assert!(
            batch().is_none(),
            "the answer returns before the flush reaches the wire"
        );
        wait_until("the activation's flush reaches the transport", || {
            batch().is_some()
        })
        .await;
        let Some(ClientFrame::PublishBatch {
            attribution,
            publishes,
            deferred_ops,
            ..
        }) = batch()
        else {
            unreachable!("the wait above holds only on a batch")
        };
        assert_eq!(attribution.as_deref(), Some(INST));
        assert!(deferred_ops.is_empty());
        let [entry] = &publishes[..] else {
            panic!("the activation buffered exactly one publish: {publishes:?}");
        };
        assert_eq!(entry.channel, OUT);
        assert_eq!(entry.body, "from the gesture");
    }

    /// A request whose target is not a mounted instance element is answered
    /// `refused` all the same. The SDK faults on a missing status — it reads one
    /// as a broken page rather than an outcome — so the drop-and-report path owes
    /// an answer just as much as the admitted one does.
    #[wasm_bindgen_test]
    async fn a_sync_request_from_a_non_component_target_is_refused() {
        const CHROME: &str = "wbt-sync-stray-chrome";
        let root = fresh_root();

        let ctrl = FakeControls::new();
        let server = ctrl.add_connection();
        let _kernel = spawn_kernel(&ctrl);

        let mut document = fixtures::doc(vec![chrome(CHROME)], vec![], vec![], vec![]);
        document.chrome_instance = CHROME.to_string();
        open(&server, &document, pages::facts());

        let stray = doc().create_element("div").expect("create a stray element");
        root.append_child(&stray).expect("append the stray element");
        let detail = dispatch_detail(
            stray.as_ref(),
            ACTIVATION_SYNC,
            &[("port", "gesture"), ("body", "{}")],
        );
        assert_eq!(
            str_field(&detail, SYNC_STATUS_FIELD).as_deref(),
            Some("refused")
        );
    }

    /// The body of an entry that publishes at a nominated element and answers with
    /// the status the kernel wrote back.
    ///
    /// Written in JS for the same reason [`ANSWERING_ENTRY`] is: it has to read the
    /// answer off the very detail object it dispatched, inside the entry, and hand
    /// it out through the reply.
    const PUBLISHING_ENTRY: &str = "\
        const port = JSON.parse(json).sync;\n\
        if (port === 'own' || port === 'other') {\n\
          const tag = port === 'own' ? '__TAG__' : '__OTHER_TAG__';\n\
          const detail = { port: 'out', body: 'from inside an entry' };\n\
          document.querySelector(tag).dispatchEvent(\n\
            new CustomEvent('__PUBLISH_EVENT__', { detail, bubbles: true, composed: true }));\n\
          return { __REPLY_KEY__: String(detail.__PSTATUS_KEY__) };\n\
        }\n\
        return undefined;";

    /// Define `instance`'s element with [`PUBLISHING_ENTRY`], aimed at `other`'s
    /// element for its cross-instance case.
    fn define_publishing_element(
        instance: &str,
        other: &str,
        hosts: Rc<RefCell<Vec<HtmlElement>>>,
    ) {
        let tag = element_name_for_instance(instance, instance);
        let source = PUBLISHING_ENTRY
            .replace("__TAG__", &tag)
            .replace("__OTHER_TAG__", &element_name_for_instance(other, other))
            .replace("__PUBLISH_EVENT__", PORT_PUBLISH)
            .replace("__REPLY_KEY__", ENTRY_REPLY_FIELD)
            .replace("__PSTATUS_KEY__", crate::contract::PUBLISH_STATUS_FIELD);
        define_test_element(&tag, move |host: HtmlElement| {
            if hosts
                .borrow()
                .iter()
                .any(|h| h.is_same_node(Some(host.as_ref())))
            {
                return;
            }
            hosts.borrow_mut().push(host.clone());
            let entry = js_sys::Function::new_with_args("json", &source);
            let detail = Object::new();
            Reflect::set(&detail, &JsValue::from_str("entry"), &entry)
                .expect("set entry on the registration detail");
            let init = CustomEventInit::new();
            init.set_detail(&detail);
            init.set_bubbles(true);
            init.set_composed(true);
            let event = CustomEvent::new_with_event_init_dict(ACTIVATION_REGISTER, &init)
                .expect("construct the registration event");
            host.dispatch_event(&event)
                .expect("dispatch the registration event");
        });
    }

    /// The publish route's three cases, through the live slot: the instance whose
    /// entry is on the stack is buffered, another instance's publish during that
    /// same entry is refused, and a publish with no entry running at all is refused
    /// too.
    ///
    /// Both refusals are `not-permitted` written on the dispatching detail, which
    /// is the whole of the routing decision since no publish falls through to a
    /// path of its own. The third case is also the take-back pin: it is dispatched
    /// after an activation that *was* buffered, so a slot left installed would
    /// answer it `ok`. Both ports are bound outputs of their own instances, so
    /// nothing here can be refused for naming a port its instance lacks.
    ///
    /// The last two cases are the router's own drops — a detail the kernel cannot
    /// attribute, and one it cannot read. They are answered too: the SDK reads a
    /// *missing* status as "no kernel listener heard it", so leaving these silent
    /// would tell an out-of-tree component its page is broken when the kernel knew
    /// exactly what was wrong.
    #[wasm_bindgen_test]
    async fn the_publish_route_buffers_the_running_entrys_publish_and_refuses_every_other() {
        const INST: &str = "wbt-pub-route";
        const NEIGHBOUR: &str = "wbt-pub-route-neighbour";
        const CHROME: &str = "wbt-pub-route-chrome";
        const OUT: &str = "ephemeral:demo.pub-route";
        const NEIGHBOUR_OUT: &str = "ephemeral:demo.pub-route-neighbour";
        let root = fresh_root();

        let hosts: Rc<RefCell<Vec<HtmlElement>>> = Rc::new(RefCell::new(Vec::new()));
        define_publishing_element(INST, NEIGHBOUR, Rc::clone(&hosts));

        let ctrl = FakeControls::new();
        let server = ctrl.add_connection();
        let _kernel = spawn_kernel(&ctrl);

        let mut document = fixtures::doc(
            vec![component(INST), inert(NEIGHBOUR), chrome(CHROME)],
            vec![],
            vec![
                fixtures::output(INST, "out", OUT),
                fixtures::output(NEIGHBOUR, "out", NEIGHBOUR_OUT),
            ],
            vec![],
        );
        document.chrome_instance = CHROME.to_string();
        open(&server, &document, pages::facts());
        wait_until("the component mounts", || !hosts.borrow().is_empty()).await;
        let host = hosts.borrow()[0].clone();

        let detail = admitted_request(&host, "own").await;
        assert_eq!(str_field(&detail, SYNC_STATUS_FIELD).as_deref(), Some("ok"));
        assert_eq!(
            str_field(&detail, ENTRY_REPLY_FIELD).as_deref(),
            Some("ok"),
            "the instance whose entry is on the stack reaches that activation's buffer"
        );

        let detail = dispatch_detail(
            host.as_ref(),
            ACTIVATION_SYNC,
            &[("port", "other"), ("body", "{}")],
        );
        assert_eq!(str_field(&detail, SYNC_STATUS_FIELD).as_deref(), Some("ok"));
        assert_eq!(
            str_field(&detail, ENTRY_REPLY_FIELD).as_deref(),
            Some("not-permitted"),
            "a publish attributed to another instance belongs in no buffer, so it is refused"
        );

        let detail = dispatch_detail(
            host.as_ref(),
            PORT_PUBLISH,
            &[("port", "out"), ("body", "out of the blue")],
        );
        assert_eq!(
            str_field(&detail, crate::contract::PUBLISH_STATUS_FIELD).as_deref(),
            Some("not-permitted"),
            "outside every activation there is no buffer to join and no other path"
        );

        let stray = doc().create_element("div").expect("create a stray element");
        root.append_child(&stray).expect("append the stray element");
        let detail = dispatch_detail(
            stray.as_ref(),
            PORT_PUBLISH,
            &[("port", "out"), ("body", "from nobody")],
        );
        assert_eq!(
            str_field(&detail, crate::contract::PUBLISH_STATUS_FIELD).as_deref(),
            Some("not-permitted"),
            "a publish the kernel cannot attribute is heard, dropped, and answered"
        );

        let detail = dispatch_detail(host.as_ref(), PORT_PUBLISH, &[("port", "out")]);
        assert_eq!(
            str_field(&detail, crate::contract::PUBLISH_STATUS_FIELD).as_deref(),
            Some("not-permitted"),
            "a detail with no body is malformed, and malformed is answered too"
        );

        // Nothing of the four refusals is on the wire, and the one publish that was
        // buffered flushed exactly once.
        wait_until("the buffered publish reaches the transport", || {
            sent_has(&ctrl, |f| matches!(f, ClientFrame::PublishBatch { .. }))
        })
        .await;
        let batches: Vec<_> = ctrl
            .sent()
            .iter()
            .filter_map(|f| serde_json::from_str::<ClientFrame>(f).ok())
            .filter_map(|f| match f {
                ClientFrame::PublishBatch { publishes, .. } => Some(publishes),
                _ => None,
            })
            .collect();
        let [publishes] = &batches[..] else {
            panic!("exactly one activation buffered anything: {batches:?}");
        };
        let [entry] = &publishes[..] else {
            panic!("that activation buffered one publish: {publishes:?}");
        };
        assert_eq!(entry.channel, OUT);
    }

    /// A gesture whose flush lands on a confined channel wakes the instance that
    /// reads it.
    ///
    /// The door turns the page inside the browser's dispatch, while the loop is
    /// parked on a readiness answer it took before it slept. A flush onto a
    /// page-local ring produces no effect at all — no frame, no verdict, no moved
    /// release deadline — so the door's hand-back is the only thing that can tell
    /// the loop to look again. Nothing else is scripted here: no server frame
    /// arrives and no deadline is due, which is exactly the quiet page the missed
    /// wake would strand.
    #[wasm_bindgen_test]
    async fn a_sync_flush_onto_a_confined_channel_wakes_its_reader() {
        const INST: &str = "wbt-sync-fanout";
        const READER: &str = "wbt-sync-fanout-reader";
        const CHROME: &str = "wbt-sync-fanout-chrome";
        const LOCAL: &str = "local:demo/fanout";
        const BODY: &str = "the gesture's own word";
        fresh_root();

        let hosts: Rc<RefCell<Vec<HtmlElement>>> = Rc::new(RefCell::new(Vec::new()));
        let activations: ActivationSink = Rc::new(RefCell::new(Vec::new()));
        define_sync_element(
            INST,
            Rc::clone(&hosts),
            Rc::clone(&activations),
            "out",
            BODY,
        );
        let readers: Rc<RefCell<Vec<HtmlElement>>> = Rc::new(RefCell::new(Vec::new()));
        define_recording_element(READER, READER, Rc::clone(&readers), Rc::clone(&activations));

        let ctrl = FakeControls::new();
        let server = ctrl.add_connection();
        let _kernel = spawn_kernel(&ctrl);

        let mut document = fixtures::doc(
            vec![component(INST), component(READER), chrome(CHROME)],
            vec![fixtures::subscription(READER, "in", LOCAL, 1, 0)],
            vec![fixtures::output(INST, "out", LOCAL)],
            vec![fixtures::local(LOCAL, 4)],
        );
        document.chrome_instance = CHROME.to_string();
        open(&server, &document, pages::facts());
        wait_until("both components mount", || {
            !hosts.borrow().is_empty() && !readers.borrow().is_empty()
        })
        .await;

        // Retried for [`a_sync_request_runs_a_whole_activation_before_the_dispatch_returns`]'s
        // reason: the registration crosses the control channel, and how many turns
        // the mount took is not what this is about.
        let host = hosts.borrow()[0].clone();
        wait_until("the request is admitted", || {
            let detail = dispatch_detail(
                host.as_ref(),
                ACTIVATION_SYNC,
                &[("port", "gesture"), ("body", "{}")],
            );
            str_field(&detail, SYNC_STATUS_FIELD).as_deref() == Some("ok")
        })
        .await;

        wait_until("the reader is activated with the gesture's publish", || {
            new_bodies_for(&activations, READER)
                .iter()
                .any(|body| body == BODY)
        })
        .await;
    }

    // ── tests ─────────────────────────────────────────────────────────────

    #[wasm_bindgen_test]
    async fn run_event_loop_wires_mount_deliver_and_reconnect() {
        const REG: &str = "wbt-entry-reg";
        const UNREG: &str = "wbt-entry-unreg";
        const CHROME: &str = "wbt-entry-chrome";
        const CHANNEL: &str = "ephemeral:demo";
        fresh_root();

        // Register the mounted component; its connectedCallback records the host
        // and registers a recording activation entry.
        let connected: Rc<RefCell<Vec<HtmlElement>>> = Rc::new(RefCell::new(Vec::new()));
        let activations: ActivationSink = Rc::new(RefCell::new(Vec::new()));
        define_recording_element(REG, REG, Rc::clone(&connected), Rc::clone(&activations));

        let ctrl = FakeControls::new();
        let server1 = ctrl.add_connection();
        let _kernel = spawn_kernel(&ctrl);

        let (ready, _ready_c) = watch_window(SURFACE_READY);
        let (reload, _reload_c) = watch_window(SURFACE_RELOAD);

        // The first document: REG bound on one channel, plus a second component
        // whose module never registered its element, plus chrome.
        let mut first = fixtures::doc(
            vec![component(REG), component(UNREG), chrome(CHROME)],
            vec![fixtures::subscription(REG, "messages", CHANNEL, 8, 0)],
            vec![],
            vec![],
        );
        first.chrome_instance = CHROME.to_string();
        open(&server1, &first, pages::facts());

        // The action-walk mounts REG (connectedCallback fires), error-cards the
        // component with no element, and emits SURFACE_READY last; the
        // registration REG's element dispatched subscribes its channel.
        wait_until(
            "REG mounted, UNREG error-carded, Subscribe sent, SURFACE_READY",
            || {
                !connected.borrow().is_empty()
                    && error_card_text(UNREG).as_deref() == Some("component module missing")
                    && sent_has(&ctrl, |f| {
                        matches!(f, ClientFrame::Subscribe { channel, .. } if channel == CHANNEL)
                    })
                    && !ready.borrow().is_empty()
            },
        )
        .await;

        // Activate the subscription so the Deliver below is accepted.
        server1
            .unbounded_send(TransportEvent::Text(frames::subscribe_result(CHANNEL, 0)))
            .expect("send subscribe result");

        // A Deliver reaches the instance's registered activation entry: the
        // recorder captures the activation, whose window carries the message new.
        server1
            .unbounded_send(TransportEvent::Text(frames::deliver(CHANNEL, "hello")))
            .expect("send deliver");
        wait_until("activation delivered to the mounted instance", || {
            !new_bodies_for(&activations, REG).is_empty()
        })
        .await;
        assert_eq!(
            new_bodies_for(&activations, REG),
            vec!["hello".to_string()],
            "the instance's activation window carries the delivered message"
        );

        // Terminal leg: close, reconnect, and a second document that drops REG's
        // subscription. The wiring the page mounted against no longer describes
        // this surface, so the page reports the change and the platform half asks
        // the bootstrap for its capped reload.
        let server2 = ctrl.add_connection();
        server1
            .unbounded_send(TransportEvent::Closed {
                code: Some(1000),
                reason: "bye".into(),
            })
            .expect("send close");
        wait_until("the run reconnected after the close", || {
            ctrl.connect_count() >= 2
        })
        .await;
        let mut second = first.clone();
        second.subscriptions.clear();
        open(&server2, &second, pages::facts());
        wait_until(
            "SURFACE_RELOAD 'bindings changed' on the dropped binding",
            || {
                reload
                    .borrow()
                    .iter()
                    .any(|d| str_field(d, "reason").as_deref() == Some("bindings changed"))
            },
        )
        .await;
    }

    #[wasm_bindgen_test]
    async fn run_event_loop_routes_two_instances_of_one_kind_independently() {
        // End-to-end multi-instance proof: two instances of one component kind,
        // each bound to its own channel. Each instance mounts its own element and
        // a `Deliver` on one channel's port reaches only that instance's element —
        // the routing key is the instance, not the shared kind.
        // Unique per-test kind: the custom-element registry is page-global across
        // the whole wasm test binary, so a kind shared with another test's
        // registration would double-define and panic.
        const KIND: &str = "wbt-two-e2e";
        fresh_root();

        // One entry per element, in mount order (p1 then p2). `connectedCallback`
        // fires on every *insertion*, not once per element — mount stages the
        // element, and chrome's first arrange reparents its wrapper into a panel
        // — so this records each host once, which is what a conformant component
        // does with its own re-entry guard (`claim_initialized`).
        let hosts: Rc<RefCell<Vec<HtmlElement>>> = Rc::new(RefCell::new(Vec::new()));
        let activations: ActivationSink = Rc::new(RefCell::new(Vec::new()));
        // Each instance has its own instance-scoped element; define both so each
        // upgrades and registers its own recording entry.
        define_recording_element("p1", KIND, Rc::clone(&hosts), Rc::clone(&activations));
        define_recording_element("p2", KIND, Rc::clone(&hosts), Rc::clone(&activations));

        const CHROME: &str = "wbt-two-e2e-chrome";
        let mut document = fixtures::doc(
            vec![
                fixtures::component_of_kind("p1", KIND),
                fixtures::component_of_kind("p2", KIND),
                chrome(CHROME),
            ],
            vec![
                fixtures::subscription("p1", "messages", "ephemeral:a", 8, 0),
                fixtures::subscription("p2", "messages", "ephemeral:b", 8, 0),
            ],
            vec![],
            vec![],
        );
        document.chrome_instance = CHROME.to_string();

        let ctrl = FakeControls::new();
        let server = ctrl.add_connection();
        let _kernel = spawn_kernel(&ctrl);

        open(&server, &document, pages::facts());

        // Both instances mount (two hosts) and both subscriptions are sent.
        wait_until("both instances mounted and both subscriptions sent", || {
            hosts.borrow().len() == 2
                && sent_has(&ctrl, |f| {
                    matches!(f, ClientFrame::Subscribe { channel, .. } if channel == "ephemeral:a")
                })
                && sent_has(&ctrl, |f| {
                    matches!(f, ClientFrame::Subscribe { channel, .. } if channel == "ephemeral:b")
                })
        })
        .await;

        // Distinct instance ids on distinct sections and elements.
        let p1_host = hosts.borrow()[0].clone();
        let p2_host = hosts.borrow()[1].clone();
        assert_eq!(
            p1_host.get_attribute("data-instance").as_deref(),
            Some("p1")
        );
        assert_eq!(
            p2_host.get_attribute("data-instance").as_deref(),
            Some("p2")
        );
        assert!(
            !p1_host.is_same_node(Some(p2_host.as_ref())),
            "independent elements"
        );

        server
            .unbounded_send(TransportEvent::Text(frames::subscribe_result(
                "ephemeral:a",
                0,
            )))
            .expect("activate a");
        server
            .unbounded_send(TransportEvent::Text(frames::subscribe_result(
                "ephemeral:b",
                0,
            )))
            .expect("activate b");

        // A deliver on p1's channel activates only p1's instance.
        server
            .unbounded_send(TransportEvent::Text(frames::deliver_at(
                "ephemeral:a",
                "for-p1",
                1,
                0xa1,
            )))
            .expect("deliver a");
        wait_until("p1 received its message", || {
            !new_bodies_for(&activations, "p1").is_empty()
        })
        .await;
        assert_eq!(
            new_bodies_for(&activations, "p2"),
            Vec::<String>::new(),
            "p2 did not receive p1's message"
        );

        // A deliver on p2's channel activates only p2's instance.
        server
            .unbounded_send(TransportEvent::Text(frames::deliver_at(
                "ephemeral:b",
                "for-p2",
                1,
                0xb1,
            )))
            .expect("deliver b");
        wait_until("p2 received its message", || {
            !new_bodies_for(&activations, "p2").is_empty()
        })
        .await;
        assert_eq!(
            new_bodies_for(&activations, "p1"),
            vec!["for-p1".to_string()],
            "p1 unchanged by p2's message"
        );
    }

    #[wasm_bindgen_test]
    async fn apply_actions_dispatches_dom_and_routes_client_frames() {
        const MK: &str = "wbt-apply-mount";
        const EK: &str = "wbt-apply-err";
        const CK: &str = "wbt-apply-clog";
        const CHROME: &str = "wbt-apply-chrome";
        fresh_root();

        // A granted attachment with no output bindings but an error channel
        // declared at floor `warn`, so warn/error reports reach that channel and
        // an Alert frame reaches the wire.
        let ctrl = FakeControls::new();
        let server = ctrl.add_connection();
        let (handle, active) = spawn_page(&ctrl);
        let mut document = reporting_doc();
        document.chrome_instance = CHROME.to_string();
        document.components.push(chrome(CHROME));
        open(
            &server,
            &document,
            AttachmentFacts {
                alert_granted: true,
                ..pages::facts()
            },
        );
        wait_until("the page is configured", || active.get()).await;

        let (ready, _ready_c) = watch_window(SURFACE_READY);

        // DOM-effect actions the executor must apply, observed in the DOM.
        dom::apply_actions(
            &[
                KernelAction::MountComponent {
                    instance: MK.into(),
                    kind: MK.into(),
                },
                KernelAction::ErrorCard {
                    instance: EK.into(),
                    kind: EK.into(),
                    reason: "boom".into(),
                },
                KernelAction::EmitReady,
            ],
            &handle,
        );
        assert!(
            doc()
                .query_selector(&element_name_for_instance(MK, MK))
                .expect("query mounted element")
                .is_some(),
            "MountComponent created the instance's element"
        );
        assert_eq!(
            error_card_text(EK).as_deref(),
            Some("boom"),
            "ErrorCard applied"
        );
        assert_eq!(ready.borrow().len(), 1, "EmitReady fired once");

        // The kernel breadcrumb and the warn ComponentLog each console.warn and
        // publish a reserved-port report; the ComponentAlert routes to an Alert.
        let warnings = capture_console_warn(|| {
            dom::apply_actions(
                &[
                    KernelAction::Report {
                        level: LogLevel::Warn,
                        message: "kernel breadcrumb".into(),
                        subject: None,
                    },
                    KernelAction::ComponentLog {
                        instance: CK.into(),
                        level: LogLevel::Warn,
                        message: "clog".into(),
                    },
                    KernelAction::ComponentAlert {
                        severity: AlertSeverity::Warning,
                        title: "atitle".into(),
                        body: "abody".into(),
                    },
                ],
                &handle,
            );
        });
        assert_eq!(
            warnings.len(),
            2,
            "kernel breadcrumb + warn component log each warn once"
        );

        // Both reports become publishes on the surface's error channel — the
        // kernel breadcrumb (source "kernel", unattributed) and the
        // ComponentLog (source "component:<instance>", attributed to it) — and the
        // ComponentAlert reaches the wire as its own frame.
        wait_until(
            "kernel report + component report + component Alert on the wire",
            || {
                error_report_has(&ctrl, "kernel", None)
                    && error_report_has(&ctrl, "component:wbt-apply-clog", Some("clog"))
                    && sent_has(
                        &ctrl,
                        |f| matches!(f, ClientFrame::Alert { title, .. } if title == "atitle"),
                    )
            },
        )
        .await;
    }

    /// A buffered publish counts one against the publishing instance's
    /// `publishes` column, against no one else's, and a refused one counts
    /// against nobody.
    ///
    /// `publishes` is the half of the per-instance breakdown that reads against a
    /// component's send budget — the column an operator consults to answer "which
    /// component drained its budget?". Its sibling tests in `dom` cover the drop
    /// column and assert only that `publishes` holds *still*, so without this the
    /// producer line could be deleted or misrouted and every per-instance
    /// `publishes` value would be permanently zero with the suite green.
    ///
    /// Driven through the live seam — a real `PORT_PUBLISH` from inside a real
    /// entry — because that listener holds the only remaining call of
    /// [`dom::count_publish`] for an immediate publish. A test that constructed a
    /// [`KernelAction::Publish`] by hand would be pinning a producer no page has.
    #[wasm_bindgen_test]
    async fn publishes_count_against_the_publishing_instance_only() {
        const A: &str = "wbt-pub-ctr-a";
        const B: &str = "wbt-pub-ctr-b";
        const CHROME: &str = "wbt-pub-ctr-chrome";
        fresh_root();

        let hosts: Rc<RefCell<Vec<HtmlElement>>> = Rc::new(RefCell::new(Vec::new()));
        define_publishing_element(A, B, Rc::clone(&hosts));

        let ctrl = FakeControls::new();
        let server = ctrl.add_connection();
        let _kernel = spawn_kernel(&ctrl);
        // Both instances get a real bound output port, so the publish under test
        // takes the accepted path rather than the UnboundPort refusal.
        let mut document = fixtures::doc(
            vec![component(A), inert(B), chrome(CHROME)],
            vec![],
            vec![
                fixtures::output(A, "out", "ephemeral:pubctr"),
                fixtures::output(B, "out", "ephemeral:pubctr-b"),
            ],
            vec![],
        );
        document.chrome_instance = CHROME.to_string();
        open(&server, &document, pages::facts());
        wait_until("the component mounts", || !hosts.borrow().is_empty()).await;
        let host = hosts.borrow()[0].clone();

        let (before_a, before_b) = (dom::instance_counters(A), dom::instance_counters(B));

        // A's entry publishes at its own element: buffered, so it counts.
        let detail = admitted_request(&host, "own").await;
        assert_eq!(
            str_field(&detail, ENTRY_REPLY_FIELD).as_deref(),
            Some("ok"),
            "the publish under test reached the buffer"
        );
        assert_eq!(
            dom::instance_counters(A).publishes - before_a.publishes,
            1,
            "the publishing instance counts exactly one"
        );
        assert_eq!(
            dom::instance_counters(B).publishes,
            before_b.publishes,
            "the sibling's column is untouched"
        );

        // The same entry publishing at B's element: refused, and a refusal is not
        // a publish either instance made.
        let detail = dispatch_detail(
            host.as_ref(),
            ACTIVATION_SYNC,
            &[("port", "other"), ("body", "{}")],
        );
        assert_eq!(
            str_field(&detail, ENTRY_REPLY_FIELD).as_deref(),
            Some("not-permitted"),
        );
        assert_eq!(
            dom::instance_counters(A).publishes - before_a.publishes,
            1,
            "a refused publish adds nothing to the dispatcher's column"
        );
        assert_eq!(
            dom::instance_counters(B).publishes,
            before_b.publishes,
            "nor to the instance it was attributed to"
        );
    }

    /// Whether some sent frame is a `Publish` on the surface's error channel whose
    /// body carries `source` (and `message`, if given).
    fn error_report_has(ctrl: &FakeControls, source: &str, message: Option<&str>) -> bool {
        sent_has(ctrl, |f| {
            let ClientFrame::Publish { channel, body, .. } = f else {
                return false;
            };
            if channel != ERRORS {
                return false;
            }
            let Ok(v) = serde_json::from_str::<serde_json::Value>(body) else {
                return false;
            };
            v["source"] == source && message.is_none_or(|m| v["message"] == m)
        })
    }

    #[wasm_bindgen_test]
    fn meta_content_reads_present_meta() {
        let d = doc();
        let meta = d.create_element("meta").expect("create meta");
        meta.set_attribute("name", "wbt-meta-present")
            .expect("set name");
        meta.set_attribute("content", "the-value")
            .expect("set content");
        d.document_element()
            .expect("document element")
            .append_child(&meta)
            .expect("append meta");
        assert_eq!(meta_content(&d, "wbt-meta-present"), "the-value");
    }

    #[wasm_bindgen_test]
    #[should_panic(expected = "missing <meta")]
    fn meta_content_panics_on_missing_meta() {
        meta_content(&doc(), "wbt-meta-never-present");
    }
}
