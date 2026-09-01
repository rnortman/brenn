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
use web_sys::Element;

use brenn_attach_client::conn::ConnConfig;

use crate::WebSysConnector;
use crate::front::{self, EventStream, SurfaceHandle};
use crate::page::SurfacePage;
use crate::runner::SurfaceRunner;
use crate::schema::{LogLevel, STALE_BUILD_CLOSE_CODE};
use crate::session::Event;

use crate::dom;
use crate::dom_host::DomHost;
use crate::logic::{ConnectIndicatorState, KernelCore, ProcessorRegistration};

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
    // per-instance grants to gate a `brenn-alert` forward. Both touches are
    // short synchronous borrows on the single-threaded page, never overlapping
    // (the event loop's borrow is released before any DOM effect that could
    // re-enter via a component event).
    let core = Rc::new(RefCell::new(KernelCore::new()));

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

    // The DOM host's own cell, published on the same edge and for the same
    // reason: a page-hosted component's first DOM call can come the instant the
    // loader instantiates it.
    publish_dom_host(Rc::new(DomHost::new(
        Rc::clone(&core),
        Rc::clone(&handle),
        Rc::clone(&door),
    )));

    spawn_local(async move {
        // The browser's run hands nothing back: the sync door holds the page cell
        // for the document's life, and the run only returns once the platform half
        // is gone anyway.
        runner.run().await;
    });
    spawn_local(run_event_loop(Rc::clone(&core), events, Rc::clone(&handle)));

    KernelHandle { handle }
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

thread_local! {
    /// The DOM capability host, `None` until [`start`] runs. Its own cell, not a
    /// field of [`ProcessorHost`], because a DOM import is called from inside a
    /// component's `receive` while the runner holds the page borrow: the seam
    /// that answers it must be reachable without touching anything the
    /// activation already holds.
    static DOM_HOST: RefCell<Option<Rc<DomHost>>> = const { RefCell::new(None) };
}

/// Run `f` against the published DOM host.
fn with_dom_host<R>(what: &str, f: impl FnOnce(&Rc<DomHost>) -> R) -> R {
    DOM_HOST.with(|cell| {
        let borrow = cell.borrow();
        let host = borrow.as_ref().unwrap_or_else(|| {
            panic!("surface kernel: {what} before start() published the dom host")
        });
        f(host)
    })
}

/// Publish the DOM host that the `brenn_dom_*` entries and the effect executor's
/// teardown reach.
///
/// Split out of [`start`] because the DOM host's own browser tests build a host
/// over a rig and need it on this same seam: the executor destroys a subtree
/// through free functions, so the only way it can reach a table is the cell.
pub fn publish_dom_host(host: Rc<DomHost>) {
    DOM_HOST.with(|cell| {
        *cell.borrow_mut() = Some(host);
    });
}

/// Drop `instance`'s DOM handle table, if a host has been published.
///
/// The effect executor calls this where it destroys an instance's subtree
/// itself, which can happen before `start()` in the executor's own browser
/// tests. An unpublished host holds no tables, so there is nothing to forget and
/// no absence to report — unlike the import entries, which are called only from
/// a component the kernel handed the loader.
pub fn forget_dom_instance(instance: &str) {
    DOM_HOST.with(|cell| {
        if let Some(host) = cell.borrow().as_ref() {
            host.forget(instance);
        }
    });
}

/// Reclaim every instance's handles naming an element inside `root`, `root`
/// itself included when `with_root`.
///
/// The effect executor calls this where it destroys a subtree itself, which the
/// guest `remove`/`set-text` paths never see. The sweep crosses tables for the
/// same reason theirs does: a handle another instance minted to an element under
/// that subtree would otherwise stay live, leaving the kernel the last strong
/// reference to a destroyed element and the holder silently mutating an orphan.
/// An unpublished host holds no tables, so the guard is a no-op there.
pub fn reclaim_dom_subtree(root: &Element, with_root: bool) {
    DOM_HOST.with(|cell| {
        if let Some(host) = cell.borrow().as_ref() {
            host.reclaim(root, with_root);
        }
    });
}

/// Lift a DOM host refusal into the thrown value the loader's shim turns into a
/// trap.
///
/// Every refusal in this family is a component bug with no runtime cause — an
/// unknown handle, a tag off the allow-list, a capability the instance was never
/// granted — so none of them is an error variant the guest could read. The
/// instance dies with its error card, exactly as it would on any other trap.
fn trap<T>(outcome: Result<T, String>) -> Result<T, JsValue> {
    outcome.map_err(|message| JsValue::from_str(&message))
}

/// Ask [`KernelCore::component_ports_gate`] whether `instance` may reach the
/// publish/defer family, reporting the refusal if it may not.
///
/// All six entries of the family — the two DOM listeners and the four headless
/// exports — must come through here so the verdict and its breadcrumb cannot
/// drift. The decision itself lives on the core; this is the seam half: apply
/// the report, answer `false`, and let the caller write the refusal back in the
/// shape its own ABI answers in.
///
/// The gate sits at the entry seams rather than at the buffered-publish seam
/// underneath them because [`SurfaceHandle`] is the cross-thread front half and
/// holds no grants: the authority is [`KernelCore`], which only these seams
/// hold. A seventh entry into this family belongs here too.
fn ports_granted(
    core: &Rc<RefCell<KernelCore>>,
    handle: &SurfaceHandle,
    instance: &str,
    what: &str,
) -> bool {
    // The borrow is a temporary of this `let`: it must die before `apply_actions`
    // can synchronously re-enter a listener that borrows the core again.
    let verdict = core.borrow().component_ports_gate(instance, what);
    match verdict {
        Ok(()) => true,
        Err(refusal) => {
            dom::apply_actions(std::slice::from_ref(&refusal), handle);
            false
        }
    }
}

/// Lift a port-vocabulary violation into the thrown value the loader's shim turns
/// into a trap, writing the breadcrumb on the way out.
///
/// The four headless port exports answer `Ok(String)` for everything the
/// contract has a word for and `Err` for the one thing it does not: a publish to
/// a port the component's specification never declared. That is the DOM family's
/// treatment of a call with no answer, applied to the port family for the same
/// reason — the component's behaviour contradicts the specification its artifact
/// is hash-bound to, and there is nothing to carry on with.
fn undeclared_port_trap(host: &ProcessorHost, instance: &str, port: &str) -> JsValue {
    let (report, reason) = crate::logic::undeclared_port(instance, port);
    dom::apply_actions(std::slice::from_ref(&report), &host.handle);
    JsValue::from_str(&reason)
}

/// The answer a port export gives for one buffer verdict: the WIT variant name
/// for a refusal, a trap for a violation.
fn port_answer<E>(
    host: &ProcessorHost,
    instance: &str,
    fault: crate::publish_buffer::PortFault<E>,
    variant: impl FnOnce(E) -> String,
) -> Result<String, JsValue> {
    match fault {
        crate::publish_buffer::PortFault::Refused(err) => Ok(variant(err)),
        crate::publish_buffer::PortFault::Undeclared(port) => {
            Err(undeclared_port_trap(host, instance, &port))
        }
    }
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
) -> Result<String, JsValue> {
    let urgency = match urgency {
        Some(raw) => match crate::Urgency::parse(&raw) {
            Some(urgency) => Some(urgency),
            // The guest's WIT enum lifts to a fixed string set, so an
            // unrecognized value is transpile-glue drift, not a component typo.
            None => return Ok("invalid-payload".to_string()),
        },
        None => None,
    };
    with_processor_host("processor publish", |host| {
        if !ports_granted(&host.core, &host.handle, instance, "ports.publish") {
            return Ok("not-permitted".to_string());
        }
        match host
            .handle
            .try_buffered_publish(instance, port, body, urgency)
        {
            Some(Ok(admission)) => {
                dom::count_admission(instance, admission);
                Ok(String::new())
            }
            Some(Err(fault)) => port_answer(host, instance, fault, crate::logic::publish_error_str),
            // TODO(surface-wasm-test-in-ci): this None arm (absent host slot →
            // "not-permitted") depends on the live wasm host slot and can only
            // be pinned by the browser test runner, unlike the variant map,
            // which is natively tested in `logic`.
            None => Ok("not-permitted".to_string()),
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
) -> Result<String, JsValue> {
    with_processor_host("processor deferred publish", |host| {
        if !ports_granted(&host.core, &host.handle, instance, "ports.publish-deferred") {
            return Ok("not-permitted".to_string());
        }
        match host
            .handle
            .try_buffered_publish_deferred(instance, port, body, deliver_after)
        {
            Some(Ok(admission)) => {
                dom::count_admission(instance, admission);
                Ok(String::new())
            }
            Some(Err(fault)) => port_answer(host, instance, fault, crate::logic::publish_error_str),
            None => Ok("not-permitted".to_string()),
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
pub fn brenn_processor_defer_cancel(
    instance: &str,
    port: &str,
    index: u32,
) -> Result<String, JsValue> {
    with_processor_host("processor defer cancel", |host| {
        if !ports_granted(&host.core, &host.handle, instance, "ports.defer-cancel") {
            return Ok("not-permitted".to_string());
        }
        match host.handle.try_buffered_defer_cancel(instance, port, index) {
            Some(Ok(())) => Ok(String::new()),
            Some(Err(fault)) => port_answer(host, instance, fault, crate::logic::defer_error_str),
            None => Ok("not-permitted".to_string()),
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
) -> Result<String, JsValue> {
    with_processor_host("processor defer edit", |host| {
        if !ports_granted(&host.core, &host.handle, instance, "ports.defer-edit") {
            return Ok("not-permitted".to_string());
        }
        match host
            .handle
            .try_buffered_defer_edit(instance, port, index, body, deliver_after)
        {
            Some(Ok(())) => Ok(String::new()),
            Some(Err(fault)) => port_answer(host, instance, fault, crate::logic::defer_error_str),
            None => Ok("not-permitted".to_string()),
        }
    })
}

/// A processor instance's `log.*` import: one component log line, attributed to
/// the instance.
#[wasm_bindgen]
pub fn brenn_processor_log(instance: &str, level: &str, message: &str) {
    with_processor_host("processor log", |host| {
        let action = host.core.borrow().route_log(instance, level, message);
        dom::apply_actions(std::slice::from_ref(&action), &host.handle);
    });
}

/// A processor instance's `alert.*` import. Gated on that instance's own `alert`
/// grant: boot proved the grant for an instance whose kind imports `alert`, and
/// this is the runtime half of that same gate — a conforming kernel never emits
/// an ungranted `Alert`.
#[wasm_bindgen]
pub fn brenn_processor_alert(instance: &str, severity: &str, title: &str, body: &str) {
    with_processor_host("processor alert", |host| {
        let action = host
            .core
            .borrow()
            .route_alert(instance, severity, title, body);
        dom::apply_actions(std::slice::from_ref(&action), &host.handle);
    });
}

/// A processor instance's `config.get` import. Answers from the map the
/// instance's own component entry carries; a miss is `None`, which is the
/// import's own `option<string>`.
///
/// Gated on that instance's own `config` grant: an ungranted read gets the
/// absence answer the import already spells, plus a breadcrumb.
#[wasm_bindgen]
pub fn brenn_processor_config_get(instance: &str, key: &str) -> Option<String> {
    with_processor_host("processor config get", |host| {
        // Bound before the match, deliberately: the borrow of a match scrutinee
        // lives to the end of the match, and the refusal arm runs a DOM effect.
        let answer = host.core.borrow().component_config_get(instance, key);
        match answer {
            Ok(value) => value,
            Err(refusal) => {
                dom::apply_actions(std::slice::from_ref(&refusal), &host.handle);
                None
            }
        }
    })
}

/// Register a headless processor instance's `receive` export with the kernel.
///
/// The tail is `handle.register_activation` — the DOM path's tail, unchanged — but
/// the admission ahead of it is [`KernelCore::on_processor_register`], because the
/// DOM gate resolves its instance from a mounted element and a processor has
/// none. The admission also decides mountability — one reading of the `dom`
/// grant, in the core that emits the host element — and the entry is forwarded
/// with it. Returns whether the registration was admitted, so the loader can
/// tell a refusal from success without reading kernel state.
#[wasm_bindgen]
pub fn brenn_processor_register(instance: &str, entry: js_sys::Function) -> bool {
    with_processor_host("processor register", |host| {
        let ProcessorRegistration {
            admitted,
            mount,
            actions,
        } = host.core.borrow_mut().on_processor_register(instance);
        dom::apply_actions(&actions, &host.handle);
        if admitted {
            host.handle.register_activation(
                instance,
                dom::wrap_activation_entry(instance, entry),
                mount,
            );
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

// ── the DOM capability seam ─────────────────────────────────────────────────
//
// One export per WIT function of `brenn:processor/dom` and
// `brenn:processor/page-dom`, each taking the instance id from the loader's own
// closure over the manifest entry — never from the component. Every one gates on
// that instance's own grant and answers a refusal by throwing, which the guest
// takes as a trap: see [`DomHost`] for why this family has no error variant.

/// `dom.root`.
#[wasm_bindgen]
pub fn brenn_dom_root(instance: &str) -> Result<u64, JsValue> {
    with_dom_host("dom root", |host| trap(host.root(instance)))
}

/// `dom.create-element`.
#[wasm_bindgen]
pub fn brenn_dom_create_element(instance: &str, tag: &str) -> Result<u64, JsValue> {
    with_dom_host("dom create-element", |host| {
        trap(host.create_element(instance, tag))
    })
}

/// `dom.set-attribute`.
#[wasm_bindgen]
pub fn brenn_dom_set_attribute(
    instance: &str,
    node: u64,
    name: &str,
    value: &str,
) -> Result<(), JsValue> {
    with_dom_host("dom set-attribute", |host| {
        trap(host.set_attribute(instance, node, name, value))
    })
}

/// `dom.remove-attribute`.
#[wasm_bindgen]
pub fn brenn_dom_remove_attribute(instance: &str, node: u64, name: &str) -> Result<(), JsValue> {
    with_dom_host("dom remove-attribute", |host| {
        trap(host.remove_attribute(instance, node, name))
    })
}

/// `dom.set-text`.
#[wasm_bindgen]
pub fn brenn_dom_set_text(instance: &str, node: u64, text: &str) -> Result<(), JsValue> {
    with_dom_host("dom set-text", |host| {
        trap(host.set_text(instance, node, text))
    })
}

/// `dom.set-style-property`.
#[wasm_bindgen]
pub fn brenn_dom_set_style_property(
    instance: &str,
    node: u64,
    name: &str,
    value: &str,
) -> Result<(), JsValue> {
    with_dom_host("dom set-style-property", |host| {
        trap(host.set_style_property(instance, node, name, value))
    })
}

/// `dom.remove-style-property`.
#[wasm_bindgen]
pub fn brenn_dom_remove_style_property(
    instance: &str,
    node: u64,
    name: &str,
) -> Result<(), JsValue> {
    with_dom_host("dom remove-style-property", |host| {
        trap(host.remove_style_property(instance, node, name))
    })
}

/// `dom.append`.
#[wasm_bindgen]
pub fn brenn_dom_append(instance: &str, parent: u64, child: u64) -> Result<(), JsValue> {
    with_dom_host("dom append", |host| {
        trap(host.append(instance, parent, child))
    })
}

/// `dom.insert-before`.
#[wasm_bindgen]
pub fn brenn_dom_insert_before(
    instance: &str,
    parent: u64,
    child: u64,
    reference: Option<u64>,
) -> Result<(), JsValue> {
    with_dom_host("dom insert-before", |host| {
        trap(host.insert_before(instance, parent, child, reference))
    })
}

/// `dom.remove`.
#[wasm_bindgen]
pub fn brenn_dom_remove(instance: &str, node: u64) -> Result<(), JsValue> {
    with_dom_host("dom remove", |host| trap(host.remove(instance, node)))
}

/// `dom.value`.
#[wasm_bindgen]
pub fn brenn_dom_value(instance: &str, node: u64) -> Result<String, JsValue> {
    with_dom_host("dom value", |host| trap(host.value(instance, node)))
}

/// `dom.set-value`.
#[wasm_bindgen]
pub fn brenn_dom_set_value(instance: &str, node: u64, value: &str) -> Result<(), JsValue> {
    with_dom_host("dom set-value", |host| {
        trap(host.set_value(instance, node, value))
    })
}

/// `dom.listen`.
#[wasm_bindgen]
pub fn brenn_dom_listen(instance: &str, node: u64, event: &str, port: &str) -> Result<(), JsValue> {
    with_dom_host("dom listen", |host| {
        trap(host.listen(instance, node, event, port))
    })
}

/// `dom.utc-offset-minutes`.
#[wasm_bindgen]
pub fn brenn_dom_utc_offset_minutes(instance: &str, epoch_ms: u64) -> Result<i32, JsValue> {
    with_dom_host("dom utc-offset-minutes", |host| {
        trap(host.utc_offset_minutes(instance, epoch_ms))
    })
}

/// `page-dom.page-root`.
#[wasm_bindgen]
pub fn brenn_dom_page_root(instance: &str) -> Result<u64, JsValue> {
    with_dom_host("page-dom page-root", |host| trap(host.page_root(instance)))
}

/// `page-dom.page-body`.
#[wasm_bindgen]
pub fn brenn_dom_page_body(instance: &str) -> Result<u64, JsValue> {
    with_dom_host("page-dom page-body", |host| trap(host.page_body(instance)))
}

/// `page-dom.instance-wrapper`.
#[wasm_bindgen]
pub fn brenn_dom_instance_wrapper(instance: &str, of: &str) -> Result<Option<u64>, JsValue> {
    with_dom_host("page-dom instance-wrapper", |host| {
        trap(host.instance_wrapper(instance, of))
    })
}

/// `page-dom.parent`.
#[wasm_bindgen]
pub fn brenn_dom_parent(instance: &str, node: u64) -> Result<Option<u64>, JsValue> {
    with_dom_host("page-dom parent", |host| trap(host.parent(instance, node)))
}

/// Drain the page's event stream, folding each [`Event`] through the DOM-free
/// core and applying the emitted actions.
async fn run_event_loop(
    core: Rc<RefCell<KernelCore>>,
    mut events: EventStream,
    handle: Rc<SurfaceHandle>,
) {
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
        let actions = core.borrow_mut().on_event(&event);
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

// Browser-level unit tests for the two page-meta reads. Run via
// wasm-bindgen-test under a headless WebDriver browser; the whole module is
// wasm32-only, so the host test sweep never compiles them. What the kernel does
// with a live component is `dom_host.rs`'s browser suite and the runner's own
// host suite; nothing here needs a page beyond the document.
#[cfg(test)]
mod tests {
    use super::*;

    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

    use crate::wasm_test_util::doc;

    wasm_bindgen_test_configure!(run_in_browser);

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
