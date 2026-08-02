//! The component browser-suite harness: mount a component the way the shell
//! does, play the kernel seams it can reach, and build the activations its entry
//! is called with. Browser (wasm32) only.
//!
//! Every in-tree component's `wasm_bindgen_test` suite performs the same three
//! steps, and a hand-copied harness per crate rots the moment the registration
//! convention, a seam's detail shape, or the activation struct moves. One copy
//! here means such a change breaks every suite at once and loudly, and lets an
//! assertion written in one crate be read by analogy in another. Each suite
//! keeps only its own assertions.

use brenn_surface_contract::{
    ACTIVATION_REGISTER, ACTIVATION_SYNC, Activation, COMPONENT_LOG, DEFER_STATUS_FIELD,
    PORT_DEFER, PORT_PUBLISH, PUBLISH_STATUS_FIELD, PortWindow, SYNC_STATUS_FIELD, SyncStatus,
    defer_status_str, element_name_for_instance, publish_status_str, sync_status_str,
};
use wasm_bindgen::{JsCast, JsValue};
use web_sys::HtmlElement;

use crate::sample_envelope;

/// Mount `instance` of `kind` the way the shell does — run the module's bind,
/// then create and connect its element — and hand back the activation entry it
/// registered alongside its connected host element.
///
/// `bind` is the crate's own `brenn_bind_instance`. It installs the module's
/// panic hook, which is put back to the harness's straight after: a failing
/// assertion must fail the test, not dispatch a component panic at a kernel that
/// is not here.
///
/// One mount per test binary: `bind` panics on a second call in one module
/// record, so a crate's whole browser suite shares one mounted instance.
pub fn mount(
    kind: &str,
    instance: &str,
    bind: impl FnOnce(String),
) -> (js_sys::Function, HtmlElement) {
    let harness_hook = std::panic::take_hook();
    bind(instance.to_string());
    std::panic::set_hook(harness_hook);

    let tag = element_name_for_instance(kind, instance);
    let entry: js_sys::Function = js_sys::Function::new_with_args(
        "tag, register",
        "const caught = { entry: null };\n\
         const listener = (event) => { caught.entry = event.detail.entry; };\n\
         document.body.addEventListener(register, listener);\n\
         document.body.appendChild(document.createElement(tag));\n\
         document.body.removeEventListener(register, listener);\n\
         return caught.entry;",
    )
    .call2(
        &JsValue::NULL,
        &JsValue::from_str(&tag),
        &JsValue::from_str(ACTIVATION_REGISTER),
    )
    .expect("connect the component's element")
    .dyn_into()
    .expect("the registration detail carries the entry function");

    let host = document()
        .query_selector(&tag)
        .expect("query_selector runs on the document")
        .expect("the connected element is in the document")
        .dyn_into::<HtmlElement>()
        .expect("the component's element is an HtmlElement");
    (entry, host)
}

/// The property [`record_ops`] hangs its live answer table off the op array
/// under. Not an array index, so it is invisible to [`recorded`]'s iteration and
/// to `length`.
const ANSWERS: &str = "brennAnswers";

/// Play every kernel seam a component can reach: record each op that bubbles to
/// the body and answer the three that owe an answer. Hands back the array they
/// land in; read it with [`recorded`] or [`take_recorded`].
///
/// Install it before the step under test. Installed before the mount, it catches
/// connect-time code that published, parked, or asked for a sync activation —
/// where silence is the assertion.
///
/// Every seam is recorded whether or not the component under test uses it, so a
/// component that reaches an unexpected one — a log line from a window it should
/// have skipped, a publish nobody asked for — shows up as an extra row rather
/// than as nothing.
///
/// The three answers start at `ok` and are read at event time, so
/// [`answer_publishes_with`] can turn a later phase's publishes into refusals.
/// Install one recorder per test: the listeners are page-lifetime and the last
/// one installed is the one whose answer the component reads.
pub fn record_ops() -> js_sys::Array {
    let install = js_sys::Function::new_with_args(
        "publishEvent, deferEvent, syncEvent, logEvent, publishKey, deferKey, syncKey, \
         publishOk, deferOk, syncOk, answersKey",
        "const ops = [];\n\
         const answers = { publish: publishOk, defer: deferOk, sync: syncOk };\n\
         ops[answersKey] = answers;\n\
         const take = (seam, key) => (event) => {\n\
           const detail = event.detail;\n\
           ops.push([seam, detail.op || '', detail.port || '',\n\
                     detail.body || detail.message || '',\n\
                     String(detail.deliver_after === undefined ? '' : detail.deliver_after)]);\n\
           if (key) { detail[key] = answers[seam]; }\n\
         };\n\
         document.body.addEventListener(publishEvent, take('publish', publishKey));\n\
         document.body.addEventListener(deferEvent, take('defer', deferKey));\n\
         document.body.addEventListener(syncEvent, take('sync', syncKey));\n\
         document.body.addEventListener(logEvent, take('log', ''));\n\
         return ops;",
    );
    let args = js_sys::Array::new();
    for value in [
        PORT_PUBLISH,
        PORT_DEFER,
        ACTIVATION_SYNC,
        COMPONENT_LOG,
        PUBLISH_STATUS_FIELD,
        DEFER_STATUS_FIELD,
        SYNC_STATUS_FIELD,
        publish_status_str(Ok(())),
        defer_status_str(Ok(())),
        sync_status_str(SyncStatus::Ok),
        ANSWERS,
    ] {
        args.push(&JsValue::from_str(value));
    }
    js_sys::Reflect::apply(&install, &JsValue::NULL, &args)
        .expect("install the kernel-playing listeners")
        .dyn_into()
        .expect("the installer answers the op array")
}

/// Answer every publish this recorder hears from now on with `status` — one of
/// [`publish_status_str`]'s wire strings.
///
/// A component's refusal arms are ordinary code with ordinary state transitions
/// (a counter that must not advance, an "already announced" flag that must not
/// be set), and a harness that only ever says `ok` leaves every one of them
/// unexercised.
///
/// The defer seam keeps answering `ok`: a tick re-park is not what these arms
/// are about, and refusing it would only make every such test die inside
/// `repark_tick`.
pub fn answer_publishes_with(ops: &js_sys::Array, status: &str) {
    let answers = js_sys::Reflect::get(ops, &JsValue::from_str(ANSWERS))
        .expect("the op array carries its answer table");
    js_sys::Reflect::set(
        &answers,
        &JsValue::from_str("publish"),
        &JsValue::from_str(status),
    )
    .expect("set the publish answer");
}

/// [`recorded`], then empty the array — so a multi-phase test reads each phase's
/// ops on their own rather than re-reading every earlier phase's.
pub fn take_recorded(ops: &js_sys::Array) -> Vec<Vec<String>> {
    let rows = recorded(ops);
    ops.set_length(0);
    rows
}

/// The ops [`record_ops`] has collected so far, each a row of
/// `[seam, op, port, body, deliver_after]` — the fields absent from a given seam
/// arriving as empty strings.
///
/// `seam` is `publish` / `defer` / `sync` / `log`; `op` is the defer family's
/// verb; `body` carries a log's message.
pub fn recorded(ops: &js_sys::Array) -> Vec<Vec<String>> {
    ops.iter()
        .map(|entry| {
            js_sys::Array::from(&entry)
                .iter()
                .map(|field| field.as_string().unwrap_or_default())
                .collect()
        })
        .collect()
}

/// One activation as the kernel's JSON: a window per `(port, body)` pair, each
/// carrying that body as its single new envelope, with `sync` naming the live
/// request's port on a sync-call activation.
///
/// The envelope is [`crate::sample_envelope`], so a serde change to
/// `MessageEnvelope` breaks the one fixture rather than four hand-built literals.
pub fn activation_json(windows: &[(&str, &str)], sync: Option<&str>, now_ms: u64) -> String {
    serde_json::to_string(&Activation {
        ports: windows
            .iter()
            .map(|(port, body)| PortWindow {
                port: (*port).to_string(),
                envelopes: vec![sample_envelope(body)],
                new_from: 0,
                dropped: 0,
            })
            .collect(),
        deferred: vec![],
        now: Some(now_ms),
        sync: sync.map(str::to_string),
    })
    .expect("the fixture activation serializes")
}

/// The live `Document`.
fn document() -> web_sys::Document {
    web_sys::window()
        .expect("a browser test runs in a window")
        .document()
        .expect("the window has a document")
}
