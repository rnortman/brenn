// The surface kinds' DOM transcripts, wasmtime half.
//
// The artifacts the page loads transpiled — `brenn_echo_stub.wasm`,
// `brenn_meeting.wasm`, `brenn_chrome.wasm` — driven through scripted
// activation sequences against one recording implementation of the
// `brenn:processor/dom` and `brenn:processor/page-dom` imports, and reduced to
// an ordered transcript of what each asked the page to do.
//
// Why this suite exists: every other test of a page-hosted kind's DOM behaviour
// is a `wasm_bindgen_test` in a browser suite nothing in CI runs
// (TODO(surface-wasm-test-in-ci)). This one runs where the rest of the Rust
// tree runs, so a migrated UI kind's actual element calls, gesture handling and
// publish taxonomy are executable coverage rather than inspection.
//
// This is not a second hosting. `ProcessorComponent` refuses `dom` structurally
// — `illegal_on(TopLevel)` — and nothing here goes through it: the harness
// builds its own linker over the same WIT and links exactly what the kind's
// specification requires, so an artifact that acquired another import fails
// here rather than at boot. Two differences from that host are deliberate and are
// what make the fixture faithful to the page:
//
//   - one instantiation drives the whole script, because a page-hosted instance
//     is instantiated once and lives for the page, and a migrated kind's view
//     handles are ordinary struct state across activations;
//   - activations may be sync calls, which is how a mount and a gesture arrive.
//
// The recording host is held to the real vocabulary: the allow-list predicates,
// the mount port and the gesture body field names all come from
// `surface/contract`, so a component that steps outside what the kernel host
// would admit fails here instead of in a browser.

use std::collections::BTreeMap;

use brenn_chrome::spec::port::{SURFACE_STATE, TOAST};
use brenn_chrome::wire::{
    CONTROL_PLANE_VERSION, InstanceState, SurfaceStateBody, SurfaceStateInstance, ToastBody,
    ToastSeverity, ToastSource,
};
use brenn_meeting::logic::{AckTarget, SNOOZE_SECS, dismiss_body, snooze_body};
use brenn_surface_contract::{
    GESTURE_EVENT_FIELD, GESTURE_LISTENER_FIELD, GESTURE_TARGET_FIELD, MOUNT_SYNC_PORT,
    dom_attribute_allowed, dom_tag_allowed,
};
use chrono::{DateTime, Duration, Utc};
use wasmtime::component::{Component, HasSelf, Linker};
use wasmtime::{Config, Engine, Store};

mod common;

mod bindings {
    wasmtime::component::bindgen!({
        world: "processor",
        path: "wit/processor.wit",
    });
}

use bindings::brenn::processor::{dom, log as wit_log, page_dom, ports, types};

/// The one input port echo-stub's specification binds.
const ECHO_IN_PORT: &str = "messages";

/// The one output port it publishes on.
const ECHO_OUT_PORT: &str = "out";

/// Echo-stub's sync port names, its own vocabulary — not bound in the
/// specification.
const SEND: &str = "send";
const SEND_CUSTOM: &str = "send-custom";
const PANIC: &str = "panic";

/// The component's scrollback cap. A copy of its own private constant; the test
/// that uses it asserts the observable bound rather than the number.
const MAX_SCROLLBACK_ENTRIES: usize = 100;

/// meeting's two bound ports, and the two sync ports its buttons ask for.
const AGENDA_PORT: &str = "agenda";
const ACKS_PORT: &str = "acks";
const DISMISS: &str = "dismiss";
const SNOOZE: &str = "snooze";

/// The instance chrome runs as in this fixture, and the one other instance
/// already registered on the page it arranges.
const CHROME_INSTANCE: &str = "chrome";
const SIBLING_INSTANCE: &str = "panel-1";

/// The sync port chrome's one delegated toast listener asks for. Its own
/// vocabulary, bound to nothing in the specification.
const TOAST_DISMISS: &str = "toast-dismiss";

/// One element of the fake page.
#[derive(Default)]
struct Element {
    attributes: BTreeMap<String, String>,
    text: Option<String>,
    /// A form control's value, which `dom.value` reads and a test seeds.
    value: String,
    children: Vec<u64>,
    parent: Option<u64>,
}

/// The listener the host owns after a `dom.listen`.
#[derive(Debug, PartialEq, Eq)]
struct Listener {
    node: u64,
    event: String,
    port: String,
}

/// The host half of the fixture: a fake element tree, the calls made against
/// it, and the answers the bus gives.
struct Page {
    /// Handle `i + 1` names entry `i`, exactly as the kernel's handle table
    /// numbers them, so a transcript reads against the same identities.
    elements: Vec<Element>,
    listeners: Vec<Listener>,
    /// Every guest-visible call, in call order.
    transcript: Vec<String>,
    /// What the component published and where, in call order.
    published: Vec<(String, String)>,
    /// What it parked and where, in call order.
    parked: Vec<(String, String, u64)>,
    /// What every publish answers. `None` is acceptance.
    publish_answer: Option<ports::PublishError>,
    /// The page beyond this instance's own subtree, which only a `page-dom`
    /// fixture has: the surface root, the body, and the kernel wrapper each
    /// registered instance hangs under.
    page_root: Option<u64>,
    body: Option<u64>,
    instance_wrappers: BTreeMap<String, u64>,
}

/// The instance's host element: the kernel mounts it before the mount
/// activation, so it is handle 1 before the component runs.
const ROOT: u64 = 1;

impl Page {
    fn new() -> Page {
        Page {
            elements: vec![Element::default()],
            listeners: Vec::new(),
            transcript: Vec::new(),
            published: Vec::new(),
            parked: Vec::new(),
            publish_answer: None,
            page_root: None,
            body: None,
            instance_wrappers: BTreeMap::new(),
        }
    }

    /// The fixture a page-authority holder runs against: this instance's host
    /// element under its own kernel wrapper, that wrapper under the surface
    /// root, the surface root under the body, and one sibling instance already
    /// registered — the shape chrome reads its own identity out of.
    ///
    /// Built by hand rather than through the recording calls, so a transcript
    /// starts at what the component did.
    fn with_page_authority(own_instance: &str, sibling: &str) -> Page {
        let mut page = Page::new();
        let own_wrapper = page.mint();
        let page_root = page.mint();
        let body = page.mint();
        let sibling_wrapper = page.mint();
        page.link(own_wrapper, ROOT);
        page.link(page_root, own_wrapper);
        page.link(body, page_root);
        page.link(page_root, sibling_wrapper);
        page.page_root = Some(page_root);
        page.body = Some(body);
        page.instance_wrappers
            .insert(own_instance.to_string(), own_wrapper);
        page.instance_wrappers
            .insert(sibling.to_string(), sibling_wrapper);
        page
    }

    /// Parent `child` to `parent` without recording a call.
    fn link(&mut self, parent: u64, child: u64) {
        self.element(child).parent = Some(parent);
        self.element(parent).children.push(child);
    }

    /// The wrapper the fixture minted for `instance`.
    fn wrapper_of(&self, instance: &str) -> u64 {
        self.instance_wrappers[instance]
    }

    /// The payloads accepted on `port`, in publish order.
    fn published_on(&self, port: &str) -> Vec<&str> {
        self.published
            .iter()
            .filter(|(on, _)| on == port)
            .map(|(_, payload)| payload.as_str())
            .collect()
    }

    fn record(&mut self, line: String) {
        self.transcript.push(line);
    }

    /// The element a handle names. A handle the host never minted is a
    /// component bug the kernel answers with a trap; here it fails the test,
    /// which is the same statement in a harness that has no error card.
    fn element(&mut self, handle: u64) -> &mut Element {
        let index = usize::try_from(handle)
            .ok()
            .and_then(|handle| handle.checked_sub(1))
            .unwrap_or_else(|| panic!("dom: handle {handle} names no element"));
        self.elements
            .get_mut(index)
            .unwrap_or_else(|| panic!("dom: handle {handle} names no element"))
    }

    fn mint(&mut self) -> u64 {
        self.elements.push(Element::default());
        self.elements.len() as u64
    }

    /// Detach a node from whatever holds it, which both `append` and `remove`
    /// do first — the browser semantics the kernel host inherits from the DOM.
    fn detach(&mut self, node: u64) {
        let parent = self.element(node).parent.take();
        if let Some(parent) = parent {
            self.element(parent).children.retain(|held| *held != node);
        }
    }

    /// The children of a node, for a test that asserts the shape the script
    /// built rather than the calls that built it.
    fn children(&mut self, node: u64) -> Vec<u64> {
        self.element(node).children.clone()
    }

    fn text_of(&mut self, node: u64) -> String {
        self.element(node).text.clone().unwrap_or_default()
    }

    /// The one descendant of `node` carrying `marker`, for a view whose parts
    /// are not all direct children.
    fn marked_descendant(&mut self, node: u64, marker: &str) -> u64 {
        let mut found = Vec::new();
        let mut pending = vec![node];
        while let Some(next) = pending.pop() {
            if self.element(next).attributes.contains_key(marker) && next != node {
                found.push(next);
            }
            pending.extend(self.children(next));
        }
        assert_eq!(
            found.len(),
            1,
            "expected exactly one descendant of {node} carrying `{marker}`, found {found:?}"
        );
        found[0]
    }

    /// The one child of `node` carrying `marker`, which is how echo-stub names
    /// each part of its view.
    fn marked_child(&mut self, node: u64, marker: &str) -> u64 {
        let children = self.children(node);
        let mut found = children
            .into_iter()
            .filter(|child| self.element(*child).attributes.contains_key(marker));
        let child = found
            .next()
            .unwrap_or_else(|| panic!("no child of {node} carries `{marker}`"));
        assert!(
            found.next().is_none(),
            "more than one child of {node} carries `{marker}`"
        );
        child
    }
}

/// Render a handle the way the transcript names it.
fn n(handle: u64) -> String {
    format!("n{handle}")
}

impl dom::Host for Page {
    fn root(&mut self) -> u64 {
        self.record(format!("dom.root -> {}", n(ROOT)));
        ROOT
    }

    fn create_element(&mut self, tag: String) -> u64 {
        // The kernel host traps here; the harness holds the artifact to the
        // same vocabulary so a kind that outgrows the allow-list is caught in
        // CI rather than on the page.
        assert!(dom_tag_allowed(&tag), "`{tag}` is not a creatable tag");
        let node = self.mint();
        self.record(format!("dom.create-element({tag}) -> {}", n(node)));
        node
    }

    fn set_attribute(&mut self, node: u64, name: String, value: String) {
        assert!(
            dom_attribute_allowed(&name),
            "`{name}` is not a settable attribute"
        );
        self.record(format!("dom.set-attribute({}, {name}, {value:?})", n(node)));
        self.element(node).attributes.insert(name, value);
    }

    fn remove_attribute(&mut self, node: u64, name: String) {
        assert!(
            dom_attribute_allowed(&name),
            "`{name}` is not a settable attribute"
        );
        self.record(format!("dom.remove-attribute({}, {name})", n(node)));
        self.element(node).attributes.remove(&name);
    }

    fn set_text(&mut self, node: u64, text: String) {
        self.record(format!("dom.set-text({}, {text:?})", n(node)));
        let element = self.element(node);
        element.children.clear();
        element.text = Some(text);
    }

    fn set_style_property(&mut self, node: u64, name: String, value: String) {
        self.record(format!(
            "dom.set-style-property({}, {name}, {value:?})",
            n(node)
        ));
    }

    fn remove_style_property(&mut self, node: u64, name: String) {
        self.record(format!("dom.remove-style-property({}, {name})", n(node)));
    }

    fn append(&mut self, parent: u64, child: u64) {
        self.record(format!("dom.append({}, {})", n(parent), n(child)));
        self.detach(child);
        self.element(child).parent = Some(parent);
        self.element(parent).children.push(child);
    }

    fn insert_before(&mut self, parent: u64, child: u64, reference: Option<u64>) {
        self.record(format!(
            "dom.insert-before({}, {}, {})",
            n(parent),
            n(child),
            reference.map(n).unwrap_or_else(|| "none".to_string()),
        ));
        self.detach(child);
        self.element(child).parent = Some(parent);
        let at = match reference {
            Some(reference) => self
                .element(parent)
                .children
                .iter()
                .position(|held| *held == reference)
                .unwrap_or_else(|| panic!("{reference} is no child of {parent}")),
            None => self.element(parent).children.len(),
        };
        self.element(parent).children.insert(at, child);
    }

    fn remove(&mut self, node: u64) {
        self.record(format!("dom.remove({})", n(node)));
        self.detach(node);
    }

    fn value(&mut self, node: u64) -> String {
        let value = self.element(node).value.clone();
        self.record(format!("dom.value({}) -> {value:?}", n(node)));
        value
    }

    fn set_value(&mut self, node: u64, value: String) {
        self.record(format!("dom.set-value({}, {value:?})", n(node)));
        self.element(node).value = value;
    }

    fn listen(&mut self, node: u64, event: String, port: String) {
        self.record(format!("dom.listen({}, {event}, {port})", n(node)));
        self.listeners.push(Listener { node, event, port });
    }

    fn utc_offset_minutes(&mut self, epoch_ms: u64) -> i32 {
        self.record(format!("dom.utc-offset-minutes({epoch_ms})"));
        0
    }
}

impl ports::Host for Page {
    fn publish(&mut self, port: String, payload: String) -> Result<(), ports::PublishError> {
        self.record(format!("ports.publish({port}, {payload:?})"));
        match &self.publish_answer {
            None => {
                self.published.push((port, payload));
                Ok(())
            }
            Some(refusal) => Err(refusal.clone()),
        }
    }

    fn publish_with_urgency(
        &mut self,
        _port: String,
        _payload: String,
        _urgency: ports::Urgency,
    ) -> Result<(), ports::PublishError> {
        unreachable!("no kind here publishes at other than the port's default urgency")
    }

    fn publish_deferred(
        &mut self,
        port: String,
        payload: String,
        deliver_after: u64,
    ) -> Result<(), ports::PublishError> {
        self.record(format!(
            "ports.publish-deferred({port}, {payload:?}, {deliver_after})"
        ));
        match &self.publish_answer {
            None => {
                self.parked.push((port, payload, deliver_after));
                Ok(())
            }
            Some(refusal) => Err(refusal.clone()),
        }
    }

    fn defer_cancel(&mut self, port: String, index: u32) -> Result<(), ports::DeferError> {
        // The repark cancels the tick it just rode in on, so this is called
        // once per parked-port activation and answers as the kernel does.
        self.record(format!("ports.defer-cancel({port}, {index})"));
        Ok(())
    }

    fn defer_edit(
        &mut self,
        _port: String,
        _index: u32,
        _payload: Option<String>,
        _deliver_after: Option<u64>,
    ) -> Result<(), ports::DeferError> {
        unreachable!("a repark cancels and re-parks; nothing here edits in place")
    }
}

impl page_dom::Host for Page {
    fn page_root(&mut self) -> u64 {
        let root = self
            .page_root
            .expect("this fixture grants no page authority");
        self.record(format!("page-dom.page-root -> {}", n(root)));
        root
    }

    fn page_body(&mut self) -> u64 {
        let body = self.body.expect("this fixture grants no page authority");
        self.record(format!("page-dom.page-body -> {}", n(body)));
        body
    }

    fn instance_wrapper(&mut self, instance: String) -> Option<u64> {
        let wrapper = self.instance_wrappers.get(&instance).copied();
        self.record(format!(
            "page-dom.instance-wrapper({instance}) -> {}",
            wrapper.map(n).unwrap_or_else(|| "none".to_string())
        ));
        wrapper
    }

    fn parent(&mut self, node: u64) -> Option<u64> {
        let parent = self.element(node).parent;
        self.record(format!(
            "page-dom.parent({}) -> {}",
            n(node),
            parent.map(n).unwrap_or_else(|| "none".to_string())
        ));
        parent
    }
}

impl wit_log::Host for Page {
    fn log(&mut self, level: wit_log::Level, message: String) {
        let level = match level {
            wit_log::Level::Trace => "trace",
            wit_log::Level::Debug => "debug",
            wit_log::Level::Info => "info",
            wit_log::Level::Warn => "warn",
            wit_log::Level::Error => "error",
        };
        self.record(format!("log.{level}({message:?})"));
    }
}

/// One instantiation of the artifact, its recording page, and the way to drive
/// activations at it.
struct Harness {
    store: Store<Page>,
    instance: bindings::Processor,
}

impl Harness {
    /// Link exactly what the kind's `requires` names and instantiate once. An
    /// artifact that acquired another import fails here, which is the
    /// deny-by-default the production host has.
    fn new(artifact: &str, page: Page) -> Harness {
        let page_authority = page.page_root.is_some();
        let engine = Engine::new(&Config::new()).expect("wasmtime engine");
        let component = Component::from_file(&engine, common::artifact_path(artifact))
            .unwrap_or_else(|err| panic!("the {artifact} component artifact: {err}"));
        let mut linker: Linker<Page> = Linker::new(&engine);
        ports::add_to_linker::<_, HasSelf<Page>>(&mut linker, |page| page).expect("link ports");
        wit_log::add_to_linker::<_, HasSelf<Page>>(&mut linker, |page| page).expect("link log");
        dom::add_to_linker::<_, HasSelf<Page>>(&mut linker, |page| page).expect("link dom");
        if page_authority {
            page_dom::add_to_linker::<_, HasSelf<Page>>(&mut linker, |page| page)
                .expect("link page-dom");
        }
        let mut store = Store::new(&engine, page);
        let instance = bindings::Processor::instantiate(&mut store, &component, &linker)
            .expect("the artifact instantiates against the linked profile");
        Harness { store, instance }
    }

    /// The three kinds this suite drives, each already mounted: every test past
    /// the mount transcript is about what an activation *after* the mount does.
    fn echo_stub() -> Harness {
        Harness::new("brenn_echo_stub", Page::new())
    }

    fn meeting() -> Harness {
        Harness::mount(Harness::new("brenn_meeting", Page::new()))
    }

    fn chrome() -> Harness {
        Harness::mount(Harness::new(
            "brenn_chrome",
            Page::with_page_authority(CHROME_INSTANCE, SIBLING_INSTANCE),
        ))
    }

    /// Drive the mount activation and discard its transcript.
    fn mount(mut harness: Harness) -> Harness {
        harness.call(mount());
        harness.transcript();
        harness
    }

    /// Drive one activation. A page-hosted component answers `Ok(None)` unless
    /// it is cancelling a gesture's default action, which none of the kinds in
    /// this suite does.
    fn call(&mut self, activation: types::Activation) {
        let reply = self
            .instance
            .call_receive(&mut self.store, &activation)
            .expect("the activation did not trap")
            .expect("the activation was not refused");
        assert_eq!(
            reply, None,
            "echo-stub cancels no default action, so it answers nothing"
        );
    }

    /// Drive one activation expecting the instance to die in it.
    ///
    /// A guest panic reaches the host as an `unreachable` trap and nothing
    /// else: the message the component wrote goes to its own log pipeline, not
    /// into the error. So the assertion available here is the fact of the trap,
    /// which is what makes the instance terminal and earns it an error card.
    fn call_expecting_a_trap(&mut self, activation: types::Activation) {
        let error = self
            .instance
            .call_receive(&mut self.store, &activation)
            .expect_err("the activation must trap");
        let error = format!("{error:#}");
        assert!(error.contains("wasm trap"), "{error}");
    }

    fn page(&mut self) -> &mut Page {
        self.store.data_mut()
    }

    /// The calls since the last take, and clear.
    fn transcript(&mut self) -> Vec<String> {
        std::mem::take(&mut self.store.data_mut().transcript)
    }

    /// Echo-stub, mounted.
    fn echo_stub_mounted() -> Harness {
        Harness::mount(Harness::echo_stub())
    }
}

/// A message id in the shape the envelope parser demands, derived from a
/// readable label so a failure names the delivery it came from.
fn message_id(label: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in label.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("00000000-0000-4000-8000-{:012x}", hash & 0xffff_ffff_ffff)
}

/// The fixed non-identifying envelope frame every delivery here wears.
fn envelope(id: &str, body: &str) -> String {
    serde_json::json!({
        "message_id": message_id(id),
        "source": "surface-dom-fixture",
        "channel": "ephemeral:site.surface.fixture",
        "sender": "surface-dom-fixture",
        "publish_ts": "2026-01-01T00:00:00Z",
        "urgency": "normal",
        "envelope_type": "brenn",
        "body": body,
    })
    .to_string()
}

/// A sync-call activation on `port`, carrying the one synthesized request the
/// kernel mints for it and nothing else.
fn sync_call(port: &str, request: String) -> types::Activation {
    types::Activation {
        ports: vec![types::PortWindow {
            port: port.to_string(),
            envelopes: vec![request],
            new_from: 0,
            dropped: 0,
        }],
        deferred: vec![],
        now: Some(NOW_MS),
        sync: Some(port.to_string()),
    }
}

/// The activation clock, fixed: nothing here reads it, and a moving one would
/// be a transcript that changes for no reason.
const NOW_MS: u64 = 1_767_225_600_000;

/// The mount activation, as the runner mints it: the reserved port, an empty
/// synthesized body, and whatever input was already pending — here, none.
fn mount() -> types::Activation {
    sync_call(MOUNT_SYNC_PORT, envelope("mount", "{}"))
}

/// A gesture, as the kernel's listener synthesizes it: the event, the listening
/// node and the nearest handle-mapped ancestor of what was hit.
fn gesture(port: &str, listener: u64) -> types::Activation {
    gesture_at(port, listener, listener)
}

/// A gesture whose target is not the listening node — a click that landed on a
/// descendant of a delegated listener, or on the gap between them.
fn gesture_at(port: &str, listener: u64, target: u64) -> types::Activation {
    let body = serde_json::json!({
        GESTURE_EVENT_FIELD: "click",
        GESTURE_LISTENER_FIELD: listener,
        GESTURE_TARGET_FIELD: target,
    })
    .to_string();
    sync_call(port, envelope("gesture", &body))
}

/// An ordinary delivery on echo-stub's one bound input port.
fn delivery(context: &[&str], new: &[&str], dropped: u32) -> types::Activation {
    delivery_on(ECHO_IN_PORT, context, new, dropped)
}

/// An ordinary delivery on `port`.
fn delivery_on(port: &str, context: &[&str], new: &[&str], dropped: u32) -> types::Activation {
    let mut envelopes: Vec<String> = context
        .iter()
        .enumerate()
        .map(|(i, body)| envelope(&format!("c{i}"), body))
        .collect();
    let new_from = envelopes.len() as u32;
    envelopes.extend(
        new.iter()
            .enumerate()
            .map(|(i, body)| envelope(&format!("m{i}"), body)),
    );
    types::Activation {
        ports: vec![types::PortWindow {
            port: port.to_string(),
            envelopes,
            new_from,
            dropped,
        }],
        deferred: vec![],
        now: Some(NOW_MS),
        sync: None,
    }
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
];

#[test]
fn the_mount_activation_builds_the_view_and_wires_its_gestures() {
    let mut harness = Harness::echo_stub();
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
        harness.page().published.is_empty(),
        "mounting publishes nothing"
    );
}

#[test]
fn a_delivery_renders_its_new_envelopes_and_sums_the_drops() {
    let mut harness = Harness::echo_stub_mounted();
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
    let mut harness = Harness::echo_stub_mounted();
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
    let mut harness = Harness::echo_stub_mounted();
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
    let mut harness = Harness::echo_stub_mounted();
    harness.page().publish_answer = Some(ports::PublishError::QuotaExceeded);
    let send = harness.page().marked_child(ROOT, "data-echo-send");
    harness.call(gesture(SEND, send));

    let transcript = harness.transcript();
    assert!(
        transcript.iter().any(|line| line.starts_with("log.error(")),
        "the one transient refusal is reported, not swallowed: {transcript:?}"
    );
    assert!(harness.page().published.is_empty());
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
    let mut harness = Harness::echo_stub_mounted();
    harness.page().publish_answer = Some(ports::PublishError::NotPermitted);
    let send = harness.page().marked_child(ROOT, "data-echo-send");
    harness.call_expecting_a_trap(gesture(SEND, send));
    assert!(
        harness.page().published.is_empty(),
        "the refused publish reached nothing"
    );
}

#[test]
fn the_panic_gesture_traps_the_instance() {
    // The fixture's third button exists to exercise the error-card path from a
    // real component; this is that path, executable.
    let mut harness = Harness::echo_stub_mounted();
    let panic_button = harness.page().marked_child(ROOT, "data-echo-panic");
    harness.call_expecting_a_trap(gesture(PANIC, panic_button));
}

#[test]
fn a_gesture_on_a_port_the_component_wired_nothing_to_is_refused_not_trapped() {
    // A sync port the component does not know is a wiring bug, not a memory
    // one: it answers err, keeps running, and the page keeps its instance.
    let mut harness = Harness::echo_stub_mounted();
    let refusal = harness
        .instance
        .call_receive(&mut harness.store, &gesture("no-such-port", ROOT))
        .expect("an unknown sync port does not trap")
        .expect_err("an unknown sync port is refused");
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
    let mut harness = Harness::echo_stub_mounted();
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
    let mut harness = Harness::meeting();
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
    let mut harness = Harness::meeting();
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
    let mut harness = escalated_meeting();
    harness.page().publish_answer = Some(ports::PublishError::QuotaExceeded);
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

#[test]
fn a_press_on_a_port_meeting_wired_nothing_to_is_refused_not_trapped() {
    let mut harness = escalated_meeting();
    let refusal = harness
        .instance
        .call_receive(&mut harness.store, &gesture("no-such-port", ROOT))
        .expect("an unknown sync port does not trap")
        .expect_err("an unknown sync port is refused");
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
    let mut harness = Harness::chrome();
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
    let mut harness = Harness::chrome();
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
