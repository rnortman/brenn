//! A recording page host for surface-placed components, as a library.
//!
//! One instantiation of a built component artifact, driven through scripted
//! activations against a fake element tree, reduced to an ordered transcript of
//! what the component asked the page to do. Everything a component can reach on
//! the page is answered here: `dom`, `page-dom`, `ports`, `log`, `alert` and
//! `config`.
//!
//! This is not a second hosting. The backend host refuses `dom` structurally,
//! and nothing here goes through it: the harness builds its own linker over the
//! same WIT and links exactly the grants the caller names, so an artifact that
//! acquired another import fails at instantiation rather than at boot. Two
//! differences from the backend host are deliberate and are what make the
//! fixture faithful to the page:
//!
//!   - one instantiation drives the whole script, because a page-hosted
//!     instance is instantiated once and lives for the page, and a mounted
//!     kind's view handles are ordinary struct state across activations;
//!   - activations may be sync calls, which is how a mount and a gesture
//!     arrive.
//!
//! The recording host is held to the real vocabulary: the allow-list
//! predicates, the mount port and the gesture body field names all come from
//! `brenn-surface-contract`, so a component that steps outside what the kernel
//! host would admit fails here instead of in a browser.
//!
//! The artifact arrives as a path, so a consumer outside this repository points
//! the harness at whatever its own build produced.

use std::collections::BTreeMap;
use std::path::Path;

use brenn_envelope::grants::ComponentGrant;
use brenn_envelope::testutils::{NOW_MS, envelope};
use brenn_surface_contract::{
    GESTURE_EVENT_FIELD, GESTURE_LISTENER_FIELD, GESTURE_TARGET_FIELD, MOUNT_SYNC_PORT,
    dom_attribute_allowed, dom_tag_allowed,
};
use wasmtime::component::{Component, HasSelf, Linker};
use wasmtime::{Config, Engine, Store};

/// The `brenn:processor` world, generated from the same WIT both the kernel and
/// the guest SDK read.
pub mod bindings {
    wasmtime::component::bindgen!({
        world: "processor",
        path: "processor.wit",
    });
}

pub use bindings::brenn::processor::{alert, config, dom, log, page_dom, ports, types};

/// One element of the fake page.
#[derive(Default)]
pub struct Element {
    pub attributes: BTreeMap<String, String>,
    pub text: Option<String>,
    /// A form control's value, which `dom.value` reads and a test seeds.
    pub value: String,
    pub children: Vec<u64>,
    pub parent: Option<u64>,
}

/// The listener the host owns after a `dom.listen`.
#[derive(Debug, PartialEq, Eq)]
pub struct Listener {
    pub node: u64,
    pub event: String,
    pub port: String,
}

/// One recorded `alert.alert` call.
#[derive(Debug, PartialEq, Eq)]
pub struct Alert {
    pub severity: String,
    pub title: String,
    pub body: String,
}

/// The host half of the fixture: a fake element tree, the calls made against
/// it, and the answers the bus gives.
pub struct Page {
    /// Handle `i + 1` names entry `i`, exactly as the kernel's handle table
    /// numbers them, so a transcript reads against the same identities.
    pub elements: Vec<Element>,
    pub listeners: Vec<Listener>,
    /// Every guest-visible call, in call order.
    pub transcript: Vec<String>,
    /// What the component published and where, in call order.
    pub published: Vec<(String, String)>,
    /// What it parked and where, in call order.
    pub parked: Vec<(String, String, u64)>,
    /// What it alerted on, in call order.
    pub alerts: Vec<Alert>,
    /// What every publish answers. `None` is acceptance.
    pub publish_answer: Option<ports::PublishError>,
    /// The operator config the component reads, fixed for the instance's
    /// lifetime as it is on a real host.
    pub config: BTreeMap<String, String>,
    /// The page beyond this instance's own subtree, which only a `page-dom`
    /// fixture has: the surface root, the body, and the kernel wrapper each
    /// registered instance hangs under.
    pub page_root: Option<u64>,
    pub body: Option<u64>,
    pub instance_wrappers: BTreeMap<String, u64>,
}

/// The instance's host element: the kernel mounts it before the mount
/// activation, so it is handle 1 before the component runs.
pub const ROOT: u64 = 1;

impl Page {
    pub fn new() -> Page {
        Page::with_config(BTreeMap::new())
    }

    /// A page whose `config.get` answers out of `config` and nothing else.
    pub fn with_config(config: BTreeMap<String, String>) -> Page {
        Page {
            elements: vec![Element::default()],
            listeners: Vec::new(),
            transcript: Vec::new(),
            published: Vec::new(),
            parked: Vec::new(),
            alerts: Vec::new(),
            publish_answer: None,
            config,
            page_root: None,
            body: None,
            instance_wrappers: BTreeMap::new(),
        }
    }

    /// The fixture a page-authority holder runs against: this instance's host
    /// element under its own kernel wrapper, that wrapper under the surface
    /// root, the surface root under the body, and one sibling instance already
    /// registered — the shape a chrome component reads its own identity out of.
    ///
    /// Built by hand rather than through the recording calls, so a transcript
    /// starts at what the component did.
    pub fn with_page_authority(own_instance: &str, sibling: &str) -> Page {
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
    pub fn link(&mut self, parent: u64, child: u64) {
        self.element(child).parent = Some(parent);
        self.element(parent).children.push(child);
    }

    /// The wrapper the fixture minted for `instance`.
    pub fn wrapper_of(&self, instance: &str) -> u64 {
        self.instance_wrappers[instance]
    }

    /// The payloads accepted on `port`, in publish order.
    pub fn published_on(&self, port: &str) -> Vec<&str> {
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
    pub fn element(&mut self, handle: u64) -> &mut Element {
        let index = usize::try_from(handle)
            .ok()
            .and_then(|handle| handle.checked_sub(1))
            .unwrap_or_else(|| panic!("dom: handle {handle} names no element"));
        self.elements
            .get_mut(index)
            .unwrap_or_else(|| panic!("dom: handle {handle} names no element"))
    }

    pub fn mint(&mut self) -> u64 {
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
    pub fn children(&mut self, node: u64) -> Vec<u64> {
        self.element(node).children.clone()
    }

    pub fn text_of(&mut self, node: u64) -> String {
        self.element(node).text.clone().unwrap_or_default()
    }

    /// The one descendant of `node` carrying `marker`, for a view whose parts
    /// are not all direct children.
    pub fn marked_descendant(&mut self, node: u64, marker: &str) -> u64 {
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

    /// The one child of `node` carrying `marker`, which is how a component
    /// names each part of its view.
    pub fn marked_child(&mut self, node: u64, marker: &str) -> u64 {
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

impl Default for Page {
    fn default() -> Page {
        Page::new()
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
        port: String,
        payload: String,
        urgency: ports::Urgency,
    ) -> Result<(), ports::PublishError> {
        self.record(format!(
            "ports.publish-with-urgency({port}, {payload:?}, {urgency:?})"
        ));
        match &self.publish_answer {
            None => {
                self.published.push((port, payload));
                Ok(())
            }
            Some(refusal) => Err(refusal.clone()),
        }
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
        port: String,
        index: u32,
        payload: Option<String>,
        deliver_after: Option<u64>,
    ) -> Result<(), ports::DeferError> {
        self.record(format!(
            "ports.defer-edit({port}, {index}, {payload:?}, {deliver_after:?})"
        ));
        Ok(())
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

impl log::Host for Page {
    fn log(&mut self, level: log::Level, message: String) {
        let level = match level {
            log::Level::Trace => "trace",
            log::Level::Debug => "debug",
            log::Level::Info => "info",
            log::Level::Warn => "warn",
            log::Level::Error => "error",
        };
        self.record(format!("log.{level}({message:?})"));
    }
}

impl alert::Host for Page {
    fn alert(&mut self, severity: alert::Severity, title: String, body: String) {
        let severity = match severity {
            alert::Severity::Info => "info",
            alert::Severity::Warning => "warning",
            alert::Severity::Critical => "critical",
        };
        self.record(format!("alert.alert({severity}, {title:?}, {body:?})"));
        self.alerts.push(Alert {
            severity: severity.to_string(),
            title,
            body,
        });
    }
}

impl config::Host for Page {
    fn get(&mut self, key: String) -> Option<String> {
        let value = self.config.get(&key).cloned();
        self.record(format!("config.get({key}) -> {value:?}"));
        value
    }
}

/// One instantiation of the artifact, its recording page, and the way to drive
/// activations at it.
pub struct Harness {
    store: Store<Page>,
    instance: bindings::Processor,
}

impl Harness {
    /// Link exactly the grants named and instantiate once. An artifact that
    /// acquired another import fails here, which is the deny-by-default the
    /// production host has.
    ///
    /// `Takeover` names no WIT interface — it is a binding right, not an import
    /// — so it links nothing and is accepted for the symmetry with a
    /// specification's `requires` list.
    pub fn new(artifact: &Path, page: Page, grants: &[ComponentGrant]) -> Harness {
        let engine = Engine::new(&Config::new()).expect("wasmtime engine");
        let component = Component::from_file(&engine, artifact)
            .unwrap_or_else(|err| panic!("the component artifact {}: {err}", artifact.display()));
        let mut linker: Linker<Page> = Linker::new(&engine);
        for grant in grants {
            match grant {
                ComponentGrant::Ports => {
                    ports::add_to_linker::<_, HasSelf<Page>>(&mut linker, |page| page)
                        .expect("link ports");
                }
                ComponentGrant::Log => {
                    log::add_to_linker::<_, HasSelf<Page>>(&mut linker, |page| page)
                        .expect("link log");
                }
                ComponentGrant::Alert => {
                    alert::add_to_linker::<_, HasSelf<Page>>(&mut linker, |page| page)
                        .expect("link alert");
                }
                ComponentGrant::Config => {
                    config::add_to_linker::<_, HasSelf<Page>>(&mut linker, |page| page)
                        .expect("link config");
                }
                ComponentGrant::Dom => {
                    dom::add_to_linker::<_, HasSelf<Page>>(&mut linker, |page| page)
                        .expect("link dom");
                }
                ComponentGrant::PageDom => {
                    page_dom::add_to_linker::<_, HasSelf<Page>>(&mut linker, |page| page)
                        .expect("link page-dom");
                }
                ComponentGrant::Takeover => {}
                other => panic!(
                    "{other:?} is not a capability a page can satisfy; this harness hosts the \
                     surface profile only"
                ),
            }
        }
        let mut store = Store::new(&engine, page);
        let instance = bindings::Processor::instantiate(&mut store, &component, &linker)
            .expect("the artifact instantiates against the linked profile");
        Harness { store, instance }
    }

    /// Drive the mount activation and discard its transcript, so a script's
    /// assertions start at what happened after the view was built.
    pub fn mount(mut harness: Harness) -> Harness {
        harness.call(mount());
        harness.transcript();
        harness
    }

    /// Drive one activation that answers nothing, which is every activation but
    /// a sync call the component replies to.
    pub fn call(&mut self, activation: types::Activation) {
        let reply = self.call_returning(activation);
        assert_eq!(
            reply, None,
            "this activation answers nothing; use `call_returning` for one that does"
        );
    }

    /// Drive one activation and hand back its reply.
    ///
    /// A sync call may answer — a gesture cancelling the browser's default
    /// action, a mount answering the kernel — and that answer is the only
    /// observable output of an activation that touches neither the page nor a
    /// port, so it is reachable rather than asserted away.
    pub fn call_returning(&mut self, activation: types::Activation) -> Option<String> {
        self.instance
            .call_receive(&mut self.store, &activation)
            .expect("the activation did not trap")
            .expect("the activation was not refused")
    }

    /// Drive one activation expecting the instance to die in it.
    ///
    /// A guest panic reaches the host as an `unreachable` trap and nothing
    /// else: the message the component wrote goes to its own log pipeline, not
    /// into the error. So the assertion available here is the fact of the trap,
    /// which is what makes the instance terminal and earns it an error card.
    pub fn call_expecting_a_trap(&mut self, activation: types::Activation) {
        let error = self
            .instance
            .call_receive(&mut self.store, &activation)
            .expect_err("the activation must trap");
        let error = format!("{error:#}");
        assert!(error.contains("wasm trap"), "{error}");
    }

    /// Drive one activation expecting the component to refuse it and keep
    /// running — a wiring bug rather than a memory one, which costs the page
    /// its activation and not its instance.
    pub fn call_expecting_a_refusal(
        &mut self,
        activation: types::Activation,
    ) -> types::ReceiveError {
        self.instance
            .call_receive(&mut self.store, &activation)
            .expect("a refused activation does not trap")
            .expect_err("the activation must be refused")
    }

    pub fn page(&mut self) -> &mut Page {
        self.store.data_mut()
    }

    /// The calls since the last take, and clear.
    pub fn transcript(&mut self) -> Vec<String> {
        std::mem::take(&mut self.store.data_mut().transcript)
    }
}

/// A sync-call activation on `port`, carrying the one synthesized request the
/// kernel mints for it and nothing else.
pub fn sync_call(port: &str, request: String) -> types::Activation {
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

/// The mount activation, as the runner mints it: the reserved port, an empty
/// synthesized body, and whatever input was already pending — here, none.
pub fn mount() -> types::Activation {
    sync_call(MOUNT_SYNC_PORT, envelope("mount", "{}"))
}

/// A gesture, as the kernel's listener synthesizes it: the event, the listening
/// node and the nearest handle-mapped ancestor of what was hit.
pub fn gesture(port: &str, listener: u64) -> types::Activation {
    gesture_at(port, listener, listener)
}

/// A gesture whose target is not the listening node — a click that landed on a
/// descendant of a delegated listener, or on the gap between them.
pub fn gesture_at(port: &str, listener: u64, target: u64) -> types::Activation {
    let body = serde_json::json!({
        GESTURE_EVENT_FIELD: "click",
        GESTURE_LISTENER_FIELD: listener,
        GESTURE_TARGET_FIELD: target,
    })
    .to_string();
    sync_call(port, envelope("gesture", &body))
}

/// An ordinary delivery on `port`: `context` is what the instance has already
/// seen, `new` is what this activation brings, `dropped` is what the window
/// lost.
pub fn delivery_on(port: &str, context: &[&str], new: &[&str], dropped: u32) -> types::Activation {
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

#[cfg(test)]
mod tests {
    use super::*;
    use bindings::brenn::processor::ports::Host as _;

    /// Urgency is a port-level choice a guest may make on any publish. The
    /// recording host answers it like a plain publish and keeps the urgency in
    /// the transcript, so a test can assert which one was chosen.
    #[test]
    fn publishing_with_urgency_records_and_publishes() {
        let mut page = Page::new();
        page.publish_with_urgency("out".to_string(), "{}".to_string(), ports::Urgency::High)
            .expect("the bus accepts it");
        assert_eq!(page.published_on("out"), vec!["{}"]);
        assert_eq!(
            page.transcript,
            vec![r#"ports.publish-with-urgency(out, "{}", Urgency::High)"#.to_string()],
        );
    }

    /// A refusal from the bus reaches an urgent publish the same way it reaches
    /// a plain one; nothing lands in the publish record.
    #[test]
    fn an_urgent_publish_carries_the_fixtures_refusal() {
        let mut page = Page::new();
        page.publish_answer = Some(ports::PublishError::NotPermitted);
        let answer =
            page.publish_with_urgency("out".to_string(), "{}".to_string(), ports::Urgency::Low);
        assert!(matches!(answer, Err(ports::PublishError::NotPermitted)));
        assert!(page.published.is_empty());
    }

    /// Editing a parked message in place is what a guest does instead of
    /// cancelling and re-parking.
    #[test]
    fn editing_a_parked_message_is_recorded() {
        let mut page = Page::new();
        page.defer_edit(
            "tick".to_string(),
            0,
            Some("{}".to_string()),
            Some(NOW_MS + 1_000),
        )
        .expect("the edit is accepted");
        assert_eq!(
            page.transcript,
            vec![format!(
                r#"ports.defer-edit(tick, 0, Some("{{}}"), Some({}))"#,
                NOW_MS + 1_000
            )],
        );
    }
}
