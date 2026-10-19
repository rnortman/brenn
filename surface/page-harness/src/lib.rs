//! A recording page host for surface-placed components, as a library.
//!
//! A built component artifact, driven through scripted activations against a
//! fake element tree, reduced to an ordered transcript of what the component
//! asked the page to do. Everything a component can reach on
//! the page is answered here: `dom`, `page-dom`, `ports`, `log`, `alert` and
//! `config`.
//!
//! This is not a second hosting. The backend host refuses `dom` structurally,
//! and nothing here goes through it: the harness builds its own linker over the
//! same WIT and links exactly the grants the class's specification declares, so
//! an artifact that acquired another import fails at instantiation rather than
//! at boot. One difference from the backend host is deliberate and is what
//! makes the fixture
//! faithful to the page: activations may be sync calls, which is how a mount and
//! a gesture arrive.
//!
//! [`Kind`] is the compiled artifact and [`Harness`] one mounted instance of
//! it. **A `Harness` instantiates the artifact once per activation**, which is
//! the contract's lifetime and the page's: linear memory lives for one
//! activation, and a conforming kind keeps its state on a retained `io` port
//! rather than in a static. A kind that kept state across calls fails its own
//! suite here, in or out of tree, rather than surviving to a browser.
//!
//! What does survive is the [`Page`]: the element tree, the handle tables and
//! the config are owned by the mount, not by the memory, so a handle minted in
//! one activation names the same element in the next.
//!
//! One channel is simulated rather than scripted: a kind's `io state` port,
//! named through [`Harness::retaining_state`]. State lives there precisely
//! because it cannot live in memory, so a script that did not round-trip it
//! would make every activation after the first look like a first mount.
//!
//! The recording host is held to the real vocabulary: the allow-list
//! predicates, the mount port and the gesture body field names all come from
//! `brenn-surface-contract`, and the grants, the port directions and the sync
//! vocabulary come from the class's own `.brenn` through [`Declared`], so a
//! component — or a script — that steps outside what the kernel host would
//! admit fails here instead of in a browser.
//!
//! The artifact and the specification both arrive as paths, so a consumer
//! outside this repository points the harness at whatever its own build
//! produced.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use brenn_envelope::grants::ComponentGrant;
use brenn_envelope::testutils::{NOW_MS, envelope};
use brenn_surface_contract::{
    MOUNT_SYNC_PORT, dom_attribute_allowed, dom_attribute_value_allowed, dom_tag_allowed,
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

/// What a class declares: the grants the page links and the port vocabulary it
/// holds the guest to.
///
/// A packaged component always has a specification, so [`Self::from_spec`] is
/// the normal route. Hand construction is public for a fixture that has no
/// `.brenn` at all.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Declared {
    /// `requires` ∪ `optional`. Both are linked: only a word with no interface
    /// behind it may be optional, so the distinction is documentary.
    pub grants: BTreeSet<ComponentGrant>,
    /// `in` and `io`: the ports an activation may window.
    pub inbound: BTreeSet<String>,
    /// `out` and `io`: the ports a publish may name.
    pub outbound: BTreeSet<String>,
    pub sync_ports: BTreeSet<String>,
    pub call_ports: BTreeSet<String>,
}

impl Declared {
    /// Parse `spec` and read the vocabulary of its one class, or of the class
    /// `class` names.
    ///
    /// Delegates to `brenn_dsl::scaffold` so that what a grant word means and
    /// what a port direction admits are the compiler's, not this crate's.
    ///
    /// Panics on a parse failure, an unknown grant word or an unresolvable
    /// class name: the specification is the test subject's own file, and a
    /// malformed one is a broken fixture rather than a condition to tolerate.
    // TODO(harness-host-rule-parity): nothing mechanically holds these refusals
    // equal to the ones the production hosts make.
    pub fn from_spec(spec: &Path, class: Option<&str>) -> Declared {
        let filename = spec.display().to_string();
        let file = brenn_dsl::parse_file(spec)
            .unwrap_or_else(|error| panic!("the specification {filename}: {error}"));
        let class = brenn_dsl::scaffold::select_class(&file, class, &filename)
            .unwrap_or_else(|error| panic!("the specification {filename}: {error}"));
        let grants = brenn_dsl::scaffold::declared_grants(class)
            .unwrap_or_else(|error| panic!("the specification {filename}: {error}"));
        let ports = brenn_dsl::scaffold::port_vocabulary(class);
        Declared {
            grants,
            inbound: ports.inbound,
            outbound: ports.outbound,
            sync_ports: ports.sync,
            call_ports: ports.call,
        }
    }
}

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
    /// What a publish on one named port answers, ahead of `publish_answer`.
    ///
    /// A script about one port's refusal — a kind's `io state` port most of all,
    /// whose publish the SDK makes and the component never sees — needs the
    /// component's own outputs to keep working, or the refusal under test is
    /// not the one that happened.
    pub publish_answer_on: BTreeMap<String, ports::PublishError>,
    /// The operator config the component reads, fixed for the instance's
    /// lifetime as it is on a real host.
    pub config: BTreeMap<String, String>,
    /// The page beyond this instance's own subtree, which only a `page-dom`
    /// fixture has: the surface root, the body, and the kernel wrapper each
    /// registered instance hangs under.
    pub page_root: Option<u64>,
    pub body: Option<u64>,
    pub instance_wrappers: BTreeMap<String, u64>,
    /// What `dom.prefers-reduced-motion` answers.
    pub reduced_motion: bool,
    /// What `dom.instance` answers: this instance's name, the one a kind's
    /// suite writes as `holder` in an `overlay-state` body to mean "this
    /// instance".
    pub instance: String,
    /// The `dom.animate` call of the next activation, counted from 1, that the
    /// browser refuses, and the message it refuses it with. Cleared when that
    /// activation ends, whatever it did.
    animate_refusal: Option<(usize, String)>,
    /// How many `dom.animate` calls the current activation has made.
    animate_calls: usize,
    /// The vocabulary this page holds its guest to, stamped by
    /// [`Kind::instantiate`] or by [`Page::declare`].
    declared: Option<Arc<Declared>>,
}

/// The instance's host element: the kernel mounts it before the mount
/// activation, so it is handle 1 before the component runs.
pub const ROOT: u64 = 1;

/// The instance name a page answers `dom.instance` with until told another —
/// the name the envelope fixtures already mint under.
pub const FIXTURE_INSTANCE: &str = "fixture";

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
            publish_answer_on: BTreeMap::new(),
            config,
            page_root: None,
            body: None,
            instance_wrappers: BTreeMap::new(),
            reduced_motion: false,
            instance: FIXTURE_INSTANCE.to_string(),
            animate_refusal: None,
            animate_calls: 0,
            declared: None,
        }
    }

    /// Answer `dom.prefers-reduced-motion` with `reduced` from now on.
    pub fn with_reduced_motion(&mut self, reduced: bool) -> &mut Page {
        self.reduced_motion = reduced;
        self
    }

    /// Answer `dom.instance` with `name` from now on.
    pub fn with_instance(&mut self, name: &str) -> &mut Page {
        self.instance = name.to_string();
        self
    }

    /// Make the `nth` `dom.animate` call (from 1) of the next activation answer
    /// `Refused(message)`, and every other call `Ok` — how a kind's suite drives
    /// the browser refusal the page otherwise owns. Cleared when that activation
    /// ends.
    pub fn with_animate_refusal(&mut self, nth: usize, message: &str) -> &mut Page {
        assert!(nth >= 1, "animate calls are counted from 1");
        self.animate_refusal = Some((nth, message.to_string()));
        self
    }

    /// Start an activation's `dom.animate` count from zero.
    fn begin_activation(&mut self) {
        self.animate_calls = 0;
    }

    /// Drop what was scripted for the activation that just ended.
    fn end_activation(&mut self) {
        self.animate_refusal = None;
        self.animate_calls = 0;
    }

    /// Hold this page to `declared` without going through a [`Kind`]: for a
    /// test that drives the host functions directly.
    ///
    /// Panics if the page already carries a vocabulary, so this route and
    /// [`Kind::instantiate`] cannot silently disagree.
    pub fn declare(&mut self, declared: Declared) {
        self.declare_shared(Arc::new(declared));
    }

    fn declare_shared(&mut self, declared: Arc<Declared>) {
        assert!(
            self.declared.is_none(),
            "this page already carries a declared vocabulary"
        );
        self.declared = Some(declared);
    }

    /// The vocabulary this page holds its guest to.
    fn declared(&self) -> &Declared {
        self.declared.as_deref().unwrap_or_else(|| {
            panic!(
                "this page carries no declared vocabulary; build it through `Kind::instantiate` \
                 or stamp one with `Page::declare`"
            )
        })
    }

    /// The fixture a page-authority holder runs against: this instance's host
    /// element under its own kernel wrapper, that wrapper under the surface
    /// root, the surface root under the body, and one sibling instance already
    /// registered — the shape a chrome component reads its own identity out of.
    /// `dom.instance` answers `own_instance`.
    ///
    /// Built by hand rather than through the recording calls, so a transcript
    /// starts at what the component did.
    pub fn with_page_authority(own_instance: &str, sibling: &str) -> Page {
        let mut page = Page::new();
        page.with_instance(own_instance);
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
        assert!(
            dom_attribute_value_allowed(&name, &value),
            "`{value}` is not a settable `{name}`"
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
        // A listen must name a port the specification declares `sync`.
        assert!(
            self.declared().sync_ports.contains(&port),
            "dom: this instance listens on port {port:?}, which its specification does not \
             declare `sync`"
        );
        self.record(format!("dom.listen({}, {event}, {port})", n(node)));
        self.listeners.push(Listener { node, event, port });
    }

    fn utc_offset_minutes(&mut self, epoch_ms: u64) -> i32 {
        self.record(format!("dom.utc-offset-minutes({epoch_ms})"));
        0
    }

    fn animate(
        &mut self,
        node: u64,
        keyframes: String,
        options: String,
    ) -> Result<(), dom::AnimateError> {
        // The kernel traps on text that is not a JSON array / object; the
        // harness fails the suite on it, which is the same statement here.
        assert!(
            matches!(
                serde_json::from_str::<serde_json::Value>(&keyframes),
                Ok(serde_json::Value::Array(_))
            ),
            "dom: the animate keyframes are not a JSON array: {keyframes}"
        );
        assert!(
            matches!(
                serde_json::from_str::<serde_json::Value>(&options),
                Ok(serde_json::Value::Object(_))
            ),
            "dom: the animate options are not a JSON object: {options}"
        );
        // An unknown handle fails the test here as it traps on the page.
        self.element(node);
        self.record(format!("dom.animate({}, {keyframes}, {options})", n(node)));
        self.animate_calls += 1;
        match &self.animate_refusal {
            Some((nth, message)) if *nth == self.animate_calls => {
                Err(dom::AnimateError::Refused(message.clone()))
            }
            _ => Ok(()),
        }
    }

    fn prefers_reduced_motion(&mut self) -> bool {
        let reduced = self.reduced_motion;
        self.record(format!("dom.prefers-reduced-motion -> {reduced}"));
        reduced
    }

    fn instance(&mut self) -> String {
        let name = self.instance.clone();
        self.record(format!("dom.instance -> {name:?}"));
        name
    }
}

impl Page {
    /// A publish must name a port the specification declares `out` or `io`.
    fn assert_outbound(&self, port: &str) {
        assert!(
            self.declared().outbound.contains(port),
            "ports: this instance publishes on port {port:?}, which its specification does not \
             declare `out` or `io`"
        );
    }

    fn publish_refusal(&self, port: &str) -> Option<ports::PublishError> {
        self.publish_answer_on
            .get(port)
            .or(self.publish_answer.as_ref())
            .cloned()
    }
}

// TODO(harness-flush-on-err): every port call below is recorded as kept; the
// kernel discards a failed activation's buffer, and the harness does not.
impl ports::Host for Page {
    fn publish(&mut self, port: String, payload: String) -> Result<(), ports::PublishError> {
        self.assert_outbound(&port);
        self.record(format!("ports.publish({port}, {payload:?})"));
        match self.publish_refusal(&port) {
            None => {
                self.published.push((port, payload));
                Ok(())
            }
            Some(refusal) => Err(refusal),
        }
    }

    fn publish_with_urgency(
        &mut self,
        port: String,
        payload: String,
        urgency: ports::Urgency,
    ) -> Result<(), ports::PublishError> {
        self.assert_outbound(&port);
        self.record(format!(
            "ports.publish-with-urgency({port}, {payload:?}, {urgency:?})"
        ));
        match self.publish_refusal(&port) {
            None => {
                self.published.push((port, payload));
                Ok(())
            }
            Some(refusal) => Err(refusal),
        }
    }

    fn publish_deferred(
        &mut self,
        port: String,
        payload: String,
        deliver_after: u64,
    ) -> Result<(), ports::PublishError> {
        self.assert_outbound(&port);
        self.record(format!(
            "ports.publish-deferred({port}, {payload:?}, {deliver_after})"
        ));
        match self.publish_refusal(&port) {
            None => {
                self.parked.push((port, payload, deliver_after));
                Ok(())
            }
            Some(refusal) => Err(refusal),
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

/// One mounted instance of the artifact: its recording page, the compiled kind
/// it is instantiated from, and the way to drive activations at it.
///
/// The artifact is instantiated per activation, so nothing a component leaves
/// in its linear memory reaches the next call. The page is the other half of
/// that rule — it is mount-owned and outlives every instance.
pub struct Harness {
    store: Store<Page>,
    kind: Kind,
    /// The instance's retained-state channel, where its kind declares one.
    state: Option<RetainedState>,
}

/// The one channel a script has to carry itself: a kind's `io state` port.
///
/// Linear memory is activation-scoped, so a conforming kind reads its state out
/// of this port's context window and publishes the new one back before it
/// returns. Nothing else in this harness is a channel — every other window is
/// the caller's script — but a state port that did not round-trip would make
/// every activation after the first one look like a first mount.
///
/// Only a successful activation's publish is retained. The page buffers and
/// flushes atomically, and the harness records each call as it is made, so the
/// discard a refusal or a trap earns is applied here.
struct RetainedState {
    port: String,
    body: Option<String>,
}

impl Harness {
    /// Compile `artifact` against the one class `spec` declares and mount one
    /// instance of it over `page`.  Grants, port directions and sync vocabulary
    /// are all read from `spec`.
    pub fn new(artifact: &Path, spec: &Path, page: Page) -> Harness {
        Kind::compile(artifact, Declared::from_spec(spec, None)).instantiate(page)
    }
}

/// One compiled artifact, instantiable as often as a caller asks.
///
/// Compilation is the expensive half and the linked profile is a property of the
/// kind, not of an instance, so both are paid once and every instantiation after
/// that is a fresh linear memory. A [`Harness`] holds its kind because it
/// instantiates from it on every activation; cloning is three `Arc`s.
#[derive(Clone)]
pub struct Kind {
    engine: Engine,
    component: Component,
    linker: Linker<Page>,
    declared: Arc<Declared>,
}

impl Kind {
    /// Compile `artifact` and link exactly the grants `declared` names. An
    /// artifact that acquired another import fails at the first
    /// [`Self::instantiate`], which is the deny-by-default the production host
    /// has.
    ///
    /// `Takeover` names no WIT interface — it is a binding right, not an import
    /// — so it links nothing and is accepted for the symmetry with a
    /// specification's `requires` list.
    pub fn compile(artifact: &Path, declared: Declared) -> Kind {
        let engine = Engine::new(&Config::new()).expect("wasmtime engine");
        let component = Component::from_file(&engine, artifact)
            .unwrap_or_else(|err| panic!("the component artifact {}: {err}", artifact.display()));
        let mut linker: Linker<Page> = Linker::new(&engine);
        for grant in &declared.grants {
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
        Kind {
            engine,
            component,
            linker,
            declared: Arc::new(declared),
        }
    }

    /// Mount one instance of this kind over `page`.
    ///
    /// Nothing is instantiated here: a guest exists only inside an activation,
    /// so the first linear memory is built by the first [`Harness::receive`].
    pub fn instantiate(&self, mut page: Page) -> Harness {
        page.declare_shared(Arc::clone(&self.declared));
        Harness {
            store: Store::new(&self.engine, page),
            kind: self.clone(),
            state: None,
        }
    }
}

impl Harness {
    /// Carry this instance's `io state` port across the activations of a
    /// script: every activation gains the port's window holding the newest body
    /// the instance published on it, as context.
    ///
    /// A kind whose specification declares one needs this whether the script
    /// drives one instance or one per activation — the port is where its state
    /// lives either way.
    pub fn retaining_state(mut self, port: &str) -> Harness {
        let declared = self.store.data().declared();
        assert!(
            declared.inbound.contains(port) && declared.outbound.contains(port),
            "ports: the retained-state port {port:?} is not declared `io` by this specification"
        );
        self.state = Some(RetainedState {
            port: port.to_string(),
            body: None,
        });
        self
    }

    /// Put `body` on the retained-state port as though the instance had
    /// published it, for a script about what a kind does with a state body it
    /// did not write — a version skew across a deploy, most of all.
    pub fn seeding_state(mut self, body: &str) -> Harness {
        self.state
            .as_mut()
            .expect("seed a state port this instance retains")
            .body = Some(body.to_string());
        self
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
        self.receive(&activation)
            .expect("the activation did not trap")
            .expect("the activation was not refused")
    }

    /// Drive one activation and answer what it did, unjudged: the outer `Err` is
    /// a trap, the inner one a refusal, and `Ok(Ok(_))` carries the reply.
    ///
    /// The `call_*` methods above are assertions over this. A host adapter that
    /// must map all three outcomes onto its own vocabulary — a trap is terminal on
    /// one host and quarantined on another — reads them here instead.
    pub fn receive(
        &mut self,
        activation: &types::Activation,
    ) -> wasmtime::Result<Result<Option<String>, types::ReceiveError>> {
        self.assert_declared(activation);
        let activation = self.windowed(activation);
        // A fresh store and a fresh instance per activation: its statics, its
        // heap and its `thread_local!`s start empty, and the store they live in
        // is dropped with them. A store keeps every instance it ever made alive
        // until it is dropped, so instantiating into a long-lived one would hold
        // a script's whole history of linear memories. The page moves across
        // because it belongs to the mount and not to the memory.
        let page = std::mem::take(self.store.data_mut());
        self.store = Store::new(&self.kind.engine, page);
        let instance = bindings::Processor::instantiate(
            &mut self.store,
            &self.kind.component,
            &self.kind.linker,
        )
        .expect("the artifact instantiates against the linked profile");
        // Where this activation's publishes start: `published` accumulates for
        // the life of the harness, and a body an earlier activation published
        // and then discarded by failing must not be retained by a later one.
        let published_from = self.store.data().published.len();
        self.store.data_mut().begin_activation();
        let outcome = instance.call_receive(&mut self.store, &activation);
        self.store.data_mut().end_activation();
        if matches!(outcome, Ok(Ok(_))) {
            self.retain_state(published_from);
        }
        outcome
    }

    /// A scripted activation names only what the class declares: a sync cause
    /// is the reserved mount port or a declared `sync` port, and every other
    /// window is a bound `in` or `io` port.
    ///
    /// The window named by the sync cause is exempt from the inbound rule: a
    /// sync-call activation carries its one synthesized request as a window on
    /// the sync port itself.
    fn assert_declared(&self, activation: &types::Activation) {
        let declared = self.store.data().declared();
        if let Some(cause) = &activation.sync {
            assert!(
                cause == MOUNT_SYNC_PORT || declared.sync_ports.contains(cause),
                "the activation is a sync call on port {cause:?}, which this specification does \
                 not declare `sync`"
            );
        }
        for window in &activation.ports {
            if Some(&window.port) == activation.sync.as_ref() {
                continue;
            }
            assert!(
                declared.inbound.contains(&window.port),
                "the activation windows port {:?}, which this specification does not declare \
                 `in` or `io`",
                window.port
            );
        }
    }

    /// The caller's activation with the retained-state window appended, where
    /// this instance has one.
    fn windowed(&self, activation: &types::Activation) -> types::Activation {
        let Some(state) = &self.state else {
            return activation.clone();
        };
        assert!(
            !activation.ports.iter().any(|w| w.port == state.port),
            "the harness windows {:?} itself; a script must not",
            state.port
        );
        // The WIT records derive `Clone`; a harness that appends a window of
        // its own must not edit the caller's script.
        let mut activation = activation.clone();
        let envelopes: Vec<String> = state
            .body
            .iter()
            .map(|body| envelope("state", body))
            .collect();
        activation.ports.push(types::PortWindow {
            port: state.port.clone(),
            // Wholly context: a sampled port holds no position, so nothing on
            // it is ever new to the instance that wrote it.
            new_from: envelopes.len() as u32,
            envelopes,
            dropped: 0,
        });
        activation
    }

    /// Take the newest body this activation published on the state port.
    ///
    /// `published_from` is where this activation's publishes start. Only they
    /// are eligible: a page discards the whole buffer of an activation that
    /// fails, so a state body an earlier activation published and lost must
    /// stay lost.
    fn retain_state(&mut self, published_from: usize) {
        let Some(port) = self.state.as_ref().map(|s| s.port.clone()) else {
            return;
        };
        let newest = self.store.data().published[published_from..]
            .iter()
            .rev()
            .find(|(p, _)| *p == port)
            .map(|(_, body)| body.clone());
        if let Some(body) = newest {
            self.state
                .as_mut()
                .expect("the port was read from the same field")
                .body = Some(body);
        }
    }

    /// Drive one activation expecting the instance to die in it.
    ///
    /// A guest panic reaches the host as an `unreachable` trap and nothing
    /// else: the message the component wrote goes to its own log pipeline, not
    /// into the error. So the assertion available here is the fact of the trap,
    /// which is what makes the instance terminal and earns it an error card.
    pub fn call_expecting_a_trap(&mut self, activation: types::Activation) {
        let error = self
            .receive(&activation)
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
        self.receive(&activation)
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
    gesture_with(port, "click", listener, target, &GestureDetail::default())
}

/// The gesture body and its parts — the kernel's own types, so a body scripted
/// here is serialized exactly as the page writes one. The harness sends
/// whatever is set, so a suite scripts the body a given event would produce.
pub use brenn_surface_contract::{
    GestureBody, GestureDetail, GestureKey, GestureModifiers, GesturePointer,
};

/// A gesture of any event type, carrying `detail` as the kernel's listener
/// synthesizes it — a tap at a position, a key press — so a kind's suite
/// drives input without a browser. The body carries no `child`, as a host
/// older than that field writes it; [`gesture_body`] scripts one.
pub fn gesture_with(
    port: &str,
    event: &str,
    listener: u64,
    target: u64,
    detail: &GestureDetail,
) -> types::Activation {
    gesture_body(
        port,
        &GestureBody {
            event: event.to_string(),
            listener,
            target,
            child: None,
            detail: detail.clone(),
        },
    )
}

/// A gesture carrying `body` verbatim — every field the kernel's listener
/// writes, `child` among them. The harness derives nothing from its fake tree:
/// the suite scripts the body a hit would produce.
pub fn gesture_body(port: &str, body: &GestureBody) -> types::Activation {
    let body = serde_json::to_string(body).expect("a gesture body serializes");
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

    use bindings::brenn::processor::dom::Host as _;

    fn echo_stub_spec() -> String {
        std::env::var("ECHO_STUB_SPEC").expect("the build names the specification")
    }

    /// echo-stub's vocabulary: the fixture every page-level check below is
    /// driven against.
    fn echo_stub_declared() -> Declared {
        Declared::from_spec(Path::new(&echo_stub_spec()), None)
    }

    fn names(ports: &BTreeSet<String>) -> Vec<&str> {
        ports.iter().map(String::as_str).collect()
    }

    /// Urgency is a port-level choice a guest may make on any publish. The
    /// recording host answers it like a plain publish and keeps the urgency in
    /// the transcript, so a test can assert which one was chosen.
    #[test]
    fn publishing_with_urgency_records_and_publishes() {
        let mut page = Page::new();
        page.declare(echo_stub_declared());
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
        page.declare(echo_stub_declared());
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

    /// Every host function reads the page's vocabulary, so a page nobody
    /// declared has no answer to give; the panic names both ways to stamp one.
    #[test]
    #[should_panic(expected = "Page::declare")]
    fn a_host_call_on_an_undeclared_page_names_both_routes() {
        let mut page = Page::new();
        let _ = page.publish("out".to_string(), "{}".to_string());
    }

    #[test]
    #[should_panic(expected = "does not declare `sync`")]
    fn listening_on_an_undeclared_port_panics() {
        let mut page = Page::new();
        page.declare(echo_stub_declared());
        page.listen(ROOT, "click".to_string(), "no-such-port".to_string());
    }

    #[test]
    fn listening_on_a_declared_sync_port_records_the_listener() {
        let mut page = Page::new();
        page.declare(echo_stub_declared());
        page.listen(ROOT, "click".to_string(), "send".to_string());
        assert_eq!(
            page.listeners,
            vec![Listener {
                node: ROOT,
                event: "click".to_string(),
                port: "send".to_string(),
            }]
        );
    }

    #[test]
    #[should_panic(expected = "does not declare `out` or `io`")]
    fn publishing_on_an_undeclared_port_panics() {
        let mut page = Page::new();
        page.declare(echo_stub_declared());
        let _ = page.publish("no-such-port".to_string(), "{}".to_string());
    }

    /// An `io` port publishes like an `out` one: it is the same half of the
    /// direction.
    #[test]
    fn publishing_on_an_out_or_io_port_is_recorded() {
        let mut page = Page::new();
        page.declare(echo_stub_declared());
        page.publish("out".to_string(), "{}".to_string())
            .expect("the bus accepts it");
        page.publish("state".to_string(), "[]".to_string())
            .expect("the bus accepts it");
        assert_eq!(page.published_on("out"), vec!["{}"]);
        assert_eq!(page.published_on("state"), vec!["[]"]);
    }

    #[test]
    fn from_spec_reads_the_grants_and_every_port_direction() {
        let declared = Declared::from_spec(Path::new(&echo_stub_spec()), None);
        assert_eq!(
            declared.grants,
            BTreeSet::from([
                ComponentGrant::Ports,
                ComponentGrant::Log,
                ComponentGrant::Dom
            ])
        );
        assert_eq!(
            names(&declared.sync_ports),
            ["animate", "panic", "pick", "send", "send-custom"]
        );
        assert_eq!(names(&declared.inbound), ["messages", "state"]);
        assert_eq!(names(&declared.outbound), ["out", "state"]);
        assert!(declared.call_ports.is_empty());
    }

    /// A module with more than one class has no single port surface, so an
    /// unnamed read is `select_class`'s diagnostic rather than a guess.
    #[test]
    #[should_panic(expected = "component classes")]
    fn from_spec_over_a_two_class_module_without_a_name_panics() {
        let spec = std::env::var("TWO_CLASS_SPEC").expect("the build names the fixture");
        Declared::from_spec(Path::new(&spec), None);
    }

    /// Which direction a port is declared in decides which set it lands in, and
    /// `io` lands in two. The one fixture that declares all five states it.
    #[test]
    fn from_spec_separates_the_four_port_vocabularies() {
        let spec = std::env::var("ALL_DIRECTIONS_SPEC").expect("the build names the fixture");
        let declared = Declared::from_spec(Path::new(&spec), None);
        assert_eq!(names(&declared.inbound), ["orders", "state"]);
        assert_eq!(names(&declared.outbound), ["receipts", "state"]);
        assert_eq!(names(&declared.sync_ports), ["press"]);
        assert_eq!(names(&declared.call_ports), ["ask"]);
    }

    /// A specification that does not parse is a broken fixture, and the panic
    /// names the file so its author knows which one.
    #[test]
    #[should_panic(expected = "malformed.brenn")]
    fn from_spec_over_a_malformed_specification_names_the_file() {
        let spec = std::env::var("MALFORMED_SPEC").expect("the build names the fixture");
        Declared::from_spec(Path::new(&spec), None);
    }

    /// So does a grant word no capability answers to — the failure a pin bump
    /// that renamed one produces.
    #[test]
    #[should_panic(expected = "unknown-grant.brenn")]
    fn from_spec_over_an_unknown_grant_word_names_the_file() {
        let spec = std::env::var("UNKNOWN_GRANT_SPEC").expect("the build names the fixture");
        Declared::from_spec(Path::new(&spec), None);
    }

    /// The two stamping routes may not disagree, so the second stamp is a
    /// panic rather than an overwrite.
    #[test]
    #[should_panic(expected = "already carries a declared vocabulary")]
    fn declaring_a_page_twice_panics() {
        let mut page = Page::new();
        page.declare(echo_stub_declared());
        page.declare(echo_stub_declared());
    }

    /// The mixed path, at the call `Kind::instantiate` makes: a page the caller
    /// stamped, handed to a kind that would stamp its own.
    #[test]
    #[should_panic(expected = "already carries a declared vocabulary")]
    fn stamping_a_kinds_vocabulary_over_a_declared_page_panics() {
        let mut page = Page::new();
        page.declare(echo_stub_declared());
        page.declare_shared(Arc::new(echo_stub_declared()));
    }

    /// The urgent and the deferred publish carry the same rule as the plain
    /// one: three doors, one check.
    #[test]
    #[should_panic(expected = "does not declare `out` or `io`")]
    fn publishing_with_urgency_on_an_undeclared_port_panics() {
        let mut page = Page::new();
        page.declare(echo_stub_declared());
        let _ = page.publish_with_urgency(
            "no-such-port".to_string(),
            "{}".to_string(),
            ports::Urgency::High,
        );
    }

    #[test]
    #[should_panic(expected = "does not declare `out` or `io`")]
    fn publishing_deferred_on_an_undeclared_port_panics() {
        let mut page = Page::new();
        page.declare(echo_stub_declared());
        let _ = page.publish_deferred("no-such-port".to_string(), "{}".to_string(), NOW_MS);
    }

    const KEYFRAMES: &str = r#"[{"opacity":0},{"opacity":1}]"#;
    const OPTIONS: &str = r#"{"duration":200}"#;

    fn animate(page: &mut Page) -> Result<(), dom::AnimateError> {
        page.animate(ROOT, KEYFRAMES.to_string(), OPTIONS.to_string())
    }

    #[test]
    fn an_animation_is_recorded_with_its_text_verbatim() {
        let mut page = Page::new();
        assert!(animate(&mut page).is_ok());
        assert_eq!(
            page.transcript,
            vec![format!("dom.animate(n1, {KEYFRAMES}, {OPTIONS})")],
        );
    }

    /// The harness substitutes nothing: options naming `"infinite"` are
    /// accepted and recorded verbatim.
    #[test]
    fn infinite_iterations_text_is_recorded_verbatim() {
        let mut page = Page::new();
        let options = r#"{"duration":200,"iterations":"infinite"}"#;
        assert!(
            page.animate(ROOT, KEYFRAMES.to_string(), options.to_string())
                .is_ok()
        );
        assert_eq!(
            page.transcript,
            vec![format!("dom.animate(n1, {KEYFRAMES}, {options})")],
        );
    }

    /// The synthesized request of a sync-call activation, parsed.
    fn body_sent(activation: &types::Activation) -> serde_json::Value {
        let envelope: serde_json::Value =
            serde_json::from_str(&activation.ports[0].envelopes[0]).expect("envelope JSON");
        let body = envelope["body"]
            .as_str()
            .expect("the envelope carries a body");
        serde_json::from_str(body).expect("the body is JSON")
    }

    /// The detail objects land under the contract's names, and an unset one is
    /// absent rather than null.
    #[test]
    fn a_gesture_with_detail_carries_it_verbatim() {
        let detail = GestureDetail {
            pointer: Some(GesturePointer {
                x: 123.5,
                y: 40.0,
                width: 1920.0,
                height: 720.0,
                button: 0,
                buttons: 1,
                pointer_type: Some("touch".to_string()),
            }),
            key: None,
            modifiers: Some(GestureModifiers {
                shift: true,
                ..GestureModifiers::default()
            }),
        };
        assert_eq!(
            body_sent(&gesture_with("press", "pointerdown", 7, 12, &detail)),
            serde_json::json!({
                "event": "pointerdown", "listener": 7, "target": 12,
                "pointer": {"x": 123.5, "y": 40.0, "width": 1920.0, "height": 720.0,
                            "button": 0, "buttons": 1, "type": "touch"},
                "modifiers": {"alt": false, "ctrl": false, "meta": false, "shift": true},
            })
        );

        let keyed = GestureDetail {
            key: Some(GestureKey {
                key: "ArrowLeft".to_string(),
                code: "ArrowLeft".to_string(),
                repeat: false,
            }),
            ..GestureDetail::default()
        };
        assert_eq!(
            body_sent(&gesture_with("press", "keydown", 7, 7, &keyed)),
            serde_json::json!({
                "event": "keydown", "listener": 7, "target": 7,
                "key": {"key": "ArrowLeft", "code": "ArrowLeft", "repeat": false},
            })
        );
    }

    #[test]
    fn a_scripted_gesture_body_names_its_child_and_omits_it_when_unset() {
        let through = GestureBody {
            event: "click".to_string(),
            listener: 7,
            target: 12,
            child: Some(9),
            detail: GestureDetail::default(),
        };
        assert_eq!(
            body_sent(&gesture_body("press", &through)),
            serde_json::json!({"event": "click", "listener": 7, "target": 12, "child": 9})
        );
        let on_listener = GestureBody {
            target: 7,
            child: None,
            ..through
        };
        assert_eq!(
            body_sent(&gesture_body("press", &on_listener)),
            serde_json::json!({"event": "click", "listener": 7, "target": 7}),
            "no child is no field, not null"
        );
    }

    #[test]
    #[should_panic(expected = "keyframes are not a JSON array")]
    fn animate_keyframes_that_are_not_an_array_panic() {
        let mut page = Page::new();
        let _ = page.animate(ROOT, r#"{"opacity":0}"#.to_string(), OPTIONS.to_string());
    }

    #[test]
    #[should_panic(expected = "keyframes are not a JSON array")]
    fn animate_keyframes_that_do_not_parse_panic() {
        let mut page = Page::new();
        let _ = page.animate(ROOT, "[{".to_string(), OPTIONS.to_string());
    }

    #[test]
    #[should_panic(expected = "options are not a JSON object")]
    fn animate_options_that_are_not_an_object_panic() {
        let mut page = Page::new();
        let _ = page.animate(ROOT, KEYFRAMES.to_string(), "[200]".to_string());
    }

    /// The scripted refusal lands on the named call alone, and does not
    /// outlive the activation it was scripted for.
    #[test]
    fn a_scripted_refusal_answers_the_named_call_and_is_cleared_after() {
        let mut page = Page::new();
        page.with_animate_refusal(2, "boom");
        page.begin_activation();
        assert!(animate(&mut page).is_ok());
        assert!(matches!(
            animate(&mut page),
            Err(dom::AnimateError::Refused(message)) if message == "boom"
        ));
        assert!(animate(&mut page).is_ok());
        page.end_activation();
        page.begin_activation();
        assert!(animate(&mut page).is_ok());
        assert!(animate(&mut page).is_ok());
    }

    #[test]
    fn reduced_motion_answers_what_the_page_was_given() {
        let mut page = Page::new();
        assert!(!page.prefers_reduced_motion());
        page.with_reduced_motion(true);
        assert!(page.prefers_reduced_motion());
        assert_eq!(
            page.transcript,
            vec![
                "dom.prefers-reduced-motion -> false".to_string(),
                "dom.prefers-reduced-motion -> true".to_string(),
            ],
        );
    }

    #[test]
    fn the_instance_answers_its_page_name() {
        let mut page = Page::new();
        assert_eq!(page.instance(), FIXTURE_INSTANCE);
        page.with_instance("motion");
        assert_eq!(page.instance(), "motion");
        assert_eq!(
            page.transcript,
            vec![
                r#"dom.instance -> "fixture""#.to_string(),
                r#"dom.instance -> "motion""#.to_string(),
            ],
        );

        let mut chrome = Page::with_page_authority("chrome", "sibling");
        assert_eq!(chrome.instance(), "chrome");
        assert_eq!(
            chrome.transcript,
            vec![r#"dom.instance -> "chrome""#.to_string()]
        );
    }

    #[test]
    fn from_spec_takes_the_class_it_is_named() {
        let spec = std::env::var("TWO_CLASS_SPEC").expect("the build names the fixture");
        let declared = Declared::from_spec(Path::new(&spec), Some("Second"));
        assert_eq!(names(&declared.outbound), ["out"]);
        assert!(declared.inbound.is_empty());
    }
}
