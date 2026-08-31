//! The kernel's DOM capability host: handle tables, element operations, and the
//! gesture listeners a component asks for. Browser target only.
//!
//! A page-hosted component reaches the DOM through the `brenn:processor/dom`
//! and `brenn:processor/page-dom` imports, which the bootstrap loader shims
//! onto the `brenn_dom_*` free functions in [`crate::entry`]. Those functions
//! carry the instance id from the loader's own closure — never from the
//! component — and every one of them lands here.
//!
//! # Handles
//!
//! A handle is a `u64` index into the calling instance's own [`HandleTable`],
//! canonical per element: the same element is always the same handle within one
//! instance, so a component comparing handles compares elements. Tables are per
//! instance, so a handle another instance minted names nothing here — which is
//! what confines *reach* to the instance's own subtree, since every handle
//! derives from its root or from its own `create-element`.
//!
//! A handle lives until the element it names is destroyed. Two operations
//! destroy elements, and both reclaim: `remove` frees the node's handle and
//! every handle inside its subtree, and `set-text` frees every handle strictly
//! inside the node it clears. A handle held across its reclamation traps,
//! exactly as an unknown one does — the slot may have been reused, and the
//! generation packed into the handle is what catches that. Reclamation matters
//! because the table holds an owned [`Element`] per live handle: without it the
//! kernel, not the component, would be what every node a rendering instance
//! ever built stays alive in. The kernel destroys subtrees of its own — the
//! error card that replaces a faulted instance's contents, and the wrapper
//! clear a mount does — and reclaims across every table there
//! ([`DomHost::reclaim`]) before dropping the destroyed instance's own
//! ([`DomHost::forget`]).
//!
//! # Traps, not error variants
//!
//! An unknown handle, a tag or attribute off the allow-list, or a call from an
//! instance that does not hold the grant is a component bug with no runtime
//! cause, so it is answered with an `Err` the entry throws — a trap, taking the
//! instance terminal with its error card. There is no recoverable arm.
//!
//! # Re-entrancy
//!
//! Every one of these calls happens synchronously inside a component's
//! `receive`, while the runner holds the page borrow across the invocation. So
//! this host is its own cell and touches neither the page nor the entries; the
//! only kernel state it reads is [`KernelCore`]'s grants, through a borrow that
//! is a `let`-temporary. The one thing here that does turn a page — a gesture
//! listener's [`SyncDoor`] request — runs on a browser event's stack, never
//! inside an activation, and the door refuses re-entry on its own.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use brenn_envelope::grants::ComponentGrant;
use js_sys::Reflect;
use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;
use wasm_bindgen::closure::Closure;
use web_sys::{Document, Element, Event, HtmlElement};

use crate::contract::{
    GESTURE_CANCEL_FIELD, GESTURE_EVENT_FIELD, GESTURE_LISTENER_FIELD, GESTURE_TARGET_FIELD,
    SURFACE_ROOT_ID, dom_attribute_allowed, dom_tag_allowed,
};
use crate::dom;
use crate::dom_table::HandleTable;
use crate::front::SurfaceHandle;
use crate::logic::{KernelCore, ungranted_capability};
use crate::sync_door::{SyncAnswer, SyncDoor};

/// Free every handle in `table` naming an element inside `root`, `root` itself
/// included when `with_root`.
///
/// The table asks nothing of the DOM, so containment — the one question only a
/// real tree can answer — is supplied here.
fn free_element_subtree(table: &mut HandleTable<Element>, root: &Element, with_root: bool) {
    table.free_within(root, with_root, |element| {
        root.contains(Some(element.as_ref()))
    });
}

/// The DOM seam a page-hosted instance's imports run through.
///
/// Holds the grants authority, the front door a refusal's breadcrumb is applied
/// through, the sync door a gesture runs its activation on, and one handle table
/// per instance. Published by [`crate::entry::start`] and held for the page's
/// life.
pub struct DomHost {
    core: Rc<RefCell<KernelCore>>,
    handle: Rc<SurfaceHandle>,
    door: Rc<SyncDoor>,
    tables: RefCell<HashMap<String, HandleTable<Element>>>,
}

impl DomHost {
    pub fn new(
        core: Rc<RefCell<KernelCore>>,
        handle: Rc<SurfaceHandle>,
        door: Rc<SyncDoor>,
    ) -> Self {
        Self {
            core,
            handle,
            door,
            tables: RefCell::new(HashMap::new()),
        }
    }

    /// Refuse `instance` unless it holds `grant`, reporting the refusal.
    ///
    /// The breadcrumb is the same drop-and-report every other privileged kernel
    /// entry writes, so an operator reads one vocabulary; the `Err` on top of it
    /// is what makes this family a trap rather than a no-op, because a DOM call
    /// an instance was never granted has no answer it could carry on.
    fn granted(&self, instance: &str, grant: ComponentGrant, what: &str) -> Result<(), String> {
        // A `let`-temporary: the borrow must die before `apply_actions`, which can
        // synchronously re-enter a listener that borrows the core again.
        let granted = self.core.borrow().instance_granted(instance, grant);
        if granted {
            return Ok(());
        }
        let refusal = ungranted_capability(instance, grant, what);
        dom::apply_actions(std::slice::from_ref(&refusal), &self.handle);
        Err(format!(
            "dom: instance {instance} is not granted the {} capability",
            grant.word()
        ))
    }

    /// Run `f` against `instance`'s handle table, creating it on first use.
    ///
    /// The lookup is split from the insert so the common case — a table that
    /// already exists, which is every call after the first — costs no `String`.
    /// This runs on every DOM import an instance makes, including the read-only
    /// ones, so an allocation here is one per node of every render.
    fn table<R>(&self, instance: &str, f: impl FnOnce(&mut HandleTable<Element>) -> R) -> R {
        let mut tables = self.tables.borrow_mut();
        match tables.get_mut(instance) {
            Some(table) => f(table),
            None => f(tables.entry(instance.to_owned()).or_default()),
        }
    }

    /// `instance`'s handle for `element`, minted if new.
    fn handle_for(&self, instance: &str, element: &Element) -> u64 {
        self.table(instance, |table| table.handle_for(element))
    }

    /// The element `handle` names for `instance`, or the trap.
    fn element(&self, instance: &str, handle: u64) -> Result<Element, String> {
        self.table(instance, |table| table.get(handle))
    }

    /// Free every handle — in *every* instance's table — naming an element
    /// inside `root`, `root` itself included when `with_root`.
    ///
    /// The sweep is deliberately not caller-scoped. The point of reclamation is
    /// that the kernel holds no strong reference to a destroyed element, and
    /// that has to hold whoever destroyed it: a `page-dom` holder removing a
    /// subtree containing elements another instance named would otherwise leave
    /// those handles detached-but-live in the victim's table for the page's
    /// life. The victim then traps on its next use of one, which is the right
    /// posture — it cannot render correctly into DOM that no longer exists.
    ///
    /// A sweep costs one `contains` call per live slot of every table, so it is
    /// gated on the one structural fact that decides whether it can free
    /// anything: every handle names an *element*, so a node with no element
    /// children has nothing under it to free. `set-text` on a leaf — one call
    /// per inline node of every rendered message — therefore touches no table at
    /// all, and `remove` of a leaf frees by identity.
    pub fn reclaim(&self, root: &Element, with_root: bool) {
        if root.first_element_child().is_none() {
            if !with_root {
                return;
            }
            for table in self.tables.borrow_mut().values_mut() {
                table.free_exact(root);
            }
            return;
        }
        for table in self.tables.borrow_mut().values_mut() {
            free_element_subtree(table, root, with_root);
        }
    }

    /// Drop `instance`'s whole handle table.
    ///
    /// Called beside [`Self::reclaim`] where the kernel itself destroys an
    /// instance's subtree — the error card that replaces a faulted instance's
    /// contents, and the wrapper clear a mount does. The sweep is what frees
    /// every table's handles to the destroyed elements; this drops what is left
    /// of the destroyed instance's own table, including handles to elements it
    /// created and never attached, because a faulted instance is terminal by
    /// construction and nothing will ever reclaim those the ordinary way.
    pub fn forget(&self, instance: &str) {
        self.tables.borrow_mut().remove(instance);
    }

    /// How many handles `instance` holds live, or `None` if it has no table at
    /// all. Test-facing: the difference between the two is what a teardown is.
    #[cfg(test)]
    pub fn live_handles(&self, instance: &str) -> Option<usize> {
        self.tables.borrow().get(instance).map(HandleTable::live)
    }

    // ── the `dom` interface ─────────────────────────────────────────────────

    /// `dom.root`: the instance's own host element.
    pub fn root(&self, instance: &str) -> Result<u64, String> {
        self.granted(instance, ComponentGrant::Dom, "dom.root")?;
        let element = dom::mounted_element(instance)
            .ok_or_else(|| format!("dom: instance {instance} has no mounted element"))?;
        Ok(self.handle_for(instance, &element))
    }

    /// `dom.create-element`: a detached element of an allowed tag.
    pub fn create_element(&self, instance: &str, tag: &str) -> Result<u64, String> {
        self.granted(instance, ComponentGrant::Dom, "dom.create-element")?;
        if !dom_tag_allowed(tag) {
            return Err(format!("dom: `{tag}` is not a creatable tag"));
        }
        let element = document()
            .create_element(tag)
            .map_err(|_| format!("dom: the document refused to create `{tag}`"))?;
        Ok(self.table(instance, |table| table.mint(&element)))
    }

    /// `dom.set-attribute`: an allowed name, and a value the host never reads.
    pub fn set_attribute(
        &self,
        instance: &str,
        node: u64,
        name: &str,
        value: &str,
    ) -> Result<(), String> {
        self.granted(instance, ComponentGrant::Dom, "dom.set-attribute")?;
        if !dom_attribute_allowed(name) {
            return Err(format!("dom: `{name}` is not a settable attribute"));
        }
        self.element(instance, node)?
            .set_attribute(name, value)
            .map_err(|_| format!("dom: setting `{name}` was refused"))
    }

    /// `dom.remove-attribute`. Held to the same allow-list as the setter: an
    /// instance that cannot set a name has no business naming it at all, and a
    /// removal of a name it could not have written would be a reach at whatever
    /// the kernel or another layer put there.
    pub fn remove_attribute(&self, instance: &str, node: u64, name: &str) -> Result<(), String> {
        self.granted(instance, ComponentGrant::Dom, "dom.remove-attribute")?;
        if !dom_attribute_allowed(name) {
            return Err(format!("dom: `{name}` is not a settable attribute"));
        }
        self.element(instance, node)?
            .remove_attribute(name)
            .map_err(|_| format!("dom: removing `{name}` was refused"))
    }

    /// `dom.set-text`: replace the children with one inert text node.
    ///
    /// The children are destroyed, so every handle strictly inside the node is
    /// reclaimed first — while the tree still says what is inside. The node
    /// itself survives, handle included.
    pub fn set_text(&self, instance: &str, node: u64, text: &str) -> Result<(), String> {
        self.granted(instance, ComponentGrant::Dom, "dom.set-text")?;
        let element = self.element(instance, node)?;
        self.reclaim(&element, false);
        element.set_text_content(Some(text));
        Ok(())
    }

    /// `dom.set-style-property`. The property name is unconstrained: painting is
    /// confined by the wrapper's own containment rather than by inspecting
    /// declarations.
    pub fn set_style_property(
        &self,
        instance: &str,
        node: u64,
        name: &str,
        value: &str,
    ) -> Result<(), String> {
        self.granted(instance, ComponentGrant::Dom, "dom.set-style-property")?;
        style(&self.element(instance, node)?)?
            .set_property(name, value)
            .map_err(|_| format!("dom: setting the `{name}` style property was refused"))
    }

    /// `dom.remove-style-property`.
    pub fn remove_style_property(
        &self,
        instance: &str,
        node: u64,
        name: &str,
    ) -> Result<(), String> {
        self.granted(instance, ComponentGrant::Dom, "dom.remove-style-property")?;
        style(&self.element(instance, node)?)?
            .remove_property(name)
            .map(|_| ())
            .map_err(|_| format!("dom: removing the `{name}` style property was refused"))
    }

    /// `dom.append`.
    pub fn append(&self, instance: &str, parent: u64, child: u64) -> Result<(), String> {
        self.granted(instance, ComponentGrant::Dom, "dom.append")?;
        let parent = self.element(instance, parent)?;
        let child = self.element(instance, child)?;
        parent
            .append_child(&child)
            .map(|_| ())
            .map_err(|_| "dom: the append was refused".to_string())
    }

    /// `dom.insert-before`, appending when `reference` is absent.
    pub fn insert_before(
        &self,
        instance: &str,
        parent: u64,
        child: u64,
        reference: Option<u64>,
    ) -> Result<(), String> {
        self.granted(instance, ComponentGrant::Dom, "dom.insert-before")?;
        let parent = self.element(instance, parent)?;
        let child = self.element(instance, child)?;
        let reference = match reference {
            Some(reference) => Some(self.element(instance, reference)?),
            None => None,
        };
        parent
            .insert_before(&child, reference.as_ref().map(|e| e.as_ref()))
            .map(|_| ())
            .map_err(|_| "dom: the insert was refused".to_string())
    }

    /// `dom.remove`: destroy the node and its subtree.
    ///
    /// Detaching is how the DOM half is done; the handle half is that this node
    /// and everything under it stop being nameable, in every table. `remove`
    /// means destroy — detach-to-hide is what the `hidden` attribute is for.
    pub fn remove(&self, instance: &str, node: u64) -> Result<(), String> {
        self.granted(instance, ComponentGrant::Dom, "dom.remove")?;
        let element = self.element(instance, node)?;
        self.reclaim(&element, true);
        element.remove();
        Ok(())
    }

    /// `dom.value`: a form control's current value, the empty string for a node
    /// that has none.
    ///
    /// Read as the `value` JS property rather than through a per-control cast:
    /// input, textarea and select each spell it, and the contract's answer for
    /// everything else is the empty string, which is exactly what a missing
    /// property reads as.
    pub fn value(&self, instance: &str, node: u64) -> Result<String, String> {
        self.granted(instance, ComponentGrant::Dom, "dom.value")?;
        let element = self.element(instance, node)?;
        Ok(
            Reflect::get(element.as_ref(), &JsValue::from_str(VALUE_PROPERTY))
                .ok()
                .and_then(|value| value.as_string())
                .unwrap_or_default(),
        )
    }

    /// `dom.set-value`, written as the same property the reader reads.
    pub fn set_value(&self, instance: &str, node: u64, value: &str) -> Result<(), String> {
        self.granted(instance, ComponentGrant::Dom, "dom.set-value")?;
        let element = self.element(instance, node)?;
        Reflect::set(
            element.as_ref(),
            &JsValue::from_str(VALUE_PROPERTY),
            &JsValue::from_str(value),
        )
        .map(|_| ())
        .map_err(|_| "dom: the value write was refused".to_string())
    }

    /// `dom.utc-offset-minutes`: the page environment's UTC offset at `epoch_ms`.
    ///
    /// The one non-DOM entry of the interface, and the one page fact a component
    /// cannot compute from its activation clock. JS reports the offset with the
    /// opposite sign to the one everybody means by "UTC+2", so it is negated
    /// here — the guest gets minutes *east* of UTC.
    pub fn utc_offset_minutes(&self, instance: &str, epoch_ms: u64) -> Result<i32, String> {
        self.granted(instance, ComponentGrant::Dom, "dom.utc-offset-minutes")?;
        let date = js_sys::Date::new(&JsValue::from_f64(epoch_ms as f64));
        Ok(-(date.get_timezone_offset() as i32))
    }

    /// `dom.listen`: attach a kernel-owned listener that answers the event with a
    /// sync-call activation on `port`.
    ///
    /// A listener dies with the element it is bound to: an element is reclaimed
    /// only when it is detached, and a detached element receives no UI events,
    /// so a listener inside a removed or cleared subtree is permanently inert.
    /// The closure is `forget`ed either way — one leaked slot per abandoned
    /// listener, which is not the steady state of any rendering pattern.
    ///
    /// Nothing about the activation happens here. The listener runs later, on the
    /// browser's own event stack, which is the whole reason the sync door exists.
    pub fn listen(
        self: &Rc<Self>,
        instance: &str,
        node: u64,
        event: &str,
        port: &str,
    ) -> Result<(), String> {
        self.granted(instance, ComponentGrant::Dom, "dom.listen")?;
        let element = self.element(instance, node)?;
        let host = Rc::clone(self);
        let instance = instance.to_string();
        let event_type = event.to_string();
        let port = port.to_string();
        let listener = Closure::<dyn Fn(Event)>::new(move |fired: Event| {
            host.on_gesture(&instance, node, &event_type, &port, &fired);
        });
        element
            .add_event_listener_with_callback(event, listener.as_ref().unchecked_ref())
            .map_err(|_| format!("dom: the `{event}` listener was refused"))?;
        listener.forget();
        Ok(())
    }

    /// Run one gesture: synthesize the request body, drive the sync door on the
    /// event's own stack, and honour the reply's one field.
    ///
    /// The body names the event, the node whose listener fired, and the nearest
    /// handle-mapped ancestor of the event's target — which is how a delegated
    /// listener on a container tells apart which child was hit. A target that
    /// maps to nothing (the event fired on a node this instance never named) is
    /// reported as the listening node itself, since that is the nearest thing the
    /// component can be told about.
    fn on_gesture(
        &self,
        instance: &str,
        listener: u64,
        event_type: &str,
        port: &str,
        fired: &Event,
    ) {
        let target = fired
            .target()
            .and_then(|target| target.dyn_into::<Element>().ok())
            .and_then(|target| self.nearest_mapped(instance, target))
            .unwrap_or(listener);
        let body = serde_json::json!({
            GESTURE_EVENT_FIELD: event_type,
            GESTURE_LISTENER_FIELD: listener,
            GESTURE_TARGET_FIELD: target,
        })
        .to_string();
        match self.door.request(instance, port, body) {
            SyncAnswer::Ok(Some(reply)) if cancels(&reply) => fired.prevent_default(),
            // Anything else lets the event proceed: no reply, a reply that did not
            // ask, an err, a trap, or a refusal. Cancelling a user's gesture is an
            // act, and only an answer that asked for it gets one.
            _ => {}
        }
    }

    /// The nearest ancestor-or-self of `element` this instance holds a handle
    /// for, walking up through parents.
    fn nearest_mapped(&self, instance: &str, element: Element) -> Option<u64> {
        // One borrow for the whole walk: the ancestor chain is walked on every
        // gesture, and the table cannot change under it — nothing here mints.
        self.table(instance, |table| {
            let mut current = Some(element);
            while let Some(node) = current {
                if let Some(handle) = table.lookup(&node) {
                    return Some(handle);
                }
                current = node.parent_element();
            }
            None
        })
    }

    // ── the `page-dom` interface ────────────────────────────────────────────

    /// `page-dom.page-root`: the surface root, which holds every wrapper.
    pub fn page_root(&self, instance: &str) -> Result<u64, String> {
        self.granted(instance, ComponentGrant::PageDom, "page-dom.page-root")?;
        let root = document()
            .get_element_by_id(SURFACE_ROOT_ID)
            .ok_or_else(|| "page-dom: the page has no surface root".to_string())?;
        Ok(self.handle_for(instance, &root))
    }

    /// `page-dom.page-body`: the document body, where page-level state is
    /// stamped.
    pub fn page_body(&self, instance: &str) -> Result<u64, String> {
        self.granted(instance, ComponentGrant::PageDom, "page-dom.page-body")?;
        let body: Element = document()
            .body()
            .ok_or_else(|| "page-dom: the document has no body".to_string())?
            .into();
        Ok(self.handle_for(instance, &body))
    }

    /// `page-dom.instance-wrapper`: another instance's kernel-owned wrapper, or
    /// `None` for one that has not registered yet — the ordinary transient of a
    /// page still coming up, not an error.
    pub fn instance_wrapper(&self, instance: &str, of: &str) -> Result<Option<u64>, String> {
        self.granted(
            instance,
            ComponentGrant::PageDom,
            "page-dom.instance-wrapper",
        )?;
        Ok(dom::wrapper_element(of).map(|wrapper| self.handle_for(instance, &wrapper)))
    }

    /// `page-dom.parent`: the node's parent, or `None` when it is detached or is
    /// the document root. Minting a handle for the parent is what makes the
    /// answer comparable: one handle per element means comparing handles compares
    /// elements.
    pub fn parent(&self, instance: &str, node: u64) -> Result<Option<u64>, String> {
        self.granted(instance, ComponentGrant::PageDom, "page-dom.parent")?;
        let element = self.element(instance, node)?;
        Ok(element
            .parent_element()
            .map(|parent| self.handle_for(instance, &parent)))
    }
}

/// The JS property both halves of the form-control seam name.
const VALUE_PROPERTY: &str = "value";

/// The page document. A missing one is not a page, so this panics rather than
/// growing an absence arm every caller would have to carry.
fn document() -> Document {
    web_sys::window()
        .expect("surface kernel: no window")
        .document()
        .expect("surface kernel: no document")
}

/// An element's inline style declaration.
fn style(element: &Element) -> Result<web_sys::CssStyleDeclaration, String> {
    element
        .dyn_ref::<HtmlElement>()
        .map(|element| element.style())
        .ok_or_else(|| "dom: this node has no inline style".to_string())
}

/// Whether a gesture reply asks for `preventDefault`.
///
/// The dialect is exactly one boolean field. A reply outside it — an empty
/// object, a non-boolean, unparsable text — is a component talking to itself in
/// two languages, and it does not get to cancel a user's gesture on the strength
/// of that.
fn cancels(reply: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(reply)
        .ok()
        .and_then(|value| value.get(GESTURE_CANCEL_FIELD)?.as_bool())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    fn element(tag: &str) -> Element {
        document().create_element(tag).expect("create an element")
    }

    /// A parent with `children` fresh children appended, all attached to a
    /// detached root so `contains` has a real tree to answer over.
    fn tree(children: usize) -> (Element, Vec<Element>) {
        let parent = element("div");
        let kids: Vec<Element> = (0..children)
            .map(|_| {
                let kid = element("span");
                parent.append_child(&kid).expect("append a child");
                kid
            })
            .collect();
        (parent, kids)
    }

    // The table's own bookkeeping — slot reuse, generations, the mint/trim
    // baseline — is tested natively in `crate::dom_table`. What is left here is
    // the half only a real tree answers: element identity across clones, and
    // `contains` deciding what a destruction takes with it.

    #[wasm_bindgen_test]
    fn a_handle_is_canonical_per_element_and_is_never_zero() {
        let mut table = HandleTable::default();
        let a = element("div");
        let b = element("div");
        let first = table.handle_for(&a);
        assert_ne!(first, 0, "zero is never a valid handle");
        assert_eq!(
            table.handle_for(&a),
            first,
            "the same element, the same handle"
        );
        assert_ne!(table.handle_for(&b), first, "two elements, two handles");
        assert_eq!(table.live(), 2);
        assert_eq!(table.get(first).expect("the element is there"), a);
    }

    #[wasm_bindgen_test]
    fn removing_a_subtree_frees_the_root_and_everything_under_it() {
        let mut table = HandleTable::default();
        let (parent, kids) = tree(3);
        let parent_handle = table.handle_for(&parent);
        let kid_handles: Vec<u64> = kids.iter().map(|kid| table.handle_for(kid)).collect();
        let bystander = element("div");
        let bystander_handle = table.handle_for(&bystander);

        free_element_subtree(&mut table, &parent, true);

        assert!(table.get(parent_handle).is_err(), "the root is gone");
        for handle in kid_handles {
            assert!(table.get(handle).is_err(), "a descendant is gone");
        }
        assert!(
            table.get(bystander_handle).is_ok(),
            "an element outside the subtree is untouched"
        );
        assert_eq!(table.live(), 1);
    }

    #[wasm_bindgen_test]
    fn clearing_the_text_frees_the_children_and_spares_the_parent() {
        let mut table = HandleTable::default();
        let (parent, kids) = tree(2);
        let parent_handle = table.handle_for(&parent);
        let kid_handles: Vec<u64> = kids.iter().map(|kid| table.handle_for(kid)).collect();

        free_element_subtree(&mut table, &parent, false);

        assert!(
            table.get(parent_handle).is_ok(),
            "the node whose text was set survives"
        );
        for handle in kid_handles {
            assert!(table.get(handle).is_err(), "a replaced child is gone");
        }
        assert_eq!(table.live(), 1);
    }

    #[wasm_bindgen_test]
    fn a_moved_node_keeps_its_handle() {
        let mut table = HandleTable::default();
        let (first_parent, kids) = tree(1);
        let kid = kids[0].clone();
        let handle = table.handle_for(&kid);

        let second_parent = element("div");
        second_parent.append_child(&kid).expect("reparent");

        assert_eq!(
            table.get(handle).expect("still named"),
            kid,
            "reparenting is not destruction"
        );
        assert!(
            !first_parent.contains(Some(kid.as_ref())),
            "the move really happened"
        );
    }

    #[wasm_bindgen_test]
    fn the_reply_dialect_cancels_on_exactly_one_shape() {
        assert!(cancels(r#"{"cancel":true}"#));
        assert!(!cancels(r#"{"cancel":false}"#));
        assert!(!cancels("{}"), "an empty object has not decided");
        assert!(
            !cancels(r#"{"cancel":"true"}"#),
            "a string is not the boolean"
        );
        assert!(!cancels("not json"));
    }

    // ── the host, end to end ───────────────────────────────────────────────

    use std::collections::HashMap;

    use brenn_attach_client::Millis;
    use brenn_surface_contract::Activation as HostActivation;
    use futures_channel::mpsc;
    use uuid::Uuid;

    use crate::activation::ActivationOutcome;
    use brenn_surface_schema::LogLevel;

    use crate::front::{self, EFFECTS_CHANNEL_CAPACITY, FrontChannels, PublishSlot};
    use crate::runner::{SharedEntries, SharedPage};
    use crate::session::Event as SessionEvent;
    use crate::test_support::{bindings as fixtures, pages};
    use crate::wasm_test_util::fresh_root;

    const CONFIG: &str = "ephemeral:site.surface.dom-host.bindings";
    const ERRORS: &str = "brenn:site.surface.dom-host.errors";
    const EPOCH: Uuid = Uuid::from_u128(0x1_d0_11);
    const NOW: Millis = Millis(1_000);

    /// A live host over a real core, front door and sync door — everything a DOM
    /// import call travels through except the loader shim above it.
    ///
    /// Assembling it here rather than reaching for the kernel's own bring-up keeps
    /// the subject to one thing: these tests are about what the host does with a
    /// call, not about how a page comes up.
    struct Rig {
        host: Rc<DomHost>,
        /// What the recording entry was called with, oldest first.
        seen: Rc<RefCell<Vec<HostActivation>>>,
        /// What the recording entry answers. `None` answers nothing.
        reply: Rc<RefCell<Option<String>>>,
        channels: FrontChannels,
        /// The door's entry table, so a test can withdraw an instance — and so
        /// a test can hold it borrowed, as an in-flight activation does.
        entries: SharedEntries,
        /// The page the door turns. Held so a test can borrow it the way the
        /// runner borrows it across an invocation.
        page: SharedPage,
        /// Held: the door panics if the effects of a turn have nowhere to go.
        _effects: mpsc::Receiver<Vec<crate::session::Effect>>,
        /// Held: dropping it is how the platform half says it has gone away.
        _events: front::EventStream,
    }

    impl Rig {
        /// The report the host wrote through the front door, if any.
        fn report(&mut self) -> Option<String> {
            match self.channels.publish_rx.try_recv() {
                Ok(PublishSlot::Report(report)) => Some(report.message),
                _ => None,
            }
        }
    }

    /// Build a rig whose instances hold exactly the grants named, each mounted
    /// with a host element of its own so `dom.root` resolves.
    fn rig(instances: &[(&str, &[&str])]) -> Rig {
        fresh_root();
        let mut components: Vec<_> = instances
            .iter()
            .map(|(instance, grants)| fixtures::component_with_grants(instance, instance, grants))
            .collect();
        components.push(fixtures::component(fixtures::CHROME));
        let mut doc = fixtures::doc(components, vec![], vec![], vec![]);
        // A surface with no error channel drops every report at the front door,
        // and a refusal's breadcrumb is one of the things under test here.
        doc.platform.error_channel = Some(ERRORS.to_string());
        doc.platform.error_report_floor = Some(LogLevel::Warn);

        let core = Rc::new(RefCell::new(KernelCore::new()));
        core.borrow_mut().on_event(&SessionEvent::Connected {
            bindings: doc.clone(),
            participant_id: pages::PRINCIPAL.to_string(),
            session_id: pages::SESSION_ID.to_string(),
            max_body_bytes: pages::BODY_CAP,
            alert_granted: false,
        });

        let mut names: Vec<&str> = instances.iter().map(|(instance, _)| *instance).collect();
        names.push(fixtures::CHROME);
        let page: SharedPage = Rc::new(RefCell::new(pages::configured_page(
            CONFIG,
            EPOCH,
            pages::facts(),
            &names,
            &doc,
            NOW,
        )));

        let seen: Rc<RefCell<Vec<HostActivation>>> = Rc::new(RefCell::new(Vec::new()));
        let reply: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(None));
        let entries: SharedEntries = Rc::new(RefCell::new(HashMap::new()));
        for (instance, _) in instances {
            let recorded = Rc::clone(&seen);
            let answer = Rc::clone(&reply);
            entries.borrow_mut().insert(
                (*instance).to_string(),
                Box::new(move |activation: &HostActivation| {
                    recorded.borrow_mut().push(activation.clone());
                    ActivationOutcome::Ok(answer.borrow().clone())
                }),
            );
            dom::mount_host(instance, instance);
        }

        let (handle, events, channels) = front::new();
        channels
            .gate
            .lock()
            .expect("the gate mutex is fresh")
            .refresh(&page.borrow());
        let (effects_tx, effects_rx) = mpsc::channel(EFFECTS_CHANNEL_CAPACITY);
        let door = Rc::new(SyncDoor::new(
            Rc::clone(&page),
            Rc::clone(&entries),
            channels.in_flight.clone(),
            effects_tx,
        ));
        let host = Rc::new(DomHost::new(core, Rc::new(handle), door));
        // The effect executor destroys subtrees through free functions, so the
        // teardown it performs reaches a table only through the published cell.
        crate::entry::publish_dom_host(Rc::clone(&host));
        Rig {
            host,
            seen,
            reply,
            channels,
            entries,
            page,
            _effects: effects_rx,
            _events: events,
        }
    }

    /// The one instance every single-instance test uses, granted `dom`.
    fn dom_rig() -> Rig {
        rig(&[("wbt-dom", &["dom"])])
    }

    const DOM: &str = "wbt-dom";

    #[wasm_bindgen_test]
    fn create_element_admits_the_allow_list_and_traps_on_everything_else() {
        // The wiring, not the predicate: `surface/contract` proves what the lists
        // say, and this proves the host asks them.
        let rig = dom_rig();
        for tag in ["div", "span", "button", "input", "h3", "li"] {
            assert!(
                rig.host.create_element(DOM, tag).is_ok(),
                "{tag} is on the list"
            );
        }
        for tag in [
            "script", "iframe", "style", "link", "meta", "base", "a", "form", "table", "DIV", "",
        ] {
            assert!(
                rig.host.create_element(DOM, tag).is_err(),
                "{tag:?} is not creatable"
            );
        }
    }

    #[wasm_bindgen_test]
    fn both_attribute_entries_are_held_to_the_same_allow_list() {
        let rig = dom_rig();
        let node = rig.host.create_element(DOM, "div").expect("an allowed tag");
        let element = rig.host.element(DOM, node).expect("a live handle");
        for name in ["data-echo-status", "aria-label", "class", "hidden", "type"] {
            rig.host
                .set_attribute(DOM, node, name, "v")
                .unwrap_or_else(|err| panic!("{name} is settable: {err}"));
            assert_eq!(
                element.get_attribute(name).as_deref(),
                Some("v"),
                "the value reaches the element unread — the host inspects names,                  never values"
            );
            rig.host
                .remove_attribute(DOM, node, name)
                .unwrap_or_else(|err| panic!("{name} is removable: {err}"));
            assert_eq!(element.get_attribute(name), None);
        }
        // The names a deny-list would have had to enumerate, refused by one rule —
        // and `id`, excluded because it is the page-global namespace chrome
        // resolves through.
        for name in [
            "onclick",
            "srcdoc",
            "href",
            "src",
            "id",
            "style",
            "http-equiv",
            "xdata-",
            "",
        ] {
            assert!(
                rig.host.set_attribute(DOM, node, name, "v").is_err(),
                "{name:?} is not settable"
            );
            assert!(
                rig.host.remove_attribute(DOM, node, name).is_err(),
                "{name:?} is not removable either — a removal it could not have \
                 written is a reach at what another layer put there"
            );
        }
    }

    #[wasm_bindgen_test]
    fn the_mutators_write_what_they_name_and_the_tree_ops_order_children() {
        let rig = dom_rig();
        let root = rig.host.root(DOM).expect("a mounted instance has a root");
        let a = rig.host.create_element(DOM, "span").expect("create");
        let b = rig.host.create_element(DOM, "span").expect("create");
        let c = rig.host.create_element(DOM, "span").expect("create");

        rig.host.set_text(DOM, a, "first").expect("set text");
        rig.host.append(DOM, root, a).expect("append");
        rig.host.append(DOM, root, c).expect("append");
        rig.host
            .insert_before(DOM, root, b, Some(c))
            .expect("insert before");

        let element = dom::mounted_element(DOM).expect("the host element");
        assert_eq!(
            child_text(&element),
            vec!["first".to_string(), String::new(), String::new()],
            "`b` went in ahead of its reference, not at the end"
        );

        // `remove` destroys, so `a` is gone from the tree and from the table.
        rig.host.remove(DOM, a).expect("remove");
        assert_eq!(element.child_element_count(), 2);
        assert!(
            rig.host.set_text(DOM, a, "still mine").is_err(),
            "a destroyed node's handle names nothing"
        );

        // `insert-before` with no reference appends.
        let d = rig.host.create_element(DOM, "span").expect("create");
        rig.host.insert_before(DOM, root, d, None).expect("append");
        assert_eq!(element.child_element_count(), 3);

        // Style and value round-trip through the same handles.
        rig.host
            .set_style_property(DOM, d, "color", "rgb(1, 2, 3)")
            .expect("set style");
        rig.host
            .remove_style_property(DOM, d, "color")
            .expect("remove style");
        let field = rig.host.create_element(DOM, "input").expect("create");
        rig.host.set_value(DOM, field, "typed").expect("set value");
        assert_eq!(rig.host.value(DOM, field).as_deref(), Ok("typed"));
        assert_eq!(
            rig.host.value(DOM, d).as_deref(),
            Ok(""),
            "a node with no value property reads as the empty string"
        );
    }

    #[wasm_bindgen_test]
    fn a_handle_names_nothing_in_another_instances_table() {
        let mut rig = rig(&[("wbt-mine", &["dom"]), ("wbt-yours", &["dom"])]);
        let mine = rig
            .host
            .create_element("wbt-mine", "div")
            .expect("an allowed tag");
        assert!(
            rig.host.set_text("wbt-yours", mine, "hijacked").is_err(),
            "the neighbour's handle names nothing here"
        );
        assert!(
            rig.host.remove("wbt-yours", mine).is_err(),
            "and cannot be detached either"
        );
        assert_eq!(
            rig.report(),
            None,
            "a foreign handle is a trap, not a refusal"
        );
    }

    #[wasm_bindgen_test]
    fn remove_and_set_text_reclaim_what_they_destroy() {
        let rig = dom_rig();
        let root = rig.host.root(DOM).expect("a mounted instance has a root");
        let entry = rig.host.create_element(DOM, "div").expect("create");
        let label = rig.host.create_element(DOM, "span").expect("create");
        let survivor = rig.host.create_element(DOM, "div").expect("create");
        rig.host.append(DOM, entry, label).expect("append");
        rig.host.append(DOM, root, entry).expect("append");
        rig.host.append(DOM, root, survivor).expect("append");

        rig.host.remove(DOM, entry).expect("remove the entry");
        assert!(
            rig.host.set_text(DOM, entry, "x").is_err(),
            "a removed node's handle traps"
        );
        assert!(
            rig.host.set_text(DOM, label, "x").is_err(),
            "and so does a handle to something inside the removed subtree"
        );
        assert!(
            rig.host.set_text(DOM, survivor, "x").is_ok(),
            "a sibling outside the subtree is untouched"
        );

        // `set-text` destroys the children and spares the node.
        let child = rig.host.create_element(DOM, "span").expect("create");
        rig.host.append(DOM, survivor, child).expect("append");
        rig.host
            .set_text(DOM, survivor, "cleared")
            .expect("clear the survivor");
        assert!(
            rig.host.set_text(DOM, child, "x").is_err(),
            "a replaced child's handle traps"
        );
        assert_eq!(
            rig.host
                .element(DOM, survivor)
                .expect("the cleared node survives")
                .text_content()
                .as_deref(),
            Some("cleared")
        );
    }

    #[wasm_bindgen_test]
    fn removing_a_leaf_reclaims_it_and_touches_nothing_else() {
        // A node with no element children cannot contain a handle, so the sweep
        // is skipped for the whole page and the one slot is freed by identity.
        // Every `set-text` of a rendered inline node takes the same road.
        let rig = dom_rig();
        let root = rig.host.root(DOM).expect("a mounted instance has a root");
        let first = rig.host.create_element(DOM, "span").expect("create");
        let second = rig.host.create_element(DOM, "span").expect("create");
        rig.host.append(DOM, root, first).expect("append");
        rig.host.append(DOM, root, second).expect("append");
        rig.host.set_text(DOM, first, "leaf").expect("write a leaf");

        rig.host.remove(DOM, first).expect("remove the leaf");
        assert!(
            rig.host.set_text(DOM, first, "x").is_err(),
            "the leaf's own handle is reclaimed"
        );
        assert!(
            rig.host.set_text(DOM, second, "x").is_ok(),
            "its sibling is untouched"
        );
        assert!(rig.host.set_text(DOM, root, "x").is_ok(), "so is the root");
    }

    #[wasm_bindgen_test]
    fn an_error_card_drops_the_faulted_instances_whole_table() {
        // A faulted instance is terminal, so nothing will ever reclaim what it
        // built the ordinary way: the card's clear is the last chance, and it
        // destroys the largest subtree in the system.
        let rig = dom_rig();
        let root = rig.host.root(DOM).expect("a mounted instance has a root");
        let child = rig.host.create_element(DOM, "span").expect("create");
        rig.host.append(DOM, root, child).expect("append");
        assert_eq!(rig.host.live_handles(DOM), Some(2));

        dom::render_error_card(DOM, DOM, "boom");

        assert_eq!(
            rig.host.live_handles(DOM),
            None,
            "the table goes with the subtree"
        );
        assert!(
            rig.host.set_text(DOM, child, "x").is_err(),
            "and every handle it held traps"
        );
    }

    #[wasm_bindgen_test]
    fn a_leaf_removal_reclaims_the_leaf_in_every_table() {
        // The leaf case takes its own all-tables loop — the fast path skips the
        // containment sweep — and it is the shape almost every removal and every
        // `set-text` of an inline node takes. Narrowing that loop to the caller's
        // table reopens the leak on the commonest operation in the system.
        let rig = rig(&[("wbt-reach", &["dom"]), ("wbt-page", &["dom", "page-dom"])]);
        let root = rig.host.root("wbt-reach").expect("a mounted root");
        let leaf = rig
            .host
            .create_element("wbt-reach", "span")
            .expect("create");
        rig.host.append("wbt-reach", root, leaf).expect("append");

        // A second table naming the same element. No import hands one instance
        // another's leaf today, so the seam is reached directly: what the
        // contract promises out-of-tree is that reclamation crosses tables, not
        // that only the routes in-tree code walks are covered.
        let element = rig
            .host
            .element("wbt-reach", leaf)
            .expect("the leaf is named");
        let theirs = rig.host.handle_for("wbt-page", &element);

        rig.host.remove("wbt-reach", leaf).expect("remove the leaf");

        assert!(
            rig.host.set_text("wbt-page", theirs, "x").is_err(),
            "the other table's handle to the destroyed leaf is reclaimed too"
        );
        assert_eq!(
            rig.host.live_handles("wbt-page"),
            Some(0),
            "and its slot is free, not merely unusable"
        );
    }

    #[wasm_bindgen_test]
    fn a_remount_reclaims_the_whole_wrapper_and_drops_the_table() {
        // Mounting is idempotent and clears whatever was there, so a re-mount is
        // a real destruction: the instance's own table goes, and so does a
        // sibling's handle to anything that was hanging under the wrapper.
        let rig = rig(&[("wbt-reach", &["dom"]), ("wbt-page", &["dom", "page-dom"])]);
        let root = rig.host.root("wbt-reach").expect("a mounted root");
        let child = rig
            .host
            .create_element("wbt-reach", "span")
            .expect("create");
        rig.host.append("wbt-reach", root, child).expect("append");
        let wrapper = rig
            .host
            .instance_wrapper("wbt-page", "wbt-reach")
            .expect("page authority")
            .expect("a mounted instance has a wrapper");
        let theirs = rig.host.create_element("wbt-page", "span").expect("create");
        rig.host
            .append("wbt-page", wrapper, theirs)
            .expect("append into the wrapper");
        assert_eq!(rig.host.live_handles("wbt-reach"), Some(2));

        dom::mount_host("wbt-reach", "wbt-reach");

        assert_eq!(
            rig.host.live_handles("wbt-reach"),
            None,
            "the re-mounted instance's table goes with its subtree"
        );
        assert!(
            rig.host.set_text("wbt-reach", child, "x").is_err(),
            "and the handles it held name nothing"
        );
        assert!(
            rig.host.set_text("wbt-page", theirs, "x").is_err(),
            "the sweep crosses tables here as it does on the guest path"
        );
        assert!(
            rig.host.set_text("wbt-page", wrapper, "x").is_ok(),
            "the wrapper itself was cleared, not destroyed"
        );
    }

    #[wasm_bindgen_test]
    fn an_error_card_reclaims_a_siblings_handles_into_the_dead_subtree() {
        // The card destroys the whole wrapper content, which can hold elements a
        // `page-dom` holder appended there: dropping only the faulted instance's
        // table would leave the kernel holding those, and the holder mutating
        // orphans.
        let rig = rig(&[("wbt-reach", &["dom"]), ("wbt-page", &["dom", "page-dom"])]);
        let wrapper = rig
            .host
            .instance_wrapper("wbt-page", "wbt-reach")
            .expect("page authority")
            .expect("a mounted instance has a wrapper");
        let theirs = rig.host.create_element("wbt-page", "span").expect("create");
        rig.host
            .append("wbt-page", wrapper, theirs)
            .expect("append into the wrapper");

        dom::render_error_card("wbt-reach", "wbt-reach", "boom");

        assert!(
            rig.host.set_text("wbt-page", theirs, "x").is_err(),
            "the sibling's handle into the carded subtree is reclaimed"
        );
        assert!(
            rig.host.set_text("wbt-page", wrapper, "x").is_ok(),
            "the wrapper survives the card that renders inside it"
        );
    }

    #[wasm_bindgen_test]
    fn a_page_authority_removal_reclaims_the_victims_handles_too() {
        // Reclamation is about what the *host* still holds, so the sweep crosses
        // instances: a `page-dom` holder destroying a subtree invalidates every
        // handle to it, whoever minted them. The victim then traps rather than
        // operating on an orphan.
        let mut rig = rig(&[("wbt-reach", &["dom"]), ("wbt-page", &["dom", "page-dom"])]);
        let mine = rig.host.root("wbt-reach").expect("a mounted root");
        let child = rig
            .host
            .create_element("wbt-reach", "span")
            .expect("create");
        rig.host.append("wbt-reach", mine, child).expect("append");

        let wrapper = rig
            .host
            .instance_wrapper("wbt-page", "wbt-reach")
            .expect("page authority")
            .expect("a registered instance has a wrapper");
        rig.host.remove("wbt-page", wrapper).expect("remove it");

        assert!(
            rig.host.set_text("wbt-reach", mine, "x").is_err(),
            "the victim's root handle is stale"
        );
        assert!(
            rig.host.set_text("wbt-reach", child, "x").is_err(),
            "and so is its handle to a node inside the removed subtree"
        );
        assert_eq!(
            rig.report(),
            None,
            "a stale handle is a trap, not a refusal with a breadcrumb"
        );
    }

    #[wasm_bindgen_test]
    fn the_grant_gate_splits_reach_from_page_authority() {
        let mut rig = rig(&[
            ("wbt-reach", &["dom"]),
            ("wbt-page", &["dom", "page-dom"]),
            ("wbt-none", &[]),
        ]);

        assert!(rig.host.create_element("wbt-reach", "div").is_ok());
        assert!(
            rig.host.page_root("wbt-reach").is_err(),
            "reach is not authority: `dom` alone opens no page entry"
        );
        let message = rig.report().expect("the refusal leaves a breadcrumb");
        assert!(message.contains("page-dom"), "{message}");
        assert!(message.contains("wbt-reach"), "{message}");

        assert!(rig.host.page_root("wbt-page").is_ok());
        assert!(rig.host.page_body("wbt-page").is_ok());
        assert!(
            rig.host
                .instance_wrapper("wbt-page", "wbt-reach")
                .expect("the entry is open")
                .is_some(),
            "a registered sibling has a wrapper"
        );
        assert_eq!(
            rig.host
                .instance_wrapper("wbt-page", "nobody")
                .expect("the entry is open"),
            None,
            "an unregistered instance is the ordinary transient, not an error"
        );

        assert!(
            rig.host.create_element("wbt-none", "div").is_err(),
            "an ungranted instance reaches nothing"
        );
        assert!(rig.host.page_root("wbt-none").is_err());
    }

    #[wasm_bindgen_test]
    fn the_page_entries_hand_back_canonical_handles() {
        let rig = rig(&[("wbt-page-2", &["dom", "page-dom"])]);
        let body = rig.host.page_body("wbt-page-2").expect("granted");
        let root = rig.host.root("wbt-page-2").expect("mounted");
        assert_ne!(body, root);
        assert_eq!(
            rig.host.page_body("wbt-page-2").expect("granted"),
            body,
            "the same element is the same handle, which is what makes `parent` \
             comparable"
        );
        let wrapper = rig
            .host
            .parent("wbt-page-2", root)
            .expect("granted")
            .expect("a mounted host element has a parent");
        assert_eq!(
            rig.host
                .instance_wrapper("wbt-page-2", "wbt-page-2")
                .expect("granted"),
            Some(wrapper),
            "the parent of an instance's root is its own kernel-owned wrapper"
        );
    }

    #[wasm_bindgen_test]
    fn a_component_cannot_forge_the_identity_its_wrapper_carries() {
        let rig = dom_rig();
        let root = rig.host.root(DOM).expect("mounted");
        rig.host
            .set_attribute(DOM, root, "data-instance", "chrome")
            .expect("`data-` is the component's own namespace");

        let wrapper = dom::wrapper_element(DOM).expect("the instance has a wrapper");
        assert_eq!(
            wrapper.get_attribute("data-instance").as_deref(),
            Some(DOM),
            "identity is the wrapper's, and the wrapper is not reachable from a \
             `dom` handle"
        );
        assert_eq!(wrapper.get_attribute("data-kind").as_deref(), Some(DOM));
        assert!(
            rig.host.parent(DOM, root).is_err(),
            "walking out of the subtree is page authority, which this instance \
             does not hold"
        );
    }

    #[wasm_bindgen_test]
    fn the_utc_offset_is_minutes_east_of_utc() {
        let rig = dom_rig();
        const EPOCH_MS: u64 = 1_700_000_000_000;
        let answer = rig.host.utc_offset_minutes(DOM, EPOCH_MS).expect("granted");
        let js = js_sys::Date::new(&JsValue::from_f64(EPOCH_MS as f64)).get_timezone_offset();
        assert_eq!(
            f64::from(answer),
            -js,
            "JS reports the offset with the opposite sign to the one everybody \
             means by UTC+2"
        );
    }

    #[wasm_bindgen_test]
    fn a_gesture_reports_the_nearest_mapped_ancestor_of_its_target() {
        // A delegated listener: one listener on the container, and the body must
        // say which child was hit — the difference between acting on a row and
        // acting on the wrong row.
        let rig = dom_rig();
        let root = rig.host.root(DOM).expect("mounted");
        let container = rig.host.create_element(DOM, "div").expect("create");
        let row = rig.host.create_element(DOM, "button").expect("create");
        rig.host.append(DOM, root, container).expect("append");
        rig.host.append(DOM, container, row).expect("append");
        // Unnamed by the component, so an event on it retargets to `row`.
        let inner = document().create_element("span").expect("create a span");
        rig.host
            .element(DOM, row)
            .expect("a live handle")
            .append_child(&inner)
            .expect("append");

        rig.host
            .listen(DOM, container, "click", "press")
            .expect("granted");
        inner
            .dyn_ref::<HtmlElement>()
            .expect("a span is an HtmlElement")
            .click();

        let activation = rig.seen.borrow().last().cloned().expect("the entry ran");
        assert_eq!(activation.sync.as_deref(), Some("press"));
        let body = activation
            .ports
            .iter()
            .find(|window| window.port == "press")
            .and_then(|window| window.envelopes.last())
            .map(|envelope| envelope.body.clone())
            .expect("the sync window carries the request");
        let body: serde_json::Value = serde_json::from_str(&body).expect("the body is JSON");
        assert_eq!(body[GESTURE_EVENT_FIELD], "click");
        assert_eq!(body[GESTURE_LISTENER_FIELD], container);
        assert_eq!(
            body[GESTURE_TARGET_FIELD], row,
            "the target is the nearest ancestor the instance holds a handle for"
        );
    }

    #[wasm_bindgen_test]
    fn only_a_reply_that_asked_cancels_the_browsers_default_action() {
        let rig = dom_rig();
        let root = rig.host.root(DOM).expect("mounted");
        let button = rig.host.create_element(DOM, "button").expect("create");
        rig.host.append(DOM, root, button).expect("append");
        rig.host
            .listen(DOM, button, "click", "press")
            .expect("granted");
        let element = rig.host.element(DOM, button).expect("a live handle");

        for (reply, cancelled) in [
            (None, false),
            (Some("{}".to_string()), false),
            (Some(r#"{"cancel":false}"#.to_string()), false),
            (Some(r#"{"cancel":true}"#.to_string()), true),
        ] {
            *rig.reply.borrow_mut() = reply.clone();
            let event = cancelable_click();
            element.dispatch_event(&event).expect("dispatch");
            assert_eq!(
                event.default_prevented(),
                cancelled,
                "reply {reply:?} decides the cancel"
            );
        }
        assert_eq!(rig.seen.borrow().len(), 4, "every press ran an activation");
    }

    #[wasm_bindgen_test]
    fn a_gesture_on_an_unregistered_instance_is_refused_without_cancelling() {
        // The door refuses when there is no entry, and a refusal is not an answer
        // that asked for `preventDefault`.
        let rig = rig(&[("wbt-gone", &["dom"])]);
        let root = rig.host.root("wbt-gone").expect("mounted");
        let button = rig
            .host
            .create_element("wbt-gone", "button")
            .expect("create");
        rig.host.append("wbt-gone", root, button).expect("append");
        rig.host
            .listen("wbt-gone", button, "click", "press")
            .expect("granted");
        let element = rig.host.element("wbt-gone", button).expect("a live handle");
        // Withdrawn after its UI was built, which is the state a torn-down or
        // faulted instance's page-lifetime listeners fire into.
        rig.entries.borrow_mut().remove("wbt-gone");

        *rig.reply.borrow_mut() = Some(r#"{"cancel":true}"#.to_string());
        let event = cancelable_click();
        element.dispatch_event(&event).expect("dispatch");
        assert!(!event.default_prevented());
    }

    #[wasm_bindgen_test]
    fn an_ungranted_instance_wires_no_listener() {
        let mut rig = rig(&[("wbt-mute", &[])]);
        assert!(rig.host.listen("wbt-mute", 1, "click", "press").is_err());
        let message = rig.report().expect("the refusal leaves a breadcrumb");
        assert!(message.contains("dom.listen"), "{message}");
    }

    #[wasm_bindgen_test]
    fn every_entry_answers_with_the_page_and_the_entries_borrowed() {
        // The invariant this host is shaped around: a DOM import is called from
        // inside a component's `receive`, and the runner holds the page borrow
        // across that invocation (the sync door reads exactly that borrow to
        // name a re-entrant request). So no entry here may reach for
        // `SharedPage` or `SharedEntries` — one that did would not refuse, it
        // would panic on `already borrowed`, from an ordinary render.
        //
        // Stated as a test rather than a convention: the borrows below are the
        // in-flight activation, and every entry is called under them.
        let rig = rig(&[("wbt-borrow", &["dom", "page-dom"])]);
        const WHO: &str = "wbt-borrow";
        let root = rig.host.root(WHO).expect("mounted");
        let child = rig.host.create_element(WHO, "div").expect("an allowed tag");
        rig.host.append(WHO, root, child).expect("append");

        let _page = rig
            .page
            .try_borrow_mut()
            .expect("nothing else holds the page here");
        let _entries = rig
            .entries
            .try_borrow_mut()
            .expect("nothing else holds the entries here");

        // Every `dom` entry, including the ones that mint and the ones that
        // read.
        rig.host.root(WHO).expect("root");
        let made = rig.host.create_element(WHO, "span").expect("create");
        rig.host
            .set_attribute(WHO, made, "class", "c")
            .expect("set-attribute");
        rig.host
            .remove_attribute(WHO, made, "class")
            .expect("remove-attribute");
        rig.host.set_text(WHO, made, "text").expect("set-text");
        rig.host
            .set_style_property(WHO, made, "color", "red")
            .expect("set-style-property");
        rig.host
            .remove_style_property(WHO, made, "color")
            .expect("remove-style-property");
        rig.host.append(WHO, root, made).expect("append");
        rig.host
            .insert_before(WHO, root, made, Some(child))
            .expect("insert-before");
        rig.host.value(WHO, made).expect("value");
        rig.host.set_value(WHO, made, "v").expect("set-value");
        rig.host.remove(WHO, made).expect("remove");
        rig.host
            .listen(WHO, child, "click", "press")
            .expect("listen");
        rig.host.utc_offset_minutes(WHO, 0).expect("utc-offset");

        // And every `page-dom` entry, which reaches further into the document
        // and no further into the kernel.
        rig.host.page_root(WHO).expect("page-root");
        rig.host.page_body(WHO).expect("page-body");
        rig.host
            .instance_wrapper(WHO, WHO)
            .expect("instance-wrapper");
        rig.host.parent(WHO, child).expect("parent");
    }

    /// The children of `element`, as their text.
    fn child_text(element: &Element) -> Vec<String> {
        let mut text = Vec::new();
        let mut child = element.first_element_child();
        while let Some(node) = child {
            text.push(node.text_content().unwrap_or_default());
            child = node.next_element_sibling();
        }
        text
    }

    /// A bubbling, cancelable `click` — cancelable because whether the reply
    /// cancelled it is the whole observation.
    fn cancelable_click() -> Event {
        let init = web_sys::CustomEventInit::new();
        init.set_bubbles(true);
        init.set_cancelable(true);
        web_sys::CustomEvent::new_with_event_init_dict("click", &init)
            .expect("construct a cancelable click")
            .into()
    }
}
