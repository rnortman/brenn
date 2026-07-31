//! The bindings document, applied: parsed, checked against what this build can
//! actually size, and indexed for the lookups the surface layer makes on every
//! delivery, activation and publish.
//!
//! A surface's wiring is retained state on its config channel, so it arrives as
//! an ordinary message and is re-applied on every attachment. Applying it is
//! three things the parsed document is not:
//!
//! 1. **The target-dependent checks.** A depth the server can serialize is not
//!    automatically one this page can allocate against — `usize` is 32-bit on
//!    the wasm target — so the schema, which is shared with a 64-bit writer,
//!    deliberately leaves that verdict to the consumer sizing the queue.
//! 2. **The derived tables.** The document is declaration-ordered lists; the
//!    hot paths ask by key: which components exist, what a channel's bound
//!    ports are, where an instance's port publishes, how deep a channel's store
//!    is, and what depths to state when subscribing it on the wire.
//! 3. **The exact body.** Two attachments whose bodies are byte-identical
//!    describe the same wiring, which is what decides whether a reconnect
//!    reloads the page. The document builder is deterministic for exactly this
//!    comparison, so it is made on the bytes rather than on a structural walk.
//!
//! Every refusal here names a broken writer. The server resolves this document
//! from config it boot-validated, and both ends of a live surface are built
//! together, so a document this kernel cannot apply is not a difference of
//! opinion to accommodate — the caller's answer to any of them is the same
//! fatal.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};

use brenn_attach_client::subs::SubscriptionDepths;
use brenn_surface_schema::bindings::{BindingsDocument, BindingsError, PlatformSection};
use brenn_surface_schema::{
    Binding, ComponentEntry, LocalChannel, OutputBinding, RESERVED_LOCAL_CHANNELS,
};

use crate::core::channel_is_transportable;

/// One surface's wiring, ready to run on.
///
/// Holds the document it was built from, the body those bytes came in as, and
/// the indexes every hot path reads. Immutable once built: a change of wiring is
/// a new document, and what the kernel does about one is decided by comparing
/// the two rather than by mutating either.
#[derive(Debug)]
pub struct AppliedBindings {
    /// The retained body exactly as delivered, for the wiring-changed
    /// comparison.
    body: String,
    doc: BindingsDocument,
    /// Instance id → index into `doc.components`.
    components: BTreeMap<String, usize>,
    /// Instance id → port → index into `doc.outputs`. Nested rather than keyed
    /// by a `(String, String)` pair so resolving a port is two borrowed probes:
    /// a pair key would have the publish path compose an owned key, and its two
    /// allocations, on every publish.
    outputs: BTreeMap<String, BTreeMap<String, usize>>,
    /// Channel address → indexes into `doc.subscriptions`, in declaration order.
    /// The fan-out table: one arriving envelope is windowed for every binding
    /// listed here, each at its own depths and loudness.
    inputs_by_channel: BTreeMap<String, Vec<usize>>,
    /// Transportable channel → the depths to state when subscribing it, folded
    /// across every binding on it.
    wire_depths: BTreeMap<String, SubscriptionDepths>,
    /// Channel → the depth its page-side store is sized to, both classes.
    store_depths: BTreeMap<String, u64>,
}

impl AppliedBindings {
    /// Parse, check and index a retained bindings body.
    ///
    /// One call for all of it because a caller has one answer to every failure:
    /// a body that does not parse, does not validate, or names depths this build
    /// cannot allocate is equally unusable, and there is nothing to be done with
    /// the parts.
    pub fn apply(body: &str) -> Result<Self, BindingsError> {
        let doc = BindingsDocument::parse(body)?;
        Self::index(body.to_string(), doc)
    }

    /// Build the indexes over a validated document, refusing the depths this
    /// build cannot allocate against.
    ///
    /// # Panics
    ///
    /// On a repeated component instance, output port, input port, or local
    /// channel address. All four are refused by [`BindingsDocument::validate`],
    /// which every parse runs, so reaching one here means the index and the
    /// validation disagree about what a well-formed document is.
    fn index(body: String, doc: BindingsDocument) -> Result<Self, BindingsError> {
        let mut components = BTreeMap::new();
        for (i, c) in doc.components.iter().enumerate() {
            let prior = components.insert(c.instance.clone(), i);
            assert!(
                prior.is_none(),
                "surface client: bindings document declares component instance {} twice",
                c.instance
            );
        }

        let mut outputs: BTreeMap<String, BTreeMap<String, usize>> = BTreeMap::new();
        for (i, b) in doc.outputs.iter().enumerate() {
            let prior = outputs
                .entry(b.instance.clone())
                .or_default()
                .insert(b.port.clone(), i);
            assert!(
                prior.is_none(),
                "surface client: bindings document binds output port {}/{} twice",
                b.instance,
                b.port
            );
        }

        let mut inputs_by_channel: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        let mut wire_depths: BTreeMap<String, SubscriptionDepths> = BTreeMap::new();
        let mut store_depths: BTreeMap<String, u64> = BTreeMap::new();

        // A declared confined channel gets a store whether or not anything binds
        // it: the page's own router accepts publishes on it, and the declared
        // ring depth is the floor under what it retains.
        let mut local_addresses = BTreeSet::new();
        for lc in &doc.local_channels {
            assert!(
                local_addresses.insert(lc.channel.as_str()),
                "surface client: bindings document declares local channel {} twice",
                lc.channel
            );
            let depth = store_depths.entry(lc.channel.clone()).or_default();
            *depth = (*depth).max(lc.ring_depth);
        }

        let mut input_ports = BTreeSet::new();
        for (i, b) in doc.subscriptions.iter().enumerate() {
            assert!(
                input_ports.insert((b.instance.as_str(), b.port.as_str())),
                "surface client: bindings document binds input port {}/{} twice",
                b.instance,
                b.port
            );
            check_sizable(b)?;
            inputs_by_channel
                .entry(b.channel.clone())
                .or_default()
                .push(i);
            // A store has one size per channel, shared by every binding on it:
            // the fold of `max(push_depth, retain_depth)`. Both halves are
            // load-bearing — `retain_depth` is what a binding reads as context
            // and `push_depth` is what it can be handed as new — and the store
            // is the only thing holding either, so a store shallower than a
            // binding's window would silently cap it.
            let depth = store_depths.entry(b.channel.clone()).or_default();
            *depth = (*depth).max(b.push_depth.max(b.retain_depth));
            if channel_is_transportable(&b.channel) {
                let stated = wire_depths
                    .entry(b.channel.clone())
                    .or_insert(SubscriptionDepths {
                        push_depth: 0,
                        retain_depth: 0,
                    });
                stated.push_depth = stated.push_depth.max(b.push_depth);
                stated.retain_depth = stated.retain_depth.max(b.retain_depth);
            }
        }

        // A reserved plane's contract depth is a floor under its store, raised
        // by whatever binds it and never lowered. Applied only to planes this
        // document declares: an undeclared plane has no store to floor.
        for reserved in RESERVED_LOCAL_CHANNELS {
            if let Some(depth) = store_depths.get_mut(reserved.address) {
                *depth = (*depth).max(reserved.ring_depth);
            }
        }

        Ok(Self {
            body,
            doc,
            components,
            outputs,
            inputs_by_channel,
            wire_depths,
            store_depths,
        })
    }

    /// The retained body these bindings were applied from.
    pub fn body(&self) -> &str {
        &self.body
    }

    /// Whether `other` describes the same wiring as these bindings.
    ///
    /// Byte equality of the retained bodies. The document builder reads no clock
    /// and no per-connection data and orders every collection, so a rebuild of
    /// unchanged config produces the same bytes — which makes "the wiring
    /// changed" a comparison the kernel can make without walking two documents
    /// field by field and getting the walk subtly wrong.
    pub fn same_wiring_as(&self, other: &AppliedBindings) -> bool {
        self.body == other.body
    }

    /// The document itself, for the readers that want a field these accessors do
    /// not index.
    pub fn document(&self) -> &BindingsDocument {
        &self.doc
    }

    /// The kernel's own wiring: where its telemetry and error reports go, and
    /// which surface-wide grants it holds.
    pub fn platform(&self) -> &PlatformSection {
        &self.doc.platform
    }

    /// The instance id of this surface's chrome component. Always names a
    /// declared component — the document is refused otherwise.
    pub fn chrome_instance(&self) -> &str {
        &self.doc.chrome_instance
    }

    /// Every declared component instance, in declaration order.
    pub fn components(&self) -> &[ComponentEntry] {
        &self.doc.components
    }

    /// The component entry for `instance`, or `None` if this surface declares no
    /// such instance.
    pub fn component(&self, instance: &str) -> Option<&ComponentEntry> {
        self.components
            .get(instance)
            .map(|&i| &self.doc.components[i])
    }

    /// Whether `instance` is a component this surface declares.
    pub fn is_declared_instance(&self, instance: &str) -> bool {
        self.components.contains_key(instance)
    }

    /// Every page-local channel this surface declares, with the ring depth its
    /// page-local router retains. Local channels have no `[[channel]]` block, so
    /// this table is the only place their parameters come from.
    pub fn local_channels(&self) -> &[LocalChannel] {
        &self.doc.local_channels
    }

    /// The output binding `instance` publishes on through `port`, or `None` for
    /// a port this surface does not wire.
    pub fn output(&self, instance: &str, port: &str) -> Option<&OutputBinding> {
        self.outputs
            .get(instance)?
            .get(port)
            .map(|&i| &self.doc.outputs[i])
    }

    /// Every output binding of one instance, in declaration order — what an
    /// activation's publish budget and port table are seeded from.
    pub fn outputs_of<'a>(&'a self, instance: &'a str) -> impl Iterator<Item = &'a OutputBinding> {
        self.doc
            .outputs
            .iter()
            .filter(move |b| b.instance == instance)
    }

    /// Every input binding of one instance, in declaration order — the positions
    /// and subscription references a registration takes.
    pub fn inputs_of<'a>(&'a self, instance: &'a str) -> impl Iterator<Item = &'a Binding> {
        self.doc
            .subscriptions
            .iter()
            .filter(move |b| b.instance == instance)
    }

    /// Every binding on `channel`, in declaration order: the fan-out table for
    /// one arriving envelope. Empty for a channel nothing binds.
    pub fn inputs_on<'a>(&'a self, channel: &'a str) -> impl Iterator<Item = &'a Binding> {
        self.inputs_by_channel
            .get(channel)
            .into_iter()
            .flatten()
            .map(|&i| &self.doc.subscriptions[i])
    }

    /// The depths to state when subscribing `channel` on the wire, folded across
    /// every binding on it, or `None` for a channel this surface does not
    /// subscribe across the wire.
    ///
    /// The fold is the client's half of the two-sided depth story: the server
    /// clamps whatever is stated to its own boot fold, so stating the max across
    /// local readers asks for exactly what some reader can use and no more.
    pub fn wire_depths(&self, channel: &str) -> Option<SubscriptionDepths> {
        self.wire_depths.get(channel).copied()
    }

    /// Every channel this surface subscribes across the wire, address-ordered,
    /// with the depths to state for it.
    pub fn wire_channels(&self) -> impl Iterator<Item = (&str, SubscriptionDepths)> {
        self.wire_depths.iter().map(|(c, d)| (c.as_str(), *d))
    }

    /// The depth the page's store for `channel` is sized to, or `None` for a
    /// channel this surface neither binds nor declares.
    pub fn store_depth(&self, channel: &str) -> Option<u64> {
        self.store_depths.get(channel).copied()
    }

    /// Every channel needing a page-side store, address-ordered, with its depth.
    /// Both classes: a confined channel's store is the channel, and a wire
    /// channel's is what its bindings read their windows out of.
    pub fn store_depths(&self) -> impl Iterator<Item = (&str, u64)> {
        self.store_depths.iter().map(|(c, d)| (c.as_str(), *d))
    }
}

/// Refuse a binding whose depths this build cannot allocate against.
///
/// Both knobs size page memory — `push_depth` a port's queue, `retain_depth` the
/// context window read out of the store — so both must be representable as a
/// `usize` here. The answer is target-dependent (`usize` is 32-bit on the wasm
/// target), which is why it is asked at the consumer rather than in the schema
/// the server also validates against. Checked once, so the sizing paths convert
/// infallibly.
///
/// `0` is legal on either knob: a depth-0 binding is context-only, with no queue
/// to size and no window to fill.
fn check_sizable(b: &Binding) -> Result<(), BindingsError> {
    check_sizable_within(b, max_sizable_depth())
}

/// The deepest binding this build can allocate against, in the document's `u64`
/// currency.
///
/// A function rather than a constant because the bound is the whole
/// target-dependence: on a 64-bit build it refuses nothing, so the refusals below
/// are reachable only by driving [`check_sizable_within`] against a smaller
/// bound.
fn max_sizable_depth() -> u64 {
    u64::try_from(usize::MAX).expect("usize fits u64 on every target this builds for")
}

/// [`check_sizable`] against a stated bound.
fn check_sizable_within(b: &Binding, max: u64) -> Result<(), BindingsError> {
    if b.push_depth > max {
        return Err(format!(
            "bindings document binding {}/{} on {} declares an unusable push_depth: {}",
            b.instance, b.port, b.channel, b.push_depth
        ));
    }
    if b.retain_depth > max {
        return Err(format!(
            "bindings document binding {}/{} on {} declares an unusable retain_depth: {}",
            b.instance, b.port, b.channel, b.retain_depth
        ));
    }
    Ok(())
}
