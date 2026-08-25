//! The bindings document: a surface's boot-resolved wiring, carried as a
//! retained message on its per-surface config channel.
//!
//! The server builds one document per surface at boot and publishes it; the
//! kernel subscribes to the config channel, parses the retained copy, and
//! applies it. The document is the surface application layer's whole
//! configuration: the component instances to mount, the input and output
//! bindings, the page-local channel table, the chrome singleton, and the
//! [`PlatformSection`] of kernel-level addresses and grants.
//!
//! **Determinism.** The body is a pure function of boot-resolved config: no
//! clock reads, no per-connection data, no map iteration order that a rebuild
//! could permute ([`ComponentEntry::config`] is a `BTreeMap`, every other
//! collection is a declaration-ordered `Vec`). Two boots on the same config
//! produce byte-identical bodies, which is what lets the kernel compare a
//! freshly delivered document against the one it is running on and reload only
//! on a real difference.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use brenn_envelope::is_local_channel;

use crate::{
    Binding, ComponentEntry, LocalChannel, LogLevel, OutputBinding, reserved_local_channel,
    surface_bindable_address,
};

/// The body-schema version stamped on every bindings document. Bumped whenever
/// the document's shape changes; a reader that does not recognize the value
/// refuses the document rather than guessing at its fields.
pub const BINDINGS_DOCUMENT_VERSION: u32 = 1;

/// Lowest admissible [`PlatformSection::status_interval_secs`]. The status
/// channel is a heartbeat, not a meter: below this the kernel would write
/// high-frequency data onto a retained channel.
pub const STATUS_INTERVAL_SECS_MIN: u32 = 5;

/// Highest admissible [`PlatformSection::status_interval_secs`]. Past an hour a
/// heartbeat says nothing about liveness.
pub const STATUS_INTERVAL_SECS_MAX: u32 = 3600;

/// One surface's boot-resolved wiring.
///
/// Strict on parse: an unrecognized field is a refusal, not a shrug. Both ends
/// of a live surface are built together, so a field this reader does not know
/// means the document was written by something other than the matching server —
/// exactly the case where guessing is worse than stopping. The entry structs
/// ([`ComponentEntry`], [`Binding`], [`OutputBinding`], [`LocalChannel`]) live
/// at the crate root because the page-local planes name them too; the strictness
/// is stated once, here, on the document that carries them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingsDocument {
    /// Body-schema version; [`BINDINGS_DOCUMENT_VERSION`] for a document this
    /// build can read.
    pub v: u32,
    /// Mounted component instances, declaration order. Each names its `instance`
    /// id (the routing/mount key) and its component `kind` (the element tag and
    /// wasm module). One kind may back several instances.
    pub components: Vec<ComponentEntry>,
    /// Channel → instance/port.
    pub subscriptions: Vec<Binding>,
    /// Instance/port → channel, each carrying the port's default urgency.
    pub outputs: Vec<OutputBinding>,
    /// Every distinct `local:` channel some binding above names, with the ring
    /// depth its page-local router must retain. Page-local channels have no
    /// `[[channel]]` block and no directory entry — the per-surface config block
    /// *is* the declaration — so this table is the only place their per-channel
    /// parameters can be resolved. Deduped, in first-binding order.
    pub local_channels: Vec<LocalChannel>,
    /// The `instance` id of this surface's chrome component: the singleton the
    /// kernel treats specially (pre-chrome connect-indicator handoff and
    /// chrome-death-is-fatal). One field rather than a per-entry flag makes the
    /// singleton invariant unrepresentable-wrong. Always names an instance in
    /// [`components`](Self::components) — every surface has exactly one chrome
    /// component, so an empty or unknown id is a refused document rather than a
    /// chromeless surface.
    pub chrome_instance: String,
    /// The kernel's own wiring, as opposed to the components'.
    pub platform: PlatformSection,
}

/// The kernel-level half of a bindings document: where the kernel's own
/// telemetry and error reports go.
///
/// It carries no capability flags. What a page's components may do is stated
/// per component in [`ComponentEntry::grants`](crate::ComponentEntry::grants) —
/// a page-wide flag every component reads alike gates nothing.
///
/// Addresses are explicit rather than derived. The kernel cannot know the
/// operator's channel prefix, and a client that reconstructs addresses from a
/// naming convention is a second implementation of that convention.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlatformSection {
    /// Where the kernel publishes its viewport document.
    pub geometry_channel: String,
    /// Where the kernel publishes its status document.
    pub status_channel: String,
    /// Status document cadence, seconds. The kernel emits a snapshot on this
    /// interval (and immediately on any transition into `failed`). The operator
    /// tunes it; there is no off state.
    pub status_interval_secs: u32,
    /// Where the kernel publishes error reports, or `None` when the surface
    /// declares no error-report channel — in which case the kernel reports
    /// nothing and keeps its console copy only.
    pub error_channel: Option<String>,
    /// The lowest level admitted to `error_channel`: a report is published iff
    /// `level >= floor`. `None` exactly when `error_channel` is `None`.
    pub error_report_floor: Option<LogLevel>,
}

/// Why a bindings document was refused. Carries a rendered message rather than
/// a code: every one of these is fatal at the reader, and the message is what
/// reaches the log a human reads.
pub type BindingsError = String;

impl BindingsDocument {
    /// Serialize to the retained body, byte-stable per the module's determinism
    /// rule.
    ///
    /// # Panics
    ///
    /// If the document does not serialize — impossible for these types (no
    /// non-string map keys, no floats, no custom `Serialize`), so a failure is a
    /// broken invariant rather than a condition to handle.
    pub fn to_body(&self) -> String {
        serde_json::to_string(self).expect("bindings document serializes to JSON")
    }

    /// Parse and validate a retained body.
    ///
    /// Three refusals in one call because a caller has the same answer to all
    /// three — a document that does not parse, does not carry this build's
    /// version, or does not satisfy [`validate`](Self::validate) is equally
    /// unusable, and there is nothing a reader can do with the parts.
    pub fn parse(body: &str) -> Result<Self, BindingsError> {
        let doc: Self = serde_json::from_str(body)
            .map_err(|e| format!("bindings document does not parse: {e}"))?;
        if doc.v != BINDINGS_DOCUMENT_VERSION {
            return Err(format!(
                "bindings document declares schema version {} — this build reads {}",
                doc.v, BINDINGS_DOCUMENT_VERSION
            ));
        }
        doc.validate()?;
        Ok(doc)
    }

    /// Check the document's internal consistency.
    ///
    /// Every rule here holds by construction on the writing side, so a failure
    /// names a broken writer, never a tolerable difference of opinion. Run on
    /// both sides for that reason: the builder proves what it wrote and the
    /// reader refuses to run on wiring it cannot resolve.
    ///
    /// Not checked here: that each `push_depth` fits the reader's `usize`. That
    /// answer is target-dependent (the wasm target's `usize` is 32-bit), so it
    /// belongs to the consumer sizing the queue, not to a shared schema rule
    /// whose verdict would differ per build.
    pub fn validate(&self) -> Result<(), BindingsError> {
        // The instance id is the mount key and the routing key; two entries
        // claiming one id leave both meanings ambiguous.
        let mut instances = BTreeSet::new();
        for c in &self.components {
            if !instances.insert(&c.instance) {
                return Err(format!(
                    "bindings document declares component instance {} twice",
                    c.instance
                ));
            }
        }
        // A port publishes onto one channel. Two entries for one port would make
        // a component's publish resolve to whichever the reader indexed first,
        // which is a coin toss dressed as configuration.
        let mut output_ports = BTreeSet::new();
        for b in &self.outputs {
            if !output_ports.insert((&b.instance, &b.port)) {
                return Err(format!(
                    "bindings document binds output port {}/{} twice",
                    b.instance, b.port
                ));
            }
        }
        // An input port reads one channel; a duplicate entry produces double
        // activations and double loss accounting.
        let mut input_ports = BTreeSet::new();
        for b in &self.subscriptions {
            if !input_ports.insert((&b.instance, &b.port)) {
                return Err(format!(
                    "bindings document binds input port {}/{} twice",
                    b.instance, b.port
                ));
            }
        }
        // A confined channel's ring depth is one number. Two entries for one
        // address state two, and a reader folding them by `max` would resolve the
        // disagreement by silently picking a winner.
        let mut local_addresses = BTreeSet::new();
        for lc in &self.local_channels {
            if !local_addresses.insert(&lc.channel) {
                return Err(format!(
                    "bindings document declares local channel {} twice",
                    lc.channel
                ));
            }
        }
        // Inputs and outputs are separate structs (an output carries a default
        // urgency an input has nothing to say about), and the channel rules read
        // only the address — so walk the channels, not the bindings.
        let binding_channels = self
            .subscriptions
            .iter()
            .map(|b| &b.channel)
            .chain(self.outputs.iter().map(|b| &b.channel));
        for channel in binding_channels {
            // Only surface-bindable channels belong in a bindings document;
            // anything else was not written by a healthy builder.
            if !surface_bindable_address(channel) {
                return Err(format!(
                    "bindings document binds channel {channel}, whose scheme is not surface-bindable"
                ));
            }
            // A local binding's channel must appear in the router table, which is
            // the only place its ring depth can come from (local channels have no
            // `[[channel]]` block). Checked here so the router can index its
            // rings infallibly: past this point a resolved local binding always
            // has a ring.
            if is_local_channel(channel)
                && !self.local_channels.iter().any(|lc| lc.channel == *channel)
            {
                return Err(format!(
                    "bindings document binds local channel {channel} but declares no router \
                     entry for it"
                ));
            }
        }
        // A reserved control plane's ring depth is contract-fixed, so a router
        // entry that restates it must restate it *exactly*. Never silently
        // honoured: the depth is the plane's semantics, not a tunable —
        // `link-state` at 0 would kill the late-attach replay the plane exists
        // for, and `toast` above 0 would resurface stale events to a late chrome.
        for lc in &self.local_channels {
            if let Some(reserved) = reserved_local_channel(&lc.channel)
                && lc.ring_depth != reserved.ring_depth
            {
                return Err(format!(
                    "bindings document declares reserved local channel {} at ring depth {}, but \
                     the contract fixes it at {}",
                    lc.channel, lc.ring_depth, reserved.ring_depth
                ));
            }
        }
        // Every binding's instance must appear in the component list. Checked
        // here so a local publish can derive an identity infallibly: past this
        // point every resolvable binding has a declared instance, and an
        // unattributable publish is what the identity model exists to prevent.
        let binding_instances = self
            .subscriptions
            .iter()
            .map(|b| (&b.instance, &b.port, &b.channel))
            .chain(
                self.outputs
                    .iter()
                    .map(|b| (&b.instance, &b.port, &b.channel)),
            );
        for (instance, port, channel) in binding_instances {
            if !self.components.iter().any(|c| c.instance == *instance) {
                return Err(format!(
                    "bindings document binds {instance}/{port} on {channel}, naming an instance \
                     absent from the component list"
                ));
            }
        }
        // The chrome singleton must resolve too, and for a sharper reason than
        // the bindings do: the kernel's chrome rules key on an exact match
        // against this id, so an id naming nobody does not fail loudly — it
        // quietly leaves the page with no chrome, no chrome-death-is-fatal, and
        // no takeover authority. Empty is refused with the rest: a surface
        // always declares exactly one chrome component, so an empty id is a
        // broken writer, not a chromeless profile.
        if !self
            .components
            .iter()
            .any(|c| c.instance == self.chrome_instance)
        {
            return Err(format!(
                "bindings document names chrome instance {:?}, which is absent from the component \
                 list",
                self.chrome_instance
            ));
        }
        // An error-report floor without a channel names a level for nowhere, and
        // a channel without a floor leaves the kernel no admission rule. Both are
        // resolved from one operator declaration, so a half-set pair is a broken
        // builder.
        match (
            &self.platform.error_channel,
            self.platform.error_report_floor,
        ) {
            (Some(_), Some(_)) | (None, None) => {}
            (Some(channel), None) => {
                return Err(format!(
                    "bindings document declares error channel {channel} with no report floor"
                ));
            }
            (None, Some(floor)) => {
                return Err(format!(
                    "bindings document declares error report floor {} with no error channel",
                    floor.as_wire_str()
                ));
            }
        }
        // The platform addresses are the kernel's own publish targets and it
        // publishes them across the wire, so each must name a surface-bindable
        // channel that crosses it — an empty, unbindable, or page-local address
        // would be accepted here and discovered as a refused publish later, far
        // from the writer that wrote it. Checked for the same reason the binding
        // channels are: the reader acts on these addresses.
        let platform_channels = [
            ("geometry_channel", &self.platform.geometry_channel),
            ("status_channel", &self.platform.status_channel),
        ]
        .into_iter()
        .chain(
            self.platform
                .error_channel
                .iter()
                .map(|channel| ("error_channel", channel)),
        );
        for (field, channel) in platform_channels {
            if !surface_bindable_address(channel) || is_local_channel(channel) {
                return Err(format!(
                    "bindings document names {field} {channel:?}, which is not a channel the \
                     kernel can publish across the wire"
                ));
            }
        }
        // The cadence arms a timer. Zero would spin it; an unbounded value would
        // silence the heartbeat the status channel exists to carry.
        if !(STATUS_INTERVAL_SECS_MIN..=STATUS_INTERVAL_SECS_MAX)
            .contains(&self.platform.status_interval_secs)
        {
            return Err(format!(
                "bindings document declares status_interval_secs {}, outside \
                 {STATUS_INTERVAL_SECS_MIN}..={STATUS_INTERVAL_SECS_MAX}",
                self.platform.status_interval_secs
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::{Abi, LOCAL_THEME_CHANNEL, NoiseLevel, Urgency};

    fn component(instance: &str) -> ComponentEntry {
        ComponentEntry {
            instance: instance.to_string(),
            kind: "protobar".to_string(),
            abi: Abi::Dom,
            parked_batch_depth: 4,
            config: BTreeMap::new(),
            grants: vec![],
        }
    }

    fn subscription(instance: &str, channel: &str) -> Binding {
        Binding {
            channel: channel.to_string(),
            instance: instance.to_string(),
            port: "in".to_string(),
            push_depth: 8,
            retain_depth: 2,
            noise: NoiseLevel::Metered,
        }
    }

    fn output(instance: &str, channel: &str) -> OutputBinding {
        OutputBinding {
            channel: channel.to_string(),
            instance: instance.to_string(),
            port: "out".to_string(),
            urgency: Urgency::Normal,
            fill_mt: 1_000,
            capacity_mt: 4_000,
        }
    }

    /// A valid document: a bound component plus the chrome singleton, one
    /// durable input, one local output, and a fully declared platform section.
    fn doc() -> BindingsDocument {
        BindingsDocument {
            v: BINDINGS_DOCUMENT_VERSION,
            components: vec![component("p1"), component("chrome")],
            subscriptions: vec![subscription("p1", "brenn:site.bar.in")],
            outputs: vec![output("p1", LOCAL_THEME_CHANNEL)],
            local_channels: vec![LocalChannel {
                channel: LOCAL_THEME_CHANNEL.to_string(),
                ring_depth: reserved_local_channel(LOCAL_THEME_CHANNEL)
                    .expect("the theme plane is reserved")
                    .ring_depth,
            }],
            chrome_instance: "chrome".to_string(),
            platform: PlatformSection {
                geometry_channel: "brenn:site.surface.bar.geometry".to_string(),
                status_channel: "brenn:site.surface.bar.status".to_string(),
                status_interval_secs: 60,
                error_channel: Some("brenn:site.surface.bar.errors".to_string()),
                error_report_floor: Some(LogLevel::Warn),
            },
        }
    }

    #[test]
    fn round_trips_through_its_body() {
        let doc = doc();
        assert_eq!(BindingsDocument::parse(&doc.to_body()).unwrap(), doc);
    }

    /// The determinism rule the reload check rests on: same input, same bytes.
    /// Rebuilt from scratch rather than cloned, so a field that serialized from
    /// an unordered collection would show up as a flake here.
    #[test]
    fn body_is_byte_stable_across_rebuilds() {
        assert_eq!(doc().to_body(), doc().to_body());
    }

    /// A config map is a `BTreeMap`, so its bytes follow key order rather than
    /// insertion order — the one collection in the document a rebuild could
    /// otherwise permute.
    #[test]
    fn config_map_serializes_in_key_order() {
        let mut a = doc();
        let mut b = doc();
        for (k, v) in [("zeta", "1"), ("alpha", "2"), ("mid", "3")] {
            a.components[0].config.insert(k.to_string(), v.to_string());
        }
        for (k, v) in [("mid", "3"), ("alpha", "2"), ("zeta", "1")] {
            b.components[0].config.insert(k.to_string(), v.to_string());
        }
        assert_eq!(a.to_body(), b.to_body());
        let body = a.to_body();
        let alpha = body.find("alpha").expect("the map reaches the body");
        let mid = body.find("mid").expect("the map reaches the body");
        let zeta = body.find("zeta").expect("the map reaches the body");
        assert!(alpha < mid && mid < zeta, "key order in the body: {body}");
    }

    /// A changed input changes the bytes — the other half of the reload check.
    #[test]
    fn changed_config_changes_the_body() {
        let before = doc().to_body();
        let mut after = doc();
        after.subscriptions[0].push_depth += 1;
        assert_ne!(before, after.to_body());
    }

    #[test]
    fn rejects_an_unreadable_version() {
        let mut doc = doc();
        doc.v = BINDINGS_DOCUMENT_VERSION + 1;
        let err = BindingsDocument::parse(&doc.to_body()).expect_err("a future version is refused");
        assert!(err.contains("schema version"), "unexpected: {err}");
    }

    #[test]
    fn rejects_an_unknown_field() {
        let body = doc().to_body();
        let with_extra = body.replace("{\"v\":", "{\"surprise\":true,\"v\":");
        let err = BindingsDocument::parse(&with_extra).expect_err("an unknown field is refused");
        assert!(err.contains("does not parse"), "unexpected: {err}");
    }

    #[test]
    fn rejects_junk() {
        let err = BindingsDocument::parse("not json").expect_err("junk is refused");
        assert!(err.contains("does not parse"), "unexpected: {err}");
    }

    #[test]
    fn rejects_a_repeated_component_instance() {
        let mut doc = doc();
        doc.components.push(component("p1"));
        let err = doc
            .validate()
            .expect_err("an instance id names one component");
        assert!(err.contains("p1") && err.contains("twice"), "{err}");
    }

    #[test]
    fn rejects_a_repeated_output_port() {
        let mut doc = doc();
        doc.outputs.push(output("p1", "brenn:site.bar.out"));
        let err = doc
            .validate()
            .expect_err("a port publishes onto one channel");
        assert!(err.contains("p1/out") && err.contains("twice"), "{err}");
    }

    /// A duplicated input row hands one port every arriving message twice, which
    /// no reader can tell from two messages.
    #[test]
    fn rejects_a_repeated_input_port() {
        let mut doc = doc();
        doc.subscriptions
            .push(subscription("p1", "brenn:site.bar.other"));
        let err = doc.validate().expect_err("a port reads one channel");
        assert!(err.contains("p1/in") && err.contains("twice"), "{err}");
    }

    /// Two input ports of one instance may read different channels; only the same
    /// port twice is ambiguous.
    #[test]
    fn admits_a_second_input_port_on_one_instance() {
        let mut doc = doc();
        let mut second = subscription("p1", "brenn:site.bar.other");
        second.port = "alt".to_string();
        doc.subscriptions.push(second);
        assert!(doc.validate().is_ok(), "distinct ports are not a conflict");
    }

    /// One address, one ring depth: two entries state two, and folding them by
    /// `max` would pick a winner nobody wrote.
    #[test]
    fn rejects_a_repeated_local_channel() {
        let mut doc = doc();
        doc.local_channels.push(LocalChannel {
            channel: "local:bar/notes".to_string(),
            ring_depth: 4,
        });
        doc.local_channels.push(LocalChannel {
            channel: "local:bar/notes".to_string(),
            ring_depth: 8,
        });
        let err = doc
            .validate()
            .expect_err("a local channel has one ring depth");
        assert!(
            err.contains("local:bar/notes") && err.contains("twice"),
            "{err}"
        );
    }

    /// Two ports of one instance may publish onto different channels; only the
    /// same port twice is ambiguous.
    #[test]
    fn admits_a_second_port_on_one_instance() {
        let mut doc = doc();
        let mut second = output("p1", "brenn:site.bar.out");
        second.port = "alt".to_string();
        doc.outputs.push(second);
        assert!(doc.validate().is_ok(), "distinct ports are not a conflict");
    }

    #[test]
    fn rejects_an_unbindable_scheme() {
        let mut doc = doc();
        doc.subscriptions[0].channel = "mqtt:sensors/kitchen".to_string();
        let err = doc.validate().expect_err("mqtt does not bind to a surface");
        assert!(err.contains("not surface-bindable"), "unexpected: {err}");
    }

    /// The rule reads outputs too, not just subscriptions: an output on an
    /// unroutable channel is equally inexplicable.
    #[test]
    fn rejects_an_unbindable_scheme_on_an_output() {
        let mut doc = doc();
        doc.outputs[0].channel = "webhook:hooks/out".to_string();
        doc.local_channels.clear();
        let err = doc
            .validate()
            .expect_err("webhook does not bind to a surface");
        assert!(err.contains("not surface-bindable"), "unexpected: {err}");
    }

    #[test]
    fn rejects_a_local_binding_with_no_router_entry() {
        let mut doc = doc();
        doc.local_channels.clear();
        let err = doc.validate().expect_err("a local binding needs a ring");
        assert!(err.contains("no router entry"), "unexpected: {err}");
    }

    #[test]
    fn rejects_a_reserved_plane_at_the_wrong_depth() {
        let mut doc = doc();
        doc.local_channels[0].ring_depth += 1;
        let err = doc
            .validate()
            .expect_err("a reserved plane's depth is contract-fixed");
        assert!(
            err.contains("the contract fixes it at"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn rejects_a_binding_naming_an_undeclared_instance() {
        let mut doc = doc();
        doc.subscriptions[0].instance = "ghost".to_string();
        let err = doc
            .validate()
            .expect_err("a binding must name a declared instance");
        assert!(
            err.contains("absent from the component list"),
            "unexpected: {err}"
        );
    }

    /// Outputs wear the instance rule too: a local publish derives its sender
    /// identity from the binding's instance, so an undeclared one is
    /// unattributable.
    #[test]
    fn rejects_an_output_naming_an_undeclared_instance() {
        let mut doc = doc();
        doc.outputs[0].instance = "ghost".to_string();
        let err = doc
            .validate()
            .expect_err("an output must name a declared instance");
        assert!(
            err.contains("absent from the component list"),
            "unexpected: {err}"
        );
    }

    /// The chrome id is checked against the component list like any binding's:
    /// an id naming nobody would otherwise disable chrome silently rather than
    /// refuse the document.
    #[test]
    fn rejects_a_chrome_instance_naming_no_component() {
        let mut doc = doc();
        doc.chrome_instance = "ghost".to_string();
        let err = doc
            .validate()
            .expect_err("chrome must name a declared instance");
        assert!(
            err.contains("absent from the component list"),
            "unexpected: {err}"
        );
    }

    /// Empty is not the chromeless state: a surface declares exactly one chrome
    /// component, so an empty id is a broken writer.
    #[test]
    fn rejects_an_empty_chrome_instance() {
        let mut doc = doc();
        doc.chrome_instance = String::new();
        let err = doc.validate().expect_err("chrome is not optional");
        assert!(
            err.contains("absent from the component list"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn rejects_a_half_declared_error_report_pair() {
        let mut with_channel = doc();
        with_channel.platform.error_report_floor = None;
        let err = with_channel
            .validate()
            .expect_err("a channel with no floor has no admission rule");
        assert!(err.contains("no report floor"), "unexpected: {err}");

        let mut with_floor = doc();
        with_floor.platform.error_channel = None;
        let err = with_floor
            .validate()
            .expect_err("a floor with no channel names a level for nowhere");
        assert!(err.contains("no error channel"), "unexpected: {err}");
    }

    /// The kernel publishes its telemetry to whatever these fields name, so a
    /// blank one is a document that fails at the publish rather than at the gate.
    #[test]
    fn rejects_an_empty_platform_channel() {
        for blank in [
            |p: &mut PlatformSection| p.geometry_channel = String::new(),
            |p: &mut PlatformSection| p.status_channel = String::new(),
            |p: &mut PlatformSection| p.error_channel = Some(String::new()),
        ] {
            let mut doc = doc();
            blank(&mut doc.platform);
            let err = doc
                .validate()
                .expect_err("a platform address must name a channel");
            assert!(
                err.contains("not a channel the kernel can publish across the wire"),
                "unexpected: {err}"
            );
        }
    }

    /// Unbindable and page-local addresses fail alike: the kernel publishes these
    /// documents over the wire, and neither scheme gets there.
    #[test]
    fn rejects_an_unroutable_platform_channel() {
        for address in ["mqtt:sensors/kitchen", LOCAL_THEME_CHANNEL, "site.bar.geo"] {
            let mut doc = doc();
            doc.platform.geometry_channel = address.to_string();
            let err = doc
                .validate()
                .expect_err("geometry does not cross the wire on this scheme");
            assert!(
                err.contains("geometry_channel"),
                "the failing field is named: {err}"
            );
        }
    }

    /// The cadence arms a timer, so the reader holds it to the same bounds the
    /// operator's config is held to — a zero would spin.
    #[test]
    fn rejects_a_status_interval_outside_the_bounds() {
        for secs in [
            0,
            STATUS_INTERVAL_SECS_MIN - 1,
            STATUS_INTERVAL_SECS_MAX + 1,
        ] {
            let mut doc = doc();
            doc.platform.status_interval_secs = secs;
            let err = doc.validate().expect_err("the cadence is bounded");
            assert!(err.contains("status_interval_secs"), "unexpected: {err}");
        }
        for secs in [STATUS_INTERVAL_SECS_MIN, STATUS_INTERVAL_SECS_MAX] {
            let mut doc = doc();
            doc.platform.status_interval_secs = secs;
            assert!(doc.validate().is_ok(), "the bounds themselves are admitted");
        }
    }

    /// A surface that declares no error-report channel at all is valid: the
    /// kernel keeps its console copy and publishes nothing.
    #[test]
    fn accepts_an_absent_error_report_pair() {
        let mut doc = doc();
        doc.platform.error_channel = None;
        doc.platform.error_report_floor = None;
        assert!(doc.validate().is_ok());
    }
}
