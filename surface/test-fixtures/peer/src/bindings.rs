//! Default-shaped bindings-document builders: the wiring every surface-layer
//! suite composes its fixture page out of.
//!
//! One instance kind, one urgency, one sink budget, one platform section — so a
//! field added to any of the schema's entry types lands here rather than in every
//! suite, and a suite that genuinely varies one knob says so in a thin local
//! wrapper instead of restating the other seven.
//!
//! The `bar` surface, its `p1`/`p2` components and its `chrome` singleton are the
//! shared cast; the addresses a suite binds are the suite's own, since which
//! channel classes are in play is exactly what most of them are about.

use std::collections::BTreeMap;

use brenn_surface_schema::bindings::{
    BINDINGS_DOCUMENT_VERSION, BindingsDocument, PlatformSection,
};
use brenn_surface_schema::{
    Binding, CallBinding, ComponentEntry, ComponentGrant, LocalChannel, NoiseLevel, OutputBinding,
    Urgency, WireChannel,
};

/// The chrome instance every fixture document names, declared like any other
/// component: a surface without one is not a document these suites build.
pub const CHROME: &str = "chrome";

/// A `dom` component of the standard kind, with a parked-batch depth deep enough
/// for one flush.
pub fn component(instance: &str) -> ComponentEntry {
    component_of_kind(instance, "protobar")
}

/// As [`component`], for a suite that cares what kind an instance is — a
/// document's kind is what a remount compares.
///
/// Granted what every in-tree instance is granted, so a suite exercising a
/// privileged seam gets the shape a real page has; a suite about the gate itself
/// says so with [`component_with_grants`].
pub fn component_of_kind(instance: &str, kind: &str) -> ComponentEntry {
    component_with_grants(
        instance,
        kind,
        &[ComponentGrant::Ports, ComponentGrant::Log],
    )
}

/// As [`component_of_kind`], with the capabilities spelled out — for the
/// suite whose subject is what an instance may do, including the ungranted case
/// (`&[]`), which no default may stand in for.
pub fn component_with_grants(
    instance: &str,
    kind: &str,
    grants: &[ComponentGrant],
) -> ComponentEntry {
    ComponentEntry {
        instance: instance.to_string(),
        kind: kind.to_string(),
        parked_batch_depth: 2,
        config: BTreeMap::new(),
        grants: grants.to_vec(),
        // Left empty here and folded from the wiring by [`imply_vocabularies`]:
        // a fixture component states a vocabulary only when the vocabulary is
        // what the suite is about.
        declared_out_ports: vec![],
        sync_ports: vec![],
        call_ports: vec![],
    }
}

/// Give every component that states no port vocabulary the one its own output
/// bindings imply, and complete the wire channel table
/// ([`imply_wire_channels`]).
///
/// The document side of the fixture rule whose home is
/// `brenn_lib::messaging::config::imply_out_port_vocabulary` — a separate
/// implementation only because a document holds bindings in a shape no
/// config-raw type does, and this crate does not depend on that one. The rule
/// itself, escape included, is stated there and not restated here.
pub fn imply_vocabularies(mut doc: BindingsDocument) -> BindingsDocument {
    for c in &mut doc.components {
        if !c.declared_out_ports.is_empty() {
            continue;
        }
        let mut ports: Vec<String> = doc
            .outputs
            .iter()
            .filter(|b| b.instance == c.instance)
            .map(|b| b.port.clone())
            .collect();
        ports.sort();
        ports.dedup();
        c.declared_out_ports = ports;
    }
    imply_wire_channels(doc)
}

/// Give every transportable channel a binding names that has no wire channel
/// entry a text-only one, in first-binding order (subscriptions, then outputs).
/// A suite whose subject is a channel declared `non_text` states that entry
/// itself; an entry already present is left as stated.
pub fn imply_wire_channels(mut doc: BindingsDocument) -> BindingsDocument {
    let bound: Vec<String> = doc.bound_wire_channels().map(str::to_owned).collect();
    for channel in bound {
        if !doc.wire_channels.iter().any(|w| w.channel == channel) {
            doc.wire_channels.push(wire(&channel));
        }
    }
    doc
}

/// One text-only transportable channel declaration.
pub fn wire(channel: &str) -> WireChannel {
    WireChannel {
        channel: channel.to_string(),
        non_text: false,
    }
}

/// One transportable channel declaration, declared `non_text`.
pub fn wire_non_text(channel: &str) -> WireChannel {
    WireChannel {
        non_text: true,
        ..wire(channel)
    }
}

/// One input binding at the caller's depths, counted at the `metered` rung so a
/// loss is on the books.
pub fn subscription(
    instance: &str,
    port: &str,
    channel: &str,
    push_depth: u64,
    retain_depth: u64,
) -> Binding {
    Binding {
        channel: channel.to_string(),
        instance: instance.to_string(),
        port: port.to_string(),
        push_depth,
        retain_depth,
        noise: NoiseLevel::Metered,
    }
}

/// One output binding at the standard urgency and sink budget.
pub fn output(instance: &str, port: &str, channel: &str) -> OutputBinding {
    output_at(instance, port, channel, Urgency::Normal)
}

/// As [`output`], for a suite asserting what urgency an unqualified publish is
/// stamped with.
pub fn output_at(instance: &str, port: &str, channel: &str, urgency: Urgency) -> OutputBinding {
    OutputBinding {
        channel: channel.to_string(),
        instance: instance.to_string(),
        port: port.to_string(),
        urgency,
        fill_mt: 1_000,
        capacity_mt: 4_000,
    }
}

/// One text-only page-local channel declaration at the caller's ring depth.
pub fn local(channel: &str, ring_depth: u64) -> LocalChannel {
    LocalChannel {
        channel: channel.to_string(),
        ring_depth,
        non_text: false,
    }
}

/// One page-local channel declaration at the caller's ring depth, declared
/// `non_text`: its ring may carry bodies that are not UTF-8 text.
pub fn local_non_text(channel: &str, ring_depth: u64) -> LocalChannel {
    LocalChannel {
        non_text: true,
        ..local(channel, ring_depth)
    }
}

/// One call binding: `instance`'s `call` port `port` answered by
/// `target_instance`'s `sync` port `target_port`. The two ports' vocabularies
/// are the caller's to state on the component entries.
pub fn call(instance: &str, port: &str, target_instance: &str, target_port: &str) -> CallBinding {
    CallBinding {
        instance: instance.to_string(),
        port: port.to_string(),
        target_instance: target_instance.to_string(),
        target_port: target_port.to_string(),
    }
}

/// The platform section: telemetry addresses of the `bar` surface, the default
/// cadence, no error reporting and no takeover grant.
pub fn platform() -> PlatformSection {
    PlatformSection {
        geometry_channel: "brenn:site.surface.bar.geometry".to_string(),
        status_channel: "brenn:site.surface.bar.status".to_string(),
        status_interval_secs: 60,
        error_channel: None,
        error_report_floor: None,
    }
}

/// Assemble a document out of the caller's wiring, at the current version and
/// with the standard platform section.
pub fn doc(
    components: Vec<ComponentEntry>,
    subscriptions: Vec<Binding>,
    outputs: Vec<OutputBinding>,
    local_channels: Vec<LocalChannel>,
) -> BindingsDocument {
    imply_vocabularies(BindingsDocument {
        v: BINDINGS_DOCUMENT_VERSION,
        components,
        subscriptions,
        outputs,
        local_channels,
        wire_channels: vec![],
        chrome_instance: CHROME.to_string(),
        platform: platform(),
        calls: vec![],
    })
}

/// `doc`'s body with its first component's grant list naming `telepathy`, a word
/// this build does not know: the body of a server newer than this page.
///
/// The grant list is set here before the splice, so the splice does not depend
/// on any fixture's default grants.
pub fn body_with_unknown_grant(mut doc: BindingsDocument) -> String {
    doc.components[0].grants = vec![ComponentGrant::Ports];
    let body = doc.to_body();
    let skewed = body.replacen(
        r#""grants":["ports"]"#,
        r#""grants":["ports","telepathy"]"#,
        1,
    );
    assert_ne!(skewed, body, "the splice lands on the grant list set above");
    skewed
}
