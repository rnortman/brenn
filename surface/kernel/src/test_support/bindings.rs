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
    Abi, Binding, ComponentEntry, LocalChannel, NoiseLevel, OutputBinding, Urgency,
};

/// The chrome instance every fixture document names, declared like any other
/// component: a surface without one is not a document these suites build.
pub(crate) const CHROME: &str = "chrome";

/// A `dom` component of the standard kind, with a parked-batch depth deep enough
/// for one flush.
pub(crate) fn component(instance: &str) -> ComponentEntry {
    component_of_kind(instance, "protobar")
}

/// As [`component`], for a suite that cares what kind an instance is — a
/// document's kind is what a remount compares.
pub(crate) fn component_of_kind(instance: &str, kind: &str) -> ComponentEntry {
    ComponentEntry {
        instance: instance.to_string(),
        kind: kind.to_string(),
        abi: Abi::Dom,
        parked_batch_depth: 2,
        config: BTreeMap::new(),
    }
}

/// One input binding at the caller's depths, counted at the `metered` rung so a
/// loss is on the books.
pub(crate) fn subscription(
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
pub(crate) fn output(instance: &str, port: &str, channel: &str) -> OutputBinding {
    output_at(instance, port, channel, Urgency::Normal)
}

/// As [`output`], for a suite asserting what urgency an unqualified publish is
/// stamped with.
pub(crate) fn output_at(
    instance: &str,
    port: &str,
    channel: &str,
    urgency: Urgency,
) -> OutputBinding {
    OutputBinding {
        channel: channel.to_string(),
        instance: instance.to_string(),
        port: port.to_string(),
        urgency,
        fill_mt: 1_000,
        capacity_mt: 4_000,
    }
}

/// One page-local channel declaration at the caller's ring depth.
pub(crate) fn local(channel: &str, ring_depth: u64) -> LocalChannel {
    LocalChannel {
        channel: channel.to_string(),
        ring_depth,
    }
}

/// The platform section: telemetry addresses of the `bar` surface, the default
/// cadence, no error reporting and no takeover grant.
pub(crate) fn platform() -> PlatformSection {
    PlatformSection {
        geometry_channel: "brenn:site.surface.bar.geometry".to_string(),
        status_channel: "brenn:site.surface.bar.status".to_string(),
        status_interval_secs: 60,
        error_channel: None,
        error_report_floor: None,
        takeover_granted: false,
    }
}

/// Assemble a document out of the caller's wiring, at the current version and
/// with the standard platform section.
pub(crate) fn doc(
    components: Vec<ComponentEntry>,
    subscriptions: Vec<Binding>,
    outputs: Vec<OutputBinding>,
    local_channels: Vec<LocalChannel>,
) -> BindingsDocument {
    BindingsDocument {
        v: BINDINGS_DOCUMENT_VERSION,
        components,
        subscriptions,
        outputs,
        local_channels,
        chrome_instance: CHROME.to_string(),
        platform: platform(),
    }
}
