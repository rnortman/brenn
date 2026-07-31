use std::collections::BTreeMap;

use brenn_surface_schema::bindings::{
    BINDINGS_DOCUMENT_VERSION, BindingsDocument, PlatformSection,
};
use brenn_surface_schema::{
    Abi, Binding, ComponentEntry, LOCAL_LINK_STATE_CHANNEL, LOCAL_THEME_CHANNEL,
    LOCAL_TOAST_CHANNEL, LocalChannel, LogLevel, NoiseLevel, OutputBinding, Urgency,
    reserved_local_channel,
};

use super::AppliedBindings;

const WIRE: &str = "brenn:site.bar.in";
const OTHER_WIRE: &str = "ephemeral:site.bar.signal";

fn component(instance: &str) -> ComponentEntry {
    ComponentEntry {
        instance: instance.to_string(),
        kind: "protobar".to_string(),
        abi: Abi::Dom,
        parked_batch_depth: 4,
        config: BTreeMap::new(),
    }
}

fn subscription(instance: &str, port: &str, channel: &str, push: u64, retain: u64) -> Binding {
    Binding {
        channel: channel.to_string(),
        instance: instance.to_string(),
        port: port.to_string(),
        push_depth: push,
        retain_depth: retain,
        noise: NoiseLevel::Metered,
    }
}

fn output(instance: &str, port: &str, channel: &str) -> OutputBinding {
    OutputBinding {
        channel: channel.to_string(),
        instance: instance.to_string(),
        port: port.to_string(),
        urgency: Urgency::Normal,
        fill_mt: 1_000,
        capacity_mt: 4_000,
    }
}

fn local(channel: &str) -> LocalChannel {
    LocalChannel {
        channel: channel.to_string(),
        ring_depth: reserved_local_channel(channel)
            .map(|r| r.ring_depth)
            .unwrap_or(3),
    }
}

/// Two components on one wire channel at different depths, a page-local plane,
/// and one output per component — enough shape for every index to have
/// something to say.
fn doc() -> BindingsDocument {
    BindingsDocument {
        v: BINDINGS_DOCUMENT_VERSION,
        components: vec![component("p1"), component("p2"), component("chrome")],
        subscriptions: vec![
            subscription("p1", "in", WIRE, 8, 2),
            subscription("p2", "in", WIRE, 3, 9),
            subscription("chrome", "theme", LOCAL_THEME_CHANNEL, 1, 0),
        ],
        outputs: vec![
            output("p1", "out", OTHER_WIRE),
            output("p2", "out", LOCAL_THEME_CHANNEL),
        ],
        local_channels: vec![local(LOCAL_THEME_CHANNEL)],
        chrome_instance: "chrome".to_string(),
        platform: PlatformSection {
            geometry_channel: "brenn:site.surface.bar.geometry".to_string(),
            status_channel: "brenn:site.surface.bar.status".to_string(),
            status_interval_secs: 60,
            error_channel: Some("brenn:site.surface.bar.errors".to_string()),
            error_report_floor: Some(LogLevel::Warn),
            takeover_granted: true,
        },
    }
}

fn applied(doc: &BindingsDocument) -> AppliedBindings {
    AppliedBindings::apply(&doc.to_body()).expect("the fixture document applies")
}

fn refusal(doc: &BindingsDocument) -> String {
    AppliedBindings::apply(&doc.to_body()).expect_err("the document is refused")
}

#[test]
fn a_document_the_schema_refuses_is_refused_here() {
    let mut doc = doc();
    doc.v = BINDINGS_DOCUMENT_VERSION + 1;
    assert!(
        refusal(&doc).contains("schema version"),
        "the schema's own refusal reaches the caller"
    );
}

#[test]
fn junk_that_does_not_parse_is_refused() {
    let err = AppliedBindings::apply("{").expect_err("junk is refused");
    assert!(err.contains("does not parse"), "{err}");
}

#[test]
fn a_repeated_component_instance_is_refused() {
    let mut doc = doc();
    doc.components.push(component("p1"));
    let err = refusal(&doc);
    assert!(err.contains("p1") && err.contains("twice"), "{err}");
}

#[test]
fn a_repeated_output_port_is_refused() {
    let mut doc = doc();
    doc.outputs.push(output("p1", "out", WIRE));
    let err = refusal(&doc);
    assert!(err.contains("p1/out") && err.contains("twice"), "{err}");
}

/// Two ports of one instance may publish onto different channels; only the same
/// port twice is ambiguous.
#[test]
fn a_second_port_on_one_instance_is_admitted() {
    let mut doc = doc();
    doc.outputs.push(output("p1", "alt", WIRE));
    let applied = applied(&doc);
    assert_eq!(
        applied.output("p1", "alt").map(|b| b.channel.as_str()),
        Some(WIRE)
    );
    assert_eq!(applied.outputs_of("p1").count(), 2);
}

#[test]
fn components_read_back_by_instance() {
    let applied = applied(&doc());
    assert_eq!(
        applied.component("p2").map(|c| c.kind.as_str()),
        Some("protobar")
    );
    assert_eq!(
        applied.component("p2").map(|c| c.parked_batch_depth),
        Some(4)
    );
    assert!(applied.is_declared_instance("chrome"));
    assert!(!applied.is_declared_instance("nonesuch"));
    assert!(applied.component("nonesuch").is_none());
    assert_eq!(applied.components().len(), 3);
}

#[test]
fn the_chrome_singleton_and_the_platform_section_read_back() {
    let applied = applied(&doc());
    assert_eq!(applied.chrome_instance(), "chrome");
    assert!(applied.is_declared_instance(applied.chrome_instance()));
    assert_eq!(applied.platform().status_interval_secs, 60);
    assert_eq!(
        applied.platform().error_report_floor,
        Some(LogLevel::Warn),
        "the platform section is handed over whole"
    );
    assert_eq!(applied.local_channels().len(), 1);
}

#[test]
fn an_output_resolves_by_instance_and_port() {
    let applied = applied(&doc());
    let out = applied.output("p1", "out").expect("p1 publishes on out");
    assert_eq!(out.channel, OTHER_WIRE);
    assert_eq!(out.urgency, Urgency::Normal);
    assert!(applied.output("p1", "nonesuch").is_none());
    assert!(
        applied.output("chrome", "out").is_none(),
        "a port is resolved on its own instance, never a sibling's"
    );
}

/// The fan-out table: one arriving envelope is windowed for every binding on the
/// channel, each at its own depths.
#[test]
fn every_binding_on_a_channel_is_listed_in_declaration_order() {
    let applied = applied(&doc());
    let bound: Vec<_> = applied
        .inputs_on(WIRE)
        .map(|b| (b.instance.as_str(), b.push_depth, b.retain_depth))
        .collect();
    assert_eq!(bound, vec![("p1", 8, 2), ("p2", 3, 9)]);
    assert_eq!(applied.inputs_on(LOCAL_THEME_CHANNEL).count(), 1);
    assert_eq!(
        applied.inputs_on("brenn:site.bar.unbound").count(),
        0,
        "a channel nothing binds fans out to nobody"
    );
}

/// The depths stated on the wire are the max across the channel's readers, per
/// knob: p1 wants the deeper push window, p2 the deeper context.
#[test]
fn wire_depths_fold_both_knobs_across_the_channels_readers() {
    let applied = applied(&doc());
    let depths = applied
        .wire_depths(WIRE)
        .expect("the channel is subscribed");
    assert_eq!(depths.push_depth, 8);
    assert_eq!(depths.retain_depth, 9);
}

#[test]
fn a_confined_channel_is_not_subscribed_on_the_wire() {
    let applied = applied(&doc());
    assert!(applied.wire_depths(LOCAL_THEME_CHANNEL).is_none());
    let channels: Vec<_> = applied.wire_channels().map(|(c, _)| c).collect();
    assert_eq!(
        channels,
        vec![WIRE],
        "an output-only channel is not subscribed either"
    );
}

/// One store per channel, deep enough for the deepest window any binding on it
/// reads — the fold of `max(push, retain)` across readers.
#[test]
fn a_wire_channels_store_holds_the_deepest_window_bound_on_it() {
    let applied = applied(&doc());
    assert_eq!(applied.store_depth(WIRE), Some(9));
}

#[test]
fn a_declared_local_channel_gets_a_store_at_its_ring_depth() {
    let mut doc = doc();
    doc.local_channels.push(LocalChannel {
        channel: "local:page/scratch".to_string(),
        ring_depth: 5,
    });
    let applied = applied(&doc);
    assert_eq!(
        applied.store_depth("local:page/scratch"),
        Some(5),
        "a channel nothing binds is still hosted"
    );
}

/// The reserved depth is a floor, not a value: a deeper binding raises it.
#[test]
fn a_reserved_planes_contract_depth_floors_its_store() {
    let mut doc = doc();
    doc.local_channels.push(local(LOCAL_TOAST_CHANNEL));
    doc.subscriptions
        .push(subscription("chrome", "toast", LOCAL_TOAST_CHANNEL, 4, 0));
    doc.local_channels.push(local(LOCAL_LINK_STATE_CHANNEL));
    let applied = applied(&doc);
    assert_eq!(
        applied.store_depth(LOCAL_TOAST_CHANNEL),
        Some(4),
        "the toast plane retains nothing of its own, so its bindings size it"
    );
    assert_eq!(
        applied.store_depth(LOCAL_LINK_STATE_CHANNEL),
        Some(1),
        "an unbound control plane still holds its last value"
    );
    assert_eq!(
        applied.store_depth(LOCAL_THEME_CHANNEL),
        Some(1),
        "a binding shallower than the contract depth does not lower it"
    );
}

#[test]
fn every_stored_channel_is_listed_address_ordered() {
    let applied = applied(&doc());
    let stored: Vec<_> = applied.store_depths().collect();
    assert_eq!(stored, vec![(WIRE, 9), (LOCAL_THEME_CHANNEL, 1)]);
    assert!(applied.store_depth("brenn:site.bar.unbound").is_none());
}

/// The reload decision: same config, same bytes, no reload; a changed knob is a
/// changed body.
#[test]
fn same_wiring_is_byte_equality_of_the_delivered_bodies() {
    let first = applied(&doc());
    let again = applied(&doc());
    assert!(first.same_wiring_as(&again));
    assert_eq!(first.body(), doc().to_body());

    let mut changed = doc();
    changed.subscriptions[0].push_depth = 9;
    assert!(!first.same_wiring_as(&applied(&changed)));
}

/// The sizing refusals, driven against a stated bound: the real bound is
/// `usize::MAX`, which a 64-bit build's `u64` depths never exceed, so a native
/// test can only reach these branches through the bounded entry point.
#[test]
fn a_depth_past_the_bound_is_unusable() {
    let deep = subscription("p1", "in", WIRE, 9, 0);
    let err = super::check_sizable_within(&deep, 8).expect_err("9 does not fit a bound of 8");
    assert!(err.contains("unusable push_depth: 9"), "{err}");

    let wide = subscription("p1", "in", WIRE, 0, 9);
    let err = super::check_sizable_within(&wide, 8).expect_err("9 does not fit a bound of 8");
    assert!(err.contains("unusable retain_depth: 9"), "{err}");

    assert!(
        super::check_sizable_within(&subscription("p1", "in", WIRE, 8, 8), 8).is_ok(),
        "the bound itself is usable"
    );
}

/// A field no accessor indexes is still reachable — the document is held, not
/// consumed.
#[test]
fn the_parsed_document_is_kept() {
    let applied = applied(&doc());
    assert_eq!(applied.document().v, BINDINGS_DOCUMENT_VERSION);
    assert_eq!(applied.document().subscriptions.len(), 3);
}
