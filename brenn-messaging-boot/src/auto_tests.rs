//! Unit tests for the auto-channel lowering pass: the realm/scheme decision, the
//! depth fold, named-channel validation, io_ports, and the resolver-side effects
//! (free-port addresses and injected grants).

use super::auto::{lower_auto_wiring, wasm_endpoint_ref};
use super::test_fixtures::{
    brenn_entry, dir_of, local_sub_raw, minimal_surface_raw, minimal_wasm_consumer, out_raw,
    resolve_with_auto, sub_raw, surface_sub_raw,
};
use super::*;
use brenn_lib::messaging::ComponentGrant;
use brenn_lib::messaging::config::{
    Depth, LinkConfigRaw, LinkEndpointRaw, LinkHostRaw, MessagingGlobalConfig, SurfaceConfigRaw,
    SurfaceIoPortRaw, SurfaceOutputRaw, SurfaceSubscriptionRaw, WasmConsumerConfigRaw,
    WasmConsumerIoPortRaw, WasmConsumerOutputRaw, WasmConsumerSubscriptionRaw,
};

/// Stock globals. Depths are not among them — every auto-channel depth is folded
/// from the ports, so these tests state their depths on the ports themselves.
fn globals() -> MessagingGlobalConfig {
    MessagingGlobalConfig::default()
}

/// A link over `endpoints`. The handle reaches boot messages and nothing else,
/// so every fixture that does not assert on the text uses the same one.
fn link(endpoints: Vec<LinkEndpointRaw>) -> LinkConfigRaw {
    LinkConfigRaw {
        link: "feed".to_string(),
        description: None,
        endpoints,
    }
}

/// An endpoint on a backend consumer's port, roles unset.
fn wasm_ep(slug: &str, port: &str) -> LinkEndpointRaw {
    LinkEndpointRaw {
        host: LinkHostRaw::Wasm {
            slug: slug.to_string(),
        },
        port: port.to_string(),
        publishes: false,
        subscribes: false,
        io_port: false,
        push_depth: None,
        retain_depth: None,
    }
}

/// An endpoint on a surface component's port, roles unset. Every fixture surface
/// here hosts one instance, `protobar`.
fn surface_ep(slug: &str, port: &str) -> LinkEndpointRaw {
    LinkEndpointRaw {
        host: LinkHostRaw::Surface {
            slug: slug.to_string(),
            instance: "protobar".to_string(),
        },
        port: port.to_string(),
        publishes: false,
        subscribes: false,
        io_port: false,
        push_depth: None,
        retain_depth: None,
    }
}

/// A backend `out` port bound to the link.
fn wasm_pub(slug: &str, port: &str) -> LinkEndpointRaw {
    LinkEndpointRaw {
        publishes: true,
        ..wasm_ep(slug, port)
    }
}

/// A backend `in` port bound to the link, with the window its binding stated.
fn wasm_sub(slug: &str, port: &str, push: Depth, retain: Depth) -> LinkEndpointRaw {
    LinkEndpointRaw {
        subscribes: true,
        push_depth: Some(push),
        retain_depth: Some(retain),
        ..wasm_ep(slug, port)
    }
}

/// A backend io_port bound to the link: both roles, and the input half's window.
fn wasm_io(slug: &str, port: &str, push: u64, retain: u64) -> LinkEndpointRaw {
    LinkEndpointRaw {
        publishes: true,
        subscribes: true,
        io_port: true,
        push_depth: Some(Depth::Bounded(push)),
        retain_depth: Some(Depth::Bounded(retain)),
        ..wasm_ep(slug, port)
    }
}

/// A surface `out` port bound to the link.
fn surface_pub(slug: &str, port: &str) -> LinkEndpointRaw {
    LinkEndpointRaw {
        publishes: true,
        ..surface_ep(slug, port)
    }
}

/// A surface `in` port bound to the link, with the window its binding stated.
fn surface_sub(slug: &str, port: &str, push: u64, retain: u64) -> LinkEndpointRaw {
    LinkEndpointRaw {
        subscribes: true,
        push_depth: Some(Depth::Bounded(push)),
        retain_depth: Some(Depth::Bounded(retain)),
        ..surface_ep(slug, port)
    }
}

/// A surface io_port bound to the link. See [`wasm_io`].
fn surface_io(slug: &str, port: &str, push: u64, retain: u64) -> LinkEndpointRaw {
    LinkEndpointRaw {
        publishes: true,
        subscribes: true,
        io_port: true,
        push_depth: Some(Depth::Bounded(push)),
        retain_depth: Some(Depth::Bounded(retain)),
        ..surface_ep(slug, port)
    }
}

/// The endpoint pair every backend fixture wires: [`publisher`]'s `out` and
/// [`subscriber`]'s `tap`, with the subscriber's own declared window.
fn etl_to_indexer(push: u64, retain: u64) -> Vec<LinkEndpointRaw> {
    vec![
        wasm_pub("etl", "out"),
        wasm_sub(
            "indexer",
            "tap",
            Depth::Bounded(push),
            Depth::Bounded(retain),
        ),
    ]
}

/// A free (channel-less) `[[wasm_consumer.subscription]]` on `port`, with the
/// given bounded depths.
fn free_sub(port: &str, push: u64, retain: u64) -> WasmConsumerSubscriptionRaw {
    WasmConsumerSubscriptionRaw {
        channel: None,
        push_depth: Some(Depth::Bounded(push)),
        retain_depth: Some(Depth::Bounded(retain)),
        ..sub_raw("brenn:unused", port)
    }
}

/// A free (channel-less) `[[wasm_consumer.output]]` on `port`.
fn free_out(port: &str) -> WasmConsumerOutputRaw {
    WasmConsumerOutputRaw {
        channel: None,
        ..out_raw(port, "brenn:unused")
    }
}

/// A publishing consumer: one address-bound subscription (so it activates) and
/// one free output port named `out`.
fn publisher(slug: &str) -> WasmConsumerConfigRaw {
    WasmConsumerConfigRaw {
        slug: slug.to_string(),
        grants: vec![ComponentGrant::Ports],
        subscriptions: vec![sub_raw("brenn:feed", "in")],
        outputs: vec![free_out("out")],
        ..minimal_wasm_consumer()
    }
}

/// A subscribing consumer: one free input port named `tap`.
fn subscriber(slug: &str, push: u64, retain: u64) -> WasmConsumerConfigRaw {
    WasmConsumerConfigRaw {
        slug: slug.to_string(),
        subscriptions: vec![free_sub("tap", push, retain)],
        ..minimal_wasm_consumer()
    }
}

/// A consumer whose port `both` is declared twice — once as a free input, once
/// as a free output — so its declaration gives the port both halves without
/// making it an io_port.
fn duplex(slug: &str, push: u64, retain: u64) -> WasmConsumerConfigRaw {
    WasmConsumerConfigRaw {
        slug: slug.to_string(),
        grants: vec![ComponentGrant::Ports],
        subscriptions: vec![free_sub("both", push, retain)],
        outputs: vec![free_out("both")],
        ..minimal_wasm_consumer()
    }
}

/// A `[[wasm_consumer.io_port]]` on `port` with the given bounded depths.
fn io_raw(port: &str, push: u64, retain: u64) -> WasmConsumerIoPortRaw {
    WasmConsumerIoPortRaw {
        port: port.to_string(),
        channel: None,
        push_depth: Some(Depth::Bounded(push)),
        retain_depth: Some(Depth::Bounded(retain)),
        noise: None,
        amplification: None,
        urgency: None,
        publish_per_activation: None,
        publish_capacity: None,
    }
}

/// A `[[surface.io_port]]` on the `protobar` instance's `port`.
fn surface_io_raw(port: &str, push: u64, retain: u64) -> SurfaceIoPortRaw {
    SurfaceIoPortRaw {
        instance: "protobar".to_string(),
        port: port.to_string(),
        channel: None,
        push_depth: Some(Depth::Bounded(push)),
        retain_depth: Some(Depth::Bounded(retain)),
        noise: None,
        urgency: None,
        publish_per_activation: None,
        publish_capacity: None,
    }
}

/// The zero-config timer shape: a consumer whose only port is an io_port.
fn timer_consumer(slug: &str) -> WasmConsumerConfigRaw {
    WasmConsumerConfigRaw {
        slug: slug.to_string(),
        grants: vec![ComponentGrant::Ports],
        io_ports: vec![io_raw("timer", 2, 8)],
        ..minimal_wasm_consumer()
    }
}

/// A surface whose `protobar` instance carries one io_port named `loop`.
fn surface_with_io_port(slug: &str) -> SurfaceConfigRaw {
    SurfaceConfigRaw {
        slug: slug.to_string(),
        io_ports: vec![surface_io_raw("loop", 2, 5)],
        ..minimal_surface_raw()
    }
}

/// A surface with one free input port and one free output port on its
/// `protobar` instance.
fn surface_with_free_ports(slug: &str) -> SurfaceConfigRaw {
    SurfaceConfigRaw {
        slug: slug.to_string(),
        subscriptions: vec![SurfaceSubscriptionRaw {
            channel: None,
            push_depth: Some(Depth::Bounded(2)),
            retain_depth: Some(Depth::Bounded(3)),
            ..surface_sub_raw("brenn:unused", "protobar", "tap")
        }],
        outputs: vec![SurfaceOutputRaw {
            instance: "protobar".to_string(),
            port: "out".to_string(),
            channel: None,
            urgency: None,
            publish_per_activation: None,
            publish_capacity: None,
        }],
        ..minimal_surface_raw()
    }
}

/// Backend-only endpoints never leave the process, so the channel is a `local:`
/// server ring — non-transportable, one entry, ring depth from the fold.
#[test]
fn a_backend_only_link_lowers_to_a_server_local_channel() {
    let consumers = vec![publisher("etl"), subscriber("indexer", 2, 6)];
    let wiring = lower_auto_wiring(
        &[link(etl_to_indexer(2, 6))],
        &consumers,
        &[],
        &[],
        &globals(),
    );
    assert!(wiring.durable_entries().is_empty());
    let entries = wiring.nondurable_entries();
    assert_eq!(entries.len(), 1);
    let entry = &entries[0];
    assert_eq!(entry.transport_type, ChannelScheme::Local);
    assert!(entry.address.starts_with("local:auto."));
    assert_eq!(entry.resolved_channel.retain_depth, Depth::Bounded(6));
    assert_eq!(
        entry.description.as_deref(),
        Some("auto channel: wasm:etl/out, wasm:indexer/tap"),
    );
    assert_eq!(wiring.wasm_channel("etl", "out"), Some(&*entry.address));
    assert_eq!(wiring.wasm_channel("indexer", "tap"), Some(&*entry.address));
}

/// An endpoint set spanning the wire needs a transportable channel, and
/// `ephemeral:` is the transportable non-durable class.
#[test]
fn a_wire_spanning_link_lowers_to_ephemeral() {
    let wiring = lower_auto_wiring(
        &[link(vec![
            wasm_pub("etl", "out"),
            surface_sub("deskbar", "tap", 2, 3),
        ])],
        &[publisher("etl")],
        &[surface_with_free_ports("deskbar")],
        &[],
        &globals(),
    );
    let entries = wiring.nondurable_entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].transport_type, ChannelScheme::Ephemeral);
    assert!(entries[0].address.starts_with("ephemeral:auto."));
}

/// Two surfaces are two pages: their traffic crosses the wire even though no
/// backend port is on the channel.
#[test]
fn a_two_surface_link_lowers_to_ephemeral() {
    let surfaces = vec![
        surface_with_free_ports("deskbar"),
        surface_with_free_ports("wallboard"),
    ];
    let wiring = lower_auto_wiring(
        &[link(vec![
            surface_pub("deskbar", "out"),
            surface_sub("wallboard", "tap", 2, 3),
        ])],
        &[],
        &surfaces,
        &[],
        &globals(),
    );
    let entries = wiring.nondurable_entries();
    assert_eq!(
        entries.len(),
        1,
        "one channel, not one page ring per surface"
    );
    assert_eq!(entries[0].transport_type, ChannelScheme::Ephemeral);
    assert!(entries[0].address.starts_with("ephemeral:auto."));
    assert_eq!(
        wiring.surface_channel("deskbar", "protobar", "out"),
        Some(&*entries[0].address),
    );
    assert_eq!(
        wiring.surface_channel("wallboard", "protobar", "tap"),
        Some(&*entries[0].address),
    );
}

/// One surface's own ports wire page-locally: per session, browser-side, and with
/// no server entry at all — the ring is the surface's, resolved from its bindings.
#[test]
fn a_single_surface_link_lowers_to_a_page_local_channel() {
    let wiring = lower_auto_wiring(
        &[link(vec![
            surface_pub("deskbar", "out"),
            surface_sub("deskbar", "tap", 2, 3),
        ])],
        &[],
        &[surface_with_free_ports("deskbar")],
        &[],
        &globals(),
    );
    assert!(wiring.durable_entries().is_empty());
    assert!(wiring.nondurable_entries().is_empty());
    let address = wiring
        .surface_channel("deskbar", "protobar", "tap")
        .expect("the free port is bound");
    assert!(address.starts_with("local:auto."));
}

/// The address depends on the endpoint *set*, not on declaration order.
#[test]
fn anonymous_address_is_order_independent() {
    let consumers = vec![publisher("etl"), subscriber("indexer", 2, 2)];
    let forward = lower_auto_wiring(
        &[link(etl_to_indexer(2, 2))],
        &consumers,
        &[],
        &[],
        &globals(),
    );
    let mut swapped = etl_to_indexer(2, 2);
    swapped.reverse();
    let reverse = lower_auto_wiring(&[link(swapped)], &consumers, &[], &[], &globals());
    assert_eq!(
        forward.nondurable_entries()[0].address,
        reverse.nondurable_entries()[0].address,
    );
}

// --- Named auto channels ---
//
// A link is anonymous, always. The named arm below is reached only by an io_port
// that gives itself an address, so every fixture here is an io_port.

/// A link's doc comment reaches the directory entry, where the generated
/// endpoint listing would otherwise be all a reader has.
#[test]
fn a_links_description_lands_on_the_synthesized_entry() {
    let consumers = vec![publisher("etl"), subscriber("indexer", 2, 2)];
    let wiring = lower_auto_wiring(
        &[LinkConfigRaw {
            description: Some("ETL batch hand-off".to_string()),
            ..link(etl_to_indexer(2, 2))
        }],
        &consumers,
        &[],
        &[],
        &globals(),
    );
    assert!(wiring.durable_entries().is_empty());
    let entry = &wiring.nondurable_entries()[0];
    assert_eq!(entry.description.as_deref(), Some("ETL batch hand-off"));
}

/// A durable channel may fold to an unbounded ring — its retention is disk, not
/// process memory.
#[test]
fn a_durable_named_channel_accepts_an_unbounded_fold() {
    let mut consumer = timer_consumer("etl");
    consumer.io_ports[0].channel = Some("brenn:etl.batches".to_string());
    consumer.io_ports[0].retain_depth = Some(Depth::Unbounded);
    let wiring = lower_auto_wiring(&[], &[consumer], &[], &[], &globals());
    assert_eq!(
        wiring.durable_entries()[0].resolved_channel.retain_depth,
        Depth::Unbounded,
    );
}

#[test]
#[should_panic(expected = "is in a reserved namespace")]
fn a_named_channel_in_the_auto_namespace_panics() {
    let mut consumer = timer_consumer("etl");
    consumer.io_ports[0].channel = Some("local:auto.mine".to_string());
    lower_auto_wiring(&[], &[consumer], &[], &[], &globals());
}

#[test]
#[should_panic(expected = "carries no scheme prefix")]
fn a_named_channel_without_a_scheme_panics() {
    let mut consumer = timer_consumer("etl");
    consumer.io_ports[0].channel = Some("etl.batches".to_string());
    lower_auto_wiring(&[], &[consumer], &[], &[], &globals());
}

/// The other half of the footgun rule, and the worse half: two auto channels on
/// one address would merge both endpoint sets' injected ACLs, handing each set's
/// ports authority the other set was authorized for.
#[test]
#[should_panic(expected = "is already declared by")]
fn two_io_ports_naming_one_channel_panic() {
    let mut consumers = vec![timer_consumer("etl"), timer_consumer("mailer")];
    consumers[0].io_ports[0].channel = Some("brenn:shared".to_string());
    consumers[1].io_ports[0].channel = Some("brenn:shared".to_string());
    lower_auto_wiring(&[], &consumers, &[], &[], &globals());
}

/// An auto channel wires pub/sub ports. An ingress/egress transport is declared
/// by its own config block, and an address on one carries no pub/sub ACL an auto
/// channel could inject.
#[test]
#[should_panic(expected = "must be a brenn:, ephemeral:, or local: address")]
fn a_named_channel_on_an_ingress_scheme_panics() {
    let mut consumer = timer_consumer("etl");
    consumer.io_ports[0].channel = Some("webhook:hook".to_string());
    lower_auto_wiring(&[], &[consumer], &[], &[], &globals());
}

#[test]
#[should_panic(expected = "must name a channel after its scheme")]
fn a_named_channel_with_an_empty_name_panics() {
    let mut consumer = timer_consumer("etl");
    consumer.io_ports[0].channel = Some("brenn:".to_string());
    lower_auto_wiring(&[], &[consumer], &[], &[], &globals());
}

/// A name outside the charset would pass boot and fail the publish-time
/// well-formedness gate instead — a runtime failure where a boot panic belongs.
#[test]
#[should_panic(expected = "RFC 3986 unreserved characters")]
fn a_named_channel_with_a_bad_charset_panics() {
    let mut consumer = timer_consumer("etl");
    consumer.io_ports[0].channel = Some("brenn:etl batches".to_string());
    lower_auto_wiring(&[], &[consumer], &[], &[], &globals());
}

/// An auto channel inherits every channel-level knob from the global defaults,
/// which includes the one that needs a path to write to.
#[test]
#[should_panic(expected = "archive_path is not set")]
fn archive_sink_without_an_archive_path_panics() {
    let defaults = MessagingGlobalConfig {
        default_sink: brenn_lib::messaging::config::Sink::Archive,
        ..globals()
    };
    let mut consumer = timer_consumer("etl");
    consumer.io_ports[0].channel = Some("brenn:etl.batches".to_string());
    lower_auto_wiring(&[], &[consumer], &[], &[], &defaults);
}

/// The hungriest subscriber sets the ring: max over subscribing endpoints of
/// `max(push_depth, retain_depth)`. Publish-only endpoints contribute nothing.
#[test]
fn fold_takes_the_max_of_maxes_over_subscribers() {
    let consumers = vec![
        publisher("etl"),
        subscriber("slow", 2, 3),
        subscriber("fast", 9, 1),
    ];
    let wiring = lower_auto_wiring(
        &[link(vec![
            wasm_pub("etl", "out"),
            wasm_sub("slow", "tap", Depth::Bounded(2), Depth::Bounded(3)),
            wasm_sub("fast", "tap", Depth::Bounded(9), Depth::Bounded(1)),
        ])],
        &consumers,
        &[],
        &[],
        &globals(),
    );
    assert_eq!(
        wiring.nondurable_entries()[0].resolved_channel.retain_depth,
        Depth::Bounded(9),
    );
}

/// The fold answers every depth question on the synthesized entry, not just
/// retention: the channel-level push rung a later third-party binding reads is
/// the same number, and on a durable channel so is the standing frontier. There
/// is no other stated number for either to come from.
#[test]
fn the_synthesized_entrys_push_and_standing_depths_are_the_fold() {
    let consumers = vec![publisher("etl"), subscriber("slow", 2, 3)];
    let nondurable = lower_auto_wiring(
        &[link(vec![
            wasm_pub("etl", "out"),
            wasm_sub("slow", "tap", Depth::Bounded(2), Depth::Bounded(3)),
        ])],
        &consumers,
        &[],
        &[],
        &globals(),
    );
    let rc = &nondurable.nondurable_entries()[0].resolved_channel;
    assert_eq!(rc.push_depth, Depth::Bounded(3));
    assert_eq!(rc.retain_depth, Depth::Bounded(3));
    assert_eq!(rc.standing_retain_depth, Depth::Bounded(3));

    let mut named = timer_consumer("clock");
    named.io_ports[0].channel = Some("brenn:etl.feed".to_string());
    named.io_ports[0].push_depth = Some(Depth::Bounded(2));
    named.io_ports[0].retain_depth = Some(Depth::Bounded(3));
    let durable = lower_auto_wiring(&[], &[named], &[], &[], &globals());
    let rc = &durable.durable_entries()[0].resolved_channel;
    assert_eq!(rc.push_depth, Depth::Bounded(3));
    assert_eq!(rc.retain_depth, Depth::Bounded(3));
    assert_eq!(rc.standing_retain_depth, Depth::Bounded(3));
}

/// A depth-0 port asks for a ring that retains nothing; the floor keeps the
/// channel able to hold the message it was created to carry.
#[test]
fn fold_has_a_floor_of_one() {
    let consumer = subscriber("indexer", 0, 0);
    let wiring = lower_auto_wiring(
        &[link(etl_to_indexer(0, 0))],
        &[publisher("etl"), consumer],
        &[],
        &[],
        &globals(),
    );
    assert_eq!(
        wiring.nondurable_entries()[0].resolved_channel.retain_depth,
        Depth::Bounded(1),
    );
}

/// An explicitly unbounded port depth still folds unbounded, which a non-durable
/// ring cannot be.
#[test]
#[should_panic(expected = "fold to retain_depth = \"unbounded\"")]
fn unbounded_fold_on_a_nondurable_channel_panics() {
    let mut consumer = subscriber("indexer", 1, 1);
    consumer.subscriptions[0].retain_depth = Some(Depth::Unbounded);
    lower_auto_wiring(
        &[link(vec![
            wasm_pub("etl", "out"),
            wasm_sub("indexer", "tap", Depth::Bounded(1), Depth::Unbounded),
        ])],
        &[publisher("etl"), consumer],
        &[],
        &[],
        &globals(),
    );
}

/// A subscribing endpoint of an auto channel states both depths. Omitting one is
/// refused directly, naming the port — not surfaced later as a confusing
/// unbounded fold on a channel whose address the operator never wrote.
#[test]
#[should_panic(expected = "\"wasm:indexer/tap\" is a subscribing endpoint of an auto channel")]
fn a_subscribing_port_without_a_push_depth_panics() {
    let consumer = subscriber("indexer", 1, 1);
    let mut endpoints = etl_to_indexer(1, 1);
    endpoints[1].push_depth = None;
    lower_auto_wiring(
        &[link(endpoints)],
        &[publisher("etl"), consumer],
        &[],
        &[],
        &globals(),
    );
}

/// The mirror of [`a_subscribing_port_without_a_push_depth_panics`]: neither
/// half is privileged.
#[test]
#[should_panic(expected = "\"wasm:indexer/tap\" is a subscribing endpoint of an auto channel")]
fn a_subscribing_port_without_a_retain_depth_panics() {
    let consumer = subscriber("indexer", 1, 1);
    let mut endpoints = etl_to_indexer(1, 1);
    endpoints[1].retain_depth = None;
    lower_auto_wiring(
        &[link(endpoints)],
        &[publisher("etl"), consumer],
        &[],
        &[],
        &globals(),
    );
}

/// The requirement reaches a surface subscription bound to a link, the one
/// endpoint shape whose reference the operator never writes anywhere: the panic
/// has to name the computed `surface:<slug>#<instance>/<port>` or they are told a
/// depth is missing with no way to find the port.
#[test]
#[should_panic(
    expected = "\"surface:deskbar#protobar/tap\" is a subscribing endpoint of an auto channel"
)]
fn a_link_bound_surface_subscription_without_a_push_depth_panics() {
    let surfaces = vec![surface_with_free_ports("deskbar")];
    let mut tap = surface_sub("deskbar", "tap", 2, 3);
    tap.push_depth = None;
    lower_auto_wiring(
        &[link(vec![wasm_pub("etl", "out"), tap])],
        &[publisher("etl")],
        &surfaces,
        &[],
        &globals(),
    );
}

/// A bare io_port — the zero-config timer shape — must still state its depths:
/// it is the only subscribing endpoint on its channel, so its numbers are the
/// whole fold.
#[test]
#[should_panic(expected = "\"wasm:tick/timer\" is a subscribing endpoint of an auto channel")]
fn a_bare_io_port_without_depths_panics() {
    let mut consumer = timer_consumer("tick");
    consumer.io_ports[0].push_depth = None;
    consumer.io_ports[0].retain_depth = None;
    lower_auto_wiring(&[], &[consumer], &[], &[], &globals());
}

#[test]
#[should_panic(expected = "is already bound by")]
fn a_port_claimed_by_two_links_panics() {
    let consumers = vec![publisher("etl"), subscriber("indexer", 2, 2)];
    lower_auto_wiring(
        &[link(etl_to_indexer(2, 2)), link(etl_to_indexer(2, 2))],
        &consumers,
        &[],
        &[],
        &globals(),
    );
}

#[test]
#[should_panic(expected = "is already bound by")]
fn a_port_bound_twice_to_one_link_panics() {
    let mut endpoints = etl_to_indexer(2, 2);
    endpoints.push(wasm_pub("etl", "out"));
    lower_auto_wiring(
        &[link(endpoints)],
        &[publisher("etl"), subscriber("indexer", 2, 2)],
        &[],
        &[],
        &globals(),
    );
}

#[test]
#[should_panic(expected = "no endpoint subscribes")]
fn a_link_with_no_subscriber_panics() {
    lower_auto_wiring(
        &[link(vec![wasm_pub("etl", "out")])],
        &[publisher("etl")],
        &[],
        &[],
        &globals(),
    );
}

#[test]
#[should_panic(expected = "no endpoint publishes")]
fn a_link_with_no_publisher_panics() {
    lower_auto_wiring(
        &[link(vec![wasm_sub(
            "indexer",
            "tap",
            Depth::Bounded(2),
            Depth::Bounded(2),
        )])],
        &[subscriber("indexer", 2, 2)],
        &[],
        &[],
        &globals(),
    );
}

#[test]
#[should_panic(expected = "endpoints is empty")]
fn a_link_with_no_endpoints_panics() {
    lower_auto_wiring(&[link(vec![])], &[], &[], &[], &globals());
}

/// An endpoint carries its roles rather than resolving them, but the port it
/// names must still be one its host declares: a phantom endpoint perturbs the
/// channel's cid and injects a transport capability and an exact matcher into a
/// real consumer's policy for a port that reads and writes nothing.
#[test]
#[should_panic(expected = "names no declared [[wasm_consumer]]")]
fn a_link_endpoint_on_an_undeclared_consumer_panics() {
    lower_auto_wiring(
        &[link(etl_to_indexer(2, 6))],
        &[publisher("etl")],
        &[],
        &[],
        &globals(),
    );
}

#[test]
#[should_panic(expected = "names no port declared on its host")]
fn a_link_endpoint_on_an_undeclared_port_panics() {
    lower_auto_wiring(
        &[link(vec![
            wasm_pub("etl", "out"),
            wasm_sub("indexer", "nonesuch", Depth::Bounded(2), Depth::Bounded(6)),
        ])],
        &[publisher("etl"), subscriber("indexer", 2, 6)],
        &[],
        &[],
        &globals(),
    );
}

#[test]
#[should_panic(expected = "names no declared [[surface]]")]
fn a_link_endpoint_on_an_undeclared_surface_panics() {
    lower_auto_wiring(
        &[link(vec![
            wasm_pub("etl", "out"),
            surface_sub("deskbar", "tap", 2, 3),
        ])],
        &[publisher("etl")],
        &[],
        &[],
        &globals(),
    );
}

#[test]
#[should_panic(expected = "not declared as a [[surface.component]]")]
fn a_link_endpoint_on_an_undeclared_instance_panics() {
    let endpoint = LinkEndpointRaw {
        host: LinkHostRaw::Surface {
            slug: "deskbar".to_string(),
            instance: "nonesuch".to_string(),
        },
        ..surface_sub("deskbar", "tap", 2, 3)
    };
    lower_auto_wiring(
        &[link(vec![wasm_pub("etl", "out"), endpoint])],
        &[publisher("etl")],
        &[surface_with_free_ports("deskbar")],
        &[],
        &globals(),
    );
}

/// A port that declares only an input publishes nothing, whatever the link says:
/// the roles the endpoint carries are the declaration's, and a mismatch would
/// hand out publish authority nobody wrote down.
#[test]
#[should_panic(expected = "says publishes = true, and its declaration says false")]
fn a_link_endpoint_claiming_a_role_its_port_lacks_panics() {
    lower_auto_wiring(
        &[link(vec![
            wasm_pub("indexer", "tap"),
            wasm_sub("indexer2", "tap", Depth::Bounded(2), Depth::Bounded(6)),
        ])],
        &[subscriber("indexer", 2, 6), subscriber("indexer2", 2, 6)],
        &[],
        &[],
        &globals(),
    );
}

/// The mirror of the publishing case: a link may not claim a subscribing half a
/// port's declaration does not give it.
#[test]
#[should_panic(expected = "says subscribes = true, and its declaration says false")]
fn a_link_endpoint_claiming_a_subscribing_half_its_port_lacks_panics() {
    let endpoint = LinkEndpointRaw {
        publishes: true,
        ..wasm_sub("etl", "out", Depth::Bounded(2), Depth::Bounded(6))
    };
    lower_auto_wiring(
        &[link(vec![endpoint, wasm_pub("etl2", "out")])],
        &[publisher("etl"), publisher("etl2")],
        &[],
        &[],
        &globals(),
    );
}

/// Under-claiming is refused as loudly as over-claiming: a withheld role would
/// bind the port to the link's channel without authorizing it there.
#[test]
#[should_panic(expected = "says publishes = false, and its declaration says true")]
fn a_link_endpoint_withholding_a_role_its_port_declares_panics() {
    lower_auto_wiring(
        &[link(vec![
            wasm_sub("duplex", "both", Depth::Bounded(2), Depth::Bounded(6)),
            wasm_pub("etl", "out"),
        ])],
        &[duplex("duplex", 2, 6), publisher("etl")],
        &[],
        &[],
        &globals(),
    );
}

/// The io_port flag must match in both directions: it affects placement and
/// grants.
#[test]
#[should_panic(expected = "says io_port = true, and its declaration says false")]
fn a_link_endpoint_claiming_io_port_over_a_plain_port_panics() {
    let endpoint = LinkEndpointRaw {
        io_port: true,
        publishes: true,
        ..wasm_sub("duplex", "both", Depth::Bounded(2), Depth::Bounded(6))
    };
    lower_auto_wiring(
        &[link(vec![
            endpoint,
            wasm_sub("indexer", "tap", Depth::Bounded(2), Depth::Bounded(6)),
        ])],
        &[duplex("duplex", 2, 6), subscriber("indexer", 2, 6)],
        &[],
        &[],
        &globals(),
    );
}

#[test]
#[should_panic(expected = "says io_port = false, and its declaration says true")]
fn a_link_endpoint_denying_a_declared_io_port_panics() {
    let endpoint = LinkEndpointRaw {
        io_port: false,
        ..wasm_io("timer", "timer", 2, 8)
    };
    lower_auto_wiring(
        &[link(vec![
            endpoint,
            wasm_sub("indexer", "tap", Depth::Bounded(2), Depth::Bounded(8)),
        ])],
        &[timer_consumer("timer"), subscriber("indexer", 2, 8)],
        &[],
        &[],
        &globals(),
    );
}

/// A surface endpoint's roles come from its instance's declarations exactly as a
/// consumer's do — the same assert over the other host's lookup.
#[test]
#[should_panic(expected = "says publishes = true, and its declaration says false")]
fn a_surface_link_endpoint_claiming_a_role_its_port_lacks_panics() {
    lower_auto_wiring(
        &[link(vec![
            surface_pub("deskbar", "tap"),
            wasm_sub("indexer", "tap", Depth::Bounded(2), Depth::Bounded(6)),
        ])],
        &[subscriber("indexer", 2, 6)],
        &[surface_with_free_ports("deskbar")],
        &[],
        &globals(),
    );
}

#[test]
#[should_panic(expected = "neither publishes nor subscribes")]
fn a_link_endpoint_in_no_direction_panics() {
    lower_auto_wiring(
        &[link(vec![wasm_ep("etl", "out"), wasm_pub("etl2", "out")])],
        &[publisher("etl"), publisher("etl2")],
        &[],
        &[],
        &globals(),
    );
}

/// The ring is folded from the endpoint's numbers and the subscriber's cursor
/// window comes from the declaration's, so a divergence sizes two different
/// windows and drops messages the operator sized for.
#[test]
#[should_panic(expected = "carries the window")]
fn a_link_endpoint_whose_window_differs_from_its_declaration_panics() {
    lower_auto_wiring(
        &[link(vec![
            wasm_pub("etl", "out"),
            wasm_sub("indexer", "tap", Depth::Bounded(2), Depth::Bounded(99)),
        ])],
        &[publisher("etl"), subscriber("indexer", 2, 6)],
        &[],
        &[],
        &globals(),
    );
}

/// Only a subscribing half has a window; one on a publish-only endpoint folds
/// into nothing and would read as tuning that took effect.
#[test]
#[should_panic(expected = "carries a window but does not subscribe")]
fn a_publishing_only_link_endpoint_carrying_a_window_panics() {
    let endpoint = LinkEndpointRaw {
        push_depth: Some(Depth::Bounded(2)),
        retain_depth: Some(Depth::Bounded(6)),
        ..wasm_pub("etl", "out")
    };
    lower_auto_wiring(
        &[link(vec![
            endpoint,
            wasm_sub("indexer", "tap", Depth::Bounded(2), Depth::Bounded(6)),
        ])],
        &[publisher("etl"), subscriber("indexer", 2, 6)],
        &[],
        &[],
        &globals(),
    );
}

/// A port already bound to a channel of its own has its answer; a link claiming
/// it too is two answers to one question.
#[test]
#[should_panic(expected = "already binds channel")]
fn a_link_endpoint_on_a_channel_bound_port_panics() {
    let bound = WasmConsumerConfigRaw {
        slug: "indexer".to_string(),
        subscriptions: vec![sub_raw("brenn:feed", "tap")],
        ..minimal_wasm_consumer()
    };
    lower_auto_wiring(
        &[link(etl_to_indexer(2, 6))],
        &[publisher("etl"), bound],
        &[],
        &[],
        &globals(),
    );
}

// --- Resolver effects ---

/// The end of the whole exercise: two consumers wired by one link resolve with
/// zero operator ACLs and zero `channel` declarations, and every boot coverage
/// assert holds because the link's grants were injected.
#[test]
fn link_bound_ports_resolve_with_no_operator_acls() {
    let consumers = vec![publisher("etl"), subscriber("indexer", 2, 6)];
    let wiring = lower_auto_wiring(
        &[link(etl_to_indexer(2, 6))],
        &consumers,
        &[],
        &[],
        &globals(),
    );
    let address = wiring.nondurable_entries()[0].address.clone();
    let bare = address.strip_prefix("local:").unwrap().to_string();
    let mut entries = wiring.nondurable_entries().to_vec();
    entries.push(brenn_entry("brenn:feed"));
    let resolved = resolve_with_auto(&consumers, &dir_of(entries), &wiring);

    let etl = &resolved[0];
    assert_eq!(etl.outputs[0].channel_address, address);
    assert!(etl.policy.allows_local_publish(&bare));
    let indexer = &resolved[1];
    assert_eq!(indexer.inputs[0].sub.channel_address, address);
    assert_eq!(indexer.inputs[0].sub.push_depth, Depth::Bounded(2));
    assert!(indexer.policy.allows_local_delivery(&bare));
    // The injection is per endpoint, not per channel: neither side gains the
    // other's authority.
    assert!(!etl.policy.allows_local_delivery(&bare));
    assert!(!indexer.policy.allows_local_publish(&bare));
}

/// The document-to-placed-channel claim, end to end: a `.brenn` document
/// declaring a link is the only input, and the ports it wires come out of
/// resolution bound to a channel no one wrote an address for.
///
/// Every other test in this file hands `lower_auto_wiring` raws it built
/// itself, which leaves the lowering pass between a document and those raws
/// unasserted from this side; this one starts a step earlier.
///
/// The indexer's port is an `io` and the etl subscribes to a declared channel
/// because a consumer must reach somewhere with the interface it is granted and
/// must have an input to activate on — the smallest document holding both is
/// this one.
#[test]
fn a_document_link_places_the_channel_its_ports_resolve_to() {
    let config = brenn_lib::config::config_from_dsl(concat!(
        "// ── packaged ──\n",
        "component Etl {\n",
        "    abi = processor; requires = [ports];\n",
        "    in ticks;\n",
        "    out events;\n",
        "}\n",
        "component Indexer {\n",
        "    abi = processor; requires = [ports];\n",
        "    io feed;\n",
        "}\n",
        "// ── packaged ──\n",
        "channel ticks at \"brenn:ticks\" {\n",
        "    push_depth = 1; retain_depth = 1; standing_retain_depth = 1;\n",
        "}\n",
        "link relay;\n",
        "new etl: Etl {\n",
        "    grants = [ports];\n",
        "    in ticks <- ticks { push_depth = 1; retain_depth = 1; }\n",
        "    out events -> relay;\n",
        "}\n",
        "new indexer: Indexer {\n",
        "    grants = [ports];\n",
        "    io feed <-> relay { push_depth = 2; retain_depth = 6; }\n",
        "}\n",
    ));
    let declared: Vec<&str> = config
        .channels
        .iter()
        .filter_map(|channel| channel.address.as_deref())
        .collect();
    let wiring = lower_auto_wiring(
        &config.links,
        &config.wasm_consumers,
        &config.surfaces,
        &declared,
        &config.messaging,
    );

    // Backend-only endpoints, so a server-local ring whose depth is the fold.
    let entries = wiring.nondurable_entries();
    assert_eq!(entries.len(), 1);
    let address = entries[0].address.clone();
    assert_eq!(entries[0].transport_type, ChannelScheme::Local);
    assert_eq!(entries[0].resolved_channel.retain_depth, Depth::Bounded(6));
    assert_eq!(
        entries[0].description.as_deref(),
        Some("auto channel: wasm:etl/events, wasm:indexer/feed"),
    );

    let mut directory = entries.to_vec();
    directory.push(brenn_entry("brenn:ticks"));
    let resolved = resolve_with_auto(&config.wasm_consumers, &dir_of(directory), &wiring);
    let bare = address.strip_prefix("local:").expect("a local address");
    let etl = &resolved[0];
    assert_eq!(etl.outputs[0].channel_address, address);
    assert!(etl.policy.allows_local_publish(bare));
    let indexer = &resolved[1];
    assert_eq!(indexer.inputs[0].sub.channel_address, address);
    assert_eq!(indexer.inputs[0].sub.push_depth, Depth::Bounded(2));
    assert!(indexer.policy.allows_local_delivery(bare));
}

/// Auto-injection means a consumer's ACL lists in config no longer enumerate its
/// full reach, so the boot log is what restores a complete accounting for a
/// config security review: one line per (principal, capability, channel).
#[test]
#[tracing_test::traced_test]
fn every_injected_grant_is_boot_logged() {
    let consumers = vec![publisher("etl"), subscriber("indexer", 2, 6)];
    let wiring = lower_auto_wiring(
        &[link(etl_to_indexer(2, 6))],
        &consumers,
        &[],
        &[],
        &globals(),
    );
    let address = wiring.nondurable_entries()[0].address.clone();
    let mut entries = wiring.nondurable_entries().to_vec();
    entries.push(brenn_entry("brenn:feed"));
    resolve_with_auto(&consumers, &dir_of(entries), &wiring);

    assert!(logs_contain("auto channel grant injected"));
    assert!(logs_contain(&format!(
        "principal=\"wasm:etl\" capability=LocalPublish channel={address}"
    )));
    assert!(logs_contain(&format!(
        "principal=\"wasm:indexer\" capability=LocalSubscribe channel={address}"
    )));
}

/// A consumer that is not an endpoint gains nothing: naming grants nothing, and
/// the address cannot be written into a binding at all.
#[test]
fn a_non_endpoint_consumer_is_not_covered() {
    let consumers = vec![
        publisher("etl"),
        subscriber("indexer", 2, 6),
        WasmConsumerConfigRaw {
            slug: "bystander".to_string(),
            subscriptions: vec![sub_raw("brenn:feed", "in")],
            ..minimal_wasm_consumer()
        },
    ];
    let wiring = lower_auto_wiring(
        &[link(etl_to_indexer(2, 6))],
        &consumers,
        &[],
        &[],
        &globals(),
    );
    let bare = wiring.nondurable_entries()[0]
        .address
        .strip_prefix("local:")
        .unwrap()
        .to_string();
    let mut entries = wiring.nondurable_entries().to_vec();
    entries.push(brenn_entry("brenn:feed"));
    let resolved = resolve_with_auto(&consumers, &dir_of(entries), &wiring);
    let bystander = &resolved[2];
    assert!(!bystander.policy.allows_local_delivery(&bare));
    assert!(!bystander.policy.allows_local_publish(&bare));
}

/// The `ports` grant is a linker-level capability, not ACL boilerplate a link
/// can absorb: without it the publish interface is never linked.
#[test]
#[should_panic(expected = "\"ports\" is not in grants")]
fn a_link_bound_output_still_needs_the_ports_grant() {
    let mut consumers = vec![publisher("etl"), subscriber("indexer", 2, 6)];
    consumers[0].grants = vec![];
    let wiring = lower_auto_wiring(
        &[link(etl_to_indexer(2, 6))],
        &consumers,
        &[],
        &[],
        &globals(),
    );
    let mut entries = wiring.nondurable_entries().to_vec();
    entries.push(brenn_entry("brenn:feed"));
    resolve_with_auto(&consumers, &dir_of(entries), &wiring);
}

/// A surface's own two ports on one page-local channel: the address is lowered
/// onto both bindings and the ring depth comes from the existing per-binding
/// fold, with no server entry and no bus ACL involved.
#[test]
fn surface_free_ports_resolve_onto_a_page_local_channel() {
    let surfaces = vec![surface_with_free_ports("deskbar")];
    let wiring = lower_auto_wiring(
        &[link(vec![
            surface_pub("deskbar", "out"),
            surface_sub("deskbar", "tap", 2, 3),
        ])],
        &[],
        &surfaces,
        &[],
        &globals(),
    );
    let address = wiring
        .surface_channel("deskbar", "protobar", "tap")
        .unwrap()
        .to_string();
    let resolved = resolve_surfaces(&surfaces, &dir_of(vec![]), &globals(), &wiring);
    let surface = &resolved[0];
    assert_eq!(surface.subscriptions[0].channel_address, address);
    assert_eq!(surface.outputs[0].channel_address, address);
    assert_eq!(surface.local_channels[0].address, address);
    assert_eq!(surface.local_channels[0].ring_depth, 3);
    // Page-local traffic has no bus gate, so no grant was injected for it.
    assert!(surface.policy.acls.ephemeral_subscribe.is_empty());
    assert!(surface.policy.acls.brenn_subscribe.is_empty());
}

/// A backend publisher and a surface subscriber span the wire, so the channel is
/// `ephemeral:` and both sides get injected transport grants — the surface's
/// subscription coverage assert passes with no authored ACL.
///
/// `ephemeral:` is the only auto scheme on which a *surface* principal — the
/// least-trusted one in the system — takes a bus-gated grant, so the injection
/// is asserted per role in both directions: a read-only page port must not come
/// away with publish authority on the channel it reads.
#[test]
fn a_wire_spanning_link_injects_grants_on_both_sides() {
    let surfaces = vec![surface_with_free_ports("deskbar")];
    let consumers = [WasmConsumerConfigRaw {
        subscriptions: vec![sub_raw("brenn:feed", "in"), free_sub("in-from-page", 2, 2)],
        ..publisher("etl")
    }];
    let wiring = lower_auto_wiring(
        &[
            link(vec![
                wasm_pub("etl", "out"),
                surface_sub("deskbar", "tap", 2, 3),
            ]),
            link(vec![
                surface_pub("deskbar", "out"),
                wasm_sub("etl", "in-from-page", Depth::Bounded(2), Depth::Bounded(2)),
            ]),
        ],
        &consumers,
        &surfaces,
        &[],
        &globals(),
    );
    let to_page = wiring
        .surface_channel("deskbar", "protobar", "tap")
        .unwrap()
        .to_string();
    let bare = to_page.strip_prefix("ephemeral:").unwrap().to_string();
    let ephemeral: Vec<ChannelEntry> = wiring
        .nondurable_entries()
        .iter()
        .filter(|e| e.transport_type == ChannelScheme::Ephemeral)
        .cloned()
        .collect();
    let resolved = resolve_surfaces(&surfaces, &dir_of(ephemeral.clone()), &globals(), &wiring);
    let surface = &resolved[0];
    assert_eq!(surface.subscriptions[0].channel_address, to_page);
    assert!(surface.policy.allows_ephemeral_delivery(&bare));
    // Its own output rides the second link's channel, publish-granted.
    let from_page = surface.outputs[0]
        .channel_address
        .strip_prefix("ephemeral:")
        .unwrap();
    assert!(surface.policy.allows_ephemeral_publish(from_page));
    assert_ne!(from_page, bare);
    // The consumer's two free ports see the same two channels from the other side.
    assert_eq!(
        wiring.wasm_channel("etl", "out").map(str::to_string),
        Some(to_page),
    );
    assert_eq!(
        wiring.wasm_channel("etl", "in-from-page"),
        Some(&*surface.outputs[0].channel_address),
    );
    assert_eq!(wasm_endpoint_ref("etl", "out"), "wasm:etl/out");

    // Each endpoint holds exactly the role its port declares, on exactly the
    // channel that port rides.
    let from_page = from_page.to_string();
    assert!(!surface.policy.allows_ephemeral_publish(&bare));
    assert!(!surface.policy.allows_ephemeral_delivery(&from_page));
    let mut entries = ephemeral.clone();
    entries.push(brenn_entry("brenn:feed"));
    let consumer = &resolve_with_auto(&consumers, &dir_of(entries), &wiring)[0];
    assert!(consumer.policy.allows_ephemeral_publish(&bare));
    assert!(!consumer.policy.allows_ephemeral_delivery(&bare));
    assert!(consumer.policy.allows_ephemeral_delivery(&from_page));
    assert!(!consumer.policy.allows_ephemeral_publish(&from_page));
}

// --- io_ports ---

/// The zero-config case: no `link`, no `channel`, no `[[channel]]`
/// block — the port gets its own server-side ring, sized by its own depths.
#[test]
fn solo_wasm_io_port_gets_its_own_backend_local_channel() {
    let consumers = vec![timer_consumer("etl")];
    let wiring = lower_auto_wiring(&[], &consumers, &[], &[], &globals());
    assert!(wiring.durable_entries().is_empty());
    let entries = wiring.nondurable_entries();
    assert_eq!(entries.len(), 1);
    let entry = &entries[0];
    assert_eq!(entry.transport_type, ChannelScheme::Local);
    assert!(entry.address.starts_with("local:auto."));
    assert_eq!(entry.resolved_channel.retain_depth, Depth::Bounded(8));
    assert_eq!(
        entry.description.as_deref(),
        Some("auto channel: wasm:etl/timer"),
    );
    assert_eq!(wiring.wasm_channel("etl", "timer"), Some(&*entry.address));
}

/// The point of the block: one name resolves to an input *and* an output on one
/// channel, and the injected grants cover both roles — so a component's
/// `publish-deferred` on the port is a wake it is authorized to receive.
#[test]
fn solo_io_port_resolves_to_an_input_and_an_output_on_one_channel() {
    let consumers = vec![timer_consumer("etl")];
    let wiring = lower_auto_wiring(&[], &consumers, &[], &[], &globals());
    let address = wiring.nondurable_entries()[0].address.clone();
    let bare = address.strip_prefix("local:").unwrap().to_string();
    let entries = wiring.nondurable_entries().to_vec();
    let resolved = resolve_with_auto(&consumers, &dir_of(entries), &wiring);

    let etl = &resolved[0];
    assert_eq!(etl.inputs.len(), 1);
    assert_eq!(etl.outputs.len(), 1);
    assert_eq!(etl.inputs[0].port, "timer");
    assert_eq!(etl.outputs[0].port, "timer");
    assert_eq!(etl.inputs[0].sub.channel_address, address);
    assert_eq!(etl.outputs[0].channel_address, address);
    assert_eq!(etl.inputs[0].sub.push_depth, Depth::Bounded(2));
    assert_eq!(etl.inputs[0].sub.retain_depth, Depth::Bounded(8));
    assert!(etl.policy.allows_local_publish(&bare));
    assert!(etl.policy.allows_local_delivery(&bare));
}

/// Naming the port's channel `brenn:` is what makes a timer's parked schedules
/// survive a restart — the one line that buys durability.
#[test]
fn named_brenn_io_port_channel_is_durable() {
    let mut consumers = vec![timer_consumer("etl")];
    consumers[0].io_ports[0].channel = Some("brenn:etl.timer".to_string());
    let wiring = lower_auto_wiring(&[], &consumers, &[], &[], &globals());
    assert!(wiring.nondurable_entries().is_empty());
    let entry = &wiring.durable_entries()[0];
    assert_eq!(entry.address, "brenn:etl.timer");
    assert_eq!(
        entry.uuid,
        brenn_lib::messaging::durable_auto_channel_uuid("etl.timer"),
    );
    assert_eq!(wiring.wasm_channel("etl", "timer"), Some("brenn:etl.timer"));
}

/// An io_port's `retain_depth` becomes its channel's `retain_depth`, and a
/// channel's `retain_depth` is what caps its channel-wide deferred set. So this
/// one number is also the ceiling on outstanding `deliver_after` schedules the
/// port may hold: a component juggling K parked wakes must declare at least K.
#[test]
fn an_io_ports_retain_depth_becomes_the_channels_deferred_cap() {
    let mut consumers = vec![timer_consumer("etl")];
    consumers[0].io_ports[0].retain_depth = Some(Depth::Bounded(12));
    let wiring = lower_auto_wiring(&[], &consumers, &[], &[], &globals());
    assert_eq!(
        wiring.nondurable_entries()[0].resolved_channel.retain_depth,
        Depth::Bounded(12),
    );
}

/// Two io_ports are two channels: the cid is derived from the endpoint set, and
/// they differ.
#[test]
fn two_io_ports_get_distinct_channels() {
    let mut consumers = vec![timer_consumer("etl")];
    consumers[0].io_ports.push(io_raw("retry", 1, 1));
    let wiring = lower_auto_wiring(&[], &consumers, &[], &[], &globals());
    assert_eq!(wiring.nondurable_entries().len(), 2);
    assert_ne!(
        wiring.wasm_channel("etl", "timer"),
        wiring.wasm_channel("etl", "retry"),
    );
}

/// Bound to a link, the io_port rides that channel and counts as both a
/// publisher and a subscriber on it — so the self-loop survives the other
/// parties joining, and its depths still feed the fold.
#[test]
fn an_io_port_in_a_link_rides_the_links_channel() {
    let consumers = vec![timer_consumer("etl"), subscriber("indexer", 2, 3)];
    let wiring = lower_auto_wiring(
        &[link(vec![
            wasm_io("etl", "timer", 2, 8),
            wasm_sub("indexer", "tap", Depth::Bounded(2), Depth::Bounded(3)),
        ])],
        &consumers,
        &[],
        &[],
        &globals(),
    );
    let entries = wiring.nondurable_entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].resolved_channel.retain_depth, Depth::Bounded(8));
    let address = entries[0].address.clone();
    let bare = address.strip_prefix("local:").unwrap().to_string();
    assert_eq!(wiring.wasm_channel("etl", "timer"), Some(&*address));
    assert_eq!(wiring.wasm_channel("indexer", "tap"), Some(&*address));

    let resolved = resolve_with_auto(&consumers, &dir_of(entries.to_vec()), &wiring);
    let etl = &resolved[0];
    assert_eq!(etl.inputs[0].sub.channel_address, address);
    assert_eq!(etl.outputs[0].channel_address, address);
    assert!(etl.policy.allows_local_publish(&bare));
    assert!(etl.policy.allows_local_delivery(&bare));
    let indexer = &resolved[1];
    assert!(indexer.policy.allows_local_delivery(&bare));
    assert!(!indexer.policy.allows_local_publish(&bare));
}

/// A backend io_port and a surface io_port on one link span the wire, so the
/// channel is `ephemeral:` and each side is granted both of its roles.
#[test]
fn io_ports_spanning_the_wire_share_one_ephemeral_channel() {
    let consumers = vec![timer_consumer("etl")];
    let surfaces = vec![surface_with_io_port("deskbar")];
    let wiring = lower_auto_wiring(
        &[link(vec![
            wasm_io("etl", "timer", 2, 8),
            surface_io("deskbar", "loop", 2, 5),
        ])],
        &consumers,
        &surfaces,
        &[],
        &globals(),
    );
    let entries = wiring.nondurable_entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].transport_type, ChannelScheme::Ephemeral);
    let bare = entries[0].address.strip_prefix("ephemeral:").unwrap();

    let resolved = resolve_surfaces(&surfaces, &dir_of(entries.to_vec()), &globals(), &wiring);
    let surface = &resolved[0];
    assert_eq!(surface.subscriptions[0].channel_address, entries[0].address);
    assert_eq!(surface.outputs[0].channel_address, entries[0].address);
    assert!(surface.policy.allows_ephemeral_delivery(bare));
    assert!(surface.policy.allows_ephemeral_publish(bare));
    // No page-local ring: the channel is on the server, not in the page.
    assert!(surface.local_channels.is_empty());

    let consumer = &resolve_with_auto(&consumers, &dir_of(entries.to_vec()), &wiring)[0];
    assert!(consumer.policy.allows_ephemeral_publish(bare));
    assert!(consumer.policy.allows_ephemeral_delivery(bare));
}

/// A surface io_port's default is page-local: per session, browser-side, no
/// server entry, and its ring resolved by the existing per-binding fold.
#[test]
fn surface_io_port_resolves_to_both_bindings_on_a_page_local_channel() {
    let surfaces = vec![surface_with_io_port("deskbar")];
    let wiring = lower_auto_wiring(&[], &[], &surfaces, &[], &globals());
    assert!(wiring.durable_entries().is_empty());
    assert!(wiring.nondurable_entries().is_empty());
    let address = wiring
        .surface_channel("deskbar", "protobar", "loop")
        .expect("the io_port is placed")
        .to_string();
    assert!(address.starts_with("local:auto."));

    let resolved = resolve_surfaces(&surfaces, &dir_of(vec![]), &globals(), &wiring);
    let surface = &resolved[0];
    assert_eq!(surface.subscriptions.len(), 1);
    assert_eq!(surface.outputs.len(), 1);
    assert_eq!(surface.subscriptions[0].channel_address, address);
    assert_eq!(surface.outputs[0].channel_address, address);
    assert_eq!(surface.local_channels[0].address, address);
    assert_eq!(surface.local_channels[0].ring_depth, 5);
    // Page-local traffic has no bus gate, so nothing was injected for it.
    assert!(surface.policy.acls.ephemeral_subscribe.is_empty());
    assert!(surface.policy.acls.ephemeral_publish.is_empty());
}

/// A page-local io_port makes no server entry, so nothing downstream would catch
/// a missing depth — the fold that refuses an unbounded ring never runs on it.
/// The port-level requirement is what covers the gap: each half is refused at the
/// port, by name, before placement.
#[test]
#[should_panic(
    expected = "\"surface:deskbar#protobar/loop\" is a subscribing endpoint of an auto channel"
)]
fn a_page_local_io_port_with_an_unset_retain_depth_refuses_to_boot() {
    let mut surfaces = vec![surface_with_io_port("deskbar")];
    surfaces[0].io_ports[0].retain_depth = None;
    let stock = MessagingGlobalConfig::default();
    lower_auto_wiring(&[], &[], &surfaces, &[], &stock);
}

/// The port queue is the other half, refused the same way.
#[test]
#[should_panic(
    expected = "\"surface:deskbar#protobar/loop\" is a subscribing endpoint of an auto channel"
)]
fn a_page_local_io_port_with_an_unset_push_depth_refuses_to_boot() {
    let mut surfaces = vec![surface_with_io_port("deskbar")];
    surfaces[0].io_ports[0].push_depth = None;
    let stock = MessagingGlobalConfig::default();
    lower_auto_wiring(&[], &[], &surfaces, &[], &stock);
}

/// Naming a page-local auto channel is how a third component in the page reaches
/// it: the operator's own `local:` binding on the same name is not a collision but
/// the sanctioned attachment, and both bindings fold into the one ring. There is no
/// ACL to leak — page-local traffic has no bus gate.
#[test]
fn a_named_page_local_auto_channel_shares_its_ring_with_an_operator_binding() {
    let mut surfaces = vec![surface_with_io_port("deskbar")];
    surfaces[0].io_ports[0].channel = Some("local:bar.loop".to_string());
    surfaces[0].subscriptions = vec![SurfaceSubscriptionRaw {
        retain_depth: Some(Depth::Bounded(9)),
        ..local_sub_raw("local:bar.loop", "chrome", "snoop")
    }];
    let wiring = lower_auto_wiring(&[], &[], &surfaces, &[], &globals());
    assert!(wiring.durable_entries().is_empty());
    assert!(wiring.nondurable_entries().is_empty());

    let resolved = resolve_surfaces(&surfaces, &dir_of(vec![]), &globals(), &wiring);
    let surface = &resolved[0];
    for sub in &surface.subscriptions {
        assert_eq!(sub.channel_address, "local:bar.loop");
    }
    assert_eq!(surface.outputs[0].channel_address, "local:bar.loop");
    // One ring, sized by the hungriest of the three bindings on it.
    assert_eq!(surface.local_channels.len(), 1);
    assert_eq!(surface.local_channels[0].address, "local:bar.loop");
    assert_eq!(surface.local_channels[0].ring_depth, 9);
    assert!(surface.policy.acls.local_subscribe.is_empty());
    assert!(surface.policy.acls.local_publish.is_empty());
}

/// A port binds exactly one channel: its own `channel` field and a link claiming
/// it are two answers to one question.
#[test]
#[should_panic(expected = "already binds channel")]
fn an_io_port_with_both_a_channel_and_a_link_panics() {
    let mut consumers = vec![timer_consumer("etl"), subscriber("indexer", 2, 3)];
    consumers[0].io_ports[0].channel = Some("brenn:etl.timer".to_string());
    lower_auto_wiring(
        &[link(vec![
            wasm_io("etl", "timer", 2, 8),
            wasm_sub("indexer", "tap", Depth::Bounded(2), Depth::Bounded(3)),
        ])],
        &consumers,
        &[],
        &[],
        &globals(),
    );
}

/// An io_port is already wired to itself, so a link binding it alone is a
/// redundant spelling of the default rather than a second way to say it.
#[test]
#[should_panic(expected = "binds one io_port and nothing else")]
fn a_link_binding_only_one_io_port_panics() {
    lower_auto_wiring(
        &[link(vec![wasm_io("etl", "timer", 2, 8)])],
        &[timer_consumer("etl")],
        &[],
        &[],
        &globals(),
    );
}

/// The footgun rule holds for an io_port's name too.
#[test]
#[should_panic(expected = "is also declared elsewhere")]
fn named_io_port_channel_colliding_with_a_declared_channel_panics() {
    let mut consumers = vec![timer_consumer("etl")];
    consumers[0].io_ports[0].channel = Some("brenn:shared".to_string());
    lower_auto_wiring(&[], &consumers, &[], &["brenn:shared"], &globals());
}

/// One name, both directions — which is exactly why the name may not also be
/// declared in a split list. An address-bound namesake is caught as a port whose
/// name binds two channels.
#[test]
#[should_panic(expected = "the port name is also claimed by an auto channel")]
fn io_port_name_colliding_with_an_address_bound_subscription_panics() {
    let mut consumers = vec![timer_consumer("etl")];
    consumers[0].subscriptions = vec![sub_raw("brenn:feed", "timer")];
    let wiring = lower_auto_wiring(&[], &consumers, &[], &[], &globals());
    let mut entries = wiring.nondurable_entries().to_vec();
    entries.push(brenn_entry("brenn:feed"));
    resolve_with_auto(&consumers, &dir_of(entries), &wiring);
}

/// The io_port registers its name once, in the input direction; a split-list
/// output wearing it is the ordinary duplicate-port-name collision.
#[test]
#[should_panic(expected = "duplicate port name")]
fn io_port_name_colliding_with_an_output_panics() {
    let mut consumers = vec![timer_consumer("etl")];
    consumers[0].outputs = vec![free_out("timer")];
    let wiring = lower_auto_wiring(&[], &consumers, &[], &[], &globals());
    let entries = wiring.nondurable_entries().to_vec();
    resolve_with_auto(&consumers, &dir_of(entries), &wiring);
}

/// The tool-result inbox owns its port name against every declaration kind.
#[test]
#[should_panic(expected = "reserved for the async tool-result inbox")]
fn io_port_named_tool_results_panics() {
    let mut consumers = vec![timer_consumer("etl")];
    consumers[0].io_ports[0].port =
        brenn_tool_registry::bus_wiring::TOOL_RESULT_INPUT_PORT.to_string();
    let wiring = lower_auto_wiring(&[], &consumers, &[], &[], &globals());
    let entries = wiring.nondurable_entries().to_vec();
    resolve_with_auto(&consumers, &dir_of(entries), &wiring);
}

/// An io_port publishes, so it needs the linker-level `ports` grant like any
/// other output — the block absorbs ACL boilerplate, not capabilities.
#[test]
#[should_panic(expected = "\"ports\" is not in grants")]
fn io_port_still_needs_the_ports_grant() {
    let mut consumers = vec![timer_consumer("etl")];
    consumers[0].grants = vec![];
    let wiring = lower_auto_wiring(&[], &consumers, &[], &[], &globals());
    let entries = wiring.nondurable_entries().to_vec();
    resolve_with_auto(&consumers, &dir_of(entries), &wiring);
}

/// The surface bookkeeping is per-direction rather than one cross-direction set,
/// so the io_port's one-name-one-channel guarantee rests on the io bindings being
/// chained into *both* loops. An address-bound namesake arrives as a port whose
/// name binds two channels.
#[test]
#[should_panic(expected = "the port name is also claimed by an auto channel")]
fn surface_io_port_name_colliding_with_an_address_bound_subscription_panics() {
    let mut surfaces = vec![surface_with_io_port("deskbar")];
    surfaces[0].subscriptions = vec![surface_sub_raw("brenn:feed", "protobar", "loop")];
    let wiring = lower_auto_wiring(&[], &[], &surfaces, &[], &globals());
    resolve_surfaces(
        &surfaces,
        &dir_of(vec![brenn_entry("brenn:feed")]),
        &globals(),
        &wiring,
    );
}

/// A *free* namesake takes the io_port's own address, so what catches it is the
/// per-direction duplicate-binding scan the io bindings are chained into.
#[test]
#[should_panic(expected = "duplicate io_port binding")]
fn surface_io_port_name_colliding_with_a_free_output_panics() {
    let mut surfaces = vec![surface_with_io_port("deskbar")];
    surfaces[0].outputs = vec![SurfaceOutputRaw {
        instance: "protobar".to_string(),
        port: "loop".to_string(),
        channel: None,
        urgency: None,
        publish_per_activation: None,
        publish_capacity: None,
    }];
    let wiring = lower_auto_wiring(&[], &[], &surfaces, &[], &globals());
    resolve_surfaces(&surfaces, &dir_of(vec![]), &globals(), &wiring);
}

#[test]
#[should_panic(expected = "not declared as a [[surface.component]]")]
fn surface_io_port_on_an_undeclared_instance_panics() {
    let mut surfaces = vec![surface_with_io_port("deskbar")];
    surfaces[0].io_ports[0].instance = "nonesuch".to_string();
    lower_auto_wiring(&[], &[], &surfaces, &[], &globals());
}

/// `local:` gives each realm a private namespace, so one bare name may name a
/// backend server ring and a surface's page ring at once. They are unrelated
/// channels sharing nothing but the spelling — which is the point of the scheme,
/// and what lets many surfaces stamped from one config template carry identical
/// `local:` names.
#[test]
fn a_surface_local_binding_may_share_a_name_with_a_backend_local_channel() {
    let mut consumers = vec![timer_consumer("etl")];
    consumers[0].io_ports[0].channel = Some("local:etl.tick".to_string());
    let mut surfaces = vec![minimal_surface_raw()];
    surfaces[0].subscriptions = vec![local_sub_raw("local:etl.tick", "protobar", "snoop")];
    let wiring = lower_auto_wiring(&[], &consumers, &surfaces, &[], &globals());

    let resolved = resolve_surfaces(&surfaces, &dir_of(vec![]), &globals(), &wiring);
    assert!(
        resolved[0]
            .local_channels
            .iter()
            .any(|channel| channel.address == "local:etl.tick"),
        "the surface binding declares its own page ring",
    );
    assert!(
        wiring
            .nondurable_entries()
            .iter()
            .any(|entry| entry.address == "local:etl.tick"),
        "the backend io_port declares its own server ring",
    );
}

/// Two channels cannot share a uuid: the identity keys cursors, parked messages,
/// and the DB row, so a clash would interleave both channels' state.
#[test]
#[should_panic(expected = "both carry uuid")]
fn duplicate_channel_uuid_panics() {
    let shared = uuid::Uuid::new_v4();
    let mut a = brenn_entry("brenn:one");
    let mut b = brenn_entry("brenn:two");
    a.uuid = shared;
    b.uuid = shared;
    super::assert_unique_channel_uuids([a, b].iter());
}
