//! Unit tests for the auto-channel lowering pass: the realm/scheme decision, the
//! depth fold, named-channel validation, the endpoint-ref grammar, io_ports, and
//! the resolver-side effects (free-port addresses and injected grants).

use super::auto::{lower_auto_wiring, wasm_endpoint_ref};
use super::test_fixtures::{
    brenn_entry, dir_of, minimal_surface_raw, minimal_wasm_consumer, out_raw, resolve_with_auto,
    sub_raw, surface_sub_raw,
};
use super::*;
use brenn_lib::messaging::config::{
    ConnectionConfigRaw, MessagingGlobalConfig, SurfaceConfigRaw, SurfaceIoPortRaw,
    SurfaceOutputRaw, SurfaceSubscriptionRaw, WasmConsumerConfigRaw, WasmConsumerIoPortRaw,
    WasmConsumerOutputRaw, WasmConsumerSubscriptionRaw, WasmGrant,
};

/// Bounded global depths: an auto channel's ring is process memory, so the stock
/// `Unbounded` defaults would fold to a rejected depth on every non-durable
/// channel. Tests that exercise that rejection use the stock defaults.
fn globals() -> MessagingGlobalConfig {
    MessagingGlobalConfig {
        default_push_depth: Depth::Bounded(4),
        default_retain_depth: Depth::Bounded(4),
        ..Default::default()
    }
}

/// A connection over `endpoints` with no name (anonymous).
fn conn(endpoints: &[&str]) -> ConnectionConfigRaw {
    ConnectionConfigRaw {
        endpoints: endpoints.iter().map(|e| e.to_string()).collect(),
        channel: None,
        uuid: None,
        description: None,
    }
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
        grants: vec![WasmGrant::Ports],
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
        grants: vec![WasmGrant::Ports],
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
fn backend_only_connection_lowers_to_a_server_local_channel() {
    let consumers = vec![publisher("etl"), subscriber("indexer", 2, 6)];
    let wiring = lower_auto_wiring(
        &[conn(&["wasm:etl/out", "wasm:indexer/tap"])],
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
fn wire_spanning_connection_lowers_to_ephemeral() {
    let wiring = lower_auto_wiring(
        &[conn(&["wasm:etl/out", "surface:deskbar#protobar/tap"])],
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
fn two_surface_connection_lowers_to_ephemeral() {
    let surfaces = vec![
        surface_with_free_ports("deskbar"),
        surface_with_free_ports("wallboard"),
    ];
    let wiring = lower_auto_wiring(
        &[conn(&[
            "surface:deskbar#protobar/out",
            "surface:wallboard#protobar/tap",
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
fn single_surface_connection_lowers_to_a_page_local_channel() {
    let wiring = lower_auto_wiring(
        &[conn(&[
            "surface:deskbar#protobar/out",
            "surface:deskbar#protobar/tap",
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
        &[conn(&["wasm:etl/out", "wasm:indexer/tap"])],
        &consumers,
        &[],
        &[],
        &globals(),
    );
    let reverse = lower_auto_wiring(
        &[conn(&["wasm:indexer/tap", "wasm:etl/out"])],
        &consumers,
        &[],
        &[],
        &globals(),
    );
    assert_eq!(
        forward.nondurable_entries()[0].address,
        reverse.nondurable_entries()[0].address,
    );
}

// --- Named auto channels ---

/// Naming a channel `brenn:` is what buys durability, and the entry's identity is
/// derived from the name so no `uuid` line is needed.
#[test]
fn named_brenn_channel_is_durable_with_a_derived_uuid() {
    let consumers = vec![publisher("etl"), subscriber("indexer", 2, 2)];
    let wiring = lower_auto_wiring(
        &[ConnectionConfigRaw {
            channel: Some("brenn:etl.batches".to_string()),
            description: Some("ETL batch hand-off".to_string()),
            ..conn(&["wasm:etl/out", "wasm:indexer/tap"])
        }],
        &consumers,
        &[],
        &[],
        &globals(),
    );
    assert!(wiring.nondurable_entries().is_empty());
    let entry = &wiring.durable_entries()[0];
    assert_eq!(entry.address, "brenn:etl.batches");
    assert_eq!(
        entry.uuid,
        brenn_lib::messaging::durable_auto_channel_uuid("etl.batches"),
    );
    assert_eq!(entry.description.as_deref(), Some("ETL batch hand-off"));
    assert_eq!(wiring.wasm_channel("etl", "out"), Some("brenn:etl.batches"));
}

/// The explicit `uuid` is the rename-stability opt-out, so it wins over the
/// derivation.
#[test]
fn explicit_uuid_names_the_durable_row() {
    let pinned = "9c1f4a4e-6d38-4a2e-9f1a-2f7c0d5b8e31";
    let wiring = lower_auto_wiring(
        &[ConnectionConfigRaw {
            channel: Some("brenn:etl.batches".to_string()),
            uuid: Some(pinned.to_string()),
            ..conn(&["wasm:etl/out", "wasm:indexer/tap"])
        }],
        &[publisher("etl"), subscriber("indexer", 2, 2)],
        &[],
        &[],
        &globals(),
    );
    assert_eq!(
        wiring.durable_entries()[0].uuid,
        uuid::Uuid::parse_str(pinned).unwrap(),
    );
}

/// A durable channel may fold to an unbounded ring — its retention is disk, not
/// process memory.
#[test]
fn durable_named_channel_accepts_an_unbounded_fold() {
    let mut consumer = subscriber("indexer", 1, 1);
    consumer.subscriptions[0].retain_depth = Some(Depth::Unbounded);
    let wiring = lower_auto_wiring(
        &[ConnectionConfigRaw {
            channel: Some("brenn:etl.batches".to_string()),
            ..conn(&["wasm:etl/out", "wasm:indexer/tap"])
        }],
        &[publisher("etl"), consumer],
        &[],
        &[],
        &globals(),
    );
    assert_eq!(
        wiring.durable_entries()[0].resolved_channel.retain_depth,
        Depth::Unbounded,
    );
}

#[test]
#[should_panic(expected = "local: cannot span the wire")]
fn named_local_channel_spanning_the_wire_panics() {
    lower_auto_wiring(
        &[ConnectionConfigRaw {
            channel: Some("local:etl.batches".to_string()),
            ..conn(&["wasm:etl/out", "surface:deskbar#protobar/tap"])
        }],
        &[publisher("etl")],
        &[surface_with_free_ports("deskbar")],
        &[],
        &globals(),
    );
}

#[test]
#[should_panic(expected = "is in a reserved namespace")]
fn named_channel_in_the_auto_namespace_panics() {
    lower_auto_wiring(
        &[ConnectionConfigRaw {
            channel: Some("local:auto.mine".to_string()),
            ..conn(&["wasm:etl/out", "wasm:indexer/tap"])
        }],
        &[publisher("etl"), subscriber("indexer", 2, 2)],
        &[],
        &[],
        &globals(),
    );
}

#[test]
#[should_panic(expected = "carries no scheme prefix")]
fn named_channel_without_a_scheme_panics() {
    lower_auto_wiring(
        &[ConnectionConfigRaw {
            channel: Some("etl.batches".to_string()),
            ..conn(&["wasm:etl/out", "wasm:indexer/tap"])
        }],
        &[publisher("etl"), subscriber("indexer", 2, 2)],
        &[],
        &[],
        &globals(),
    );
}

/// The footgun rule: an auto channel whose address another declaration already
/// owns would leak its injected ACLs onto a channel other parties legitimately
/// use.
#[test]
#[should_panic(expected = "is also declared elsewhere")]
fn named_channel_colliding_with_a_declared_channel_panics() {
    lower_auto_wiring(
        &[ConnectionConfigRaw {
            channel: Some("brenn:shared".to_string()),
            ..conn(&["wasm:etl/out", "wasm:indexer/tap"])
        }],
        &[publisher("etl"), subscriber("indexer", 2, 2)],
        &[],
        &["brenn:shared"],
        &globals(),
    );
}

/// The other half of the footgun rule, and the worse half: two auto channels on
/// one address would merge both endpoint sets' injected ACLs, handing each
/// connection's endpoints authority the other connection authorized.
#[test]
#[should_panic(expected = "is already declared by")]
fn two_connections_naming_one_channel_panic() {
    let consumers = vec![
        publisher("etl"),
        subscriber("indexer", 2, 2),
        publisher("mailer"),
        subscriber("archiver", 2, 2),
    ];
    lower_auto_wiring(
        &[
            ConnectionConfigRaw {
                channel: Some("brenn:shared".to_string()),
                ..conn(&["wasm:etl/out", "wasm:indexer/tap"])
            },
            ConnectionConfigRaw {
                channel: Some("brenn:shared".to_string()),
                ..conn(&["wasm:mailer/out", "wasm:archiver/tap"])
            },
        ],
        &consumers,
        &[],
        &[],
        &globals(),
    );
}

/// The same rule across the two spellings: an io_port's own name and a
/// connection's name are one address space.
#[test]
#[should_panic(expected = "is already declared by")]
fn an_io_port_and_a_connection_naming_one_channel_panic() {
    let mut consumers = vec![
        timer_consumer("etl"),
        publisher("mailer"),
        subscriber("archiver", 2, 2),
    ];
    consumers[0].io_ports[0].channel = Some("brenn:shared".to_string());
    lower_auto_wiring(
        &[ConnectionConfigRaw {
            channel: Some("brenn:shared".to_string()),
            ..conn(&["wasm:mailer/out", "wasm:archiver/tap"])
        }],
        &consumers,
        &[],
        &[],
        &globals(),
    );
}

/// A connection wires pub/sub ports. An ingress/egress transport is declared by
/// its own config block, and an address on one carries no pub/sub ACL an auto
/// channel could inject.
#[test]
#[should_panic(expected = "must be a brenn:, ephemeral:, or local: address")]
fn named_channel_on_an_ingress_scheme_panics() {
    lower_auto_wiring(
        &[ConnectionConfigRaw {
            channel: Some("webhook:hook".to_string()),
            ..conn(&["wasm:etl/out", "wasm:indexer/tap"])
        }],
        &[publisher("etl"), subscriber("indexer", 2, 2)],
        &[],
        &[],
        &globals(),
    );
}

#[test]
#[should_panic(expected = "must name a channel after its scheme")]
fn named_channel_with_an_empty_name_panics() {
    lower_auto_wiring(
        &[ConnectionConfigRaw {
            channel: Some("brenn:".to_string()),
            ..conn(&["wasm:etl/out", "wasm:indexer/tap"])
        }],
        &[publisher("etl"), subscriber("indexer", 2, 2)],
        &[],
        &[],
        &globals(),
    );
}

/// A name outside the charset would pass boot and fail the publish-time
/// well-formedness gate instead — a runtime failure where a boot panic belongs.
#[test]
#[should_panic(expected = "RFC 3986 unreserved characters")]
fn named_channel_with_a_bad_charset_panics() {
    lower_auto_wiring(
        &[ConnectionConfigRaw {
            channel: Some("brenn:etl batches".to_string()),
            ..conn(&["wasm:etl/out", "wasm:indexer/tap"])
        }],
        &[publisher("etl"), subscriber("indexer", 2, 2)],
        &[],
        &[],
        &globals(),
    );
}

#[test]
#[should_panic(expected = "is not a valid UUID")]
fn unparseable_connection_uuid_panics() {
    lower_auto_wiring(
        &[ConnectionConfigRaw {
            channel: Some("brenn:etl.batches".to_string()),
            uuid: Some("not-a-uuid".to_string()),
            ..conn(&["wasm:etl/out", "wasm:indexer/tap"])
        }],
        &[publisher("etl"), subscriber("indexer", 2, 2)],
        &[],
        &[],
        &globals(),
    );
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
    lower_auto_wiring(
        &[ConnectionConfigRaw {
            channel: Some("brenn:etl.batches".to_string()),
            ..conn(&["wasm:etl/out", "wasm:indexer/tap"])
        }],
        &[publisher("etl"), subscriber("indexer", 2, 2)],
        &[],
        &[],
        &defaults,
    );
}

#[test]
#[should_panic(expected = "uuid is set but no channel name is")]
fn uuid_on_an_anonymous_connection_panics() {
    lower_auto_wiring(
        &[ConnectionConfigRaw {
            uuid: Some("9c1f4a4e-6d38-4a2e-9f1a-2f7c0d5b8e31".to_string()),
            ..conn(&["wasm:etl/out", "wasm:indexer/tap"])
        }],
        &[publisher("etl"), subscriber("indexer", 2, 2)],
        &[],
        &[],
        &globals(),
    );
}

#[test]
#[should_panic(expected = "uuid is set but channel")]
fn uuid_on_a_nondurable_named_connection_panics() {
    lower_auto_wiring(
        &[ConnectionConfigRaw {
            channel: Some("ephemeral:etl.batches".to_string()),
            uuid: Some("9c1f4a4e-6d38-4a2e-9f1a-2f7c0d5b8e31".to_string()),
            ..conn(&["wasm:etl/out", "wasm:indexer/tap"])
        }],
        &[publisher("etl"), subscriber("indexer", 2, 2)],
        &[],
        &[],
        &globals(),
    );
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
        &[conn(&["wasm:etl/out", "wasm:slow/tap", "wasm:fast/tap"])],
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

/// A depth-0 port asks for a ring that retains nothing; the floor keeps the
/// channel able to hold the message it was created to carry.
#[test]
fn fold_has_a_floor_of_one() {
    let consumer = subscriber("indexer", 0, 0);
    let wiring = lower_auto_wiring(
        &[conn(&["wasm:etl/out", "wasm:indexer/tap"])],
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

/// The stock global defaults are unbounded, which a non-durable ring cannot be.
#[test]
#[should_panic(expected = "fold to retain_depth = \"unbounded\"")]
fn unbounded_fold_on_a_nondurable_channel_panics() {
    let mut consumer = subscriber("indexer", 1, 1);
    consumer.subscriptions[0].push_depth = None;
    consumer.subscriptions[0].retain_depth = None;
    lower_auto_wiring(
        &[conn(&["wasm:etl/out", "wasm:indexer/tap"])],
        &[publisher("etl"), consumer],
        &[],
        &[],
        &MessagingGlobalConfig::default(),
    );
}

/// The fold takes `max(push_depth, retain_depth)` per subscribing port, so
/// bounding the retain half alone does not save the channel: the unwritten push
/// half still inherits the stock unbounded global and dominates the max.
#[test]
#[should_panic(expected = "fold to retain_depth = \"unbounded\"")]
fn an_inherited_push_depth_folds_unbounded_despite_a_bounded_retain_depth() {
    let mut consumer = subscriber("indexer", 1, 1);
    consumer.subscriptions[0].push_depth = None;
    consumer.subscriptions[0].retain_depth = Some(Depth::Bounded(4));
    lower_auto_wiring(
        &[conn(&["wasm:etl/out", "wasm:indexer/tap"])],
        &[publisher("etl"), consumer],
        &[],
        &[],
        &MessagingGlobalConfig::default(),
    );
}

/// The mirror of
/// [`an_inherited_push_depth_folds_unbounded_despite_a_bounded_retain_depth`]:
/// neither half is privileged, so bounding push alone fails the same way.
#[test]
#[should_panic(expected = "fold to retain_depth = \"unbounded\"")]
fn an_inherited_retain_depth_folds_unbounded_despite_a_bounded_push_depth() {
    let mut consumer = subscriber("indexer", 1, 1);
    consumer.subscriptions[0].push_depth = Some(Depth::Bounded(4));
    consumer.subscriptions[0].retain_depth = None;
    lower_auto_wiring(
        &[conn(&["wasm:etl/out", "wasm:indexer/tap"])],
        &[publisher("etl"), consumer],
        &[],
        &[],
        &MessagingGlobalConfig::default(),
    );
}

#[test]
#[should_panic(expected = "is malformed")]
fn endpoint_with_an_unknown_prefix_panics() {
    lower_auto_wiring(
        &[conn(&["app:etl/out", "wasm:indexer/tap"])],
        &[publisher("etl"), subscriber("indexer", 2, 2)],
        &[],
        &[],
        &globals(),
    );
}

#[test]
#[should_panic(expected = "is malformed")]
fn wasm_endpoint_without_a_port_panics() {
    lower_auto_wiring(
        &[conn(&["wasm:etl", "wasm:indexer/tap"])],
        &[publisher("etl"), subscriber("indexer", 2, 2)],
        &[],
        &[],
        &globals(),
    );
}

#[test]
#[should_panic(expected = "names no declared [[wasm_consumer]]")]
fn endpoint_naming_an_unknown_consumer_panics() {
    lower_auto_wiring(
        &[conn(&["wasm:nonesuch/out", "wasm:indexer/tap"])],
        &[subscriber("indexer", 2, 2)],
        &[],
        &[],
        &globals(),
    );
}

#[test]
#[should_panic(expected = "names no port declared on [[wasm_consumer]]")]
fn endpoint_naming_an_undeclared_port_panics() {
    lower_auto_wiring(
        &[conn(&["wasm:etl/nonesuch", "wasm:indexer/tap"])],
        &[publisher("etl"), subscriber("indexer", 2, 2)],
        &[],
        &[],
        &globals(),
    );
}

#[test]
#[should_panic(expected = "names instance")]
fn surface_endpoint_naming_an_undeclared_instance_panics() {
    lower_auto_wiring(
        &[conn(&["wasm:etl/out", "surface:deskbar#nonesuch/tap"])],
        &[publisher("etl")],
        &[surface_with_free_ports("deskbar")],
        &[],
        &globals(),
    );
}

/// The message the pass rejects `reference` with, driven through the real
/// lowering pass against a fixture that declares `etl`, `indexer`, and the
/// `deskbar` surface. The endpoint ref is the whole operator-facing syntax of
/// the feature, so each rejection is pinned by the text that names the mistake.
fn endpoint_rejection(reference: &str) -> String {
    let consumers = vec![publisher("etl"), subscriber("indexer", 2, 2)];
    let surfaces = vec![surface_with_free_ports("deskbar")];
    let connection = ConnectionConfigRaw {
        endpoints: vec![reference.to_string(), "wasm:indexer/tap".to_string()],
        ..conn(&[])
    };
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        lower_auto_wiring(
            std::slice::from_ref(&connection),
            &consumers,
            &surfaces,
            &[],
            &globals(),
        );
    }))
    .expect_err("the endpoint ref must be rejected");
    std::panic::set_hook(previous);
    payload
        .downcast::<String>()
        .expect("config panics carry formatted messages")
        .to_string()
}

/// Every shape the grammar cannot parse lands on one message that spells both
/// forms out, so the operator sees the syntax rather than the branch that
/// tripped.
#[test]
fn malformed_endpoint_refs_are_rejected() {
    for reference in [
        "wasm:/out",                 // empty slug
        "wasm:etl/",                 // empty port
        "wasm:etl/bad port",         // port outside the unreserved charset
        "surface:deskbar/tap",       // no `#instance`
        "surface:#protobar/tap",     // empty slug
        "surface:deskbar#/tap",      // empty instance
        "surface:deskbar#protobar/", // empty port
        "surface:deskbar#protobar/bad port",
    ] {
        let message = endpoint_rejection(reference);
        assert!(
            message.contains("is malformed"),
            "endpoint {reference:?} must be rejected as malformed, got {message:?}",
        );
    }
}

/// The surface twin of the unknown-consumer rejection: a ref naming no declared
/// block is dead config either way.
#[test]
fn endpoint_naming_an_unknown_surface_panics() {
    let message = endpoint_rejection("surface:nonesuch#protobar/out");
    assert!(
        message.contains("names no declared [[surface]]"),
        "got {message:?}",
    );
}

/// A connection binds ports that already declare themselves; it cannot mint one.
#[test]
fn surface_endpoint_naming_an_undeclared_port_panics() {
    let message = endpoint_rejection("surface:deskbar#protobar/nonesuch");
    assert!(
        message.contains("names no port declared on surface"),
        "got {message:?}",
    );
}

/// The one-channel-per-port rule holds on the surface side too.
#[test]
#[should_panic(expected = "already binds channel")]
fn endpoint_naming_an_address_bound_surface_port_panics() {
    let mut surfaces = vec![surface_with_free_ports("deskbar")];
    surfaces[0].outputs[0].channel = Some("brenn:elsewhere".to_string());
    lower_auto_wiring(
        &[conn(&["surface:deskbar#protobar/out", "wasm:indexer/tap"])],
        &[subscriber("indexer", 2, 2)],
        &surfaces,
        &[],
        &globals(),
    );
}

/// A port binds exactly one channel: an address on the binding and a connection
/// claiming it are two answers to one question.
#[test]
#[should_panic(expected = "already binds channel")]
fn endpoint_naming_an_address_bound_port_panics() {
    let mut consumer = publisher("etl");
    consumer.outputs[0].channel = Some("brenn:elsewhere".to_string());
    lower_auto_wiring(
        &[conn(&["wasm:etl/out", "wasm:indexer/tap"])],
        &[consumer, subscriber("indexer", 2, 2)],
        &[],
        &[],
        &globals(),
    );
}

#[test]
#[should_panic(expected = "is already bound by")]
fn port_claimed_by_two_connections_panics() {
    let consumers = vec![publisher("etl"), subscriber("indexer", 2, 2)];
    lower_auto_wiring(
        &[
            conn(&["wasm:etl/out", "wasm:indexer/tap"]),
            conn(&["wasm:etl/out", "wasm:indexer/tap"]),
        ],
        &consumers,
        &[],
        &[],
        &globals(),
    );
}

#[test]
#[should_panic(expected = "is already bound by")]
fn port_listed_twice_in_one_connection_panics() {
    lower_auto_wiring(
        &[conn(&["wasm:etl/out", "wasm:indexer/tap", "wasm:etl/out"])],
        &[publisher("etl"), subscriber("indexer", 2, 2)],
        &[],
        &[],
        &globals(),
    );
}

#[test]
#[should_panic(expected = "no endpoint subscribes")]
fn connection_with_no_subscriber_panics() {
    lower_auto_wiring(
        &[conn(&["wasm:etl/out"])],
        &[publisher("etl")],
        &[],
        &[],
        &globals(),
    );
}

#[test]
#[should_panic(expected = "no endpoint publishes")]
fn connection_with_no_publisher_panics() {
    lower_auto_wiring(
        &[conn(&["wasm:indexer/tap"])],
        &[subscriber("indexer", 2, 2)],
        &[],
        &[],
        &globals(),
    );
}

#[test]
#[should_panic(expected = "endpoints is empty")]
fn connection_with_no_endpoints_panics() {
    lower_auto_wiring(&[conn(&[])], &[], &[], &[], &globals());
}

// --- Resolver effects ---

/// The end of the whole exercise: two consumers wired by one connection resolve
/// with zero operator ACLs and zero `[[channel]]` blocks, and every boot coverage
/// assert holds because the connection's grants were injected.
#[test]
fn connection_bound_ports_resolve_with_no_operator_acls() {
    let consumers = vec![publisher("etl"), subscriber("indexer", 2, 6)];
    let wiring = lower_auto_wiring(
        &[conn(&["wasm:etl/out", "wasm:indexer/tap"])],
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

/// Auto-injection means a consumer's ACL lists in config no longer enumerate its
/// full reach, so the boot log is what restores a complete accounting for a
/// config security review: one line per (principal, capability, channel).
#[test]
#[tracing_test::traced_test]
fn every_injected_grant_is_boot_logged() {
    let consumers = vec![publisher("etl"), subscriber("indexer", 2, 6)];
    let wiring = lower_auto_wiring(
        &[conn(&["wasm:etl/out", "wasm:indexer/tap"])],
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
        &[conn(&["wasm:etl/out", "wasm:indexer/tap"])],
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

/// The `ports` grant is a linker-level capability, not ACL boilerplate a
/// connection can absorb: without it the publish interface is never linked.
#[test]
#[should_panic(expected = "\"ports\" is not in grants")]
fn connection_bound_output_still_needs_the_ports_grant() {
    let mut consumers = vec![publisher("etl"), subscriber("indexer", 2, 6)];
    consumers[0].grants = vec![];
    let wiring = lower_auto_wiring(
        &[conn(&["wasm:etl/out", "wasm:indexer/tap"])],
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
        &[conn(&[
            "surface:deskbar#protobar/out",
            "surface:deskbar#protobar/tap",
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
    let resolved = resolve_surfaces(&surfaces, &dir_of(vec![]), &[], &globals(), &wiring);
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
fn wire_spanning_connection_injects_grants_on_both_sides() {
    let surfaces = vec![surface_with_free_ports("deskbar")];
    let consumers = [WasmConsumerConfigRaw {
        subscriptions: vec![sub_raw("brenn:feed", "in"), free_sub("in-from-page", 2, 2)],
        ..publisher("etl")
    }];
    let wiring = lower_auto_wiring(
        &[
            conn(&["wasm:etl/out", "surface:deskbar#protobar/tap"]),
            conn(&["surface:deskbar#protobar/out", "wasm:etl/in-from-page"]),
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
    let resolved = resolve_surfaces(&surfaces, &dir_of(vec![]), &ephemeral, &globals(), &wiring);
    let surface = &resolved[0];
    assert_eq!(surface.subscriptions[0].channel_address, to_page);
    assert!(surface.policy.allows_ephemeral_delivery(&bare));
    // Its own output rides the second connection's channel, publish-granted.
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

/// Naming a surface-only connection with an explicit `ephemeral:` address moves
/// its channel onto the server ring instead of the default page ring, so every
/// session of that surface shares one copy.
#[test]
fn a_named_ephemeral_channel_lands_on_the_server_ring_not_the_page() {
    let surfaces = vec![surface_with_free_ports("deskbar")];
    let wiring = lower_auto_wiring(
        &[ConnectionConfigRaw {
            channel: Some("ephemeral:page.share".to_string()),
            ..conn(&[
                "surface:deskbar#protobar/out",
                "surface:deskbar#protobar/tap",
            ])
        }],
        &[],
        &surfaces,
        &[],
        &globals(),
    );
    assert!(wiring.durable_entries().is_empty());
    let entries = wiring.nondurable_entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].address, "ephemeral:page.share");
    assert_eq!(entries[0].transport_type, ChannelScheme::Ephemeral);
    assert_eq!(
        entries[0].uuid,
        brenn_lib::messaging::nondurable_channel_uuid(ChannelScheme::Ephemeral, "page.share"),
    );

    let resolved = resolve_surfaces(&surfaces, &dir_of(vec![]), entries, &globals(), &wiring);
    let surface = &resolved[0];
    assert_eq!(
        surface.subscriptions[0].channel_address,
        "ephemeral:page.share",
    );
    assert_eq!(surface.outputs[0].channel_address, "ephemeral:page.share");
    // Named ephemeral is the opposite of the page-local default: no page ring at
    // all, and the traffic is bus-gated on both roles.
    assert!(surface.local_channels.is_empty());
    assert!(surface.policy.allows_ephemeral_delivery("page.share"));
    assert!(surface.policy.allows_ephemeral_publish("page.share"));
}

/// Naming a backend-only connection `local:` keeps it on the server ring it
/// would have taken anonymously, with an address an operator can grep for.
#[test]
fn a_named_backend_local_channel_keeps_its_server_ring() {
    let consumers = vec![publisher("etl"), subscriber("indexer", 2, 6)];
    let wiring = lower_auto_wiring(
        &[ConnectionConfigRaw {
            channel: Some("local:etl.batches".to_string()),
            ..conn(&["wasm:etl/out", "wasm:indexer/tap"])
        }],
        &consumers,
        &[],
        &[],
        &globals(),
    );
    assert!(wiring.durable_entries().is_empty());
    let entries = wiring.nondurable_entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].address, "local:etl.batches");
    assert_eq!(entries[0].transport_type, ChannelScheme::Local);
    assert_eq!(
        entries[0].uuid,
        brenn_lib::messaging::nondurable_channel_uuid(ChannelScheme::Local, "etl.batches"),
    );

    let mut dir_entries = entries.to_vec();
    dir_entries.push(brenn_entry("brenn:feed"));
    let resolved = resolve_with_auto(&consumers, &dir_of(dir_entries), &wiring);
    assert_eq!(resolved[0].outputs[0].channel_address, "local:etl.batches");
    assert!(resolved[0].policy.allows_local_publish("etl.batches"));
    assert!(!resolved[0].policy.allows_local_delivery("etl.batches"));
    assert_eq!(
        resolved[1].inputs[0].sub.channel_address,
        "local:etl.batches",
    );
    assert!(resolved[1].policy.allows_local_delivery("etl.batches"));
    assert!(!resolved[1].policy.allows_local_publish("etl.batches"));
}

// --- io_ports ---

/// The zero-config case: no `[[connection]]`, no `channel`, no `[[channel]]`
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

/// Listed in a connection, the io_port rides that channel and counts as both a
/// publisher and a subscriber on it — so the self-loop survives the other
/// parties joining, and its depths still feed the fold.
#[test]
fn io_port_in_a_connection_rides_the_connection_channel() {
    let consumers = vec![timer_consumer("etl"), subscriber("indexer", 2, 3)];
    let wiring = lower_auto_wiring(
        &[conn(&["wasm:etl/timer", "wasm:indexer/tap"])],
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

/// A backend io_port and a surface io_port on one connection span the wire, so
/// the channel is `ephemeral:` and each side is granted both of its roles.
#[test]
fn io_ports_spanning_the_wire_share_one_ephemeral_channel() {
    let consumers = vec![timer_consumer("etl")];
    let surfaces = vec![surface_with_io_port("deskbar")];
    let wiring = lower_auto_wiring(
        &[conn(&["wasm:etl/timer", "surface:deskbar#protobar/loop"])],
        &consumers,
        &surfaces,
        &[],
        &globals(),
    );
    let entries = wiring.nondurable_entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].transport_type, ChannelScheme::Ephemeral);
    let bare = entries[0].address.strip_prefix("ephemeral:").unwrap();

    let resolved = resolve_surfaces(&surfaces, &dir_of(vec![]), entries, &globals(), &wiring);
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

    let resolved = resolve_surfaces(&surfaces, &dir_of(vec![]), &[], &globals(), &wiring);
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

/// An unset `retain_depth` on a page-local io_port is the feature's one silent
/// path. Placement makes no server entry, so the depth fold — the check that
/// refuses an unbounded global — never runs, and the ring falls to the
/// per-binding floor of 1 with nothing to catch it. That 1 is the port's whole
/// retention and, per the deferred coupling, the cap on its outstanding
/// self-schedules.
#[test]
fn a_page_local_io_port_with_an_unset_retain_depth_silently_gets_a_ring_of_one() {
    let mut surfaces = vec![surface_with_io_port("deskbar")];
    surfaces[0].io_ports[0].retain_depth = None;
    let stock = MessagingGlobalConfig::default();
    let wiring = lower_auto_wiring(&[], &[], &surfaces, &[], &stock);
    assert!(
        wiring.nondurable_entries().is_empty(),
        "a page-local channel has no server entry, so the fold that refuses an \
         unbounded global never runs",
    );

    let resolved = resolve_surfaces(&surfaces, &dir_of(vec![]), &[], &stock, &wiring);
    let surface = &resolved[0];
    assert_eq!(surface.local_channels.len(), 1);
    assert_eq!(surface.local_channels[0].ring_depth, 1);
}

/// The port queue is the other half, and it is not silent: a page-local binding
/// resolves `push_depth` binding → global like every surface binding, so the
/// stock unbounded default refuses to boot. Page-local escapes the ring fold,
/// not the depth defaults.
#[test]
#[should_panic(expected = "resolves to push_depth = Unbounded")]
fn a_page_local_io_port_with_an_unset_push_depth_refuses_to_boot() {
    let mut surfaces = vec![surface_with_io_port("deskbar")];
    surfaces[0].io_ports[0].push_depth = None;
    let stock = MessagingGlobalConfig::default();
    let wiring = lower_auto_wiring(&[], &[], &surfaces, &[], &stock);
    resolve_surfaces(&surfaces, &dir_of(vec![]), &[], &stock, &wiring);
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
        ..surface_sub_raw("local:bar.loop", "chrome", "snoop")
    }];
    let wiring = lower_auto_wiring(&[], &[], &surfaces, &[], &globals());
    assert!(wiring.durable_entries().is_empty());
    assert!(wiring.nondurable_entries().is_empty());

    let resolved = resolve_surfaces(&surfaces, &dir_of(vec![]), &[], &globals(), &wiring);
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

/// A port binds exactly one channel: its own `channel` field and a connection
/// claiming it are two answers to one question.
#[test]
#[should_panic(expected = "already binds channel")]
fn io_port_with_both_a_channel_and_a_connection_panics() {
    let mut consumers = vec![timer_consumer("etl"), subscriber("indexer", 2, 3)];
    consumers[0].io_ports[0].channel = Some("brenn:etl.timer".to_string());
    lower_auto_wiring(
        &[conn(&["wasm:etl/timer", "wasm:indexer/tap"])],
        &consumers,
        &[],
        &[],
        &globals(),
    );
}

/// An io_port is already wired to itself, so a connection naming it alone is a
/// redundant spelling of the default rather than a second way to say it.
#[test]
#[should_panic(expected = "names one io_port and nothing else")]
fn connection_naming_only_one_io_port_panics() {
    lower_auto_wiring(
        &[conn(&["wasm:etl/timer"])],
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
        crate::tool_registry::bus_wiring::TOOL_RESULT_INPUT_PORT.to_string();
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
        &[],
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
    resolve_surfaces(&surfaces, &dir_of(vec![]), &[], &globals(), &wiring);
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
    surfaces[0].subscriptions = vec![surface_sub_raw("local:etl.tick", "protobar", "snoop")];
    let wiring = lower_auto_wiring(&[], &consumers, &surfaces, &[], &globals());

    let resolved = resolve_surfaces(&surfaces, &dir_of(vec![]), &[], &globals(), &wiring);
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
