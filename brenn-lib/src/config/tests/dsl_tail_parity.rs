//! Parity gates between a statement tail's vocabulary and the config structs
//! that statement lowers into.
//!
//! A tail vocabulary is the union of the tail fields across the families its
//! statement can become, because which family a statement is depends on the
//! address it names and the front end does not know that. So a key set here
//! faces several structs, and each of them accounts for every key: as a field
//! it fills, or as a key this family masks out and refuses at its own token.
//!
//! The destructure is the other direction: a field added, removed or renamed on
//! a config struct fails to compile below, and a field the vocabulary neither
//! spells nor names as the statement's own head fails the assertion, named.

use brenn_dsl::model::{InTail, IoTail, MountTail, OutTail, SubscribeTail};

use crate::config::repo::MountConfigRaw;
use crate::messaging::config::{
    MessagingSubscriptionRaw, SurfaceIoPortRaw, SurfaceOutputRaw, SurfaceSubscriptionRaw,
    WasmConsumerIoPortRaw, WasmConsumerOutputRaw, WasmConsumerSubscriptionRaw,
};
use crate::mqtt::config::AppMqttIngressSubscriptionRaw;
use crate::webhook::config::AppWebhookSubscriptionRaw;

/// Every field is a tail key or a listed head field; every key is a field here
/// or a listed mask.
///
/// The two lists are the only two shapes an inequality can honestly take. A
/// `head` field is one the statement itself carries — the address a `subscribe`
/// names, the port an `in` binds — so no tail key could reach it. A `masked`
/// key is one the union vocabulary admits because a sibling family fills it,
/// and this family refuses at its own token; the refusal sites are in
/// `dsl_lower`.
fn assert_tail_parity(what: &str, fields: &[&str], keys: &[&str], head: &[&str], masked: &[&str]) {
    for field in fields {
        let spelled = keys.contains(field);
        let carried = head.contains(field);
        assert!(
            spelled != carried,
            "{what}: field `{field}` is {} — a config field is either a tail key or a field \
             the statement's head carries, never both and never neither",
            if spelled { "both" } else { "neither" },
        );
    }
    for name in head {
        assert!(
            fields.contains(name),
            "{what}: `{name}` names no field of the config struct",
        );
    }
    for key in keys {
        let filled = fields.contains(key);
        let refused = masked.contains(key);
        assert!(
            filled != refused,
            "{what}: the tail's `{key}` is {} — a key of the union is either a field of this \
             family or one it masks and refuses, never both and never neither",
            if filled { "both" } else { "neither" },
        );
    }
    for name in masked {
        assert!(
            keys.contains(name),
            "{what}: `{name}` is masked and is no key of the tail vocabulary",
        );
    }
}

#[test]
fn a_mount_tail_accounts_for_every_field_of_its_raw() {
    let fields = field_names!(MountConfigRaw {
        repo,
        access,
        working_dir,
        auto_pull,
        primary,
    });
    assert_tail_parity("mount", &fields, MountTail::<()>::KEYS, &["repo"], &[]);
}

#[test]
fn a_subscribe_tail_accounts_for_the_three_families_it_can_become() {
    let messaging = field_names!(MessagingSubscriptionRaw {
        channel,
        push_depth,
        retain_depth,
        noise,
        wake_min,
    });
    assert_tail_parity(
        "messaging subscription",
        &messaging,
        SubscribeTail::KEYS,
        &["channel"],
        &[],
    );

    let mqtt = field_names!(AppMqttIngressSubscriptionRaw {
        channel,
        push_depth,
        retain_depth,
        noise,
        wake_min,
    });
    assert_tail_parity(
        "mqtt ingress subscription",
        &mqtt,
        SubscribeTail::KEYS,
        &["channel"],
        &[],
    );

    // The endpoint's traffic is not a channel whose volume the app tunes, so
    // `noise` is masked and refused at its own token.
    let webhook = field_names!(AppWebhookSubscriptionRaw {
        endpoint,
        push_depth,
        retain_depth,
        wake_min,
    });
    assert_tail_parity(
        "webhook subscription",
        &webhook,
        SubscribeTail::KEYS,
        &["endpoint"],
        &["noise"],
    );
}

#[test]
fn an_in_tail_accounts_for_the_consumer_and_surface_families() {
    let consumer = field_names!(WasmConsumerSubscriptionRaw {
        channel,
        port,
        push_depth,
        retain_depth,
        noise,
        wake_min,
        amplification,
    });
    assert_tail_parity(
        "consumer subscription",
        &consumer,
        InTail::<()>::KEYS,
        &["channel", "port"],
        &[],
    );

    // A page's throughput is the page's, not a knob the binding sets.
    let surface = field_names!(SurfaceSubscriptionRaw {
        channel,
        instance,
        port,
        push_depth,
        retain_depth,
        noise,
        wake_min,
    });
    assert_tail_parity(
        "surface subscription",
        &surface,
        InTail::<()>::KEYS,
        &["channel", "instance", "port"],
        &["amplification"],
    );
}

#[test]
fn an_out_tail_accounts_for_the_two_output_families() {
    let consumer = field_names!(WasmConsumerOutputRaw {
        port,
        channel,
        urgency,
        publish_per_activation,
        publish_capacity,
    });
    assert_tail_parity(
        "consumer output",
        &consumer,
        OutTail::<()>::KEYS,
        &["channel", "port"],
        &[],
    );

    let surface = field_names!(SurfaceOutputRaw {
        instance,
        port,
        channel,
        urgency,
        publish_per_activation,
        publish_capacity,
    });
    assert_tail_parity(
        "surface output",
        &surface,
        OutTail::<()>::KEYS,
        &["channel", "instance", "port"],
        &[],
    );
}

#[test]
fn an_io_tail_accounts_for_the_two_io_families() {
    let consumer = field_names!(WasmConsumerIoPortRaw {
        port,
        channel,
        push_depth,
        retain_depth,
        noise,
        amplification,
        urgency,
        publish_per_activation,
        publish_capacity,
    });
    assert_tail_parity(
        "consumer io port",
        &consumer,
        IoTail::<()>::KEYS,
        &["channel", "port"],
        &[],
    );

    let surface = field_names!(SurfaceIoPortRaw {
        instance,
        port,
        channel,
        push_depth,
        retain_depth,
        noise,
        urgency,
        publish_per_activation,
        publish_capacity,
    });
    assert_tail_parity(
        "surface io port",
        &surface,
        IoTail::<()>::KEYS,
        &["channel", "instance", "port"],
        &["amplification"],
    );
}
