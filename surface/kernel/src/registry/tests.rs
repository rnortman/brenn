//! The registration table and the two reconcile passes, driven against a real
//! store collection and a real subscription plane — so what the page actually
//! retains, primes and puts on the wire is what the assertions read.

use std::collections::BTreeMap;

use brenn_attach_client::subs::SubscriptionDepths;
use brenn_attach_proto::SubscribeOutcome;
use brenn_envelope::{ChannelScheme, MessageEnvelope, Urgency};
use brenn_surface_schema::bindings::{
    BINDINGS_DOCUMENT_VERSION, BindingsDocument, PlatformSection,
};
use brenn_surface_schema::{
    Abi, Binding, ComponentEntry, LOCAL_THEME_CHANNEL, LOCAL_TOAST_CHANNEL, LocalChannel,
    NoiseLevel, OutputBinding, Urgency as SchemaUrgency, reserved_local_channel,
};

use super::*;

const WIRE: &str = "brenn:site.bar.in";
const OTHER_WIRE: &str = "ephemeral:site.bar.signal";
const EPOCH: Uuid = Uuid::from_u128(0x5107);

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

/// Chrome plus two components: `p1` reads one wire channel through two ports,
/// `p2` reads the same channel at different depths and a page-local plane. Enough
/// shape for a shared subscription, a fold across bindings, and both classes of
/// store.
fn doc(subscriptions: Vec<Binding>) -> BindingsDocument {
    BindingsDocument {
        v: BINDINGS_DOCUMENT_VERSION,
        components: vec![component("p1"), component("p2"), component("chrome")],
        subscriptions,
        outputs: vec![OutputBinding {
            channel: OTHER_WIRE.to_string(),
            instance: "p1".to_string(),
            port: "out".to_string(),
            urgency: SchemaUrgency::Normal,
            fill_mt: 1_000,
            capacity_mt: 4_000,
        }],
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
            error_channel: None,
            error_report_floor: None,
            takeover_granted: false,
        },
    }
}

/// The standard wiring: two of `p1`'s ports and one of `p2`'s on one wire
/// channel, plus `p2` on the theme plane.
fn standard() -> Vec<Binding> {
    vec![
        subscription("p1", "in", WIRE, 8, 2),
        subscription("p1", "aux", WIRE, 1, 0),
        subscription("p2", "in", WIRE, 3, 9),
        subscription("p2", "theme", LOCAL_THEME_CHANNEL, 1, 0),
    ]
}

fn applied(subscriptions: Vec<Binding>) -> AppliedBindings {
    AppliedBindings::apply(&doc(subscriptions).to_body()).expect("the fixture document applies")
}

/// A live subscription plane: an attachment is up, so an acquisition emits its
/// `Subscribe` immediately.
fn plane() -> Subscriptions {
    let mut subs = Subscriptions::new();
    subs.go_live();
    subs
}

/// Answer every `Subscribe` in `frames`, so the plane's channels are `Active`
/// and a later release emits its `Unsubscribe` rather than deferring it.
fn ack(subs: &mut Subscriptions, frames: &[ClientFrame]) {
    for frame in frames {
        if let ClientFrame::Subscribe { channel, .. } = frame {
            subs.on_subscribe_result(channel, SubscribeOutcome::Ok, 0, None)
                .expect("the fixture answers a pending channel");
        }
    }
}

fn subscribed(frames: &[ClientFrame]) -> Vec<(&str, u64, u64)> {
    frames
        .iter()
        .filter_map(|f| match f {
            ClientFrame::Subscribe {
                channel,
                push_depth,
                retain_depth,
                ..
            } => Some((channel.as_str(), *push_depth, *retain_depth)),
            _ => None,
        })
        .collect()
}

fn unsubscribed(frames: &[ClientFrame]) -> Vec<&str> {
    frames
        .iter()
        .filter_map(|f| match f {
            ClientFrame::Unsubscribe { channel } => Some(channel.as_str()),
            _ => None,
        })
        .collect()
}

fn env(channel: &str, body: &str) -> MessageEnvelope {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(&(channel, body), &mut hasher);
    MessageEnvelope {
        message_id: Uuid::from_u128(u128::from(std::hash::Hasher::finish(&hasher))),
        source: "test".into(),
        channel: channel.into(),
        sender: "surface:bar#p1".into(),
        publish_ts: chrono::DateTime::from_timestamp(0, 0).expect("a representable instant"),
        body: body.into(),
        reply_to: None,
        delivery_deadline: None,
        deliver_after: None,
        impetus: None,
        urgency: Urgency::Normal,
        envelope_type: ChannelScheme::Brenn,
    }
}

fn wire_depths(push_depth: u64, retain_depth: u64) -> SubscriptionDepths {
    SubscriptionDepths {
        push_depth,
        retain_depth,
    }
}

fn readers(stores: &SurfaceStores, channel: &str) -> Vec<BindingKey> {
    let mut held: Vec<BindingKey> = stores
        .get(channel)
        .expect("the channel has a store")
        .readers()
        .cloned()
        .collect();
    held.sort();
    held
}

/// The stores with the standard wiring in force, ready for registrations.
fn stores_for(bindings: &AppliedBindings) -> SurfaceStores {
    let mut stores = new_stores(EPOCH);
    reconcile_stores(bindings, &mut stores);
    stores
}

#[test]
fn every_declared_channel_gets_a_store_at_its_folded_depth() {
    let bindings = applied(standard());
    let stores = stores_for(&bindings);
    // max(8,2) across p1/in, max(1,0), max(3,9) across p2/in.
    assert_eq!(stores.get(WIRE).expect("wire store").depth(), 9);
    assert_eq!(
        stores
            .get(LOCAL_THEME_CHANNEL)
            .expect("theme store")
            .depth(),
        reserved_local_channel(LOCAL_THEME_CHANNEL)
            .expect("reserved")
            .ring_depth,
    );
}

#[test]
fn the_reserved_planes_outlive_a_document_that_never_names_them() {
    let bindings = applied(standard());
    let stores = stores_for(&bindings);
    let toast = stores.get(LOCAL_TOAST_CHANNEL).expect("the toast plane");
    assert_eq!(
        toast.depth(),
        reserved_local_channel(LOCAL_TOAST_CHANNEL)
            .expect("reserved")
            .ring_depth,
        "a plane no document declares keeps its contract depth"
    );
}

#[test]
fn a_surviving_store_keeps_its_contents_and_a_dropped_one_goes() {
    let bindings = applied(standard());
    let mut stores = stores_for(&bindings);
    stores
        .get_mut(WIRE)
        .expect("wire store")
        .insert(env(WIRE, "one"));

    reconcile_stores(&bindings, &mut stores);
    assert_eq!(
        stores.get(WIRE).expect("wire store").retained().count(),
        1,
        "a reconcile that changes nothing manufactures no loss"
    );

    let narrowed = applied(vec![subscription("p2", "theme", LOCAL_THEME_CHANNEL, 1, 0)]);
    reconcile_stores(&narrowed, &mut stores);
    assert!(
        stores.get(WIRE).is_none(),
        "a channel no binding names loses its store"
    );
}

#[test]
fn a_shrink_reports_the_positions_it_outran() {
    let bindings = applied(standard());
    let mut stores = stores_for(&bindings);
    let key = BindingKey::new("p1", "in");
    let store = stores.get_mut(WIRE).expect("wire store");
    store.attach(key.clone(), 8);
    for body in ["a", "b", "c", "d"] {
        store.insert(env(WIRE, body));
    }

    let shallow = applied(vec![subscription("p1", "in", WIRE, 1, 0)]);
    let report = reconcile_stores(&shallow, &mut stores);
    let (channel, overflow) = report
        .retired
        .first()
        .expect("the shrink retired something");
    assert_eq!(channel, WIRE);
    assert_eq!(overflow[0].subscriber, key);
    assert_eq!(overflow[0].evicted, 3, "four retained trimmed to one");
    assert!(report.lost_schedules.is_empty());
}

#[test]
fn a_dropped_store_reports_the_schedules_that_died_with_it() {
    let bindings = applied(vec![subscription("p2", "theme", LOCAL_THEME_CHANNEL, 1, 0)]);
    let mut stores = stores_for(&bindings);
    stores
        .get_mut(LOCAL_THEME_CHANNEL)
        .expect("theme store")
        .park("surface:bar#p2", env(LOCAL_THEME_CHANNEL, "later"), 9_000)
        .expect("the fixture parks within the plane's cap");

    // A document declaring no local channel at all: the plane is reserved, so its
    // store survives — but a plain page-local channel would not, and the theme
    // plane's own schedules are what a dropped store would owe an account of.
    let mut doc = doc(Vec::new());
    doc.local_channels.clear();
    let narrowed = AppliedBindings::apply(&doc.to_body()).expect("applies");
    let report = reconcile_stores(&narrowed, &mut stores);
    assert!(
        report.lost_schedules.is_empty(),
        "a reserved plane is never dropped, so its schedules are never lost"
    );

    // The same store, now on an ordinary page-local address the document drops.
    let plain = "local:site.notes";
    let mut doc = doc.clone();
    doc.local_channels.push(LocalChannel {
        channel: plain.to_string(),
        ring_depth: 4,
    });
    let with_plain = AppliedBindings::apply(&doc.to_body()).expect("applies");
    reconcile_stores(&with_plain, &mut stores);
    let store = stores
        .get_mut(plain)
        .expect("the plain channel has a store");
    for body in ["scheduled", "scheduled too"] {
        store
            .park("surface:bar#p2", env(plain, body), 9_000)
            .expect("within the cap");
    }
    let report = reconcile_stores(&narrowed, &mut stores);
    assert_eq!(
        report.lost_schedules,
        vec![(
            plain.to_string(),
            vec!["surface:bar#p2".to_string(), "surface:bar#p2".to_string()],
        )],
        "one entry per parked message, naming who parked it"
    );
}

#[test]
fn registering_before_the_first_document_holds_nothing() {
    let mut stores = new_stores(EPOCH);
    let mut subs = plane();
    let mut regs = Registrations::new();
    assert!(regs.register("p1", None, &mut stores, &mut subs).is_empty());
    assert!(regs.is_registered("p1"));
    assert!(subs.held_channels().is_empty());
}

#[test]
fn a_registration_takes_a_position_per_binding_and_one_subscription() {
    let bindings = applied(standard());
    let mut stores = stores_for(&bindings);
    let mut subs = plane();
    let mut regs = Registrations::new();

    let frames = regs.register("p1", Some(&bindings), &mut stores, &mut subs);
    assert_eq!(
        subscribed(&frames),
        vec![(WIRE, 8, 9)],
        "one Subscribe for two bindings, stating the fold across every binding on the channel"
    );
    assert_eq!(subs.refcount(WIRE), 2, "one reference per binding");
    assert_eq!(
        readers(&stores, WIRE),
        vec![BindingKey::new("p1", "aux"), BindingKey::new("p1", "in")],
    );
}

#[test]
fn two_instances_on_one_channel_share_one_subscription() {
    let bindings = applied(standard());
    let mut stores = stores_for(&bindings);
    let mut subs = plane();
    let mut regs = Registrations::new();

    let first = regs.register("p1", Some(&bindings), &mut stores, &mut subs);
    ack(&mut subs, &first);
    let second = regs.register("p2", Some(&bindings), &mut stores, &mut subs);
    assert!(
        subscribed(&second).is_empty(),
        "the channel is already open; the second instance only takes a reference"
    );
    assert_eq!(subs.refcount(WIRE), 3);
    assert_eq!(
        readers(&stores, LOCAL_THEME_CHANNEL),
        vec![BindingKey::new("p2", "theme")],
        "a confined binding takes a position and no subscription"
    );
}

#[test]
fn a_position_coming_into_existence_is_primed_from_the_retained_tail() {
    let bindings = applied(standard());
    let mut stores = stores_for(&bindings);
    stores
        .get_mut(WIRE)
        .expect("wire store")
        .insert(env(WIRE, "published before it bound"));
    let mut subs = plane();
    let mut regs = Registrations::new();
    regs.register("p1", Some(&bindings), &mut stores, &mut subs);

    let store = stores.get_mut(WIRE).expect("wire store");
    for port in ["in", "aux"] {
        assert!(
            store.has_deliverable(&BindingKey::new("p1", port)),
            "attach is a delivery point: {port} wakes on what it bound after"
        );
    }
}

#[test]
fn a_depth_zero_binding_holds_no_position_and_still_subscribes() {
    let bindings = applied(vec![subscription("p1", "sample", WIRE, 0, 5)]);
    let mut stores = stores_for(&bindings);
    stores
        .get_mut(WIRE)
        .expect("wire store")
        .insert(env(WIRE, "context only"));
    let mut subs = plane();
    let mut regs = Registrations::new();

    let frames = regs.register("p1", Some(&bindings), &mut stores, &mut subs);
    assert_eq!(
        subscribed(&frames),
        vec![(WIRE, 0, 5)],
        "never activates me is not never show me: the channel is still subscribed"
    );
    assert!(
        readers(&stores, WIRE).is_empty(),
        "a sampled binding holds no position"
    );
}

#[test]
fn a_second_reconcile_changes_nothing_and_re_primes_nothing() {
    let bindings = applied(standard());
    let mut stores = stores_for(&bindings);
    let mut subs = plane();
    let mut regs = Registrations::new();
    let frames = regs.register("p1", Some(&bindings), &mut stores, &mut subs);
    ack(&mut subs, &frames);

    let store = stores.get_mut(WIRE).expect("wire store");
    store.insert(env(WIRE, "one"));
    let key = BindingKey::new("p1", "in");
    store
        .serve(
            &key,
            brenn_attach_client::subs::SubscriptionDepths {
                push_depth: 8,
                retain_depth: 2,
            },
        )
        .expect("a push-enabled position is served");
    assert!(!store.has_deliverable(&key));

    let again = regs.reconcile(&bindings, &mut stores, &mut subs);
    assert!(again.is_empty(), "nothing changed, so nothing goes out");
    assert_eq!(subs.refcount(WIRE), 2);
    assert!(
        !stores
            .get(WIRE)
            .expect("wire store")
            .has_deliverable(&BindingKey::new("p1", "in")),
        "a surviving position is never re-primed"
    );
}

#[test]
fn a_binding_that_vanished_loses_its_position_and_its_reference() {
    let bindings = applied(standard());
    let mut stores = stores_for(&bindings);
    let mut subs = plane();
    let mut regs = Registrations::new();
    let frames = regs.register("p1", Some(&bindings), &mut stores, &mut subs);
    ack(&mut subs, &frames);

    // `p1/aux` gone, the rest of the wiring — and so the channel's fold —
    // unchanged.
    let narrowed = applied(vec![
        subscription("p1", "in", WIRE, 8, 2),
        subscription("p2", "in", WIRE, 3, 9),
        subscription("p2", "theme", LOCAL_THEME_CHANNEL, 1, 0),
    ]);
    reconcile_stores(&narrowed, &mut stores);
    let frames = regs.reconcile(&narrowed, &mut stores, &mut subs);
    assert!(
        unsubscribed(&frames).is_empty(),
        "the surviving binding still holds the subscription open"
    );
    assert_eq!(subs.refcount(WIRE), 1);
    assert_eq!(readers(&stores, WIRE), vec![BindingKey::new("p1", "in")]);
}

#[test]
fn the_last_binding_off_a_channel_closes_its_subscription() {
    let bindings = applied(standard());
    let mut stores = stores_for(&bindings);
    let mut subs = plane();
    let mut regs = Registrations::new();
    let frames = regs.register("p1", Some(&bindings), &mut stores, &mut subs);
    ack(&mut subs, &frames);

    let unwired = applied(vec![subscription("p2", "theme", LOCAL_THEME_CHANNEL, 1, 0)]);
    reconcile_stores(&unwired, &mut stores);
    let frames = regs.reconcile(&unwired, &mut stores, &mut subs);
    assert_eq!(unsubscribed(&frames), vec![WIRE]);
    assert_eq!(subs.refcount(WIRE), 0);
    assert!(
        regs.is_registered("p1"),
        "an instance the operator un-wired is not failed and not deregistered"
    );
}

#[test]
fn a_port_rebound_to_another_channel_moves_its_position() {
    let bindings = applied(vec![subscription("p1", "in", WIRE, 4, 0)]);
    let mut stores = stores_for(&bindings);
    let mut subs = plane();
    let mut regs = Registrations::new();
    let frames = regs.register("p1", Some(&bindings), &mut stores, &mut subs);
    ack(&mut subs, &frames);

    let rebound = applied(vec![subscription("p1", "in", OTHER_WIRE, 4, 0)]);
    reconcile_stores(&rebound, &mut stores);
    let frames = regs.reconcile(&rebound, &mut stores, &mut subs);
    assert_eq!(unsubscribed(&frames), vec![WIRE]);
    assert_eq!(subscribed(&frames), vec![(OTHER_WIRE, 4, 0)]);
    assert!(stores.get(WIRE).is_none(), "the old channel is unwired");
    assert_eq!(
        readers(&stores, OTHER_WIRE),
        vec![BindingKey::new("p1", "in")],
    );
}

#[test]
fn a_changed_fold_closes_the_subscription_and_states_it_afresh() {
    let bindings = applied(vec![subscription("p1", "in", WIRE, 4, 0)]);
    let mut stores = stores_for(&bindings);
    let mut subs = plane();
    let mut regs = Registrations::new();
    let frames = regs.register("p1", Some(&bindings), &mut stores, &mut subs);
    ack(&mut subs, &frames);

    let deeper = applied(vec![subscription("p1", "in", WIRE, 4, 6)]);
    reconcile_stores(&deeper, &mut stores);
    let frames = regs.reconcile(&deeper, &mut stores, &mut subs);
    assert_eq!(unsubscribed(&frames), vec![WIRE]);
    assert_eq!(subscribed(&frames), vec![(WIRE, 4, 6)]);
    assert_eq!(subs.refcount(WIRE), 1);
    assert_eq!(
        readers(&stores, WIRE),
        vec![BindingKey::new("p1", "in")],
        "the position survives the restatement",
    );
}

#[test]
fn a_changed_fold_restates_a_subscription_still_awaiting_its_result() {
    let bindings = applied(vec![subscription("p1", "in", WIRE, 4, 0)]);
    let mut stores = stores_for(&bindings);
    let mut subs = plane();
    let mut regs = Registrations::new();
    // Deliberately unanswered: a second document can arrive before the first
    // document's subscribes come back.
    let frames = regs.register("p1", Some(&bindings), &mut stores, &mut subs);
    assert_eq!(subscribed(&frames), vec![(WIRE, 4, 0)]);

    let deeper = applied(vec![subscription("p1", "in", WIRE, 4, 6)]);
    reconcile_stores(&deeper, &mut stores);
    let frames = regs.reconcile(&deeper, &mut stores, &mut subs);
    assert!(
        frames.is_empty(),
        "nothing goes out while the peer is still answering the old subscribe"
    );
    assert_eq!(subs.refcount(WIRE), 1);
    assert_eq!(subs.depths(WIRE), Some(wire_depths(4, 6)));
    assert_eq!(
        readers(&stores, WIRE),
        vec![BindingKey::new("p1", "in")],
        "the position survives the restatement",
    );

    // The plane enacts it when the outstanding result lands.
    let ack = subs
        .on_subscribe_result(WIRE, SubscribeOutcome::Ok, 0, None)
        .expect("the channel is pending");
    assert_eq!(unsubscribed(&ack.frames), vec![WIRE]);
    assert_eq!(subscribed(&ack.frames), vec![(WIRE, 4, 6)]);
}

#[test]
fn a_changed_fold_restates_a_pending_channel_two_instances_hold() {
    let bindings = applied(vec![
        subscription("p1", "in", WIRE, 4, 0),
        subscription("p2", "in", WIRE, 2, 0),
    ]);
    let mut stores = stores_for(&bindings);
    let mut subs = plane();
    let mut regs = Registrations::new();
    regs.register("p1", Some(&bindings), &mut stores, &mut subs);
    regs.register("p2", Some(&bindings), &mut stores, &mut subs);
    assert_eq!(subs.refcount(WIRE), 2);

    // Both holders are released before either is retaken, so the restatement is
    // one statement rather than two.
    let deeper = applied(vec![
        subscription("p1", "in", WIRE, 4, 6),
        subscription("p2", "in", WIRE, 2, 0),
    ]);
    reconcile_stores(&deeper, &mut stores);
    assert!(
        regs.reconcile(&deeper, &mut stores, &mut subs).is_empty(),
        "nothing goes out while the peer is still answering the old subscribe"
    );
    assert_eq!(subs.refcount(WIRE), 2);
    assert_eq!(subs.depths(WIRE), Some(wire_depths(4, 6)));
}

#[test]
fn deregistration_drops_the_positions_and_releases_the_references() {
    let bindings = applied(standard());
    let mut stores = stores_for(&bindings);
    let mut subs = plane();
    let mut regs = Registrations::new();
    let frames = regs.register("p1", Some(&bindings), &mut stores, &mut subs);
    ack(&mut subs, &frames);
    let frames = regs.register("p2", Some(&bindings), &mut stores, &mut subs);
    ack(&mut subs, &frames);

    let frames = regs.deregister("p1", &mut stores, &mut subs);
    assert!(
        unsubscribed(&frames).is_empty(),
        "p2 still holds the channel open"
    );
    assert_eq!(subs.refcount(WIRE), 1);
    assert!(!regs.is_registered("p1"));
    assert_eq!(readers(&stores, WIRE), vec![BindingKey::new("p2", "in")]);

    let frames = regs.deregister("p2", &mut stores, &mut subs);
    assert_eq!(unsubscribed(&frames), vec![WIRE]);
    assert!(readers(&stores, WIRE).is_empty());
    assert!(
        stores.get(WIRE).is_some(),
        "the store stays: the channel is still declared"
    );
    assert!(regs.is_empty());
}

#[test]
fn a_failed_instance_loses_its_positions_and_keeps_its_references() {
    let bindings = applied(standard());
    let mut stores = stores_for(&bindings);
    let mut subs = plane();
    let mut regs = Registrations::new();
    let frames = regs.register("p1", Some(&bindings), &mut stores, &mut subs);
    ack(&mut subs, &frames);
    regs.register("p2", Some(&bindings), &mut stores, &mut subs);

    assert!(regs.fail("p1", &mut stores), "the transition is reported");
    assert!(!regs.fail("p1", &mut stores), "and reported once");
    assert!(regs.is_failed("p1"));
    assert_eq!(
        readers(&stores, WIRE),
        vec![BindingKey::new("p2", "in")],
        "a terminal instance is owed nothing",
    );
    assert_eq!(
        subs.refcount(WIRE),
        3,
        "its references stay: it is still registered, and its siblings read the channel"
    );

    let frames = regs.reconcile(&bindings, &mut stores, &mut subs);
    assert!(frames.is_empty());
    assert_eq!(
        readers(&stores, WIRE),
        vec![BindingKey::new("p2", "in")],
        "a later reconcile does not re-attach it",
    );
}

#[test]
fn a_failed_instances_references_are_diffed_against_a_changed_document() {
    let bindings = applied(standard());
    let mut stores = stores_for(&bindings);
    let mut subs = plane();
    let mut regs = Registrations::new();
    let frames = regs.register("p1", Some(&bindings), &mut stores, &mut subs);
    ack(&mut subs, &frames);
    regs.register("p2", Some(&bindings), &mut stores, &mut subs);
    assert!(regs.fail("p1", &mut stores), "the transition is reported");
    assert_eq!(subs.refcount(WIRE), 3);

    // `p1/aux` gone; the channel's fold is unchanged, so the subscription is not
    // restated — one of the terminal instance's two references simply goes.
    let narrowed = applied(vec![
        subscription("p1", "in", WIRE, 8, 2),
        subscription("p2", "in", WIRE, 3, 9),
        subscription("p2", "theme", LOCAL_THEME_CHANNEL, 1, 0),
    ]);
    reconcile_stores(&narrowed, &mut stores);
    let frames = regs.reconcile(&narrowed, &mut stores, &mut subs);
    assert!(
        frames.is_empty(),
        "its siblings still hold the channel, and it is attached nothing"
    );
    assert_eq!(subs.refcount(WIRE), 2);
    assert_eq!(
        readers(&stores, WIRE),
        vec![BindingKey::new("p2", "in")],
        "a terminal instance is still owed nothing",
    );

    // And what it holds is exactly what deregistration releases.
    let frames = regs.deregister("p1", &mut stores, &mut subs);
    assert!(unsubscribed(&frames).is_empty(), "p2 still reads it");
    assert_eq!(subs.refcount(WIRE), 1);
}

#[test]
#[should_panic(expected = "registered twice")]
fn registering_an_instance_twice_panics() {
    let bindings = applied(standard());
    let mut stores = stores_for(&bindings);
    let mut subs = plane();
    let mut regs = Registrations::new();
    regs.register("p1", Some(&bindings), &mut stores, &mut subs);
    regs.register("p1", Some(&bindings), &mut stores, &mut subs);
}

#[test]
#[should_panic(expected = "deregistration of unregistered instance")]
fn deregistering_an_unregistered_instance_panics() {
    let mut stores = new_stores(EPOCH);
    let mut subs = plane();
    let mut regs = Registrations::new();
    regs.deregister("p1", &mut stores, &mut subs);
}

#[test]
#[should_panic(expected = "failing unregistered instance")]
fn failing_an_unregistered_instance_panics() {
    let mut stores = new_stores(EPOCH);
    let mut regs = Registrations::new();
    regs.fail("p1", &mut stores);
}

#[test]
#[should_panic(expected = "has no store")]
fn reconciling_before_the_stores_panics() {
    let bindings = applied(standard());
    // No store pass: the wire channel the bindings name has nowhere to hold a
    // position.
    let mut stores = new_stores(EPOCH);
    let mut subs = plane();
    let mut regs = Registrations::new();
    regs.register("p1", Some(&bindings), &mut stores, &mut subs);
}

#[test]
fn an_instance_the_document_never_wires_holds_nothing() {
    let bindings = applied(standard());
    let mut stores = stores_for(&bindings);
    let mut subs = plane();
    let mut regs = Registrations::new();

    let frames = regs.register("chrome", Some(&bindings), &mut stores, &mut subs);
    assert!(frames.is_empty());
    assert!(subs.held_channels().is_empty());
    assert!(regs.instances().eq(["chrome"]));
}
