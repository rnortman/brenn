//! Two-phase connect tests. Driven through the connect state's own methods
//! against a real subscription plane, so what the surface actually puts on the
//! wire in each phase is what the assertions read.

use std::collections::BTreeMap;

use brenn_attach_client::subs::{DeliverDisposition, SubscribeAck};
use brenn_attach_proto::{Cursor, GapInfo, GapReason, SubscribeOutcome};
use brenn_surface_schema::bindings::{
    BINDINGS_DOCUMENT_VERSION, BindingsDocument, PlatformSection,
};
use brenn_surface_schema::{
    Abi, Binding, ComponentEntry, LOCAL_THEME_CHANNEL, LocalChannel, NoiseLevel, Urgency,
    reserved_local_channel,
};

use uuid::Uuid;

use super::*;
use crate::registry::{Registrations, new_stores, reconcile_stores};

const CONFIG: &str = "ephemeral:site.surface.bar.bindings";
const WIRE: &str = "brenn:site.bar.in";

fn cursor(token: &str) -> Cursor {
    serde_json::from_value(serde_json::Value::String(token.to_string()))
        .expect("cursor from a JSON string")
}

fn subscribe(channel: &str, push: u64, retain: u64, resume: Option<&str>) -> ClientFrame {
    ClientFrame::Subscribe {
        channel: channel.to_string(),
        push_depth: push,
        retain_depth: retain,
        resume: resume.map(cursor),
    }
}

fn facts() -> AttachmentFacts {
    AttachmentFacts {
        version: 1,
        participant_id: "surface:bar".to_string(),
        session_id: "sess-1".to_string(),
        heartbeat_secs: 20,
        max_body_bytes: 65_536,
        max_frame_bytes: 70_000,
        alert_granted: true,
    }
}

fn ack(replay_count: u32) -> SubscribeAck {
    SubscribeAck {
        frames: Vec::new(),
        live: true,
        replay_count,
        gap: None,
    }
}

/// One component bound to one wire channel and one page-local plane: enough
/// shape for a document to be valid and for a second version of it to differ.
fn doc(kind: &str) -> BindingsDocument {
    BindingsDocument {
        v: BINDINGS_DOCUMENT_VERSION,
        components: vec![ComponentEntry {
            instance: "chrome".to_string(),
            kind: kind.to_string(),
            abi: Abi::Dom,
            parked_batch_depth: 4,
            config: BTreeMap::new(),
        }],
        subscriptions: vec![Binding {
            channel: WIRE.to_string(),
            instance: "chrome".to_string(),
            port: "in".to_string(),
            push_depth: 4,
            retain_depth: 2,
            noise: NoiseLevel::Metered,
        }],
        outputs: vec![brenn_surface_schema::OutputBinding {
            channel: LOCAL_THEME_CHANNEL.to_string(),
            instance: "chrome".to_string(),
            port: "theme".to_string(),
            urgency: Urgency::Normal,
            fill_mt: 1_000,
            capacity_mt: 4_000,
        }],
        local_channels: vec![LocalChannel {
            channel: LOCAL_THEME_CHANNEL.to_string(),
            ring_depth: reserved_local_channel(LOCAL_THEME_CHANNEL)
                .map(|r| r.ring_depth)
                .expect("the theme plane is reserved"),
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

fn body(kind: &str) -> String {
    doc(kind).to_body()
}

fn connect() -> SurfaceConnect {
    SurfaceConnect::new(CONFIG.to_string())
}

/// Phase 1 plus the config channel's acknowledgement: the state every phase-2
/// test starts from.
fn awaiting_config(subs: &mut Subscriptions) -> SurfaceConnect {
    let mut connect = connect();
    connect.on_attached(facts(), subs);
    subs.on_subscribe_result(CONFIG, SubscribeOutcome::Ok, 1, None)
        .expect("the config channel is pending");
    connect
}

#[test]
fn a_page_starts_detached_holding_only_its_config_address() {
    let connect = connect();
    assert_eq!(connect.config_channel(), CONFIG);
    assert!(connect.is_config_channel(CONFIG));
    assert!(!connect.is_config_channel(WIRE));
    assert_eq!(connect.phase(), Phase::Detached);
    assert!(connect.facts().is_none());
    assert!(connect.bindings().is_none());
}

#[test]
#[should_panic(expected = "declares no config channel")]
fn an_empty_config_address_is_a_broken_boot() {
    SurfaceConnect::new(String::new());
}

#[test]
#[should_panic(expected = "does not cross the wire")]
fn a_confined_config_address_is_a_broken_boot() {
    SurfaceConnect::new(LOCAL_THEME_CHANNEL.to_string());
}

#[test]
fn phase_one_subscribes_the_config_channel_and_nothing_else() {
    let mut subs = Subscriptions::new();
    // A registration taken while detached: its reference is held, but the wiring
    // that authorizes it has not arrived on this attachment.
    subs.acquire(
        WIRE,
        SubscriptionDepths {
            push_depth: 4,
            retain_depth: 2,
        },
        ResumePolicy::Resume,
    );
    let mut connect = connect();

    let frames = connect.on_attached(facts(), &mut subs);

    assert_eq!(frames, vec![subscribe(CONFIG, 1, 1, None)]);
    assert_eq!(connect.phase(), Phase::AwaitingConfig);
    assert_eq!(connect.facts().expect("attached").session_id, "sess-1");
    assert_eq!(connect.facts().expect("attached").max_body_bytes, 65_536);
    assert!(!subs.is_active(WIRE));
}

/// The application channels are subscribed by the wiring, not by the attachment:
/// a component registered before the document lands holds nothing on the wire, and
/// what it holds afterwards is the document's own fold.
#[test]
fn the_application_channels_wait_for_phase_two() {
    let mut subs = Subscriptions::new();
    let mut stores = new_stores(Uuid::from_u128(0x5107));
    let mut regs = Registrations::new();
    let mut connect = awaiting_config(&mut subs);

    // Registered while the surface has no wiring: nothing to reconcile against.
    assert!(
        regs.register("chrome", connect.bindings(), &mut stores, &mut subs)
            .is_empty()
    );
    assert_eq!(subs.refcount(WIRE), 0);
    assert!(!subs.is_active(WIRE));

    connect
        .on_config_deliver(&body("protobar"))
        .expect("the fixture document applies");
    let wiring = connect.bindings().expect("phase 2 put the wiring in force");
    reconcile_stores(wiring, &mut stores);

    // The depths are the document's fold, and this is where the `Subscribe` for
    // them is composed.
    assert_eq!(
        regs.reconcile(wiring, &mut stores, &mut subs),
        vec![subscribe(WIRE, 4, 2, None)]
    );
    assert!(
        subs.resubscribe_survivors().is_empty(),
        "the reconcile already opened everything the wiring names"
    );
}

#[test]
#[should_panic(expected = "already live")]
fn a_second_attachment_under_one_page_panics() {
    let mut subs = Subscriptions::new();
    let mut connect = connect();
    connect.on_attached(facts(), &mut subs);
    connect.on_attached(facts(), &mut subs);
}

#[test]
fn an_empty_config_replay_is_a_broken_peer() {
    let connect = connect();
    let err = connect
        .on_config_ack(&ack(0))
        .expect_err("an empty replay is fatal");
    assert!(err.contains(CONFIG), "{err}");
    assert!(err.contains("retains no bindings document"), "{err}");
}

#[test]
fn a_gap_on_the_config_channel_is_a_broken_peer() {
    let connect = connect();
    let err = connect
        .on_config_ack(&SubscribeAck {
            gap: Some(GapInfo {
                reason: GapReason::EpochChanged,
            }),
            ..ack(1)
        })
        .expect_err("a gap answers a claim that was never made");
    assert!(err.contains("resume claim"), "{err}");
}

#[test]
fn a_replayed_document_is_what_the_ack_promises() {
    assert!(connect().on_config_ack(&ack(1)).is_ok());
}

#[test]
fn phase_two_puts_the_wiring_in_force() {
    let mut subs = Subscriptions::new();
    let mut connect = awaiting_config(&mut subs);

    let applied = connect
        .on_config_deliver(&body("protobar"))
        .expect("the fixture document applies");

    assert!(applied.first_of_attachment);
    // Nothing was in force before it, so nothing changed under the page.
    assert!(!applied.wiring_changed);
    assert_eq!(connect.phase(), Phase::Configured);
    let bindings = connect.bindings().expect("configured");
    assert_eq!(bindings.chrome_instance(), "chrome");
    assert_eq!(
        bindings.component("chrome").expect("declared").kind,
        "protobar"
    );
}

#[test]
fn a_document_this_kernel_cannot_apply_is_fatal() {
    let mut subs = Subscriptions::new();
    let mut connect = awaiting_config(&mut subs);

    let err = connect
        .on_config_deliver("{\"v\":1,\"nonsense\":true}")
        .expect_err("an unusable document is fatal");

    assert!(err.contains("bindings document is unusable"), "{err}");
    // Nothing half-applied.
    assert!(connect.bindings().is_none());
    assert_eq!(connect.phase(), Phase::AwaitingConfig);
}

#[test]
fn an_unchanged_document_across_a_reconnect_reloads_nothing() {
    let mut subs = Subscriptions::new();
    let mut connect = awaiting_config(&mut subs);
    connect
        .on_config_deliver(&body("protobar"))
        .expect("applies");

    connect.on_detached(&mut subs);
    connect.on_attached(facts(), &mut subs);
    subs.on_subscribe_result(CONFIG, SubscribeOutcome::Ok, 1, None)
        .expect("pending");
    let applied = connect
        .on_config_deliver(&body("protobar"))
        .expect("applies");

    assert!(applied.first_of_attachment);
    assert!(!applied.wiring_changed);
}

#[test]
fn a_changed_document_across_a_reconnect_is_a_changed_wiring() {
    let mut subs = Subscriptions::new();
    let mut connect = awaiting_config(&mut subs);
    connect
        .on_config_deliver(&body("protobar"))
        .expect("applies");

    connect.on_detached(&mut subs);
    connect.on_attached(facts(), &mut subs);
    subs.on_subscribe_result(CONFIG, SubscribeOutcome::Ok, 1, None)
        .expect("pending");
    let applied = connect
        .on_config_deliver(&body("protogrid"))
        .expect("applies");

    assert!(applied.first_of_attachment);
    assert!(applied.wiring_changed);
    assert_eq!(
        connect
            .bindings()
            .expect("configured")
            .component("chrome")
            .expect("declared")
            .kind,
        "protogrid"
    );
}

#[test]
fn a_second_document_mid_attachment_is_compared_like_any_other() {
    let mut subs = Subscriptions::new();
    let mut connect = awaiting_config(&mut subs);
    connect
        .on_config_deliver(&body("protobar"))
        .expect("applies");

    let restated = connect
        .on_config_deliver(&body("protobar"))
        .expect("applies");
    assert!(!restated.first_of_attachment);
    assert!(!restated.wiring_changed);

    let changed = connect
        .on_config_deliver(&body("protogrid"))
        .expect("applies");
    assert!(!changed.first_of_attachment);
    assert!(changed.wiring_changed);
}

#[test]
fn the_config_subscription_carries_no_cursor_on_any_reconnect() {
    let mut subs = Subscriptions::new();
    let mut connect = awaiting_config(&mut subs);
    // The document's own delivery mints a cursor the plane would echo for an
    // ordinary channel.
    assert_eq!(
        subs.on_deliver(CONFIG, 1, cursor("c1"), 0)
            .expect("the config channel is active"),
        DeliverDisposition::Accept { dropped: 0 }
    );
    connect
        .on_config_deliver(&body("protobar"))
        .expect("applies");

    connect.on_detached(&mut subs);
    let frames = connect.on_attached(facts(), &mut subs);

    // Cursorless: a fresh retained replay every time, so phase 2 re-runs.
    assert_eq!(frames, vec![subscribe(CONFIG, 1, 1, None)]);
}

#[test]
fn a_detach_drops_the_config_subscription_and_keeps_the_wiring() {
    let mut subs = Subscriptions::new();
    let mut connect = awaiting_config(&mut subs);
    connect
        .on_config_deliver(&body("protobar"))
        .expect("applies");

    connect.on_detached(&mut subs);

    assert_eq!(connect.phase(), Phase::Detached);
    assert!(connect.facts().is_none());
    assert!(!subs.is_active(CONFIG));
    assert_eq!(subs.refcount(CONFIG), 0);
    // The page is still running on it, and it is what the next document is
    // compared against.
    assert!(connect.bindings().is_some());
}

#[test]
fn a_detach_leaves_the_application_channels_and_their_cursors_alone() {
    let mut subs = Subscriptions::new();
    let mut connect = awaiting_config(&mut subs);
    subs.acquire(
        WIRE,
        SubscriptionDepths {
            push_depth: 4,
            retain_depth: 2,
        },
        ResumePolicy::Resume,
    );
    subs.on_subscribe_result(WIRE, SubscribeOutcome::Ok, 0, None)
        .expect("pending");
    subs.on_deliver(WIRE, 3, cursor("c3"), 0).expect("active");

    connect.on_detached(&mut subs);
    connect.on_attached(facts(), &mut subs);

    assert_eq!(subs.refcount(WIRE), 1);
    assert_eq!(
        subs.resubscribe_survivors(),
        vec![subscribe(WIRE, 4, 2, Some("c3"))]
    );
}

#[test]
fn a_detach_with_no_attachment_behind_it_is_tolerated() {
    let mut subs = Subscriptions::new();
    let mut connect = connect();

    // A connection that dropped while negotiating reports a detach the surface
    // never got a phase 1 for.
    connect.on_detached(&mut subs);

    assert_eq!(connect.phase(), Phase::Detached);
    assert_eq!(subs.refcount(CONFIG), 0);
}

#[test]
#[should_panic(expected = "with no attachment")]
fn a_config_delivery_with_no_attachment_panics() {
    let mut connect = connect();
    let _ = connect.on_config_deliver(&body("protobar"));
}
