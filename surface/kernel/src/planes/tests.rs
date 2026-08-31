use std::collections::BTreeMap;

use brenn_attach_client::router::{GuardedBody, Origin, PlanePolicy};
use brenn_envelope::{ChannelScheme, MessageEnvelope, Urgency};
use brenn_surface_schema::bindings::{
    BINDINGS_DOCUMENT_VERSION, BindingsDocument, PlatformSection,
};
use brenn_surface_schema::{
    CONTROL_PLANE_VERSION, ComponentEntry, LOCAL_LINK_STATE_CHANNEL, LOCAL_OVERLAY_STATE_CHANNEL,
    LOCAL_TAKEOVER_CHANNEL, LOCAL_THEME_CHANNEL, LocalChannel, OverlayStateBody, TakeoverAction,
    TakeoverBody,
};
use chrono::{DateTime, TimeZone, Utc};

use super::SurfacePlanes;
use crate::bindings::AppliedBindings;

const PLAIN_LOCAL: &str = "local:site.notes";

fn component(instance: &str) -> ComponentEntry {
    ComponentEntry {
        instance: instance.to_string(),
        kind: "protobar".to_string(),
        parked_batch_depth: 4,
        config: BTreeMap::new(),
        grants: vec![],
    }
}

/// Chrome plus one ordinary component, with the two guarded planes and a plain
/// page-local channel declared.
fn doc(instances: &[&str]) -> BindingsDocument {
    BindingsDocument {
        v: BINDINGS_DOCUMENT_VERSION,
        components: instances.iter().map(|i| component(i)).collect(),
        subscriptions: Vec::new(),
        outputs: Vec::new(),
        local_channels: vec![
            LocalChannel {
                channel: LOCAL_TAKEOVER_CHANNEL.to_string(),
                ring_depth: 1,
            },
            LocalChannel {
                channel: LOCAL_OVERLAY_STATE_CHANNEL.to_string(),
                ring_depth: 1,
            },
            LocalChannel {
                channel: PLAIN_LOCAL.to_string(),
                ring_depth: 3,
            },
        ],
        chrome_instance: "chrome".to_string(),
        platform: PlatformSection {
            geometry_channel: "brenn:site.surface.bar.geometry".to_string(),
            status_channel: "brenn:site.surface.bar.status".to_string(),
            status_interval_secs: 60,
            error_channel: None,
            error_report_floor: None,
        },
    }
}

fn applied(instances: &[&str]) -> AppliedBindings {
    AppliedBindings::apply(&doc(instances).to_body()).expect("the fixture document applies")
}

/// A policy pointed at a surface declaring `chrome` and `p1`.
fn planes() -> SurfacePlanes {
    let mut planes = SurfacePlanes::new();
    planes.apply(&applied(&["chrome", "p1"]));
    planes
}

fn overlay_body(holder: Option<&str>) -> String {
    serde_json::to_string(&OverlayStateBody {
        v: CONTROL_PLANE_VERSION,
        holder: holder.map(str::to_string),
        since_stamp: 42,
    })
    .expect("the overlay body serializes")
}

fn takeover_body(instance: &str) -> String {
    serde_json::to_string(&TakeoverBody {
        v: CONTROL_PLANE_VERSION,
        action: TakeoverAction::Request,
        instance: instance.to_string(),
    })
    .expect("the takeover body serializes")
}

fn at(secs: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(secs, 0)
        .single()
        .expect("a valid instant")
}

fn envelope(channel: &str, body: String, publish_ts: DateTime<Utc>) -> MessageEnvelope {
    MessageEnvelope {
        message_id: uuid::Uuid::nil(),
        source: "surface:bar".to_string(),
        channel: channel.to_string(),
        sender: "surface:bar#chrome".to_string(),
        publish_ts,
        body,
        reply_to: None,
        delivery_deadline: None,
        deliver_after: None,
        impetus: None,
        urgency: Urgency::Normal,
        envelope_type: ChannelScheme::Local,
    }
}

fn carried(guarded: GuardedBody) -> String {
    match guarded {
        GuardedBody::Carry(body) => body,
        GuardedBody::Refused(reason) => panic!("expected the body to carry, refused: {reason}"),
    }
}

fn refused(guarded: GuardedBody) -> String {
    match guarded {
        GuardedBody::Refused(reason) => reason,
        GuardedBody::Carry(_) => panic!("expected a refusal"),
    }
}

#[test]
fn a_kernel_only_plane_admits_the_kernel_and_no_component() {
    let planes = planes();
    assert!(planes.admits(LOCAL_LINK_STATE_CHANNEL, Origin::Attacher));
    assert!(!planes.admits(LOCAL_LINK_STATE_CHANNEL, Origin::Sub("p1")));
}

#[test]
fn a_component_plane_admits_components_and_not_the_kernel() {
    let planes = planes();
    for channel in [
        LOCAL_THEME_CHANNEL,
        LOCAL_TAKEOVER_CHANNEL,
        LOCAL_OVERLAY_STATE_CHANNEL,
    ] {
        assert!(planes.admits(channel, Origin::Sub("p1")), "{channel}");
        assert!(!planes.admits(channel, Origin::Attacher), "{channel}");
    }
}

#[test]
fn an_operator_declared_local_channel_is_a_component_channel() {
    let planes = planes();
    assert!(planes.admits(PLAIN_LOCAL, Origin::Sub("p1")));
    assert!(!planes.admits(PLAIN_LOCAL, Origin::Attacher));
}

#[test]
fn an_undefined_reserved_address_admits_nobody() {
    let planes = planes();
    assert!(!planes.admits("local:brenn/nonesuch", Origin::Sub("p1")));
    assert!(!planes.admits("local:brenn/nonesuch", Origin::Attacher));
}

#[test]
fn an_unguarded_plane_carries_the_body_through() {
    let planes = planes();
    let body = carried(planes.guard(PLAIN_LOCAL, Origin::Sub("p1"), "{\"a\":1}".to_string()));
    assert_eq!(body, "{\"a\":1}");
}

#[test]
fn a_takeover_body_is_stamped_with_its_publisher() {
    let planes = planes();
    let body = carried(planes.guard(
        LOCAL_TAKEOVER_CHANNEL,
        Origin::Sub("p1"),
        takeover_body("chrome"),
    ));
    let parsed: TakeoverBody = serde_json::from_str(&body).expect("the stamped body parses");
    assert_eq!(
        parsed.instance, "p1",
        "the forged instance is overwritten with the publisher"
    );
}

#[test]
fn an_unparseable_takeover_body_is_carried_unchanged() {
    let planes = planes();
    let body = carried(planes.guard(
        LOCAL_TAKEOVER_CHANNEL,
        Origin::Sub("p1"),
        "not json".to_string(),
    ));
    assert_eq!(body, "not json");
}

#[test]
#[should_panic(expected = "the kernel does not publish on local:brenn/takeover")]
fn the_kernel_may_not_state_a_takeover() {
    let planes = planes();
    let _ = planes.guard(
        LOCAL_TAKEOVER_CHANNEL,
        Origin::Attacher,
        takeover_body("p1"),
    );
}

#[test]
#[should_panic(expected = "the kernel does not publish on local:brenn/overlay-state")]
fn the_kernel_may_not_report_an_overlay() {
    let planes = planes();
    let _ = planes.guard(
        LOCAL_OVERLAY_STATE_CHANNEL,
        Origin::Attacher,
        overlay_body(None),
    );
}

#[test]
fn only_chrome_may_report_the_overlay() {
    let planes = planes();
    let reason = refused(planes.guard(
        LOCAL_OVERLAY_STATE_CHANNEL,
        Origin::Sub("p1"),
        overlay_body(Some("p1")),
    ));
    assert!(reason.contains("chrome instance"), "{reason}");
}

#[test]
fn an_unparseable_overlay_body_is_refused() {
    let planes = planes();
    let reason = refused(planes.guard(
        LOCAL_OVERLAY_STATE_CHANNEL,
        Origin::Sub("chrome"),
        "not json".to_string(),
    ));
    assert!(reason.contains("unparseable body"), "{reason}");
}

#[test]
fn an_overlay_holder_the_surface_does_not_declare_is_refused() {
    let planes = planes();
    let reason = refused(planes.guard(
        LOCAL_OVERLAY_STATE_CHANNEL,
        Origin::Sub("chrome"),
        overlay_body(Some("ghost")),
    ));
    assert!(reason.contains("ghost"), "{reason}");
}

/// Both halves of the holder field pass the guard: a declared holder acquiring the
/// overlay, and a null holder releasing it. A guard that refused the release would
/// wedge the page in fullscreen with no component able to hand it back.
#[test]
fn chrome_reporting_a_declared_holder_or_a_release_is_carried() {
    for holder in [Some("p1"), None] {
        let planes = planes();
        let body = carried(planes.guard(
            LOCAL_OVERLAY_STATE_CHANNEL,
            Origin::Sub("chrome"),
            overlay_body(holder),
        ));
        assert_eq!(body, overlay_body(holder));
    }
}

#[test]
#[should_panic(expected = "a confined publish implies applied bindings")]
fn guarding_the_overlay_before_the_wiring_arrives_panics() {
    let planes = SurfacePlanes::new();
    let _ = planes.guard(
        LOCAL_OVERLAY_STATE_CHANNEL,
        Origin::Sub("chrome"),
        overlay_body(None),
    );
}

#[test]
fn observing_the_overlay_plane_records_the_holder_and_its_instant() {
    let mut planes = planes();
    planes.observe(&envelope(
        LOCAL_OVERLAY_STATE_CHANNEL,
        overlay_body(Some("p1")),
        at(1_700_000_000),
    ));
    let overlay = planes.overlay().expect("an overlay is recorded");
    assert_eq!(overlay.holder, "p1");
    assert_eq!(overlay.since, at(1_700_000_000));
}

#[test]
fn a_released_overlay_clears_the_record() {
    let mut planes = planes();
    planes.observe(&envelope(
        LOCAL_OVERLAY_STATE_CHANNEL,
        overlay_body(Some("p1")),
        at(1),
    ));
    planes.observe(&envelope(
        LOCAL_OVERLAY_STATE_CHANNEL,
        overlay_body(None),
        at(2),
    ));
    assert!(planes.overlay().is_none());
}

#[test]
fn traffic_on_another_plane_records_nothing() {
    let mut planes = planes();
    planes.observe(&envelope(
        LOCAL_TAKEOVER_CHANNEL,
        takeover_body("p1"),
        at(1),
    ));
    assert!(planes.overlay().is_none());
}

#[test]
fn a_holder_retired_between_the_guard_and_the_release_is_not_recorded() {
    let mut planes = planes();
    let parked = envelope(LOCAL_OVERLAY_STATE_CHANNEL, overlay_body(Some("p1")), at(1));
    planes.apply(&applied(&["chrome"]));
    planes.observe(&parked);
    assert!(planes.overlay().is_none());
}

#[test]
fn applying_wiring_without_the_recorded_holder_drops_it() {
    let mut planes = planes();
    planes.observe(&envelope(
        LOCAL_OVERLAY_STATE_CHANNEL,
        overlay_body(Some("p1")),
        at(1),
    ));
    planes.apply(&applied(&["chrome"]));
    assert!(planes.overlay().is_none());
}

#[test]
fn applying_wiring_that_still_declares_the_holder_keeps_it() {
    let mut planes = planes();
    planes.observe(&envelope(
        LOCAL_OVERLAY_STATE_CHANNEL,
        overlay_body(Some("p1")),
        at(1),
    ));
    planes.apply(&applied(&["chrome", "p1", "p2"]));
    assert_eq!(
        planes.overlay().map(|o| o.holder.as_str()),
        Some("p1"),
        "a live wedge is exactly what a reconnect must keep reporting"
    );
}
