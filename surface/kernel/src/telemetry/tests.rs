use std::collections::BTreeMap;

use brenn_surface_schema::bindings::{
    BINDINGS_DOCUMENT_VERSION, BindingsDocument, PlatformSection,
};
use brenn_surface_schema::telemetry::{
    DEVICE_PIXEL_RATIO_MAX, GeometryDocument, Health, StatusDocument, VIEWPORT_DIMENSION_MAX,
};
use brenn_surface_schema::{
    Abi, Binding, ComponentEntry, InstanceCounters, InstanceReport, InstanceState, NoiseLevel,
    OverlayReport, StatusCounters,
};
use chrono::{TimeZone, Utc};

use super::{StatusReport, derive_health, expected_pumps, geometry_body, status_body};
use crate::bindings::AppliedBindings;

const WIRE: &str = "brenn:site.bar.in";

fn component(instance: &str) -> ComponentEntry {
    ComponentEntry {
        instance: instance.to_string(),
        kind: "protobar".to_string(),
        abi: Abi::Dom,
        parked_batch_depth: 4,
        config: BTreeMap::new(),
    }
}

fn subscription(instance: &str, port: &str) -> Binding {
    Binding {
        channel: WIRE.to_string(),
        instance: instance.to_string(),
        port: port.to_string(),
        push_depth: 4,
        retain_depth: 1,
        noise: NoiseLevel::Metered,
    }
}

/// `chrome` with no bound input, `p1` with two.
fn doc() -> BindingsDocument {
    BindingsDocument {
        v: BINDINGS_DOCUMENT_VERSION,
        components: vec![component("chrome"), component("p1")],
        subscriptions: vec![subscription("p1", "in"), subscription("p1", "aux")],
        outputs: Vec::new(),
        local_channels: Vec::new(),
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

fn applied(doc: &BindingsDocument) -> AppliedBindings {
    AppliedBindings::apply(&doc.to_body()).expect("the fixture document applies")
}

fn mounted(instance: &str, ports_attached: u32) -> InstanceReport {
    InstanceReport {
        instance: instance.to_string(),
        kind: "protobar".to_string(),
        state: InstanceState::Mounted,
        reason: None,
        ports_attached,
    }
}

fn counters() -> StatusCounters {
    StatusCounters {
        deliveries: 7,
        publishes: 3,
        errors: 1,
        telemetry_dropped: 4,
        instances: BTreeMap::new(),
    }
}

fn report<'a>(
    instances: &'a [InstanceReport],
    counters: &'a StatusCounters,
    overlay: Option<&'a OverlayReport>,
) -> StatusReport<'a> {
    StatusReport {
        instances,
        uptime_secs: 90,
        counters,
        // Deliberately not the reporter's `telemetry_dropped` above: the page's
        // own count is the one the document carries.
        telemetry_dropped: 9,
        overlay,
    }
}

/// A healthy surface: both declared instances mounted, `p1` covering both of its
/// bound input ports.
fn healthy() -> Vec<InstanceReport> {
    vec![mounted("chrome", 0), mounted("p1", 2)]
}

#[test]
fn every_declared_instance_is_an_expected_pumps_key() {
    let expected = expected_pumps(&applied(&doc()));
    assert_eq!(expected.get("chrome"), Some(&0));
    assert_eq!(expected.get("p1"), Some(&2));
    assert_eq!(expected.len(), 2);
}

#[test]
fn a_fully_mounted_surface_is_ok() {
    let expected = expected_pumps(&applied(&doc()));
    assert_eq!(derive_health(&healthy(), &expected), Health::Ok);
}

#[test]
fn a_failed_instance_degrades_the_surface() {
    let expected = expected_pumps(&applied(&doc()));
    let mut instances = healthy();
    instances[1].state = InstanceState::Failed;
    assert_eq!(derive_health(&instances, &expected), Health::Degraded);
}

#[test]
fn an_instance_short_of_its_pumps_degrades_the_surface() {
    let expected = expected_pumps(&applied(&doc()));
    let instances = vec![mounted("chrome", 0), mounted("p1", 1)];
    assert_eq!(derive_health(&instances, &expected), Health::Degraded);
}

#[test]
fn an_omitted_instance_degrades_the_surface() {
    let expected = expected_pumps(&applied(&doc()));
    assert_eq!(
        derive_health(&[mounted("p1", 2)], &expected),
        Health::Degraded,
        "an incomplete table must not read healthy"
    );
    assert_eq!(derive_health(&[], &expected), Health::Degraded);
}

#[test]
fn the_geometry_body_carries_the_reading_and_the_session() {
    let body = geometry_body("sess-1", 1280, 800, 2.0).expect("a plausible viewport");
    let doc = GeometryDocument::parse(&body).expect("the composed body parses");
    assert_eq!(doc.session, "sess-1");
    assert_eq!(doc.viewport.width, 1280);
    assert_eq!(doc.viewport.height, 800);
    assert_eq!(doc.device_pixel_ratio, 2.0);
}

#[test]
fn an_implausible_viewport_is_refused_rather_than_published() {
    let err = geometry_body("sess-1", VIEWPORT_DIMENSION_MAX + 1, 800, 1.0)
        .expect_err("an impossible width is refused");
    assert!(err.contains("width"), "{err}");
    let err = geometry_body("sess-1", 1280, 800, DEVICE_PIXEL_RATIO_MAX + 1.0)
        .expect_err("an impossible density is refused");
    assert!(err.contains("device_pixel_ratio"), "{err}");
    let err =
        geometry_body("sess-1", 1280, 800, f64::NAN).expect_err("a non-finite dpr is refused");
    assert!(err.contains("device_pixel_ratio"), "{err}");
}

#[test]
fn the_status_body_carries_the_report_and_the_derived_health() {
    let bindings = applied(&doc());
    let instances = healthy();
    let counters = counters();
    let overlay = OverlayReport {
        holder: "p1".to_string(),
        since: Utc.timestamp_opt(1_700_000_000, 0).single().expect("valid"),
    };
    let body = status_body(
        "sess-1",
        &bindings,
        &report(&instances, &counters, Some(&overlay)),
    )
    .expect("a report consistent with its wiring");
    let doc = StatusDocument::parse(&body).expect("the composed body parses");
    assert_eq!(doc.session, "sess-1");
    assert_eq!(doc.health, Health::Ok);
    assert_eq!(doc.uptime_secs, 90);
    assert_eq!(doc.instances, instances);
    assert_eq!(
        doc.counters,
        StatusCounters {
            telemetry_dropped: 9,
            ..counters
        },
        "the reporter's three totals ride through; the refusal count is the page's"
    );
    assert_eq!(doc.overlay.as_ref().map(|o| o.holder.as_str()), Some("p1"));
}

#[test]
fn a_degraded_table_is_summarized_as_such() {
    let bindings = applied(&doc());
    let instances = vec![mounted("chrome", 0), mounted("p1", 0)];
    let counters = counters();
    let body = status_body("sess-1", &bindings, &report(&instances, &counters, None))
        .expect("a report consistent with its wiring");
    let doc = StatusDocument::parse(&body).expect("the composed body parses");
    assert_eq!(doc.health, Health::Degraded);
}

#[test]
fn a_report_naming_an_undeclared_instance_is_refused() {
    let bindings = applied(&doc());
    let instances = vec![mounted("ghost", 0)];
    let counters = counters();
    let err = status_body("sess-1", &bindings, &report(&instances, &counters, None))
        .expect_err("an undeclared instance is refused");
    assert!(err.contains("ghost"), "{err}");
}

#[test]
fn a_report_whose_kind_is_not_the_configured_one_is_refused() {
    let bindings = applied(&doc());
    let mut instances = healthy();
    instances[1].kind = "protofoo".to_string();
    let counters = counters();
    let err = status_body("sess-1", &bindings, &report(&instances, &counters, None))
        .expect_err("a mismatched kind is refused");
    assert!(err.contains("kind"), "{err}");
}

#[test]
fn counters_naming_an_undeclared_instance_are_refused() {
    let bindings = applied(&doc());
    let instances = healthy();
    let mut counters = counters();
    counters
        .instances
        .insert("ghost".to_string(), InstanceCounters::default());
    let err = status_body("sess-1", &bindings, &report(&instances, &counters, None))
        .expect_err("an undeclared counter key is refused");
    assert!(err.contains("ghost"), "{err}");
}

#[test]
fn an_overlay_holder_the_surface_does_not_declare_is_refused() {
    let bindings = applied(&doc());
    let instances = healthy();
    let counters = counters();
    let overlay = OverlayReport {
        holder: "ghost".to_string(),
        since: Utc.timestamp_opt(1, 0).single().expect("valid"),
    };
    let err = status_body(
        "sess-1",
        &bindings,
        &report(&instances, &counters, Some(&overlay)),
    )
    .expect_err("an undeclared holder is refused");
    assert!(err.contains("ghost"), "{err}");
}

#[test]
fn a_rule_of_the_document_schema_reaches_the_caller() {
    let bindings = applied(&doc());
    let mut instances = healthy();
    instances.push(mounted("p1", 2));
    let counters = counters();
    let err = status_body("sess-1", &bindings, &report(&instances, &counters, None))
        .expect_err("a repeated instance is refused");
    assert!(err.contains("more than once"), "{err}");
}
