//! Runtime surface telemetry: the geometry and status documents a live surface
//! session publishes to its derived per-surface channels, and the server-written
//! `disconnected` stamp.
//!
//! The shell reports raw facts over the `ClientFrame::Geometry` / `Status`
//! frames; this module validates them against the surface's configured instance
//! set (the shell is untrusted even when authenticated), derives the health
//! summary **server-side** from the reported instance states, and composes the
//! documents the session publishes via the platform-telemetry publish path. The
//! document shapes themselves are
//! [`brenn_surface_schema::telemetry`](brenn_surface_schema::telemetry) — shared
//! with the kernel, which is where composition ends up. Every document on a
//! given channel is latest-wins on a retained-depth-bounded channel.

use std::collections::{HashMap, HashSet};

use brenn_lib::messaging::config::ResolvedSurface;
use brenn_lib::messaging::{Messenger, PublishResult, Urgency};
/// The health summary this module derives, on its way into a status document.
pub use brenn_surface_schema::telemetry::Health;
use brenn_surface_schema::telemetry::{
    DisconnectedStamp, GeometryDocument, MAX_INSTANCE_REASON_BYTES, StatusDocument,
    TELEMETRY_DOCUMENT_VERSION, validate_viewport,
};
use brenn_surface_schema::{InstanceReport, InstanceState, OverlayReport, StatusCounters};

use super::description::surface_status_channel;

// TODO(attach-cutover): the frame validation, health derivation and document
// composition here are duplicated by `brenn_surface_kernel::telemetry`, which
// authors both documents from the wiring the page holds. Everything but the
// server-written disconnected stamp goes when the telemetry frames go.

/// Validate a `ClientFrame::Geometry` report's bounds. `Err` names the violated
/// rule (never echoing client values) for the protocol-violation log.
pub fn validate_geometry(width: u32, height: u32, device_pixel_ratio: f64) -> Result<(), String> {
    validate_viewport(width, height, device_pixel_ratio)
}

/// Build the geometry document as a JSON string. Bounds are assumed
/// already validated by [`validate_geometry`].
pub fn geometry_body(session: &str, width: u32, height: u32, device_pixel_ratio: f64) -> String {
    GeometryDocument::new(session.to_string(), width, height, device_pixel_ratio).to_body()
}

/// The facts one `ClientFrame::Status` frame reports, borrowed for the length of
/// the frame's handling.
///
/// One bundle rather than a positional list threaded through validation and
/// document-building: the fields travel together everywhere, several share a
/// shape (so a transposition would compile), and the set grows with the status
/// schema — each added field would otherwise churn three signatures and every
/// call site.
pub struct StatusReport<'a> {
    pub instances: &'a [InstanceReport],
    pub uptime_secs: u64,
    pub counters: &'a StatusCounters,
    /// The overlay chrome holds, as the kernel recorded it; `None` when none is
    /// held.
    pub overlay: Option<&'a OverlayReport>,
}

/// Validate a `ClientFrame::Status` report against the surface's configured
/// instance set. A report naming an instance the surface does not configure, or
/// naming the same instance more than once, is a protocol violation (the
/// contract is a *subset* of the configured instances — a multiset with repeats
/// is not a subset, and repeats would bloat the retained body and let a
/// contradictory pair, e.g. `mounted` + `failed` for one instance, both land in
/// the document); an over-long `reason` is likewise a violation. Rejecting
/// duplicates also caps `instances.len()` at the configured count. `Err` names
/// the rule without echoing client values.
///
/// `counters.instances` wears the same configured-instance rule, and for the
/// same reason: it is a client-supplied map whose keys name principals, so an
/// unconfigured key is either a broken shell or a client inventing a principal —
/// and the retained status document is where an operator reads attribution. The
/// map's own type rejects duplicate keys, so only membership needs checking, and
/// membership bounds its size at the configured count. A key may be absent (an
/// instance that did nothing counts nothing); it may not be unknown.
///
/// `overlay`'s `holder` wears the same rule for the same reason: it names a
/// principal, it reaches the retained document an operator reads, and an
/// unconfigured one is a shell inventing a component.
pub fn validate_status(
    report: &StatusReport<'_>,
    configured_instances: &HashMap<String, String>,
) -> Result<(), String> {
    let instances = report.instances;
    if let Some(overlay) = report.overlay
        && !configured_instances.contains_key(overlay.holder.as_str())
    {
        return Err(format!(
            "Status overlay names unconfigured holder {:?}",
            overlay.holder
        ));
    }
    for instance in report.counters.instances.keys() {
        if !configured_instances.contains_key(instance.as_str()) {
            return Err(format!(
                "Status counters name unconfigured instance {instance:?}"
            ));
        }
    }
    let mut seen: HashSet<&str> = HashSet::with_capacity(instances.len());
    for report in instances {
        if !seen.insert(report.instance.as_str()) {
            return Err(format!(
                "Status reports instance {:?} more than once",
                report.instance
            ));
        }
        match configured_instances.get(report.instance.as_str()) {
            Some(kind) if *kind == report.kind => {}
            Some(_) => {
                return Err(format!(
                    "Status instance {:?} reports a kind that does not match its configured kind",
                    report.instance
                ));
            }
            None => {
                return Err(format!(
                    "Status names unconfigured instance {:?}",
                    report.instance
                ));
            }
        }
        if let Some(reason) = &report.reason
            && reason.len() > MAX_INSTANCE_REASON_BYTES
        {
            return Err(format!(
                "Status reason for instance {:?} exceeds {MAX_INSTANCE_REASON_BYTES} bytes",
                report.instance
            ));
        }
    }
    Ok(())
}

/// Derive surface health from the reported instance states and pump attachment.
/// `expected_pumps` maps **every** configured instance to the number of
/// subscription bindings it should have an attached pump for. A live report is
/// `Ok` only when every configured instance is present in the report, `Mounted`,
/// and covers its expected pumps; otherwise `Degraded`. Requiring every configured
/// instance closes the "shell omits its failed instances (or reports an empty
/// list) and the snapshot reads `ok`" hole — server-side derivation is only a
/// defense against an untrusted shell if a missing instance is not-ok, not
/// silently ignored. `Disconnected` is never derived from a live report (it is a
/// server-written stamp).
pub fn derive_health(
    instances: &[InstanceReport],
    expected_pumps: &HashMap<String, u32>,
) -> Health {
    let all_ok = expected_pumps.iter().all(|(instance, &expected)| {
        instances.iter().any(|report| {
            report.instance == *instance
                && report.state == InstanceState::Mounted
                && report.ports_attached >= expected
        })
    });
    if all_ok { Health::Ok } else { Health::Degraded }
}

/// Build the status document as a JSON string from a live report: the
/// server-derived `health`, the reporting `session`, and the shell-reported
/// instances / uptime / counters.
///
/// # Panics
///
/// If the composed document fails the schema's own rules. Every rule it checks
/// is one [`validate_status`] has already enforced on the report, or one
/// [`derive_health`] cannot violate, so a failure here means the two rule sets
/// have drifted — a bug in this build, not a bad frame. Running the schema check
/// on the way out is what makes the published body enforced-by-construction
/// against the gate every reader will apply.
pub fn status_body(session: &str, health: Health, report: &StatusReport<'_>) -> String {
    let doc = StatusDocument {
        v: TELEMETRY_DOCUMENT_VERSION,
        session: session.to_string(),
        health,
        uptime_secs: report.uptime_secs,
        instances: report.instances.to_vec(),
        counters: report.counters.clone(),
        overlay: report.overlay.cloned(),
    };
    doc.validate()
        .expect("status document composed from a validated report");
    doc.to_body()
}

/// Build a server-written `disconnected` stamp: the terminal snapshot when the
/// last session for a slug closes, and the boot stamp. `session` is the closing
/// session for a terminal snapshot and `None` for a boot stamp.
///
/// # Panics
///
/// If the composed stamp fails the schema's own rules — an empty `reason` or an
/// empty `session`. Every caller passes a literal reason and a minted session
/// id, so a failure is a broken caller in this build rather than bad input; the
/// alternative is publishing a retained stamp every reader refuses, which is how
/// "is this surface down?" would silently stop having an answer.
pub fn disconnected_body(session: Option<&str>, epoch: uuid::Uuid, reason: &str) -> String {
    let stamp = DisconnectedStamp::new(
        session.map(str::to_string),
        chrono::Utc::now(),
        epoch,
        reason.to_string(),
    );
    stamp
        .validate()
        .expect("disconnected stamp composed from server-held facts");
    stamp.to_body()
}

/// Publish a boot `disconnected` stamp (`reason: "server restart"`, the new bus
/// `epoch`) to every configured surface's status channel, once
/// at boot after the boot-published documents. A durable status channel's
/// retained row survives a restart; without this stamp a dead or not-yet-connected
/// wall would read "healthy as of before the restart" until a reader did timestamp
/// math. Published via the platform path (send-budget exempt).
///
/// # Panics
///
/// Any non-`Ok` outcome is a broken boot invariant — the status channel is
/// boot-declared, single-writer, and covered by the surface's injected
/// geometry/status grant, and the platform path is send-budget exempt — so it
/// panics rather than starting with a stale retained value.
pub async fn publish_boot_disconnected_stamps(
    messenger: &Messenger,
    prefix: &str,
    surfaces: &[ResolvedSurface],
    epoch: uuid::Uuid,
) {
    for surface in surfaces {
        let channel = surface_status_channel(prefix, &surface.slug);
        let body = disconnected_body(None, epoch, "server restart");
        match messenger
            .publish_from_surface_platform(&surface.slug, &channel, &body, Urgency::Normal)
            .await
        {
            PublishResult::Ok { .. } => {}
            other => panic!(
                "boot: surface {} disconnected boot stamp publish to {channel} did not succeed \
                 ({other:?}) — the status channel is boot-declared, single-writer, and covered by \
                 the surface's injected geometry/status grant, and the platform path is send-budget \
                 exempt, so any failure is a broken boot invariant. Refusing to start.",
                surface.slug,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(instance: &str, kind: &str, state: InstanceState, ports: u32) -> InstanceReport {
        InstanceReport {
            instance: instance.to_string(),
            kind: kind.to_string(),
            state,
            reason: None,
            ports_attached: ports,
        }
    }

    #[test]
    fn geometry_bounds() {
        assert!(validate_geometry(1920, 1080, 2.0).is_ok());
        assert!(validate_geometry(0, 1080, 1.0).is_err());
        assert!(validate_geometry(1920, 40_000, 1.0).is_err());
        assert!(validate_geometry(1920, 1080, 0.0).is_err());
        assert!(validate_geometry(1920, 1080, 100.0).is_err());
        assert!(validate_geometry(1920, 1080, f64::NAN).is_err());
        assert!(validate_geometry(1920, 1080, f64::INFINITY).is_err());
    }

    /// The document shape is pinned in `brenn-surface-schema`; what this side
    /// owes is that the frame's values reach the body it composes.
    #[test]
    fn geometry_body_carries_the_reported_viewport() {
        let doc = brenn_surface_schema::telemetry::GeometryDocument::parse(&geometry_body(
            "sess", 1920, 515, 2.0,
        ))
        .expect("the composed body is a valid geometry document");
        assert_eq!(doc.session, "sess");
        assert_eq!(doc.viewport.width, 1920);
        assert_eq!(doc.viewport.height, 515);
        assert_eq!(doc.device_pixel_ratio, 2.0);
    }

    fn configured_map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(i, k)| (i.to_string(), k.to_string()))
            .collect()
    }

    fn expected_map(pairs: &[(&str, u32)]) -> HashMap<String, u32> {
        pairs.iter().map(|(i, n)| (i.to_string(), *n)).collect()
    }

    /// Counters carrying a per-instance breakdown over `instances`, all zero —
    /// the shape matters here, not the values.
    fn counters_for(instances: &[&str]) -> StatusCounters {
        StatusCounters {
            instances: instances
                .iter()
                .map(|i| (i.to_string(), Default::default()))
                .collect(),
            ..Default::default()
        }
    }

    /// A status report over the given facts, at a fixed uptime (no test here
    /// reads it).
    fn status_report<'a>(
        instances: &'a [InstanceReport],
        counters: &'a StatusCounters,
        overlay: Option<&'a OverlayReport>,
    ) -> StatusReport<'a> {
        StatusReport {
            instances,
            uptime_secs: 1,
            counters,
            overlay,
        }
    }

    #[test]
    fn status_validation_subset() {
        let configured = configured_map(&[("p1", "protobar"), ("clock", "mode-clock")]);
        let none = StatusCounters::default();
        let ok = vec![report("p1", "protobar", InstanceState::Mounted, 1)];
        assert!(validate_status(&status_report(&ok, &none, None), &configured).is_ok());
        // Unconfigured instance.
        let bad = vec![report("ghost", "protobar", InstanceState::Mounted, 1)];
        assert!(validate_status(&status_report(&bad, &none, None), &configured).is_err());
        // Configured instance, wrong kind.
        let wrong = vec![report("p1", "mode-clock", InstanceState::Mounted, 1)];
        assert!(validate_status(&status_report(&wrong, &none, None), &configured).is_err());
        // Over-long reason.
        let mut long = report("p1", "protobar", InstanceState::Failed, 0);
        long.reason = Some("x".repeat(MAX_INSTANCE_REASON_BYTES + 1));
        let long = vec![long];
        assert!(validate_status(&status_report(&long, &none, None), &configured).is_err());
        // Duplicate instance — a multiset with repeats is not a subset.
        let dup = vec![
            report("p1", "protobar", InstanceState::Mounted, 1),
            report("p1", "protobar", InstanceState::Failed, 0),
        ];
        assert!(validate_status(&status_report(&dup, &none, None), &configured).is_err());
    }

    /// The per-instance counter map wears the configured-instance rule: a key
    /// naming an unconfigured instance is a violation, exactly as in `instances`.
    /// Attribution the operator reads must name principals the operator declared.
    #[test]
    fn status_validation_counters_instances_must_be_configured() {
        let configured = configured_map(&[("p1", "protobar"), ("clock", "mode-clock")]);
        let ok = vec![report("p1", "protobar", InstanceState::Mounted, 1)];
        // Both configured instances, including one the `instances` list omits —
        // the two lists are independent subsets of the same configured set.
        let both = counters_for(&["p1", "clock"]);
        assert!(validate_status(&status_report(&ok, &both, None), &configured).is_ok());
        // An instance that counted nothing may simply be absent.
        let empty = counters_for(&[]);
        assert!(validate_status(&status_report(&ok, &empty, None), &configured).is_ok());
        // A key naming a component the surface does not configure.
        let ghost = counters_for(&["ghost"]);
        let err = validate_status(&status_report(&ok, &ghost, None), &configured)
            .expect_err("an unconfigured counter key is a violation");
        assert!(
            err.contains("counters") && err.contains("ghost"),
            "the rule names the counters map and the offending key: {err}"
        );
    }

    #[test]
    fn health_derivation() {
        let expected = expected_map(&[("p1", 1), ("clock", 0)]);
        // All mounted with enough pumps ⇒ ok.
        let ok = vec![
            report("p1", "protobar", InstanceState::Mounted, 1),
            report("clock", "mode-clock", InstanceState::Mounted, 0),
        ];
        assert_eq!(derive_health(&ok, &expected), Health::Ok);
        // One failed ⇒ degraded.
        let failed = vec![
            report("p1", "protobar", InstanceState::Failed, 0),
            report("clock", "mode-clock", InstanceState::Mounted, 0),
        ];
        assert_eq!(derive_health(&failed, &expected), Health::Degraded);
        // Mounted but pumpless ⇒ degraded (p1 present but under its pump count;
        // clock omitted, which is independently not-ok).
        let pumpless = vec![report("p1", "protobar", InstanceState::Mounted, 0)];
        assert_eq!(derive_health(&pumpless, &expected), Health::Degraded);
        // Pending ⇒ degraded.
        let pending = vec![report("p1", "protobar", InstanceState::Pending, 1)];
        assert_eq!(derive_health(&pending, &expected), Health::Degraded);
        // Empty report while instances are configured ⇒ degraded, never ok: a
        // shell that reports nothing (or omits its failed instances) must not read
        // healthy.
        assert_eq!(derive_health(&[], &expected), Health::Degraded);
        // A report covering only a subset of configured instances ⇒ degraded, even
        // when every reported instance is itself healthy.
        let partial = vec![report("p1", "protobar", InstanceState::Mounted, 1)];
        assert_eq!(derive_health(&partial, &expected), Health::Degraded);
    }

    /// The overlay `p1` holds from the epoch, for the document-shape tests.
    fn held_overlay() -> OverlayReport {
        OverlayReport {
            holder: "p1".to_string(),
            since: chrono::DateTime::UNIX_EPOCH,
        }
    }

    /// The reported facts and the *server-derived* health both reach the body —
    /// the report is the shell's, the summary is not.
    #[test]
    fn status_body_carries_the_report_and_the_derived_health() {
        let instances = vec![report("p1", "protobar", InstanceState::Mounted, 1)];
        let overlay = held_overlay();
        let counters = StatusCounters {
            deliveries: 10,
            publishes: 2,
            errors: 1,
            instances: [(
                "p1".to_string(),
                brenn_surface_schema::InstanceCounters {
                    publishes: 2,
                    drops: 5,
                },
            )]
            .into_iter()
            .collect(),
        };
        let body = status_body(
            "sess",
            Health::Degraded,
            &StatusReport {
                instances: &instances,
                uptime_secs: 86_400,
                counters: &counters,
                overlay: Some(&overlay),
            },
        );
        let doc = brenn_surface_schema::telemetry::StatusDocument::parse(&body)
            .expect("the composed body is a valid status document");
        assert_eq!(doc.session, "sess");
        assert_eq!(doc.health, Health::Degraded);
        assert_eq!(doc.uptime_secs, 86_400);
        assert_eq!(doc.instances, instances);
        assert_eq!(doc.counters, counters);
        // The held overlay reaches the document, holder and start both: this
        // field is the whole reason a wedged surface is distinguishable from a
        // healthy one in the retained snapshot.
        assert_eq!(doc.overlay, Some(overlay));
    }

    #[test]
    fn status_overlay_holder_must_be_a_configured_instance() {
        // Same rule the instance reports wear, for the same reason: the holder
        // names a principal that reaches the retained document, and the shell is
        // untrusted even when authenticated.
        let configured: HashMap<String, String> = [
            ("p1".to_string(), "protobar".to_string()),
            ("clock".to_string(), "mode-clock".to_string()),
        ]
        .into_iter()
        .collect();
        let none = StatusCounters::default();
        let instances = vec![report("p1", "protobar", InstanceState::Mounted, 1)];
        let held = held_overlay();
        assert!(
            validate_status(&status_report(&instances, &none, Some(&held)), &configured).is_ok()
        );
        let ghost = OverlayReport {
            holder: "ghost".to_string(),
            since: chrono::DateTime::UNIX_EPOCH,
        };
        let err = validate_status(&status_report(&instances, &none, Some(&ghost)), &configured)
            .expect_err("an unconfigured holder is a violation");
        assert!(err.contains("unconfigured holder"), "unexpected: {err}");
    }

    /// Both stamp flavours: the boot stamp names no session, the terminal one
    /// names the session that closed. Each carries the bus epoch a reader
    /// compares against a live document's.
    #[test]
    fn disconnected_body_covers_both_stamp_flavours() {
        let boot = DisconnectedStamp::parse(&disconnected_body(
            None,
            uuid::Uuid::nil(),
            "server restart",
        ))
        .expect("the boot stamp is valid");
        assert_eq!(boot.session, None);
        assert_eq!(boot.health, Health::Disconnected);
        assert_eq!(boot.reason, "server restart");
        assert_eq!(boot.epoch, uuid::Uuid::nil());

        let terminal = DisconnectedStamp::parse(&disconnected_body(
            Some("sess"),
            uuid::Uuid::nil(),
            "session closed",
        ))
        .expect("the terminal stamp is valid");
        assert_eq!(terminal.session.as_deref(), Some("sess"));
        assert_eq!(terminal.reason, "session closed");
    }

    /// A stamp with no reason is one every reader refuses, so the composer
    /// refuses to publish it instead of leaving the channel's latest-wins row
    /// unreadable.
    #[test]
    #[should_panic(expected = "disconnected stamp composed from server-held facts")]
    fn a_reasonless_stamp_does_not_compose() {
        disconnected_body(Some("sess"), uuid::Uuid::nil(), "");
    }

    /// **The two rule sets must not drift.** `status_body` panics on a document
    /// its own crate's `validate` refuses, and the input is a client frame — so
    /// every schema-side rejection has to be one `validate_status` already
    /// refuses, or the panic becomes reachable from the wire. One case per
    /// `StatusDocument::validate` rule that a report can express.
    #[test]
    fn every_schema_status_rule_is_refused_by_the_frame_validator_first() {
        let configured = configured_map(&[("p1", "protobar")]);
        let none = StatusCounters::default();

        // Duplicate instance.
        let dup = vec![
            report("p1", "protobar", InstanceState::Mounted, 1),
            report("p1", "protobar", InstanceState::Failed, 0),
        ];
        // Over-long reason.
        let mut long = report("p1", "protobar", InstanceState::Failed, 0);
        long.reason = Some("x".repeat(MAX_INSTANCE_REASON_BYTES + 1));
        let long = vec![long];
        // An empty instance id is unconfigurable, so the frame validator refuses
        // it as an unconfigured instance before the schema calls it empty.
        let empty_id = vec![report("", "protobar", InstanceState::Mounted, 1)];

        for (case, instances) in [("duplicate", dup), ("reason", long), ("empty id", empty_id)] {
            let refused = validate_status(&status_report(&instances, &none, None), &configured);
            assert!(refused.is_err(), "{case}: the frame validator must refuse");
            // And the schema would have refused it too — which is what makes the
            // pair a covering, not merely an overlapping, rule set.
            let doc = StatusDocument {
                v: TELEMETRY_DOCUMENT_VERSION,
                session: "sess".to_string(),
                health: Health::Ok,
                uptime_secs: 1,
                instances,
                counters: none.clone(),
                overlay: None,
            };
            assert!(doc.validate().is_err(), "{case}: the schema rule is real");
        }

        // `health` is the one schema rule no report can express: the derivation
        // has no `Disconnected` answer.
        let mounted = vec![report("p1", "protobar", InstanceState::Mounted, 1)];
        assert_ne!(
            derive_health(&mounted, &expected_map(&[("p1", 1)])),
            Health::Disconnected
        );

        // The boundary-valid shape composes: at-cap reason, one report per
        // configured instance, no overlay — the case most likely to straddle a
        // future rule.
        let mut at_cap = report("p1", "protobar", InstanceState::Failed, 0);
        at_cap.reason = Some("x".repeat(MAX_INSTANCE_REASON_BYTES));
        let at_cap = vec![at_cap];
        let boundary = status_report(&at_cap, &none, None);
        assert!(validate_status(&boundary, &configured).is_ok());
        let body = status_body(
            "sess",
            derive_health(&at_cap, &expected_map(&[("p1", 1)])),
            &boundary,
        );
        assert!(StatusDocument::parse(&body).is_ok());
    }
}
