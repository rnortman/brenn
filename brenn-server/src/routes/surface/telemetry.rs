//! Runtime surface telemetry: the geometry and status documents a live surface
//! session publishes to its derived per-surface channels.
//!
//! The shell reports raw facts over the `ClientFrame::Geometry` / `Status`
//! frames; this module validates them (the shell is untrusted even when
//! authenticated), derives the health summary **server-side** from the reported
//! instance states, and builds the server-stamped JSON documents (`v: 1`, a
//! server-clock `ts`, the reporting `session`, the boot `epoch`) the session
//! publishes via the platform-telemetry publish path. Both documents are
//! latest-wins on a retained-depth-bounded durable channel.

use std::collections::{HashMap, HashSet};

use brenn_lib::messaging::config::ResolvedSurface;
use brenn_lib::messaging::{Messenger, PublishResult, Urgency};
use brenn_surface_proto::{InstanceReport, InstanceState, StatusCounters};
use serde::Serialize;
use serde_json::json;

use super::description::surface_status_channel;

/// Body-schema version stamped on every telemetry document (`v: 1`).
const SCHEMA_VERSION: u32 = 1;

/// Physically-plausible viewport dimension bounds (CSS pixels), not UX policy: a
/// generous window a real display could present. Out of range ⇒ protocol
/// violation (a conforming shell reports the real viewport).
const MIN_DIMENSION: u32 = 1;
const MAX_DIMENSION: u32 = 32_768;

/// Device-pixel-ratio bounds: generous physical plausibility, finite required.
const MIN_DPR: f64 = 0.1;
const MAX_DPR: f64 = 16.0;

/// Per-instance `reason` cap (bytes) in a status report — bounds the status body
/// so `BodyTooLarge` stays structurally unreachable for a conforming shell.
const MAX_REASON_BYTES: usize = 256;

/// Derived surface health, computed server-side from the reported instance
/// states and pump attachment. Serialized lowercase for the status document.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Health {
    /// Every instance mounted and every bound subscription has an attached pump.
    Ok,
    /// At least one instance failed or one binding is pumpless, but the session
    /// is live.
    Degraded,
    /// No session attached (a terminal or boot stamp). Not derivable from a live
    /// report; produced by the server-written snapshot path.
    Disconnected,
}

/// Validate a `ClientFrame::Geometry` report's bounds. `Err` names the violated
/// rule (never echoing client values) for the protocol-violation log.
pub fn validate_geometry(width: u32, height: u32, device_pixel_ratio: f64) -> Result<(), String> {
    if !(MIN_DIMENSION..=MAX_DIMENSION).contains(&width) {
        return Err(format!(
            "Geometry width out of bounds {MIN_DIMENSION}..={MAX_DIMENSION}"
        ));
    }
    if !(MIN_DIMENSION..=MAX_DIMENSION).contains(&height) {
        return Err(format!(
            "Geometry height out of bounds {MIN_DIMENSION}..={MAX_DIMENSION}"
        ));
    }
    if !device_pixel_ratio.is_finite() || !(MIN_DPR..=MAX_DPR).contains(&device_pixel_ratio) {
        return Err(format!(
            "Geometry device_pixel_ratio not finite in {MIN_DPR}..={MAX_DPR}"
        ));
    }
    Ok(())
}

/// Build the geometry document as a JSON string. Bounds are assumed
/// already validated by [`validate_geometry`].
pub fn geometry_body(
    surface: &str,
    session: &str,
    width: u32,
    height: u32,
    device_pixel_ratio: f64,
) -> String {
    let body = json!({
        "v": SCHEMA_VERSION,
        "surface": surface,
        "session": session,
        "ts": chrono::Utc::now().to_rfc3339(),
        "viewport": { "width": width, "height": height },
        "device_pixel_ratio": device_pixel_ratio,
    });
    serde_json::to_string(&body).expect("geometry document serializes to JSON")
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
pub fn validate_status(
    instances: &[InstanceReport],
    counters: &StatusCounters,
    configured_instances: &HashMap<String, String>,
) -> Result<(), String> {
    for instance in counters.instances.keys() {
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
            && reason.len() > MAX_REASON_BYTES
        {
            return Err(format!(
                "Status reason for instance {:?} exceeds {MAX_REASON_BYTES} bytes",
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
/// server-derived `health`, the reporting `session`, the boot `epoch`, and the
/// shell-reported instances / uptime / counters. `reason` is `null` for a live
/// report (the server-written stamps carry the closing reason).
#[allow(clippy::too_many_arguments)]
pub fn status_body(
    surface: &str,
    session: &str,
    epoch: uuid::Uuid,
    health: Health,
    uptime_secs: u64,
    instances: &[InstanceReport],
    counters: StatusCounters,
) -> String {
    let body = json!({
        "v": SCHEMA_VERSION,
        "surface": surface,
        "session": session,
        "ts": chrono::Utc::now().to_rfc3339(),
        "epoch": epoch,
        "health": health,
        "reason": serde_json::Value::Null,
        "uptime_secs": uptime_secs,
        "instances": instances,
        "counters": counters,
    });
    serde_json::to_string(&body).expect("status document serializes to JSON")
}

/// Build a server-written `disconnected` status document: the terminal
/// snapshot when the last session for a slug closes, and the boot stamp. Unlike a
/// live report, `health` is fixed `disconnected`, `reason` names the cause, and
/// there is no shell-reported uptime or counters (both `null` — a server-written
/// stamp has no page uptime and no shell counter totals). `session` is the closing
/// session for a terminal snapshot and `None` for a boot stamp; `instances`
/// carries the last-known list for a terminal snapshot or empty for a boot stamp.
pub fn disconnected_body(
    surface: &str,
    session: Option<&str>,
    epoch: uuid::Uuid,
    reason: &str,
    instances: &[InstanceReport],
) -> String {
    let body = json!({
        "v": SCHEMA_VERSION,
        "surface": surface,
        "session": session,
        "ts": chrono::Utc::now().to_rfc3339(),
        "epoch": epoch,
        "health": Health::Disconnected,
        "reason": reason,
        "uptime_secs": serde_json::Value::Null,
        "instances": instances,
        "counters": serde_json::Value::Null,
    });
    serde_json::to_string(&body).expect("disconnected status document serializes to JSON")
}

/// Publish a boot `disconnected` stamp (`reason: "server restart"`, the new bus
/// `epoch`, empty instances) to every configured surface's status channel, once
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
        let body = disconnected_body(&surface.slug, None, epoch, "server restart", &[]);
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

    #[test]
    fn geometry_body_schema() {
        let s = geometry_body("bar", "sess", 1920, 515, 2.0);
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["v"], json!(1));
        assert_eq!(v["surface"], json!("bar"));
        assert_eq!(v["session"], json!("sess"));
        assert_eq!(v["viewport"], json!({ "width": 1920, "height": 515 }));
        assert_eq!(v["device_pixel_ratio"], json!(2.0));
        assert!(v["ts"].is_string());
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

    #[test]
    fn status_validation_subset() {
        let configured = configured_map(&[("p1", "protobar"), ("clock", "mode-clock")]);
        let none = StatusCounters::default();
        let ok = vec![report("p1", "protobar", InstanceState::Mounted, 1)];
        assert!(validate_status(&ok, &none, &configured).is_ok());
        // Unconfigured instance.
        let bad = vec![report("ghost", "protobar", InstanceState::Mounted, 1)];
        assert!(validate_status(&bad, &none, &configured).is_err());
        // Configured instance, wrong kind.
        let wrong = vec![report("p1", "mode-clock", InstanceState::Mounted, 1)];
        assert!(validate_status(&wrong, &none, &configured).is_err());
        // Over-long reason.
        let mut long = report("p1", "protobar", InstanceState::Failed, 0);
        long.reason = Some("x".repeat(MAX_REASON_BYTES + 1));
        assert!(validate_status(&[long], &none, &configured).is_err());
        // Duplicate instance — a multiset with repeats is not a subset.
        let dup = vec![
            report("p1", "protobar", InstanceState::Mounted, 1),
            report("p1", "protobar", InstanceState::Failed, 0),
        ];
        assert!(validate_status(&dup, &none, &configured).is_err());
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
        assert!(validate_status(&ok, &counters_for(&["p1", "clock"]), &configured).is_ok());
        // An instance that counted nothing may simply be absent.
        assert!(validate_status(&ok, &counters_for(&[]), &configured).is_ok());
        // A key naming a component the surface does not configure.
        let err = validate_status(&ok, &counters_for(&["ghost"]), &configured)
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

    #[test]
    fn status_body_schema() {
        let epoch = uuid::Uuid::nil();
        let instances = vec![report("p1", "protobar", InstanceState::Mounted, 1)];
        let s = status_body(
            "bar",
            "sess",
            epoch,
            Health::Degraded,
            86_400,
            &instances,
            StatusCounters {
                deliveries: 10,
                publishes: 2,
                errors: 1,
                instances: [(
                    "p1".to_string(),
                    brenn_surface_proto::InstanceCounters {
                        publishes: 2,
                        drops: 5,
                    },
                )]
                .into_iter()
                .collect(),
            },
        );
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["v"], json!(1));
        assert_eq!(v["surface"], json!("bar"));
        assert_eq!(v["health"], json!("degraded"));
        assert_eq!(v["reason"], json!(null));
        assert_eq!(v["uptime_secs"], json!(86_400));
        assert_eq!(v["epoch"], json!("00000000-0000-0000-0000-000000000000"));
        assert_eq!(v["instances"][0]["instance"], json!("p1"));
        assert_eq!(v["counters"]["deliveries"], json!(10));
        // The per-instance breakdown reaches the retained document — the plane
        // an operator (or the LLM, via MessageChannelGet) actually reads. The
        // document is where attribution has to land; counting it page-side and
        // dropping it here would be counting for nobody.
        assert_eq!(
            v["counters"]["instances"],
            json!({ "p1": { "publishes": 2, "drops": 5 } })
        );
    }

    #[test]
    fn disconnected_body_boot_stamp_schema() {
        // Boot stamp: no session, empty instances, null uptime/counters.
        let s = disconnected_body("bar", None, uuid::Uuid::nil(), "server restart", &[]);
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["v"], json!(1));
        assert_eq!(v["surface"], json!("bar"));
        assert_eq!(v["session"], json!(null));
        assert_eq!(v["health"], json!("disconnected"));
        assert_eq!(v["reason"], json!("server restart"));
        assert_eq!(v["uptime_secs"], json!(null));
        assert_eq!(v["counters"], json!(null));
        assert_eq!(v["instances"], json!([]));
        assert_eq!(v["epoch"], json!("00000000-0000-0000-0000-000000000000"));
        assert!(v["ts"].is_string());
    }

    #[test]
    fn disconnected_body_terminal_snapshot_carries_session_and_instances() {
        // Terminal snapshot: the closing session id and the last-known instances.
        let instances = vec![report("p1", "protobar", InstanceState::Failed, 0)];
        let s = disconnected_body(
            "bar",
            Some("sess"),
            uuid::Uuid::nil(),
            "session closed",
            &instances,
        );
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["session"], json!("sess"));
        assert_eq!(v["health"], json!("disconnected"));
        assert_eq!(v["reason"], json!("session closed"));
        assert_eq!(v["instances"][0]["instance"], json!("p1"));
        assert_eq!(v["instances"][0]["state"], json!("failed"));
    }
}
