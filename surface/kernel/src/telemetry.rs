//! The two documents a live surface publishes about itself: its viewport and
//! its mount status.
//!
//! Both are authored here. The kernel observes the viewport, owns the instance
//! table, and holds the wiring that says what a working surface looks like, so
//! it is the only party that knows either — and both go out as ordinary
//! publishes on the channels the surface's wiring names, subject to the same
//! gates as any other publish.
//!
//! What the kernel does *not* author is the `disconnected` stamp: a gone page
//! cannot report, so that status is the server's responsibility.
//!
//! **Provenance is the envelope's.** Who published and when are the envelope's
//! `sender` and `publish_ts`, stamped by the peer and unforgeable here, so
//! neither document restates the surface's identity or carries a body-authored
//! timestamp. What the body carries is what only the page knows: which
//! attachment reported (so concurrent tabs are distinguishable), the viewport,
//! the mount table, the lifetime counters, and the overlay.
//!
//! **Health is derived from the wiring, not asserted.** A surface is `ok` only
//! when every instance its wiring declares is mounted with all of its bound
//! input ports attached; anything less is `degraded`. Requiring *every* declared
//! instance is what stops an incomplete table from reading healthy.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use brenn_surface_schema::InstanceState;
use brenn_surface_schema::telemetry::{
    GeometryDocument, Health, StatusDocument, TELEMETRY_DOCUMENT_VERSION, TelemetryError,
    validate_viewport,
};
use brenn_surface_schema::telemetry::{InstanceReport, OverlayReport, StatusCounters};

use crate::bindings::AppliedBindings;

/// Every declared instance mapped to the number of input ports it should have an
/// attached pump for.
///
/// Every instance is a key, zero included: health requires each of them to be
/// present in the table and mounted, and an instance with no bound input port
/// still has to have mounted.
pub fn expected_pumps(bindings: &AppliedBindings) -> BTreeMap<String, u32> {
    let mut expected: BTreeMap<String, u32> = bindings
        .components()
        .iter()
        .map(|c| (c.instance.clone(), 0))
        .collect();
    for binding in &bindings.document().subscriptions {
        if let Some(count) = expected.get_mut(&binding.instance) {
            *count += 1;
        }
    }
    expected
}

/// Summarize the surface from its instance table and its expected pumps.
///
/// `Ok` only when every expected instance is present, `Mounted`, and covers its
/// expected pumps; otherwise `Degraded`. Never `Disconnected` — that is the
/// server's stamp, and a live snapshot exists because the page wrote it.
pub fn derive_health(
    instances: &[InstanceReport],
    expected_pumps: &BTreeMap<String, u32>,
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

/// The live facts one status document reports, borrowed for the length of its
/// composition.
///
/// One bundle rather than a positional list: the fields travel together, several
/// share a shape (so a transposition would compile), and the set grows with the
/// document schema.
pub struct StatusReport<'a> {
    /// The kernel's instance table.
    pub instances: &'a [InstanceReport],
    /// Seconds since the page loaded.
    pub uptime_secs: u64,
    /// Lifetime totals, surface-wide and per instance.
    pub counters: &'a StatusCounters,
    /// Telemetry documents the peer refused, from the page's own count. Stated
    /// over whatever [`counters`](Self::counters) carries: only the layer that
    /// settles a telemetry publish's outcome knows this total, so a reporter's
    /// value for it is not a fact about anything.
    pub telemetry_dropped: u64,
    /// The overlay chrome holds, as the plane policy recorded it.
    pub overlay: Option<&'a OverlayReport>,
}

/// Compose the geometry document body for one viewport reading.
///
/// `Err` names the bound the reading violated. Refused rather than published:
/// the document is a fact about a physical display, and a reading outside the
/// plausible range is one the page misread — publishing it would put a number
/// its own schema refuses into the retained window every reader parses.
pub fn geometry_body(
    session: &str,
    width: u32,
    height: u32,
    device_pixel_ratio: f64,
) -> Result<String, TelemetryError> {
    validate_viewport(width, height, device_pixel_ratio)?;
    Ok(GeometryDocument::new(session.to_string(), width, height, device_pixel_ratio).to_body())
}

/// Compose the status document body from the kernel's own report, with the
/// health summary derived from `bindings`.
///
/// `Err` names a report that contradicts the wiring it was assembled from: an
/// instance, a counter key or an overlay holder the surface does not declare, an
/// instance whose kind is not the one configured, or any rule the document
/// schema itself imposes. Every one of them holds by construction — the table,
/// the counters and the wiring all descend from the same document — so a failure
/// is this build disagreeing with itself, which is the caller's fatal rather
/// than something to publish around.
pub fn status_body(
    session: &str,
    bindings: &AppliedBindings,
    report: &StatusReport<'_>,
) -> Result<String, TelemetryError> {
    check_declared(bindings, report)?;
    let doc = StatusDocument {
        v: TELEMETRY_DOCUMENT_VERSION,
        session: session.to_string(),
        health: derive_health(report.instances, &expected_pumps(bindings)),
        uptime_secs: report.uptime_secs,
        instances: report.instances.to_vec(),
        counters: StatusCounters {
            telemetry_dropped: report.telemetry_dropped,
            ..report.counters.clone()
        },
        overlay: report.overlay.cloned(),
    };
    doc.validate()?;
    Ok(doc.to_body())
}

/// Check every principal a status report names against the wiring.
///
/// Three fields name instances — the table, the per-instance counters, and the
/// overlay holder — and all three reach the retained document an operator reads
/// attribution off. The document schema cannot check them: it does not hold the
/// wiring.
fn check_declared(
    bindings: &AppliedBindings,
    report: &StatusReport<'_>,
) -> Result<(), TelemetryError> {
    if let Some(overlay) = report.overlay
        && !bindings.is_declared_instance(&overlay.holder)
    {
        return Err(format!(
            "status overlay names undeclared holder {:?}",
            overlay.holder
        ));
    }
    for instance in report.counters.instances.keys() {
        if !bindings.is_declared_instance(instance) {
            return Err(format!(
                "status counters name undeclared instance {instance:?}"
            ));
        }
    }
    for entry in report.instances {
        match bindings.component(&entry.instance) {
            Some(component) if component.kind == entry.kind => {}
            Some(_) => {
                return Err(format!(
                    "status instance {:?} reports a kind that is not its configured kind",
                    entry.instance
                ));
            }
            None => {
                return Err(format!(
                    "status names undeclared instance {:?}",
                    entry.instance
                ));
            }
        }
    }
    Ok(())
}
