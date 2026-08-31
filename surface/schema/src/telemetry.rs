//! The documents a surface writes about itself: viewport geometry, mount
//! status, the terminal/boot `disconnected` stamp, and error reports.
//!
//! Three of the four ride the surface's two derived telemetry channels
//! (`…surface.<slug>.geometry` and `…surface.<slug>.status`), latest-wins on a
//! retained window; error reports ride the surface's declared error channel,
//! many-writer by design.
//!
//! **Who writes what.** The kernel authors the geometry and status documents —
//! it observes the viewport and owns the mount table, so it is the only party
//! that knows them. The `disconnected` stamp is the one document the kernel
//! cannot write (a page that is gone reports nothing), so the server authors it
//! at boot and at the last session's teardown.
//!
//! **Provenance is the envelope's, not the body's.** *Who* published and *when*
//! are `MessageEnvelope::sender` and `publish_ts`, server-stamped on every
//! publish and unforgeable by a client — so no document restates the surface
//! slug or a body-authored timestamp. The `disconnected` stamp is the exception
//! that proves it: it carries a server clock read and the bus epoch because the
//! server writes it, and because "when did the surface go down" is a fact about
//! a session that no longer exists.
//!
//! **Discriminating the status channel.** Both status-channel documents carry
//! `v` and `health`; a reader tells them apart by `health` — `disconnected` is
//! the stamp, anything else is a live snapshot. The two shapes are otherwise
//! disjoint and each refuses the other's fields on parse.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use std::collections::BTreeMap;

use crate::{InstanceState, LogLevel};

/// The body-schema version stamped on every telemetry document. Bumped whenever
/// a document's shape changes; a reader that does not recognize the value
/// refuses the document rather than guessing at its fields.
pub const TELEMETRY_DOCUMENT_VERSION: u32 = 1;

/// Physically-plausible viewport dimension bounds (CSS pixels), not UX policy: a
/// generous window a real display could present.
pub const VIEWPORT_DIMENSION_MIN: u32 = 1;
/// Upper viewport dimension bound; see [`VIEWPORT_DIMENSION_MIN`].
pub const VIEWPORT_DIMENSION_MAX: u32 = 32_768;

/// Device-pixel-ratio bounds: generous physical plausibility, finite required.
pub const DEVICE_PIXEL_RATIO_MIN: f64 = 0.1;
/// Upper device-pixel-ratio bound; see [`DEVICE_PIXEL_RATIO_MIN`].
pub const DEVICE_PIXEL_RATIO_MAX: f64 = 16.0;

/// Per-instance `reason` cap (bytes) in a status document — bounds the body so
/// `BodyTooLarge` stays structurally unreachable for a conforming writer.
pub const MAX_INSTANCE_REASON_BYTES: usize = 256;

/// Why a telemetry document was refused. Carries a rendered message rather than
/// a code: every one of these is fatal at the reader, and the message is what
/// reaches the log a human reads.
pub type TelemetryError = String;

/// Surface health, as summarized in a status document. Serialized lowercase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Health {
    /// Every instance mounted and every bound subscription has an attached pump.
    Ok,
    /// At least one instance failed or one binding is pumpless, while the
    /// session is live.
    Degraded,
    /// No session attached. Never a live snapshot's summary: this is the value
    /// the server-authored [`DisconnectedStamp`] carries.
    Disconnected,
}

/// The viewport a [`GeometryDocument`] reports, in CSS pixels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Viewport {
    pub width: u32,
    pub height: u32,
}

/// The surface's viewport, published once after connect and on debounced
/// resize.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeometryDocument {
    /// Body-schema version; [`TELEMETRY_DOCUMENT_VERSION`] for a document this
    /// build can read.
    pub v: u32,
    /// The reporting attachment's server-minted session id, self-reported from
    /// the handshake. Distinguishes concurrent tabs of one surface, which share
    /// an envelope sender.
    pub session: String,
    pub viewport: Viewport,
    /// Display density.
    pub device_pixel_ratio: f64,
}

impl GeometryDocument {
    /// Build a document at this build's schema version.
    pub fn new(session: String, width: u32, height: u32, device_pixel_ratio: f64) -> Self {
        Self {
            v: TELEMETRY_DOCUMENT_VERSION,
            session,
            viewport: Viewport { width, height },
            device_pixel_ratio,
        }
    }

    /// Serialize to the published body.
    ///
    /// # Panics
    ///
    /// If the document does not serialize. The only reachable cause is a
    /// non-finite `device_pixel_ratio`, which [`validate`](Self::validate)
    /// refuses — so a panic here names a writer that skipped validation.
    pub fn to_body(&self) -> String {
        serde_json::to_string(self).expect("geometry document serializes to JSON")
    }

    /// Parse and validate a published body. Version, shape, and bounds in one
    /// call: a reader has the same answer to all three.
    pub fn parse(body: &str) -> Result<Self, TelemetryError> {
        check_body_version(body, "geometry")?;
        let doc: Self = serde_json::from_str(body)
            .map_err(|e| format!("geometry document does not parse: {e}"))?;
        doc.validate()?;
        Ok(doc)
    }

    /// Check the document's bounds. Every rule holds by construction on the
    /// writing side, so a failure names a broken writer.
    pub fn validate(&self) -> Result<(), TelemetryError> {
        check_session(&self.session, "geometry")?;
        validate_viewport(
            self.viewport.width,
            self.viewport.height,
            self.device_pixel_ratio,
        )
    }
}

/// Check a viewport against the physical-plausibility bounds. Separate from
/// [`GeometryDocument::validate`] so a writer can refuse the raw observation
/// before it composes a document out of it.
pub fn validate_viewport(
    width: u32,
    height: u32,
    device_pixel_ratio: f64,
) -> Result<(), TelemetryError> {
    if !(VIEWPORT_DIMENSION_MIN..=VIEWPORT_DIMENSION_MAX).contains(&width) {
        return Err(format!(
            "viewport width out of bounds {VIEWPORT_DIMENSION_MIN}..={VIEWPORT_DIMENSION_MAX}"
        ));
    }
    if !(VIEWPORT_DIMENSION_MIN..=VIEWPORT_DIMENSION_MAX).contains(&height) {
        return Err(format!(
            "viewport height out of bounds {VIEWPORT_DIMENSION_MIN}..={VIEWPORT_DIMENSION_MAX}"
        ));
    }
    if !device_pixel_ratio.is_finite()
        || !(DEVICE_PIXEL_RATIO_MIN..=DEVICE_PIXEL_RATIO_MAX).contains(&device_pixel_ratio)
    {
        return Err(format!(
            "device_pixel_ratio not finite in {DEVICE_PIXEL_RATIO_MIN}..={DEVICE_PIXEL_RATIO_MAX}"
        ));
    }
    Ok(())
}

/// One instance's mount status inside a [`StatusDocument`]. The kernel writes
/// the raw facts it already tracks at its mount/attach/panic decision points,
/// and derives [`Health`] over the set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceReport {
    /// The instance id (routing/mount key), one of the surface's configured
    /// instances.
    pub instance: String,
    /// The component kind backing the instance.
    pub kind: String,
    pub state: InstanceState,
    /// Short failure reason when `state` is `Failed` (module missing, element
    /// undefined, component panic, terminal port event); `None` otherwise.
    pub reason: Option<String>,
    /// Count of delivery pumps attached to this instance's ports.
    pub ports_attached: u32,
}

/// Kernel-side lifetime totals carried in a [`StatusDocument`]. The extensible
/// counters object; v1 ships the kernel's own totals. Server-side drop counters
/// are a future additive export.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusCounters {
    /// Deliveries received by the kernel over the connection's lifetime.
    pub deliveries: u64,
    /// Publishes the kernel has sent.
    pub publishes: u64,
    /// Error-level reports emitted. Each count is one console line an operator
    /// can read; not a count of deaths (one death emits a varying number).
    pub errors: u64,
    /// Telemetry documents the peer refused — rate-limited or over the body cap.
    ///
    /// Page-derived, unlike the three totals above: only the layer that settles a
    /// telemetry publish's outcome knows it, so the page states this field from
    /// its own count and whatever a reporter put here is discarded. A dropped
    /// latest-wins document costs staleness only, so this is the sole account of
    /// how stale the plane has been.
    pub telemetry_dropped: u64,
    /// Per-instance breakdown, keyed by instance id. The surface's totals above
    /// answer "is the wall working?"; this answers "which component is doing
    /// it?" — the same principal grain the bus meters and attributes publishes
    /// at, carried onto the plane an operator reads.
    ///
    /// An instance that has neither published nor dropped may be absent — the map
    /// reports what happened, so an absent key reads as zero.
    pub instances: BTreeMap<String, InstanceCounters>,
}

/// One instance's lifetime totals within [`StatusCounters`].
///
/// Deliberately not a copy of the surface-wide triple. A column earns its place
/// here by varying per instance without bound *and* answering a question the
/// surface totals cannot. `deliveries` would duplicate what
/// [`InstanceReport::ports_attached`] already tells an operator about a live
/// instance, and `errors` is the surface-wide report-count invariant's number —
/// every count there is one Error-level line an operator can read — which is a
/// property of the reporting path, not a per-instance fact. What each instance
/// *sent*, what it *lost*, and how often its activations *failed* are the
/// per-instance facts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceCounters {
    /// Publishes the kernel queued on this instance's behalf. Counted at the seam
    /// — a publish this instance asked for, whether or not the bus later
    /// accepted it — so it is the instance's *attempt* rate, which is what
    /// reads against its send budget.
    pub publishes: u64,
    /// Messages dropped from this instance's port queues by push overflow
    /// (drop-oldest, counted). Sustained non-zero drops mean the component is
    /// not keeping up with its bindings' `push_depth`.
    pub drops: u64,
    /// Activation failures reported for this instance, one per occurrence: both
    /// `Err` outcomes and traps, whatever level the failure was reported at.
    ///
    /// Traps are included deliberately — the killing trap is a correct part of
    /// the instance's history, and the counting site cannot distinguish the two
    /// cases anyway. A climbing count on a `mounted` instance is a live,
    /// continuing failure the surface's own `health` cannot express; a frozen
    /// one means the failures stopped.
    ///
    /// The count is here; the failure *text* is not, and is not meant to be.
    /// This field carries numbers a reader compares across ticks. The text
    /// travels a separate path with its own dedup and retention, so the count
    /// may climb faster than reason lines appear; a reader cannot assume 1:1
    /// correspondence between this counter and the error reports.
    pub activation_failures: u64,
}

/// The held-overlay fact a [`StatusDocument`] carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OverlayReport {
    /// The instance holding the overlay — one of the surface's configured
    /// instances.
    pub holder: String,
    /// When the hold began: the publish time of the chrome transition the kernel
    /// recorded it from.
    pub since: DateTime<Utc>,
}

/// A live surface's mount snapshot, published on the status interval and
/// immediately on any transition into `failed`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatusDocument {
    /// Body-schema version; [`TELEMETRY_DOCUMENT_VERSION`] for a document this
    /// build can read.
    pub v: u32,
    /// The reporting attachment's server-minted session id.
    pub session: String,
    /// The summary over [`instances`](Self::instances) and their pump
    /// attachment. Never `disconnected` — a document exists because a session
    /// wrote it.
    pub health: Health,
    /// Seconds since the page loaded.
    pub uptime_secs: u64,
    /// Per-instance mount state, at most one entry per instance.
    pub instances: Vec<InstanceReport>,
    /// Lifetime totals, surface-wide and per instance.
    pub counters: StatusCounters,
    /// The overlay chrome holds, or `None` when it holds none.
    ///
    /// Reported, not judged: a held overlay is a takeover doing its job as often
    /// as it is a wedge, so `health` does not read it. The field is what makes
    /// the two distinguishable to whoever does.
    pub overlay: Option<OverlayReport>,
}

impl StatusDocument {
    /// Serialize to the published body.
    ///
    /// # Panics
    ///
    /// If the document does not serialize — impossible for these types (no
    /// non-string map keys, no floats), so a failure is a broken invariant
    /// rather than a condition to handle.
    pub fn to_body(&self) -> String {
        serde_json::to_string(self).expect("status document serializes to JSON")
    }

    /// Parse and validate a published body.
    pub fn parse(body: &str) -> Result<Self, TelemetryError> {
        check_body_version(body, "status")?;
        let doc: Self = serde_json::from_str(body)
            .map_err(|e| format!("status document does not parse: {e}"))?;
        doc.validate()?;
        Ok(doc)
    }

    /// Check the document's internal consistency.
    ///
    /// Not checked here: that each instance is one the surface configures, and
    /// that its `kind` matches. That answer lives in the boot-resolved bindings,
    /// which this crate's schema layer does not hold; the writer resolves it
    /// from the bindings document it is running on.
    pub fn validate(&self) -> Result<(), TelemetryError> {
        check_session(&self.session, "status")?;
        if self.health == Health::Disconnected {
            return Err(
                "status document summarizes health as disconnected, which only a server-written \
                 stamp carries"
                    .to_string(),
            );
        }
        let mut seen: Vec<&str> = Vec::with_capacity(self.instances.len());
        for report in &self.instances {
            if report.instance.is_empty() {
                return Err("status document reports an instance with an empty id".to_string());
            }
            // A multiset with repeats is not the subset of the configured
            // instances the document promises to be, and a contradictory pair
            // (`mounted` + `failed` for one instance) would otherwise both land
            // in the retained snapshot an operator reads.
            if seen.contains(&report.instance.as_str()) {
                return Err(format!(
                    "status document reports instance {:?} more than once",
                    report.instance
                ));
            }
            seen.push(report.instance.as_str());
            if let Some(reason) = &report.reason
                && reason.len() > MAX_INSTANCE_REASON_BYTES
            {
                return Err(format!(
                    "status document reason for instance {:?} exceeds \
                     {MAX_INSTANCE_REASON_BYTES} bytes",
                    report.instance
                ));
            }
        }
        Ok(())
    }
}

/// The server-written `disconnected` stamp: the boot stamp for every configured
/// surface, and the terminal snapshot when a slug's last session closes.
///
/// Deliberately not a status document with null fields. The server does not
/// author a surface's mount state — the page does — so a stamp reports only
/// what the server itself knows: which session ended, when, under which bus
/// epoch, and why. A durable status channel's retained row survives a restart,
/// so without this stamp a dead or not-yet-connected wall would read "healthy as
/// of before the restart" until a reader did timestamp math.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DisconnectedStamp {
    /// Body-schema version; [`TELEMETRY_DOCUMENT_VERSION`] for a document this
    /// build can read.
    pub v: u32,
    /// The session that closed, or `None` for a boot stamp (which no session
    /// precedes within this process).
    pub session: Option<String>,
    /// Server clock read at the stamp. Present where the live documents have no
    /// timestamp: the envelope's `publish_ts` says when the *stamp* was
    /// published, which for the boot stamp is the only time there is.
    pub ts: DateTime<Utc>,
    /// The bus epoch in force when the stamp was written. A reader comparing it
    /// against a live document's epoch tells "the surface is down" from "the
    /// server restarted under it".
    pub epoch: Uuid,
    /// Always [`Health::Disconnected`]; carried so one field discriminates every
    /// document on the status channel.
    pub health: Health,
    /// Why: `"server restart"` for a boot stamp, the close cause for a terminal
    /// snapshot.
    pub reason: String,
}

impl DisconnectedStamp {
    /// Build a stamp at this build's schema version, with the fixed
    /// `disconnected` health.
    pub fn new(session: Option<String>, ts: DateTime<Utc>, epoch: Uuid, reason: String) -> Self {
        Self {
            v: TELEMETRY_DOCUMENT_VERSION,
            session,
            ts,
            epoch,
            health: Health::Disconnected,
            reason,
        }
    }

    /// Serialize to the published body.
    ///
    /// # Panics
    ///
    /// If the stamp does not serialize — impossible for these types, so a
    /// failure is a broken invariant rather than a condition to handle.
    pub fn to_body(&self) -> String {
        serde_json::to_string(self).expect("disconnected stamp serializes to JSON")
    }

    /// Parse and validate a published body.
    pub fn parse(body: &str) -> Result<Self, TelemetryError> {
        check_body_version(body, "disconnected stamp")?;
        let doc: Self = serde_json::from_str(body)
            .map_err(|e| format!("disconnected stamp does not parse: {e}"))?;
        doc.validate()?;
        Ok(doc)
    }

    /// Check the stamp's internal consistency.
    pub fn validate(&self) -> Result<(), TelemetryError> {
        if self.health != Health::Disconnected {
            return Err(format!(
                "disconnected stamp carries health {:?}, which no stamp writes",
                self.health
            ));
        }
        if let Some(session) = &self.session {
            check_session(session, "disconnected stamp")?;
        }
        if self.reason.is_empty() {
            return Err("disconnected stamp carries an empty reason".to_string());
        }
        Ok(())
    }
}

/// The flat error-report body a surface publishes to its declared error
/// channel: the surface's own claims, honestly attributed by the envelope
/// sender the server binds. Opaque to the server, which applies only the
/// ordinary body cap.
///
/// Unversioned, unlike the telemetry documents: the report is three flat fields
/// with no shape to negotiate, and a reader that finds a field it does not know
/// has still read the level and the message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorReportDocument {
    /// What produced the report, e.g. `"component:<kind>"`. Truncated by the
    /// writer to `MAX_LOG_SOURCE_BYTES`.
    pub source: String,
    /// The report text, truncated by the writer to `MAX_LOG_MESSAGE_BYTES`.
    pub message: String,
    pub level: LogLevel,
}

impl ErrorReportDocument {
    /// Serialize to the published body.
    ///
    /// # Panics
    ///
    /// If the report does not serialize — impossible for three owned strings and
    /// a unit enum, so a failure is a broken invariant.
    pub fn to_body(&self) -> String {
        serde_json::to_string(self).expect("error report body serializes to JSON")
    }
}

/// Refuse a body whose schema version this build does not read, ahead of the
/// typed parse.
///
/// Read `v` on its own, ignoring every other field, because the document types
/// deny unknown fields: a future version that adds one would otherwise fail the
/// typed parse first and be reported as a field this build does not know, which
/// is the misdiagnosis carrying `v` exists to prevent.
///
/// A body with no readable `v` is not judged here — the typed parse below names
/// the real problem, whether that is malformed JSON or a missing field.
fn check_body_version(body: &str, document: &str) -> Result<(), TelemetryError> {
    #[derive(Deserialize)]
    struct VersionPeek {
        v: u32,
    }
    match serde_json::from_str::<VersionPeek>(body) {
        Ok(peek) => check_version(peek.v, document),
        Err(_) => Ok(()),
    }
}

/// Refuse a schema version this build does not read.
fn check_version(v: u32, document: &str) -> Result<(), TelemetryError> {
    if v != TELEMETRY_DOCUMENT_VERSION {
        return Err(format!(
            "{document} document declares schema version {v} — this build reads \
             {TELEMETRY_DOCUMENT_VERSION}"
        ));
    }
    Ok(())
}

/// Refuse an empty session id. A document that names no session cannot be
/// attributed to one of a surface's concurrent attachments, which is the field's
/// only job.
fn check_session(session: &str, document: &str) -> Result<(), TelemetryError> {
    if session.is_empty() {
        return Err(format!("{document} document carries an empty session id"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::{Value, json};

    use super::*;

    fn report(instance: &str, state: InstanceState) -> InstanceReport {
        InstanceReport {
            instance: instance.to_string(),
            kind: "protobar".to_string(),
            state,
            reason: None,
            ports_attached: 1,
        }
    }

    fn status() -> StatusDocument {
        StatusDocument {
            v: TELEMETRY_DOCUMENT_VERSION,
            session: "sess".to_string(),
            health: Health::Ok,
            uptime_secs: 86_400,
            instances: vec![report("p1", InstanceState::Mounted)],
            counters: StatusCounters {
                deliveries: 10,
                publishes: 2,
                errors: 1,
                telemetry_dropped: 3,
                instances: BTreeMap::from([(
                    "p1".to_string(),
                    InstanceCounters {
                        publishes: 2,
                        drops: 5,
                        activation_failures: 4,
                    },
                )]),
            },
            overlay: Some(OverlayReport {
                holder: "p1".to_string(),
                since: DateTime::UNIX_EPOCH,
            }),
        }
    }

    fn stamp() -> DisconnectedStamp {
        DisconnectedStamp::new(
            Some("sess".to_string()),
            DateTime::UNIX_EPOCH,
            Uuid::nil(),
            "session closed".to_string(),
        )
    }

    #[test]
    fn geometry_round_trips_through_its_body() {
        let doc = GeometryDocument::new("sess".to_string(), 1920, 515, 2.0);
        assert_eq!(GeometryDocument::parse(&doc.to_body()).unwrap(), doc);
        let v: Value = serde_json::from_str(&doc.to_body()).unwrap();
        assert_eq!(v["v"], json!(1));
        assert_eq!(v["session"], json!("sess"));
        assert_eq!(v["viewport"], json!({ "width": 1920, "height": 515 }));
        assert_eq!(v["device_pixel_ratio"], json!(2.0));
    }

    /// Who and when are the envelope's, so the body restates neither. A body
    /// field for either would be a client-authored duplicate of a server-stamped
    /// fact.
    #[test]
    fn live_documents_carry_no_surface_or_timestamp() {
        for body in [
            GeometryDocument::new("sess".to_string(), 800, 600, 1.0).to_body(),
            status().to_body(),
        ] {
            let v: Value = serde_json::from_str(&body).unwrap();
            assert_eq!(
                v["surface"],
                json!(null),
                "unexpected surface field: {body}"
            );
            assert_eq!(v["ts"], json!(null), "unexpected ts field: {body}");
        }
    }

    #[test]
    fn geometry_validation_bounds() {
        let ok = |w, h, dpr| GeometryDocument::new("sess".to_string(), w, h, dpr).validate();
        assert!(ok(1920, 1080, 2.0).is_ok());
        assert!(ok(0, 1080, 1.0).is_err());
        assert!(ok(1920, 40_000, 1.0).is_err());
        assert!(ok(1920, 1080, 0.0).is_err());
        assert!(ok(1920, 1080, 100.0).is_err());
        assert!(ok(1920, 1080, f64::NAN).is_err());
        assert!(ok(1920, 1080, f64::INFINITY).is_err());
        let mut anonymous = GeometryDocument::new(String::new(), 1920, 1080, 1.0);
        assert!(anonymous.validate().is_err());
        anonymous.session = "sess".to_string();
        assert!(anonymous.validate().is_ok());
    }

    #[test]
    fn geometry_refuses_a_foreign_version_and_unknown_fields() {
        let mut doc = GeometryDocument::new("sess".to_string(), 800, 600, 1.0);
        doc.v = TELEMETRY_DOCUMENT_VERSION + 1;
        let err =
            GeometryDocument::parse(&doc.to_body()).expect_err("a foreign version is refused");
        assert!(err.contains("schema version"), "unexpected: {err}");
        let junk = r#"{"v":1,"session":"s","viewport":{"width":1,"height":1},
                       "device_pixel_ratio":1.0,"tilt":3}"#;
        assert!(GeometryDocument::parse(junk).is_err());
    }

    /// A future version that *adds* a field is named by its version, not by the
    /// field this build does not know — which is the diagnostic the `v` field
    /// exists to produce, and which the strict shape would otherwise shadow.
    #[test]
    fn a_future_version_is_named_by_its_version_not_its_new_field() {
        for (document, body) in [
            (
                "geometry",
                json!({
                    "v": TELEMETRY_DOCUMENT_VERSION + 1,
                    "session": "sess",
                    "viewport": { "width": 800, "height": 600 },
                    "device_pixel_ratio": 1.0,
                    "tilt": 3,
                }),
            ),
            ("status", {
                let mut v: Value = serde_json::from_str(&status().to_body()).unwrap();
                v["v"] = json!(TELEMETRY_DOCUMENT_VERSION + 1);
                v["mood"] = json!("fine");
                v
            }),
            ("disconnected stamp", {
                let mut v: Value = serde_json::from_str(&stamp().to_body()).unwrap();
                v["v"] = json!(TELEMETRY_DOCUMENT_VERSION + 1);
                v["mood"] = json!("fine");
                v
            }),
        ] {
            let body = body.to_string();
            let err = match document {
                "geometry" => GeometryDocument::parse(&body).unwrap_err(),
                "status" => StatusDocument::parse(&body).unwrap_err(),
                _ => DisconnectedStamp::parse(&body).unwrap_err(),
            };
            assert!(
                err.contains("schema version") && !err.contains("unknown field"),
                "{document}: {err}"
            );
        }
    }

    #[test]
    fn status_round_trips_through_its_body() {
        let doc = status();
        assert_eq!(StatusDocument::parse(&doc.to_body()).unwrap(), doc);
        let v: Value = serde_json::from_str(&doc.to_body()).unwrap();
        assert_eq!(v["v"], json!(1));
        assert_eq!(v["health"], json!("ok"));
        assert_eq!(v["uptime_secs"], json!(86_400));
        assert_eq!(v["instances"][0]["instance"], json!("p1"));
        assert_eq!(v["instances"][0]["state"], json!("mounted"));
        // The per-instance breakdown reaches the retained document — the plane an
        // operator actually reads. Counting it page-side and dropping it here
        // would be counting for nobody.
        assert_eq!(
            v["counters"]["instances"],
            json!({ "p1": { "publishes": 2, "drops": 5, "activation_failures": 4 } })
        );
        assert_eq!(v["overlay"]["holder"], json!("p1"));
        assert_eq!(v["overlay"]["since"], json!("1970-01-01T00:00:00Z"));
    }

    /// The absent overlay is `null` rather than a missing key: a reader asking
    /// "what holds the overlay?" gets an answer from every live document, and
    /// "nothing" is an answer.
    #[test]
    fn status_reports_no_overlay_as_null() {
        let doc = StatusDocument {
            overlay: None,
            ..status()
        };
        let v: Value = serde_json::from_str(&doc.to_body()).unwrap();
        assert_eq!(v["overlay"], json!(null));
    }

    #[test]
    fn status_refuses_a_repeated_instance() {
        let doc = StatusDocument {
            instances: vec![
                report("p1", InstanceState::Mounted),
                report("p1", InstanceState::Failed),
            ],
            ..status()
        };
        let err = doc.validate().expect_err("a repeated instance is refused");
        assert!(err.contains("more than once"), "unexpected: {err}");
    }

    #[test]
    fn status_refuses_an_over_long_reason_and_an_empty_instance_id() {
        let mut failed = report("p1", InstanceState::Failed);
        failed.reason = Some("x".repeat(MAX_INSTANCE_REASON_BYTES + 1));
        let doc = StatusDocument {
            instances: vec![failed.clone()],
            ..status()
        };
        assert!(doc.validate().is_err());
        failed.reason = Some("x".repeat(MAX_INSTANCE_REASON_BYTES));
        let doc = StatusDocument {
            instances: vec![failed],
            ..status()
        };
        assert!(doc.validate().is_ok());
        let doc = StatusDocument {
            instances: vec![report("", InstanceState::Mounted)],
            ..status()
        };
        assert!(doc.validate().is_err());
    }

    /// The one health value a live snapshot cannot carry: a document exists
    /// because a session wrote it, and the server's stamp is the only writer of
    /// `disconnected`.
    #[test]
    fn status_refuses_disconnected_health() {
        let doc = StatusDocument {
            health: Health::Disconnected,
            ..status()
        };
        let err = doc
            .validate()
            .expect_err("a live disconnected doc is refused");
        assert!(err.contains("disconnected"), "unexpected: {err}");
    }

    #[test]
    fn status_refuses_a_foreign_version_and_unknown_fields() {
        let doc = StatusDocument {
            v: TELEMETRY_DOCUMENT_VERSION + 1,
            ..status()
        };
        assert!(StatusDocument::parse(&doc.to_body()).is_err());
        let mut v: Value = serde_json::from_str(&status().to_body()).unwrap();
        v["mood"] = json!("fine");
        assert!(StatusDocument::parse(&v.to_string()).is_err());
    }

    #[test]
    fn disconnected_stamp_round_trips_in_both_flavours() {
        let terminal = stamp();
        assert_eq!(
            DisconnectedStamp::parse(&terminal.to_body()).unwrap(),
            terminal
        );
        let v: Value = serde_json::from_str(&terminal.to_body()).unwrap();
        assert_eq!(v["v"], json!(1));
        assert_eq!(v["session"], json!("sess"));
        assert_eq!(v["health"], json!("disconnected"));
        assert_eq!(v["reason"], json!("session closed"));
        assert_eq!(v["ts"], json!("1970-01-01T00:00:00Z"));
        assert_eq!(v["epoch"], json!("00000000-0000-0000-0000-000000000000"));

        let boot = DisconnectedStamp::new(
            None,
            DateTime::UNIX_EPOCH,
            Uuid::nil(),
            "server restart".to_string(),
        );
        assert_eq!(DisconnectedStamp::parse(&boot.to_body()).unwrap(), boot);
        let v: Value = serde_json::from_str(&boot.to_body()).unwrap();
        assert_eq!(v["session"], json!(null));
    }

    #[test]
    fn disconnected_stamp_refuses_a_live_health_and_an_empty_reason() {
        let doc = DisconnectedStamp {
            health: Health::Ok,
            ..stamp()
        };
        assert!(doc.validate().is_err());
        let doc = DisconnectedStamp {
            reason: String::new(),
            ..stamp()
        };
        assert!(doc.validate().is_err());
        let doc = DisconnectedStamp {
            session: Some(String::new()),
            ..stamp()
        };
        assert!(doc.validate().is_err());
    }

    /// One channel, two shapes, one discriminating field — and neither parses as
    /// the other, so a reader that ignores `health` still cannot confuse them.
    #[test]
    fn the_two_status_channel_documents_are_not_confusable() {
        assert!(StatusDocument::parse(&stamp().to_body()).is_err());
        assert!(DisconnectedStamp::parse(&status().to_body()).is_err());
        for (body, health) in [
            (status().to_body(), json!("ok")),
            (stamp().to_body(), json!("disconnected")),
        ] {
            let v: Value = serde_json::from_str(&body).unwrap();
            assert_eq!(v["health"], health);
        }
    }

    #[test]
    fn error_report_body_is_three_flat_fields() {
        let doc = ErrorReportDocument {
            source: "component:protobar".to_string(),
            message: "boom".to_string(),
            level: LogLevel::Error,
        };
        let v: Value = serde_json::from_str(&doc.to_body()).unwrap();
        assert_eq!(
            v,
            json!({ "source": "component:protobar", "message": "boom", "level": "error" })
        );
    }
}
