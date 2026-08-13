//! Typed payload for automation fire-error events.
//!
//! Shared across the fire error-report, unsatisfiable-cron, and startup
//! catchup emission sites.

use serde::Serialize;

/// Payload for `automation:error` events.
///
/// Fields are alphabetical so `serde_json` (which sorts keys via BTreeMap)
/// produces deterministic output.
#[derive(Serialize)]
pub struct AutomationErrorPayload<'a> {
    pub detail: &'a str,
    pub error_class: &'a str,
    pub fire_time: &'a str,
    pub job_id: i64,
    pub name: &'a str,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Wire-shape regression guard: serialized output must be byte-identical to
    /// the reference `json!` literal from each emission site.
    #[test]
    fn automation_error_payload_matches_reference() {
        let payload = AutomationErrorPayload {
            job_id: 42,
            name: "daily-check",
            fire_time: "2026-05-18T00:00:00+00:00",
            error_class: "action_error",
            detail: "something went wrong",
        };
        let produced = serde_json::to_string(&payload)
            .expect("AutomationErrorPayload serialization is infallible");
        let reference = serde_json::json!({
            "job_id": 42_i64,
            "name": "daily-check",
            "fire_time": "2026-05-18T00:00:00+00:00",
            "error_class": "action_error",
            "detail": "something went wrong",
        })
        .to_string();
        assert_eq!(produced, reference);

        let payload2 = AutomationErrorPayload {
            job_id: 7,
            name: "weekly",
            fire_time: "2026-05-18T09:00:00+00:00",
            error_class: "unsatisfiable_cron",
            detail: "compute_next returned None",
        };
        let produced2 = serde_json::to_string(&payload2)
            .expect("AutomationErrorPayload serialization is infallible");
        let reference2 = serde_json::json!({
            "job_id": 7_i64,
            "name": "weekly",
            "fire_time": "2026-05-18T09:00:00+00:00",
            "error_class": "unsatisfiable_cron",
            "detail": "compute_next returned None",
        })
        .to_string();
        assert_eq!(produced2, reference2);

        let payload3 = AutomationErrorPayload {
            job_id: 1,
            name: "startup-job",
            fire_time: "2026-05-18T00:00:00+00:00",
            error_class: "unsatisfiable_cron",
            detail: "cron expression no longer has future occurrences (detected at startup)",
        };
        let produced3 = serde_json::to_string(&payload3)
            .expect("AutomationErrorPayload serialization is infallible");
        let reference3 = serde_json::json!({
            "job_id": 1_i64,
            "name": "startup-job",
            "fire_time": "2026-05-18T00:00:00+00:00",
            "error_class": "unsatisfiable_cron",
            "detail": "cron expression no longer has future occurrences (detected at startup)",
        })
        .to_string();
        assert_eq!(produced3, reference3);
    }
}
