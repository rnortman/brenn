use brenn_lib::obs::alerting::AlertDispatcher;
use tracing::warn;

pub(in crate::active_bridge) fn handle_rate_limit_utilization(
    evt: &brenn_cc::protocol::incoming::RateLimitEventMessage,
    alert_dispatcher: &AlertDispatcher,
) {
    let info = match evt.rate_limit_info.as_ref() {
        Some(i) => i,
        None => return,
    };

    let status = info.get("status").and_then(|s| s.as_str());
    // Observe "status" on every RateLimit event so drift on this gate field
    // is detected (design spec: all four fields, including "status", are
    // observed). quality-2 fix: the three downstream fields are only
    // reachable after this gate, so status must be observed here.
    crate::cc_schema_drift::observe(alert_dispatcher, "rate_limit_info.status", status.is_some());

    if status != Some("allowed_warning") {
        return; // Only log utilization on warning events.
    }

    let util = info.get("utilization").and_then(|v| v.as_f64());
    let rl_type = info.get("rateLimitType").and_then(|v| v.as_str());
    let resets_at = info.get("resetsAt").and_then(|v| v.as_i64());

    let ok_util = crate::cc_schema_drift::observe(
        alert_dispatcher,
        "rate_limit_info.utilization",
        util.is_some(),
    );
    let ok_type = crate::cc_schema_drift::observe(
        alert_dispatcher,
        "rate_limit_info.rateLimitType",
        rl_type.is_some(),
    );
    let ok_resets = crate::cc_schema_drift::observe(
        alert_dispatcher,
        "rate_limit_info.resetsAt",
        resets_at.is_some(),
    );

    if let (true, true, true, Some(util), Some(rl_type), Some(resets_at)) =
        (ok_util, ok_type, ok_resets, util, rl_type, resets_at)
    {
        warn!(
            utilization = util,
            window = rl_type,
            resets_at,
            "rate limit warning (no UI consumer)"
        );
    }
}
