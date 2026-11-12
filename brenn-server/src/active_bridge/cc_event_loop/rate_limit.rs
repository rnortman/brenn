use brenn_obs::alerting::{AlertDispatcher, AlertSeverity};
use brenn_ws_types::WsServerMessage;
use tracing::{info, warn};

use super::super::ActiveBridge;
use super::super::limit::{RateLimitInfo, RateLimitStatus, bucket_refusal};

/// Act on a `rate_limit_event`'s status: a rejection is told to the user and,
/// under a subscription profile, flags the refused bucket; a status this build
/// does not know is surfaced as drift and otherwise ignored.
pub(in crate::active_bridge) fn handle_rate_limit_status(
    bridge: &ActiveBridge,
    info: Option<&RateLimitInfo<'_>>,
    alert_dispatcher: &AlertDispatcher,
) {
    let Some(info) = info else {
        return;
    };
    match &info.status {
        RateLimitStatus::Allowed => info!("CC rate limit status: allowed"),
        // Near a limit, not at one; `handle_rate_limit_utilization` logs it.
        RateLimitStatus::AllowedWarning => {}
        // `handle_rate_limit_utilization` observes the field's absence.
        RateLimitStatus::Missing => {}
        RateLimitStatus::Unrecognized(status) => {
            warn!(
                conversation_id = bridge.conversation_id,
                status, "CC rate_limit_info.status has a value this build does not know"
            );
            alert_dispatcher.alert_once_per_process(
                AlertSeverity::Warning,
                "CC schema drift: rate_limit_info.status".to_string(),
                status,
                format!(
                    "Claude Code sent rate_limit_info.status {status:?}, which Brenn does not \
                     recognise. It is neither shown to the user nor treated as a refusal."
                ),
            );
        }
        RateLimitStatus::Rejected => {
            warn!(conversation_id = bridge.conversation_id, "CC rate limited");
            bridge.broadcast(WsServerMessage::Error {
                message: "Rate limited by Claude API".to_string(),
            });
            let Some(profile) = bridge.cc_profile_name() else {
                return;
            };
            if bridge.agent_status.profile(&profile).kind
                != brenn_envelope::agent::AccountKind::Subscription
            {
                return;
            }
            let model = bridge
                .active_model_slug
                .lock()
                .expect("active_model_slug lock")
                .clone();
            let refusal = bucket_refusal(
                info,
                model.as_deref(),
                &bridge.conversation_id.to_string(),
                crate::agent_status::now_ms(),
            );
            crate::cc_schema_drift::observe(
                alert_dispatcher,
                "rate_limit_info.resetsAt",
                info.resets_at_s.is_some(),
            );
            bridge
                .agent_status
                .bucket_refused(profile, refusal.bucket, refusal.flag);
        }
    }
}

pub(in crate::active_bridge) fn handle_rate_limit_utilization(
    info: Option<&RateLimitInfo<'_>>,
    alert_dispatcher: &AlertDispatcher,
) {
    let Some(info) = info else {
        return;
    };

    // Observe "status" on every RateLimit event so drift on this gate field
    // is detected: the three downstream fields are only reachable after this
    // gate, so status must be observed here.
    crate::cc_schema_drift::observe(
        alert_dispatcher,
        "rate_limit_info.status",
        info.status != RateLimitStatus::Missing,
    );

    if info.status != RateLimitStatus::AllowedWarning {
        return; // Only log utilization on warning events.
    }

    let ok_util = crate::cc_schema_drift::observe(
        alert_dispatcher,
        "rate_limit_info.utilization",
        info.utilization.is_some(),
    );
    let ok_type = crate::cc_schema_drift::observe(
        alert_dispatcher,
        "rate_limit_info.rateLimitType",
        info.rate_limit_type.is_some(),
    );
    let ok_resets = crate::cc_schema_drift::observe(
        alert_dispatcher,
        "rate_limit_info.resetsAt",
        info.resets_at_s.is_some(),
    );

    if let (true, true, true, Some(util), Some(rl_type), Some(resets_at)) = (
        ok_util,
        ok_type,
        ok_resets,
        info.utilization,
        info.rate_limit_type,
        info.resets_at_s,
    ) {
        warn!(
            utilization = util,
            window = rl_type,
            resets_at,
            "rate limit warning (no UI consumer)"
        );
    }
}
