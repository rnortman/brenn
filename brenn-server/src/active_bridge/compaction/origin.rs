//! Brenn-side classification of CC result-frame origins for the compaction
//! state machine. This is Brenn policy (how the state machine should react to a
//! turn), not protocol mechanism — the wire type lives in `brenn-cc`.

use brenn_cc::protocol::incoming::ResultMessage;
use brenn_common::sanitize_untrusted_str;
use brenn_lib::obs::alerting::{AlertDispatcher, AlertSeverity};
use tracing::warn;

/// Byte cap applied to the CC-controlled `origin.kind` before it reaches any log
/// line, pager alert body, or the process-lifetime alert dedup set. Bounds both
/// injection surface (newlines/control bytes are stripped by the sanitizer) and
/// the per-entry memory of the dedup set.
pub(super) const MAX_ORIGIN_KIND_BYTES: usize = 128;

/// How a completed turn should be treated by the compaction state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TurnOrigin {
    /// A turn Brenn initiated (user message, system message, persist prompt).
    /// The mid-compaction arms act on it. Unknown origin kinds fail safe here.
    Foreground,
    /// A CC-autonomous turn (e.g. `task-notification` after a background
    /// subagent completes). The mid-compaction arms ignore it — CC is still
    /// working on the turn they are waiting for.
    Background,
}

/// The wire kind CC stamps on autonomous task-notification result frames.
/// Note the hyphen — distinct from the `task_notification` (underscore)
/// system-message subtype.
const TASK_NOTIFICATION_KIND: &str = "task-notification";

/// Classify a completed turn from its `origin` stamp.
///
/// Fail-safe direction: an unknown kind is treated as `Foreground` (worst case
/// an early `/compact`, benign and now paged), never `Background` (which would
/// wedge a mid-compaction arm forever if CC ever stamped `origin` on ordinary
/// results). Unknown kinds also fire a deduped `Warning` alert naming the kind,
/// signalling a CC upgrade to adapt to.
pub(super) fn classify_turn_origin(
    result: &ResultMessage,
    alert_dispatcher: &AlertDispatcher,
) -> TurnOrigin {
    match &result.origin {
        None => TurnOrigin::Foreground,
        Some(origin) if origin.kind == TASK_NOTIFICATION_KIND => TurnOrigin::Background,
        Some(origin) => {
            // origin.kind is attacker-influenceable CC-output content (posture
            // boundary B3): sanitize before it reaches logs, the pager body, and
            // the dedup key.
            let kind = sanitize_untrusted_str(&origin.kind, MAX_ORIGIN_KIND_BYTES);
            warn!(
                kind = %kind,
                "unrecognized CC result origin kind — treating as foreground; possible CC upgrade"
            );
            alert_dispatcher.alert_once_per_process(
                AlertSeverity::Warning,
                "Unknown CC result origin kind".into(),
                &kind,
                format!(
                    "CC stamped an unrecognized origin kind ({kind}) on a result frame. Brenn is \
                     treating it as a foreground turn. This likely means CC was upgraded with a \
                     new autonomous-turn kind that the compaction state machine should learn to \
                     recognize."
                ),
            );
            TurnOrigin::Foreground
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_cc::protocol::incoming::ResultOrigin;
    use brenn_lib::obs::alerting::make_counting_alerter;
    use std::sync::atomic::Ordering;

    fn result_with_origin(origin: Option<ResultOrigin>) -> ResultMessage {
        ResultMessage {
            subtype: Some("success".into()),
            is_error: Some(false),
            num_turns: Some(1),
            origin,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn absent_origin_is_foreground_no_alert() {
        let (ad, count, handle) = make_counting_alerter();
        assert_eq!(
            classify_turn_origin(&result_with_origin(None), &ad),
            TurnOrigin::Foreground
        );
        drop(ad);
        handle.await.unwrap();
        assert_eq!(
            count.load(Ordering::SeqCst),
            0,
            "no alert for absent origin"
        );
    }

    #[tokio::test]
    async fn task_notification_is_background_no_alert() {
        let (ad, count, handle) = make_counting_alerter();
        let r = result_with_origin(Some(ResultOrigin {
            kind: "task-notification".into(),
            extra: serde_json::Value::Null,
        }));
        assert_eq!(classify_turn_origin(&r, &ad), TurnOrigin::Background);
        drop(ad);
        handle.await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 0, "no alert for known kind");
    }

    #[tokio::test]
    async fn unknown_kind_is_foreground_and_alerts_once() {
        let (ad, count, handle) = make_counting_alerter();
        let r = result_with_origin(Some(ResultOrigin {
            kind: "kind-from-the-future".into(),
            extra: serde_json::Value::Null,
        }));
        // Classify twice — the deduped alert must fire exactly once.
        assert_eq!(classify_turn_origin(&r, &ad), TurnOrigin::Foreground);
        assert_eq!(classify_turn_origin(&r, &ad), TurnOrigin::Foreground);
        drop(ad);
        handle.await.unwrap();
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "unknown kind alerts exactly once per process (deduped)"
        );
    }
}
