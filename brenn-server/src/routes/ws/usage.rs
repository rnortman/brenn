//! `record_usage`, `touch_ui_activity`, `build_graf_env_from` (free fn).

use brenn_lib::usage::{self as usage, EventType};

use super::connection::WsConnection;

// impl WsConnection — usage recording and UI activity tracking
impl WsConnection {
    /// UI-channel activity: notify the active bridge so its idle-hook
    /// timer can cancel-and-rearm. No-op when no current bridge exists.
    /// Called at the top of UI-channel handlers (todo mutations, artifact
    /// viewer interactions). See `docs/designs/idle-hooks.md` § "Defining
    /// 'idle'".
    ///
    /// Called *before* per-handler validation (e.g. `reject_todo_no_graf`)
    /// — the user's intent to engage the conversation counts even when
    /// the specific operation is invalid.
    pub(super) async fn touch_ui_activity(&self, reason: &str) {
        if let Some(conv_id) = self.current_conversation_id
            && let Some(bridge) = self.state.active_bridges.get(conv_id).await
        {
            bridge.touch_ui_activity(reason).await;
        }
    }

    /// Record a usage event. Convenience wrapper around `usage::record_ui_event`,
    /// `record_send_message`, and `record_stop_request` that handles the DB
    /// lock and reads the gap from `state.usage_session_gap_secs`.
    pub(super) async fn record_usage(&self, event_type: EventType) {
        let db_conn = self.state.db.lock().await;
        match event_type {
            EventType::SendMessage => {
                usage::record_send_message(
                    &db_conn,
                    self.device_id,
                    self.user_id,
                    &self.app_slug,
                    self.current_conversation_id,
                    self.state.usage_session_gap_secs,
                );
            }
            EventType::StopRequest => {
                usage::record_stop_request(
                    &db_conn,
                    self.device_id,
                    self.user_id,
                    &self.app_slug,
                    self.current_conversation_id,
                    self.state.usage_session_gap_secs,
                );
            }
            // Caller bugs: these types have dedicated recording functions and
            // must never be routed through record_usage.
            EventType::WsConnect | EventType::WsDisconnect | EventType::LlmTurn => {
                panic!(
                    "record_usage called with {:?}; use the dedicated recording function",
                    event_type
                );
            }
            EventType::TodoRefresh
            | EventType::TodoDone
            | EventType::TodoSchedule
            | EventType::TodoReorder
            | EventType::SwitchConversation
            | EventType::NewConversation
            | EventType::RequestCompaction
            | EventType::RunTarget
            | EventType::SetConversationPrivacy => {
                usage::record_ui_event(
                    &db_conn,
                    self.device_id,
                    self.user_id,
                    &self.app_slug,
                    self.current_conversation_id,
                    event_type,
                    self.state.usage_session_gap_secs,
                );
            }
        }
    }
}

/// Build the env vars for a graf subprocess invocation.
///
/// Combines `GRAF_MANIFEST` (when an app manifest exists) with
/// `GRAF_USER_TZ` (the current user's timezone). The LLM-path knob is
/// graf's `today` MCP/CLI parameter; this env var is the server-side
/// default.
///
/// Split as a free function (rather than only a method on
/// `WsConnection`) so tests can exercise it without standing up the
/// full connection.
pub(super) fn build_graf_env_from(
    app_config: &brenn_lib::config::AppConfig,
    tz: chrono_tz::Tz,
) -> Vec<(String, String)> {
    let mut env = Vec::new();
    if let Some(pair) = brenn_graf::graf_manifest_env(app_config) {
        env.push(pair);
    }
    env.push(brenn_graf::graf_user_tz_env(tz));
    env
}

#[cfg(test)]
mod tests {
    use super::super::testing::*;
    use super::build_graf_env_from;

    /// `build_graf_env_from` emits `GRAF_USER_TZ` with the correct IANA
    /// name. No manifest in the test app → no `GRAF_MANIFEST` entry; the
    /// env slice is just the single TZ pair.
    #[test]
    fn build_graf_env_emits_user_tz_without_manifest() {
        let apps = test_apps();
        let app = apps.get("test").unwrap();
        let env = build_graf_env_from(app, chrono_tz::America::New_York);
        let keys: Vec<&str> = env.iter().map(|(k, _)| k.as_str()).collect();
        assert!(
            keys.contains(&"GRAF_USER_TZ"),
            "env must include GRAF_USER_TZ: {env:?}",
        );
        assert!(
            !keys.contains(&"GRAF_MANIFEST"),
            "test app has no graf manifest, GRAF_MANIFEST should not appear: {env:?}",
        );
        let tz_val = env
            .iter()
            .find(|(k, _)| k == "GRAF_USER_TZ")
            .map(|(_, v)| v.as_str())
            .unwrap();
        assert_eq!(tz_val, "America/New_York");
    }

    /// Different zone in, different zone out.
    #[test]
    fn build_graf_env_threads_different_zones() {
        let apps = test_apps();
        let app = apps.get("test").unwrap();
        let env = build_graf_env_from(app, chrono_tz::Asia::Tokyo);
        let tz_val = env
            .iter()
            .find(|(k, _)| k == "GRAF_USER_TZ")
            .map(|(_, v)| v.as_str())
            .expect("GRAF_USER_TZ must be present");
        assert_eq!(tz_val, "Asia/Tokyo");
    }
}
