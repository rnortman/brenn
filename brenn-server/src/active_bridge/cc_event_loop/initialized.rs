//! `SessionEvent::Initialized` handling: cwd mapping, CC version-floor check,
//! metadata persistence, and tool/permission-mode validation alerts.

use std::path::Path;

use brenn_cc::session::SessionInfo;
use brenn_lib::conversation;
use brenn_lib::obs::alerting::AlertDispatcher;
use brenn_lib::ws_types::{PermissionModeValue, WsServerMessage};
use tracing::{error, info, warn};

use super::super::ActiveBridge;

pub(in crate::active_bridge) async fn handle_initialized(
    bridge: &ActiveBridge,
    info: &SessionInfo,
    alert_dispatcher: &AlertDispatcher,
) {
    // Translate CC-reported cwd to host path for storage.
    // For bare-process apps this is identity; for containerized apps this maps
    // the container path back to the host path so all downstream code (artifact
    // validation, stable URL computation) works with host paths.
    let host_cwd = match bridge.path_mapper.to_host(Path::new(&info.cwd)) {
        Some(p) => p.to_string_lossy().to_string(),
        None => {
            // CC reported a cwd outside the mapped container root. This is an
            // anomaly — we explicitly set -w to the container_working_dir. Alert
            // and store the raw path (artifact serving will be broken for this
            // conversation, but that's better than panicking the whole server).
            error!(
                cc_cwd = %info.cwd,
                conversation_id = bridge.conversation_id,
                app_slug = %bridge.app_slug,
                "CC reported cwd outside mapped container root — artifact serving will not work"
            );
            alert_dispatcher.alert(
                brenn_lib::obs::alerting::AlertSeverity::Warning,
                "CC cwd outside container mapping".into(),
                format!(
                    "CC reported cwd {:?} which is outside the container path mapping",
                    info.cwd,
                ),
            );
            info.cwd.clone()
        }
    };

    let init_elapsed = bridge.spawn_instant.elapsed();
    info!(
        conversation_id = bridge.conversation_id,
        init_elapsed_ms = init_elapsed.as_millis() as u64,
        session_id = %info.session_id,
        model = %info.model,
        cc_cwd = %info.cwd,
        host_cwd = %host_cwd,
        mcp_server_count = info.mcp_servers.len(),
        mcp_servers = ?info.mcp_servers,
        "CC session initialized"
    );
    // Version-floor check: Brenn requires CC >= 2.1.123 for stream-derived
    // context tracking (result.modelUsage.contextWindow). Panicking here
    // satisfies the "shoot itself in the head before doing the wrong thing"
    // posture from CLAUDE.md — continuing on an old CC would produce silent
    // telemetry errors (no modelUsage ever arrives).
    const MIN_CC_VERSION: &str = "2.1.123";
    match info.claude_code_version.as_deref() {
        Some(v) if brenn_lib::util::version_at_least(v, MIN_CC_VERSION) => {
            // Happy path — version is sufficient.
        }
        Some(v) => {
            error!(
                session_id = %info.session_id,
                actual = v,
                minimum = MIN_CC_VERSION,
                "CC version below minimum — refusing to operate"
            );
            panic!(
                "CC reported version {v} but Brenn requires >= {MIN_CC_VERSION} \
                 for stream-derived context tracking. Upgrade CC."
            );
        }
        None => {
            error!("CC init frame missing claude_code_version");
            panic!(
                "CC's system/init frame did not include claude_code_version. \
                 Cannot verify minimum-version requirement (>= {MIN_CC_VERSION})."
            );
        }
    }

    // Capture CC version for model_window_cache upserts.
    *bridge.cc_version.lock().expect("cc_version lock") = info.claude_code_version.clone();

    let conn = bridge.db.lock().await;
    conversation::set_cc_session_id(&conn, bridge.conversation_id, &info.session_id);
    conversation::set_init_metadata(&conn, bridge.conversation_id, &info.model, &host_cwd);

    // Seed max_tokens from the cache so the first ContextUsage broadcast
    // (from the first assistant message) has the correct denominator.
    // On cache miss, leave seed_max_tokens as None — the broadcast is
    // deferred until the result frame provides the authoritative value.
    let seed = brenn_lib::model_window_cache::get(&conn, &info.model);
    drop(conn);
    match seed {
        Some((max_tokens, _ver, _updated)) => {
            *bridge.seed_max_tokens.lock().expect("seed_max_tokens lock") = Some(max_tokens);
        }
        None => {
            info!(
                model = %info.model,
                "no cached window size — context usage deferred until result frame"
            );
            // seed_max_tokens stays None (set at bridge construction).
        }
    }

    // Runtime tool validation: alert on unknown CC tools.
    let mut unknown: Vec<&str> = info
        .tools
        .iter()
        .filter(|t| !t.starts_with("mcp__"))
        .filter(|t| !brenn_lib::config::CC_KNOWN_TOOLS.contains(&t.as_str()))
        .map(|s| s.as_str())
        .collect();
    if !unknown.is_empty() {
        warn!(unknown_tools = ?unknown, "CC reports tools not in CC_KNOWN_TOOLS");
        // Dedup by the sorted tool-name set: if CC adds the same two tools
        // on every spawn, we only page once per brenn restart. Sorting makes
        // the key stable across non-deterministic iteration orders.
        unknown.sort();
        let dedup_key = unknown.join(",");
        alert_dispatcher.alert_once_per_process(
            brenn_lib::obs::alerting::AlertSeverity::Warning,
            "Unknown CC tools detected".into(),
            &dedup_key,
            format!(
                "CC reports tools not in CC_KNOWN_TOOLS: {unknown:?}. \
                 These tools are not in the vetted list and will be blocked \
                 if any app uses disabled_tools. \
                 Update CC_KNOWN_TOOLS in brenn-lib/src/config.rs after vetting."
            ),
        );
    }
    // Verify CC honored `--permission-mode auto`. The flag is pinned by a
    // spawn-side test; this is the matching runtime check that CC actually
    // applied it. If CC ever silently drops/renames the mode, we want loud
    // signal, not a silent regression into per-tool approval prompts.
    match info.permission_mode.as_ref() {
        Some(PermissionModeValue::Auto) => {
            // Happy path. No log/alert.
        }
        Some(PermissionModeValue::Other(other)) => {
            warn!(
                conversation_id = bridge.conversation_id,
                app_slug = %bridge.app_slug,
                session_id = %info.session_id,
                expected = "auto",
                actual = %other,
                "CC permission_mode mismatch — expected 'auto'"
            );
            alert_dispatcher.alert_once_per_process(
                brenn_lib::obs::alerting::AlertSeverity::Warning,
                "CC permission_mode mismatch".into(),
                other,
                format!(
                    "CC reported permission_mode={other:?} in system/init \
                     but Brenn spawned it with --permission-mode auto. \
                     This likely means a CC upgrade changed the flag's \
                     effect. Auto-approval classifier is NOT active; every \
                     tool call will round-trip through Brenn's approval UI."
                ),
            );
        }
        None => {
            warn!(
                conversation_id = bridge.conversation_id,
                app_slug = %bridge.app_slug,
                session_id = %info.session_id,
                "CC init frame missing permission_mode field"
            );
            alert_dispatcher.alert_once_per_process(
                brenn_lib::obs::alerting::AlertSeverity::Warning,
                "CC permission_mode missing from init".into(),
                "missing",
                "CC's system/init frame did not include the \
                 permission_mode field. Brenn can no longer verify \
                 CC honored --permission-mode auto. Likely a CC schema \
                 change."
                    .into(),
            );
        }
    }

    bridge.broadcast(WsServerMessage::PermissionMode {
        mode: info.permission_mode.clone(),
    });

    // The system/init message is purely informational — readiness is signalled
    // by the control_response received during spawn. CC may emit init at any
    // time (or not at all on a stale --resume session until stdin activity);
    // we record the metadata and move on. No state broadcast or drain happens
    // here. See docs/designs/init-not-required.md.
}
