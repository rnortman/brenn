use std::sync::atomic::AtomicBool;
use std::time::Instant;

use brenn_lib::obs::alerting::AlertDispatcher;
use brenn_lib::ws_types::WsServerMessage;
use tracing::{debug, warn};

use crate::active_bridge::ActiveBridge;

use super::state::ContextUsage;

// Stream-derived context tracking helpers
// ---------------------------------------------------------------------------

/// Clamp to 100 and avoid integer overflow when computing usage_pct.
///
/// Shared by `update_context_from_assistant` and `update_max_tokens_from_result`
/// so both sites use the same overflow-safe, clamped formula.
fn compute_usage_pct(current: u64, max: u64) -> u8 {
    let pct = (current as u128 * 100) / (max.max(1) as u128);
    pct.min(100) as u8
}

/// Update context fill from the assistant message's token-usage fields.
///
/// Only top-level messages drive the user-facing pill (A11). Subagent messages
/// have a non-None `parent_tool_use_id` and are skipped. Broadcasts
/// `ContextUsage` immediately so the seeded `max_tokens` is visible on the wire
/// before the matching `result` arrives (A8).
///
/// Returns `Some(new_slug)` on a genuine mid-session model switch
/// (`Some(old)` → `Some(new)`). The caller must then do a cache lookup for
/// `new_slug` and set `seed_max_tokens` accordingly. Returns `None` on initial
/// slug assignment or same-model message — no caller action required.
pub(in crate::active_bridge) fn update_context_from_assistant(
    bridge: &ActiveBridge,
    msg: &brenn_cc::protocol::incoming::AssistantMessage,
    alert_dispatcher: &AlertDispatcher,
) -> Option<String> {
    if msg.parent_tool_use_id.is_some() {
        return None; // Subagent — skip.
    }

    // Track active top-level model slug for modelUsage lookup at result time.
    // Done unconditionally before the usage check — slug tracking is independent
    // of whether this particular message carries usage tokens (correctness-5).
    // On genuine slug change (Some(old) → Some(new)), null context_usage and
    // seed_max_tokens so the caller can re-seed from the cache. Initial
    // assignment (None → Some) does not signal — the init-time seed is correct.
    // `update_max_tokens_from_result` overwrites with the authoritative
    // contextWindow value at turn-end (correctness-1 fix).
    //
    // Efficiency: borrow-compare with as_deref() first so the steady-state path
    // (same slug) pays zero heap allocations. Allocate only on the rare branches
    // (initial assignment or genuine switch). The genuine-switch branch also sets
    // `seed_switch = None` directly, avoiding the redundant seed_max_tokens
    // lock-read below (seed is already known to be None on that branch).
    let mut seed_switch: Option<Option<u64>> = None; // Some(None) on genuine switch
    let new_model_slug: Option<String> = if let Some(model) = msg.message.model.as_deref() {
        let mut slot = bridge
            .active_model_slug
            .lock()
            .expect("active_model_slug lock");
        let is_genuine_switch = matches!(slot.as_deref(), Some(prev) if prev != model);
        let is_initial_assignment = slot.is_none();
        if is_genuine_switch || is_initial_assignment {
            *slot = Some(model.to_string());
        }
        if is_genuine_switch {
            drop(slot);
            // Clear stale state; signal caller to re-seed from cache.
            *bridge
                .seed_max_tokens
                .lock()
                .expect("seed_max_tokens lock on model change") = None;
            *bridge
                .context_usage
                .lock()
                .expect("context_usage lock on model change") = None;
            // Record that seed is already None — skip the lock below.
            seed_switch = Some(None);
            Some(model.to_string())
        } else {
            None // Initial assignment or same model — no signal.
        }
    } else {
        None
    };

    let Some(cc_usage) = msg.message.usage.as_ref() else {
        return new_model_slug;
    };

    // Observe cache-token fields for schema-drift detection.
    // Per-callsite AtomicBool caches bypass the global HAVE_SEEN mutex on the
    // steady-state path (field present after first observation).
    {
        static SEEN_CACHE_READ: AtomicBool = AtomicBool::new(false);
        crate::cc_schema_drift::observe_with_cache(
            alert_dispatcher,
            "assistant.usage.cache_read_input_tokens",
            cc_usage.cache_read_input_tokens.is_some(),
            &SEEN_CACHE_READ,
        );
    }
    {
        static SEEN_CACHE_CREATE: AtomicBool = AtomicBool::new(false);
        crate::cc_schema_drift::observe_with_cache(
            alert_dispatcher,
            "assistant.usage.cache_creation_input_tokens",
            cc_usage.cache_creation_input_tokens.is_some(),
            &SEEN_CACHE_CREATE,
        );
    }

    let current = cc_usage.cache_read_input_tokens.unwrap_or(0)
        + cc_usage.cache_creation_input_tokens.unwrap_or(0)
        + cc_usage.input_tokens.unwrap_or(0);

    // Resolve max_tokens before entering the context_usage mutation block.
    // seed_max_tokens is None when no cache entry exists (fresh deployment or
    // mid-session switch to an unseen model) — defer broadcast rather than
    // using a hardcoded guess that would be wrong for 1M models.
    // On genuine slug change, seed_switch already carries None (set above);
    // skip the redundant lock acquisition on that rare branch.
    let seed = seed_switch
        .unwrap_or_else(|| *bridge.seed_max_tokens.lock().expect("seed_max_tokens lock"));

    let (snapshot, changed) = {
        let mut slot = bridge.context_usage.lock().expect("context_usage lock");
        let max = slot.as_ref().map(|u| u.max_tokens).or(seed);

        let Some(max) = max else {
            // No denominator available yet — defer broadcast until the
            // result frame provides the authoritative contextWindow value.
            debug!("context usage broadcast deferred — no max_tokens available yet");
            return new_model_slug;
        };

        let changed = slot
            .as_ref()
            .is_none_or(|prev| prev.current_tokens != current || prev.max_tokens != max);
        let updated = ContextUsage {
            current_tokens: current,
            max_tokens: max,
            usage_pct: compute_usage_pct(current, max),
            checked_at: Instant::now(),
        };
        *slot = Some(updated.clone());
        (updated, changed)
    };

    // Broadcast immediately so the seeded max_tokens is observable on the
    // wire before the matching result arrives (A8). A follow-up broadcast
    // from handle_turn_completed (after update_max_tokens_from_result)
    // overwrites with the authoritative value. Skip when unchanged to avoid
    // redundant frames within a single turn.
    if changed {
        broadcast_context_usage(bridge, &snapshot);
    }
    new_model_slug
}

/// Broadcast a `ContextUsage` message. No-op for non-singleton apps.
pub(super) fn broadcast_context_usage(bridge: &ActiveBridge, usage: &ContextUsage) {
    let Some(config) = bridge.compaction_config.as_ref() else {
        return; // Non-singleton apps: never broadcast context.
    };
    bridge.broadcast(WsServerMessage::ContextUsage {
        usage_pct: usage.usage_pct,
        current_tokens: usage.current_tokens,
        max_tokens: usage.max_tokens,
        reminder_pct: config.reminder_pct,
        red_pct: config.red_pct,
        reminder_tokens: config.reminder_tokens,
        red_tokens: config.red_tokens,
    });
}

/// Sync inner for `update_max_tokens_from_result`.
///
/// Performs all logic against a borrowed `&Connection` — no DB lock acquired
/// here. Called by `handle_turn_completed` inside the unified DB scope and
/// by the async wrapper for standalone test callers.
///
/// # Locking order
/// The caller holds `bridge.db` (tokio mutex). This function may additionally
/// acquire `active_model_slug`, `context_usage`, and `cc_version` std mutexes
/// under it. **No code path may acquire `bridge.db.lock()` while already
/// holding any of these std mutexes — doing so deadlocks. See
/// docs/adr/2026/05/17-db-lock-coalesce-turn-end/design.md.**
pub(super) fn update_max_tokens_from_result_sync(
    bridge: &ActiveBridge,
    result: &brenn_cc::protocol::incoming::ResultMessage,
    alert_dispatcher: &AlertDispatcher,
    conn: &rusqlite::Connection,
) -> bool {
    let Some(model_usage) = result.model_usage.as_ref() else {
        // Compaction-result frames carry no modelUsage — expected, not an error.
        return false;
    };

    // Empty map is drift: every observed result in the recorded session had at
    // least one entry. Alert once per process when we see a non-empty map go
    // empty (i.e., first observation was non-empty, then we see empty).
    {
        static SEEN_NON_EMPTY: AtomicBool = AtomicBool::new(false);
        crate::cc_schema_drift::observe_with_cache(
            alert_dispatcher,
            "result.modelUsage.non_empty",
            !model_usage.is_empty(),
            &SEEN_NON_EMPTY,
        );
    }
    if model_usage.is_empty() {
        return false;
    }

    let active = bridge
        .active_model_slug
        .lock()
        .expect("active_model_slug lock")
        .clone();

    let Some((slug, entry)) =
        pick_and_observe_model_usage(model_usage, active.as_deref(), alert_dispatcher)
    else {
        return false;
    };

    // Active model's contextWindow MUST be a number (requirements: malformed is
    // unexpected → panic). None here means CC sent the entry with null or a
    // non-numeric contextWindow — refuse to corrupt telemetry.
    let max = entry.context_window.unwrap_or_else(|| {
        panic!(
            "modelUsage[{slug}].contextWindow was null/missing on the active \
             model entry — protocol violation"
        )
    });

    {
        let mut slot = bridge.context_usage.lock().expect("context_usage lock");
        if let Some(u) = slot.as_mut() {
            u.max_tokens = max;
            u.usage_pct = compute_usage_pct(u.current_tokens, max);
        } else {
            // Authoritative window size arrived but context_usage is None
            // (no prior assistant message populated it). Log at WARN so an
            // engineer debugging a missing context pill has a log signal.
            // This is expected when a result arrives before the first
            // assistant message (unlikely but possible under race conditions
            // or unusual CC behaviour). Returns true so the cache upsert
            // still runs — we've learned the window size even though we
            // can't yet broadcast it (errhandling-3). Note: `true` here
            // means "window size was learned / cache should be updated",
            // NOT "context_usage snapshot was mutated". The sole caller
            // (`handle_turn_completed`) re-reads context_usage and guards
            // with `if let Some(cur)` before broadcasting.
            warn!(
                max_tokens = max,
                "authoritative contextWindow observed but context_usage is None — \
                 will broadcast on next assistant message"
            );
        }
    }

    let cc_version = bridge.cc_version.lock().expect("cc_version lock").clone();
    brenn_lib::model_window_cache::upsert(conn, slug, max, cc_version.as_deref());
    true
}

/// Update max_tokens in `context_usage` from `result.modelUsage`.
///
/// Returns `true` when the update succeeded (modelUsage was present and the
/// active model's entry was found); `false` when the result lacks modelUsage
/// (compaction frames, etc.) — the caller skips the ContextUsage broadcast
/// in that case (A10).
///
/// Async wrapper: acquires `bridge.db` and delegates to
/// `update_max_tokens_from_result_sync`. Only called from tests;
/// `handle_turn_completed` calls the sync inner directly.
#[cfg(test)]
async fn update_max_tokens_from_result(
    bridge: &ActiveBridge,
    result: &brenn_cc::protocol::incoming::ResultMessage,
    alert_dispatcher: &AlertDispatcher,
) -> bool {
    let conn = bridge.db.lock().await;
    update_max_tokens_from_result_sync(bridge, result, alert_dispatcher, &conn)
}

/// Combined pick + drift-observe pass over `model_usage`.
///
/// 3-step matching strategy: (1) exact match, (2) suffix-bearing match
/// (e.g. active = "claude-opus-4-7", key = "claude-opus-4-7[1m]"),
/// (3) log WARN and return None. Non-active entries are observed for
/// contextWindow drift via `observe_with_cache` in the same pass, collapsing
/// the previous two-pass pattern (separate pick + observe loop) into one.
///
/// The per-callsite drift `static` is owned here so callers don't need to
/// declare their own.
fn pick_and_observe_model_usage<'a>(
    map: &'a std::collections::HashMap<String, brenn_cc::protocol::incoming::ModelUsageEntry>,
    active: Option<&'a str>,
    alert_dispatcher: &AlertDispatcher,
) -> Option<(&'a str, &'a brenn_cc::protocol::incoming::ModelUsageEntry)> {
    static SEEN_CONTEXT_WINDOW: AtomicBool = AtomicBool::new(false);

    let active = active?;

    let mut exact: Option<(&str, &brenn_cc::protocol::incoming::ModelUsageEntry)> = None;
    let mut suffix: Option<(&str, &brenn_cc::protocol::incoming::ModelUsageEntry)> = None;

    for (k, e) in map {
        if k.as_str() == active {
            exact = Some((k.as_str(), e));
        } else if k.starts_with(active) && k[active.len()..].starts_with('[') {
            // Suffix-bearing variant of the active model (e.g. "claude-opus-4-7[1m]"
            // when active is "claude-opus-4-7"). This is the same model with a context
            // suffix, not a subagent — skip drift observation. If both exact and suffix
            // entries exist, exact wins (returned below); the suffix entry is not used.
            // Tie-break by lexicographic key order (deterministic if CC ever exposes
            // two suffix variants for the same base model in one result).
            let is_better = suffix.is_none_or(|(prev_k, _)| k.as_str() < prev_k);
            if is_better {
                suffix = Some((k.as_str(), e));
            }
        } else {
            // Non-active (subagent) entry — observe for contextWindow drift.
            crate::cc_schema_drift::observe_with_cache(
                alert_dispatcher,
                "modelUsage[*].contextWindow",
                e.context_window.is_some(),
                &SEEN_CONTEXT_WINDOW,
            );
        }
    }

    if let Some(pair) = exact {
        return Some(pair);
    }
    if let Some(pair) = suffix {
        return Some(pair);
    }

    let available_keys: Vec<&str> = map.keys().map(|s| s.as_str()).collect();
    warn!(
        active,
        ?available_keys,
        "no modelUsage entry found for active model slug"
    );
    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::super::super::ActiveBridges;
    use super::super::super::test_support::drain_broadcast;
    use super::*;
    use brenn_lib::conversation;
    use brenn_lib::ws_types::WsServerMessage;

    use crate::active_bridge::test_fixtures::TestBridgeConfig;
    use std::time::Duration;
    use tokio::sync::broadcast;

    #[tokio::test]
    async fn update_context_from_assistant_uses_cache_fields() {
        // cache_read = 50k, cache_creation = 5k, input = 1k → current = 56k.
        use brenn_cc::protocol::incoming::{AssistantContent, AssistantMessage, Usage};
        let (tx, _rx) = broadcast::channel(64);
        let db = brenn_lib::db::init_db_memory();
        let active_bridges = ActiveBridges::new();
        let (uid, conv_id) = {
            let conn = db.lock().await;
            let uid = brenn_lib::auth::user::create_user(&conn, "u", "$argon2id$fake");
            let cid = conversation::create_conversation(&conn, uid, "test", false);
            (uid, cid)
        };
        let config = brenn_lib::config::CompactionConfig {
            reminder_pct: 60,
            soft_pct: 75,
            red_pct: 80,
            hard_pct: 95,
            reminder_tokens: None,
            soft_tokens: None,
            red_tokens: None,
            hard_tokens: None,
            idle_duration: Duration::from_secs(300),
        };
        let bridge = crate::active_bridge::ActiveBridge::inject_for_test_full(
            uid,
            conv_id,
            "test",
            db,
            tx,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                active_bridges: Some(active_bridges),
                singleton: true,
                compaction_config: Some(config),
                ..Default::default()
            },
        );
        // Seed max_tokens so broadcast_context_usage doesn't short-circuit.
        *bridge.seed_max_tokens.lock().expect("lock") = Some(200_000);

        let (ad, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        let msg = AssistantMessage {
            message: AssistantContent {
                role: "assistant".into(),
                content: vec![],
                model: Some("claude-sonnet-4-6".into()),
                usage: Some(Usage {
                    input_tokens: Some(1_000),
                    output_tokens: None,
                    cache_read_input_tokens: Some(50_000),
                    cache_creation_input_tokens: Some(5_000),
                    extra: serde_json::Value::Null,
                }),
            },
            uuid: "u".into(),
            parent_tool_use_id: None,
        };
        // Subscribe before the call so we can assert the broadcast fired (A8,
        // test-6: ContextUsage must be visible on the wire before any result).
        let mut broadcast_rx2 = bridge.subscribe();

        update_context_from_assistant(&bridge, &msg, &ad);
        let usage = bridge
            .context_usage
            .lock()
            .expect("lock")
            .clone()
            .expect("populated");
        assert_eq!(usage.current_tokens, 56_000);

        // Verify the immediate ContextUsage broadcast fired (A8: observable on
        // the wire before the matching result arrives — test-6).
        let broadcast_msg =
            tokio::time::timeout(std::time::Duration::from_secs(1), broadcast_rx2.recv())
                .await
                .expect("broadcast timed out")
                .expect("broadcast channel closed");
        match broadcast_msg {
            WsServerMessage::ContextUsage {
                current_tokens,
                max_tokens,
                ..
            } => {
                assert_eq!(
                    current_tokens, 56_000,
                    "broadcast ContextUsage must carry the new current_tokens"
                );
                assert_eq!(
                    max_tokens, 200_000,
                    "broadcast ContextUsage must carry the seeded max_tokens"
                );
            }
            other => panic!("expected ContextUsage broadcast, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn update_context_from_assistant_skips_subagent() {
        // A message with parent_tool_use_id must not update context_usage.
        use brenn_cc::protocol::incoming::{AssistantContent, AssistantMessage, Usage};
        let (tx, _rx) = broadcast::channel(64);
        let db = brenn_lib::db::init_db_memory();
        let active_bridges = ActiveBridges::new();
        let (uid, conv_id) = {
            let conn = db.lock().await;
            let uid = brenn_lib::auth::user::create_user(&conn, "u2", "$argon2id$fake");
            let cid = conversation::create_conversation(&conn, uid, "test", false);
            (uid, cid)
        };
        let bridge = crate::active_bridge::ActiveBridge::inject_for_test_full(
            uid,
            conv_id,
            "test",
            db,
            tx,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                active_bridges: Some(active_bridges),
                ..Default::default()
            },
        );
        let (ad, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        let msg = AssistantMessage {
            message: AssistantContent {
                role: "assistant".into(),
                content: vec![],
                model: Some("claude-haiku-3-5".into()),
                usage: Some(Usage {
                    input_tokens: Some(10_000),
                    output_tokens: None,
                    cache_read_input_tokens: Some(5_000),
                    cache_creation_input_tokens: None,
                    extra: serde_json::Value::Null,
                }),
            },
            uuid: "u".into(),
            parent_tool_use_id: Some("parent-tool-use-id".into()),
        };
        update_context_from_assistant(&bridge, &msg, &ad);
        // context_usage must remain None because this was a subagent message.
        assert!(bridge.context_usage.lock().expect("lock").is_none());
    }

    #[tokio::test]
    async fn pick_modelusage_entry_prefers_suffix_match() {
        use brenn_cc::protocol::incoming::ModelUsageEntry;
        use std::collections::HashMap;
        let mut map = HashMap::new();
        map.insert(
            "claude-opus-4-7[1m]".to_string(),
            ModelUsageEntry {
                context_window: Some(1_000_000),
                max_output_tokens: None,
                cost_usd: None,
                extra: serde_json::Value::Null,
            },
        );
        map.insert(
            "claude-haiku-3-5".to_string(),
            ModelUsageEntry {
                context_window: Some(200_000),
                max_output_tokens: None,
                cost_usd: None,
                extra: serde_json::Value::Null,
            },
        );
        let (ad, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        let result = pick_and_observe_model_usage(&map, Some("claude-opus-4-7"), &ad);
        let (slug, entry) = result.expect("should find a match");
        assert_eq!(slug, "claude-opus-4-7[1m]");
        assert_eq!(entry.context_window, Some(1_000_000));
    }

    /// Rule 2 suffix tie-break — when two suffix-matched keys exist for the
    /// same base model, the lexicographically smaller key wins regardless of
    /// `HashMap` iteration order.
    #[tokio::test]
    async fn pick_modelusage_entry_suffix_tiebreak_is_deterministic() {
        use brenn_cc::protocol::incoming::ModelUsageEntry;
        use std::collections::HashMap;
        let mut map = HashMap::new();
        map.insert(
            "claude-opus-4-7[200k]".to_string(),
            ModelUsageEntry {
                context_window: Some(200_000),
                max_output_tokens: None,
                cost_usd: None,
                extra: serde_json::Value::Null,
            },
        );
        map.insert(
            "claude-opus-4-7[1m]".to_string(),
            ModelUsageEntry {
                context_window: Some(1_000_000),
                max_output_tokens: None,
                cost_usd: None,
                extra: serde_json::Value::Null,
            },
        );
        let (ad, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        // "claude-opus-4-7[1m]" < "claude-opus-4-7[200k]" lexicographically,
        // so [1m] must win regardless of HashMap iteration order.
        let result = pick_and_observe_model_usage(&map, Some("claude-opus-4-7"), &ad);
        let (slug, _entry) = result.expect("should find a match");
        assert_eq!(slug, "claude-opus-4-7[1m]");
    }

    /// Rule 1 (exact match) — map key equals active exactly.
    #[tokio::test]
    async fn pick_modelusage_entry_exact_match() {
        use brenn_cc::protocol::incoming::ModelUsageEntry;
        use std::collections::HashMap;
        let mut map = HashMap::new();
        map.insert(
            "claude-haiku-3-5".to_string(),
            ModelUsageEntry {
                context_window: Some(200_000),
                max_output_tokens: None,
                cost_usd: None,
                extra: serde_json::Value::Null,
            },
        );
        let (ad, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        // Exact match: active == map key, no suffix needed.
        let result = pick_and_observe_model_usage(&map, Some("claude-haiku-3-5"), &ad);
        let (slug, entry) = result.expect("exact match should be found");
        assert_eq!(slug, "claude-haiku-3-5");
        assert_eq!(entry.context_window, Some(200_000));
    }

    #[tokio::test]
    async fn pick_modelusage_entry_returns_none_on_no_active() {
        use brenn_cc::protocol::incoming::ModelUsageEntry;
        use std::collections::HashMap;
        let mut map = HashMap::new();
        map.insert(
            "claude-opus-4-7[1m]".to_string(),
            ModelUsageEntry {
                context_window: Some(1_000_000),
                max_output_tokens: None,
                cost_usd: None,
                extra: serde_json::Value::Null,
            },
        );
        let (ad, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        // No active slug → cannot pick.
        assert!(pick_and_observe_model_usage(&map, None, &ad).is_none());
    }

    #[test]
    fn compute_usage_pct_basic() {
        assert_eq!(compute_usage_pct(50_000, 200_000), 25);
        assert_eq!(compute_usage_pct(200_000, 200_000), 100);
        assert_eq!(compute_usage_pct(210_000, 200_000), 100); // clamp
        assert_eq!(compute_usage_pct(0, 200_000), 0);
        assert_eq!(compute_usage_pct(0, 0), 0); // no div-by-zero
    }

    /// Seed = 200k; result carries contextWindow = 1_000_000 for the active
    /// model. After `update_max_tokens_from_result`, `max_tokens` must flip to 1M.
    #[tokio::test]
    async fn update_max_tokens_from_modelusage_overrides_seed() {
        use brenn_cc::protocol::incoming::{ModelUsageEntry, ResultMessage};
        use std::collections::HashMap;

        let db = brenn_lib::db::init_db_memory();
        let (tx, _rx) = broadcast::channel(64);
        let active_bridges = ActiveBridges::new();
        let (uid, conv_id) = {
            let conn = db.lock().await;
            let uid = brenn_lib::auth::user::create_user(&conn, "seed-test", "$argon2id$fake");
            let cid = conversation::create_conversation(&conn, uid, "test", false);
            (uid, cid)
        };
        let bridge = crate::active_bridge::ActiveBridge::inject_for_test_full(
            uid,
            conv_id,
            "test",
            db,
            tx,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                active_bridges: Some(active_bridges),
                singleton: true,
                ..Default::default()
            },
        );
        // Pre-seed with 200k — this is what handle_initialized would set.
        *bridge.seed_max_tokens.lock().expect("lock") = Some(200_000);
        // Simulate an assistant message having already updated context_usage with
        // the seed max_tokens.
        *bridge.context_usage.lock().expect("lock") = Some(ContextUsage {
            current_tokens: 50_000,
            max_tokens: 200_000,
            usage_pct: 25,
            checked_at: Instant::now(),
        });
        // Set active model slug so pick_modelusage_entry can match.
        *bridge.active_model_slug.lock().expect("lock") = Some("claude-opus-4-7".to_string());

        let mut map = HashMap::new();
        map.insert(
            "claude-opus-4-7[1m]".to_string(),
            ModelUsageEntry {
                context_window: Some(1_000_000),
                max_output_tokens: None,
                cost_usd: None,
                extra: serde_json::Value::Null,
            },
        );
        let result = ResultMessage {
            subtype: None,
            duration_ms: None,
            duration_api_ms: None,
            is_error: None,
            num_turns: None,
            session_id: None,
            total_cost_usd: None,
            usage: None,
            result: None,
            stop_reason: None,
            model_usage: Some(map),
            origin: None,
            extra: serde_json::Value::Null,
        };

        let (ad, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        let updated = update_max_tokens_from_result(&bridge, &result, &ad).await;
        assert!(updated, "update should succeed");

        let usage = bridge
            .context_usage
            .lock()
            .expect("lock")
            .clone()
            .expect("context_usage must be set");
        assert_eq!(
            usage.max_tokens, 1_000_000,
            "max_tokens should flip from 200k seed to 1M from modelUsage"
        );

        // Verify the cache upsert actually ran with the matched key (not the
        // bare active slug). A bug that writes the bare slug "claude-opus-4-7"
        // instead of the matched "claude-opus-4-7[1m]" would break cross-session
        // seeding for 1M models (test-2 / correctness-13).
        let conn = bridge.db.lock().await;
        let cached = brenn_lib::model_window_cache::get(&conn, "claude-opus-4-7[1m]")
            .expect("cache should have an entry for the matched slug");
        assert_eq!(
            cached.0, 1_000_000,
            "model_window_cache must store the matched key and 1M value"
        );
        // The bare slug must NOT have a cache entry (the write used the matched key).
        assert!(
            brenn_lib::model_window_cache::get(&conn, "claude-opus-4-7").is_none(),
            "cache must not contain an entry under the bare (unsuffixed) slug"
        );
    }

    /// When `result.modelUsage` is absent, `update_max_tokens_from_result`
    /// returns `false` and `context_usage` is unchanged (A10 guard).
    #[tokio::test]
    async fn update_max_tokens_returns_false_when_modelusage_absent() {
        use brenn_cc::protocol::incoming::ResultMessage;

        let db = brenn_lib::db::init_db_memory();
        let (tx, _rx) = broadcast::channel(64);
        let active_bridges = ActiveBridges::new();
        let (uid, conv_id) = {
            let conn = db.lock().await;
            let uid = brenn_lib::auth::user::create_user(&conn, "absent-test", "$argon2id$fake");
            let cid = conversation::create_conversation(&conn, uid, "test", false);
            (uid, cid)
        };
        let bridge = crate::active_bridge::ActiveBridge::inject_for_test_full(
            uid,
            conv_id,
            "test",
            db,
            tx,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                active_bridges: Some(active_bridges),
                singleton: true,
                ..Default::default()
            },
        );
        *bridge.context_usage.lock().expect("lock") = Some(ContextUsage {
            current_tokens: 40_000,
            max_tokens: 200_000,
            usage_pct: 20,
            checked_at: Instant::now(),
        });

        let result = ResultMessage {
            subtype: None,
            duration_ms: None,
            duration_api_ms: None,
            is_error: None,
            num_turns: None,
            session_id: None,
            total_cost_usd: None,
            usage: None,
            result: None,
            stop_reason: None,
            model_usage: None, // <-- absent
            origin: None,
            extra: serde_json::Value::Null,
        };

        let (ad, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        let updated = update_max_tokens_from_result(&bridge, &result, &ad).await;
        assert!(!updated, "should return false when modelUsage is absent");

        let usage = bridge
            .context_usage
            .lock()
            .expect("lock")
            .clone()
            .expect("context_usage must still be set");
        assert_eq!(
            usage.max_tokens, 200_000,
            "max_tokens must be unchanged when modelUsage is absent"
        );
    }

    /// A result frame where the active model's `contextWindow` is `None` must
    /// panic (protocol violation — "refuse to corrupt telemetry").
    #[tokio::test]
    #[should_panic(expected = "protocol violation")]
    async fn update_max_tokens_panics_on_null_context_window_for_active_model() {
        use brenn_cc::protocol::incoming::{ModelUsageEntry, ResultMessage};
        use std::collections::HashMap;

        let db = brenn_lib::db::init_db_memory();
        let (tx, _rx) = broadcast::channel(64);
        let active_bridges = ActiveBridges::new();
        let (uid, conv_id) = {
            let conn = db.lock().await;
            let uid = brenn_lib::auth::user::create_user(&conn, "null-cw-test", "$argon2id$fake");
            let cid = conversation::create_conversation(&conn, uid, "test", false);
            (uid, cid)
        };
        let bridge = crate::active_bridge::ActiveBridge::inject_for_test_full(
            uid,
            conv_id,
            "test",
            db,
            tx,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                active_bridges: Some(active_bridges),
                singleton: true,
                ..Default::default()
            },
        );
        *bridge.context_usage.lock().expect("lock") = Some(ContextUsage {
            current_tokens: 50_000,
            max_tokens: 200_000,
            usage_pct: 25,
            checked_at: Instant::now(),
        });
        *bridge.active_model_slug.lock().expect("lock") = Some("claude-opus-4-7".to_string());

        let mut map = HashMap::new();
        map.insert(
            "claude-opus-4-7[1m]".to_string(),
            ModelUsageEntry {
                context_window: None, // <-- null on active model — must panic
                max_output_tokens: None,
                cost_usd: None,
                extra: serde_json::Value::Null,
            },
        );
        let result = ResultMessage {
            subtype: None,
            duration_ms: None,
            duration_api_ms: None,
            is_error: None,
            num_turns: None,
            session_id: None,
            total_cost_usd: None,
            usage: None,
            result: None,
            stop_reason: None,
            model_usage: Some(map),
            origin: None,
            extra: serde_json::Value::Null,
        };

        let (ad, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        update_max_tokens_from_result(&bridge, &result, &ad).await;
        // Should have panicked — if we reach here the test fails.
    }

    // -----------------------------------------------------------------------
    // pick_and_observe_model_usage — drift-observation branch test
    // -----------------------------------------------------------------------

    /// Verify that non-active (subagent) entries pass through `observe_with_cache`
    /// and that the drift alert fires when the field subsequently disappears.
    ///
    /// The `SEEN_CONTEXT_WINDOW` static is process-global, so we use a
    /// `CountingAlerter` and a two-call sequence: first with `present=true`
    /// (ensures the field is in HAVE_SEEN regardless of prior test state), then
    /// with `present=false` (must fire the drift alert).
    #[tokio::test]
    async fn pick_and_observe_model_usage_observes_subagent_drift() {
        use brenn_cc::protocol::incoming::ModelUsageEntry;
        use brenn_lib::obs::alerting::{AlertDispatcher, CountingAlerter, RateLimiter};
        use std::collections::HashMap;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU32, Ordering};

        let count = Arc::new(AtomicU32::new(0));
        let (ad, _h) =
            AlertDispatcher::new(CountingAlerter(count.clone()), RateLimiter::new(1000, 60));

        let mut map = HashMap::new();
        // Active model entry.
        map.insert(
            "claude-sonnet-4-6".to_string(),
            ModelUsageEntry {
                context_window: Some(200_000),
                max_output_tokens: None,
                cost_usd: None,
                extra: serde_json::Value::Null,
            },
        );
        // Subagent entry: present=true — warms HAVE_SEEN and SEEN_CONTEXT_WINDOW.
        map.insert(
            "claude-haiku-3-5-subagent".to_string(),
            ModelUsageEntry {
                context_window: Some(200_000),
                max_output_tokens: None,
                cost_usd: None,
                extra: serde_json::Value::Null,
            },
        );
        let result = pick_and_observe_model_usage(&map, Some("claude-sonnet-4-6"), &ad);
        assert!(result.is_some(), "should pick the active entry");
        // No alert yet — subagent field is present.
        tokio::task::yield_now().await;
        // (May be 0 or existing — but we care about the delta after next call.)

        let before = count.load(Ordering::SeqCst);

        // Now simulate the subagent dropping contextWindow.
        map.insert(
            "claude-haiku-3-5-subagent".to_string(),
            ModelUsageEntry {
                context_window: None, // field disappeared
                max_output_tokens: None,
                cost_usd: None,
                extra: serde_json::Value::Null,
            },
        );
        pick_and_observe_model_usage(&map, Some("claude-sonnet-4-6"), &ad);
        tokio::task::yield_now().await;

        let after = count.load(Ordering::SeqCst);
        assert_eq!(
            after - before,
            1,
            "drift alert must fire when subagent contextWindow disappears"
        );
    }

    /// - seed_max_tokens must be unchanged.
    #[tokio::test]
    async fn slug_change_returns_new_slug_and_nulls_state() {
        use brenn_cc::protocol::incoming::{AssistantContent, AssistantMessage, Usage};
        let (tx, _rx) = broadcast::channel(64);
        let db = brenn_lib::db::init_db_memory();
        let active_bridges = ActiveBridges::new();
        let (uid, conv_id) = {
            let conn = db.lock().await;
            let uid = brenn_lib::auth::user::create_user(&conn, "switch-test", "$argon2id$fake");
            let cid = conversation::create_conversation(&conn, uid, "test", false);
            (uid, cid)
        };
        let config = brenn_lib::config::CompactionConfig {
            reminder_pct: 60,
            soft_pct: 75,
            red_pct: 80,
            hard_pct: 95,
            reminder_tokens: None,
            soft_tokens: None,
            red_tokens: None,
            hard_tokens: None,
            idle_duration: Duration::from_secs(300),
        };
        let bridge = crate::active_bridge::ActiveBridge::inject_for_test_full(
            uid,
            conv_id,
            "test",
            db,
            tx,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                active_bridges: Some(active_bridges),
                singleton: true,
                compaction_config: Some(config),
                ..Default::default()
            },
        );
        let (ad, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();

        // --- Part 1: Initial assignment (None → Some) must not signal. ---
        *bridge.active_model_slug.lock().expect("lock") = None;
        *bridge.seed_max_tokens.lock().expect("lock") = Some(200_000);
        let initial_msg = AssistantMessage {
            message: AssistantContent {
                role: "assistant".into(),
                content: vec![],
                model: Some("claude-sonnet-4-6".into()),
                usage: None,
            },
            uuid: "u-init".into(),
            parent_tool_use_id: None,
        };
        let ret = update_context_from_assistant(&bridge, &initial_msg, &ad);
        assert_eq!(ret, None, "initial assignment must return None (no signal)");
        // seed_max_tokens must be unchanged on initial assignment.
        assert_eq!(
            *bridge.seed_max_tokens.lock().expect("lock"),
            Some(200_000),
            "seed_max_tokens must not be modified on initial slug assignment"
        );

        // --- Part 2: Genuine switch (Some(sonnet) → Some(opus)) must signal. ---
        // Seed state as it would be after a real turn.
        *bridge.seed_max_tokens.lock().expect("lock") = Some(200_000);
        *bridge.context_usage.lock().expect("lock") = Some(ContextUsage {
            current_tokens: 50_000,
            max_tokens: 200_000,
            usage_pct: 25,
            checked_at: Instant::now(),
        });

        let switch_msg = AssistantMessage {
            message: AssistantContent {
                role: "assistant".into(),
                content: vec![],
                model: Some("claude-opus-4-7[1m]".into()),
                usage: Some(Usage {
                    input_tokens: Some(1_000),
                    output_tokens: None,
                    cache_read_input_tokens: None,
                    cache_creation_input_tokens: None,
                    extra: serde_json::Value::Null,
                }),
            },
            uuid: "u-switch".into(),
            parent_tool_use_id: None,
        };
        let ret = update_context_from_assistant(&bridge, &switch_msg, &ad);
        assert_eq!(
            ret,
            Some("claude-opus-4-7[1m]".to_string()),
            "genuine slug change must return Some(new_slug)"
        );
        // context_usage must be None (nulled by the slug-change block so stale
        // denominator cannot be used for a broadcast).
        assert!(
            bridge.context_usage.lock().expect("lock").is_none(),
            "context_usage must be None after slug change (no broadcast on \
             slug-change message itself)"
        );
        // seed_max_tokens must be None (cleared; caller re-populates from cache).
        assert_eq!(
            *bridge.seed_max_tokens.lock().expect("lock"),
            None,
            "seed_max_tokens must be None after slug change (caller re-populates \
             from cache lookup)"
        );
    }

    // -----------------------------------------------------------------------
    // broadcast-suppression-test
    // -----------------------------------------------------------------------

    /// Call update_context_from_assistant twice with identical token counts.
    /// Only one ContextUsage broadcast should be emitted (the second call sees
    /// no change and suppresses the broadcast).
    #[tokio::test]
    async fn duplicate_context_update_suppresses_second_broadcast() {
        use brenn_cc::protocol::incoming::{AssistantContent, AssistantMessage, Usage};
        let (tx, _rx) = broadcast::channel(64);
        let db = brenn_lib::db::init_db_memory();
        let active_bridges = ActiveBridges::new();
        let (uid, conv_id) = {
            let conn = db.lock().await;
            let uid = brenn_lib::auth::user::create_user(&conn, "suppress-test", "$argon2id$fake");
            let cid = conversation::create_conversation(&conn, uid, "test", false);
            (uid, cid)
        };
        let config = brenn_lib::config::CompactionConfig {
            reminder_pct: 60,
            soft_pct: 75,
            red_pct: 80,
            hard_pct: 95,
            reminder_tokens: None,
            soft_tokens: None,
            red_tokens: None,
            hard_tokens: None,
            idle_duration: Duration::from_secs(300),
        };
        let bridge = crate::active_bridge::ActiveBridge::inject_for_test_full(
            uid,
            conv_id,
            "test",
            db,
            tx,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                active_bridges: Some(active_bridges),
                singleton: true,
                compaction_config: Some(config),
                ..Default::default()
            },
        );
        *bridge.seed_max_tokens.lock().expect("lock") = Some(200_000);

        let mut broadcast_rx = bridge.subscribe();

        let make_msg = || AssistantMessage {
            message: AssistantContent {
                role: "assistant".into(),
                content: vec![],
                model: Some("claude-sonnet-4-6".into()),
                usage: Some(Usage {
                    input_tokens: Some(10_000),
                    output_tokens: None,
                    cache_read_input_tokens: Some(40_000),
                    cache_creation_input_tokens: None,
                    extra: serde_json::Value::Null,
                }),
            },
            uuid: "u".into(),
            parent_tool_use_id: None,
        };
        let (ad, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();

        // First call: produces a broadcast.
        update_context_from_assistant(&bridge, &make_msg(), &ad);
        // Second call with identical token counts: must not produce a broadcast.
        update_context_from_assistant(&bridge, &make_msg(), &ad);

        let msgs = drain_broadcast(&mut broadcast_rx);
        let context_usage_count = msgs
            .iter()
            .filter(|m| matches!(m, WsServerMessage::ContextUsage { .. }))
            .count();
        assert_eq!(
            context_usage_count, 1,
            "identical token counts should emit exactly one ContextUsage broadcast, got {context_usage_count}"
        );
    }

    // -----------------------------------------------------------------------
    // Deferred-broadcast tests (no max_tokens available)
    // -----------------------------------------------------------------------

    /// When seed_max_tokens is None and context_usage is None, no broadcast
    /// should be emitted and context_usage must remain None.
    #[tokio::test]
    async fn update_context_from_assistant_defers_when_no_seed() {
        use brenn_cc::protocol::incoming::{AssistantContent, AssistantMessage, Usage};
        let (tx, _rx) = broadcast::channel(64);
        let db = brenn_lib::db::init_db_memory();
        let active_bridges = ActiveBridges::new();
        let (uid, conv_id) = {
            let conn = db.lock().await;
            let uid = brenn_lib::auth::user::create_user(&conn, "defer-test", "$argon2id$fake");
            let cid = conversation::create_conversation(&conn, uid, "test", false);
            (uid, cid)
        };
        let config = brenn_lib::config::CompactionConfig {
            reminder_pct: 60,
            soft_pct: 75,
            red_pct: 80,
            hard_pct: 95,
            reminder_tokens: None,
            soft_tokens: None,
            red_tokens: None,
            hard_tokens: None,
            idle_duration: Duration::from_secs(300),
        };
        let bridge = crate::active_bridge::ActiveBridge::inject_for_test_full(
            uid,
            conv_id,
            "test",
            db,
            tx,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                active_bridges: Some(active_bridges),
                singleton: true,
                compaction_config: Some(config),
                ..Default::default()
            },
        );
        // seed_max_tokens stays None (bridge default).
        assert_eq!(*bridge.seed_max_tokens.lock().expect("lock"), None);

        let mut broadcast_rx = bridge.subscribe();
        let (ad, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();

        let msg = AssistantMessage {
            message: AssistantContent {
                role: "assistant".into(),
                content: vec![],
                model: Some("claude-sonnet-4-6".into()),
                usage: Some(Usage {
                    input_tokens: Some(10_000),
                    output_tokens: None,
                    cache_read_input_tokens: None,
                    cache_creation_input_tokens: None,
                    extra: serde_json::Value::Null,
                }),
            },
            uuid: "u".into(),
            parent_tool_use_id: None,
        };
        update_context_from_assistant(&bridge, &msg, &ad);

        // context_usage must remain None — no denominator available.
        assert!(
            bridge.context_usage.lock().expect("lock").is_none(),
            "context_usage must remain None when no max_tokens is available"
        );
        // No ContextUsage broadcast must have been emitted.
        let msgs = drain_broadcast(&mut broadcast_rx);
        let context_count = msgs
            .iter()
            .filter(|m| matches!(m, WsServerMessage::ContextUsage { .. }))
            .count();
        assert_eq!(
            context_count, 0,
            "no ContextUsage broadcast should fire when seed_max_tokens is None"
        );
    }

    /// When `context_usage` already holds a valid `ContextUsage` (from a prior
    /// message) and `seed_max_tokens` is `None`, `max` resolves via
    /// `slot.as_ref().map(|u| u.max_tokens)` and a broadcast must still fire.
    /// Ensures the `or(seed)` chain does not silently defer when the existing
    /// `context_usage` already carries the denominator.
    #[tokio::test]
    async fn context_from_assistant_uses_existing_context_usage_when_seed_is_none() {
        use brenn_cc::protocol::incoming::{AssistantContent, AssistantMessage, Usage};
        let (tx, _rx) = broadcast::channel(64);
        let db = brenn_lib::db::init_db_memory();
        let active_bridges = ActiveBridges::new();
        let (uid, conv_id) = {
            let conn = db.lock().await;
            let uid = brenn_lib::auth::user::create_user(&conn, "ctx-seed-none", "$argon2id$fake");
            let cid = conversation::create_conversation(&conn, uid, "test", false);
            (uid, cid)
        };
        let config = brenn_lib::config::CompactionConfig {
            reminder_pct: 60,
            soft_pct: 75,
            red_pct: 80,
            hard_pct: 95,
            reminder_tokens: None,
            soft_tokens: None,
            red_tokens: None,
            hard_tokens: None,
            idle_duration: Duration::from_secs(300),
        };
        let bridge = crate::active_bridge::ActiveBridge::inject_for_test_full(
            uid,
            conv_id,
            "test",
            db,
            tx,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                active_bridges: Some(active_bridges),
                singleton: true,
                compaction_config: Some(config),
                ..Default::default()
            },
        );
        // seed_max_tokens stays None; context_usage carries a prior denominator.
        assert_eq!(*bridge.seed_max_tokens.lock().expect("lock"), None);
        *bridge.context_usage.lock().expect("lock") = Some(ContextUsage {
            current_tokens: 10_000,
            max_tokens: 200_000,
            usage_pct: 5,
            checked_at: Instant::now(),
        });

        let mut broadcast_rx = bridge.subscribe();
        let (ad, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();

        let msg = AssistantMessage {
            message: AssistantContent {
                role: "assistant".into(),
                content: vec![],
                model: Some("claude-sonnet-4-6".into()),
                usage: Some(Usage {
                    input_tokens: Some(20_000),
                    output_tokens: None,
                    cache_read_input_tokens: None,
                    cache_creation_input_tokens: None,
                    extra: serde_json::Value::Null,
                }),
            },
            uuid: "u".into(),
            parent_tool_use_id: None,
        };
        update_context_from_assistant(&bridge, &msg, &ad);

        // A broadcast must fire — denominator came from existing context_usage.
        let msgs = drain_broadcast(&mut broadcast_rx);
        let context_count = msgs
            .iter()
            .filter(|m| matches!(m, WsServerMessage::ContextUsage { .. }))
            .count();
        assert_eq!(
            context_count, 1,
            "ContextUsage broadcast must fire when context_usage carries a valid \
             max_tokens even if seed_max_tokens is None"
        );
        // The broadcast must use the existing max_tokens denominator.
        let cu = bridge.context_usage.lock().expect("lock");
        assert_eq!(
            cu.as_ref().unwrap().max_tokens,
            200_000,
            "max_tokens must be preserved from existing context_usage when seed is None"
        );
    }

    /// Part 1b of slug_change_returns_new_slug_and_nulls_state: initial
    /// assignment with usage present. The seed must not be disturbed and a
    /// broadcast must fire (using the pre-seeded denominator).
    #[tokio::test]
    async fn initial_slug_assignment_with_usage_broadcasts_and_preserves_seed() {
        use brenn_cc::protocol::incoming::{AssistantContent, AssistantMessage, Usage};
        let (tx, _rx) = broadcast::channel(64);
        let db = brenn_lib::db::init_db_memory();
        let active_bridges = ActiveBridges::new();
        let (uid, conv_id) = {
            let conn = db.lock().await;
            let uid =
                brenn_lib::auth::user::create_user(&conn, "init-assign-usage", "$argon2id$fake");
            let cid = conversation::create_conversation(&conn, uid, "test", false);
            (uid, cid)
        };
        let config = brenn_lib::config::CompactionConfig {
            reminder_pct: 60,
            soft_pct: 75,
            red_pct: 80,
            hard_pct: 95,
            reminder_tokens: None,
            soft_tokens: None,
            red_tokens: None,
            hard_tokens: None,
            idle_duration: Duration::from_secs(300),
        };
        let bridge = crate::active_bridge::ActiveBridge::inject_for_test_full(
            uid,
            conv_id,
            "test",
            db,
            tx,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                active_bridges: Some(active_bridges),
                singleton: true,
                compaction_config: Some(config),
                ..Default::default()
            },
        );
        let (ad, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();

        // Initial state: no active_model_slug yet; seed pre-populated from cache.
        *bridge.active_model_slug.lock().expect("lock") = None;
        *bridge.seed_max_tokens.lock().expect("lock") = Some(200_000);

        let mut broadcast_rx = bridge.subscribe();

        let msg = AssistantMessage {
            message: AssistantContent {
                role: "assistant".into(),
                content: vec![],
                model: Some("claude-sonnet-4-6".into()),
                usage: Some(Usage {
                    input_tokens: Some(10_000),
                    output_tokens: None,
                    cache_read_input_tokens: None,
                    cache_creation_input_tokens: None,
                    extra: serde_json::Value::Null,
                }),
            },
            uuid: "u-init-usage".into(),
            parent_tool_use_id: None,
        };
        let ret = update_context_from_assistant(&bridge, &msg, &ad);

        // Initial assignment: return value must be None (no signal).
        assert_eq!(ret, None, "initial assignment must return None");
        // seed_max_tokens must be unchanged.
        assert_eq!(
            *bridge.seed_max_tokens.lock().expect("lock"),
            Some(200_000),
            "seed_max_tokens must not be modified on initial slug assignment"
        );
        // A ContextUsage broadcast must have been emitted using the seeded denominator.
        let msgs = drain_broadcast(&mut broadcast_rx);
        let context_count = msgs
            .iter()
            .filter(|m| matches!(m, WsServerMessage::ContextUsage { .. }))
            .count();
        assert_eq!(
            context_count, 1,
            "ContextUsage broadcast must fire on initial slug assignment with usage present"
        );
    }
}
