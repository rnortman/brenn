//! What the event loop reports to the agent-status reporter: sessions starting
//! and ending, turns, and refusals.

use super::super::super::test_support::{
    await_fence, drain_broadcast, event_fence, make_bridge_no_loop, recv_broadcast,
};
use super::super::*;

use brenn_cc::protocol::incoming::{RateLimitEventMessage, ResultMessage};
use brenn_cc::session::SessionInfo;
use brenn_envelope::agent::{AgentSessionEnded, bucket, decode};
use brenn_messaging::agent_status::{ACCOUNTS_ADDRESS, ENDED_ADDRESS, SESSIONS_ADDRESS};
use brenn_obs::alerting::{AlertDispatcher, CountingAlerter, RateLimiter, noop_alert_dispatcher};
use brenn_ws_types::{CcState, PermissionModeValue};
use std::sync::Arc;
use tokio::sync::broadcast;

use crate::active_bridge::test_fixtures::TestBridgeConfig;
use crate::agent_status::tests::{Recording, accounts_of, profiles, sessions_of};
use crate::agent_status::{AgentStatusReporter, Publishing};

struct Rig {
    bridge: Arc<ActiveBridge>,
    event_tx: mpsc::Sender<SessionEvent>,
    broadcast_rx: broadcast::Receiver<WsServerMessage>,
    reporter: AgentStatusReporter,
    sink: Arc<Recording>,
}

impl Rig {
    /// Wait for the reporter, then take what it published.
    async fn published(&self) -> Vec<(&'static str, String)> {
        self.reporter.flush().await;
        self.sink.take()
    }

    async fn send(&self, event: SessionEvent) {
        let fence = event_fence(&self.bridge);
        self.event_tx.send(event).await.unwrap();
        await_fence(fence).await;
    }
}

/// A bridge running under `profile` whose reporter publishes into a
/// recording, with the reporter's boot publish taken off it.
async fn rig(profile: Option<&str>, alert_dispatcher: AlertDispatcher) -> Rig {
    let sink = Arc::new(Recording::default());
    let reporter = AgentStatusReporter::start(
        &profiles(),
        Some(Publishing {
            sink: sink.clone(),
            retained_accounts: None,
        }),
    );
    reporter.flush().await;
    sink.take();
    let (bridge, event_tx, event_rx, broadcast_rx, alert_dispatcher, _active_bridges) =
        make_bridge_no_loop(
            "test",
            alert_dispatcher,
            TestBridgeConfig {
                agent_status: Some(reporter.clone()),
                ..Default::default()
            },
        )
        .await;
    *bridge.cc_profile.lock().expect("cc_profile lock") = profile.map(str::to_string);
    tokio::spawn(cc_event_loop(event_rx, bridge.clone(), alert_dispatcher));
    Rig {
        bridge,
        event_tx,
        broadcast_rx,
        reporter,
        sink,
    }
}

async fn quiet_rig(profile: Option<&str>) -> Rig {
    rig(profile, noop_alert_dispatcher().0).await
}

fn rate_limit(info: serde_json::Value) -> SessionEvent {
    SessionEvent::RateLimit(RateLimitEventMessage {
        uuid: Some("rl-1".into()),
        session_id: None,
        rate_limit_info: Some(info),
        extra: serde_json::Value::Object(Default::default()),
    })
}

/// A `result` frame. `cost` is what it states in
/// `model_usage_since_last_result` (`None` leaves the field absent);
/// `total_cost_usd` is CC's running total, which is never booked.
fn result(
    total_cost_usd: Option<f64>,
    cost: Option<f64>,
    is_error: bool,
    text: Option<&str>,
) -> SessionEvent {
    SessionEvent::TurnCompleted(Box::new(ResultMessage {
        subtype: Some(if is_error { "error" } else { "success" }.into()),
        duration_ms: Some(10),
        duration_api_ms: Some(10),
        is_error: Some(is_error),
        num_turns: Some(1),
        session_id: Some("sess-1".into()),
        total_cost_usd,
        model_usage_since_last_result: cost
            .map(crate::active_bridge::test_support::stated_cost_map),
        usage: None,
        result: text.map(str::to_string),
        stop_reason: None,
        model_usage: None,
        origin: None,
        extra: serde_json::Value::Object(Default::default()),
    }))
}

fn initialized() -> SessionEvent {
    SessionEvent::Initialized(SessionInfo {
        session_id: "cc-sess-1".into(),
        tools: vec!["Read".into()],
        model: "claude-opus-4".into(),
        cwd: "/home/user/project".into(),
        claude_code_version: Some("2.1.123".into()),
        mcp_servers: Vec::new(),
        permission_mode: Some(PermissionModeValue::Auto),
    })
}

fn key_record(published: &[(&'static str, String)]) -> Option<brenn_envelope::agent::AgentAccount> {
    published
        .iter()
        .rev()
        .find(|(address, _)| *address == ACCOUNTS_ADDRESS)
        .map(|(_, body)| {
            accounts_of(body)
                .into_iter()
                .find(|a| a.account == "work-key")
                .expect("the key's record")
        })
}

#[tokio::test]
async fn a_rejection_under_a_subscription_flags_its_bucket() {
    let mut rig = quiet_rig(Some("personal")).await;
    rig.send(rate_limit(serde_json::json!({
        "status": "rejected",
        "rateLimitType": "five_hour",
        "resetsAt": 4_000_000_000_u64,
    })))
    .await;

    match recv_broadcast(&mut rig.broadcast_rx).await {
        WsServerMessage::Error { message } => assert!(message.contains("Rate limited")),
        other => panic!("expected Error, got {other:?}"),
    }
    let published = rig.published().await;
    assert_eq!(published.len(), 1, "{published:?}");
    assert_eq!(published[0].0, ACCOUNTS_ADDRESS);
    let personal = accounts_of(&published[0].1)
        .into_iter()
        .find(|a| a.account == "personal")
        .expect("the subscription's record");
    let flag = &personal.exhausted_buckets[bucket::FIVE_HOUR];
    assert_eq!(flag.until_ms, Some(4_000_000_000_000));
    assert_eq!(flag.kind.as_deref(), Some("five_hour"));
    assert_eq!(
        flag.session.as_deref(),
        Some(rig.bridge.conversation_id.to_string().as_str())
    );
}

#[tokio::test]
async fn a_rejection_under_a_key_or_no_profile_flags_nothing() {
    for profile in [Some("work-key"), None] {
        let mut rig = quiet_rig(profile).await;
        rig.send(rate_limit(serde_json::json!({"status": "rejected"})))
            .await;
        assert!(matches!(
            recv_broadcast(&mut rig.broadcast_rx).await,
            WsServerMessage::Error { .. }
        ));
        assert!(rig.published().await.is_empty(), "{profile:?}");
    }
}

#[tokio::test]
async fn allowed_warning_tells_the_user_nothing() {
    let mut rig = quiet_rig(Some("personal")).await;
    rig.send(rate_limit(serde_json::json!({
        "status": "allowed_warning",
        "utilization": 0.9,
        "rateLimitType": "five_hour",
        "resetsAt": 4_000_000_000_u64,
    })))
    .await;
    assert!(drain_broadcast(&mut rig.broadcast_rx).is_empty());
    assert!(rig.published().await.is_empty());
}

#[tokio::test]
async fn an_unknown_status_is_drift_and_nothing_else() {
    let count = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let (dispatcher, _h) =
        AlertDispatcher::new(CountingAlerter(count.clone()), RateLimiter::new(10, 60));
    let mut rig = rig(Some("personal"), dispatcher).await;
    rig.send(rate_limit(
        serde_json::json!({"status": "agent-status-test-unknown"}),
    ))
    .await;
    assert!(drain_broadcast(&mut rig.broadcast_rx).is_empty());
    assert!(rig.published().await.is_empty());
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_costed_turn_under_a_key_charges_it() {
    let rig = quiet_rig(Some("work-key")).await;
    rig.send(result(Some(0.4), Some(0.4), false, Some("done")))
        .await;
    let key = key_record(&rig.published().await).expect("the turn published accounts");
    assert_eq!(key.usd, 0.4);
    assert_eq!(key.turns, Some(1));
    assert_eq!(key.exhausted, None);
}

/// The key is charged what each result states, whatever CC's running
/// `total_cost_usd` does: here it rises, then falls below the last value as
/// after a respawn that restored a stale total.
#[tokio::test]
async fn a_key_is_charged_what_each_result_states() {
    let rig = quiet_rig(Some("work-key")).await;
    rig.send(result(Some(5.4), Some(0.4), false, Some("one")))
        .await;
    rig.send(result(Some(5.7), Some(0.3), false, Some("two")))
        .await;
    let key = key_record(&rig.published().await).expect("the turns published accounts");
    assert!((key.usd - 0.7).abs() < 1e-9, "charged {}", key.usd);
    assert_eq!(key.turns, Some(2));

    rig.send(result(Some(3.1), Some(0.1), false, Some("after a respawn")))
        .await;
    let key = key_record(&rig.published().await).expect("the turn published accounts");
    assert!((key.usd - 0.8).abs() < 1e-9, "charged {}", key.usd);
    assert_eq!(key.turns, Some(3));
}

#[tokio::test]
async fn a_result_stating_a_cost_without_a_running_total_is_a_turn() {
    let rig = quiet_rig(Some("work-key")).await;
    rig.send(result(None, Some(0.4), false, Some("done"))).await;
    let key = key_record(&rig.published().await).expect("the turn published accounts");
    assert!((key.usd - 0.4).abs() < 1e-9, "charged {}", key.usd);
    assert_eq!(key.turns, Some(1));
}

#[tokio::test]
async fn a_frame_without_cost_that_is_not_a_refusal_is_not_a_turn() {
    let rig = quiet_rig(Some("work-key")).await;
    rig.send(initialized()).await;
    rig.published().await;
    rig.send(result(None, None, false, Some("compacted"))).await;
    rig.send(result(None, None, true, Some("overloaded"))).await;
    let published = rig.published().await;
    assert!(
        published
            .iter()
            .all(|(address, _)| *address != ACCOUNTS_ADDRESS),
        "{published:?}"
    );
}

#[tokio::test]
async fn a_refusal_without_cost_flags_the_key() {
    let rig = quiet_rig(Some("work-key")).await;
    rig.send(result(
        None,
        None,
        true,
        Some("API Error: Credit balance is too low"),
    ))
    .await;
    let key = key_record(&rig.published().await).expect("the refusal published accounts");
    assert_eq!(key.turns, Some(1));
    assert_eq!(key.usd, 0.0);
    let flag = key.exhausted.expect("the key is flagged");
    assert_eq!(
        flag.message.as_deref(),
        Some("API Error: Credit balance is too low")
    );
    assert_eq!(flag.until_ms, None);
}

/// A refused turn states `{}`: a measured zero, still one turn, still a flag.
#[tokio::test]
async fn a_refusal_stating_an_empty_cost_flags_the_key_and_charges_nothing() {
    let rig = quiet_rig(Some("work-key")).await;
    let SessionEvent::TurnCompleted(mut refused) = result(
        Some(0.0),
        None,
        true,
        Some("API Error: Credit balance is too low"),
    ) else {
        unreachable!()
    };
    refused.model_usage_since_last_result = Some(std::collections::BTreeMap::new());
    rig.send(SessionEvent::TurnCompleted(refused)).await;
    let key = key_record(&rig.published().await).expect("the refusal published accounts");
    assert_eq!(key.turns, Some(1));
    assert_eq!(key.usd, 0.0);
    assert!(key.exhausted.is_some(), "the key is flagged");
}

#[tokio::test]
async fn a_marker_on_a_subscription_flags_nothing() {
    let rig = quiet_rig(Some("personal")).await;
    rig.send(result(None, None, true, Some("credit balance is too low")))
        .await;
    assert!(rig.published().await.is_empty());
}

#[tokio::test]
async fn initialized_starts_a_session_and_an_unexpected_death_ends_it_not_ok() {
    let rig = quiet_rig(Some("personal")).await;
    rig.send(initialized()).await;
    let started = rig.published().await;
    let (address, body) = started.last().expect("a sessions snapshot");
    assert_eq!(*address, SESSIONS_ADDRESS);
    let session = &sessions_of(body)[0];
    assert_eq!(session.conversation_id, rig.bridge.conversation_id);
    assert_eq!(session.app, "test");
    assert_eq!(session.cc_session_id.as_deref(), Some("cc-sess-1"));
    assert_eq!(session.account.as_deref(), Some("personal"));

    rig.send(SessionEvent::Died(brenn_cc::error::CcError::SendFailed))
        .await;
    let published = rig.published().await;
    let (address, body) = published.last().expect("an ended fact");
    assert_eq!(*address, ENDED_ADDRESS);
    let ended: AgentSessionEnded = decode(body).expect("an ended body");
    assert!(!ended.ok);
    assert_eq!(
        ended.session.state,
        brenn_envelope::chat::CcState::Error,
        "the ended run carries the Error status the reset broadcast"
    );
    assert_eq!(
        published
            .iter()
            .filter(|(a, _)| *a == ENDED_ADDRESS)
            .count(),
        1
    );
}

#[tokio::test]
async fn a_drain_ends_the_session_ok() {
    let rig = quiet_rig(None).await;
    rig.send(initialized()).await;
    rig.bridge
        .drain_on_idle
        .store(true, std::sync::atomic::Ordering::SeqCst);
    rig.send(SessionEvent::Died(brenn_cc::error::CcError::ProcessDied {
        exit_status: None,
    }))
    .await;
    let published = rig.published().await;
    let (address, body) = published.last().expect("an ended fact");
    assert_eq!(*address, ENDED_ADDRESS);
    let ended: AgentSessionEnded = decode(body).expect("an ended body");
    assert!(ended.ok);
    assert_eq!(ended.session.account, None);
}

#[tokio::test]
async fn a_status_broadcast_reaches_the_session() {
    let rig = quiet_rig(None).await;
    rig.send(initialized()).await;
    rig.published().await;
    rig.bridge.broadcast(WsServerMessage::Status {
        state: CcState::Thinking,
    });
    let published = rig.published().await;
    assert_eq!(
        sessions_of(&published.last().expect("a snapshot").1)[0].state,
        brenn_envelope::chat::CcState::Thinking
    );
}
