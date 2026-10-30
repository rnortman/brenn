//! A providing consumer's failed activation answers the provided-tool calls it
//! was handed.
//!
//! The calls are real `ProvidedTool::execute` futures publishing through the
//! executor's system policy onto the consumer's request inbox, so the ids the
//! dispatcher fails are the ones the host minted and the answers are the ones
//! their callers receive. The `processor-multiport` fixture stands in for a
//! provider: it reads every port and fails on a sentinel on any of them.

use super::*;

use std::collections::HashMap;

use brenn_lib::messaging::config::{MessagingGlobalConfig, ProvidedToolDecl};
use brenn_lib::messaging::{SubscriberEntry, SubscriberEntryKind};
use brenn_lib::tools::ResolvedToolGrant;
use brenn_tool_registry::bus_wiring::{request_inbox_name, tool_executor_system_policy};
use brenn_tool_registry::{
    AsyncTool, PROVIDED_TOOL_CEILING, ProvidedTool, TOOL_EXECUTOR_COMPONENT, ToolCtx, ToolError,
};
use serde_json::{Value, json};

/// A provider whose request inbox is its `tool-requests` port and whose
/// `kick` port carries the fixture's failure sentinels.
struct Provider {
    messenger: Arc<brenn_messaging::Messenger>,
    inbox: ChannelEntry,
    kick: ChannelEntry,
    sub: ParticipantId,
    runtime: Arc<ProviderRuntime>,
    tool: Arc<ProvidedTool>,
    cfg: WasmConsumerConfig,
    _store: tempfile::NamedTempFile,
}

async fn provider(slug: &str) -> Provider {
    let sub = ParticipantId::for_wasm(slug);
    let inbox = testutils::test_channel_entry(
        &request_inbox_name(slug),
        vec![SubscriberEntry {
            kind: SubscriberEntryKind::Wasm(slug.to_string()),
            push_depth: Depth::Bounded(4),
            retain_depth: Depth::Bounded(4),
            noise: NoiseLevel::Silent,
            wake_min: None,
        }],
    );
    let kick = (*testutils::wasm_channel_entry(slug, "kick", Depth::Bounded(4), Depth::Bounded(4)))
        .clone();
    // The fixture publishes one summary on `out` per activation it completes.
    let out = testutils::test_channel_entry(&format!("{slug}:out"), vec![]);
    let entries = vec![inbox.clone(), kick.clone(), out.clone()];
    let db = init_db_memory_lib_slice();
    {
        let conn = db.lock().await;
        upsert_channels(&conn, &entries);
    }
    let messenger = brenn_messaging::Messenger::new(
        db,
        Arc::new(MessagingDirectory::with_entries(entries.clone())),
        Arc::from("test"),
        Arc::new(IndexMap::new()),
        Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(testutils::wasm_registrations(wasm_policies_from_entries(
        &entries,
    )))
    .with_subscriber_registrations(testutils::system_registrations(HashMap::from([(
        TOOL_EXECUTOR_COMPONENT.to_string(),
        tool_executor_system_policy(),
    )])));
    attach_input_ports(
        &messenger,
        slug,
        &sub,
        &[(&inbox, Depth::Bounded(4)), (&kick, Depth::Bounded(4))],
    )
    .await;

    let (alert_dispatcher, _alerts) = noop_alert_dispatcher();
    let runtime = ProviderRuntime::new(alert_dispatcher.clone());
    runtime.attach(messenger.clone());
    let tool = Arc::new(ProvidedTool::new(
        slug,
        &ProvidedToolDecl {
            name: "echo-for-user".to_string(),
            mcp_name: "EchoForUser".to_string(),
            description: "Echo, for a user.".to_string(),
            input_schema: json!({ "type": "object" }),
            max_concurrency: 4,
            auto_approve: false,
        },
        runtime.clone(),
    ));

    let store = tempfile::NamedTempFile::new().unwrap();
    let output_ports = HashMap::from([("out".to_string(), test_out_spec(out.address.clone()))]);
    let component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
        declared_out_ports: output_ports.keys().cloned().collect(),
        output_ports,
        input_amplification_mt: HashMap::from([
            (TOOL_REQUEST_INPUT_PORT.to_string(), 1000),
            ("kick".to_string(), 1000),
        ]),
        grants: [ComponentGrant::Ports].into_iter().collect(),
        alerter: noop_proc_alerter(),
        ..ProcessorLoadSpec::minimal(std::path::Path::new(MULTIPORT_WASM), slug)
    }));
    let port = |port: &str, entry: &ChannelEntry| WasmInputPort {
        port: port.to_string(),
        sub: ResolvedSubscription {
            channel_uuid: entry.uuid,
            channel_address: entry.address.clone(),
            push_depth: Depth::Bounded(4),
            retain_depth: Depth::Bounded(4),
            noise: NoiseLevel::Silent,
            wake_min: WakeMin::Normal,
        },
        amplification_mt: 1000,
    };
    let cfg = WasmConsumerConfig {
        slug: slug.to_string(),
        component,
        notify: Arc::new(Notify::new()),
        messenger: messenger.clone(),
        alert_dispatcher,
        inputs: vec![port(TOOL_REQUEST_INPUT_PORT, &inbox), port("kick", &kick)],
        outputs: vec![],
        sync_ports: std::collections::BTreeSet::new(),
        activation_pacing: unthrottled_pacing(),
        provider: Some(runtime.clone()),
    };
    Provider {
        messenger,
        inbox,
        kick,
        sub,
        runtime,
        tool,
        cfg,
        _store: store,
    }
}

impl Provider {
    /// Start a call to the provided tool with a full ceiling ahead of it.
    fn call(&self) -> tokio::task::JoinHandle<Result<Value, ToolError>> {
        let tool = self.tool.clone();
        tokio::spawn(async move {
            let ctx = ToolCtx {
                caller: ParticipantId::for_app("caller", "test-origin"),
                grant: ResolvedToolGrant {
                    acl: vec![],
                    rate_limit: None,
                },
                acting_conversation_id: None,
                acting_user: None,
                deadline: Some(tokio::time::Instant::now() + PROVIDED_TOOL_CEILING),
            };
            tool.execute(&ctx, json!({})).await
        })
    }

    /// The ids of the requests published to the inbox, once there are `n`.
    async fn published_ids(&self, n: usize) -> Vec<String> {
        for _ in 0..1000 {
            let ids: Vec<String> = testutils::owed_everywhere(&self.messenger, &self.sub)
                .await
                .iter()
                .map(|(_, env)| {
                    let body: Value = serde_json::from_str(&env.body).expect("request is JSON");
                    body["id"].as_str().expect("request has an id").to_string()
                })
                .collect();
            if ids.len() == n {
                return ids;
            }
            tokio::task::yield_now().await;
        }
        panic!("{n} requests were not published");
    }

    /// Run one delivery whose `kick` port carries `sentinel`.
    async fn fail_with(&self, sentinel: &str) {
        testutils::insert_bus_message(&self.messenger, &self.kick, sentinel, ChannelScheme::Brenn)
            .await;
        drain_step(&self.cfg, &self.sub, MountDebt::Settled).await;
    }
}

/// What the trap diagnostic says when the fixture traps on its sentinel.
const TRAP_TEXT: &str = "wasm `unreachable` instruction executed";

/// The internal detail a call was answered with.
fn internal_detail(outcome: Result<Value, ToolError>) -> String {
    match outcome {
        Err(ToolError::Internal(detail)) => detail,
        other => panic!("expected an internal answer, got {other:?}"),
    }
}

/// An activation that errs having been handed two live requests answers both
/// at once, naming the diagnostic; a request it was handed that the provider
/// had already completed keeps its answer.
#[tokio::test]
async fn an_erring_activation_answers_the_requests_it_was_handed() {
    let p = provider("prov-err").await;
    let calls = [p.call(), p.call(), p.call()];
    let ids = p.published_ids(3).await;
    let answered = ids[0].clone();
    p.runtime
        .complete("prov-err", &answered, Ok(json!("answered")))
        .expect("the request is pending");

    p.fail_with("__err__").await;
    // The call tasks have not run since the failure, so their guards still
    // hold: only the failure can have removed these records.
    for id in &ids[1..] {
        assert_eq!(
            p.runtime.acting_user_for("prov-err", id),
            None,
            "no failed request's record outlives the failure"
        );
    }

    let mut answers = Vec::new();
    for call in calls {
        answers.push(
            tokio::time::timeout(std::time::Duration::from_secs(10), call)
                .await
                .expect("the failed activation answers without waiting for the ceiling")
                .expect("the call task completes"),
        );
    }
    assert_eq!(
        answers
            .iter()
            .filter(|a| **a == Ok(json!("answered")))
            .count(),
        1,
        "the completed request keeps its answer: {answers:?}"
    );
    let failed: Vec<String> = answers
        .into_iter()
        .filter(|a| a.is_err())
        .map(internal_detail)
        .collect();
    assert_eq!(
        failed.len(),
        2,
        "both live requests are answered: {failed:?}"
    );
    for detail in failed {
        assert!(
            detail.starts_with("provider activation failed: ")
                && detail.contains("deliberate err on __err__ sentinel"),
            "the answer names the diagnostic: {detail:?}"
        );
    }
}

/// The trap arm is a separate disposition branch and fails its requests too.
#[tokio::test]
async fn a_trapping_activation_answers_the_requests_it_was_handed() {
    let p = provider("prov-trap").await;
    let call = p.call();
    let ids = p.published_ids(1).await;

    p.fail_with("__trap__").await;
    // The call task has not run since the failure, so its guard still holds:
    // only the failure can have removed the record.
    assert_eq!(
        p.runtime.acting_user_for("prov-trap", &ids[0]),
        None,
        "no failed request's record outlives the failure"
    );

    let outcome = tokio::time::timeout(std::time::Duration::from_secs(10), call)
        .await
        .expect("the trapped activation answers without waiting for the ceiling")
        .expect("the call task completes");
    let detail = internal_detail(outcome);
    assert!(
        detail.starts_with("provider activation failed: ") && detail.contains(TRAP_TEXT),
        "the answer names the trap: {detail:?}"
    );
}

/// The host builds every body on a request inbox, so a failed activation
/// handed one that is not a provided-tool request has found a host bug.
#[tokio::test]
#[should_panic(expected = "is not a provided-tool request")]
async fn a_failed_activation_handed_a_body_that_is_not_a_request_panics() {
    let p = provider("prov-garbled").await;
    testutils::insert_bus_message(
        &p.messenger,
        &p.inbox,
        "not a request",
        ChannelScheme::Brenn,
    )
    .await;

    p.fail_with("__err__").await;
}

/// A request the provider was handed as new by an activation that succeeded is
/// not failed by a later activation that fails, where it is only context: the
/// provider may still answer it.
#[tokio::test]
async fn a_failed_activation_leaves_requests_it_saw_only_as_context() {
    let p = provider("prov-context").await;
    let call = p.call();
    let ids = p.published_ids(1).await;
    // A delivery the fixture serves without failing; it hands the request as
    // new and publishes nothing the test reads.
    drain_step(&p.cfg, &p.sub, MountDebt::Settled).await;

    p.fail_with("__err__").await;

    assert_eq!(
        p.runtime.acting_user_for("prov-context", &ids[0]),
        Some(None),
        "the request is still pending for its provider"
    );
    p.runtime
        .complete("prov-context", &ids[0], Ok(json!("late")))
        .expect("the provider can still answer it");
    assert_eq!(call.await.unwrap(), Ok(json!("late")));
}
