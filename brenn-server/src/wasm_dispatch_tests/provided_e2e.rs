//! A tool a compiled component provides, end to end.
//!
//! The `processor-provider-test` fixture provides `echo-for-user`. A caller —
//! an app conversation through `invoke_for_llm`, or the `processor-tool-test`
//! component through `call-async` — reaches it through the one registry; the
//! request lands on the provider's `brenn:tool-requests/<slug>` inbox; the
//! guest serves it through a real `WasmProvidesHost` over the process's
//! `ProviderRuntime`, acting for the caller's user through `call-async-for`
//! where its mode says so; and the caller receives the provider's completion.
//! The guest and the executor are stepped by hand; the conversation's call
//! runs on a spawned task.

use super::*;

use std::collections::BTreeMap;
use std::path::Path;

use brenn_lib::messaging::config::ProvidedToolDecl;
use brenn_lib::tools::ResolvedToolGrant;
use brenn_messaging::MountDebt;
use brenn_oauth::testutil;
use brenn_tool_registry::testutil::{clause, grant};
use brenn_tool_registry::{ToolError, WasmProvidesHost, WasmToolHost};
use brenn_wasm::{ProvidesCallError, ProvidesHost, ToolCallError};
use serde_json::{Value, json};

use super::provider_rig::{CallerSetup, ProviderRig, ProviderSetup};

const PROVIDER_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../brenn-wasm/target/components/brenn_processor_provider_test.wasm"
);

/// The providing instance.
const PROVIDER: &str = "prov";
/// The component that calls the provided tool through `call-async`.
const CALLER: &str = "agenda";
/// The provided tool's MCP name, as an agent sees it.
const MCP_NAME: &str = "EchoForUser";

/// The fixture's one `provide` statement, as its spec declares it.
fn echo_for_user() -> ProvidedToolDecl {
    ProvidedToolDecl {
        name: "echo-for-user".to_string(),
        mcp_name: MCP_NAME.to_string(),
        description: "Test fixture: answer by the `mode` argument, for the caller's user."
            .to_string(),
        input_schema: json!({ "type": "object" }),
        max_concurrency: 2,
        auto_approve: true,
    }
}

/// The provider rig, with the OAuth stub its nested calls reach.
struct Rig {
    base: ProviderRig,
    stub: testutil::Stub,
    /// The stub's `host:port`, as an `api-request` names it.
    host: String,
    _vault: tempfile::TempDir,
}

impl std::ops::Deref for Rig {
    type Target = ProviderRig;

    fn deref(&self) -> &ProviderRig {
        &self.base
    }
}

/// Stand up the rig. `provider_acl` is the provider's grant on `api-request`,
/// none when empty; `caller_args` is what the calling component passes `echo-for-user`.
async fn rig(provider_acl: Vec<&[(&str, &str)]>, caller_args: Value) -> Rig {
    let (base, stub) = testutil::start_stub().await;
    stub.lock().unwrap().api_body = br#"{"items":[]}"#.to_vec();
    let host = base
        .strip_prefix("http://")
        .expect("the stub is plain http")
        .to_string();
    let (service, vault) = testutil::service(vec![testutil::provider_config("stub", &base)]);
    // Alice's credential, and the same account connected under bob: a call
    // acting for alice must send alice's.
    let mut alices = testutil::credential("stub", "alice@example.com", "RT-alice");
    alices.access_token = "AT-alice".to_string();
    service.vault().store("alice", &alices).await;
    let mut bobs = testutil::credential("stub", "alice@example.com", "RT-bob");
    bobs.access_token = "AT-bob".to_string();
    service.vault().store("bob", &bobs).await;

    // A provider whose test makes no nested call holds no grant for one.
    let grants: BTreeMap<String, ResolvedToolGrant> = if provider_acl.is_empty() {
        BTreeMap::new()
    } else {
        BTreeMap::from([(
            "api-request".to_string(),
            grant(provider_acl.into_iter().map(clause).collect()),
        )])
    };
    let base = ProviderRig::build(ProviderSetup {
        slug: PROVIDER,
        decls: vec![echo_for_user()],
        native: brenn_oauth::tools::registered(
            Some(&service),
            BTreeMap::from([testutil::key_provider_config("maps", &base)]),
        ),
        grants,
        component: ProcessorLoadSpec {
            grants: [ComponentGrant::Tools, ComponentGrant::Provides]
                .into_iter()
                .collect(),
            ..ProcessorLoadSpec::minimal(Path::new(PROVIDER_WASM), PROVIDER)
        },
        caller: Some(CallerSetup {
            slug: CALLER,
            tool: "echo-for-user",
            args: caller_args,
        }),
    })
    .await;

    Rig {
        base,
        stub,
        host,
        _vault: vault,
    }
}

impl Rig {
    /// A `provides` host over the same table for another instance, granted
    /// what the provider is granted.
    fn host_for(&self, slug: &str) -> WasmProvidesHost {
        WasmProvidesHost::new(
            self.runtime.clone(),
            Arc::new(WasmToolHost::new(
                self.registry.clone(),
                self.provider_grants.clone(),
                slug.to_string(),
                self.alert.clone(),
            )),
        )
    }
}

/// A conversation's call is served by the provider acting for the
/// conversation's user: the nested `api-request` sends alice's OAuth
/// credential, under a clause naming any user, and the API's answer is the
/// conversation's result.
#[tokio::test]
async fn an_app_call_acts_for_its_user_through_the_provider() {
    let r = rig(
        vec![&[
            ("provider", "stub"),
            ("user", "*"),
            ("account", "alice@example.com"),
            ("method", "GET"),
            ("host", "*"),
            ("path", "/calendar/v3/calendars/*/events"),
        ]],
        json!({}),
    )
    .await;
    let call = r.call_for_alice(
        MCP_NAME,
        json!({
            "mode": "api",
            "request": {
                "provider": "stub",
                "account": "alice@example.com",
                "method": "GET",
                "host": r.host,
                "path": "/calendar/v3/calendars/primary/events",
            },
        }),
    );
    let id = r.requested_id().await;

    // The provider's request activation issues the nested call for alice.
    r.serve().await;
    let nested = r.owed(&r.executor_sub(), 1).await;
    assert_eq!(nested[0]["tool"], "api-request");
    assert_eq!(nested[0]["serving"], id.as_str());
    assert!(nested[0].get("user").is_none() && nested[0].get("acting_user").is_none());

    // The executor runs it stamped with the pending call's user.
    r.executor.drain_step().await;
    {
        let seen = &r.stub.lock().unwrap().api_seen;
        assert_eq!(seen.len(), 1, "exactly one request reached the API");
        assert_eq!(
            seen[0].headers.get("authorization").unwrap(),
            "Bearer AT-alice"
        );
    }
    let results = r.owed(&r.provider_sub, 1).await;
    assert_eq!(results[0]["serving"], id.as_str());

    // The provider's result activation completes the conversation's call.
    r.serve().await;
    let answer = call.await.unwrap().expect("the provider answered");
    assert_eq!(answer["status"], 200, "{answer}");
    assert_eq!(answer["body"], r#"{"items":[]}"#);
    assert!(r.runtime.serving(PROVIDER, &id).is_none());
}

/// A stamped call writes: the nested `POST` to a key provider is admitted by
/// the provider's `method = "POST"` clause because it acts for alice, where
/// the same call with no acting user only reads.
#[tokio::test]
async fn a_stamped_nested_call_writes_with_a_key() {
    let r = rig(
        vec![&[
            ("provider", "maps"),
            ("method", "POST"),
            ("path", "/v1/places:search"),
        ]],
        json!({}),
    )
    .await;
    let call = r.call_for_alice(
        MCP_NAME,
        json!({
            "mode": "api",
            "request": {
                "provider": "maps",
                "method": "POST",
                "host": r.host,
                "path": "/v1/places:search",
                "body": "{}",
            },
        }),
    );
    r.requested_id().await;
    r.serve().await;
    r.executor.drain_step().await;
    r.serve().await;

    let answer = call.await.unwrap().expect("the provider answered");
    assert_eq!(answer["status"], 200, "{answer}");
    let seen = &r.stub.lock().unwrap().api_seen;
    assert_eq!(seen.len(), 1, "exactly one request reached the API");
    assert_eq!(seen[0].method, "POST");
    assert_eq!(seen[0].headers.get("x-api-key").unwrap(), testutil::KEY);
}

/// A component's `call-async` to the provided tool is answered on the
/// component's own result inbox, through the executor, and the answer carries
/// no `serving`: the component serves nobody.
#[tokio::test]
async fn a_components_call_is_answered_on_its_result_inbox() {
    let args = json!({ "mode": "echo", "n": 1 });
    let r = rig(vec![], args.clone()).await;
    let caller = r.caller();
    testutils::insert_bus_message(&r.messenger, &caller.trigger, "go", ChannelScheme::Brenn).await;
    drain_step(&caller.component, &caller.sub, MountDebt::Settled).await;

    // The executor admits the call and hands it to the provider without
    // waiting for the answer.
    r.executor.drain_step().await;
    r.requested_id().await;
    r.serve().await;

    let results = r.owed(&caller.sub, 1).await;
    assert_eq!(results[0]["tool"], "echo-for-user");
    assert_eq!(results[0]["call_id"], "call-1");
    assert_eq!(results[0]["outcome"]["ok"], args);
    assert!(
        results[0].get("serving").is_none(),
        "a plain call's result names no request it serves: {}",
        results[0]
    );
}

/// Completing an id the host never minted is `not-pending`, and leaves the
/// real request to be answered.
#[tokio::test]
async fn a_made_up_id_is_not_pending() {
    let r = rig(vec![], json!({})).await;
    let call = r.call_for_alice(MCP_NAME, json!({ "mode": "stale" }));
    r.requested_id().await;
    r.serve().await;
    let answer = call.await.unwrap().expect("the provider answered");
    let drew = answer["stale"]
        .as_str()
        .expect("the fixture names what it drew");
    assert!(drew.contains("not-pending"), "{drew}");
    assert_eq!(
        answer["dropped"], true,
        "drop_not_pending drops not-pending"
    );
}

/// A nested call Brenn refuses at queue time is answered with the refusal's
/// text as the SDK renders it: the message alone, not the error variant.
#[tokio::test]
async fn a_refused_nested_call_is_answered_with_its_text() {
    let r = rig(vec![], json!({})).await;
    let call = r.call_for_alice(
        MCP_NAME,
        json!({
            "mode": "api",
            "request": { "provider": "stub", "method": "GET", "host": r.host, "path": "/" },
        }),
    );
    r.requested_id().await;
    r.serve().await;
    let answer = call.await.unwrap();
    assert_eq!(
        answer,
        Err(ToolError::Internal(
            "call-async api-request: not-granted".to_string()
        ))
    );
}

/// A provider activation that fails answers the request it was handed at
/// once, not at the ceiling.
#[tokio::test]
async fn a_failed_provider_activation_answers_its_caller_at_once() {
    let r = rig(vec![], json!({})).await;
    let call = r.call_for_alice(MCP_NAME, json!({ "mode": "fail" }));
    r.requested_id().await;
    r.serve().await;
    let answer = tokio::time::timeout(std::time::Duration::from_secs(10), call)
        .await
        .expect("answered well inside the ceiling")
        .unwrap();
    let Err(ToolError::Internal(detail)) = answer else {
        panic!("expected an internal answer, got {answer:?}");
    };
    assert!(
        detail.starts_with("provider activation failed") && detail.contains("fail mode"),
        "{detail}"
    );
}

/// An id pending for one instance can neither be completed nor served by
/// another: both are refused as an unknown id would be, and the instance it
/// belongs to still answers it.
#[tokio::test]
async fn another_instance_cannot_complete_or_act_for_a_request() {
    let r = rig(vec![], json!({})).await;
    let call = r.call_for_alice(MCP_NAME, json!({ "mode": "echo" }));
    let id = r.requested_id().await;

    let other = r.host_for("other");
    let nested = json!({ "provider": "stub", "method": "GET", "host": r.host, "path": "/" });
    assert!(matches!(
        other.queue_async_for(&id, "api-request", &nested.to_string(), "c1"),
        Err(ToolCallError::Denied(_))
    ));
    assert_eq!(
        other.complete(&id, r#"{"ok": "hijacked"}"#),
        Err(ProvidesCallError::NotPending)
    );

    r.serve().await;
    let answer = call.await.unwrap().expect("the provider answered");
    assert_eq!(answer["mode"], "echo");
}
