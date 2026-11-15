//! The real `brenn_wasm::ProvidesHost` over the process's [`ProviderRuntime`].
//!
//! One host is built per WASM consumer granted `provides`. It answers that
//! consumer's completions against the pending-call table, and resolves its
//! `call-async-for` calls exactly as [`WasmToolHost`] resolves `call-async`,
//! with the served id added to the request body as `serving`. The id is
//! checked against the table *for this consumer* on every call; the acting
//! user never leaves the table — the executor reads it from the record when
//! the nested call is admitted.

use std::sync::Arc;

use brenn_messaging_store::blob::HeldAttachments;
use brenn_wasm::{ProvidesCallError, ProvidesHost, QueuedToolRequest, ToolCallError};
use serde::Deserialize;
use serde_json::Value;

use super::descriptor::{MAX_ASYNC_RESULT_BYTES, ToolError};
use super::provided::{CompleteError, ProviderRuntime};
use super::tool::ToolOutput;
use super::wasm_host::WasmToolHost;

// The WASM host refuses outcome text over its cap before this host parses it.
// The answer budget is the async result cap less the envelope margin, so every
// compactly written outcome within the budget is within the text cap.
const _: () = assert!(brenn_wasm::PROCESSOR_MAX_COMPLETE_OUTCOME_BYTES == MAX_ASYNC_RESULT_BYTES);

/// A per-consumer provides host: the shared pending-call table, this
/// consumer's slug, and the tool host its nested calls resolve through.
pub struct WasmProvidesHost {
    runtime: Arc<ProviderRuntime>,
    slug: String,
    tools: Arc<WasmToolHost>,
}

impl WasmProvidesHost {
    /// Build the host for the providing consumer whose tool host is `tools`:
    /// its nested calls resolve through that host, so they are admitted by the
    /// consumer's tool grants, and its slug is the host's.
    pub fn new(runtime: Arc<ProviderRuntime>, tools: Arc<WasmToolHost>) -> Self {
        Self {
            runtime,
            slug: tools.slug().to_string(),
            tools,
        }
    }
}

/// A completion as the guest writes it: `{"ok": value}` or
/// `{"err": {"kind", "detail"}}`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
enum WireOutcome {
    #[serde(rename = "ok")]
    Ok(Value),
    #[serde(rename = "err")]
    Err(WireError),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireError {
    kind: String,
    detail: String,
}

/// Read a guest's outcome JSON into the answer its caller receives. Which
/// answers a provider may give is the runtime's rule; this only reads the
/// wire form, so an admission kind reaches the runtime and is refused there.
fn parse_outcome(outcome_json: &str) -> Result<Result<Value, ToolError>, String> {
    let wire: WireOutcome = serde_json::from_str(outcome_json).map_err(|e| {
        format!(
            "the outcome is not {{\"ok\": value}} or {{\"err\": {{\"kind\", \"detail\"}}}}: {e}"
        )
    })?;
    Ok(match wire {
        WireOutcome::Ok(value) => Ok(value),
        WireOutcome::Err(WireError { kind, detail }) => Err(ToolError::from_wire(&kind, detail)
            .ok_or_else(|| format!("error kind {kind:?} is not a tool error kind"))?),
    })
}

impl ProvidesHost for WasmProvidesHost {
    fn complete(
        &self,
        id: &str,
        outcome_json: &str,
        attachments: HeldAttachments,
    ) -> Result<(), ProvidesCallError> {
        let outcome =
            match parse_outcome(outcome_json).map_err(ProvidesCallError::InvalidOutcome)? {
                Ok(value) => Ok(ToolOutput { value, attachments }),
                Err(_) if !attachments.is_empty() => {
                    return Err(ProvidesCallError::InvalidOutcome(
                        "an error outcome carries no attachments".to_string(),
                    ));
                }
                Err(e) => Err(e),
            };
        self.runtime
            .complete(&self.slug, id, outcome)
            .map_err(|e| match e {
                CompleteError::NotPending => ProvidesCallError::NotPending,
                CompleteError::InvalidOutcome(detail) => ProvidesCallError::InvalidOutcome(detail),
            })
    }

    fn queue_async_for(
        &self,
        serving: &str,
        tool: &str,
        args_json: &str,
        call_id: &str,
        with_attachments: bool,
    ) -> Result<QueuedToolRequest, ToolCallError> {
        if self.runtime.serving(&self.slug, serving).is_none() {
            return Err(ToolCallError::Denied(
                "the pending call this request serves is not pending for this component"
                    .to_string(),
            ));
        }
        self.tools
            .resolve_async(tool, args_json, call_id, Some(serving), with_attachments)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provided::ProvidedTool;
    use crate::provided::tests::{PROVIDER, decl, messenger, pending_ctx, published_id};
    use crate::testutil::{ExecuteValue, clause, git_repo_pull_only, grant};
    use crate::tool::{ActingUser, AsyncTool};
    use brenn_obs::alerting::noop_alert_dispatcher;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn runtime() -> Arc<ProviderRuntime> {
        let (alert, _drain) = noop_alert_dispatcher();
        ProviderRuntime::new(alert)
    }

    /// The host `slug` sees, its nested calls granted `git-repo-pull` on repo
    /// `r` only.
    fn host(runtime: &Arc<ProviderRuntime>, slug: &str) -> WasmProvidesHost {
        let (alert, _drain) = noop_alert_dispatcher();
        let tools = WasmToolHost::new(
            Arc::new(git_repo_pull_only()).into(),
            BTreeMap::from([(
                "git-repo-pull".to_string(),
                grant(vec![clause(&[("repo", "r")])]),
            )]),
            slug.to_string(),
            false,
            alert,
        );
        WasmProvidesHost::new(runtime.clone(), Arc::new(tools))
    }

    /// A call to the provider's `echo-for-user` in flight through `execute`,
    /// and the id it was published under.
    struct InFlight {
        runtime: Arc<ProviderRuntime>,
        id: String,
        call: tokio::task::JoinHandle<Result<Value, ToolError>>,
    }

    async fn in_flight() -> InFlight {
        let runtime = runtime();
        let messenger = messenger().await;
        runtime.attach(messenger.clone());
        let tool = Arc::new(ProvidedTool::new(PROVIDER, &decl(1), runtime.clone()));
        let call = tokio::spawn(async move {
            let ctx = pending_ctx(Some(ActingUser {
                id: 1,
                username: "alice".to_string(),
            }));
            tool.execute_value(&ctx, json!({ "q": 1 })).await
        });
        let id = published_id(&messenger).await;
        InFlight { runtime, id, call }
    }

    #[tokio::test]
    async fn a_completion_answers_the_caller_with_the_outcome() {
        let f = in_flight().await;
        host(&f.runtime, PROVIDER)
            .complete(&f.id, r#"{"ok": {"n": 1}}"#, HeldAttachments::none())
            .unwrap();
        assert_eq!(f.call.await.unwrap(), Ok(json!({ "n": 1 })));
    }

    #[tokio::test]
    async fn each_provider_error_kind_reaches_the_caller() {
        for (kind, want) in [
            ("denied", ToolError::Denied("d".into())),
            ("invalid_args", ToolError::InvalidArgs("d".into())),
            ("internal", ToolError::Internal("d".into())),
        ] {
            let f = in_flight().await;
            let outcome = json!({ "err": { "kind": kind, "detail": "d" } }).to_string();
            host(&f.runtime, PROVIDER)
                .complete(&f.id, &outcome, HeldAttachments::none())
                .unwrap();
            assert_eq!(f.call.await.unwrap(), Err(want));
        }
    }

    /// A malformed outcome, an unknown kind and an admission kind are each
    /// `invalid-outcome`; the call stays pending and a valid answer still
    /// reaches it.
    #[tokio::test]
    async fn a_refused_outcome_leaves_the_call_pending() {
        let f = in_flight().await;
        let host = host(&f.runtime, PROVIDER);
        for outcome in [
            "not json",
            r#"{"value": 1}"#,
            r#"{"ok": 1, "extra": 2}"#,
            r#"{"err": {"kind": "teapot", "detail": ""}}"#,
            r#"{"err": {"kind": "internal"}}"#,
            r#"{"err": {"kind": "rate_limited", "detail": ""}}"#,
        ] {
            let refused = host.complete(&f.id, outcome, HeldAttachments::none());
            assert!(
                matches!(refused, Err(ProvidesCallError::InvalidOutcome(_))),
                "{outcome}: {refused:?}"
            );
        }
        assert!(f.runtime.serving(PROVIDER, &f.id).is_some());
        host.complete(&f.id, r#"{"ok": null}"#, HeldAttachments::none())
            .unwrap();
        assert_eq!(f.call.await.unwrap(), Ok(Value::Null));
    }

    #[tokio::test]
    async fn an_id_not_pending_for_this_component_is_not_pending() {
        let f = in_flight().await;
        let made_up = "0".repeat(32);
        for (slug, id) in [("other", f.id.as_str()), (PROVIDER, made_up.as_str())] {
            assert_eq!(
                host(&f.runtime, slug).complete(id, r#"{"ok": 1}"#, HeldAttachments::none()),
                Err(ProvidesCallError::NotPending)
            );
        }
        assert!(f.runtime.serving(PROVIDER, &f.id).is_some());
    }

    #[tokio::test]
    async fn a_nested_call_carries_the_served_id_and_no_user() {
        let runtime = runtime();
        let guard = runtime.begin(PROVIDER, "echo-for-user", &pending_ctx(None));
        let id = guard.id().as_str();
        let queued = host(&runtime, PROVIDER)
            .queue_async_for(id, "git-repo-pull", r#"{"repos":["r"]}"#, "c1", false)
            .unwrap();
        assert_eq!(queued.channel, "brenn:tools/git-repo-pull");
        assert_eq!(queued.reply_to, format!("brenn:tool-results/{PROVIDER}"));
        let body: Value = serde_json::from_str(&queued.body_json).unwrap();
        assert_eq!(
            body,
            json!({
                "v": 1,
                "tool": "git-repo-pull",
                "call_id": "c1",
                "caller": format!("wasm:{PROVIDER}"),
                "args": { "repos": ["r"] },
                "serving": id,
            })
        );
    }

    /// Another instance's id, and an id whose call has ended, are refused
    /// `denied` before the tool is looked at.
    #[tokio::test]
    async fn a_nested_call_serving_an_id_not_pending_here_is_denied() {
        let runtime = runtime();
        let guard = runtime.begin(PROVIDER, "echo-for-user", &pending_ctx(None));
        let id = guard.id().as_str().to_string();
        let args = r#"{"repos":["r"]}"#;
        let foreign =
            host(&runtime, "other").queue_async_for(&id, "git-repo-pull", args, "c1", false);
        assert!(
            matches!(foreign, Err(ToolCallError::Denied(_))),
            "{foreign:?}"
        );
        drop(guard);
        let ended =
            host(&runtime, PROVIDER).queue_async_for(&id, "git-repo-pull", args, "c1", false);
        assert!(matches!(ended, Err(ToolCallError::Denied(_))), "{ended:?}");
    }

    /// A nested call with attachments is judged by the same rule as
    /// `call-async`'s: a tool that takes none refuses them, last.
    #[tokio::test]
    async fn a_nested_call_with_attachments_needs_an_accepting_tool() {
        let runtime = runtime();
        let guard = runtime.begin(PROVIDER, "echo-for-user", &pending_ctx(None));
        let host = host(&runtime, PROVIDER);
        let id = guard.id().as_str();
        assert_eq!(
            host.queue_async_for(id, "git-repo-pull", r#"{"repos":["r"]}"#, "c1", true),
            Err(ToolCallError::AttachmentsNotAccepted)
        );
        assert!(matches!(
            host.queue_async_for(
                id,
                "git-repo-pull",
                r#"{"repos":["elsewhere"]}"#,
                "c1",
                true
            ),
            Err(ToolCallError::Denied(_))
        ));
    }

    /// Serving a pending id lends no grant: the nested call is admitted by
    /// the provider's own grants, as `call-async` is.
    #[tokio::test]
    async fn a_nested_call_is_admitted_by_the_providers_own_grants() {
        let runtime = runtime();
        let guard = runtime.begin(PROVIDER, "echo-for-user", &pending_ctx(None));
        let id = guard.id().as_str();
        let host = host(&runtime, PROVIDER);
        assert_eq!(
            host.queue_async_for(id, "api-request", "{}", "c1", false),
            Err(ToolCallError::NotGranted)
        );
        assert!(matches!(
            host.queue_async_for(
                id,
                "git-repo-pull",
                r#"{"repos":["elsewhere"]}"#,
                "c1",
                false
            ),
            Err(ToolCallError::Denied(_))
        ));
    }

    /// A provider holding no tool grant serves a pending id and is still
    /// refused every nested call `not-granted`.
    #[tokio::test]
    async fn a_provider_with_no_tool_grant_is_not_granted_a_nested_call() {
        let runtime = runtime();
        let guard = runtime.begin(PROVIDER, "echo-for-user", &pending_ctx(None));
        let (alert, _drain) = noop_alert_dispatcher();
        let tools = WasmToolHost::new(
            Arc::new(git_repo_pull_only()).into(),
            BTreeMap::new(),
            PROVIDER.to_string(),
            false,
            alert,
        );
        let host = WasmProvidesHost::new(runtime.clone(), Arc::new(tools));
        assert_eq!(
            host.queue_async_for(
                guard.id().as_str(),
                "git-repo-pull",
                r#"{"repos":["r"]}"#,
                "c1",
                false
            ),
            Err(ToolCallError::NotGranted)
        );
    }

    /// An `ok` completion's attachments reach the caller beside the value; an
    /// `err` completion with any is refused `invalid-outcome`, its holds drop,
    /// and the call stays pending for a valid answer.
    #[tokio::test]
    async fn a_completions_attachments_ride_an_ok_outcome_only() {
        use crate::provided::tests::blob_messenger;
        use brenn_messaging_store::testutils::{held, seal_bytes};

        let runtime = runtime();
        let (messenger, _blob_dir) = blob_messenger().await;
        let store = messenger.blob_store().unwrap().clone();
        runtime.attach(messenger.clone());
        let tool = Arc::new(ProvidedTool::new(PROVIDER, &decl(1), runtime.clone()));
        let call = tokio::spawn(async move {
            tool.execute(&pending_ctx(None), json!({}), HeldAttachments::none())
                .await
        });
        let id = published_id(&messenger).await;
        let host = host(&runtime, PROVIDER);

        let made = seal_bytes(&store, "text/plain", b"made");
        let err = json!({ "err": { "kind": "internal", "detail": "d" } }).to_string();
        assert_eq!(
            host.complete(&id, &err, held(&[("made", &made)])),
            Err(ProvidesCallError::InvalidOutcome(
                "an error outcome carries no attachments".to_string()
            ))
        );
        assert!(runtime.serving(PROVIDER, &id).is_some(), "still pending");
        drop(made);
        assert_eq!(
            store.usage().sealed,
            0,
            "the refused completion held it last"
        );

        let made = seal_bytes(&store, "text/plain", b"made");
        let made_id = made.id().clone();
        host.complete(&id, r#"{"ok": {"n": 1}}"#, held(&[("made", &made)]))
            .unwrap();
        drop(made);
        let output = call.await.unwrap().expect("answered");
        assert_eq!(output.value, json!({ "n": 1 }));
        assert_eq!(
            output
                .attachments
                .refs()
                .get(&brenn_envelope::AttachmentName::new("made").unwrap())
                .unwrap()
                .id,
            made_id
        );
    }
}
