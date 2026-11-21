//! A provider rig whose component calls a Google-shaped API, end to end, with
//! the OAuth test stub standing in for every API host.
//!
//! The component holds a `store` with its in-flight rows, its `api-request`
//! grant admits a clause list on the stub's host, and its `config` names that
//! host. The stub answers by request path and records every request. What
//! differs per component — its provider (an OAuth or a key provider), its
//! clauses' fields, its `config` keys — the suite passes in.

use super::*;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;

use brenn_envelope::addressing::TOOL_REQUEST_INPUT_PORT;
use brenn_lib::messaging::config::{MessagingGlobalConfig, ProvidedToolDecl};
use brenn_lib::tools::ResolvedToolGrant;
use brenn_messaging_store::blob::{BlobLimits, BlobUsage};
use brenn_oauth::testutil;
use brenn_oauth::tools::BlobStoreSlot;
use brenn_tool_registry::testutil::{clause, grant};
use brenn_tool_registry::{HeldAttachments, RegisteredTool, ToolError, ToolOutput};
use serde_json::Value;
use tokio::task::JoinHandle;

use super::kit_file_e2e::drop_caller;
use super::provider_rig::{CallerSetup, ConversationSetup, ProviderRig, ProviderSetup};
use crate::active_bridge::ActiveBridge;

/// The store namespace a provider component keeps its in-flight rows in.
const INFLIGHT_NS: &str = "inflight";
/// The store namespace a provider component keeps its queued calls in.
const BACKLOG_NS: &str = "backlog";

/// The stub, started before the rig so the suite can build its provider over
/// the stub's URL.
pub(super) struct Started {
    /// The stub's base URL, `http://<host>`.
    pub base: String,
    /// The stub's `host:port`, which every API host in `config` is.
    pub host: String,
    pub stub: testutil::Stub,
}

/// Start the stub on an ephemeral port.
pub(super) async fn start() -> Started {
    let (base, stub) = testutil::start_stub().await;
    let host = base
        .strip_prefix("http://")
        .expect("the stub is plain http")
        .to_string();
    Started { base, host, stub }
}

/// What a stub rig's provider is built from.
pub(super) struct StubProvider<'a> {
    pub slug: &'a str,
    /// The component's artifact.
    pub wasm: &'a str,
    pub decls: Vec<ProvidedToolDecl>,
    pub native: Vec<RegisteredTool>,
    /// The `api-request` grant's clauses, each as its fields but `host`; every
    /// clause is on the stub's host.
    pub clauses: Vec<Vec<(&'a str, &'a str)>>,
    pub config: HashMap<String, String>,
    /// The component's grants, as its instance holds them.
    pub grants: BTreeSet<ComponentGrant>,
    /// The blob store's limits when the grants hold `blobs`;
    /// [`production_blob_limits`] when `None`.
    pub blob_limits: Option<BlobLimits>,
    /// The slot the `native` tools' `api-request` was built over; the rig
    /// attaches its blob store there, when it has one.
    pub api_blobs: BlobStoreSlot,
    pub caller: Option<CallerSetup<'a>>,
    /// Apps whose conversations call the registry, if the suite has any.
    pub conversations: Option<ConversationSetup>,
    /// The limits every provider activation runs under in place of the
    /// production ones; the production limits when `None`.
    pub limits: Option<ActivationLimits>,
    /// The bus cap on a message body, `[messaging] max_body_bytes`; the
    /// default when `None`.
    pub max_body_bytes: Option<usize>,
    /// The OAuth service's vault, held for the rig's life.
    pub vault: tempfile::TempDir,
}

/// The blob store limits a deployment runs with when it states none.
pub(super) fn production_blob_limits() -> BlobLimits {
    let defaults = MessagingGlobalConfig::default();
    BlobLimits {
        max_blob_bytes: defaults.max_blob_bytes,
        budget_bytes: defaults.blob_budget_bytes,
    }
}

/// `n` bytes no two runs of which repeat at a short period, for files moved
/// through the rig.
pub(super) fn file_bytes(n: usize) -> Vec<u8> {
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    (0..n)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}

/// The provider rig, with the store the component keeps and the stub its
/// nested calls reach.
pub(super) struct StubRig {
    base: ProviderRig,
    pub stub: testutil::Stub,
    /// The stub's `host:port`.
    pub host: String,
    /// The apps of a rig built with conversations.
    apps: Option<Arc<IndexMap<String, brenn_lib::config::AppConfig>>>,
    _dir: tempfile::TempDir,
    _vault: tempfile::TempDir,
}

impl std::ops::Deref for StubRig {
    type Target = ProviderRig;

    fn deref(&self) -> &ProviderRig {
        &self.base
    }
}

/// Decoded query pairs, a repeated key once per value.
pub(super) type Pairs = Vec<(String, String)>;

/// A request path and every query pair, decoded, in the order sent; a key
/// sent more than once appears once per value.
fn split(path_and_query: &str) -> (String, Pairs) {
    let (path, query) = path_and_query
        .split_once('?')
        .unwrap_or((path_and_query, ""));
    (
        path.to_string(),
        url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect(),
    )
}

/// The query pairs a test expects, as owned strings, sorted as
/// [`StubRig::seen`] sorts what the stub received.
pub(super) fn pairs(expected: &[(&str, &str)]) -> Pairs {
    let mut pairs: Pairs = expected
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    pairs.sort();
    pairs
}

/// Assert `answer` is the `internal` refusal of a nested call the grant does
/// not admit: Brenn's text and the clause guidance.
pub(super) fn assert_could_not_be_issued(answer: Result<Value, ToolError>) {
    let Err(ToolError::Internal(detail)) = answer else {
        panic!("expected an internal answer, got {answer:?}");
    };
    assert!(
        detail.starts_with("api-request could not be issued: ")
            && detail.ends_with(
                "If this names the clause, the instance's `tool api-request` grant must admit \
                 the method, host and path."
            ),
        "{detail}"
    );
}

impl StubRig {
    /// Build the provider over `started`'s stub: its `api-request` grant holds
    /// `setup.clauses` on the stub's host, and it holds `setup.grants`, with
    /// its store open and, when the grants hold `blobs`, a blob store.
    pub async fn build(started: Started, setup: StubProvider<'_>) -> StubRig {
        let Started { host, stub, .. } = started;
        let grants: BTreeMap<String, ResolvedToolGrant> = BTreeMap::from([(
            "api-request".to_string(),
            grant(
                setup
                    .clauses
                    .iter()
                    .map(|fields| {
                        let mut fields = fields.clone();
                        fields.push(("host", &host));
                        clause(&fields)
                    })
                    .collect(),
            ),
        )]);
        let dir = tempfile::tempdir().expect("a store directory");
        let store_path = dir.path().join(format!("{}.db", setup.slug));
        let apps = setup
            .conversations
            .as_ref()
            .map(|conversations| Arc::new(conversations.apps.clone()));
        let base = ProviderRig::build(ProviderSetup {
            slug: setup.slug,
            decls: setup.decls,
            native: setup.native,
            grants,
            component: ProcessorLoadSpec {
                config: setup.config,
                store_path: Some(&store_path),
                grants: setup.grants,
                ..ProcessorLoadSpec::minimal(Path::new(setup.wasm), setup.slug)
            },
            caller: setup.caller,
            conversations: setup.conversations,
            blob_limits: Some(setup.blob_limits.unwrap_or_else(production_blob_limits)),
            limits: setup.limits,
            max_body_bytes: setup.max_body_bytes,
        })
        .await;
        base.provider.component.open_store();
        if let Some(store) = base.messenger.blob_store() {
            setup.api_blobs.attach(store.clone());
        }

        StubRig {
            base,
            stub,
            host,
            apps,
            _dir: dir,
            _vault: setup.vault,
        }
    }

    /// A fresh conversation of `app_slug` acting for `username`; panics on a
    /// rig built without conversations.
    pub async fn conversation(&self, app_slug: &str, username: &str) -> Arc<ActiveBridge> {
        ActiveBridge::test_new_for_conversation(
            self.messenger.clone(),
            self.apps
                .clone()
                .expect("the rig was built with conversations"),
            app_slug,
            username,
            self.registry.clone(),
        )
        .await
    }

    /// Run the one nested call the provider issued, and wait for its result
    /// on the provider's inbox; returns that result.
    pub async fn run_nested(&self) -> Value {
        self.run_nested_calls(1).await.remove(0)
    }

    /// Run the `n` nested calls the provider issued, and wait for their
    /// results on the provider's inbox; returns those results.
    pub async fn run_nested_calls(&self, n: usize) -> Vec<Value> {
        self.owed(&self.executor_sub(), n).await;
        self.executor.drain_step().await;
        self.owed(&self.provider_sub, n).await
    }

    /// Wait until the executor is owed one request, then run one executor
    /// drain step on a task of its own, for a nested call the stub's gate
    /// holds: the step returns once the stub answers it.
    pub async fn start_nested(&self) -> tokio::task::JoinHandle<()> {
        self.owed(&self.executor_sub(), 1).await;
        let executor = self.executor.clone();
        tokio::spawn(async move { executor.drain_step().await })
    }

    /// Serve the request, then run each of its `steps` nested calls and hand
    /// the provider its result, ending on the activation that answers.
    pub async fn run(&self, steps: usize) {
        self.serve().await;
        for _ in 0..steps {
            self.run_nested().await;
            self.serve().await;
        }
    }

    /// Call `mcp_name` as alice with `args`, handing it `attachments`, and
    /// [`run`](Self::run) the request through `steps` nested calls; the whole
    /// output, once the request is [finished](Self::assert_finished).
    pub async fn call_running(
        &self,
        mcp_name: &str,
        args: Value,
        attachments: HeldAttachments,
        steps: usize,
    ) -> Result<ToolOutput, ToolError> {
        let call = self.call_for_alice_with(mcp_name, args, attachments);
        let request = self.requested_id().await;
        self.run(steps).await;
        let answer = call.await.unwrap();
        self.assert_finished(&request);
        answer
    }

    /// The blob store's usage.
    pub fn blob_usage(&self) -> BlobUsage {
        self.messenger.blob_store().expect("a blob store").usage()
    }

    /// The production grant of the activation the provider is owed next, by
    /// the host's fuel rule over every envelope it is handed, the new bodies
    /// and the pending requests' not handed new, and the attachments of both,
    /// as for a component granted `blobs`.
    pub async fn production_grant(&self) -> u64 {
        let ports = self.owed_windows().await;
        let envelopes: usize = ports.iter().map(|port| port.entries.len()).sum();
        let mut body_bytes = 0u64;
        let mut attachment_bytes = 0u64;
        let mut new_ids = Vec::new();
        for port in &ports {
            for (_, envelope) in &port.entries[port.new_from..] {
                body_bytes += envelope.body.len() as u64;
                attachment_bytes += envelope
                    .attachments
                    .iter()
                    .map(|(_, blob)| blob.size)
                    .sum::<u64>();
                if port.port == TOOL_REQUEST_INPUT_PORT {
                    let request: Value =
                        serde_json::from_str(envelope.body.as_text().unwrap()).unwrap();
                    new_ids.push(request["id"].as_str().unwrap().to_string());
                }
            }
        }
        let new_ids: Vec<&str> = new_ids.iter().map(String::as_str).collect();
        let pending = self.runtime.pending_load(&self.provider.slug, &new_ids);
        attachment_bytes += pending
            .attachments
            .iter()
            .map(|blob| blob.size)
            .sum::<u64>();
        brenn_wasm::processor_fuel(
            envelopes,
            body_bytes,
            pending.body_bytes,
            Some(attachment_bytes),
        )
    }

    /// The stub's answer to a request for `path`.
    pub fn answer(&self, path: &str, body: impl Into<Vec<u8>>) {
        self.stub
            .lock()
            .unwrap()
            .api_bodies
            .insert(path.to_string(), body.into());
    }

    /// The stub answers every request for `path` with `status` and `body`;
    /// requests for other paths are answered as before.
    pub fn respond(&self, path: &str, status: u16, body: Vec<u8>) {
        self.stub
            .lock()
            .unwrap()
            .api_failures
            .insert(path.to_string(), (status, body));
    }

    /// The stub answers every `method` request for `path` with `status` and
    /// `body`, ahead of `respond` and `answer` for that path; other methods
    /// on it are answered as before.
    pub fn respond_to(&self, method: &str, path: &str, status: u16, body: Vec<u8>) {
        self.stub
            .lock()
            .unwrap()
            .api_by_method
            .insert((method.to_string(), path.to_string()), (status, body));
    }

    /// Every API request the stub saw, as method, path and decoded query
    /// pairs, sorted.
    pub fn seen(&self) -> Vec<(String, String, Pairs)> {
        self.stub
            .lock()
            .unwrap()
            .api_seen
            .iter()
            .map(|seen| {
                let (path, mut query) = split(&seen.path_and_query);
                query.sort();
                (seen.method.clone(), path, query)
            })
            .collect()
    }

    /// Wait until the stub has received `n` API requests.
    pub async fn saw(&self, n: usize) {
        for _ in 0..3000 {
            if self.stub.lock().unwrap().api_seen.len() >= n {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("the stub never received {n} requests");
    }

    /// Make a late answer of the request `call`'s next nested call issues,
    /// one the stub's gate holds: run the executor's drain step on a task of
    /// its own, wait until the stub has received `seen` requests, which puts
    /// the step past the executor's check that its request is pending, then
    /// drop the caller, which ends the request. Returns the drain step; it
    /// finishes once the gate lets the stub answer.
    pub async fn end_held(
        &self,
        call: JoinHandle<Result<ToolOutput, ToolError>>,
        seen: usize,
    ) -> JoinHandle<()> {
        let step = self.start_nested().await;
        self.saw(seen).await;
        drop_caller(call).await;
        step
    }

    /// Let `gate` admit one answer for each of `steps`, from
    /// [`end_held`](Self::end_held), wait for every step, and assert each
    /// answer is owed to the provider with its row still in flight; returns
    /// those answers.
    pub async fn answer_late(
        &self,
        gate: &tokio::sync::Semaphore,
        steps: Vec<JoinHandle<()>>,
    ) -> Vec<Value> {
        let n = steps.len();
        gate.add_permits(n);
        for step in steps {
            step.await.expect("the executor's drain step");
        }
        let answers = self.owed(&self.provider_sub, n).await;
        assert_eq!(self.inflight_keys().len(), n, "{n} requests in flight");
        answers
    }

    /// Whether the activation handed `n` late answers settled every one: no
    /// row is left. Panics unless it left none or all `n`.
    pub fn settled_late(&self, n: usize) -> bool {
        let left = self.inflight_keys().len();
        assert!(left == 0 || left == n, "{left} rows left");
        left == 0
    }

    /// The body of every API request the stub saw that carried one, in the
    /// order received.
    pub fn bodies(&self) -> Vec<Vec<u8>> {
        self.stub
            .lock()
            .unwrap()
            .api_seen
            .iter()
            .filter(|seen| !seen.body.is_empty())
            .map(|seen| seen.body.clone())
            .collect()
    }

    /// The `Authorization` header of every API request the stub saw.
    pub fn bearers(&self) -> Vec<String> {
        self.stub
            .lock()
            .unwrap()
            .api_seen
            .iter()
            .map(|seen| {
                seen.headers
                    .get("authorization")
                    .expect("a bearer was sent")
                    .to_str()
                    .unwrap()
                    .to_string()
            })
            .collect()
    }

    /// The keys of the component's in-flight rows, read from its store.
    pub fn inflight_keys(&self) -> Vec<String> {
        self.keys(INFLIGHT_NS)
    }

    /// The keys of the component's backlog rows, read from its store.
    pub fn backlog_keys(&self) -> Vec<String> {
        self.keys(BACKLOG_NS)
    }

    /// The keys of the component's rows in namespace `ns`.
    fn keys(&self, ns: &str) -> Vec<String> {
        self.provider
            .component
            .kv_store_for_testing()
            .scan_for_testing(ns)
            .into_iter()
            .map(|(key, _)| String::from_utf8(key).expect("a UTF-8 key"))
            .collect()
    }

    /// Plant a raw in-flight row under `key`.
    pub fn plant_inflight(&self, key: &str, value: &[u8]) {
        self.provider
            .component
            .kv_store_for_testing()
            .put_for_testing(INFLIGHT_NS, key.as_bytes(), value);
    }

    /// Whether the request `id` was answered and left nothing behind, as
    /// [`assert_finished`](Self::assert_finished) asserts.
    pub fn finished(&self, id: &str) -> bool {
        self.inflight_keys().is_empty() && self.runtime.serving(&self.provider.slug, id).is_none()
    }

    /// The request `id` was answered and left nothing behind.
    pub fn assert_finished(&self, id: &str) {
        assert!(
            self.inflight_keys().is_empty(),
            "the answered request left no row"
        );
        assert!(self.runtime.serving(&self.provider.slug, id).is_none());
    }
}
