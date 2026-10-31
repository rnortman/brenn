//! A provider rig whose component calls a Google-shaped API, end to end, with
//! the OAuth test stub standing in for every API host.
//!
//! The component holds a `store` with its in-flight rows, its `api-request`
//! grant admits a clause list on the stub's host, and its `config` names that
//! host. The stub answers by request path and records every request. What
//! differs per component — its provider (an OAuth or a key provider), its
//! clauses' fields, its `config` keys — the suite passes in.

use super::*;

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use brenn_lib::messaging::config::ProvidedToolDecl;
use brenn_lib::tools::ResolvedToolGrant;
use brenn_oauth::testutil;
use brenn_tool_registry::testutil::{clause, grant};
use brenn_tool_registry::{RegisteredTool, ToolError};
use serde_json::Value;

use super::provider_rig::{CallerSetup, ProviderRig, ProviderSetup};

/// The store namespace a provider component keeps its in-flight rows in.
const INFLIGHT_NS: &str = "inflight";

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
    pub caller: Option<CallerSetup<'a>>,
    /// The OAuth service's vault, held for the rig's life.
    pub vault: tempfile::TempDir,
}

/// The provider rig, with the store the component keeps and the stub its
/// nested calls reach.
pub(super) struct StubRig {
    base: ProviderRig,
    pub stub: testutil::Stub,
    /// The stub's `host:port`.
    pub host: String,
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
    /// `setup.clauses` on the stub's host, and it holds every grant a
    /// provider component needs, with its store open.
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
        let base = ProviderRig::build(ProviderSetup {
            slug: setup.slug,
            decls: setup.decls,
            native: setup.native,
            grants,
            component: ProcessorLoadSpec {
                config: setup.config,
                store_path: Some(&store_path),
                grants: [
                    ComponentGrant::Log,
                    ComponentGrant::Store,
                    ComponentGrant::Config,
                    ComponentGrant::Tools,
                    ComponentGrant::Provides,
                ]
                .into_iter()
                .collect(),
                ..ProcessorLoadSpec::minimal(Path::new(setup.wasm), setup.slug)
            },
            caller: setup.caller,
        })
        .await;
        base.provider.component.open_store();

        StubRig {
            base,
            stub,
            host,
            _dir: dir,
            _vault: setup.vault,
        }
    }

    /// Run the one nested call the provider issued, and wait for its result
    /// on the provider's inbox; returns that result.
    pub async fn run_nested(&self) -> Value {
        self.owed(&self.executor_sub(), 1).await;
        self.executor.drain_step().await;
        self.owed(&self.provider_sub, 1).await.remove(0)
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
    pub fn fail(&self, path: &str, status: u16, body: Vec<u8>) {
        self.stub
            .lock()
            .unwrap()
            .api_failures
            .insert(path.to_string(), (status, body));
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
        self.provider
            .component
            .kv_store_for_testing()
            .scan_for_testing(INFLIGHT_NS)
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

    /// The request `id` was answered and left nothing behind.
    pub fn assert_finished(&self, id: &str) {
        assert!(
            self.inflight_keys().is_empty(),
            "the answered request left no row"
        );
        assert!(self.runtime.serving(&self.provider.slug, id).is_none());
    }
}
