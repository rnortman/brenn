//! The `google-maps-tools` component, end to end.
//!
//! The compiled component serves its provided tools through the one registry:
//! an app conversation acting for alice calls a tool, the request lands on the
//! component's `brenn:tool-requests/<slug>` inbox, the component issues its
//! nested `api-request`s through a real `WasmProvidesHost`, the executor runs
//! each against the OAuth test stub through a key provider, and the result
//! comes back on the component's `brenn:tool-results/<slug>` inbox. Every
//! Google host the component's `config` names is the stub, which answers by
//! request path. The component and the executor are stepped by hand; the
//! conversation's call runs on a spawned task.

use super::*;

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use brenn_lib::config::{ApiKeyProviderConfig, PACKAGED, config_from_dsl};
use brenn_lib::messaging::config::ProvidedToolDecl;
use brenn_lib::tools::ResolvedToolGrant;
use brenn_oauth::testutil;
use brenn_tool_registry::ToolError;
use brenn_tool_registry::testutil::{clause, grant};
use serde_json::{Value, json};

use super::provider_rig::{ProviderRig, ProviderSetup};

const MAPS_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../brenn-wasm/target/components/brenn_google_maps_tools.wasm"
);

/// The component's packaged specification.
const MAPS_SPEC: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../config/specs/google-maps-tools.brenn"
);

/// The providing instance.
const PROVIDER: &str = "maps";
/// The key provider the instance's `config` names.
const KEY_PROVIDER: &str = "maps";
/// The store namespace the component keeps its in-flight rows in.
const INFLIGHT_NS: &str = "inflight";

const GEOCODE_ADDRESS: &str = "/v4/geocode/address";
const GEOCODE_LOCATION: &str = "/v4/geocode/location";
const CURRENT_CONDITIONS: &str = "/v1/currentConditions:lookup";

/// The instance's `api-request` clauses, as method and exact path, each on the
/// stub's host: one per endpoint the component calls.
const CLAUSES: &[(&str, &str)] = &[
    ("GET", "/v1/currentConditions:lookup"),
    ("GET", "/v1/forecast/hours:lookup"),
    ("GET", "/v1/forecast/days:lookup"),
    ("GET", "/v1/publicAlerts:lookup"),
    ("GET", GEOCODE_ADDRESS),
    ("GET", GEOCODE_LOCATION),
    ("POST", "/v1/places:searchText"),
    ("GET", "/v1/places/*"),
    ("POST", "/directions/v2:computeRoutes"),
];

/// The component's `provide` statements, lowered from its packaged
/// specification under one instance of its class.
fn provided() -> Vec<ProvidedToolDecl> {
    let spec =
        std::fs::read_to_string(MAPS_SPEC).unwrap_or_else(|e| panic!("read {MAPS_SPEC}: {e}"));
    let config = config_from_dsl(&format!(
        r#"{PACKAGED}{spec}{PACKAGED}
new {PROVIDER}: GoogleMapsTools {{
    grants = [log, store, config, tools, provides];
    store_path = "/var/lib/brenn/maps.db";
    tool api-request {{}}
}}
"#
    ));
    config
        .wasm_consumers
        .into_iter()
        .find(|consumer| consumer.slug == PROVIDER)
        .expect("the instance lowers")
        .provides
}

/// One Geocoding v4 result for Mountain View.
fn geocode_body() -> Value {
    json!({
        "results": [{
            "placeId": "ChIJiQHsW0m3j4ARm69rRkrUF3w",
            "location": { "latitude": 37.4220, "longitude": -122.0841 },
            "formattedAddress": "Mountain View, CA, USA",
            "granularity": "APPROXIMATE",
            "types": ["locality", "political"],
        }],
    })
}

/// Current conditions as the Weather API answers them, pretty-printed as
/// Google sends it.
fn current_body() -> String {
    serde_json::to_string_pretty(&json!({
        "currentTime": "2026-10-30T17:00:00Z",
        "timeZone": { "id": "America/Los_Angeles" },
        "isDaytime": true,
        "weatherCondition": {
            "iconBaseUri": "https://maps.gstatic.com/weather/v1/sunny",
            "description": { "text": "Sunny", "languageCode": "en" },
            "type": "CLEAR",
        },
        "temperature": { "degrees": 18.5, "unit": "CELSIUS" },
        "feelsLikeTemperature": { "degrees": 18.0, "unit": "CELSIUS" },
        "relativeHumidity": 40,
    }))
    .unwrap()
}

/// The provider rig, with the store the component keeps and the stub its
/// nested calls reach.
struct Rig {
    base: ProviderRig,
    stub: testutil::Stub,
    _dir: tempfile::TempDir,
    _vault: tempfile::TempDir,
}

impl std::ops::Deref for Rig {
    type Target = ProviderRig;

    fn deref(&self) -> &ProviderRig {
        &self.base
    }
}

/// The rig with every clause and the stub's key provider as
/// `testutil::key_provider_config` builds it.
async fn rig() -> Rig {
    rig_with(CLAUSES, |_| {}).await
}

/// Stand up the rig: the provider's `api-request` grant holds `clauses`, and
/// `key` adjusts the key provider before the registry is built.
async fn rig_with(clauses: &[(&str, &str)], key: impl FnOnce(&mut ApiKeyProviderConfig)) -> Rig {
    let (base, stub) = testutil::start_stub().await;
    let host = base
        .strip_prefix("http://")
        .expect("the stub is plain http")
        .to_string();
    let (service, vault) = testutil::service(vec![]);
    let (key_name, mut key_config) = testutil::key_provider_config(KEY_PROVIDER, &base);
    key(&mut key_config);

    let grants: BTreeMap<String, ResolvedToolGrant> = BTreeMap::from([(
        "api-request".to_string(),
        grant(
            clauses
                .iter()
                .map(|(method, path)| {
                    clause(&[
                        ("provider", KEY_PROVIDER),
                        ("method", method),
                        ("host", &host),
                        ("path", path),
                    ])
                })
                .collect(),
        ),
    )]);
    let dir = tempfile::tempdir().expect("a store directory");
    let store_path = dir.path().join("google-maps-tools.db");
    let base = ProviderRig::build(ProviderSetup {
        slug: PROVIDER,
        decls: provided(),
        native: brenn_oauth::tools::registered(
            Some(&service),
            BTreeMap::from([(key_name, key_config)]),
        ),
        grants,
        component: ProcessorLoadSpec {
            config: HashMap::from([
                ("provider".to_string(), KEY_PROVIDER.to_string()),
                ("weather_host".to_string(), host.clone()),
                ("geocoding_host".to_string(), host.clone()),
                ("places_host".to_string(), host.clone()),
                ("routes_host".to_string(), host.clone()),
            ]),
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
            ..ProcessorLoadSpec::minimal(Path::new(MAPS_WASM), PROVIDER)
        },
        caller: None,
    })
    .await;
    base.provider.component.open_store();

    Rig {
        base,
        stub,
        _dir: dir,
        _vault: vault,
    }
}

/// A rig whose `api-request` grant admits every clause but `path`'s.
async fn rig_without_clause(path: &str) -> Rig {
    let clauses: Vec<(&str, &str)> = CLAUSES
        .iter()
        .copied()
        .filter(|(_, admitted)| *admitted != path)
        .collect();
    rig_with(&clauses, |_| {}).await
}

/// Assert `answer` is the `internal` refusal of a nested call the grant does
/// not admit: Brenn's text and the clause guidance.
fn assert_could_not_be_issued(answer: Result<Value, ToolError>) {
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

/// A request path and its query, decoded.
fn split(path_and_query: &str) -> (String, BTreeMap<String, String>) {
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

/// The query pairs a test expects, as owned strings.
fn pairs(expected: &[(&str, &str)]) -> BTreeMap<String, String> {
    expected
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

impl Rig {
    /// Run the one nested call the provider issued, and wait for its result
    /// on the provider's inbox; returns that result.
    async fn run_nested(&self) -> Value {
        self.owed(&self.executor_sub(), 1).await;
        self.executor.drain_step().await;
        self.owed(&self.provider_sub, 1).await.remove(0)
    }

    /// The stub's answer to a request for `path`.
    fn answer(&self, path: &str, body: impl Into<Vec<u8>>) {
        self.stub
            .lock()
            .unwrap()
            .api_bodies
            .insert(path.to_string(), body.into());
    }

    /// Every API request the stub saw, as method, path and decoded query.
    fn seen(&self) -> Vec<(String, String, BTreeMap<String, String>)> {
        self.stub
            .lock()
            .unwrap()
            .api_seen
            .iter()
            .map(|seen| {
                let (path, query) = split(&seen.path_and_query);
                (seen.method.clone(), path, query)
            })
            .collect()
    }

    /// The keys of the component's in-flight rows, read from its store.
    fn inflight_keys(&self) -> Vec<String> {
        self.provider
            .component
            .kv_store_for_testing()
            .scan_for_testing(INFLIGHT_NS)
            .into_iter()
            .map(|(key, _)| String::from_utf8(key).expect("a UTF-8 key"))
            .collect()
    }
}

/// Weather for a place geocodes the place, then looks up the weather at the
/// first result's location: two nested calls, each a keyed `GET` on the
/// stub, and one compact answer naming the resolved place.
#[tokio::test]
async fn weather_for_a_place_geocodes_then_looks_up() {
    let r = rig().await;
    r.answer(GEOCODE_ADDRESS, geocode_body().to_string());
    r.answer(CURRENT_CONDITIONS, current_body());
    let call = r.call_for_alice(
        "Weather",
        json!({ "kind": "current", "place": "Mountain View" }),
    );
    let id = r.requested_id().await;

    // Step 1: the geocode, issued serving the request.
    r.serve().await;
    let nested = r.owed(&r.executor_sub(), 1).await;
    assert_eq!(nested[0]["tool"], "api-request");
    assert_eq!(nested[0]["serving"], id.as_str());
    assert_eq!(nested[0]["call_id"], format!("{id}/1"));
    r.executor.drain_step().await;
    r.owed(&r.provider_sub, 1).await;
    assert_eq!(r.inflight_keys(), [format!("{id}/1")]);

    // Step 2: the weather lookup at the geocoded location.
    r.serve().await;
    assert_eq!(r.inflight_keys(), [format!("{id}/2")]);
    let result = r.run_nested().await;
    assert_eq!(result["call_id"], format!("{id}/2"));
    r.serve().await;

    let answer = call.await.unwrap().expect("the component answered");
    assert_eq!(
        answer,
        json!({
            "location": { "lat": 37.4220, "lng": -122.0841, "place": "Mountain View, CA, USA" },
            "time": "2026-10-30T17:00:00Z",
            "timezone": "America/Los_Angeles",
            "units": { "temperature": "°C", "speed": "km/h", "distance": "km", "precipitation": "mm" },
            "condition": "Sunny",
            "humidity_percent": 40,
            "temperature": 18.5,
            "feels_like": 18.0,
            "is_daytime": true,
        })
    );

    let seen = r.seen();
    assert_eq!(
        seen,
        [
            (
                "GET".to_string(),
                GEOCODE_ADDRESS.to_string(),
                pairs(&[("addressQuery", "Mountain View")]),
            ),
            (
                "GET".to_string(),
                CURRENT_CONDITIONS.to_string(),
                pairs(&[
                    ("location.latitude", "37.422"),
                    ("location.longitude", "-122.0841"),
                    ("unitsSystem", "METRIC"),
                ]),
            ),
        ]
    );
    let stub = r.stub.lock().unwrap();
    for request in &stub.api_seen {
        assert_eq!(
            request.headers.get("x-api-key").unwrap(),
            testutil::KEY,
            "{}",
            request.path_and_query
        );
    }
    assert_eq!(
        stub.api_seen[0].headers.get("x-goog-fieldmask").unwrap(),
        "results.placeId,results.location,results.formattedAddress,results.granularity,results.types"
    );
    drop(stub);
    assert!(
        r.inflight_keys().is_empty(),
        "the answered request left no row"
    );
    assert!(r.runtime.serving(PROVIDER, &id).is_none());
}

/// Geocode by coordinates is one reverse lookup, answered with Google's
/// results compacted.
#[tokio::test]
async fn geocode_reverse_answers_compactly() {
    let r = rig().await;
    r.answer(GEOCODE_LOCATION, geocode_body().to_string());
    let call = r.call_for_alice("Geocode", json!({ "lat": 37.422, "lng": -122.0841 }));
    r.requested_id().await;
    r.serve().await;
    r.run_nested().await;
    r.serve().await;

    let answer = call.await.unwrap().expect("the component answered");
    assert_eq!(
        answer,
        json!({
            "results": [{
                "address": "Mountain View, CA, USA",
                "lat": 37.4220,
                "lng": -122.0841,
                "place_id": "ChIJiQHsW0m3j4ARm69rRkrUF3w",
                "granularity": "APPROXIMATE",
                "types": ["locality", "political"],
            }],
        })
    );
    assert_eq!(
        r.seen(),
        [(
            "GET".to_string(),
            GEOCODE_LOCATION.to_string(),
            pairs(&[
                ("location.latitude", "37.422"),
                ("location.longitude", "-122.0841"),
            ]),
        )]
    );
}

/// Google's error answer is the tool's error: its status and message, and
/// nothing else of its body.
#[tokio::test]
async fn a_google_error_body_is_the_tools_error() {
    let r = rig().await;
    {
        let mut stub = r.stub.lock().unwrap();
        stub.api_statuses.push_back(403);
        stub.api_body = json!({
            "error": {
                "code": 403,
                "message": "Geocoding API has not been used in project 1234 before or it is disabled.",
                "status": "PERMISSION_DENIED",
                "details": [{ "reason": "SERVICE_DISABLED", "metadata": { "consumer": "projects/1234" } }],
            },
        })
        .to_string()
        .into_bytes();
    }
    let call = r.call_for_alice("Geocode", json!({ "address": "Mountain View" }));
    r.requested_id().await;
    r.serve().await;
    r.run_nested().await;
    r.serve().await;

    assert_eq!(
        call.await.unwrap(),
        Err(ToolError::Internal(
            "Google PERMISSION_DENIED (HTTP 403): Geocoding API has not been used in project 1234 \
             before or it is disabled."
                .to_string()
        ))
    );
}

/// An answer the key provider cut at its `max_response_bytes` is not
/// compacted: the request is answered `internal`, naming the cut.
#[tokio::test]
async fn a_truncated_answer_is_internal() {
    let r = rig_with(CLAUSES, |key| key.raw.max_response_bytes = 64).await;
    r.answer(GEOCODE_ADDRESS, geocode_body().to_string());
    let call = r.call_for_alice("Geocode", json!({ "address": "Mountain View" }));
    r.requested_id().await;
    r.serve().await;
    let result = r.run_nested().await;
    assert_eq!(result["outcome"]["ok"]["truncated"], true, "{result}");
    r.serve().await;

    let answer = call.await.unwrap();
    let Err(ToolError::Internal(detail)) = answer else {
        panic!("expected an internal answer, got {answer:?}");
    };
    assert!(
        detail.starts_with("Google's answer (")
            && detail.ends_with("exceeded the key provider's max_response_bytes and was cut"),
        "{detail}"
    );
}

/// An in-flight row past its TTL is swept by the next activation, and a
/// result whose row is gone — here a second delivery of one already
/// answered — is ignored without failing the activation it arrives in.
#[tokio::test]
async fn a_stale_result_is_ignored_and_a_swept_row_is_gone() {
    let r = rig().await;
    r.answer(GEOCODE_ADDRESS, geocode_body().to_string());
    // A row a request lost to a restart left behind, issued long ago.
    const ABANDONED: &str = "00000000000000000000000000000000/1";
    r.provider.component.kv_store_for_testing().put_for_testing(
        INFLIGHT_NS,
        ABANDONED.as_bytes(),
        br#"{"issued_at_ms":0}"#,
    );
    assert_eq!(r.inflight_keys(), [ABANDONED]);

    // The first activation sweeps the abandoned row and records its own.
    let first = r.call_for_alice("Geocode", json!({ "address": "Mountain View" }));
    let first_id = r.requested_id().await;
    r.serve().await;
    assert_eq!(r.inflight_keys(), [format!("{first_id}/1")]);
    let result = r.run_nested().await;
    r.serve().await;
    first.await.unwrap().expect("the first request is answered");
    assert!(r.inflight_keys().is_empty());

    // The same result again, beside a new request: the duplicate finds no
    // row and is ignored, and the activation goes on to serve the request.
    let second = r.call_for_alice("Geocode", json!({ "address": "Mountain View" }));
    let second_id = r.requested_id().await;
    testutils::insert_bus_message(
        &r.messenger,
        &r.provider_results,
        &result.to_string(),
        ChannelScheme::Brenn,
    )
    .await;
    r.owed(&r.provider_sub, 2).await;
    r.serve().await;
    assert_eq!(r.inflight_keys(), [format!("{second_id}/1")]);
    r.run_nested().await;
    r.serve().await;
    let answer = second
        .await
        .unwrap()
        .expect("the second request is answered");
    assert_eq!(answer["results"][0]["address"], "Mountain View, CA, USA");
    assert_eq!(r.seen().len(), 2, "the duplicate reached no API");
}

/// A nested call the instance's grant does not admit is refused when it is
/// queued: the request is answered `internal` with Brenn's text and the
/// clause guidance, and nothing reaches the API.
#[tokio::test]
async fn a_clause_miss_is_internal_could_not_be_issued() {
    let r = rig_without_clause(GEOCODE_ADDRESS).await;
    let call = r.call_for_alice("Geocode", json!({ "address": "Mountain View" }));
    r.requested_id().await;
    r.serve().await;

    assert_could_not_be_issued(call.await.unwrap());
    r.owed(&r.executor_sub(), 0).await;
    assert!(r.seen().is_empty(), "a refused call reaches no API");
    assert!(r.inflight_keys().is_empty(), "a refused call leaves no row");
}

/// A place-based Weather call whose lookup the grant does not admit is refused
/// at its second step: the geocode runs, its row is deleted, and the request is
/// answered `internal` with nothing queued and no row left for the lookup.
#[tokio::test]
async fn a_clause_miss_at_the_second_step_is_internal_could_not_be_issued() {
    let r = rig_without_clause(CURRENT_CONDITIONS).await;
    r.answer(GEOCODE_ADDRESS, geocode_body().to_string());
    let call = r.call_for_alice(
        "Weather",
        json!({ "kind": "current", "place": "Mountain View" }),
    );
    let id = r.requested_id().await;
    r.serve().await;
    r.run_nested().await;
    r.serve().await;

    assert_could_not_be_issued(call.await.unwrap());
    r.owed(&r.executor_sub(), 0).await;
    assert_eq!(
        r.seen(),
        [(
            "GET".to_string(),
            GEOCODE_ADDRESS.to_string(),
            pairs(&[("addressQuery", "Mountain View")]),
        )]
    );
    assert!(r.inflight_keys().is_empty(), "neither step left a row");
    assert!(r.runtime.serving(PROVIDER, &id).is_none());
}
