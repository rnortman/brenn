//! The wrapper's decisions, in `std` plus `serde`/`serde_json`: reading the
//! instance's `config`, validating a tool's arguments, building each Google
//! call as `api-request` arguments, reading `api-request`'s answer, and
//! compacting Google's JSON into the tool's result. Nothing here names the
//! guest SDK, so the whole module runs on the host under test. Tools are the
//! generated `Provided` enum, whose SDK-facing parts are wasm32-only.

// The host test's crate root is this file, so it declares the generated
// module itself; in the component, `lib.rs` does.
#[cfg(test)]
mod spec;

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::spec::Provided;

/// An in-flight row older than this is garbage: longer than the host's
/// 240 s provided-tool ceiling plus `api-request`'s 60 s HTTP timeout, so no
/// row whose result can still arrive is ever swept.
pub const INFLIGHT_TTL_MS: u64 = 300_000;

/// The most bytes a free-text argument may hold. Every body and query string
/// the wrapper builds from caller text therefore has a known upper size.
pub const TEXT_MAX_BYTES: usize = 1024;

/// A compact result larger than this is a bug in a size bound, answered
/// `internal` rather than sent.
pub const COMPACT_RESULT_CEILING: usize = 256 * 1024;

/// The most characters of a Google error message copied into a result.
const GOOGLE_MESSAGE_MAX_CHARS: usize = 1000;

/// Geocoding v4's field mask: only what the compact result carries.
const GEOCODE_FIELD_MASK: &str =
    "results.placeId,results.location,results.formattedAddress,results.granularity,results.types";

/// The most results `Geocode` returns, and its default.
const GEOCODE_MAX: u64 = 5;
const GEOCODE_DEFAULT_MAX: u64 = 3;

/// The unit system Google is asked to answer in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Units {
    Metric,
    Imperial,
}

/// The instance's `config` map, validated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Settings {
    /// The `api_key_provider` every call names.
    pub provider: String,
    pub weather_host: String,
    pub geocoding_host: String,
    pub places_host: String,
    pub routes_host: String,
    pub units: Units,
    /// Passed to Google as `languageCode` when set.
    pub language: Option<String>,
}

impl Settings {
    /// Read the map through `get`. A missing required key, an empty value, a
    /// host containing `/`, or a `units` other than `METRIC`/`IMPERIAL` is an
    /// error naming the key: nothing a misconfigured instance answered would
    /// be right.
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Settings, String> {
        let required = |key: &str| -> Result<String, String> {
            match get(key) {
                None => Err(format!("config {key} is required")),
                Some(value) if value.is_empty() => Err(format!("config {key} is empty")),
                Some(value) => Ok(value),
            }
        };
        let host = |key: &str| -> Result<String, String> {
            let value = required(key)?;
            if value.contains('/') {
                return Err(format!(
                    "config {key}: {value:?} is not a host[:port]; it contains `/`"
                ));
            }
            Ok(value)
        };
        let units = match get("units").as_deref() {
            None | Some("METRIC") => Units::Metric,
            Some("IMPERIAL") => Units::Imperial,
            Some(other) => {
                return Err(format!(
                    "config units: {other:?} is neither METRIC nor IMPERIAL"
                ));
            }
        };
        let language = match get("language") {
            Some(value) if value.is_empty() => return Err("config language is empty".to_string()),
            other => other,
        };
        Ok(Settings {
            provider: required("provider")?,
            weather_host: host("weather_host")?,
            geocoding_host: host("geocoding_host")?,
            places_host: host("places_host")?,
            routes_host: host("routes_host")?,
            units,
            language,
        })
    }
}

/// The error kinds a request is answered with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    Denied,
    InvalidArgs,
    Internal,
}

/// A request's error answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Failure {
    pub kind: ErrorKind,
    pub detail: String,
}

fn internal(detail: impl Into<String>) -> Failure {
    Failure {
        kind: ErrorKind::Internal,
        detail: detail.into(),
    }
}

/// The method, host and path of a nested call, for the messages that name it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Target {
    pub method: String,
    pub host: String,
    pub path: String,
}

/// One `api-request` call's arguments.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ApiCall {
    pub provider: String,
    pub method: String,
    pub host: String,
    pub path: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub query: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
}

impl ApiCall {
    pub fn target(&self) -> Target {
        Target {
            method: self.method.clone(),
            host: self.host.clone(),
            path: self.path.clone(),
        }
    }
}

/// A nested call to issue: the request's step it is, and its arguments.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Call {
    pub step: u8,
    pub api: ApiCall,
}

/// The correlation id of a request's nested call: `<pending id>/<step>`.
pub fn call_id(id: &str, step: u8) -> String {
    format!("{id}/{step}")
}

/// What the store holds for a request between the activation that issues its
/// nested call and the one that receives the answer, keyed by the call id.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InFlight {
    /// The pending id as the host spells it.
    pub id: String,
    #[serde(with = "provided_name")]
    pub tool: Provided,
    pub step: u8,
    /// The caller's arguments, already validated once.
    pub args: Value,
    /// What earlier steps learned that later steps need.
    pub derived: Map<String, Value>,
    /// The nested call outstanding.
    pub target: Target,
    pub issued_at_ms: u64,
}

impl InFlight {
    pub fn new(id: &str, tool: Provided, args: &Value, call: &Call, now_ms: u64) -> InFlight {
        InFlight {
            id: id.to_string(),
            tool,
            step: call.step,
            args: args.clone(),
            derived: Map::new(),
            target: call.api.target(),
            issued_at_ms: now_ms,
        }
    }

    pub fn call_id(&self) -> String {
        call_id(&self.id, self.step)
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("an InFlight row serializes")
    }

    /// A row this module did not write is a corrupt store.
    pub fn decode(bytes: &[u8]) -> Result<InFlight, String> {
        serde_json::from_slice(bytes).map_err(|e| format!("in-flight row does not parse: {e}"))
    }
}

/// Whether a stored in-flight row has outlived any result that could still
/// arrive. Only the row's issue time is read: a row that old is garbage
/// whatever else it holds, including a row a different build wrote in a shape
/// this one does not parse. A row with no readable issue time is a corrupt
/// store.
pub fn row_expired(bytes: &[u8], now_ms: u64) -> Result<bool, String> {
    #[derive(Deserialize)]
    struct Issued {
        issued_at_ms: u64,
    }
    let row: Issued = serde_json::from_slice(bytes)
        .map_err(|e| format!("in-flight row has no readable issue time: {e}"))?;
    Ok(row.issued_at_ms.saturating_add(INFLIGHT_TTL_MS) < now_ms)
}

/// The in-flight state a result for request `id`, correlated by `call_id`,
/// answers; `row` is what the store holds under `call_id`. No row means the
/// result is a duplicate or arrived after its row was swept, and is ignored.
/// A row belonging to another request is a host fault.
pub fn claim(id: &str, call_id: &str, row: Option<&[u8]>) -> Result<Option<InFlight>, String> {
    let Some(bytes) = row else {
        return Ok(None);
    };
    let state = InFlight::decode(bytes)?;
    if state.id != id {
        return Err(format!(
            "result {call_id} serves {id}, but its in-flight row is {}'s",
            state.id
        ));
    }
    Ok(Some(state))
}

/// A stored row names its tool by the registry name the specification gives
/// it; a name the specification no longer declares does not parse.
mod provided_name {
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serializer};

    use crate::spec::Provided;

    pub fn serialize<S: Serializer>(tool: &Provided, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(tool.name())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Provided, D::Error> {
        let name = String::deserialize(deserializer)?;
        Provided::from_name(&name)
            .ok_or_else(|| D::Error::custom(format!("no provided tool is named `{name}`")))
    }
}

/// What a request does next after its nested call answered.
#[derive(Clone, Debug, PartialEq)]
pub enum Next {
    Complete(Value),
    Fail(Failure),
}

type Args = Map<String, Value>;

fn object<'a>(args: &'a Value, allowed: &[&str]) -> Result<&'a Args, String> {
    let Value::Object(map) = args else {
        return Err("the arguments must be a JSON object".to_string());
    };
    if let Some(unknown) = map.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(format!(
            "unknown argument `{unknown}`; this tool takes {}",
            allowed
                .iter()
                .map(|key| format!("`{key}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    Ok(map)
}

/// A free-text argument, capped at [`TEXT_MAX_BYTES`] before anything else is
/// done with it.
fn text<'a>(args: &'a Args, key: &str) -> Result<Option<&'a str>, String> {
    match args.get(key) {
        None => Ok(None),
        Some(Value::String(value)) => {
            if value.len() > TEXT_MAX_BYTES {
                return Err(format!("`{key}` is longer than {TEXT_MAX_BYTES} bytes"));
            }
            if value.trim().is_empty() {
                return Err(format!("`{key}` is empty"));
            }
            Ok(Some(value))
        }
        Some(_) => Err(format!("`{key}` must be a string")),
    }
}

/// An integer argument in `min..=max`.
fn integer(args: &Args, key: &str, min: u64, max: u64) -> Result<Option<u64>, String> {
    match args.get(key) {
        None => Ok(None),
        Some(value) => match value.as_u64() {
            Some(n) if (min..=max).contains(&n) => Ok(Some(n)),
            _ => Err(format!("`{key}` must be an integer from {min} to {max}")),
        },
    }
}

/// A latitude and longitude in range.
fn checked(lat: f64, lng: f64, what: &str) -> Result<(f64, f64), String> {
    if !(-90.0..=90.0).contains(&lat) {
        return Err(format!("{what}: latitude {lat} is outside -90 to 90"));
    }
    if !(-180.0..=180.0).contains(&lng) {
        return Err(format!("{what}: longitude {lng} is outside -180 to 180"));
    }
    Ok((lat, lng))
}

/// `lat` and `lng` as numbers, both or neither.
fn coordinates(args: &Args) -> Result<Option<(f64, f64)>, String> {
    let number = |key: &str| -> Result<Option<f64>, String> {
        match args.get(key) {
            None => Ok(None),
            Some(value) => value
                .as_f64()
                .map(Some)
                .ok_or_else(|| format!("`{key}` must be a number")),
        }
    };
    match (number("lat")?, number("lng")?) {
        (None, None) => Ok(None),
        (Some(lat), Some(lng)) => checked(lat, lng, "`lat`/`lng`").map(Some),
        _ => Err("`lat` and `lng` go together; pass both or neither".to_string()),
    }
}

/// One decimal number as a coordinate text spells it: an optional sign,
/// digits, and an optional fraction. No exponent, no `inf`, no `NaN`.
fn decimal(part: &str) -> Option<f64> {
    let digits = part.strip_prefix(['+', '-']).unwrap_or(part);
    let (whole, fraction) = digits.split_once('.').unwrap_or((digits, "0"));
    let all_digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    if !all_digits(whole) || !all_digits(fraction) {
        return None;
    }
    part.parse().ok()
}

/// A text of the form `<lat>,<lng>`, read as coordinates. `Ok(None)` when the
/// text is not of that form; an error when it is but is out of range. Google's
/// Geocoding v4 does not resolve coordinates written as an address, so such a
/// text is never sent to it as one.
pub fn parse_lat_lng(text: &str, key: &str) -> Result<Option<(f64, f64)>, String> {
    let Some((lat, lng)) = text.split_once(',') else {
        return Ok(None);
    };
    match (decimal(lat.trim()), decimal(lng.trim())) {
        (Some(lat), Some(lng)) => checked(lat, lng, &format!("`{key}`")).map(Some),
        _ => Ok(None),
    }
}

/// Validate a request's arguments and build its first nested call. An error is
/// the request's `invalid_args` detail, naming the argument.
pub fn plan(tool: Provided, args: &Value, settings: &Settings) -> Result<Call, String> {
    match tool {
        Provided::Geocode => {
            let geocode = GeocodeArgs::parse(args)?;
            Ok(Call {
                step: 1,
                api: geocode_call(&geocode.query, settings),
            })
        }
    }
}

/// What a geocode looks up.
#[derive(Clone, Debug, PartialEq)]
enum GeocodeQuery {
    Address(String),
    Location(f64, f64),
}

#[derive(Clone, Debug, PartialEq)]
struct GeocodeArgs {
    query: GeocodeQuery,
    max: usize,
}

impl GeocodeArgs {
    fn parse(args: &Value) -> Result<GeocodeArgs, String> {
        let args = object(args, &["address", "lat", "lng", "max"])?;
        let address = text(args, "address")?;
        let location = coordinates(args)?;
        let max = integer(args, "max", 1, GEOCODE_MAX)?.unwrap_or(GEOCODE_DEFAULT_MAX);
        let query = match (address, location) {
            (Some(_), Some(_)) => {
                return Err("pass `address` or `lat` and `lng`, not both".to_string());
            }
            (None, None) => return Err("pass `address` or `lat` and `lng`".to_string()),
            (None, Some((lat, lng))) => GeocodeQuery::Location(lat, lng),
            (Some(address), None) => match parse_lat_lng(address, "address")? {
                Some((lat, lng)) => GeocodeQuery::Location(lat, lng),
                None => GeocodeQuery::Address(address.to_string()),
            },
        };
        Ok(GeocodeArgs {
            query,
            max: usize::try_from(max).expect("max is at most 5"),
        })
    }
}

/// A Geocoding v4 lookup: forward by `addressQuery`, reverse by location,
/// both as query parameters so the path is fixed and no caller text becomes a
/// path segment.
fn geocode_call(query: &GeocodeQuery, settings: &Settings) -> ApiCall {
    let mut params = BTreeMap::new();
    let path = match query {
        GeocodeQuery::Address(address) => {
            params.insert("addressQuery".to_string(), address.clone());
            "/v4/geocode/address"
        }
        GeocodeQuery::Location(lat, lng) => {
            params.insert("location.latitude".to_string(), lat.to_string());
            params.insert("location.longitude".to_string(), lng.to_string());
            "/v4/geocode/location"
        }
    };
    if let Some(language) = &settings.language {
        params.insert("languageCode".to_string(), language.clone());
    }
    ApiCall {
        provider: settings.provider.clone(),
        method: "GET".to_string(),
        host: settings.geocoding_host.clone(),
        path: path.to_string(),
        query: params,
        headers: BTreeMap::from([(
            "X-Goog-FieldMask".to_string(),
            GEOCODE_FIELD_MASK.to_string(),
        )]),
        body: None,
    }
}

/// A nested call's typed error, as the executor wrote it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NestedError {
    pub kind: String,
    pub detail: String,
}

/// `text` cut to `max` characters, ending in `…` when cut.
fn cut(text: &str, max: usize) -> String {
    match text.char_indices().nth(max) {
        None => text.to_string(),
        Some((at, _)) => format!("{}…", &text[..at]),
    }
}

/// The detail of a nested call that issued but could not be queued: Brenn's
/// refusal text plus the one sentence of operator guidance.
pub fn not_issued(refusal: &str) -> String {
    format!(
        "api-request could not be issued: {refusal}. If this names the clause, the instance's \
         `tool api-request` grant must admit the method, host and path."
    )
}

/// One line for the log: status, size and truncation, or the error kind.
/// Never a body or a header.
pub fn result_summary(outcome: &Result<Value, NestedError>) -> String {
    match outcome {
        Ok(value) => format!(
            "status {} bytes {} truncated {}",
            value.get("status").unwrap_or(&Value::Null),
            value.get("bytes").unwrap_or(&Value::Null),
            value.get("truncated").unwrap_or(&Value::Null),
        ),
        Err(error) => format!("error {}", error.kind),
    }
}

/// Google's JSON answer from an `api-request` result, or the request's error
/// answer. Only `error.status` and `error.message` are ever copied out of a
/// non-2xx body.
pub fn read_api_result(
    outcome: Result<Value, NestedError>,
    target: &Target,
) -> Result<Value, Failure> {
    let value = match outcome {
        Ok(value) => value,
        Err(error) => {
            let NestedError { kind, detail } = error;
            return Err(match kind.as_str() {
                "denied" => Failure {
                    kind: ErrorKind::Denied,
                    detail: format!(
                        "api-request refused the nested {} to {}{}: {detail}. A POST needs a \
                         request with a calling user (an agent or `brenn-tool`).",
                        target.method, target.host, target.path
                    ),
                },
                "invalid_args" => internal(format!(
                    "api-request rejected the wrapper's request: {detail}"
                )),
                "internal" => internal(format!("api-request: {detail}")),
                other => internal(format!("{other}: {detail}")),
            });
        }
    };
    let status = value
        .get("status")
        .and_then(Value::as_u64)
        .ok_or_else(|| internal("api-request's answer carries no status"))?;
    if value.get("truncated").and_then(Value::as_bool) == Some(true) {
        let size = match value.get("bytes").and_then(Value::as_u64) {
            Some(bytes) => format!("{bytes} bytes"),
            None => "of unstated length".to_string(),
        };
        return Err(internal(format!(
            "Google's answer ({size}) exceeded the key provider's max_response_bytes and was cut"
        )));
    }
    if value.get("body_base64").is_some() {
        return Err(internal(format!(
            "Google returned a non-text body ({status})"
        )));
    }
    if (300..400).contains(&status) {
        return Err(internal(format!(
            "Google answered {status} with a redirect, which Brenn does not follow"
        )));
    }
    let body = value
        .get("body")
        .and_then(Value::as_str)
        .ok_or_else(|| internal("api-request's answer carries no body"))?;
    if status >= 400 {
        return Err(google_error(status, body));
    }
    if !(200..300).contains(&status) {
        return Err(internal(format!(
            "Google answered HTTP {status}, which is neither a success nor an error"
        )));
    }
    serde_json::from_str(body).map_err(|_| {
        internal(format!(
            "Google returned HTTP {status} with a body that is not JSON"
        ))
    })
}

/// A 4xx/5xx answer: Google's status and message when the body is Google's
/// standard error shape, nothing of it otherwise. 400 and 404 are the caller's
/// arguments; every other status is the wrapper's or Google's.
fn google_error(status: u64, body: &str) -> Failure {
    let kind = if status == 400 || status == 404 {
        ErrorKind::InvalidArgs
    } else {
        ErrorKind::Internal
    };
    let parsed: Option<Value> = serde_json::from_str(body).ok();
    let error = parsed.as_ref().and_then(|body| body.get("error"));
    let field = |name: &str| error.and_then(|e| e.get(name)).and_then(Value::as_str);
    let detail = match (field("status"), field("message")) {
        (Some(google_status), Some(message)) => format!(
            "Google {google_status} (HTTP {status}): {}",
            cut(message, GOOGLE_MESSAGE_MAX_CHARS)
        ),
        _ => format!("Google HTTP {status}, body not in the standard error shape"),
    };
    Failure { kind, detail }
}

/// What the request does with its nested call's answer.
pub fn advance(state: &InFlight, outcome: Result<Value, NestedError>) -> Next {
    let body = match read_api_result(outcome, &state.target) {
        Ok(body) => body,
        Err(failure) => return Next::Fail(failure),
    };
    let compact = match (state.tool, state.step) {
        (Provided::Geocode, 1) => match GeocodeArgs::parse(&state.args) {
            Ok(args) => compact_geocode(&body, args.max),
            Err(reason) => Err(internal(format!(
                "stored geocode arguments no longer parse: {reason}"
            ))),
        },
        (tool, step) => Err(internal(format!(
            "in-flight row names step {step} of {}, which has no such step",
            tool.name()
        ))),
    };
    match compact {
        Ok(value) => bounded(value, COMPACT_RESULT_CEILING),
        Err(failure) => Next::Fail(failure),
    }
}

/// `value` as the request's answer, unless it serializes larger than
/// `ceiling` — a bound that was wrong, reported rather than sent.
pub fn bounded(value: Value, ceiling: usize) -> Next {
    let size = serde_json::to_vec(&value)
        .expect("a JSON value serializes")
        .len();
    if size > ceiling {
        return Next::Fail(internal(format!(
            "compact result of {size} bytes exceeds the wrapper's ceiling"
        )));
    }
    Next::Complete(value)
}

fn shape(api: &str) -> Failure {
    internal(format!("unexpected {api} API response shape"))
}

/// Copy `from[key]` into `to[name]` when present and not null; absent fields
/// are omitted, never invented.
fn copy(to: &mut Args, name: &str, from: Option<&Value>) {
    if let Some(value) = from.filter(|value| !value.is_null()) {
        to.insert(name.to_string(), value.clone());
    }
}

/// A Geocoding v4 answer as at most `max` results. Google omits `results`
/// when nothing matched, which is an empty list.
fn compact_geocode(body: &Value, max: usize) -> Result<Value, Failure> {
    let Value::Object(body) = body else {
        return Err(shape("Geocoding"));
    };
    let results: &[Value] = match body.get("results") {
        None => &[],
        Some(Value::Array(results)) => results,
        Some(_) => return Err(shape("Geocoding")),
    };
    let mut compact = Vec::new();
    for result in results.iter().take(max) {
        let Value::Object(result) = result else {
            return Err(shape("Geocoding"));
        };
        let mut out = Args::new();
        let location = result.get("location");
        copy(&mut out, "address", result.get("formattedAddress"));
        copy(&mut out, "lat", location.and_then(|l| l.get("latitude")));
        copy(&mut out, "lng", location.and_then(|l| l.get("longitude")));
        copy(&mut out, "place_id", result.get("placeId"));
        copy(&mut out, "granularity", result.get("granularity"));
        copy(&mut out, "types", result.get("types"));
        compact.push(Value::Object(out));
    }
    let mut answer = json!({ "results": compact });
    if results.len() > max {
        answer["results_omitted"] = json!(results.len() - max);
    }
    Ok(answer)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: BTreeMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |key| map.get(key).cloned()
    }

    const BASE: &[(&str, &str)] = &[
        ("provider", "maps"),
        ("weather_host", "weather.example.com"),
        ("geocoding_host", "geocode.example.com"),
        ("places_host", "places.example.com"),
        ("routes_host", "routes.example.com"),
    ];

    fn settings() -> Settings {
        Settings::from_lookup(lookup(BASE)).unwrap()
    }

    fn with(extra: &[(&'static str, &'static str)]) -> Vec<(&'static str, &'static str)> {
        let mut pairs: Vec<_> = BASE
            .iter()
            .filter(|(k, _)| !extra.iter().any(|(e, _)| e == k))
            .copied()
            .collect();
        pairs.extend_from_slice(extra);
        pairs
    }

    fn without(key: &str) -> Vec<(&'static str, &'static str)> {
        BASE.iter().filter(|(k, _)| *k != key).copied().collect()
    }

    #[test]
    fn settings_read_every_key() {
        let s = Settings::from_lookup(lookup(&with(&[("units", "IMPERIAL"), ("language", "de")])))
            .unwrap();
        assert_eq!(s.provider, "maps");
        assert_eq!(s.weather_host, "weather.example.com");
        assert_eq!(s.geocoding_host, "geocode.example.com");
        assert_eq!(s.places_host, "places.example.com");
        assert_eq!(s.routes_host, "routes.example.com");
        assert_eq!(s.units, Units::Imperial);
        assert_eq!(s.language.as_deref(), Some("de"));
    }

    #[test]
    fn settings_default_to_metric_and_no_language() {
        let s = settings();
        assert_eq!(s.units, Units::Metric);
        assert_eq!(s.language, None);
        let metric = Settings::from_lookup(lookup(&with(&[("units", "METRIC")]))).unwrap();
        assert_eq!(metric.units, Units::Metric);
    }

    #[test]
    fn settings_refuse_a_missing_required_key_by_name() {
        for key in [
            "provider",
            "weather_host",
            "geocoding_host",
            "places_host",
            "routes_host",
        ] {
            let err = Settings::from_lookup(lookup(&without(key))).unwrap_err();
            assert_eq!(err, format!("config {key} is required"));
        }
    }

    #[test]
    fn settings_refuse_bad_values() {
        let err = Settings::from_lookup(lookup(&with(&[("places_host", "")]))).unwrap_err();
        assert_eq!(err, "config places_host is empty");
        let err = Settings::from_lookup(lookup(&with(&[("routes_host", "routes.example.com/v2")])))
            .unwrap_err();
        assert!(err.starts_with("config routes_host:"), "{err}");
        let err = Settings::from_lookup(lookup(&with(&[("units", "metric")]))).unwrap_err();
        assert!(err.starts_with("config units:"), "{err}");
        let err = Settings::from_lookup(lookup(&with(&[("language", "")]))).unwrap_err();
        assert_eq!(err, "config language is empty");
    }

    fn geocode(args: Value) -> Result<ApiCall, String> {
        plan(Provided::Geocode, &args, &settings()).map(|call| {
            assert_eq!(call.step, 1);
            call.api
        })
    }

    fn query(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn geocode_forward_is_a_query_on_a_fixed_path() {
        let call = geocode(json!({ "address": "1600 Amphitheatre Pkwy, Mountain View" })).unwrap();
        assert_eq!(call.provider, "maps");
        assert_eq!(call.method, "GET");
        assert_eq!(call.host, "geocode.example.com");
        assert_eq!(call.path, "/v4/geocode/address");
        assert_eq!(
            call.query,
            query(&[("addressQuery", "1600 Amphitheatre Pkwy, Mountain View")])
        );
        assert_eq!(
            call.headers,
            query(&[("X-Goog-FieldMask", GEOCODE_FIELD_MASK)])
        );
        assert_eq!(call.body, None);
    }

    #[test]
    fn geocode_reverse_sends_the_location() {
        let call = geocode(json!({ "lat": 37.422, "lng": -122.0841 })).unwrap();
        assert_eq!(call.path, "/v4/geocode/location");
        assert_eq!(
            call.query,
            query(&[
                ("location.latitude", "37.422"),
                ("location.longitude", "-122.0841")
            ])
        );
        assert_eq!(
            call.headers,
            query(&[("X-Goog-FieldMask", GEOCODE_FIELD_MASK)])
        );
    }

    #[test]
    fn geocode_reads_a_coordinate_address_as_a_location() {
        let call = geocode(json!({ "address": " 37.422 , -122.0841" })).unwrap();
        assert_eq!(call.path, "/v4/geocode/location");
        assert_eq!(call.query["location.latitude"], "37.422");
        assert_eq!(call.query["location.longitude"], "-122.0841");
        // Not the coordinate form: geocoded as text.
        for address in ["1e5,3", "inf,0", "12,Main St", "37.4.2,1"] {
            let call = geocode(json!({ "address": address })).unwrap();
            assert_eq!(call.path, "/v4/geocode/address", "{address}");
        }
        let err = geocode(json!({ "address": "91,0" })).unwrap_err();
        assert!(err.contains("latitude 91"), "{err}");
    }

    #[test]
    fn geocode_passes_the_language() {
        let s = Settings::from_lookup(lookup(&with(&[("language", "fr")]))).unwrap();
        let call = plan(Provided::Geocode, &json!({ "address": "Paris" }), &s).unwrap();
        assert_eq!(call.api.query["languageCode"], "fr");
    }

    #[test]
    fn geocode_serializes_as_api_request_args() {
        let call = geocode(json!({ "address": "Paris" })).unwrap();
        assert_eq!(
            serde_json::to_value(&call).unwrap(),
            json!({
                "provider": "maps",
                "method": "GET",
                "host": "geocode.example.com",
                "path": "/v4/geocode/address",
                "query": { "addressQuery": "Paris" },
                "headers": { "X-Goog-FieldMask": GEOCODE_FIELD_MASK },
            })
        );
    }

    #[test]
    fn geocode_refuses_bad_arguments_by_name() {
        let cases = [
            (json!({}), "pass `address` or `lat` and `lng`"),
            (
                json!({ "address": "Paris", "lat": 1, "lng": 2 }),
                "pass `address` or `lat` and `lng`, not both",
            ),
            (
                json!({ "lat": 1 }),
                "`lat` and `lng` go together; pass both or neither",
            ),
            (json!({ "lat": "1", "lng": 2 }), "`lat` must be a number"),
            (json!({ "address": 7 }), "`address` must be a string"),
            (json!({ "address": "  " }), "`address` is empty"),
            (
                json!({ "address": "Paris", "max": 0 }),
                "`max` must be an integer from 1 to 5",
            ),
            (
                json!({ "address": "Paris", "max": 6 }),
                "`max` must be an integer from 1 to 5",
            ),
            (
                json!({ "address": "Paris", "max": 2.5 }),
                "`max` must be an integer from 1 to 5",
            ),
            (json!(["Paris"]), "the arguments must be a JSON object"),
        ];
        for (args, want) in cases {
            assert_eq!(geocode(args.clone()).unwrap_err(), want, "{args}");
        }
        let err = geocode(json!({ "address": "Paris", "near": "x" })).unwrap_err();
        assert!(err.starts_with("unknown argument `near`"), "{err}");
        let err = geocode(json!({ "lat": 1, "lng": 181 })).unwrap_err();
        assert!(err.contains("longitude 181"), "{err}");
    }

    #[test]
    fn geocode_caps_the_address_in_bytes() {
        // Multi-byte characters: the cap is UTF-8 bytes, not characters.
        let at_cap = "é".repeat(TEXT_MAX_BYTES / 2);
        assert_eq!(at_cap.len(), TEXT_MAX_BYTES);
        assert!(geocode(json!({ "address": at_cap })).is_ok());
        let over = format!("{at_cap}a");
        assert_eq!(
            geocode(json!({ "address": over })).unwrap_err(),
            "`address` is longer than 1024 bytes"
        );
    }

    fn target() -> Target {
        Target {
            method: "POST".to_string(),
            host: "places.example.com".to_string(),
            path: "/v1/places:searchText".to_string(),
        }
    }

    fn nested(kind: &str, detail: &str) -> Result<Value, NestedError> {
        Err(NestedError {
            kind: kind.to_string(),
            detail: detail.to_string(),
        })
    }

    fn http(status: u64, body: &str) -> Result<Value, NestedError> {
        Ok(json!({
            "status": status,
            "headers": { "content-type": "application/json" },
            "bytes": body.len(),
            "truncated": false,
            "body": body,
        }))
    }

    fn read(outcome: Result<Value, NestedError>) -> Result<Value, Failure> {
        read_api_result(outcome, &target())
    }

    fn failure(kind: ErrorKind, detail: &str) -> Failure {
        Failure {
            kind,
            detail: detail.to_string(),
        }
    }

    #[test]
    fn a_denied_nested_call_is_denied_naming_it() {
        assert_eq!(
            read(nested("denied", "a component may only GET or HEAD")),
            Err(failure(
                ErrorKind::Denied,
                "api-request refused the nested POST to places.example.com/v1/places:searchText: \
                 a component may only GET or HEAD. A POST needs a request with a calling user \
                 (an agent or `brenn-tool`)."
            ))
        );
    }

    #[test]
    fn other_nested_errors_are_internal() {
        assert_eq!(
            read(nested("invalid_args", "bad query")),
            Err(failure(
                ErrorKind::Internal,
                "api-request rejected the wrapper's request: bad query"
            ))
        );
        assert_eq!(
            read(nested("internal", "timed out")),
            Err(failure(ErrorKind::Internal, "api-request: timed out"))
        );
        assert_eq!(
            read(nested("rate_limited", "slow down")),
            Err(failure(ErrorKind::Internal, "rate_limited: slow down"))
        );
    }

    #[test]
    fn a_truncated_answer_is_internal() {
        let outcome = Ok(json!({
            "status": 200, "headers": {}, "bytes": 300000, "truncated": true, "body": "{\"res",
        }));
        assert_eq!(
            read(outcome),
            Err(failure(
                ErrorKind::Internal,
                "Google's answer (300000 bytes) exceeded the key provider's max_response_bytes \
                 and was cut"
            ))
        );
        let unstated = Ok(json!({
            "status": 200, "headers": {}, "bytes": null, "truncated": true, "body": "",
        }));
        assert!(
            read(unstated)
                .unwrap_err()
                .detail
                .contains("(of unstated length)")
        );
    }

    #[test]
    fn a_binary_body_is_internal() {
        let outcome = Ok(json!({
            "status": 200, "headers": {}, "bytes": 3, "truncated": false, "body_base64": "AAEC",
        }));
        assert_eq!(
            read(outcome),
            Err(failure(
                ErrorKind::Internal,
                "Google returned a non-text body (200)"
            ))
        );
    }

    #[test]
    fn a_redirect_is_internal() {
        assert_eq!(
            read(http(302, "")),
            Err(failure(
                ErrorKind::Internal,
                "Google answered 302 with a redirect, which Brenn does not follow"
            ))
        );
    }

    fn google(code: u64, status: &str, message: &str) -> String {
        json!({ "error": { "code": code, "status": status, "message": message, "details": [{ "secret": "x" }] } })
            .to_string()
    }

    #[test]
    fn a_google_error_carries_its_status_and_message_only() {
        assert_eq!(
            read(http(400, &google(400, "INVALID_ARGUMENT", "bad location"))),
            Err(failure(
                ErrorKind::InvalidArgs,
                "Google INVALID_ARGUMENT (HTTP 400): bad location"
            ))
        );
        assert_eq!(
            read(http(404, &google(404, "NOT_FOUND", "no such place"))),
            Err(failure(
                ErrorKind::InvalidArgs,
                "Google NOT_FOUND (HTTP 404): no such place"
            ))
        );
        assert_eq!(
            read(http(
                403,
                &google(403, "PERMISSION_DENIED", "API not enabled")
            )),
            Err(failure(
                ErrorKind::Internal,
                "Google PERMISSION_DENIED (HTTP 403): API not enabled"
            ))
        );
        assert_eq!(
            read(http(503, &google(503, "UNAVAILABLE", "later"))),
            Err(failure(
                ErrorKind::Internal,
                "Google UNAVAILABLE (HTTP 503): later"
            ))
        );
    }

    #[test]
    fn a_long_google_message_is_cut() {
        let message = "x".repeat(GOOGLE_MESSAGE_MAX_CHARS + 50);
        let detail = read(http(500, &google(500, "INTERNAL", &message)))
            .unwrap_err()
            .detail;
        assert_eq!(
            detail,
            format!(
                "Google INTERNAL (HTTP 500): {}…",
                "x".repeat(GOOGLE_MESSAGE_MAX_CHARS)
            )
        );
    }

    #[test]
    fn an_error_body_not_in_googles_shape_is_not_copied() {
        assert_eq!(
            read(http(403, "<html>key=SECRET</html>")),
            Err(failure(
                ErrorKind::Internal,
                "Google HTTP 403, body not in the standard error shape"
            ))
        );
        assert_eq!(
            read(http(400, "{\"error\": \"SECRET\"}")),
            Err(failure(
                ErrorKind::InvalidArgs,
                "Google HTTP 400, body not in the standard error shape"
            ))
        );
    }

    #[test]
    fn a_success_body_must_be_json() {
        assert_eq!(
            read(http(200, "not json")),
            Err(failure(
                ErrorKind::Internal,
                "Google returned HTTP 200 with a body that is not JSON"
            ))
        );
        assert_eq!(read(http(200, "{\"a\": 1}")), Ok(json!({ "a": 1 })));
    }

    #[test]
    fn an_answer_without_status_or_body_is_internal() {
        assert_eq!(
            read(Ok(json!({ "truncated": false }))),
            Err(failure(
                ErrorKind::Internal,
                "api-request's answer carries no status"
            ))
        );
        assert_eq!(
            read(Ok(json!({ "status": 200, "truncated": false }))),
            Err(failure(
                ErrorKind::Internal,
                "api-request's answer carries no body"
            ))
        );
        assert!(read(http(101, "")).unwrap_err().detail.contains("HTTP 101"));
    }

    #[test]
    fn the_summary_names_status_size_and_truncation_only() {
        assert_eq!(
            result_summary(&http(200, "{\"secret\": 1}")),
            "status 200 bytes 13 truncated false"
        );
        assert_eq!(result_summary(&nested("denied", "secret")), "error denied");
    }

    #[test]
    fn not_issued_carries_brenns_text_and_the_guidance() {
        let detail = not_issued("call-async api-request: denied: no clause admits GET");
        assert!(detail.starts_with(
            "api-request could not be issued: call-async api-request: denied: no clause admits GET."
        ));
        assert!(detail.ends_with("must admit the method, host and path."));
    }

    fn geocode_state(args: Value) -> InFlight {
        let call = plan(Provided::Geocode, &args, &settings()).unwrap();
        InFlight::new(&"a".repeat(32), Provided::Geocode, &args, &call, 1_000)
    }

    const GEOCODE_BODY: &str = r#"{
      "results": [
        { "placeId": "ChIJ1", "location": { "latitude": 37.42, "longitude": -122.08 },
          "formattedAddress": "1600 Amphitheatre Pkwy, Mountain View, CA 94043, USA",
          "granularity": "ROOFTOP", "types": ["street_address"] },
        { "placeId": "ChIJ2", "location": { "latitude": 37.4, "longitude": -122.1 },
          "formattedAddress": "Mountain View, CA, USA", "granularity": "APPROXIMATE",
          "types": ["locality", "political"] },
        { "placeId": "ChIJ3", "formattedAddress": "Santa Clara County, CA, USA" }
      ]
    }"#;

    #[test]
    fn geocode_compacts_googles_answer() {
        let state = geocode_state(json!({ "address": "Googleplex", "max": 2 }));
        assert_eq!(
            advance(&state, http(200, GEOCODE_BODY)),
            Next::Complete(json!({
                "results": [
                    { "address": "1600 Amphitheatre Pkwy, Mountain View, CA 94043, USA",
                      "lat": 37.42, "lng": -122.08, "place_id": "ChIJ1",
                      "granularity": "ROOFTOP", "types": ["street_address"] },
                    { "address": "Mountain View, CA, USA", "lat": 37.4, "lng": -122.1,
                      "place_id": "ChIJ2", "granularity": "APPROXIMATE",
                      "types": ["locality", "political"] },
                ],
                "results_omitted": 1,
            }))
        );
    }

    #[test]
    fn geocode_omits_absent_fields_and_keeps_the_default_max() {
        let state = geocode_state(json!({ "address": "Santa Clara County" }));
        let Next::Complete(answer) = advance(&state, http(200, GEOCODE_BODY)) else {
            panic!("expected an answer");
        };
        assert_eq!(answer["results"].as_array().unwrap().len(), 3);
        assert_eq!(
            answer["results"][2],
            json!({ "address": "Santa Clara County, CA, USA", "place_id": "ChIJ3" })
        );
        assert!(answer.get("results_omitted").is_none());
    }

    #[test]
    fn geocode_with_no_match_is_an_empty_list() {
        let state = geocode_state(json!({ "lat": 0, "lng": 0 }));
        assert_eq!(
            advance(&state, http(200, "{}")),
            Next::Complete(json!({ "results": [] }))
        );
    }

    #[test]
    fn geocode_refuses_an_unexpected_shape() {
        let state = geocode_state(json!({ "address": "Paris" }));
        for body in ["[]", "{\"results\": {}}", "{\"results\": [1]}"] {
            assert_eq!(
                advance(&state, http(200, body)),
                Next::Fail(failure(
                    ErrorKind::Internal,
                    "unexpected Geocoding API response shape"
                )),
                "{body}"
            );
        }
    }

    #[test]
    fn advance_passes_a_failed_call_through() {
        let state = geocode_state(json!({ "address": "Paris" }));
        assert_eq!(
            advance(
                &state,
                http(403, &google(403, "PERMISSION_DENIED", "no key"))
            ),
            Next::Fail(failure(
                ErrorKind::Internal,
                "Google PERMISSION_DENIED (HTTP 403): no key"
            ))
        );
    }

    #[test]
    fn advance_refuses_a_step_the_tool_does_not_have() {
        let mut state = geocode_state(json!({ "address": "Paris" }));
        state.step = 2;
        assert_eq!(
            advance(&state, http(200, "{}")),
            Next::Fail(failure(
                ErrorKind::Internal,
                "in-flight row names step 2 of geocode, which has no such step"
            ))
        );
    }

    #[test]
    fn the_result_ceiling_reports_rather_than_sends() {
        let value = json!({ "text": "x".repeat(100) });
        assert_eq!(bounded(value.clone(), 1_000), Next::Complete(value.clone()));
        let size = serde_json::to_vec(&value).unwrap().len();
        assert_eq!(
            bounded(value, 50),
            Next::Fail(failure(
                ErrorKind::Internal,
                &format!("compact result of {size} bytes exceeds the wrapper's ceiling")
            ))
        );
    }

    #[test]
    fn in_flight_round_trips_with_a_string_id() {
        let state = geocode_state(json!({ "address": "Paris", "max": 1 }));
        assert_eq!(state.call_id(), format!("{}/1", "a".repeat(32)));
        assert!(state.call_id().len() <= 128);
        assert_eq!(
            state.target,
            Target {
                method: "GET".to_string(),
                host: "geocode.example.com".to_string(),
                path: "/v4/geocode/address".to_string(),
            }
        );
        let bytes = state.encode();
        let wire: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(wire["id"], json!("a".repeat(32)));
        assert_eq!(wire["tool"], json!("geocode"));
        assert_eq!(InFlight::decode(&bytes).unwrap(), state);
        assert!(InFlight::decode(b"{}").is_err());
    }

    #[test]
    fn a_row_expires_only_after_the_ttl() {
        let row = geocode_state(json!({ "address": "Paris" })).encode();
        assert_eq!(row_expired(&row, 1_000), Ok(false));
        assert_eq!(row_expired(&row, 1_000 + INFLIGHT_TTL_MS), Ok(false));
        assert_eq!(row_expired(&row, 1_000 + INFLIGHT_TTL_MS + 1), Ok(true));
    }

    #[test]
    fn a_row_another_build_wrote_still_expires() {
        let state = geocode_state(json!({ "address": "Paris" }));
        let mut wire: Value = serde_json::from_slice(&state.encode()).unwrap();
        wire["tool"] = json!("no-such-tool");
        wire["added_later"] = json!(1);
        wire.as_object_mut().unwrap().remove("target");
        let row = serde_json::to_vec(&wire).unwrap();
        assert!(InFlight::decode(&row).is_err());
        assert_eq!(row_expired(&row, 1_000 + INFLIGHT_TTL_MS), Ok(false));
        assert_eq!(row_expired(&row, 1_000 + INFLIGHT_TTL_MS + 1), Ok(true));
    }

    #[test]
    fn a_row_with_no_issue_time_is_a_corrupt_store() {
        for row in [&b"{}"[..], b"not json", br#"{"issued_at_ms":"soon"}"#] {
            let error = row_expired(row, 0).unwrap_err();
            assert!(
                error.starts_with("in-flight row has no readable issue time: "),
                "{error}"
            );
        }
    }

    #[test]
    fn a_result_claims_its_requests_row() {
        let state = geocode_state(json!({ "address": "Paris" }));
        let call_id = state.call_id();
        assert_eq!(
            claim(&state.id, &call_id, Some(&state.encode())),
            Ok(Some(state.clone()))
        );
    }

    #[test]
    fn a_result_with_no_row_is_ignored() {
        assert_eq!(
            claim(&"a".repeat(32), &format!("{}/1", "a".repeat(32)), None),
            Ok(None)
        );
    }

    #[test]
    fn a_result_for_another_requests_row_is_a_host_fault() {
        let state = geocode_state(json!({ "address": "Paris" }));
        let other = "b".repeat(32);
        assert_eq!(
            claim(&other, &state.call_id(), Some(&state.encode())),
            Err(format!(
                "result {}/1 serves {other}, but its in-flight row is {}'s",
                "a".repeat(32),
                "a".repeat(32)
            ))
        );
    }

    #[test]
    fn a_result_whose_row_does_not_parse_is_a_corrupt_store() {
        let error = claim(&"a".repeat(32), "x/1", Some(b"{}")).unwrap_err();
        assert!(
            error.starts_with("in-flight row does not parse: "),
            "{error}"
        );
    }

    #[test]
    fn advance_refuses_stored_arguments_that_no_longer_parse() {
        let mut state = geocode_state(json!({ "address": "Paris" }));
        state.args = json!({ "address": "Paris", "max": 9 });
        assert_eq!(
            advance(&state, http(200, GEOCODE_BODY)),
            Next::Fail(failure(
                ErrorKind::Internal,
                "stored geocode arguments no longer parse: `max` must be an integer from 1 to 5"
            ))
        );
    }

    #[test]
    fn a_row_names_every_provided_tool_by_its_registry_name() {
        let state = geocode_state(json!({ "address": "Paris" }));
        for tool in Provided::ALL {
            let row = InFlight {
                tool,
                ..state.clone()
            };
            let wire: Value = serde_json::from_slice(&row.encode()).unwrap();
            assert_eq!(wire["tool"], json!(tool.name()));
            assert_eq!(InFlight::decode(&row.encode()).unwrap().tool, tool);
        }
    }

    #[test]
    fn a_row_naming_an_undeclared_tool_does_not_parse() {
        let state = geocode_state(json!({ "address": "Paris" }));
        let mut wire: Value = serde_json::from_slice(&state.encode()).unwrap();
        wire["tool"] = json!("Geocode");
        let error = InFlight::decode(&serde_json::to_vec(&wire).unwrap()).unwrap_err();
        assert!(
            error.contains("no provided tool is named `Geocode`"),
            "{error}"
        );
    }
}
