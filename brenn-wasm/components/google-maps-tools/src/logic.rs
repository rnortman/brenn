//! The wrapper's decisions, in `std`, `serde_json` and `brenn-provider-kit`:
//! reading the instance's `config`, validating a tool's arguments, building
//! each Google call as `api-request` arguments, reading `api-request`'s
//! answer, and compacting Google's JSON into the tool's result. Nothing here
//! names the guest SDK, so the whole module runs on the host under test.
//! Tools are the generated `Provided` enum, whose SDK-facing parts are
//! wasm32-only.

// The host test's crate root is this file, so it declares the generated
// module itself; in the component, `lib.rs` does.
#[cfg(test)]
mod spec;

use std::collections::BTreeMap;

use brenn_provider_kit::{
    Answer, ApiCall, Args, COMPACT_RESULT_CEILING, Call, ErrorKind, Failure, NestedError, Next,
    Query, QueryValue, Tool, Unreadable, bounded, capped, choice, copy, cut, flag,
    google_error_detail, host, integer, internal, no_such_step, object, query, read_api_result,
    required, stored,
};
use serde_json::{Value, json};

use crate::spec::Provided;

/// The most characters a free-text argument may hold. Every body and query
/// string the wrapper builds from caller text therefore has a known upper size.
pub const TEXT_MAX_CHARS: usize = 1024;

/// The most characters a `page` token may hold. Google's tokens are opaque and
/// run to a few hundred characters.
pub const PAGE_TOKEN_MAX_CHARS: usize = 4096;

/// The most characters a `place-details` `id` may hold.
pub const PLACE_ID_MAX_CHARS: usize = 1024;

/// What to narrow when a step's answer cannot be read whole: the route
/// SearchAlongRoute searches along, the places a search answers with, a place's
/// reviews, or the days of a daily forecast.
const ROUTE_HINT: &str = "search along a shorter route";
const PLACES_HINT: &str = "ask for fewer places with `max`";
const REVIEWS_HINT: &str = "ask without `reviews`";
const DAYS_HINT: &str = "ask for fewer `days`";

/// Geocoding v4's field mask: only what the compact result carries.
const GEOCODE_FIELD_MASK: &str =
    "results.placeId,results.location,results.formattedAddress,results.granularity,results.types";

/// The most results `Geocode` returns, and its default.
const GEOCODE_MAX: u64 = 5;
const GEOCODE_DEFAULT_MAX: u64 = 3;

/// Weather's steps: the geocode of a `place`, then the Weather lookup. A
/// request given coordinates starts at the lookup.
const WEATHER_GEOCODE_STEP: u8 = 1;
const WEATHER_LOOKUP_STEP: u8 = 2;

/// Google's hourly page size, which is also its maximum.
const HOURLY_PAGE_SIZE: u64 = 24;
/// The hourly horizon from the current hour: Google's range, and the default.
const HOURS_MAX: u64 = 240;
const HOURS_DEFAULT: u64 = 24;
/// The daily horizon: Google's range, and the default.
const DAYS_MAX: u64 = 10;
const DAYS_DEFAULT: u64 = 5;

/// The most alerts one answer carries, and the most UTF-8 bytes of an alert's
/// description, which sizes the answer against the byte-counted result
/// ceiling.
const ALERTS_MAX: usize = 20;
const ALERT_DESCRIPTION_MAX_BYTES: usize = 1000;
/// The most instructions and safety recommendations carried per alert.
const ALERT_ADVICE_MAX: usize = 5;

/// How Google splits a forecast day, stated once in a daily answer.
const DAY_PARTS: &str = "day 07:00–19:00, night 19:00–07:00 local";

/// The Routes API's field mask for `Directions`: the route, its legs and their
/// steps, no polyline, and `fallbackInfo`, which Google sets when it did not
/// compute the route in the routing mode asked for.
const DIRECTIONS_FIELD_MASK: &str = "routes.description,routes.warnings,routes.distanceMeters,\
routes.duration,routes.staticDuration,routes.legs.distanceMeters,routes.legs.duration,\
routes.legs.steps.distanceMeters,routes.legs.steps.staticDuration,routes.legs.steps.travelMode,\
routes.legs.steps.navigationInstruction,routes.legs.steps.transitDetails,fallbackInfo";
/// The Routes API path a route is computed at.
const COMPUTE_ROUTES_PATH: &str = "/directions/v2:computeRoutes";
/// The most steps a `Directions` answer carries across its legs, and the
/// default.
const DIRECTIONS_MAX_STEPS: u64 = 200;
const DIRECTIONS_DEFAULT_MAX_STEPS: u64 = 40;
/// The most route warnings carried.
const ROUTE_WARNINGS_MAX: usize = 10;
/// The prefix that makes a waypoint text a Places place id.
const PLACE_ID_PREFIX: &str = "place_id:";

/// Places text search's field mask: only what the compact result carries,
/// plus `nextPageToken`, which says whether Google holds more.
const PLACES_SEARCH_FIELD_MASK: &str = "places.id,places.displayName,places.formattedAddress,\
places.location,places.primaryTypeDisplayName,places.rating,places.userRatingCount,\
places.priceLevel,places.currentOpeningHours.openNow,places.businessStatus,nextPageToken";
/// The Places API path a text search posts to.
const SEARCH_TEXT_PATH: &str = "/v1/places:searchText";
/// PlacesSearch's steps: the geocode of a `near` place, then the search. A
/// request with no `near`, or `near` as coordinates, starts at the search.
const PLACES_GEOCODE_STEP: u8 = 1;
const PLACES_SEARCH_STEP: u8 = 2;
/// The most places one search answers with (Google's page size range), and
/// the default.
const PLACES_MAX: u64 = 20;
const PLACES_DEFAULT_MAX: u64 = 10;
/// The bias circle's radius in meters: the tool's range, and the default when
/// `near` is given without one.
const RADIUS_MAX_METERS: u64 = 50_000;
const RADIUS_DEFAULT_METERS: u64 = 5_000;

/// Place Details' field mask: what the compact result carries. Details masks
/// carry no `places.` prefix.
const PLACE_DETAILS_FIELD_MASK: &str = "id,displayName,formattedAddress,location,\
primaryTypeDisplayName,rating,userRatingCount,priceLevel,businessStatus,\
currentOpeningHours.openNow,regularOpeningHours.weekdayDescriptions,nationalPhoneNumber,\
internationalPhoneNumber,websiteUri,googleMapsUri";
/// What `reviews: true` adds to the mask: user-submitted content, billed at
/// Google's Enterprise + Atmosphere tier and refused on a Demo Key.
const PLACE_DETAILS_REVIEW_FIELDS: &str = "reviews,editorialSummary";
/// The Places API path a place's details are read at, before its id.
const PLACE_DETAILS_PATH: &str = "/v1/places/";
/// The most reviews carried (Google returns at most five), and the most UTF-8
/// bytes of each review's text, which sizes the answer against the
/// byte-counted result ceiling.
const REVIEWS_MAX: usize = 5;
const REVIEW_TEXT_MAX_BYTES: usize = 600;
/// The most opening-hours lines carried: one per weekday.
const WEEKDAYS: usize = 7;

/// SearchAlongRoute's route field mask: the polyline the search follows, the
/// route's size, and `fallbackInfo`, which Google sets when it did not
/// compute the route in the routing mode asked for.
const ALONG_ROUTE_FIELD_MASK: &str =
    "routes.polyline.encodedPolyline,routes.distanceMeters,routes.duration,fallbackInfo";
/// SearchAlongRoute's steps: the route, then the search along it.
const ALONG_ROUTE_ROUTE_STEP: u8 = 1;
const ALONG_ROUTE_SEARCH_STEP: u8 = 2;
/// The longest encoded polyline a search along a route is sent with. It is the
/// one field of unbounded size the wrapper ever puts into a nested call's
/// arguments; this leaves room under their 64 KiB cap for the rest of the
/// search body, whose caller text is capped at [`TEXT_MAX_CHARS`], and for
/// escaping the polyline's `\` twice at the one-in-64 rate Google's encoding
/// alphabet gives it. A polyline denser in `\` than that can exceed the args
/// cap, which the nested call refuses.
pub const POLYLINE_MAX_BYTES: usize = 48 * 1024;

/// The unit system Google is asked to answer in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Units {
    Metric,
    Imperial,
}

impl Units {
    /// Google's name for the system: the Weather API's `unitsSystem` and the
    /// Routes API's `units`.
    fn google(self) -> &'static str {
        match self {
            Units::Metric => "METRIC",
            Units::Imperial => "IMPERIAL",
        }
    }

    /// The unit a route's distances are stated in.
    fn distance_label(self) -> &'static str {
        match self {
            Units::Metric => "km",
            Units::Imperial => "mi",
        }
    }

    /// Meters in one [`Units::distance_label`].
    fn meters_per_distance_unit(self) -> f64 {
        match self {
            Units::Metric => 1000.0,
            Units::Imperial => 1609.344,
        }
    }

    /// The units a Weather answer's numbers are in, stated in the answer.
    fn weather_labels(self) -> Value {
        match self {
            Units::Metric => json!({
                "temperature": "°C", "speed": "km/h", "distance": "km", "precipitation": "mm",
            }),
            Units::Imperial => json!({
                "temperature": "°F", "speed": "mph", "distance": "mi", "precipitation": "in",
            }),
        }
    }
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
            provider: required(&get, "provider")?,
            weather_host: host(&get, "weather_host")?,
            geocoding_host: host(&get, "geocoding_host")?,
            places_host: host(&get, "places_host")?,
            routes_host: host(&get, "routes_host")?,
            units,
            language,
        })
    }
}

impl Tool for Provided {
    fn name(self) -> &'static str {
        Provided::name(self)
    }

    fn from_name(name: &str) -> Option<Provided> {
        Provided::from_name(name)
    }
}

/// A request's in-flight row, naming one of this component's tools.
pub type InFlight = brenn_provider_kit::InFlight<Provided>;

/// A free-text argument, capped at [`TEXT_MAX_CHARS`] before anything else is
/// done with it.
fn text<'a>(args: &'a Args, key: &str) -> Result<Option<&'a str>, String> {
    capped(args, key, TEXT_MAX_CHARS)
}

/// The largest latitude either side of the equator, in degrees.
const LAT_MAX: f64 = 90.0;
/// The largest longitude either side of the prime meridian, in degrees.
const LNG_MAX: f64 = 180.0;

/// A latitude and longitude in range.
fn checked(lat: f64, lng: f64, what: &str) -> Result<(f64, f64), String> {
    if !(-LAT_MAX..=LAT_MAX).contains(&lat) {
        return Err(format!(
            "{what}: latitude {lat} is outside -{LAT_MAX} to {LAT_MAX}"
        ));
    }
    if !(-LNG_MAX..=LNG_MAX).contains(&lng) {
        return Err(format!(
            "{what}: longitude {lng} is outside -{LNG_MAX} to {LNG_MAX}"
        ));
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
        Provided::Weather => {
            let weather = WeatherArgs::parse(args)?;
            Ok(match &weather.at {
                Where::Text(_) => Call {
                    step: WEATHER_GEOCODE_STEP,
                    api: geocode_call(&weather.at, settings),
                },
                Where::At(lat, lng) => Call {
                    step: WEATHER_LOOKUP_STEP,
                    api: weather_call(&weather, *lat, *lng, settings),
                },
            })
        }
        Provided::Directions => {
            let directions = DirectionsArgs::parse(args)?;
            Ok(Call {
                step: 1,
                api: compute_routes_call(
                    &directions.route,
                    directions.when.as_ref(),
                    RouteRequest::Directions,
                    settings,
                ),
            })
        }
        Provided::PlacesSearch => {
            let search = PlacesSearchArgs::parse(args)?;
            Ok(match &search.near {
                Some(near @ Where::Text(_)) => Call {
                    step: PLACES_GEOCODE_STEP,
                    api: geocode_call(near, settings),
                },
                Some(Where::At(lat, lng)) => Call {
                    step: PLACES_SEARCH_STEP,
                    api: places_search_call(&search, Some((*lat, *lng)), settings),
                },
                None => Call {
                    step: PLACES_SEARCH_STEP,
                    api: places_search_call(&search, None, settings),
                },
            })
        }
        Provided::PlaceDetails => {
            let details = PlaceDetailsArgs::parse(args)?;
            Ok(Call {
                step: 1,
                api: place_details_call(&details, settings),
            })
        }
        Provided::SearchAlongRoute => {
            let along = AlongRouteArgs::parse(args)?;
            Ok(Call {
                step: ALONG_ROUTE_ROUTE_STEP,
                api: compute_routes_call(&along.route, None, RouteRequest::Polyline, settings),
            })
        }
    }
}

/// Where a tool is asked about: coordinates, or a place text to geocode.
#[derive(Clone, Debug, PartialEq)]
enum Where {
    At(f64, f64),
    Text(String),
}

/// The text argument `key` or `lat` and `lng`, exactly one. A text of the
/// form `<lat>,<lng>` is coordinates.
fn place_or_coordinates(args: &Args, key: &str) -> Result<Where, String> {
    match (text(args, key)?, coordinates(args)?) {
        (Some(_), Some(_)) => Err(format!("pass `{key}` or `lat` and `lng`, not both")),
        (None, None) => Err(format!("pass `{key}` or `lat` and `lng`")),
        (None, Some((lat, lng))) => Ok(Where::At(lat, lng)),
        (Some(text), None) => place_text(text, key),
    }
}

/// The place text of argument `key`: coordinates when it has the form
/// `<lat>,<lng>`, otherwise a text to geocode.
fn place_text(text: &str, key: &str) -> Result<Where, String> {
    Ok(match parse_lat_lng(text, key)? {
        Some((lat, lng)) => Where::At(lat, lng),
        None => Where::Text(text.to_string()),
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WeatherKind {
    Current,
    Hourly,
    Daily,
    Alerts,
}

impl WeatherKind {
    /// Every kind, in the order the tool's `input_schema` lists them.
    const ALL: [WeatherKind; 4] = [
        WeatherKind::Current,
        WeatherKind::Hourly,
        WeatherKind::Daily,
        WeatherKind::Alerts,
    ];

    /// The tool's spelling.
    fn name(self) -> &'static str {
        match self {
            WeatherKind::Current => "current",
            WeatherKind::Hourly => "hourly",
            WeatherKind::Daily => "daily",
            WeatherKind::Alerts => "alerts",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct WeatherArgs {
    kind: WeatherKind,
    at: Where,
    /// The hourly horizon; read for `hourly` only.
    hours: u64,
    /// The daily horizon; read for `daily` only.
    days: u64,
    page: Option<String>,
}

impl WeatherArgs {
    /// Every argument the tool takes.
    const KEYS: &[&str] = &["kind", "lat", "lng", "place", "hours", "days", "page"];

    fn parse(args: &Value) -> Result<WeatherArgs, String> {
        let args = object(args, Self::KEYS)?;
        let Some(kind) = choice(args, "kind", &WeatherKind::ALL, WeatherKind::name)? else {
            return Err("`kind` is required".to_string());
        };
        let page = capped(args, "page", PAGE_TOKEN_MAX_CHARS)?;
        let at = place_or_coordinates(args, "place")?;
        let hours = integer(args, "hours", 1, HOURS_MAX)?;
        let days = integer(args, "days", 1, DAYS_MAX)?;
        if hours.is_some() && kind != WeatherKind::Hourly {
            return Err("`hours` applies to `kind: hourly` only".to_string());
        }
        if days.is_some() && kind != WeatherKind::Daily {
            return Err("`days` applies to `kind: daily` only".to_string());
        }
        if page.is_some() && !matches!(kind, WeatherKind::Hourly | WeatherKind::Alerts) {
            return Err("`page` applies to `kind: hourly` and `kind: alerts` only".to_string());
        }
        Ok(WeatherArgs {
            kind,
            at,
            hours: hours.unwrap_or(HOURS_DEFAULT),
            days: days.unwrap_or(DAYS_DEFAULT),
            page: page.map(str::to_string),
        })
    }
}

/// A Weather API lookup at a location: one page, never followed.
fn weather_call(args: &WeatherArgs, lat: f64, lng: f64, settings: &Settings) -> ApiCall {
    let mut params = query([
        ("location.latitude", lat.to_string()),
        ("location.longitude", lng.to_string()),
    ]);
    let mut set = |key: &str, value: String| params.insert(key.to_string(), QueryValue::One(value));
    let units = settings.units.google().to_string();
    let path = match args.kind {
        WeatherKind::Current => {
            set("unitsSystem", units);
            "/v1/currentConditions:lookup"
        }
        WeatherKind::Hourly => {
            set("unitsSystem", units);
            set("hours", args.hours.to_string());
            set("pageSize", HOURLY_PAGE_SIZE.to_string());
            "/v1/forecast/hours:lookup"
        }
        WeatherKind::Daily => {
            set("unitsSystem", units);
            set("days", args.days.to_string());
            set("pageSize", args.days.to_string());
            "/v1/forecast/days:lookup"
        }
        // Alerts take no unit system. Google pages at the compact answer's
        // cap, so `next_page` reaches every alert.
        WeatherKind::Alerts => {
            set("pageSize", ALERTS_MAX.to_string());
            "/v1/publicAlerts:lookup"
        }
    };
    if let Some(page) = &args.page {
        set("pageToken", page.clone());
    }
    if let Some(language) = &settings.language {
        set("languageCode", language.clone());
    }
    ApiCall {
        provider: settings.provider.clone(),
        account: None,
        method: "GET".to_string(),
        host: settings.weather_host.clone(),
        path: path.to_string(),
        query: params,
        headers: BTreeMap::new(),
        body: None,
    }
}

#[derive(Clone, Debug, PartialEq)]
struct GeocodeArgs {
    query: Where,
    max: usize,
}

impl GeocodeArgs {
    /// Every argument the tool takes.
    const KEYS: &[&str] = &["address", "lat", "lng", "max"];

    fn parse(args: &Value) -> Result<GeocodeArgs, String> {
        let args = object(args, Self::KEYS)?;
        let query = place_or_coordinates(args, "address")?;
        let max = integer(args, "max", 1, GEOCODE_MAX)?.unwrap_or(GEOCODE_DEFAULT_MAX);
        Ok(GeocodeArgs {
            query,
            max: usize::try_from(max).expect("max is at most 5"),
        })
    }
}

/// A Geocoding v4 lookup: forward by `addressQuery`, reverse by location,
/// both as query parameters so the path is fixed and no caller text becomes a
/// path segment.
fn geocode_call(place: &Where, settings: &Settings) -> ApiCall {
    let (path, mut params) = match place {
        Where::Text(address) => (
            "/v4/geocode/address",
            query([("addressQuery", address.clone())]),
        ),
        Where::At(lat, lng) => (
            "/v4/geocode/location",
            query([
                ("location.latitude", lat.to_string()),
                ("location.longitude", lng.to_string()),
            ]),
        ),
    };
    if let Some(language) = &settings.language {
        params.insert(
            "languageCode".to_string(),
            QueryValue::One(language.clone()),
        );
    }
    ApiCall {
        provider: settings.provider.clone(),
        account: None,
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

/// How a route is travelled: the tool's `mode` and the Routes API's
/// `travelMode`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TravelMode {
    Drive,
    Walk,
    Bicycle,
    Transit,
}

impl TravelMode {
    /// Every mode, in the order the tools' `input_schema`s list them.
    const ALL: [TravelMode; 4] = [
        TravelMode::Drive,
        TravelMode::Walk,
        TravelMode::Bicycle,
        TravelMode::Transit,
    ];

    fn parse(args: &Args) -> Result<TravelMode, String> {
        Ok(choice(args, "mode", &TravelMode::ALL, TravelMode::name)?.unwrap_or(TravelMode::Drive))
    }

    /// The tool's spelling, as the answer states it.
    fn name(self) -> &'static str {
        match self {
            TravelMode::Drive => "drive",
            TravelMode::Walk => "walk",
            TravelMode::Bicycle => "bicycle",
            TravelMode::Transit => "transit",
        }
    }

    fn google(self) -> &'static str {
        match self {
            TravelMode::Drive => "DRIVE",
            TravelMode::Walk => "WALK",
            TravelMode::Bicycle => "BICYCLE",
            TravelMode::Transit => "TRANSIT",
        }
    }
}

/// One end of a route: coordinates, a Places place id, or an address Google
/// resolves itself.
#[derive(Clone, Debug, PartialEq)]
enum Waypoint {
    At(f64, f64),
    PlaceId(String),
    Address(String),
}

impl Waypoint {
    /// The required text argument `key`: `<lat>,<lng>` is coordinates,
    /// `place_id:<id>` a place id, anything else an address.
    fn parse(args: &Args, key: &str) -> Result<Waypoint, String> {
        let Some(text) = text(args, key)? else {
            return Err(format!("`{key}` is required"));
        };
        if let Some((lat, lng)) = parse_lat_lng(text, key)? {
            return Ok(Waypoint::At(lat, lng));
        }
        if let Some(id) = text.trim().strip_prefix(PLACE_ID_PREFIX) {
            let id = id.trim();
            if id.is_empty() {
                return Err(format!("`{key}`: `{PLACE_ID_PREFIX}` names no place id"));
            }
            return Ok(Waypoint::PlaceId(id.to_string()));
        }
        Ok(Waypoint::Address(text.to_string()))
    }

    /// The Routes API `Waypoint`.
    fn google(&self) -> Value {
        match self {
            Waypoint::At(lat, lng) => {
                json!({ "location": { "latLng": { "latitude": lat, "longitude": lng } } })
            }
            Waypoint::PlaceId(id) => json!({ "placeId": id }),
            Waypoint::Address(address) => json!({ "address": address }),
        }
    }
}

impl std::fmt::Display for Waypoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Waypoint::At(lat, lng) => write!(f, "{lat},{lng}"),
            Waypoint::PlaceId(id) => write!(f, "{PLACE_ID_PREFIX}{id}"),
            Waypoint::Address(address) => f.write_str(address),
        }
    }
}

/// When a route is wanted: leaving at, or arriving by, an RFC 3339 time.
#[derive(Clone, Debug, PartialEq)]
enum When {
    DepartAt(String),
    ArriveBy(String),
}

/// The two ends of a route and how it is travelled.
#[derive(Clone, Debug, PartialEq)]
struct RouteEnds {
    origin: Waypoint,
    destination: Waypoint,
    mode: TravelMode,
}

impl RouteEnds {
    fn parse(args: &Args) -> Result<RouteEnds, String> {
        Ok(RouteEnds {
            origin: Waypoint::parse(args, "origin")?,
            destination: Waypoint::parse(args, "destination")?,
            mode: TravelMode::parse(args)?,
        })
    }

    /// The first route of a Routes answer. No route is the caller's: none
    /// joins the two ends by that mode.
    fn first<'a>(&self, body: &'a Value) -> Result<&'a Value, Failure> {
        let Value::Object(body) = body else {
            return Err(shape("Routes"));
        };
        let Some(route) = first_of(body, "routes", "Routes")? else {
            return Err(Failure {
                kind: ErrorKind::InvalidArgs,
                detail: format!(
                    "no {} route found from {} to {}",
                    self.mode.name(),
                    self.origin,
                    self.destination
                ),
            });
        };
        record(route, "Routes")
    }
}

#[derive(Clone, Debug, PartialEq)]
struct DirectionsArgs {
    route: RouteEnds,
    when: Option<When>,
    max_steps: usize,
}

impl DirectionsArgs {
    /// Every argument the tool takes.
    const KEYS: &[&str] = &[
        "origin",
        "destination",
        "mode",
        "depart_at",
        "arrive_by",
        "max_steps",
    ];

    fn parse(args: &Value) -> Result<DirectionsArgs, String> {
        let args = object(args, Self::KEYS)?;
        let route = RouteEnds::parse(args)?;
        // Google checks the times themselves and answers a malformed one with a 400.
        let when = match (text(args, "depart_at")?, text(args, "arrive_by")?) {
            (Some(_), Some(_)) => {
                return Err("pass `depart_at` or `arrive_by`, not both".to_string());
            }
            (Some(time), None) => Some(When::DepartAt(time.to_string())),
            (None, Some(_)) if route.mode != TravelMode::Transit => {
                // Google ignores an arrival time on every other mode; a route
                // that silently leaves now is not the route asked for.
                return Err("`arrive_by` applies to `mode: transit` only".to_string());
            }
            (None, Some(time)) => Some(When::ArriveBy(time.to_string())),
            (None, None) => None,
        };
        let max_steps = integer(args, "max_steps", 1, DIRECTIONS_MAX_STEPS)?
            .unwrap_or(DIRECTIONS_DEFAULT_MAX_STEPS);
        Ok(DirectionsArgs {
            route,
            when,
            max_steps: usize::try_from(max_steps).expect("max_steps is at most 200"),
        })
    }
}

/// What a `computeRoutes` call asks Google for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RouteRequest {
    /// `Directions`: legs and steps, no polyline.
    Directions,
    /// SearchAlongRoute: an overview polyline and the route's size.
    Polyline,
}

/// A Routes `computeRoutes` call: one route, no alternatives.
fn compute_routes_call(
    route: &RouteEnds,
    when: Option<&When>,
    request: RouteRequest,
    settings: &Settings,
) -> ApiCall {
    let mut body = Args::new();
    body.insert("origin".to_string(), route.origin.google());
    body.insert("destination".to_string(), route.destination.google());
    body.insert("travelMode".to_string(), json!(route.mode.google()));
    // Google refuses a routing preference on any mode but driving.
    if route.mode == TravelMode::Drive {
        body.insert("routingPreference".to_string(), json!("TRAFFIC_AWARE"));
    }
    match when {
        Some(When::DepartAt(time)) => {
            body.insert("departureTime".to_string(), json!(time));
        }
        Some(When::ArriveBy(time)) => {
            body.insert("arrivalTime".to_string(), json!(time));
        }
        None => {}
    }
    body.insert("computeAlternativeRoutes".to_string(), json!(false));
    let mask = match request {
        RouteRequest::Directions => DIRECTIONS_FIELD_MASK,
        RouteRequest::Polyline => {
            // Pinned rather than left to Google's default: an overview line is
            // what keeps the polyline under `POLYLINE_MAX_BYTES` on any
            // ordinary route.
            body.insert("polylineQuality".to_string(), json!("OVERVIEW"));
            ALONG_ROUTE_FIELD_MASK
        }
    };
    body.insert("units".to_string(), json!(settings.units.google()));
    if let Some(language) = &settings.language {
        body.insert("languageCode".to_string(), json!(language));
    }
    ApiCall {
        provider: settings.provider.clone(),
        account: None,
        method: "POST".to_string(),
        host: settings.routes_host.clone(),
        path: COMPUTE_ROUTES_PATH.to_string(),
        query: Query::new(),
        headers: BTreeMap::from([("X-Goog-FieldMask".to_string(), mask.to_string())]),
        body: Some(Value::Object(body).to_string()),
    }
}

#[derive(Clone, Debug, PartialEq)]
struct PlaceDetailsArgs {
    id: String,
    reviews: bool,
}

impl PlaceDetailsArgs {
    /// Every argument the tool takes.
    const KEYS: &[&str] = &["id", "reviews"];

    fn parse(args: &Value) -> Result<PlaceDetailsArgs, String> {
        let args = object(args, Self::KEYS)?;
        let Some(id) = capped(args, "id", PLACE_ID_MAX_CHARS)? else {
            return Err("`id` is required".to_string());
        };
        // The id becomes the last path segment; a Places id is URL-safe base64.
        if !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(
                "`id` must be a bare Places place id (letters, digits, `_` and `-`), as \
                 PlacesSearch's `id` or Geocode's `place_id`"
                    .to_string(),
            );
        }
        Ok(PlaceDetailsArgs {
            id: id.to_string(),
            reviews: flag(args, "reviews")?,
        })
    }
}

/// A Place Details lookup. Reviews and the editorial summary are requested
/// only when asked for: they are billed at a higher tier.
fn place_details_call(args: &PlaceDetailsArgs, settings: &Settings) -> ApiCall {
    let mask = if args.reviews {
        format!("{PLACE_DETAILS_FIELD_MASK},{PLACE_DETAILS_REVIEW_FIELDS}")
    } else {
        PLACE_DETAILS_FIELD_MASK.to_string()
    };
    let mut params = Query::new();
    if let Some(language) = &settings.language {
        params.insert(
            "languageCode".to_string(),
            QueryValue::One(language.clone()),
        );
    }
    ApiCall {
        provider: settings.provider.clone(),
        account: None,
        method: "GET".to_string(),
        host: settings.places_host.clone(),
        path: format!("{PLACE_DETAILS_PATH}{}", args.id),
        query: params,
        headers: BTreeMap::from([("X-Goog-FieldMask".to_string(), mask)]),
        body: None,
    }
}

/// A Places text search's terms: what to look for, and the most places one
/// page answers with.
#[derive(Clone, Debug, PartialEq)]
struct SearchText {
    query: String,
    max: usize,
}

impl SearchText {
    fn parse(args: &Args) -> Result<SearchText, String> {
        let Some(query) = text(args, "query")? else {
            return Err("`query` is required".to_string());
        };
        let max = integer(args, "max", 1, PLACES_MAX)?.unwrap_or(PLACES_DEFAULT_MAX);
        Ok(SearchText {
            query: query.to_string(),
            max: usize::try_from(max).expect("max is at most 20"),
        })
    }
}

/// What limits a Places text search to an area.
enum SearchScope<'a> {
    Anywhere,
    /// Biased toward a circle of `radius` meters around a point.
    Near {
        lat: f64,
        lng: f64,
        radius: u64,
    },
    /// Along a route's encoded polyline.
    Along(&'a str),
}

/// A Places text search: one page of at most `search.max` places.
fn search_text_call(
    search: &SearchText,
    open_now: bool,
    scope: SearchScope<'_>,
    settings: &Settings,
) -> ApiCall {
    let mut body = Args::new();
    body.insert("textQuery".to_string(), json!(search.query));
    body.insert("pageSize".to_string(), json!(search.max));
    if open_now {
        body.insert("openNow".to_string(), json!(true));
    }
    match scope {
        SearchScope::Anywhere => {}
        SearchScope::Near { lat, lng, radius } => {
            body.insert(
                "locationBias".to_string(),
                json!({ "circle": {
                    "center": { "latitude": lat, "longitude": lng },
                    "radius": radius,
                } }),
            );
        }
        SearchScope::Along(polyline) => {
            body.insert(
                "searchAlongRouteParameters".to_string(),
                json!({ "polyline": { "encodedPolyline": polyline } }),
            );
        }
    }
    if let Some(language) = &settings.language {
        body.insert("languageCode".to_string(), json!(language));
    }
    ApiCall {
        provider: settings.provider.clone(),
        account: None,
        method: "POST".to_string(),
        host: settings.places_host.clone(),
        path: SEARCH_TEXT_PATH.to_string(),
        query: Query::new(),
        headers: BTreeMap::from([(
            "X-Goog-FieldMask".to_string(),
            PLACES_SEARCH_FIELD_MASK.to_string(),
        )]),
        body: Some(Value::Object(body).to_string()),
    }
}

#[derive(Clone, Debug, PartialEq)]
struct AlongRouteArgs {
    route: RouteEnds,
    search: SearchText,
}

impl AlongRouteArgs {
    /// Every argument the tool takes.
    const KEYS: &[&str] = &["origin", "destination", "query", "mode", "max"];

    fn parse(args: &Value) -> Result<AlongRouteArgs, String> {
        let args = object(args, Self::KEYS)?;
        let route = RouteEnds::parse(args)?;
        let search = SearchText::parse(args)?;
        Ok(AlongRouteArgs { route, search })
    }
}

/// A Places text search along a route's encoded polyline.
fn along_route_search_call(args: &AlongRouteArgs, polyline: &str, settings: &Settings) -> ApiCall {
    search_text_call(&args.search, false, SearchScope::Along(polyline), settings)
}

#[derive(Clone, Debug, PartialEq)]
struct PlacesSearchArgs {
    search: SearchText,
    /// Where the search is biased toward: coordinates, or a place to geocode.
    near: Option<Where>,
    /// The bias circle's radius; read only with `near`.
    radius: u64,
    open_now: bool,
}

impl PlacesSearchArgs {
    /// Every argument the tool takes.
    const KEYS: &[&str] = &["query", "near", "radius", "open_now", "max"];

    fn parse(args: &Value) -> Result<PlacesSearchArgs, String> {
        let args = object(args, Self::KEYS)?;
        let search = SearchText::parse(args)?;
        let near = text(args, "near")?
            .map(|near| place_text(near, "near"))
            .transpose()?;
        let radius = integer(args, "radius", 1, RADIUS_MAX_METERS)?;
        if radius.is_some() && near.is_none() {
            return Err("`radius` applies with `near` only".to_string());
        }
        Ok(PlacesSearchArgs {
            search,
            near,
            radius: radius.unwrap_or(RADIUS_DEFAULT_METERS),
            open_now: flag(args, "open_now")?,
        })
    }
}

/// A Places text search, biased toward a circle around `center` when there
/// is one.
fn places_search_call(
    args: &PlacesSearchArgs,
    center: Option<(f64, f64)>,
    settings: &Settings,
) -> ApiCall {
    let scope = match center {
        None => SearchScope::Anywhere,
        Some((lat, lng)) => SearchScope::Near {
            lat,
            lng,
            radius: args.radius,
        },
    };
    search_text_call(&args.search, args.open_now, scope, settings)
}

/// A Google 4xx/5xx answer as the request's failure. 400 and 404 are the
/// caller's arguments; every other status is the operator's or Google's,
/// 403 included: a restricted key, a disabled API or billing.
fn google_error(status: u64, body: &str) -> Failure {
    let kind = if status == 400 || status == 404 {
        ErrorKind::InvalidArgs
    } else {
        ErrorKind::Internal
    };
    Failure {
        kind,
        detail: google_error_detail(status, body),
    }
}

/// What the request does with its nested call's answer: complete, fail, or
/// issue its next step.
pub fn advance(state: &InFlight, outcome: Result<Value, NestedError>, settings: &Settings) -> Next {
    let body = match read_api_result(outcome, &state.target) {
        Ok(Answer::Body(body)) => body,
        // Every step reads a JSON answer; none accepts an empty one.
        Ok(Answer::NoBody { status }) => {
            return Next::Fail(internal(format!(
                "Google answered HTTP {status} with no body"
            )));
        }
        Ok(Answer::Relay(value)) => return Next::Complete(value),
        Ok(Answer::Unreadable(why)) => return Next::Fail(unreadable(state, &why)),
        Ok(Answer::Error { status, body }) => return Next::Fail(google_error(status, &body)),
        Err(failure) => return Next::Fail(failure),
    };
    let next = match (state.tool, state.step) {
        (Provided::Geocode, 1) => stored(state, GeocodeArgs::parse)
            .and_then(|args| compact_geocode(&body, args.max))
            .map(Next::Complete),
        (Provided::Weather, WEATHER_GEOCODE_STEP) => stored(state, WeatherArgs::parse)
            .and_then(|args| weather_after_geocode(&args, &body, settings)),
        (Provided::Weather, WEATHER_LOOKUP_STEP) => stored(state, WeatherArgs::parse)
            .and_then(|args| compact_weather(&args, &state.derived, &body, settings.units))
            .map(Next::Complete),
        (Provided::Directions, 1) => stored(state, DirectionsArgs::parse)
            .and_then(|args| compact_directions(&args, &body, settings.units))
            .map(Next::Complete),
        (Provided::PlacesSearch, PLACES_GEOCODE_STEP) => stored(state, PlacesSearchArgs::parse)
            .and_then(|args| places_after_geocode(&args, &body, settings)),
        (Provided::PlacesSearch, PLACES_SEARCH_STEP) => stored(state, PlacesSearchArgs::parse)
            .and_then(|args| compact_places_search(&args, &state.derived, &body))
            .map(Next::Complete),
        (Provided::PlaceDetails, 1) => stored(state, PlaceDetailsArgs::parse)
            .and_then(|args| compact_place_details(&args, &body))
            .map(Next::Complete),
        (Provided::SearchAlongRoute, ALONG_ROUTE_ROUTE_STEP) => {
            stored(state, AlongRouteArgs::parse)
                .and_then(|args| search_along(&args, &body, settings))
        }
        (Provided::SearchAlongRoute, ALONG_ROUTE_SEARCH_STEP) => {
            stored(state, AlongRouteArgs::parse)
                .and_then(|args| compact_along_route(&args, &state.derived, &body))
                .map(Next::Complete)
        }
        (tool, step) => Err(no_such_step(tool, step)),
    };
    match next {
        Ok(Next::Complete(value)) => bounded(value, COMPACT_RESULT_CEILING, None),
        Ok(next) => next,
        Err(failure) => Next::Fail(failure),
    }
}

/// A step's answer that cannot be read whole, cut at the read limit or not
/// delivered, as the request's failure. SearchAlongRoute's route step holds
/// nothing but the polyline, two scalars and `fallbackInfo`, so a cut there
/// is a route too long to search along: the caller's. Every other answer is
/// `internal`, naming what to narrow where an argument narrows it.
fn unreadable(state: &InFlight, why: &Unreadable) -> Failure {
    match (state.tool, state.step) {
        (Provided::SearchAlongRoute, ALONG_ROUTE_ROUTE_STEP) => match why {
            Unreadable::Cut(_) => Failure {
                kind: ErrorKind::InvalidArgs,
                detail: format!("route too long to search along: {}", why.render(None)),
            },
            Unreadable::Refused(_) => why.failure(Some(ROUTE_HINT)),
        },
        (Provided::PlacesSearch, PLACES_SEARCH_STEP)
        | (Provided::SearchAlongRoute, ALONG_ROUTE_SEARCH_STEP) => why.failure(Some(PLACES_HINT)),
        (Provided::PlaceDetails, 1) => stored(state, PlaceDetailsArgs::parse).map_or_else(
            |failure| failure,
            |args| why.failure(args.reviews.then_some(REVIEWS_HINT)),
        ),
        (Provided::Weather, WEATHER_LOOKUP_STEP) => stored(state, WeatherArgs::parse).map_or_else(
            |failure| failure,
            |args| why.failure((args.kind == WeatherKind::Daily).then_some(DAYS_HINT)),
        ),
        (Provided::Geocode, 1)
        | (Provided::Weather, WEATHER_GEOCODE_STEP)
        | (Provided::PlacesSearch, PLACES_GEOCODE_STEP)
        | (Provided::Directions, 1) => why.failure(None),
        (tool, step) => no_such_step(tool, step),
    }
}
/// A geocoded place: the first result's location and the address Google
/// resolved the text to.
#[derive(Clone, Debug, PartialEq)]
struct Located {
    lat: f64,
    lng: f64,
    address: Option<String>,
}

impl Located {
    /// The compact `location` object, and the form a later step's `derived`
    /// carries it in.
    fn to_map(&self) -> Args {
        let mut out = Args::new();
        out.insert("lat".to_string(), json!(self.lat));
        out.insert("lng".to_string(), json!(self.lng));
        if let Some(address) = &self.address {
            out.insert("place".to_string(), json!(address));
        }
        out
    }
}

/// The first location a Geocoding v4 answer for `text` gives. No result is
/// the caller's: no location matches the text.
fn first_location(body: &Value, text: &str) -> Result<Located, Failure> {
    let Value::Object(body) = body else {
        return Err(shape("Geocoding"));
    };
    let Some(first) = first_of(body, "results", "Geocoding")? else {
        return Err(Failure {
            kind: ErrorKind::InvalidArgs,
            detail: format!("no location found for {text}"),
        });
    };
    let coordinate = |key: &str| {
        first
            .get("location")
            .and_then(|location| location.get(key))
            .and_then(Value::as_f64)
    };
    let (Some(lat), Some(lng)) = (coordinate("latitude"), coordinate("longitude")) else {
        return Err(shape("Geocoding"));
    };
    Ok(Located {
        lat,
        lng,
        address: first
            .get("formattedAddress")
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}

/// Weather's lookup at the location its `place` geocoded to; the location
/// rides in `derived` to the answer.
fn weather_after_geocode(
    args: &WeatherArgs,
    body: &Value,
    settings: &Settings,
) -> Result<Next, Failure> {
    let Where::Text(place) = &args.at else {
        return Err(internal(
            "in-flight row names weather's geocode step for a request given coordinates",
        ));
    };
    after_geocode(body, place, |lat, lng| Call {
        step: WEATHER_LOOKUP_STEP,
        api: weather_call(args, lat, lng, settings),
    })
}

/// The call `next` builds at the first location the geocode of `text`
/// answered with; the location rides in `derived` to the answer.
fn after_geocode(
    body: &Value,
    text: &str,
    next: impl FnOnce(f64, f64) -> Call,
) -> Result<Next, Failure> {
    let located = first_location(body, text)?;
    Ok(Next::Call {
        call: next(located.lat, located.lng),
        derived: located.to_map(),
    })
}

/// The compact `location` of an answer about `at`: the caller's coordinates,
/// or what the geocode step resolved the text to, which `derived` carries.
/// `step` names the step reading it, for the error when `derived` does not.
fn resolved_location(at: &Where, derived: &Args, step: &str) -> Result<Value, Failure> {
    match at {
        Where::At(lat, lng) => Ok(Value::Object(
            Located {
                lat: *lat,
                lng: *lng,
                address: None,
            }
            .to_map(),
        )),
        Where::Text(_) => {
            if !(derived.contains_key("lat") && derived.contains_key("lng")) {
                return Err(internal(format!(
                    "in-flight row for {step} carries no geocoded location"
                )));
            }
            Ok(Value::Object(derived.clone()))
        }
    }
}

/// PlacesSearch's search around where its `near` place geocoded to; the
/// location rides in `derived` to the answer.
fn places_after_geocode(
    args: &PlacesSearchArgs,
    body: &Value,
    settings: &Settings,
) -> Result<Next, Failure> {
    let Some(Where::Text(near)) = &args.near else {
        return Err(internal(
            "in-flight row names places-search's geocode step for a request with no place to \
             geocode",
        ));
    };
    after_geocode(body, near, |lat, lng| Call {
        step: PLACES_SEARCH_STEP,
        api: places_search_call(args, Some((lat, lng)), settings),
    })
}

/// A Places text search answer as PlacesSearch's compact result: `location`
/// when the search was biased toward a place, then the places and `more`.
fn compact_places_search(
    args: &PlacesSearchArgs,
    derived: &Args,
    body: &Value,
) -> Result<Value, Failure> {
    let mut out = Args::new();
    if let Some(near) = &args.near {
        out.insert(
            "location".to_string(),
            resolved_location(near, derived, "places-search's search")?,
        );
    }
    compact_places(&mut out, body, args.search.max)?;
    Ok(Value::Object(out))
}

/// A Places text search answer's places, at most `max` with `places_omitted`
/// counting the rest, and `more`: whether Google holds a further page. Google
/// omits `places` when nothing matched, which is an empty list.
fn compact_places(out: &mut Args, body: &Value, max: usize) -> Result<(), Failure> {
    if !body.is_object() {
        return Err(shape("Places"));
    }
    let none = Value::Array(Vec::new());
    first_items(
        out,
        "places",
        body.get("places").unwrap_or(&none),
        max,
        "Places",
        |place| Ok(Value::Object(compact_place(record(place, "Places")?))),
    )?;
    out.insert("more".to_string(), json!(page_token(body).is_some()));
    Ok(())
}

/// A Google `Place` as the compact place PlacesSearch, SearchAlongRoute and
/// PlaceDetails all answer with.
fn compact_place(place: &Value) -> Args {
    let mut out = Args::new();
    copy(&mut out, "id", place.get("id"));
    copy(&mut out, "name", at(place, &["displayName", "text"]));
    copy(&mut out, "address", place.get("formattedAddress"));
    copy(&mut out, "lat", at(place, &["location", "latitude"]));
    copy(&mut out, "lng", at(place, &["location", "longitude"]));
    copy(
        &mut out,
        "type",
        at(place, &["primaryTypeDisplayName", "text"]),
    );
    copy(&mut out, "rating", place.get("rating"));
    copy(&mut out, "rating_count", place.get("userRatingCount"));
    copy(&mut out, "price_level", place.get("priceLevel"));
    copy(
        &mut out,
        "open_now",
        at(place, &["currentOpeningHours", "openNow"]),
    );
    copy(&mut out, "status", place.get("businessStatus"));
    out
}

/// A Place Details answer as PlaceDetails' compact result. `summary` and
/// `reviews` appear only when they were asked for and Google gave them.
fn compact_place_details(args: &PlaceDetailsArgs, body: &Value) -> Result<Value, Failure> {
    if body.get("id").is_none() {
        return Err(shape("Places"));
    }
    let mut out = compact_place(body);
    if let Some(hours) = at(body, &["regularOpeningHours", "weekdayDescriptions"]) {
        first_items(&mut out, "hours", hours, WEEKDAYS, "Places", |line| {
            string_item(line, "Places")
        })?;
    }
    copy(&mut out, "phone", body.get("nationalPhoneNumber"));
    copy(
        &mut out,
        "phone_international",
        body.get("internationalPhoneNumber"),
    );
    copy(&mut out, "website", body.get("websiteUri"));
    copy(&mut out, "maps_url", body.get("googleMapsUri"));
    if args.reviews {
        copy(&mut out, "summary", at(body, &["editorialSummary", "text"]));
        if let Some(reviews) = body.get("reviews") {
            first_items(
                &mut out,
                "reviews",
                reviews,
                REVIEWS_MAX,
                "Places",
                |review| {
                    let review = record(review, "Places")?;
                    let mut item = Args::new();
                    copy(&mut item, "rating", review.get("rating"));
                    copy(
                        &mut item,
                        "when",
                        review.get("relativePublishTimeDescription"),
                    );
                    copy(
                        &mut item,
                        "author",
                        at(review, &["authorAttribution", "displayName"]),
                    );
                    if let Some(text) = at(review, &["text", "text"]).and_then(Value::as_str) {
                        item.insert(
                            "text".to_string(),
                            json!(cut(text, REVIEW_TEXT_MAX_BYTES).0),
                        );
                    }
                    Ok(Value::Object(item))
                },
            )?;
        }
    }
    Ok(Value::Object(out))
}

/// SearchAlongRoute's search along the route its first step computed. The
/// route's compact summary rides in `derived` to the answer; the polyline goes
/// into the search's body and is not stored.
fn search_along(args: &AlongRouteArgs, body: &Value, settings: &Settings) -> Result<Next, Failure> {
    let route = args.route.first(body)?;
    let polyline = at(route, &["polyline", "encodedPolyline"])
        .and_then(Value::as_str)
        .filter(|polyline| !polyline.is_empty())
        .ok_or_else(|| shape("Routes"))?;
    if polyline.len() > POLYLINE_MAX_BYTES {
        return Err(Failure {
            kind: ErrorKind::InvalidArgs,
            detail: format!(
                "route too long to search along ({}-byte polyline)",
                polyline.len()
            ),
        });
    }
    let mut summary = Args::new();
    distance(
        &mut summary,
        "distance",
        route.get("distanceMeters"),
        settings.units,
    )?;
    minutes(&mut summary, "duration_minutes", route.get("duration"))?;
    fallback(&mut summary, body)?;
    Ok(Next::Call {
        call: Call {
            step: ALONG_ROUTE_SEARCH_STEP,
            api: along_route_search_call(args, polyline, settings),
        },
        derived: Args::from_iter([("route".to_string(), Value::Object(summary))]),
    })
}

/// A search along a route as SearchAlongRoute's compact result: the route the
/// first step computed, then the places and `more`.
fn compact_along_route(
    args: &AlongRouteArgs,
    derived: &Args,
    body: &Value,
) -> Result<Value, Failure> {
    let Some(route @ Value::Object(_)) = derived.get("route") else {
        return Err(internal(
            "in-flight row for search-along-route's search carries no route",
        ));
    };
    let mut out = Args::new();
    out.insert("route".to_string(), route.clone());
    compact_places(&mut out, body, args.search.max)?;
    Ok(Value::Object(out))
}

/// Google's `fallbackInfo` on a Routes answer as `fallback`. Its presence is
/// the signal that Google did not compute the route in the routing mode asked
/// for, so it is carried even when none of the inner fields read here is set.
fn fallback(to: &mut Args, body: &Value) -> Result<(), Failure> {
    match body.get("fallbackInfo") {
        None | Some(Value::Null) => Ok(()),
        Some(Value::Object(info)) => {
            let mut fallback = Args::new();
            copy(&mut fallback, "routing_mode", info.get("routingMode"));
            copy(&mut fallback, "reason", info.get("reason"));
            to.insert("fallback".to_string(), Value::Object(fallback));
            Ok(())
        }
        Some(_) => Err(shape("Routes")),
    }
}

/// The value at `path` through nested objects.
fn at<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    path.iter().try_fold(value, |value, key| value.get(*key))
}

/// Insert `inner` as `to[name]` unless nothing was copied into it.
fn nest(to: &mut Args, name: &str, inner: Args) {
    if !inner.is_empty() {
        to.insert(name.to_string(), Value::Object(inner));
    }
}

/// The first `max` items of a Google list, each through `map`, into
/// `to[name]`, with `<name>_omitted` counting what was cut. A value that is
/// not a list is a shape error, as is any item `map` refuses.
fn first_items(
    to: &mut Args,
    name: &str,
    list: &Value,
    max: usize,
    api: &str,
    map: impl Fn(&Value) -> Result<Value, Failure>,
) -> Result<(), Failure> {
    let Value::Array(items) = list else {
        return Err(shape(api));
    };
    let kept = items
        .iter()
        .take(max)
        .map(map)
        .collect::<Result<Vec<_>, _>>()?;
    to.insert(name.to_string(), Value::Array(kept));
    if items.len() > max {
        to.insert(format!("{name}_omitted"), json!(items.len() - max));
    }
    Ok(())
}

/// A list item that must be an object; anything else is a shape error.
fn record<'a>(item: &'a Value, api: &str) -> Result<&'a Value, Failure> {
    if item.is_object() {
        Ok(item)
    } else {
        Err(shape(api))
    }
}

fn string_item(item: &Value, api: &str) -> Result<Value, Failure> {
    match item {
        Value::String(_) => Ok(item.clone()),
        _ => Err(shape(api)),
    }
}

/// The first item of the Google list at `body[key]`: `None` when the list is
/// absent or empty, a shape error when `body[key]` is not a list.
fn first_of<'a>(body: &'a Args, key: &str, api: &str) -> Result<Option<&'a Value>, Failure> {
    match body.get(key) {
        None => Ok(None),
        Some(Value::Array(items)) => Ok(items.first()),
        Some(_) => Err(shape(api)),
    }
}

/// Google's `nextPageToken`, when it names a further page: an empty token
/// names none.
fn page_token(body: &Value) -> Option<&str> {
    body.get("nextPageToken")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
}

/// Google's `nextPageToken` as `next_page`, when it names a further page.
fn next_page(to: &mut Args, body: &Value) {
    if let Some(token) = page_token(body) {
        to.insert("next_page".to_string(), json!(token));
    }
}

/// A Weather API answer as the tool's compact result for its kind.
fn compact_weather(
    args: &WeatherArgs,
    derived: &Args,
    body: &Value,
    units: Units,
) -> Result<Value, Failure> {
    let location = resolved_location(&args.at, derived, "weather's lookup")?;
    if !body.is_object() {
        return Err(shape("Weather"));
    }
    let mut out = Args::new();
    out.insert("location".to_string(), location);
    match args.kind {
        WeatherKind::Current => compact_current(&mut out, body, units)?,
        WeatherKind::Hourly => compact_hourly(&mut out, body, units)?,
        WeatherKind::Daily => compact_daily(&mut out, body, units)?,
        WeatherKind::Alerts => compact_alerts(&mut out, body)?,
    }
    Ok(Value::Object(out))
}

/// The time zone and units every forecast answer states.
fn weather_header(out: &mut Args, body: &Value, units: Units) {
    copy(
        out,
        "timezone",
        body.get("timeZone").and_then(|tz| tz.get("id")),
    );
    out.insert("units".to_string(), units.weather_labels());
}

/// Where Google puts the wind and precipitation of a reading; current
/// conditions nest them, hours and day parts flatten them.
const WIND_SPEED: &[&str] = &["wind", "speed", "value"];
const WIND_DIRECTION: &[&str] = &["wind", "direction", "cardinal"];
const PRECIP_PERCENT: &[&str] = &["precipitation", "probability", "percent"];
const PRECIP_TYPE: &[&str] = &["precipitation", "probability", "type"];
const PRECIP_AMOUNT: &[&str] = &["precipitation", "qpf", "quantity"];

/// The sky and air of any reading: current conditions, an hour or a day part.
fn sky(out: &mut Args, from: &Value) {
    copy(
        out,
        "condition",
        at(from, &["weatherCondition", "description", "text"]),
    );
    copy(out, "humidity_percent", from.get("relativeHumidity"));
    copy(out, "uv_index", from.get("uvIndex"));
    copy(out, "cloud_cover_percent", from.get("cloudCover"));
}

/// The temperature and how it feels, of current conditions or an hour.
fn temperatures(out: &mut Args, from: &Value) {
    copy(out, "temperature", at(from, &["temperature", "degrees"]));
    copy(
        out,
        "feels_like",
        at(from, &["feelsLikeTemperature", "degrees"]),
    );
}

/// The conditions an hour and a day part share, flattened.
fn period(out: &mut Args, from: &Value) {
    sky(out, from);
    copy(out, "precip_probability_percent", at(from, PRECIP_PERCENT));
    copy(out, "precip_type", at(from, PRECIP_TYPE));
    copy(out, "precip_amount", at(from, PRECIP_AMOUNT));
    copy(out, "wind_speed", at(from, WIND_SPEED));
    copy(out, "wind_direction", at(from, WIND_DIRECTION));
}

fn compact_current(out: &mut Args, from: &Value, units: Units) -> Result<(), Failure> {
    if from.get("temperature").is_none() {
        return Err(shape("Weather"));
    }
    copy(out, "time", from.get("currentTime"));
    weather_header(out, from, units);
    sky(out, from);
    temperatures(out, from);
    copy(out, "is_daytime", from.get("isDaytime"));
    copy(out, "dew_point", at(from, &["dewPoint", "degrees"]));
    copy(
        out,
        "thunderstorm_percent",
        from.get("thunderstormProbability"),
    );
    let mut wind = Args::new();
    copy(&mut wind, "speed", at(from, WIND_SPEED));
    copy(&mut wind, "gust", at(from, &["wind", "gust", "value"]));
    copy(&mut wind, "direction", at(from, WIND_DIRECTION));
    nest(out, "wind", wind);
    let mut precipitation = Args::new();
    copy(
        &mut precipitation,
        "probability_percent",
        at(from, PRECIP_PERCENT),
    );
    copy(&mut precipitation, "type", at(from, PRECIP_TYPE));
    copy(&mut precipitation, "amount", at(from, PRECIP_AMOUNT));
    nest(out, "precipitation", precipitation);
    copy(
        out,
        "pressure_mb",
        at(from, &["airPressure", "meanSeaLevelMillibars"]),
    );
    copy(out, "visibility", at(from, &["visibility", "distance"]));
    let mut today = Args::new();
    copy(
        &mut today,
        "high",
        at(
            from,
            &["currentConditionsHistory", "maxTemperature", "degrees"],
        ),
    );
    copy(
        &mut today,
        "low",
        at(
            from,
            &["currentConditionsHistory", "minTemperature", "degrees"],
        ),
    );
    nest(out, "today", today);
    Ok(())
}

/// A `google.type.Date`, or the date part of a `DateTime`, as `YYYY-MM-DD`.
fn date(value: &Value) -> Option<String> {
    let part = |key: &str| value.get(key).and_then(Value::as_u64);
    Some(format!(
        "{:04}-{:02}-{:02}",
        part("year")?,
        part("month")?,
        part("day")?
    ))
}

/// A `google.type.DateTime` as local `YYYY-MM-DDTHH:MM`. Google's JSON omits
/// a zero hour or minute, so an absent one is zero.
fn local_time(value: &Value) -> Option<String> {
    let part = |key: &str| value.get(key).map_or(Some(0), Value::as_u64);
    Some(format!(
        "{}T{:02}:{:02}",
        date(value)?,
        part("hours")?,
        part("minutes")?
    ))
}

fn compact_hourly(out: &mut Args, body: &Value, units: Units) -> Result<(), Failure> {
    let hours = body.get("forecastHours").ok_or_else(|| shape("Weather"))?;
    weather_header(out, body, units);
    let limit = usize::try_from(HOURLY_PAGE_SIZE).expect("a page size fits usize");
    first_items(out, "hours", hours, limit, "Weather", |hour| {
        let hour = record(hour, "Weather")?;
        let mut item = Args::new();
        if let Some(time) = hour.get("displayDateTime").and_then(local_time) {
            item.insert("time".to_string(), json!(time));
        }
        temperatures(&mut item, hour);
        period(&mut item, hour);
        copy(&mut item, "is_daytime", hour.get("isDaytime"));
        Ok(Value::Object(item))
    })?;
    next_page(out, body);
    Ok(())
}

fn compact_daily(out: &mut Args, body: &Value, units: Units) -> Result<(), Failure> {
    let days = body.get("forecastDays").ok_or_else(|| shape("Weather"))?;
    weather_header(out, body, units);
    let limit = usize::try_from(DAYS_MAX).expect("a day count fits usize");
    first_items(out, "days", days, limit, "Weather", |day| {
        let day = record(day, "Weather")?;
        let mut item = Args::new();
        if let Some(date) = day.get("displayDate").and_then(date) {
            item.insert("date".to_string(), json!(date));
        }
        copy(&mut item, "high", at(day, &["maxTemperature", "degrees"]));
        copy(&mut item, "low", at(day, &["minTemperature", "degrees"]));
        copy(
            &mut item,
            "feels_like_high",
            at(day, &["feelsLikeMaxTemperature", "degrees"]),
        );
        copy(
            &mut item,
            "feels_like_low",
            at(day, &["feelsLikeMinTemperature", "degrees"]),
        );
        copy(&mut item, "sunrise", at(day, &["sunEvents", "sunriseTime"]));
        copy(&mut item, "sunset", at(day, &["sunEvents", "sunsetTime"]));
        for (name, key) in [("day", "daytimeForecast"), ("night", "nighttimeForecast")] {
            if let Some(part) = day.get(key) {
                let mut compact = Args::new();
                period(&mut compact, part);
                copy(
                    &mut compact,
                    "thunderstorm_percent",
                    part.get("thunderstormProbability"),
                );
                nest(&mut item, name, compact);
            }
        }
        Ok(Value::Object(item))
    })?;
    out.insert("day_parts".to_string(), json!(DAY_PARTS));
    next_page(out, body);
    Ok(())
}

/// Public alerts. An answer holding only `regionCode` is `alerts: []`: Google
/// answers that way when no alert is active and when a provider's alert
/// lacked a required field, and does not say which.
fn compact_alerts(out: &mut Args, body: &Value) -> Result<(), Failure> {
    if body.get("weatherAlerts").is_none() && body.get("regionCode").is_none() {
        return Err(shape("Weather"));
    }
    copy(out, "region", body.get("regionCode"));
    let empty = Value::Array(Vec::new());
    let alerts = body.get("weatherAlerts").unwrap_or(&empty);
    first_items(out, "alerts", alerts, ALERTS_MAX, "Weather", compact_alert)?;
    next_page(out, body);
    Ok(())
}

/// One alert. Google's terms require its source's name and link wherever the
/// alert is shown, so `source` carries both. An alert Google sends without
/// them is still relayed, with whichever part it has or no `source`.
fn compact_alert(alert: &Value) -> Result<Value, Failure> {
    let alert = record(alert, "Weather")?;
    let mut item = Args::new();
    copy(&mut item, "title", at(alert, &["alertTitle", "text"]));
    copy(&mut item, "event", alert.get("eventType"));
    copy(&mut item, "severity", alert.get("severity"));
    copy(&mut item, "certainty", alert.get("certainty"));
    copy(&mut item, "urgency", alert.get("urgency"));
    copy(&mut item, "area", alert.get("areaName"));
    copy(&mut item, "starts", alert.get("startTime"));
    copy(&mut item, "ends", alert.get("expirationTime"));
    if let Some(description) = alert.get("description").and_then(Value::as_str) {
        item.insert(
            "description".to_string(),
            json!(cut(description, ALERT_DESCRIPTION_MAX_BYTES).0),
        );
    }
    if let Some(lines) = alert.get("instruction") {
        first_items(
            &mut item,
            "instructions",
            lines,
            ALERT_ADVICE_MAX,
            "Weather",
            |line| string_item(line, "Weather"),
        )?;
    }
    if let Some(recommendations) = alert.get("safetyRecommendations") {
        first_items(
            &mut item,
            "recommendations",
            recommendations,
            ALERT_ADVICE_MAX,
            "Weather",
            |recommendation| {
                let recommendation = record(recommendation, "Weather")?;
                let mut compact = Args::new();
                copy(&mut compact, "directive", recommendation.get("directive"));
                copy(&mut compact, "subtext", recommendation.get("subtext"));
                Ok(Value::Object(compact))
            },
        )?;
    }
    let mut source = Args::new();
    copy(&mut source, "name", at(alert, &["dataSource", "name"]));
    copy(
        &mut source,
        "url",
        at(alert, &["dataSource", "authorityUri"]),
    );
    nest(&mut item, "source", source);
    Ok(Value::Object(item))
}

fn shape(api: &str) -> Failure {
    internal(format!("unexpected {api} API response shape"))
}

/// A Geocoding v4 answer as at most `max` results. Google omits `results`
/// when nothing matched, which is an empty list.
fn compact_geocode(body: &Value, max: usize) -> Result<Value, Failure> {
    if !body.is_object() {
        return Err(shape("Geocoding"));
    }
    let none = Value::Array(Vec::new());
    let mut answer = Args::new();
    let results = body.get("results").unwrap_or(&none);
    first_items(
        &mut answer,
        "results",
        results,
        max,
        "Geocoding",
        |result| {
            let result = record(result, "Geocoding")?;
            let mut out = Args::new();
            copy(&mut out, "address", result.get("formattedAddress"));
            copy(&mut out, "lat", at(result, &["location", "latitude"]));
            copy(&mut out, "lng", at(result, &["location", "longitude"]));
            copy(&mut out, "place_id", result.get("placeId"));
            copy(&mut out, "granularity", result.get("granularity"));
            copy(&mut out, "types", result.get("types"));
            Ok(Value::Object(out))
        },
    )?;
    Ok(Value::Object(answer))
}

/// A Routes answer as `Directions`' compact result: Google's one route, its
/// legs, and at most `max_steps` steps across them, with `steps_omitted`
/// counting the rest.
fn compact_directions(args: &DirectionsArgs, body: &Value, units: Units) -> Result<Value, Failure> {
    let route = args.route.first(body)?;
    let none = Value::Array(Vec::new());
    let mut out = Args::new();
    out.insert("mode".to_string(), json!(args.route.mode.name()));
    copy(&mut out, "summary", route.get("description"));
    // Google requires its warnings shown (walking and cycling routes are in
    // beta), so the list is always carried, empty when there are none.
    first_items(
        &mut out,
        "warnings",
        route.get("warnings").unwrap_or(&none),
        ROUTE_WARNINGS_MAX,
        "Routes",
        |warning| string_item(warning, "Routes"),
    )?;
    distance(&mut out, "distance", route.get("distanceMeters"), units)?;
    minutes(&mut out, "duration_minutes", route.get("duration"))?;
    minutes(
        &mut out,
        "duration_without_traffic_minutes",
        route.get("staticDuration"),
    )?;
    fallback(&mut out, body)?;
    let Value::Array(legs) = route.get("legs").unwrap_or(&none) else {
        return Err(shape("Routes"));
    };
    let mut room = args.max_steps;
    let mut omitted = 0;
    let mut compact_legs = Vec::with_capacity(legs.len());
    for leg in legs {
        let leg = record(leg, "Routes")?;
        let mut item = Args::new();
        distance(&mut item, "distance", leg.get("distanceMeters"), units)?;
        minutes(&mut item, "duration_minutes", leg.get("duration"))?;
        let Value::Array(steps) = leg.get("steps").unwrap_or(&none) else {
            return Err(shape("Routes"));
        };
        let kept = steps.len().min(room);
        room -= kept;
        omitted += steps.len() - kept;
        let mut compact_steps = Vec::with_capacity(kept);
        for step in &steps[..kept] {
            let step = record(step, "Routes")?;
            compact_steps.push(compact_step(step, units)?);
        }
        item.insert("steps".to_string(), Value::Array(compact_steps));
        compact_legs.push(Value::Object(item));
    }
    out.insert("legs".to_string(), Value::Array(compact_legs));
    if omitted > 0 {
        out.insert("steps_omitted".to_string(), json!(omitted));
    }
    Ok(Value::Object(out))
}

/// One step of a route. A transit step carries the leg the rider takes: line,
/// stops, and each boarding and alighting time as local text with its own
/// zone and as Google's UTC instant.
fn compact_step(step: &Value, units: Units) -> Result<Value, Failure> {
    let mut item = Args::new();
    copy(
        &mut item,
        "instruction",
        at(step, &["navigationInstruction", "instructions"]),
    );
    copy(
        &mut item,
        "maneuver",
        at(step, &["navigationInstruction", "maneuver"]),
    );
    distance(&mut item, "distance", step.get("distanceMeters"), units)?;
    // A step has no traffic-aware duration; its static one is all Google sends.
    minutes(&mut item, "duration_minutes", step.get("staticDuration"))?;
    copy(&mut item, "mode", step.get("travelMode"));
    if let Some(details) = step.get("transitDetails") {
        let mut transit = Args::new();
        copy(&mut transit, "line", at(details, &["transitLine", "name"]));
        copy(
            &mut transit,
            "line_short",
            at(details, &["transitLine", "nameShort"]),
        );
        copy(&mut transit, "headsign", details.get("headsign"));
        copy(
            &mut transit,
            "vehicle",
            at(details, &["transitLine", "vehicle", "type"]),
        );
        copy(
            &mut transit,
            "agency",
            details.pointer("/transitLine/agencies/0/name"),
        );
        copy(
            &mut transit,
            "from",
            at(details, &["stopDetails", "departureStop", "name"]),
        );
        transit_time(&mut transit, details, "departs", "departureTime");
        copy(
            &mut transit,
            "to",
            at(details, &["stopDetails", "arrivalStop", "name"]),
        );
        transit_time(&mut transit, details, "arrives", "arrivalTime");
        copy(&mut transit, "stops", details.get("stopCount"));
        nest(&mut item, "transit", transit);
    }
    Ok(Value::Object(item))
}

/// One end of a transit ride, Google's `field` (`departureTime` or
/// `arrivalTime`), as `<name>` (local wall-clock text), `<name>_timezone` (the
/// zone that text is in) and `<name>_at` (the RFC 3339 UTC instant).
fn transit_time(to: &mut Args, details: &Value, name: &str, field: &str) {
    copy(
        to,
        name,
        at(details, &["localizedValues", field, "time", "text"]),
    );
    copy(
        to,
        &format!("{name}_timezone"),
        at(details, &["localizedValues", field, "timeZone"]),
    );
    copy(
        to,
        &format!("{name}_at"),
        at(details, &["stopDetails", field]),
    );
}

/// Google's `distanceMeters` as `{ value, unit }` in `units`, to two decimals.
/// Absent is omitted; anything but a number is a shape error.
fn distance(
    to: &mut Args,
    name: &str,
    meters: Option<&Value>,
    units: Units,
) -> Result<(), Failure> {
    let Some(meters) = meters.filter(|value| !value.is_null()) else {
        return Ok(());
    };
    let meters = meters.as_f64().ok_or_else(|| shape("Routes"))?;
    let value = (meters / units.meters_per_distance_unit() * 100.0).round() / 100.0;
    to.insert(
        name.to_string(),
        json!({ "value": value, "unit": units.distance_label() }),
    );
    Ok(())
}

/// A Google `Duration` (`"1234s"`, `"3.5s"`) as whole minutes, rounded up.
/// Absent is omitted; any other form is a shape error.
fn minutes(to: &mut Args, name: &str, duration: Option<&Value>) -> Result<(), Failure> {
    let Some(duration) = duration.filter(|value| !value.is_null()) else {
        return Ok(());
    };
    let seconds = duration
        .as_str()
        .and_then(|text| text.strip_suffix('s'))
        .filter(|text| !text.starts_with(['+', '-']))
        .and_then(decimal)
        .ok_or_else(|| shape("Routes"))?;
    // Whole minutes: a route's duration is days at most.
    let whole = (seconds / 60.0).ceil() as u64;
    to.insert(name.to_string(), json!(whole));
    Ok(())
}

#[cfg(test)]
mod tests {
    use brenn_provider_kit::testutil::{failure, google, http, nested, truncated};
    use brenn_provider_kit::{
        Bound, ToolBounds, check, hold_probes, hold_required, input_schema, probes,
    };

    use super::*;

    /// The refusals of a `kind` and a `mode` the tools do not take, as the
    /// model reads them.
    const KIND_CHOICES: &str = "`kind` must be one of `current`, `hourly`, `daily`, `alerts`";
    const MODE_CHOICES: &str = "`mode` must be one of `drive`, `walk`, `bicycle`, `transit`";

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

    /// The `config` map in `docs/google-maps-platform.md`'s instance example is
    /// one this parser accepts, with the values the guide states.
    #[test]
    fn the_google_maps_guide_config_map_parses() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../docs/google-maps-platform.md");
        let doc = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("{} must be readable: {error}", path.display()));
        let mut lines = doc
            .lines()
            .map(str::trim)
            .skip_while(|line| *line != "config = {");
        assert_eq!(
            lines.next(),
            Some("config = {"),
            "docs/google-maps-platform.md carries no `config = {{` line"
        );
        let pairs: Vec<(String, String)> = lines
            .take_while(|line| *line != "};")
            .map(|line| {
                let (key, value) = line
                    .trim_end_matches(',')
                    .split_once(" = ")
                    .unwrap_or_else(|| panic!("{line:?} is not `key = \"value\"`"));
                let value = value
                    .strip_prefix('"')
                    .and_then(|v| v.strip_suffix('"'))
                    .unwrap_or_else(|| panic!("{line:?} has no quoted value"));
                (key.to_string(), value.to_string())
            })
            .collect();
        let pairs: Vec<(&str, &str)> = pairs
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let s = Settings::from_lookup(lookup(&pairs)).unwrap_or_else(|error| {
            panic!("the guide's config map fails every activation: {error}")
        });
        assert_eq!(s.provider, "google-maps");
        assert_eq!(s.weather_host, "weather.googleapis.com");
        assert_eq!(s.geocoding_host, "geocode.googleapis.com");
        assert_eq!(s.places_host, "places.googleapis.com");
        assert_eq!(s.routes_host, "routes.googleapis.com");
        assert_eq!(s.units, Units::Imperial);
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

    fn pairs(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Query parameters each sent once.
    fn params(pairs: &[(&str, &str)]) -> Query {
        pairs.iter().map(|(k, v)| (k.to_string(), one(v))).collect()
    }

    fn one(value: &str) -> QueryValue {
        QueryValue::One(value.to_string())
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
            params(&[("addressQuery", "1600 Amphitheatre Pkwy, Mountain View")])
        );
        assert_eq!(
            call.headers,
            pairs(&[("X-Goog-FieldMask", GEOCODE_FIELD_MASK)])
        );
        assert_eq!(call.body, None);
    }

    #[test]
    fn geocode_reverse_sends_the_location() {
        let call = geocode(json!({ "lat": 37.422, "lng": -122.0841 })).unwrap();
        assert_eq!(call.path, "/v4/geocode/location");
        assert_eq!(
            call.query,
            params(&[
                ("location.latitude", "37.422"),
                ("location.longitude", "-122.0841")
            ])
        );
        assert_eq!(
            call.headers,
            pairs(&[("X-Goog-FieldMask", GEOCODE_FIELD_MASK)])
        );
    }

    #[test]
    fn geocode_reads_a_coordinate_address_as_a_location() {
        let call = geocode(json!({ "address": " 37.422 , -122.0841" })).unwrap();
        assert_eq!(call.path, "/v4/geocode/location");
        assert_eq!(call.query["location.latitude"], one("37.422"));
        assert_eq!(call.query["location.longitude"], one("-122.0841"));
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
        assert_eq!(call.api.query["languageCode"], one("fr"));
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
    fn geocode_caps_the_address_in_characters() {
        // Two-byte characters: the cap counts characters, as the schema's
        // `maxLength` does, not UTF-8 bytes.
        let at_cap = "é".repeat(TEXT_MAX_CHARS);
        assert_eq!(at_cap.len(), 2 * TEXT_MAX_CHARS);
        assert!(geocode(json!({ "address": at_cap })).is_ok());
        let over = format!("{at_cap}a");
        assert_eq!(
            geocode(json!({ "address": over })).unwrap_err(),
            "`address` is longer than 1024 characters"
        );
    }

    #[test]
    fn a_google_error_maps_its_status_to_a_kind() {
        let cases = [
            (400, "INVALID_ARGUMENT", ErrorKind::InvalidArgs),
            (404, "NOT_FOUND", ErrorKind::InvalidArgs),
            // A restricted key, a disabled API or billing: the operator's.
            (403, "PERMISSION_DENIED", ErrorKind::Internal),
            (429, "RESOURCE_EXHAUSTED", ErrorKind::Internal),
            (503, "UNAVAILABLE", ErrorKind::Internal),
        ];
        for (code, status, kind) in cases {
            assert_eq!(
                google_error(code, &google(code, status, "no")),
                failure(kind, &format!("Google {status} (HTTP {code}): no")),
                "{code}"
            );
        }
        assert_eq!(
            google_error(400, "<html>key=SECRET</html>"),
            failure(
                ErrorKind::InvalidArgs,
                "Google HTTP 400, body not in the standard error shape"
            )
        );
    }

    fn geocode_state(args: Value) -> InFlight {
        let call = plan(Provided::Geocode, &args, &settings()).unwrap();
        InFlight::new(
            &"a".repeat(32),
            Provided::Geocode,
            &args,
            &call,
            Args::new(),
            1_000,
        )
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
            advance(&state, http(200, GEOCODE_BODY), &settings()),
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
        let Next::Complete(answer) = advance(&state, http(200, GEOCODE_BODY), &settings()) else {
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
            advance(&state, http(200, "{}"), &settings()),
            Next::Complete(json!({ "results": [] }))
        );
    }

    #[test]
    fn geocode_refuses_an_unexpected_shape() {
        let state = geocode_state(json!({ "address": "Paris" }));
        for body in ["[]", "{\"results\": {}}", "{\"results\": [1]}"] {
            assert_eq!(
                advance(&state, http(200, body), &settings()),
                Next::Fail(failure(
                    ErrorKind::Internal,
                    "unexpected Geocoding API response shape"
                )),
                "{body}"
            );
        }
    }

    #[test]
    fn advance_answers_a_google_error_through_the_status_mapping() {
        let state = geocode_state(json!({ "address": "Paris" }));
        assert_eq!(
            advance(
                &state,
                http(403, &google(403, "PERMISSION_DENIED", "no key")),
                &settings()
            ),
            Next::Fail(failure(
                ErrorKind::Internal,
                "Google PERMISSION_DENIED (HTTP 403): no key"
            ))
        );
    }

    #[test]
    fn advance_relays_a_missing_credential_unchanged() {
        let value = json!({
            "error": "credential_invalid",
            "provider": "maps",
            "account": null,
            "detail": "no account is connected for this provider",
        });
        assert_eq!(
            advance(
                &geocode_state(json!({ "address": "Paris" })),
                Ok(value.clone()),
                &settings()
            ),
            Next::Complete(value)
        );
    }

    /// Request `args` of `tool`, its row moved to `step`.
    fn at_step(tool: Provided, args: Value, step: u8) -> InFlight {
        let call = plan(tool, &args, &settings()).unwrap();
        let mut state = InFlight::new(&"a".repeat(32), tool, &args, &call, Args::new(), 1_000);
        state.step = step;
        state
    }

    #[test]
    fn an_unreadable_answer_names_what_to_narrow_per_step() {
        let route = json!({ "origin": "Paris", "destination": "Lyon", "query": "tea" });
        let cases = [
            (Provided::Geocode, json!({ "address": "Paris" }), 1, None),
            (
                Provided::Weather,
                json!({ "kind": "current", "place": "Paris" }),
                WEATHER_GEOCODE_STEP,
                None,
            ),
            (
                Provided::Weather,
                json!({ "kind": "hourly", "lat": 1, "lng": 2 }),
                WEATHER_LOOKUP_STEP,
                None,
            ),
            (
                Provided::Weather,
                json!({ "kind": "daily", "lat": 1, "lng": 2 }),
                WEATHER_LOOKUP_STEP,
                Some("ask for fewer `days`"),
            ),
            (
                Provided::Directions,
                json!({ "origin": "Paris", "destination": "Lyon" }),
                1,
                None,
            ),
            (
                Provided::PlacesSearch,
                json!({ "query": "tea", "near": "Paris" }),
                PLACES_GEOCODE_STEP,
                None,
            ),
            (
                Provided::PlacesSearch,
                json!({ "query": "tea" }),
                PLACES_SEARCH_STEP,
                Some("ask for fewer places with `max`"),
            ),
            (Provided::PlaceDetails, json!({ "id": "ChIJ1" }), 1, None),
            (
                Provided::PlaceDetails,
                json!({ "id": "ChIJ1", "reviews": true }),
                1,
                Some("ask without `reviews`"),
            ),
            (
                Provided::SearchAlongRoute,
                route.clone(),
                ALONG_ROUTE_SEARCH_STEP,
                Some("ask for fewer places with `max`"),
            ),
        ];
        let hinted = |why: &str, hint: Option<&str>| match hint {
            Some(hint) => format!("{why}; {hint}"),
            None => why.to_string(),
        };
        for (tool, args, step, hint) in cases {
            let state = at_step(tool, args, step);
            let what = format!("{} step {step}", tool.name());
            assert_eq!(
                advance(&state, truncated(200, json!(300000), "{\"res"), &settings()),
                Next::Fail(failure(
                    ErrorKind::Internal,
                    &hinted(
                        "Google's answer (300000 bytes) exceeds api-request's read limit for \
                         this provider (max_response_bytes)",
                        hint
                    )
                )),
                "{what}"
            );
            assert_eq!(
                advance(
                    &state,
                    nested("internal", "exceeds max_body_bytes"),
                    &settings()
                ),
                Next::Fail(failure(
                    ErrorKind::Internal,
                    &hinted(
                        "the host did not deliver api-request's answer: exceeds max_body_bytes",
                        hint
                    )
                )),
                "{what}"
            );
        }
        // A refused route answer may be a transport fault: not the caller's.
        let state = at_step(Provided::SearchAlongRoute, route, ALONG_ROUTE_ROUTE_STEP);
        assert_eq!(
            advance(&state, nested("internal", "connection reset"), &settings()),
            Next::Fail(failure(
                ErrorKind::Internal,
                "the host did not deliver api-request's answer: connection reset; search along \
                 a shorter route"
            ))
        );
    }

    /// The empty-answer arm runs before `advance` dispatches on the step, so
    /// one step stands for all of them.
    #[test]
    fn an_empty_answer_is_internal() {
        let state = at_step(Provided::Geocode, json!({ "address": "Paris" }), 1);
        for status in [200, 204] {
            assert_eq!(
                advance(&state, http(status, ""), &settings()),
                Next::Fail(failure(
                    ErrorKind::Internal,
                    &format!("Google answered HTTP {status} with no body")
                )),
                "HTTP {status}"
            );
        }
    }

    #[test]
    fn an_unreadable_answer_at_a_step_the_tool_does_not_have_is_internal() {
        let state = at_step(Provided::Geocode, json!({ "address": "Paris" }), 2);
        assert_eq!(
            advance(&state, truncated(200, json!(300000), "{\"res"), &settings()),
            Next::Fail(failure(
                ErrorKind::Internal,
                "in-flight row names step 2 of geocode, which has no such step"
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

    fn weather(args: Value) -> Result<Call, String> {
        plan(Provided::Weather, &args, &settings())
    }

    fn weather_state(args: Value, call: &Call, derived: Args) -> InFlight {
        InFlight::new(
            &"a".repeat(32),
            Provided::Weather,
            &args,
            call,
            derived,
            1_000,
        )
    }

    #[test]
    fn weather_at_coordinates_is_one_lookup() {
        let call = weather(json!({ "kind": "current", "lat": 37.422, "lng": -122.0841 })).unwrap();
        assert_eq!(call.step, WEATHER_LOOKUP_STEP);
        assert_eq!(call.api.provider, "maps");
        assert_eq!(call.api.method, "GET");
        assert_eq!(call.api.host, "weather.example.com");
        assert_eq!(call.api.path, "/v1/currentConditions:lookup");
        assert_eq!(
            call.api.query,
            params(&[
                ("location.latitude", "37.422"),
                ("location.longitude", "-122.0841"),
                ("unitsSystem", "METRIC"),
            ])
        );
        assert!(call.api.headers.is_empty());
        assert_eq!(call.api.body, None);
        // A coordinate place is not geocoded.
        let call = weather(json!({ "kind": "current", "place": "37.422,-122.0841" })).unwrap();
        assert_eq!(call.step, WEATHER_LOOKUP_STEP);
        assert_eq!(call.api.query["location.latitude"], one("37.422"));
    }

    #[test]
    fn weather_hourly_asks_for_one_page_of_the_horizon() {
        let call = weather(json!({ "kind": "hourly", "lat": 1, "lng": 2 })).unwrap();
        assert_eq!(call.api.path, "/v1/forecast/hours:lookup");
        assert_eq!(
            call.api.query,
            params(&[
                ("location.latitude", "1"),
                ("location.longitude", "2"),
                ("unitsSystem", "METRIC"),
                ("hours", "24"),
                ("pageSize", "24"),
            ])
        );
        let s = Settings::from_lookup(lookup(&with(&[("units", "IMPERIAL"), ("language", "de")])))
            .unwrap();
        let call = plan(
            Provided::Weather,
            &json!({ "kind": "hourly", "lat": 1, "lng": 2, "hours": 240, "page": "tok" }),
            &s,
        )
        .unwrap();
        assert_eq!(call.api.query["hours"], one("240"));
        assert_eq!(call.api.query["pageSize"], one("24"));
        assert_eq!(call.api.query["pageToken"], one("tok"));
        assert_eq!(call.api.query["unitsSystem"], one("IMPERIAL"));
        assert_eq!(call.api.query["languageCode"], one("de"));
    }

    #[test]
    fn weather_daily_is_one_page_of_days() {
        let call = weather(json!({ "kind": "daily", "lat": 1, "lng": 2 })).unwrap();
        assert_eq!(call.api.path, "/v1/forecast/days:lookup");
        assert_eq!(call.api.query["days"], one("5"));
        assert_eq!(call.api.query["pageSize"], one("5"));
        let call = weather(json!({ "kind": "daily", "lat": 1, "lng": 2, "days": 10 })).unwrap();
        assert_eq!(call.api.query["days"], one("10"));
        assert_eq!(call.api.query["pageSize"], one("10"));
        assert!(!call.api.query.contains_key("pageToken"));
    }

    #[test]
    fn weather_alerts_take_no_unit_system_and_page_at_the_cap() {
        let call = weather(json!({ "kind": "alerts", "lat": 1, "lng": 2, "page": "tok" })).unwrap();
        assert_eq!(call.api.path, "/v1/publicAlerts:lookup");
        assert_eq!(
            call.api.query,
            params(&[
                ("location.latitude", "1"),
                ("location.longitude", "2"),
                ("pageSize", "20"),
                ("pageToken", "tok"),
            ])
        );
        assert_eq!(ALERTS_MAX, 20);
    }

    #[test]
    fn weather_for_a_place_geocodes_first() {
        let call = weather(json!({ "kind": "current", "place": "Mountain View" })).unwrap();
        assert_eq!(call.step, WEATHER_GEOCODE_STEP);
        assert_eq!(call.api.host, "geocode.example.com");
        assert_eq!(call.api.path, "/v4/geocode/address");
        assert_eq!(call.api.query, params(&[("addressQuery", "Mountain View")]));
        assert_eq!(
            call.api.headers,
            pairs(&[("X-Goog-FieldMask", GEOCODE_FIELD_MASK)])
        );
    }

    #[test]
    fn weather_refuses_bad_arguments_by_name() {
        let cases = [
            (json!({ "lat": 1, "lng": 2 }), "`kind` is required"),
            (
                json!({ "kind": "minutely", "lat": 1, "lng": 2 }),
                KIND_CHOICES,
            ),
            (json!({ "kind": 1, "lat": 1, "lng": 2 }), KIND_CHOICES),
            (
                json!({ "kind": "current" }),
                "pass `place` or `lat` and `lng`",
            ),
            (
                json!({ "kind": "current", "place": "Paris", "lat": 1, "lng": 2 }),
                "pass `place` or `lat` and `lng`, not both",
            ),
            (
                json!({ "kind": "daily", "lat": 1, "lng": 2, "hours": 5 }),
                "`hours` applies to `kind: hourly` only",
            ),
            (
                json!({ "kind": "hourly", "lat": 1, "lng": 2, "days": 5 }),
                "`days` applies to `kind: daily` only",
            ),
            (
                json!({ "kind": "current", "lat": 1, "lng": 2, "page": "tok" }),
                "`page` applies to `kind: hourly` and `kind: alerts` only",
            ),
            (
                json!({ "kind": "daily", "lat": 1, "lng": 2, "page": "tok" }),
                "`page` applies to `kind: hourly` and `kind: alerts` only",
            ),
            (
                json!({ "kind": "hourly", "lat": 1, "lng": 2, "hours": 241 }),
                "`hours` must be an integer from 1 to 240",
            ),
            (
                json!({ "kind": "hourly", "lat": 1, "lng": 2, "hours": 0 }),
                "`hours` must be an integer from 1 to 240",
            ),
            (
                json!({ "kind": "daily", "lat": 1, "lng": 2, "days": 11 }),
                "`days` must be an integer from 1 to 10",
            ),
            (
                json!({ "kind": "hourly", "lat": 1, "lng": 2, "page": "" }),
                "`page` is empty",
            ),
        ];
        for (args, want) in cases {
            assert_eq!(weather(args.clone()).unwrap_err(), want, "{args}");
        }
        let err = weather(json!({ "kind": "current", "place": "x", "address": "y" })).unwrap_err();
        assert!(err.starts_with("unknown argument `address`"), "{err}");
        let err = weather(json!({ "kind": "current", "place": "95,1" })).unwrap_err();
        assert!(err.contains("latitude 95"), "{err}");
    }

    #[test]
    fn weather_caps_place_and_page_in_characters() {
        let place = "é".repeat(TEXT_MAX_CHARS);
        assert!(weather(json!({ "kind": "current", "place": place })).is_ok());
        assert_eq!(
            weather(json!({ "kind": "current", "place": format!("{place}a") })).unwrap_err(),
            "`place` is longer than 1024 characters"
        );
        let page = "ü".repeat(PAGE_TOKEN_MAX_CHARS);
        let ok = json!({ "kind": "hourly", "lat": 1, "lng": 2, "page": page });
        assert_eq!(weather(ok).unwrap().api.query["pageToken"], one(&page));
        let over = json!({ "kind": "hourly", "lat": 1, "lng": 2, "page": format!("{page}t") });
        assert_eq!(
            weather(over).unwrap_err(),
            "`page` is longer than 4096 characters"
        );
    }

    const CURRENT_BODY: &str = r#"{
      "currentTime": "2025-07-01T12:33:24.606422609Z",
      "timeZone": { "id": "America/Los_Angeles" },
      "isDaytime": true,
      "weatherCondition": { "iconBaseUri": "https://maps.example.com/i", "description": { "text": "Partly cloudy", "languageCode": "en" }, "type": "PARTLY_CLOUDY" },
      "temperature": { "degrees": 16, "unit": "CELSIUS" },
      "feelsLikeTemperature": { "degrees": 15.5, "unit": "CELSIUS" },
      "dewPoint": { "degrees": 12, "unit": "CELSIUS" },
      "heatIndex": { "degrees": 16, "unit": "CELSIUS" },
      "relativeHumidity": 78, "uvIndex": 0, "thunderstormProbability": 0, "cloudCover": 40,
      "precipitation": { "probability": { "percent": 3, "type": "RAIN" }, "qpf": { "quantity": 0, "unit": "MILLIMETERS" } },
      "airPressure": { "meanSeaLevelMillibars": 1013.2 },
      "wind": { "direction": { "degrees": 270, "cardinal": "WEST" }, "speed": { "value": 10, "unit": "KILOMETERS_PER_HOUR" }, "gust": { "value": 19, "unit": "KILOMETERS_PER_HOUR" } },
      "visibility": { "distance": 16, "unit": "KILOMETERS" },
      "currentConditionsHistory": { "maxTemperature": { "degrees": 21 }, "minTemperature": { "degrees": 11 } }
    }"#;

    const MOUNTAIN_VIEW: &str = r#"{ "results": [
      { "placeId": "ChIJ1", "location": { "latitude": 37.39, "longitude": -122.08 },
        "formattedAddress": "Mountain View, CA, USA" },
      { "placeId": "ChIJ2", "location": { "latitude": 1, "longitude": 2 } }
    ] }"#;

    #[test]
    fn weather_for_a_place_looks_up_where_the_geocode_landed() {
        let args = json!({ "kind": "current", "place": "Mountain View" });
        let step1 = weather(args.clone()).unwrap();
        let state = weather_state(args.clone(), &step1, Args::new());
        let Next::Call { call, derived } = advance(&state, http(200, MOUNTAIN_VIEW), &settings())
        else {
            panic!("expected the lookup step");
        };
        assert_eq!(call.step, WEATHER_LOOKUP_STEP);
        assert_eq!(call.api.host, "weather.example.com");
        assert_eq!(call.api.path, "/v1/currentConditions:lookup");
        assert_eq!(call.api.query["location.latitude"], one("37.39"));
        assert_eq!(call.api.query["location.longitude"], one("-122.08"));
        let want = json!({ "lat": 37.39, "lng": -122.08, "place": "Mountain View, CA, USA" });
        assert_eq!(Value::Object(derived.clone()), want);

        // The lookup's row is the same request's, and carries the location
        // through the store.
        let step2 = state.next(&call, derived, 2_000);
        assert_eq!(step2.call_id(), format!("{}/2", "a".repeat(32)));
        assert_eq!(
            (step2.tool, &step2.args, step2.step, step2.issued_at_ms),
            (Provided::Weather, &args, WEATHER_LOOKUP_STEP, 2_000)
        );
        assert_eq!(step2.target, call.api.target());
        let step2 = InFlight::decode(&step2.encode()).unwrap();
        let Next::Complete(answer) = advance(&step2, http(200, CURRENT_BODY), &settings()) else {
            panic!("expected an answer");
        };
        assert_eq!(answer["location"], want);
    }

    #[test]
    fn weather_for_a_place_nothing_matches_is_the_callers() {
        let args = json!({ "kind": "daily", "place": "Nowhere Special" });
        let state = weather_state(args.clone(), &weather(args).unwrap(), Args::new());
        for body in ["{}", "{\"results\": []}"] {
            assert_eq!(
                advance(&state, http(200, body), &settings()),
                Next::Fail(failure(
                    ErrorKind::InvalidArgs,
                    "no location found for Nowhere Special"
                )),
                "{body}"
            );
        }
        for body in [
            "[]",
            "{\"results\": [{\"placeId\": \"x\"}]}",
            "{\"results\": 1}",
        ] {
            assert_eq!(
                advance(&state, http(200, body), &settings()),
                Next::Fail(failure(
                    ErrorKind::Internal,
                    "unexpected Geocoding API response shape"
                )),
                "{body}"
            );
        }
    }

    #[test]
    fn weather_geocode_step_for_coordinates_is_internal() {
        let args = json!({ "kind": "current", "lat": 1, "lng": 2 });
        let call = Call {
            step: WEATHER_GEOCODE_STEP,
            api: geocode_call(&Where::Text("Mountain View".to_string()), &settings()),
        };
        let state = weather_state(args, &call, Args::new());
        assert_eq!(
            advance(&state, http(200, MOUNTAIN_VIEW), &settings()),
            Next::Fail(failure(
                ErrorKind::Internal,
                "in-flight row names weather's geocode step for a request given coordinates"
            ))
        );
    }

    #[test]
    fn weather_lookup_without_its_geocoded_location_is_internal() {
        let args = json!({ "kind": "current", "place": "Mountain View" });
        let call = Call {
            step: WEATHER_LOOKUP_STEP,
            api: weather_call(&WeatherArgs::parse(&args).unwrap(), 1.0, 2.0, &settings()),
        };
        let state = weather_state(args, &call, Args::new());
        assert_eq!(
            advance(&state, http(200, CURRENT_BODY), &settings()),
            Next::Fail(failure(
                ErrorKind::Internal,
                "in-flight row for weather's lookup carries no geocoded location"
            ))
        );
    }

    /// The answer to a Weather lookup made at coordinates.
    fn weather_answer(args: Value, body: &str, settings: &Settings) -> Next {
        let call = plan(Provided::Weather, &args, settings).unwrap();
        advance(
            &weather_state(args, &call, Args::new()),
            http(200, body),
            settings,
        )
    }

    #[test]
    fn weather_current_is_compacted() {
        let answer = weather_answer(
            json!({ "kind": "current", "lat": 37.4, "lng": -122.1 }),
            CURRENT_BODY,
            &settings(),
        );
        assert_eq!(
            answer,
            Next::Complete(json!({
                "location": { "lat": 37.4, "lng": -122.1 },
                "time": "2025-07-01T12:33:24.606422609Z",
                "timezone": "America/Los_Angeles",
                "units": { "temperature": "°C", "speed": "km/h", "distance": "km", "precipitation": "mm" },
                "condition": "Partly cloudy",
                "is_daytime": true,
                "temperature": 16, "feels_like": 15.5, "dew_point": 12, "humidity_percent": 78,
                "uv_index": 0, "cloud_cover_percent": 40, "thunderstorm_percent": 0,
                "wind": { "speed": 10, "gust": 19, "direction": "WEST" },
                "precipitation": { "probability_percent": 3, "type": "RAIN", "amount": 0 },
                "pressure_mb": 1013.2,
                "visibility": 16,
                "today": { "high": 21, "low": 11 },
            }))
        );
        let imperial = Settings::from_lookup(lookup(&with(&[("units", "IMPERIAL")]))).unwrap();
        let Next::Complete(answer) = weather_answer(
            json!({ "kind": "current", "lat": 1, "lng": 2 }),
            r#"{ "temperature": { "degrees": 60 } }"#,
            &imperial,
        ) else {
            panic!("expected an answer");
        };
        // Absent fields are omitted, never invented.
        assert_eq!(
            answer,
            json!({
                "location": { "lat": 1.0, "lng": 2.0 },
                "units": { "temperature": "°F", "speed": "mph", "distance": "mi", "precipitation": "in" },
                "temperature": 60,
            })
        );
    }

    fn hour(day: u64, hour: Option<u64>) -> Value {
        let mut when = json!({ "year": 2025, "month": 7, "day": day, "utcOffset": "-25200s" });
        if let Some(hour) = hour {
            when["hours"] = json!(hour);
        }
        json!({
            "interval": { "startTime": "2025-07-01T19:00:00Z" },
            "displayDateTime": when,
            "weatherCondition": { "description": { "text": "Clear" } },
            "temperature": { "degrees": 20 }, "feelsLikeTemperature": { "degrees": 19 },
            "precipitation": { "probability": { "percent": 10, "type": "RAIN" }, "qpf": { "quantity": 0.1 } },
            "wind": { "direction": { "cardinal": "NORTH" }, "speed": { "value": 7 } },
            "relativeHumidity": 50, "uvIndex": 3, "cloudCover": 5, "isDaytime": false,
            "airPressure": { "meanSeaLevelMillibars": 1000 },
        })
    }

    #[test]
    fn weather_hourly_is_compacted_with_its_next_page() {
        let body = json!({
            "forecastHours": [hour(1, Some(12)), hour(2, None)],
            "timeZone": { "id": "America/Los_Angeles" },
            "nextPageToken": "next-tok",
        });
        let answer = weather_answer(
            json!({ "kind": "hourly", "lat": 1, "lng": 2 }),
            &body.to_string(),
            &settings(),
        );
        let Next::Complete(answer) = answer else {
            panic!("expected an answer: {answer:?}");
        };
        assert_eq!(answer["timezone"], "America/Los_Angeles");
        assert_eq!(answer["units"]["temperature"], "°C");
        assert_eq!(answer["next_page"], "next-tok");
        assert_eq!(
            answer["hours"][0],
            json!({
                "time": "2025-07-01T12:00", "condition": "Clear", "temperature": 20, "feels_like": 19,
                "precip_probability_percent": 10, "precip_type": "RAIN", "precip_amount": 0.1,
                "wind_speed": 7, "wind_direction": "NORTH", "humidity_percent": 50, "uv_index": 3,
                "cloud_cover_percent": 5, "is_daytime": false,
            })
        );
        // Google's JSON omits a zero hour.
        assert_eq!(answer["hours"][1]["time"], "2025-07-02T00:00");
        assert!(answer.get("hours_omitted").is_none());
    }

    #[test]
    fn weather_hourly_beyond_a_page_is_counted_not_dropped() {
        let hours: Vec<Value> = (0..25).map(|h| hour(1, Some(h % 24))).collect();
        let body = json!({ "forecastHours": hours });
        let Next::Complete(answer) = weather_answer(
            json!({ "kind": "hourly", "lat": 1, "lng": 2 }),
            &body.to_string(),
            &settings(),
        ) else {
            panic!("expected an answer");
        };
        assert_eq!(answer["hours"].as_array().unwrap().len(), 24);
        assert_eq!(answer["hours_omitted"], 1);
        assert!(answer.get("next_page").is_none());
    }

    #[test]
    fn weather_daily_is_compacted_with_day_and_night() {
        let part = |text: &str| {
            json!({
                "weatherCondition": { "description": { "text": text } },
                "precipitation": { "probability": { "percent": 20, "type": "SNOW" } },
                "relativeHumidity": 40, "uvIndex": 6, "thunderstormProbability": 5, "cloudCover": 30,
            })
        };
        let body = json!({
            "forecastDays": [{
                "displayDate": { "year": 2025, "month": 2, "day": 3 },
                "daytimeForecast": part("Sunny"),
                "nighttimeForecast": part("Clear"),
                "maxTemperature": { "degrees": 25 }, "minTemperature": { "degrees": 12 },
                "feelsLikeMaxTemperature": { "degrees": 26 }, "feelsLikeMinTemperature": { "degrees": 11 },
                "sunEvents": { "sunriseTime": "2025-02-03T14:00:00Z" },
            }],
            "timeZone": { "id": "Europe/Paris" },
        });
        let Next::Complete(answer) = weather_answer(
            json!({ "kind": "daily", "lat": 1, "lng": 2, "days": 1 }),
            &body.to_string(),
            &settings(),
        ) else {
            panic!("expected an answer");
        };
        assert_eq!(answer["timezone"], "Europe/Paris");
        assert_eq!(answer["day_parts"], DAY_PARTS);
        let day_part = |text: &str| {
            json!({
                "condition": text, "precip_probability_percent": 20, "precip_type": "SNOW",
                "humidity_percent": 40, "uv_index": 6, "cloud_cover_percent": 30,
                "thunderstorm_percent": 5,
            })
        };
        assert_eq!(
            answer["days"],
            json!([{
                "date": "2025-02-03", "high": 25, "low": 12, "feels_like_high": 26,
                "feels_like_low": 11, "sunrise": "2025-02-03T14:00:00Z",
                "day": day_part("Sunny"), "night": day_part("Clear"),
            }])
        );
    }

    #[test]
    fn weather_daily_beyond_the_cap_is_counted_with_its_next_page() {
        let days: Vec<Value> = (1..=DAYS_MAX + 1)
            .map(|d| json!({ "displayDate": { "year": 2025, "month": 2, "day": d } }))
            .collect();
        let body = json!({ "forecastDays": days, "nextPageToken": "more-days" });
        let Next::Complete(answer) = weather_answer(
            json!({ "kind": "daily", "lat": 1, "lng": 2, "days": 10 }),
            &body.to_string(),
            &settings(),
        ) else {
            panic!("expected an answer");
        };
        let dates: Vec<Value> = (1..=DAYS_MAX)
            .map(|d| json!({ "date": format!("2025-02-{d:02}") }))
            .collect();
        assert_eq!(answer["days"], Value::Array(dates));
        assert_eq!(answer["days_omitted"], 1);
        assert_eq!(answer["next_page"], "more-days");
        // The cap is the API's 10, not the `days` asked for: a shorter
        // request answered with more days relays them.
        let Next::Complete(answer) = weather_answer(
            json!({ "kind": "daily", "lat": 1, "lng": 2, "days": 2 }),
            &body.to_string(),
            &settings(),
        ) else {
            panic!("expected an answer");
        };
        assert_eq!(answer["days"].as_array().unwrap().len(), 10);
        assert_eq!(answer["days_omitted"], 1);
    }

    fn alert(n: usize) -> Value {
        json!({
            "alertId": format!("urn:{n}"),
            "alertTitle": { "text": "Flash Flood Warning", "languageCode": "en" },
            "eventType": "FLASH_FLOOD", "areaName": "Wake, NC",
            "description": "d".repeat(ALERT_DESCRIPTION_MAX_BYTES + 1),
            "severity": "SEVERE", "certainty": "LIKELY", "urgency": "IMMEDIATE",
            "instruction": ["one", "two", "three", "four", "five", "six"],
            "safetyRecommendations": [{ "directive": "Move to higher ground", "subtext": "now" }],
            "timezoneOffset": "-14400s",
            "startTime": "2025-08-06T18:24:00Z", "expirationTime": "2025-08-06T21:30:00Z",
            "dataSource": { "publisher": "NOAA", "name": "National Weather Service", "authorityUri": "https://weather.example.com/" },
            "polygon": "{}",
        })
    }

    #[test]
    fn weather_alerts_are_compacted_with_their_source() {
        let body =
            json!({ "weatherAlerts": [alert(1)], "regionCode": "US", "nextPageToken": "p2" });
        let Next::Complete(answer) = weather_answer(
            json!({ "kind": "alerts", "lat": 1, "lng": 2 }),
            &body.to_string(),
            &settings(),
        ) else {
            panic!("expected an answer");
        };
        assert_eq!(
            answer,
            json!({
                "location": { "lat": 1.0, "lng": 2.0 },
                "region": "US",
                "alerts": [{
                    "title": "Flash Flood Warning", "event": "FLASH_FLOOD", "severity": "SEVERE",
                    "certainty": "LIKELY", "urgency": "IMMEDIATE", "area": "Wake, NC",
                    "starts": "2025-08-06T18:24:00Z", "ends": "2025-08-06T21:30:00Z",
                    "description": format!("{}…", "d".repeat(ALERT_DESCRIPTION_MAX_BYTES)),
                    "instructions": ["one", "two", "three", "four", "five"],
                    "instructions_omitted": 1,
                    "recommendations": [{ "directive": "Move to higher ground", "subtext": "now" }],
                    "source": { "name": "National Weather Service", "url": "https://weather.example.com/" },
                }],
                "next_page": "p2",
            })
        );
    }

    #[test]
    fn weather_alert_description_is_cut_in_bytes() {
        let mut long = alert(1);
        long["description"] = json!("é".repeat(ALERT_DESCRIPTION_MAX_BYTES));
        assert_eq!(
            compact_alert(&long).unwrap()["description"],
            format!("{}…", "é".repeat(ALERT_DESCRIPTION_MAX_BYTES / 2))
        );
    }

    #[test]
    fn weather_alert_without_a_full_source_is_still_relayed() {
        let mut bare = alert(1);
        bare.as_object_mut().unwrap().remove("dataSource");
        let mut nameless = alert(2);
        nameless["dataSource"] = json!({ "authorityUri": "https://weather.example.com/" });
        let body = json!({ "weatherAlerts": [bare, nameless], "regionCode": "US" });
        let Next::Complete(answer) = weather_answer(
            json!({ "kind": "alerts", "lat": 1, "lng": 2 }),
            &body.to_string(),
            &settings(),
        ) else {
            panic!("expected an answer");
        };
        assert_eq!(answer["alerts"][0]["title"], "Flash Flood Warning");
        assert!(answer["alerts"][0].get("source").is_none());
        assert_eq!(
            answer["alerts"][1]["source"],
            json!({ "url": "https://weather.example.com/" })
        );
    }

    #[test]
    fn weather_alerts_only_a_region_is_an_empty_list() {
        let Next::Complete(answer) = weather_answer(
            json!({ "kind": "alerts", "lat": 1, "lng": 2 }),
            r#"{ "regionCode": "FR" }"#,
            &settings(),
        ) else {
            panic!("expected an answer");
        };
        assert_eq!(
            answer,
            json!({ "location": { "lat": 1.0, "lng": 2.0 }, "region": "FR", "alerts": [] })
        );
    }

    #[test]
    fn weather_alerts_beyond_the_cap_are_counted() {
        let alerts: Vec<Value> = (0..ALERTS_MAX + 3).map(alert).collect();
        let body = json!({ "weatherAlerts": alerts, "regionCode": "US" });
        let Next::Complete(answer) = weather_answer(
            json!({ "kind": "alerts", "lat": 1, "lng": 2 }),
            &body.to_string(),
            &settings(),
        ) else {
            panic!("expected an answer");
        };
        assert_eq!(answer["alerts"].as_array().unwrap().len(), ALERTS_MAX);
        assert_eq!(answer["alerts_omitted"], 3);
    }

    #[test]
    fn weather_refuses_an_unexpected_shape_per_kind() {
        let cases = [
            ("current", r#"{ "currentTime": "x" }"#),
            ("hourly", r#"{ "timeZone": { "id": "UTC" } }"#),
            ("hourly", r#"{ "forecastHours": {} }"#),
            ("hourly", r#"{ "forecastHours": [1] }"#),
            ("daily", r#"{ "forecastHours": [] }"#),
            ("alerts", r#"{ "nextPageToken": "x" }"#),
            (
                "alerts",
                r#"{ "regionCode": "US", "weatherAlerts": [{ "instruction": [1] }] }"#,
            ),
            (
                "alerts",
                r#"{ "regionCode": "US", "weatherAlerts": [{ "safetyRecommendations": ["x"] }] }"#,
            ),
            (
                "alerts",
                r#"{ "regionCode": "US", "weatherAlerts": [{ "instruction": null }] }"#,
            ),
            ("alerts", r#"{ "regionCode": "US", "weatherAlerts": [1] }"#),
            ("daily", r#"{ "forecastDays": [[]] }"#),
            ("current", "[]"),
        ];
        for (kind, body) in cases {
            assert_eq!(
                weather_answer(
                    json!({ "kind": kind, "lat": 1, "lng": 2 }),
                    body,
                    &settings()
                ),
                Next::Fail(failure(
                    ErrorKind::Internal,
                    "unexpected Weather API response shape"
                )),
                "{kind} {body}"
            );
        }
    }

    fn directions(args: Value) -> Result<ApiCall, String> {
        plan(Provided::Directions, &args, &settings()).map(|call| {
            assert_eq!(call.step, 1);
            call.api
        })
    }

    /// The JSON body a call posts.
    fn posted(call: &ApiCall) -> Value {
        serde_json::from_str(call.body.as_deref().expect("a body")).expect("a JSON body")
    }

    #[test]
    fn directions_posts_one_compute_routes_call() {
        let call = directions(json!({
            "origin": "37.422,-122.0841",
            "destination": "Mountain View",
        }))
        .unwrap();
        assert_eq!(call.provider, "maps");
        assert_eq!(call.method, "POST");
        assert_eq!(call.host, "routes.example.com");
        assert_eq!(call.path, "/directions/v2:computeRoutes");
        assert!(call.query.is_empty());
        assert_eq!(
            call.headers,
            pairs(&[("X-Goog-FieldMask", DIRECTIONS_FIELD_MASK)])
        );
        assert!(!DIRECTIONS_FIELD_MASK.contains("polyline"));
        assert_eq!(
            posted(&call),
            json!({
                "origin": { "location": { "latLng": { "latitude": 37.422, "longitude": -122.0841 } } },
                "destination": { "address": "Mountain View" },
                "travelMode": "DRIVE",
                "routingPreference": "TRAFFIC_AWARE",
                "computeAlternativeRoutes": false,
                "units": "METRIC",
            })
        );
    }

    #[test]
    fn directions_reads_each_waypoint_form() {
        let body = posted(
            &directions(json!({
                "origin": " place_id:ChIJ1 ",
                "destination": "12 Main St, Springfield",
            }))
            .unwrap(),
        );
        assert_eq!(body["origin"], json!({ "placeId": "ChIJ1" }));
        assert_eq!(
            body["destination"],
            json!({ "address": "12 Main St, Springfield" })
        );
        let err = directions(json!({ "origin": "place_id: ", "destination": "x" })).unwrap_err();
        assert_eq!(err, "`origin`: `place_id:` names no place id");
        let err = directions(json!({ "origin": "x", "destination": "0,181" })).unwrap_err();
        assert!(
            err.contains("`destination`") && err.contains("longitude 181"),
            "{err}"
        );
    }

    #[test]
    fn directions_maps_each_mode_and_prefers_traffic_only_driving() {
        for (mode, google) in [
            ("drive", "DRIVE"),
            ("walk", "WALK"),
            ("bicycle", "BICYCLE"),
            ("transit", "TRANSIT"),
        ] {
            let body = posted(
                &directions(json!({ "origin": "a", "destination": "b", "mode": mode })).unwrap(),
            );
            assert_eq!(body["travelMode"], google, "{mode}");
            assert_eq!(
                body.get("routingPreference").is_some(),
                mode == "drive",
                "{mode}"
            );
        }
    }

    #[test]
    fn directions_pass_units_and_language() {
        let s = Settings::from_lookup(lookup(&with(&[("units", "IMPERIAL"), ("language", "de")])))
            .unwrap();
        let call = plan(
            Provided::Directions,
            &json!({ "origin": "a", "destination": "b" }),
            &s,
        )
        .unwrap();
        let body = posted(&call.api);
        assert_eq!(body["units"], "IMPERIAL");
        assert_eq!(body["languageCode"], "de");
    }

    #[test]
    fn directions_send_a_departure_or_a_transit_arrival() {
        let body = posted(
            &directions(json!({
                "origin": "a", "destination": "b", "depart_at": "2026-10-30T17:00:00Z",
            }))
            .unwrap(),
        );
        assert_eq!(body["departureTime"], "2026-10-30T17:00:00Z");
        assert!(body.get("arrivalTime").is_none());
        let body = posted(
            &directions(json!({
                "origin": "a", "destination": "b", "mode": "transit",
                "arrive_by": "2026-10-30T09:30:00.250-07:00",
            }))
            .unwrap(),
        );
        assert_eq!(body["arrivalTime"], "2026-10-30T09:30:00.250-07:00");
        assert!(body.get("departureTime").is_none());
    }

    #[test]
    fn directions_refuse_an_arrival_google_would_ignore() {
        for mode in ["drive", "walk", "bicycle"] {
            assert_eq!(
                directions(json!({
                    "origin": "a", "destination": "b", "mode": mode,
                    "arrive_by": "2026-10-30T17:00:00Z",
                }))
                .unwrap_err(),
                "`arrive_by` applies to `mode: transit` only",
                "{mode}"
            );
        }
        // Without a mode the route is driven.
        assert!(
            directions(
                json!({ "origin": "a", "destination": "b", "arrive_by": "2026-10-30T17:00:00Z" })
            )
            .is_err()
        );
    }

    #[test]
    fn directions_refuse_bad_arguments_by_name() {
        let cases = [
            (json!({ "destination": "b" }), "`origin` is required"),
            (json!({ "origin": "a" }), "`destination` is required"),
            (
                json!({ "origin": 1, "destination": "b" }),
                "`origin` must be a string",
            ),
            (
                json!({ "origin": "a", "destination": " " }),
                "`destination` is empty",
            ),
            (
                json!({ "origin": "a", "destination": "b", "mode": "fly" }),
                MODE_CHOICES,
            ),
            (
                json!({ "origin": "a", "destination": "b", "mode": 1 }),
                MODE_CHOICES,
            ),
            (
                json!({
                    "origin": "a", "destination": "b", "mode": "transit",
                    "depart_at": "2026-10-30T17:00:00Z", "arrive_by": "2026-10-30T18:00:00Z",
                }),
                "pass `depart_at` or `arrive_by`, not both",
            ),
            (
                json!({ "origin": "a", "destination": "b", "max_steps": 0 }),
                "`max_steps` must be an integer from 1 to 200",
            ),
            (
                json!({ "origin": "a", "destination": "b", "max_steps": 201 }),
                "`max_steps` must be an integer from 1 to 200",
            ),
        ];
        for (args, want) in cases {
            assert_eq!(directions(args.clone()).unwrap_err(), want, "{args}");
        }
        let err = directions(json!({ "origin": "a", "destination": "b", "via": "c" })).unwrap_err();
        assert!(err.starts_with("unknown argument `via`"), "{err}");
    }

    #[test]
    fn directions_cap_both_ends_in_characters() {
        let at_cap = "é".repeat(TEXT_MAX_CHARS);
        assert!(directions(json!({ "origin": at_cap, "destination": at_cap })).is_ok());
        let over = format!("{at_cap}a");
        assert_eq!(
            directions(json!({ "origin": over, "destination": "b" })).unwrap_err(),
            "`origin` is longer than 1024 characters"
        );
        assert_eq!(
            directions(json!({ "origin": "a", "destination": over })).unwrap_err(),
            "`destination` is longer than 1024 characters"
        );
    }

    fn directions_answer(args: Value, body: &str, settings: &Settings) -> Next {
        let call = plan(Provided::Directions, &args, settings).unwrap();
        let state = InFlight::new(
            &"a".repeat(32),
            Provided::Directions,
            &args,
            &call,
            Args::new(),
            1_000,
        );
        advance(&state, http(200, body), settings)
    }

    fn drive_step(n: u64) -> Value {
        json!({
            "distanceMeters": 1000 * n,
            "staticDuration": format!("{}s", 60 * n),
            "travelMode": "DRIVE",
            "navigationInstruction": { "maneuver": "TURN_LEFT", "instructions": format!("Turn left {n}") },
        })
    }

    #[test]
    fn directions_compact_a_driven_route() {
        let body = json!({
            "routes": [{
                "description": "I-280 S",
                "warnings": ["This route has tolls."],
                "distanceMeters": 12345,
                "duration": "1234s",
                "staticDuration": "1100s",
                "legs": [{
                    "distanceMeters": 12345,
                    "duration": "1234s",
                    "steps": [
                        drive_step(1),
                        { "distanceMeters": 45, "staticDuration": "3.5s", "travelMode": "DRIVE",
                          "navigationInstruction": { "instructions": "Arrive" } },
                    ],
                }],
            }],
        });
        assert_eq!(
            directions_answer(
                json!({ "origin": "a", "destination": "b" }),
                &body.to_string(),
                &settings()
            ),
            Next::Complete(json!({
                "mode": "drive",
                "summary": "I-280 S",
                "warnings": ["This route has tolls."],
                "distance": { "value": 12.35, "unit": "km" },
                "duration_minutes": 21,
                "duration_without_traffic_minutes": 19,
                "legs": [{
                    "distance": { "value": 12.35, "unit": "km" },
                    "duration_minutes": 21,
                    "steps": [
                        { "instruction": "Turn left 1", "maneuver": "TURN_LEFT",
                          "distance": { "value": 1.0, "unit": "km" }, "duration_minutes": 1,
                          "mode": "DRIVE" },
                        { "instruction": "Arrive", "distance": { "value": 0.05, "unit": "km" },
                          "duration_minutes": 1, "mode": "DRIVE" },
                    ],
                }],
            }))
        );
    }

    #[test]
    fn directions_state_distances_in_the_instances_units() {
        let s = Settings::from_lookup(lookup(&with(&[("units", "IMPERIAL")]))).unwrap();
        let body = json!({ "routes": [{ "distanceMeters": 16093, "duration": "0s" }] });
        let Next::Complete(answer) = directions_answer(
            json!({ "origin": "a", "destination": "b", "mode": "walk" }),
            &body.to_string(),
            &s,
        ) else {
            panic!("expected an answer");
        };
        assert_eq!(answer["distance"], json!({ "value": 10.0, "unit": "mi" }));
        assert_eq!(answer["duration_minutes"], 0);
        assert_eq!(answer["mode"], "walk");
        // No warnings from Google is an empty list, still carried; no legs is
        // an empty list.
        assert_eq!(answer["warnings"], json!([]));
        assert_eq!(answer["legs"], json!([]));
        assert!(answer.get("steps_omitted").is_none());
        assert!(answer.get("timezone").is_none());
    }

    #[test]
    fn directions_name_each_transit_ride() {
        let body = json!({
            "routes": [{
                "distanceMeters": 9000,
                "duration": "1500s",
                "legs": [{
                    "steps": [
                        { "distanceMeters": 200, "staticDuration": "150s", "travelMode": "WALK",
                          "navigationInstruction": { "instructions": "Walk to Aeroporto" } },
                        { "distanceMeters": 8800, "staticDuration": "1200s", "travelMode": "TRANSIT",
                          "transitDetails": {
                              "stopDetails": {
                                  "departureStop": { "name": "Aeroporto" },
                                  "arrivalStop": { "name": "Saldanha" },
                                  "departureTime": "2026-10-30T10:29:42Z",
                                  "arrivalTime": "2026-10-30T10:49:42Z",
                              },
                              "localizedValues": {
                                  "departureTime": { "time": { "text": "10:29" }, "timeZone": "Europe/Lisbon" },
                                  "arrivalTime": { "time": { "text": "10:49" }, "timeZone": "Europe/Lisbon" },
                              },
                              "headsign": "São Sebastião",
                              "headway": "420s",
                              "transitLine": {
                                  "agencies": [{ "name": "Metropolitano de Lisboa, E.P.E." }, { "name": "Other" }],
                                  "name": "Linha Vermelha",
                                  "nameShort": "V",
                                  "vehicle": { "name": { "text": "Subway" }, "type": "SUBWAY" },
                              },
                              "stopCount": 7,
                          } },
                    ],
                }],
            }],
        });
        let Next::Complete(answer) = directions_answer(
            json!({ "origin": "Aeroporto", "destination": "Saldanha", "mode": "transit" }),
            &body.to_string(),
            &settings(),
        ) else {
            panic!("expected an answer");
        };
        assert!(answer.get("timezone").is_none());
        let steps = &answer["legs"][0]["steps"];
        assert_eq!(
            steps[0],
            json!({ "instruction": "Walk to Aeroporto", "distance": { "value": 0.2, "unit": "km" },
                    "duration_minutes": 3, "mode": "WALK" })
        );
        assert_eq!(
            steps[1],
            json!({
                "distance": { "value": 8.8, "unit": "km" },
                "duration_minutes": 20,
                "mode": "TRANSIT",
                "transit": {
                    "line": "Linha Vermelha", "line_short": "V", "headsign": "São Sebastião",
                    "vehicle": "SUBWAY", "agency": "Metropolitano de Lisboa, E.P.E.",
                    "from": "Aeroporto", "departs": "10:29", "departs_timezone": "Europe/Lisbon",
                    "departs_at": "2026-10-30T10:29:42Z",
                    "to": "Saldanha", "arrives": "10:49", "arrives_timezone": "Europe/Lisbon",
                    "arrives_at": "2026-10-30T10:49:42Z",
                    "stops": 7,
                },
            })
        );
    }

    #[test]
    fn directions_give_each_transit_time_its_own_zone() {
        let ride = |from: &str, departs: Value, to: &str, arrives: Value, utc: [&str; 2]| {
            json!({ "travelMode": "TRANSIT", "transitDetails": {
                "stopDetails": {
                    "departureStop": { "name": from }, "arrivalStop": { "name": to },
                    "departureTime": utc[0], "arrivalTime": utc[1],
                },
                "localizedValues": { "departureTime": departs, "arrivalTime": arrives },
            } })
        };
        let body = json!({
            "routes": [{ "legs": [{ "steps": [
                ride(
                    "Lisboa Oriente",
                    json!({ "time": { "text": "23:40" } }),
                    "Badajoz",
                    json!({ "time": { "text": "04:05" }, "timeZone": "Europe/Madrid" }),
                    ["2026-10-30T23:40:00Z", "2026-10-31T03:05:00Z"],
                ),
                ride(
                    "Badajoz",
                    json!({ "time": { "text": "05:00" }, "timeZone": "Europe/Madrid" }),
                    "Madrid Atocha",
                    json!({ "time": { "text": "10:15" }, "timeZone": "Europe/Madrid" }),
                    ["2026-10-31T04:00:00Z", "2026-10-31T09:15:00Z"],
                ),
            ] }] }],
        });
        let Next::Complete(answer) = directions_answer(
            json!({ "origin": "Lisboa", "destination": "Madrid", "mode": "transit" }),
            &body.to_string(),
            &settings(),
        ) else {
            panic!("expected an answer");
        };
        assert!(answer.get("timezone").is_none());
        let transit = |n: usize| answer["legs"][0]["steps"][n]["transit"].clone();
        assert_eq!(
            transit(0),
            json!({
                "from": "Lisboa Oriente", "departs": "23:40", "departs_at": "2026-10-30T23:40:00Z",
                "to": "Badajoz", "arrives": "04:05", "arrives_timezone": "Europe/Madrid",
                "arrives_at": "2026-10-31T03:05:00Z",
            })
        );
        assert_eq!(
            transit(1),
            json!({
                "from": "Badajoz", "departs": "05:00", "departs_timezone": "Europe/Madrid",
                "departs_at": "2026-10-31T04:00:00Z",
                "to": "Madrid Atocha", "arrives": "10:15", "arrives_timezone": "Europe/Madrid",
                "arrives_at": "2026-10-31T09:15:00Z",
            })
        );
    }

    fn fallback_answer(fallback: Value) -> Next {
        let body = json!({
            "routes": [{ "duration": "1800s", "staticDuration": "1800s" }],
            "fallbackInfo": fallback,
        });
        directions_answer(
            json!({ "origin": "a", "destination": "b" }),
            &body.to_string(),
            &settings(),
        )
    }

    #[test]
    fn directions_say_when_google_fell_back() {
        let Next::Complete(answer) = fallback_answer(
            json!({ "routingMode": "FALLBACK_TRAFFIC_UNAWARE", "reason": "LATENCY_EXCEEDED" }),
        ) else {
            panic!("expected an answer");
        };
        assert_eq!(
            answer["fallback"],
            json!({ "routing_mode": "FALLBACK_TRAFFIC_UNAWARE", "reason": "LATENCY_EXCEEDED" })
        );
        assert_eq!(answer["duration_minutes"], 30);
        assert_eq!(answer["duration_without_traffic_minutes"], 30);
    }

    #[test]
    fn directions_carry_a_fallback_whose_fields_they_cannot_read() {
        for fallback in [json!({}), json!({ "mode": "OTHER" })] {
            let Next::Complete(answer) = fallback_answer(fallback.clone()) else {
                panic!("expected an answer for {fallback}");
            };
            assert_eq!(answer["fallback"], json!({}), "{fallback}");
        }
    }

    #[test]
    fn directions_refuse_a_fallback_that_is_not_an_object() {
        for fallback in [json!("x"), json!([]), json!(1)] {
            assert_eq!(
                fallback_answer(fallback.clone()),
                Next::Fail(failure(
                    ErrorKind::Internal,
                    "unexpected Routes API response shape"
                )),
                "{fallback}"
            );
        }
    }

    #[test]
    fn directions_cap_steps_across_the_route() {
        let body = json!({
            "routes": [{
                "legs": [
                    { "steps": [drive_step(1), drive_step(2)] },
                    { "steps": [drive_step(3), drive_step(4), drive_step(5)] },
                ],
            }],
        });
        let Next::Complete(answer) = directions_answer(
            json!({ "origin": "a", "destination": "b", "max_steps": 3 }),
            &body.to_string(),
            &settings(),
        ) else {
            panic!("expected an answer");
        };
        let instructions = |leg: usize| -> Vec<Value> {
            answer["legs"][leg]["steps"]
                .as_array()
                .unwrap()
                .iter()
                .map(|step| step["instruction"].clone())
                .collect()
        };
        assert_eq!(
            instructions(0),
            [json!("Turn left 1"), json!("Turn left 2")]
        );
        assert_eq!(instructions(1), [json!("Turn left 3")]);
        assert_eq!(answer["steps_omitted"], 2);
    }

    #[test]
    fn directions_cap_steps_at_forty_by_default() {
        let steps: Vec<Value> = (1..=41).map(drive_step).collect();
        let body = json!({ "routes": [{ "legs": [{ "steps": steps }] }] });
        let Next::Complete(answer) = directions_answer(
            json!({ "origin": "a", "destination": "b" }),
            &body.to_string(),
            &settings(),
        ) else {
            panic!("expected an answer");
        };
        let kept = answer["legs"][0]["steps"].as_array().unwrap();
        assert_eq!(kept.len(), 40);
        assert_eq!(kept[39]["instruction"], "Turn left 40");
        assert_eq!(answer["steps_omitted"], 1);
    }

    #[test]
    fn directions_with_no_route_is_the_callers() {
        assert_eq!(
            directions_answer(
                json!({ "origin": "37.42,-122.08", "destination": "place_id:ChIJ1", "mode": "transit" }),
                "{}",
                &settings()
            ),
            Next::Fail(failure(
                ErrorKind::InvalidArgs,
                "no transit route found from 37.42,-122.08 to place_id:ChIJ1"
            ))
        );
        assert_eq!(
            directions_answer(
                json!({ "origin": "a", "destination": "b" }),
                r#"{ "routes": [] }"#,
                &settings()
            ),
            Next::Fail(failure(
                ErrorKind::InvalidArgs,
                "no drive route found from a to b"
            ))
        );
    }

    #[test]
    fn directions_refuse_an_unexpected_shape() {
        for body in [
            "[]",
            r#"{ "routes": {} }"#,
            r#"{ "routes": [1] }"#,
            r#"{ "routes": [{ "warnings": "tolls" }] }"#,
            r#"{ "routes": [{ "warnings": [1] }] }"#,
            r#"{ "routes": [{ "distanceMeters": "12" }] }"#,
            r#"{ "routes": [{ "duration": 12 }] }"#,
            r#"{ "routes": [{ "duration": "12" }] }"#,
            r#"{ "routes": [{ "duration": "-12s" }] }"#,
            r#"{ "routes": [{ "duration": "1e3s" }] }"#,
            r#"{ "routes": [{ "legs": {} }] }"#,
            r#"{ "routes": [{ "legs": [1] }] }"#,
            r#"{ "routes": [{ "legs": [{ "steps": {} }] }] }"#,
            r#"{ "routes": [{ "legs": [{ "steps": [1] }] }] }"#,
            r#"{ "routes": [{ "legs": [{ "steps": [{ "staticDuration": "x" }] }] }] }"#,
        ] {
            assert_eq!(
                directions_answer(
                    json!({ "origin": "a", "destination": "b" }),
                    body,
                    &settings()
                ),
                Next::Fail(failure(
                    ErrorKind::Internal,
                    "unexpected Routes API response shape"
                )),
                "{body}"
            );
        }
    }

    #[test]
    fn a_userless_directions_call_is_denied_naming_the_post() {
        let args = json!({ "origin": "a", "destination": "b" });
        let call = plan(Provided::Directions, &args, &settings()).unwrap();
        let state = InFlight::new(
            &"a".repeat(32),
            Provided::Directions,
            &args,
            &call,
            Args::new(),
            1_000,
        );
        assert_eq!(
            advance(
                &state,
                nested("denied", "a caller with no acting user may only read"),
                &settings()
            ),
            Next::Fail(failure(
                ErrorKind::Denied,
                "api-request refused the nested POST routes.example.com/directions/v2:computeRoutes: \
                 a caller with no acting user may only read"
            ))
        );
    }

    fn places_search(args: Value) -> Result<Call, String> {
        plan(Provided::PlacesSearch, &args, &settings())
    }

    fn places_state(args: Value, call: &Call, derived: Args) -> InFlight {
        InFlight::new(
            &"a".repeat(32),
            Provided::PlacesSearch,
            &args,
            call,
            derived,
            1_000,
        )
    }

    #[test]
    fn places_search_without_near_posts_one_search() {
        let call = places_search(json!({ "query": "coffee" })).unwrap();
        assert_eq!(call.step, PLACES_SEARCH_STEP);
        assert_eq!(call.api.provider, "maps");
        assert_eq!(call.api.method, "POST");
        assert_eq!(call.api.host, "places.example.com");
        assert_eq!(call.api.path, "/v1/places:searchText");
        assert!(call.api.query.is_empty());
        assert_eq!(
            call.api.headers,
            pairs(&[("X-Goog-FieldMask", PLACES_SEARCH_FIELD_MASK)])
        );
        assert_eq!(
            posted(&call.api),
            json!({ "textQuery": "coffee", "pageSize": 10 })
        );
    }

    #[test]
    fn places_search_field_mask_requests_what_the_answer_carries() {
        assert_eq!(
            PLACES_SEARCH_FIELD_MASK.split(',').collect::<Vec<_>>(),
            [
                "places.id",
                "places.displayName",
                "places.formattedAddress",
                "places.location",
                "places.primaryTypeDisplayName",
                "places.rating",
                "places.userRatingCount",
                "places.priceLevel",
                "places.currentOpeningHours.openNow",
                "places.businessStatus",
                "nextPageToken",
            ]
        );
    }

    #[test]
    fn places_search_near_coordinates_biases_without_geocoding() {
        let call = places_search(json!({
            "query": "coffee", "near": "37.422, -122.0841", "radius": 800,
            "open_now": true, "max": 20,
        }))
        .unwrap();
        assert_eq!(call.step, PLACES_SEARCH_STEP);
        assert_eq!(
            posted(&call.api),
            json!({
                "textQuery": "coffee",
                "pageSize": 20,
                "openNow": true,
                "locationBias": { "circle": {
                    "center": { "latitude": 37.422, "longitude": -122.0841 },
                    "radius": 800,
                } },
            })
        );
        // `open_now: false` is Google's default and is not sent.
        let call = places_search(json!({ "query": "coffee", "open_now": false })).unwrap();
        assert!(posted(&call.api).get("openNow").is_none());
    }

    #[test]
    fn places_search_passes_the_language() {
        let s = Settings::from_lookup(lookup(&with(&[("language", "fr")]))).unwrap();
        let call = plan(Provided::PlacesSearch, &json!({ "query": "café" }), &s).unwrap();
        assert_eq!(posted(&call.api)["languageCode"], "fr");
    }

    #[test]
    fn places_search_near_a_place_geocodes_first() {
        let call = places_search(json!({ "query": "coffee", "near": "Mountain View" })).unwrap();
        assert_eq!(call.step, PLACES_GEOCODE_STEP);
        assert_eq!(call.api.host, "geocode.example.com");
        assert_eq!(call.api.path, "/v4/geocode/address");
        assert_eq!(call.api.query, params(&[("addressQuery", "Mountain View")]));
        assert_eq!(
            call.api.headers,
            pairs(&[("X-Goog-FieldMask", GEOCODE_FIELD_MASK)])
        );
    }

    #[test]
    fn places_search_near_a_place_searches_where_the_geocode_landed() {
        let args = json!({ "query": "coffee", "near": "Mountain View" });
        let step1 = places_search(args.clone()).unwrap();
        let state = places_state(args.clone(), &step1, Args::new());
        let Next::Call { call, derived } = advance(&state, http(200, MOUNTAIN_VIEW), &settings())
        else {
            panic!("expected the search step");
        };
        assert_eq!(call.step, PLACES_SEARCH_STEP);
        assert_eq!(call.api.host, "places.example.com");
        assert_eq!(call.api.path, "/v1/places:searchText");
        // No radius given: the default circle.
        assert_eq!(
            posted(&call.api)["locationBias"],
            json!({ "circle": {
                "center": { "latitude": 37.39, "longitude": -122.08 },
                "radius": 5000,
            } })
        );
        let want = json!({ "lat": 37.39, "lng": -122.08, "place": "Mountain View, CA, USA" });
        assert_eq!(Value::Object(derived.clone()), want);

        // The search's row carries the location through the store to the answer.
        let step2 = InFlight::decode(&state.next(&call, derived, 2_000).encode()).unwrap();
        assert_eq!(step2.call_id(), format!("{}/2", "a".repeat(32)));
        assert_eq!(step2.target, call.api.target());
        let Next::Complete(answer) = advance(&step2, http(200, "{}"), &settings()) else {
            panic!("expected an answer");
        };
        assert_eq!(
            answer,
            json!({ "location": want, "places": [], "more": false })
        );
    }

    #[test]
    fn places_search_near_a_place_nothing_matches_is_the_callers() {
        let args = json!({ "query": "coffee", "near": "Nowhere Special" });
        let state = places_state(args.clone(), &places_search(args).unwrap(), Args::new());
        assert_eq!(
            advance(&state, http(200, "{}"), &settings()),
            Next::Fail(failure(
                ErrorKind::InvalidArgs,
                "no location found for Nowhere Special"
            ))
        );
    }

    #[test]
    fn places_search_steps_without_their_state_are_internal() {
        // A geocode step for a request with nothing to geocode.
        let args = json!({ "query": "coffee", "near": "1,2" });
        let call = Call {
            step: PLACES_GEOCODE_STEP,
            api: geocode_call(&Where::Text("Mountain View".to_string()), &settings()),
        };
        assert_eq!(
            advance(
                &places_state(args, &call, Args::new()),
                http(200, MOUNTAIN_VIEW),
                &settings()
            ),
            Next::Fail(failure(
                ErrorKind::Internal,
                "in-flight row names places-search's geocode step for a request with no place \
                 to geocode"
            ))
        );
        // A search step for a place whose geocoded location was not carried.
        let args = json!({ "query": "coffee", "near": "Mountain View" });
        let search = PlacesSearchArgs::parse(&args).unwrap();
        let call = Call {
            step: PLACES_SEARCH_STEP,
            api: places_search_call(&search, Some((1.0, 2.0)), &settings()),
        };
        assert_eq!(
            advance(
                &places_state(args, &call, Args::new()),
                http(200, "{}"),
                &settings()
            ),
            Next::Fail(failure(
                ErrorKind::Internal,
                "in-flight row for places-search's search carries no geocoded location"
            ))
        );
    }

    #[test]
    fn places_search_refuses_bad_arguments_by_name() {
        let cases = [
            (json!({}), "`query` is required"),
            (json!({ "query": 1 }), "`query` must be a string"),
            (json!({ "query": " " }), "`query` is empty"),
            (json!({ "query": "a", "near": "" }), "`near` is empty"),
            (
                json!({ "query": "a", "radius": 100 }),
                "`radius` applies with `near` only",
            ),
            (
                json!({ "query": "a", "near": "b", "radius": 0 }),
                "`radius` must be an integer from 1 to 50000",
            ),
            (
                json!({ "query": "a", "near": "b", "radius": 50_001 }),
                "`radius` must be an integer from 1 to 50000",
            ),
            (
                json!({ "query": "a", "near": "b", "radius": 1.5 }),
                "`radius` must be an integer from 1 to 50000",
            ),
            (
                json!({ "query": "a", "open_now": "yes" }),
                "`open_now` must be true or false",
            ),
            (
                json!({ "query": "a", "max": 0 }),
                "`max` must be an integer from 1 to 20",
            ),
            (
                json!({ "query": "a", "max": 21 }),
                "`max` must be an integer from 1 to 20",
            ),
        ];
        for (args, want) in cases {
            assert_eq!(places_search(args.clone()).unwrap_err(), want, "{args}");
        }
        let err = places_search(json!({ "query": "a", "lat": 1 })).unwrap_err();
        assert!(err.starts_with("unknown argument `lat`"), "{err}");
        let err = places_search(json!({ "query": "a", "near": "1,200" })).unwrap_err();
        assert!(
            err.contains("`near`") && err.contains("longitude 200"),
            "{err}"
        );
        // The radius bounds themselves are accepted.
        for radius in [1, 50_000] {
            let call = places_search(json!({ "query": "a", "near": "1,2", "radius": radius }));
            assert_eq!(
                posted(&call.unwrap().api)["locationBias"]["circle"]["radius"],
                radius
            );
        }
    }

    #[test]
    fn places_search_caps_query_and_near_in_characters() {
        let at_cap = "é".repeat(TEXT_MAX_CHARS);
        assert!(places_search(json!({ "query": at_cap, "near": at_cap })).is_ok());
        let over = format!("{at_cap}a");
        assert_eq!(
            places_search(json!({ "query": over })).unwrap_err(),
            "`query` is longer than 1024 characters"
        );
        assert_eq!(
            places_search(json!({ "query": "a", "near": over })).unwrap_err(),
            "`near` is longer than 1024 characters"
        );
    }

    fn places_answer(args: Value, body: &Value) -> Next {
        let call = plan(Provided::PlacesSearch, &args, &settings()).unwrap();
        advance(
            &places_state(args, &call, Args::new()),
            http(200, &body.to_string()),
            &settings(),
        )
    }

    fn place(n: usize) -> Value {
        json!({ "id": format!("ChIJ{n}"), "displayName": { "text": format!("Cafe {n}"), "languageCode": "en" } })
    }

    #[test]
    fn places_search_compacts_googles_answer() {
        let body = json!({
            "places": [
                {
                    "id": "ChIJ1",
                    "displayName": { "text": "Blue Cup", "languageCode": "en" },
                    "formattedAddress": "1 Main St, Springfield",
                    "location": { "latitude": 37.39, "longitude": -122.08 },
                    "primaryTypeDisplayName": { "text": "Coffee Shop", "languageCode": "en" },
                    "rating": 4.6,
                    "userRatingCount": 312,
                    "priceLevel": "PRICE_LEVEL_INEXPENSIVE",
                    "currentOpeningHours": { "openNow": true },
                    "businessStatus": "OPERATIONAL",
                },
                { "id": "ChIJ2", "displayName": { "text": "New Cafe" } },
            ],
            "nextPageToken": "tok",
        });
        let Next::Complete(answer) = places_answer(json!({ "query": "coffee" }), &body) else {
            panic!("expected an answer");
        };
        assert_eq!(
            answer,
            json!({
                "places": [
                    {
                        "id": "ChIJ1", "name": "Blue Cup", "address": "1 Main St, Springfield",
                        "lat": 37.39, "lng": -122.08, "type": "Coffee Shop",
                        "rating": 4.6, "rating_count": 312, "price_level": "PRICE_LEVEL_INEXPENSIVE",
                        "open_now": true, "status": "OPERATIONAL",
                    },
                    { "id": "ChIJ2", "name": "New Cafe" },
                ],
                "more": true,
            })
        );
    }

    #[test]
    fn places_search_near_coordinates_states_them() {
        let Next::Complete(answer) = places_answer(
            json!({ "query": "coffee", "near": "1,2" }),
            &json!({ "places": [place(1)], "nextPageToken": "" }),
        ) else {
            panic!("expected an answer");
        };
        assert_eq!(answer["location"], json!({ "lat": 1.0, "lng": 2.0 }));
        // An empty token names no further page.
        assert_eq!(answer["more"], false);
    }

    #[test]
    fn places_search_beyond_max_is_counted() {
        let body = json!({ "places": (1..=5).map(place).collect::<Vec<_>>() });
        let Next::Complete(answer) = places_answer(json!({ "query": "coffee", "max": 3 }), &body)
        else {
            panic!("expected an answer");
        };
        assert_eq!(answer["places"].as_array().unwrap().len(), 3);
        assert_eq!(answer["places"][2]["id"], "ChIJ3");
        assert_eq!(answer["places_omitted"], 2);
        assert_eq!(answer["more"], false);
        assert!(answer.get("location").is_none());
    }

    #[test]
    fn places_search_refuses_an_unexpected_shape() {
        for body in [
            json!([]),
            json!({ "places": {} }),
            json!({ "places": null }),
            json!({ "places": [1] }),
        ] {
            assert_eq!(
                places_answer(json!({ "query": "coffee" }), &body),
                Next::Fail(failure(
                    ErrorKind::Internal,
                    "unexpected Places API response shape"
                )),
                "{body}"
            );
        }
    }

    #[test]
    fn a_userless_places_search_is_denied_naming_the_post() {
        let args = json!({ "query": "coffee" });
        let call = places_search(args.clone()).unwrap();
        assert_eq!(
            advance(
                &places_state(args, &call, Args::new()),
                nested("denied", "a caller with no acting user may only read"),
                &settings()
            ),
            Next::Fail(failure(
                ErrorKind::Denied,
                "api-request refused the nested POST places.example.com/v1/places:searchText: \
                 a caller with no acting user may only read"
            ))
        );
    }

    fn place_details(args: Value) -> Result<ApiCall, String> {
        plan(Provided::PlaceDetails, &args, &settings()).map(|call| {
            assert_eq!(call.step, 1);
            call.api
        })
    }

    #[test]
    fn place_details_gets_one_place_without_reviews() {
        let call = place_details(json!({ "id": "ChIJj61dQgK6j4AR4GeTYWZsKWw" })).unwrap();
        assert_eq!(call.provider, "maps");
        assert_eq!(call.method, "GET");
        assert_eq!(call.host, "places.example.com");
        assert_eq!(call.path, "/v1/places/ChIJj61dQgK6j4AR4GeTYWZsKWw");
        assert!(call.query.is_empty());
        assert_eq!(call.body, None);
        assert_eq!(
            call.headers["X-Goog-FieldMask"]
                .split(',')
                .collect::<Vec<_>>(),
            [
                "id",
                "displayName",
                "formattedAddress",
                "location",
                "primaryTypeDisplayName",
                "rating",
                "userRatingCount",
                "priceLevel",
                "businessStatus",
                "currentOpeningHours.openNow",
                "regularOpeningHours.weekdayDescriptions",
                "nationalPhoneNumber",
                "internationalPhoneNumber",
                "websiteUri",
                "googleMapsUri",
            ]
        );
        let call = place_details(json!({ "id": "ChIJ1", "reviews": false })).unwrap();
        assert_eq!(call.headers["X-Goog-FieldMask"], PLACE_DETAILS_FIELD_MASK);
    }

    #[test]
    fn place_details_ask_for_reviews_only_on_request() {
        let call = place_details(json!({ "id": "ChIJ1", "reviews": true })).unwrap();
        assert_eq!(
            call.headers["X-Goog-FieldMask"],
            format!("{PLACE_DETAILS_FIELD_MASK},reviews,editorialSummary")
        );
    }

    #[test]
    fn place_details_pass_the_language() {
        let s = Settings::from_lookup(lookup(&with(&[("language", "fr")]))).unwrap();
        let call = plan(Provided::PlaceDetails, &json!({ "id": "ChIJ1" }), &s).unwrap();
        assert_eq!(call.api.query, params(&[("languageCode", "fr")]));
    }

    #[test]
    fn place_details_refuse_bad_arguments_by_name() {
        const BARE: &str = "`id` must be a bare Places place id (letters, digits, `_` and `-`), \
                            as PlacesSearch's `id` or Geocode's `place_id`";
        let cases = [
            (json!({}), "`id` is required"),
            (json!({ "id": 1 }), "`id` must be a string"),
            (json!({ "id": " " }), "`id` is empty"),
            (json!({ "id": "place_id:ChIJ1" }), BARE),
            (json!({ "id": "ChIJ1/reviews" }), BARE),
            (json!({ "id": "ChIJ%2F1" }), BARE),
            (json!({ "id": "ChIJ1?x=1" }), BARE),
            (
                json!({ "id": "ChIJ1", "reviews": "yes" }),
                "`reviews` must be true or false",
            ),
        ];
        for (args, want) in cases {
            assert_eq!(place_details(args.clone()).unwrap_err(), want, "{args}");
        }
        let err = place_details(json!({ "id": "ChIJ1", "fields": "*" })).unwrap_err();
        assert!(err.starts_with("unknown argument `fields`"), "{err}");
        let at_cap = "a".repeat(PLACE_ID_MAX_CHARS);
        assert!(place_details(json!({ "id": at_cap })).is_ok());
        assert_eq!(
            place_details(json!({ "id": format!("{at_cap}a") })).unwrap_err(),
            "`id` is longer than 1024 characters"
        );
    }

    fn details_answer(args: Value, body: &Value) -> Next {
        let call = plan(Provided::PlaceDetails, &args, &settings()).unwrap();
        let state = InFlight::new(
            &"a".repeat(32),
            Provided::PlaceDetails,
            &args,
            &call,
            Args::new(),
            1_000,
        );
        advance(&state, http(200, &body.to_string()), &settings())
    }

    fn review(n: usize, text: &str) -> Value {
        json!({
            "rating": 5,
            "relativePublishTimeDescription": format!("{n} weeks ago"),
            "text": { "text": text, "languageCode": "en" },
            "authorAttribution": { "displayName": format!("Reviewer {n}"), "uri": "https://example.com/r" },
        })
    }

    fn details_body() -> Value {
        json!({
            "id": "ChIJ1",
            "displayName": { "text": "Blue Cup", "languageCode": "en" },
            "formattedAddress": "1 Main St, Springfield",
            "location": { "latitude": 37.39, "longitude": -122.08 },
            "primaryTypeDisplayName": { "text": "Coffee Shop", "languageCode": "en" },
            "rating": 4.6,
            "userRatingCount": 312,
            "priceLevel": "PRICE_LEVEL_INEXPENSIVE",
            "businessStatus": "OPERATIONAL",
            "currentOpeningHours": { "openNow": true },
            "regularOpeningHours": { "openNow": true, "weekdayDescriptions": ["Monday: 7:00 AM – 6:00 PM"] },
            "nationalPhoneNumber": "(555) 010-0000",
            "internationalPhoneNumber": "+1 555-010-0000",
            "websiteUri": "https://example.com/",
            "googleMapsUri": "https://maps.google.com/?cid=1",
            "editorialSummary": { "text": "A small cafe.", "languageCode": "en" },
            "reviews": [review(1, "Good.")],
        })
    }

    #[test]
    fn place_details_compact_googles_answer() {
        let want = json!({
            "id": "ChIJ1", "name": "Blue Cup", "address": "1 Main St, Springfield",
            "lat": 37.39, "lng": -122.08, "type": "Coffee Shop",
            "rating": 4.6, "rating_count": 312, "price_level": "PRICE_LEVEL_INEXPENSIVE",
            "status": "OPERATIONAL", "open_now": true,
            "hours": ["Monday: 7:00 AM – 6:00 PM"],
            "phone": "(555) 010-0000", "phone_international": "+1 555-010-0000",
            "website": "https://example.com/", "maps_url": "https://maps.google.com/?cid=1",
        });
        // Not asked for: a summary or reviews Google sent anyway are not carried.
        assert_eq!(
            details_answer(json!({ "id": "ChIJ1" }), &details_body()),
            Next::Complete(want.clone())
        );
        let Next::Complete(answer) =
            details_answer(json!({ "id": "ChIJ1", "reviews": true }), &details_body())
        else {
            panic!("expected an answer");
        };
        let mut with_reviews = want;
        with_reviews["summary"] = json!("A small cafe.");
        with_reviews["reviews"] = json!([{
            "rating": 5, "when": "1 weeks ago", "author": "Reviewer 1", "text": "Good.",
        }]);
        assert_eq!(answer, with_reviews);
        // Only what Google gave is stated.
        assert_eq!(
            details_answer(
                json!({ "id": "ChIJ1", "reviews": true }),
                &json!({ "id": "ChIJ1" })
            ),
            Next::Complete(json!({ "id": "ChIJ1" }))
        );
    }

    #[test]
    fn place_details_cut_review_text_in_bytes_and_count_reviews_beyond_the_cap() {
        // Two-byte characters: the cut counts UTF-8 bytes.
        let long = "é".repeat(REVIEW_TEXT_MAX_BYTES / 2 + 1);
        let mut body = details_body();
        body["reviews"] = json!((1..=6).map(|n| review(n, &long)).collect::<Vec<_>>());
        let Next::Complete(answer) =
            details_answer(json!({ "id": "ChIJ1", "reviews": true }), &body)
        else {
            panic!("expected an answer");
        };
        let reviews = answer["reviews"].as_array().unwrap();
        assert_eq!(reviews.len(), REVIEWS_MAX);
        assert_eq!(answer["reviews_omitted"], 1);
        assert_eq!(
            reviews[0]["text"],
            format!("{}…", "é".repeat(REVIEW_TEXT_MAX_BYTES / 2))
        );
        // A character straddling the bound is dropped whole.
        let straddling = format!("a{long}");
        body["reviews"] = json!([review(1, &straddling)]);
        let Next::Complete(answer) =
            details_answer(json!({ "id": "ChIJ1", "reviews": true }), &body)
        else {
            panic!("expected an answer");
        };
        assert_eq!(
            answer["reviews"][0]["text"],
            format!("a{}…", "é".repeat(REVIEW_TEXT_MAX_BYTES / 2 - 1))
        );
        let exact = "é".repeat(REVIEW_TEXT_MAX_BYTES / 2);
        body["reviews"] = json!([review(1, &exact)]);
        let Next::Complete(answer) =
            details_answer(json!({ "id": "ChIJ1", "reviews": true }), &body)
        else {
            panic!("expected an answer");
        };
        assert_eq!(answer["reviews"][0]["text"], exact);
    }

    #[test]
    fn place_details_refuse_an_unexpected_shape() {
        for body in [
            json!([]),
            json!({}),
            json!({ "id": "ChIJ1", "regularOpeningHours": { "weekdayDescriptions": "Mon" } }),
            json!({ "id": "ChIJ1", "regularOpeningHours": { "weekdayDescriptions": [1] } }),
            json!({ "id": "ChIJ1", "reviews": {} }),
            json!({ "id": "ChIJ1", "reviews": [1] }),
        ] {
            assert_eq!(
                details_answer(json!({ "id": "ChIJ1", "reviews": true }), &body),
                Next::Fail(failure(
                    ErrorKind::Internal,
                    "unexpected Places API response shape"
                )),
                "{body}"
            );
        }
    }

    fn along_route(args: Value) -> Result<Call, String> {
        plan(Provided::SearchAlongRoute, &args, &settings())
    }

    fn along_state(args: &Value, call: &Call, derived: Args) -> InFlight {
        InFlight::new(
            &"a".repeat(32),
            Provided::SearchAlongRoute,
            args,
            call,
            derived,
            1_000,
        )
    }

    #[test]
    fn search_along_route_computes_an_overview_route_first() {
        let call = along_route(json!({
            "origin": "37.422,-122.0841", "destination": "place_id:ChIJ2", "query": "gas",
        }))
        .unwrap();
        assert_eq!(call.step, ALONG_ROUTE_ROUTE_STEP);
        assert_eq!(call.api.method, "POST");
        assert_eq!(call.api.host, "routes.example.com");
        assert_eq!(call.api.path, "/directions/v2:computeRoutes");
        assert!(call.api.query.is_empty());
        assert_eq!(
            call.api.headers,
            pairs(&[(
                "X-Goog-FieldMask",
                "routes.polyline.encodedPolyline,routes.distanceMeters,routes.duration,fallbackInfo"
            )])
        );
        assert_eq!(
            posted(&call.api),
            json!({
                "origin": { "location": { "latLng": { "latitude": 37.422, "longitude": -122.0841 } } },
                "destination": { "placeId": "ChIJ2" },
                "travelMode": "DRIVE",
                "routingPreference": "TRAFFIC_AWARE",
                "computeAlternativeRoutes": false,
                "polylineQuality": "OVERVIEW",
                "units": "METRIC",
            })
        );
        let walk = along_route(
            json!({ "origin": "a", "destination": "b", "query": "gas", "mode": "walk" }),
        )
        .unwrap();
        let body = posted(&walk.api);
        assert_eq!(body["travelMode"], "WALK");
        assert!(body.get("routingPreference").is_none());
    }

    #[test]
    fn search_along_route_refuses_bad_arguments_by_name() {
        let cases = [
            (
                json!({ "destination": "b", "query": "q" }),
                "`origin` is required",
            ),
            (
                json!({ "origin": "a", "query": "q" }),
                "`destination` is required",
            ),
            (
                json!({ "origin": "a", "destination": "b" }),
                "`query` is required",
            ),
            (
                json!({ "origin": "a", "destination": "b", "query": " " }),
                "`query` is empty",
            ),
            (
                json!({ "origin": "a", "destination": "b", "query": "q", "mode": "fly" }),
                MODE_CHOICES,
            ),
            (
                json!({ "origin": "a", "destination": "b", "query": "q", "max": 0 }),
                "`max` must be an integer from 1 to 20",
            ),
            (
                json!({ "origin": "a", "destination": "b", "query": "q", "max": 21 }),
                "`max` must be an integer from 1 to 20",
            ),
        ];
        for (args, want) in cases {
            assert_eq!(along_route(args.clone()).unwrap_err(), want, "{args}");
        }
        // Directions' time argument and PlacesSearch's own arguments are not
        // SearchAlongRoute's.
        for (name, value) in [
            ("depart_at", json!("2026-10-30T17:00:00Z")),
            ("open_now", json!(true)),
            ("near", json!("c")),
            ("radius", json!(1000)),
        ] {
            let mut args = json!({ "origin": "a", "destination": "b", "query": "q" });
            args[name] = value;
            let err = along_route(args).unwrap_err();
            assert!(
                err.starts_with(&format!("unknown argument `{name}`")),
                "{err}"
            );
        }
        let over = "a".repeat(TEXT_MAX_CHARS + 1);
        assert_eq!(
            along_route(json!({ "origin": "a", "destination": "b", "query": over })).unwrap_err(),
            "`query` is longer than 1024 characters"
        );
    }

    /// A route answer for SearchAlongRoute's first step.
    fn polyline_route(polyline: &str, extra: Value) -> String {
        let mut body = json!({
            "routes": [{
                "polyline": { "encodedPolyline": polyline },
                "distanceMeters": 80467,
                "duration": "3000s",
            }],
        });
        if let Value::Object(extra) = extra {
            body.as_object_mut().unwrap().extend(extra);
        }
        body.to_string()
    }

    #[test]
    fn search_along_route_searches_along_the_polyline_and_keeps_the_route() {
        let args = json!({ "origin": "a", "destination": "b", "query": "gas", "max": 3 });
        let step1 = along_route(args.clone()).unwrap();
        let state = along_state(&args, &step1, Args::new());
        let body = polyline_route(
            "_p~iF~ps|U_ulLnnqC",
            json!({ "fallbackInfo": { "routingMode": "FALLBACK_TRAFFIC_UNAWARE", "reason": "SERVER_ERROR" } }),
        );
        let Next::Call { call, derived } = advance(&state, http(200, &body), &settings()) else {
            panic!("expected the search step");
        };
        assert_eq!(call.step, ALONG_ROUTE_SEARCH_STEP);
        assert_eq!(call.api.method, "POST");
        assert_eq!(call.api.host, "places.example.com");
        assert_eq!(call.api.path, "/v1/places:searchText");
        assert_eq!(
            call.api.headers,
            pairs(&[("X-Goog-FieldMask", PLACES_SEARCH_FIELD_MASK)])
        );
        assert_eq!(
            posted(&call.api),
            json!({
                "textQuery": "gas",
                "pageSize": 3,
                "searchAlongRouteParameters": { "polyline": { "encodedPolyline": "_p~iF~ps|U_ulLnnqC" } },
            })
        );
        let route = json!({
            "distance": { "value": 80.47, "unit": "km" },
            "duration_minutes": 50,
            "fallback": { "routing_mode": "FALLBACK_TRAFFIC_UNAWARE", "reason": "SERVER_ERROR" },
        });
        // The route's summary is what the search's row carries; the polyline is not.
        assert_eq!(Value::Object(derived.clone()), json!({ "route": route }));

        // Step 1's body reaches the answer only through the stored row.
        let step2 = InFlight::decode(&state.next(&call, derived, 2_000).encode()).unwrap();
        assert_eq!(step2.call_id(), format!("{}/2", "a".repeat(32)));
        let places = json!({ "places": [place(1)], "nextPageToken": "more" });
        assert_eq!(
            advance(&step2, http(200, &places.to_string()), &settings()),
            Next::Complete(json!({
                "route": route,
                "places": [{ "id": "ChIJ1", "name": "Cafe 1" }],
                "more": true,
            }))
        );
    }

    #[test]
    fn search_along_route_without_a_fallback_states_none() {
        let args = json!({ "origin": "a", "destination": "b", "query": "gas" });
        let state = along_state(&args, &along_route(args.clone()).unwrap(), Args::new());
        let Next::Call { derived, .. } = advance(
            &state,
            http(200, &polyline_route("abc", json!({}))),
            &settings(),
        ) else {
            panic!("expected the search step");
        };
        assert_eq!(
            derived["route"],
            json!({ "distance": { "value": 80.47, "unit": "km" }, "duration_minutes": 50 })
        );
    }

    #[test]
    fn search_along_route_refuses_a_route_too_long_to_search_along() {
        let args = json!({ "origin": "a", "destination": "b", "query": "gas" });
        let state = along_state(&args, &along_route(args.clone()).unwrap(), Args::new());
        // An encoded polyline cycles through its 64-symbol alphabet, ASCII 63
        // to 126, so one byte in 64 is `\`, which is escaped once in the body
        // and again in the args.
        let at_cap: String = (63u8..=126)
            .cycle()
            .take(POLYLINE_MAX_BYTES)
            .map(char::from)
            .collect();
        let Next::Call { call, .. } = advance(
            &state,
            http(200, &polyline_route(&at_cap, json!({}))),
            &settings(),
        ) else {
            panic!("a polyline at the cap is searched along");
        };
        // With that polyline and a query whose every byte escapes to the
        // longest form, the search args stay under the nested call's 64 KiB
        // args cap.
        let mut longest = AlongRouteArgs::parse(&args).unwrap();
        longest.search.query = "\u{1}".repeat(TEXT_MAX_CHARS);
        let api = along_route_search_call(&longest, &at_cap, &settings());
        assert!(serde_json::to_vec(&api).unwrap().len() < 64 * 1024);
        assert!(
            posted(&call.api)["searchAlongRouteParameters"]["polyline"]["encodedPolyline"]
                == at_cap
        );
        let over = format!("{at_cap}a");
        assert_eq!(
            advance(
                &state,
                http(200, &polyline_route(&over, json!({}))),
                &settings()
            ),
            Next::Fail(failure(
                ErrorKind::InvalidArgs,
                &format!(
                    "route too long to search along ({}-byte polyline)",
                    POLYLINE_MAX_BYTES + 1
                )
            ))
        );
    }

    #[test]
    fn search_along_route_refuses_a_route_whose_answer_was_cut() {
        let args = json!({ "origin": "a", "destination": "b", "query": "gas" });
        let state = along_state(&args, &along_route(args.clone()).unwrap(), Args::new());
        let cut = |bytes: Value| {
            truncated(
                200,
                bytes,
                "{\"routes\":[{\"polyline\":{\"encodedPolyline\":\"abc",
            )
        };
        assert_eq!(
            advance(&state, cut(json!(300000)), &settings()),
            Next::Fail(failure(
                ErrorKind::InvalidArgs,
                "route too long to search along: Google's answer (300000 bytes) exceeds \
                 api-request's read limit for this provider (max_response_bytes)"
            ))
        );
        assert_eq!(
            advance(&state, cut(Value::Null), &settings()),
            Next::Fail(failure(
                ErrorKind::InvalidArgs,
                "route too long to search along: Google's answer (of unstated length) exceeds \
                 api-request's read limit for this provider (max_response_bytes)"
            ))
        );
    }

    #[test]
    fn search_along_route_with_no_route_is_the_callers() {
        let args = json!({ "origin": "a", "destination": "b", "query": "gas", "mode": "bicycle" });
        let state = along_state(&args, &along_route(args.clone()).unwrap(), Args::new());
        assert_eq!(
            advance(&state, http(200, "{}"), &settings()),
            Next::Fail(failure(
                ErrorKind::InvalidArgs,
                "no bicycle route found from a to b"
            ))
        );
    }

    #[test]
    fn search_along_route_refuses_an_unexpected_route_shape() {
        let args = json!({ "origin": "a", "destination": "b", "query": "gas" });
        let state = along_state(&args, &along_route(args.clone()).unwrap(), Args::new());
        for body in [
            "[]".to_string(),
            r#"{ "routes": [{ "distanceMeters": 1 }] }"#.to_string(),
            r#"{ "routes": [{ "polyline": { "encodedPolyline": "" } }] }"#.to_string(),
            r#"{ "routes": [{ "polyline": { "encodedPolyline": 1 } }] }"#.to_string(),
            polyline_route("abc", json!({ "fallbackInfo": "x" })),
            polyline_route(
                "abc",
                json!({ "routes": [{ "polyline": { "encodedPolyline": "abc" }, "duration": 5 }] }),
            ),
        ] {
            assert_eq!(
                advance(&state, http(200, &body), &settings()),
                Next::Fail(failure(
                    ErrorKind::Internal,
                    "unexpected Routes API response shape"
                )),
                "{body}"
            );
        }
    }

    #[test]
    fn search_along_route_search_without_its_route_is_internal() {
        let args = json!({ "origin": "a", "destination": "b", "query": "gas" });
        let call = Call {
            step: ALONG_ROUTE_SEARCH_STEP,
            api: along_route_search_call(
                &AlongRouteArgs::parse(&args).unwrap(),
                "abc",
                &settings(),
            ),
        };
        assert_eq!(
            advance(
                &along_state(&args, &call, Args::new()),
                http(200, "{}"),
                &settings()
            ),
            Next::Fail(failure(
                ErrorKind::Internal,
                "in-flight row for search-along-route's search carries no route"
            ))
        );
    }

    #[test]
    fn a_userless_search_along_route_is_denied_naming_the_post() {
        let args = json!({ "origin": "a", "destination": "b", "query": "gas" });
        let call = along_route(args.clone()).unwrap();
        assert_eq!(
            advance(
                &along_state(&args, &call, Args::new()),
                nested("denied", "a caller with no acting user may only read"),
                &settings()
            ),
            Next::Fail(failure(
                ErrorKind::Denied,
                "api-request refused the nested POST routes.example.com/directions/v2:computeRoutes: \
                 a caller with no acting user may only read"
            ))
        );
    }

    /// Every argument `tool` takes, with the bound its parser holds it to,
    /// and the arguments it requires: the parser's own constants and key
    /// lists, as `every_tools_schema_states_what_its_parser_enforces` holds
    /// the spec to them. Some rules the parser holds are in no bound here,
    /// and the schema leaves them to the descriptions: every rule across
    /// arguments, and two on one text value. A place text of the form
    /// `<lat>,<lng>` is refused when out of range, and a waypoint of
    /// `place_id:` with no id is refused; the schema admits both.
    fn bounds(tool: Provided) -> ToolBounds<'static> {
        let (keys, required): (&'static [&'static str], &'static [&'static str]) = match tool {
            Provided::Weather => (WeatherArgs::KEYS, &["kind"]),
            Provided::Geocode => (GeocodeArgs::KEYS, &[]),
            Provided::Directions => (DirectionsArgs::KEYS, &["origin", "destination"]),
            Provided::PlacesSearch => (PlacesSearchArgs::KEYS, &["query"]),
            Provided::PlaceDetails => (PlaceDetailsArgs::KEYS, &["id"]),
            Provided::SearchAlongRoute => {
                (AlongRouteArgs::KEYS, &["origin", "destination", "query"])
            }
        };
        let text = Bound::Text {
            max_chars: TEXT_MAX_CHARS,
            format: None,
        };
        let integer = |max| Bound::Integer { min: 1, max };
        let property = |key: &'static str| {
            let bound = match (tool, key) {
                (Provided::Weather, "kind") => {
                    Bound::Enum(WeatherKind::ALL.map(WeatherKind::name).to_vec())
                }
                (Provided::Weather, "page") => Bound::Text {
                    max_chars: PAGE_TOKEN_MAX_CHARS,
                    format: None,
                },
                (Provided::Weather, "hours") => integer(HOURS_MAX),
                (Provided::Weather, "days") => integer(DAYS_MAX),
                (Provided::Weather | Provided::Geocode, "lat") => Bound::Number {
                    min: -LAT_MAX,
                    max: LAT_MAX,
                },
                (Provided::Weather | Provided::Geocode, "lng") => Bound::Number {
                    min: -LNG_MAX,
                    max: LNG_MAX,
                },
                (_, "place" | "address" | "origin" | "destination" | "query" | "near") => {
                    text.clone()
                }
                (Provided::Geocode, "max") => integer(GEOCODE_MAX),
                (Provided::Directions | Provided::SearchAlongRoute, "mode") => {
                    Bound::Enum(TravelMode::ALL.map(TravelMode::name).to_vec())
                }
                (Provided::Directions, "depart_at" | "arrive_by") => Bound::Text {
                    max_chars: TEXT_MAX_CHARS,
                    format: Some("date-time"),
                },
                (Provided::Directions, "max_steps") => integer(DIRECTIONS_MAX_STEPS),
                (Provided::PlacesSearch | Provided::SearchAlongRoute, "max") => integer(PLACES_MAX),
                (Provided::PlacesSearch, "radius") => integer(RADIUS_MAX_METERS),
                (Provided::PlacesSearch, "open_now") | (Provided::PlaceDetails, "reviews") => {
                    Bound::Bool
                }
                (Provided::PlaceDetails, "id") => Bound::Token {
                    max_chars: PLACE_ID_MAX_CHARS,
                },
                _ => panic!("{tool:?} takes `{key}`, which no bound here states"),
            };
            (key, bound)
        };
        ToolBounds {
            properties: keys.iter().copied().map(property).collect(),
            required,
        }
    }

    /// Arguments `tool` admits into which a probe of `key` is set: the
    /// required ones, a location by coordinates where the tool needs one,
    /// and an optional argument only where a probe of `key` is refused
    /// without it. The location is `key` itself when `key` is the location's
    /// text, which the coordinates would contradict. Every other optional
    /// argument is absent, so the parser admitting these arguments shows it
    /// does not require it.
    fn admitted_args(tool: Provided, key: &str) -> Value {
        let mut args = match tool {
            Provided::Weather => {
                json!({ "kind": if key == "days" { "daily" } else { "hourly" }, "lat": 0, "lng": 0 })
            }
            Provided::Geocode => json!({ "lat": 0, "lng": 0 }),
            Provided::Directions => json!({ "origin": "a", "destination": "b" }),
            Provided::PlacesSearch => json!({ "query": "coffee" }),
            Provided::PlaceDetails => json!({ "id": "abc" }),
            Provided::SearchAlongRoute => {
                json!({ "origin": "a", "destination": "b", "query": "coffee" })
            }
        };
        let map = args.as_object_mut().unwrap();
        match (tool, key) {
            (_, "place" | "address") => {
                map.remove("lat");
                map.remove("lng");
                map.insert(key.to_string(), json!("Springfield"));
            }
            (Provided::PlacesSearch, "radius") => {
                map.insert("near".to_string(), json!("Springfield"));
            }
            (Provided::Directions, "arrive_by") => {
                map.insert("mode".to_string(), json!("transit"));
            }
            _ => {}
        }
        args
    }

    /// Each tool's `input_schema` states exactly the bounds its parser
    /// enforces, and the parser admits and refuses every bound's probes,
    /// naming the argument it refuses: the schema and the parser are held to
    /// the same table from both sides.
    #[test]
    fn every_tools_schema_states_what_its_parser_enforces() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../config/specs/google-maps-tools.brenn");
        let spec = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("{} must be readable: {error}", path.display()));
        for tool in Provided::ALL {
            let name = tool.name();
            let bounds = bounds(tool);
            if let Err(error) = check(&input_schema(&spec, name), &bounds) {
                panic!("`{name}`'s input_schema: {error}");
            }
            let plan_with = |args: &Value| plan(tool, args, &settings()).map(drop);
            for key in bounds.required {
                hold_required(name, key, &admitted_args(tool, key), plan_with);
            }
            for (key, bound) in &bounds.properties {
                let probes = probes(bound)
                    .unwrap_or_else(|| panic!("`{name}`'s `{key}` has no probes: {bound:?}"));
                hold_probes(name, key, &probes, &admitted_args(tool, key), plan_with);
            }
        }
    }
}
