# Google Maps Platform endpoints through `api-request`

The `google-maps-tools` component answers weather, geocoding, place search,
place details, directions and search along a route. This document lists Maps
Platform endpoints the component does not wrap, each with one `api-request`
call that reaches it through the `google-maps` key provider. The operator's
setup is in `docs/google-maps-platform.md`.

Every example below is the argument object of one `api-request` call. Brenn
attaches the key in the `X-Goog-Api-Key` header; never send that header, a
`key` query parameter or `account`. Query values are strings. A JSON body is
passed as a string in `body`, and Brenn sets `Content-Type:
application/json` for it.

A host an example names must be in the provider's `api_hosts`, and the
method, host and path must be admitted by a clause of the caller's
`api-request` grant. A call that is not is refused before anything is sent;
the operator adds the host or the clause.

Google's answer is returned as it is: the status, the headers and the body,
unread by Brenn. A 4xx answer's body carries Google's reason.

Shapes were read from Google's reference pages on 2026-10-30; each section
names its page. Google may change them.

## Air quality now

Current air quality at a location, as air quality indexes and, when asked
for, pollutant concentrations.
Reference: `https://developers.google.com/maps/documentation/air-quality/reference/rest/v1/currentConditions/lookup`.

```json
{
  "provider": "google-maps",
  "method": "POST",
  "host": "airquality.googleapis.com",
  "path": "/v1/currentConditions:lookup",
  "body": "{\"location\": {\"latitude\": 37.4220, \"longitude\": -122.0841}, \"extraComputations\": [\"LOCAL_AQI\", \"POLLUTANT_CONCENTRATION\"], \"languageCode\": \"en\"}"
}
```

`location` is required. The answer is `{ dateTime, regionCode, indexes[],
pollutants[], healthRecommendations }`. `indexes` holds the Universal AQI
(returned unless `universalAqi` is `false`) and the local AQI (only with the
`LOCAL_AQI` extra computation); `pollutants` is returned only for requests
naming `POLLUTANT_ADDITIONAL_INFO`, `DOMINANT_POLLUTANT_CONCENTRATION` or
`POLLUTANT_CONCENTRATION`.

## Weather in the past 24 hours

Hourly observed weather, up to 24 hours back from the last hour.
Reference: `https://developers.google.com/maps/documentation/weather/reference/rest/v1/history.hours/lookup`.

```json
{
  "provider": "google-maps",
  "method": "GET",
  "host": "weather.googleapis.com",
  "path": "/v1/history/hours:lookup",
  "query": {
    "location.latitude": "37.4220",
    "location.longitude": "-122.0841",
    "hours": "12",
    "unitsSystem": "METRIC"
  }
}
```

`hours` is 1–24 (default 24) and `pageSize` 1–24 (default 24). The answer is
`{ historyHours[], timeZone, nextPageToken? }`; each hour carries
`displayDateTime`, `weatherCondition.description.text`, `temperature`,
`feelsLikeTemperature`, `precipitation`, `wind`, `relativeHumidity` and more.
When `nextPageToken` is present, repeat the call with it as `pageToken` and
every other parameter unchanged. `unitsSystem` defaults to `METRIC`.

## Places near a point

Places of given types within a circle, ranked by popularity or distance.
Reference: `https://developers.google.com/maps/documentation/places/web-service/nearby-search`.

```json
{
  "provider": "google-maps",
  "method": "POST",
  "host": "places.googleapis.com",
  "path": "/v1/places:searchNearby",
  "headers": {
    "X-Goog-FieldMask": "places.id,places.displayName,places.formattedAddress,places.location"
  },
  "body": "{\"includedTypes\": [\"restaurant\"], \"maxResultCount\": 10, \"locationRestriction\": {\"circle\": {\"center\": {\"latitude\": 37.4220, \"longitude\": -122.0841}, \"radius\": 1000.0}}}"
}
```

The field mask is required; without it Google answers an error.
`locationRestriction.circle.radius` is 0–50 000 meters and must be set above
0. `includedTypes` (Google's place types) and `maxResultCount` (1–20, default
20) are optional; `rankPreference` is `POPULARITY` (default) or `DISTANCE`.
The answer is `{ places[] }` with the masked fields. A place's `id` is what
the `PlaceDetails` tool takes.

## Place autocomplete

Up to five predicted places or queries for partial text, as a search box
would offer.
Reference: `https://developers.google.com/maps/documentation/places/web-service/place-autocomplete`.

```json
{
  "provider": "google-maps",
  "method": "POST",
  "host": "places.googleapis.com",
  "path": "/v1/places:autocomplete",
  "body": "{\"input\": \"pizza\", \"locationBias\": {\"circle\": {\"center\": {\"latitude\": 37.4220, \"longitude\": -122.0841}, \"radius\": 500.0}}}"
}
```

`input` is required; the field mask is optional here. `locationBias` favours
an area without excluding results outside it, and cannot be combined with
`locationRestriction`. The answer is `{ suggestions[] }`, each a
`placePrediction` (with `placeId` and `text.text`) or a `queryPrediction`.

## Travel times between many places

Distance and duration from each origin to each destination, without the
routes themselves.
Reference: `https://developers.google.com/maps/documentation/routes/reference/rest/v2/TopLevel/computeRouteMatrix`.

```json
{
  "provider": "google-maps",
  "method": "POST",
  "host": "routes.googleapis.com",
  "path": "/distanceMatrix/v2:computeRouteMatrix",
  "headers": {
    "X-Goog-FieldMask": "originIndex,destinationIndex,status,condition,distanceMeters,duration"
  },
  "body": "{\"origins\": [{\"waypoint\": {\"address\": \"Springfield, IL\"}}, {\"waypoint\": {\"location\": {\"latLng\": {\"latitude\": 37.4220, \"longitude\": -122.0841}}}}], \"destinations\": [{\"waypoint\": {\"placeId\": \"<a place id>\"}}], \"travelMode\": \"DRIVE\"}"
}
```

The field mask is required, and must include `status`: without it every
element appears to be OK. Each origin is `{ "waypoint": {…} }`, a waypoint
being `{ "address": … }`, `{ "placeId": … }` or `{ "location": { "latLng":
{…} } }`. Origins plus destinations given as a place id or address are at
most 50, and origins × destinations at most 625 (100 for `TRANSIT`). The
answer is a list of elements, one per origin–destination pair, each with
`originIndex`, `destinationIndex`, `status`, `condition` (`ROUTE_EXISTS` or
`ROUTE_NOT_FOUND`), `distanceMeters` and `duration` (seconds, as `"1234s"`).

## Time zone at a place

The IANA zone and its offsets at a location and moment.
Reference: `https://developers.google.com/maps/documentation/timezone/requests-timezone`.

```json
{
  "provider": "google-maps",
  "method": "GET",
  "host": "maps.googleapis.com",
  "path": "/maps/api/timezone/json",
  "query": {
    "location": "37.4220,-122.0841",
    "timestamp": "1733428634"
  }
}
```

`location` is `<lat>,<lng>` and `timestamp` seconds since the Unix epoch,
both required; `language` is optional. The answer is `{ status, timeZoneId,
timeZoneName, rawOffset, dstOffset }` with offsets in seconds; read `status`
first, since anything but `OK` comes with an `errorMessage` instead.

## Elevation

Ground elevation in meters at one or more points.
Reference: `https://developers.google.com/maps/documentation/elevation/requests-elevation`.

```json
{
  "provider": "google-maps",
  "method": "GET",
  "host": "maps.googleapis.com",
  "path": "/maps/api/elevation/json",
  "query": {
    "locations": "37.4220,-122.0841"
  }
}
```

`locations` (required) takes `<lat>,<lng>` points, at most 512; a sampled
path request takes `path` instead. The answer is `{ status, results[] }`,
each result with `elevation`, `location` and `resolution`; read `status`
first, as for Time Zone.

## How the key reaches each API

Brenn sends the key only as the `X-Goog-Api-Key` header; it adds no `key`
query parameter. Google's API platform documents that header as the key's
header form for its REST APIs, and the Places pages show it in their
examples. The other pages differ:

- **Air Quality, the route matrix and Weather history** do not say how the
  key is passed, or show only `key=`.
- **Time Zone and Elevation** are on `maps.googleapis.com/maps/api/…`, an
  older family. Their pages show the key only as a `key=` query parameter,
  and the Elevation page calls it required.

Whether each of these accepts the header has not been checked against a
live key. One test call settles it for a deployment: an answer whose
`status` or error names a missing or invalid key means that API does not
take the header, and it cannot be used through Brenn.
