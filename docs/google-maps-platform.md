# Google Maps Platform with an API key

This guide sets Brenn up so an assistant can use Google Maps Platform —
weather, geocoding, places and directions — with one API key that belongs to
the deployment. The operator places the key in a file on the host. Brenn
reads it at boot and attaches it to each request as a header, itself; it
never passes the key into an agent's container or into a component.

Nobody connects an account: the key is the deployment's, every caller the
grant admits sends the same key, and what it spends is the operator's.

The assistant reaches Google in one of two ways:

- **The `google-maps-tools` component** (section 4) gives it six native
  tools — `Weather`, `Geocode`, `PlacesSearch`, `PlaceDetails`, `Directions`
  and `SearchAlongRoute` — that build Google's requests and answer with
  compact JSON. This is the route to use.
- **The raw `api-request` route** (section 7) lets it call any Maps Platform
  endpoint directly, assembling the request and reading Google's answer
  itself. It is the fallback for endpoints the component does not wrap.

The configuration keys are documented in `docs/config-dsl.md`, under *API-key
providers*, *Messaging defaults* and *Provided tools*. This guide covers the
Google side, the key file, the configuration, the component, what to tell the
assistant, an acceptance check, the raw route, and rotation. Connecting users'
own Google accounts (Gmail, Calendar) is a separate setup, in
`docs/google-workspace.md`; the two can be declared together or alone.

## 1. A key

There are two ways to get a key. The configuration and everything after this
section are the same for both: Brenn does not tell them apart.

### For testing: the Maps Demo Key

Checked on 2026-10-29 against Google's page
`https://developers.google.com/maps/demo-key` (last updated 2026-10-22). What
that page said then:

- The Maps Demo Key is technically an API key, used in place of a standard
  API key.
- It costs nothing and needs no billing information.
- It is intended for testing and prototyping only.
- It needs a personal Google Account. Workspace accounts are not eligible.
- It has a daily usage limit. Past the limit, usage is paused until the
  following day, with no risk of charges.
- It supports Maps Grounding Lite, Places API (New) without user-generated
  content, Geocoding API v4, Compute Routes and the Weather API. That covers
  every call the component makes, with one exception below.
- It is obtained with the page's **Get a Demo Key** button, and can be
  retrieved later from the Cloud console.

"Without user-generated content" means user-submitted photos and reviews.
`PlaceDetails` asks for reviews only when called with `reviews: true`; on the
Demo Key that call is Google's error, returned as the tool's error. Every
other call works.

Google may change any of this; read the page again before relying on it.

### For real use: a standard key with billing

1. In the Google Cloud console, create a project (for example `brenn-maps`)
   and attach a billing account to it.
2. **APIs & Services → Library**: enable the four APIs the component calls —
   the **Weather API**, the **Geocoding API**, **Places API (New)** and the
   **Routes API** — plus any API the raw route (section 7) will call, such as
   the **Air Quality API**, the **Time Zone API** or the **Elevation API**.
   Enable only what a grant will use.
3. **APIs & Services → Credentials → Create credentials → API key**.
4. Restrict the key:
   - **API restrictions**: only the APIs enabled in step 2.
   - **Application restrictions**: **IP addresses**, listing the public
     address the Brenn server's requests leave from.
5. **Billing → Budgets & alerts**: create a budget for the project with an
   email alert. The key's spend is the operator's, whoever's conversation
   made the calls; a budget alert is how a runaway loop is noticed.

## 2. The key file

Put the key in a file on the host, readable by the server's user only:

```
install -m 0600 -o <server user> /dev/null /etc/brenn/google/maps-api-key
$EDITOR /etc/brenn/google/maps-api-key
```

The file holds the key and nothing else; surrounding whitespace is trimmed.
Brenn refuses to start if the file is missing, empty, readable or writable by
group or others, or holds anything but visible ASCII and spaces (a pasted
control character or byte-order mark, for example).

Brenn checks the file's mode and nothing else about where it is. Placing it
outside every path an agent can see — working directories, repo mounts,
state directories, container homes, `extra_mounts` host paths and the
repo-sync directory — is the operator's job. A key file inside one of them is
a key the agent can read, and no check catches it.

## 3. Configuration

Two blocks: the bus's body cap, and the key provider.

```
// Every nested api-request answer crosses the bus as a tool result, so the
// body cap must carry the key provider's read limit (max_response_bytes,
// 256 KiB default) plus the growth of a pretty-printed body as a JSON
// string, its headers and the result envelope: 256 KiB × 1.5. The 64 KiB
// default drops a long transit route (~100 KB) and crowds a 24-hour
// forecast (~50 KB). This cap is bus-wide (see below for what else it
// scales); an answer that still does not fit is answered `internal` by the
// host, never dropped.
messaging { max_body_bytes = 393216; }

api_key_provider google-maps {
  key_file = "/etc/brenn/google/maps-api-key";
  header = "X-Goog-Api-Key";
  api_hosts = [
    "https://weather.googleapis.com",
    "https://geocode.googleapis.com",
    "https://places.googleapis.com",
    "https://routes.googleapis.com"
  ];
}
```

`X-Goog-Api-Key` is the header Google's API platform documents for the key.
The key is sent only in that header and only to these origins; a host not
listed is refused before anything is sent. The four hosts are the ones the
component calls. Add a host only for an endpoint the raw route will call
(section 7), such as `https://airquality.googleapis.com` or
`https://maps.googleapis.com`.

No `server.vault_dir` is needed for a key provider. The provider's name,
`google-maps` here, is what grants and calls name as `provider`.

**The bus body cap, derived.** A tool the component serves makes its Google
call as a nested `api-request`, and the answer comes back to the component as
a message on the bus. Google's body travels as a JSON string inside
`api-request`'s answer, inside the tool-result envelope, so it grows by its
escaping, its headers and the envelope. The bus refuses any body over
`max_body_bytes`, 64 KiB by default
([Messaging defaults](config-dsl.md#messaging-defaults)). The key provider
reads up to `max_response_bytes`, 256 KiB by default. At the defaults, a
long transit route, a place with reviews, or a 24-hour forecast can be read
from Google and still not fit the bus. 393 216 bytes is the read limit with a
half again for escaping and the envelope.

What raising it costs: the cap is bus-wide, and other limits are derived from
it. The attach client's frame cap (`max_client_frame_bytes`) is six times the
cap plus 136 KiB, about 2.4 MiB at 393 216 bytes. The MQTT publish payload
cap and a guest component's publish and sync-reply caps equal it. The
permitted growth of a fragment channel between reaps, which
`docs/security-posture.md` bounds by default rate × `max_body_bytes` × the
reap gap, is six times the 64 KiB figure.

The alternative, for an operator who will not raise the cap: leave
`messaging` out and set `max_response_bytes = 40960;` in the
`api_key_provider` block, so every answer Brenn reads fits the default cap.
A Google answer over 40 KiB is then cut and the tool answers `internal`
naming `max_response_bytes`. Long transit routes, places asked for with
reviews, and possibly 24-hour forecasts fail that way. With neither block, an
answer between the two limits is answered `internal` naming `max_body_bytes`
(section 6, step 4).

Changing either block takes a restart. A reload that changes them is refused.

## 4. The component

`google-maps-tools` is a WASM component shipped with Brenn. Its class,
`GoogleMapsTools`, is imported from the release's module root. One instance
serves every agent granted its tools. The import goes at the top of the
document, with any other `use` lines: a `use` after any other declaration is a
syntax error.

```
use @google-maps-tools::*;
```

Then the inbox tuning, the instance and the agent:

```
// Each live request has one nested call outstanding: Σ max_concurrency (18)
// results can arrive between two activations, twice over if one is
// re-delivered. The family default of 16 would drop some; a dropped result is
// a request answered `internal` four minutes later.
channel at "brenn:tool-results/maps" {
  push_depth = 1;
  retain_depth = 64;
  standing_retain_depth = 64;
  noise = alarm;
}

new maps: GoogleMapsTools {
  grants = [log, store, config, tools, provides];
  store_path = "/var/lib/brenn/google-maps-tools.db";
  tool api-request {
    allow { provider = "google-maps"; method = "GET";  host = "weather.googleapis.com"; path = "/v1/currentConditions:lookup"; }
    allow { provider = "google-maps"; method = "GET";  host = "weather.googleapis.com"; path = "/v1/forecast/hours:lookup"; }
    allow { provider = "google-maps"; method = "GET";  host = "weather.googleapis.com"; path = "/v1/forecast/days:lookup"; }
    allow { provider = "google-maps"; method = "GET";  host = "weather.googleapis.com"; path = "/v1/publicAlerts:lookup"; }
    allow { provider = "google-maps"; method = "GET";  host = "geocode.googleapis.com"; path = "/v4/geocode/address"; }
    allow { provider = "google-maps"; method = "GET";  host = "geocode.googleapis.com"; path = "/v4/geocode/location"; }
    allow { provider = "google-maps"; method = "POST"; host = "places.googleapis.com";  path = "/v1/places:searchText"; }
    allow { provider = "google-maps"; method = "GET";  host = "places.googleapis.com";  path = "/v1/places/*"; }
    allow { provider = "google-maps"; method = "POST"; host = "routes.googleapis.com";  path = "/directions/v2:computeRoutes"; }
    rate_limit { burst = 20; sustained_per_minute = 60; }
  }
  config = {
    provider = "google-maps",
    weather_host = "weather.googleapis.com",
    geocoding_host = "geocode.googleapis.com",
    places_host = "places.googleapis.com",
    routes_host = "routes.googleapis.com",
    units = "IMPERIAL"
  };
}

agent PersonalAssistant {
  container = "sandbox";
  allowed_users = ["alice"];
  tool weather {}
  tool geocode {}
  tool places-search {}
  tool place-details {}
  tool directions {}
  tool search-along-route {}
}
```

**The result inbox.** The `channel at "brenn:tool-results/maps"` block sizes
the inbox the instance's nested answers arrive on (the slug after the `/` is
the instance's name). Each request the component serves has at most one
Google call outstanding, so at most Σ `max_concurrency` = 18 answers are
outstanding at once, and an answer may be delivered twice: 18 × 2 = 36,
rounded up to 64, which also covers the 32 ceiling on Σ `max_concurrency`
once. The default window of 16 is below 18 and would lose answers under load,
each lost answer leaving its request to be answered `internal` after the
240-second ceiling. `push_depth = 1` is the inert channel rung, and
`noise = alarm` makes a drop here an alert.

**The clauses.** The instance holds nine clauses, one per endpoint, and
nothing wider. Only the details path takes `*`, for its one variable segment,
the place id. A clause that does not admit a call's method, host and path is
refused before anything is sent (section 6, step 4). The `rate_limit` is one
bucket for the instance across every user it serves, and is the spend bound
on the key: a burst of ten two-call tools, then one Google call a second.
Callers over it are delayed, never refused.

**Acting for the caller.** Places text search and Routes are `POST`s. A
component may make a `POST` through a key provider only while serving a
request whose caller had a user — an agent's conversation or `brenn-tool` —
and the call then acts as that user. `PlacesSearch`, `Directions` and
`SearchAlongRoute` asked for by a component on its own account are answered
`denied`, with the reason. `Weather`, `Geocode` and `PlaceDetails` are reads
and work for any caller.

**`config`.** The map is flat:

- `provider` — the `api_key_provider` name.
- `weather_host`, `geocoding_host`, `places_host`, `routes_host` — the
  `host[:port]` each API is called on. Each must be one of the provider's
  `api_hosts` and admitted by a clause.
- `units` — `METRIC` (the default) or `IMPERIAL`: temperatures, speeds,
  distances and precipitation in every answer.
- `language` — optional, a BCP-47 tag such as `en` or `de`, passed to Google
  as `languageCode` for descriptions, addresses and directions.

A missing key, an empty host, a host containing `/`, or another `units`
value fails every activation with the key named.

**The agent's grants.** Each `tool <name> {}` grants one of the six tools;
grant only the ones the assistant should have. A grant may carry its own
`rate_limit`, one bucket per app, as a per-app bound below the instance's:

```
  tool weather { rate_limit { burst = 10; sustained_per_minute = 30; } }
```

**What Google bills.** Places fields are billed by tier, and the component's
field masks decide which. `PlacesSearch` and `SearchAlongRoute` ask for the
place id (Essentials), name, address, location, type and business status
(Pro), and rating, rating count, price level and open-now (Enterprise).
`PlaceDetails` asks for the same plus weekly opening hours, phone numbers,
website and the Google Maps link (Enterprise); with `reviews: true` it adds
reviews and the editorial summary, the Enterprise + Atmosphere tier.
Google's pricing pages are the authority on the tiers' prices.

## 5. Telling the assistant

The tools appear in the assistant's tool list with their descriptions, which
state each tool's arguments and answer. What the assistant cannot learn from
them is the household's context. Add lines like these to its instructions
(its system prompt or the `CLAUDE.md` it reads):

```
You have Google Maps tools: Weather, Geocode, PlacesSearch, PlaceDetails,
Directions and SearchAlongRoute.
- There is no "here". Weather and the other tools take a place name or
  "<lat>,<lng>". When the user means home, use: <the household's usual place,
  e.g. "Springfield, IL">.
- Hourly weather comes 24 hours to a page. When an answer has `next_page`,
  pass it back as `page` with the same place and `hours` for the next 24.
- When you relay a weather alert, name its source when the alert gives one.
  An empty alerts list is Google's answer when no alert is active, and also
  when a provider's alert lacked a required field, so it is not proof that
  none is in force.
- For anything these tools do not cover (air quality, time zones, weather
  history, nearby search, …), see docs/providers/google-maps.md, which lists
  each endpoint with an `api-request` example.
```

The last line works only if the assistant can read
`docs/providers/google-maps.md` — through a repo mount of a Brenn checkout,
or a copy in its working tree — and holds an `api-request` grant for the
endpoints it lists (section 7). Leave the line out otherwise.

## 6. Acceptance

Do this once per deployment, after a restart with the blocks of sections 3
and 4 in place.

1. **Weather.** Ask the assistant for the current weather at a named place.
   Expect one `Weather` call in the transcript, followed by an answer naming
   the conditions and the temperature. The diagnostic log shows two
   `api-request` lines for `google-maps`, each with status 200: one for
   `geocode.googleapis.com` `/v4/geocode/address`, then one for
   `weather.googleapis.com` `/v1/currentConditions:lookup`.
2. **Weather alerts.** Ask the assistant for weather alerts at a named place.
   Expect a `Weather` call with `kind: alerts`; the diagnostic log shows an
   `api-request` line for `/v1/publicAlerts:lookup` with status 200. An
   answer listing alerts and an answer saying none were returned are both a
   pass: the step exercises the request, not the weather.
3. **Directions.** Ask the assistant for directions between two places.
   Expect a `Directions` call; the diagnostic log shows a `POST` to
   `routes.googleapis.com` `/directions/v2:computeRoutes` with status 200.
4. **When a step fails**, the tool's error says whose answer it is.
   - A `detail` quoting a Google 400 or 403 is Google's answer, returned as
     it is; Brenn does not retry. The causes: a mistyped key; an API not
     enabled; an IP restriction that does not match the server; a used-up
     Demo Key; for `Weather` only, a key header the Weather API does not
     accept (its reference shows the key only as a `key=` query parameter);
     a rejected alerts page size (the component sends `pageSize=20`, for
     which Google's reference gives no range; if Google rejects it, every
     alerts lookup fails this way and the component needs a release that
     changes the size); or a rejected Routes mask path (the component
     requests `fallbackInfo`, a response field Google's reference lists that
     has not been tried with a key; if Google rejects it, every `Directions`
     and `SearchAlongRoute` call fails this way and the component needs a
     release that drops the path).
   - An `internal` error whose `detail` starts "api-request could not be
     issued" is Brenn's refusal before any call. It is almost always a clause
     that does not admit the method, host or path; the detail names it.
   - An `internal` error whose `detail` says "result of … bytes exceeds
     [messaging] max_body_bytes", with a Warning alert naming the tool, is
     the bus cap at its default: the `messaging` block of section 3 was not
     applied. It takes a restart.

   Fix the cause and restart.
5. **Placement.** Read the configuration and check that `key_file` lies
   under none of the paths section 2 lists. Brenn checks only the file's
   mode.
6. **The key not in the container home.** In the agent's container home,
   search for the key without putting it on a command line (bash):

   ```
   grep -rF -f <(LC_ALL=C tr -cd ' -~' < /etc/brenn/google/maps-api-key \
     | sed -e 's/^ *//' -e 's/ *$//') <container home>
   ```

   Nothing should match. The pattern is the key as Brenn sends it. Brenn
   trims the file and then refuses any byte of what remains that is not
   visible ASCII or a space, so every other byte in the file is padding:
   `tr` drops those bytes and `sed` the spaces left at either end. A carriage
   return, a no-break space or a blank line in the file does not hide a
   match.
7. **The key not in the log directory.** Brenn writes its logs to the directory
   `logging.log_dir` names (`/var/log/brenn` by default): the diagnostic log,
   the security log and each conversation's transcript. Search it with the
   same pattern:

   ```
   grep -rF -f <(LC_ALL=C tr -cd ' -~' < /etc/brenn/google/maps-api-key \
     | sed -e 's/^ *//' -e 's/ *$//') /var/log/brenn
   ```

   Nothing should match. If the server's console output is kept elsewhere,
   such as a systemd journal, search that too. Each call's final log line
   names the provider, the user, the method, host and path, and the status;
   never the key or the query.
8. **Mode.** `ls -l /etc/brenn/google/maps-api-key` shows `-rw-------`.

What Brenn does not check: it does not inspect Google's answer. A provider
that quoted request headers in its own error body would return the key to
the caller. The component copies only Google's error status and message into
its answer and logs no body, but the full answer reaches it on its
`brenn:tool-results` inbox on the bus. On the raw route the full answer is
the tool result, and Brenn keeps it as it keeps every tool result: in the
conversation's transcript in the log directory, and in the conversation's
history in the database (`/var/lib/brenn/brenn.db` by default). Step 7 finds
a copy in the log directory; it does not search the database or the bus
store. Nothing prevents any of these copies.

## 7. The raw `api-request` route

For an endpoint the component does not wrap, the assistant can call Google
itself through `api-request`. `docs/providers/google-maps.md` lists the
endpoints with an example call each: air quality, weather history, nearby
search, autocomplete, route matrices, time zones and elevation. Each host it
names must be in the provider's `api_hosts` (section 3) and admitted by a
clause of the agent's grant.

Only apps with a `container`, and not `multiuser`, may hold `api-request`,
even when the grant names only a key provider. Give the key provider its own
clauses. A clause that names `user` or `account` admits no key call, and one
that names `google-maps` together with either is refused at boot.

The assistant, able to read current air quality and run Places nearby
searches, with a rate limit:

```
agent PersonalAssistant {
  container = "sandbox";
  allowed_users = ["alice"];
  tool api-request {
    allow { provider = "google-maps"; method = "POST"; host = "airquality.googleapis.com"; path = "/v1/currentConditions:lookup"; }
    allow { provider = "google-maps"; method = "POST"; host = "places.googleapis.com"; path = "/v1/places:searchNearby"; }
    rate_limit { burst = 5; sustained_per_minute = 30; }
  }
}
```

The rate limit applies per app, across all its users. Since every call is
billed to the same key, it is the bound on how fast one conversation can
spend.

A WASM component calling a key provider on its own account may use `GET`
and `HEAD` only. A component serving a provided-tool call whose caller had a
user (an agent, `brenn-tool`, or another component serving such a call) acts
as that user and may use any method its clause admits — which is how
`google-maps-tools` makes the Places and Routes `POST`s for a conversation.
A request from a component calling on its own account stays read-only; the
wrapper answers it `denied`.

Tell the assistant how to call, in its instructions:

```
Google Maps Platform is also available through the `api-request` tool with
provider "google-maps". Brenn attaches the key; never send X-Goog-Api-Key
yourself, and never pass `account`. Endpoints and examples are in
docs/providers/google-maps.md.
```

An unknown provider name is refused with the list of configured providers,
so a wrong name is corrected at the first call.

## 8. Rotating and removing

- **The key.** Create a new key (or, for the Demo Key, retrieve the new one),
  write it over the key file keeping mode `0600`, and restart Brenn. A key
  file replaced in place is not noticed until the restart. Delete the old key
  in the console once Brenn is running on the new one.
- **A suspected leak.** Delete the key in the console first; that cuts it off
  at Google whatever holds a copy. Then place a new one and restart. A key a
  provider echoed is also wherever that tool result is kept (section 6): the
  conversation's transcript in the log directory, the conversation's history
  in the database, and any copy of either a log rotation or backup made.
- **Everything.** Remove the `google-maps-tools` instance, the agents' grants
  of its tools, the `api_key_provider` block and every grant clause naming
  it, restart, then delete the key file and the key in the console.
