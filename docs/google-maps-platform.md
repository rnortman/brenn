# Google Maps Platform with an API key

This guide sets Brenn up so an assistant can call Google Maps Platform REST
APIs — Weather, Places, Routes, Geocoding, Air Quality, Time Zone — with one
API key that belongs to the deployment. The operator places the key in a file
on the host. Brenn reads it at boot and attaches it to each request as a
header, itself; it never passes the key into an agent's container.

Nobody connects an account: the key is the deployment's, every caller the
grant admits sends the same key, and what it spends is the operator's.

The configuration keys are documented in `docs/config-dsl.md`, under *API-key
providers*. This guide covers the Google side, the key file, a working block,
the grant, what to tell the assistant, and an acceptance check. Connecting
users' own Google accounts (Gmail, Calendar) is a separate setup, in
`docs/google-workspace.md`; the two can be declared together or alone.

## 1. A key

There are two ways to get a key. The block, the grant and everything after
this section are the same for both: Brenn does not tell them apart.

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
  both acceptance calls in section 6.
- It is obtained with the page's **Get a Demo Key** button, and can be
  retrieved later from the Cloud console.

Google may change any of this; read the page again before relying on it.

### For real use: a standard key with billing

1. In the Google Cloud console, create a project (for example `brenn-maps`)
   and attach a billing account to it.
2. **APIs & Services → Library**: enable each API the assistant will call —
   for example the **Weather API**, **Places API (New)**, **Routes API**,
   **Geocoding API**, **Air Quality API** and **Time Zone API**. Enable only
   what a grant will use.
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

```
api_key_provider google-maps {
  key_file = "/etc/brenn/google/maps-api-key";
  header = "X-Goog-Api-Key";
  api_hosts = [
    "https://weather.googleapis.com",
    "https://places.googleapis.com",
    "https://routes.googleapis.com",
    "https://airquality.googleapis.com",
    "https://maps.googleapis.com",
  ];
}
```

`X-Goog-Api-Key` is the header every Maps Platform API takes the key in. The
key is sent only in that header and only to these origins; a host not listed
is refused before anything is sent. List only the hosts the enabled APIs use.

No `server.vault_dir` is needed for a key provider. The provider's name,
`google-maps` here, is what grants and calls name as `provider`.

Changing this block takes a restart. A reload that changes it is refused.

## 4. Grant

Only apps with a `container`, and not `multiuser`, may hold `api-request`,
even when the grant names only a key provider. Give the key provider its own
clauses. A clause that names `user` or `account` admits no key call, and one
that names `google-maps` together with either is refused at boot.

The assistant, able to read the weather and run Places text searches, with a
rate limit:

```
agent PersonalAssistant {
  container = "sandbox";
  allowed_users = ["alice"];
  tool api-request {
    allow { provider = "google-maps"; method = "GET"; host = "weather.googleapis.com"; }
    allow { provider = "google-maps"; method = "POST"; host = "places.googleapis.com"; path = "/v1/places:searchText"; }
    rate_limit { burst = 5; sustained_per_minute = 30; }
  }
}
```

The rate limit applies per app, across all its users. Since every call is
billed to the same key, it is the bound on how fast one conversation can
spend.

A WASM component may call a key provider too, with `GET` and `HEAD` only.
Places text search and Routes are `POST`s and are refused to a component.

## 5. Telling the assistant

The assistant learns the provider and hosts from its instructions (its system
prompt or the `CLAUDE.md` it reads), not from Brenn. Add lines like these:

```
Google Maps Platform is available through the `api-request` tool with
provider "google-maps". Brenn attaches the key; never send X-Goog-Api-Key
yourself, and never pass `account`.
- Weather: host weather.googleapis.com, for example
  GET /v1/currentConditions:lookup with query location.latitude and
  location.longitude.
- Places: host places.googleapis.com, POST /v1/places:searchText with a JSON
  body {"textQuery": "..."}. Places requires an X-Goog-FieldMask header
  naming the fields to return, such as
  "places.displayName,places.formattedAddress".
```

An unknown provider name is refused with the list of configured providers,
so a wrong name is corrected at the first call.

## 6. Acceptance

Do this once per deployment, after a restart with the block in place.

1. **Weather.** Ask the assistant for the current weather at a latitude and
   longitude. It calls `api-request` with `GET` on
   `weather.googleapis.com/v1/currentConditions:lookup` and a
   `location.latitude`/`location.longitude` query. The result has status 200.
2. **Places.** Ask the assistant to find a place by name. It calls
   `api-request` with `POST` on
   `places.googleapis.com/v1/places:searchText` and an `X-Goog-FieldMask`
   header. The result has status 200.

   A 400 or 403 in either step is Google's answer, returned as it is. Brenn
   does not read it and does not retry: a mistyped key, an API not enabled,
   an IP restriction that does not match the server, or a used-up Demo Key
   all look like this. Read the body, fix the cause, and restart if the key
   file changed.
3. **Placement.** Read the configuration and check that `key_file` lies
   under none of the paths section 2 lists. Brenn checks only the file's
   mode.
4. **The key not in the container home.** In the agent's container home,
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
5. **The key not in the log directory.** Brenn writes its logs to the directory
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
6. **Mode.** `ls -l /etc/brenn/google/maps-api-key` shows `-rw-------`.

What Brenn does not check: it does not inspect the answer. A provider that
quoted request headers in its own error body would return the key in the
tool result, and Brenn keeps that result as it keeps every tool result: in
the conversation's transcript in the log directory, and in the conversation's
history in the database (`/var/lib/brenn/brenn.db` by default). For a
component the result goes to its `brenn:tool-results` inbox on the bus, and a
component that logs it puts it in the diagnostic log. Step 5 finds a copy in
the log directory; it does not search the database or the bus store. Nothing
prevents any of these copies.

## 7. Rotating and removing

- **The key.** Create a new key (or, for the Demo Key, retrieve the new one),
  write it over the key file keeping mode `0600`, and restart Brenn. A key
  file replaced in place is not noticed until the restart. Delete the old key
  in the console once Brenn is running on the new one.
- **A suspected leak.** Delete the key in the console first; that cuts it off
  at Google whatever holds a copy. Then place a new one and restart. A key a
  provider echoed is also wherever that tool result is kept (§6): the
  conversation's transcript in the log directory, the conversation's history
  in the database, and any copy of either a log rotation or backup made.
- **Everything.** Remove the `api_key_provider` block and every grant clause
  naming it, restart, then delete the key file and the key in the console.
