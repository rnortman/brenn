# Connecting Google accounts

This guide sets Brenn up so the people who use it can connect their own Google
accounts (Gmail and Calendar), and their assistant can read and act through
them. Each user's tokens live in a vault on the host. They never enter an
agent's container: Brenn attaches the credential to each request itself.

The assistant reaches Google in one of two ways:

- **The `google-workspace-tools` component** (section 5) gives it four native
  tools — `GmailLabels`, `GmailSearch`, `GmailRead` and `GmailThread` — that
  build Gmail's requests for whichever user is calling and answer with
  compact JSON, the message text decoded. This is the route to use for
  reading mail.
- **The raw `api-request` route** (section 8) lets it call any Gmail or
  Calendar endpoint directly, assembling the request and reading Google's
  answer itself. It is the route for Calendar, for writes, and for Gmail
  endpoints the component does not wrap.

The configuration keys are documented in `docs/config-dsl.md`, under *OAuth
providers and the credential vault*, *Messaging defaults* and *Provided
tools*. This guide covers the Google side, the vault, a working
configuration, the grants, the component, what to tell the assistant, the
first connect, the raw route, and rotation.

Google Maps Platform APIs (Weather, Places, Routes) take a deployment-wide API
key rather than a user's account; they are set up in
`docs/google-maps-platform.md`.

## 1. The Google Cloud project

One project serves every user of this Brenn deployment.

1. In the Google Cloud console, create a project (for example `brenn-alice`).
2. **APIs & Services → Library**: enable the **Gmail API** and the **Google
   Calendar API**. Add others (People, Drive) only when a grant needs them.
3. **Google Auth Platform → Branding / Audience**: user type **External**. Set
   the app name. Brenn shows the same name as `app_name`, so the user
   recognises it on the consent screen. Then **publish** the app. Leave it
   unverified: an app in *Testing* status gets refresh tokens that expire after
   seven days. A published app's tokens last until the user revokes them,
   changes their password, or leaves them unused for six months.
4. **Data access**: add every scope that any `scope_sets` entry below names.
   The consent screen offers the union.
5. **Clients → Create client → Web application**. Under *Authorized redirect
   URIs*, add exactly
   `<server.public_url>/oauth/google/callback`
   (for example `https://brenn.example.com/oauth/google/callback`). Download
   the client's JSON.
6. Put the download on the host where Brenn can read it, mode `0600`, owned by
   the server's user (for example `/etc/brenn/google/client_secret.json`).
   Brenn refuses to start if the mode is wider, or if the file's
   `redirect_uris` does not contain the callback above.

Gmail's read and modify scopes are *restricted*. An unverified app shows users
an "unverified app" warning before consent. Google may also refuse restricted
scopes to an unverified app entirely. If so, the consent attempt ends with
"nothing was connected", and calendar scopes, which are only *sensitive*,
still connect on their own. The first acceptance step (section 7) finds out
which applies to your project.

## 2. The vault

Create the vault directory once, as the server's user:

```
install -d -m 0700 /var/lib/brenn/vault
```

It must not lie inside, or contain, any path the configuration gives an
agent: working directories, repo mounts, state directories, container homes,
`extra_mounts` host paths, or the repo-sync directory. Brenn checks this at
boot and on every reload. A container's `extra_args` reach podman unread, so
a mount written there is not checked and could expose the vault: mount host
paths through `extra_mounts`. Brenn creates `<provider>/<username>/` beneath it. Each credential is
the file `<account>.json`, mode `0600`.

## 3. Configuration

```
server {
  public_url = "https://brenn.example.com";
  vault_dir = "/var/lib/brenn/vault";
}

oauth_provider google {
  app_name           = "Brenn";
  client_secret_file = "/etc/brenn/google/client_secret.json";
  authorize_url      = "https://accounts.google.com/o/oauth2/v2/auth";
  token_url          = "https://oauth2.googleapis.com/token";
  revoke_url         = "https://oauth2.googleapis.com/revoke";
  userinfo_url       = "https://openidconnect.googleapis.com/v1/userinfo";
  account_field      = "email";
  base_scopes        = ["openid", "https://www.googleapis.com/auth/userinfo.email"];
  scope_sets = {
    gmail_readonly    = ["https://www.googleapis.com/auth/gmail.readonly"],
    gmail_organize    = ["https://www.googleapis.com/auth/gmail.labels", "https://www.googleapis.com/auth/gmail.modify"],
    gmail_compose     = ["https://www.googleapis.com/auth/gmail.compose"],
    calendar_readonly = ["https://www.googleapis.com/auth/calendar.readonly"],
    calendar_events   = ["https://www.googleapis.com/auth/calendar.events"],
  };
  extra_authorize_params = { access_type = "offline", prompt = "consent", include_granted_scopes = "true" };
  api_hosts          = ["https://gmail.googleapis.com", "https://www.googleapis.com", "https://people.googleapis.com"];
}
```

`access_type = "offline"` and `prompt = "consent"` make Google issue a refresh
token on every connect, including a reconnect. Without them a reconnect can
come back with no refresh token, and Brenn stores nothing.

The component (section 5) needs one more block, the bus's body cap:

```
// Every nested api-request answer crosses the bus as a tool result, so the
// body cap must carry the OAuth provider's read limit (max_response_bytes,
// 256 KiB default) plus the growth of the body as a JSON string, its headers
// and the result envelope: 256 KiB × 1.5. This cap is bus-wide; an answer
// that still does not fit is answered `internal` by the host, never dropped.
messaging { max_body_bytes = 393216; }
```

**The bus body cap, derived.** Google's body travels as a JSON string inside
`api-request`'s answer, inside the tool-result envelope, so it grows by its
escaping (every `"` and `\` in Google's JSON becomes two bytes), its headers
and the envelope; the bus refuses any body over `max_body_bytes`, 64 KiB by
default. The provider reads up to `max_response_bytes`, 256 KiB by default,
and 393 216 is that limit with a half again for the escaping and the
envelope — the same pair, for the same reason, as the Maps guide, which
gives the full account of what raising the cap costs
(`docs/google-maps-platform.md`, section 3). A message body is base64url and
does not grow when escaped, so an ordinary message's body fits the pair
with room to spare. A message whose sender has filled its headers with quotes or
backslashes is different: those escape twice, once by Google and once by
`api-request`, and such a message can be refused on the bus cap — `internal`
naming `max_body_bytes` — by whichever of these tools reads it, and the
request that read it fails, a search page that listed it included. No cap
setting closes that, and that message is simply not readable through these
tools; it is the sender's doing, not the operator's configuration, and mail
is untrusted input.

Operators who already have the Maps guide's `messaging` block need nothing
more: the cap is bus-wide, and the value is the same.

The alternative, for an operator who will not raise the cap: leave
`messaging` out and set `max_response_bytes = 40960;` in the
`oauth_provider` block, so every answer Brenn reads fits the default cap.
For Gmail that costs most HTML mail: a message over 40 KiB is answered by
`GmailRead` with its headers and attachments but no text (section 5). It
also costs long threads: `GmailThread` refuses a thread whose messages'
headers together pass the limit, and such a thread is read one message at a
time (section 7, step 8).

Changing these blocks, or replacing the client file, takes a restart. A
reload that changes them is refused.

## 4. Grants

Only apps with a `container`, and not `multiuser`, may hold `api-request`,
`oauth-connect`, `oauth-accounts` and `oauth-disconnect`. Each conversation
acts with its owner's credentials, never anyone else's.

The assistant, able to connect and manage its owner's accounts:

```
agent PersonalAssistant {
  container = "sandbox";
  allowed_users = ["alice"];
  tool oauth-connect    { allow { provider = "google"; } }
  tool oauth-accounts   { allow { provider = "google"; } }
  tool oauth-disconnect { allow { provider = "google"; } }
}
```

Reading mail is granted through the component's tools (section 5). Calendar,
writes, and Gmail endpoints the tools do not cover are granted as
`api-request` clauses (section 8).

## 5. The component

The `google-workspace-tools` component answers the assistant's Gmail tools
with stamped `api-request` calls for whichever user is calling. Its class,
`GoogleWorkspaceTools`, is imported from the release's module root. One
instance serves every agent granted its tools. The import goes at the top of
the document, with any other `use` lines: a `use` after any other
declaration is a syntax error.

```
use @google-workspace-tools::*;
```

Then the inbox tuning and the instance:

```
// Each live request has one nested call outstanding: Σ max_concurrency
// results can arrive between two activations, twice over if one is
// re-delivered. The four Gmail tools declare Σ = 12, which needs 24, rounded
// up to 32. 64 holds the 32 ceiling on Σ twice over, so tools joining the
// class change nothing here.
channel at "brenn:tool-results/workspace" {
  push_depth = 1;
  retain_depth = 64;
  standing_retain_depth = 64;
  noise = alarm;
}

new workspace: GoogleWorkspaceTools {
  grants = [log, store, config, tools, provides];
  store_path = "/var/lib/brenn/google-workspace-tools.db";
  tool api-request {
    allow { provider = "google"; user = "*"; method = "GET"; host = "gmail.googleapis.com"; path = "/gmail/v1/users/me/labels"; }
    allow { provider = "google"; user = "*"; method = "GET"; host = "gmail.googleapis.com"; path = "/gmail/v1/users/me/labels/*"; }
    allow { provider = "google"; user = "*"; method = "GET"; host = "gmail.googleapis.com"; path = "/gmail/v1/users/me/messages"; }
    allow { provider = "google"; user = "*"; method = "GET"; host = "gmail.googleapis.com"; path = "/gmail/v1/users/me/messages/*"; }
    allow { provider = "google"; user = "*"; method = "GET"; host = "gmail.googleapis.com"; path = "/gmail/v1/users/me/threads/*"; }
    rate_limit { burst = 60; sustained_per_minute = 120; }
  }
  config = {
    provider = "google",
    gmail_host = "gmail.googleapis.com"
  };
}
```

The assistant, granted the four tools beside its account tools (section 4):

```
agent PersonalAssistant {
  container = "sandbox";
  allowed_users = ["alice"];
  tool oauth-connect    { allow { provider = "google"; } }
  tool oauth-accounts   { allow { provider = "google"; } }
  tool oauth-disconnect { allow { provider = "google"; } }
  tool gmail-labels {}
  tool gmail-search {}
  tool gmail-read {}
  tool gmail-thread {}
}
```

An `api-request` grant (section 8) may sit beside these; the tools do not
need one.

**The result inbox.** The `channel at "brenn:tool-results/workspace"` block
sizes the inbox the instance's nested answers arrive on (the slug after the
`/` is the instance's name). Each request the component serves has at most
one Google call outstanding, so at most Σ `max_concurrency` = 12 answers are
outstanding at once, and an answer may be delivered twice: 12 × 2 = 24,
rounded up to 32. The block sets 64, which covers the 32 ceiling on Σ
`max_concurrency` twice over, so the Calendar tools joining this class later
change nothing deployed. The default window of 16 is below 24 and would lose
answers under load, each lost answer leaving its request to be answered
`internal` after the 240-second ceiling. `push_depth = 1` is the inert
channel rung, and `noise = alarm` makes a drop here an alert.

**The clauses.** The instance holds five clauses, one per Gmail endpoint the
tools call — the label list, one label, the message search, one message, one
thread — every one a `GET`, and nothing wider. The `*` segments take a label,
message or thread id. A clause that does not admit a call's method, host and
path is refused before anything is sent (section 7).

**Acting for the caller.** `user = "*"` admits a call for whichever user is
calling: each call the component makes for a request is stamped with the
user of the conversation that asked, and uses that user's credential. Nothing
is lent and no user is named in the configuration. The consequence: the
instance cannot read on its own account. A component calling these tools on
its own account, with no user behind the request, is answered `denied`, with
the reason. This is a property of serving any caller, not a defect.

**The rate limit.** The `rate_limit` is one bucket for the instance across
every user it serves. `GmailSearch` at its largest (`max = 25`) is 27 calls —
the search, one per message, and one for label names — so a burst of 60
admits two such searches without stalling. Callers over the limit are
delayed, never refused. Google's own quota, 250 units per user per second
with a list or a get costing 5, is the real bound on a user.

**`config`.** The map is flat:

- `provider` — the `oauth_provider` name.
- `gmail_host` — the `host[:port]` Gmail is called on. It must be one of the
  provider's `api_hosts` and admitted by the clauses.

A missing key, an empty one, or a host containing `/` fails every activation
with the key named.

**The user's connection.** Each user connects their account with the
`gmail_readonly` scope set (section 7, step 1); the tools read nothing else.
What the tools answer when that is not in place:

- **No usable credential** — no account connected, its file gone, a refresh
  Google refused: the answer is `{"error": "credential_invalid", …}`, and the
  assistant runs `oauth-connect` and relays its steps.
- **Several accounts connected** — a call that does not name `account` is
  answered `denied`, listing them; the assistant asks again with one.
- **Connected without the Gmail read scope** — Google answers 403, and the
  tool answers `denied` with Google's message and a sentence saying to
  reconnect with the scope set.

**What an answer holds.** `GmailRead` answers the message's text, decoded,
up to `max_chars` characters (12 000 by default, 200 to 60 000), and lists
attachments without fetching them. A message larger than the read limit
(`max_response_bytes`) is answered with its headers, its attachments and
Gmail's preview, `text` null and `text_omitted` saying why. Header values,
attachment names and MIME types are bounded in bytes and a cut header is
flagged; a long text is cut to the wrapper's result ceiling and says so; a
long thread is trimmed to its newest messages and `messages_omitted` says
how many were left out; and a thread whose headers alone are too large to
read is refused — the `internal` answer names the provider's read limit
(`max_response_bytes`) or the bus cap (`max_body_bytes`) — and a smaller
`max` does not help.

## 6. Telling the assistant

The tools appear in the assistant's tool list with their descriptions, which
state each tool's arguments and answer. Add lines like these to its
instructions (its system prompt or the `CLAUDE.md` it reads):

```
You have Gmail tools: GmailLabels, GmailSearch, GmailRead and GmailThread.
- Pass `account` only when the user has several Google accounts connected;
  the refusal names them.
- An answer {"error": "credential_invalid", ...} means the user has no usable
  Google connection: run oauth-connect for provider "google" with the
  gmail_readonly scope set and relay its steps to the user.
- GmailThread lists a thread's messages without their text; read a message's
  text with GmailRead.
- For anything else (attachments, Calendar, the profile, history), see
  docs/providers/google.md and call api-request with provider "google".
  Brenn attaches the user's token; never send an Authorization header.
```

The last line works only if the assistant can read
`docs/providers/google.md` — through a repo mount of a Brenn checkout, or a
copy in its working tree — and holds an `api-request` grant for the
endpoints it lists (section 8). Leave the line out otherwise.

## 7. First connect: acceptance

Do this once per deployment, in order, after a restart with the blocks of
sections 3 and 5 in place.

1. **Connect your own account first.** In a conversation with the assistant,
   ask it to connect Google with every scope set. Open the link in the browser
   you use for Brenn. Choose the account, go through the "unverified app"
   warning (*Advanced* → *Go to Brenn*), and tick every box. The page should say
   **Connected `<account>` to Brenn for Brenn user `<you>`**. Ask the assistant to list connected
   accounts. Confirm that the Gmail scopes appear among the granted scopes. If
   they are missing, Google refused restricted scopes to the unverified app;
   only calendar access will work.
2. **A family member connects from a phone.** Same steps, on their phone. If
   the link opens in a browser where they are not signed in to Brenn, they see
   the Brenn login page. After logging in they land back on the callback and
   the connect completes, provided they finish within ten minutes of the link
   being made.
3. **Labels.** Ask the assistant to list your Gmail labels. Expect one
   `GmailLabels` call; the diagnostic log shows one `api-request` line for
   `gmail.googleapis.com` `/gmail/v1/users/me/labels`, status 200.
4. **Search.** Ask for your unread mail from the last week. Expect one
   `GmailSearch` call; the diagnostic log shows one line for
   `/gmail/v1/users/me/messages`, then one per message listed, each status
   200, and one more for the labels when the mail carries labels of your
   own. The answer's senders, subjects and labels are readable, your own
   labels by name. This step is the live check of the request shapes the
   search sends.
5. **Read.** Ask the assistant to read one HTML message, a newsletter for
   example. Expect a `GmailRead` call whose answer has the text without tags,
   the attachments listed, and `text_source` stated. If a message in another
   charset than UTF-8 is at hand, read it too: `decoding_notes` is absent or
   names the charset.
6. **Thread.** Ask for a conversation with several messages. Expect a
   `GmailThread` call listing them oldest first.
7. **No account.** A second Brenn user who has not connected asks the
   assistant for their mail. The assistant is answered `credential_invalid`
   and offers to connect, with a link.
8. **When a step fails**, the tool's error says whose answer it is.
   - A `denied` quoting Google's 403 is Google's refusal: the account was
     connected without the Gmail read scope (reconnect with
     `gmail_readonly`), or Google refuses restricted scopes to the
     unverified app (step 1).
   - An `internal` error whose `detail` starts "api-request could not be
     issued" is Brenn's refusal before any call. It is almost always a clause
     that does not admit the method, host or path; the detail names it.
   - An `internal` error whose `detail` says "result of … bytes exceeds
     [messaging] max_body_bytes", on ordinary mail, is the bus cap at its
     default: the `messaging` block of section 3 was not applied, and it
     takes a restart. Under the recommended pair, from any of these tools, it
     is a message whose sender-written headers are oversized, and the request
     that read it failed — a single read, a thread, or a search page that
     listed it (a narrower query, one that lists no such message, gets the
     listing back); that message is not readable through these tools, and
     neither is a message that carries the same headers (a reply, whose
     subject is copied from it); there is nothing to raise and nothing to
     fix.
   - A `GmailRead` answer with `text_omitted` on an ordinary message means
     `max_response_bytes` was lowered (section 3).
   - A `GmailThread` answer `internal` saying Google's answer "exceeded the
     provider's max_response_bytes and was cut" is a thread whose headers
     alone are too large to read. A smaller `max` does not help. Two
     remedies, in this order:
     1. **Read the messages one at a time.** The assistant finds the
        thread's messages with `GmailSearch`, whose lines carry `thread_id`,
        and reads each with `GmailRead` (section 6 already tells it to read
        a thread's messages that way). This changes no configuration.
     2. **Raise the read limit.** Set `max_response_bytes = 524288;` in the
        `oauth_provider` block — its ceiling — with
        `messaging { max_body_bytes = 524288; }`. This pair is not the
        half-again of section 3: at the ceiling `api-request` shortens its
        own answer to fit the largest result a tool may return, less room
        for the envelope, so its answer never passes 524 288 bytes and a
        larger cap gains nothing. The raise is bus-wide: `max_body_bytes`
        bounds every publish, and the attach frame, MQTT publish and guest
        caps derive from it (`docs/config-dsl.md`, *Messaging defaults*).
        It takes a restart. A thread larger than the result ceiling is
        trimmed to its newest messages, as section 5 says; that is not a
        failure. If this detail comes back with `max_response_bytes`
        already at 524288, the thread is beyond the configuration remedy —
        the raise took effect, and reading the messages one at a time is the
        only remedy left.

   Fix the cause and restart.
9. **No token in the container.** In the agent's container home, `grep -r`
   for the start of the access token (`ya29.`) and for the refresh token
   (`1//`). Nothing should match.
10. **Vault modes.** `ls -la` of the vault shows `0700` directories and
    `0600` files, owned by the server's user.
11. **Disconnect.** Ask the assistant to disconnect one account. Then confirm
    that Brenn no longer appears at `myaccount.google.com/permissions` for
    that account, and that its file is gone from the vault.

## 8. The raw `api-request` route

For Calendar, for writes, and for Gmail endpoints the component does not
wrap, the assistant can call Google itself through `api-request`.
`docs/providers/google.md` lists the endpoints with an example call each:
the Gmail profile, attachments, a message in another form, history, thread
listings and drafts; the Calendar list, events, free/busy, colors and
settings. Each host it names must be in the provider's `api_hosts`
(section 3) and admitted by a clause of the agent's grant.

The assistant, reading only, beside its other grants:

```
tool api-request { allow { provider = "google"; method = "GET"; } }
```

The same assistant, also allowed to create drafts and to label mail. Each
write clause names the exact path. `drafts` does not admit `drafts/send`:

```
tool api-request {
  allow { provider = "google"; method = "GET"; }
  allow { provider = "google"; method = "POST"; host = "gmail.googleapis.com"; path = "/gmail/v1/users/me/drafts"; }
  allow { provider = "google"; method = "POST"; host = "gmail.googleapis.com"; path = "/gmail/v1/users/me/messages/batchModify"; }
}
```

A WASM automation with no conversation behind it, reading one user's calendar.
Its clause lends it that user's credential. A lent credential is read-only:

```
new agenda_sync: AgendaSync {
  grants = [ports, tools];
  tool api-request {
    allow { provider = "google"; user = "alice"; account = "alice@example.com"; method = "GET"; host = "www.googleapis.com"; path = "/calendar/v3/calendars/*/events"; }
    rate_limit { burst = 5; sustained_per_minute = 30; }
  }
}
```

The rate limit applies per app, or per component, across all its users.

Tell the assistant how to call, in its instructions:

```
Gmail and Google Calendar are also available through the `api-request` tool
with provider "google". Brenn attaches the user's token; never send an
Authorization header. Pass `account` only when the user has several Google
accounts connected. Endpoints and examples are in docs/providers/google.md.
```

## 9. Rotating and removing by hand

- **A user's credential.** Delete
  `<vault_dir>/google/<username>/<account>.json`. The next request for it
  reports `credential_invalid`, and the assistant offers to reconnect. Delete
  the file *and* remove Brenn at `myaccount.google.com/permissions` to cut
  access at Google too. Deleting the file alone leaves the token valid at
  Google until it is revoked.
- **The client secret.** Create a new secret on the client in the console,
  download the JSON over the old file (keeping mode `0600`), and restart Brenn.
  Existing refresh tokens keep working. Delete the old secret in the console
  once Brenn is running on the new one. If Google refuses the client's
  credentials (a deleted secret, a deleted client), Brenn alerts once per
  provider until restart, and users' requests fail without asking them to
  reconnect: their credentials are fine, and reconnecting cannot work until
  the client file is fixed.
- **Everything.** Remove the component: the `google-workspace-tools`
  instance, its channel block, and the agents' grants of its tools. Then the
  `oauth_provider` block, the `vault_dir` key and every grant of
  `api-request`, `oauth-connect`, `oauth-accounts` and `oauth-disconnect`
  naming the provider; restart, then delete the vault directory and the
  component's store file.
- **A stale consent.** If a connect reports that Google did not issue a
  long-lived credential, the user removes Brenn at
  `myaccount.google.com/permissions` and connects again.
