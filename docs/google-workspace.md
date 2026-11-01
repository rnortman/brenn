# Connecting Google accounts

This guide sets Brenn up so the people who use it can connect their own Google
accounts (Gmail and Calendar), and their assistant can read and act through
them. Each user's tokens live in a vault on the host. They never enter an
agent's container: Brenn attaches the credential to each request itself.

The assistant reaches Google in one of two ways:

- **The `google-workspace-tools` component** (section 5) gives it
  native tools — `GmailLabels`, `GmailSearch`, `GmailRead` and `GmailThread`
  for reading mail, `GmailDraft` and `GmailModifyLabels` for drafting replies
  and filing mail, `CalendarList`, `CalendarEvents`, `CalendarEvent` and
  `CalendarFreeBusy` for reading the calendar, and `CalendarCreate`,
  `CalendarUpdate` and `CalendarDelete` for changing it — that build
  Google's requests for whichever user is calling and answer with compact
  JSON, the message text decoded. This is the route to use for reading mail
  and calendars, creating drafts, changing labels and creating, changing and
  deleting events. No tool sends, deletes or trashes mail; the Calendar
  write tools create, change and delete events and send invitations only
  when asked (`notify`).
- **The raw `api-request` route** (section 8) lets it call any Gmail or
  Calendar endpoint directly, assembling the request and reading Google's
  answer itself. It is the route for the endpoints the component does not
  wrap.

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
`api-request`, and such a message's read can be refused on the bus cap by
whichever of these tools reads it. A search page that lists it is answered
with its other messages and names that one in `unavailable`, with the reason;
a thread that holds it is read again without `To` and `Cc`, which answers
when those were the oversized headers; any other read of it is answered
`internal` naming `max_body_bytes`. No cap setting closes that, and that
message is simply not readable through these tools; it is the sender's
doing, not the operator's configuration, and mail is untrusted input.

Operators who already have the Maps guide's `messaging` block need nothing
more: the cap is bus-wide, and the value is the same.

The alternative, for an operator who will not raise the cap: leave
`messaging` out and set `max_response_bytes = 40960;` in the
`oauth_provider` block, so every answer Brenn reads fits the default cap.
For Gmail that costs most HTML mail: a message over 40 KiB is answered by
`GmailRead` with its headers and attachments but no text (section 5). It
also costs long threads: a thread whose messages' headers together pass the
limit is answered without `to` and `cc`, and one whose remaining headers
still pass it is refused and read one message at a time (section 7,
step 18). For Calendar it costs wide windows: `CalendarEvents` and
`CalendarFreeBusy` refuse a window whose answer passes the limit and say
what to narrow.

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

Reading mail and calendars, creating drafts, changing labels and creating,
changing and deleting events are granted through the component's tools
(section 5); no tool sends, deletes or trashes mail, and the Calendar write
tools send invitations only when asked (`notify`). Gmail endpoints the tools do not
cover, and Calendar's colors and settings, are granted as `api-request`
clauses (section 8).

## 5. The component

The `google-workspace-tools` component answers the assistant's Gmail and
Calendar tools with stamped `api-request` calls for whichever user is calling. Its class,
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
// re-delivered. The config check holds Σ to at most 32, and 64 holds that
// twice over, so tools joining the class change nothing here.
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
    allow { provider = "google"; user = "*"; method = "GET"; host = "www.googleapis.com"; path = "/calendar/v3/users/me/calendarList"; }
    allow { provider = "google"; user = "*"; method = "GET"; host = "www.googleapis.com"; path = "/calendar/v3/calendars/*/events"; }
    allow { provider = "google"; user = "*"; method = "GET"; host = "www.googleapis.com"; path = "/calendar/v3/calendars/*/events/*"; }
    allow { provider = "google"; user = "*"; method = "POST"; host = "www.googleapis.com"; path = "/calendar/v3/freeBusy"; }
    allow { provider = "google"; user = "*"; method = "POST"; host = "gmail.googleapis.com"; path = "/gmail/v1/users/me/drafts"; }
    allow { provider = "google"; user = "*"; method = "POST"; host = "gmail.googleapis.com"; path = "/gmail/v1/users/me/messages/batchModify"; }
    allow { provider = "google"; user = "*"; method = "POST"; host = "www.googleapis.com"; path = "/calendar/v3/calendars/*/events"; }
    allow { provider = "google"; user = "*"; method = "PATCH"; host = "www.googleapis.com"; path = "/calendar/v3/calendars/*/events/*"; }
    allow { provider = "google"; user = "*"; method = "DELETE"; host = "www.googleapis.com"; path = "/calendar/v3/calendars/*/events/*"; }
    rate_limit { burst = 60; sustained_per_minute = 120; }
  }
  config = {
    provider = "google",
    gmail_host = "gmail.googleapis.com",
    calendar_host = "www.googleapis.com"
  };
}
```

The assistant, granted every tool the instance provides beside its account tools
(section 4):

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
  tool gmail-draft {}
  tool gmail-modify-labels {}
  tool calendar-list {}
  tool calendar-events {}
  tool calendar-event {}
  tool calendar-free-busy {}
  tool calendar-create {}
  tool calendar-update {}
  tool calendar-delete {}
}
```

An `api-request` grant (section 8) may sit beside these; the tools do not
need one.

**The result inbox.** The `channel at "brenn:tool-results/workspace"` block
sizes the inbox the instance's nested answers arrive on (the slug after the
`/` is the instance's name). Each request the component serves has at most
one Google call outstanding, so at most Σ `max_concurrency` answers are
outstanding at once, and an answer may be delivered twice: the window must
hold 2 × Σ. The config check holds Σ to at most 32
(`PROVIDER_CONCURRENCY_CEILING`), and the block sets 64, which holds that
twice over, so tools joining this class later change nothing deployed. The
default window of 16 is too small for a full window and would lose answers
under load, each lost answer leaving its request to be answered `internal`
after the 240-second ceiling. `push_depth = 1` is the inert
channel rung, and `noise = alarm` makes a drop here an alert.

**The clauses.** The instance holds one clause per endpoint the tools call
— Gmail's label list, one label, the message search, one message, one
thread, draft creation and the batch label change, and Calendar's calendar
list, one calendar's events, one event, free/busy, and creating, changing
and deleting one event — and nothing wider. The reads are `GET`s. Every
other method is admitted only for a stamped call: the `POST` for free/busy,
which changes nothing; the `POST` for `drafts`, which creates a draft in the
user's Drafts and sends nothing; the `POST` for `messages/batchModify`,
which adds and removes labels on the messages named; and on Calendar the
`POST` for `calendars/*/events`, which creates an event, the `PATCH` for
`calendars/*/events/*`, which changes one, and the `DELETE` for the same
path, which deletes one. Each write path is exact: a clause's path matches
only a path with the same number of segments and every literal segment
equal, with no prefix match. So `drafts` does not admit `drafts/send`;
`calendars/*/events/*` admits neither `…/move` nor `…/instances`, which
have one segment more; and `calendars/*/events` does not admit
`calendars/*/clear`, which has the same number of segments but whose
literal `clear` is not `events`. The `*` segments take a label, message,
thread, calendar or event id. A clause that does not admit a call's method,
host and path is refused before anything is sent (section 7).

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
admits two such searches without stalling. `GmailRead` and `GmailThread` are
at most three: the read, one narrower read when the first is too large, and
label names. `GmailDraft` is one call, two for a reply (the original's
headers, then the draft); `GmailModifyLabels` is one call whatever the
number of messages. Each Calendar read is one call; `CalendarCreate` and
`CalendarUpdate` are one, `CalendarDelete` two (the read, then the delete).
Callers over the limit are
delayed, never refused. Google's own quota, 250 units per user per second
with a list or a get costing 5, is the real bound on a user.

**`config`.** The map is flat:

- `provider` — the `oauth_provider` name.
- `gmail_host` — the `host[:port]` Gmail is called on. It must be one of the
  provider's `api_hosts` and admitted by the clauses.
- `calendar_host` — the `host[:port]` Calendar is called on, under the same
  two conditions.

A missing key, an empty one, or a host containing `/` fails every activation
with the key named.

**The user's connection.** Each user connects their account with the
`gmail_readonly`, `gmail_compose`, `gmail_organize`, `calendar_readonly` and
`calendar_events` scope sets (section 7, step 1). Each tool needs one:

- `gmail_readonly` — `GmailLabels`, `GmailSearch`, `GmailRead` and
  `GmailThread`, and the read of the original that a reply draft makes
  first.
- `gmail_compose` — `GmailDraft`.
- `gmail_organize` — `GmailModifyLabels`.
- `calendar_readonly` — the four Calendar read tools.
- `calendar_events` — `CalendarCreate`, `CalendarUpdate` and
  `CalendarDelete`, including the read `CalendarDelete` makes first.

On Gmail the tools write drafts and labels and nothing else: no send, no
delete, no trash. Neither the scopes nor the clauses alone make that true:

- **Sending.** `gmail.modify` and `gmail.compose` can send at the API level.
  What keeps mail from being sent is the component's exact clauses: the
  component never calls `drafts/send` or `messages/send`, and no clause in
  the block above admits either. `gmail.send` goes in no scope set.
- **Trashing.** `gmail.modify` can trash at the API level, and the
  `batchModify` clause admits any body, including one that adds the `TRASH`
  or `SPAM` label — which moves mail to a folder Gmail empties after 30
  days. What keeps mail from being trashed is the tool itself:
  `GmailModifyLabels` refuses `TRASH` and `SPAM` in `add` before any call.
  It may remove them, which restores mail.

On Calendar the tools create, change and delete events, and can email
guests: an invitation, an update or a cancellation is sent only when the
assistant passes `notify` as `all` or `externalOnly`; by default nobody is
mailed. `calendar.events` can do more at the API level, and the
`PATCH` and `DELETE` clauses admit any body or query. What keeps the tools
from more: the exact clauses — no clause admits `events/*/move`,
`calendars/*/clear` or `events/*/instances` — and the tools themselves:
`CalendarDelete` refuses a recurring series unless told `series: true`,
`CalendarUpdate` changes only the fields named and clears none, and no tool
creates or deletes a calendar.

What the tools answer when the connection is not in place:

- **No usable credential** — no account connected, its file gone, a refresh
  Google refused: the answer is `{"error": "credential_invalid", …}`, and the
  assistant runs `oauth-connect` and relays its steps.
- **Several accounts connected** — a call that does not name `account` is
  answered `denied`, listing them; the assistant asks again with one.
- **Connected without the scope a call needs** — Google answers 403, and the
  tool answers `denied` with Google's message and a sentence naming the
  scope the failing call needs and saying to reconnect with that scope set.
  The sentence names it by the set's word: "Gmail read" is `gmail_readonly`,
  "Gmail compose" is `gmail_compose`, "Gmail organize" is `gmail_organize`,
  "Calendar read" is `calendar_readonly`, "Calendar events" is
  `calendar_events`. A reply draft's read of the original names the Gmail
  read scope, its draft the Gmail compose scope; `CalendarDelete`'s read of
  the event names the Calendar events scope, as its delete does. A
  Calendar tool is answered the same way when the Google Calendar API is not
  enabled in the project (section 1).

**What an answer holds.** `GmailRead` answers the message's text, decoded,
up to `max_chars` characters (12 000 by default, 200 to 60 000), and lists
attachments without fetching them. A message too large to read whole —
over the read limit (`max_response_bytes`), or refused on the bus cap
(`max_body_bytes`) — is answered with its headers, its attachments and
Gmail's preview, `text` null and `text_omitted` saying why. Header values,
attachment names and MIME types are bounded in bytes and a cut header is
flagged; a long text is cut to the wrapper's result ceiling and says so; a
long thread is trimmed to its newest messages and `messages_omitted` says
how many were left out. A thread whose headers are too large to read whole
is read again without `to` and `cc`, and `recipients_omitted` says why; a
thread whose `from`, `subject` and `date` alone are too large is refused —
the `internal` answer names the provider's read limit or the bus cap — and a
smaller `max` does not help. A search page answers every message it could
read; one it could not — deleted since the listing, or its read cut or
refused — is named in `unavailable` with the reason.

The Calendar tools answer Google's times as Google gives them — `{date_time,
time_zone?}` for a timed event, `{date}` for an all-day one — and convert
nothing. `CalendarList` answers each calendar's `id`, name, access role and
time zone, 100 to a page. `CalendarEvents` answers one calendar's events in
a window, recurring events expanded, at most 50 to a page; each line's
`self_response` is "the answer to the invitation of the calendar read —
`needsAction`, `declined`, `tentative` or `accepted` — when that calendar is
an attendee: on `primary` the user's own, on another person's calendar that
person's; a declined event is on that calendar but is not a commitment".
`CalendarEvent` answers one event with its description (not HTML-decoded,
bounded by `max_chars` as `GmailRead`'s text is) and up to 50 attendees with
their response counts; past 50, Google returns one entry and the answer says
`attendees_omitted: true` instead of counts. Whose entry that is, and whose
`organizer.self` and `self_response` are: "The calendar read is the user on
`primary` and another person on that person's calendar: there
`organizer.self` and `self_response` are that person's, and the user's own
response is the user's entry in `attendees`." `CalendarFreeBusy` answers
the busy intervals of up to 50 calendars in a window, or Google's reasons it
could not answer for a calendar; a calendar with reasons is unknown, not
free. Text Google holds — summaries, locations, names, emails — is bounded
in bytes, and a cut field is listed in its object's `fields_truncated`. A
Calendar read too large to answer whole is not read again narrower:
`CalendarEvents` and `CalendarFreeBusy` are refused `internal` saying what to
narrow (fewer events, a shorter window, fewer calendars), and `CalendarList`
and `CalendarEvent` are refused naming the limit.

The two Gmail writes answer what Google did. `GmailDraft` answers the
draft's `draft_id`, `message_id` and `thread_id`; the draft is in the user's
Gmail Drafts, for the user to review and send from Gmail. A reply draft also
answers `in_reply_to`, the derived `subject` when the caller gave none, and
`in_thread: false` when Gmail filed the draft as a new conversation instead
of in the original's. `GmailModifyLabels` answers `accepted: true` with the
number of messages and the labels added and removed: Google applies the
change to every message named or to none. When a write's answer is lost —
cut, refused, or an error from Google that does not say the call was
refused — the tool is answered `internal` saying the write may have
happened: the draft may exist (look in Drafts before asking again), or the
labels may already be changed (asking again is safe). The component never
retries a write.

The three Calendar writes answer what Google holds. `CalendarCreate` answers
the new event as `CalendarEvent` does, with its new `id`; `CalendarUpdate`
answers the event as Google holds it after the change, the same way. Times
are written as given: two `YYYY-MM-DD` dates make an all-day event (`end`
the day after the last day), two RFC 3339 times with an offset a timed one;
nothing is converted. An update changes only the fields named, `attendees`
replaces the whole list, and a given `start` and `end` replace the event's
time, which may move it between all-day and timed. `CalendarDelete` reads
the event first and answers `deleted: true` with its `id`, its `summary` and
`start` as read, and `series: true` when it was a recurring series; it
refuses a series without `series: true`, and an event already cancelled.
When a write's answer is lost, the tool is answered `internal` saying the
write may have happened: the event may have been created (look for it with
`CalendarEvents` in its window before asking again, or the user has two),
the change may already be applied (asking again with the same arguments is
safe), or the event may already be deleted (asking again is safe: a deleted
event is refused as cancelled). The component never retries a write.

## 6. Telling the assistant

The tools appear in the assistant's tool list with their descriptions, which
state each tool's arguments and answer. Add lines like these to its
instructions (its system prompt or the `CLAUDE.md` it reads):

```
You have Gmail tools: GmailLabels, GmailSearch, GmailRead, GmailThread,
GmailDraft and GmailModifyLabels.
You have Google Calendar tools: CalendarList, CalendarEvents, CalendarEvent,
CalendarFreeBusy, CalendarCreate, CalendarUpdate and CalendarDelete.
- Pass `account` only when the user has several Google accounts connected;
  the refusal names them.
- An answer {"error": "credential_invalid", ...} means the user has no usable
  Google connection: run oauth-connect for provider "google" with the
  gmail_readonly, gmail_compose, gmail_organize, calendar_readonly and
  calendar_events scope sets and relay its steps to the user.
- GmailThread lists a thread's messages without their text; read a message's
  text with GmailRead.
- GmailDraft creates a draft the user sends from Gmail and never sends; a
  reply draft's answer says `in_thread: false` when Gmail filed it as a new
  conversation instead of in the original's — the draft is in Drafts and is
  not threaded; tell the user and do not draft again. Giving `subject` the
  thread's own subject is what makes threading likely.
- GmailModifyLabels takes label ids from GmailLabels, and never adds `TRASH`
  or `SPAM`.
- Calendar times (`from`, `to`) are RFC 3339 with an offset, in the user's
  time zone: 2026-11-02T09:00:00-05:00. All-day events come back with
  `date`, timed ones with `date_time`; nothing is converted.
- CalendarCreate and CalendarUpdate take `start` and `end` by the same rule:
  both RFC 3339 with an offset, or both YYYY-MM-DD for an all-day event
  (`end` the day after the last day).
- `notify` on the Calendar writes defaults to nobody; pass `all` only when
  the user wants the guests emailed.
- `attendees` on CalendarUpdate is the whole new list; read the current one
  with CalendarEvent first.
- CalendarDelete deletes a recurring series only with `series: true`; ask
  the user before passing it.
- In CalendarEvents, `self_response` is "the answer to the invitation of the
  calendar read — `needsAction`, `declined`, `tentative` or `accepted` — when
  that calendar is an attendee: on `primary` the user's own, on another
  person's calendar that person's; a declined event is on that calendar but
  is not a commitment".
- In CalendarEvent, "The calendar read is the user on `primary` and another
  person on that person's calendar: there `organizer.self` and
  `self_response` are that person's, and the user's own response is the
  user's entry in `attendees`."
- For anything else (attachments, the profile, history, Calendar's colors
  and settings), see docs/providers/google.md and call api-request
  with provider "google". Brenn attaches the user's token; never send an
  Authorization header.
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
   accounts. Confirm that the Gmail and Calendar scopes appear among the
   granted scopes: `gmail.readonly`, `gmail.compose`, `gmail.labels`,
   `gmail.modify`, `calendar.readonly` and `calendar.events`. If
   they are missing, or the page said nothing was connected and a connect
   with `calendar_readonly` and `calendar_events` alone succeeds, Google
   refused restricted scopes to the unverified app; only calendar access will
   work. Then name `calendar_readonly` and `calendar_events` alone in the
   `oauth-connect` line of the section 6 text, so the assistant's connects
   stop asking for the refused scopes.
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
7. **Draft and label.** Ask the assistant to draft a reply to one message of
   that conversation, then to label the message with one of your own labels.
   Expect one `GmailDraft` call; the diagnostic log shows one `GET` line for
   `/gmail/v1/users/me/messages/<id>`, then one `POST` line for
   `/gmail/v1/users/me/drafts`, each status 200, and nothing else. The draft
   is in Gmail's Drafts. Its subject is the one the assistant gave, when it
   gave `subject` (section 6 tells it to give the thread's own); otherwise it
   is the original's with `Re:` before it, and the answer's `subject` names
   it. When the
   answer has no `in_thread` key, the draft shows in the original
   conversation in Gmail — that is the expected outcome; when the answer
   says `in_thread: false`, the draft is in Drafts as its own conversation,
   which is the tool reporting Google's choice and not a fault. Note which
   outcome your account gave: this step is the live check of whether Gmail
   files a reply draft in its conversation. Nothing is sent: delete the
   draft in Gmail when done. Then expect one `GmailModifyLabels` call; the
   log shows one `POST` line for `/gmail/v1/users/me/messages/batchModify`,
   status 204, and the label is on the message in Gmail. Remove it the same
   way, by asking.
8. **Calendars.** Ask the assistant to list your calendars. Expect one
   `CalendarList` call; the diagnostic log shows one `api-request` line for
   `www.googleapis.com` `/calendar/v3/users/me/calendarList`, status 200.
   Your own calendar is marked `primary: true`. This step is the live check
   of the Calendar shapes the tools read.
9. **This week.** Ask what is on your calendar this week. Expect one
   `CalendarEvents` call on `primary` whose `from` and `to` carry your
   offset; the log shows one line for `/calendar/v3/calendars/primary/events`.
   Recurring meetings appear as their occurrences, all-day events with
   `date`, and an invitation you declined with `self_response: "declined"`.
10. **One event.** Ask for the details of one of those events, on `primary`.
    Expect one `CalendarEvent` call answering its description, its attendees
    with `attendees_total` and `attendee_responses`, and, when you are a
    guest, `self_response` — on `primary`, your own response.
11. **Free/busy.** Ask when you are free on Tuesday. Expect one
    `CalendarFreeBusy` call; the log shows one `POST` line for
    `/calendar/v3/freeBusy`, status 200, and the answer lists your busy
    intervals for the day.
12. **Another calendar.** Ask for this week's events on a calendar other
    than your own from step 8 — a shared or subscribed one, such as a
    holidays calendar. Expect `CalendarEvents` with that calendar's `id` as
    `calendar`; an id with `#` appears in the log as `%23`.
13. **Create.** Ask the assistant to create an event tomorrow with one
    attendee — another of your own addresses — without saying whom to
    notify. Expect one `CalendarCreate` call; the log shows one `POST` line
    for `/calendar/v3/calendars/primary/events` with `sendUpdates=none`, the
    event is in Google Calendar, and no invitation mail arrives. This is the
    live check of `sendUpdates` and of the timed shape. Then ask for an
    all-day event by giving dates: the check of the `date` shape.
14. **Update.** Ask to move the timed event an hour later. Expect one
    `CalendarUpdate` call; the log shows one `PATCH` line, and the event is
    moved in Google Calendar, its description and attendees untouched. Then
    ask for the all-day event to be given a time: expect one `PATCH` line and
    the event timed in Google Calendar. This is the live check of the patch
    semantics the tool relies on: a time object sent with the other kind's
    key `null` clears that key. If Google refuses the `PATCH`, or saves the
    event holding both a date and a time (the tool then answers `internal`
    naming `events.patch`), record it beside this step.
15. **Delete.** Ask to delete the timed event. Expect one `CalendarDelete`
    call; the log shows one `GET` line, then one `DELETE` line, status 204,
    and the answer names its summary. Delete the all-day event the same way.
16. **A series.** Ask to delete a recurring meeting without saying "the
    whole series". Expect the refusal naming it as a series and no `DELETE`
    line in the log.
17. **No account.** A second Brenn user who has not connected asks the
    assistant for their mail. The assistant is answered `credential_invalid`
    and offers to connect, with a link.
18. **When a step fails**, the tool's error says whose answer it is.
    - A `denied` quoting Google's 403 is Google's refusal: the account was
      connected without the Gmail read scope (reconnect with
      `gmail_readonly`), or Google refuses restricted scopes to the
      unverified app (step 1). On `GmailDraft` it names the Gmail compose
      scope (reconnect with `gmail_compose`), or the Gmail read scope when it
      was the read of a reply's original; on `GmailModifyLabels` the Gmail
      organize scope (reconnect with `gmail_organize`). On a Calendar tool it
      is the account connected without the Calendar read scope (reconnect
      with `calendar_readonly`), or on `CalendarCreate`, `CalendarUpdate` or
      `CalendarDelete` without the Calendar events scope (reconnect with
      `calendar_events`), or the Google Calendar API not enabled in the
      project (section 1, step 2). On a Calendar write it can also be a
      calendar the user may only read; the hint then names a scope the
      account already has.
    - A `GmailDraft` or `GmailModifyLabels` answer `internal` saying the
      draft may have been created, or the labels may already have been
      changed, is a write whose answer was lost: Google may have acted. For
      a draft, look in Drafts before asking again, or the user has two; for
      labels, asking again with the same arguments is safe. Nothing needs
      changing.
    - A `CalendarCreate`, `CalendarUpdate` or `CalendarDelete` answer
      `internal` saying the event may have been created, the change may
      already have been applied, or the event may already be deleted, is a
      write whose answer was lost: Google may have acted. For a create, look
      for the event with `CalendarEvents` before asking again, or the user
      has two; for an update or a delete, asking again with the same
      arguments is safe. Nothing needs changing.
    - A `CalendarDelete` answer `invalid_args` naming the event as a
      recurring series, or as already cancelled, is the tool's own refusal:
      a series is deleted only with `series: true`, and a cancelled event has
      nothing left to delete.
    - A `GmailModifyLabels` answer `invalid_args` saying `add` may not name
      `TRASH` or `SPAM` is the tool's own refusal: these tools never move
      mail to Trash or Spam.
    - A Calendar tool's `invalid_args` quoting Google's 400 is usually a
      `from` or `to` without an offset, or a `to` not after `from`; a 404 is
      an event or calendar id Google does not know — most often an event read
      without the `calendar` it is on. On `CalendarCreate` and
      `CalendarUpdate`, a `start`/`end` of different kinds, one without the
      other, `time_zone` without them, or a recurring timed event without
      `time_zone`, is the tool's own refusal; Google's 400 is usually an
      inverted window, a time zone it does not know, or a malformed date or
      time.
    - An `internal` error whose `detail` starts "api-request could not be
      issued" is Brenn's refusal before any call. It is almost always a clause
      that does not admit the method, host or path; the detail names it.
    - An `internal` error whose `detail` says "result of … bytes exceeds
      [messaging] max_body_bytes", on ordinary mail, is the bus cap at its
      default: the `messaging` block of section 3 was not applied, and it
      takes a restart. Under the recommended pair it is a message whose
      sender-written headers are oversized; that message is not readable
      through these tools, and neither is a message that carries the same
      headers (a reply, whose subject is copied from it); there is nothing to
      raise and nothing to fix. The same detail appears without failing the
      request where the tools have somewhere to put it: a search page names
      such a message in `unavailable` and answers the rest, a thread is read
      again without `to` and `cc` (`recipients_omitted` quotes it), and a
      `GmailRead` answer without its body quotes it in `text_omitted`.
    - A reason that says "the host did not deliver api-request's answer" and
      names neither `max_response_bytes` nor `max_body_bytes` — in
      `unavailable`, `text_omitted`, `recipients_omitted`, or a `GmailThread`,
      `CalendarEvents`, `CalendarEvent` or `CalendarFreeBusy` `internal` — is not about those limits: it quotes
      the error `api-request` answered, most often that it could not reach
      Google or could not refresh the access token. Those are transient; a
      retry with the same arguments reads the message, thread, window or
      event whole, and there is nothing to raise. The
      bullets below are about the two limits.
    - A `CalendarEvents` or `CalendarFreeBusy` answer `internal` naming
      `max_response_bytes` or `max_body_bytes` and saying "ask for fewer
      events" or "a shorter `from`/`to` window" is a window too large to read
      whole; the assistant narrows it and asks again, and nothing needs
      changing. On ordinary windows it is the `messaging` block not applied,
      as above. A `CalendarEvent` or `CalendarList` answer `internal` naming
      either limit is the same cause without a narrowing argument; under the
      recommended pair it is rare, and there is nothing to narrow.
    - A `GmailRead` answer with `text_omitted` on an ordinary message means
      `max_response_bytes` was lowered (section 3), or, when it quotes
      `max_body_bytes`, that the `messaging` block was not applied.
    - A `GmailThread` answer with `recipients_omitted` naming
      `max_response_bytes` or `max_body_bytes` is a thread whose headers were
      too large to read whole; it was read again without `to` and `cc`, and
      needs nothing. A `GmailThread` answer `internal` saying Google's answer
      "exceeds api-request's read limit for this provider
      (max_response_bytes)", or that the host did not deliver it because it
      exceeds `max_body_bytes`, is a thread whose `from`, `subject` and
      `date` alone are too large to read. A smaller `max` does not help. Two
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
19. **No token in the container.** In the agent's container home, `grep -r`
    for the start of the access token (`ya29.`) and for the refresh token
    (`1//`). Nothing should match.
20. **Vault modes.** `ls -la` of the vault shows `0700` directories and
    `0600` files, owned by the server's user.
21. **Disconnect.** Ask the assistant to disconnect one account. Then confirm
    that Brenn no longer appears at `myaccount.google.com/permissions` for
    that account, and that its file is gone from the vault.

## 8. The raw `api-request` route

For Gmail endpoints the component does not wrap, and for Calendar's colors
and settings, the assistant can call Google itself through `api-request`.
`docs/providers/google.md` lists the endpoints with an example call each:
the Gmail profile, attachments, a message in another form, history, thread
listings and draft listings; Calendar's colors and settings; and Calendar's
events in a window for a caller without a user, as the automation below is.
For the assistant, the calendar list, events, one event and free/busy,
creating, changing and deleting events, draft creation and label changes are
the component's tools (section 5). Each host it names must be in the provider's `api_hosts`
(section 3) and admitted by a clause of the agent's grant.

The assistant, reading only, beside its other grants:

```
tool api-request { allow { provider = "google"; method = "GET"; } }
```

Creating drafts and changing labels are `GmailDraft` and
`GmailModifyLabels` (section 5); the assistant needs no raw clause for
either. A raw write clause is for an endpoint the tools do not cover, names
the exact method, host and path, and is the operator's decision.

A raw clause bounds which endpoint the assistant calls, not what the call
does: a raw `POST`, `PUT` or `DELETE` clause on `gmail.googleapis.com`
admits any body, and the call can do whatever the user's connected scopes
allow there. With `gmail.compose` and `gmail.modify` connected, that
includes sending, trashing and permanently deleting drafts. The tools'
promise — no send, no delete, no trash (section 5) — holds for the assistant
only while its raw grant on `gmail.googleapis.com` is `GET`. A raw write
clause there gives up that promise, whatever the tools refuse. For example:

- **Sending:** `drafts/send` or `messages/send`. The tools cannot send
  because no clause admits a send path. `gmail.send` goes in no scope set.
- **Trashing:** `messages/batchModify`, or a message's or a thread's
  `modify` or `trash`. `GmailModifyLabels` refuses `TRASH` and `SPAM` in
  `add`, but that refusal covers only the tool's own calls.
- **Deleting:** a `DELETE` on `drafts/*`, which deletes a draft outright,
  not to Trash.

A raw write clause on `www.googleapis.com` likewise admits any body and gives
up the Calendar promises: a `POST` on `events/*/move` moves an event to
another calendar, a `POST` on `calendars/*/clear` deletes every event of a
calendar, and a write with `sendUpdates=all` mails every guest whatever the
user asked. The Calendar writes the assistant needs are `CalendarCreate`,
`CalendarUpdate` and `CalendarDelete`; it needs no raw clause for them.

A WASM automation with no conversation behind it, reading one user's calendar.
The component's tools serve only a caller with a user of its own (section 5),
so this is the raw route's: its clause lends it that user's credential. A
lent credential is read-only:

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
Gmail and Google Calendar endpoints the tools do not cover are available
through the `api-request` tool with provider "google". Brenn attaches the user's token; never send an
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
