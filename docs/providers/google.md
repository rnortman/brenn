# Gmail and Google Calendar endpoints through `api-request`

The `google-workspace-tools` component answers Gmail's labels, searches,
messages and threads as the tools `GmailLabels`, `GmailSearch`, `GmailRead`
and `GmailThread`. This document lists Gmail and Google Calendar endpoints
those tools do not cover, each with one `api-request` call that reaches it
through the `google` OAuth provider. The operator's setup is in
`docs/google-workspace.md`.

Every example below is the argument object of one `api-request` call. Brenn
attaches the calling user's bearer token; never send an `Authorization`
header. Query values are strings, or an array of strings for a key Google
takes more than once. A JSON body is passed as a string in `body`, and Brenn
sets `Content-Type: application/json` for it.

`account` names one of the user's connected Google accounts. Leave it out
when the user has one; when the user has several and it is left out, the call
is refused and the refusal lists them. An answer
`{"error": "credential_invalid", …}` means the user has no usable Google
credential: run `oauth-connect` for the provider and relay its steps.

A host an example names must be in the provider's `api_hosts`, and the
method, host and path must be admitted by a clause of the caller's
`api-request` grant. A call that is not is refused before anything is sent;
the operator adds the host or the clause.

Google's answer is returned as it is: the status, the headers and the body,
unread by Brenn. A 4xx answer's body carries Google's reason; a 403 naming
insufficient authentication scopes means the account was connected without
the scope the endpoint needs. Every example asks for `prettyPrint=false`,
which drops Google's indentation, and most carry a `fields` mask, which
drops fields the example does not use; both keep the answer under the
provider's read limit (`max_response_bytes`, 256 KiB by default), past which
the body is cut and `truncated` is `true`.

A list answer with `nextPageToken` has more: repeat the call with it as
`pageToken` and every other parameter unchanged.

Shapes follow Google's reference pages, which each section names. Google may
change them.

## Gmail

Every Gmail call is on `gmail.googleapis.com`, under `/gmail/v1/users/me`,
the account the token belongs to.

### The user's profile

The account's address, its message and thread totals, and the current
history id.
Reference: `https://developers.google.com/workspace/gmail/api/reference/rest/v1/users/getProfile`.

```json
{
  "provider": "google",
  "method": "GET",
  "host": "gmail.googleapis.com",
  "path": "/gmail/v1/users/me/profile",
  "query": {
    "prettyPrint": "false"
  }
}
```

The answer is `{ emailAddress, messagesTotal, threadsTotal, historyId }`.
`historyId` is where a later *History since a point* call starts.

### An attachment

One attachment's bytes. The message id is a `GmailSearch` or `GmailRead` id;
the attachment id is the `attachmentId` of the part, which
`GmailRead` does not answer. Read it from messages.get with `format=full`
(neither `minimal` nor `metadata` carries the parts) and a `fields` mask that
names the part tree without `data`, so a large text part does not push the
answer past the read limit:
`payload(filename,mimeType,body(size,attachmentId),parts(filename,mimeType,body(size,attachmentId),parts(filename,mimeType,body(size,attachmentId),parts(filename,mimeType,body(size,attachmentId)))))`.
Reference: `https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.messages.attachments/get`.

```json
{
  "provider": "google",
  "method": "GET",
  "host": "gmail.googleapis.com",
  "path": "/gmail/v1/users/me/messages/<message id>/attachments/<attachment id>",
  "query": {
    "prettyPrint": "false"
  }
}
```

The answer is `{ size, data }`: `data` is the attachment's bytes in base64url
(URL-safe alphabet), a third larger than the attachment. An attachment whose
encoded form passes the read limit — about 190 KB of attachment at the
256 KiB default — comes back cut, `truncated: true`, and cannot be read
whole through `api-request`. The clause that admits it names the path's two
ids as `*`: `/gmail/v1/users/me/messages/*/attachments/*`.

### A message in another form

`GmailRead` answers a message's headers, its text decoded and its
attachments listed. For the message as Google stores it, ask for
`format=raw` (the whole RFC 2822 message, base64url, in `raw`),
`format=full` (the part tree, each part's `data` included unless the mask
leaves it out) or `format=minimal` (ids, labels, snippet and sizes, no headers or body), with a
`fields` mask for the fields wanted.
Reference: `https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.messages/get`.

```json
{
  "provider": "google",
  "method": "GET",
  "host": "gmail.googleapis.com",
  "path": "/gmail/v1/users/me/messages/<message id>",
  "query": {
    "format": "minimal",
    "fields": "id,threadId,labelIds,snippet,sizeEstimate,historyId,internalDate",
    "prettyPrint": "false"
  }
}
```

`format` is `minimal`, `full`, `raw` or `metadata`; with `metadata`,
`metadataHeaders` takes an array of header names, one value each. A `raw`
message is the whole message, attachments included, a third larger than its
size: a large one is cut at the read limit.

### History since a point

What changed in the mailbox after a history id: messages added or deleted,
labels added or removed.
Reference: `https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.history/list`.

```json
{
  "provider": "google",
  "method": "GET",
  "host": "gmail.googleapis.com",
  "path": "/gmail/v1/users/me/history",
  "query": {
    "startHistoryId": "<history id>",
    "historyTypes": ["messageAdded", "labelAdded"],
    "maxResults": "100",
    "prettyPrint": "false"
  }
}
```

`startHistoryId` is required: a profile's or a message's `historyId`.
`historyTypes` is optional, any of `messageAdded`, `messageDeleted`,
`labelAdded` and `labelRemoved`. The answer is
`{ history[], historyId, nextPageToken? }`, each record with the ids of the
messages it touched. A start too old for Google to keep (about a week) is
answered 404; start again from the profile's `historyId`.

### Threads matching a search

The threads, rather than the messages, a Gmail search matches. `GmailSearch`
lists messages, each with its `thread_id`; this lists each thread once.
Reference: `https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.threads/list`.

```json
{
  "provider": "google",
  "method": "GET",
  "host": "gmail.googleapis.com",
  "path": "/gmail/v1/users/me/threads",
  "query": {
    "q": "from:alice@example.com newer_than:30d",
    "maxResults": "20",
    "prettyPrint": "false"
  }
}
```

`q` takes Gmail's search syntax; `maxResults` is at most 500 (default 100).
The answer is `{ threads[], nextPageToken?, resultSizeEstimate }`, each thread
`{ id, snippet, historyId }`. A thread's `id` is what `GmailThread` takes.

### Drafts

The user's drafts, then one of them. Reading drafts needs only the Gmail read
scope; creating or sending one is a write and needs its own scope and clause.
Reference: `https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.drafts/list`.

```json
{
  "provider": "google",
  "method": "GET",
  "host": "gmail.googleapis.com",
  "path": "/gmail/v1/users/me/drafts",
  "query": {
    "maxResults": "20",
    "prettyPrint": "false"
  }
}
```

The answer is `{ drafts[], nextPageToken?, resultSizeEstimate }`, each draft
`{ id, message: { id, threadId } }`. The draft's message id is also a message
id `GmailRead` reads. One draft by its `id`:
Reference: `https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.drafts/get`.

```json
{
  "provider": "google",
  "method": "GET",
  "host": "gmail.googleapis.com",
  "path": "/gmail/v1/users/me/drafts/<draft id>",
  "query": {
    "format": "metadata",
    "prettyPrint": "false"
  }
}
```

`format` is as for a message. The answer is `{ id, message }`.

### A label's counts

`GmailLabels { id }` answers one label with its counts. The same call made
directly, as an example of a `fields` mask:
Reference: `https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.labels/get`.

```json
{
  "provider": "google",
  "method": "GET",
  "host": "gmail.googleapis.com",
  "path": "/gmail/v1/users/me/labels/INBOX",
  "query": {
    "fields": "id,name,messagesTotal,messagesUnread,threadsTotal,threadsUnread",
    "prettyPrint": "false"
  }
}
```

A `fields` mask names the fields wanted, comma-separated; a nested field is
`parent/child`, and `parent(a,b)` keeps several under one parent. A mask that
names a field the answer does not have is answered 400.

## Google Calendar

Every Calendar call is on `www.googleapis.com`, under `/calendar/v3`. These
need the account connected with a Calendar scope (`calendar_readonly` in the
operator guide's block). A calendar id is `primary` for the user's own
calendar, or an `id` from the calendar list; ids contain `@` and sometimes
`#`, and go in the path as they are — Brenn encodes them.

### The calendar list

The calendars the user has in their list: their own, shared and subscribed.
Reference: `https://developers.google.com/workspace/calendar/api/v3/reference/calendarList/list`.

```json
{
  "provider": "google",
  "method": "GET",
  "host": "www.googleapis.com",
  "path": "/calendar/v3/users/me/calendarList",
  "query": {
    "fields": "items(id,summary,primary,accessRole,timeZone),nextPageToken",
    "prettyPrint": "false"
  }
}
```

The answer is `{ items[], nextPageToken? }`; `accessRole` is `owner`,
`writer`, `reader` or `freeBusyReader` (busy times only).

### Events in a window

A calendar's events between two times, recurring events expanded into their
occurrences, in start order.
Reference: `https://developers.google.com/workspace/calendar/api/v3/reference/events/list`.

```json
{
  "provider": "google",
  "method": "GET",
  "host": "www.googleapis.com",
  "path": "/calendar/v3/calendars/primary/events",
  "query": {
    "timeMin": "2026-11-02T00:00:00Z",
    "timeMax": "2026-11-09T00:00:00Z",
    "singleEvents": "true",
    "orderBy": "startTime",
    "maxResults": "50",
    "fields": "items(id,summary,start,end,location,status,attendees(email,responseStatus)),nextPageToken,timeZone",
    "prettyPrint": "false"
  }
}
```

`timeMin` and `timeMax` are RFC 3339 times with an offset; an event is listed
when it overlaps the window. `orderBy=startTime` needs `singleEvents=true`.
`q` searches the events' text; `maxResults` is at most 2 500 (default 250).
Each event's `start` and `end` are `{ dateTime, timeZone? }`, or
`{ date }` for an all-day event.

### One event

One event by its `id` from a listing.
Reference: `https://developers.google.com/workspace/calendar/api/v3/reference/events/get`.

```json
{
  "provider": "google",
  "method": "GET",
  "host": "www.googleapis.com",
  "path": "/calendar/v3/calendars/primary/events/<event id>",
  "query": {
    "prettyPrint": "false"
  }
}
```

The answer is the whole event: `summary`, `description`, `start`, `end`,
`location`, `attendees`, `organizer`, `recurrence` (for a recurring event's
master), `hangoutLink`, `conferenceData` and more. A `fields` mask keeps it
small.

### Free and busy

The busy intervals of one or more calendars in a window, without the events.
Reference: `https://developers.google.com/workspace/calendar/api/v3/reference/freebusy/query`.

```json
{
  "provider": "google",
  "method": "POST",
  "host": "www.googleapis.com",
  "path": "/calendar/v3/freeBusy",
  "query": {
    "prettyPrint": "false"
  },
  "body": "{\"timeMin\": \"2026-11-02T08:00:00Z\", \"timeMax\": \"2026-11-02T18:00:00Z\", \"items\": [{\"id\": \"primary\"}, {\"id\": \"alice@example.com\"}]}"
}
```

This is a `POST` although it changes nothing, so it is admitted only for a
call that carries a user — a conversation, or a component serving a
conversation's request — and only by a clause naming `POST` and this path; a
lent credential is read-only. The answer is
`{ timeMin, timeMax, calendars: { <id>: { busy[]: { start, end }, errors? } } }`;
a calendar the user cannot see answers `errors` instead of `busy`.

### Colors and settings

The palette an event's or a calendar's `colorId` indexes, then the user's
Calendar settings (time zone, week start, default event length).
Reference: `https://developers.google.com/workspace/calendar/api/v3/reference/colors/get`.

```json
{
  "provider": "google",
  "method": "GET",
  "host": "www.googleapis.com",
  "path": "/calendar/v3/colors",
  "query": {
    "prettyPrint": "false"
  }
}
```

The answer is `{ calendar: { <colorId>: { background, foreground } },
event: { … }, updated }`.
Reference: `https://developers.google.com/workspace/calendar/api/v3/reference/settings/list`.

```json
{
  "provider": "google",
  "method": "GET",
  "host": "www.googleapis.com",
  "path": "/calendar/v3/users/me/settings",
  "query": {
    "prettyPrint": "false"
  }
}
```

The answer is `{ items[], nextPageToken? }`, each `{ id, value }`: `timezone`
is the user's time zone, `weekStart` `0` for Sunday, `defaultEventLength`
minutes.
