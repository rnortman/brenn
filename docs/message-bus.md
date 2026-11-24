# The Channel Model

A pub/sub substrate for graphs of heterogeneous agents — coded algorithms, LLM
decision-makers, human interfaces — connected asynchronously.

---

## 1. The philosophical core

Most messaging systems are built around a **transfer of custody**. A message is a
thing that belongs to someone; it is handed to a broker; the broker owes it to a
consumer; the debt is settled by an ack; the message then ceases to exist. Queue
depth is the size of an outstanding obligation, and everything else in the design
follows from wanting that obligation to be small and eventually zero.

This model rejects custody entirely. **A channel is not a conduit for messages in
transit. It is a rolling window over a time series that simply exists.** The last
N messages published to a channel are there. They are there whether anyone
subscribed, whether anyone read them, whether anyone ever will. Nothing consumes
them; reading does not remove them; there is no debt to settle and therefore
nothing to be owed.

The consequence worth stating plainly: **a channel cannot be pressured to stop
being.** There is no backpressure in this system, not as an omission but as a
structural fact. Pressure only propagates through a system where downstream
capacity constrains upstream acceptance, and that constraint only exists when
delivery is an obligation. Remove the obligation and there is nothing for
pressure to act on.

This is the right call for this domain, because the producers are mostly
exogenous. The dishwasher finishes. Email arrives. A human speaks into a
microphone. A calendar event begins. None of these can be told to wait, and a
system that appears to tell them to wait is only relocating the queue while
converting bounded loss into unbounded latency. The honest design accepts the
world's rate and bounds its own memory of it.

The other thing to note is that resources are always finite, even if you spend
a lot of money. There's no such thing as an unbounded queue; the framework can
pretend to offer that, but it's a lie. All queues are bounded. You always drop
eventually. A backpressure system drops at ingess (deny requests). A system
that pretends queues are unbounded gets OOM-killed or runs out of disk space.
This system generally chooses to drop-oldest and increment a drop counter so
the loss is visible.

## 2. Mechanics

### 2.1 Channels

A channel has one parameter: **depth N**, the number of most-recent messages
retained. Publishing appends; when the window is full the oldest is evicted.

The window is defined by *count*, but should be *reasoned about* as duration.
N is meaningful only against arrival rate:

| Channel | Rate | N | Horizon |
|---|---|---|---|
| `power.dishwasher.finished` | ~1/day | 100 | ~3 months |
| `mail.arrived` | ~200/day | 100 | ~12 hours |
| `sensor.temperature.kitchen` | 1/min | 10 | 10 minutes |

Same N, wildly different guarantees. The design-time obligation is not "pick a
number" but "pick the outage you intend to survive, then multiply." A channel at
one message per decade with N=10 holds a century of history, and this is not a
degenerate case — it is the model working as intended.

A durable channel states a third number, **`standing_retain_depth`** — the
reaper's disk frontier. It is what the channel keeps for readers that do not
exist yet: a non-subscriber pull, a subscription that has not been written, a
subscriber that comes into existence later. It is therefore the **ceiling on
every depth stated about the channel**. The channel's own `push_depth` and
`retain_depth` rungs, every static subscriber's depths, and every runtime
dynamic subscribe are all held at or below it; a config block that exceeds it
fails boot naming the channel, and a dynamic subscribe that exceeds it is
refused with a typed error. A depth above the standing buffer would be a
promise the disk does not keep — the reaper is free to evict anything past the
frontier — and letting the union of subscribers raise the frontier instead
would hide the effective retention from the person who wrote the number. One
number, in one place, is the whole retention story for a channel. A subscriber
that needs a deeper window means raising the channel's standing depth,
deliberately, in the block that records the sizing decision.

A non-durable channel has no third number: its retained window *is* its
standing buffer, so `retain_depth` is both the window and the ceiling. Page-realm
`local:` bindings have no server-side entry at all; their queue is browser memory
the kernel bounds with its own contract-fixed rings (§2.6).

Channels are **ephemeral** (in-memory) or **durable** (disk-backed). This is an
orthogonal axis to depth, and it is a statement about *whether history survives a
restart*, not about delivery guarantees. Durable + long N is what a transactional
need looks like here. Ephemeral + short N is what light-bulb state looks like —
and note the reasoning is not "this data is unimportant" but "after a restart,
stale actuator state is worse than no state, so we would rather re-observe than
remember."

There is a second dimension, **transportability**, which has nothing to do with
any of that. This is simply whether ingress and egress are possible, whether
that channel extends from one system into a distributed system. `local:` channels
are not transportable; `brenn:` (durable) and `ephemeral:` channels are. There
is presently no durable and not-transportable channel type, but in principle
there could be.

A third, independent property is **`non_text`**: whether the channel may carry
non-text bodies (§2.1.2). The default is text-only. A declared channel states
it in its block (`non_text = true`); an auto channel takes it from the ports
bound to it (§2.6); a surface's page-local channel takes it from the surface
bindings on its address, and is `non_text` when any of them says so (§2.6);
a system-minted channel is text-only unless it is a `webhook:` or `mqtt:`
ingress channel whose tuning block states `non_text = true`, which selects
the raw ingress shape (§2.7.1). It is a static
fact about the channel, read from configuration at every boot and never
persisted, and it does not look at subscribers: a non-text publish to a
text-only channel is refused at the publishing door, and a reader that can
only represent text is refused when it is *bound* to a `non_text` channel, not
when a message arrives. Text is allowed on a `non_text` channel; the
declaration widens what the channel may carry and narrows nothing.

A durable channel that holds a non-text message, visible or parked, cannot be
redeclared text-only: boot and reload refuse it, naming the channel. The way
out is to keep the declaration, or to give the channel a new address;
retention removes the messages in time on a bounded channel, so on an
unbounded durable channel the declaration is in practice permanent. A reload
also refuses to redeclare a durable channel text-only while any `remote`'s
publish ACL covers it, under its old address or its new one, whatever protocol
version the remote attaches at; a
restart applies it. A ring is rebuilt empty when its declaration changes, as
for any other attribute.

### 2.1.1 Which schemes each placement may bind

The two component placements — a top-level WASM consumer on the backend, and a
page-placed instance on a surface — bind the same three pub/sub schemes. A
component's specification says nothing about schemes, and both hosts honour that:

| Scheme | Backend consumer | Surface instance |
|---|---|---|
| `brenn:` | subscribe + publish, durable store | subscribe + publish, over the wire — the server is the authority |
| `ephemeral:` | subscribe + publish, server-side ring | subscribe + publish, over the wire onto that same ring |
| `local:` | subscribe + publish, server-side ring in the **server** realm | subscribe + publish, the page's own store in the **page** realm |
| `webhook:`, `mqtt:` | subscribe only — they are ingress | not bindable: a page reaches no broker and no endpoint |
| `pwa_push:` | an egress adapter, not a channel | not bindable, same reason |

`local:` is two disjoint namespaces sharing one scheme, and that is deliberate:
a confined channel's contents never leave the host that holds them, so the
server's `local:foo` and a page's `local:foo` are different channels (§2.6).

The table's source is `bindable_schemes` in `brenn-envelope`, which every
config front end and every boot builder reads — a diagnostic from one and a
panic from the other, one table. The three places it deviates from the raw
capability families, and why, are documented on that function. Agents are a
separate row and a narrower one: an agent may not bind `local:` at all, which is
a trust decision about LLM principals rather than a property of the scheme.

That the *hosting* is the same per scheme and not merely the admissibility is
what the host-conformance suite pins: it runs one scenario body over each scheme
a host binds, with identical assertions.

Removing a declared `[[channel]]` block — deleting it, renaming it, or
commenting it out to debug — does not retire the channel. Its history stays, and
any dynamic subscriptions on it lie dormant: not delivered to, not deleted, and
folded back in on the first boot where a block with the same uuid declares the
channel again. Deleting the channel's row from the database is how an operator
retires both for good; the next boot then prunes the subscriptions as drift.

The uuid is the identity in all of this, not the address. A block restored under
a *different* uuid declares a different channel, and the dormant rows go on
waiting for the old one; if that new block also reuses the old address, the boot
refuses it rather than leaving a channel with no row of its own. Deleting the old
row first is the way to start fresh at an address.

### 2.1.2 What a message body is

A message body is **bytes with an optional content type** (`MessageBody` in
`brenn-envelope`). The channel holds them byte-exact; nothing on the bus
decodes, re-encodes or guesses.

- **No content type** means UTF-8 text of unstated type. This is what every
  body was before bodies became bytes, and what every string-taking publish
  door still produces.
- **The text rule** (`is_text_content_type`). A body is *text* when its content
  type is absent, or when its type is `text`, its type/subtype is
  `application/json`, or its subtype ends in `+json` — and it carries no
  `charset` parameter other than `utf-8` or `us-ascii`. Everything else is a
  *non-text* body. `text/plain; charset=iso-8859-1` is therefore non-text, and
  so is `application/octet-stream` holding bytes that happen to be valid UTF-8:
  the kind follows the content type, never the bytes. A text body must be valid
  UTF-8 or it cannot be constructed.
- **The content type** is a media type (`type/subtype` plus `;`-separated
  parameters, ASCII, at most 255 bytes). It is stored and returned verbatim
  with the body, and it does not count toward the size cap.
- **The size cap** (`max_body_bytes`) counts raw bytes, for text and non-text
  bodies alike. On a raw ingress channel (§2.7.1) the transport's payload is
  the body and obeys the cap: an MQTT payload over it is dropped for that
  channel and logged, and a raw webhook endpoint's `transport_ceiling_bytes`
  may not exceed it. On a wrapped ingress channel the wrapper is the body and
  the transport's own limit bounds it, as before.
- **Search** (`search` on a query, FTS on a durable channel, substring match on
  a ring) covers text bodies only. A non-text body never matches a search and
  is returned by a query without one like any other message.
- **The rule is recorded at write time.** A durable channel stores a text body
  as a TEXT value and a non-text body as a BLOB value in the same column, and
  the full-text index covers the TEXT rows. Widening the text rule later
  therefore ships with a migration of the rows it reclassifies.

Who is shown the content type today:

| Reader | Typed text body | Non-text body | `transport` (raw ingress) |
|---|---|---|---|
| Query API (`Messenger::query`), host code | text and type | bytes and type | `MessageEnvelope.transport` |
| A conversation (pushed batches, `MessageChannelGet`, `BrennPendingList`, `BrennMessageEdit`) | `"body": "…"`, then `"content_type": "…"` | `"body": {"binary": true, "content_type": "…", "size": N}` — a placeholder, never the bytes | after `envelope_type` |
| The eviction archive | `"body"`, then `"content_type"` | `"body_base64"` (standard padded base64), then `"content_type"` | after `envelope_type` |
| 0.2.0 components (`bus.envelope`, `bus.deferred-entry`) | text and type, on the backend and on a page, whether the row arrived over the page's wire or stayed on the page | bytes and type on the backend; on a page, at rest on any channel declared `non_text`, page-local or wire-bound (shown on the node rig; `e2e/tests/bytes.spec.ts` runs the publish, the read-back and the delivery in Chromium, and an SDK-built kind's binary state on a page-local `io` port), and in a sync call or reply | in the header, on the backend; no page binds an ingress channel |
| 0.1.0 components (`envelope-json`, `deferred-entry.payload`) | bare text, **no type** | never bound: refused at boot and at reload, on the backend and on a page | only on a channel narrowed back to text-only that still retains raw text messages (§2.7.1) |
| Attach v4 sessions (`Deliver`, `DeferredView`), served to remotes only | bare text, **no type** | never bound: a remote's subscribe is answered `Unavailable`; left out of a deferred view | never: no session binds an ingress channel |
| Attach v5 and v6 sessions (`Deliver`, `DeferredView`): a remote's, and every surface page's (v6) | text and type | bytes and type | never: no session binds an ingress channel |

The last column is the one envelope key a message a transport adaptor
published in the raw ingress shape (§2.7.1) carries beyond the others: an
object with one member named for the transport, `{"mqtt": {…}}` or
`{"webhook": {…}}`. Every other message has no `transport` key.

The two string carriers lose a typed text body's *type*, never its content.
They cannot carry a non-text body at all, so they are kept off every channel
declared `non_text` (§2.1) before any message flows — on a page as on the
backend, where a served 0.1.0 instance bound to a `non_text` channel,
page-local or wire-bound, is refused at boot and at reload — and a text-only
channel never holds a non-text body. On a page that applies a bindings document
withdrawing a channel's declaration, page-local or wire-bound, the kernel
empties that channel's store if it holds a non-text body, retained or parked,
before any reader sees it. Reaching a string carrier with one anyway is a
host bug and a panic.

One exception is cut rather than refused. A remote may hold a v4 and a v5
attach session at once, and both mirror the one parked set that belongs to the
remote. A non-text entry the v5 session parked is left out of the deferred view
sent to the v4 session (counted on its detach line, logged once per channel and
attribution). For that session an empty view means nothing *it can represent*
is standing; it cannot cancel or edit what it cannot see, and nothing it can
name is affected.

What can publish a typed or non-text body: a component built against the
byte-carrying contract `brenn:processor@0.2.0`, through its publish, deferred
publish, deferred edit and sync call, onto a channel declared `non_text` for a
non-text body and onto any channel for a typed text one. A non-text body
offered for a text-only channel is refused to that component
(`text-only-channel`) and nothing is stored. A remote attacher on attach
protocol v5, through `Publish` and `PublishBatch` (entries and deferred edits),
on the same terms: a non-text body offered for a text-only channel is answered
`TextOnlyChannel` — the whole batch, if any body in it is refused — and nothing
is stored. A 0.2.0 component hosted on a surface page, on the same terms, on
page-local and wire-bound channels alike: the page's wire is attach v6 and
carries every body with its type, and the page refuses a non-text body for a
text-only channel to the component before anything is sent. A transport
adaptor — an MQTT client or a webhook endpoint — onto its ingress channel when
that channel takes the raw shape (§2.7.1): the payload with the publisher's or
endpoint's content type, judged by the rule above, and dropped (MQTT) or
refused (webhook) when it fails it. A conversation, through `BrennSend` and
`BrennMessageEdit`'s `content_type`, publishes a typed *text* body only: a type
that is not text by the rule above is refused, and a conversation publishes
bytes as an attachment (§2.1.2a). What still cannot publish a typed body:
a 0.1.0 component, a remote's attach v4 session and every host door that takes
a string; every body they produce is untyped text.

### 2.1.2a Blobs: bytes a message names

A body is capped at `max_body_bytes` because it is copied into every
subscriber's window and held in rings. A large object — a photo, a recording,
a file — rides beside the body instead, as a **blob**: an immutable run of
bytes in the host's blob store, which a message names by reference. The store
is in `brenn-messaging-store/src/blob/`.

**A blob is a message's attachment, not a store of its own.** It has no name,
no listing and no lifetime of its own, and nothing looks one up except
through a message or a hold. The lifetime rule:

> A blob lives exactly as long as a message the host holds names it, or host
> code holds it open. When nothing does, it is deleted.

"Holds" means two kinds of holder, counted in two places:

- **An in-memory hold.** Host code holds a blob from sealing it or from
  redeeming a reference (`Messenger::retain_blob`). A ring channel's entry
  (`ephemeral:`, `local:`) holds every blob its message names, retained or
  parked, until the ring drops the entry — overflow, a depth-0 append, a park
  refused at the cap, a cancel, an edit's overwrite, the channel removed at
  reload. A copy of an envelope a reader was handed is **not** a holder.
- **A durable row.** A row of a `brenn:` channel that names a blob is counted
  in the database, in the transaction that inserts it, and uncounted by a
  trigger in the transaction of whatever statement deletes it — eviction,
  withdraw, cancel, chat teardown, an operator's `sqlite3`.

"Deleted" has two moments. A blob stops being **redeemable** the instant its
last holder lets go — for a durable row, when the deleting statement commits.
Its **bytes** are unlinked at once if no durable row ever named it (every blob
only ring messages named); otherwise by the next **reap**, which runs after
`Messenger::cancel`, after a durable cancel through
`cancel_deferred_if_present`, at the end of every hourly bus-GC pass, and at
boot. Until then the file stays on disk and charged to the budget, and
nothing can reach it. A removal site with no reap call after it only delays
the unlink to the next hourly pass; which blobs are deleted is decided by the
two counts, never by who called.

**Attachments.** `MessageEnvelope.attachments` is a map from name to
reference, empty on a message that has none, and the key is absent from
every serialization when it is empty.

- A **name** is 1 to 64 bytes, first `[A-Za-z0-9]`, then `[A-Za-z0-9._-]`.
  It is producer-chosen text a conversation is shown, so the grammar is
  narrow; a filename that does not fit belongs in the body, keyed by the
  name.
- At most **32 attachments** per message. Two names may name the same blob.
- A **reference** is `{"id": …, "size": N, "content_type": "…"}`. The id is
  32 lowercase hex characters minted from random bytes, not derived from
  content, and opaque: a reader must not parse it. There is no digest.

Which reader sees what:

| Reader | `attachments` |
|---|---|
| Host code (`Messenger::query`, the stores) | the references |
| 0.2.0 components (`bus.envelope` header), on the backend and on a page | the references, in full |
| Attach v5 sessions (`Deliver`), a remote's, replayed and live | the references, in full |
| Attach v6 sessions, a remote's and every page's | the references, in full, and the bytes by handle where the session has a blob door |
| 0.2.0 components on a page granted `blobs` | the references, and the bytes of an attachment the page fetched, by place, below |
| A conversation (pushed batches, `MessageChannelGet`, `BrennPendingList`, the intercept outcome) | each name with `size` and `content_type`; **no id**. It names an attachment by (message id, name), below |
| The eviction archive | the references, in full. The bytes are not archived: an archived message's attachments are not recoverable |
| 0.1.0 components (`envelope-json`), attach v4 sessions | nothing; the envelope is byte-identical to one without attachments |

**Who creates and redeems a blob.** A backend WASM component granted
`blobs` does, through the `brenn:processor/blobs@0.3.0` interface
(`brenn-wasm/wit/processor-0.3.wit`, `docs/component-packages.md`): it opens
an attachment of a message in this activation's windows by its place —
port, index, name — never by id, reads it by byte range, creates a blob by
positional writes and seals it, and publishes a message naming blobs it
opened or made, with `ports` granted as well. Forwarding an attachment is a
hold on the same blob: no byte is copied. Every blob the activation made and
no published message names is deleted when the activation ends, and an `Err`
or trap publishes nothing and deletes everything it made. A conversation
does too, by name, below. Host code's
`Messenger::publish_from_system_with_attachments` is a further publishing
entry; the tool substrate publishes a call's request and result through it
(below). The host tool `api-request` creates one on a caller's behalf: asked
with `response_attachment`, it streams a 2xx answer's body into a new blob
and answers it as an attachment of the result, and only for a caller that can
open it — a conversation, or a component granted `blobs`
(`docs/config-dsl.md`, "The OAuth tools"). Its `body_attachment` sends an
attachment the call carries as the request body. A remote attacher on attach v6 does, by handle, below. A component on a
page granted `blobs` reads attachments from bytes the page fetched, and makes
and forwards blobs, which the page uploads or opens on the backend after the
activation returns `ok` (below, "A surface page makes blobs"). v4/v5 remotes see references as metadata only. A remote's replay
store and a page's rings do not hold the blob, so a reference there can
outlive it. `ports.publish` and its siblings
carry no attachments: a component that republishes a message through them
sends the body alone.

**A conversation is a holder by name.** It is never shown a blob id. It
names an attachment as **(message id, attachment name)**, and a message's
body as (message id, body), and nothing it does with a blob puts the bytes
through the model's context: `BrennSend`'s `attachments` forwards an
attachment by reference (a hold on the same blob, no byte copied);
`MqttSend`'s `attachment` publishes an attachment's bytes to a broker
(§2.7.2); `brenn-tool save` writes an attachment's or a body's bytes to a file in the
conversation's environment; `brenn-tool send` attaches a file of the
conversation's own to a `BrennSend`, which seals a new blob the published
message names. The two commands are described in `docs/agent-tool-cli.md`
and need the conversation's shell tool. One function
admits a name, `Messenger::admit_conversation_name`, and every verb calls it.
The rule as built asks two things **of the one message named**:

- **The conversation was handed it.** The host keeps, per conversation, the
  messages it handed over — a push delivery, `MessageChannelGet`,
  `BrennPendingList`, an edit's answer, and the answer to the conversation's
  own send, by its id. The record is in memory on the `Messenger`: it
  survives a bridge being retired and re-spawned and is lost at a server
  restart. It holds at most 1,024 messages per conversation, oldest dropped
  first, and at most 256 conversations, the least recently fed dropped
  whole. It is cleared at a compaction boundary, at a spawn with no session
  to resume (conversation start), and at every user turn whose text as
  Claude Code receives it begins with `/`. Every drop and every clear costs a
  re-read (`not_read`) and never admits wrongly. Two resets of the
  conversation's context are not seen by the host — a resume Claude Code
  answers with a fresh session, and a slash command sent while a turn is
  running — and are tracked as `blob-handle-integrity` in `TODO.md`.
- **It is still retained on a channel the app may read**: the app's policy
  allows the channel and the directory resolves it, and the message is
  inside the **channel's** window — the whole ring on a ring channel; on a
  durable channel the newest `standing_retain_depth` retained rows, checked
  by rank so a row past that depth that the hourly reaper has not yet removed
  is refused. A parked message is not retained. The subscriber's own
  `retain_depth` plays no part. The app's own result inbox
  (`brenn:app-tool-results/<app-slug>`, §2.7) is readable for this purpose by
  that arm alone: no policy reaches it, and the record is the only way in.

**One exception: the tool substrate's channels.** On a channel of the four
tool namespaces (`brenn:tools/`, `brenn:tool-requests/`, `brenn:tool-results/`,
`brenn:app-tool-results/`) a conversation names an attachment only on its own
app's result inbox. An app whose policy covers `tools/`, `tool-requests/` or
`tool-results/` reads those channels' messages — their bodies, and each
attachment's name, size and content type — and naming one of their
attachments is refused `tool_channel_attachment`, logged at `warn`; naming
such a message's body works as on any channel. The reason: a tool call's
blobs are passed between the call's parties — the caller, the tool, and the
conversation the result is handed to — and a grant to watch the call traffic
was written when that traffic held no blobs. It would otherwise reach every
file any user's conversation or component hands a tool.

On a ring channel the check and the hold are one call under the ring's lock,
which hands out a hold on the one blob named, not on the message's other
attachments; on a durable channel the row is read and the blob held under
one database guard. A retained message naming a blob the store cannot hold is
a host bug and panics. Once a name is admitted the hold keeps the bytes for
the transfer whatever happens to the message.

The rule is per message, not per blob: an attachment of an evicted message is
refused even while another retained, readable message names the same blob,
and is admitted under that other message once the conversation has read it.
A rule asked of the blob would admit that case; it needs a lookup from a blob
to the retained messages naming it, which ring channels lack
(`conversation-blob-name-per-message` in `TODO.md`).

A file a conversation attaches is written through a writer that reserves the
file's whole declared size before any byte crosses, so the store's two keys
refuse it up front; if the publish is refused or the connection ends, the
writer and the sealed blob drop and nothing remains.

**A remote attacher is a holder by handle.** On attach v6 a remote's session
has a blob door when its profile grants one and the host has a blob store;
`Welcome.blobs` says so and states the bounds below. The session keeps a table
of **handles** the client mints, each an upload being written or a held blob,
and every blob frame names one:

- **Opening.** `BlobOpen` names an attachment as **(channel, message id,
  attachment name)**, never by blob id, and takes a hold on that one blob. The
  remote must be allowed to subscribe to the channel, asked afresh at every
  open; a channel outside that authority is a protocol violation. The message
  must be inside the **channel's** window, as for a conversation — the whole
  ring, or the newest `standing_retain_depth` retained rows of a durable
  channel; the remote's own subscribe depth and ceiling play no part — and the
  tool substrate's four namespaces are refused `ToolChannel`. Both rules are
  the conversation door's, from the one admission function both doors call,
  `Messenger::admit_retained_part`; the conversation's read record is not part
  of it. The answers otherwise are `NotRetained` and `NoSuchAttachment`.
  `BlobRead` then reads a byte range through the handle, at most one chunk
  (256 KiB) an answer, and keeps working after the message is evicted.
- **Uploading.** Only a remote with publish authority (`Welcome.blobs.upload`)
  may `BlobCreate`: the whole declared size is reserved against the store's
  two keys before a byte is sent, `BlobWrite` fills it by position and
  `BlobSeal` makes it a held blob.
- **Publishing.** `Publish` and each `PublishBatch` entry carry `attachments`,
  names mapped to held handles, onto a channel the remote may publish to; the
  message holds the blob as any message does, and a forward of an opened
  attachment copies no byte. A handle not held answers `AttachmentNotHeld` and
  publishes nothing.
- **The session is the holder.** `BlobClose` gives a handle up. When the
  session ends — however it ends — the table drops: unsealed uploads are
  deleted, holds are let go, and nothing a remote made survives a detach unless
  a message names it. The table drops before the session stops counting
  against `max_sessions`.
- **Bounds**, per session, each a typed answer: 64 handles (`HandleLimit`);
  bytes held, 4 × `max_blob_bytes`, the declared sizes of the session's uploads
  and the sizes of its held blobs, sealed uploads and opened attachments alike,
  counted per handle (`BytesHeldLimit` on a create and on an open); 4 read
  answers in flight (`TooManyReads`). An open hold keeps its blob charged after
  the naming message is evicted, which is why opens count. Across a remote's
  sessions the bounds multiply by `max_sessions`; nothing bounds a remote over
  time.

**A surface page is a reader by place.** A page speaks attach v6. Its session
has a blob door when an instance on its surface is granted `blobs`; the door
uploads (`Welcome.blobs.upload`) only when such an instance has a declared wire
output (see "A surface page makes blobs" below), and is read-only otherwise. The door is the remote's,
under the remote's admission: the server asks `Messenger::admit_retained_part`
at every open, and the channels a page may open on are those its surface's own
bindings name. An activation on a page is one synchronous stretch, so a
component's `read` answers from bytes the page already holds; the page fetches
them before the message reaches a component.

- **The fetch bound.** `fetch_bytes` on a surface component's `in` binding is
  the largest attachment, in bytes, the page fetches on that channel
  (`docs/config-dsl.md`, "Instances and bindings"). Unstated, it is 1 MiB on an
  instance granted `blobs` and 0 on any other; at most 64 MiB; legal only on a
  transportable channel and on an instance granted `blobs`. A channel's bound on
  a surface is the largest among its bindings there; an `io` port takes the
  default. A channel whose bound is above zero is **gated**; an attachment on it
  whose `size` is at most the bound is **eligible**. An eligible attachment
  larger than the session's bytes held (`Welcome.blobs.max_bytes_held`, 4 ×
  `max_blob_bytes`) is never opened — the case of an attachment stored under a
  larger `max_blob_bytes` than the one now configured — and its row does not
  wait for it.
- **The arrival hold.** On a gated channel a delivered row enters the page's
  store only once every eligible attachment it names has **settled** — fetched,
  refused, failed — or its deadline has passed. A row that names nothing
  eligible, arriving at an empty hold, is stored in the same pass, as on any
  other channel. Three rules move the hold after every event that can change
  it:
  1. **Start.** At most 4 held rows of a channel fetch at once; the oldest
     waiting row starts next, and its deadline runs from then.
  2. **Release.** While the oldest held row is settled or past its deadline, it
     enters the store as a delivered row does. Release is strictly in arrival
     order, so the store's order is the channel's. A row released by its
     deadline keeps its fetches running; bytes that land later are readable in a
     later activation.
  3. **Overflow.** The hold has room for the store's depth plus 4 rows. A row
     arriving at a full hold evicts the oldest **waiting** row, which is never
     stored and is counted as a drop for every reader of the channel, so each
     binding's `noise` sees it. A fetching row is never evicted; with no row
     waiting, the oldest row is released as it is instead.

  The hold adds no frame to the wire and no wait to the server or a publisher;
  nothing acknowledges and nothing is redelivered. It keeps its rows across a
  reconnect, and they are not delivered again, because a subscription's resume
  token advances when a `Deliver` is accepted.
- **A row's deadline.** 2 s, plus 1 s for each 256 KiB chunk of each eligible
  attachment — at least one per attachment, since each costs a read answer — at
  most 30 s, counted from when the row started fetching. The ceiling keeps the
  hold a property of the page and not of one binding's `fetch_bytes`. What it
  costs: a row whose eligible attachments come to more than 28 chunks — one
  attachment over 7 MiB, or 29 or more of any size — is handed over with its
  bytes only on a link that moves them in 30 s (64 MiB needs about 18 Mbit/s);
  on a slower link it is released at the ceiling, `read` answers
  `unavailable`, and the bytes land for a later activation.
- **The aggregate bound.** A row waits for the batches ahead of it as well as
  its own deadline. With `H` = store depth + 4, a row that is not dropped enters
  the store at most

  `⌈H / 4⌉ · (30 s + 15 s)`

  after it was delivered, the 15 s being the page's connect timeout, during
  which a deadline cannot fire; while the page stays attached the term is
  absent. The bound is a function of the store's depth alone. No fetch bound
  lowers it: a message may name 32 attachments, each at most any bound above
  zero is eligible, and 32 one-byte attachments already reach the 30 s
  ceiling.

  | Store depth | `H` | Batches | Attached | With a connect attempt at every deadline |
  |---|---|---|---|---|
  | 1 | 5 | 2 | 60 s | 90 s |
  | 4 | 8 | 2 | 60 s | 90 s |
  | 20 | 24 | 6 | 180 s | 270 s |
  | 100 | 104 | 26 | 780 s (13 min) | 1,170 s (19.5 min) |

  It grows with the depth, and the depth (`push_depth`, `retain_depth`) is the
  term an operator moves. A deep window on a gated channel is a long worst case.

  What a channel sees when every message on it carries a single attachment of
  at most its bound is smaller, and is **not a bound**: a publisher that
  attaches more moves the channel to the table above.

  | Single attachment of at most | Store depth | A row's deadline | Attached | With a connect attempt at every deadline |
  |---|---|---|---|---|
  | 256 KiB (one-chunk frames) | 1 | 3 s | 6 s | 36 s |
  | 1 MiB (the default bound) | 1 | 6 s | 12 s | 42 s |
  | 1 MiB | 20 | 6 s | 36 s | 126 s |

  Either figure is reached only when every deadline ahead of a row expires: a
  server that delivers and answers no blob frame, a page offline with rows
  held, or a link slower than one chunk a second throughout — an operating
  regime for a phone on a poor link, not only a fault. A fetch that answers
  releases its row at once.
- **Every reader of a gated channel waits.** The page holds one store per
  channel, so a binding of an instance without the grant, on a channel another
  instance on the page gated, sees the channel's rows as late as the gated
  instance does, and so does a row with no attachment behind one with. Any
  `fetch_bytes` above zero gates the channel. The remedy for a reader that must
  not wait on another instance's attachments is a channel no binding on that
  surface gates. A surface with no gated channel behaves as if the hold did not
  exist.
- **The page's cache.** Fetched bytes are held per (channel, message id,
  attachment name); the same blob named by two messages is fetched twice. Bytes
  being fetched, reserved at the attachment's full size, plus bytes fetched are
  at most 64 MiB across the page. When a fetch does not fit, the page drops the
  bytes of messages already in a store, oldest arrival first, and never a held
  row's; those attachments then read `unavailable` and are not fetched again.
  An entry that still does not fit is never fetched. An entry lives while its
  message is in the channel's hold or the page's store of the channel, and its
  bytes go, and any fetch of it is closed, when the message leaves that window.
  The server's handle is closed as soon as an attachment's bytes are on the
  page, so its handle and bytes-held bounds bound fetches in flight, not what
  the page caches.
- **What a component is answered.** A component opens an attachment by its
  place in its own activation's windows — port, index, name — and never by id.
  `open` answers `not-permitted` for a place that names no attachment there,
  and for an instance without the grant or a call outside its own innermost
  activation; `not-found` when the server answered that the message left the
  channel's window; `quota-exceeded` past 64 live handles; otherwise a handle,
  whether or not the page holds the bytes, since `open` reads nothing. `read`
  answers `too-large` over 1 MiB a call, `out-of-range` past the size, the bytes
  when the page holds them, and **`unavailable`** otherwise: an attachment over
  the channel's bound, one whose fetch was refused or failed, one whose bytes
  were dropped for room, or one still on its way. A sync activation — a peer's
  call, a mount, a gesture — reads the same way, from bytes the page holds at
  that instant. A component reads an attachment through any binding whose window
  holds its message, whatever that binding's own `fetch_bytes`: the cache is
  the page's. A handle lives only for its activation.
- **What the page reports.** A fetch the server refuses or that fails, an
  attachment over the bytes held, and a drop of fetched bytes for room are each
  reported on the page's error channel — the tool-channel refusal at `info`,
  every other kind at `warn` — **once per kind of trouble, per channel, per
  connection**: the first occurrence on a channel after the page attaches is
  reported, its text says later ones of that kind on that channel are not
  reported until the page reconnects, and a reconnect resets it. The component
  is told every time, through `read` answering `unavailable`. A server answer
  that `NotRetained` is not reported: the window moved. An answer the page did
  not ask for, a refusal that the attachment its own `Deliver` named exists, or
  an open whose size or content type differs from the reference ends the
  connection.
- **Reconnect.** Fetched bytes stay; fetches in flight end and their partial
  bytes are discarded; held rows stay held, in order, and their deadlines keep
  running, so a row whose deadline passes while the page is offline is released
  without its bytes. At the next attach every unfetched entry of a held row or
  a stored message is fetched again; one of a message already handed over wakes
  nobody, and its bytes are there for the next activation that reads them.

**A surface page makes blobs.** A component on a page granted `blobs` creates,
writes and seals blobs, publishes messages that name them, and forwards
attachments it opened, through the same interface as on the backend. The page
is not the store, and one activation is one synchronous stretch, so a page
holds two kinds of blob, which a component never tells apart:

- **page bytes** — a blob the page made, its bytes in page memory and nowhere
  else;
- **a backend reference** — a blob in the backend store, known by the place it
  was delivered (wire channel, message id, attachment name); the page may or
  may not hold a fetched copy.

Nothing leaves the page for a blob before its activation returns `ok`. On `err`
or a trap the activation's publishes are dropped and every buffer it made with
them; the server never saw any of it.

| the blob | published on a wire channel | published on a page-local channel |
|---|---|---|
| made in this activation | uploaded | the message holds the page's bytes |
| opened from a wire message | forwarded by reference | the message carries the reference |
| opened from a page-local message, page bytes | uploaded from the page's bytes | shared, no copy |
| opened from a page-local message, a reference | forwarded by reference | the reference, shared |

No pairing is refused for where its blob came from, and none drops the
attachment from its message.

- **Page-local attachments.** A page-local (`local:`) message's attachments live
  in page memory. The bytes a page made live while a retained or parked message
  of a page-local channel names them, and are freed when none does — this
  section's lifetime rule with the page as the host. A `defer-edit` keeps them; a
  `defer-cancel` releases them. Nothing is uploaded and nothing is persisted; a
  reload loses them with the channel. A page-local attachment is a way to hold
  bytes above the body cap on a page, not a store for component state: state is
  a retained channel's body. A backend reference named in a page-local message
  stays a reference: a reader opens it, and `read` answers the bytes while the
  page still holds them for the wire message that delivered them and
  `unavailable` otherwise; published onward to a wire channel it is forwarded
  by reference. The page copies nothing on its own: a component that needs the
  bytes to outlive the wire message reads them and makes a blob.
- **The upload.** A wire flush that names an attachment is held by the page,
  per instance in completion order, and every later flush of that instance waits
  behind it. Once the page is attached under a document that still lets the
  instance attach on that channel (the instance is granted `blobs` and the
  channel is one of its wire outputs) and still subscribes the channel each
  forwarded reference was delivered on, and the instance has nothing else in
  flight, the page creates and writes each blob it made, opens each reference it
  forwards, waits for every handle to be answered held, and only then sends the
  batch naming them. The server's per-session bounds (64 handles, 4 ×
  `max_blob_bytes` held) cover uploads and the page's own fetches together;
  while an upload waits for room no new fetch starts.
- **The verdict rule.** What the page can judge it answers in the call: the
  page's own bounds below, a door without `upload` (`not-permitted` on a wire
  port), the server's largest blob, handles and bytes held as the last
  `Welcome` advertised them. What only the server knows arrives after `ok`: a
  refused create (`BudgetExhausted`, `StorageFailed`, a limit), a refused open
  (the forwarded message left the server's window), a flush that no longer fits
  the door or the document of a later attachment. Each **loses the flush
  whole** — every entry of the activation's wire batch, never the message
  without its attachment — frees its bytes, and is announced: a toast, a
  console line, and a `warn` report with the instance as subject on the
  surface's error channel when the page is attached, the first per instance and
  kind of loss reported while attached at `warn` and later repeats at `debug`
  (a loss while detached is at `warn` on the console and demotes nothing).
  The page stays up and the
  instance's next flush proceeds. The same activation's page-local publishes
  were committed at the flush and stay. On a surface `budget-exhausted` from
  `create` or `write` is the page's own budget; the backend store's budget is
  never answered in a call.
- **Detach.** A flush waiting or being uploaded at a detach stays on the page,
  within its bounds, and is uploaded again from nothing after the next attach,
  if the new document still lets it. A batch already sent and unanswered at a
  detach may have been applied, so it is never sent again: it is announced lost
  by toast and console at once, and the count of such flushes is reported on
  the error channel after the next attach.
- **The page's bounds**, each a typed answer:

  | Bound | Value | Refusal |
  |---|---|---|
  | Largest write | 1 MiB | `write`: `too-large` |
  | Largest blob | the smaller of 64 MiB and the door's `max_blob_bytes` | `write`: `blob-too-large` |
  | Bytes one activation writes | 64 MiB | `write`: `quota-exceeded` |
  | Page-made bytes held at once | 64 MiB | `create`, `write`: `budget-exhausted` |
  | Handles one activation holds, writers and blobs | 64 | `create`, `open`: `quota-exceeded` |
  | Written ranges of one writer | 1,024 | `write`: `too-fragmented` |
  | Distinct blobs one activation's wire publishes name | 32, and no more than the door's `max_handles` | `publish`: `plain(quota-exceeded)` |
  | Bytes of those blobs together | the door's `max_bytes_held` | `publish`: `plain(quota-exceeded)` |

  Made and fetched bytes have separate bounds: fetched bytes are a cache the
  page may drop for room, since the server still has them, while made bytes are
  the only copy and nothing drops them for room. A page holds at most 128 MiB of
  attachment bytes.

**Bounds.** Three keys in the `messaging` section (`docs/config-dsl.md`):
`max_blob_bytes` caps one blob's extent (default 64 MiB), `blob_budget_bytes`
caps the store's charged total (default 1 GiB), `blob_dir` is where the files
live (default the database path with `.blobs` appended). Raising
`max_blob_bytes` does not let a component make a larger blob: apart from that
key, the host has a fixed per-activation write bound — at most 64 MiB of blob
writes in one activation, summed over every `write` — a writer does not
outlive its activation, and `seal` needs every byte of the blob's extent
written. So no component can create a blob larger than that bound, whatever
`max_blob_bytes` is; a higher value admits larger blobs from host code only,
and no production host code makes one. A conversation's bound does scale with
the key: one `brenn-tool send` may attach files whose sizes together are at
most `max_blob_bytes`, each at most `max_blob_bytes`, and a conversation has
at most eight socket connections open at once, so at most
`8 × max_blob_bytes` charged and unsealed at a time; nothing bounds it over
time. The per-activation bounds are in
`docs/security-posture.md` §7.6. A write over either
store key is refused to the producer; nothing is evicted to make room and nothing
waits. A blob is charged from its first write until its file is unlinked. No
per-channel setting exists: a reference is inert to a reader that does not
know it.

**Rows nothing evicts.** Rows of a durable channel that left the
configuration are kept and never evicted, and so are the blobs they name.

**Backup and restore.** The database and the blob directory are **one unit**
to back up, restore and roll back. At boot the store reconciles the two and
refuses to start — naming the blob and the messages — when a message names a
blob with no reference record, when a referenced blob's file is missing or
has the wrong length, when a blob's count disagrees with its references,
when the directory holds an entry that is not a blob file or its owner
record, when another
open blob store — another host's, or a second host over the same database —
holds the directory's lock, or when SQLite's `synchronous` is below `FULL`.
Files no row names are deleted at boot, so two hosts must never share a blob
directory: the lock stops the second while the first runs, and the owner
record stops it while the first is down.

*The owner record.* The directory holds one file besides its blobs, `owner`,
holding the store generation of the database it belongs to
(`messaging_store_identity.generation`). The first boot over an empty
directory writes it (through a temporary `owner.tmp`, renamed into place);
every later boot compares it with the database's and refuses to start, naming
both generations, when they differ, and when the directory holds blob files
and no record, or a record that is not one generation line. Restoring or
moving the database and its directory together keeps the two equal. The two
remedies:

- *A fresh or replaced database beside an old directory* — the record names
  another generation. The old files are another store's and this database
  names none of them: remove the directory, or point `messaging { blob_dir }`
  elsewhere, and the host claims a new one.
- *A lost or damaged record beside the database that does own the files* —
  write the database's generation back:
  `sqlite3 <database> "SELECT generation FROM messaging_store_identity" > <blob_dir>/owner`.

The host never adopts a foreign directory and never repairs a record on its
own. So a
database restored beside a blob directory that still holds every file its
rows name boots; a database restored alone, or beside a directory that has
moved on since the snapshot — as it does at every eviction — refuses when any
message in it names a blob since deleted. A database-only snapshot, a deploy's
rollback snapshot included, is therefore sound only while no message in it
carries an attachment. To start a host that refuses anyway, delete the
message rows the panic names with `sqlite3`; the trigger keeps the counts
right, and the next boot's sweep removes what they named.

**Mutability.** A blob is immutable once sealed, and nothing in the store
forecloses a mutable kind later: the id is random rather than a content hash,
the reference carries no digest, and the bytes are one file written in place.
Three choices lean on immutability: `size` and `content_type` are copied into
every reference, the bytes are synced once when a durable row first names
them, and reads take no lock. A mutable blob would need a flag fixed at
create and written into the reference, a write entry on a sealed blob, and a
rule for each of those three. The cost statement is the `blob` module's
rustdoc.

**A tool call carries attachments.** An asynchronous registry call and its
result carry named attachments by reference, in both directions, for every
caller and every tool the asynchronous path serves:

- A component hands a tool blobs with `blobs.call-async` (or, as a provider's
  nested call, `blobs.call-async-for`), and a provider answers with blobs
  through `blobs.complete` (`docs/component-packages.md`, "Blobs and tool
  calls"). The request on `brenn:tools/<tool>`, the request on a provider's
  `brenn:tool-requests/<slug>` and the result on a component's
  `brenn:tool-results/<slug>` carry them in the envelope like any message;
  their bodies are unchanged. A result carries attachments only when its
  outcome is the tool's own `ok`; an error, or a result the executor replaced
  because it was too large, carries none.
- A conversation hands a tool attachments in the reserved argument
  `attachments`, offered only by a tool whose declaration accepts them, naming
  each as `BrennSend` does — `(message id, attachment name)`, under the rule
  below. A tool result that carries attachments is published on the app's
  result inbox `brenn:app-tool-results/<app-slug>` (§2.7), recorded as read
  for the calling conversation alone, and shown to it as
  `{"result": <value>, "message_id": "<uuid>", "attachments": {<name>:
  {"size", "content_type"}}}`; `BrennSend`, `brenn-tool save` and a later tool
  call name its attachments by that `message_id`. After the record is reset
  they cannot be named again; the conversation calls the tool again. A result
  without attachments is the bare value, as before, and publishes nothing.

Who holds a tool call's blob, at each moment: a component's queued call holds
it in its buffered request until the activation ends, and drops it on an
`Err` or trap; once published, the request row holds it for that channel's
window — on a provider's request inbox, whatever becomes of the call; the
executor holds it while it admits and runs the call; a provider's completion
holds what it names from the call, through the pending call to the waiter,
and a later `Err` or trap of the provider's activation does not drop it; a
completion whose waiter is gone drops its holds; and the result row holds the
result's blobs for its inbox's window, plus any hold a conversation's
`BrennSend`, save or tool call takes by naming one. No holder is timed or
keyed by a conversation, and no byte is copied on the path. After a restart a
blob held only in flight is gone — no row names it, and the store unlinks it
at the next start — and one a durable
row names is redeemable while the row stays in its window.

What is not built: save and attach for a conversation without a shell tool,
attachments on `calls.call` and `tools.call-fast`, a page making a blob or
forwarding an attachment (`create`, `writer` and `publish` on a surface), and
a page reading part of an attachment larger than its channel's fetch bound
(`surface-blob-partial-read-over-bound` in `TODO.md`).

### 2.1.3 Limits on a remote's stream of messages

At default configuration, for a remote attacher on attach v5 or v6. "Operator" names
the setting that raises a limit; "fixed" is a compile-time constant. An ingress
channel (`webhook:`, `mqtt:`) is outside this section: a remote cannot
subscribe to one, and its subscribe to such an address ends the session as a
protocol violation.

| Limit | Default | Raised by |
|---|---|---|
| Body cap, one message | 65,536 bytes raw | Operator: `messaging { max_body_bytes }` (restart) |
| v5 frame cap | 204,800 bytes | Derived from the body cap |
| v6 frame cap | 274,432 bytes | Derived from the body cap |
| Blob chunk, one `BlobWrite` or `BlobRead` answer (v6) | 262,144 bytes | Fixed |
| Blob size (v6) | 64 MiB | Operator: `messaging { max_blob_bytes }` |
| Blob pool, the whole host (v6) | 1 GiB | Operator: `messaging { blob_budget_bytes }` |
| Blob handles per session (v6) | 64 | Fixed |
| Bytes held per session (v6): writers, sealed uploads and opened attachments | 4 × `max_blob_bytes` (256 MiB) | Follows `max_blob_bytes` |
| Blob read answers in flight per session (v6) | 4 | Fixed |
| `PublishBatch`, whole frame | 65,536 charged bytes over at most 512 operations; and 4 MiB of bodies per activation, never the binding one below a 4 MiB body cap | The first follows the body cap; the second is fixed |
| Connection publish bucket (single `Publish`) | burst 60, then 1 per second, per connection | Operator: `publish_burst`, `publish_per_sec` on the `remote` block, up to the bus's `default_send_rate` |
| Per-(sender, channel) send rate (single `Publish`) | burst 240, then 30 per second | Operator: `messaging { default_send_rate }`, or `send_rate` on the channel |
| Attach send budget (every durable `Publish`; every `PublishBatch` entry on any channel) | 256 tokens, then 1 per 15 seconds, shared by all the remote's sessions | **Fixed** |
| Sessions per remote | 2 | Operator: `max_sessions` |
| Channel window | the channel's `retain_depth` | Operator |
| Session push queue / writer queue | 256 / 256 frames | Fixed |

What a remote can sustain:

- **Onto an `ephemeral:` channel with single `Publish` frames** — the
  streaming path: 1 message per second of up to 64 KiB after a burst of 60.
  With `publish_per_sec = 30`, 30 messages per second, up to about 1.9 MiB/s
  at the default body cap. Beyond 30 per second needs `default_send_rate`
  raised as well.
- **Onto a durable `brenn:` channel**: 4 messages per minute sustained, and no
  setting raises it. With single frames the burst before that is 60 at once,
  then 1 per second until the 256-token budget is spent, then 4 per minute.
- **With `PublishBatch`**: up to 256 entries at once, then 4 entries per
  minute, on any channel class. A batch is not a streaming carrier for a
  remote.
- **Delivery to a subscribed remote**: one `Deliver` frame per live message,
  with no rate limit of its own. A subscriber more than 256 frames behind loses
  the queued copies and reads the rest from the channel's window by cursor;
  what it loses past the window is reported in `dropped`. The window's depth,
  as a duration at the publish rate, is how long a stall it survives.

So a camera frame that fits 64 KiB travels from a remote onto an ephemeral
channel at 1 per second with no configuration and at up to 30 per second with
one `remote`-block setting; larger frames need the body cap raised, which
raises it bus-wide, or a blob per frame on v6; a durable channel is not a
video carrier. These are limits, not measurements.

A camera frame over the body cap, sent on v6 as a blob per frame onto an
`ephemeral:` channel, costs **four client frames plus one write per 256 KiB**
(create, the writes, seal, publish, close) and three answers, none waited for;
**one connection-bucket token** (the publish) — so the same `publish_per_sec`
setting that governs body-sized frames governs these, 1 a second by default
and 30 with one `remote`-block setting — and no send-budget token; and on the
host **a file create, one file write per 256 KiB and, when the channel's
window rolls past the frame, an unlink** (no sync on a ring channel). The
close travels behind the publish (publish-or-drop), so a frame the connection
bucket refuses is dropped with its blob and the daemon holds no handle past
the frame it is sending; a daemon that kept handles to retry refused frames
would meet the 64-handle bound within seconds at the default rate. A reader
pays an open, one read per 256 KiB and a close. Against that, raising
`max_body_bytes` costs one frame and no filesystem operation per camera frame,
and raises the cap for every channel, ring and durable, and every subscriber's
window copy. On a durable channel a blob per frame also costs a send-budget
token and two fsyncs, and is not a video carrier, as before. These too are
limits, not measurements.

**The archive line format.** An `archive` sink writes one JSON object per
evicted message, one per line. The fields are the envelope's, in its order —
`message_id`, `source`, `channel`, `sender`, `publish_ts`, the body entries,
whichever of `reply_to`, `delivery_deadline`, `deliver_after` and `impetus` are
present, then `urgency`, `envelope_type`, and `transport` on a message that
arrived in the raw ingress shape (§2.7.1). The body entries are `body` (a JSON string) for a
text body, or `body_base64` for a non-text one, followed by `content_type` when
the body has one. An untyped text body's line is exactly the line written
before bodies became bytes.

### 2.1.4 Limits on a stream delivered to a surface

The delivery half of the path from a publisher to a 0.2.0 kind on a surface
page: bus, attach session, websocket, page kernel, activation. Defaults;
"fixed" is a compile-time constant. An ingress channel (`webhook:`, `mqtt:`) is
outside this section: a surface cannot bind one (§2.1.1).

| Limit | Default | Raised by |
|---|---|---|
| Body cap, one message | 65,536 bytes raw | Operator: `messaging { max_body_bytes }` (restart) |
| Channel window (`ephemeral:`) | the channel's `retain_depth`, a count, at most 4,096; no byte bound | Operator; the 4,096 is fixed |
| Session push queue / writer queue | 256 / 256 frames | Fixed |
| A live `Deliver` | one row per frame; no rate limit and no byte budget on delivery | — |
| A replay or catch-up `Deliver` | up to `max(push_depth, retain_depth)` rows in one frame, with no size cap | the binding's depths |
| Server-to-page frame size | unbounded: the 274,432-byte v6 frame cap bounds what a page sends | — |
| The page's inbound queue | unbounded; drained one frame per browser task | — |
| The page's store | `max(push_depth, retain_depth)` rows per channel, a count | the binding's depths |
| One activation's input | every bound port's window, with no byte limit; the 4 MiB per-activation bound is on publishes only | — |
| Sessions per surface / per user on one surface | 64 / 16 | Fixed |
| Fetch bound, the largest attachment a page fetches on a channel | 1 MiB on an instance granted `blobs`, 0 otherwise; at most 64 MiB | Operator: `fetch_bytes` on the `in` binding |
| The page's attachment cache, bytes reserved and fetched | 64 MiB per page | Fixed |
| Arrival hold, rows per gated channel | store depth + 4, of which at most 4 fetch at once | the binding's depths |
| A held row's deadline | 2 s + 1 s per 256 KiB chunk of each eligible attachment, at most 30 s | Fixed |
| The aggregate hold, delivery to store on a gated channel | `⌈(store depth + 4) / 4⌉ · 30 s` attached, `· 45 s` with a connect attempt at every deadline: 60 s / 90 s at depth 1 | the binding's depths; no fetch bound lowers it |
| Blob handles / read answers in flight per page session | 64 / 4, across every channel | Fixed |

What follows:

- **Rate.** Delivery adds no rate limit to what the publisher sustains: for a
  remote onto an `ephemeral:` channel, what §2.1.3 says.
- **Falling behind.** The server keeps at most 256 undelivered frames per
  session and then reads the channel's window by cursor; the page keeps at
  most the store's depth. A kind slower than the stream is activated with the
  newest `push_depth` rows and the rest are drops — what a frame stream wants:
  `push_depth = 1`, `retain_depth = 1`. Each activation instantiates the kind
  afresh, so its cost floors the frame interval; `docs/page-rig-timings.md`
  has the node
  and Chromium figures.
- **Loudness.** A binding's `noise` decides what a drop does. At `fatal` the
  first overrun kills the instance; a stream binding states `silent` or
  `metered`.
- **Reconnect.** A replay pass is one frame of up to the window's depth times
  the body cap, on every reconnect.
- **Attachments.** On a gated channel (§2.1.2a, "A surface page is a reader by
  place") a frame with one attachment of `c` chunks (`c = ⌈size / 256 KiB⌉`)
  costs, server to page, the `Deliver`, one open answer and `c` binary read
  answers; page to server, one open, `c` reads and one close. None draws on a
  token bucket. The open and the reads are two round trips in series, the
  reads of one attachment overlapping up to 4 wide; up to 4 frames of a
  channel fetch at once, and the page's reads share 4 in flight across every
  channel. With `R_o` and `R_r` the round trips of an open and of a read
  answer, a channel is handed at most
  `min(4 / (R_o + R_r · ⌈c / 4⌉), 4 / (c · R_r))` attachments a second, each
  `R_o + R_r · ⌈c / 4⌉` later than a body-only message would be — for
  one-chunk frames `4 / (R_o + R_r)`: 200 a second at 10 ms a round trip, 40 at
  50 ms. Past that rate the hold drops waiting frames oldest first and hands
  over what it fetched; the rate handed over does not fall. A newer frame does
  nothing to an older one being fetched. The deadline and its ceiling bound a
  fetch that does not answer and are not terms of the rate. The terms that move
  without an interface or wire change: the 4 rows fetching at once and the
  hold's deadlines (kernel constants), the server's handles, reads in flight
  and bytes held (advertised in `Welcome.blobs`; bytes held follows
  `max_blob_bytes`), the second round trip (a later client method that reads
  through an opening handle), and the operator's `fetch_bytes` and depths. A
  read answer carries at most 256 KiB, which bounds `c` and not the rule.
- **Uploads.** A page's upload of a blob it made goes the other way on the
  same socket: one create, the blob in writes of at most 256 KiB, a seal, then
  the batch. The page composes one write at a time, but the browser's socket
  queues the whole upload at once, because the transport exposes no send-side
  flow information, so everything the page sends after an upload starts — every
  component's publishes and the upload's own batch — leaves behind it, up to
  64 MiB later. Inbound frames are not delayed, and each answer the server sends
  keeps the connection's liveness. A create the server refuses does not stop the
  writes already queued; the server drops them.

So a camera frame that fits 64 KiB, sent by a remote onto a `non_text`
`ephemeral:` channel, reaches a 0.2.0 kind on a surface at 1 per second with
no configuration and at up to 30 per second with one `remote`-block setting;
a larger frame travels as a blob per frame, which a kind granted `blobs` on
the page reads once the page has fetched it, at the rate above, up to the
1 MiB default bound with no configuration beyond the grant, or the body cap is
raised. These are limits, not measurements;
`docs/page-rig-timings.md` has what one delivered row costs a 0.2.0 kind in
Chromium, with a kind on the page as the publisher — no browser run has a
remote as the publisher.

### 2.2 Subscriptions

A subscription carries two independent parameters. Their independence is the
central design insight.

**`push_depth` — what wakes me.**
Purely about *activation*. `push_depth=0` means "never activate me on this
channel." `push_depth=5` means "activate me on new messages; if I am behind, or a
burst arrived, coalesce the pending activations into one and hand me at most the
5 most recent; discard older ones."

**`retain_depth` — what I can see.**
Purely about *visibility*. A private window of size M ≤ N onto the channel's
window, where N is the channel's standing depth (§2.1) and `M ≤ N` is enforced,
not advisory: boot refuses a static subscription over the ceiling and the
dynamic-subscribe path refuses a runtime one. Always the most recent M. It does
not *cause* activation; a node with
`push_depth=0, retain_depth=50` sees fifty messages and is never woken by any of
them. (It is bounded by `push_depth` when that is nonzero — see the invariant
below — but that is a consistency constraint, not a coupling of purpose.)

The two useful shapes:

- **`push_depth=0, retain_depth>0`** — a query channel. Something else drives
  this node; when it runs, it reads current state or recent history here.
- **`push_depth>0, retain_depth ≤ push_depth`** — a trigger channel, optionally
  with history, so that when activated by *another* channel the node can still
  see messages it has already been handed.

Other configurations are valid but pointless. `push_depth=5` with
`retain_depth=10` means you get the last 10 messages but only 5 the 5 most recent
are considered "new" or "unseen". It's a meaningless distinction because you
actually see up to 10 unseen messages.

That last sentence is about *window reads* — what the window holds when the
subscriber looks. It is not what every subscriber is handed on wake: a
conversation subscriber is handed only the window's new entries, so a burst
larger than `push_depth` coalesces to one activation carrying the newest few and
the rest are visible only to an explicit window read. Nothing counts them as
lost, either: the drop counter charges only unseen sequences that fell out of the
retained window, so an under-sized `push_depth` is silent while an under-sized
`retain_depth` is not. Size `push_depth` by asking whether each message must
reach the subscriber individually or whether the newest on wake suffices — the
signal-versus-fact call of §3.

Most frameworks fuse these two ideas into one queue, and the fusion is the source
of a great deal of pain: you cannot ask to see history without also asking to be
woken by it, and you cannot ask to be woken without inheriting a delivery
backlog. Separating them means **activation policy and visibility policy are
tuned against different pressures** — activation against the node's processing
cost and coalescing tolerance, visibility against the node's analytic needs.

**Attach is a delivery point.** A subscription coming into existence is owed up
to `push_depth` of the channel's retained window, immediately, as unseen. There
is one priming and no parameter to choose another. Nothing in the bus
distinguishes "old" from "new" beyond seen and unseen: a message published before
a subscriber existed is unseen *to that subscriber*, and `deliver_after` makes
any recency assumption false by construction anyway. A consumer whose semantics
depend on staleness judges by a timestamp inside the message, not by asking the
bus to hide history from it.

Re-subscribing is not a new attach. An existing subscription keeps its position
and only retunes its depths, so a restart resumes where it left off rather than
re-reading the window.

### 2.3 Coalescing as the default activation semantic

Coalescing is not a degradation path. It is the correct semantics for a node
subscribed to a signal: if five temperature readings arrive while I was busy, I
want to be woken once, with the current temperature, not woken five times to
process four stale values before reaching the truth. A system that guarantees
five activations is guaranteeing four units of wasted work and one unit of
avoidable latency.

Where the node needs to *know* it skipped, a per-subscriber **`drop_counter`**
reports it. Loss is bounded, visible, and attributable — which is the property
that distinguishes this from naive drop-oldest middleware, where overload is
absorbed as invisible staleness.

### 2.4 `deliver_after`

A message may carry a delivery time. This single mechanism is the substrate for
all time-based behavior in the graph: timers, timeouts, retries with backoff,
debouncing, scheduled reminders, "nudge me if this hasn't been decided in six
hours." A node schedules its own future activation by publishing to a channel it
also subscribes to.

The elegance is in what it *removes*: there is no separate scheduler subsystem,
no cron abstraction, no timer service with its own lifecycle and failure modes.
Time-based triggering is publication with a coordinate on the time axis, and
everything that is true of messages is true of timers.

### 2.4a A sync call is not a message on the bus

A component can also be *called* synchronously: one request in, one reply out,
inside one ordinary activation of the callee. None of it is a publication. The
request is minted around the caller's payload, rides in a fabricated window of
its own, and is delivered to exactly one instance — the target — and to nobody
else. It is never retained, never routed, never seen by a subscriber, and its
address family (`local:brenn/sync/<port>`) is bindable by nothing. A call has no
`deliver_after`, no urgency ladder, no drop count, and no cursor.

So none of the reasoning in this document applies to it, and the inverse is the
part worth remembering: **RPC across the wire is pub/sub with a reply channel**,
which the bus already supports and which the sync call deliberately is not. A
call goes to an instance in the same placement, its caller is blocked for the
whole of the callee's activation, and the call graph is acyclic by refusal at
compile time. Anything that needs to cross a host boundary, survive a restart,
fan out, or be retried is a message, not a call.

Under the byte-carrying contract a call's request and its reply are message
bodies — bytes with an optional content type — like any other. When the two
ends speak different contracts, each end is held to its own:

- A non-text request to a callee built against the string contract is refused
  before the callee runs (`text-only-peer` to the caller). A typed text
  request reaches it as its text.
- A reply is judged against the callee's own contract only. A 0.2.0 callee
  may reply with a non-text body to any caller; its activation ends ok and its
  buffer flushes. A 0.1.0 caller cannot read that reply, so the *caller's*
  activation traps, and the alert names the caller, whose wiring is the one to
  change. A typed text reply reaches a 0.1.0 caller as its text.

### 2.5 State by retention

Because channels persist independent of consumption, a retained channel *is* a
state variable. There is no separate key-value store, no state API, no question
of how state and messages stay consistent. Current state is `retain_depth=1` on
the channel that carries it. Recent history is `retain_depth=k`. The distinction
between "a message bus" and "a database" collapses into a single parameter.

### 2.6 Auto channels and in/out ports

Most channels are shared infrastructure and are declared as such: a `[[channel]]`
block, ACL entries on each participant, a binding per port. Two shapes do not
want that ceremony — a component's timer loop back to itself, and a private wire
from one component to one other — and for those the channel is declared *by the
ports being connected*. Such a channel is an **auto channel**. There is no
`[[channel]]` block and no operator-written ACL: the declaration that wires the
ports is the authorization signal, and each endpoint is granted exactly the
publish or subscribe reach its own role needs.

Two spellings:

- **`link`** — an anonymous channel handle that bindings target where they would
  otherwise name a channel. The ports bound to it state their own tuning and no
  channel of their own ("free ports"), so the tuning stays with the subscriber it
  describes and the wire stays with the link.
- **`io_port`** — one port name resolving to an input *and* an output on the same
  channel. This is the sanctioned timer idiom: `deliver_after` on the port is a
  wake the component is guaranteed to receive, because the two halves
  structurally cannot be wired to different channels. Wiring a self-loop out of
  two separately-configured bindings is how it silently fails.

An auto channel with no name is **anonymous**: its address is `auto.<cid>`, where
the cid is derived from the endpoint set. The `auto` namespace is reserved —
nothing else may declare, bind, or write an ACL matcher into it — so an anonymous
channel is reachable *only* through the declaration that created it. A link is
always anonymous. An `io_port` may instead be given a name, which makes it an
ordinary directory entry that third parties may bind with ordinary bindings and
ordinary ACLs (naming grants nothing by itself; deny-by-default still holds), and
a `brenn:` name is what makes it durable, which is what makes a parked schedule
survive a restart.

Scheme, when the channel is anonymous, follows from where the endpoints live —
non-transportable while everything sits on one side of a wire, transportable only
when the wiring spans one:

| Endpoint set | Scheme | Realm |
|---|---|---|
| All backend components | `local:` | Server ring |
| All on one surface | `local:` | Page ring — per session, browser-side |
| Backend + surface, or two surfaces | `ephemeral:` | Server ring plus the wire |

The two `local:` rows are two different realms that share a scheme, and they
cannot exchange a message. A page-local channel is per *session*: each open
session of the surface has its own copy, which is the natural reading of a
self-loop. Wanting one channel shared across a surface's sessions, or shared with
the backend, means naming it `ephemeral:`.

Each realm's `local:` namespace is private, so one bare name appearing in both —
or in the page realms of a dozen surfaces stamped from one config template — is
legitimate and means nothing. Those are distinct channels that happen to be
spelled alike. That privacy is what `local:` is for.

An auto channel's depths are folded rather than declared: the fold is the max over
subscribing endpoints of `max(push_depth, retain_depth)`, floor 1, and it answers
all three depth questions — `retain_depth`, the channel-level `push_depth` rung a
later third-party binding reads, and (when durable) `standing_retain_depth`
(§2.1). One value for all three means an auto channel satisfies the depth
ceiling by construction. Every
subscribing port therefore states both of its own depths; an auto channel has
nothing else to derive from, and boot refuses a port that leaves either unwritten.
`sink`, `send_rate` and channel-level `noise` inherit the global defaults. A
channel that needs channel-level tuning has outgrown auto declaration; write the
`[[channel]]` block.

`non_text` (§2.1) is folded the same way: a port states it on its binding tail,
and an auto channel is `non_text` when any of its endpoints says so. This is
how a component keeps binary state on its own `io` port with no declared
channel: `io state { push_depth = 0; retain_depth = 1; non_text = true; }`.
On a declared channel the same key on a port is a check, not a declaration: a
port stating `non_text = true` bound to a channel that does not is refused
when the plan is built, so `config-check` reports it.

On a surface binding the key does what it does on the backend. Where the
channel stays on the page — a `local:` address outside the reserved
`local:brenn/` planes, a free `io` port, or a link whose endpoints are all on
that surface — it declares a page-local channel, and the page ring is
`non_text` when any surface binding on its address says so; a server-realm
`channel` declaration at a `local:` address is never read for it. Where the
channel leaves the page — a declared `ephemeral:` or `brenn:` channel — it is
a check: stated on a port whose channel is declared text-only, it is refused
when the plan is built, and a port that does not state it may be bound to a
channel declared `non_text`. On a link with an endpoint off the surface the
statement is the declaration, by the fold above. Stated on a reserved plane it
is refused. A surface's config channel declared `non_text` is refused: a page
reads its bindings document as text, so the declaration would admit only
bodies no page can apply. A 0.1.0 kind bound to any `non_text` channel,
page-local or wire-bound, is refused at boot and at reload.

The page's wire is attach v6. On any channel declared `non_text`, page-local
or wire-bound, a page-hosted `brenn:processor@0.2.0` kind publishes, parks and
edits non-text bodies, is delivered them, and reads them back byte for byte
with their content type; on any channel it keeps typed text with its type; it
passes any body in a sync call between 0.2.0 kinds. A non-text body for a
text-only channel is refused with `text-only-channel`. This is shown on the
node rig, against a stand-in for the browser's websocket;
`e2e/tests/bytes.spec.ts` runs the publish, the read-back and the delivery in
Chromium, and an SDK-built kind's binary state on a page-local `io` port. A
remote attacher on v5 is served a `non_text` channel; one on v4 is
answered `Unavailable`.

**The timer-cap coupling.** A channel's `retain_depth` also caps its channel-wide
deferred set, so on an auto channel the fold above is what bounds how many
`deliver_after` schedules the port may hold outstanding. A component juggling K
parked wakes must declare at least K, on top of its actual retention need. Over
the cap the schedule is refused and host-logged, and the component is not told —
so this is a number to size deliberately.

There is no default to leave it to. Both halves must also be *bounded* on a
non-durable channel: the fold takes `max(push_depth, retain_depth)` per subscribing
port, so an explicit `"unbounded"` on either half folds unbounded and refuses to
boot in the server realm. The two outs are bounded depths on the subscribing
port(s), or a `brenn:` name. A page-local port escapes only the fold: it has no
server entry, so an unset `retain_depth` gives a ring of depth 1 — the one
structural exception, because the page kernel keeps that ring whether or not a
binding asks — but its `push_depth` is a page queue nobody else can size, and an
unstated one refuses to boot.

### 2.7 System-minted channels

Some channels are minted by the system rather than declared by the operator: the
webhook ingress channels (`webhook:<slug>`), the MQTT ingress channels
(`mqtt:<client>:<topic>`), the async-tool substrate's request and result
channels (`brenn:tools/<tool>`, `brenn:tool-results/<slug>`,
`brenn:app-tool-results/<app-slug>`), the per-user
device-location channels (`brenn:device-location.<username>`), and the MQTT
egress channels (`brenn:egress/mqtt/<client>`, §2.7.3). Nothing in config
brings these into existence as channels — an endpoint block, a subscription, a
registered tool, a registered user or an `mqtt_client` block does — so there
is no declaring `[[channel]]` block to carry their depths.

They still have to be *sized*, and the sizing rule is the same one everything
else obeys: bounded, and visible to whoever ought to be deciding. Each family has
a bounded in-code default:

| Family | push | retain | standing |
|---|---|---|---|
| `webhook:<slug>`, `mqtt:<client>:<topic>` | 1 | 100 | 100 |
| `brenn:tools/<tool>`, `brenn:tool-results/<slug>`, `brenn:app-tool-results/<app-slug>` | 1 | 16 | 16 |
| `brenn:device-location.<username>` | 1 | 2000 | 2000 |
| `brenn:egress/mqtt/<client>` | 1 | 256 | 256 |

Ingress channels are fact channels; at their arrival rates a hundred messages is
a horizon of days to months, which is what "sized for the outage you intend to
survive" means for them. The tool channels' executor and consumers are in-process
and eager, so their window covers a burst arriving while the executor is busy.
The channel-level push rung is near-inert on these families — every
subscriber on them states its own depths — so 1 is the honest floor.

A `brenn:device-location.<username>` channel holds every location report from
that user's devices. Brenn mints one per registered user, at boot from the users
table and at registration, and writes it only as the system participant
`system:device-location`, from the user's own authenticated connection; whoever
the operator grants the channel may read it. The PWA reports one fix per device
every five minutes while the page is visible, so the default 2000 is about a
week of one device reporting around the clock, or about a day of seven. That
window is the history a reader can ask about and the whole retention story:
nothing is deleted when a user stops sharing or a device is unenrolled. The host
reads the window to decide every report it admits, so a larger window is also a
larger cost per report (`docs/config-dsl.md`, the device-location tool). A device
silent for longer than the window has no message left in it, counts as not
sharing, and is re-enabled from the user menu.

The window is also where each device's sharing state lives, and one window
serves all of a user's devices. Size it as a count of other devices' reports,
not only as history: a device's last fix must survive every report the user's
other devices make before its next one. A window smaller than that pushes the
fix out, the device's next report is refused as not sharing, and its sharing
turns off with no error shown. At `retain_depth = 1` only the device that
reported last stays sharing — a configuration for a user with one device.

The family's send rate is the host's: 240 messages in a burst and 30 a second
sustained per user channel, whatever `[messaging] default_send_rate` says, and a
tuning block stating `send_rate` on the family is refused. A device's stop shares
that bucket with the user's fixes, and a stop the rate refused would leave the
device recorded as sharing; the rate is far above what the PWA sends, so only a
session of the same user flooding reports can empty it.

The family's tuning is restart-only, and a reload enforces it: a reload whose
document adds, removes or changes a tuning block for `brenn:device-location.`
or for one user's address is refused with `NEEDS_RESTART`, so every channel in
the family — minted at boot or at a later registration — carries the depths the
process booted with.

Each message body is JSON, tagged by `kind`:

- `{"kind": "fix", "device_id": 7, "device": "phone", "latitude": 52.37,
  "longitude": 4.89, "accuracy_m": 12.0, "time_zone": "Europe/Amsterdam"}` — a
  position report. `latitude` and `longitude` are decimal degrees, `accuracy_m`
  metres, `time_zone` the browser's IANA zone or `null` when the server does not
  know the name.
- `{"kind": "stopped", "device_id": 7, "device": "phone"}` — the device stopped
  sharing: the user's menu action, or the browser withdrawing its permission.

`device_id` is the stable key for a device; `device` is its display name at
publish time and may change between messages. The envelope's `publish_ts` is
the server's receipt time; no browser timestamp is carried. A device is sharing
while its newest message in the window is a `fix`. A reader believes only
envelopes whose `sender` is `system:device-location`: an operator's `acl
publish` matcher can let any component write to a user's channel, and the
`DeviceLocation` tool skips and logs anything else. The body has **no
compatibility guarantee**: it may change between releases, and a reader
outside this repository is built against the release it runs with.

A consumer's `brenn:tool-results/<slug>` inbox feeds its reserved `tool-results`
input port: the substrate subscribes the consumer to its own inbox at that
window and delivers each result as an activation on that port, so the numbers
tuned below are what the guest sees. That subscription inherits the channel's
`noise` and `wake_min` as any configured one does, and takes both its depth
rungs from the channel's `retain_depth`; the channel's own push rung is the
near-inert one described just above.

A `brenn:tools/<tool>` request channel is the other way round: its only
substrate subscriber is the tool executor, folded in as a system participant.
A system participant's overflow rung is the one its own spec states, whatever
the channel says: `Silent` for every participant but the MQTT egress one,
whose rung is `Alarm` (§2.7.3). A `noise` written on a tuning block of any
system family, a request channel's included, is inert today
(`TODO(system-participant-noise-inert)`).

A request channel's window is also how many requests the executor can be
behind on that tool. Each drain serves at most the window's newest unseen
requests and steps over the older ones, as any subscriber's overflow does
(drop oldest). One activation may queue up to 64 asynchronous calls, and the
default window is 16: a request pushed past the window before the executor
dequeues it is lost, attachments or not. No result is published for it; the
drain logs how many requests it stepped over and counts them, and the defaults
still guarantee the case (`TODO(tool-request-window-overflow-silent)`). Its
blobs go when GC evicts its row. The window is also what keeps a request's
attachments alive until the executor takes its own holds: a row evicted and
reaped in the instant between the dequeue and that hold is answered
`internal`, naming the attachment. An operator who expects bursts of calls to
one tool sizes that tool's `brenn:tools/<tool>` window for the burst.

An app's `brenn:app-tool-results/<app-slug>` inbox is minted for every app
granted at least one asynchronous registry tool, at boot and when a reload
grants the first one. It holds the attachments of the tool results handed to
that app's conversations (§2.1.2a): each message's body is
`{"v":1,"tool":"<name>"}` and its attachments are the result's — the tool's
value is not in it, since the conversation receives that in place. A result
without attachments is never published there. Nothing subscribes to it, and
**no policy can cover it**: `AppPolicy` answers no for the namespace whatever
its matchers say, so nothing lists, reads, subscribes to or is delivered from
an app's result inbox, its own app included, and an authored matcher reaching
into `app-tool-results/` is refused at start. A conversation names one of its
attachments only through the read record, which the host writes for the
conversation that made the call and for no other.

Its window, as duration: with no subscriber, `standing_retain_depth` is the
number that counts. A conversation names a result's attachment until that
many newer attachment-carrying results exist on the inbox (`not_retained`
after that), and the next hourly GC pass deletes the row and the reap its
blobs. So an app's conversations *together* may receive 16 later
attachment-carrying results, at the default, before an earlier one's
attachments stop being nameable — ample for a conversation that forwards or
saves a result within its turn or the next, and the number to raise for an
app with many conversations fetching at once. The inbox is durable, and the
read record is in memory: after a restart the inbox holds up to its standing
depth of results no conversation can name, and their blobs stay charged to the
blob budget until that many new results arrive and a GC pass runs.

**Tuning them.** A `[[channel]]` block addressing one of these channels does not
declare it; it *tunes* it. Synthesis still owns creation, identity and
description, so `uuid` and `description` are rejected on a tuning block, and all
three depths are required — a block that tunes states every number. `noise`,
`wake_min`, `sink` and `send_rate` are optional and inherit the `[messaging]`
globals, exactly as on a declaring block — except `send_rate` on a
`brenn:tool-results/` or `brenn:app-tool-results/` inbox, which is the host's
and refused in the block. An app's result inbox is tuned by its exact address,
which must name an app holding an asynchronous tool grant, or by the prefix
`brenn:app-tool-results/`; a block over `brenn:tool-results/` sizes component
inboxes only. An
explicit `"unbounded"` is legal
here: unbounded is something an operator asks for in so many words, never
something a default hands out.

A tuning block may be keyed by `address_prefix` instead of `address`, standing
for a whole family of dynamically named channels — the MQTT case especially,
where channels are minted at runtime. A prefix must end at a segment boundary
(`/`, `.`, or the `mqtt:<client>:` colon) so it cannot reach past the family it
names. Resolution per concrete address is: the exact block, else the longest
matching prefix block, else the family default. A key written without a scheme
names a `brenn:` channel, the same spelling rule declaring blocks follow, so
`tools/git-repo-pull` and `brenn:tools/git-repo-pull` are one block — writing
both is a duplicate.

`retain_depth = 0` is refused: the window is also what the channel's system
participants subscribe at, and zero would leave them without a position at all.

Exact `webhook:` and tool blocks are boot-checked against the endpoints, tools
and grants that exist, so a typoed address fails to boot rather than silently
tuning nothing. Exact `mqtt:` blocks and every prefix block are not checked
against a population: the MQTT population is open-ended and a prefix is a
standing rule for a family whose membership is dynamic. An exact `mqtt:` key is
still checked for *shape* — it must be a well-formed `mqtt:<client>:<topic>`
with a legal filter — since a spelling no mint path can produce would tune
nothing for the same reason a typo would. An exact `brenn:device-location.`
key is the same: its population — the registered users — changes at runtime,
so the block is checked only for shape, a remainder that is a legal username.

Nothing about a resolved channel is persisted — depths are re-read from config
each boot — so retuning takes effect on restart with no migration. Lowering
`standing_retain_depth` lets the next reap pass evict; raising it cannot
resurrect what was already evicted.

**Dynamic subscriptions on these channels are re-authorized at every
convergence, not only at boot.** An `mqtt:` channel is more often minted by a
running session's `MessageSubscribe` than by any block, and the subscription
behind it is authority the document grants and can take back. A reload asks the
question a boot asks, over the same rules: a row the new ACLs no longer cover is
folded out and left dormant — durable row and cursor kept, and its broker filter
dropped when no other subscriber needs it — a dormant row the new document
authorizes again is folded back in at the depths it was granted, with its filter
and route restored, and a row on a channel the document now declares as a
`subscribe` line is replaced by the static entry and deleted. The status body
reports the three as `dynamic_revoked`, `dynamic_revived` and `dynamic_pruned`.
A reload that would **retune** the channel itself while a dynamic subscription
sits on it — folded or dormant — is refused instead, and needs a restart; so is
one that **removes** a channel the subscription is still folded into, or one
carrying a dormant subscription on a system-minted address — `mqtt:`,
`webhook:`, `brenn:tools/…`, `brenn:tool-results/…`,
`brenn:device-location.…` — whose channel a fresh boot
rebuilds from its stored row with no block declaring it. Removing an
operator-declared channel — one a fresh boot cannot rebuild that way — under a
*dormant* subscription is applied and leaves it as a restart leaves it: dormant,
durable row and cursor kept. Only a durable channel's subscription can be
dormant at all: a non-durable one lives in memory and is always folded.
Re-declaring the channel revives it at the next restart,
not at the reload that re-declares it — unless the same document also declares a
`subscribe` line for the pair, which the reload answers as a boot does, by
deleting the row and folding the static entry in. The re-declared block keeps
the channel's identity by construction, since a declaration derives its uuid
from its address; a `uuid_pins` entry that moves it is refused, because the
address still belongs to the stored row the dormant subscription names.

**An `mqtt:` filter is subscribed at its client's QoS.** The broker
subscription behind an `mqtt:<client>:<filter>` channel is made at that
client's `[[mqtt_client]].qos`, and at nothing else. Every subscriber of the
address — a `subscribe` line, a component binding, a runtime
`MessageSubscribe` — shares that one broker subscription, and none of them can
choose or change its QoS: `MessageSubscribe` takes no `qos`. A message arrives
at the lower of the QoS it was published at and the QoS the broker granted,
which is at most the client's. Editing the client's `qos` restarts the client,
and its first connect subscribes every filter it holds again at the new QoS.

A client's `qos` is 0 or 1. Brenn does not subscribe at QoS 2: its MQTT client
library forgets, on every lost connection, which QoS 2 messages it has
received, so on a kept session a message could arrive twice, or a PUBREL the
broker sends again could end the connection on every reconnect. A QoS 2
message can still arrive, out of session state an earlier configuration
subscribed at 2 left at the broker; it is delivered at least once, not exactly
once. A publish keeps its own QoS, 0 to 2.

### 2.7.1 The two ingress shapes

A `webhook:` or `mqtt:` channel takes what its transport receives in one of two
shapes. Which one is a fact of the channel, read from configuration: a tuning
block — exact or prefix — stating `non_text = true` makes the channel *raw*;
without it the channel is *wrapped*. No subscriber and no message selects the
shape, and the adaptor reads it from the channel entry it publishes on.

```
channel at prefix "mqtt:broker:cam/" { push_depth = 1; retain_depth = 16; standing_retain_depth = 16; non_text = true; untyped_content_type = "application/octet-stream"; }
channel at "webhook:scanner"        { push_depth = 1; retain_depth = 100; standing_retain_depth = 100; non_text = true; }
```

**Wrapped**, the default, is the shape every ingress channel had before the
opt-in existed, unchanged. The body is the transport's wrapper JSON —
`MqttEnvelope` or `WebhookEnvelope` — as untyped text, and the message has no
`transport` key. An MQTT payload that is not text is a placeholder inside the
wrapper and its bytes reach no channel; a webhook body must be UTF-8 or the
request is refused before authentication.

**Raw.** The body is the payload itself, byte-exact, with a content type, and
the message carries the transport's metadata in the envelope's `transport` key
(§2.1.2):

- MQTT: `{"mqtt": {"client_slug", "topic", "qos", "received_at"}}` — the
  client, the topic the message was actually published on, the delivery QoS
  and the host's receive time; the wrapper's fields less the payload.
- Webhook: `{"webhook": {"headers", "key_id", "client_ip", "received_at",
  "endpoint_slug"}}` — the wrapper's fields less the body: the request headers
  with credential-bearing values replaced by `"[redacted]"` and non-UTF-8
  values left out, the key that authenticated the request, the client address,
  the receive time and the endpoint.

A conversation reads a raw message as any other: a text body verbatim with its
type, a non-text body as the placeholder, and the `transport` object beside
it. A 0.2.0 component reads the bytes as the body and the metadata from the
header; `brenn-guest`'s `mqtt_transport()` and `webhook_transport()` read it,
and its `mqtt_body()` and `webhook_body()` read the wrapper, each refusing the
other shape by name. A 0.1.0 component cannot bind a raw channel: boot and
reload refuse it, as on any `non_text` channel (§2.1). The size cap applies to
a raw body (§2.1.2).

**Which channels an inbound MQTT message reaches.** Each `mqtt:` filter a
client holds is asserted at the broker as its own subscription, with an MQTT 5
Subscription Identifier derived from the filter string, the same in every
process. An inbound message is published once into the
channel of each subscription the broker names on it, provided that channel's
filter covers the message's topic, and into no other channel. A copy naming a
subscription the process no longer holds, or one whose filter does not cover
the topic, is dropped with a `warn`; the identifier never places a message in a
channel whose filter does not match it.

- *Overlapping filters.* A client holding `a/#` and `a/b` receives a publish to
  `a/b` once in each of the two channels — the broker sends one copy per
  subscription, and each copy reaches its own subscription's channel — unless
  the two filters share an identifier (below).
- *Two filters with one identifier.* The identifier is a 28-bit function of the
  filter string, so two different filters on one client can have the same one
  — about one chance in 268 million for a given pair. The broker's copies for
  the two are then indistinguishable, and a message both filters cover is
  delivered twice into each of their channels, as on a broker without
  Subscription Identifiers. A message only one of them covers is routed
  exactly. A `warn` names the pair when the second is subscribed. The same
  holds when one of the pair is a filter this process no longer holds and a
  resumed broker session still does: the held filter's channel receives that
  filter's copy as a second row.
- *Across a restart.* With a non-zero `session_expiry_secs` the broker keeps
  the session while Brenn is down and delivers what it queued when Brenn
  returns. Each queued message reaches the channel of the filter it was queued
  for, if this process holds that filter; one queued for a filter no longer
  held is dropped with a `warn`.
- *Retained replay.* The retained messages the broker sends for a new
  subscription reach that subscription's channel only; a wider channel the
  client already holds does not receive them. A second subscriber joining a
  filter already held makes the broker replay into the shared channel again,
  and every reconnect replays the retained messages of every filter into its
  own channel again; both add rows that repeat earlier ones.
- *A broker without Subscription Identifiers.* When the broker's CONNACK says
  it does not support them, SUBSCRIBEs carry none and messages are routed by
  topic into every covering channel; overlapping filters then deliver a message
  once per covering filter into each. A `warn` says so once per connection.
- *What bounds what a new subscriber sees.* The channel's retain depth (100 by
  default for an `mqtt:` channel, §2.7's table); a subscription asking for a
  deeper `retain_depth` than the channel's standing depth is refused; and a
  read's own `limit`.
- *When the replay is there.* `MessageSubscribe` answering `"subscribed"` means
  the subscription is recorded and the SUBSCRIBE has been sent to the broker —
  not that the broker has answered or that any retained message has arrived.
  The replay arrives afterwards, one message at a time: one on an exact topic
  within moments, many under a wildcard over a time that grows with their
  number, and nothing marks the last one. A read made at once may return none
  or part of it. A subscription with `push_depth` above 0 is told as messages
  arrive.

**MQTT on a raw channel.** The body's content type is the publisher's MQTT 5
Content-Type property when the broker delivered one. A payload with none takes
the channel's `untyped_content_type`, when the tuning block states it; without
it, the payload is untyped text. `untyped_content_type` is legal only on an
`mqtt:` tuning block that also states `non_text = true`, its value a media type
the bus accepts; it never looks at the payload's bytes, it is the same for every
message on the channel, and the message does not record whether the publisher
or the channel stated its type. A publisher on MQTT 3.1.1, which has no
Content-Type, or one that sets none, needs it to deliver binary: without it a
payload that is not UTF-8 cannot be untyped text and is dropped. A channel
declaring a non-text type makes every untyped payload non-text, even one whose
bytes are UTF-8; text publishers on such a filter set a Content-Type, or the
filter is split across two channels.

Each payload is judged per channel. A bad one is dropped for that channel —
one `warn` naming client, topic, channel, size and reason, nothing stored, no
substitute type — and is still published on any other channel the message is
routed to:

| Input | Outcome |
|---|---|
| Content-Type malformed, empty or over 255 bytes, whatever the channel declares | dropped |
| Text Content-Type (§2.1.2's rule), payload not UTF-8 | dropped |
| Text Content-Type, payload UTF-8 | typed text body |
| Any other well-formed Content-Type | non-text body, byte-exact |
| No Content-Type, channel declares none, payload UTF-8 | untyped text body |
| No Content-Type, channel declares none, payload not UTF-8 | dropped; the log line names `untyped_content_type` |
| No Content-Type, channel declares a non-text type | non-text body of that type, UTF-8 or not |
| No Content-Type, channel declares a text type | text body of that type if UTF-8, else dropped |
| Payload over `max_body_bytes` | dropped |

The bus's text rule, not the wrapped shape's classification, decides text on a
raw channel: a UTF-8 payload sent as `application/json` is a typed text body
there, and still a placeholder on a wrapped channel. An empty payload is an
empty body of whatever kind its typing gives.

**Webhook on a raw channel.** The body is typed with the endpoint's content
type, normalized as endpoint resolution normalizes it — no parameters from the
sender's header, which stays in `headers`. The pre-authentication UTF-8 refusal
(400 `{"error":"schema"}`) applies when that type is a text type by §2.1.2's
rule, and not otherwise; the default endpoint type, `application/json`, is a
text type, so an endpoint that takes arbitrary bytes declares a non-text type
such as `application/octet-stream`. Authentication and the replay check run
over the bytes as received. A raw endpoint states a `transport_ceiling_bytes`
of at most `messaging.max_body_bytes` — the default ceiling, 1 MiB, is over the
default cap — and its content type must be a media type the bus accepts;
config-check, boot and reload refuse an endpoint that breaks either, so the
existing 413 is the refusal a sender meets for a body over the cap. During a
reload's commit an endpoint of the old document can briefly serve requests
against the new document's channel; a request whose shape, size or type then
does not fit the channel is answered 500 with a `warn` and nothing is stored.
On an endpoint without replay protection the sender's retry meets the new
endpoint. On a replay-protected endpoint the replay check has already recorded
the delivery when the 500 is answered, so the retry is refused 409 as a
duplicate and raises a replay security event: the message is lost, as with any
500 answered after the replay check. A default endpoint's responses are
unchanged on every path.

**Changing a channel's shape.** Widening a channel to raw is a tuning change
like any other: a reload applies it when every subscriber on the channel is
moving, and otherwise needs a restart. Narrowing a raw channel back to wrapped
is refused by a reload with `NEEDS_RESTART`, since the transport keeps
publishing through a reload. At the restart, a channel that is text-only and
still holds a non-text message, visible or parked, refuses the start, as a
declared channel does (§2.1); for a system-minted address the remedy is to keep
`non_text = true` in the tuning block or to delete the channel's stored
messages. `untyped_content_type` changes the same way: a change to the word is a
tuning change, and removing it together with `non_text` is a narrowing. A
dynamic `mqtt:` channel minted by a running session takes the tuning the
process booted with, `non_text` and `untyped_content_type` included, from its
first message; a retune reaches it at the next restart.

A shape change rewrites nothing already stored. After widening, the window
holds wrapper messages without `transport` and raw messages with it until the
old ones age out; a reader tells them apart by the key, and the SDK accessors
name the other shape when asked for the wrong one. After a narrowing that the
restart admits — only text bodies retained — a 0.1.0 component may bind the
channel and receives raw text messages whose `envelope-json` carries
`transport` and whose body is not a wrapper. Deleting the channel's stored
messages starts it clean in either direction.

### 2.7.2 Publishing to MQTT: `MqttSend`

`MqttSend` is a conversation's publish to an `mqtt:<client>:<topic>` address.
A call gives its payload in exactly one of three arguments, `body`,
`body_base64` and `attachment`, read in four forms:

| Form | Arguments | Bytes published | MQTT 5 Content Type |
|---|---|---|---|
| text | `body`: a string | the string's UTF-8 bytes, verbatim | `content_type`, if given |
| JSON | `body`: an object or an array | the value's JSON text | `content_type`, if given |
| base64 | `body_base64`: a string | the decoded bytes | `content_type`, if given |
| attachment | `attachment`: `{"message_id": …, "name": …}` | the blob's bytes | the blob's recorded content type |

Text is published as given. A JSON value is published as its serialization
(`serde_json`, compact), with no promise about key order or number spelling;
neither form adds a Content Type, so the same JSON given as text and as a
value differs at most in its spelling. A number, boolean or null `body` is refused at
admission: its JSON text, as a string, says the same thing without being
re-spelled. An object `body` with a top-level `binary_base64` key is refused
whatever else it holds, naming `body_base64`, so a caller of the retired
binary object form never sends base64 text to a device as JSON; a JSON
payload that itself has such a key is given as its text in `body_base64`.
`body_base64` is for exact bytes the conversation composes itself.
`content_type` is a top-level argument beside `body` or `body_base64`, and
must be a string the bus accepts as a content type (§2.1.2); no Content Type
is sent when it is absent. The attachment is for an
attachment of a message the conversation was shown: it is named by
(message id, attachment name) under the rule of §2.1.2a, refused with the
same `reason` kinds as a `BrennSend` forward (`malformed_message_id`,
`malformed_attachment_name`, `not_read`, `channel_not_readable`,
`not_retained`, `no_such_attachment`, `tool_channel_attachment`), and its
bytes never pass through the model's context; `content_type` beside it is an
argument error, since it publishes its own. A call carrying more than one of
the three payload arguments, or none, is an argument error.

A file of the conversation's own reaches MQTT through a channel: the
conversation sends it to a channel it may publish to with `brenn-tool send`
(`docs/agent-tool-cli.md`), which answers with the new message's id, and
names the attachment from that answer in `MqttSend`'s `attachment`.

**The lifetime.** `lifetime_s` is required: a whole number of seconds from 1
to 4294967295, or the string `"forever"`. It is how long Brenn keeps trying to
hand the message to the broker, counted from the commit, or from
`deliver_after` when one is given, and what is left of
it when the message is handed over is sent as the MQTT 5 Message Expiry
Interval, rounded up to whole seconds. `"forever"` sends no expiry. A missing,
zero, negative, fractional or otherwise malformed value is an argument error
naming both forms.

**The reply address.** `reply_to`, optional, names a topic as a full
`mqtt:<client>:<topic>` address and is sent as the MQTT 5 Response Topic, so a
responder knows where to answer. It is admitted only when it is a concrete
topic name (no wildcard), on the client the message is published through, and
covered by the app's own `acl publish` entries: a sender needs publish rights
on its own reply topic, so that it cannot get a responder to publish where it
may not. A malformed or wildcard address and one on another client are
argument errors; a topic the ACL does not cover is refused with the security
signal a denied target raises. The topic is stored with the message in its
`MqttPublish` transport metadata, not as the envelope's `reply_to`, which
names a bus channel. No Correlation Data is sent, so a sender matches a
response to its request by the response's payload; an inbound PUBLISH's
Response Topic and Correlation Data are not read (`TODO.md`'s
`mqtt-correlation-data-and-inbound-response-topic`). The Response Topic counts
toward the packet size the broker's maximum is checked against.

**A scheduled publish.** `deliver_after`, optional, is an RFC 3339 time in
`BrennSend`'s forms: absent, `null` or `""` is none, and a time at or before
now publishes at once. A later time parks the message on the client's egress
channel with the bus's ordinary deferral (§2.4): it is committed, admitted and
charged its send-rate token and budget unit at the call, holds no place in
the channel until its release time, and is invisible to the reader until
then. At release the dispatcher moves it into retention, where it takes its
place in channel order after whatever was committed before that moment, and
the reader hands it on like any commit. Its lifetime counts from the stated
release time, so its expiry is fixed at the commit; release rewrites nothing.
The dispatcher releases a moment after the stated time and the reader then
needs a connection, so a lifetime of a second or two can expire unsent, and a
server down across the whole of `[deliver_after, deliver_after + lifetime]`
releases the message at its next start and reports it `expired`. Its
statuses, when it names a `status_to`, begin at release. Until release the
sender's `BrennPendingList` lists it — its `transport` names the client, the
topic and the expiry — and `BrennMessageCancel` withdraws it by the
`message_id` the call returned, under the lock every release takes, so a
cancelled message is never retained, never handed and told nothing;
`BrennMessageEdit` refuses it (cancel and send again). The three tools serve
an app holding only the `mqtt_publish` grant. `BrennPendingList`'s `channel`
filter never names an egress channel; the unfiltered list shows these
messages. A client removed while its channel holds a parked message reports
it `dropped`, `removed`, as an unread one (§2.7.3, *Reload*). The client's
deferred cap is its egress channel's retain depth, shared by every sender to
that client, so one sender can fill it; a call over it is refused naming the
cap and refunds its budget unit. Admission happens once, at the commit: an
app whose ACL is narrowed, or that is removed from the configuration, after
it parked a publish still has it released and sent; the operator's recall is
removing the client, or the message's row. There is no bound on how far
ahead `deliver_after` may be, as for `BrennSend`.

**Commit, not delivery.** A call that passes every check is committed to the
client's egress channel (§2.7.3) and answers at once:
`{"ok": true, "message_id": "<uuid>", "remaining_budget": n,
"client_connected": bool}`. `client_connected` is whether the client's session
is connected at that instant; when it is not, the answer also carries a `note`
naming the session's health and saying the message is committed and waits for
a connection within its lifetime. A client that is not connected is not an
error. What happens to the message afterwards — delivered, refused by the
broker, expired, lost with a connection — is never in the answer. Without a
`status_to` it is told to the operator, not to the caller (§2.7.3). With
`status_to` naming a `brenn:` channel the app may publish to, Brenn publishes
the message's delivery statuses there, and the caller reads them with an
ordinary subscription (§2.7.3, *Delivery status*). At QoS 1 a message may be
delivered twice after a reconnect.

**The bound.** Each MQTT client publishes at most
`outbound_payload_cap_bytes` (default 4 MiB, `docs/config-dsl.md`) per
`MqttSend` call, measured on the bytes published: the string's UTF-8 length,
the serialized length of a JSON value, the decoded length of `body_base64`,
the attachment's recorded size. A call
over it is refused with `reason` `payload_too_large`. A component's
`mqtt-publish` is held to this bound as well as to `[messaging] max_body_bytes`,
and a payload over either answers `invalid-payload`.

**The carrier.** A message's payload is its body when it fits
`[messaging] max_body_bytes`. An inline payload over that cap is written to
the blob store and committed as one attachment named `payload` beside an
empty body; an attachment payload is committed as the attachment it names.
Either way the bytes published are the payload's and the Content Type sent is
the one the call stated, or the attachment's recorded type. With no blob store
configured an over-cap inline payload is refused with `reason`
`payload_unstorable`, as is one the blob store refuses, naming the store's
reason.

**Order.** A call is checked in this order, and every refusal before the
budget spends none: the arguments; the attachment's admission, which takes a
hold on the blob and reads no byte; then the bus's admission — the address,
the app's `mqtt_publish` grant and per-topic ACL, the client and its egress
channel, the `reply_to` when one is named, the `status_to` when one is named,
QoS and content type, the
payload's shape, the bound, the body cap's
need for a blob store; the send rate; the send budget; the write of an
over-cap inline payload to the blob store, whose refusal refunds the budget
unit; then the commit, or for a future `deliver_after` the park, which checks
the reader's position and the client's deferred cap in the same step as the
write and refunds the budget unit on either refusal. A denied call learns nothing of the client's bound, and a call
refused on its target answers the same whether or not it names a `reply_to`. A
client the server does not configure is answered as not configured, which is
not a security event. A source attachment's message evicted after admission
does not affect the call: the hold keeps the bytes.

**The broker's maximum packet size.** When the broker's CONNACK advertises a
Maximum Packet Size, a packet that would exceed it is never sent, and the
session stays up. The message, `MqttSend`'s or a component's, is already
committed: it is reported as rejected — to the operator, naming the packet's
size and the broker's maximum, or on its `status_to` as `rejected` with no
reason code. A broker that advertises no maximum arms no check.

**A component's publish.** A component's `mqtt-publish` and
`mqtt-publish-expiring` go through the same egress channel. The call is
admitted at once by the same bus admission as `MqttSend` — the grant and
per-topic ACL, the client and its channel, QoS and content type, the bound —
after the component's own per-activation checks, and buffered with the
activation's other publishes. When the activation returns ok the buffer is
committed together: its MQTT publishes and its publishes to durable channels
in one transaction, in call order. When it errs or traps the buffer is
discarded. The call answers the admission and never a broker outcome; what
happens after commit goes to the operator (§2.7.3), with `wasm:<slug>` as the
sender: a component cannot name a `status_to` or a `reply_to`. `mqtt-publish` is committed with no lifetime and
`mqtt-publish-expiring` with a lifetime of its seconds, counted from the
commit. Nothing in the call waits on the broker or its session.

**A recurring automation's publish.** An automation job (`AutoCreate`,
`AutoEdit`) can fire an `mqtt_publish` action: a `to` of the form
`mqtt:<client>:<topic>`, a payload fixed when the job is created or edited —
text, a JSON object or array stored as its JSON text, or bytes given as
base64; never an attachment, since a job that fires for months would have
to hold a blob — and `MqttSend`'s `content_type`, `qos`, `retain`,
`lifetime_s`, `status_to` and `reply_to`. Its arguments are read by the same parser as
`MqttSend`'s; `deliver_after` is refused by name, since the job's trigger is
its time. At create and edit the action is put to the same bus admission,
spending nothing, under the owner app; a refusal is answered as `MqttSend`'s
is, security signal included, and the payload is also held to the body cap.
At each fire the engine commits the message through the same admission as
`MqttSend`, under the owner app, with the lifetime counted from that commit;
the fire is `ok` at commit and never reports delivery, which goes to the
operator or to the action's `status_to`. A refused fire is recorded with an
outcome naming the cause — `auth` for a grant, ACL or client that is gone
(an ACL that no longer covers the stored `reply_to` included),
`action_error` for an argument the admission no longer accepts or the send
rate — counts toward the job's
consecutive failures and auto-disable, and raises the job's error report.
With `persistent` set, a slot missed while the server was down fires once at
restart, late, and a slot whose fire was cut off by a crash between commit
and bookkeeping fires again, so a device command should set a state rather
than toggle one.

**What bounds a fire.** An automation fire, `brenn:` or MQTT, draws no
conversation's send budget: the tick reacts to nothing, and it fires with
nobody attending. Writing the job (`AutoCreate`, or an `AutoEdit` of its
action or trigger or one that enables it) is the reaction, and draws one unit
of the writing conversation's send budget. The engine bounds fires itself,
with three caps from the configuration's `automation` block
(`docs/config-dsl.md`, "Automation limits", gives their defaults):
`max_jobs_per_app`, `max_fires_per_hour_per_job` and
`max_fires_per_day_per_app`, the last counting all of one app's
fires over a sliding 24 hours, kept apart from the jobs so that deleting or
re-creating a job, or a restart, refills nothing. A fire over the per-app cap
is recorded with the outcome `app_fire_cap` and its slot is skipped, not
queued; it does not count toward the job's consecutive failures, and the job
fires again by itself at its next slot at which the window has room. The first
such refusal of an app in any 24 hours is reported to the owner's
conversation, whichever job it was, and later ones are not. A `send_message`
fire delivered into a conversation still costs that conversation one unit for
the turn it provokes (§7 of `docs/chat-protocol.md`). The engine is a host
cron loop that the rule against a scheduler subsystem excludes, kept for now
and not a sanctioned exception; its replacement by component-owned schedules
is `TODO.md`'s `scheduling-component-managed`, and it must carry an equivalent
bound (`docs/security-posture.md` §7.7).

### 2.7.3 MQTT egress

Every `[[mqtt_client]]` has one egress channel, `brenn:egress/mqtt/<client>`,
minted with the client at boot and by a reload that adds it. It is a durable,
`non_text` channel the host alone writes: `MqttSend`, a component's
`mqtt-publish` and an automation's `mqtt_publish` fire commit to it, and no publish ACL, component output or surface
output may name it. A component's
`in` port may read a declared client's channel. Each message carries the
publish in the envelope's `transport` key — `{"mqtt_publish": {"client_slug",
"topic", "qos", "retain", "content_type", "expires"}}`, where `expires` is
`{"at": "<rfc3339>"}` or `"never"`, `content_type` is absent when the call
stated none, `status_to` is the canonical `brenn:` address the call named
for its delivery statuses, absent when it named none, and `response_topic` is
the bare reply topic sent as the Response Topic, absent when the call named no
`reply_to` — and its payload as the body or as one attachment (§2.7.2).

**Admission.** `MqttSend`'s publish, and an automation's fire, is admitted by
the bus as any publish is,
against the client's channel: authority, the client and its channel, the
MQTT checks, the body cap, the send rate (drawn per sender and client) and the
budget. A client being added or removed by a reload can be absent for a
moment; a publish in that moment is answered as for an unknown client. The
commit needs one thing more: the reader's position on the channel, checked in
the same step as the write. A removed client's channel takes commits until
the reload closes it (Reload, below), and every one of them is then either
published or reported; from the close on, a publish is answered as for an
unknown client, whatever the directory and the client table still say,
including a publish admitted before the close and committed after it.

A component's publish is admitted by the same checks at the call, and
committed at the activation's flush with no send rate drawn: the component's
per-activation ceilings and the client's token bucket bound it instead. A
component never meets a closed channel: a consumer can name only declared
clients, a reload that removes a client changes every consumer naming it, and
such a consumer is stopped, its activation in flight flushed, before the
client's reader is retired. A flush that finds no reader's position on the
channel is a host bug and panics.

**The reader.** Each client's session has one reader, a host task holding the
system participant `system:mqtt-outbound`'s position on that client's channel.
It hands the channel's messages to the session in channel order, one cursor,
no other subscriber's business. It reads only while the session is connected.
For each message it first commits the `handed` status, when the message names a
`status_to` and its lifetime has not passed; then moves the cursor past the
message, durably; then hands it to the MQTT library, or reports it expired. So
a crash after the cursor moves, or while the message is in flight, loses it
and reports nothing: delivery is at most once across a crash. What a crash can lose is bounded by the in-flight limit.

**The in-flight limit.** The reader keeps at most
`min(Receive Maximum, window)` messages handed and not yet resolved: the
broker's Receive Maximum from the current connection's CONNACK (65535 when it
states none) and the channel's retain depth. Both are someone's stated
intent — the broker's of what it will hold unacknowledged, the operator's of
how many of this client's messages may be pending at once — and handing more
moves messages from the durable channel into memory for no throughput. The
count covers every QoS, which keeps channel order.

**Lifetime.** A message's lifetime is judged when the reader reaches it, on
the wall clock: one whose `expires` has passed is stepped over and reported
expired, never sent; otherwise what is left is sent as the Message Expiry
Interval, rounded up. A message waits on its channel while the client is
disconnected and is judged when the client is next connected, so an expired
message is reported then, not at the instant it expired. Lifetime is judged
at hand-off only: a message already in flight when the connection is lost can
be sent after its lifetime (*What the MQTT library decides across a
reconnect*, below).

**Outcomes and who hears of them.** The call answered at commit and reports
none. A message that names a `status_to` has each outcome published there
(*Delivery status*, below). The operator hears of every outcome but a
delivery: each is a `warn` line naming the client, the kind, the cause or the
broker's reason, the message id, the sender and the topic, and a count per
client and kind; and a `Warning` alert, unless the outcome was published on
the message's `status_to` and was not a broker's rejection:

| Outcome | When |
|---|---|
| delivered | the broker acknowledged it (QoS 1, 2), or it was written (QoS 0); logged at `debug` and counted, no alert |
| rejected | the broker refused it with a reason code, or its packet is over the broker's Maximum Packet Size |
| expired | its lifetime passed before the reader handed it over, or before it handed over again a message it held for resubmission |
| unknown | its session ended before the broker answered (QoS 0, 2): the broker kept no session across a lost connection, or the client stopped or failed while disconnected; or the session stopped and the broker did not answer within the client's `stop_wait_secs` |
| dropped | the session stopped, or its client was removed, with messages the reader held or still owed; reported as a count, and one by one for those naming a `status_to`. Nothing is handed or resent once a stop begins |

A broker's rejection is always alerted, whether or not its sender was told:
it means Brenn's per-topic ACL admits a topic the broker's configuration
forbids, which only the operator can fix, and naming a `status_to` cannot
silence it. Its alert says when the sender was told. Every other failure told
on its `status_to` — an oversize packet's rejection among them — is logged and
counted and raises no alert. One with no `status_to`, or whose status could
not be published, is alerted.
Alerts are coalesced per client and kind: the first failure of a kind raises
an alert and opens an episode; later failures of that kind are logged and
counted only; the client's next delivery, its reader running dry, or its
removal by a reload closes every open episode and raises one summary alert per
episode that withheld any, with how many. A reconnect that finds 200 expired messages raises two
alerts, not 200.

A status that could not be published (*Delivery status*, below) is a `warn`
line naming the client, the message id, the status kind, the address and the
refusal; a count per client; and an alert under a fifth kind, *status not
published*. That episode closes only when the reader runs dry or the client
is removed, never on a delivery or a published status: a throttled status channel refuses only the
excess, and one client serves many senders, so refused and published statuses
interleave, and closing on either would raise an alert per alternation. A
cause that outlasts the run alerts again in the next.

**QoS across a reconnect.** A client keeps its MQTT session state across a
lost connection and asks the broker to keep the broker's half for the client's
`session_expiry_secs` (`docs/config-dsl.md`, `mqtt_client`). What becomes of a
message in flight at the loss depends on whether the broker kept the session,
which the next CONNACK says:

| In flight at the loss | The broker kept the session | The broker kept no session (expiry 0, expired, or broker restarted) |
|---|---|---|
| QoS 0, already written | reported `delivered` at the write, as always | same |
| QoS 0, accepted but not yet written | sent on the new connection | `unknown`, `connection_lost` |
| QoS 1 | replayed by the MQTT library under its original packet identifier; `delivered` or `rejected` as the broker answers; a subscriber may see it twice if only the acknowledgement was lost | sent again as a new publish, ahead of anything newer, under its lifetime check; a subscriber may see it twice |
| QoS 2 | the exchange is completed where it stopped; the subscriber sees it exactly once | `unknown`, `connection_lost`; not sent again |

An inbound QoS 1 message can likewise arrive twice across a loss. Nothing new
is handed while the client is disconnected. The in-flight limit counts what a
resumed session carried over, so after a resume the reader hands a new message
only once what is in flight is below the new connection's limit. The library's
replay of a PUBLISH does not set the DUP flag, which MQTT 5 requires on a
re-delivery (MQTT-3.3.1-1); it does not change what a subscriber receives, and
Brenn cannot set it.

**What the MQTT library decides across a reconnect.** Two limits of a kept
client, stated here and only here:

- *A replay is not recalled.* A message already in flight when the connection
  is lost is replayed when the session resumes, however long that takes, with
  the Message Expiry Interval it was first sent with. Brenn judges lifetime at
  hand-off only and cannot recall one publish from the MQTT library without
  giving up the session (`TODO(mqtt-expired-replay-not-stopped)`). A sender
  for whom a late delivery is worse than none publishes with a lifetime the
  broker will enforce and expects that an outage can stretch it.
- *A received message waits for the next connection.* A message the library
  had received when the connection failed and had not yet passed on is
  delivered into its channel when the client next connects. If the client
  ends before it connects again — the server stops, the client's
  configuration is reloaded, or the client fails for good — that message is
  dropped with the client, and the drop is logged at `warn` with the number of
  received messages dropped. A crash logs nothing. At QoS 1 on a kept session
  the broker sends it again to the next client unless its acknowledgement had
  already gone out; at QoS 0, or with no kept session, it is lost, as a
  message published while Brenn is down is. Brenn's MQTT library acknowledges
  a QoS 1 message when it receives it, before the message is in its channel,
  so an acknowledged message can be among those dropped
  (`TODO(mqtt-inbound-acked-before-routed)`).

**An unsolicited acknowledgement.** When the broker acknowledges or releases a
packet identifier the client's session state does not hold, the MQTT library
ends the connection, and on a kept session it would do so at every reconnect.
Brenn discards the session once: it logs the fault at `error`, raises a
`warning` alert, and connects with a clean start. The broker's queued messages
for the client are lost, publishes in flight end as in the column above where
the broker kept no session, and every filter is subscribed again. If the
connection that clean start made ends the same way, the client stops — its
health reads `failed` and a `critical` alert says so — until its
`mqtt_client` block is reloaded or the server restarts. Any other ending of a
connection in between rearms the single discard.

When a client's
configuration changes, its session is stopped and replaced: what was in
flight is waited for, up to the client's `stop_wait_secs`, and reported as
what it turned out to be; only what the broker had not answered within it is
reported unknown. What the reader held for resubmission is dropped and
counted, not handed to the successor. The successor's reader resumes from the
same cursor.

**Stopping.** A session stops for a server shutdown, a configuration change
or its client's removal. From the moment the stop is requested the reader
hands nothing: what it has not read stays on the channel, ahead of the
position. The one message it may be in the middle of handing as the stop
lands is either taken by the client, and waited for like the rest, or finds
the session already letting go of its connection and is reported `dropped`,
`stopped`. It waits for the broker's answers to what is already in flight, and
reports each as it comes, while the connection is serviced as usual —
acknowledgements read, inbound messages routed, keep-alive kept. The wait ends
when nothing is in flight, when the connection ends, or after the client's
`stop_wait_secs` (60 by default), whichever is first; what is still
unanswered then is reported unknown. Only after the reader's last report has
committed is the DISCONNECT sent. A stop with nothing in flight does not
wait. A server shutdown stops every session at once and does not exit until
each has finished.

**Overflow.** The egress channel is a window like any other: an outage longer
than the window drops the oldest. Window overflow is the bus's to report, not
the reader's: the participant's subscriber entry on each egress channel
carries the `Alarm` rung, so the hourly eviction pass — which runs whether or
not the client is connected — and the reader's own advance each count what
the position was outrun by into the channel's drop tally, write a `warn` line,
and raise a `Warning` alert naming the channel and the count. The store says
how many, not which. A client that is down longer than the window is reported
within one eviction pass of the loss.

**Sizing.** The default window is 256 messages. A prefix block
`channel at prefix "brenn:egress/mqtt/" { … }` tunes every client's; an exact
block `channel at "brenn:egress/mqtt/<client>" { … }` tunes one, and must name
a declared client. The window is the outage to survive, counted in this
client's publishes, and also the in-flight ceiling. An activation commits its
publishes at once, so a window smaller than one activation's publishes to the
client drops the oldest of them even while the client is connected. A retune
needs a restart:
a reload that changes an egress channel's tuning is refused. The channel is
durable and the cursor survives, so the restart loses nothing.

**Reload.** A reload that adds a client mints its channel, gives the reader a
position on it and starts the reader before any policy can name the client.
One that removes a client retires the reader before the channel leaves: the
reader hands nothing more and reports what it had outstanding. The reload
then closes the channel: in one step with respect to commits, parks and
releases it drops the position, takes everything the channel held unread and
removes every message still parked on it, whoever sent it, and reports all of
it `dropped`, `removed` — by count to the operator, and to each message's
`status_to` when it names one: the unread in channel order, then the parked in
release order. A release before the close lands among the unread; a park after
it is answered as for an unknown client. The subscriber entry is removed, and the
channel leaves the directory. The close is the same whether or not the reader
was still running, so a removal that races a server shutdown is reported the
same way. A reload that removes or changes a client waits for that
client's stop (Stopping, above): the removed clients' readers are retired
together, each waiting up to its client's `stop_wait_secs`, and then the
changed clients' sessions are stopped together, under the same rule. A reload
that both removes and changes clients against brokers that do not answer can
therefore wait up to two bounds. A client deleted from the configuration
while the server is down is not reported: no reader is started for it at the
next start, and what its channel holds, retained or parked, is told nothing
(`TODO(mqtt-egress-removed-client-rows)`); a parked message there is never
released, since its channel is not in the directory. The removed
channel's stored row and retained messages stay in the database
(`TODO(mqtt-egress-removed-client-rows)`). One that changes a client keeps the
channel and the cursor and starts a reader on the successor session.

**A fresh position.** A position created on a channel that already retains
messages — a client added again under a slug that was removed — skips them:
they were committed for the earlier client and are not published. Each was
already reported, an outcome while its client ran or `dropped`, `removed` at
the removal, except what a client deleted while the server was down left
unread (Reload, above). The same position first deletes every message still
parked on the channel, which only such a client can have left: a parked
message would otherwise be released above the new position and published to
the new client's broker. The deletion comes before the skip, so a release
racing the open is either skipped or finds nothing to release. Both are logged at
`warn` with their counts, and neither is reported to a sender. A restart resumes an existing position and
hands off exactly what was not handed.

#### Delivery status

A sender learns what became of its publish by naming a `status_to`: a `brenn:`
channel it could publish to itself. The host publishes there `handed`, then
one outcome. It is the bus's own idiom — an outcome is a fact, a fact is a
message on a channel, the sender reads it with an ordinary subscription — and
there is nothing to wait on or poll. `MqttSend` takes a `status_to`; a
component cannot name one.

**Admission.** A named `status_to` is checked after the publish's target and
before its QoS and payload, and each refusal commits nothing and spends
nothing: it must be a well-formed `brenn:` address; the app must hold the
`publish` grant and its `brenn_publish` ACL must admit the channel — the
publish right, not visibility: a channel it may only read is refused; the
channel must exist; and the largest status about a message to this topic must
fit `[messaging] max_body_bytes`. Outside the ACL and not existing answer with
one string, as a `BrennSend` denial does, and both, like an absent grant, are
security events; the malformed and over-cap refusals are argument errors.

**The body.** A status is untyped JSON text from sender
`system:delivery-status`:

```json
{"v":1,"message_id":"<uuid>","kind":"rejected","reason":"Not authorized",
 "mqtt":{"client":"ha","topic":"lights/k/set","qos":1,"reason_code":135}}
```

The top level is general: `v`, `message_id` (the id the publish answered),
`kind`, and `reason` (`rejected`) or `cause` (`unknown`, `dropped`). `mqtt` is
the transport's member — client, topic, QoS, and the broker's MQTT 5 reason
code when a broker refused the message, on `rejected` and on `unknown` with
cause `resend_refused` — and a status need not carry it. Kinds are closed
in v1: a new kind moves `v`. Causes are open: a reader treats a cause word it
does not know as some other cause. Unknown fields are ignored.

A reader believes only `sender` `system:delivery-status`: the channel is by
rule one the publishing app may itself write, so anything else on it may be
the app's own traffic. The sender says the host wrote a status, not whose
message it concerns. Every app whose `brenn_publish` ACL admits a channel may
name it, and every conversation of one app publishes as that app, so a reader
matches `message_id` against the ids its own publishes returned and ignores
the rest.

| kind | cause | meaning |
|---|---|---|
| `handed` | | the message went to the MQTT library |
| `delivered` | | the broker acknowledged it (QoS 1, 2), or it was written to the connection (QoS 0: a broker cannot refuse a QoS 0 message, and one that will not take it drops it unannounced) |
| `rejected` | | the broker refused it, `reason` its reason string or its reason code's name; or its packet was over the broker's Maximum Packet Size, with no `reason_code`; no earlier copy of it had been accepted |
| `expired` | | never sent: its lifetime passed before the MQTT library accepted it |
| `unknown` | `connection_lost` | may have been sent: its session ended before the broker answered (QoS 0, 2), and it is not sent again. The broker kept no session across a lost connection, the client stopped or failed while disconnected (the broker may still hold the session, and the exchange with it), or a QoS 0 write failed |
| `unknown` | `lifetime_ended` | may have been sent: the library accepted it, its session ended before the broker answered (QoS 1), and its lifetime passed while it waited to be sent again |
| `unknown` | `resend_refused` | may have been sent: the library accepted it, its session ended before the broker answered (QoS 1), and when it was sent again the broker refused it (`reason_code` is the broker's) or its packet was over the new connection's Maximum Packet Size (no `reason_code`); the broker's reason string goes to the operator's log, not into the status |
| `unknown` | `stopped` | may have been sent: in flight when the session stopped or its client began to be removed, and the broker did not answer within the client's `stop_wait_secs`; or, at QoS 1, accepted and waiting to be sent again, because its session ended first, and not sent again |
| `dropped` | `stopped` | never sent: the library never accepted it, and the session stopped or its client began to be removed while the reader held it; it is not handed during the stop |
| `dropped` | `removed` | never sent: not yet read when its client was removed |
| `dropped` | `left_retention` | its payload left the blob store before it was handed over |

**What a sender may infer.**

- `expired` is reported when the reader reaches the message, and nothing is
  read while the client is disconnected, so it comes at reconnect, not at the
  moment the lifetime ends. A message first reached past its lifetime gets
  `expired` and no `handed`.
- `expired` and `dropped` mean the message was not sent, whether or not a
  `handed` came first. A `handed` followed by `expired` or `dropped` is a
  message the MQTT library never accepted: the library had no room for it, or
  the connection went away between the `handed` and the hand-off. A message
  that ended without being delivered, and of which the library accepted a
  copy the broker never answered, is `unknown`, whatever its QoS, with its
  cause saying why. `rejected`, `expired` and `dropped` are said only of a
  message with no such copy.
- One `rejected` is wrong. At QoS 2, a `rejected` whose `reason_code` is 146
  (Packet Identifier not found) is the broker failing the last step of an
  exchange whose first step it had accepted: the broker took the message
  (`TODO(mqtt-qos2-completion-refusal-told-rejected)`).
- With a session expiry above 0, a `handed` can be followed by its outcome
  only after the client reconnects, however long that takes: a message in
  flight at a loss waits for the resumed session's answer.
- `delivered` at QoS 0 says the message was written to the connection, not
  that the broker took it. A sender that needs the broker's answer publishes
  at QoS 1 or 2.
- `handed` with no outcome after a server restart means the server did not
  stop cleanly — it crashed or was killed — and nobody knows. A clean stop
  commits an outcome for every `handed` before the process exits.
- No `handed` by the end of the lifetime means the message was not sent —
  except when the `handed` status itself could not be published, in which
  case the message may have been sent and the operator, not the sender,
  knows. A sender for whom a duplicate is worse than a loss does not resend
  on a missing `handed` alone.
- An outcome can arrive without its `handed` when only the `handed` was
  refused.
- A crash between the `handed` status and the cursor sends the message after
  restart with a second `handed`.
- Silences. A message the egress channel's window dropped before the
  reader reached it gets no status: the store reports overflow as a count. A
  message in flight when the server crashed has a `handed` and never an
  outcome, and a server that dies while a removal is being reported leaves
  the rest of that removal unreported. And nothing is published while the client has no connection: `expired`
  comes when a connection next lets the reader reach the message — hours
  later if the client is down that long, never if it stays down or the window
  drops the message first. What a removed client's channel still holds
  unread or parked is told nothing when the client is deleted from the configuration
  while the server is down (Reload, above;
  `TODO(mqtt-egress-removed-client-rows)`); a message sent with `lifetime_s`
  `"forever"` then has no end of lifetime to infer from. A clean shutdown is not a silence: it waits for every session's
  reader and commits every status before the process exits.

**When a status is not published.** A status is published at report time on
the sender's authority as it stands then: the app named in the stored sender
must still exist, hold `publish`, and admit the channel in its
`brenn_publish` ACL; the channel must still exist; the channel's send rate
and body cap apply. A status refused for any of these is never retried and
never holds a message back: the operator is told (*status not published*,
above), and the outcome of a failure whose status was refused is alerted as
though no `status_to` had been named.

**Sizing.** The status channel's window is the sender's to size; a status
that falls out of it before it is read is gone. Its `send_rate` is drawn
under `system:delivery-status` and is shared by every sender naming that
channel: it must cover two statuses per publish into it. A conversation
subscribed to its own status channel with a push depth is woken by its
statuses.

### 2.8 The reload pair

Two channels are the config-reload facility, and they are **declared, not
minted**: `brenn:config.reload` and `brenn:config.status`. Nothing in the system
brings them into existence — a deployment stamps `ConfigReload()` from brenn's
`@config-reload` library module, or it has no reload facility. Both, or neither:
a document declaring one without the other is refused at boot.

| address | kind | shape |
|---|---|---|
| `brenn:config.reload` | signal | push 1; short window. Request N + 1 subsumes N — every request means "converge to whatever is on disk now" — so requests arriving while a reload runs coalesce into one further reload. The body is not read; publishing anything is the request. |
| `brenn:config.status` | state | push 1, retain 1, deeper standing. The retained head is the last outcome; the standing tail is the last few, for an operator reading backwards through a refusal. |

Declared rather than minted for two reasons. The addresses have to be fixed —
there is one process and one document, and the agent that asks for a reload
cannot read configuration to learn a name — but everything *else* about them is
the deployment's: the windows are sized at the stamp, and access is granted with
ordinary ACLs. An agent that may trigger a reload is one the deployer wrote
`publish` on the request channel for, and nothing else in the system can. A
minted pair would have put that authority somewhere other than the ACL that
every other authority in the document is written in.

The participant on them is `system:config-reload`, built by the host when the
pair is declared: it subscribes to the request channel and publishes to the
status channel, and to neither in the other direction.

The status body is JSON and an additive external contract — an LLM reads it —
so fields are added, never renamed or removed, and `v` bumps only on an
incompatible reshape:

```json
{
  "v": 1,
  "outcome": "booted" | "applied" | "unchanged" | "refused",
  "trigger": "boot" | "bus" | "signal",
  "generation": 3,
  "at": "2026-09-04T12:00:00Z",
  "document_sha256": "…",
  "root": "/home/brenn/config/brenn-prod.brenn",
  "running_document_sha256": "…",
  "delta": { "consumers_added": [], "consumers_removed": [], "consumers_changed": [],
             "channels_added": [], "channels_removed": [], "channels_changed": [],
             "channels_described": [] },
  "refusals": []
}
```

`running_document_sha256` is the document the process is *projecting*;
`document_sha256` is the one the outcome was about. A reader answers "is this
process running what is on disk" by comparing the retained
`running_document_sha256` against `brenn config-check`'s hash of the tree. Boot
publishes `booted` with `generation: 0`, so the retained state always describes
the process now running, and `generation` counts applied reloads since that
boot. What a reload will and will not converge is `docs/config-dsl.md`.

### 2.9 The agent-status channels

Three channels carry what the host knows about its Claude Code sessions and the
Claude accounts they spend, and like the reload pair they are **declared, not
minted**. A deployment stamps `AgentStatus()` from brenn's `@agent-status`
library module, or declares the three itself. All three, or none: a document
declaring some without the rest is refused at boot.

| address | kind | body |
|---|---|---|
| `ephemeral:agent.sessions` | state, in memory; `retain_depth` at least 1 | `AgentSessions`: every conversation with a running Claude Code process — its app, state, account, model and the cost of this run. Each snapshot subsumes the one before. Empty after a restart, which reads as "no sessions". |
| `brenn:agent.ended` | fact, durable; a window sized for the longest consumer outage | `AgentSessionEnded`: one per run of a session that ended, with its duration and whether the end was intentional. A conversation that wakes again runs under the same id. |
| `brenn:agent.accounts` | state, durable; `standing_retain_depth` at least 1 | `AgentAccounts`: every configured Claude profile, with the month's spend and turn count for an API-key profile and the refusal flags observed on either kind. This is the account ledger's storage, read back at boot. |

The bodies are the JSON types of `brenn_envelope::agent`, each carrying `v`;
instants are epoch milliseconds. The participant writing them is
`system:agent-status`, and other principals reach the channels with ordinary
ACLs. What the ledger counts and how a flag leaves it is
`docs/claude-accounts.md`.

**Matching an end to a snapshot.** When a run ends, brenn publishes the
sessions snapshot that no longer lists it before the run's `agent.ended` fact.
A reader's ports are read one after another, not at one instant, so a reader
bound to both channels can hold the ended fact beside an older snapshot that
still lists the run. The pair `conversation_id`, `started_at_ms` identifies a
run on both channels; match on it.

**The rate.** A system publish draws on no send budget but passes the
per-(sender, channel) send-rate gate. The channels need at least 240 in a burst
and 30 a second sustained. The `AgentStatus` assembly writes that rate on each
channel, so `[messaging] default_send_rate` does not reach them; a hand-written
declaration that states no `send_rate` takes the default, and boot refuses any
of the three below the floor. The sessions channel takes a publish per session
start, per change of a session's state, per costed turn and per end — three to
six per turn of one conversation — and is the one that scales with the host;
`agent.ended` takes one per ended run, and `agent.accounts` one per costed turn
under a key and one per flag raised or dropped. A host busier than its declared
rate must declare a higher one.

**A refused publish.** Any publish the bus refuses on these channels ends the
server process, the send-rate gate's refusal included: a dropped snapshot would
leave false state on the bus, or a ledger that was not stored.

**Do not wake on your own activity.** A subscriber's own activity is in these
snapshots. Subscribe with `push_depth = 0` and read on your own schedule, or
expect to be woken by your own turns: a conversation that pushes on the
sessions channel, or on the accounts channel while it runs under an API-key
profile, runs turns back to back with nobody asking, and on a key each one is
billed.

**The writer.** A reader believes only envelopes whose `sender` is
`system:agent-status`. An operator's `acl publish` matcher can let another
principal write these channels; nobody else should hold it, and Brenn refuses
to boot when the retained accounts message was written by anyone else.

## 3. Signals and facts

The model's real demand on its user is a question it forces at design time:
**does message N+1 subsume message N?**

- **Signals** — temperature, presence, bulb state, position, current setpoint.
  The newest message contains everything the older ones did. Dropping old is not
  a compromise; it is the semantically correct operation, because a stale sample
  has negative value in a control loop.
- **Facts** — dishwasher finished, email arrived, timer fired, "cancel the 1:1."
  Nothing subsumes them. Missing one means a side effect that never happens.

The system does not enforce the distinction, and cannot. What it offers is a
control surface fine-grained enough to express both: short ephemeral windows with
aggressive coalescing for signals, long durable windows with generous push depth
for facts. The sizing discipline in §2.1 is the mechanism by which fact channels
are made safe — not delivery guarantees, but **a window horizon that exceeds the
worst outage you intend to survive.**

This is worth being explicit about, because it is the point most likely to be
mistaken for a weakness. The alternatives for guaranteed delivery of facts are:
an unbounded queue (a promise the universe will not honor), backpressure (you
cannot tell the dishwasher not to finish), or a bounded window sized to your
outage tolerance. The third is the only one that exists. The others are the third
with the sizing decision hidden from the person who ought to be making it.

A useful idiom that falls out: **pair fact channels with state channels.** Emit
`dishwasher.finished` events *and* maintain `dishwasher.state`. An agent that was
down and missed events can reconcile against current state rather than replay
history it no longer has. This is a pattern for the framework's idioms rather
than its plumbing, and it converts most drop scenarios from "lost" to
"recoverable."

## 4. Why this fits agent graphs specifically

**Late joiners are first-class.** An agent authored at 3pm — by an LLM, in
response to "ping me when the dishwasher finishes" — subscribes to a channel with
history already in it. Under a custody model it would arrive to an empty pipe and
would have to be specially bootstrapped. Here it simply looks.

**Observability is free.** A debugger, a logger, an LLM asked "why did that
happen?", a supervisor watching for degradation — all are ordinary subscribers.
They cost the channel nothing, cannot slow the graph, and see exactly the same
history the participating agents saw. In a custody model, observation either
competes for messages or requires a parallel tee-ing mechanism.

**LLM agents want windows, not messages.** The natural unit of input to a
language model is *recent context*, not a single event. `retain_depth` is that
context window, expressed in the transport rather than assembled by each agent
from its own private accumulation.

**The graph is inspectable and mutable.** Because subscriptions are declarative
(channel, push_depth, retain_depth) and the channel is oblivious to them, an
agent that authors automations is adding edges to a graph rather than negotiating
delivery contracts. "Stop doing that" removes an edge. Nothing needs to be
drained, acked, or unwound.

---

## Summary

| Concern | Mechanism |
|---|---|
| History | Channel depth N, rolling window |
| Durability across restart | Ephemeral vs. durable channel |
| Activation | `push_depth`, with coalescing |
| Visibility | `retain_depth`, independent of activation |
| Loss awareness | Per-subscriber `drop_counter` |
| Time | `deliver_after` |
| State | Retained channel + `retain_depth=1` |
| Backpressure | None, by design |

The design's thesis in one line: **stop modeling messages as obligations and
start modeling channels as facts about the recent past, then size the past you
choose to remember.**
