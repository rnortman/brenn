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
| Attach v5 sessions (`Deliver`, `DeferredView`): a remote's, and every surface page's | text and type | bytes and type | never: no session binds an ingress channel |

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
page-local and wire-bound channels alike: the page's wire is attach v5 and
carries every body with its type, and the page refuses a non-text body for a
text-only channel to the component before anything is sent. A transport
adaptor — an MQTT client or a webhook endpoint — onto its ingress channel when
that channel takes the raw shape (§2.7.1): the payload with the publisher's or
endpoint's content type, judged by the rule above, and dropped (MQTT) or
refused (webhook) when it fails it. What still cannot:
a 0.1.0 component, a remote's attach v4 session, a conversation and every host
door that takes a string; every body they produce is untyped text.

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
| Attach v5 sessions (`Deliver`), a remote's and every page's, replayed and live | the references, in full |
| A conversation (pushed batches, `MessageChannelGet`, `BrennPendingList`, the intercept outcome) | each name with `size` and `content_type`; **no id** |
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
or trap publishes nothing and deletes everything it made. Host code's
`Messenger::publish_from_system_with_attachments` is the other publishing
entry, and nothing in the server calls it yet. Remotes, pages and
conversations see references as metadata only, and no surface host serves
the interface. A remote's replay store and a page's rings do not hold the
blob, so a reference there can outlive it. `ports.publish` and its siblings
carry no attachments: a component that republishes a message through them
sends the body alone.

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
and no production host code makes one. The per-activation bounds are in
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

What is not built: the `blobs` interface on a page, a conversation tool,
connectors that produce blobs, the surface's handling of them, attachments on
tool calls and their results, and transfer of bytes over attach or to a
page.

### 2.1.3 Limits on a remote's stream of messages

At default configuration, for a remote attacher on attach v5. "Operator" names
the setting that raises a limit; "fixed" is a compile-time constant. An ingress
channel (`webhook:`, `mqtt:`) is outside this section: a remote cannot
subscribe to one, and its subscribe to such an address ends the session as a
protocol violation.

| Limit | Default | Raised by |
|---|---|---|
| Body cap, one message | 65,536 bytes raw | Operator: `messaging { max_body_bytes }` (restart) |
| v5 frame cap | 204,800 bytes | Derived from the body cap |
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
raises it bus-wide; a durable channel is not a video carrier. These are limits,
not measurements.

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
| Server-to-page frame size | unbounded: the 204,800-byte v5 frame cap bounds what a page sends | — |
| The page's inbound queue | unbounded; drained one frame per browser task | — |
| The page's store | `max(push_depth, retain_depth)` rows per channel, a count | the binding's depths |
| One activation's input | every bound port's window, with no byte limit; the 4 MiB per-activation bound is on publishes only | — |
| Sessions per surface / per user on one surface | 64 / 16 | Fixed |

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

So a camera frame that fits 64 KiB, sent by a remote onto a `non_text`
`ephemeral:` channel, reaches a 0.2.0 kind on a surface at 1 per second with
no configuration and at up to 30 per second with one `remote`-block setting;
larger frames need the body cap raised. These are limits, not measurements;
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

The page's wire is attach v5. On any channel declared `non_text`, page-local
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
channels (`brenn:tools/<tool>`, `brenn:tool-results/<slug>`), and the per-user
device-location channels (`brenn:device-location.<username>`). Nothing in config
brings these into existence — an endpoint block, a subscription, a registered
tool or a registered user does — so there is no declaring `[[channel]]` block to
carry their depths.

They still have to be *sized*, and the sizing rule is the same one everything
else obeys: bounded, and visible to whoever ought to be deciding. Each family has
a bounded in-code default:

| Family | push | retain | standing |
|---|---|---|---|
| `webhook:<slug>`, `mqtt:<client>:<topic>` | 1 | 100 | 100 |
| `brenn:tools/<tool>`, `brenn:tool-results/<slug>` | 1 | 16 | 16 |
| `brenn:device-location.<username>` | 1 | 2000 | 2000 |

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
substrate subscriber is the tool executor, folded in as a system participant,
and a system participant's overflow rung is `Silent` whatever the channel says.
A `noise` written on a request-channel block is inert today
(`TODO(system-participant-noise-inert)`).

**Tuning them.** A `[[channel]]` block addressing one of these channels does not
declare it; it *tunes* it. Synthesis still owns creation, identity and
description, so `uuid` and `description` are rejected on a tuning block, and all
three depths are required — a block that tunes states every number. `noise`,
`wake_min`, `sink` and `send_rate` are optional and inherit the `[messaging]`
globals, exactly as on a declaring block — except `send_rate` on a
`brenn:tool-results/` inbox, which is the host's and refused in the block. An
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
substitute type — and is still published on any other channel it matches:

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
