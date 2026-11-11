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
bound to it (§2.6); every system-minted channel is text-only. It is a static
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
  bodies alike.
- **Search** (`search` on a query, FTS on a durable channel, substring match on
  a ring) covers text bodies only. A non-text body never matches a search and
  is returned by a query without one like any other message.
- **The rule is recorded at write time.** A durable channel stores a text body
  as a TEXT value and a non-text body as a BLOB value in the same column, and
  the full-text index covers the TEXT rows. Widening the text rule later
  therefore ships with a migration of the rows it reclassifies.

Who is shown the content type today:

| Reader | Typed text body | Non-text body |
|---|---|---|
| Query API (`Messenger::query`), host code | text and type | bytes and type |
| A conversation (pushed batches, `MessageChannelGet`, `BrennPendingList`, `BrennMessageEdit`) | `"body": "…"`, then `"content_type": "…"` | `"body": {"binary": true, "content_type": "…", "size": N}` — a placeholder, never the bytes |
| The eviction archive | `"body"`, then `"content_type"` | `"body_base64"` (standard padded base64), then `"content_type"` |
| 0.2.0 components (`bus.envelope`, `bus.deferred-entry`) | bytes and type | bytes and type |
| 0.1.0 components (`envelope-json`, `deferred-entry.payload`) | bare text, **no type** | never bound: refused at boot and at reload |
| Attach v4 sessions (`Deliver`, `DeferredView`) | bare text, **no type** | never bound: a surface binding is refused at plan, a remote's subscribe is answered `Unavailable`; left out of a deferred view |
| Attach v5 sessions (`Deliver`, `DeferredView`) | text and type | bytes and type |

The two string carriers lose a typed text body's *type*, never its content.
They cannot carry a non-text body at all, so they are kept off every channel
declared `non_text` (§2.1) before any message flows, and a text-only channel
never holds a non-text body. Reaching a string carrier with one anyway is a
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
is stored. What still cannot: a 0.1.0 component, an attach v4 session (the
page's, and a remote's that negotiated v4), a conversation and every host door
that takes a string; every body they produce is untyped text.

### 2.1.3 Limits on a remote's stream of messages

At default configuration, for a remote attacher on attach v5. "Operator" names
the setting that raises a limit; "fixed" is a compile-time constant.

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
present, then `urgency` and `envelope_type`. The body entries are `body` (a JSON string) for a
text body, or `body_base64` for a non-text one, followed by `content_type` when
the body has one. An untyped text body's line is exactly the line written
before bodies became bytes.

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
when the plan is built, so `config-check` reports it. A surface binding may not state it, and a
surface may not be bound to a `non_text` channel, until the surface host
carries bytes and the page speaks attach v5. A remote attacher on v5 is served
a `non_text` channel; one on v4 is answered `Unavailable`.

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
