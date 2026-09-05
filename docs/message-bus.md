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
(`mqtt:<client>:<topic>`), and the async-tool substrate's request and result
channels (`brenn:tools/<tool>`, `brenn:tool-results/<slug>`). Nothing in config
brings these into existence — an endpoint block, a subscription, or a registered
tool does — so there is no declaring `[[channel]]` block to carry their depths.

They still have to be *sized*, and the sizing rule is the same one everything
else obeys: bounded, and visible to whoever ought to be deciding. Each family has
a bounded in-code default:

| Family | push | retain | standing |
|---|---|---|---|
| `webhook:<slug>`, `mqtt:<client>:<topic>` | 1 | 100 | 100 |
| `brenn:tools/<tool>`, `brenn:tool-results/<slug>` | 1 | 16 | 16 |

Ingress channels are fact channels; at their arrival rates a hundred messages is
a horizon of days to months, which is what "sized for the outage you intend to
survive" means for them. The tool channels' executor and consumers are in-process
and eager, so their window covers a burst arriving while the executor is busy.
The channel-level push rung is near-inert on all four families — every
subscriber on them states its own depths — so 1 is the honest floor.

**Tuning them.** A `[[channel]]` block addressing one of these channels does not
declare it; it *tunes* it. Synthesis still owns creation, identity and
description, so `uuid` and `description` are rejected on a tuning block, and all
three depths are required — a block that tunes states every number. `noise`,
`wake_min`, `sink` and `send_rate` are optional and inherit the `[messaging]`
globals, exactly as on a declaring block. An explicit `"unbounded"` is legal
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
nothing for the same reason a typo would.

Nothing about a resolved channel is persisted — depths are re-read from config
each boot — so retuning takes effect on restart with no migration. Lowering
`standing_retain_depth` lets the next reap pass evict; raising it cannot
resurrect what was already evicted.

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
