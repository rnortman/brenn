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
window. Always the most recent M. It does not *cause* activation; a node with
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

Most frameworks fuse these two ideas into one queue, and the fusion is the source
of a great deal of pain: you cannot ask to see history without also asking to be
woken by it, and you cannot ask to be woken without inheriting a delivery
backlog. Separating them means **activation policy and visibility policy are
tuned against different pressures** — activation against the node's processing
cost and coalescing tolerance, visibility against the node's analytic needs.

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

- **`[[connection]]`** — a top-level block listing endpoints (`wasm:<slug>/<port>`,
  or `surface:<slug>#<instance>/<port>`). The ports it names are declared on
  their own component's block with no `channel` of their own ("free ports"), so
  the tuning stays with the subscriber it describes and the wire stays with the
  connection.
- **`io_port`** — one port name resolving to an input *and* an output on the same
  channel. This is the sanctioned timer idiom: `deliver_after` on the port is a
  wake the component is guaranteed to receive, because the two halves
  structurally cannot be wired to different channels. Wiring a self-loop out of
  two separately-configured bindings is how it silently fails.

An auto channel with no name is **anonymous**: its address is `auto.<cid>`, where
the cid is derived from the endpoint set. The `auto` namespace is reserved —
nothing else may declare, bind, or write an ACL matcher into it — so an anonymous
channel is reachable *only* through the declaration that created it. Giving the
channel a name instead makes it an ordinary directory entry that third parties
may bind with ordinary bindings and ordinary ACLs (naming grants nothing by
itself; deny-by-default still holds), and a `brenn:` name is what makes it
durable, which is what makes a parked schedule survive a restart.

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

An auto channel's depth is folded rather than declared: `retain_depth` is the max
over subscribing endpoints of `max(push_depth, retain_depth)`, floor 1. Everything
else — `sink`, `send_rate`, channel-level `noise`, `standing_retain_depth` —
inherits the global defaults. A channel that needs channel-level tuning has
outgrown auto declaration; write the `[[channel]]` block.

**The timer-cap coupling.** A channel's `retain_depth` also caps its channel-wide
deferred set, so on an auto channel the fold above is what bounds how many
`deliver_after` schedules the port may hold outstanding. A component juggling K
parked wakes must declare at least K, on top of its actual retention need. Over
the cap the schedule is refused and host-logged, and the component is not told —
so this is a number to size deliberately.

Leaving it to the default is not an option in the server realm: the stock global
depth defaults are unbounded, and an unbounded fold on a non-durable channel
refuses to boot. Because the fold takes `max(push_depth, retain_depth)` per
subscribing port, *both* halves have to resolve bounded — bounding one while the
other inherits the unbounded default still folds unbounded. So the three outs are:
write both depths on the subscribing port(s), bound both
`[messaging].default_push_depth` and `[messaging].default_retain_depth`, or give
the channel a `brenn:` name. A page-local port escapes only the fold, not the
defaults: it has no server entry, so an unset `retain_depth` never consults the
globals and silently gives a ring of depth 1 — but its `push_depth` still
resolves binding → global like every surface binding, and the stock unbounded
default refuses to boot there too.

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
