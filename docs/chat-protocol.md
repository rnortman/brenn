# The Chat Protocol (v1)

This is the normative specification of the wire vocabulary a peer speaks to hold
a conversation with a Brenn-hosted LLM over the message bus. It is written for a
peer author who reads no Rust: an out-of-tree WASM component, a browser surface,
a gateway bridging some other transport.

The implementation is `brenn-envelope/src/chat.rs`. **Any change to the
vocabulary updates that file and this document in the same commit.** Neither one
is allowed to move first.

Read [the channel model](message-bus.md) before this. Chat rides ordinary
channels with ordinary semantics, and the parts of this document that surprise
you are usually the channel model, not the chat protocol.

---

## 1. The channel family

A conversation is a set of channels derived from its owning app's slug and its
own numeric id. The names are minted from one configured prefix (`[llm_chat]
prefix`, `chat` by default):

```
brenn:<prefix>.app.<app-slug>.in.<conversation-id>          commands to the conversation
brenn:<prefix>.app.<app-slug>.out.<conversation-id>         the conversation record
ephemeral:<prefix>.app.<app-slug>.stream.<conversation-id>  token traffic
ephemeral:<prefix>.app.<app-slug>.wake.<conversation-id>    pre-warm signal
brenn:<prefix>.app.<app-slug>.approvals.<conversation-id>   RESERVED — see §8
```

The conversation id is the last segment because it is the only one minted at
runtime. That ordering is what makes grants expressible at three grains without
wildcards:

- one conversation, one leaf — an exact name;
- one leaf of every conversation of an app — the prefix
  `<prefix>.app.<slug>.<leaf>.` (a "fleet" grant: may drive every conversation,
  may not forge any record);
- every leaf of every conversation of an app — the prefix
  `<prefix>.app.<slug>.`.

A conversation therefore owns no subtree of its own; its channels are siblings
under the per-leaf subtrees.

**Schemes.** `brenn:` is durable — the messages survive a restart, and the
channel's retained window *is* the readable history. `ephemeral:` is in-memory
and lost on restart. The two ephemeral leaves are the ones where losing a message
is better than paying to persist it.

**Authority.** Reaching any of these channels requires an operator-authored
grant, exactly like any other channel. An app's own LLM does **not** get its
conversation's channels for free; the server-side harness that wraps the LLM
holds that authority, and the LLM does not act under it.

---

## 2. Framing

Every body on every chat channel is a JSON object of the form:

```json
{"v": 1, "type": "<name>", ...fields}
```

- `v` is the protocol version. This document specifies `v = 1`. A body with a
  missing, non-integer, or different `v` is rejected outright — a future `v = 2`
  is not absorbed by any tolerance rule below.
- `type` names the command, event, or stream event. The remaining keys are that
  type's fields.

**Evolution within v1 is additive only:** new optional fields, and new event
types. A breaking change bumps `v`.

**Tolerance is asymmetric, deliberately.**

- Unknown *fields* are ignored everywhere. Do not fail on a key you do not
  recognize; it is a later Brenn adding an optional field.
- An unknown *event* `type` (on `out` or `stream`) must be tolerated — treat it
  as an event this build does not implement and carry on. Peers that fail here
  break the day Brenn adds an event.
- An unknown *command* `type` (on `in`) is **rejected** by Brenn. A command is a
  request to do something; silently shrugging at one Brenn cannot execute would
  be worse than saying no.

**Absent optionals are omitted, not sent as `null`.** Do the same when
publishing.

All text fields carry raw text as the model produced it — markdown, code, prose.
No HTML crosses this wire, and a peer that renders must do its own escaping.

---

## 3. Attribution, correlation, and history

### 3.1 Attribution is the envelope, never the body

Who published a command is the **envelope's `sender`**, assigned by Brenn at
publish time. There is no sender field in a command body, and a peer cannot
claim to be another peer. Do not invent one.

A `user_message` event echoes the origin in its own `sender` field so a late
subscriber reading history knows who spoke:

- input that arrived over the bus carries the publishing participant's id;
- input that arrived over the legacy browser websocket carries `legacy-ws:<username>`.

The `legacy-ws:` form is deliberately not a participant id, so a peer parsing
senders as participant ids cannot mistake one for the other.

### 3.2 Correlation

Every command may carry `correlation`: an opaque string of the peer's choosing,
echoed on the events that command produces. Peers that do not care omit it.

**The outcome rule: one event carries the correlation per outcome.**

| command | success carries it on | failure carries it on |
|---|---|---|
| `send` | `user_message` | `error` |
| `set_model` | `model_changed` | `error` |
| `stop` | `ack` | `error` |
| `compact` | `ack` | `error` |

Most commands have exactly one outcome. A `send` that also names a `model` has
two — the model change and the message — and reports each. Commands whose success
has a natural record event never `ack`; `ack` exists precisely for the verbs that
have no other outcome to hang a correlation on.

### 3.3 History is the retained window

There is no history API. A conversation's `out` channel retains
`[llm_chat] retained_window` messages (1000 by default), and that window is the
history a subscriber can read. A peer sees as far back as its own retain depth
allows and no further; sizing the window is an operator decision about how much
past to keep, not a delivery guarantee.

Ordering and sequence come from the envelope, never from an event body.

### 3.4 The stream is decoration; durable is truth

`stream` carries token batches for the message currently being generated. Loss is
expected and never recovered — there is no retransmit. A peer whose own position
bookkeeping tells it that it dropped stream messages must discard the partial
text it has accumulated for that `turn` and wait for the durable
`assistant_message`, which is authoritative over anything seen on the stream.

A peer that cannot tolerate that should simply not subscribe to `stream`.

### 3.5 Urgency and waking

A conversation that nobody is talking to has no process running. Publishing to
`in` at or above the configured wake threshold (`[llm_chat] wake_min`, `normal`
by default) buys the conversation a subprocess; publishing below it leaves the
command parked until something else wakes the conversation, at which point it is
drained in order.

Urgency levels are `very-low`, `low`, `normal`, `high`. Publish a real user
command at `normal` or above. Use `wake` — whose bodies are ignored entirely, the
message's existence being the whole signal — to pre-warm a conversation you are
about to talk to.

A woken conversation stays alive for `[llm_chat] idle_timeout_secs` after its
last interaction, so a peer driving it in bursts does not pay startup per burst.

---

## 4. Commands (peer → conversation, on `in`)

### `send`

Submit user text.

| field | type | required | meaning |
|---|---|---|---|
| `text` | string | yes | the message |
| `model` | string | no | sticky model alias, applied to this message and onward |
| `attachments` | array | no | file references — **rejected in v1**, see below |
| `correlation` | string | no | echoed on the outcome |

The text is handed to the harness, which injects it at the end of the current
tool-use round, or immediately when the conversation is idle. There is no
busy-gate and no separate "steer" verb: injection timing is the only semantics
either could have.

`model` names an alias the server knows (e.g. `default`, `sonnet`). An unknown
alias rejects the whole command — the text is *not* sent — with a correlated
`error`. If the model change fails for an internal reason instead (no harness
running, harness refused), the text is still delivered and a correlated `error`
reports the model failure: text delivery is not hostage to model stickiness.

**Attachments are rejected in v1.** The field exists in the schema so that
support is purely additive when it lands, but a bus `send` naming any attachment
is refused whole with a correlated `error`. Upload ids resolve through a per-user
registry, and a bus sender maps to no user; sending the text without the files
would silently misrepresent the request.

### `stop`

Interrupt generation gracefully — the harness finishes with a result rather than
being killed.

| field | type | required | meaning |
|---|---|---|---|
| `correlation` | string | no | echoed on the `ack` |

Idempotent: stopping an idle conversation is an acknowledged no-op, and the `ack`
is the same either way. `stop` always works — it is never refused for
resource reasons (§6).

### `set_model`

Change the sticky model without sending text.

| field | type | required | meaning |
|---|---|---|---|
| `model` | string | yes | the alias |
| `correlation` | string | no | echoed on `model_changed` or `error` |

Same alias validation as `send`'s `model`. The set of aliases a conversation
accepts is published as a `models` event.

### `compact`

Ask the harness to compact its context.

| field | type | required | meaning |
|---|---|---|---|
| `correlation` | string | no | echoed on the `ack` |

Acked on success only; a failure (a compaction already running, for one) is a
correlated `error`.

---

## 5. Events (conversation → peers, on `out`)

This channel is the conversation record. Everything a subscriber needs to
reconstruct what happened is here.

### `user_message`

An accepted `send`, echoed exactly once regardless of which door it arrived
through.

| field | type | meaning |
|---|---|---|
| `text` | string | the accepted text |
| `attachments` | array | attachment metadata (`upload_id`, `filename`, `media_type`, `size`); omitted when empty |
| `sender` | string | who sent it — §3.1 |
| `correlation` | string | the command's correlation, when it had one |

### `assistant_message`

A completed assistant message. Authoritative over anything seen on `stream`.

| field | type | meaning |
|---|---|---|
| `text` | string | the message |
| `turn` | string | server-minted opaque id shared with this message's `tokens` batches |

### `system_message`

A Brenn-generated message in the conversation thread.

| field | type | meaning |
|---|---|---|
| `text` | string | the message |
| `category` | string | origin tag — use it to decide prominence; the text is the payload either way |

Categories: `messages_received`, `event_drain`, `compaction_reminder`,
`compaction_hard_trigger`, `compaction_idle_prompt`, `idle_hook`,
`compaction_user_request`, `ui_error`, `device_slug_reminder`, `graf_error`,
`compaction_failed`, `debug_snapshot`.

### `status`

The harness changed state.

| field | type | meaning |
|---|---|---|
| `state` | string | one of `idle`, `connecting`, `thinking`, `awaiting_approval`, `compacting`, `error` |

This is the transient state of the live session, not the stored conversation's
status.

### `error`

A command that was rejected or did not take (with that command's `correlation`),
or a conversation-level failure (without one).

| field | type | meaning |
|---|---|---|
| `message` | string | what went wrong |
| `correlation` | string | present iff a specific command caused it |

### `ack`

A command that has no other outcome event to carry its correlation — `stop` and
`compact`.

| field | type | meaning |
|---|---|---|
| `command` | string | the verb acknowledged, as it appears in the command's `type` |
| `correlation` | string | the command's correlation, when it had one |

### `model_changed`

The effective sticky model changed.

| field | type | meaning |
|---|---|---|
| `model` | string | the new alias |
| `correlation` | string | the correlation of the command that changed it, when it came from one |

### `models`

The model list the conversation accepts. Published when the adapter starts and
whenever the list changes; the retained window makes it visible to a subscriber
that arrives later.

| field | type | meaning |
|---|---|---|
| `available` | array | objects of `value` (the alias to pass), `display_name`, `description` |

### `tool_use`

A completed tool use, summarized as raw text.

| field | type | meaning |
|---|---|---|
| `tool_name` | string | the tool |
| `summary` | string | human-readable summary |

### `context_usage`

Context-window telemetry, emitted after each internal context check.

| field | type | meaning |
|---|---|---|
| `usage_pct` | number | fraction of the context window in use, 0-100 |
| `current_tokens` | number | tokens in use |
| `max_tokens` | number | window size |
| `reminder_pct` | number | percentage at which the warning stage fires |
| `red_pct` | number | percentage at which the danger stage fires |
| `reminder_tokens` | number | absolute count at which the warning stage fires, when configured; fires in addition to `reminder_pct` |
| `red_tokens` | number | absolute count at which the danger stage fires, when configured |

### `cost_usage`

Cost telemetry, emitted after each turn completes.

| field | type | meaning |
|---|---|---|
| `last_turn_usd` | number | cost of the turn just finished |
| `since_last_compaction_usd` | number | cumulative session cost since the last compaction |
| `last_24h_usd` | number | sum across every conversation this server ran in the last 24 wall hours |

---

## 6. Stream events (conversation → peers, on `stream`)

### `tokens`

A batch of tokens.

| field | type | meaning |
|---|---|---|
| `text` | string | the batch |
| `kind` | string | `text` (visible assistant output) or `thinking` (reasoning content) |
| `turn` | string | matches the `turn` on the `assistant_message` this batch is building toward |

See §3.4 for what to do when you notice loss.

---

## 7. Impetus: what a peer may not claim

A conversation holds an **impetus pool**: a bounded stock that every unattended
turn-provoking injection draws one unit from. An incoming `send` or `compact`
costs one unit; so does each batch of bus messages delivered into the
conversation as context. This is what stops two pieces of machinery from talking
to each other forever.

The pool is restored by attention, and attention is carried on the envelope. A
message may carry:

```json
"impetus": "replenish"
```

which tells the redeeming consumer: treat this message as the product of live
user interaction, and restore the allowance it governs to full. A conversation
redeems it on an accepted `send` or `compact` — reset first, then draw the one
unit for the turn.

**Setting the field requires a capability grant that almost nothing holds.**
Impetus is not a body field a peer can assert; it is an envelope field checked at
publish time. A publish carrying impetus from a principal without the mint
capability is **refused whole** — nothing is stored, and the field is never
silently stripped. Assume you cannot mint it unless the operator has told you
otherwise, and never set it from anything other than a real user gesture.

**Consequences a peer must handle:**

- A `send` or `compact` arriving without impetus at an exhausted pool is refused
  whole: no `user_message`, no `ack`, no work done, and a correlated `error`
  naming the condition. Whole includes the `model` a `send` carried — a refused
  send leaves the conversation's model exactly where it was, and publishes no
  `model_changed`. The remedy is attended interaction, not retrying.
- `stop` and `set_model` draw nothing and are never refused this way. Impetus on
  them redeems nothing — they provoke no turn.
- A `send` rejected before acceptance (attachments present, unknown model alias)
  neither redeems impetus nor draws from the pool.

---

## 8. The reserved `approvals` leaf

`brenn:<prefix>.app.<slug>.approvals.<id>` is **named and reserved in v1, with
no defined semantics**. The name is fixed now so that the tool-call permission
flow cannot collide with the grammar when it is built, and so that "may chat" and
"may approve tool calls" are separately grantable today.

Peers must not publish to it, must not subscribe to it, and must not assume
anything about what will appear on it. Brenn does not provision it.

---

## 9. Malformed commands

A body Brenn cannot decode is answered with an `error` on the record — correlated
when the correlation was recoverable — and recorded as a schema violation against
the publishing sender. The conversation survives it and processes the next
command.

That record is not an IP ban signal. A body that reaches the adapter has already
passed authentication and the channel's publish ACL, so a peer sending garbage is
an authenticated principal misbehaving; the operator's lever is revoking its
grants. Volume is the signal, so a peer that keeps getting rejected should expect
attention, not a ban.

---

## 10. Changing this protocol

1. Additive changes only within `v = 1`: new optional fields, new event types.
   Anything else bumps `v`.
2. The vocabulary lives in exactly two places — `brenn-envelope/src/chat.rs` and
   this document — and they change in the same commit. A test derives the
   complete tag set from the Rust types and fails if a tag is missing here, so
   forgetting is a red build rather than a silent drift.
3. Adding an event type is safe for peers by construction (they tolerate unknown
   types). Adding a *command* type is not symmetric: an older Brenn rejects it.
   Peers must not assume a command exists because this document describes it;
   check the Brenn version you are talking to.
