# Brenn Security Posture & Threat Model

**Status:** Standing policy. Stable unless the nature, users, or use cases of the
application change, or a major new surface is added.
**Primary audience:** the security-focused code reviewer.
**Scope:** Application-level security — the assets worth protecting, who is and is
not trusted and why, the trust boundaries that follow from the architecture, the
threats faced at each boundary, and the invariants the system must uphold there.
**Out of scope:** Deployment, infrastructure, and operations (CI/CD, secret
management, fail2ban infrastructure, hosting). Where an operational concern
manifests as a security policy the code must enforce (e.g. cookies must be
`Secure` when served over a public bind), the *policy* belongs here; the
operational mechanism does not.

> This document is a policy and a review bar. It states what *must* be true, not
> what the code *currently* does. It deliberately contains no file/line citations,
> no accounting of present implementation, and no per-finding audit results — all
> of those drift with every refactor and belong in exploration or review
> artifacts, not here. The reviewer's job is to hold the code against the policy
> below, not to confirm a line number.

---

## 1. Security Philosophy (the bar)

Two project principles override everything else in this document and set the
overall review bar.

**Backend robustness.** The backend must *never do the wrong thing*. It fails
fast: it shuts itself down rather than tolerate a bug or proceed past an
unexpected condition. No fallbacks, no swallowed errors, no "keep calm and carry
on" on a security-relevant path. (Transient external failures — an API briefly
unavailable — are normal and are handled normally; they are not "unexpected.")

**Backend is the brain; the browser is a rendering layer.** All application and
security logic lives in the backend. The browser receives rendered output and
displays it; it holds no authority.

These yield three standing rules a reviewer applies everywhere:

1. **The backend is the only security authority.** Any security decision that can
   be observed or influenced by the browser, by the Claude Code subprocess, or by
   a WASM guest, but is *enforced* by that party rather than by the backend, is a
   finding.
2. **Unexpected input from a trust boundary fails loud, not quiet.** A silent
   fallback on a security-relevant path — defaulting to "allow", swallowing a
   parse error on a control message, accepting an unrecognized decision — is a
   finding. Unexpected or malformed input from an untrusted boundary must also be
   *surfaced* as a security/observability signal, not merely dropped.
3. **Rendered output is a sink.** Any externally-influenced value that becomes
   markup the browser renders must be escaped or passed through the sanctioned
   rendering boundary. A raw, unescaped, externally-influenced value reaching the
   browser as markup is a finding.

---

## 2. Actors and Trust Model

Trust here is a deliberate decision, not an observation about who happens to be
well-behaved. Each actor's trust level states *what we rely on them not to do*,
and the reason.

| Actor | Trust decision | Why |
|---|---|---|
| **Operator** | Fully trusted. The trust anchor. | The operator configures the system (apps, mounts, MCP servers, extra subprocess args, WASM grants, auto-approve rules). Operator config defines the security policy; it is *not* an attack surface in this model. Anyone who can write operator config already owns the host. |
| **Backend** | The trusted computing base (TCB). | It is the only component that makes security decisions. Everything else is downstream of it and is constrained by it. |
| **Human user (authenticated)** | Authenticated, bounded. | A user who holds a valid session is trusted to act *within their own authority*. They are **not** trusted to stay within it voluntarily: they may act maliciously, and must be structurally prevented from exceeding their authority (reading another user's data, reaching another app, escaping their sanctioned filesystem scope). |
| **Claude Code subprocess** | **Semi-trusted.** Launched by us, but its output is attacker-influenceable. | We spawn and parameterize CC, so we trust *how it is configured*. We do **not** trust *what it emits*: its output is shaped by the conversation, by any tool/MCP servers it talks to, and by any file or web content it ingests, all of which can carry adversarial content. CC output must therefore be treated as attacker-influenceable even though the process is "ours." |
| **WASM guest — in-tree** | Sandboxed-trusted. | First-party code, but deliberately sandboxed to contain its *own* bugs. The sandbox is a blast-radius limiter, not a statement that the code is hostile. |
| **WASM guest — out-of-tree** | **Untrusted. Adversarial.** | Third-party extension code is a first-class extension surface and must be assumed hostile. The sandbox is the security boundary. A guarantee that holds only for a well-behaved guest is **not** a security guarantee. |
| **Unauthenticated network client** | Untrusted. | Anyone who can reach the listener before authenticating: login/registration traffic, URL probing, inbound webhooks. Adversarial by default. |

**The key trust asymmetry to internalize:** "we launched it" (CC) and "it's our
code" (in-tree WASM) do **not** confer trust in the *content* that crosses the
boundary. The reviewer must apply the untrusted bar to CC *output* and to
*any* WASM guest's host-facing behavior regardless of provenance.

---

## 3. Assets to Protect

In rough order of severity if compromised:

- **The host machine.** Filesystem, subprocess execution, and network egress.
  No actor below the backend — not CC, not any WASM guest, not any browser user —
  may read, write, or execute outside the scope the operator sanctioned for it.
- **User credentials and sessions.** Password material, session and CSRF tokens,
  device identifiers. Compromise means account takeover.
- **Cross-user and cross-app isolation.** Conversation history, per-app data,
  per-app filesystem scope, per-component WASM storage. One user or one app must
  not reach another's data.
- **Integrity of the security signal stream.** The system's defensive monitoring
  depends on trustworthy, structured security-event records. Forging, suppressing,
  or splitting those records (e.g. via injection into a log field) is an attack on
  the system's ability to defend itself.

---

## 4. Trust Boundaries

A trust boundary is where data or control crosses from a less-trusted party to a
more-trusted one (or vice versa). These follow from the architecture and are
stable as long as the architecture is.

```
                       [ Operator config ]  (trust anchor)
                                |
   Unauth network --(B0)--> [   Backend (TCB)   ] <--(B1)-- Authenticated browser
                            /   |     ^   \
                      (B2) /(B6)|     |(B3) \(B4)
                          v     v     |      v
        hosted-app <-- (spawned)  [ CC subprocess ]   [ WASM host / guests ] <--(B5)-- inbound
        subprocess                                                                     webhooks
                                                                                      (external)
```

(The backend spawns and drives every party below it: CC, the hosted-app
subprocesses, and the WASM guests. B6 is the backend → hosted-app subprocess
boundary; the inputs that flow across it may be CC- or browser-influenced, but the
backend is the invoker.)

| ID | Boundary | Direction of distrust |
|----|----------|----------------------|
| **B0** | Unauthenticated network → backend | client untrusted |
| **B1** | Authenticated browser → backend | browser-supplied data untrusted; identity is bounded to the session |
| **B2** | Backend → CC (spawn parameters) | backend controls; risk is untrusted values leaking into spawn parameters |
| **B3** | CC → backend (its output stream) | CC output attacker-influenceable |
| **B4** | Backend ↔ WASM guest | guest untrusted (especially out-of-tree) |
| **B5** | Inbound webhook → backend → WASM | external party untrusted |
| **B6** | Backend → hosted-app subprocess | risk is injection through the argument/input vector |

---

## 5. Boundary B0 — Unauthenticated Network → Backend

**Who:** anyone who can reach the listener without a session.

**Threats:** credential guessing and brute force; user enumeration; account
creation abuse; resource exhaustion; reconnaissance via URL probing; forged or
unauthenticated requests to endpoints that must be reachable pre-auth.

**Policies the backend must uphold:**

- **Credentials are never stored reversibly.** Passwords are stored only as
  salted hashes using a memory-hard algorithm with current best-practice
  parameters. A regression to a weaker scheme, weaker parameters, or any
  reversible storage is a finding.
- **No authentication oracle.** Wrong-password and unknown-user must be
  indistinguishable to the client, in both response and timing. Any path that
  lets an attacker distinguish "user exists" from "user does not" is a finding.
- **Account creation is gated.** Self-service registration must require a
  credential the operator controls, and that credential must be single-use and
  unguessable. The gating check must be robust against races (re-validated around
  the commit).
- **All secrets and tokens are cryptographically random.** Session tokens, CSRF
  tokens, and any registration/invite credentials must come from a CSPRNG with
  enough entropy to make guessing infeasible. Predictable or low-entropy security
  tokens are a finding.
- **Everything that is not explicitly pre-auth requires authentication.** The set
  of endpoints reachable without a session is a deliberate, minimal allowlist
  (only what is genuinely required before login, e.g. the login/registration
  surface and assets a PWA must fetch pre-auth). Adding a route that exposes data
  or state-change capability without an auth check is a finding.
- **Unrecognized requests are a defensive signal.** Probing for nonexistent
  endpoints must be recorded as a security signal (without alert spam), so that
  external defenses can act on it.
- **Rate limiting is enforced on attacker-reachable surfaces**, keyed on a
  trustworthy client identity. The client identity used for rate limiting and for
  attribution must only trust a forwarded-address header when the deployment sits
  behind a trusted proxy; trusting it otherwise lets an attacker spoof identity
  and defeat both rate limiting and attribution.

**What the reviewer verifies:** credentials are hashed, not reversible, with
sound parameters; the auth path has no enumeration/timing oracle; registration is
gated and race-safe; tokens are CSPRNG; the pre-auth route set is minimal and
deliberate; unknown URLs and abuse are recorded as security signals; rate
limiting cannot be trivially bypassed via a spoofed client identity.

---

## 6. Boundary B1 — Authenticated Browser → Backend

**Who:** a user holding a valid session. Trusted to act within their authority,
not trusted to stay within it.

**Threats:** privilege escalation across users or apps; acting on data the user
does not own; replaying or forging identity fields; cross-site request forgery;
injecting content that the backend later renders or logs; resource exhaustion via
oversized or high-rate messages; smuggling state into server-side policy (e.g.
auto-approval rules).

**Policies the backend must uphold:**

- **Authority is always re-derived from the authenticated session, never from
  browser-supplied identity.** Every state-changing or data-reading operation
  must determine *who* is acting and *what they may touch* from the session, not
  from any user-id / owner / app field in the message. Trusting a
  browser-supplied identity over the session is a critical finding.
- **Cross-user and cross-app access is denied by default.** Access to an app, a
  conversation, a file, or any per-user resource must be checked against the
  session's authority on every access. The default is denial; access is granted
  only by an explicit operator-configured or ownership-based rule.
- **The message protocol is closed and fail-closed.** Inbound messages are
  validated against a closed schema; unknown message types are rejected, not
  silently tolerated. Malformed or unexpected input is rejected *and* surfaced as
  a security signal. A schema that silently accepts unknown variants on this
  boundary is a finding.
- **Every field that drives a sensitive action is validated against an allowlist
  or bound**, not merely deserialized. Identifiers, model selectors, sizes,
  counts, and paths that flow onward to a sink (filesystem, subprocess, another
  user's view, a log) must be constrained at intake. Absence of validation on
  such a field is a finding; presence of validation that is only cosmetic (does
  not actually constrain the dangerous downstream use) is also a finding.
- **State-changing requests are protected against cross-site forgery.** The
  protection may be structural (the established session-bound channel) or
  token-based, but any new state-changing entry point added outside the existing
  protected pattern must carry equivalent protection. An unprotected
  state-changing entry point is a finding.
- **Untrusted boundaries are bounded.** Inbound data must have a size/rate bound
  appropriate to the boundary so that a single client cannot exhaust memory, CPU,
  or a shared resource. A new ingest path with no bound where one is structurally
  expected is a finding.
- **The browser may answer interactive prompts, but may not silently rewrite an
  already-approved action.** Where the protocol lets the browser supply data that
  feeds back into a privileged operation (e.g. answering a tool's question, or
  contributing to an auto-approval policy), that contribution must be bounded and
  validated, and it must not let the browser change a security-relevant parameter
  of an action *after* the user approved a different version of it. See §7 for
  the approval-gate interaction.

**Rendered output back to the browser.** The browser renders backend-produced
markup directly. Therefore every value the backend sends as markup must already
be escaped or rendered through the sanctioned rendering boundary (§10). The
security of the browser depends entirely on the backend's escaping; the browser
performs no sanitization of its own and must not be relied on to.

**What the reviewer verifies:** authority comes from the session everywhere; the
inbound schema is closed and rejects+signals unknowns; every security-relevant
field is bound/allowlisted at intake; state-changing entry points carry CSRF-
equivalent protection; ingest is bounded; any browser-supplied data that feeds a
privileged operation is validated and cannot rewrite an approved action; every
value sent to the browser as markup is escape-guaranteed at its source.

### 6.1 Accepted risk — durable resume seq is the global message rowid

Durable-channel surface deliveries carry `Pos::Durable{seq}` where `seq` is the
global `messaging_messages.id` (SQLite rowid), not a per-channel counter. An
authenticated surface client subscribed to one channel can therefore observe
gaps between the seq values it receives and infer that *other* messages were
published on *other* channels in between — a cross-channel traffic-analysis side
channel.

- **What it leaks:** aggregate system-wide publish volume and timing only.
  Never message content, channel identity/address, or sender — the client sees
  only that *some* activity occurred between two of its own deliveries.
- **Why it is accepted:** the deployment model is single-operator. Every
  authenticated browser principal already belongs to the operator, so the leak
  crosses no tenant boundary — there is exactly one tenant, and it is inferring
  metadata about its own system.
- **Why the global rowid is used** (not a per-channel seq): it survives
  push-row GC, is subscriber-independent, and is the one value that serves both
  durable replay sources (parked push rows and the retained message window). A
  dense per-channel seq would need a schema column plus a re-derivation of those
  three properties.
- **Revisit trigger:** any move to a multi-user / multi-tenant deployment. At
  that point a per-channel seq becomes an isolation requirement, and the
  `Resume::Durable` / `Pos::Durable` resume-token contract is re-cut — an
  accepted contract break under the house no-compat-shim rule, costing each
  client at most one `BeyondRetained`-class gap on upgrade.

### 6.2 Accepted risk — the surface Subscribe/Unsubscribe rate bucket is per-connection

The surface WS session meters `Subscribe`/`Unsubscribe` frames with a
per-connection token bucket (`SUBSCRIBE_BURST` tokens, 1/sec refill). The bucket
lives for the connection's lifetime, so a client that drops and re-opens the WS
socket mints a fresh full bucket each time. Nothing bounds or signals the WS
*connection* rate itself beyond the per-surface session-count cap
(`MAX_SESSIONS_PER_SURFACE`, with a per-(surface, user) sub-cap
`MAX_SESSIONS_PER_USER_PER_SURFACE` bounding how much of it one account holds);
a reconnect is a normal close, not a fail2ban signal.

- **What it enables:** an authenticated principal (or a stolen session cookie, or
  a hostile out-of-tree surface component) can loop connect → spend the full
  Subscribe burst → drop → reconnect, sustaining subscribe/replay DB work above
  the 1/sec steady-state the bucket implies.
- **Why the blast radius is bounded:** the work *per* Subscribe is
  config-bounded — the resolved `push_depth`/`retain_depth` of a durable surface
  binding are boot-required to be bounded, so each subscribe's parked load and
  retained re-send are capped, and the residual per-connection replay-dedup state
  is hard-capped (`REPLAY_SENT_MAX`). The abuse is therefore sustained
  *degradation* (extra contention on the shared DB connection mutex), never
  unbounded resource growth.
- **Why it is accepted:** the deployment model is single-operator. The only
  principal who can drive this loop is the operator (or someone who has already
  compromised the operator's session, which grants far more than a subscribe
  storm). The leak/abuse crosses no tenant boundary because there is exactly one
  tenant.
- **Revisit trigger:** any move to a multi-user / multi-tenant deployment, or any
  deployment exposing surface WS endpoints to principals outside the operator's
  own trust. At that point the subscribe bucket (or a coarser
  connects-per-interval bucket) must key on the authenticated identity or client
  IP rather than the connection, and a WS-connect-rate security signal should
  feed fail2ban.

### 6.3 Accepted risk — durable surface publish writes are gated only by the per-connection bucket

A surface component with an operator-authored durable output binding
(`grants = ["publish"]` + a covering `publish_acl` matcher + an
`[[surface.output]]` onto a `brenn:` channel — all three, all boot-validated)
publishes to the durable channel via `Messenger::publish_from_surface`. Every
such publish is a `System`-origin send: it takes **no** per-conversation send
budget (that gate is for LLM/automation origins), so the only rate limit is the
same per-connection publish token bucket that already meters ephemeral surface
publishes, plus the session body cap. Unlike an ephemeral publish, a durable
publish **writes to SQLite** (a message row + per-subscriber pending-push rows).

- **What it enables:** the same reconnect-churn loop as §6.2 (connect → spend the
  full publish burst → drop → reconnect mints a fresh bucket), now driving durable
  *writes* — one message row plus one pending-push row per subscribed durable
  consumer per admitted publish — above the bucket's 1/sec steady state.
  Additionally — and unlike the ephemeral arm, which shares one bus-level
  per-sender gate across every connection of a `surface:<slug>` participant — the
  durable arm has **no aggregate gate at all**: N concurrent WS sessions for the
  same surface multiply admitted durable writes by N, with only each connection's
  own bucket in the way (no reconnect churn required). See
  `TODO(surface-publish-budget-layering)` in `bootstrap/messaging.rs`, whose
  durable analogue is precisely this "no aggregate gate" gap.
- **Why the blast radius is bounded:** growth is not unbounded. Per-subscriber
  push windows GC-retire rows beyond the binding's boot-bounded `push_depth`, and
  the retained window GCs per channel config; a channel with no durable
  subscribers persists only the message row, itself subject to the channel's
  retained-window GC. The residual is the same shape as §6.2 — sustained
  *degradation* (extra shared-DB-mutex contention and disk churn), never unbounded
  resource growth. Parent D7 already made the "no cross-restart DB send budget for
  surfaces in v1" call; this note records that the durable arm adds no unmetered
  work beyond that decision.
- **Why it is accepted:** identical to §6.2 — single-operator deployment, the only
  principal who can drive the loop is the operator (or someone who has already
  compromised the operator's session), and the abuse crosses no tenant boundary
  because there is exactly one tenant. The content reaching durable consumers is
  honestly attributed (`surface:<slug>` sender), so no downstream authorization is
  keyed on forgeable provenance (§8).
- **Revisit trigger:** the same as §6.2 — any multi-user / multi-tenant
  deployment, or exposing surface WS endpoints beyond the operator's trust. At
  that point durable surface publish needs a budget keyed on authenticated
  identity (a cross-restart send budget in front of `publish_from_surface`, per
  parent D7's "additive gate" note), not just the per-connection bucket.

---

## 7. Boundaries B2 / B3 — The Claude Code Subprocess

CC is the most security-load-bearing boundary. The framing: **we control how CC
is launched (B2); we do not control what CC emits (B3).** CC output may shape
what the user sees and what host operations are proposed, but it must never (a)
bypass the human-approval gate for a privileged action, (b) reach the browser as
unescaped markup, or (c) inject into a host operation outside its sanctioned
scope.

### 7.1 Spawn parameters (B2)

CC's launch parameters — arguments, environment, working directory, the set of
servers it may talk to — come from operator configuration. The permission posture
CC runs under is established at spawn and must not be weakenable by any value that
originates outside operator config.

**Policies:**

- **No browser- or CC-supplied value reaches CC's launch parameters except
  through operator config or a validating transform.** A value that must pass
  through (e.g. a user-selected timezone) is validated/normalized to a known-safe
  form before it becomes a spawn parameter; anything else is a finding.
- **The configured permission posture is authoritative and non-overridable.** The
  spawn must not admit a path — including operator "extra args" — that silently
  downgrades the permission posture CC runs under. The posture must be asserted at
  spawn and re-checked against what CC reports at startup; a divergence must be
  surfaced, not ignored.
- **Version dependencies that the security posture relies on are enforced.** If a
  security-relevant behavior of CC depends on a minimum version, the backend must
  refuse to run against an older one rather than silently lose the guarantee.

### 7.2 CC's output stream (B3)

CC's output is parsed and acted upon. The control path (where CC asks the backend
to make a decision) and the data path (content for display) have *different*
required dispositions of the unexpected.

**Policies:**

- **The control path is fail-closed.** A control request the backend does not
  recognize cannot be answered safely and must terminate the session and raise a
  high-severity alert — never be answered with a default or a guess. Making the
  control schema tolerant of unknown control requests, or answering one with a
  default verdict, is a finding.
- **The data path tolerates the unexpected but never silently.** Content-bearing
  messages may carry unknown subtypes (CC evolves), but unknowns must be surfaced
  as an observability signal, not swallowed.
- **Intake is bounded.** A single line/message from CC must have a hard size
  bound; exceeding it is a fatal session error, not a quiet truncation.

### 7.3 The approval gate — the central invariant

Any host side effect that CC proposes — running a subprocess, reading or writing a
file, committing — must be authorized before it executes, by exactly one of:

1. an **operator-configured auto-approve rule**, or
2. **explicit human approval**.

**Policies:**

- **No un-approved privileged action executes.** A path that performs a host side
  effect on CC's behalf without either an operator rule match or human approval is
  a **critical** finding.
- **Observation is not authorization.** Hooks or interception points that merely
  observe a tool call must not be repurposed into granting permission; doing so
  can bypass the real permission decision. "Optimizing" an observation point into
  an allow is a finding.
- **What executes must equal what was approved.** When a human approves an action,
  the action that runs must be the one displayed and approved. Where the protocol
  permits the browser to supply data back into the action (the interactive-answer
  case), that data must be a bounded, validated patch — it must not be able to
  rewrite a security-relevant parameter (command, path, target) of an
  already-approved action so that the executed action diverges from the approved
  one. An unbounded rewrite of an approved action without re-approval is a
  critical finding.
- **Auto-approve policy is operator-owned, with one bounded exception.** The
  auto-approve rule set is operator-controlled server-side. Where the protocol
  lets the browser contribute auto-approve patterns, those patterns must be
  validated before any rule is created, must be scoped so a rule cannot leak to
  another app or another user, and the matching grammar must be free of
  catastrophic-backtracking / resource-exhaustion hazards. A browser-contributed
  pattern that is persisted without validation or without scope binding is a
  finding.
- **The matching grammar is conservative.** Auto-approve matching must err toward
  *not* matching: input it cannot confidently parse (e.g. a shell construct it
  cannot fully decompose) must never be auto-approved. A grammar change that
  auto-approves on incomplete understanding is a finding.

### 7.4 CC-influenced sinks

CC-supplied data reaches several sinks. The standing policies, by sink class:

- **Subprocess arguments (hosted apps, version control).** CC-influenced values
  may become subprocess arguments. The argument-vector discipline (no shell
  interpretation) is mandatory and excludes shell injection. The *residual* risk
  is **option/path injection**: a value the receiving program interprets as a
  flag (leading `-`) or a path escape (`..`). The backend must either neutralize
  this at the boundary or the receiving program must treat such positionals
  strictly as data and enforce its own containment. Where containment is delegated
  to the receiving program, the reviewer must confirm that program actually
  enforces it — a delegated guarantee that no one enforces is a finding.
- **Filesystem reads/writes from a CC-supplied path.** Any path that originates
  outside the TCB must be confined to its sanctioned root by canonicalization plus
  a containment check that defeats `..` and symlink escape. A path outside all
  sanctioned roots must hard-deny. A path sink without this pattern is a finding.
- **Markup for the browser.** CC text rendered for display must pass the markdown/
  rendering trust boundary (§10). The approval-UI display of a proposed tool call
  must escape every CC-supplied string before it becomes markup.

**What CC is trusted with (and the limit of that trust):** CC-supplied
identifiers and labels may be used as opaque keys, stored, logged, and displayed —
**after escaping** where they become markup. They must **not** silently become a
security decision (a path, a command, an authorization) without validation. A
reviewer who finds a CC-supplied value used directly as a security decision
without validation has found a finding.

**What the reviewer verifies (B2/B3):** no untrusted value leaks into spawn
parameters; the permission posture cannot be downgraded; the control path is
fail-closed and the data path surfaces unknowns; the approval gate authorizes
every privileged action and what-runs equals what-was-approved; browser-
contributed auto-approve patterns are validated and scoped; every CC-influenced
sink (subprocess args, filesystem paths, markup) is guarded per the policies
above.

---

## 8. Boundary B4/B5 — WASM Guests

WASM serves two roles and the security bar is set by the harder one. Out-of-tree
guests are untrusted, third-party, adversarial; in-tree guests are first-party
but sandboxed to contain their own bugs. **The reviewer applies the
untrusted-guest bar to everything.** A guarantee that holds only for a
well-behaved guest is not a security guarantee.

**Threats:** sandbox escape (reaching host capabilities the guest was not
granted); resource exhaustion (CPU, memory, wall-clock, call volume, storage)
including denial of service against the host or other guests; cross-component data
access; injection or amplification through guest-produced strings that reach host
logs/alerts; laundering external-origin content through a guest so it appears
internally-originated.

**Policies the host must uphold:**

- **Deny-by-default capability model.** A guest can reach a host capability *only*
  if the operator explicitly granted it. The enforcement must be structural and
  fail-closed (refuse to run a guest that requires an ungranted capability), not a
  warn-and-continue. A capability reachable without a grant, or a grant check that
  degrades instead of refusing, is a finding.
- **No ambient host access.** Guests get no implicit access to filesystem,
  network, clock, randomness, or environment. Every host-exposed function is a
  deliberate, granted capability. A newly exposed host function without a grant
  gate is a finding.
- **Every untrusted-guest execution is resource-bounded.** A guest that runs on
  untrusted or external input must be bounded in CPU, wall-clock, and memory so it
  cannot hang or starve the host or block other work. A path that runs an
  untrusted guest with no effective CPU/wall-clock bound — particularly one fed by
  external input — is a finding. (If a given guest world cannot yet be bounded, it
  must be restricted to operator-trusted, in-tree guests only; accepting an
  out-of-tree guest into an unbounded world is a finding.)
- **Output volume is bounded.** Per-activation call budgets and payload-size
  limits must bound what a guest can emit, and guest-produced strings that reach
  host logs/alerts/diagnostics must be sanitized (length-bounded and made
  control-character-safe) so a guest cannot flood or corrupt host observability.
- **Per-component storage is isolated.** A guest's storage must be inaccessible to
  other components, and guest-supplied keys/namespaces must be handled as data
  (parameterized, never concatenated into query text), with bounds that prevent
  resource abuse. Transaction handling must be fail-safe: a failure that cannot be
  cleanly rolled back must escalate (up to host shutdown) rather than leave
  inconsistent state.
- **No trust laundering across the boundary.** Content a guest forwards from an
  external origin must not become indistinguishable from internally-originated
  content. Until provenance is carried across the boundary, **downstream consumers
  must not base any authorization decision on a message's claimed sender or type
  for any channel that admits guest-forwarded content.** A downstream
  authorization decision keyed on forgeable provenance is a finding.

**What the reviewer verifies:** capabilities are deny-by-default and
structurally enforced; no ambient access exists; every untrusted-guest execution
is CPU/wall-clock/memory bounded (or restricted to in-tree only); every WASM
consumer's activation rate is bounded by the per-component activation pacer (no
unpaced consumer path exists); output volume and guest strings are bounded and
sanitized; per-component storage is isolated and injection-safe; no downstream
authorization trusts forgeable provenance.

### 8.1 MQTT echo and republish loops

Brenn runs one MQTT session per `[[mqtt_client]]`, shared by both the publish
(egress) path and the ingress-delivery path. Because publisher and subscriber are
now the same MQTT client id, a publish to a topic that matches one of that
session's own subscribed bridge filters is delivered back to Brenn as an ingress
message — standard MQTT semantics (there is no `nolocal` suppression). This is
deliberate and does not create an unbounded loop:

- **LLM republish loop.** Each ingress→publish→ingress hop consumes the
  conversation's send budget (`SendBudget::Conversation`), and an ingress message
  only *wakes* the LLM when its subscription has `push_depth > 0`. The normal LLM
  MQTT mode is `push_depth == 0` (pull), which never wakes and so costs nothing.
- **WASM republish loop.** Bounded in code by the per-component activation pacer
  (`ActivationPacer`, `brenn/src/wasm_dispatch`). Every consumer activation —
  external wake, deadline wake, clamp self-renotify, and the startup sweep — is
  admitted through a per-component token bucket over *activations* before its drain
  step: sustained rate is capped at one activation per `activation_min_period_ms`
  (default 1 s) after an `activation_burst` (default 60) burst. The gate **delays**
  activations, it never drops them — but "never drops" is a property of the *pacer*,
  not the composed system: while a consumer is paced, ingress continues and its
  bounded input channels still evict oldest rows past their retention limit
  (surfaced to the guest as `dropped`, unchanged from before pacing). Because
  pacing removes a consumer's ability to outrun a flood, a peer with publish
  rights to a subscribed filter can force such eviction of legitimate messages
  more readily than before; broker ACLs remain the write-side gate for that.
  Within each hop, egress is now bounded first by per-sink publish token buckets —
  one bucket per output port and per ACL-allowed MQTT client, so MQTT egress is
  covered too, not only the bus. Each bucket defaults to one token per new input
  envelope (amplification 1.0) plus a fill of 1.0, capacity 1.0 — i.e. roughly one
  publish per new envelope per sink, operator-tunable via `amplification` /
  `publish_per_activation` / `publish_capacity`, with `amplification < 1.0`
  attenuating below 1:1. The global per-activation quotas
  (`PROCESSOR_MAX_PUBLISHES_PER_ACTIVATION`,
  `PROCESSOR_MAX_PUBLISH_CALLS_PER_ACTIVATION`) remain as outer backstops behind
  the buckets, so a maximally-amplifying looping component's worst-case sustained
  pressure is `PROCESSOR_MAX_PUBLISHES_PER_ACTIVATION` publishes per period —
  bounded, and now conservative by default rather than only tunable downward. Echo fan-out does not defeat the limit: echoed rows
  batch into the *next* activation's snapshot (batched multi-port delivery), so N
  pending echoes cost one paced activation, not N. Throttle entry is
  **security-logged** (`WasmActivationThrottled`, component-attributed — no `ip`,
  never fail2ban-matched) and **phone-alerted** — the *first* throttle episode per
  process per slug pages; later episodes in the same process are security-log-only
  (the alert dedups for the process lifetime), so detection of a subsequent loop
  depends on reading the security log — so a triggered loop is *detected*, not just
  bounded. This pacing is per
  `[[wasm_consumer]]` and applies identically to in-tree and out-of-tree
  components; there is no opt-out (an operator who truly wants "unlimited" can
  author an extreme `activation_min_period_ms = 1` with a huge burst). **Residual,
  named honestly:** a loop that paces *itself* below the sustained rate is not
  throttled and never alerts — by design, it is indistinguishable from legitimate
  traffic; broker ACLs (the write-side authority — a component that cannot publish
  to its own input topics cannot loop) and operator review of config for
  input-filter/publish-matcher topic-space overlap remain the gates for that case,
  and the per-period cost is bounded regardless.
- **Retained self-publish.** `retain=true` to a self-matched filter echoes now and
  re-delivers on every reconnect (`OnEverySubscribe`) — already true before
  unification, cross-session.

The write-side trust boundary is unchanged: broker ACLs remain the authority over
which topics a client may publish to and subscribe from. Inbound MQTT payloads are
untrusted (prompt-injection vector); the broker ACL, not `nolocal`, is what keeps a
client from injecting into topics it should not reach.

---

## 9. Boundary B6 — Hosted-App Subprocesses

Hosted-app and version-control subprocesses are invoked by the backend with
inputs that may be CC- or browser-influenced.

**Threats:** command injection; option injection; path escape; resource
exhaustion via unbounded subprocess output; log corruption via subprocess output.

**Policies:**

- **No shell.** Subprocesses are invoked through an argument vector, never by
  building a shell command line with interpolated external input. Any sink that
  constructs a shell string from external input is a finding.
- **Static parameters stay static.** Working directory, container configuration,
  and any privileged invocation parameters come from operator config, not from CC
  or the browser.
- **Option/path injection is contained.** Externally-influenced positionals must
  be treated as data by the receiving program, and any path argument must be
  contained to its sanctioned scope. Where Brenn does not itself enforce
  containment, the receiving program must, and the reviewer must confirm that the
  guarantee actually lives somewhere — it cannot be assumed by both sides to be
  the other's job.
- **Output is bounded and sanitized.** Subprocess output is size-bounded and
  time-bounded, and is stripped of control characters before it reaches any log or
  security-signal field, so subprocess output cannot forge or split a
  security-event record.

**What the reviewer verifies:** no shell construction anywhere on this boundary;
privileged invocation parameters come only from operator config; option/path
injection is contained by someone and that someone is identified; subprocess
output is bounded and log-sanitized.

---

## 10. Rendering / XSS Boundary

The server renders untrusted content (CC-authored and artifact text) into markup
the browser displays. This server-side rendering path is *the* XSS trust boundary;
the browser does no sanitization and must not be relied on to.

**Policies:**

- **Raw HTML never passes through.** The renderer must strip embedded raw HTML
  from untrusted source content. Re-enabling raw HTML passthrough is a critical
  finding.
- **All rendered text is escaped.** Body text, code, and any interpolated
  user/CC/guest value reaching markup must be escaped. A rendering path that emits
  an unescaped externally-influenced value is a finding.
- **Dangerous link schemes are neutralized.** Links carrying executable or
  otherwise dangerous schemes (e.g. `javascript:`) must not survive rendering into
  an active form. Where a content-security policy is relied upon as the mitigation
  for a residual scheme risk, its sufficiency must be a conscious, reviewed
  decision rather than an accident.
- **Defense in depth at the policy layer.** A content-security policy should
  constrain script sources and the most dangerous sinks. Treat missing
  hardening directives as defense-in-depth gaps to weigh, given that the primary
  guard (raw-HTML stripping + escaping) is what actually stops injection.

**Why this matters even though CC is "ours":** CC is semi-trusted (§7). A
compromised or manipulated tool server can feed CC content that attempts HTML or
script injection. The rendering boundary is the standing defense, which is why CC-
and artifact-derived markup is treated as untrusted regardless of CC's
provenance.

**What the reviewer verifies:** raw HTML is stripped; every rendered value is
escaped at its source; dangerous link schemes do not survive into active markup;
the CSP's role as a backstop is deliberate and adequate for the residual cases.

**Surface components rendering markdown browser-side.** A surface component may
render publisher-supplied markdown in the browser, but **by DOM-API construction
only**: pulldown-cmark events are turned into DOM nodes via `createElement` (a
fixed, safe tag set) and `createTextNode` — no HTML string is ever produced, no
`innerHTML` is used, raw HTML events (`Html`/`InlineHtml`) are dropped at the
parser, and **no anchor element is ever created** (links degrade to their child
text). This is strictly stronger than escape-then-`innerHTML`, because
nothing ever parses untrusted text as markup — injection is impossible by
construction. Because no navigation affordance exists at all, the §12
dangerous-scheme question is moot for this path. This does **not** relax the
rule above: there is still no browser sanitizer, because there is still no
untrusted-HTML parse. The `'wasm-unsafe-eval'` analysis (§10.1) is unaffected —
no new CSP relaxation and no external fetch (images create no element and load
nothing).

### 10.1 `'wasm-unsafe-eval'` on surface documents

The strict Content-Security-Policy pins `script-src 'self'` site-wide. Surface
HTML documents (`GET /surface/{slug}`) carry one relaxation:
`script-src 'self' 'wasm-unsafe-eval'`. Every other response — legacy app
pages, login, static assets, the `/surface-static/` module tree — keeps the
strict policy byte-for-byte.

**What the keyword permits.** `'wasm-unsafe-eval'` unlocks *all* WebAssembly
compilation APIs at once: not only the streaming forms
(`WebAssembly.compileStreaming` / `instantiateStreaming`, whose fetched
`Response` is still origin-constrained by `connect-src`), but equally the
non-streaming forms — `WebAssembly.compile(bytes)`,
`WebAssembly.instantiate(bytes)`, `new WebAssembly.Module(bytes)` — which take
arbitrary `BufferSource`s that **no** CSP directive origin-constrains. The
bytes may come from anywhere page JS can already reach (a WS envelope, pasted
text, base64 in any content channel). The keyword is therefore **not** a
"same-origin wasm bytes only" guarantee — that framing is false.

**Why it exists.** Every shipped major engine (Chrome ≥97, Firefox ≥102,
Safari ≥16) refuses *all* wasm compilation under `script-src 'self'` without
`'wasm-unsafe-eval'` (or the broader `'unsafe-eval'`), including
`instantiateStreaming` of a same-origin response; no shipped CSP mechanism
allows origin-gated wasm compilation. The surface shell and every component are
wasm modules (wasm-bindgen `--target web`, `fetch` +
`instantiateStreaming` with a non-streaming `instantiate(bytes)` fallback), and
the shell is DOM-bound by design, so moving compilation into a worker with its
own CSP is not viable. Dropping the keyword means surfaces cannot run at all.

**The bounding argument.** The risk is bounded not by any origin restriction on
the bytes but by two facts: (a) invoking any `WebAssembly.*` API requires
pre-existing arbitrary JS execution, which the rest of the policy
(`script-src 'self'`, no `unsafe-inline`, no `unsafe-eval`) already denies to
injection-class attackers — unlike `'unsafe-eval'`, it creates no new
string→code primitive; and (b) a wasm module's capabilities are strictly a
subset of the instantiating JS's — it touches the outside world only through
imports that JS supplies.

**Scope and mechanism.** The relaxation is confined to surface documents
because only they benefit; legacy pages would otherwise carry a bytes→code
capability they never use, against deny-by-default posture. The strict policy
is the unconditional default: the `surface_page` handler stamps a response
*extension* marker (never a header, so it cannot leak to the client), and the
outer security-headers path swaps the relaxed CSP in only when the marker is
present. There is deliberately no "respect a handler-supplied CSP header" path
— that would let any future handler silently weaken the policy; a handler must
opt in through the marker, and only `surface_page` does.

---

## 11. What Counts as a Finding (reviewer rubric)

Raise a finding when any of the following hold. Severity in brackets is guidance.

1. **Fail-open on a security path.** [Critical] A control message, permission
   decision, capability/grant check, auth check, or path-containment check that
   defaults to *allow* / proceeds on the unexpected case instead of failing loud
   and surfacing a signal. (§1, §5–§9)
2. **Authority not re-derived from the session.** [Critical] A handler that trusts
   a browser-supplied identity/owner/user field instead of the authenticated
   session. (§6)
3. **Approval-gate bypass or approved-action substitution.** [Critical] A
   privileged action (subprocess, file read/write, commit) executed without an
   operator rule match or explicit human approval; **or** browser-supplied data
   that rewrites a security-relevant parameter of an already-approved action so
   what runs diverges from what was approved, without re-approval or bounded
   validation. (§7.3)
4. **Unescaped external value into rendered markup.** [High] A value that reaches
   the browser as markup whose producer does not escape or safely render it.
   (§6, §10)
5. **Raw HTML or dangerous link scheme survives rendering.** [High] Re-enabling
   raw-HTML passthrough, or rendering an executable-scheme link into active form
   without mitigation. Introducing `innerHTML` — or any equivalent markup parse
   of untrusted text (`insertAdjacentHTML`, `DOMParser` into a live tree) — under
   `surface/`, or creating an anchor element in a surface component's rendered
   output, falls under this class at the same [High] tag. (§10)
6. **WASM capability or isolation escape.** [Critical] A guest reaching a
   capability outside its grants; a host function exposed without a grant gate; an
   untrusted guest run without effective resource bounds (especially on external
   input); or cross-component storage access. (§8)
7. **Shell command construction.** [Critical] A subprocess invoked via a shell
   string with interpolated external input. **Option/path injection** (an
   unguarded `-`-prefixed positional or `..` path with no containment owner) is
   [High]. (§9)
8. **Filesystem path sink without canonicalize + containment.** [High] An
   externally-derived path joined to a root without canonicalization and a
   containment check that defeats `..` and symlinks. (§7.4, §9)
9. **Missing/weakened security signal.** [Medium] A malformed or unauthorized
   input path that drops input silently without recording a security signal; or a
   closed protocol schema (inbound browser messages, CC control requests) made
   tolerant of unknown variants it should reject. (§6, §7.2)
10. **Log/observability integrity.** [Medium] External input reaching a log/signal
    field unsanitized such that it can forge or split a security-event record.
    (§3, §8, §9)
11. **Crypto/secret handling regression.** [High] Reversible password storage,
    weakened hashing parameters, non-CSPRNG security tokens, predictable
    session/CSRF/registration values, or non-constant-time secret comparison on an
    oracle-exposed path. (§5)
12. **Resource-exhaustion gap on an untrusted boundary.** [Medium] A boundary that
    ingests external input without a size/CPU/wall-clock bound where one is
    structurally expected. (§6, §8)

A reviewer should **not** raise as a finding: operator-config trust (the operator
is the trust anchor); a behavior the project has consciously accepted as tracked
debt (raise only *new* regressions of that class); or a defense-in-depth gap
explicitly weighed and accepted as such (raise it only if the *primary* guard for
that risk is also missing). (NOTE: Config *footguns* may be raised as concerns; if
it is difficult for an operator to create a secure configuration, or easy for them
to get it wrong in a non-obvious way, that's a legitimate complaint.)

---

## 12. Open Questions (policy / judgment)

These are genuine policy and threat-tolerance questions for the operator/owner.
They are *not* code-detail questions — those belong in review and exploration
artifacts, not in this standing policy.

- **Out-of-tree extension acceptance.** Out-of-tree WASM guests are first-class
  per project philosophy and are treated as adversarial here. Which guest *worlds*
  is the operator willing to expose to out-of-tree code, given that an unbounded
  world must be restricted to in-tree until it can be bounded (§8)? This is a risk-
  tolerance decision, not a code question.
- **`javascript:` / dangerous-scheme link policy.** Is stripping dangerous link
  schemes at render time required policy, or is the content-security policy an
  acceptable sole mitigation for the residual case (§10)? A defense-in-depth call
  for the owner.
- **CSRF posture for future entry points.** The current state-changing surface is
  protected structurally and by token where applicable. Is that the standing
  policy for *all* future entry points, including any non-form, non-session-channel
  endpoint, or should a uniform token requirement be mandated (§6)?
- **Forwarded-external provenance.** Until provenance is carried across the WASM
  boundary, downstream consumers must not authorize on claimed sender/type (§8).
  Is "no authorization on provenance for guest-admitting channels" the accepted
  long-term policy, or is carrying provenance a required capability the system
  must add before such channels are trusted?

---

## 13. Maintenance

This document changes only when the *security model* changes — not when the code
changes. Update it when:

- a trust boundary is added, removed, or re-scoped;
- the trust decision for an actor changes (e.g. a previously trusted party becomes
  attacker-influenceable, or a new class of actor appears);
- a new asset worth protecting, or a new major external-input surface, is
  introduced;
- a standing policy or invariant is added, removed, or changed.

Do **not** update this document to track an implementation change, a bug fix, a
refactor, or the resolution of a specific review finding. If a proposed edit would
be invalidated by a future code change, it does not belong here — it belongs in an
exploration or review artifact.
