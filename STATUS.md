# Brenn Status

Forward-looking only. Done work in git. Code debt in TODO.md.

Vision: graf `personal-assistant-vision.md`. PRD: `docs/designs/persistent-assistant.md`.

## North Stars

All present, all weighted. Near-term bias: N1+N2.

### N1. Always-present PA

The little buddy on your shoulder. Mostly silent, occasionally surfaces a reminder, suggestion, or "hey, I noticed X." Mobile-first, many touches per day, often Brenn-initiated rather than user-initiated. Concrete gap: Bob is on Brenn ~1×/2–3 days, laptop-only, intentional-mode use; vision is many×/day, phone-first, ambient.

Success metric: mobile share, daily-active frequency, ratio of unsolicited Brenn moments to user-initiated opens.

### N2. Agentic orchestration beyond UI

Brenn does meaningful work without the user in the seat. Scheduled triggers, event-driven runs, multi-step jobs across hours/days. "Background research while you sleep." "Process the inbox at 7am." "Remind at the right moment, not the calendared one." Required by N1 — buddy moments need prep work and the right-moment trigger.

### N3. Home / device intelligence

Brenn replaces Google Home, drives HomeAssistant, knows where the user is and what's around them. Voice in/out, ambient. Existing LIFX + HA infra is the seed. Different surface area (voice, devices, low-latency) — pursued opportunistically, not as a primary path right now.

### N4. Coding / research uplift

Brenn beats raw Claude Code as a coding/research driver. Codebase awareness, multi-agent review, persistent research threads, branch hygiene. Different audience (technical user) than N1/N3 (Bob, ambient consumer). Today direct CC is superior; closing that gap is its own track.

### N5. Idle-allotment burndown — *tactic, not a star*

Use leftover Anthropic subscription tokens for background work each 7-day window. Whichever workstream ships consumes it: N1+N2 → background prep producing tomorrow's buddy moments; N4 → background coding chores. Not its own direction.

### N6. Signal, not noise

The phone goes quiet. Push notifications become rare and meaningful — what doesn't earn a now-ping waits for the next sit-down with Brenn. Inbound streams (email, homelab logs, eventually SMS / calendar invites / RSS) get triaged: cheap filters where filters work, LLM judgment where they don't. Brenn proactively unsubscribes from sources that keep failing the test. Operative principle: **cheap layer first** — filters > rules > LLM-per-message. The LLM's job is largely to *write filters*, codifying recurring patterns into rules, leaving judgment-cases for itself.

Success metric: pushes-per-day-per-user trending down, ratio-of-pushes-that-mattered up. Inverse-but-complementary to N1 (N1 surfaces the right moment; N6 kills the wrong ones).

Substrate: JMAP/Email + AE + push-policy. Near-term concrete steps: research Fastmail JMAP write surface (sieve-script management for filter CRUD), design write-capability gating (per-action-class allowlist; preview-before-activate for filter creates; approval flow for destructive operations).

## Why Brenn vs LangChain / OpenClaw / etc.

Brenn is a thin auditable Rust harness around CC subprocess. Not an agent framework — CC is already the agent. We don't write `Tool`, `Chain`, `Memory`, `Provider`, `Channel` abstractions because we don't need them; CC has tools, the host process has session/auth/storage, and the catalog is hand-curated UI.

OpenClaw is ~1.9M LoC of TypeScript and a transitive npm jungle. LangChain is a build-your-own-agent toolkit for problems we explicitly delegated downstream. Both assume in-process API calls; we host the agent as a subprocess and treat it as the brain. Differentiation is small surface, auditability, and refusing to grow into the OpenClaw shape.

## Workstreams

### Buddy reach: Device ID → Usage observability → PWA Push

The N1 implementation track. Sequenced because each layer is the prereq for measuring whether the next one moved Bob's behavior. Without observability we ship features blind.

- **Device ID.** Persistent identity (cookie/token, `devices` table, last-seen, viewport, type). Foundation for both push routing and per-device usage attribution. First concrete step: `SetUserTimezone` virtual tool. Closes `brenn-ui-tz-bypass-fix` along the way.
- **Usage observability.** Sessions table, per-event log, LLM session summaries at compaction or session end. Local SQLite only. Baseline lands *before* push so push impact is measurable in real numbers, not vibes.
- **PWA Push.** VAPID + subscription storage + service worker + push as a transport in messaging. Per-device targeting (we pick who/what/when). The actual lever for Brenn-initiated moments.

Native app is deferred. Android-only sideload + self-hosted F-Droid is the realistic path if push priority limits or home-screen widgets eventually force it. iOS stays PWA — paying Apple for two devices isn't earned yet.

### Automation Engine

The general system underneath "do X when Y." Trigger → Condition → Action with cross-cutting concerns (max-wait, urgency, batching, cooldown, debouncing). Modeled on HomeAssistant's automation engine. Subsumes idle-hooks, daily CC restart, scheduled messaging, event-queue delivery — building any of those narrowly throws away the obvious common abstraction.

Time-based scheduling absorbed by messaging deliver-after. AE owns state-change and event triggers (CC idle, client connected, repo dirty, etc.).

Design: `brainstorm-automation-engine.md`.

### Messaging

How Brenn talks to the world (and itself), bidirectionally. Built on the event queue. MVP (intra-Brenn channels + deliver-after) shipped; this workstream is everything above:

- Discord transport (two-way).
- Push transport (see Buddy reach).
- Pub/sub fan-out (broadcast channels — household-updates, etc.).
- App template/instance split for unambiguous per-user channel addressing.

Designs: `intra-brenn-messaging.md`, `brainstorm-messaging-and-channels.md`, `push-targets-and-messaging.md`.

### Repo Management

Repos as first-class managed resources. Agents request, share, and operate on them with proper ACLs.

- Agent-managed repos (PA tools to add/remove at runtime).
- PR workflow via Forgejo MCP.
- Repo config → DB for dynamic management.
- Release notes broadcasting (changelog delta → structured context block to relevant conversations, so perma-conversations learn about new tool capabilities they're running on top of).

Sync hardening punch list in `tech:`. Deferred items in `repo-sync-future-work.md`.

### PA LLM Integration Depth

Making the LLM a first-class participant in the UI, not just chat. The 4-mode model from the vision doc.

- **Mode 4 — Select and chat.** Shipped.
- **Mode 3 — Echo to CC / observer mode.** Direct-manipulation actions injected as structured context so the LLM notices patterns and can comment. Unblocks `graf-subprocess-error-surfacing`, `graf-query-error-visibility`, `todo-ui-refresh-on-state-change` (the pattern is "LLM gets told, decides what to do" instead of UI error panels).
- **Mode 2 — LLM-curated views.** LLM queries graf, applies judgment, tells Brenn "render these specific tasks in this order, with this layout." DisplayFile-equivalent for the task list and other panels.

### Structured LLM Messaging

Unify how Brenn injects structured context into the CC stream. Convention is live in select-and-chat (multi-block user messages, compact-JSON context blocks). Migrate event batch delivery, file attachment notifications, and compaction reminders to use it.

### Agent Task Lists & Knowledge

Persistent task lists and KB *for the agent itself*, separate from user-facing graf data. Lets agents track their own work across compactions, accept assignments from humans and other agents, maintain personal knowledge that survives session boundaries.

Hard problems: agent-vs-user data separation (own graf repo? domain-based partitions? two manifests?); cross-agent task identity (do tasks carry IDs as they hop Alice → Alice's PA → Bob's PA → Bob?); reliability against fallible LLM actors (Brenn is the reliable actor; track outstanding requests, escalate on timeout).

Acceptance test: **no task ever gets dropped on the floor.** A task accepted by an agent — directly or via hand-off — is either completed, escalated, or visible as overdue; never silently lost.

Messaging and AE first bricks have shipped, unblocking design. Design: `brainstorm-agent-tasks.md`.

### PA Direct Manipulation UI

Components that work without LLM round-trip — fast, native-feeling. Mode 1 from the vision doc.

Pending: task detail view, due-date badges, labels/filters UI, lint error display, domain-based information boundaries.

### JMAP / Email & Calendar

Email JMAP works (read-only, currently lives inside pfin; extraction owed but not blocking). Open work:

- Calendar via JMAP — degoogling track. Google Calendar connector still works for what's on Google; the migration is the trigger for adding calendar to JMAP.
- Gated write capabilities — per-action-class allowlist (mark-read auto, filter-create with preview, delete/send-as approval), per-app grants. Design owed.
- Fastmail filter (sieve-script) API research — the cheap-layer primitive for N6.

### Pfin

Maintenance mode. `integration-pfin-config-access`, `integration-pfin-hooks`.

## Future / Backburner

Real ideas, not active work.

- **Mobile Capture.** General "capture a thought" surface — text, photo, audio — that the LLM triages downstream (todo? graf doc? ping someone?). Likely a native app eventually for OS-level share-sheet integration.
- **Gamification.** Streaks, badges, dopamine. The honest reason: a todo list only beats sticky notes if using it is at least as rewarding. The "reach for the phone instead of the envelope" problem.
- **Managed Subagents.** Brenn-owned subagent lifecycle, decoupled from parent CC. Survives parent restarts, runs in scoped environments, reports back via messaging. Use cases: source-code consultation, async research, scheduled briefings, restricted-execution agents.
- **Native Android app.** Trigger: priority-differentiated push (per-channel importance) or home-screen widget. Path: APK sideload + self-hosted F-Droid repo. iOS stays PWA.
- **Channels + tmux transport.** Contingency for Anthropic OAuth enforcement against third-party harnesses. Migrate CC integration off NDJSON-stdio onto Anthropic's Channels protocol with tmux-hosted CC. Not active; revisit on signal.
- **DB-backed app access + admin UI.** Move per-app access control (`allowed_users` on each `[[app]]`) out of TOML config into the database, with an admin interface to manage grants. Today access changes mean editing a config file and restarting (config is load-once-at-boot). Premature for the current 2-user deployment, but the right shape once multi-tenant or self-service delegation becomes real. Scope: new `(user, app)` grant table, runtime hot-reload (or per-request read) of access, admin routes + catalog UI + ts-rs schemas. Note the dual role of `allowed_users` today — it's also the "owner username" resolver for singleton apps (messaging/MQTT/webhook/automation derive owner from `allowed_users.first()`), so a migration must preserve or explicitly replace that. (Moved from TODO.md slug `db-app-access` during the 2026-06-03 burndown — too big for a TODO, real as a feature.)

## Open Architectural Questions

- AE ↔ Messaging dependency direction. Likely AE-on-Messaging (deliver-after = scheduling primitive).
- Singleton-as-push-target ambiguity. Gates per-device push routing.
- Singleton semantics: per-app vs per-user. Blocks unambiguous channel addresses.
- App template/instance split. Config refactor.
