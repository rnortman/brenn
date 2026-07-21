# Brenn

Brenn is a browser-based framework for hosting AI-driven applications backed by Claude Code as a subprocess. The browser is a thin rendering layer; the backend is the brain. Apps like **pfin** (personal finance) and **graf** (knowledge base) run inside Brenn.

Brenn is more than a harness for running LLMs with a UI on them. Brenn is also a WASM-based application framework with a pub/sub messaging bus (supporting an internal protocol as well as webhook, MQTT, and PWA Push interfaces). This framework can host any application, AI or not, as WASM components with locked-down capabilities. This allows containment of potential damage from buggy applications. LLM conversations can connect to the same messaging bus, allowing complex systems to be built of both LLM-driven and logic-driven automations.

## Architecture

```
Browser (PWA)  ←→  WebSocket  ←→  Rust/Axum Backend  ←→  Claude Code (subprocess)
 (TS → JS)           (JSON)       (session mgmt,         (NDJSON stdio)
                                   auth, routing,
                                   markdown rendering)
```

Claude Code runs as a child process. Communication is bidirectional NDJSON over stdio. The backend manages sessions, authentication, tool approval routing, and markdown rendering. The browser receives pre-rendered HTML and displays it.

## Non-Negotiable Principles

### Backend: BETTER DEAD THAN WRONG

Robust does not mean "takes a licking and keeps on ticking." Robust means **does not do the wrong thing, ever; bites the cyanide capsule before it allows the wrong thing to happen.**

- **Fail fast on errors.** There is no uptime SLA. Panic rather than tolerate a bug.
- **No fallbacks. No swallowing errors. No "keep calm and carry on."** Period, end of story.
- **Panic if anything unexpected happens.** (Transient errors like an API being unavailable are not unexpected — handle those normally.)
- Every `unwrap()` is a conscious decision that this should panic if wrong. Every `?` propagation is intentional. No `let _ = ...` on Results.

### Frontend: Thou Shalt Not Put Application Logic in JS/TS

#### "Legacy" TS UI

- The browser is a **rendering layer**. Backend delivers rendered HTML; frontend sets `innerHTML`.
- UI/interaction logic in the browser is allowed, but **only barely as much as must be in the browser** to make the UI feel responsive and native.
- **All application logic stays in the backend, in Rust.**
- All messages between backend and frontend have **TypeScript schemas generated from Rust** (via `ts-rs`).
- No bundlers. No React. No NestJS. No npm swamp. Just `tsc` and `ts-rs`.
- Dependencies are minimal and each one is a deliberate choice.

#### New work-in-progress WASM-based UI

At time of this writing this is embryonic. The new UI approach moves Rust into the frontend via WASM. Almost all of the TS issues are not applicable to this new approach.

- Application logic and heavy lifting in the browser is OK if it's in Rust
- Communication is via extending the backend's message bus to Rust components in the frontend
- No `ts-rs` needed. TS/JS exist as browser API shims only and never parse messages
- Frontend code is still somewhat *untrusted* even when authenticated (see security posture)

### Logging and Security

The full security posture including threat model is documented in docs/security-posture.md. Read that whenever doing security reviews or security-related design or coding.

- **Browser sends non-compliant message?** Reject and log. That log feeds fail2ban.
- **Browser requests unrecognized URL?** 404 + log for fail2ban.
- **Anything sketchy from the frontend is signal for fail2ban.**
- **Claude Code sends unrecognized messages?** Log and surface (not fail2ban). Alert on phone — probably indicates a CC upgrade we need to adapt to.
- We need real observability and telemetry. Not janky, not enterprise-scaled-to-the-observable-universe. Solid and appropriate.

### Catalog, Not Generation

UI components are hand-crafted, not LLM-generated on the fly. Claude selects from a stable catalog of dialogs, cards, and flows. Users build muscle memory. Adding a new component is mechanical (~200 lines JS/CSS + schema + routing), and that's fine.

## Tech Stack

- **Backend:** Rust, Axum, tokio
- **Frontend:** TypeScript compiled to JS (no bundler), Web Components (Lit or vanilla)
- **Type bridge:** `ts-rs` (Rust structs → TypeScript types)
- **Markdown rendering:** Server-side (pulldown-cmark); plain `<pre><code>` output, no syntax highlighting
- **Auth:** Cookie/token-based sessions (proper security, not rolled-our-own crypto)
- **Storage:** Server-side session storage (conversation history persists across devices/refreshes)
- **CC integration:** Subprocess with NDJSON stdio, PreToolUse/PostToolUse hooks for approval routing. Requires Claude Code **>= 2.1.111** (Brenn spawns with `--permission-mode auto` by default, introduced in that version).

### WASM extensibility

There is a wasmtime extension capability here. The purpose of this is two fold: first to provide an *external* extension surface for out-of-tree components. Second to sandbox as much of Brenn itself as can be done without creating performance problems to limit the damage caused by bugs within Brenn. Both uses of WASM are legitimate and first-class.

In particular, whenever considering WASM-related design questions, remember: out-of-tree components are first class extension mechanisms. This means we need to look not only for in-tree impacts but consider the out-of-tree impacts as well.

### Building

`make build` does everything and is your go-to. It compiles backend and frontend.
`make check` runs checks.
`make launchdev` builds and starts the dev server in the background (uses `brenn.dev.toml`, binds to `127.0.0.1:3000`).
`make stopdev` stops it.
See Makefile for other targets that you don't need because those two do everything.

**IMPORTANT:** Run `make launchdev` exactly as-is. No `2>&1`, no `| tail`, no `| head`, no output redirection of any kind. The Makefile backgrounds the server process, and piping or redirecting its output will cause the pipe to hang forever because the backgrounded server holds the file descriptors open. Just run `make launchdev` bare.

Note that `git commit` will run `make check` as a pre-commit hook. This can be very slow -- several minutes. Give the commit command a long timeout.

## Working With Claude Code (Meta)

The built-in Explore agent uses Haiku. Haiku is cheap and fast but not the best model. For almost all purposes, a general subagent running Sonnet will be better at exploring and summarizing than the Explore built-in agent. **Always use Sonnet instead of the built-in Explore agent.**

## TODO System

Poor man's issue tracker. Two pieces that stay in sync:

- **`TODO.md`** at the repo root — the master list. Each entry has a slug (e.g., `config-file`), a one-line description, and optionally a note about context or urgency.
- **`TODO(slug)` comments in the code** — mark the exact spot where the work needs to happen. The slug matches an entry in `TODO.md`.

When you complete a TODO, remove both the `TODO.md` entry and the code comment. When you add a new one, add both. The slugs are the join key.

Don't use TODOs for vague aspirations. Every TODO should describe a concrete thing that needs to happen, in a place where it's obvious what "done" means.

## Code Standards

- Rust edition 2024, stable toolchain.
- `cargo clippy` must pass with no warnings. `cargo test` must pass.
- Frontend: `tsc --strict`. No `any` types.
- Comments follow `docs/comment-standard.md`. Two of its rules are backed by a mechanical scrub gate: Rule 1 (no references to ephemeral docs) and Rule 9 (generic names — `alice`, `example.com` — in comments, examples, and fixtures).
- No backwards-compatibility shims for internal APIs. Just update all callers at once. This includes config file changes; it should be normal to be forced to update config when deploying a new version. Backend/frontend protocol changes also must not have compat shims -- the frontend auto-refreshes after backend deploy anyway, just update the frontend at the same time as the backend. Backward compat is a *maybe* for external APIs/interfaces/contracts (e.g. webhook schemas, WASM/WIT contracts, MQTT schemas); any backward compat shims *must* be called out clearly at the design stage (or earlier, in requirements) otherwise the assumption is no backward compat.

## Tool vs Bash

**IMPORTANT:** Prefer tools (Read, Grep, Search, etc.) to Bash. Many Bash invocations will trigger human user review to ask for permissions. This slows down the whole process substantially. If a built-in tool can do the job, use that. It is perfectly acceptable to use Bash when the built-in tools don't do what you need.

### Running in the right cwd

Your cwd is persistent between commands; avoid running `cd`. You can build the frontend from root with `make build` so there's no reason to change directory.

If you are in the wrong directory, change back to project root. Do not prepend every command with `cd` because that requires user approval.

```bash
# Correct: be in ~/src/brenn already
make build
git status
git diff

# INCORRECT: cd in every Bash invocation
cd /home/rortman/src/brenn; git diff

# INCORRECT: Using -C arg to git
git -C /home/alice/src/brenn diff # DO NOT DO THIS
```
