# TODOs

## `scrub-template-drift-cache-skip`

`repo_template_matches_the_tracked_public_config` (scrub/tests/rules.rs) guards
`scrub/repo-template/gitleaks.toml` against drift from the live `.gitleaks.toml`,
but the xtask test cache keys the scrub::rules binary only on its own bytes plus
an env key that omits both gitleaks files (`collect_env_inputs` in
xtask/src/test_run.rs lists only the brenn config TOMLs). Drift between the two
gitleaks files therefore leaves the binary cached-as-passed and the check
skipped until the binary is recompiled for some other reason. A real template
drift can pass unnoticed. Pre-existing; unrelated to the write-exemption work.
Done when those two files feed the env key (or the check moves out of the cached
path) so the drift check runs on every relevant change.

Code site: xtask/src/test_run.rs, `collect_env_inputs`.


## `section-ref-burndown`

~968 pre-existing section-symbol references to ephemeral design docs in the
Rust tree (comment-standard Rule 1). Grandfathered: the scrub rule is
diff-only, so tree scans skip it and only newly touched lines are flagged.
Post-release cleanup, blocks nothing.

No code site: the instances are the work list.


## `takeover-parser-symmetry-guard`

The takeover anti-spoof guarantee holds only because the router's
parse-failure passthrough (`inject_takeover_instance`) and chrome's
parse-failure rejection (`on_takeover`) use the identical `TakeoverBody` serde
type with the same strictness. Nothing structural enforces that cross-crate
symmetry; a future loosening of chrome's parser (tolerant `Value` parse,
`#[serde(default)]` fields, a v2 body) would silently let an unstamped,
router-forwarded body through and reopen instance forgery.

Latent, not exploitable today (parsers identical). Done when the passthrough is
closed at the trust boundary (router drops what it cannot stamp) or the
strictness symmetry is pinned structurally.

Code site (`TODO(takeover-parser-symmetry-guard)`):
`surface/client/src/core/mod.rs`, `inject_takeover_instance`.

---

## `plane-version-check`

Every control-plane body carries a `v` version field stamped with
`CONTROL_PLANE_VERSION`, but the consumers deserialize it and never read it, so
any `v` (0, 7, 255) is folded as current. When a v2 body arrives it is silently
misinterpreted under v1 semantics instead of dropped-and-reported. This spans
all planes (theme, takeover, link-state, surface-state, toast); the versioning
rule is a cross-plane contract decision, not a per-consumer patch.

Done when the planes uniformly either check `v == CONTROL_PLANE_VERSION`
(drop-and-warn on mismatch) or drop the field until versioning is enforced.

Code site (`TODO(plane-version-check)`):
`surface/proto/src/lib.rs`, the `CONTROL_PLANE_VERSION` const.

---

## `kernel-registration-gate-lifecycle`

The kernel's activation-registration gate (`KernelCore.registered`) only ever
grows: nothing clears it on unmount, error-card teardown, or binding removal,
and the kernel never calls `ClientHandle::deregister_activation`. Correct today
because an instance id is page-unique-forever — a layout change reloads the
page, and a failed instance is terminal. If an instance's element is ever torn
down and a fresh element for the same id remounts within one page life, the gate
rejects the remount as a duplicate while the core still holds the old detached
host's entry (whose `Publisher` dispatches can no longer bubble to
`#surface-root`).

Done when instance-death teardown clears the gate and calls
`deregister_activation`, distinguishing death (deregister + clear) from Phase-3
chrome reparent (preserve delivery, never deregister). Wire it with the
kernel-driven death path, which is a later increment / Phase-3 concern.

Code site (`TODO(kernel-registration-gate-lifecycle)`):
`surface/kernel/src/logic.rs`, the `KernelCore.registered` field.

---

## `buffered-publish-routing-test`

The buffered-vs-gesture publish split — `ClientHandle::try_buffered_publish`
(instance-match) and the driver `invoke`'s in-flight-slot install/take
(`surface/client/src/driver.rs`) — has no direct test. Both are wasm-only
(`cfg(target_arch = "wasm32")`), and the client crate runs its unit tests
*natively* (`cargo test`); it has no wasm-bindgen-test harness, and
`make surface-wasm-test` runs only the shell and component-support suites. The
routing decision is covered behaviorally through component-support's fake-kernel
tests, but the real handle/driver slot glue is unverified.

Done when the client crate is wired into the browser test runner (entangled with
`surface-wasm-test-in-ci`) and a wasm-bindgen-test drives match / mismatch /
no-flight and the slot take-back.

Code sites (`TODO(buffered-publish-routing-test)`):
`surface/client/src/handle.rs` (`try_buffered_publish`),
`surface/client/src/driver.rs` (wasm `invoke`).

---

## `confirm-set-hard-cap-e2e`

The confirm-set-specific Violation wiring (`add_confirm` →
`ConfirmCapAction::Violation` → session kill) has unit coverage on `add_confirm`
only; no ws test drives a real client past the hard cap and asserts the session
is killed and logged. A first attempt over the parked-replay path hung: the
replay path does not appear to enforce the hard cap (it keeps sending
heartbeats), so the kill fires only on live sends — and whether replay *should*
enforce the cap wants a design answer before a correct e2e test is written.

Done when a ws test drives a live-send client past the hard cap, asserts the
session closes, and the replay-vs-live enforcement question is settled.

Code site (`TODO(confirm-set-hard-cap-e2e)`):
`brenn-server/src/routes/surface/session.rs`, `CONFIRM_SET_HARD_CAP`.

---

## `surface-wasm-test-in-ci`

`make check` now *type-checks* the browser-side wasm test suites
(`surface-wasm-check`'s second, scoped `--all-targets` invocation), so they can
no longer rot silently. They are still never **run** by any gate: `make
surface-wasm-test` needs a WebDriver browser driver and is in neither
`CARGO_CHECK_STEPS` nor `check-ci`. A type-checked suite that never runs still
answers no behavioral question — and these are the XSS-adjacent
text-not-markup pins, the DOM seam, mount/unmount, and port dispatch.

Done when `check-ci` runs `make surface-wasm-test`. **Blocked on host
provisioning, and the ordering is load-bearing:** CI is a persistent
`runs-on: shell` host runner, not an image; build tools are installed by
workflow steps via `cargo install`, and chromedriver is not cargo-installable
(Fedora: `dnf install chromedriver`), so it must be installed on the runner box
*first*. Landing the `check-ci` step before that turns CI red on every push to
main — which is also the auto-deploy-to-staging path.
wasm-bindgen-test-runner needs no provisioning: CI already installs
wasm-bindgen-cli, which ships it.

Local `make check` deliberately does *not* run them — no chromedriver
requirement on contributors. The compile gate is what keeps local commits from
rotting the suite; CI is what catches behavioral regressions before staging.

Code site (`TODO(surface-wasm-test-in-ci)`): `Makefile`, the
`surface-wasm-test` target; `surface/kernel/src/entry.rs`, the buffered-publish
`None` arm (absent host slot → `"not-permitted"`), which depends on the live
wasm host slot and can only be pinned by the browser test runner.

---

## `chrome-stale-sections-on-shrink`

Chrome's `apply_layout` (wasm half) iterates only the *current* `instances`, and
`ChromeCore.base_layout` is never re-validated when the arrangeable set changes.
A layout section (its `data-panel` slot + label header) created for an instance
that later leaves the set keeps its stamps forever, and a `base_layout` naming a
departed instance stays the base with that panel silently unfilled.

Latent today: within a page lifetime the instance *set* is fixed (only mount
states change), and any config change that adds/removes an instance forces a full
reload. Becomes a live layout-corruption bug the day dynamic instance add/remove
lands. Fix then: clear `data-panel`/label on sections whose `data-instance` is no
longer in `instances`, and drop `base_layout` when it fails re-validation against
the changed set.

Code site (`TODO(chrome-stale-sections-on-shrink)`):
`surface/chrome/src/logic.rs` (`ChromeCore::on_surface_state`).

## `ingress-retirement`

**Urgency: near-term — user wants this tackled tomorrow afternoon.**

The `ingress` row-kind is the last non-scheme value in the
`messaging_messages.envelope_type` column. It survives only as a storage-only
codec variant (`EnvelopeTypeColumn::Ingress`) plus the channel-less ingress
message/render machinery. Its one live writer is repo_sync, which enqueues pull
results as channel-less `ingress` rows instead of publishing onto a real bus
channel. Retire it in three steps ("done" is when the `Ingress` variant and the
ingress-only code paths are gone):

1. **Modernize repo_sync**: publish pull results onto a real bus channel
   (`brenn:` scheme) via the normal publish path, instead of writing channel-less
   ingress rows through `insert_ingress_message_raw`.
2. **One-time migration** of the existing prod ingress rows (~76, all
   `ingress_source = 'repo_sync:pulled'`) onto that channel — or delete them if
   the history is worthless; decide at migration time.
3. **Delete the remnants**: `EnvelopeTypeColumn` collapses to a bare
   `ChannelScheme`; remove `IngressOrBus` / `IngressEvent` and the ingress
   decode/render (`[Event]` card) paths, `insert_ingress_message*`, and the
   `ingress_*` columns/queries in `brenn-lib/src/messaging/db/ingress.rs`.

Code sites (`TODO(ingress-retirement)`):
`brenn-lib/src/messaging/db/envelope_column.rs` (`EnvelopeTypeColumn::Ingress`),
`brenn-lib/src/repo_sync_cursor.rs` (the two `insert_ingress_message_raw`
writers), `brenn-lib/src/messaging/publish/mod.rs` (`insert_ingress_message`
writer), `brenn-lib/src/messaging/ingress.rs` (`IngressOrBus`).

---

## `tool-registry-migrate-git-family`

Only `git-repo-pull` has migrated to the first-class tool registry
(`brenn-server/src/tool_registry/`). The remaining git tools — ListRepos,
Status, GitRepoCommitAndPush, GitRepoRun — still ride the legacy PreToolUse /
PostToolUse intercept in `brenn-server/src/active_bridge/brenn_tools/git.rs`.
Migrating them is mechanical follow-up (one tool already proves the pattern):
give each a `ToolDescriptor` + `FastTool`/`AsyncTool` impl and delete its
intercept arm.

Code site: `brenn-server/src/active_bridge/brenn_tools/git.rs`,
`TODO(tool-registry-migrate-git-family)`.

---

## `tool-registry-absorb-apptool`

The legacy `AppTool` display registry (`build_tool_registry` in
`brenn-server/src/tools/mod.rs`) coexists with the first-class
`tool_registry::ToolRegistry`. `ActiveBridge` carries both `tool_registry` and
`tools`, a naming trap. The `AppTool` per-tool metadata (summary formatting,
auto-approve) should eventually fold into `ToolDescriptor` so there is a single
tool table.

Code site: `brenn-server/src/tools/mod.rs` (`build_tool_registry`),
`TODO(tool-registry-absorb-apptool)`.

---

## `tool-registry-unregistered-tool-sweep`

At bootstrap, `brenn:tools/*` may hold durable pending request rows for a tool
that is no longer registered (binary/config changed across restart). Executing
a request against a removed tool is wrong-thing territory; the sweep should
alert and delete those rows at boot. Not built this cycle: the async tool set
is fixed in code (only `git-repo-pull`), so a pending row can only name a
registered tool — the case is unreachable until tools become dynamically
(de)registerable.

Code site: `brenn-server/src/bootstrap/messaging/mod.rs` (async-tool request
channel wiring in `build_messaging`),
`TODO(tool-registry-unregistered-tool-sweep)`.

---

## `tool-registry-idempotency-dedupe`

`ToolDescriptor.idempotency` supports `RequiresKey`, but the executor-side
dedupe table (`tool_call_dedupe`, keyed `(tool, caller, idempotency_key)`, 24h
TTL) is not built this cycle. The convention (field name, key shape, TTL) is
fixed so cycle-2+ tools and guests are written against it; only the table is
deferred. Registering a `RequiresKey` tool panics until it exists.

Code site: `brenn-server/src/tool_registry/registry.rs` (`ToolRegistry::new`
registration panic), `TODO(tool-registry-idempotency-dedupe)`.

---

## `meeting-tick-visibility`

A headless (no-layout-slot) meeting component ticks at 1 s for the entire ±1 h
window around a meeting's start even while hidden — ~7200 wakeups + full
recompute per meeting, a battery/CPU cost on a kiosk with no user-visible
benefit. Design §4.2 scopes the 1 s countdown rate to "while a panel is visible."

The naive fix (gate 1 s on the host carrying `data-panel`) is wrong: a hidden
meeting must still fire its `takeover-request` near the boundary, and that request
is precisely what makes the shell overlay it visible — so coarse-ticking a hidden
meeting would delay its own takeover by up to 60 s. The correct fix computes the
exact next phase-boundary as the wakeup and uses the 1 s rate only for smooth
countdown when a panel is actually shown — a scheduling-model change, not a bucket
flip, so it warrants a design pass.

Code site: `surface/components/meeting/src/logic.rs` (`recompute`,
`next_tick_secs`), `TODO(meeting-tick-visibility)`.

---

## `test-cache-concurrent-report`

Two concurrent cache-enabled `xtask test` runs in one target dir share a single
JUnit report path (`<target>/nextest/<profile>/junit.xml`). Run B can overwrite
that file between run A's nextest write and run A's read, so A can parse B's
results and record them under A's environment key — a false pass record that
becomes a persistent false skip until the binary or env key next changes.

Local-only: CI runs cache-off (`BRENN_TEST_CACHE=0`) and serial
(`BRENN_CHECK_JOBS=1`), so neither concurrency nor the cache is in play there.
The design (§3.6) reasoned only about interleaved *cache* writes (safe via atomic
rename) and concluded "no locking needed"; it did not account for the shared JUnit
report as concurrent state. A robust fix is a concurrency-model decision — either
a run-level advisory lock around the run+record section (contradicting §3.6's "no
locking needed" framing) or a per-run report path (needs a nextest mechanism whose
availability must not be pre-judged per design-delta-1) — and so warrants a design
pass rather than a respond-mode patch.

Code site: `xtask/src/test_run.rs` (`run_cached`, JUnit read),
`TODO(test-cache-concurrent-report)`.

---

## `nextest-e2e-verification`

One item remains: a green cache-off CI run on a pushed branch (nextest active).
It requires an actual push, so it cannot run in this environment — and it
self-resolves on the first push to `main`, since CI runs automatically. When it
goes green, remove this entry and its code comment.

All local verification (filterset DSL, JUnit report shape, per-suite pass gate,
cache record + fast no-op, cold-vs-warm hash-cost timing, single-leaf-crate touch
selectivity, WASM-fixture invalidation, `BRENN_TEST_CACHE=0`, and the §4 flake
shakeout — three genuine full cache-bypassed runs, all green) is recorded in the
ADR implementation log:
`docs/adr/2026/07/11-make-check-speedup/implementation-log.md`.

Code site: `xtask/src/test_run.rs` (`build_filterset`), `TODO(nextest-e2e-verification)`.

---

## `wasm-dead-subscribe-acl-check`

A `[[wasm_consumer]]` with a non-empty `subscribe_acl` / `mqtt_subscribe_acl` /
`webhook_acl` whose matchers cover none of the consumer's static subscriptions boots
silently. For a WASM consumer those matchers are provably dead — no `WasmGrant` maps to
`DynamicSubscribe`, so nothing can ever exercise them (unlike the LLM side, where an ACL
without a static sub legitimately pre-authorizes future dynamic subs). Consider a boot
check (2g) rejecting ACL-without-covering-sub for WASM consumers. This diverges WASM from
the shared subscribe_acl convention (the same gap exists pre-existing for `subscribe_acl`
on `brenn:`), so it needs a design decision before landing.

Code site: `brenn/src/bootstrap/messaging.rs` in `resolve_wasm_consumers`, alongside
checks 2c–2f. `TODO(wasm-dead-subscribe-acl-check)`.

---

## `xtask-wasi-macro-cleanup`

The WASI-free gate is enforced in two places: the `wasm_component_rule` / `wasm_guest_component_rule`
Makefile macros (`Makefile:246-251`, `273-278`) and `xtask check-wit`. The macro-embedded grep is left
in place until `xtask check-wit` proves itself (belt-and-suspenders on a security-relevant gate).
Once `xtask check-wit` has run in CI for a while without issues, remove the grep from the Makefile
macros so artifact production is not self-gating and the gate lives only in xtask.

Code site: `Makefile:246-251` (WASI grep in `wasm_component_rule`), `Makefile:273-278` (WASI grep
in `wasm_guest_component_rule`). `TODO(xtask-wasi-macro-cleanup)`.

---

## `drop-counters-export`

`Messenger::drop_counters` (push-window overflow drops, metered/alarm noise
levels) is an in-memory map with no production reader — only tests query it.
The surface's loudness ladder does not discharge this: the kernel keeps its own
per-binding metered drop counters, and those are kernel-internal too (test- and
accessor-visible, exported nowhere). Both maps are unread; this entry covers
both.
An unread counter implies observability that doesn't exist. Blocked on
deciding what telemetry looks like for Brenn (small-deployment scale — the existing
surfaces are the db, brenn.log, and AlertDispatcher; there is no metrics
endpoint). Once counters are actually readable somewhere, also reconsider the
global `Silent` default for subscription `noise` — silent-by-default loss and
unread counters are a coupled pair; changing one without the other is
pointless.

UPDATE: Not blocked on telemtry: the best telemetry option is Brenn's bus itself,
with retained channels.
See docs/adr/2026/07/12-surface-ui-round2/retro-fixes.md for discussion.

Code site: `brenn-lib/src/messaging/mod.rs`
(`record_push_and_check_overflow`), `TODO(drop-counters-export)`.

---

## `wasm-provenance-chain`

**⛔ DECIDED — WON'T DO. DO NOT DELETE THIS REMINDER. DO NOT RESURFACE THIS. ⛔**

This has a resurrection history (triaged 2026-06-10, re-surfaced 2026-07-10) and now
exists specifically to stop the cycle. The decision is final: **the sender of a
republished message IS the component that published it, full stop.** A message's
trustworthiness derives from what the operator knows about that component and the wiring
the operator explicitly authored under the ACLs they granted — the same edge-based model
every existing enforcement gate already uses. There is no per-message origin marker and
there will not be one.

Per-message origin chains fail on their own terms: (a) provenance is **ill-defined** for
anything but a pure forwarder — a component reading N inputs (plus retained context, store
state, config) has no framework-answerable "origin" for its output, and pub/sub graphs are
cyclic, so chains would need loop-suppression/truncation (BGP-AS-path pathologies, per
message); (b) the chain is **unverifiable** — a component would self-annotate its own
provenance, which a buggy component omits and a hostile out-of-tree component falsifies, so
it can never be a security boundary; (c) it would **tax every component forever** via a
`MessageEnvelope`/WIT contract change to carry, at best, honest-component documentation.
The residual real risk (operator memory of transitive wiring decaying as configs grow) is
an inspection/tooling problem over static config the host fully knows, not per-message
envelope machinery; the `ports.publish` WIT doc-comment warning to operators already covers
the acute case and stays.

Do not add an origin-chain field. Do not add WIT/host plumbing for it. Do not delete this
entry — it exists to stop reviewers/burndowns from resurrecting the work.

(Original intent, for context only — NOT a call to action: messages emitted via the
`ports.publish` WIT import appear on the bus with `sender = "wasm:<slug>"` and
`envelope_type = brenn`; a webhook body forwarded through a component is indistinguishable
from a host-internal message by downstream subscribers. The rejected proposal was to add an
origin-chain field to `MessageEnvelope` plus host/WIT plumbing so forwarding components
annotate their origin chain. Code site: `brenn-wasm/wit/processor.wit`, `ports.publish` doc
comment — the operator warning there is kept.)

---

## `summary-real-decision` [blocked-on: wasm-frontend-port]

**NB: leave this as blocked / wont-do.** The "wasm-frontend-port" blocker is not a TODO but it is a real thing we may very well do in the near future. This TODO is probably not worth doing if we are going to do that, and instead because part of the requirements for that port. Consider doing this only if we definitively decide not to do the wasm frontend.

`emit_tool_summary` in `active_bridge.rs` always passes `Allow { updated_input: None }` to `format_summary`, so interactive tools (ProposeReconciliation, BatchReconcile) can't show accurate detail in their summary lines — they fall back to generic "approved" text. The real user decision needs to be threaded through so summaries can show e.g. "10 accepted, 2 rejected" or the selected proposal label.


## `todo-ui-refresh-on-state-change`

**⛔ NOT NOW — TOMBSTONE. DO NOT RESURFACE. DO NOT DELETE THIS REMINDER. ⛔**

Triaged and deferred. This is **a new feature, not a bug to fix**, and it is
likely to be obsoleted by the dynamic-UI / WASM frontend work that is (probably)
coming soon. Building the live-connection registry / `invalidate_todo_state`
plumbing now would be throwaway. Reviewers/burndowns keep rediscovering the
staleness and proposing to fix it — don't. Leave this entry as the marker that
the decision was "not now," and do not delete it.

(Original description, for context only — NOT a call to action: the todo UI is
refreshed via `send_todo_state` only after Brenn-originated mutations. LLM
graf-MCP mutations, git pulls, and `graf_reindex` all leave the UI stale until
reconnect. The clean shape would be a single `invalidate_todo_state(trigger)`
entry point fired from each source, but no live-connection registry exists to
reach the affected WS connections.)

## `task-death-supervision`

**⛔ DECIDED — WON'T DO. DO NOT DELETE THIS REMINDER. DO NOT RESURFACE THIS. ⛔**

Covers ALL process-lifetime background tasks with intentionally-dropped
`JoinHandle`s: `bus_gc_loop`, `spawn_deliver_after_task`, `spawn_deadline_task`,
`session_cleanup_loop`, `ingress_cleanup_loop` (all in `brenn/src/bootstrap/mod.rs`).

Reviewers and burndowns keep rediscovering that these tasks "die silently" on panic
and proposing a supervisory wrapper. They are wrong about "silently," and the
decision is final: **every panic is logged (structured `tracing::error!`,
`panic=true`, with location) AND fires a Critical phone alert via the global panic
hook (`brenn-lib/src/obs/panic_hook.rs`).** The residual gap — the process keeps
running with that one task dead until someone restarts it — is ACCEPTED. Alert +
manual restart is the intended and sufficient mitigation. We are NOT adding per-task
supervision, nor process-crash-on-task-death.

Do not add a supervisor. Do not file per-task variants of this. Do not delete this
entry — it exists to stop the cycle.

---

## `unenroll-live-session-teardown`

**⛔ DECIDED — WON'T DO. DO NOT DELETE THIS REMINDER. DO NOT RESURFACE THIS. ⛔**

This keeps getting re-discovered and re-proposed. The decision is made and final:
**unenroll is rare, and the CLI already prints a NOTE telling the admin to
restart the server if they want to cut off existing sessions. That is good
enough.** We are not building a live-session registry or session revocation for
this. Do not propose one. The `brenn-cli device unenroll` output at
`brenn-cli/src/main.rs:211-215` is the intended and sufficient mitigation.

This entry is kept ONLY to stop reviewers/burndowns from resurrecting the work.
If you are reading this thinking "but there's a teardown gap" — yes, we know,
it's documented and accepted. Leave it alone. Do not delete this entry.

(Original gap description, for context only — NOT a call to action: (1) already-open
WS sessions keep dispatching until server restart; (2) `resolve_or_create_device`
mints a new device row for the same authenticated user post-unenroll while the
login session is still valid. Code sites: `brenn/src/routes/ws/dispatch.rs:17-33`,
`brenn-lib/src/auth/device.rs::unenroll_device` and `resolve_or_create_device`.)




---

## `processor-typed-gaps`

The surface's resume layer classifies why replay could not cover a requested
resume point — epoch change, hole past the retained ring, resume beyond the
retained window — and hands the reason to the page
(`SubscribeResult.gap`, consumed in
`surface/client/src/core/mod.rs::on_subscribe_result`). The backend's
`processor.wit` world has no equivalent: a wasmtime-hosted component cannot
tell "I resumed cleanly" from "the bus lost my place", so it cannot decide
whether its own derived state is trustworthy after a restart.

Backend adoption is an **external ABI change** and therefore rides the next
`processor.wit` world bump rather than being bolted on: the sync follow-on
already bumps the world additively (new world carrying the sync `call`
export), and typed resume-layer gap signalling joins that same bump. Doing it
sooner means either breaking the frozen external ABI or minting a second
world for one field.

Done when the bumped `processor.wit` world carries the resume-layer gap
reason, the guest SDK surfaces it, and the wasmtime host populates it from the
resume path.

---

## `processor-transplant-browser-engine`

The surface-half transplant parity test
(`frontend/src/processor-transplant.test.ts`) exercises the real
jco-transpiled artifact, but under node's WebAssembly engine rather than a
real browser engine. The harness resolves artifacts by filesystem path: it
dynamic-imports a `file://` URL and reads the core wasm bytes with
`readFileSync`. The wasm-bindgen headless-browser runner has no filesystem, so
the test cannot move there as written.

The residual uncovered case is narrow and specific: **the transpiled guest
running under a browser engine specifically.** The guest-on-transpiled-hosting
behavior itself is already covered by this test, and the kernel's side of the
activation contract is covered by `surface/kernel/src/logic.rs` core tests and
the loader cases in `frontend/src/surface.test.ts`. Nothing here is unverified;
what is missing is only the browser-engine execution environment.

Done when `surface/dist` is served in the browser test fixture and those two
filesystem calls (the `file://` dynamic import and the `readFileSync` of core
wasm) are swapped for `fetch`.

Code site (`TODO(processor-transplant-browser-engine)`):
`frontend/src/processor-transplant.test.ts`, the header note on how artifacts
are resolved.

---

## `local-replay-on-register`

Registering an activation instance does not, on its own, mint an activation, and
`reconcile_registered` never seeds pending from a `local:` ring. So an instance
that registers after the last publish on a depth-1 `local:` plane sees the
retained value only when the *next* message arrives — on a control plane whose
whole point is that no next message is coming, that is no handoff. The
consolidation design wants the late-attaching-chrome handoff to be gap-free on
attach (consolidation design §5.3 "last-value replay", §6.1 "replays the current
state on attach — no gap at the handoff").

Unobservable in-tree today: zero `local:` bindings exist in any config, and
chrome — the handoff's only consumer — is Phase 3. Adding an activation cause on
registration is a design decision (design §4.6 names the activation causes and
this is not among them), so Phase 3 is where this must be answered.

Done when registration of an instance with retained `local:` context either mints
a first activation replaying that context, or the design explicitly rules it out.

Code sites (`TODO(local-replay-on-register)`):
`surface/client/src/core/tests/local.rs` (the two late-registrant tests).

---

## `automation-croner-dst-verify`

The DST-spike behavior of the croner schedule evaluation is asserted by reasoning,
not by verification against croner's actual handling of the spring-forward gap and
the fall-back repeat. Done when the DST spike tests run against a pinned croner
version and the observed behavior is recorded.

Code site (`TODO(automation-croner-dst-verify)`): `brenn-lib/src/automation/job.rs`.

---

## `automation-fires-cleanup`

Automation fire rows are pruned by a simple age sweep. If fire volume ever makes
the sweep expensive, a more sophisticated prune (retention by job, per-N batching)
is the follow-up. Not urgent: current volume is trivial.

Code sites (`TODO(automation-fires-cleanup)`):
`brenn-lib/src/automation/db.rs` (the prune statement),
`brenn-lib/src/automation/fire.rs` (the sweep loop).

---

## `automation-fire-semantics-tests`

Some fire-semantics cases (overlap suppression, catch-up-after-downtime edges)
are covered by reasoning in comments rather than tests. Done when those cases have
direct tests.

Code site (`TODO(automation-fire-semantics-tests)`): `brenn-lib/src/automation/fire.rs`.

---

## `event-cleanup-undelivered`

Events enqueued to a conversation that is later abandoned are never delivered and
never cleaned up; the rows accumulate. Done when abandoned-conversation cleanup
also retires their undelivered events.

Code site (`TODO(event-cleanup-undelivered)`): `brenn-lib/src/conversation/mod.rs`.

---

## `export-usage-broken-mount-test`

The export-usage tool's broken-mount failure path has no test — exercising it needs
a mount that fails on write, which the current fixtures cannot produce. Done when
the harness can inject a failing mount.

Code site (`TODO(export-usage-broken-mount-test)`):
`brenn-server/src/active_bridge/brenn_tools/export_usage.rs`.

---

## `mqtt-dynamic-subscribe-acl`

A documented pre-Phase-1 hole in dynamic MQTT subscribe ACL coverage, retained as a
regression marker on the test that pins the current (closed) behavior. Done when the
marker's premise is re-verified and it can be deleted.

Code site (`TODO(mqtt-dynamic-subscribe-acl)`): `brenn-server/src/mqtt_subscribe.rs`.

---

## `quota-statement-vs-commit`

The WASM store's quota gate meters at statement time, not commit time, so a
transaction can exceed the cap between the two. The empirical gate test measures
the real divergence. Done when the measurement settles whether commit-time metering
is required or the statement-time gate is provably sufficient.

Code site (`TODO(quota-statement-vs-commit)`): `brenn-wasm/src/store.rs`.

---

## `replay-generic-bounded-scan`

`replay-generic` scans unbounded where the design calls for a bounded range scan.
Correct but not bounded; done when the scan takes the designed bound.

Code site (`TODO(replay-generic-bounded-scan)`):
`brenn-wasm/components/replay-generic/src/lib.rs`.

---

## `unify-gc`

The bus GC loop is spawned separately from the other cleanup loops; unifying them
under one sweep scheduler was deferred. Cosmetic/structural, not a defect.

Code site (`TODO(unify-gc)`): `brenn-server/src/bootstrap/mod.rs`.

---

## `wasm-messenger-test-helper`

`mk_entry` is inline-constructed in four test sites; it wants one shared helper.
Test hygiene only.

Code site (`TODO(wasm-messenger-test-helper)`):
`brenn-server/src/active_bridge/bridge_io.rs`.

---

## `scrub-tree-auto-gate`

Wire the `scrub-tree` release-gate sweep into an automated check so the
green-tree invariant (and the stale-exclude panic that is meant to force
cleanup after the GitHub migration) fires on its own instead of only when
someone remembers to run `make scrub-tree`. Blocked on a decision: CI runs
`make check-ci` without installing `brenn-scrub`, so wiring it into
check-common/check-ci either needs the binary installed in CI or a hermetic
`cargo run -p scrub` invocation (which changes the design's deliberate
"verify the installed binary" semantics).

Code site (`TODO(scrub-tree-auto-gate)`): `Makefile` (`scrub-tree` target).

---

## `fleet-sha-pin-actions`

The public CI workflow pins marketplace actions by mutable tag
(`actions/checkout@v7`, `Swatinem/rust-cache@v2`, `actions/setup-node@v4`,
`actions/cache@v4`). Tags can move, so this is looser than the sha256-pinned
gitleaks download in the scrub job. Converge to commit-SHA pins as a fleet-wide
change (pfin/graf carry the same slug); brenn's action set is wider than the
sibling check jobs, so this is not byte-identical with them.

Code site (`TODO(fleet-sha-pin-actions)`): `.github/workflows/ci.yml`
(marketplace-action `uses:` lines in the `check` and `scrub` jobs).

---

## `ci-wasm-tool-pins-drift`

The `WIT_BINDGEN_VERSION` (`0.58.0`) and `WASM_TOOLS_VERSION` (`1.249.0`)
literals in the public CI workflow duplicate the same versions embedded in the
Makefile's wit-bindgen-cli and wasm-tools preflight messages, with no
derivation linking them (unlike `WASM_BINDGEN_VERSION`, which the workflow
extracts from `Cargo.toml` so it cannot drift). Those two Makefile preflights
are presence-only (`command -v`), not version asserts, so a missed sync surfaces
as a confusing generated-bindings diff far from the cause instead of a version
error. Done when the pins live in one authoritative place — e.g. promoted to
Makefile variables referenced by version-asserting preflights and extracted into
the workflow the same way `WASM_BINDGEN_VERSION` is — so a bump is a single edit.
Deferred here: this TODO is scoped to the CI workflow only; aligning the fleet
(pfin/graf carry the same shape) wants an owner decision.

Code site (`TODO(ci-wasm-tool-pins-drift)`): `.github/workflows/ci.yml`
(the `WIT_BINDGEN_VERSION` / `WASM_TOOLS_VERSION` env vars in the `check` job).
