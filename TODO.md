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

Code site (`TODO(scrub-template-drift-cache-skip)`): scrub/tests/rules.rs,
`repo_template_matches_the_tracked_public_config` (the guard the cache skip
weakens). The fix lands in xtask/src/test_run.rs, `collect_env_inputs`.


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
`surface/schema/src/lib.rs`, the `CONTROL_PLANE_VERSION` const.

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
   `ChannelScheme`; remove `IngressEvent` and the ingress decode/render
   (`[Event]` card) paths, `insert_ingress_message*`, the `ingress_*`
   columns/queries in `brenn-lib/src/messaging/db/ingress.rs`, and with them the
   `messaging_pending_pushes` table itself — these rows are all it still carries,
   and `dispatch_row` plus the dispatcher's ingress scan die with them.

Code sites (`TODO(ingress-retirement)`):
`brenn-lib/src/messaging/db/envelope_column.rs` (`EnvelopeTypeColumn::Ingress`),
`brenn-lib/src/repo_sync_cursor.rs` (the two `insert_ingress_message_raw`
writers), `brenn-lib/src/messaging/publish/mod.rs` (`insert_ingress_message`
writer), `brenn-lib/src/messaging/ingress.rs` (`Event`).

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

The table must also cover **cursor-incarnation replay**, not just a caller
retrying: a request is retained for its channel's window, and an executor
position that is created fresh — first boot, or a config remove-then-re-add
cycle that deletes and re-mints the row — re-executes whatever the window still
holds. Every async tool registrable today declares `Idempotency::Natural`, so
the replay is harmless now; a `RequiresKey` tool is only safe once dedupe keys
survive across executor incarnations. `call_id` already rides every request and
is the correlation a duplicate result carries.

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

`Messenger::metered_drops` (the drops the noise ladder counted at metered/alarm
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
(`enact_overflow_noise`), `TODO(drop-counters-export)`.

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

---

## `dispatcher-completion-kick`

When a dispatcher scan skips a subscriber whose key is already `in_flight`, the
rows it skipped are not re-scanned when that in-flight pass completes: the
supervisor's normal-completion arm removes the key from `in_flight` but never
calls `dispatch_kick()`. Skipped rows therefore wait up to `POLL_INTERVAL`
(60 s) for the next periodic scan. Done when completion re-scans — one
`dispatch_kick()` in the completion arm, or a "another scan wanted" flag the
supervisor honors.

Code site (`TODO(dispatcher-completion-kick)`):
`brenn-lib/src/messaging/dispatcher.rs` (the supervisor task's normal-completion
arm).

---

## `intercept-noop-shape`

`is_noop_tool_response` expects `{"content":[{"type":"text","text":"__NOOP__"}]}`
but the live PostToolUse `tool_response` from a `BrennSend` does not match, so
every send logs "PostToolUse tool_response was not the expected `__NOOP__`"
while the response really is the noop. Pure log noise, on the hottest path there
is. Done when the check accepts the shape `noop_mcp.py` actually produces (and
still rejects a genuinely different response).

Code site (`TODO(intercept-noop-shape)`):
`brenn-server/src/intercept_helpers.rs` (`warn_if_unexpected_tool_response`).

---

## `substrate-deferred-view-count-shortcut`

The WASM drain builds a deferred-window view per bound output port every
activation. For a durable (`brenn:`) port that is a `DbStore::deferred_for_sender`
SQL read under the global db mutex, paid even by the common case of a component
that never parks a message. Short-circuit the empty case cheaply. The naive fix
(a per-`DbStore` count) does not fit — `DbStore` is a throwaway handle minted per
call in `Messenger::store_for` — so the count must live on the `Messenger`/registry,
be seeded at boot, and be kept accurate across every park/cancel/release/quota
site, including the durable park via `insert_pushes` that does not route through
`DbStore::park`. A stale count is a correctness bug (a missed deferred view means a
component cannot see or cancel its own parked message), so this needs a design
decision about cache ownership and mutation routing. Done when the drain loop skips
the query for a port with zero deferred messages.

A per-`DbStore` count is now structurally possible — `Messenger::store_for` caches
one `DbStore` per channel rather than minting a throwaway handle per call — but the
routing problem above is unchanged: the count must still be seeded at boot and kept
accurate across every park/cancel/release/quota site, including the durable park via
`insert_pushes` that does not route through `DbStore::park`.

Code site (`TODO(substrate-deferred-view-count-shortcut)`):
`brenn-server/src/wasm_dispatch/mod.rs` (the `for out in &cfg.outputs`
deferred-view loop in `drain_step`).

---

## `deferred-flush-drop-signal`

A `publish-deferred` refused at the channel's deferred cap (the channel's
`retain_depth`) during a WASM activation flush is dropped with a host `warn!` and
a counter increment, and nothing tells the component. A dropped schedule is a
timer that never fires — the component believes it parked a wake and simply never
runs again. That is a silent wrong outcome in the exact idiom the io_port block
exists to make safe, and the guest's only recourse today is to poll its own
per-output `deferred-window` and infer the absence.

The flush has no error channel back to the guest, so fixing it is a WIT/substrate
change (a per-port error report, or making the deferred publish a call whose
refusal the guest can observe), not a patch at the drop site. Done when an
over-cap deferred publish reaches the component that issued it.

Both park arms of the flush refuse this way — the durable one against the row
count, the non-durable one at the ring's cap — so the fix is one report path, not
two.

Code site (`TODO(deferred-flush-drop-signal)`):
`brenn-lib/src/messaging/publish/mod.rs` (the refusal-reporting loop at the end of
`publish_from_wasm`, which both park arms feed).

---

## `ring-deferred-recall`

The LLM recall tools — `BrennMessageCancel`, `BrennMessageEdit`,
`BrennPendingList` — reach durable parked messages only. They name a message by
bare uuid, and `Messenger::cancel` / `edit` / `list_pending` look that uuid up in
`messaging_messages`, so a deferred publish to a non-durable channel (accepted on
every scheme, and it returns a message id to its publisher) answers
`UnknownMessage` and is absent from the pending list. That is a
publisher-visible difference by channel class, not a consequence of where the
bytes live: the class-uniform substrate already exists —
`RetentionStore::cancel_deferred` / `edit_deferred` / `deferred_for_sender` serve
the WASM ports' `defer-cancel` / `defer-edit` on *both* stores, and the ring's
parked set is uuid-addressable with sender-scoped cancel and replace.

It needs its own small API design, which is why it is not just a patch: the tools
carry no channel argument, and the only global uuid index is the durable table, so
unification means either a channel parameter on the tools or messenger-side
resolution across every ring's deferred set, plus outcome mapping (`NotDeferred`
vs `UnknownMessage` vs `AlreadyDelivered`) and ring-side coverage of the full
`EditFields` (urgency, `delivery_deadline`, `reply_to` — the last crossing the
uuid-vs-address representation split the two stores keep). The scope is disclosed
in the three tool descriptions meanwhile.

Done when the LLM recall tools reach ring-parked messages, or the durable-only
scope is ratified in `docs/message-bus.md`.

Code site (`TODO(ring-deferred-recall)`): `brenn-lib/src/messaging/edit.rs`
(`Messenger::cancel`).

---

## `surface-op-send-budget`

A surface `PublishBatch` draws its send-budget tokens per *publish*; the control
ops it carries are free. So an ops-only flush draws one token however many ops it
carries, up to `MAX_PUBLISHES_PER_ACTIVATION`, while a publish-only flush of the
same width draws the full 256. Each applied durable op is its own DB write and
each touched channel costs a view recompute plus a slug-wide fan-out, so a
conforming-shaped client can buy roughly two orders of magnitude more backend
write work per token through ops than through publishes. Bounded (op cap,
frame-size cap, authenticated principal, session count), so this is a calibration
gap rather than a hole.

Not a local patch, which is why it is ledgered: `SURFACE_SEND_BURST` is
deliberately equal to `brenn_budget::MAX_PUBLISHES_PER_ACTIVATION` so that a full
bucket admits exactly one maximal conforming flush, and boot asserts it. Pricing
ops at one token each makes the maximal conforming flush 512 units — 256
publishes plus 256 ops, two separate kernel-side ceilings — so the bucket would
refuse truthful traffic and the kernel would re-park and retry it to the outbox
cap. Fixing it therefore means deciding what the budget meters (publishes, or
units of flush work), re-sizing the burst against that answer, and restating the
operator-facing `burst` config whose documented unit is publishes.

What the amplification also buys, beyond the write work: every view restatement
runs under the process-wide `deferred_view_gate`
(`brenn-lib/src/messaging/mod.rs`), held across the deferred-set read, and the
release sweep takes that same gate while running on the single dispatcher loop
that also wakes ordinary subscribers (`push_released_surface_views`;
`brenn-lib/src/messaging/dispatcher.rs`). Op-driven recomputes therefore queue
ahead of sweeps and subscriber wakes — bus-wide dispatch delay, on the order of
fractions of a second to seconds of aggregate delay under a deliberate burst, not
merely delay for the surface that caused it. A raced (`NotDeferred`) op restates
the view by design — the restatement heals a mirror whose earlier emission was
dropped on a full push queue — so the composition needs nothing actually parked.
Removing that restatement is not the fix.

Done when ops are priced and the burst is sized so one maximal conforming flush
still fits, **and** it has been re-assessed whether pricing alone re-bounds the
gate/dispatcher composition or whether the sweep's gated emission must
additionally move off the dispatcher loop.

Code sites (`TODO(surface-op-send-budget)`):
`brenn-server/src/routes/attach/publish.rs` and
`brenn-server/src/routes/surface/session.rs` (the `draw` in
`handle_publish_batch`); `brenn-lib/src/messaging/mod.rs`
(`push_released_surface_views`, the sweep-side gate take on the dispatcher loop).

---

## `chat-bus-attachments`

A `send` command arriving on a conversation's chat command channel may name
attachments in its schema, but the server rejects any such command whole. Upload
ids resolve through a per-user pending-upload registry, and a bus command's
sender is a `ParticipantId` with no user mapping — there is no correct user to
resolve against, and a partial send (text without the files) would silently
misrepresent what the peer asked for. Lifting the restriction needs a decision
about how a bus peer acquires and proves ownership of an upload; the schema
field is already in place so the change is purely additive when that decision
lands. Done when a bus `send` with attachments resolves them and reaches the
harness with the files attached.

Code sites (`TODO(chat-bus-attachments)`): `brenn-envelope/src/chat.rs`, the
`attachments` field on `ChatCommand::Send`; the rejection itself in
`brenn-server/src/active_bridge/bus_chat.rs`.


## `chat-surface-mints-impetus`

A message may carry *impetus* — publish-time-checked evidence that live user
interaction produced it — and a conversation redeems it by resetting its impetus
pool, the stock every unattended turn-provoking bus injection draws from. Setting
the field requires the `mint_impetus` capability, and **nothing in production
holds or sets it**: the capability is not authorable from TOML, no surface or
WASM publish path carries the field, and every internal wrapper passes `None`.
Only the legacy websocket door refills a pool today.

Consequence: a conversation driven purely over the bus — or an observer
conversation fed by ambience — has a bounded runway (the pool ceiling's worth of
unattended turns) per attended legacy-door touch, then stalls: sends are refused
with a correlated `error`, ambience is held unadvanced. Someone whose only door
to Brenn is a bus surface has no way to restart it. That is transitional, not the
intended end state.

The chat-surface project (voice gateway behind it) is the first minter: author
the `SurfaceGrant` → `MintImpetus` mapping, carry the field on the surface
publish frames, and derive `Impetus::Replenish` from a genuine user gesture —
never from component say-so alone. Done when an attended bus send refills the
pool it draws from.

Code site (`TODO(chat-surface-mints-impetus)`): `brenn-lib/src/access/mod.rs`,
the `AppCapability::MintImpetus` variant.


## `chat-history-on-demand`

A conversation's record channel retains `[llm_chat].retained_window` messages,
and that window is the whole history story for a bus peer today: a subscriber
reads back as far as its own retain depth and no further. There is no way to ask
for older messages, so a peer that wants the start of a long conversation cannot
get it. Wants a request/response on the chat tree (or the surface websocket's
existing paging shape) that serves a deeper slice on demand rather than forcing
the retained window to be sized for the worst case. Done when a peer can read
past its retain depth without the channel retaining more.

Code site (`TODO(chat-history-on-demand)`):
`brenn-lib/src/messaging/chat_provision.rs`, where the record channel's retained
window is fixed.


## `chat-deletion-teardown`

`Messenger::deprovision_conversation_chat_channels` removes a conversation's chat
channels, their retained record, and every cursor on them — and nothing calls it,
because no conversation-deletion path exists in the tree yet. Whoever builds
conversation deletion has to call it in the deletion's own transaction; a delete
that does not will leak a directory entry, two channel rows, the whole retained
record, and a cursor row per deleted conversation, with nothing failing to say
so. Done when conversation deletion tears the chat family down with the
conversation.

Code site (`TODO(chat-deletion-teardown)`):
`brenn-lib/src/messaging/chat_provision.rs`, on
`deprovision_conversation_chat_channels`.


## `dormant-missing-app-cursor`

A durable dynamic subscription goes dormant — kept in its table, kept out of the
directory — whenever config stops standing behind it: a revoked ACL, a standing
depth tightened below the granted one, an undeclared channel. The boot cursor
reconcile counts those registrations so the position survives to the boot that
restores the config. One class cannot: when the missing config is the app's own
`[[app]]` block, the merge classifies its rows dormant (a missing policy fails
closed) but the reconcile resolves each dormant row's conversation through the
same apps map, so it resolves nothing, the position is unjustified, and the
cursor row is deleted as an orphan — under a warn that calls a revertible
operator edit a host wiring bug. Restoring the block then re-primes at the
retained tail: duplicate delivery of what the app already saw, and silent
uncounted loss of whatever the channel evicted meanwhile.

Needs a decision before code. Either the dormant row's conversation resolves
without the apps map — the conversations table keys `(user_id, app_slug)` and the
singleton invariant makes a slug-only lookup unique, but "which user owns this
conversation when the app config is gone" is a new answer, not a refactor — or
the loss is ratified for this class, in which case the reconcile says so and the
`app_owner` warn stops misdiagnosing it. Done when a dormant row for an app
absent from the apps map either keeps its position or documentedly loses it, with
a test either way.

Code site (`TODO(dormant-missing-app-cursor)`):
`brenn-lib/src/messaging/reconcile.rs`, the dormant justification loop in
`Messenger::reconcile_subscriber_cursors`.


## `attach-cutover`

`brenn-attach-client` holds generalized copies of machinery that is still live in
`brenn-surface-kernel`: the backoff PRNG and the frame/duration helpers
(`core/util.rs`), the per-channel ring store (`core/store.rs`), and the
connection/backoff lifecycle, the wire-subscription refcounts, and the
outbox/retry plane (`core/mod.rs`). Both copies compile, and nothing links them —
the compiler cannot tell you when one is fixed and the other is not, so every bug
found in one has to be found twice until the kernel embeds the crate.

The two have already diverged deliberately: the crate's retry timer arms only
when a tick could actually send something, where the kernel's arms whenever any
outbox has a queued flush. The crate is the surviving copy, so the cutover
deletes the kernel's rather than reconciling them.

Done when the surface kernel embeds `brenn-attach-client`'s types and every
listed kernel copy is deleted — an explicit inventory, not a best-effort sweep,
because a piece missed here becomes a permanent silent fork.

Code sites (`TODO(attach-cutover)`): `surface/kernel/src/core/util.rs`
(`SplitMix64`, `frame_type_name`, `duration_ms`),
`surface/kernel/src/core/store.rs` (`SurfaceChannelStore`),
`surface/kernel/src/core/mod.rs` (`RETRY_INTERVAL_MS` and the outbox/retry plane,
`enter_backoff` and the connection lifecycle around it, `acquire_channel_ref` and
the wire-subscription plane).

