# TODOs

## `test-task-panic-visibility`

A panic on a connection task spawned by `spawn_test_server`
(brenn-server/src/test_support/http.rs) is absorbed by tokio and asserted
against by nothing, so any regression that panics server-side after the last
frame a test reads — the detach path, the unregistration, a telemetry publish —
passes green across the whole brenn-server route suite. Found when four surface
suites were panicking the server on teardown and still reporting `ok`; those
rigs are fixed, the blindness is not.

Needs a design call before it can be built: a global panic hook is process-wide
and would have to distinguish a deliberate `#[should_panic]` from a swallowed
task panic (~2500 tests, some multi-thread, run concurrently in one process),
and a drop-time assertion aborts on double panic. Done when a connection task
that panics fails the test that provoked it, without breaking `#[should_panic]`.

Code site (`TODO(test-task-panic-visibility)`):
brenn-server/src/test_support/http.rs, `spawn_test_server`.


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
`surface/kernel/src/planes.rs`, `inject_takeover_instance` (the router's
parse-failure passthrough, called by `SurfacePlanes::guard`).

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
and the kernel never calls `SurfaceHandle::deregister_activation`. Correct today
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

The sync seam is a second consumer of the gate: a sync request is refused until
the requesting instance's entry is registered, so a remount the gate wrongly
rejects loses its gestures as well as its deliveries.

Code site (`TODO(kernel-registration-gate-lifecycle)`):
`surface/kernel/src/logic.rs`, the `KernelCore.registered` field.

---

## `surface-single-publish-tightening`

A surface attachment still sends single `ClientFrame::Publish` frames under a
**component-instance attribution**: the kernel's error-report path attributes a
report to the component it is about (`SurfaceOutbound::report`,
`surface/kernel/src/outbound.rs`), so the sender sub-identity on the frame is a
component id even though the sender is the kernel. That is legitimate and
deliberate — the report draws down that component's budget, not its neighbours'
— so the server cannot simply reject a surface session's single `Publish` that
claims a component attribution, which is the tightening the gesture-activation
work went looking for once component-origin publishes all became batches.

What is true after that work: every *component-origin* publish leaves a surface
as a `PublishBatch`. Nothing on the server asserts it, so a future
component-origin single `Publish` would be admitted silently.

Done when the server can tell the kernel's own attributed traffic from a
component's — a distinguishing mark on the frame, or a per-attribution posture
in the attach profile — and rejects (+ logs, fail2ban posture) a surface
session's single `Publish` that is neither.

Code site (`TODO(surface-single-publish-tightening)`):
`brenn-server/src/routes/attach/publish.rs`, `handle_publish`.

---

## `surface-wasm-test-in-ci`

`make check` now *type-checks* the browser-side wasm test suites
(`surface-wasm-check`'s second, scoped `--all-targets` invocation), so they can
no longer rot silently. They are still never **run** by any gate: `make
surface-wasm-test` needs a WebDriver browser driver and is in neither
`CARGO_CHECK_STEPS` nor `check-ci`. A type-checked suite that never runs still
answers no behavioral question — and these are the XSS-adjacent
text-not-markup pins, the DOM seam, mount/unmount, port dispatch, and the whole
sync-call seam (the `brenn-activation-sync` listener, the door's answer
vocabulary, the publish route's buffered/refused split), which exists only in
the browser and is pinned only here.

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

## `e2e-in-ci`

`make e2e` — the Playwright browser suite in `e2e/tests/` — is run by no gate:
it is in neither `CARGO_CHECK_STEPS` nor `NONCARGO_CHECK_STEPS`, not in
`check-ci`, and not in `.github/workflows/ci.yml`. Only an operator running it
by hand answers anything, and four of its six specs sat red — a stale
component-element selector — for an unbounded span before anyone noticed. The
suite covers layout switching, malformed-layout last-good retention,
per-instance content isolation, and reload/durable-snapshot restore; acceptance
decisions have already leaned on one of those specs while it was dead, which is
the specific harm an ungated suite does.

Done when `check-ci` runs `make e2e`. Blocked on two provisioning facts, and
the ordering is load-bearing the same way `surface-wasm-test-in-ci`'s is: CI is
a persistent `runs-on: shell` host whose build tools are installed by workflow
steps, and Playwright's chromium is not one of them (`npx playwright install
chromium` plus its system libraries); and the target boots the built binary as
a real server on port 3100, so the runner needs that port free and must
tolerate a backgrounded server on a capacity-1 runner shared with every
project's deploys. Landing the `check-ci` step before both hold turns CI red on
every push to main, which is also the auto-deploy-to-staging path.

Until then the operator-side trigger stands in for the gate: run `make e2e`
before tagging a release, and after any change under `surface/`.

Code site (`TODO(e2e-in-ci)`): `Makefile`, the `e2e` target.

---

## `e2e-tag-scheme-tie`

`e2e/tests/bar.spec.ts` (`publishVia`) locates a mounted component by its
custom-element tag, `` `brenn-echo-stub--${instance}` ``, re-encoding in
TypeScript a scheme whose only home is `element_name_for_instance`
(`surface/contract/src/lib.rs`). Nothing mechanical ties the two — a comment is
the whole link, and a comment does not break a build. That drift already
happened once and cost four specs a 20-second `toBeAttached` timeout each, with
no diagnosis beyond "it times out".

Selecting on something other than the tag does not fix it. Three elements carry
`data-instance="<instance>"` for a placed instance — chrome's layout `section`,
the kernel's wrapper `div`, and the component element itself — and only the
last routes, because `instance_for_target` (`surface/kernel/src/dom.rs`)
resolves by node identity over the mounted-element registry, so a publish event
dispatched from the wrapper reaches nothing. Every selector that picks the right
one of the three re-encodes some Rust-side literal (the tag, or `wrapper_id`'s
`brenn-surface-wrapper-` prefix): one unlinked literal traded for another.

Done when a Rust-side change to the tag scheme fails the TypeScript build
rather than a Playwright wait — the scheme emitted into a generated constant the
spec imports. Needs a design call first: the e2e/TS side deliberately has no
bundler and no `ts-rs` bridge, so who emits the constant, when it runs, and
which gate proves it current is a new seam rather than a local edit.

Code site (`TODO(e2e-tag-scheme-tie)`): `e2e/tests/bar.spec.ts`, `publishVia`.

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
`surface/kernel/src/inbound.rs::on_subscribe_result`). The backend's
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


## `runner-drain-host-departure`

`SurfaceRunner::run_terminal_drain` (`surface/kernel/src/runner.rs`) awaits the
front door's four channels and nothing else. Going terminal disarms every
deadline and drops the transport, so after it those channels are the drain's only
wake sources — and `host_gone()` (the event sink's `is_closed`, which the run's
documented lifeline is) is re-read only when a command arrives or a channel
closes. A platform half that drops its event receiver while holding idle senders
parks the drain forever, leaking the task and the whole page with it. Everywhere
before terminal the loop re-reads `host_gone` on a bounded cadence, because the
driver always has an armed deadline or a socket to wake on.

Blocked on the seam the cutover round defines: the fix is either a liveness
signal the platform half holds (a new parameter on `SurfaceRunner::new`, which
constrains a platform half that is not written yet) or a stated ordering contract
on it (drop the control senders before the event receiver). `futures` mpsc offers
no closed-notification a select arm could take, so there is no third option that
is purely local to the runner.

Done when the drain's wait resolves on the platform half's departure however it
happens, and a test pins it: drop the event receiver mid-drain with a control
sender still alive, and the run terminates.

Code site (`TODO(runner-drain-host-departure)`): `surface/kernel/src/runner.rs`,
at `run_terminal_drain`.


## `terminal-drain-release-deadline`

A terminal attachment freezes every confined deferred schedule on the page.
Reaching terminal disarms all three of the driver's deadlines
(`attach/client/src/driver.rs`, the terminal arm) and
`SurfaceRunner::run_terminal_drain` (`surface/kernel/src/runner.rs`) selects no io
arm at all, so `Input::ReleaseDue` never fires again. A tick parked before or
during the terminal transition never releases: mode-clock's theme stops tracking
the schedule and protobar's expired slots linger, on a page chrome is drawing a
death banner over.

Detached is not affected and is what the cycle's design names — the main loop
keeps serving `ReleaseDue` for the whole reconnect, so the offline ticker keeps
ticking. Terminal is the state nobody ruled on. It is a behavioural change from
the `PersistentTimer` shape these components migrated off, where a raw
`setTimeout` kept firing regardless of the attachment.

The decision comes first: a terminal page is dead and says so, and whether its
components should keep re-rendering behind that banner is a product call, not a
patch. Implementing "keep ticking" then means keeping the release deadline armed
through terminal, which contradicts the driver's stated shape (`AttachDriver::wait`
documents pending-forever as terminal's shape by construction, so an embedder
winding down is never handed timer events) and adds an io arm to a drain that
currently touches no driver — the same wait `runner-drain-host-departure` is
already blocked on redesigning.

Done when either the drain releases parked confined messages and a test pins a
tick firing after terminal, or the contract states that a terminal page's
schedules freeze and the components' docs say so.

Code site (`TODO(terminal-drain-release-deadline)`): `surface/kernel/src/runner.rs`,
at `run_terminal_drain`.


## `batch-frame-cap`

The websocket read cap and the per-activation publish caps contradict each other
for batches. `max_client_frame_bytes` (`attach/proto/src/lib.rs`, and the same
derivation in `brenn-server/src/routes/surface/mod.rs`) sizes the cap at
`6 × max_body_bytes + 8 KiB` — one worst-case-escaped body plus slack — while an
activation may buffer `MAX_PUBLISHES_PER_ACTIVATION` = 256 publishes totalling
`MAX_PUBLISH_BYTES_PER_ACTIVATION` = 4 MiB (`brenn-budget`), and a flush travels
as one `PublishBatch` frame with no size-based split. At the default 64 KiB body
cap the frame cap is ~392 KiB, so a component that legally buffers roughly seven
near-max publishes in one activation composes a frame the server's read cap
refuses — and an oversized read is a protocol violation with a fail2ban signal,
against the operator's own browser.

Pre-existing: the live surface route already derives the cap this way. The
decision is a real one, which is why it is not a patch: sizing the cap for the
worst config-legal batch raises the bytes the server will read off an
unauthenticated-until-`Welcome` socket, while splitting or capping flush
composition breaks the property that one activation's flush is one atomic batch
judged against a single server clock read.

Done when the two contracts agree — the cap covers every config-legal frame, or
composition cannot produce one over it — and the comments at both code sites
state the contract that was chosen rather than the mismatch.

Code sites (`TODO(batch-frame-cap)`): `attach/proto/src/lib.rs`
(`max_client_frame_bytes`) and `brenn-server/src/routes/attach/socket.rs`
(`InboundError::Oversized`, which is where the violation is raised).

## `chat-conversation-provision-chokepoint`

A conversation row and its chat channel family have to appear together, and the
bus has to be told: every creation site owes `provision_conversation_chat_channels`
under the database lock and `republish_chat_roster` outside it. Nothing enforces
that — it is a two-call convention spelled out in a doc comment on
`create_conversation` — and the tree has already missed it twice (the send-message
create path and the singleton first-attach path, both fixed by hand). The lazy
mint inside delivery was the third: `MessageTargets::ensure_app_conversation`
(`brenn-lib/src/messaging/store/targets.rs`) is a synchronous method holding the
caller's `&Connection` and can discharge neither obligation itself, so its one
caller, `Messenger::attach_conversation`, now provisions in the mint's lock
scope and republishes the roster after the guard drops. Every known creation
site therefore discharges the convention today; what is missing is the structure
that makes a future one unable to skip it.

Needs a design call before code: the choke point is a `Messenger` method that
creates-or-adopts, provisions and announces, which means reshaping the creation
APIs the send path and the automation creators call, and deciding what a
synchronous creator like the delivery-path one does instead — take the messenger
and a deferred announce queue, stop creating and answer `None`, or move the mint
out of the delivery path. Deferred because it is a feature cycle's worth of
reshaping that fixes no known-live failure: with the attach site closed, the
remaining exposure is a creation site nobody has written yet. Done when a
conversation cannot be created without its channels and the roster snapshot that
names it, with the creation sites routed through one call.

Code sites (`TODO(chat-conversation-provision-chokepoint)`):
`brenn-lib/src/conversation/mod.rs`, on `create_conversation` (where the
convention is documented), and `brenn-lib/src/messaging/store/targets.rs`, on
`MessageTargets::ensure_app_conversation` (the site whose caller discharges it).


## `bridge-upgrade-rejection-terminal`

Cross-repo. The same slug is filed in `brenn-pod2`'s `TODO.md`, where the
consuming half lives; this entry is brenn's half. The slug is the join key —
move both together.

`NativeConnector::connect` (`attach/client/src/transport/native.rs`) collapses a
rejected websocket upgrade into a stringly `TransportError`, and
`AttachDriver::connect` reduces that to a unit `ConnInput::ConnectFailed` — the
same answer a refused TCP connect, a bad hostname, or a restarting server
produces. An embedder therefore cannot distinguish a hopeless credential from a
transient outage, and guessing from a run of indistinguishable failures would
kill a daemon for an ordinary server restart.

Stakes, traced through the pod: the pod's futile-attachment heuristic never
engages on a rejected upgrade, because `futile` counts only attachments that
were established and then detached, and a failed dial establishes nothing. So a
persistent `401` — a token typo, a revoked remote — re-dials **unbounded**:
backoff caps at 30 s, roughly 160 dials an hour, forever, each minting one
server-side `AuthFailure`. That is precisely the fail2ban signal, so the
operator's own pod ends up banning the operator's own IP.

Deferred rather than fixed with the round it was found in: the status plumbing
touches `TransportError`, `ConnInput::ConnectFailed`, and every matcher and test
fixture on that path in both repos, then needs a pod pin bump and a pod-side
*policy* call — which statuses are terminal, and what a headless appliance does
on terminal exit — that belongs with the pod deployment story on the table.
Nothing here loses data or crashes: the blast radius is wasted dials, log spam,
and a self-inflicted ban that heals once the credential is fixed.

Done = the handshake's HTTP status (a status, never the response body) reaches
the embedder as structured data on a failed connect, so a consumer can go
terminal on `401`/`403` instead of re-dialling into a ban.

Code site (`TODO(bridge-upgrade-rejection-terminal)`):
`attach/client/src/transport/native.rs`, the `connect_async` error mapping in
`NativeConnector::connect`.


## `bridge-violation-close-code`

Cross-repo. The same slug is filed in `brenn-pod2`'s `TODO.md`, where the
consuming half lives; this entry is brenn's half. The slug is the join key —
move both together.

When the attach route judges a frame a protocol violation it tears the
attachment down by dropping the context (`brenn-server/src/routes/attach/session.rs`,
after the `AttachProtocolViolation` event). No `Message::Close` is ever written
on this route, so the attacher sees only `TransportClosed { code: None }` — the
same thing a network blip produces. The two want opposite responses: a blip
wants a reconnect, a refusal wants the process to stop, and an attacher that
reconnects and re-sends the refused statement earns the same close forever,
minting a security event each round.

Stakes: bounded, which is why this is deferred. The pod's
`max_futile_attachments = 3` heuristic does genuinely terminate the loop — three
consecutive attachments that spoke and were answered nothing end the run. An
explicit close code is strictly better signal (immediate, unambiguous,
distinguishes violation from network flap) rather than a missing backstop, and
its client half is already built, which is why this is a TODO and not a
won't-do. Cost is a server protocol addition plus the pin-bump-and-policy tail
in the pod.

Done = the remote route closes a violated attachment with a dedicated close code
— a code only, never the violation detail, which is a security record and not
something to hand the offender — mirroring the surface route's stale-build use
of the already-built client mechanism (`ConnConfig::terminal_close_code` /
`ConnEvent::PeerClosedTerminal`; the pod already wires the receiving half and
passes `terminal_close_code: None` today).

Code site (`TODO(bridge-violation-close-code)`):
`brenn-server/src/routes/attach/session.rs`, the violation-teardown path in
`run_attach_session`.


## `tool-schema-derive`

`ToolDescriptor.input_schema` is a hand-written JSON-schema projection of each
tool's args struct, with nothing tying the two together: a field renamed, added,
or made optional in the args struct leaves the schema stating the old shape, and
the only symptom is an LLM composing calls that fail to deserialize. Same drift
shape the surface help sidecars had before they were generated from code, but
this one travels over MCP stdio rather than a bus channel, so the sidecar
mechanism does not reach it.

Needs a dependency decision before it can be built: deriving the schema means
adopting a JSON-schema derive crate (`schemars` is the obvious candidate) across
every tool's args struct, which is a new public-ish dependency on the MCP
projection path and worth a deliberate call rather than a drive-by add.

Done = every descriptor's `input_schema` is derived from its args struct and no
hand-written schema literal remains in the registry.

Code site (`TODO(tool-schema-derive)`):
`brenn-server/src/tool_registry/descriptor.rs`, the `input_schema` field.


## `bazel-teardown`

Both gates and both release paths are Bazel's now: the required GitHub check is
`make bazel-check`, and the deploy pipeline builds and packages
`//deploy:release_package`. The cargo half is still in the tree, and everything
in this list is what comes out when it goes:

- the Makefile's cargo check lanes (`CARGO_CHECK_STEPS`, `NONCARGO_*_STEPS`,
  `check-common`, `check-ci`, the parallelism knob and the step-ordering machinery
  around them), the cargo `build`/`release`/`release-musl` targets, and the
  WASM/wasm-bindgen/jco preflight and pin variables — leaving the thin verb layer
  (`check`, `build`, `launchdev`, `stopdev`, `npm-audit`, `scrub-*`).
- xtask's reimplementation-of-Bazel half: the blake3 test-result cache
  (`test_run.rs`), the lane scheduler (`parallel.rs`), the drift-compare core of
  `check_wit.rs`, the crate discovery/classification machinery and
  `lint-allowlist.toml`, and the `check` lane orchestration in `main.rs`. The
  policy guards, the sync guards, the WIT world-equivalence check, the policy
  parity check and `xtask deny` all stay.
- the committed generated files and the gates pinning them: the 37 ts-rs `.ts`
  files under `frontend/src/generated/`, `frontmatter.generated.ts`, the seven
  raw-WIT `bindings.rs`, the surface `help.md` sidecars, and `package-lock.json`
  in both npm trees (the pnpm lockfiles are what the build reads). With no
  committed copy there is nothing to drift and the gates have nothing left to
  compare.
- the scheduled `cargo-parity` CI job, and `TODO(scrub-template-drift-cache-skip)`
  — which closes with the cache it describes.

Gated on an event, not on a decision: it runs after a Bazel-built release has
been deployed to staging and then run clean in production. Until that has
happened the cargo lanes are the rollback path — the deploy pipeline can be
re-pointed at them in one commit — and the comparison run that would catch a
verdict divergence needs both sides alive. Deleting early trades a
reversible cutover for an irreversible one.

Done = the list above is deleted, `make check` is the verb layer over
`bazel test`, and cargo buildability is formally unsupported.

Code site (`TODO(bazel-teardown)`): `Makefile`, at `CARGO_CHECK_STEPS`.


## `bazel-ci-cache-pressure`

The required GitHub check builds two configurations into one Bazel disk cache —
the dev graph for `make bazel-check`, the release package for the packaging step
— and `build:ci` caps that cache's GC at 8G inside GitHub's ~10G per-repo cache
budget. Nobody has measured what it actually reaches. If it saturates, Bazel's
GC and GitHub's eviction trade warm hits away between runs and the symptom is
green runs drifting back toward twenty cold minutes, with nothing reporting it.

The observability half is done: the `check` job prints the cache size every run
and annotates a warning past 90% of the cap, which it reads out of `.bazelrc` so
that lowering the cap moves the watermark with it. This entry is the decision
that needs the data. Read the reported sizes after a few weeks of warm
main-branch runs.

Done = one of: the sizes sit comfortably under the cap and this entry closes; or
the pressure is real and one remediation lands — lower
`--experimental_disk_cache_gc_max_size` for `build:ci` (stated once, as
`<digits>G`, which is the shape the step's reader and xtask's sync guard both
hold it to) so one entry cannot crowd
the whole GitHub budget, move the release-package step off the required check
onto the weekly `cache-canary` job (accepting that release-lane breakage is then
caught weekly rather than per-merge), or split the dev and release caches into
separately keyed `actions/cache` entries.

Code site (`TODO(bazel-ci-cache-pressure)`): `.github/workflows/ci.yml`, the
`Report bazel disk cache size` step.


## `sw-registration-csp-blocked`

Both pages register the service worker from an inline `<script>`
(brenn-server/src/routes/app.rs, the landing page and the app shell), and the
CSP is `script-src 'self'` on every page (the surface relaxation only adds
`'wasm-unsafe-eval'`). Neither variant permits inline script, so both
registrations are dead lines and every app page load logs a CSP console error —
permanent noise that trains people to ignore the console.

The consequence is worse than noise. The served manifest hardcodes a
`POST /share-target` action (brenn-server/src/routes/statics.rs) and the worker's
fetch handler (frontend/src/sw.ts) is the only thing that answers it; nothing
server-side serves that URL. The only registration that actually runs is
`enablePush()` (frontend/src/push.ts), reached solely from the user-gesture menu
item — so on a device where push was never enabled there is no worker at all,
and an OS share into the installed PWA lands in the global 404 fallback, which
logs an `UnrecognizedUrl` security event against the user's own IP. That chain
is code-verified only: nobody has exercised a share from an installed PWA, so
the fixing cycle's first job is confirming the live behavior.

Needs a design call before it can be built: where registration should live
(module entry versus a shared external snippet — the landing page loads only
`nav-on-message.js`, the app page `main.js`), whether the `{ scope: "/" }`
option must match `push.ts`, and the open question of whether the landing page
needs a worker at all.

Done = the worker is registered on page load by CSP-legal means (an external
script, no inline) on whichever pages the fixing design decides need one, at
minimum the app shell; both inline `<script>` registration lines are gone; the
app page's console is clean; and a share into an installed PWA has been verified
live to reach the worker.

Code site (`TODO(sw-registration-csp-blocked)`): brenn-server/src/routes/app.rs,
both inline registration sites — `landing_page` and `render_app_shell`.

