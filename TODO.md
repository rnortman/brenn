# TODOs

## `config-document-inputs`

Loading or checking a document now takes two adjacent `Option<&Path>`
positionals — the root path and the module root — threaded unchanged through
seven signatures (`load_config`, `load_config_from`, `check_config`, `read_dsl`,
`compile`, `run_config_check`, `run_config_diff`) and read at ~30 call sites.
`load_config_from(path, module_root, fallback_dir)` is the sharp one: the two
options mean entirely different things, swapping them compiles, and the
resulting failure is either "no packaged module X" against the wrong root or
nothing at all for a document with no `@` import.

The module root is the first of a class the design names but defers: environment
facts a document must not state. The next one — a module-store URL, a pinned
release id, a provenance root — repeats the same seven-signature edit and widens
the same swap hazard by one more anonymous positional.

The shape that fixes it is a named struct threaded instead, e.g.
`DocumentInputs { root, module_root }`, built once from the CLI and passed down.
What makes it more than a refactor is that the current signatures are the ones
the slice-4 design specifies, and the struct's field set is a decision about how
the *deferred* environment facts arrive — so it wants to be settled with the
module-store work rather than guessed at ahead of it.

Code site (`TODO(config-document-inputs)`): `brenn-lib/src/config/brenn.rs`, on
`load_config_from`.

Done = one named input value carries the root and the module root from the CLI
to `compile`, and no signature in that chain takes two bare `Option<&Path>`.

## `dsl-vocabulary-config-parity`

`brenn-dsl`'s attr vocabularies and rule tables were hand transcriptions of
something in `brenn-lib` — the vocabulary of a config struct, or the behavior of
a boot-time builder. The failure this entry exists for is a field added to a
config struct that nobody adds a DSL key for: it surfaces as
`` `some_new_knob` is not a server key `` to whoever migrates a config months
later, and the fix at that point is a reconciliation across every struct pair.

Most of that is now either shared-sourced or gated, and needs nothing further:

- **The attr vocabularies.** `brenn-lib/src/config/dsl_lower.rs` builds the real
  config structs with exhaustive struct literals, so a field added to a gated
  struct fails to compile at its literal, and a vocabulary field renamed fails
  to compile where lowering reads it. What the literal does not police is the
  developer who answers that compile error by hardcoding a value instead of
  adding a DSL key — that is the residual below.
- **The resolver key tables and the statement tails.** String lists, which no
  struct literal reaches, so they are gated:
  `brenn-lib/src/config/tests/dsl_key_parity.rs` holds every field of
  `SurfaceComponentRaw` / `WasmConsumerConfigRaw` to a key the DSL admits or a
  listed omission with a reason, and
  `brenn-lib/src/config/tests/dsl_tail_parity.rs` does the same for the mount,
  subscribe, `in`, `out` and `io` tails against the `KEYS` each vocabulary
  emits. The omissions ledger is those two lists, which a check reads.
- **The addressing vocabulary** — schemes, uuid seeds, reserved segments,
  charsets, segment boundaries — is single-sourced in
  `brenn-envelope/src/addressing.rs` and read from there by the runtime, the
  guests and the DSL.
- **The grant vocabularies** — component, attach, and the `AppCapability` words
  an agent states, with their plane-word expansions — are single-sourced in
  `brenn-envelope/src/grants.rs`. `derive.rs` derives its compound tokens and
  every `(plane, scheme, token)` expansion from `AppCapability::transport()` and
  `llm_authorable()`; the policy builders read the same maps.
- **`bindable`, `EntityKind` and `Plane`** live in `brenn-envelope/src/grants.rs`
  as `bindable_schemes`, read by the DSL's position walk and by the boot
  validators in `brenn-messaging-boot/src/{surfaces,wasm}.rs`.
- **The channel-model presence rules** live in
  `brenn-envelope/src/channel_model.rs`, read by `derive.rs`'s
  `check_channel_model` and by both builders in
  `brenn-lib/src/messaging/config.rs`.
- **The ACL `Family` table** is gated by
  `brenn-lib/src/config/tests/acl_family_parity.rs`: the four ACL-bearing raw
  structs and `RemoteConfigRaw` are destructured field by field against
  `Family::held_by`, so a family added on one side without the other fails to
  compile or fails the assert.
- **The remote-ceilings and mqtt-sink tail shapes** are `REMOTE_CEILING_KEYS` and
  `MQTT_SINK_KEYS` in `derive.rs`, gated against their raw structs by the same
  test.

What remains transcribed is dispatch prose and one hand list — sites where the
runtime counterpart is a set of match arms, not a struct a destructure can
reach:

- The kindword-dispatch ledgers in `brenn-lib/src/config/dsl_lower.rs`: the
  `send_rate` key set, the configuration-section arms, the attachment-handler
  type words and their per-variant field sets, and the webhook-signature scheme
  words and theirs. Each arm ends in an exhaustive struct literal, so the
  raw-field direction is held; a *new* section, type word or scheme is caught by
  nothing.
- The per-family key sets the two `amplification` refusals in
  `surface_bindings` name — hand lists inside diagnostic messages, which no
  reflected destructure reaches. (The key set the surface component body reads
  is gated by `dsl_key_parity`.)
- `Family::absent_reason` in `brenn-dsl/src/derive.rs`: prose stating why an
  entity type holds no list of a family, mirroring the policy builders'
  structure in `brenn-lib/src/access/resolve.rs`.
- The two channel-model rules deliberately left runtime-only, named at
  `brenn-envelope/src/channel_model.rs`: a non-durable channel's `retain_depth`
  must be bounded, and a tuning block's must not be zero. Both are value rules,
  not presence rules, and the DSL does not own values.
- The hand-listed consumer grant words in
  `brenn-lib/src/messaging/config.rs`'s `every_consumer_grant_word_lowers_to_its_variant`:
  a variant added without a DSL spelling fails nothing there.

Done = each residual above either reads a shared source the runtime also reads,
or is gone.

Code sites (`TODO(dsl-vocabulary-config-parity)`):
`brenn-lib/src/config/dsl_lower.rs`, at the `send_rate` key set, at the
configuration-section kindword arms, at the `amplification` refusal key sets the
surface component's binding refusals name, at the webhook signature scheme words
and their per-variant field sets, and at the attachment handler type words and
their per-variant field sets.


## `bindings-doc-typed-grants`

`ComponentEntry::grants` is `Vec<String>` while its sibling fields on the same
struct are typed (`abi: Abi`), and `ComponentGrant` — the enum those strings
spell — is already a dependency of the schema crate. So the closed vocabulary
degrades to free strings on the wire and every reader owes a word-to-variant
reparse, plus its own decision about an unknown word. That is the shape the
single grant vocabulary exists to delete.

Fix = derive `Serialize` on `ComponentGrant` (it derives `Deserialize` today,
and a test already pins the serde spelling equal to `word()`), type the field as
the enum, and let the writer clone instead of rendering. Build skew then becomes
one serde error at document parse for free. Weigh against it: a typed field
fails the *whole* document parse on an unknown word, which loses the pointed
"this word, this instance" skew report a hand parse could give — so decide the
skew diagnostic first, then the type.

Done = the bindings document carries the vocabulary, not its spelling, and no
reader reparses.

Code sites (`TODO(bindings-doc-typed-grants)`): surface/schema/src/lib.rs, at
`ComponentEntry::grants`.


## `acl-field-spelling-home`

`Family::field_name(AclShape)` in `brenn-dsl/src/derive.rs` encodes brenn-lib's
struct-naming conventions inside the crate that deliberately cannot see them:
that `WasmConsumerConfigRaw` suffixes its ACL fields with `_acl`, that the view
structs drop the `brenn_` qualifier, and a variant named `ConsumerConfig` after a
brenn-lib type. So a rename on the brenn-lib side fails the parity gate with a
message pointing at another crate, and the fix for it is an edit in brenn-dsl for
a change that happened entirely in brenn-lib; every future ACL-holding struct
with a fourth spelling adds a fourth variant here.

Fix = keep `Family::name()`, the one spelling the DSL owns, and move the two
transforms beside `held()` in `brenn-lib/src/config/tests/acl_family_parity.rs`,
where the structs are. Weigh against it: the transforms were put in brenn-dsl so
that the mapping is part of the vocabulary rather than left to the test author,
which is the tradeoff to re-decide rather than a refactor to do in passing.

Done = brenn-dsl exports one ACL family spelling and carries no knowledge of how
a brenn-lib struct fields it.

Code sites (`TODO(acl-field-spelling-home)`): brenn-dsl/src/derive.rs, at
`AclShape`.


## `budget-refusal-per-path`

`brenn_budget::RefusalKind` is one enum spanning two paths, so neither host's
conversion is total: the page's publish arm cannot see `InvalidDeliverAfter` and
its defer arm cannot see `InvalidPayload`, and each writes an `unreachable!` for
a state the type permits (`surface/kernel/src/publish_buffer.rs`,
`brenn-wasm/src/lib.rs` — four arms in all). The same shape also makes both
hosts pay for `InvalidPayload(String)`: the classification always formats the
detail, and the page drops it, because the surface contract's `invalid-payload`
carries none. That allocation is on a guest-drivable refusal path.

Fix = one of two shapes, and the choice is a design decision rather than a
mechanical edit, because the single enum is what the shared-classification
design deliberately chose over per-family vocabularies. Either split into
`PublishRefusalKind`/`DeferRefusalKind` so both hosts' matches are total and the
`unreachable!`s (and the module doc's "no `NotPermitted` variant" argument)
disappear, or keep one enum and make the detail path-shaped
(`InvalidPayload { len, max }` and friends) so a host formats only if it has
somewhere to put it.

Done = no host writes an `unreachable!` over a gate classification, and no
refusal formats a string its host discards.

Code sites (`TODO(budget-refusal-per-path)`): brenn-budget/src/refusal.rs, at
`RefusalKind`.


## `surface-instance-acl-bound`

A surface-placed instance may state its own `acl`, and the front end derives one
from its bindings when it does not. Half of what that statement promises is
enforced: an explicit statement must cover every binding of that plane, refused
at derive time. The other half is not enforced anywhere — an instance's ACL set
on the wire planes (`brenn:`/`ephemeral:`) is never checked against its
surface's, so an operator can write a bound wider than the surface principal
holds and get a document that loads. Nothing widens at runtime (the surface's
own binding-coverage check still refuses the binding), so what is missing is the
refusal that says the config is lying, not a containment hole.

The carrier stops at the front end: lowering reads only `grants` off the
per-instance authority the derive layer computes, and the raw surface-component
struct has no ACL fields for the rest to cross into.

Fix = decide where the check belongs. Either carry the per-instance ACL families
into the raw config (with the key-parity re-pin that forces), resolve them, and
assert at boot — instance bindings within the instance's set, and the instance's
wire-plane set within the surface's — or state both asserts at derive time,
where both authorities are already in hand, and say so where the design says
boot.

Done = an instance ACL wider than its surface's is refused, and the derived
per-instance authority is either consumed whole or not carried. When it lands it
also owes `brenn config-check` a fixture: that gate family is one of the three
the check tool's boot-gate tests were meant to cover, and it stands substituted
today because there is no gate to certify (`brenn-bootstrap/src/config_check.rs`).

Code sites (`TODO(surface-instance-acl-bound)`): brenn-lib/src/config/dsl_lower.rs,
in `surface_components` where the per-instance authority's `grants` is read.


## `dsl-mcp-ref-index`

`RMcp::Ref` carries the referenced server's name, so lowering finds the
definition by scanning `resolved.mcp_servers` and comparing dotted handles,
backed by an `expect` that a match exists. Every other cross-reference in the
resolved model carries an index (`RChanRef::Decl(ChanId)`). The scan is
irrelevant at config scale; what it costs is a cross-crate invariant encoded as
a string-match outcome — a differently normalised handle in resolution turns
into a boot panic that blames the document.

Fix = have resolution mint an id for an mcp reference the way it does for a
channel, and index directly.

Code sites (`TODO(dsl-mcp-ref-index)`): brenn-lib/src/config/dsl_lower.rs, in
`mcp_servers`.


## `dsl-list-element-span`

A projection refusal on a bad list *element* carries a span covering the whole
list, so the caret lands on the opening `[` and the reader has to count elements
by hand to find the one the message's ordinal names. Scalar values are
positioned precisely; only the list path is coarse. The span comes out of the
bridge's held-node re-entry, so narrowing it likely means carrying per-element
spans through fltk's list projection — an upstream shape question, not a local
edit.

Done = the assertion in
`model.rs:a_projection_refusal_is_positioned_at_the_list_or_at_the_value` moves
from the list's line/col to the offending element's, and the message no longer
needs an element ordinal to be actionable.

Code site (`TODO(dsl-list-element-span)`): brenn-dsl/src/model.rs, the list half
of that test.


## `dsl-fmt-trivia-placement`

`brennfmt` renders a comment written after a statement's `;` *before* the
semicolon, and renders a comment on its own line inside a body at column zero
rather than at the body's indent. Both come from fltk's unparser: a suppressed
terminal is re-emitted after the trivia that followed it, and trivia is written
before the enclosing nest takes effect. A preserved blank line is trivia by the
same rule, so a statement followed by a blank line can end up with its `;` alone
on a line below the blank. Comments survive and the output is
idempotent, so this is cosmetic — but it is what a `.brenn` file looks like
after formatting, so it is worth fixing upstream. The canonical goldens pin the
current placement and will change when it is fixed.

Done = a comment keeps its source-relative position and indentation through a
format pass, with the fix in fltk and the goldens updated here.

Code site (`TODO(dsl-fmt-trivia-placement)`): brenn-dsl/grammar/brenn.fltkfmt.


## `dsl-fmt-block-blank-line`

A statement whose last token is a body's `}` — a binding with an attribute
tail, a nested `new` — leaves a blank line before its enclosing block's closing
brace. The break after the inner `}` and the break before the outer one are two
hard lines in a row, and fltk's unparser renders that pair as a blank. Saying
only one of them is not an option: dropping the inner break runs the next
statement onto the same line. Cosmetic, idempotent, and pinned by the canonical
goldens.

Done = a block-ended statement is followed by exactly one newline whatever
follows it, with the fix in fltk and the goldens updated here.

Code site (`TODO(dsl-fmt-block-blank-line)`): brenn-dsl/grammar/brenn.fltkfmt.


## `dsl-fmt-rawstring-indent`

An indented multi-line raw string (`"""…"""` written inside a block) has its
continuation lines re-indented by a format pass, and the re-indentation lands
*in the string's value* — the pass is neither format-stable nor value-stable. A
formatter that changes what a config means is a correctness defect, not
cosmetics; the fix belongs in the formatting core, not in the grammar's layout
rules. In-tree exposure is self-limiting: an affected file can never be
byte-identical to its own formatting, so it can never pass the canonical-format
gate. The bite is out-of-tree `brennfmt --in-place`. Until it is fixed, the
corpus's only multi-line raw string stays at indentation zero and in-tree
configs write multi-line values as a single-line string with `\n` escapes.

Done = an indented raw string round-trips a format pass with its value
unchanged, with the fix in fltk and the goldens updated here.

Code site (`TODO(dsl-fmt-rawstring-indent)`): brenn-dsl/src/bin/brennfmt.rs.


## `dsl-fmt-orphan-terminator`

Two layout warts in the formatter's statement handling. A comment or a blank
line following a `;`-terminated item binds to that item, so the `;` is emitted
*after* the comment and lands orphaned on a line of its own; and a tail-block
statement (`mount r { working_dir = true; }`) immediately followed by a block
statement inside the same body gains one extra leading space. Both are layout
only — no value changes, and each output is its own fixed point, so `--check`
accepts it and the canonical fixtures record it as intended output
(`brenn-dsl/tests/corpus/lexical.canonical.brenn`,
`entities.canonical.brenn`, `statements.canonical.brenn`). Every in-tree and
out-of-tree `.brenn` written since carries the form too, so the fix is a mass
golden churn: tracked here so it lands as one event with a stated before/after
rather than as an unexplained reformat.

Done = a comment or blank line after a `;`-terminated item leaves the `;` with
its item, the tail-block/block pair indents like its neighbours, and every
corpus golden is regenerated in one commit.

Code site (`TODO(dsl-fmt-orphan-terminator)`): brenn-dsl/src/bin/brennfmt.rs.


## `config-syntax-in-operator-messages`

Boot panics, tool errors and config doc comments still spell config concepts in
TOML table notation — `[[remote]]`, `[[app]]`, `[[surface]]`,
`[[app.acl.mqtt_subscribe]]`, `[[wasm_consumer.output]]` — a syntax that no
longer has a spelling now that documents are the only front end. An operator who
trips `config: duplicate [[remote]] slug ...` is sent looking for a table that
cannot exist, and `grep '\[\[app\]\]'` over their config finds nothing. The
doc comments have the same effect on the next maintainer, and because they are
the only description of these fields' authoring shape, they keep minting new
`[[...]]`-worded prose by imitation.

The `[[wasm_consumer.tool_grant]]` and `[[wasm_consumer.mqtt_output]]` mentions
are in scope like the rest, and now name concepts that do have a spelling — a
`tool` statement and a `client` matcher tail. The `[[connection]]` mentions are
not: that arm is gone, and the sites that named it were rewritten to speak of
links when it was replaced.

Roughly 750 sites across `brenn-lib`, `brenn-messaging-boot`, `brenn-server`,
`brenn-bootstrap` and `brenn-wasm` — about 330 in string literals (the
operator-visible half) and 420 in comments. It is not a sed: each site needs the
DSL spelling of the concept it names, several panic strings are asserted on by
substring in `surface_tests.rs` / `auto_tests.rs` / the boot suites, and
half-migrating is worse than either end state because a reader cannot then tell
which mentions are stale and which are load-bearing. A grep gate — `[[` inside a
string literal or doc line under the config, messaging, webhook, mqtt and access
modules — is what keeps it from regrowing, and where that gate lives is the same
open question `dsl-vocabulary-config-parity` has.

Done = no config concept is named in table notation in a string an operator or an
agent can see, the raw-struct field docs name the DSL spelling, and a check fails
when a new one appears.

Code sites (`TODO(config-syntax-in-operator-messages)`):
`brenn-lib/src/access/raw.rs` at the matcher field docs;
`brenn-lib/src/messaging/remote.rs` at the remote validation panics;
`brenn-messaging-boot/src/surfaces.rs` at the surface binding validators.


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


## `section-ref-burndown`

~968 pre-existing section-symbol references to ephemeral design docs in the
Rust tree (comment-standard Rule 1). Grandfathered: the scrub rule is
diff-only, so tree scans skip it and only newly touched lines are flagged.
Post-release cleanup, blocks nothing.

Same class, same burndown: 37 comment lines across 26 `.rs` files cite
`docs/designs/*.md` paths, a directory that does not exist in the public tree
(`docs/` holds five files); `repo-sync.md` alone accounts for 12 of them across
9 files. Re-derive both figures before working the list —
`grep -rn "docs/designs/" --include='*.rs' .` is what produced them. These are worse than the
section-symbol refs — the referent is unresolvable outright, and the rationale
each comment defers to lives only in the private ops annex, so replacing one
means deciding what of that rationale may be restated publicly. That decision
is the owner's, and it is why this burns down by hand rather than by sweep.
Unlike the section-symbol refs there is no gate at all here: a dead doc path is
mechanically checkable (path shape plus a stat), so part of the work is
deciding whether a `docs`-path-existence guard beside the scrub rules is worth
its false reds.

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

Done when instance-death teardown clears the gate, clears the dying instance's
`KernelCore.activation_error_memo` entry, and calls `deregister_activation`,
distinguishing death (deregister + clear) from Phase-3 chrome reparent (preserve
delivery, never deregister). Wire it with the kernel-driven death path, which is
a later increment / Phase-3 concern.

Two further consumers of that death path. The sync seam is a second consumer of
the gate: a sync request is refused until the requesting instance's entry is
registered, so a remount the gate wrongly rejects loses its gestures as well as
its deliveries. The activation-failure memo is keyed by instance id the same
way: a remount inheriting its predecessor's entry has its first failure demoted
to `Debug` whenever the text matches, so it reaches neither the error channel
nor `counters.errors`.

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
`attach/server/src/publish.rs`, `handle_publish`.

---

## `surface-wasm-test-in-ci`

The browser-side wasm test suites — wasm-bindgen-test, real browser — now have
**no runner at all**. `make surface-wasm-test` drove them out of band until the
cargo teardown removed it, and no Bazel rule runs a wasm-bindgen-test binary.
Nor are they compiled by any gate: each crate's `_test` target is host-only and
cfgs the browser half out, and the wasm32 clippy lane over `//surface/...`
builds the `_module` shared library, which carries no `cfg(test)`. So a suite
can go red — or stop compiling — on an ordinary edit to the code it covers, and
nothing says so; that has already happened once, to the kernel's publish-route
test, when a per-instance grant gate landed against a fixture that granted
nothing. And a suite that never runs answers no behavioral question — these are
the XSS-adjacent text-not-markup pins, the DOM seam, mount/unmount, port
dispatch, and the whole sync-call seam (the `brenn-activation-sync` listener,
the door's answer vocabulary, the publish route's buffered/refused split), which
exists only in the browser and is pinned only here.

Done when a gate runs them. Two things have to land, in order. First a Bazel
rule that drives `wasm-bindgen-test-runner` against a WebDriver browser — no
off-the-shelf rules_rust support exists, so this is real rule work, and it is
what makes a local run possible again too. Then the CI step, which is
**blocked on host provisioning and the ordering is load-bearing:** CI is a
persistent `runs-on: shell` host runner, not an image, and chromedriver has to
be installed on the runner box *first* (Fedora: `dnf install chromedriver`).
Landing the CI step before that turns main red on every push, which is also
the auto-deploy-to-staging path. The local gate must stay opt-in regardless:
contributors are not asked to install a browser driver.

Code site (`TODO(surface-wasm-test-in-ci)`): `surface/kernel/src/dom_host.rs`,
the browser suite over the live DOM capability host, which is the whole of what
the five migrated kinds render through and is compiled by nothing in CI.

---

## `surface-counters-host-testable`

The kernel's lifetime counters — the `DELIVERIES`/`PUBLISHES`/`ERRORS` scalars,
the `INSTANCE_COUNTERS` map, and `bump`/`bump_instance`/`read_counters` — live
in `surface/kernel/src/dom.rs`, which is `#[cfg(target_arch = "wasm32")]`. None
of them touches web-sys, but that placement puts every "this trigger moves that
column" assertion behind the browser runner `surface-wasm-test-in-ci` says does
not exist. It is not only unrun but uncompiled, so a rewritten counter test can
fail to build and nothing says so.

The consequence is sharpest for `counters.activation_failures`, whose *entire*
write path — the `CountActivationFailure` executor arm — is wasm-only: naming
the wrong column there, or dropping the arm from the match, ships green while
the status column reads zero forever.

Two ways out, and the choice is a layering decision rather than an edit: land
the wasm runner (`surface-wasm-test-in-ci`, blocked on runner provisioning), or
move the counters into a host-compilable module so the column-selection tests
run in the host sweep and only the DOM-touching cases stay behind the browser
gate. The second moves counter ownership out of the executor layer the kernel
currently pins it to.

Done = every "this trigger moves that column" assertion runs under `make check`.

Code sites (`TODO(surface-counters-host-testable)`):
`surface/kernel/src/dom.rs`, at the counter thread-locals.

---

## `config-check-offline-residue`

`brenn config-check` runs the messaging resolution passes that read only
`BrennConfig` (`brenn_messaging_boot::resolve_messaging_offline`), so the
per-instance surface gates decide its verdict. Two things it still cannot see,
and they are different problems:

- **Wasm-consumer resolution.** `resolve_wasm_consumers` takes the resolved
  mqtt-client map, which is built by reading `password_file` / `ca_file` off
  disk, so the pass is environment-coupled for a reason that has nothing to do
  with what it checks. Separating client identity from the secret reads would
  let the consumer gates join the offline pass. That is this entry.
- **The per-instance import⊆grants assert** (`validate_surface_assets`) reads
  the built `.wasm` component trees. A config checker does not have them and
  should not grow a build. It is boot-and-CI-with-artifacts territory, listed
  here only so it is not re-litigated into this slug.

Done = on a machine holding no secrets, `brenn config-check` fails a
`[[wasm_consumer]]` whose grants and wiring disagree.

Code site (`TODO(config-check-offline-residue)`):
`brenn-messaging-boot/src/offline.rs`, on `resolve_messaging_offline`.

---

## `e2e-in-ci`

`make e2e` — the Playwright browser suite in `e2e/tests/` — is run by no gate:
not by `make check`, and not by `.github/workflows/ci.yml`. Only an operator running it
by hand answers anything, and four of its six specs sat red — a stale
component-element selector — for an unbounded span before anyone noticed. The
suite covers layout switching, malformed-layout last-good retention,
per-instance content isolation, and reload/durable-snapshot restore; acceptance
decisions have already leaned on one of those specs while it was dead, which is
the specific harm an ungated suite does.

Done when a CI job runs `make e2e`. Blocked on two provisioning facts, and
the ordering is load-bearing the same way `surface-wasm-test-in-ci`'s is: CI is
a persistent `runs-on: shell` host whose extra tools are installed by workflow
steps, and Playwright's chromium is not one of them (`npx playwright install
chromium` plus its system libraries); and the target boots the built binary as
a real server on port 3100, so the runner needs that port free and must
tolerate a backgrounded server on a capacity-1 runner shared with every
project's deploys. Landing the CI step before both hold turns main red on
every push, which is also the auto-deploy-to-staging path.

Until then the operator-side trigger stands in for the gate: run `make e2e`
before tagging a release, and after any change under `surface/`.

Part of this suite's charter: the browser-executed proof that a component whose
grants do not admit an import is actually refused in a live page. The refusal
itself is a boot-time host panic with per-instance host tests
(`surface/server/src/lib.rs`, `validate_surface_assets`); what only a browser can
show is that the refused instance never renders.

Code site (`TODO(e2e-in-ci)`): `Makefile`, the `e2e` target.

---

## `e2e-tag-scheme-tie`

`e2e/tests/bar.spec.ts` (`publishVia`) locates a mounted component by the
attributes the kernel stamps on its host element, `data-kind` and
`data-instance`, re-encoding in TypeScript a naming whose only home is
`mount_host` (`surface/kernel/src/dom.rs`). Nothing mechanical ties the two — a
comment is the whole link, and a comment does not break a build. That drift already
happened once and cost four specs a 20-second `toBeAttached` timeout each, with
no diagnosis beyond "it times out".

Selecting on something else does not fix it. Two elements carry
`data-instance="<instance>"` for a placed instance — chrome's layout `section`
and the kernel's wrapper `div` — and every selector that picks the right one
re-encodes some Rust-side literal (the attribute pair, or `wrapper_id`'s
`brenn-surface-wrapper-` prefix): one unlinked literal traded for another.

Done when a Rust-side change to the attribute scheme fails the TypeScript build
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
   columns/queries in `brenn-messaging-store/src/db/ingress.rs`, and with them the
   `messaging_pending_pushes` table itself — these rows are all it still carries,
   and `dispatch_row` plus the dispatcher's ingress scan die with them.

Code sites (`TODO(ingress-retirement)`):
`brenn-messaging-store/src/db/envelope_column.rs` (`EnvelopeTypeColumn::Ingress`),
`brenn-messaging/src/repo_sync_cursor.rs` (the two `insert_ingress_message_raw`
writers), `brenn-messaging/src/publish/mod.rs` (`insert_ingress_message`
writer), `brenn-messaging-store/src/ingress.rs` (`Event`).

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
`brenn-render/src/tools/mod.rs`) coexists with the first-class
`tool_registry::ToolRegistry`. `ActiveBridge` carries both `tool_registry` and
`tools`, a naming trap. The `AppTool` per-tool metadata (summary formatting,
auto-approve) should eventually fold into `ToolDescriptor` so there is a single
tool table.

Code site: `brenn-render/src/tools/mod.rs` (`build_tool_registry`),
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

Code site: `brenn-messaging-boot/src/lib.rs` (async-tool request
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

Code site: `brenn-tool-registry/src/registry.rs` (`ToolRegistry::new`
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

## `wasm-dead-subscribe-acl-check`

A `[[wasm_consumer]]` with a non-empty `subscribe_acl` / `mqtt_subscribe_acl` /
`webhook_acl` whose matchers cover none of the consumer's static subscriptions boots
silently. For a WASM consumer those matchers are provably dead — no `ComponentGrant` maps to
`DynamicSubscribe`, so nothing can ever exercise them (unlike the LLM side, where an ACL
without a static sub legitimately pre-authorizes future dynamic subs). Consider a boot
check (2g) rejecting ACL-without-covering-sub for WASM consumers. This diverges WASM from
the shared subscribe_acl convention (the same gap exists pre-existing for `subscribe_acl`
on `brenn:`), so it needs a design decision before landing.

Code site: `brenn-messaging-boot/src/wasm.rs` in `resolve_wasm_consumers`, alongside
checks 2c–2f. `TODO(wasm-dead-subscribe-acl-check)`.

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

Code site: `brenn-messaging/src/lib.rs`
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
`session_cleanup_loop`, `ingress_cleanup_loop` (all in `brenn-bootstrap/src/shutdown.rs`).

Reviewers and burndowns keep rediscovering that these tasks "die silently" on panic
and proposing a supervisory wrapper. They are wrong about "silently," and the
decision is final: **every panic is logged (structured `tracing::error!`,
`panic=true`, with location) AND fires a Critical phone alert via the global panic
hook (`brenn-obs/src/panic_hook.rs`).** The residual gap — the process keeps
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
login session is still valid. Code sites: `brenn-server/src/routes/ws/dispatch.rs`,
`brenn-db/src/auth/device.rs::unenroll_device` and `resolve_or_create_device`.)




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

Code site (`TODO(automation-croner-dst-verify)`): `brenn-automation/src/job.rs`.

---

## `automation-fires-cleanup`

Automation fire rows are pruned by a simple age sweep. If fire volume ever makes
the sweep expensive, a more sophisticated prune (retention by job, per-N batching)
is the follow-up. Not urgent: current volume is trivial.

Code sites (`TODO(automation-fires-cleanup)`):
`brenn-automation/src/db.rs` (the prune statement),
`brenn-automation/src/fire.rs` (the sweep loop).

---

## `automation-fire-semantics-tests`

Some fire-semantics cases (overlap suppression, catch-up-after-downtime edges)
are covered by reasoning in comments rather than tests. Done when those cases have
direct tests.

Code site (`TODO(automation-fire-semantics-tests)`): `brenn-automation/src/fire.rs`.

---

## `event-cleanup-undelivered`

Events enqueued to a conversation that is later abandoned are never delivered and
never cleaned up; the rows accumulate. Done when abandoned-conversation cleanup
also retires their undelivered events.

Code site (`TODO(event-cleanup-undelivered)`): `brenn-db/src/conversation/mod.rs`.

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

Code site (`TODO(unify-gc)`): `brenn-bootstrap/src/lib.rs`.

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
someone remembers to run `make scrub-tree`. Blocked on a decision: CI never
installs `brenn-scrub`, so wiring the sweep into `make check` or a CI job
either needs the binary installed there or a hermetic `bazel run //scrub`
invocation (which changes the design's deliberate "verify the installed
binary" semantics).

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
`brenn-messaging/src/dispatcher.rs` (the supervisor task's normal-completion
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
`brenn-wasm-dispatch/src/lib.rs` (the `for out in &cfg.outputs`
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
`brenn-messaging/src/publish/mod.rs` (the refusal-reporting loop at the end of
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

Code site (`TODO(ring-deferred-recall)`): `brenn-messaging/src/edit.rs`
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
(`brenn-messaging/src/lib.rs`), held across the deferred-set read, and the
release sweep takes that same gate while running on the single dispatcher loop
that also wakes ordinary subscribers (`push_released_surface_views`;
`brenn-messaging/src/dispatcher.rs`). Op-driven recomputes therefore queue
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
`attach/server/src/publish.rs` (both draws, in `handle_publish_batch`);
`brenn-messaging/src/lib.rs` (`push_released_surface_views`, the sweep-side gate
take on the dispatcher loop).

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
holds or sets it**: the capability is not authorable in a config, no surface or
WASM publish path carries the field, and every internal wrapper passes `None`.
Only the legacy websocket door refills a pool today.

Consequence: a conversation driven purely over the bus — or an observer
conversation fed by ambience — has a bounded runway (the pool ceiling's worth of
unattended turns) per attended legacy-door touch, then stalls: sends are refused
with a correlated `error`, ambience is held unadvanced. Someone whose only door
to Brenn is a bus surface has no way to restart it. That is transitional, not the
intended end state.

The chat-surface project (voice gateway behind it) is the first minter: author
the `AttachGrant` → `MintImpetus` mapping, carry the field on the surface
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
`brenn-messaging/src/chat_provision.rs`, where the record channel's retained
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
`brenn-messaging/src/chat_provision.rs`, on
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
`brenn-messaging/src/reconcile.rs`, the dormant justification loop in
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
derivation in `brenn-server/src/routes/surface.rs`) sizes the cap at
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
(`max_client_frame_bytes`) and `attach/server/src/socket.rs`
(`InboundError::Oversized`, which is where the violation is raised).

## `chat-conversation-provision-chokepoint`

A conversation row and its chat channel family have to appear together, and the
bus has to be told: every creation site owes `provision_conversation_chat_channels`
under the database lock and `republish_chat_roster` outside it. Nothing enforces
that — it is a two-call convention spelled out in a doc comment on
`create_conversation` — and the tree has already missed it twice (the send-message
create path and the singleton first-attach path, both fixed by hand). The lazy
mint inside delivery was the third: `MessageTargets::ensure_app_conversation`
(`brenn-messaging-store/src/store/targets.rs`) is a synchronous method holding the
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
`brenn-db/src/conversation/mod.rs`, on `create_conversation` (where the
convention is documented), and `brenn-messaging-store/src/store/targets.rs`, on
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
attachment down by dropping the context (`attach/server/src/session.rs`,
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
`attach/server/src/session.rs`, the violation-teardown path in
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
`brenn-tool-registry/src/descriptor.rs`, the `input_schema` field.


## `bazel-teardown`

The cargo build and check lanes are gone: `make check` is the verb layer over
`bazel test`, the xtask machinery that reimplemented Bazel is deleted, the
`cargo-parity` CI job is deleted, and cargo buildability is formally
unsupported. `Cargo.toml`/`Cargo.lock` stay as `crate.from_cargo` inputs.

What is left is the last item on the original list: the committed generated
files and the gates pinning them — the 37 ts-rs `.ts` files under
`frontend/src/generated/`, `frontmatter.generated.ts`, the seven raw-WIT
`bindings.rs`, and the surface `help.md` sidecars. Every one of them is a build
artifact now, so with no committed copy there is nothing to drift and the
`generated_parity_test` / `generated_tree_parity_test` gates, the per-crate
`help_sidecar_matches_generator` tests, and the frontend's committed-copy
exclusions all have nothing left to compare.

Done = those files and their gates are deleted, and every consumer reads the
generated tree.

Code site (`TODO(bazel-teardown)`): `frontend/BUILD.bazel`, at
`generated_types_parity_test`.


## `bazel-ci-cache-pressure`

The required GitHub check builds two configurations into one Bazel disk cache —
the dev graph for `make bazel-check`, the release package for the packaging step
— and `build:ci` caps that cache's GC at 8G inside GitHub's ~10G per-repo cache
budget. Nobody has measured what it actually reaches. If it saturates, Bazel's
GC and GitHub's eviction trade warm hits away between runs and the symptom is
green runs drifting back toward twenty cold minutes, with nothing reporting it.

The repository cache shares that one `actions/cache` entry with it, so the
budget question is over the pair.

The observability half is done: the `check` job prints both cache sizes every
run and annotates a warning past 90% of the cap, which it reads out of
`.bazelrc` so that lowering the cap moves the watermark with it. (No watermark
on the repository cache — it has no size cap to place one against; the GC flags
bazel offers for fetched repos are age-based and belong to the repo contents
cache, which `build:ci` disables so the entry carries downloads only. Its number
is reported and read by a human.) This entry is the decision that needs the
data. Read the reported sizes after a few weeks of warm main-branch runs.

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
`Report bazel cache sizes` step.


## `bazel-ci-timings`

The bazel-optimization program's whole payoff claim rests on incremental runs
being cheap, and nothing has measured that. Record per-step CI durations for
~2 weeks of post-landing runs across the three commit shapes the program's test
plan named — docs-only, component-only, source — plus the per-target test times
the run summary already prints.

Done = the numbers are read and one of three outcomes is chosen: close (the
incremental floor is fine and the work paid off), reopen `crate-split` for a
tranche 3 per the reopen condition recorded there, or target the specific slow
suites the data names (cheapening the wasm engine tests with a shared engine or
precompiled guests is the obvious candidate).

Separate from `bazel-ci-cache-pressure` on purpose: the two read-outs share a
cadence but answer different questions — durations here, cache saturation there
— and they prescribe different remedies.

Code site (`TODO(bazel-ci-timings)`): `.github/workflows/ci.yml`, beside the
`Report bazel cache sizes` step.


## `crate-split`

`brenn-lib` and `brenn-server` are each one `rust_test(crate = ...)` target over
a whole crate, so any source edit re-runs every test in it. On the CD runner the
brenn-server target took 472s at ~2,500 tests; it is at 1,348 and brenn-lib at
967 as the tranches land. Only
finer crates reduce that work — within-target partitions (wrapper targets,
libtest filters, sharding) all keep the whole crate in the input closure, so
they re-run exactly as much.

The program is four tranches. Tranche 0 (the cargo/xtask teardown) and tranche
1 have landed: `brenn-approval-rules`, `brenn-obs`, `brenn-ws-types` out of
brenn-lib, then `brenn-render` (319 tests) and `brenn-git` (now 77) out of
brenn-server. Tranche 2 is under way: `brenn-automation` (78), `brenn-db`
(129 — the connection handle, the base schema, and the `auth`, `conversation`
and `cost_samples` DAOs over it), `brenn-webhook` (57), `brenn-pwa-push`
(145) and `brenn-mqtt` (69, plus the 16-test mosquitto integration suite) have
left brenn-lib. Remaining:

- Tranche 1 residue: four leaf sinks stayed in brenn-server — `path_validate`
  (19 tests), `client_ip` (20), `cc_schema_drift` (5), `pid_file` (3). They
  share no through-line with each other or with the render cluster, and 47
  tests does not repay four crates' ceremony; each belongs with whichever
  tranche-2/3 crate its consumer lands in.
- Tranche 2, brenn-lib: `automation`, the data layer, `webhook`, `pwa_push` and
  the `mqtt` runtime have left. The seam that works on the `config` hub — sink
  the interface, raise the runtime — is now applied three times and is the
  prescription: what production code below the subsystem reads (its config
  blocks, the resolved types `ResolvedConfig` holds, and the data those carry —
  the webhook `SignatureScheme`, the push `EndpointPolicy` and VAPID keypair,
  MQTT addressing) stays in brenn-lib; the wire path rises into its own crate. Moving the aggregate
  above the subsystems instead does not work: `messaging` reads
  `config::{AppConfig, AppConfigRaw, LlmChatConfig, ServerConfig}` in
  production (brenn-lib's `messaging/{gates,config,remote}.rs`,
  `brenn-messaging-store`'s `store/targets.rs`, and `brenn-messaging`'s
  `lib.rs`, `chat_roster.rs` and `chat_provision.rs`),
  and the subsystems depend on `messaging`, so an aggregate above them is above
  `messaging` too and closes the cycle from the other side.
- The subsystems on that seam are done. `mqtt` cut where its extra edge said it
  had to: `messaging` and `access` production code parse MQTT addresses and
  validate topic filters, so `mqtt::{address, config, error}` stayed in
  brenn-lib and only the wire half — `service`, `connection`, `state`,
  `payload`, `egress` — rose. What is left in brenn-lib below `messaging` is
  the `config` aggregate itself and the leaf sinks
  (`token_bucket`, `mcp_tool_names`, `model_window_cache`, `runtime_dir`,
  `subprocess`), none of which repays a crate on its own. The usage cluster
  did repay one and has left: `brenn-usage-db` (32 tests) sits directly above
  `brenn-db`, owns `usage_sessions`/`usage_events`, and carries the CSV/JSON
  export writers with the row types they serialize.
- Tranche 2, brenn-server: `brenn-bootstrap` (401 tests) has left — the
  composition root with `cli` and `pid_file`, and the two boot-dependent test
  trees that had to move up with it (the tree then at
  `routes/surface/ws_tests.rs`, the surface
  boot harness, `wasm_dispatch/tests/e2e.rs`). The route it took is the
  prescription for the rest: the modules the root wires are `pub`, the fixture
  layers those tests are built on (`test_support`, `routes/surface/
  test_fixtures`, `wasm_dispatch/tests`) are `pub` behind brenn-server's new
  `testutils` feature, and the migration composition stayed below in
  `brenn-server/src/db.rs` so production and test open through one function.
  The root then shed its own biggest half: `brenn-messaging-boot` (339 tests)
  is the boot-time lowering of the messaging configuration — channel
  derivation, auto wiring, surface and consumer resolution, `build_messaging`
  itself — which referenced nothing else in the root, so it extracts *below*
  it. What stayed in `brenn-bootstrap` is the 62 tests that stand a whole wired
  server up (the surface WS round trip and the dispatch end-to-end family);
  they reach the lowering through its `testutils`-gated `test_fixtures`.
  `brenn-wasm-dispatch` (40 tests) followed it out, in the other direction:
  the dispatch task reads no server type, so it sits *below* brenn-server with
  its four guest fixtures, and only the three suites that also drive the tool
  executor and the repo-sync fixtures stayed (`brenn-server/src/
  wasm_dispatch_tests/`, built on the harness the lower crate exposes behind
  `testutils`). Still in brenn-server: the four `*_intercept` modules
  (`active_bridge/brenn_tools` dispatches into all four in production, which is
  why they belong to `active_bridge`'s tranche-3 treatment).
- Tranche 3: brenn-lib `messaging`, brenn-server `routes` and `active_bridge`.
  A first coupling pass over all three says none of them leaves whole:
  - `active_bridge` (482 tests) reads `tool_registry`, `repo_sync`,
    `idle_hooks`, `cc_schema_drift`, `mqtt_router`, `messaging_router`,
    `hooks`, `routes::upload::ResolvedAttachment` and all four `*_intercept`
    modules in production, and `state` and `routes` read it back. Taking it out
    means taking most of brenn-server with it; the cut has to be inside it.
  - `routes` holds the reverse shape: `state` holds
    `routes::{surface::SurfaceRuntime, remote::RemoteRuntime}` as production
    fields while `routes` reads `state` 47 times. Either those runtime types
    come down or `state` goes up; that decision is the first step of any
    routes tranche. Two of the three came down already. The attachment layer
    (`routes::attach`, 143 tests) was a pure sink — its whole production
    surface is `brenn-attach-proto`, `brenn-messaging`, `brenn-lib`,
    `brenn-obs` and `brenn-envelope`, with no reference to `state`,
    `active_bridge` or any sibling route — so it left as `brenn-attach-server`
    at `attach/server`, beside the proto and client crates it already shares a
    protocol with. `state.attach_registry`, `messaging_router`'s fan-out, the
    surface and remote routes and `brenn-bootstrap`'s `AppState` literal name
    it from above.

    `SurfaceRuntime` came down next, as `brenn-surface-server` at
    `surface/server` (123 tests): the whole boot half of `routes::surface` —
    the config lowering into runtimes, the attachment profile, the bindings and
    self-description documents, asset validation, the single-writer sweeps and
    the disconnected stamp — reads no `state` and no sibling route, so the cut
    ran *inside* what was then `routes/surface/mod.rs` rather than around it.
    What stayed in
    brenn-server is the part that needs `AppState`: `authorize_surface`,
    `surface_ws_handler`, `page.rs`, the conformance suite, and the rigs that
    stand a whole state up. The seam is the general one for this tranche —
    a `state` field's *type* can come down even when the route that reads
    `state` cannot.

    `RemoteRuntime` came down the same way, as `brenn-remote-server` (14
    tests): the `[[remote]]` lowering, the attachment profile, and the
    bearer-credential comparison read no `state` and no sibling route, so the
    cut ran inside `routes/remote/mod.rs`. `authenticate_remote` came down with
    them, taking the runtime map and the alert dispatcher as arguments instead
    of reading them off `AppState`, which keeps the whole uniform-401 posture
    (dummy token, one security event, one refusal) in one place. What stayed in
    brenn-server is `remote_ws_handler` and the three suites that stand a whole
    `AppState` up.

    Left of `routes`: `ws` (248 tests, which reads `state` 24 times and
    `active_bridge` 18), `webhooks` (43),
    `upload`/`file`/`redirector`/`target_handler` (86), `page` and the
    `AppState` halves of `surface` and `remote` — all of them
    `State<AppState>` handlers, and `ws`'s sub-modules are `impl WsConnection`
    blocks over a struct that holds `AppState`, so they cannot be split apart
    from each other either (the same inherent-impl constraint the messaging
    runtime hit). Nothing further comes out of `routes` without deciding who
    owns the state fields.
  - **What blocks the rest, stated once.** Every remaining brenn-server module
    is inside one production cycle through `AppState`. `state` holds
    `active_bridges`, `mqtt_event_router` and the route runtimes;
    `active_bridge` holds `mqtt_router::MqttEventRouterImpl` as a production
    field (`bridge.rs`), names `messaging_router::DeliveryBinding`
    (`bridge_io.rs`) and `routes::upload::ResolvedAttachment` (`user_send.rs`),
    and dispatches into the four `*_intercept` modules, `idle_hooks`,
    `cc_schema_drift` and `mqtt_subscribe`; and `mqtt_router` holds an
    `AppState` in its `OnceCell` router state, so active_bridge reaches
    `AppState` through it. The intercepts and every `active_bridge`
    sub-module (`brenn_tools` 126 tests, `compaction` 75, `cc_event_loop` 70,
    `bus_chat` 50, `permission_sync` 29, `tool_card` 10) take `&ActiveBridge`,
    so none of them is separable from the struct. Breaking any of it needs a
    decision that is design work, not extraction: either the late-binding
    routers stop holding `AppState` (a registry or a narrower context type), or
    the state fields' ownership is inverted. Both are out of bounds under the
    program's no-invented-abstraction rule.
  - The service layer below the bridge has left. `repo_sync` split at the
    reactor seam: its git plumbing (26 tests) and its clone/trigger vocabulary
    (4) went down into `brenn-git`, which now also carries the `pull` path the
    reactor, the hooks and the `git-repo-pull` tool all call; the reactor and
    the manager stayed in brenn-server, holding `ActiveBridges` as they always
    did. `brenn-hooks` (36) and `brenn-tool-registry` (62) followed the
    plumbing down, and `git-fixture` gained the scratch remote-and-clone
    helpers all three suites build on. Still in brenn-server from that layer:
    `repo_sync`'s reactor/manager half (45 tests), which is `active_bridge`'s
    tranche-3 problem.
  - `messaging` (1,032) has the same seam the three subsystems took: every
    back-edge into it from `access`, `config`, `mqtt`, `webhook`,
    `repo_sync_cursor` and `tools` lands in `messaging::config` or in the value
    types in `messaging.rs` (`ChannelScheme`, `Urgency`, `WakeMin`,
    `ParticipantId`, `ChannelEntry`, `MessagingDirectory`, `gates`), so the
    addressing/config half stays and the runtime (`publish`, `store`, `db`,
    `dispatcher`, `subscribe`, `query`, `ingress`, `edit`, `remote`, `system`,
    `live`, `conversations`, `chat_*`, `reconcile`) rises with the messaging
    DDL set. All three layers have landed: the below-facing vocabulary is
    `brenn-lib/src/messaging/` (`addressing`, `config`, `directory`, `gates`,
    `identity`, `remote`, `test_support`, 967 tests with the rest of brenn-lib),
    the persistence layer is `brenn-messaging-store` (277), and the engine is
    `brenn-messaging` (470), which glob-re-exports the vocabulary so the moved
    code's own paths resolve — callers name the vocabulary through
    `brenn_lib::messaging`. The engine had to rise as one crate: every runtime
    module is an `impl Messenger` block, and Rust forbids an inherent impl on a
    foreign type, so there is no smaller compiling slice. `repo_sync_cursor`
    rose with it, since it reads `messaging::db` in production.

    The store split is the one cut that constraint leaves: `db`, `store` and
    `ingress` name `Messenger` nowhere, so they sit below the engine and stay
    cached on an engine edit. `brenn-messaging` binds the three modules at its
    crate root privately, so the engine's own `crate::db`/`crate::store`/
    `crate::ingress` paths resolve; every crate above names
    `brenn_messaging_store` directly, which is what keeps the store edge visible
    in the BUILD files and cacheable per crate. The two crates whose *whole* use
    of messaging was the tables — `brenn-mqtt` and `brenn-pwa-push` — repoint at
    `brenn_messaging_store::` and drop the engine dependency outright. What is
    left in the engine is the `impl Messenger` blocks plus `format`,
    `dispatcher`, `testutils` and `repo_sync_cursor`; splitting any of it needs
    the inherent-impl problem solved, not a coupling map.
  - `brenn-bootstrap` was the fourth target over the ~300-test criterion and is
    the one that opened. Its `messaging/` subtree had **zero** `crate::`
    references — the lowering reads `brenn-lib`, `brenn-messaging`,
    `brenn-server` and the surface crates and nothing of the root — so it left
    as `brenn-messaging-boot` with no cycle to break and nothing to invert. The
    only edges the other way were the root's two production calls
    (`messaging_configured`, `build_messaging`) and three of its test modules
    reaching the boot fixtures, which is what the new crate's `testutils`
    feature is for.
- **Where a subsystem's DDL lives is settled.** A crate's migration set covers
  exactly the tables its own production code touches; the DDL lives in the
  lowest crate whose production code writes the table; every composition point
  that opens a database runs the sets for everything it wires and nothing else.
  A set a crate owns is `run_*_migrations`; a composition of several crates'
  sets is `run_*slice_migrations`, so the two never share a name. So
  `brenn_db::run_migrations` is base + `pwa_push_subscriptions` (that one
  because `unenroll_device` deletes its rows inside the unenroll transaction),
  `brenn_usage_db::run_usage_migrations` is the two usage tables,
  `brenn_lib::db::run_slice_migrations` composes both of those,
  `brenn_messaging_store::db::run_slice_migrations` adds the messaging tables on
  top, `brenn_messaging::slice::run_slice_migrations` adds
  `brenn_messaging::repo_sync_cursor::run_repo_sync_cursor_migrations` (the
  advance cursor, whose only production writer is the engine's
  `upsert_and_enqueue`), and `brenn-server/src/db.rs`'s
  `run_server_slice_migrations` composes that with automation. A crate that
  extracts takes its DDL and
  its registration obligation with it — the usage set moved with
  `brenn-usage-db` and the messaging set with `brenn-messaging`, then down again
  with `brenn-messaging-store`.

**The residue against the ~300-test criterion**, for the gate that accepts or
rejects it: `brenn-server` (1,348) and `brenn-lib` (967) are blocked by the two
cycles written down above, `brenn-messaging` (470) by the inherent-impl
constraint, and `brenn-messaging-boot` (339) is over the count but not over what
the count was a proxy for — it runs in 0.39s locally, ~8s at the repo's measured
CD factor, against `brenn-server`'s 472s that started this program. Test count
was the criterion because no timing data existed; the timings now in each
target's `size` comment are the better unit, and every target except the three
blocked ones is inside a `small` budget.

**Accepted at the 2026-08-13 follow-up gate**, on the first real CI
measurements: `brenn-server_test` runs ~100s there against the 472s that opened
this program, `brenn-bootstrap_test` ~94s, and the whole `make bazel-check`
9m33s. Both residue targets sit at the top of the tree, so nearly any source
edit re-runs both — that pair, running in parallel, is the incremental floor,
and only the deferred state-ownership inversion lowers it. Reopen condition: if
the `bazel-ci-timings` read-out shows typical incremental runs dominated by the
residue targets, commission the state-ownership/registry design cycle as a
tranche 3; otherwise this entry closes with the residue standing.

Every extraction is monotonic: a module whose dependency cycle will not break
cleanly stays put and is recorded rather than forced apart with an invented
abstraction layer. Per tranche, the sum of executed tests across new and
remaining targets must equal the pre-tranche count.

Two rules the bootstrap extraction paid for, to apply from tranche 3 on:
publish the items the crate above actually reaches, not whole modules — the
map-first step already enumerates that closure, and promoting a module
wholesale hands clippy lints (`len_without_is_empty`, missing `Default`) the
job of deciding what the public API is. And a first-party dependency that only
the `testutils` half of a crate names goes in `deps` through
`testutils_deps()` (`bazel/features/defs.bzl`), so the build edge tracks the
same condition the Cargo manifest's `optional = true` states.

Done = tranche 3 landed, or the residue recorded and accepted at a review gate.

Code sites (`TODO(crate-split)`): `brenn-lib/BUILD.bazel` and
`brenn-server/BUILD.bazel`, above their `rust_test` targets.


## `attach-upgrade-preamble`

`surface_ws_handler` (`brenn-server/src/routes/surface.rs`) and
`remote_ws_handler` (`brenn-server/src/routes/remote.rs`) each carry ~60
near-identical lines between authorization and upgrade: mint the session id,
open the push channel at `PUSH_QUEUE_FRAMES`, build `active_channels` /
`drain_notify`, build the `AttachSessionHandle`, read `session_caps()`,
`try_register`, turn both `RegisterRejection` arms into a `warn!` + 503, compute
`max_client_frame_bytes`, and fill the 17-field `AttachSessionParams`. They
differ in the account source (session cookie vs `remote:<slug>`), the registry
key, the surface-only build-id handshake and `last_detach` stamp, and the
`warn!` field names.

The cost is that `AttachSessionParams` gaining a field is two edits, and the
register-before-upgrade ordering — load-bearing against a check-then-register
race — is stated twice and can drift once. A third attacher copies one of the
two blocks.

What blocks a straight hoist is that the two handlers hold different runtime
types (`SurfaceRuntime` reaches its messenger through a method, the remote's
through a field), and the surface's handshake sits *between* register and
upgrade, so a shared `register_and_run` needs either a hook parameter or a
trait over the runtimes — an abstraction whose shape is a design question, not
a refactor. It is also the wrong moment: `routes` is a tranche-3 extraction
under `crate-split`, whose coupling map does not exist yet and which decides
which crate this seam belongs to.

Done = the tranche-3 routes work either lands the shared preamble in
`brenn-attach-server` or records why the two copies stay.

Code sites (`TODO(attach-upgrade-preamble)`): `brenn-server/src/routes/surface.rs`
and `brenn-server/src/routes/remote.rs`, above each handler.


## `bazel-fixture-list-guard`

Seventeen test targets now declare, by hand, which WASM component fixtures they
stage: the twelve `WASM_TEST_SUITES` entries and `brenn-wasm_test` in
`brenn-wasm/BUILD.bazel`, `brenn-server_test`'s six in
`brenn-server/BUILD.bazel`, `brenn-wasm-dispatch_test`'s four,
`brenn-bootstrap_test`'s one and `//surface/server:server_test`'s one. Each list
was derived by reading that target's sources for artifact stems, and nothing
mechanizes the derivation.

Under-declaration is loud: a test that opens a component its target does not
stage panics on a missing runfile the first time it runs. Over-declaration is
silent forever. A suite that stops loading a component keeps the stale edge, the
all-to-all invalidation this narrowing removed grows back one commit at a time,
and the CI timings that `crate-split` is supposed to read get polluted by an
over-declaration nobody can see.

The durable answer is a guard reading `COMPONENT_NAMES`, `WASM_TEST_SUITES` and
`brenn-server_test`'s `data` out of the BUILD files and the `brenn_*` artifact
stems out of each target's sources, reporting both directions. What it needs
before it can be written is a derivation rule that is sound in both: stems reach
a suite through helpers it calls (`replay_artifact()` in
`brenn-wasm/tests/common/mod.rs` hardcodes `brenn_replay`), and not every
`brenn_*.wasm` literal is a fixture read — `brenn-server/src/router.rs` and
`surface/server/src/lib.rs` fabricate a `brenn_surface_kernel_bg.wasm` in a temp
dir, which no fixture target builds. A guard that demanded that one, or that
missed the helper-reached ones, would be worse than none: this gate's red has to
keep meaning the change is wrong.

Done = the rule is settled and the guard reports both directions with tests, or
this entry is closed with the argument for leaving the lists hand-held.

Code sites (`TODO(bazel-fixture-list-guard)`): `brenn-wasm/BUILD.bazel`, above
`WASM_TEST_SUITES`; `brenn-server/BUILD.bazel`, above the `brenn-server_test`
target; `brenn-wasm-dispatch/BUILD.bazel`, above its test target;
`surface/server/BUILD.bazel`, inside its test target's `data`.


## `wasm-guest-tests-unrun`

Twenty-six `#[test]` functions inside deployed WASM guest components have no
runner: 21 in `brenn-wasm/components/replay/src/lib.rs` and 5 in
`brenn-wasm/components/replay-generic/src/lib.rs`. Both packages declare only
`wasm_guest_cdylib` + `wasm_component`, so the wasm-platform clippy build
type-checks the test module and nothing executes it. The deleted `xtask test`
lane ran the root workspace only, so this predates the Bazel cycle rather than
being caused by it.

What is dark is not incidental: the replay component is the anti-replay gate in
front of the ingress, and its tests pin `parse_sent_at_ms` (Hinnant
days-from-epoch including pre-epoch and century-leap cases), `validate_client_id`
path-traversal rejection, nonce and timestamp shape validation, the
`parse_envelope` malformed-input arms, and the prune-gate predicate;
replay-generic's pin the big-endian `entry_key` layout, its chronological
ordering property, and `CAP < 4096` so the store returns 429 before the host's
scan trap returns 500. Every one of those is a silent-wrong-answer failure mode
in a component that ships.

`xtask/src/test_target_guard.rs` structurally cannot notice this: its rule keys
on a package declaring a `rust_library` or `rust_binary`, and these declare
neither. Stretching the guard to cover guest packages is the wrong fix — it
would demand a target that no rule can produce today.

Done = the tests run under a gate, or they are deleted with the argument for
losing the coverage. The tests are annotated "native, no WASM roundtrip" and
touch no host imports, so the cheap route is a host-target `rust_test` over the
logic module; the obstacle is that `src/lib.rs` also carries
`bindings::export!(Component with_types_in bindings)` and the generated
`bindings.rs`, which do not compile off wasm32 — so it needs either a split of
the pure-logic code into a host-buildable module the guest crate includes, or a
wasm32 test rule (which is the same missing rule work `surface-wasm-test-in-ci`
describes, without the browser half).

Code sites (`TODO(wasm-guest-tests-unrun)`):
`brenn-wasm/components/replay/BUILD.bazel` and
`brenn-wasm/components/replay-generic/BUILD.bazel`, above the guest cdylib.


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



## `build-test-count-guard`

Every `rust_test` size rationale in the tree states a test count ("745 tests",
"967 tests", "77 tests") and nothing holds those numbers to the sources. They
are also the ledger the review chain does its conservation arithmetic against
when tests move between crates, so a count that is approximately right stops
being useful as a check. Two of them drifted by two — in opposite directions —
inside a single round, when a helper and its tests moved from brenn-messaging
to brenn-lib and neither comment followed.

The obvious guard — parse the `N tests` figure out of the comment above each
`rust_test` and compare it against the test attributes under that crate's `src/`
— needs a derivation rule the tree does not have. Attribute counting is not the
executed count: ts-rs's derive emits one `export_bindings_*` test per exported
type (brenn-ws-types runs 105 with 71 hand-written), and any future derive that
generates tests widens the gap. The guard has to either model the generators or
read counts from a run, and which of those the repo wants is the open question.

Done = a gate fails when a stated count does not match what the target runs, or
the counts are removed from the comments in favour of something checkable.

Code site (`TODO(build-test-count-guard)`): `brenn-lib/BUILD.bazel`, above the
`brenn-lib_test` target.


## `unused-crate-deps-gate`

Nothing in the tree flags a declared dependency that no source names: there is no
`unused_crate_dependencies` lint configured anywhere, in `.bazelrc`, in any
`rustc_flags`, or as a crate-root attribute, and no udeps-style manifest sweep.
A spurious dep is therefore invisible to `make check` forever.

The driver is that every crate extraction hand-copies a dep list from the crate
it was cut out of, and the copies are not re-derived from the sources that
survived the cut. One already shipped: `surface/server` was declared with
`//brenn-obs` in `FIRST_PARTY_DEPS` and two `brenn-obs` lines in its
`Cargo.toml` while no file under `surface/server/src/` ever named `brenn_obs` —
the `AlertDispatcher` uses that justified it stayed behind in `brenn-server`.
That was caught by a human reading the diff, not by a gate, and the remaining
`crate-split` tranches cut more crates the same way. The cost of a miss is a
permanent invalidation edge: every edit to the phantom dependency recompiles the
crate, re-runs its tests, and cascades into everything above it.

Two open questions decide the shape. Which mechanism: `unused_crate_dependencies`
as a rustc lint, which would have to reach every `rust_library` in the build
(a repo-wide `rustc_flags` in `.bazelrc` or an aspect, since there is no single
crate root to annotate), versus a manifest-level sweep over the `Cargo.toml`
files, which are advisory here — Bazel is the build of record — and so would
check a second copy of the truth rather than the one that matters. And how to
absorb the existing violations across ~30 crates, whose count nobody has
measured; the lint's known false-positive shapes (deps used only by macro
expansion, by `cfg`-gated code, or re-exported without being named) may need
per-crate allowances, and a gate that has to be suppressed in a dozen places is
noise rather than a check.

No code site: this is a repo-wide build concern with no single place the comment
would belong, so the entry lives here only.

Done = a gate fails when a crate declares a dependency its sources do not use,
with the existing violations either cleared or explicitly allowed; or this entry
closes with the measurement that says the false-positive rate on this tree makes
the gate worse than the hand audit.

## `register-page-invite-oracle`

`GET /auth/register` returns a bare 404 whenever the `invite_codes` table holds
no unused row (`brenn-server/src/routes/register.rs`, the `has_unused_invite_codes`
early return). That is the steady state — prod has two codes, both consumed — so
the `Register` link that `brenn-server/src/routes/login.rs` renders
unconditionally on the login page leads to a dead end for every visitor.

The same branch leaks a bit of live server state to anyone on the internet:
200 vs 404 answers "is an onboarding window open right now?" without a session.
It is not an access path — the code is 16 bytes from a CSPRNG, and a wrong code
on `POST` raises `AuthFailure` for fail2ban — but the response varies with
internal state, and the leak is *silent*: this branch deliberately does not log
("this is NOT a security event"), while the global `not_found` fallback logs
`UnrecognizedUrl` for every other unrouted path. Probing this one endpoint is
the only free probe on the pre-auth surface, which cuts against the
§5 policy in `docs/security-posture.md` that unrecognized requests are a
defensive signal.

The fix is to make the response invariant to invite state: always render the
form (or a neutral invite-only page that does not reveal whether a code is
outstanding) and let `POST` remain the only path that distinguishes valid from
invalid. `POST /auth/register` is already reachable regardless of what `GET`
returns — the route registers `.post(register_submit)` unconditionally and the
handler never consults `has_unused_invite_codes` — so this adds no new
attacker-reachable surface. A submission with no invite outstanding redirects to
`?error=invite` like any bad code and logs the `AuthFailure` that today's 404
suppresses.

Two things this entry does not cover. The unconditional
`alert(AlertSeverity::Info, "Registration attempt: ...")` at the top of
`register_submit` fires before any validation, and the alert limiter is a single
severity-blind global window (10/60s in prod), so cheap garbage `POST`s can
starve `Critical` alerts; that is a separate concern about the alerting contract,
not about this page. And whether the neutral page should exist at all, versus
always showing the real form, is a UX call for the operator — showing the form
means a stranger can submit codes at a server with no open invites, which is
arguably the fail2ban signal we want rather than a cost.

Code sites (`TODO(register-page-invite-oracle)`):
brenn-server/src/routes/register.rs, at the `has_unused_invite_codes` early
return in `register_page`; brenn-server/src/routes/login.rs, at the
unconditional `Register` link in `login_page`.

Done = `GET /auth/register` returns the same status regardless of whether an
unused invite code exists, and the login page's `Register` link leads somewhere
that explains itself.

## `dsl-doc-examples-ungated`

`docs/config-dsl.md` carries ~26 fenced DSL blocks. Exactly one — the `Chrome`
spec — is held against anything (`brenn-lib/src/config/tests/config_files.rs`,
`the_dsl_doc_transcribes_the_chrome_spec_verbatim`, which compares the block to
`config/specs/chrome.brenn` byte for byte). Every other snippet is prose: the
`channel` declaration, the tuning form, the consumer and surface instances, the
`agent` block with its `mount`/`mcp_server`/`subscribe` tails, the `assembly`
block, and the `acl` matcher-tail example. Nothing parses them.

So a key-set or grammar change — renaming a body key, dropping a `CONSUMER_KEYS`
entry, retiring the free-`io` form, changing a binding arrow — leaves the one
reference an operator copies from full of examples that no longer compile, and
no gate notices.

What makes this more than mechanical: most snippets are fragments, not
documents. A `channel` block alone is not a loadable root (a durable channel
needs a uuid pin; a root needs server keys), so gating them means deciding what
a wrapping root looks like, whether the fixture or the doc is the source of
truth, and which snippets are normative versus deliberately illustrative. That
is an authoring decision about the document, not just a test to write.

Two shapes to weigh. (a) Self-contained snippets move to fixture files (e.g.
`docs/examples/*.brenn`), join the format and compile gates by glob, and the doc
transcribes them under a table-driven version of the existing
transcription test. (b) The doc stays the source and a harness extracts each
block, wraps it in a minimal root, and compiles it — no duplication, but the
wrapping is machinery the doc's author cannot see.

Code site (`TODO(dsl-doc-examples-ungated)`):
brenn-lib/src/config/tests/config_files.rs, on the transcription test that
gates the one block.

Done = every block in `docs/config-dsl.md` that is presented as compilable is
compiled by `make check`, and the blocks that are not are marked as fragments in
the prose.

## `shared-processor-bindings`

Four raw-WIT fixture components — `processor-exhaust`, `processor-mem-exhaust`,
`processor-mqtt-test`, `processor-tool-test` — each carry their own
`wit_bindgen_rust` target and their own committed `src/bindings.rs`, and the
four files are byte-identical: the same generation of the same
`brenn:processor` world, ~3,100 lines apiece. Every world change costs a 4×
regenerated-file churn that buries the hand-written half of the diff, and the
four `generated_parity_test`s can only ever fail together.

The two candidate shapes differ in what coverage survives, which is why this is
not a mechanical edit: one shared `wit_bindgen_rust` target with a single
committed copy keeps the fixtures' raw-WIT independence (they exist to exercise
the world without the guest SDK) but has to decide where a bindings file that
belongs to no crate lives; moving the fixtures onto `brenn-guest` deletes the
committed copies entirely and with them the only in-tree coverage of the
non-SDK path.

Code site (`TODO(shared-processor-bindings)`):
`brenn-wasm/components/processor-exhaust/BUILD.bazel`, on the `bindings`
target.

Done = one generation of the processor world's bindings is committed once, and
a world change regenerates one file.

## `surface-fault-report`

Every surface component state machine takes the same two steps on a port
delivery it does not like: it reports a malformed body as one operator log line
naming channel, sender and message id, and it reports a latest-wins window
carrying more than one new message as a `push_depth` misconfiguration. Both
lines are deliberately identical across components so a buggy publisher is
grep-able the same way whichever component caught it.

They used to live in one place, `surface/component-support`, which is gone with
the dom carrier. Each migrated kind spells both itself, because a page-hosted
component compiles for wasm32 against the guest crate universe and the shared
home was a root-workspace crate with host-only dependencies. Five copies of a
line whose whole point is being one line is the failure mode this exists to
prevent.

The shape is a guest-side home — the guest SDK, or a small crate in the guest
workspace beside it — holding the report struct, its `log_message`, and the
latest-wins window report, taking the raw envelope JSON the SDK's window hands
over.

Code sites (`TODO(surface-fault-report)`):
`surface/components/mode-clock/src/logic.rs`, above the two local copies;
`surface/components/protobar/src/logic.rs` and
`surface/components/meeting/src/logic.rs`, above each one's own copy of the
report; and `surface/chrome/src/logic.rs`, above chrome's latest-wins copy.

Done = one spelling of each line, reachable from a page-hosted component, and
every migrated kind's copies deleted.

## `surface-guest-wire-crate`

A page-hosted kind cannot name `brenn-surface-schema`, so every wire shape it
touches is re-typed inside its own crate and held to the real one by a host-side
parity test: mode-clock re-spells `ThemeBody`, `CONTROL_PLANE_VERSION` and the
`"dark"`/`"light"` strings (`the_wire_strings_are_the_shared_ones`,
`the_theme_body_is_the_shared_shape`), and `FaultReport`/`ContractViolation` are
duplicated per kind (see `surface-fault-report`). The tax scales with how much
vocabulary a kind touches, and each parity test only catches drift on the fields
somebody remembered to pin. chrome is the worst case ahead: theme, layout,
`surface-state`, the toast and panel bodies.

The mechanism this reaches for already exists and is unexplored for this crate:
`brenn-envelope` is built twice, once per crate universe, as
`//brenn-envelope:brenn-envelope` and `//brenn-envelope:brenn-envelope_wasm`,
with a `brenn-wasm/components/host-crates/` symlink so the guest workspace can
resolve it as a path dependency. `surface/schema`'s dependency set (envelope,
chrono, serde, serde_json, uuid) is guest-shaped. What is unresolved, and is why
this is not a mechanical change: adding it makes the guest workspace resolve a
new first-party crate and repins the guest hub, and the alternative — carving a
small wire-only crate out of `surface/schema` rather than twinning the whole of
it — is a different cut of the same seam.

The gate this entry set — decide it before meeting and chrome migrate — was
crossed unresolved: both kinds migrated with their own re-spellings. The bill is
now five kinds, roughly fifteen wire shapes and ten host-side parity tests, and
`surface/chrome/src/wire.rs` alone is 278 lines of it held by six of those
tests.

Code sites (`TODO(surface-guest-wire-crate)`):
`surface/components/mode-clock/src/logic.rs`, above the re-spelled control-plane
vocabulary; `surface/components/meeting/src/logic.rs`, above its takeover
vocabulary; and `surface/chrome/src/wire.rs`, at the head of the module.

Done = a page-hosted kind imports the shared wire shapes, and the re-spellings
and their parity tests are deleted.

## `surface-guest-mount-idiom`

Every page-hosted UI kind hand-copies the same ~25 lines of mount bookkeeping:
an `Option<View>` field, a `view()` accessor whose `expect` string is
byte-identical in four crates, and a mount arm in the activation handler that
builds the view. The design's promise is that a UI kind is a `Processor` impl;
what a kind actually reproduces from an example is a `Processor` impl plus that
idiom, and an out-of-tree author gets no help with it from the SDK.

Two shapes, and choosing between them is the work: a `dom::Mounted<V>` cell in
the guest SDK (a data type; every kind keeps its own mount arm), or a
`Processor::mount()` trait method that `export_processor!` dispatches to when
the activation names `dom::MOUNT`, leaving `receive` to see only deliveries and
gestures. The second is the better shape and is a change to the guest SDK's
`Processor` trait — the first-class out-of-tree extension surface — so it wants
a design cycle rather than a drive-by: it moves what a component author must
implement, and neither the SDK nor the kernel can name the instance or the kind
in the panic message today, which is the other half of what makes the copied
`expect` unhelpful.

Code sites (`TODO(surface-guest-mount-idiom)`):
`brenn-wasm/components/guest/src/lib.rs`, at `dom::MOUNT`.

Done = one home for the mount lifecycle in the SDK, and the five kinds' copies
of the `Option<View>`/`expect`/mount-arm idiom deleted.

---

## `surface-envelope-json-memo`

A served window is retained context followed by what is new, so encoding
envelopes at window assembly re-encodes the retained prefix on every activation,
and re-encodes the same envelope once per subscribing instance. The envelope was
itself decoded from JSON at frame parse and that wire text was discarded. The
steady-state cost per envelope is one decode plus (retain_depth × activations ×
subscribers) encodes, on the browser's delivery hot path — work that grows with
the product of retain depth and message rate rather than with new messages. It
is invisible at today's depths and shows up as main-thread jank exactly when
someone widens a retain window to survive a longer outage, which is the sizing
knob the message-bus doc tells them to reach for.

The fix is memoisation: keep the JSON beside the envelope in the channel store
entry (an `Rc<str>` computed at insert, or the wire text the frame already
carried), and clone it at window assembly. That changes `attach/client`'s store
entry and `ServedWindow`, which is vocabulary shared with the non-surface attach
client and which the envelope-lowering design deliberately left holding decoded
envelopes — so it wants a design cycle, not a drive-by.

Code site (`TODO(surface-envelope-json-memo)`):
`surface/kernel/src/activation.rs`, at `window_ports`.

Done = an envelope is encoded once per page lifetime, and a wider retain depth
costs no extra encoding.

---

## `surface-instance-counter-column-action`

`InstanceCounters` has three columns and is documented as a struct that admits
new ones on a stated rule, but the kernel action that reaches the counters names
its column in the variant: `KernelAction::CountActivationFailure { instance }`,
executed as `bump_instance(instance, |c| &mut c.activation_failures, 1)`. The
executor re-supplies a field selector the action could have carried. Every
further column decided host-side therefore costs a new variant, a new executor
arm, and an appended action in every pinned effect vector in the kernel's logic
and session tests — while the generic form,
`CountInstance { instance, column: InstanceColumn }` with one enum-to-selector
match in the executor, costs that match once and nothing per column after.

Deferred rather than done in place because the action's name and its executor
line are what the counter's design specifies, and there is no second host-side
column pending: the generic shape is worth deciding with the column that needs
it, not speculatively ahead of it.

Code sites (`TODO(surface-instance-counter-column-action)`):
`surface/kernel/src/logic.rs`, at `KernelAction::CountActivationFailure`.

Done = one counting action carrying its column, one executor arm, and a new
per-instance column reachable from the kernel core without a new variant.

## `model-picker-lock-heuristic`

The input bar locks the model picker whenever the server offers exactly one
model (`renderModelPicker` in `frontend/src/components/input-bar.ts`), reading
"one offered" as "one allowed". Those are the same thing only when the offered
list was narrowed by the app's allow-list.

They come apart when the model cache is stale: with two models allowed and only
one of them in the cache, the client is offered one entry, locks the picker, and
labels it with the model actually in effect — which may be the other one, absent
from the list, so the label shows a raw alias with no description. The user
cannot then select the allowed model the server did offer.

Reachable only until the app's next spawn refreshes the cache, and the server
already warns per spawn about allow-list entries CC did not report. What to do
instead is a UX question rather than a bug fix: whether a one-entry list should
stay lockable, become selectable (today `cycleModel` refuses lists shorter than
two), or hide the picker when the effective model is not among the offered ones.

Code site (`TODO(model-picker-lock-heuristic)`):
`frontend/src/components/input-bar.ts`, in `renderModelPicker()`.

Done = the picker is inert only when the model it shows is genuinely the only
one the user may select, and the mixed case has a stated behavior.

## `per-app-model-preference`

The browser's model preference is a single origin-wide key —
`preferredModel` in `LocalSettings` (`frontend/src/settings.ts`), persisted
under `brenn-settings`. Apps are all served from `/app/<slug>` on that one
origin, so the key is shared across every app the user opens.

That was harmless while every app offered the same CC-reported model list. A
per-app allow-list breaks the assumption: opening an app whose `models` list
excludes the stored preference clears the preference — correctly, for that app
— and thereby un-prefers the model in every other app too. The more apps adopt
`models`, the more routine the collision.

The shape of the fix is a per-app map, `preferredModels: Record<string, string
| null>`, indexed by the app slug the component already holds. What needs
deciding first is what happens to the single stored key on the first load
after the change (drop it, or seed every app from it) and whether a preference
is really per-app rather than per-app-per-user, which is a product question
about how the picker is meant to feel.

Code site (`TODO(per-app-model-preference)`):
`frontend/src/components/app.ts`, in `resolveCurrentModel()`.

Done = the preference is stored and cleared per app, and a clear in one app
provably leaves another app's preference intact.
