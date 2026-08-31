# Changelog

All notable changes to Brenn are documented here.

## [0.18.0] — 2026-08-31

The story here is that a component now ships as a **package** — the artifact,
the specification its author wrote, and a record binding the two — and the host
re-computes that binding at boot and refuses to start when anything disagrees.
Around that, the processor/DOM split is gone: one component vocabulary, one
artifact shape, and page access is a capability rather than an ABI. Two new
command-line flags tell the server where the installed modules and packages
live, so the same document checks on a workstation and boots on a host. And the
TOML config front end is gone, which makes the config DSL the only notation.

Nearly everything below is breaking. Budget a config edit and a unit-file edit
for this upgrade, not a binary swap. `docs/config-dsl.md` is the new prose
reference for the configuration language, and `docs/component-packages.md` the
normative contract for out-of-tree component authors.

### Added

- **Component packages.** A backend WASM component is no longer a bare `.wasm`.
  It is a directory named for the package, holding `package.json` (record v2),
  the artifact, and — for a processor-world component — a verbatim copy of the
  author's `<name>.brenn` specification. Before loading an instance the host
  resolves `<components root>/<package name>/`, re-hashes the artifact and the
  packaged specification against the record, and compares the packaged
  specification's hash to the hash of the specification the *configuration*
  compiled against. Byte-identical or refuse. Every failure is a panic naming
  the path and the remedy.
  **Operator action:** install one package directory per component and point
  the server at their parent with the new `serve --components <DIR>`. A host
  started without it panics naming the flag as soon as a configuration loads a
  component.
- **`brenn --modules <DIR>` and packaged-module imports.** A deployment no
  longer restates a component's specification; it imports the author's file:
  `use @processor-demo::*;`, resolved as `<module root>/processor-demo.brenn`.
  The root is an environment fact, so it is named on the command line and never
  in the document — the same document checks against a source checkout on a
  workstation and boots against the installed tree on a host. `--modules` is a
  global flag and must precede the subcommand; a document with an `@` import and
  no `--modules` is refused naming the flag.
  **Operator action:** the release now stages a flat `modules/` tree beside the
  packages; pass it as `--modules`. Surface kinds are imported this way too, so
  a deployment needs the module root whether or not it runs backend components.
- **Page capabilities and the sync channel.** `dom` gives a component a handle
  table over its own subtree and nothing else; `page-dom` — held by exactly one
  instance per surface, its chrome — is the separate authority to reach outside
  it. The element and attribute vocabularies are fixed allow-lists, admitting
  nothing that can navigate, fetch, or execute; `docs/security-posture.md`
  carries the admission rule. Alongside them, an activation may now carry a
  **sync** port: a live request the component answers in the same turn, which is
  how a DOM event listener gets to cancel or proceed in band.

### Changed

- **BREAKING: one component vocabulary at both placements.** The
  processor/DOM component split is gone. `abi = dom;` no longer exists —
  `abi = processor;` is the one artifact shape both hosts load, and where an
  instance runs is decided by where it is placed and what it is granted. What
  this costs a configuration, concretely:
  - Every component instance now carries a required `grants` list, at both
    placements, in one vocabulary. A surface-placed component that renders needs
    `dom`; a chrome needs `page-dom` as well. `takeover` moved off the `surface`
    block and onto the instance that requests the overlay.
  - Every class writes `requires` (a component that needs nothing writes
    `requires = [];`). Spec fit is checked in both directions: a required word
    the instance was not granted is refused, and so is a granted word the class
    never asked for.
  - Ports are required unless the class marks them `optional`. An unbound port
    is no longer legal by default.
  - `component_path` is gone from the vocabulary entirely; a document that still
    states it is refused as an unknown key. The package name is the whole
    reference.
  - A webhook's `replay_protection` names its guard by installed package:
    `component = "replay-generic";` in place of the old artifact path.
- **BREAKING: the release tree changed shape.** Components moved out of `lib/`:
  the deploy manifest is now `components/deployed-components.txt` and each entry
  is a `components/<name>/` package directory. New alongside it are `modules/`
  (the module root) and `scripts/manifest_names.sh` (the manifest grammar, which
  a deploying repo's preflight execs rather than transcribing). Deploy tooling
  that copied `lib/*.wasm` will not find them.
- **BREAKING: surface components ship as jco-transpiled directories.** The
  wasm-bindgen `brenn_<kind>.js` / `_bg.wasm` quadruple is replaced by
  `surface/processor/<kind>/`, carrying a v2 `manifest.json` with the kind's
  packaged specification and its hash. Boot re-derives every stated filename,
  re-hashes each, and binds per instance against the specification the
  configuration compiled against. **Operator action:** install the surface asset
  tree wholesale rather than overlaying it — a file from a prior release
  surviving beside a fresh record is a boot refusal. The same holds for the
  module root and the components root: sync them, never overlay.
- **BREAKING for component authors: `receive` changed shape.** It now returns
  `result<option<string>, receive-error>`; `activation` gains a `sync` field
  naming the live port when the activation is a sync call. `ok(none)` on every
  ordinary activation, `ok(some(reply))` only on a sync call, and a reply to a
  cause that asked nothing is a trap. The `processor` world also imports the new
  `dom` and `page-dom` interfaces. There is no compatibility shim: a component
  built against the 0.17 world does not load.

### Removed

- **BREAKING:** TOML config is no longer loadable. `--config` accepts only a
  `.brenn` document; any other extension is a startup panic, and the no-`--config`
  fallback probes `brenn.brenn` only. A `brenn.toml` beside it is ignored.
  Convert the file before upgrading — `brenn config-check <file>.brenn` validates
  the result, and `brenn config-diff` compares two documents as configurations.

## [0.17.0] — 2026-08-22

### Changed

- A new config DSL replaces TOML config

## [0.16.4] — 2026-08-14

### Fixed

- Containers were not properly shut down and could sometimes be orphaned.

### Changed

- More Bazel/CI speed optimizations

## [0.16.3] — 2026-08-11

### Changed

- Bazel/CI speed optimizations

## [0.16.2] — 2026-08-10

### Changed

- Bazel is now the build system.

## [0.16.1] — 2026-08-07

### Fixed

- Help channel text (channels published by components to teach LLMs how to use
  them) had documentation rot. Now help text is generated within and from the
  code itself to help prevent this.

## [0.16.0] — 2026-08-02

The main stories here are: We are preparing to bring voice assistants in (by
extending the message bus to remotes over a generic websocket protocol, and
putting LLM conversations on the bus), and we refactored a lot of code in the
pixel-surface and the websocket, which fixed various bugs that had never been
logged as bugs and also just made the code much better and easier to maintain.

### Added

- **Remote attachers.** A non-browser process can attach to the message bus over
  the same websocket the browser uses: new `[[remote]]` config block and
  `/remote/<slug>/ws` route, file-backed bearer token (mode-checked `0600`;
  unreadable or empty is a boot refusal), authority re-derived per frame from the
  config's channel matchers. No mTLS.
- **Per-app conversation roster** — `brenn:<prefix>.app.<app-slug>.roster`, a
  full snapshot of an app's conversation ids with a single reserved writer
  (`docs/chat-protocol.md` §9). Completes the chat protocol that shipped
  undocumented in 0.15.0, below.
- **`make wasm-toolchain-install`.** Guest toolchain pins now live only in the
  `Makefile`; preflights, CI, and the check-wit gate read them from there.

### Changed

- **The browser websocket protocol is now a pure message-bus extension** — it
  knows nothing about DOM, pixels, or components, and everything
  surface-specific rides as ordinary messages on ordinary channels.
  **Operator action:** every surface needs a `[[channel]]` block for its bindings
  document (`ephemeral:surface.surface.<slug>.bindings`, both depths `1`) or the
  surface will not come up. Delivery is now single-target per (attachment,
  channel); fan-out to instances and ports happens in the attacher.
- **Guest WebAssembly proposals are a closed allow-list** rather than whatever
  the wasmtime release defaults to. Notably relaxed-simd is no longer accepted —
  its results are implementation-dependent, which a byte-identical transcript
  cannot survive. Out-of-envelope guests are refused at compile time with the
  proposal named. Ships with wasmtime 47 (from 45), clearing RUSTSEC-2026-0222.
- **Gesture handlers run inside an activation,** so their publishes get the same
  flush boundary, cancelability, and offline parking as everything else, and
  every component gets a guaranteed activation at mount. Breaking for component
  authors: the free publish functions are gone in favor of `wire_gesture`,
  `PersistentTimer` is deleted (use a deferred self-publish on an in/out port),
  and `Activation` gains a `sync` field.
- **Latest-wins ports no longer fold.** More than one new message is an operator
  misconfiguration, so the port takes the latest and publishes an error rather
  than papering over it. **Operator action:** set `push_depth = 1` on every
  latest-wins binding. Event streams and accumulating ports are unaffected.
- `surface/proto` is now `surface/schema`, and the wire contract and its client
  moved to new `attach/proto` and `attach/client` crates. The attach protocol is
  at v3; both ends land together and there are no compatibility shims.

### Fixed

- **surface:** no more burst of `rejected layout doc` warnings on page load. A
  subscriber's catch-up replay was one frame per retained message, so each
  arrived in its own activation looking like the only new message; a catch-up run
  is now a single frame.
- **chat:** an app's lazily minted singleton conversation could exist without its
  chat channels and go unnamed on the app's roster.
- **wasm:** the guest yield fix for a component livelock had shipped on a line
  that never executed.
- `make e2e` runs again, and no longer carries a hardcoded date that turned the
  tree red once it passed.

## [0.15.0] — 2026-07-30

> The chat-over-pub/sub entries below were added retroactively; the work shipped
> in 0.15.0 but went undocumented at the time.

### Added

- **Chat with a Brenn-hosted LLM over the message bus.** Any authorized peer can
  drive a conversation and read its record without a browser, keeping Brenn's
  conversation context and tool calling. Each app+conversation gets `in`
  (commands), `out` (the durable record), `stream` (tokens), and `wake`
  (pre-warm) under a configurable prefix; the conversation id sits last in the
  address so grants work at one-conversation, all-conversations, or whole-app
  grain without wildcards. Commands are `send`, `stop`, `set_model`, `compact` —
  no busy-gate, and text sent mid-turn is injected at the end of the current
  tool-use round. There is no history API: the `out` retained window is the
  history. The stream is decoration, the durable record is truth. Raw text only,
  no HTML. Protocol `v1`, additive-only; `docs/chat-protocol.md` is the normative
  spec, written for a peer author who reads no Rust.
- **`[llm_chat]` config section** — `prefix`, `retained_window`, `wake_min`,
  `idle_timeout_secs`. A command published below `wake_min` parks instead of
  buying a subprocess. Malformed values are a boot refusal.
- **Impetus.** A bounded per-conversation pool that unattended turn-provoking
  injections draw from and human attention refills. Minting it requires a
  capability no configuration can grant, so an automation loop is bounded by a
  stock only a person can restore. A command arriving at an exhausted pool is
  refused whole.
- Auto and anonymous channels which come into being not with a separate spec and
  ACLs but because two component ports are connected to one another. ACLs are
  automatically granted and depth is automatically computed. If a name is given,
  it's a normal named channel; otherwise it's a private anonymous channel given
  only a UUID which is not discoverable by any means except to the ports
  connected to it.
- In/out ports, for a component that relies on listening to its own outputs
  (e.g. for timers/scheduling). This is one port that's both an input and an
  output and must be connected to the same channel. It automatically gets an
  anonymous local channel there if the operator doesn't connect something
  different.

### Changed

- **Behavior change for every deployed LLM app.** Authority over a conversation's
  chat channels moved from the app's own LLM to the server-side harness that
  wraps it. Previously each app was granted its whole chat tree, which put bus
  tools in every LLM prompt and let a conversation prompt itself. Bus identity is
  unchanged and there is no compatibility shim. A conversation is also no longer
  ambience-injected with its own messages.
- Unification of ephemeral/durable channels on the messaging substrate resulted
  in many configuration and default-behavior changes, and many bugs fixed (not
  individually enumerated here). The tldr is that every channel (other that auto
  channels) need explicit depth specifications in the config file. Nothing
  defaults to `unbounded` anymore, but the operator can still use that in a
  config file explicitly.

### Fixed

- **scrub:** `brenn-scrub tree` handles submodules. A tracked gitlink is now
  recognized from the index and scanned as the pointer text git records for it
  — the same text the staged and push scans see in a diff — with one stderr
  line naming the path and saying the submodule's own contents belong to
  another repository. Previously a repository carrying a submodule could not be
  swept at all: a checked-out submodule directory aborted the scan, and an
  uninitialized one was mislabeled a staged deletion and skipped.
- **scrub:** `brenn-scrub tree` refuses a repository with an unresolved merge
  instead of destroying the conflicted file. A path in conflict is three index
  entries, and the tree sweep processed all three: the second one copied the
  mirror's hardlink back onto the worktree file, truncating it to zero bytes,
  and the run then reported the empty result as a clean tree. Unmerged entries
  are now a hard refusal naming the path, and mirroring one path twice is
  refused outright.

## [0.14.3] — 2026-07-27

### Added

- Extensive refactoring of message bus substrate on the backend brings parity to
  all transports: both LLMs and WASM apps can now publish and subscribe with all
  features in parity across `brenn:`, `ephemeral:`, and `local:` channels. Many
  redundant internal code paths unified onto common traits.
- WASM apps gain the ability to query retained messages on inputs, set
  `deliver_after` on output messages, and view/edit/cancel deferred/pending
  output messages, giving them the ability to self-schedule timed events
  (replacing the need for timers, cron, and scheduling).

## [0.14.2] — 2026-07-22

### Added

- **surface:** the surface status document now reports overlay state — whether a
  fullscreen takeover is showing, which component holds it, and since when. A
  bar stuck fullscreen is now visible to health tooling instead of reporting
  `health: ok`.

### Fixed

- **surface:** the deskbar no longer wedges fullscreen when a meeting is
  rescheduled or replaced while inside its takeover window. Takeover requests and
  releases published from within a component activation are now stamped with the
  publishing identity, so a release is always attributable and the fullscreen
  overlay clears cleanly no matter when the replacement arrives.
- **surface:** the deskbar no longer logs spurious "dropped takeover release …
  does not hold the overlay" warnings at theme boundaries, on reconnects, and at
  other odd times. The chrome layer now processes only newly delivered
  control-plane messages instead of re-folding the retained last value on every
  screen update.
- **surface:** dismissing or snoozing a meeting now applies only to that specific
  occurrence. Previously a dismissal was keyed by meeting id alone and never
  aged out, so it silently suppressed every future meeting that reused the same
  id.
- **surface:** messages published to a `local:` channel prior to the consumer
  mounting the channel are now delivered as new instead of only existing as
  retained context.

## [0.14.1] — 2026-07-22

### Fixed

- **scrub:** hook mode resolves the repo — and its `.gitleaks.toml` — from the
  write destination rather than the session's working directory. A write into a
  different repo is now scanned against that repo's config, and a write to an
  ungated destination passes instead of being refused.
- **xtask:** `xtask check` no longer fails intermittently. Lanes that overlap
  the tree walk are now read-only, eliminating a readdir/stat race in which
  transient files written by one lane vanished while a sibling stat'd them.
- **xtask:** a failing `xtask check` lane now reports its own name and panic
  message instead of a generic "a scoped thread panicked".
- **xtask:** `xtask check` builds the WASM components it reads, so check-wit no
  longer aborts with "artifact not found" on a fresh tree.

### Internal

- brenn-cli's binary-spawning tests moved to integration tests, locating the
  binary through `CARGO_BIN_EXE_brenn-cli` instead of guessing a `target/debug`
  path that only existed after a prior build. Suite grew from 17 to 24 tests.
- New `git-fixture` dev crate runs git-touching tests in a scrubbed, hermetic
  environment, with a canary that detects fixture escape into the real repo and
  an xtask gate against unallowlisted raw git spawns.

## [0.14.0] — 2026-07-21

First public release.
