# Changelog

All notable changes to Brenn are documented here.

## Unreleased

### Added

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
