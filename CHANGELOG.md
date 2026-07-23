# Changelog

All notable changes to Brenn are documented here.

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
