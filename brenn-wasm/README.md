# brenn-wasm

Host-side wasmtime integration for WASM components in the Brenn workspace.

Style note: concise, precise, no padding. Audience: smart human/LLM.

## Developer setup

One-time per workstation (re-run on version bump), from the repo root:

```sh
make wasm-toolchain-install
```

That installs `wit-bindgen-cli` and `wasm-tools` at the pinned versions. The
pins live in exactly one place — `WIT_BINDGEN_CLI_VERSION` and
`WASM_TOOLS_VERSION` near the top of the repo-root `Makefile` — and every other
consumer derives them from there: the build-rule preflights and this install
target expand the variables, the public CI workflow and the private CD pipeline
grep those two lines, and `xtask check-wit` parses them to assert the generator
it shells out to. Bumping is one edit to the Makefile.

These two pins are the guest build path; the host runtime is wasmtime 47 (the
`brenn-wasm` dep). cargo-component is retired — not installed, not invoked.

`wasm32-unknown-unknown` is declared in the workspace `rust-toolchain.toml` and
pulled in automatically by rustup on first build.

## Toolchain bump procedure

Bumping wasmtime or the guest generator pair (`wit-bindgen-cli` / `wasm-tools`)
takes one manual step that no gate performs: a **previous-generator load check**.

Every WASM fixture in the tree is rebuilt by whatever pin is current, so the
suite only ever exercises new-host/new-guest. Out-of-tree components lag the pin
by definition, which makes new-host/previous-generator-guest the pairing that
actually ships in the field — and the one with no automated coverage. Automating
it is deliberately deferred (a committed golden `.wasm` breaks the
no-binaries/hermetic fixture posture; a second generator toolchain doubles the
pin-sync surface), and the risk is accepted while zero out-of-tree components
exist. Revisit at the first external pin, when the ABI story gets its pass.

Until then, on every bump:

1. Install the **previous** generator pair to a scratch root:
   ```sh
   cargo install --locked --root /tmp/prev-gen wit-bindgen-cli --version <previous>
   cargo install --locked --root /tmp/prev-gen wasm-tools --version <previous>
   ```
2. Rebuild one component (`components/replay` is the usual choice) with that pair
   on `PATH`. The build rules assert the installed tool versions against the
   Makefile pins, which are already the *new* ones at this point, so override both
   on the command line — command-line assignments beat the Makefile's:
   ```sh
   PATH=/tmp/prev-gen/bin:$PATH make -B \
       brenn-wasm/target/components/brenn_replay.wasm \
       WIT_BINDGEN_CLI_VERSION=<previous> WASM_TOOLS_VERSION=<previous>
   ```
   The rule writes straight to `target/components/brenn_replay.wasm`, so no manual
   swap; it also regenerates `components/replay/src/bindings.rs` in place with the
   previous generator.
3. Run the engine suites against it:
   ```sh
   cargo test -p brenn-wasm --test replay_engine --test replay_engine_bounds
   ```
4. Restore the tree's own artifact and bindings with `make wasm-components` (both
   are rebuilt from the current pins), then check `git status` is clean.

A failure here means the bump breaks components already in the field, which is a
release blocker, not a test to relax.

## Building

```sh
make wasm-components   # build demonstrator component artifact only
make build             # full build (includes wasm-components)
make test              # runs host tests (depends on wasm-components)
```

The WASM component source lives at `components/replay/` (non-workspace crate,
targets `wasm32-unknown-unknown`, wrapped into a component by `wasm-tools
component new`). The artifact is copied to `target/components/brenn_replay.wasm`
as a stable host-resolvable path.

## WIT

`wit/replay.wit` is the single source of truth for the `brenn:replay` WIT
world. Both host (via `wasmtime::component::bindgen!`) and guest (via
`wit-bindgen`) reference this file directly.

## Architecture note

wasmtime is a large dependency. It lives in `brenn-wasm` rather than
`brenn-lib` to avoid inflating every other crate's compile time and binary
size. Iter 3 adds `brenn-wasm` as a dep of the `brenn` binary crate; no other
workspace member gains the wasmtime dep.
