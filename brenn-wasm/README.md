# brenn-wasm

Host-side wasmtime integration for WASM components in the Brenn workspace.

Style note: concise, precise, no padding. Audience: smart human/LLM.

## Toolchain

Nothing to install: `wit-bindgen` and `wasm-tools` are hermetic archives the
build fetches. Their pins are `WIT_BINDGEN_VERSION` and `WASM_TOOLS_VERSION` in
`MODULE.bazel`, which is also where the download URLs and checksums are, so a
bump is one edit there.

These two pins are the guest build path; the host runtime is wasmtime 47 (the
`brenn-wasm` dep). cargo-component is retired — not installed, not invoked.

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

1. Point `MODULE.bazel`'s `WIT_BINDGEN_VERSION` and `WASM_TOOLS_VERSION` back at
   the **previous** pair, with their checksums, on a scratch branch.
2. Build one component with that pair — `components/replay` is the usual choice:
   ```sh
   bazel build //brenn-wasm/components/replay:component
   ```
3. Restore the new pins, and run the engine suites against the artifact the
   previous generator produced by staging it over the fixture the suites read.
4. Discard the scratch branch and confirm `git status` is clean.

A failure here means the bump breaks components already in the field, which is a
release blocker, not a test to relax.

## Building

```sh
bazel build //brenn-wasm/...   # the component artifacts and the host crate
make build                     # the whole graph
make check                     # the gate, host tests included
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
