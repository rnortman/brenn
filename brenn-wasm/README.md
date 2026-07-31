# brenn-wasm

Host-side wasmtime integration for WASM components in the Brenn workspace.

Style note: concise, precise, no padding. Audience: smart human/LLM.

## Developer setup

One-time per workstation (re-run on version bump):

```sh
cargo install --locked wit-bindgen-cli --version 0.60.0
cargo install --locked wasm-tools --version 1.255.0
```

These two pins are the guest build path; the host runtime is wasmtime 47 (the
`brenn-wasm` dep). The same literals appear in the Makefile preflights and in
`.github/workflows/ci.yml`, and all of them move together on a bump.
cargo-component is retired — not installed, not invoked.

`wasm32-unknown-unknown` is declared in the workspace `rust-toolchain.toml` and
pulled in automatically by rustup on first build.

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
