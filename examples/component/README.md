# Example out-of-tree component

A `brenn:processor` component built in its own Bazel module with brenn's rules.
Copy this directory to start a component repository; the contract it builds
against is `docs/component-packages.md` in brenn, and the gates it runs are the
ones brenn's own components run.

```
bazel test //...
bazel run @brenn//brenn-dsl:dsl_cli -- check --modules spec config/dev.brenn
```

The first line builds the component, packages it, and runs the WASI-import,
grant-parity and deploy-manifest gates. The second is the author's fit check:
it compiles a root document that imports the packaged module and wires the
component. From brenn's root, `make example-check` runs both.

## What is the consumer's to state

bzlmod reads three things from the root module only, so a consumer repeats
them and brenn cannot supply them:

- **The Rust toolchain.** `rust.toolchain(...)` with brenn's `RUST_VERSION`
  and `extra_target_triples = ["wasm32-unknown-unknown"]`. brenn's crates
  compile under the consumer's toolchain; a version below brenn's floor fails
  at compile.
- **The `fltk` override.** `git_override(module_name = "fltk", commit = ...)`
  at brenn's commit. fltk is in no registry, and a dependency's `git_override`
  is ignored. Without it module resolution fails with "fltk not found".
- **The fltk serde flag** in `.bazelrc`:
  `--@fltk//crates/fltk-serde-core:serde=@brenn//brenn-dsl:fltk_serde`.
  Without it `@brenn//brenn-dsl:dsl_cli` fails to compile with
  mismatched-serde-instance errors.

And one naming rule: the crate hub is not called `crates` or `wasm_crates`;
rules_rust refuses two hubs of one name across modules.

## Layout

- `spec/example-caser.brenn` — the author-owned specification; the directory
  is also the module root the fit check and a deployment read.
- `src/lib.rs` — the guest. `src/spec.rs` is generated from the spec and is
  not tracked.
- `BUILD.bazel` — scaffold, core module, component, package, gates, and a
  components root (`:components`) shaped like a bundle's `components/`.
- `config/dev.brenn` — a root document wiring the component.
- `Cargo.toml` / `Cargo.lock` / `cargo-bazel-lock.json` — this workspace's
  own third-party crates. Serde is not among them: it comes from brenn's guest
  serde instance (`@brenn//brenn-wasm/components/guest:serde`), because a
  derive from any other instance satisfies none of brenn-guest's bounds.
