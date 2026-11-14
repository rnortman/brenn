// WIT bindings for the `brenn:processor/processor@0.3.0` world, owned by
// brenn-guest: the 0.2.0 world with `blobs` imported. A component built on
// them is a 0.2.0 component, and imports `blobs` only if it calls it.
//
// The world is read over the three WIT files in package order: the 0.2.0
// package re-declares only `bus`, `ports` and `calls` and imports every other
// interface at `@0.1.0`, and the 0.3.0 package holds `blobs` alone. The files
// are named directly (not the directory) to avoid coupling to replay.wit
// (package brenn:replay), a separate world in the same wit/ directory.
//
// Parameters are borrowed (`Borrowing { duplicate_if_necessary: true }`), so a
// published body is lowered straight from the caller's slice and the only copy
// is the canonical ABI's.
//
// `pub_export_macro = true` re-exports `export!` so downstream components can
// write `brenn_guest::export_processor!(MyType)` without owning any bindings.
// `default_bindings_module = "brenn_guest::bindings"` makes the macro resolve
// trait paths through our re-export (the established wasi-crate / spin-sdk
// SDK pattern).
//
// The `component-type` custom section emitted by `generate!` travels through
// the rlib into the final cdylib, so componentization works from a dependency.
// The generated `receive` lowering takes one flattened argument per canonical-ABI
// field, which is more than the lint's threshold. Nothing here is hand-written.
#![allow(clippy::too_many_arguments)]

wit_bindgen::generate!({
    world: "brenn:processor/processor@0.3.0",
    path: ["../../wit/processor.wit", "../../wit/processor-0.2.wit", "../../wit/processor-0.3.wit"],
    generate_all,
    ownership: Borrowing { duplicate_if_necessary: true },
    pub_export_macro: true,
    default_bindings_module: "brenn_guest::bindings",
});
