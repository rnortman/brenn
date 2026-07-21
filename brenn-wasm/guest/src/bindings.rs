// WIT bindings for the `brenn:processor` world, owned by brenn-guest.
//
// Using the processor.wit file directly (not the directory) to avoid coupling
// to replay.wit (package brenn:replay), which lives in the same wit/ directory
// but is a separate world. This matches `cargo component`'s
// `target = { path = "../../wit/processor.wit" }` convention.
//
// `pub_export_macro = true` re-exports `export!` so downstream components can
// write `brenn_guest::export_processor!(MyType)` without owning any bindings.
// `default_bindings_module = "brenn_guest::bindings"` makes the macro resolve
// trait paths through our re-export (the established wasi-crate / spin-sdk
// SDK pattern).
//
// The `component-type` custom section emitted by `generate!` travels through
// the rlib into the final cdylib, so componentization works from a dependency.
wit_bindgen::generate!({
    world: "processor",
    path: "../wit/processor.wit",
    pub_export_macro: true,
    default_bindings_module: "brenn_guest::bindings",
});
