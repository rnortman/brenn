//! The pre-bind panic path, in its own integration binary.
//!
//! `register_component` (and every other identity-bearing entry) reads the
//! module's bound instance and panics if nothing bound it yet. The in-`lib`
//! browser suite cannot exercise that path: its single `BOUND_INSTANCE`
//! thread-local is bound once for the whole binary, and a test that left the
//! module unbound would break every sibling. This binary owns its own
//! `BOUND_INSTANCE`, never binds, and asserts the fail-fast trap and its
//! message.
//!
//! wasm32-only, matching the crate's wasm-gated body; run under
//! wasm-bindgen-test via `make surface-wasm-test`.
#![cfg(target_arch = "wasm32")]

use brenn_surface_component_support::{ActivationError, register_component};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
#[should_panic(expected = "ran before brenn_bind_instance")]
fn register_component_before_bind_panics() {
    // No bind has run in this binary, so the module does not know which
    // instance it is. `register_component` resolves the bound instance first —
    // before touching the DOM — so it traps here rather than guessing a wrong
    // subject onto this instance's element tag and panic attribution.
    register_component(
        "prebind-kind",
        |_host| {},
        |_a, _p| Ok::<(), ActivationError>(()),
    );
}
