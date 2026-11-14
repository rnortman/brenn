// Test fixture built against a `brenn:processor@0.2.0` whose `bus.envelope`
// differs from the host's. It exists to be refused at load on the type; its
// `receive` is never run.

// The generated `receive` lowering takes one flattened argument per canonical-ABI
// field, which is more than the lint's threshold. Nothing there is hand-written.
#![allow(clippy::too_many_arguments)]

wit_bindgen::generate!({
    world: "brenn:processor/processor@0.2.0",
    path: "wit/stale-processor.wit",
});

struct Component;

impl Guest for Component {
    fn receive(_a: Activation) -> Result<Option<Body>, ReceiveError> {
        Ok(None)
    }
}

export!(Component);
