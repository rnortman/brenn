//! A `brenn:processor` component built outside brenn's tree.
//!
//! Every new envelope on the `text` port is recased and republished on `cased`
//! as one JSON document. The shape an author copies is all here: the generated
//! `spec` module for the port surface, a serde-derived payload bound to its
//! output port once, a third-party crate from this workspace's own hub, and
//! the `Processor` impl the host calls.

mod spec;

use brenn_guest::{Activation, Error, OutPort, Processor};
use heck::{ToKebabCase, ToShoutySnakeCase, ToSnakeCase, ToUpperCamelCase};
use serde::Serialize;

/// What goes out on `cased`, one document per inbound body.
///
/// `Serialize` must derive from brenn's guest serde instance (the `serde` alias
/// in the BUILD file); a derive from any other instance satisfies none of
/// brenn-guest's port bounds.
#[derive(Serialize)]
struct Cased {
    original: String,
    kebab: String,
    snake: String,
    upper_camel: String,
    shouty_snake: String,
}

impl spec::CasedPayload for Cased {}

/// The typed handle for the output port; publishing anything but a `Cased`
/// through it is a compile error.
const CASED: OutPort<Cased> = spec::cased();

fn recase(original: &str) -> Cased {
    Cased {
        original: original.to_owned(),
        kebab: original.to_kebab_case(),
        snake: original.to_snake_case(),
        upper_camel: original.to_upper_camel_case(),
        shouty_snake: original.to_shouty_snake_case(),
    }
}

struct ExampleCaser;

impl Processor for ExampleCaser {
    fn receive(activation: Activation) -> Result<Option<String>, Error> {
        for window in activation.port_windows() {
            // The host binds only the ports the specification declares, so a
            // window on any other name is the host's error, not input.
            spec::InPort::of(&window)?;
            for envelope in window.new_envelopes() {
                let envelope = envelope?;
                let cased = recase(&envelope.body);
                spec::log::info(format!("recased {:?} as {}", envelope.body, cased.kebab));
                CASED.publish(&cased)?;
            }
        }
        Ok(None)
    }
}

brenn_guest::export_processor!(ExampleCaser);
