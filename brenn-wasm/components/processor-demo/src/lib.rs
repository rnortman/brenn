// Demo WASM component for the `brenn:processor` world.
//
// For each new envelope (activation.ports[*].envelopes[new_from..]):
//   - Parses the JSON; returns Err(MalformedEnvelope) on parse failure.
//   - Asserts the `channel` field is non-empty; returns Err(ProcessingFailed)
//     if not.
//   - If the body is exactly the sentinel `"__trap__"`, traps unconditionally
//     so the always-trap acceptance criterion is exercisable.
//   - If `envelope_type == Webhook`: parses `body` as a WebhookEnvelope,
//     then publishes the inner `body` on port "out".
//     A publish error returns Err(ProcessingFailed) with the error diagnostic.
//
// Does not import `store` (import-GC strips it; host links it regardless —
// exercising the subset-instantiation property).
//
// Returns Ok on success (all new entries processed).

use brenn_guest::{Activation, Error, MessageEnvelopeExt, Processor, publish};

struct ProcessorDemo;

impl Processor for ProcessorDemo {
    fn receive(activation: Activation) -> Result<(), Error> {
        for window in activation.port_windows() {
            for env in window.new_envelopes() {
                let env = env?;

                // Assert `channel` field is present and non-empty.
                if env.channel.is_empty() {
                    return Err(Error::failed(
                        "envelope missing non-empty 'channel' field",
                    ));
                }

                // Sentinel body triggers a deliberate trap — exercising the
                // always-trap acceptance criterion.
                if env.body == "__trap__" {
                    unreachable!(
                        "processor-demo: deliberate trap on sentinel body __trap__"
                    );
                }

                // If webhook envelope: extract inner body and publish on "out".
                if env.envelope_type == brenn_guest::ChannelScheme::Webhook {
                    let webhook = env.webhook_body()?;
                    publish("out", &webhook.body)?;
                }
            }
        }

        Ok(())
    }
}

brenn_guest::export_processor!(ProcessorDemo);
