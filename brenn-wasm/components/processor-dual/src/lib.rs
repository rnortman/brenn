// Multi-port routing test fixture for the `brenn:processor` world.
//
// For each new message in any port-window, republishes its body unchanged —
// content type and bytes — to both "out1" and "out2" on separate calls.
// Designed to catch keying bugs where all publishes resolve to the first
// port's channel_address instead of the per-port binding.
//
// Behaviour:
//   - For each new message: publish(body) to "out1", then to "out2".
//   - A publish error returns Err(ProcessingFailed) with per-port diagnostic.
//   - Returns Ok on success.
//
// Special directive (a text body "__urgency__:<level>"): publishes a short marker
// string to "out1" via publish_with_urgency using the named urgency level.
// Used by the typed-urgency-publish integration test.
// Supported levels: very-low, low, normal, high.
//
// Does not import `store` (import-GC strips it; host links it regardless —
// exercising the subset-instantiation property).

use brenn_guest::{Activation, Body, Error, Processor, Urgency, publish, publish_with_urgency};

struct ProcessorDual;

impl Processor for ProcessorDual {
    fn receive(activation: Activation) -> Result<Option<Body>, Error> {
        for window in activation.port_windows() {
            for message in window.new_messages() {
                let body = message.body();
                // A body that is not text is never a directive.
                let directive = if body.is_text() {
                    body.as_text()?.trim().strip_prefix("__urgency__:")
                } else {
                    None
                };
                if let Some(level) = directive {
                    // Directive: publish a fixed marker to "out1" with the
                    // named urgency. Integration test asserts the urgency field.
                    let urgency = match level.trim() {
                        "very-low" => Urgency::VeryLow,
                        "low" => Urgency::Low,
                        "normal" => Urgency::Normal,
                        "high" => Urgency::High,
                        other => {
                            return Err(Error::failed(format!(
                                "unknown urgency level in directive: {other}"
                            )));
                        }
                    };
                    publish_with_urgency("out1", "urgency-marker", urgency)?;
                } else {
                    // Port-name is prepended in publish() diagnostics on error.
                    publish("out1", body)?;
                    publish("out2", body)?;
                }
            }
        }
        Ok(None)
    }
}

brenn_guest::export_processor!(ProcessorDual);
