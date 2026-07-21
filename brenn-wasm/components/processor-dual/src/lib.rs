// Multi-port routing test fixture for the `brenn:processor` world.
//
// For each new envelope in any port-window, publishes the raw envelope JSON
// to both "out1" and "out2" on separate calls. Designed to catch keying bugs
// where all publishes resolve to the first port's channel_address instead of
// the per-port binding.
//
// Behaviour:
//   - For each new envelope: publish(raw_json) to "out1", then to "out2".
//   - A publish error returns Err(ProcessingFailed) with per-port diagnostic.
//   - Returns Ok on success.
//
// Special directive (body == "__urgency__:<level>"): publishes a short marker
// string to "out1" via publish_with_urgency using the named urgency level.
// Used by the typed-urgency-publish integration test (design §4).
// Supported levels: very-low, low, normal, high.
//
// Does not import `store` (import-GC strips it; host links it regardless —
// exercising the subset-instantiation property).

use brenn_guest::{Activation, Error, Processor, Urgency, publish, publish_with_urgency};

struct ProcessorDual;

impl Processor for ProcessorDual {
    fn receive(activation: Activation) -> Result<(), Error> {
        for window in activation.port_windows() {
            // Zip parsed envelopes (for directive inspection) with their raw
            // JSON strings (for raw-passthrough publish in the default case).
            for (env_result, raw) in window.new_envelopes().zip(window.new_raw()) {
                let env = env_result?;
                let body = env.body.trim();
                if let Some(level) = body.strip_prefix("__urgency__:") {
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
                    // Default: publish the raw envelope JSON to both output ports.
                    // Port-name is prepended in publish() diagnostics on error.
                    publish("out1", raw)?;
                    publish("out2", raw)?;
                }
            }
        }
        Ok(())
    }
}

brenn_guest::export_processor!(ProcessorDual);
