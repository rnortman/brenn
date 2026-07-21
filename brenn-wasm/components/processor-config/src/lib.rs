// Test-only fixture for the brenn:processor config import.
//
// Directive-driven: when envelopes are present, the body is a JSON object with
// a "cmd" field.
//
//   {"cmd":"get","key":"test-key"}
//     → publishes the string value of the key, or "absent" if missing.
//
//   {"cmd":"get_parsed","key":"parsed-key"}
//     → attempts config::get_parsed::<u32>; publishes "ok:<n>" on success,
//       "absent" if the key is missing, or propagates Err on parse failure.
//
//   {"cmd":"require","key":"req-key"}
//     → attempts config::require::<String>; publishes "ok:<value>" on success
//       or propagates Err on missing/parse failure.
//
// Legacy path: if the activation has no port windows (empty activation), reads
// config key "test-key" and publishes the value (or "absent") — backward
// compatible with the original two integration tests.
//
// The "out" port must be bound by the host; an unbound port returns
// PublishError::NotPermitted (treated as a test-infrastructure bug — panic).

use brenn_guest::{Activation, Error, MessageEnvelopeExt, Processor, config, publish};

struct ProcessorConfig;

impl Processor for ProcessorConfig {
    fn receive(activation: Activation) -> Result<(), Error> {
        let windows: Vec<_> = activation.port_windows().collect();
        if windows.is_empty() {
            // Legacy path: no port windows → read "test-key" directly.
            let value = config::get("test-key").unwrap_or_else(|| "absent".to_string());
            publish("out", &value).unwrap_or_else(|e| {
                panic!("processor-config: publish failed: {e:?}")
            });
            return Ok(());
        }
        for window in &windows {
            for result in window.new_envelopes() {
                let env = result?;
                let directive: serde_json::Value = env.json_body()?;
                let cmd = directive["cmd"]
                    .as_str()
                    .ok_or_else(|| Error::malformed("directive missing 'cmd' field"))?;
                let key = directive["key"]
                    .as_str()
                    .ok_or_else(|| Error::malformed("directive missing 'key' field"))?;
                let payload = match cmd {
                    "get" => config::get(key).unwrap_or_else(|| "absent".to_string()),
                    "get_parsed" => match config::get_parsed::<u32>(key)? {
                        Some(n) => format!("ok:{n}"),
                        None => "absent".to_string(),
                    },
                    "require" => format!("ok:{}", config::require::<String>(key)?),
                    other => return Err(Error::malformed(format!("unknown cmd: {other}"))),
                };
                publish("out", &payload).unwrap_or_else(|e| {
                    panic!("processor-config: publish failed: {e:?}")
                });
            }
        }
        Ok(())
    }
}

brenn_guest::export_processor!(ProcessorConfig);
