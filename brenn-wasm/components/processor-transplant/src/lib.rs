// The transplant fixture: one artifact, two hostings, one transcript.
//
// Every observable this fixture produces is a function of the activation it was
// handed plus its own config map, so a host that implements the contract
// correctly produces byte-identical transcripts under wasmtime and in a
// browser. Nothing here is host-aware.
//
// Per activation, in order:
//   1. Read config keys "greeting" (present) and "absent" (missing).
//   2. Log one info line.
//   3. Publish a summary of every port window to "out".
//   4. Publish one marker per new envelope body to "out".
//   5. Honour the sentinels below.
//
// Sentinels, matched against a new envelope's body:
//   "__err__"  — return Err after the publishes above have been buffered.
//                A conforming host discards the whole buffer: the transcript
//                shows an err activation with no flushed publishes.
//   "__trap__" — trap after the same publishes. Same discard, plus the
//                instance is terminal afterwards.
//
// Deliberately stateless across activations. The two hostings genuinely differ
// here — the wasmtime host builds a fresh store per invocation, a browser
// instance's linear memory lives as long as the instance — and neither the
// contract nor `processor.wit` promises either behaviour. A fixture that
// carried a counter would pin that divergence into the transcript and fail for
// a reason the contract never claimed.

use brenn_guest::{Activation, Error, Processor, config, log, publish};

#[derive(serde::Serialize)]
struct PortSummary<'a> {
    port: &'a str,
    /// `message_id` of every envelope in the window, context first — the
    /// window's identity, independent of body encoding.
    ids: Vec<String>,
    new_from: usize,
    dropped: u32,
}

#[derive(serde::Serialize)]
struct ActivationSummary<'a> {
    ports: Vec<PortSummary<'a>>,
    /// Present key, then a deliberately absent one — `null` distinguishes
    /// "no such key" from "empty value" in the transcript.
    greeting: Option<String>,
    absent: Option<String>,
}

struct ProcessorTransplant;

impl Processor for ProcessorTransplant {
    fn receive(activation: Activation) -> Result<(), Error> {
        let windows: Vec<_> = activation.port_windows().collect();

        let mut ports = Vec::with_capacity(windows.len());
        let mut markers: Vec<String> = Vec::new();
        let mut sentinel: Option<&'static str> = None;

        for window in &windows {
            let mut ids = Vec::new();
            for result in window.context_envelopes() {
                ids.push(result?.message_id.to_string());
            }
            let new_from = ids.len();
            for result in window.new_envelopes() {
                let env = result?;
                ids.push(env.message_id.to_string());
                match env.body.as_str() {
                    "__err__" => sentinel = Some("__err__"),
                    "__trap__" => sentinel = Some("__trap__"),
                    body => markers.push(format!("{}:{}", window.port(), body)),
                }
            }
            ports.push(PortSummary {
                port: window.port(),
                ids,
                new_from,
                dropped: window.dropped(),
            });
        }

        let summary = ActivationSummary {
            ports,
            greeting: config::get("greeting"),
            absent: config::get("absent"),
        };

        log::info("transplant activation");

        publish(
            "out",
            &serde_json::to_string(&summary)
                .map_err(|e| Error::failed(format!("serialize summary: {e}")))?,
        )?;
        for marker in &markers {
            publish("out", marker)?;
        }

        match sentinel {
            Some("__err__") => Err(Error::failed("transplant: deliberate err sentinel")),
            Some("__trap__") => unreachable!("transplant: deliberate trap sentinel"),
            _ => Ok(()),
        }
    }
}

brenn_guest::export_processor!(ProcessorTransplant);
