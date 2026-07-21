// Multi-port activation summary fixture for the `brenn:processor` world.
//
// On each receive, publishes ONE summary message to port "out" — a JSON array
// of `{"port": <name>, "len": <total>, "new_from": <index>, "dropped": <count>,
// "context_count": <context_envelopes_count>}` objects in received (cfg.inputs)
// order — then returns Ok.
//
// Sentinels in new envelope bodies (checked before the summary publish):
//   "__trap__" — traps (unreachable!); no output produced.
//   "__err__"  — returns Err(ProcessingFailed); no output produced.
//
// This fixture makes activation count and multi-port window composition directly
// assertable from the output channel: one summary per activation, with per-port
// slot counts verifiable even when individual port windows are empty or pure-context.
//
// Does not import `store` (import-GC strips it; host links it regardless —
// exercising the subset-instantiation property).

use brenn_guest::{Activation, Error, Processor, publish, serde_json};

#[derive(serde::Serialize)]
struct PortSummary<'a> {
    port: &'a str,
    len: usize,
    new_from: usize,
    dropped: u32,
    /// Count of context envelopes as seen through `context_envelopes()`.
    /// Must equal `new_from`; a transposition in the slice index would flip this.
    context_count: usize,
}

struct ProcessorMultiport;

impl Processor for ProcessorMultiport {
    fn receive(activation: Activation) -> Result<(), Error> {
        // Check sentinels and collect summary entries.
        let windows: Vec<_> = activation.port_windows().collect();

        let mut summary_parts: Vec<PortSummary<'_>> = Vec::with_capacity(windows.len());
        for window in &windows {
            for env in window.new_envelopes() {
                let env = env?;
                if env.body == "__trap__" {
                    unreachable!(
                        "processor-multiport: deliberate trap on __trap__ sentinel"
                    );
                }
                if env.body == "__err__" {
                    return Err(Error::failed(
                        "processor-multiport: deliberate err on __err__ sentinel",
                    ));
                }
            }
            // Count context envelopes through `context_envelopes()` to exercise
            // the parsed iterator path (not just the raw slice). Must equal new_from;
            // a [..new_from] ↔ [new_from..] transposition would flip this vs new_raw.
            let context_count = window.context_envelopes().count();
            summary_parts.push(PortSummary {
                port: window.port(),
                len: window.new_raw().len() + window.context_raw().len(), // total envelopes
                // context_raw().len() == new_from by PortWindow invariant:
                // context is ..new_from, so its length equals the split index.
                new_from: window.context_raw().len(),
                dropped: window.dropped(),
                context_count,
            });
        }

        let json = serde_json::to_string(&summary_parts)
            .map_err(|e| Error::failed(format!("serialize summary: {e}")))?;
        publish("out", &json)?;
        Ok(())
    }
}

brenn_guest::export_processor!(ProcessorMultiport);
