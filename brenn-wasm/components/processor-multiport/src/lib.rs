// Multi-port activation summary fixture for the `brenn:processor` world.
//
// On each receive, publishes ONE summary message to port "out" — a JSON array
// of `{"port": <name>, "len": <total>, "new_from": <index>, "dropped": <count>,
// "context_count": <context_envelopes_count>}` objects in received (cfg.inputs)
// order — then returns Ok.
//
// Sentinels in new envelope bodies (checked before the summary publish):
//   "__trap__"  — traps (unreachable!); no output produced.
//   "__err__"   — returns Err(ProcessingFailed); no output produced.
//
// One sentinel is checked over *context* envelopes instead:
//   "__err_on_context__" — returns Err(ProcessingFailed). An activation that
//                 carries nothing new can still be dispositioned, which is what
//                 a mount activation over a port with no backlog looks like.
//   "__trap_on_context__" — traps (unreachable!). The same case on the other
//                 disposition arm: a host quarantines what an activation
//                 consumed, and a mount that consumed nothing has nothing to
//                 name.
//   "__reply__" — buffers one publish, then answers the activation with the
//                 same per-port summary an ordinary activation publishes. Only
//                 a sync-call activation may be answered, so on an async one a
//                 host that reads this ok flushes a buffer it should have
//                 discarded; on a sync one the summary is how a case reads back
//                 what the request's activation was windowed.
//   "__long_reply__" — answers with a reply far past any deployment's body cap,
//                 buffering nothing first. A reply is capped like a publish
//                 body, and this is the shape that reaches the cap with an
//                 empty buffer behind it, so the trap under test is the cap's
//                 and not a refused publish upstream of it.
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
    fn receive(activation: Activation) -> Result<Option<String>, Error> {
        // Check sentinels and collect summary entries.
        let windows: Vec<_> = activation.port_windows().collect();

        let mut summary_parts: Vec<PortSummary<'_>> = Vec::with_capacity(windows.len());
        let mut answering = false;
        for window in &windows {
            // Checked over context rather than new, so an activation carrying
            // nothing new can still fail: that is the only way to reach the
            // disposition of a mount activation over a port with no backlog.
            for env in window.context_envelopes() {
                let env = env?;
                if env.body == "__err_on_context__" {
                    return Err(Error::failed(
                        "processor-multiport: deliberate err on __err_on_context__ sentinel",
                    ));
                }
                if env.body == "__trap_on_context__" {
                    unreachable!(
                        "processor-multiport: deliberate trap on __trap_on_context__ sentinel"
                    );
                }
            }
            for env in window.new_envelopes() {
                let env = env?;
                if env.body == "__trap__" {
                    unreachable!("processor-multiport: deliberate trap on __trap__ sentinel");
                }
                if env.body == "__err__" {
                    return Err(Error::failed(
                        "processor-multiport: deliberate err on __err__ sentinel",
                    ));
                }
                if env.body == "__reply__" {
                    answering = true;
                }
                if env.body == "__long_reply__" {
                    return Ok(Some("x".repeat(4096)));
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
        if answering {
            // The buffer is deliberately non-empty beside the answer: on an
            // async activation that is what makes the host's discard observable
            // rather than vacuous, and on a sync one it is what makes
            // flush-before-reply observable.
            publish("out", "buffered-before-reply")?;
            return Ok(Some(json));
        }
        publish("out", &json)?;
        Ok(None)
    }
}

brenn_guest::export_processor!(ProcessorMultiport);
