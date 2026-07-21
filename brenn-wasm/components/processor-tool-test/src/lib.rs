// Async-tool end-to-end test fixture for the `brenn:processor` world
// (design §6 "End-to-end").
//
// This is the guest half of the cycle-2 consumption path, proven in cycle 1: a
// real compiled WASM consumer holding a `git-repo-pull` grant makes an async
// tool call and later acts on the result activation.
//
// Two activation shapes, distinguished by the input port an envelope arrives on:
//
//   - Any port OTHER than "tool-results" (the trigger): on a new envelope, fire
//     one `call-async("git-repo-pull", {"repos":["testclone"]}, "call-1")`. The
//     request is validated + buffered at call time and rides the activation's
//     transactional flush — it reaches the bus iff this `receive` returns Ok, so
//     a trap fires no tool call. The guest then returns Ok immediately (run to
//     completion); the result arrives later as a separate activation. If a
//     trigger envelope carries `TRAP_MARKER`, the guest instead aborts the
//     activation *after* buffering the call, so the trap-discard guarantee can
//     be observed: the buffered request must never reach the bus.
//
//   - The "tool-results" port (`TOOL_RESULT_INPUT_PORT`): the async result
//     inbox. Each new result envelope is republished verbatim to the "out" port
//     so the test can observe that the result activation reached and was
//     processed by the guest (the full guest-side loop, not just bus delivery).
//
// A host error on either call aborts the activation with a `ProcessingFailed`
// diagnostic so the test sees the failure rather than a silent no-op.

#[allow(dead_code, clippy::all)]
mod bindings;

use bindings::Guest;
use bindings::brenn::processor::ports::publish;
use bindings::brenn::processor::tools::call_async;
use bindings::brenn::processor::types::{Activation, ReceiveError};

/// Logical input port the async result inbox is delivered on
/// (`bus_wiring::TOOL_RESULT_INPUT_PORT`). Any other port is a trigger.
const TOOL_RESULTS_PORT: &str = "tool-results";
/// Output port bound to the channel the test reads.
const OUT_PORT: &str = "out";
/// The async tool this fixture calls, and its fixed args + correlation id. The
/// slug matches the fixture clone the test mounts.
const TOOL: &str = "git-repo-pull";
const ARGS: &str = r#"{"repos":["testclone"]}"#;
const CALL_ID: &str = "call-1";
/// A trigger envelope containing this marker makes the guest abort the activation
/// after buffering the async call, exercising the transactional trap-discard
/// guarantee (the buffered request must not reach the bus on a failed activation).
const TRAP_MARKER: &str = "TRAP_AFTER_CALL";

struct ProcessorToolTest;

impl Guest for ProcessorToolTest {
    fn receive(a: Activation) -> Result<(), ReceiveError> {
        for pw in &a.ports {
            let new_from = pw.new_from as usize;
            if new_from >= pw.envelopes.len() {
                // Pure-context / empty window — nothing new to act on.
                continue;
            }
            if pw.port == TOOL_RESULTS_PORT {
                // Result activation: forward each new result envelope verbatim to
                // "out" so the test observes the outcome the guest received.
                for env in &pw.envelopes[new_from..] {
                    publish(OUT_PORT, env).map_err(|e| {
                        ReceiveError::ProcessingFailed(format!("publish out: {e:?}"))
                    })?;
                }
            } else {
                // Trigger: fire the async tool call. Buffered now, flushed on Ok.
                call_async(TOOL, ARGS, CALL_ID)
                    .map_err(|e| ReceiveError::ProcessingFailed(format!("call-async: {e:?}")))?;
                // Trap-discard probe: a marked trigger aborts the activation after
                // the request is buffered. The transactional flush must then drop
                // the buffer (requests reach the bus iff `receive` returns Ok).
                if pw.envelopes[new_from..]
                    .iter()
                    .any(|env| env.contains(TRAP_MARKER))
                {
                    return Err(ReceiveError::ProcessingFailed(
                        "intentional trap after call-async".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

bindings::export!(ProcessorToolTest with_types_in bindings);
