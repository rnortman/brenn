// Memory-exhaustion test fixture for the `brenn:processor` world.
//
// This component attempts to allocate ~100 MiB in 1 MiB chunks using
// `Vec::try_reserve_exact` (safe, fallible, stable) on any non-empty new
// portion, exercising the `trap_on_grow_failure` path in
// `ProcessorComponent::handle`.
//
// Behaviour:
//   - Pure-context window (no new envelopes): return Ok(()).
//   - Any new envelope: allocate ~100 MiB in 1 MiB chunks. Each chunk is
//     touched and black_boxed to prevent elision. If any `try_reserve_exact`
//     returns Err (graceful-guest path), return Ok(()) — this arm is the load-
//     bearing discriminator: with `trap_on_grow_failure = true` the host traps
//     inside `memory.grow` before `try_reserve_exact` can return Err, so this
//     Ok branch is never reached and the host sees ProcessorOutcome::Trap.
//     With the flag off the allocator returns -1 → Err → Ok, masking the cap
//     excess — the test fails.

#[allow(dead_code, clippy::all)]
mod bindings;

use bindings::brenn::processor::types::{Activation, ReceiveError};
use bindings::Guest;

struct ProcessorMemExhaust;

const CHUNK_BYTES: usize = 1024 * 1024; // 1 MiB per chunk
const TARGET_BYTES: usize = 100 * 1024 * 1024; // ~100 MiB total

/// Allocate up to `TARGET_BYTES` in `CHUNK_BYTES` chunks via `try_reserve_exact`.
///
/// Each chunk is touched with a write and black_boxed to prevent the compiler
/// from eliding the allocation. Chunks are accumulated in an outer Vec so the
/// allocator cannot reclaim them during the loop.
///
/// With `trap_on_grow_failure = true` this function never returns: the host traps
/// inside `memory.grow` before `try_reserve_exact` can return `Err`. With the flag
/// off, `try_reserve_exact` returns `Err` on cap excess and the function returns
/// normally — that is the regression-detection arm (the test fails because
/// `ProcessorOutcome::Ok` is observed instead of `Trap`).
#[inline(never)]
fn try_exhaust_memory() {
    let num_chunks = TARGET_BYTES / CHUNK_BYTES;
    // Hold all chunks alive so the allocator cannot reuse them.
    let mut held: Vec<Vec<u8>> = Vec::new();
    for _ in 0..num_chunks {
        let mut chunk: Vec<u8> = Vec::new();
        if chunk.try_reserve_exact(CHUNK_BYTES).is_err() {
            // Graceful-guest arm: with trap_on_grow_failure=true this is
            // unreachable (host traps before try_reserve_exact returns).
            return;
        }
        // Touch the allocation and pin it so neither the chunk nor the push
        // can be optimised out.
        chunk.push(std::hint::black_box(1u8));
        held.push(std::hint::black_box(chunk));
    }
    // Prevent the held Vec itself from being dropped before all iterations.
    std::hint::black_box(&held);
}

impl Guest for ProcessorMemExhaust {
    fn receive(a: Activation) -> Result<(), ReceiveError> {
        let has_new = a
            .ports
            .iter()
            .any(|pw| (pw.new_from as usize) < pw.envelopes.len());

        if has_new {
            // With trap_on_grow_failure=true: host traps inside memory.grow before
            // try_exhaust_memory returns. With the flag off: function returns normally
            // and we fall through to Ok(()), which the test detects as a regression.
            try_exhaust_memory();
        }

        Ok(())
    }
}

bindings::export!(ProcessorMemExhaust with_types_in bindings);
