// Fault-injection test component for brenn-wasm integration tests.
//
// Sentinel ops keyed on x-brenn-fault-test header:
//   LEAK_TX           — begin a transaction and return without commit/rollback.
//                       Host's HostTransaction::drop detects the leak and traps.
//   TRAP              — trigger a wasm unreachable trap. Host sees a runtime error.
//   SPIN              — infinite loop over a volatile counter (optimizer cannot elide).
//                       Drives the fuel / epoch-deadline traps (H1046, design §4).
//   GROW              — repeated large allocation in a loop. Drives the memory.grow cap
//                       trap (H1046, design §4).
//   LEAK_TX_THEN_TRAP — begin a transaction, then SPIN forever inside it. Combined with a
//                       short epoch deadline this traps mid-transaction, exercising the
//                       host's leaked-tx rollback on a resource-exhaustion trap cause
//                       (H1046, design §2.5.1 / §4).
//
// Any other header value or absent header → Ok(()) (no-op).
// Not for production use.
//
// No WASI panic handler is used. The `trap()` function calls
// core::arch::wasm32::unreachable() directly, producing a wasm trap without
// going through the std panic-stderr path. Built with --target
// wasm32-unknown-unknown so the host linker (which has no WASI provider)
// can instantiate the component without import errors.

use bindings::brenn::replay::store;
use bindings::brenn::replay::types::{CheckInput, ReplayError};
use bindings::Guest;

#[allow(dead_code, clippy::all)]
mod bindings;

struct Component;

impl Guest for Component {
    fn check(input: CheckInput) -> Result<(), ReplayError> {
        let op = input
            .headers
            .iter()
            .find(|h| h.name == "x-brenn-fault-test")
            .map(|h| h.value.as_str())
            .unwrap_or("");

        match op {
            "LEAK_TX" => {
                // Begin a transaction and return without commit/rollback.
                // Host's HostTransaction::drop will detect the leak and return Err (trap).
                let _tx = store::begin()
                    .map_err(|e| ReplayError::MalformedInput(format!("begin failed: {e}")))?;
                Ok(())
            }
            "TRAP" => {
                // Trigger a wasm unreachable — host sees a runtime trap.
                // SAFETY: wasm32::unreachable() is the canonical "trap" instruction.
                // We're in wasm32 context by the time this runs (the component is
                // only loaded by the test harness when targeting wasm32-unknown-unknown).
                trap()
            }
            "SPIN" => {
                // Infinite loop. Consumes fuel forever and never yields, so the host's
                // fuel cap (or, with high fuel, the epoch deadline) fires and traps.
                spin()
            }
            "GROW" => {
                // Repeated large allocation. Drives the store memory cap; with
                // trap_on_grow_failure(true) the host traps inside memory.grow.
                grow()
            }
            "LEAK_TX_THEN_TRAP" => {
                // Begin a transaction, then spin forever inside it. With a short epoch
                // deadline the guest traps mid-transaction; the WIT drop destructor never
                // runs, so the host's leaked-tx cleanup must roll back.
                let _tx = store::begin()
                    .map_err(|e| ReplayError::MalformedInput(format!("begin failed: {e}")))?;
                spin()
            }
            _ => Ok(()),
        }
    }
}

// Infinite loop over a volatile counter so the optimizer cannot elide it or prove
// divergence and replace the body with `unreachable`. Never returns.
#[inline(never)]
fn spin() -> ! {
    let mut counter: u64 = 0;
    loop {
        // SAFETY: reading/writing a stack local through a volatile pointer is sound;
        // the volatile access forces the loop body to be emitted.
        unsafe {
            let p = &mut counter as *mut u64;
            let v = core::ptr::read_volatile(p);
            core::ptr::write_volatile(p, v.wrapping_add(1));
        }
    }
}

// Allocate ever-larger buffers until the host's memory cap forces a trap inside
// memory.grow (trap_on_grow_failure). Each Vec is touched (written) so the pages
// are actually committed, and leaked via mem::forget so they are not freed between
// iterations. Returns Ok only if the host did not trap (should be unreachable under
// the production cap), in which case the test would fail loudly.
#[inline(never)]
fn grow() -> Result<(), ReplayError> {
    // 1 MiB chunks; ~17 iterations crosses the 16 MiB cap.
    for _ in 0..64 {
        let mut chunk: Vec<u8> = Vec::with_capacity(1024 * 1024);
        // Touch the allocation so the pages are committed (forces memory.grow).
        chunk.resize(1024 * 1024, 0xAB);
        // Read one byte back through a volatile load so the writes are not elided.
        let last = chunk.len() - 1;
        // SAFETY: `last` is in bounds.
        let _ = unsafe { core::ptr::read_volatile(chunk.as_ptr().add(last)) };
        core::mem::forget(chunk);
    }
    Ok(())
}

// Standalone trap function — prevents the compiler from inlining unreachable
// into the match arm in a way that triggers unexpected optimizations.
#[inline(never)]
fn trap() -> ! {
    // On wasm32, trigger unreachable instruction directly to avoid WASI panic handler.
    // On non-wasm32 (native compile checks), use unreachable!() — this code is
    // never executed outside wasm32 anyway; the test loads the .wasm artifact.
    #[cfg(target_arch = "wasm32")]
    core::arch::wasm32::unreachable();
    #[cfg(not(target_arch = "wasm32"))]
    unreachable!("trap() is only called from wasm32 context")
}

bindings::export!(Component with_types_in bindings);
