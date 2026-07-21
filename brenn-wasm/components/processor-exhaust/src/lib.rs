// Fuel-exhaustion test fixture for the `brenn:processor` world (design §4).
//
// This component unconditionally spins in an infinite loop on any non-empty
// new portion, exercising the fuel-exhaustion path in `ProcessorComponent::handle`.
//
// Behaviour:
//   - If any port-window has new envelopes (new_from < envelopes.len()): enters an
//     infinite loop — the host fuel budget traps it as ProcessorOutcome::Trap.
//   - If all port-windows are pure context (no new entries): returns Ok so the
//     full-size-window-does-not-spuriously-trap AC is verifiable separately.

#[allow(dead_code, clippy::all)]
mod bindings;

use bindings::brenn::processor::types::{Activation, ReceiveError};
use bindings::Guest;

struct ProcessorExhaust;

/// Infinite spin loop that the optimizer cannot eliminate.
///
/// `std::hint::black_box` around the counter prevents the compiler from proving
/// the loop terminates and optimising it into a single return. Wasmtime's fuel
/// mechanism traps the loop before it ever finishes.
#[inline(never)]
fn spin_forever() {
    let mut x: u64 = std::hint::black_box(1u64);
    loop {
        x = std::hint::black_box(x).wrapping_add(std::hint::black_box(1u64));
        // The compiler cannot prove black_box(x) == 0 is never true, so it cannot
        // optimise this into a diverging instruction or remove the loop body.
        if std::hint::black_box(x) == std::hint::black_box(0u64) {
            return;
        }
    }
}

impl Guest for ProcessorExhaust {
    fn receive(a: Activation) -> Result<(), ReceiveError> {
        for port_window in &a.ports {
            let new_from = port_window.new_from as usize;
            if new_from < port_window.envelopes.len() {
                // Spin forever — exhausts wasmtime fuel, causing a trap.
                // The host converts this to ProcessorOutcome::Trap (design §2.2).
                spin_forever();
            }
        }

        Ok(())
    }
}

bindings::export!(ProcessorExhaust with_types_in bindings);
