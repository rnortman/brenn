// Store round-trip fixture for the `brenn:processor` world.
//
// Default activation (no new envelopes): runs the full store round-trip and
// returns Ok. Exercises all six store operations via the brenn-guest RAII
// Transaction wrapper — no explicit rollback() calls needed.
//
// Special envelope body commands (new envelopes drive dispatch):
//
//   "__raii_rollback__"
//     Begins a transaction, writes sentinel key "raii-key" = b"raii-value",
//     then returns Err(ProcessingFailed) WITHOUT explicitly rolling back.
//     Transaction::drop handles the rollback (brenn-guest RAII guard).
//     Host test asserts: outcome is Err (not Trap) and the key is absent,
//     proving the guard eliminated the leaked-tx footgun.
//
// Default round-trip sequence:
//   begin → put → commit
//   begin → get → commit (assert value matches)
//   begin → scan → commit (assert result)
//   begin → delete → commit
//   begin → get → commit (assert absent after delete)
//   begin → put → (drop guard: RAII rollback) → begin → get → commit (assert absent)

use brenn_guest::{Activation, Error, Processor, store};

struct ProcessorStoreRt;

fn round_trip() -> Result<(), Error> {
    let ns = "test-ns";
    let key = b"hello";
    let value = b"world";

    // ── begin → put → commit ──────────────────────────────────────────────
    let tx = store::begin()?;
    tx.put(ns, key, value)?;
    tx.commit()?;

    // ── begin → get → assert value matches → commit ───────────────────────
    let tx = store::begin()?;
    let got = tx.get(ns, key)?;
    tx.commit()?;
    match got {
        Some(ref v) if v == value => {}
        Some(v) => {
            return Err(Error::failed(format!(
                "store-rt: value mismatch: got {v:?}, want {value:?}"
            )));
        }
        None => {
            return Err(Error::failed("store-rt: key absent after commit"));
        }
    }

    // ── begin → scan → assert result → commit ────────────────────────────
    let tx = store::begin()?;
    let pairs = tx.scan(ns, b"", None, 0)?;
    tx.commit()?;
    if pairs.len() != 1 || pairs[0].0 != key || pairs[0].1 != value {
        return Err(Error::failed(format!(
            "store-rt: scan mismatch: got {pairs:?}, want [({key:?}, {value:?})]"
        )));
    }

    // ── begin → delete → commit ───────────────────────────────────────────
    let tx = store::begin()?;
    tx.delete(ns, key)?;
    tx.commit()?;

    // ── begin → get → assert absent after delete → commit ─────────────────
    let tx = store::begin()?;
    let after_delete = tx.get(ns, key)?;
    tx.commit()?;
    if after_delete.is_some() {
        return Err(Error::failed(
            "store-rt: key still present after delete",
        ));
    }

    // ── begin → put → RAII rollback (guard Drop) ──────────────────────────
    // The original fixture called rollback() explicitly. With brenn-guest the
    // guard drops at the end of the block and calls rollback automatically,
    // proving the RAII guard works on the success path too.
    {
        let tx = store::begin()?;
        tx.put(ns, key, value)?;
        // `tx` drops here → Transaction::drop → rollback().
    }

    // ── begin → get → assert absent after RAII rollback → commit ──────────
    let tx = store::begin()?;
    let after_rollback = tx.get(ns, key)?;
    tx.commit()?;
    if after_rollback.is_some() {
        return Err(Error::failed(
            "store-rt: key present after RAII rollback — rollback did not persist",
        ));
    }

    Ok(())
}

impl Processor for ProcessorStoreRt {
    fn receive(activation: Activation) -> Result<(), Error> {
        for window in activation.port_windows() {
            for env in window.new_envelopes() {
                let env = env?;
                if env.body == "__raii_rollback__" {
                    // RAII rollback test: begin tx, put sentinel, return Err.
                    // Transaction::drop handles rollback — no explicit call.
                    let tx = store::begin()?;
                    tx.put("test-ns", b"raii-key", b"raii-value")?;
                    return Err(Error::failed(
                        "store-rt: deliberate err with live transaction (RAII rollback)",
                    ));
                }
            }
        }

        // No sentinel: run the full round-trip.
        round_trip()
    }
}

brenn_guest::export_processor!(ProcessorStoreRt);
