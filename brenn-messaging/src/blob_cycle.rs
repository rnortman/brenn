//! The whole life of a blob only a ring message names, timed: create, write
//! about 100 KiB, seal, publish it as an attachment on an `ephemeral:` channel
//! of depth 1, and let the next publish evict it, which deletes it.
//!
//! The test binary runs a few cycles so the loop cannot rot. The `manual`
//! target `//brenn-messaging:blob_cycle_measure` runs the ignored
//! [`measure`] test for many and prints cycles per second; the figure is
//! recorded in `docs/blob-store-timings.md` and decides nothing.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::Messenger;
use crate::db::init_db_memory;
use crate::testutils::{TempBlobDir, blob_messenger, ephemeral_channel_entry, publish_attached};

const PUBLISHER: &str = "blob-cycle";
const CHANNEL: &str = "blob-cycle";
/// The size of each cycle's blob.
const BLOB_BYTES: usize = 100 * 1024;
/// The bytes each cycle writes, in one positional write.
static PIECE: [u8; BLOB_BYTES] = [0x5a; BLOB_BYTES];

/// A messenger with one `ephemeral:` channel retaining one message, a blob
/// store, and a system publisher allowed onto the channel `cycles + 1` times
/// without meeting its send rate.
async fn messenger(cycles: u32, blobs: &TempBlobDir) -> (Arc<Messenger>, String) {
    let mut ring = ephemeral_channel_entry(CHANNEL, 1);
    ring.resolved_channel.send_rate = crate::config::SendRate {
        burst: cycles + 1,
        refill_interval_secs: 1,
        refill: 1,
    };
    let address = ring.address.clone();
    let m = blob_messenger(init_db_memory(), vec![ring], PUBLISHER, blobs.path()).await;
    (m, address)
}

/// Run `cycles` cycles and return how long they took. Each cycle's publish
/// evicts the previous cycle's message, and with it the only holder of its
/// blob; a final plain publish evicts the last. The store is empty at the end.
async fn run(cycles: u32) -> Duration {
    // Declared first, so the directory outlives the messenger, whose ring
    // still holds a blob if a cycle panics.
    let blobs = TempBlobDir::new();
    let (m, address) = messenger(cycles, &blobs).await;
    let store = Arc::clone(m.blob_store().expect("the messenger has a blob store"));
    let started = Instant::now();
    for _ in 0..cycles {
        let mut writer = store
            .create("application/octet-stream")
            .expect("create a blob");
        writer.write(0, &PIECE).expect("write the blob");
        let hold = writer.seal().expect("seal the blob");
        publish_attached(&m, PUBLISHER, &address, "cycle", &[("frame", &hold)], None).await;
        drop(hold);
        assert_eq!(
            store.usage().sealed,
            1,
            "the publish evicted the previous blob"
        );
    }
    publish_attached(&m, PUBLISHER, &address, "last", &[], None).await;
    let elapsed = started.elapsed();
    let usage = store.usage();
    assert_eq!(
        (usage.sealed, usage.unsealed, usage.charged_bytes),
        (0, 0, 0),
        "every blob is gone"
    );
    elapsed
}

#[tokio::test]
async fn ten_cycles_leave_the_store_empty() {
    run(10).await;
}

/// Ten thousand cycles, timed. Run by `//brenn-messaging:blob_cycle_measure`.
#[tokio::test]
#[ignore = "a measurement: run by //brenn-messaging:blob_cycle_measure"]
async fn measure() {
    const CYCLES: u32 = 10_000;
    let elapsed = run(CYCLES).await;
    println!(
        "blob cycle: {CYCLES} cycles of {BLOB_BYTES} bytes in {:.3} s, {:.0} cycles/s",
        elapsed.as_secs_f64(),
        f64::from(CYCLES) / elapsed.as_secs_f64()
    );
}
