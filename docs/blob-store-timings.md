# Blob store timings: the life of a blob only a ring message names

One run of the blob cycle measurement, recorded so the cost of the store on an
`ephemeral:` channel has a figure. **The figure decides nothing**: no default,
bound or design choice is tuned to it, and nothing asserts on time.

## What one cycle is

Create a blob, write 102,400 bytes to it in one positional write, seal it,
publish a message naming it on an `ephemeral:` channel that retains one
message, and let the next cycle's publish evict that message. The eviction
drops the ring entry's hold, which is the blob's last holder, so the blob's
file is unlinked and its charge returned inside that publish. No SQLite
statement and no fsync run anywhere in the cycle. After the last cycle a
plain publish evicts the last blob, and the run asserts that the store holds
nothing and charges nothing.

The function is `run` in `brenn-messaging/src/blob_cycle.rs`. The ordinary
test binary runs it for ten cycles (`ten_cycles_leave_the_store_empty`), so
the loop cannot rot. The figure below comes from the ignored `measure` test,
which runs it for ten thousand.

## Conditions

- Command: `bazel test //brenn-messaging:blob_cycle_measure --test_output=all`
  (the target is `manual`; it prints the figure and asserts nothing on time).
- Build: Bazel's default `fastbuild`, not the `release` configuration's
  `opt`. rustc 1.95.0.
- Machine: AMD Ryzen 5 5600X (6 cores, 12 threads), Linux 7.1. The blob
  directory is under the test's temporary directory, on btrfs on NVMe.
- The timer covers the ten thousand cycles and the final publish, not the
  messenger's construction.
- One run on a development machine, beside a second run of the same command
  minutes earlier for scale. Read the figure as an order of magnitude.

## Result

| Cycles | Blob bytes | Elapsed s | Cycles / s |
|---:|---:|---:|---:|
| 10,000 | 102,400 | 1.392 | 7,183 |

The earlier run of the same command printed 0.908 s, 11,016 cycles/s. The
spread between the two is the machine, not the code.

# Blob forwarding timings: a component reads an attachment and forwards it

One run of the forwarding measurement, recorded so the cost of a component
reading a blob and forwarding it by reference has a figure. **The figure
decides nothing**: no default, bound or design choice is tuned to it, and
nothing asserts on time.

## What one activation is

The host creates a blob, writes 102,400 bytes to it and seals it, and
publishes a message naming it on an `ephemeral:` channel that retains one
message, evicting the previous iteration's. The `processor-blobs-test`
component granted `blobs` is activated on that message: it opens the
attachment, reads it in full in one call, and publishes a message naming the
same blob on a second `ephemeral:` channel that retains one message, then
publishes its report on a `brenn:` channel. The forward copies no byte of the
blob; the read copies all of them into the guest. Both channels being rings,
the blob is never synced or adopted, and the eviction of the second
channel's previous message drops the last hold on the previous blob. After
the last activation a plain publish on each ring evicts the last blob, and
the run asserts every report and that the store holds nothing and charges
nothing.

The function is `forward_loop` in `brenn-wasm-dispatch/src/tests/blobs.rs`.
The ordinary test binary runs it for ten activations
(`ten_forwards_leave_the_store_empty`), so the loop cannot rot. The figure
below comes from the ignored `measure` test, which runs it for ten thousand.

## Conditions

- Command: `bazel test //brenn-wasm-dispatch:blob_forward_measure
  --test_output=all` (the target is `manual`; it prints the figure and
  asserts nothing on time).
- Build: Bazel's default `fastbuild`, not the `release` configuration's
  `opt`. rustc 1.95.0.
- Machine: AMD Ryzen 5 5600X (6 cores, 12 threads), Linux 7.1. The blob
  directory is under the test's temporary directory, on btrfs on NVMe.
- The timer covers the ten thousand iterations — the host's blob creation and
  publish, and the whole activation through its flush, including the report's
  insert on the `brenn:` channel — not the messenger's construction or the
  reading of the reports afterwards.
- One run on a development machine, beside a second run of the same command
  for scale. Read the figure as an order of magnitude. No run without the
  blob was made, so it does not say what share of the time the blob costs.

## Result

| Activations | Blob bytes | Elapsed s | Activations / s |
|---:|---:|---:|---:|
| 10,000 | 102,400 | 37.430 | 267 |

The second run of the same command printed 37.629 s, 266 activations/s.
