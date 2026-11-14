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
