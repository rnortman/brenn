# Page rig timings: state at rest on a page-local `io` port, and binary rows delivered over the socket

One run of the page rig's measure target, recorded so the two contracts can be
compared size for size, and so the cost of a binary row delivered to a `0.2.0`
kind over the page's websocket has a figure. These are **node figures**: no
browser has run the surface byte path or the page's binary websocket frames,
and the entry `surface-byte-crossing-in-browser` in `TODO.md` tracks that.

## Conditions

- Command: `bazel test //frontend:page_rig_measure --test_output=all`
  (the target is `manual`; timings are printed and never asserted).
- Engine: node v22.20.0, V8. **Not a browser.** The rig runs the production
  kernel bundle, the production `surface.ts` and the transpiled fixture kinds
  against a scripted attach peer, through a stand-in `WebSocket`. The page
  speaks attach v5, so every delivery and publish below crosses that stand-in
  as a binary frame.
- One run on a development machine. Read the figures as ratios between the
  tables, not as absolute costs.
- The `measure` page's `Welcome` states `max_body_bytes` 4,194,304 B. The third
  size is bounded by `MAX_PUBLISH_BYTES_PER_ACTIVATION` (4 MiB of raw bytes per
  activation), which the 0.1.0 stored body at that size (4,000,018 B) sits
  just under. A delivered row is bounded by neither: the frame cap is the
  page-to-server one.
- Each activation is timed from delivery of its command, or of the row, to
  quiescence. `state` reads the retained state, advances a 4-byte counter in
  it and publishes it back; `noop` is an activation whose state window is
  filled but unused. Every `state` result's checksum is checked against the
  expected bytes.
- The workload is the same raw bytes and step on both contracts: on creation
  byte *i* is `(i·31+7) mod 256`; each step increments the little-endian `u32`
  in bytes 0..4.

`noop` on `keeper` (empty windows), N 500: mean 0.953 / median 0.885 / min
0.828 ms.

## `0.1.0 base64 in JSON`

`processor-page-test`; the state is base64 inside a JSON body,
`{"v":1,"state":"…"}`, on a text-only page-local channel.

| Raw B | Stored B | N | `state` mean / median / min ms | `noop` mean / median / min ms |
|---:|---:|---:|---|---|
| 49,149 | 65,550 | 200 | 6.162 / 5.983 / 5.888 | 5.497 / 5.384 / 5.319 |
| 1,048,576 | 1,398,122 | 40 | 111.321 / 110.922 / 109.673 | 98.963 / 98.345 / 97.447 |
| 3,000,000 | 4,000,018 | 20 | 317.387 / 317.276 / 313.388 | 281.833 / 281.106 / 279.041 |

## `0.2.0 binary at rest`

`processor-page-bytes-test`; the state is the raw bytes as an
`application/octet-stream` body on a page-local channel declared `non_text`.

| Raw B | Stored B | N | `state` mean / median / min ms | `noop` mean / median / min ms |
|---:|---:|---:|---|---|
| 49,149 | 49,149 | 200 | 1.086 / 0.974 / 0.947 | 0.938 / 0.853 / 0.817 |
| 1,048,576 | 1,048,576 | 40 | 3.732 / 3.528 / 3.151 | 1.692 / 1.520 / 1.277 |
| 3,000,000 | 3,000,000 | 20 | 9.297 / 9.377 / 8.266 | 3.113 / 2.815 / 2.275 |

By `state` median, `0.2.0 binary at rest` is about 6.1×, 31.4× and 33.8×
faster than `0.1.0 base64 in JSON` at the three sizes, in node.

## `0.2.0 binary row delivered`

`processor-page-bytes-test` as the `measure` page's `b-stream` instance,
subscribed push 1 / retain 1 to `ephemeral:rig.stream`, a wire channel
declared `non_text`. The peer delivers one `application/octet-stream` row of
the size below per iteration, each differing in its first four bytes; the
instance reports the row's count, length and FNV-1a 64 rather than its bytes,
and every checksum is checked. The same untimed warm-ups as the tables above
(10, 5 and 3).

| Raw B | N | delivery to quiescence mean / median / min ms |
|---:|---:|---|
| 49,149 | 200 | 1.005 / 0.886 / 0.858 |
| 1,048,576 | 40 | 3.345 / 3.191 / 2.788 |
| 3,000,000 | 20 | 8.926 / 8.197 / 7.460 |

Each delivery is a fresh instantiation of the kind, so these figures floor
the interval of a stream delivered to a surface. No browser has run this path
(`surface-byte-crossing-in-browser`).
