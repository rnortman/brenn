# Page rig timings: state at rest on a page-local `io` port, and binary rows delivered over the socket

One run of the page rig's measure target, recorded so the two contracts can be
compared size for size, and so the cost of a binary row delivered to a `0.2.0`
kind over the page's websocket has a figure. The first tables are **node
figures**. The byte path and the page's binary websocket also run in Chromium
(`e2e/tests/bytes.spec.ts`), and **Chromium figures** follow the node tables.

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
the interval of a stream delivered to a surface. The Chromium table below has
the same row delivered in a browser.

## Chromium figures

### Conditions

- Command: `make e2e-measure` (runs `e2e/tests/bytes-measure.spec.ts` alone on
  a fresh e2e server; timings are printed and never asserted, every sample's
  checksum is).
- `@playwright/test` 1.61.1, Chromium `chromium-1228`, headless. One run on a
  development machine, 2026-11-13, against the server `make e2e` builds.
- The e2e server's `max_body_bytes` is 4,194,304 B (`brenn.e2e.brenn`), so the
  third size fits a publish.
- The page is the `bytes` surface: `processor-page-bytes-test` instances
  `stater` (a page-local `io state` port declared `non_text`, push 0 / retain
  1) and `streamer` (`out` and `in stream`, push 1 / retain 1, both on
  `ephemeral:bytes.stream`, a wire channel declared `non_text`), each driven
  by an `EchoStub` instance over page-local text links.

### What a figure includes

The clock is `performance.now()` in the page, which Chromium coarsens (the
figures step in 0.1 ms). A sample runs from the driver's dispatched click to
the driver's scrollback gaining the fixture's report(s), so it is the time of
a chain, not of one activation:

- a `state` or `noop` figure spans three activations — the driver's press
  activation, the fixture's activation, and the driver's render activation;
  two of them are 0.2.0 SDK activations of `EchoStub`;
- a delivered-row figure spans the emitting activation, the row out over the
  page's websocket, the server, the row back in, the delivery activation, and
  two driver renders (the emitting report and the delivery report).

The node figures above time one delivery to quiescence; the two are not
comparable figure for figure. The `noop` floor on the same chain is given so a
reader can subtract it. N for the smallest delivered size is 100, not the node
rig's 200: every emitted row draws one token of the streaming instance's
attach send budget (256 per instance for the life of the server, refilled one
per 15 s), and the run's 178 rows, warm-ups included, stay under it. The 0.1.0
base64 workload was not run in Chromium.

`noop` on `stater` (empty windows), N 500, warm-up 50: mean 1.720 / median
1.700 / min 1.300 ms.

### `0.2.0 binary at rest`, Chromium

| Raw B | N | `state` mean / median / min ms | `noop` mean / median / min ms |
|---:|---:|---|---|
| 49,149 | 200 | 2.137 / 2.100 / 1.700 | 1.698 / 1.700 / 1.400 |
| 1,048,576 | 40 | 5.175 / 5.100 / 4.500 | 2.573 / 2.500 / 2.300 |
| 3,000,000 | 20 | 11.990 / 10.800 / 10.300 | 4.170 / 4.200 / 3.800 |

Warm-ups 10, 5 and 3. Each size runs on a freshly loaded page, so the one
holder's state ring starts empty.

### `0.2.0 binary row delivered`, Chromium

| Raw B | N | click to delivery report mean / median / min ms |
|---:|---:|---|
| 49,149 | 100 | 45.232 / 45.100 / 44.100 |
| 1,048,576 | 40 | 24.215 / 23.450 / 22.300 |
| 3,000,000 | 20 | 62.095 / 59.750 / 56.800 |

Warm-ups 10, 5 and 3; every row `application/octet-stream`, differing in its
first four bytes, and the delivery report's count, length and FNV-1a 64
checked on every sample. The smallest size is slower than the middle one on
every sample of this run (min 44.1 ms against 22.3 ms). The run does not say
why; a delay of about 40 ms on the smallest frames has the shape of Nagle's
algorithm meeting a delayed acknowledgement on the loopback socket, and the
server does not set `TCP_NODELAY` — unverified, tracked as
`surface-small-row-delivery-latency` in `TODO.md`.
