# Page rig timings: state at rest on a page-local `io` port

One run of the page rig's state workload, `0.1.0` against `0.2.0`, recorded so
the two contracts can be compared size for size. These are **node figures**:
no browser has run the surface byte path, and the entry
`surface-byte-crossing-in-browser` in `TODO.md` tracks that.

## Conditions

- Command: `bazel test //frontend:page_rig_measure --test_output=all`
  (the target is `manual`; timings are printed and never asserted).
- Engine: node v22.20.0, V8. **Not a browser.** The rig runs the production
  kernel bundle, the production `surface.ts` and the transpiled fixture kinds
  against a scripted attach peer.
- One run on a development machine. Read the figures as ratios between the two
  tables, not as absolute costs.
- The `measure` page's `Welcome` states `max_body_bytes` 4,194,304 B. The third
  size is bounded by `MAX_PUBLISH_BYTES_PER_ACTIVATION` (4 MiB of raw bytes per
  activation), which the 0.1.0 stored body at that size (4,000,018 B) sits
  just under.
- Each activation is timed from delivery of its command to quiescence. `state`
  reads the retained state, advances a 4-byte counter in it and publishes it
  back; `noop` is an activation whose state window is filled but unused.
  Every `state` result's checksum is checked against the expected bytes.
- The workload is the same raw bytes and step on both contracts: on creation
  byte *i* is `(i·31+7) mod 256`; each step increments the little-endian `u32`
  in bytes 0..4.

`noop` on `keeper` (empty windows), N 500: mean 0.903 / median 0.841 / min
0.778 ms.

## `0.1.0 base64 in JSON`

`processor-page-test`; the state is base64 inside a JSON body,
`{"v":1,"state":"…"}`, on a text-only page-local channel.

| Raw B | Stored B | N | `state` mean / median / min ms | `noop` mean / median / min ms |
|---:|---:|---:|---|---|
| 49,149 | 65,550 | 200 | 6.151 / 6.018 / 5.896 | 5.537 / 5.423 / 5.334 |
| 1,048,576 | 1,398,122 | 40 | 111.469 / 111.563 / 109.240 | 98.178 / 97.724 / 97.074 |
| 3,000,000 | 4,000,018 | 20 | 315.866 / 315.744 / 312.182 | 282.100 / 281.395 / 278.103 |

## `0.2.0 binary at rest`

`processor-page-bytes-test`; the state is the raw bytes as an
`application/octet-stream` body on a page-local channel declared `non_text`.

| Raw B | Stored B | N | `state` mean / median / min ms | `noop` mean / median / min ms |
|---:|---:|---:|---|---|
| 49,149 | 49,149 | 200 | 1.037 / 0.943 / 0.903 | 0.891 / 0.807 / 0.765 |
| 1,048,576 | 1,048,576 | 40 | 3.665 / 3.641 / 3.079 | 1.609 / 1.468 / 1.225 |
| 3,000,000 | 3,000,000 | 20 | 9.888 / 9.476 / 8.216 | 3.015 / 2.795 / 2.386 |

By `state` median, `0.2.0 binary at rest` is about 6.4×, 30.6× and 33.3×
faster than `0.1.0 base64 in JSON` at the three sizes, in node. No browser has
run this path (`surface-byte-crossing-in-browser`).
