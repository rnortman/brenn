import { expect, test, type Page } from "@playwright/test";
import {
  expectedState,
  fnv1a64,
  generatedState,
  withCounter,
} from "../../frontend/src/page-rig/state";
import { BYTES_SOCKET_PATH, hex, reportWhere, run, windowOf } from "./bytes-helpers";
import { openSurface } from "./echo-stub";

/**
 * Browser e2e for the surface byte path: a `brenn:processor@0.2.0` kind
 * (`processor-page-bytes-test`, served from the harness's second mount) keeps
 * non-text bodies on the page and sends and receives them over the page's
 * websocket, in Chromium, each crossing compared byte for byte in node.
 *
 *  1. A non-UTF-8 body rests on a page-local `io` port declared `non_text` and
 *     a later activation reads it back with its content type.
 *  2. A non-UTF-8 body leaves in a binary websocket frame, arrives in one, and
 *     reaches the kind with its content type and channel.
 *  3. The largest measured size (3,000,000 B) crosses once at rest and once
 *     over the socket, so the body-cap setting and the `emit`/`stream` path
 *     that `bytes-measure.spec.ts` times are held on every run.
 *  4. A guest-SDK kind (`processor-sdk-bytes-test`, served from the harness's
 *     third mount) keeps the same binary state at the largest size through
 *     `RetainedState` and its own byte codec on a `non_text` page-local `io`
 *     port, created and stepped twice.
 *
 * Runs after `bar.spec.ts`, whose first test needs pristine channels, and
 * before `surface-reload.spec.ts`, which rewrites the live document. Every
 * test opens the page afresh, so page-local state does not carry between
 * them; `bytes_wire` and `bytes_stream` retain one row on the server across
 * navigations, so every predicate matches the body or checksum it expects.
 */

/** Every byte value once, so not UTF-8. */
const A = generatedState(256);
/** Each of `A`'s bytes inverted, so a cross-wired channel fails rather than passes. */
const B = A.map((b) => 255 - b);

const LARGEST = 3_000_000;

test("a non-UTF-8 body rests on a page-local io port and is read back byte for byte", async ({
  page,
}) => {
  await openSurface(page, "bytes");
  const published = await run(
    page,
    "drive-keeper",
    `publish memo application/octet-stream ${hex(A)}`,
    () => true,
  );
  expect(published.results).toEqual(["ok"]);

  const read = await run(page, "drive-keeper", "noop", (w) => w === "ok");
  const memo = windowOf(read, "memo");
  expect(memo, "the noop activation's report has a memo window").toBeDefined();
  expect(
    memo?.envelopes.map((e) => [e.content_type, e.hex]),
  ).toEqual([["application/octet-stream", hex(A)]]);
});

/** The binary payloads of the `bytes` page's socket, each direction. */
interface Frames {
  sent: Buffer[];
  received: Buffer[];
}

/** Record the `bytes` page's websocket frames from before navigation. */
function recordFrames(page: Page): Frames {
  const frames: Frames = { sent: [], received: [] };
  page.on("websocket", (ws) => {
    if (new URL(ws.url()).pathname !== BYTES_SOCKET_PATH) {
      return;
    }
    // Only a `Buffer` is a binary frame; a text frame's string never counts.
    ws.on("framesent", ({ payload }) => {
      if (Buffer.isBuffer(payload)) {
        frames.sent.push(payload);
      }
    });
    ws.on("framereceived", ({ payload }) => {
      if (Buffer.isBuffer(payload)) {
        frames.received.push(payload);
      }
    });
  });
  return frames;
}

/** Whether some binary frame holds `body` as a contiguous run of bytes. */
function carries(frames: Buffer[], body: Uint8Array): boolean {
  return frames.some((f) => f.indexOf(body) >= 0);
}

test("a non-UTF-8 body leaves over the page's websocket and is delivered over it, byte for byte", async ({
  page,
}) => {
  const frames = recordFrames(page);
  await openSurface(page, "bytes");
  const published = await run(
    page,
    "drive-keeper",
    `publish out image/jpeg ${hex(B)}`,
    () => true,
  );
  expect(published.results).toEqual(["ok"]);

  await expect
    .poll(() => carries(frames.sent, B), {
      message: "a binary frame the page sent carries the body",
    })
    .toBe(true);
  await expect
    .poll(() => carries(frames.received, B), {
      message: "a binary frame the page received carries the body",
    })
    .toBe(true);

  const delivered = await reportWhere(
    page,
    "drive-keeper",
    "the body delivered on feed",
    (r) => windowOf(r, "feed")?.envelopes.at(-1)?.hex,
    hex(B),
  );
  const newest = windowOf(delivered, "feed")?.envelopes.at(-1);
  expect(newest?.content_type).toBe("image/jpeg");
  expect(newest?.header["channel"]).toBe("ephemeral:bytes.wire");
});

test("the largest measured size crosses once at rest and once over the socket", async ({
  page,
}) => {
  await openSurface(page, "bytes");
  const pattern = generatedState(LARGEST);

  const created = await run(page, "drive-stater", `state ${LARGEST}`, () => true);
  expect(created.results).toEqual([expectedState(pattern, 0)]);
  const stepped = await run(page, "drive-stater", `state ${LARGEST}`, () => true);
  expect(stepped.results).toEqual([expectedState(pattern, 1)]);

  const emitted = withCounter(pattern, 1);
  const sum = fnv1a64(emitted);
  const sent = await run(
    page,
    "drive-streamer",
    `emit out ${LARGEST} 1`,
    () => true,
  );
  expect(sent.results).toEqual([`emit:${LARGEST}:${sum}`]);
  const delivered = await reportWhere(
    page,
    "drive-streamer",
    "the emitted row delivered on stream",
    (r) => r.streams[0]?.fnv1a,
    sum,
  );
  expect(delivered.streams[0]).toMatchObject({ count: 1, len: LARGEST });
});

test("a guest-SDK kind keeps its binary state through RetainedState, created and stepped twice", async ({
  page,
}) => {
  await openSurface(page, "bytes");
  const pattern = generatedState(LARGEST);
  const results: string[] = [];
  for (let i = 0; i < 3; i++) {
    const report = await run(page, "drive-sdk-stater", `state ${LARGEST}`, () => true);
    results.push(...report.results);
  }
  expect(results).toEqual([0, 1, 2].map((steps) => expectedState(pattern, steps)));
});
