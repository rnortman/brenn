import { expect, test, type Page } from "@playwright/test";
import type { BlobsReport } from "../../frontend/src/page-rig/page";
import { fnv1a64 } from "../../frontend/src/page-rig/state";
import { hex } from "./bytes-helpers";
import { CHAIN_TIMEOUT, echoStubHost, openSurface, publishVia } from "./echo-stub";

/**
 * Browser e2e for a surface page reading message attachments: a backend
 * `processor-blobs-test` consumer (`blobs_producer`, served from the harness's
 * fourth mount) makes a blob and publishes it as an attachment on a link that
 * spans the wire; on the `blobs` page a `processor-page-blobs-test` instance
 * (`reader`, served from the fifth mount, granted `blobs`) is woken by the
 * message and reads the attachment whole, in Chromium, through the real
 * kernel, glue and blob door. Every comparison is made in node.
 *
 *  1. A 300,000-byte pattern blob, two read answers long, arrives as binary
 *     websocket frames and the activation the message wakes reads it whole:
 *     the reader's arrival report carries its length and the hash node
 *     computes.
 *  2. A short non-UTF-8 blob written from hex is read back by range from a
 *     later activation and compared byte for byte.
 *
 * `blobs_frames` retains one row on the server and in the page's window, so
 * every navigation replays the previous test's frame; each predicate matches
 * the name and hash it expects.
 */

/** The `blobs` page's websocket path; the URL also carries a build query. */
const BLOBS_SOCKET_PATH = "/surface/blobs/ws";

/**
 * The bytes `processor-blobs-test`'s `fill <w> 0 <len>` writes. Its period,
 * 251, divides no chunk size, so a read answer served from the wrong offset
 * carries different bytes.
 */
function pattern(len: number): Uint8Array {
  const bytes = new Uint8Array(len);
  for (let i = 0; i < len; i++) {
    bytes[i] = (i * 31 + 7) % 251;
  }
  return bytes;
}

/** One report `processor-page-blobs-test` publishes on `report`, as its body carries it. */
type ReaderReport = Omit<BlobsReport, "instance" | "shape">;

/**
 * Every report in the driver `instance`'s scrollback, oldest first: each entry
 * is the text-shape envelope JSON whose `body` string is the report.
 */
async function readerReports(page: Page, instance: string): Promise<ReaderReport[]> {
  const texts = await echoStubHost(page, instance)
    .locator("[data-echo-message]")
    .allTextContents();
  return texts.map((text) => {
    const envelope = JSON.parse(text) as { body?: unknown };
    if (typeof envelope.body !== "string") {
      throw new Error(`a scrollback entry carries no string body: ${text}`);
    }
    return JSON.parse(envelope.body) as ReaderReport;
  });
}

/** Poll the reader's driver until some arrival carries `word`. */
async function expectArrival(page: Page, word: string): Promise<void> {
  await expect
    .poll(
      async () =>
        (await readerReports(page, "drive-reader")).flatMap((r) =>
          r.arrivals.flatMap((a) => a.attachments),
        ),
      {
        message: `the reader's arrival report reads ${word}`,
        timeout: CHAIN_TIMEOUT,
      },
    )
    .toContain(word);
}

/** The binary payloads the `blobs` page's socket received. */
function recordReceived(page: Page): Buffer[] {
  const received: Buffer[] = [];
  page.on("websocket", (ws) => {
    if (new URL(ws.url()).pathname !== BLOBS_SOCKET_PATH) {
      return;
    }
    // Only a `Buffer` is a binary frame; a text frame's string never counts.
    ws.on("framereceived", ({ payload }) => {
      if (Buffer.isBuffer(payload)) {
        received.push(payload);
      }
    });
  });
  return received;
}

/**
 * Whether some binary frame ends with `bytes`. A read answer's bytes are its
 * frame's only body, so they are the frame's last bytes.
 */
function endsSomeFrame(frames: Buffer[], bytes: Uint8Array): boolean {
  return frames.some(
    (f) => f.length >= bytes.length && f.subarray(f.length - bytes.length).equals(bytes),
  );
}

test("a 300,000-byte attachment crosses as binary frames and the activation its message wakes reads it whole", async ({
  page,
}) => {
  const received = recordReceived(page);
  await openSurface(page, "blobs");
  const LEN = 300_000;
  const bytes = pattern(LEN);

  await publishVia(
    page,
    "drive-producer",
    `create application/octet-stream;fill 0 0 ${LEN};seal 0;publish a frame big=0`,
  );
  await expectArrival(page, `big:${LEN}:${fnv1a64(bytes)}`);

  // A read answer carries at most 256 KiB, so the blob crosses in two.
  const CHUNK = 256 * 1024;
  expect(
    endsSomeFrame(received, bytes.subarray(0, CHUNK)),
    "a binary frame the page received ends with the blob's first 256 KiB",
  ).toBe(true);
  expect(
    endsSomeFrame(received, bytes.subarray(CHUNK)),
    "a binary frame the page received ends with the blob's bytes from 256 KiB to its end",
  ).toBe(true);
});

test("a non-UTF-8 attachment written from hex is read back by range byte for byte", async ({
  page,
}) => {
  const received = recordReceived(page);
  await openSurface(page, "blobs");
  /** Not UTF-8: a lone continuation byte, an overlong lead, and 0xff/0xfe. */
  const bytes = Uint8Array.from([0xff, 0xfe, 0x00, 0x80, 0xc3, 0x28, 0x01, 0x7f]);
  const sum = fnv1a64(bytes);

  await publishVia(
    page,
    "drive-producer",
    `create application/octet-stream;write 0 0 ${hex(bytes)};seal 0;publish a small raw=0`,
  );
  await expectArrival(page, `raw:${bytes.length}:${sum}`);
  expect(
    endsSomeFrame(received, bytes),
    "a binary frame the page received ends with the blob",
  ).toBe(true);

  const expected = ["blob:0", `hex:${hex(bytes.subarray(2, 6))}`, `all:${bytes.length}:${sum}`];
  await publishVia(page, "drive-reader", "open feed:0 raw;read 0 2 4;read-all 0");
  await expect
    .poll(
      async () =>
        (await readerReports(page, "drive-reader")).some(
          (r) => JSON.stringify(r.results) === JSON.stringify(expected),
        ),
      {
        message: `the reader's range read answers ${expected.join(" ")}`,
        timeout: CHAIN_TIMEOUT,
      },
    )
    .toBe(true);
});
