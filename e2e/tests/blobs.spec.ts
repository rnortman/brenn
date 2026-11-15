import { randomBytes } from "node:crypto";
import { expect, test, type Page } from "@playwright/test";
import type { BlobsReport } from "../../frontend/src/page-rig/page";
import { fnv1a64 } from "../../frontend/src/page-rig/state";
import { split } from "../../frontend/src/page-rig/peer";
import { hex } from "./bytes-helpers";
import { CHAIN_TIMEOUT, echoStubHost, openSurface, publishVia } from "./echo-stub";

/**
 * Browser e2e for a surface page reading, making and forwarding message
 * attachments. The reading side: a backend
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
 * The producing side, with `maker` (a `processor-blobs-test` on the page,
 * granted `blobs`) and the backend producer's `feed`, which holds the latest
 * message the maker published on `blobs_made`:
 *
 *  3. The maker fills and seals a 300,000-byte pattern blob, two chunks long,
 *     and publishes it on the wire channel `blobs_made`; the page's socket
 *     sends its create, writes and seal, and the backend producer opens it in
 *     `feed` and reads it whole from the store, reporting the length and the
 *     hash node computes.
 *  4. The backend producer publishes an attachment on `blobs_frames`; the
 *     maker opens it in its `feed` and publishes it onward on `blobs_made`;
 *     the backend producer reads it back, and the page's socket sent no
 *     create, write or seal.
 *  5. The maker publishes a non-UTF-8 blob on the page-local `blobs_kept`;
 *     `kept-reader` reads it whole in the activation it wakes and by range in
 *     a later one.
 *
 * `blobs_frames` and `blobs_made` retain one row on the server and in each
 * window, so every navigation replays the previous test's frame; each
 * predicate matches the name and hash it expects, and every blob the
 * producing cases make begins with fresh random bytes, so no earlier run's
 * report matches.
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

/** One report `processor-blobs-test` publishes on `report`, as its body carries it. */
interface ScriptReport {
  results: string[];
}

/**
 * Every report in the driver `instance`'s scrollback, oldest first: each entry
 * is the text-shape envelope JSON whose `body` string is the report.
 */
async function readerReports(page: Page, instance: string): Promise<ReaderReport[]> {
  return reports<ReaderReport>(page, instance);
}

/** As `readerReports`, of whichever shape the driven kind publishes. */
async function reports<T>(page: Page, instance: string): Promise<T[]> {
  const texts = await echoStubHost(page, instance)
    .locator("[data-echo-message]")
    .allTextContents();
  return texts.map((text) => {
    const envelope = JSON.parse(text) as { body?: unknown };
    if (typeof envelope.body !== "string") {
      throw new Error(`a scrollback entry carries no string body: ${text}`);
    }
    return JSON.parse(envelope.body) as T;
  });
}

/** Poll a reader's driver until some arrival carries `word`. */
async function expectArrival(page: Page, word: string, driver = "drive-reader"): Promise<void> {
  await expect
    .poll(
      async () =>
        (await readerReports(page, driver)).flatMap((r) =>
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

/** A frame the `blobs` page's socket sent: its kind and its header's fields. */
interface SentFrame {
  type: string;
  header: Record<string, unknown>;
}

/**
 * Every frame the `blobs` page's socket sends, in order: a text frame's JSON,
 * a binary frame's header.
 */
function recordSent(page: Page): SentFrame[] {
  const sent: SentFrame[] = [];
  page.on("websocket", (ws) => {
    if (new URL(ws.url()).pathname !== BLOBS_SOCKET_PATH) {
      return;
    }
    ws.on("framesent", ({ payload }) => {
      const text = Buffer.isBuffer(payload) ? split(payload).header : payload;
      const header = JSON.parse(text) as Record<string, unknown>;
      sent.push({ type: String(header["type"]), header });
    });
  });
  return sent;
}

/** The sent frames of the upload kinds: create, write and seal. */
function uploads(sent: SentFrame[]): SentFrame[] {
  return sent.filter((f) => ["BlobCreate", "BlobWrite", "BlobSeal"].includes(f.type));
}

/**
 * `len` random bytes, the first of them 0xff so they are never UTF-8. No
 * earlier run's report names their hash.
 */
function fresh(len: number): Uint8Array {
  const bytes = new Uint8Array(randomBytes(len));
  bytes[0] = 0xff;
  return bytes;
}

/**
 * Script the backend producer to open the attachment `name` of the latest
 * message on its `feed` (`blobs_made`) and read it whole, until its report
 * carries `bytes`' length and hash. The page's flush reaches the server some
 * time after the page's script ran, so an early script finds the previous
 * message, or none, and the script is sent again.
 */
async function expectBackendReads(page: Page, name: string, bytes: Uint8Array): Promise<void> {
  const expected = JSON.stringify(["blob:0", `all:${bytes.length}:${fnv1a64(bytes)}`]);
  await expect(async () => {
    await publishVia(page, "drive-producer", `open feed:0 ${name};read-all 0`);
    await expect
      .poll(
        async () =>
          (await reports<ScriptReport>(page, "drive-producer")).some(
            (r) => JSON.stringify(r.results) === expected,
          ),
        { message: `the backend producer reads ${expected}`, timeout: 3_000 },
      )
      .toBe(true);
  }).toPass({ timeout: CHAIN_TIMEOUT });
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

test("a page component makes a 300,000-byte non-UTF-8 blob, publishes it on a wire channel, and the backend reads it back byte-exact", async ({
  page,
}) => {
  const sent = recordSent(page);
  await openSurface(page, "blobs");
  const LEN = 300_000;
  const head = fresh(8);
  const bytes = pattern(LEN);
  bytes.set(head, 0);

  await publishVia(
    page,
    "drive-maker",
    `create application/octet-stream;write 0 0 ${hex(head)};fill 0 8 ${LEN - 8};seal 0;publish a made big=0`,
  );
  await expectBackendReads(page, "big", bytes);

  // The upload crossed from this page: one create of the whole size, the
  // bytes in more than one write, and the seal.
  const made = uploads(sent);
  expect(made.filter((f) => f.type === "BlobCreate").map((f) => f.header["size"])).toEqual([LEN]);
  const writes = made.filter((f) => f.type === "BlobWrite");
  expect(writes.length, "the blob crossed in more than one write").toBeGreaterThan(1);
  expect(writes.reduce((n, f) => n + Number(f.header["len"]), 0)).toBe(LEN);
  expect(made.filter((f) => f.type === "BlobSeal")).toHaveLength(1);
});

test("an attachment the backend published is forwarded by reference through a page component and arrives intact", async ({
  page,
}) => {
  const sent = recordSent(page);
  await openSurface(page, "blobs");
  const bytes = fresh(64);

  await publishVia(
    page,
    "drive-producer",
    `create application/octet-stream;write 0 0 ${hex(bytes)};seal 0;publish a source src=0`,
  );
  // The page's `reader` shares the maker's window on `blobs_frames`: once it
  // has read the message, the maker's `feed` holds it too.
  await expectArrival(page, `src:${bytes.length}:${fnv1a64(bytes)}`);

  await publishVia(page, "drive-maker", "open feed:0 src;publish a forwarded fwd=0");
  await expectBackendReads(page, "fwd", bytes);
  expect(
    sent.some((f) => f.type === "BlobOpen" && f.header["name"] === "src"),
    "the page opened the attachment it forwarded",
  ).toBe(true);
  expect(uploads(sent), "forwarding uploads nothing").toEqual([]);
});

test("a page component keeps a non-UTF-8 blob on a page-local channel, read whole and by range by another", async ({
  page,
}) => {
  const sent = recordSent(page);
  await openSurface(page, "blobs");
  const bytes = fresh(32);

  await publishVia(
    page,
    "drive-maker",
    `create application/octet-stream;write 0 0 ${hex(bytes)};seal 0;publish b kept k=0`,
  );
  await expectArrival(page, `k:${bytes.length}:${fnv1a64(bytes)}`, "drive-kept-reader");

  const expected = ["blob:0", `hex:${hex(bytes.subarray(3, 11))}`];
  await publishVia(page, "drive-kept-reader", "open feed:0 k;read 0 3 8");
  await expect
    .poll(
      async () =>
        (await readerReports(page, "drive-kept-reader")).some(
          (r) => JSON.stringify(r.results) === JSON.stringify(expected),
        ),
      {
        message: `the kept reader's range read answers ${expected.join(" ")}`,
        timeout: CHAIN_TIMEOUT,
      },
    )
    .toBe(true);
  expect(uploads(sent), "a page-local attachment uploads nothing").toEqual([]);
});
