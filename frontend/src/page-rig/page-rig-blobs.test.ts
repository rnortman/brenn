// @vitest-environment node
//
// The page rig's `blobs` page: the production kernel bundle, the production
// `surface.ts` and the transpiled `processor-page-blobs-test` kind, granted
// `blobs`, behind a `Welcome` that opens a read-only blob door, against the
// scripted attach peer serving the bytes of every attachment it delivers.
//
// `p-blobs` reads `feed`, which the page gates at its fetch bound: a row's
// attachments are fetched before the row reaches the window, and the
// activation it wakes reads them whole. `b-asker` calls `p-blobs` on `ask`,
// whose sync activation runs the request as a script against its windows.
//
// One page for the file. Cases run in order and share it; each delivers its
// own row, and `feed` holds only the newest, so `feed:0` is always the row the
// case delivered last.

import { afterAll, describe, expect, it } from "vitest";
import { type Page, blobsReport, only, openPage } from "./page.js";
import {
    type Json,
    type RigAttachment,
    bytesOfHex,
    fillRaw,
    hexOf,
    readClientFrame,
} from "./peer.js";
import { fnv1a64 } from "./state.js";

const OCTETS = "application/octet-stream";

/** Bytes that are not UTF-8. */
const NOT_UTF8 = new Uint8Array([0xff, 0x00, 0xfe, 0x80]);

const hexOfText = (text: string): string => Buffer.from(text, "utf8").toString("hex");

/**
 * `len` bytes of period 251, which divides no chunk size: a chunk read from
 * the wrong offset carries different bytes.
 */
function pattern(len: number): Uint8Array {
    const bytes = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
        bytes[i] = (i * 31 + 7) % 251;
    }
    return bytes;
}

function attachment(name: string, bytes: Uint8Array): RigAttachment {
    return { name, bytes, contentType: OCTETS };
}

/** The arrival word `p-blobs` reports for an attachment read whole. */
function readWhole(name: string, bytes: Uint8Array): string {
    return `${name}:${bytes.length}:${fnv1a64(bytes)}`;
}

let page: Page | null = null;

function rig(): Page {
    if (page === null) {
        throw new Error("no page: the bring-up case did not produce one");
    }
    return page;
}

afterAll(() => {
    page?.finish();
});

/** The arrivals `p-blobs` reported for the one row `deliverAttached` delivered. */
async function arrive(attachments: RigAttachment[]): Promise<string[]> {
    const reports = await rig().deliverAttached(rig().facts.blobs_feed_channel, "row", attachments);
    const arrivals = blobsReport(only(reports, "p-blobs")).arrivals;
    expect(arrivals).toHaveLength(1);
    return arrivals[0]?.attachments ?? [];
}

/** `b-asker` calls `p-blobs` on `ask` with `script`; the reply's lines, which must be `text/plain`. */
async function ask(script: string): Promise<string[]> {
    const reports = await rig().command("b-asker", `call peer text/plain ${hexOfText(script)}`);
    expect(reports.map((r) => r.instance)).toEqual(["p-blobs", "b-asker"]);
    expect(blobsReport(only(reports, "p-blobs")).sync).toBe("ask");
    const [result] = only(reports, "b-asker").results;
    const prefix = "ok:text/plain:";
    if (result === undefined || !result.startsWith(prefix)) {
        throw new Error(`b-asker's call came back ${String(result)}`);
    }
    return Buffer.from(result.slice(prefix.length), "hex").toString("utf8").split("\n");
}

describe("page rig: a component reads attachments through the page's blob door", () => {
    it("brings every instance up through bootstrap, behind a read-only door", async () => {
        page = await openPage("blobs");
        const instances = page.facts.instances.map((i) => i.instance);
        expect(instances).toContain("p-blobs");
        expect(page.status().map((row) => [row.instance, row.state])).toEqual(
            instances.map((i) => [i, "mounted"]),
        );
        expect(blobsReport(only(page.reports("p-blobs"), "p-blobs")).results).toEqual([]);
    });

    it("rebuilds the codec's read answer byte for byte and reads the blob frames Rust writes", () => {
        const sample = rig().kit.blob_read_sample;
        const values: Record<string, Json> = sample.values;
        expect(hexOf(fillRaw(rig().kit.blob_answers.read_ok, values, bytesOfHex(sample.bytes)))).toBe(sample.hex);

        const frames = rig()
            .kit.client_samples.map((s) => readClientFrame("text" in s ? s.text : bytesOfHex(s.hex)))
            .filter((f) => f.type.startsWith("Blob"));
        expect(frames).toEqual([
            {
                type: "BlobOpen",
                handle: 5,
                channel: "ephemeral:rig.blobs-feed",
                messageId: "00000000-0000-0000-0000-000000000004",
                name: "frame.jpg",
            },
            { type: "BlobRead", correlation: 6, handle: 5, offset: 8_192, len: 4_096 },
            { type: "BlobClose", handle: 5 },
            { type: "BlobCreate", handle: 7, contentType: OCTETS, size: 300_000 },
            { type: "BlobWrite", handle: 7, offset: 4_096, bytes: NOT_UTF8 },
            { type: "BlobSeal", handle: 7 },
        ]);
    });

    it("hands the activation a row wakes its attachments, several chunks and not UTF-8, byte for byte", async () => {
        const several = pattern(10_000);
        const before = rig().blobFrames().length;
        const arrived = await arrive([attachment("frame.bin", several), attachment("raw.bin", NOT_UTF8)]);
        expect(arrived).toEqual([readWhole("frame.bin", several), readWhole("raw.bin", NOT_UTF8)]);

        // Each crossed the socket: one open and one close apiece, and the
        // 10,000 bytes in three reads of the door's 4,096-byte chunk, each at
        // the offset its predecessor ended.
        const frames = rig().blobFrames().slice(before);
        const opened = new Map(frames.flatMap((f) => (f.type === "BlobOpen" ? [[f.handle, f.name] as const] : [])));
        expect([...opened.values()].sort()).toEqual(["frame.bin", "raw.bin"]);
        const reads = (name: string): [number, number][] =>
            frames.flatMap((f) =>
                f.type === "BlobRead" && opened.get(f.handle) === name ? [[f.offset, f.len] as [number, number]] : [],
            );
        expect(reads("frame.bin")).toEqual([
            [0, 4_096],
            [4_096, 4_096],
            [8_192, 1_808],
        ]);
        expect(reads("raw.bin")).toEqual([[0, 4]]);
        expect(frames.filter((f) => f.type === "BlobClose")).toHaveLength(2);

        // A range read in a later activation, across a chunk boundary.
        const reports = await rig().command(
            "p-blobs",
            "open feed:0 frame.bin; read 0 4090 12; size 0; type 0; open feed:0 raw.bin; read 1 0 4",
        );
        expect(blobsReport(only(reports, "p-blobs")).results).toEqual([
            "blob:0",
            `hex:${hexOf(several.slice(4_090, 4_102))}`,
            "10000",
            OCTETS,
            "blob:1",
            `hex:${hexOf(NOT_UTF8)}`,
        ]);
    });

    it("states the page's limits, and a guest's drop frees its handle for the next open", async () => {
        expect(await arrive([attachment("small.bin", NOT_UTF8)])).toEqual([readWhole("small.bin", NOT_UTF8)]);
        // The 65th live handle is refused; `drop` reaches the kernel through the
        // glue's dispose, and its slot opens the next.
        const reports = await rig().command(
            "p-blobs",
            "limits; repeat 64 open feed:0 small.bin; open feed:0 small.bin; drop 0; open feed:0 small.bin",
        );
        expect(blobsReport(only(reports, "p-blobs")).results).toEqual([
            "limits:1048576:1048576",
            "blob*64",
            "quota-exceeded",
            "ok",
            "blob:64",
        ]);
    });

    it("answers not-found for an attachment the server no longer retains", async () => {
        rig().refuseNextOpen();
        expect(await arrive([attachment("gone.bin", NOT_UTF8)])).toEqual(["gone.bin:not-found"]);
    });

    it("answers unavailable for an attachment whose read failed", async () => {
        rig().failNextRead();
        expect(await arrive([attachment("bad.bin", NOT_UTF8)])).toEqual(["bad.bin:unavailable"]);
    });

    it("opens an attachment over the fetch bound, reads it unavailable and never fetches it", async () => {
        const over = pattern(rig().facts.blobs_fetch_bytes + 1);
        const before = rig().blobFrames().length;
        expect(await arrive([attachment("big.bin", over), attachment("small.bin", NOT_UTF8)])).toEqual([
            "big.bin:unavailable",
            readWhole("small.bin", NOT_UTF8),
        ]);
        const opened = rig()
            .blobFrames()
            .slice(before)
            .flatMap((f) => (f.type === "BlobOpen" ? [f.name] : []));
        expect(opened).toEqual(["small.bin"]);
    });

    it("reads an attachment's bytes in a sync activation a call wakes", async () => {
        const bytes = pattern(5_000);
        expect(await arrive([attachment("frame.bin", bytes)])).toEqual([readWhole("frame.bin", bytes)]);
        // The reply is the request window's script run against `feed`, which
        // only the `ask` activation has: the read ran in the sync activation.
        expect(await ask("open feed:0 frame.bin;read 0 4093 6")).toEqual([
            "blob:0",
            `hex:${hexOf(bytes.slice(4_093, 4_099))}`,
        ]);
    });

    it("answers a sync activation's reads with typed refusals and does not trap", async () => {
        const over = pattern(rig().facts.blobs_fetch_bytes + 1);
        expect(await arrive([attachment("big.bin", over)])).toEqual(["big.bin:unavailable"]);
        expect(await ask("open feed:0 big.bin;read 0 0 8")).toEqual(["blob:0", "unavailable"]);
        // The request's own window is not a window a handle opens on.
        expect(await ask("open-raw ask 0 any")).toEqual(["not-permitted"]);
        expect(rig().status().find((row) => row.instance === "p-blobs")?.state).toBe("mounted");
    });
});
