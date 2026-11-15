// @vitest-environment node
//
// The page rig's `make` page: the production kernel bundle, the production
// `surface.ts` and the transpiled `processor-blobs-test` kind, granted
// `blobs`, behind a `Welcome` that opens a blob door with upload, against the
// scripted attach peer taking every upload it is sent.
//
// `p-maker` runs each script on `in`: it makes blobs and publishes them on
// the wire channel behind `a` and the page-local channel behind `b`, and opens
// what the peer delivers to its `feed`, which never wakes it, to forward it.
// `p-reader` is woken by the page-local channel and reads every attachment of
// it whole. `p-spent` is the instance a spent writer's trap ends.
//
// One page for the file. Cases run in order and share it; the two that end an
// instance run last.

import { afterAll, describe, expect, it, vi } from "vitest";
import { expectConsoleError } from "../test-setup.js";
import { type Page, blobsReport, only, openPage, scriptReport } from "./page.js";
import { type BlobFrame, type CreateRefusal, type WirePublish, hexOf, textOf } from "./peer.js";
import { fnv1a64 } from "./state.js";

const OCTETS = "application/octet-stream";

/** Bytes that are not UTF-8. */
const NOT_UTF8 = new Uint8Array([0xff, 0x00, 0xfe, 0x80]);

/** The bytes `processor-blobs-test`'s `fill <w> <offset> <len>` writes. */
function pattern(offset: number, len: number): Uint8Array {
    const bytes = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
        bytes[i] = ((offset + i) * 31 + 7) % 251;
    }
    return bytes;
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

/** `p-maker`'s publishes on the wire channel behind `a`, oldest first. */
function made(): WirePublish[] {
    return rig().wire(rig().facts.made_channel);
}

/** The blob frames the kernel sent after the first `from`. */
function blobFramesFrom(from: number): BlobFrame[] {
    return rig().blobFrames().slice(from);
}

/** The kinds of `frames`, in order. */
function kinds(frames: BlobFrame[]): string[] {
    return frames.map((f) => f.type);
}

/** `instance`, of `processor-blobs-test`, runs `script`; its one report's result words. */
async function run(instance: string, script: string): Promise<string[]> {
    const reports = await rig().command(instance, script);
    return scriptReport(only(reports, instance)).results;
}

/** A script that makes a 100-byte blob and publishes it on `a` as `doomed`. */
const MAKE_AND_PUBLISH = `create ${OCTETS}; fill 0 0 100; seal 0; publish a doomed blob=0`;

describe("page rig: a component makes and forwards blobs through the page's upload door", () => {
    it("brings every instance up through bootstrap, behind a door with upload", async () => {
        page = await openPage("make");
        const instances = page.facts.instances.map((i) => i.instance);
        expect(instances).toEqual(expect.arrayContaining(["p-maker", "p-spent", "p-reader"]));
        expect(page.status().map((row) => [row.instance, row.state])).toEqual(
            instances.map((i) => [i, "mounted"]),
        );
        expect(scriptReport(only(page.reports("p-maker"), "p-maker")).results).toEqual([]);
    });

    it("uploads a made blob of several chunks, not UTF-8, and the peer receives it byte for byte", async () => {
        const from = blobFramesFrom(0).length;
        expect(
            await run("p-maker", `create ${OCTETS}; fill 0 0 10000; seal 0; publish a made blob=0 again=0`),
        ).toEqual(["writer:0", "ok", "blob:0", "ok"]);

        // One create for the blob named twice, its 10,000 bytes in order in the
        // door's 4,096-byte chunks, the seal, and the close once the batch
        // naming it was answered.
        const frames = blobFramesFrom(from);
        expect(kinds(frames)).toEqual(["BlobCreate", "BlobWrite", "BlobWrite", "BlobWrite", "BlobSeal", "BlobClose"]);
        const [create] = frames;
        expect(create).toMatchObject({ type: "BlobCreate", contentType: OCTETS, size: 10_000 });
        expect(
            frames.flatMap((f) => (f.type === "BlobWrite" ? [[f.offset, f.bytes.length] as [number, number]] : [])),
        ).toEqual([
            [0, 4_096],
            [4_096, 4_096],
            [8_192, 1_808],
        ]);
        expect(new Set(frames.map((f) => f.handle)).size).toBe(1);

        const [publish] = made().slice(-1);
        expect(publish?.frame).toBe("PublishBatch");
        expect(publish?.attribution).toBe("p-maker");
        expect(publish === undefined ? null : textOf(publish.body)).toBe("made");
        const bytes = pattern(0, 10_000);
        expect(publish?.attachments).toEqual([
            { name: "blob", bytes, contentType: OCTETS },
            { name: "again", bytes, contentType: OCTETS },
        ]);
    });

    it("uploads two distinct made blobs of one flush, each create ahead of its own chunks", async () => {
        const from = blobFramesFrom(0).length;
        expect(
            await run(
                "p-maker",
                `create ${OCTETS}; fill 0 0 5000; seal 0; create ${OCTETS}; fill 1 0 6000; seal 1; ` +
                    "publish a two first=0 second=1",
            ),
        ).toEqual(["writer:0", "ok", "blob:0", "writer:1", "ok", "blob:1", "ok"]);

        // Each upload's chunks and seal follow its own create, never the other's.
        const frames = blobFramesFrom(from);
        expect(kinds(frames)).toEqual([
            "BlobCreate",
            "BlobWrite",
            "BlobWrite",
            "BlobSeal",
            "BlobCreate",
            "BlobWrite",
            "BlobWrite",
            "BlobSeal",
            "BlobClose",
            "BlobClose",
        ]);
        const [first, second] = [frames[0], frames[4]];
        expect(first).toMatchObject({ type: "BlobCreate", size: 5_000 });
        expect(second).toMatchObject({ type: "BlobCreate", size: 6_000 });
        expect(frames.slice(1, 4).every((f) => f.handle === first?.handle)).toBe(true);
        expect(frames.slice(5, 8).every((f) => f.handle === second?.handle)).toBe(true);

        const [publish] = made().slice(-1);
        expect(publish === undefined ? null : textOf(publish.body)).toBe("two");
        expect(publish?.attachments).toEqual([
            { name: "first", bytes: pattern(0, 5_000), contentType: OCTETS },
            { name: "second", bytes: pattern(0, 6_000), contentType: OCTETS },
        ]);
    });

    it("forwards a delivered attachment by reference: opened at the flush, nothing written", async () => {
        const photo = pattern(5, 3_000);
        const delivered = await rig().deliverAttached(rig().facts.make_feed_channel, "row", [
            { name: "photo.bin", bytes: photo, contentType: "image/jpeg" },
        ]);
        expect(delivered).toEqual([]);

        const from = blobFramesFrom(0).length;
        expect(await run("p-maker", "open feed:0 photo.bin; publish a fwd photo=0")).toEqual(["blob:0", "ok"]);
        const frames = blobFramesFrom(from);
        expect(kinds(frames)).toEqual(["BlobOpen", "BlobClose"]);
        expect(frames[0]).toMatchObject({ channel: rig().facts.make_feed_channel, name: "photo.bin" });

        const [publish] = made().slice(-1);
        expect(publish === undefined ? null : textOf(publish.body)).toBe("fwd");
        expect(publish?.attachments).toEqual([{ name: "photo", bytes: photo, contentType: "image/jpeg" }]);
    });

    it("keeps a made blob on a page-local channel, read whole and by range, with nothing uploaded", async () => {
        const from = blobFramesFrom(0).length;
        const reports = await rig().command(
            "p-maker",
            `create ${OCTETS}; write 0 0 ${hexOf(NOT_UTF8)}; fill 0 4 5000; seal 0; publish b kept blob=0`,
        );
        expect(scriptReport(only(reports, "p-maker")).results).toEqual(["writer:0", "ok", "ok", "blob:0", "ok"]);
        const bytes = new Uint8Array([...NOT_UTF8, ...pattern(4, 5_000)]);
        expect(blobsReport(only(reports, "p-reader")).arrivals.map((a) => a.attachments)).toEqual([
            [`blob:${bytes.length}:${fnv1a64(bytes)}`],
        ]);

        // A later activation reads a range across the first write's end.
        const read = await rig().command("p-reader", "open feed:0 blob; read 0 2 8; size 0; type 0");
        expect(blobsReport(only(read, "p-reader")).results).toEqual([
            "blob:0",
            `hex:${hexOf(bytes.slice(2, 10))}`,
            String(bytes.length),
            OCTETS,
        ]);
        expect(blobFramesFrom(from)).toEqual([]);
    });

    it("frees a dropped writer's slot: the glue's dispose reaches the kernel", async () => {
        expect(
            await run("p-maker", "repeat 64 create text/plain; create text/plain; drop-writer 0; create text/plain"),
        ).toEqual(["writer*64", "quota-exceeded", "ok", "writer:64"]);
    });

    it("answers each refusal a component can provoke as its own variant, and sends nothing", async () => {
        const published = made().length;
        const from = blobFramesFrom(0).length;
        const many = Array.from({ length: 33 }, (_, i) => `n${i}=0`).join(" ");
        expect(
            await run(
                "p-maker",
                "create nonsense; " +
                    `create ${OCTETS}; fill 0 4 4; seal 0; fill 0 0 1048577; fill 0 0 4; seal 0; ` +
                    `publish a bad !=0; publish a twice x=0 x=0; publish a many ${many}`,
            ),
        ).toEqual([
            "invalid-content-type",
            "writer:0",
            "ok",
            "gap:0:4",
            "too-large",
            "ok",
            "blob:0",
            "invalid-attachment-name",
            "duplicate-attachment-name",
            "too-many-attachments",
        ]);
        expect(blobFramesFrom(from)).toEqual([]);
        expect(made()).toHaveLength(published);
    });

    it.each<[CreateRefusal, string]>([
        ["budget_exhausted", "BudgetExhausted"],
        ["storage_failed", "StorageFailed"],
    ])("loses a flush whose create is refused (%s) whole, announces it and stays up", async (refusal, reason) => {
        const warn = vi.spyOn(console, "warn");
        try {
            rig().refuseNextCreate(refusal);
            const published = made().length;
            const from = blobFramesFrom(0).length;
            // The report rode the batch that was lost with the blob.
            expect(await rig().command("p-maker", MAKE_AND_PUBLISH)).toEqual([]);
            // The bytes already queued behind the create, and the seal the
            // server answers for a handle it never made; no batch.
            const frames = kinds(blobFramesFrom(from));
            expect(frames[0]).toBe("BlobCreate");
            expect(frames.slice(-1)).toEqual(["BlobSeal"]);
            expect(made()).toHaveLength(published);
            const announced = warn.mock.calls.flat().filter((a) => typeof a === "string" && a.includes("was lost"));
            expect(announced).toHaveLength(1);
            expect(announced[0]).toMatch(
                new RegExp(`p-maker: a publish batch on .*${rig().facts.made_channel}.* was lost .*${reason}`),
            );
        } finally {
            warn.mockRestore();
        }
        expect(await run("p-maker", "publish-plain a after")).toEqual(["ok"]);
        const [publish] = made().slice(-1);
        expect(publish === undefined ? null : [textOf(publish.body), publish.attachments]).toEqual(["after", []]);
    });

    it("sends nothing for a blob made and published by an activation that then fails", async () => {
        expectConsoleError(/activation failed on 'p-maker'/);
        const published = made().length;
        const from = blobFramesFrom(0).length;
        expect(await rig().command("p-maker", `${MAKE_AND_PUBLISH}; fail`)).toEqual([]);
        expect(blobFramesFrom(from)).toEqual([]);
        expect(made()).toHaveLength(published);

        expect(await run("p-maker", "publish-plain a after-fail")).toEqual(["ok"]);
        expect(blobFramesFrom(from)).toEqual([]);
        expect(made().map((p) => textOf(p.body)).slice(published)).toEqual(["after-fail"]);
    });

    it("traps a spent writer's write", async () => {
        expectConsoleError(/activation failed on 'p-spent'/);
        expectConsoleError(/instance 'p-spent' failed/);
        expect(await rig().command("p-spent", "spent write")).toEqual([]);
        expect(rig().status().find((row) => row.instance === "p-spent")?.state).toBe("failed");
    });

    it("sends nothing for a blob made and published by an activation that then traps", async () => {
        expectConsoleError(/activation failed on 'p-maker'/);
        expectConsoleError(/instance 'p-maker' failed/);
        const published = made().length;
        const from = blobFramesFrom(0).length;
        expect(await rig().command("p-maker", `${MAKE_AND_PUBLISH}; trap`)).toEqual([]);
        expect(rig().status().find((row) => row.instance === "p-maker")?.state).toBe("failed");

        // The page's next wire publish, another instance's, goes out alone.
        expect(blobsReport(only(await rig().command("p-reader", "limits"), "p-reader")).results).toEqual([
            "limits:1048576:1048576",
        ]);
        expect(blobFramesFrom(from)).toEqual([]);
        expect(made()).toHaveLength(published);
    });
});
