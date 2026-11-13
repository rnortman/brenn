// @vitest-environment node
//
// The page rig's gated cases: the production kernel bundle, the production
// `surface.ts` and the transpiled `processor-page-test` and
// `processor-page-bytes-test` kinds, brought up by the page's own `bootstrap`
// against the scripted attach peer, in node.
//
// One page for the file. Cases run in order and share it, so each asserts on
// bodies it sent itself and on the reports its own commands produced. The case
// that leaves `caller` failed runs last.

import { afterAll, describe, expect, it } from "vitest";
import {
    type BytesReport,
    type Page,
    type Report,
    type StringReport,
    bytesReport,
    only,
    openPage,
    stringReport,
} from "./page.js";
import { expectConsoleError } from "../test-setup.js";
import {
    type Json,
    bytesOfHex,
    fillBinary,
    hexOf as hexOfBytes,
    readClientFrame,
    sampleTemplate,
    textOf,
} from "./peer.js";
import { expectedState, fnv1a64, generatedState, withCounter } from "./state.js";

/**
 * Text a mangled crossing would show: non-ASCII, an astral-plane character, a
 * control character, JSON-significant characters and a line separator.
 */
const TEXT = 'é中😀\u0001"\\{}[]:,</script>\u2028';
const SECOND_TEXT = "second ✓ 𝄞";
const FAR_FUTURE_MS = 4_102_444_800_000;

const hexOf = (text: string): string => Buffer.from(text, "utf8").toString("hex");

/** The envelope a report shows, parsed from the bytes the guest was handed. */
interface Envelope {
    channel: string;
    body: string;
    envelope_type: string;
}

function envelopeOf(hex: string): Envelope {
    return JSON.parse(Buffer.from(hex, "hex").toString("utf8")) as Envelope;
}

/** Byte for byte: the UTF-8 bytes of `body` are those of `sent`. */
function expectSameBytes(body: string, sent: string): void {
    expect(Buffer.from(body, "utf8").equals(Buffer.from(sent, "utf8"))).toBe(true);
}

function windowOf(report: Report, port: string): StringReport["windows"][number] {
    const window = stringReport(report).windows.find((w) => w.port === port);
    if (window === undefined) {
        throw new Error(`${report.instance}'s report has no '${port}' window`);
    }
    return window;
}

function bytesWindowOf(report: Report, port: string): BytesReport["windows"][number] {
    const window = bytesReport(report).windows.find((w) => w.port === port);
    if (window === undefined) {
        throw new Error(`${report.instance}'s report has no '${port}' window`);
    }
    return window;
}

/** A string-contract envelope as JSON, every field it carries. */
function envelopeFields(hex: string): Record<string, unknown> {
    return JSON.parse(Buffer.from(hex, "hex").toString("utf8")) as Record<string, unknown>;
}

/** Typed text a string-contract reader must see as bare text. */
const CSV = "a,b\n1,2\né\n";
/** Bytes that are not UTF-8, in hex. */
const NOT_UTF8 = "ff00fe";
/** `text` as a body typed `contentType`. */
function typed(contentType: string, text: string): { bytes: Uint8Array; contentType: string } {
    return { bytes: new Uint8Array(Buffer.from(text, "utf8")), contentType };
}

function bytesDeferredOf(
    report: Report,
    port: string,
): BytesReport["deferred"][number]["entries"] {
    const window = bytesReport(report).deferred.find((w) => w.port === port);
    if (window === undefined) {
        throw new Error(`${report.instance}'s report has no '${port}' deferred window`);
    }
    return window.entries;
}

function deferredOf(report: Report, port: string): StringReport["deferred"][number]["entries"] {
    const window = stringReport(report).deferred.find((w) => w.port === port);
    if (window === undefined) {
        throw new Error(`${report.instance}'s report has no '${port}' deferred window`);
    }
    return window.entries;
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

describe("page rig: kernel bundle, surface.ts and a transpiled kind in node", () => {
    it("brings every instance up through bootstrap", async () => {
        page = await openPage("cases");
        const instances = page.facts.instances.map((i) => i.instance);
        for (const instance of instances) {
            const [mount] = page.reports(instance);
            expect(mount?.results).toEqual([]);
            for (const window of mount?.windows ?? []) {
                expect(window.envelopes).toEqual([]);
            }
        }
        expect(
            page.status().map((row) => [row.instance, row.state]),
        ).toEqual(instances.map((i) => [i, "mounted"]));
    });

    it("hands a retained page-local body back to its publisher, byte for byte", async () => {
        const published = await rig().command("keeper", `publish memo ${hexOf(TEXT)}`);
        expect(only(published, "keeper").results).toEqual(["ok"]);

        const next = only(await rig().command("keeper", "noop"), "keeper");
        const memo = windowOf(next, "memo");
        expect(memo.envelopes).toHaveLength(1);
        const envelope = envelopeOf(memo.envelopes[0] as string);
        expect(envelope.channel).toBe("local:rig/memo");
        expect(envelope.envelope_type).toBe("local");
        expectSameBytes(envelope.body, TEXT);
    });

    it("activates a second reader of the page-local channel with the same body", () => {
        const reader = rig().reports("reader");
        const woken = reader[reader.length - 1] as Report;
        expect(reader).toHaveLength(2);
        const feed = windowOf(woken, "feed");
        expect(feed.envelopes).toHaveLength(1);
        expect(feed.new_from).toBe(0);
        const envelope = envelopeOf(feed.envelopes[0] as string);
        expect(envelope.channel).toBe("local:rig/memo");
        expectSameBytes(envelope.body, TEXT);
    });

    it("shows, edits and cancels a parked publish in the deferred view", async () => {
        const parked = await rig().command(
            "keeper",
            `defer timer ${hexOf(TEXT)} ${FAR_FUTURE_MS}`,
        );
        expect(only(parked, "keeper").results).toEqual(["ok"]);

        const viewed = only(await rig().command("keeper", "noop"), "keeper");
        const entries = deferredOf(viewed, "timer");
        expect(entries.map((e) => [e.index, e.deliver_after])).toEqual([[0, FAR_FUTURE_MS]]);
        expect(entries[0]?.hex).toBe(hexOf(TEXT));

        const edited = only(
            await rig().command("keeper", `edit timer 0 ${hexOf(SECOND_TEXT)}`),
            "keeper",
        );
        expect(edited.results).toEqual(["ok"]);
        const afterEdit = deferredOf(only(await rig().command("keeper", "noop"), "keeper"), "timer");
        expect(afterEdit.map((e) => [e.index, e.hex, e.deliver_after])).toEqual([
            [0, hexOf(SECOND_TEXT), FAR_FUTURE_MS],
        ]);

        const cancelled = only(await rig().command("keeper", "cancel timer 0"), "keeper");
        expect(cancelled.results).toEqual(["ok"]);
        const afterCancel = only(await rig().command("keeper", "noop"), "keeper");
        expect(deferredOf(afterCancel, "timer")).toEqual([]);

        for (const report of rig().reports("keeper")) {
            expect(windowOf(report, "timer").envelopes).toEqual([]);
        }
    });

    it("carries a sync call's request to the callee and its reply back to the caller", async () => {
        const reports = await rig().command("keeper", `call peer ${hexOf(TEXT)}`);
        expect(reports.map((r) => r.instance)).toEqual(["responder", "keeper"]);

        const responder = only(reports, "responder");
        expect(responder.sync).toBe("ask");
        expect(responder.results).toEqual([]);
        const ask = windowOf(responder, "ask");
        expect(ask.envelopes).toHaveLength(1);
        expectSameBytes(envelopeOf(ask.envelopes[0] as string).body, TEXT);

        expect(only(reports, "keeper").results).toEqual([`ok:${hexOf(TEXT)}`]);
    });

    it("refuses an oversize publish with its variant and leaves the instance running", async () => {
        const cap = rig().facts.max_body_bytes;
        const refused = only(
            await rig().command("keeper", `publish-fill memo ${cap + 1}`),
            "keeper",
        );
        expect(refused.results).toEqual(["invalid-payload"]);

        const after = only(await rig().command("keeper", "noop"), "keeper");
        expect(after.results).toEqual(["ok"]);
        const memo = windowOf(after, "memo");
        expect(memo.envelopes).toHaveLength(1);
        expectSameBytes(envelopeOf(memo.envelopes[0] as string).body, TEXT);
        expect(rig().status().find((row) => row.instance === "keeper")?.state).toBe("mounted");
    });

    it("carries a wire delivery into the subscriber's window, byte for byte", async () => {
        const reports = await rig().deliver(rig().facts.feed_channel, TEXT);
        const feed = windowOf(only(reports, "responder"), "feed");
        expect(feed.envelopes).toHaveLength(1);
        const envelope = envelopeOf(feed.envelopes[0] as string);
        expect(envelope.channel).toBe(rig().facts.feed_channel);
        expect(envelope.envelope_type).toBe("ephemeral");
        expectSameBytes(envelope.body, TEXT);
    });

    it("runs the base64 state workload through the retained state port", async () => {
        expect(rig().facts.state_instances).toEqual(["stater"]);
        const results: string[] = [];
        for (let i = 0; i < 3; i++) {
            const report = only(await rig().command("stater", "state 1024"), "stater");
            results.push(...report.results);
        }
        const generated = generatedState(1024);
        expect(results).toEqual([0, 1, 2].map((steps) => expectedState(generated, steps)));
        for (const report of rig().reports("stater")) {
            expect(report.windows).toEqual([]);
        }
    });

    it("keeps the guest SDK's binary state on a non_text page-local io port, stepped twice", async () => {
        expect(rig().facts.sdk_state_instances).toEqual(["s-stater"]);
        const results: string[] = [];
        for (let i = 0; i < 3; i++) {
            const report = bytesReport(only(await rig().command("s-stater", "state 1024"), "s-stater"));
            results.push(...report.results);
        }
        const generated = generatedState(1024);
        expect(results).toEqual([0, 1, 2].map((steps) => expectedState(generated, steps)));
    });

    it("reads every client frame sample the kit emits", () => {
        // Every value, not only the type: an optional field read under a name
        // Rust no longer writes would come back null or empty, not throw.
        const frames = rig().kit.client_samples.map((sample) =>
            readClientFrame("text" in sample ? sample.text : bytesOfHex(sample.hex)),
        );
        const body = (hex: string, contentType: string | null) => ({
            bytes: bytesOfHex(hex),
            contentType,
        });
        const far = 4_102_444_800_000;
        expect(frames).toEqual([
            { type: "Hello" },
            { type: "Subscribe", channel: "ephemeral:rig.in.keeper" },
            {
                type: "Publish",
                attribution: "keeper",
                channel: "ephemeral:rig.report",
                body: body(hexOf("a,b"), "text/csv"),
                correlation: 1,
            },
            {
                type: "PublishBatch",
                attribution: "b-keeper",
                correlation: 2,
                publishes: [
                    { channel: "ephemeral:rig.report", body: body(hexOf("{}"), null), deliverAfter: null },
                    {
                        channel: "ephemeral:rig.bytes-out",
                        body: body(hexOf("# é"), "text/markdown"),
                        deliverAfter: null,
                    },
                    {
                        channel: "ephemeral:rig.bytes-feed",
                        body: body("ff00", "application/octet-stream"),
                        deliverAfter: far,
                    },
                ],
                deferredOps: [
                    {
                        channel: "ephemeral:rig.bytes-feed",
                        messageId: "00000000-0000-0000-0000-000000000001",
                        kind: "edit",
                        body: body("ffd8", "image/jpeg"),
                        deliverAfter: far + 1,
                    },
                    {
                        channel: "ephemeral:rig.bytes-feed",
                        messageId: "00000000-0000-0000-0000-000000000002",
                        kind: "edit",
                        body: null,
                        deliverAfter: far + 2,
                    },
                    {
                        channel: "ephemeral:rig.feed",
                        messageId: "00000000-0000-0000-0000-000000000003",
                        kind: "cancel",
                        body: null,
                        deliverAfter: null,
                    },
                ],
            },
        ]);
    });

    it("assembles each binary server sample byte for byte from its template", () => {
        const kit = rig().kit;
        expect(kit.server_samples.length).toBeGreaterThan(0);
        for (const sample of kit.server_samples) {
            const bodies = sample.bodies.map((b) => ({
                bytes: bytesOfHex(b.hex),
                contentType: b.content_type,
            }));
            const values: Record<string, Json> = sample.values;
            expect(hexOfBytes(fillBinary(sampleTemplate(kit, sample), values, bodies))).toBe(sample.hex);
        }
    });

    it("keeps a typed body on a page-local channel and hands it back with its type", async () => {
        const published = only(
            await rig().command("b-keeper", `publish memo text/csv ${hexOf(CSV)}`),
            "b-keeper",
        );
        expect(published.results).toEqual(["ok"]);

        const memo = bytesWindowOf(only(await rig().command("b-keeper", "noop"), "b-keeper"), "memo");
        expect(memo.envelopes).toHaveLength(1);
        expect(memo.envelopes[0]?.content_type).toBe("text/csv");
        expect(memo.envelopes[0]?.hex).toBe(hexOf(CSV));
        expect(memo.envelopes[0]?.header["channel"]).toBe(rig().facts.shared_channel);
    });

    it("hands a string-contract reader the typed body as bare text", () => {
        const plain = rig().reports("plain");
        expect(plain).toHaveLength(2);
        const feed = windowOf(plain[1] as Report, "feed");
        expect(feed.envelopes).toHaveLength(1);
        const envelope = envelopeFields(feed.envelopes[0] as string);
        expect(envelope["channel"]).toBe(rig().facts.shared_channel);
        expectSameBytes(envelope["body"] as string, CSV);
        expect("content_type" in envelope).toBe(false);
    });

    it("carries a non-UTF-8 call request and its reply between two 0.2.0 kinds", async () => {
        const reports = await rig().command(
            "b-keeper",
            `call peer application/octet-stream ${NOT_UTF8}`,
        );
        expect(reports.map((r) => r.instance)).toEqual(["b-responder", "b-keeper"]);

        const responder = only(reports, "b-responder");
        expect(responder.sync).toBe("ask");
        const ask = bytesWindowOf(responder, "ask");
        expect(ask.envelopes.map((e) => [e.content_type, e.hex])).toEqual([
            ["application/octet-stream", NOT_UTF8],
        ]);
        expect(only(reports, "b-keeper").results).toEqual([
            `ok:application/octet-stream:${NOT_UTF8}`,
        ]);
    });

    it("refuses a non-text body on a page-local channel as text-only-channel", async () => {
        const plainBefore = rig().reports("plain").length;
        const refused = only(
            await rig().command("b-keeper", "publish memo image/jpeg ffd8ff"),
            "b-keeper",
        );
        expect(refused.results).toEqual(["text-only-channel"]);

        const memo = bytesWindowOf(only(await rig().command("b-keeper", "noop"), "b-keeper"), "memo");
        expect(memo.envelopes.map((e) => [e.content_type, e.hex])).toEqual([["text/csv", hexOf(CSV)]]);
        expect(rig().reports("plain")).toHaveLength(plainBefore);
    });

    it("refuses a non-text deferred publish and a non-text edit on an undeclared channel", async () => {
        const deferred = only(
            await rig().command("b-keeper", `defer memo application/octet-stream ${NOT_UTF8} ${FAR_FUTURE_MS}`),
            "b-keeper",
        );
        expect(deferred.results).toEqual(["text-only-channel"]);

        const parked = only(
            await rig().command("b-keeper", `defer memo text/csv ${hexOf(CSV)} ${FAR_FUTURE_MS}`),
            "b-keeper",
        );
        expect(parked.results).toEqual(["ok"]);
        const edited = only(
            await rig().command("b-keeper", `edit memo 0 application/octet-stream ${NOT_UTF8}`),
            "b-keeper",
        );
        expect(edited.results).toEqual(["text-only-channel"]);
        const viewed = only(await rig().command("b-keeper", "noop"), "b-keeper");
        expect(bytesDeferredOf(viewed, "memo").map((e) => [e.content_type, e.hex])).toEqual([
            ["text/csv", hexOf(CSV)],
        ]);

        const cancelled = only(await rig().command("b-keeper", "cancel memo 0"), "b-keeper");
        expect(cancelled.results).toEqual(["ok"]);
    });

    it("keeps a non-UTF-8 body on a channel declared non_text and hands it back with its type", async () => {
        const published = only(
            await rig().command("b-keeper", `publish timer application/octet-stream ${NOT_UTF8}`),
            "b-keeper",
        );
        expect(published.results).toEqual(["ok"]);

        const timer = bytesWindowOf(only(await rig().command("b-keeper", "noop"), "b-keeper"), "timer");
        expect(timer.envelopes.map((e) => [e.content_type, e.hex])).toEqual([
            ["application/octet-stream", NOT_UTF8],
        ]);
        expect(timer.envelopes[0]?.header["channel"]).toBe(rig().facts.bytes_timer_channel);
    });

    it("parks, edits and cancels a non-UTF-8 body on a channel declared non_text", async () => {
        const parked = only(
            await rig().command("b-keeper", `defer timer application/octet-stream ${NOT_UTF8} ${FAR_FUTURE_MS}`),
            "b-keeper",
        );
        expect(parked.results).toEqual(["ok"]);
        const viewed = only(await rig().command("b-keeper", "noop"), "b-keeper");
        expect(
            bytesDeferredOf(viewed, "timer").map((e) => [e.index, e.content_type, e.hex, e.deliver_after]),
        ).toEqual([[0, "application/octet-stream", NOT_UTF8, FAR_FUTURE_MS]]);

        const edited = only(await rig().command("b-keeper", "edit timer 0 image/jpeg ffd8ff"), "b-keeper");
        expect(edited.results).toEqual(["ok"]);
        const afterEdit = only(await rig().command("b-keeper", "noop"), "b-keeper");
        expect(
            bytesDeferredOf(afterEdit, "timer").map((e) => [e.index, e.content_type, e.hex, e.deliver_after]),
        ).toEqual([[0, "image/jpeg", "ffd8ff", FAR_FUTURE_MS]]);

        const cancelled = only(await rig().command("b-keeper", "cancel timer 0"), "b-keeper");
        expect(cancelled.results).toEqual(["ok"]);
        const afterCancel = only(await rig().command("b-keeper", "noop"), "b-keeper");
        expect(bytesDeferredOf(afterCancel, "timer")).toEqual([]);
    });

    it("refuses an untyped body that is not UTF-8 as invalid-body with its reason", async () => {
        const refused = only(await rig().command("b-keeper", "publish memo - ff"), "b-keeper");
        expect(refused.results).toEqual(["invalid-body:untyped body is not valid UTF-8"]);
    });

    it("carries a typed text body on a wire-bound port with its type", async () => {
        const out = rig().facts.bytes_out_channel;
        const sent = only(
            await rig().command("b-keeper", `publish out text/csv ${hexOf(CSV)}`),
            "b-keeper",
        );
        expect(sent.results).toEqual(["ok"]);
        const wire = rig().wire(out);
        expect(wire.map((p) => [p.frame, p.attribution])).toEqual([["PublishBatch", "b-keeper"]]);
        expect(wire[0]?.body.contentType).toBe("text/csv");
        expectSameBytes(textOf(wire[0]?.body ?? { bytes: new Uint8Array(), contentType: null }), CSV);
    });

    it("sends an untyped body on a wire-bound port as a 0.1.0 kind would", async () => {
        const out = rig().facts.bytes_out_channel;
        const before = rig().wire(out).length;
        const sent = only(await rig().command("b-keeper", `publish out - ${hexOf(TEXT)}`), "b-keeper");
        expect(sent.results).toEqual(["ok"]);
        const wire = rig().wire(out).slice(before);
        expect(wire.map((p) => [p.frame, p.attribution])).toEqual([["PublishBatch", "b-keeper"]]);
        expect(wire[0]?.body.contentType).toBeNull();
        expectSameBytes(textOf(wire[0]?.body ?? { bytes: new Uint8Array(), contentType: null }), TEXT);
    });

    it("delivers a non-UTF-8 typed row on a non_text wire channel to a 0.2.0 kind, byte for byte", async () => {
        const channel = rig().facts.bytes_feed_channel;
        const feed = bytesWindowOf(
            only(
                await rig().deliver(channel, {
                    bytes: bytesOfHex(NOT_UTF8),
                    contentType: "application/octet-stream",
                }),
                "b-keeper",
            ),
            "feed",
        );
        expect(feed.envelopes.map((e) => [e.content_type, e.hex])).toEqual([
            ["application/octet-stream", NOT_UTF8],
        ]);
        expect(feed.envelopes[0]?.header["channel"]).toBe(channel);
    });

    it("publishes a non-UTF-8 typed body on a non_text wire channel, byte for byte", async () => {
        const channel = rig().facts.bytes_wire_channel;
        const sent = only(
            await rig().command("b-keeper", `publish bout image/jpeg ${NOT_UTF8}`),
            "b-keeper",
        );
        expect(sent.results).toEqual(["ok"]);
        const wire = rig().wire(channel);
        expect(wire.map((p) => [p.frame, p.attribution, p.body.contentType, hexOfBytes(p.body.bytes)])).toEqual([
            ["PublishBatch", "b-keeper", "image/jpeg", NOT_UTF8],
        ]);
    });

    it("parks, edits and cancels a non-UTF-8 body on a non_text wire channel through the peer", async () => {
        const channel = rig().facts.bytes_wire_channel;
        const held = () =>
            rig()
                .parked(channel, "b-keeper")
                .map((m) => [m.body.contentType, hexOfBytes(m.body.bytes), m.deliverAfter]);
        const viewed = async () =>
            bytesDeferredOf(only(await rig().command("b-keeper", "noop"), "b-keeper"), "bout").map((e) => [
                e.content_type,
                e.hex,
                e.deliver_after,
            ]);

        const parked = only(
            await rig().command("b-keeper", `defer bout application/octet-stream ${NOT_UTF8} ${FAR_FUTURE_MS}`),
            "b-keeper",
        );
        expect(parked.results).toEqual(["ok"]);
        expect(held()).toEqual([["application/octet-stream", NOT_UTF8, FAR_FUTURE_MS]]);
        expect(await viewed()).toEqual(held());

        const edited = only(await rig().command("b-keeper", "edit bout 0 image/png 89504e47ff"), "b-keeper");
        expect(edited.results).toEqual(["ok"]);
        expect(held()).toEqual([["image/png", "89504e47ff", FAR_FUTURE_MS]]);
        expect(await viewed()).toEqual(held());

        const cancelled = only(await rig().command("b-keeper", "cancel bout 0"), "b-keeper");
        expect(cancelled.results).toEqual(["ok"]);
        expect(held()).toEqual([]);
        expect(await viewed()).toEqual([]);
        expect(rig().wire(channel)).toHaveLength(1);
    });

    it("emits the workload with its counter on a non_text wire channel, byte for byte", async () => {
        const channel = rig().facts.bytes_wire_channel;
        const before = rig().wire(channel).length;
        const expected = withCounter(generatedState(64), 5);
        const sent = only(await rig().command("b-keeper", "emit bout 64 5"), "b-keeper");
        expect(sent.results).toEqual([`emit:64:${fnv1a64(expected)}`]);
        const wire = rig().wire(channel).slice(before);
        expect(wire.map((p) => [p.frame, p.attribution, p.body.contentType, hexOfBytes(p.body.bytes)])).toEqual([
            ["PublishBatch", "b-keeper", "application/octet-stream", hexOfBytes(expected)],
        ]);
    });

    it("fails an emit under the counter's four bytes without publishing, and keeps the instance", async () => {
        const channel = rig().facts.bytes_wire_channel;
        const before = rig().wire(channel).length;
        expectConsoleError(/activation failed on 'b-keeper': .*emit: raw-len 3 is under 4/);
        const reports = await rig().command("b-keeper", "emit bout 3 1");
        expect(reports.map((r) => r.instance)).not.toContain("b-keeper");
        expect(rig().wire(channel)).toHaveLength(before);
        expect(rig().status().find((row) => row.instance === "b-keeper")?.state).toBe("mounted");
    });

    it("refuses a non-text body on a text-only wire channel, published, emitted, parked or edited", async () => {
        const out = rig().facts.bytes_out_channel;
        const wireBefore = rig().wire(out).length;
        const refused = only(await rig().command("b-keeper", `publish out image/jpeg ${NOT_UTF8}`), "b-keeper");
        expect(refused.results).toEqual(["text-only-channel"]);
        const emitted = only(await rig().command("b-keeper", "emit out 64 1"), "b-keeper");
        expect(emitted.results).toEqual(["text-only-channel"]);
        const deferred = only(
            await rig().command("b-keeper", `defer out application/octet-stream ${NOT_UTF8} ${FAR_FUTURE_MS}`),
            "b-keeper",
        );
        expect(deferred.results).toEqual(["text-only-channel"]);
        expect(rig().parked(out, "b-keeper")).toEqual([]);

        const parked = only(
            await rig().command("b-keeper", `defer out text/csv ${hexOf(CSV)} ${FAR_FUTURE_MS}`),
            "b-keeper",
        );
        expect(parked.results).toEqual(["ok"]);
        const edited = only(
            await rig().command("b-keeper", `edit out 0 application/octet-stream ${NOT_UTF8}`),
            "b-keeper",
        );
        expect(edited.results).toEqual(["text-only-channel"]);
        const kept = rig().parked(out, "b-keeper");
        expect(kept.map((m) => [m.body.contentType, hexOfBytes(m.body.bytes)])).toEqual([["text/csv", hexOf(CSV)]]);
        const viewed = only(await rig().command("b-keeper", "noop"), "b-keeper");
        expect(bytesDeferredOf(viewed, "out").map((e) => [e.content_type, e.hex])).toEqual([
            ["text/csv", hexOf(CSV)],
        ]);

        const cancelled = only(await rig().command("b-keeper", "cancel out 0"), "b-keeper");
        expect(cancelled.results).toEqual(["ok"]);
        expect(rig().parked(out, "b-keeper")).toEqual([]);
        expect(rig().wire(out)).toHaveLength(wireBefore);
    });

    it("refuses a non-text call to a 0.1.0 callee as text-only-peer, without running it", async () => {
        const responderBefore = rig().reports("responder").length;
        const reports = await rig().command(
            "b-mixed",
            `call peer application/octet-stream ${NOT_UTF8}`,
        );
        expect(reports.map((r) => r.instance)).toEqual(["b-mixed"]);
        expect(only(reports, "b-mixed").results).toEqual(["text-only-peer"]);
        expect(rig().reports("responder")).toHaveLength(responderBefore);
    });

    it("hands a 0.1.0 callee a typed text request as bare text", async () => {
        const request = "# heading ✓";
        const reports = await rig().command("b-mixed", `call peer text/markdown ${hexOf(request)}`);
        expect(reports.map((r) => r.instance)).toEqual(["responder", "b-mixed"]);
        const ask = windowOf(only(reports, "responder"), "ask");
        const envelope = envelopeFields(ask.envelopes[0] as string);
        expectSameBytes(envelope["body"] as string, request);
        expect("content_type" in envelope).toBe(false);
        expect(only(reports, "b-mixed").results).toEqual([`ok:-:${hexOf(request)}`]);
    });

    it("delivers a typed text row to a 0.2.0 kind with its type", async () => {
        const channel = rig().facts.bytes_feed_channel;
        const feed = bytesWindowOf(
            only(await rig().deliver(channel, typed("text/csv", CSV)), "b-keeper"),
            "feed",
        );
        expect(feed.envelopes).toHaveLength(1);
        expect(feed.envelopes[0]?.content_type).toBe("text/csv");
        expect(feed.envelopes[0]?.hex).toBe(hexOf(CSV));
        expect(feed.envelopes[0]?.header["channel"]).toBe(channel);
    });

    it("delivers an untyped wire row to a 0.2.0 kind with no content type", async () => {
        const channel = rig().facts.bytes_feed_channel;
        const feed = bytesWindowOf(only(await rig().deliver(channel, TEXT), "b-keeper"), "feed");
        expect(feed.envelopes.map((e) => [e.content_type, e.hex])).toEqual([[null, hexOf(TEXT)]]);
    });

    it("delivers a typed text row to a 0.1.0 kind as bare text", async () => {
        const channel = rig().facts.feed_channel;
        const feed = windowOf(
            only(await rig().deliver(channel, typed("text/csv", CSV)), "responder"),
            "feed",
        );
        expect(feed.envelopes).toHaveLength(1);
        const envelope = envelopeFields(feed.envelopes[0] as string);
        expect(envelope["channel"]).toBe(channel);
        expectSameBytes(envelope["body"] as string, CSV);
        expect("content_type" in envelope).toBe(false);
    });

    it("traps a 0.1.0 caller handed a non-text reply, after the callee ran ok", async () => {
        const trap = "call port peer of caller was answered with a non-text reply";
        expectConsoleError(new RegExp(`activation failed on 'caller': surface client: ${trap}`));
        expectConsoleError(new RegExp(`instance 'caller' failed: surface client: ${trap}`));
        const ask = "reply application/octet-stream ff00";
        const reports = await rig().command("caller", `call peer ${hexOf(ask)}`);
        expect(reports.map((r) => r.instance)).toEqual(["b-responder"]);
        expect(only(reports, "b-responder").results).toEqual(["ok"]);
        const row = rig().status().find((r) => r.instance === "caller");
        expect(row?.state).toBe("failed");
        expect(row?.reason).toMatch(new RegExp(`^surface client: ${trap} \\(application/octet-stream, 2 bytes\\)`));
    });
});
