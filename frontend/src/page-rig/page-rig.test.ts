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
import { readClientFrame } from "./peer.js";
import { expectedState, generatedState } from "./state.js";

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
/**
 * What a 0.2.0 guest is told when it publishes a typed body on a port bound to
 * a channel that leaves the page.
 */
const WIRE_DIAGNOSTIC =
    "this port is bound to a channel that leaves the page, and this brenn's page carries " +
    "only untyped text off the page: publish without a content type, or bind the port to a " +
    "page-local channel";

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

    it("reads every client frame sample the kit emits", () => {
        // Every value, not only the type: an optional field read under a name
        // Rust no longer writes would come back null or empty, not throw.
        const frames = rig().kit.client_samples.map((sample) =>
            readClientFrame(JSON.stringify(sample)),
        );
        expect(frames).toEqual([
            { type: "Hello" },
            { type: "Subscribe", channel: "ephemeral:rig.in.keeper" },
            {
                type: "Publish",
                attribution: "keeper",
                channel: "ephemeral:rig.report",
                body: "{}",
                correlation: 1,
            },
            {
                type: "PublishBatch",
                attribution: "keeper",
                correlation: 2,
                publishes: [{ channel: "ephemeral:rig.report", body: "{}" }],
                deferredOps: [expect.anything()],
            },
        ]);
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

    it("refuses a typed body on a wire-bound port with the diagnostic, and sends nothing", async () => {
        const out = rig().facts.bytes_out_channel;
        const refused = only(
            await rig().command("b-keeper", `publish out text/csv ${hexOf(CSV)}`),
            "b-keeper",
        );
        expect(refused.results).toEqual([`invalid-body:${WIRE_DIAGNOSTIC}`]);
        expect(rig().wire(out)).toEqual([]);
    });

    it("sends an untyped body on a wire-bound port as a 0.1.0 kind would", async () => {
        const out = rig().facts.bytes_out_channel;
        const sent = only(await rig().command("b-keeper", `publish out - ${hexOf(TEXT)}`), "b-keeper");
        expect(sent.results).toEqual(["ok"]);
        const wire = rig().wire(out);
        expect(wire.map((p) => [p.frame, p.attribution])).toEqual([["PublishBatch", "b-keeper"]]);
        expectSameBytes((wire[0] as { body: string }).body, TEXT);
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

    it("delivers a wire row to a 0.2.0 kind with no content type", async () => {
        const channel = rig().facts.bytes_feed_channel;
        const feed = bytesWindowOf(only(await rig().deliver(channel, TEXT), "b-keeper"), "feed");
        expect(feed.envelopes).toHaveLength(1);
        expect(feed.envelopes[0]?.content_type).toBeNull();
        expect(feed.envelopes[0]?.hex).toBe(hexOf(TEXT));
        expect(feed.envelopes[0]?.header["channel"]).toBe(channel);
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
