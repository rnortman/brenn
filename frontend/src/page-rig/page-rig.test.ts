// @vitest-environment node
//
// The page rig's gated cases: the production kernel bundle, the production
// `surface.ts` and the transpiled `processor-page-test` kind, brought up by the
// page's own `bootstrap` against the scripted attach peer, in node.
//
// One page for the file. Cases run in order and share it, so each asserts on
// bodies it sent itself and on the reports its own commands produced.

import { afterAll, describe, expect, it } from "vitest";
import { type Page, type Report, only, openPage } from "./page.js";
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

function windowOf(report: Report, port: string): Report["windows"][number] {
    const window = report.windows.find((w) => w.port === port);
    if (window === undefined) {
        throw new Error(`${report.instance}'s report has no '${port}' window`);
    }
    return window;
}

function deferredOf(report: Report, port: string): Report["deferred"][number]["entries"] {
    const window = report.deferred.find((w) => w.port === port);
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
});
