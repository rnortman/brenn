// The golden activation, frontend half.
//
// `surface/test-fixtures/activation.json` is bytes the surface kernel
// serialized: a `surface/kernel` unit test builds the same activation through
// the real window assembly and pins `serde_json::to_string` against that file.
// This half reads the same file, drives it through the production
// `activationEntry` lift from `surface.ts`, and hands it to a real transpiled
// guest.
//
// The point is the shared artifact. Rust serialization on one side and a
// TypeScript `as` assertion on the other cannot disagree visibly: the assertion
// checks nothing, so `tsc --strict` is blind to it and every unit test on either
// side agrees with itself. One file both sides pin against is what makes the
// disagreement a failure. The seam exists only on the string contract, so the
// guest is a string-contract one.
//
// The guest is `processor-page-test`, the raw `brenn:processor@0.1.0` fixture:
// its report carries every window it was handed — each envelope as the hex of
// the exact string — and every deferred entry, `now` and `sync`, and on a sync
// activation it replies with the request's body. So this asserts the guest read
// back, byte for byte, what the kernel wrote.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { beforeAll, describe, expect, it } from "vitest";
import type { StringReport } from "./page-rig/page.js";
import {
    activationEntry,
    type KernelActivation,
    type ProcessorInstance,
} from "./surface.js";
import {
    REPO_ROOT,
    instantiateTranspiled,
    requireTranspiledTree,
} from "./test-helpers/transpiled-processor.js";

const DIST = "surface/page-rig-dist";
const KIND = "processor-page-test";
const GOLDEN = resolve(REPO_ROOT, "surface/test-fixtures/activation.json");

/** The fixture's report, less the attribution the rig adds. */
type Report = Omit<StringReport, "instance" | "contract">;

/** The body of one envelope, which is all this suite reads out of one. */
interface Envelope {
    body: string;
}

/** Lower-case hex of the UTF-8 bytes of `text`, as the fixture reports strings. */
function hex(text: string): string {
    return Buffer.from(text, "utf8").toString("hex");
}

describe("the golden activation", () => {
    // The bytes exactly as the kernel emitted them: the file carries a trailing
    // newline for the tree's sake and the serialization does not.
    const golden = readFileSync(GOLDEN, "utf8").trimEnd();
    const parsed = JSON.parse(golden) as KernelActivation;

    let reports: Report[];
    let answer: string | { reply: string } | undefined;

    beforeAll(async () => {
        requireTranspiledTree(DIST, KIND);
        reports = [];
        const instance = await instantiateTranspiled<ProcessorInstance>(DIST, KIND, {
            "brenn:processor/calls": {
                call: () => {
                    throw new Error("the golden activation runs no commands");
                },
            },
            "brenn:processor/ports": {
                publish: (port: string, payload: string) => {
                    expect(port).toBe("report");
                    reports.push(JSON.parse(payload) as Report);
                },
                publishWithUrgency: () => {},
                publishDeferred: () => {},
                deferCancel: () => {},
                deferEdit: () => {},
            },
        });
        // No try/catch: a throw here is the production failure mode, and the
        // report a thrown `TypeError` gives is better than any wrapper's.
        answer = activationEntry(instance)(golden);
    });

    it("is a record the declared kernel shape describes", () => {
        // The fixture is the kernel's output, so this is the declared type
        // meeting the real bytes rather than an assertion about the fixture.
        expect(parsed.ports.length).toBeGreaterThan(0);
        for (const window of parsed.ports) {
            for (const envelope of window.envelopes) {
                expect(typeof envelope).toBe("string");
                expect(() => JSON.parse(envelope) as Envelope).not.toThrow();
            }
        }
        expect(parsed.sync).not.toBeNull();
        expect(parsed.now).not.toBeNull();
        expect(parsed.deferred.length).toBeGreaterThan(0);
    });

    it("answers the sync call the kernel minted with the request's body", () => {
        // A lift that dropped `sync` turns a sync call into a fire-and-forget
        // activation, which answers nothing, and fails here rather than
        // silently.
        const request = parsed.ports.find((window) => window.port === parsed.sync);
        expect(request).toBeDefined();
        const last = request?.envelopes[request.envelopes.length - 1] ?? "";
        expect(answer).toEqual({ reply: (JSON.parse(last) as Envelope).body });
    });

    it("hands the guest every window byte for byte as the kernel wrote it", () => {
        expect(reports).toHaveLength(1);
        const [report] = reports;
        expect(report.windows).toEqual(
            parsed.ports.map((window) => ({
                port: window.port,
                new_from: window.new_from,
                dropped: window.dropped,
                envelopes: window.envelopes.map(hex),
            })),
        );
    });

    it("hands the guest the deferred view, the clock and the sync port", () => {
        const [report] = reports;
        expect(report.deferred).toEqual(
            parsed.deferred.map((window) => ({
                port: window.port,
                entries: window.entries.map((entry) => ({
                    index: entry.index,
                    hex: hex(entry.payload),
                    deliver_after: entry.deliver_after,
                })),
            })),
        );
        expect(report.now).toBe(parsed.now);
        expect(report.sync).toBe(parsed.sync);
        expect(report.results).toEqual([]);
    });

    it("carries every window shape the kernel can hand a component", () => {
        // The golden is only worth its brittleness if the shapes it exists to
        // carry are actually in it: loss the component can see, a pure-context
        // window, and an empty one. Each of the assertions above is read through
        // one of these, so a regeneration that flattened them would leave the
        // suite green and blind.
        expect(parsed.ports.some((window) => window.dropped > 0)).toBe(true);
        expect(
            parsed.ports.some(
                (window) =>
                    window.envelopes.length > 0 &&
                    window.new_from === window.envelopes.length,
            ),
        ).toBe(true);
        expect(parsed.ports.some((window) => window.envelopes.length === 0)).toBe(
            true,
        );
    });
});
