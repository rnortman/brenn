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
// disagreement a failure.
//
// The guest is `processor-transplant`, because it is the one in-tree component
// that reports what it was handed: its summary publish names every window, the
// `message_id` of every envelope it parsed, `new_from`, `dropped` and `now`. So
// this asserts more than "the glue accepted it" — it asserts the guest read back
// what the kernel wrote.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { beforeAll, describe, expect, it } from "vitest";
import { activationEntry, type KernelActivation } from "./surface.js";
import {
    REPO_ROOT,
    instantiateTranspiled,
    requireTranspiledTree,
} from "./test-helpers/transpiled-processor.js";

const KIND = "processor-transplant";
const GOLDEN = resolve(REPO_ROOT, "surface/test-fixtures/activation.json");

/** The `message_id` of one envelope, which is all this suite reads out of one. */
interface Envelope {
    message_id: string;
    body: string;
}

/** One port window as the transplant guest's summary publish reports it. */
interface PortSummary {
    port: string;
    ids: string[];
    new_from: number;
    dropped: number;
}

/** The transplant guest's summary publish, reduced to what this suite reads. */
interface ActivationSummary {
    ports: PortSummary[];
    deferred: {
        port: string;
        entries: { index: number; payload: string; deliver_after: number }[];
    }[];
    now: number | null;
}

describe("the golden activation", () => {
    // The bytes exactly as the kernel emitted them: the file carries a trailing
    // newline for the tree's sake and the serialization does not.
    const golden = readFileSync(GOLDEN, "utf8").trimEnd();
    const parsed = JSON.parse(golden) as KernelActivation;

    let publishes: string[];
    let answer: string | { reply: string } | undefined;

    beforeAll(async () => {
        requireTranspiledTree(KIND);
        publishes = [];
        const instance = await instantiateTranspiled(KIND, {
            "brenn:processor/config": { get: () => undefined },
            "brenn:processor/log": { log: () => {} },
            "brenn:processor/ports": {
                publish: (_port: string, payload: string) => {
                    publishes.push(payload);
                },
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

    it("answers the sync call the kernel minted", () => {
        // The request body is the guest's `__reply__` marker, so the guest
        // reports its sync accessors back: the port it read out of `sync`, that
        // port compared against the mount item, and the ports this activation
        // delivered — everything the lift's `sync` mapping feeds. A lift that
        // dropped `sync` turns a sync call into a fire-and-forget activation and
        // fails here rather than silently.
        const delivered = parsed.ports
            .map((window) => window.port)
            .filter((port) => port !== parsed.sync);
        expect(answer).toEqual({
            reply:
                `replied:${parsed.sync}:mount=true:request=__reply__:` +
                `delivered=[${delivered.join(",")}]`,
        });
    });

    it("is read back by the guest as the kernel wrote it", () => {
        const summary = JSON.parse(publishes[0]) as ActivationSummary;
        expect(summary.ports).toEqual(
            parsed.ports.map((window) => ({
                port: window.port,
                ids: window.envelopes.map(
                    (envelope) => (JSON.parse(envelope) as Envelope).message_id,
                ),
                new_from: window.new_from,
                dropped: window.dropped,
            })),
        );
        expect(summary.deferred).toEqual(parsed.deferred);
        expect(summary.now).toBe(parsed.now);
    });

    it("delivers every new envelope's body to the guest", () => {
        // The guest publishes one `<port>:<body>` marker per new envelope — bar
        // the `__reply__` sentinel, which it answers instead of echoing. So this
        // pins the *contents* crossing the seam, not just the count: a lift that
        // truncated a window or reordered it shows up here, and the count is
        // exact, so an extra or missing publish does too.
        const expected = parsed.ports.flatMap((window) =>
            window.envelopes
                .slice(window.new_from)
                .map((envelope) => (JSON.parse(envelope) as Envelope).body)
                .filter((body) => !body.startsWith("__"))
                .map((body) => `${window.port}:${body}`),
        );
        expect(publishes.slice(1)).toEqual(expected);
        expect(publishes).toHaveLength(1 + expected.length);
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
