// @vitest-environment node
//
// The 0.1.0 state baseline: time per activation of the retained round trip
// through the production kernel bundle, the production `surface.ts` and the
// transpiled `processor-page-test` kind, with the state carried the way a
// 0.1.0 kind must carry binary today — base64 inside a JSON body on a
// page-local `io` port. Timings are printed, never asserted.
//
// Each size has its own state-holder on the `measure` page: a retained state is
// of one size, and the fixture refuses a stored state of another.

import { afterAll, describe, expect, it } from "vitest";
import { type Page, type Report, only, openPage } from "./page.js";
import { expectedState, generatedState } from "./state.js";

/** Raw state sizes, with the iterations timed and the untimed warm-up at each. */
const SIZES = [
    { raw: 49_149, n: 200, warm: 10 },
    { raw: 1_048_576, n: 40, warm: 5 },
    { raw: 3_000_000, n: 20, warm: 3 },
] as const;
const KEEPER_N = 500;
const KEEPER_WARM = 50;
/** The stored body around the base64: `{"v":1,"state":"…"}`. */
const WRAPPER_BYTES = 18;
const TIMEOUT_MS = 600_000;

interface Figures {
    mean: number;
    median: number;
    min: number;
}

function figures(samples: number[]): Figures {
    const sorted = [...samples].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    const median =
        sorted.length % 2 === 0
            ? ((sorted[mid - 1] as number) + (sorted[mid] as number)) / 2
            : (sorted[mid] as number);
    return {
        mean: samples.reduce((a, b) => a + b, 0) / samples.length,
        median,
        min: sorted[0] as number,
    };
}

const ms = (f: Figures): string =>
    `mean ${f.mean.toFixed(3)} / median ${f.median.toFixed(3)} / min ${f.min.toFixed(3)} ms`;

/** One command on `instance`, timed from delivery to quiescence; its one report. */
async function timed(page: Page, instance: string, line: string): Promise<[number, Report]> {
    const start = performance.now();
    const reports = await page.command(instance, line);
    const elapsed = performance.now() - start;
    return [elapsed, only(reports, instance)];
}

let page: Page | null = null;

function rig(): Page {
    if (page === null) {
        throw new Error("no page: the bring-up case did not produce one");
    }
    return page;
}

const lines: string[] = [];

afterAll(() => {
    page?.finish();
    console.log(
        [`page rig 0.1.0 state baseline (node ${process.version}, V8; not a browser):`, ...lines].join("\n"),
    );
});

describe("page rig baseline: base64 state on a page-local io port", () => {
    it("brings the measure page up through bootstrap", { timeout: TIMEOUT_MS }, async () => {
        page = await openPage("measure");
        expect(page.facts.state_instances).toHaveLength(SIZES.length);
        expect(page.status().every((row) => row.state === "mounted")).toBe(true);
        lines.push(`Welcome max_body_bytes ${page.facts.max_body_bytes}`);
    });

    it("times an activation with empty windows on keeper", { timeout: TIMEOUT_MS }, async () => {
        const samples: number[] = [];
        for (let i = 0; i < KEEPER_WARM + KEEPER_N; i++) {
            const [elapsed, report] = await timed(rig(), "keeper", "noop");
            expect(report.results).toEqual(["ok"]);
            if (i >= KEEPER_WARM) {
                samples.push(elapsed);
            }
        }
        lines.push(`noop on keeper: N ${KEEPER_N}, ${ms(figures(samples))}`);
    });

    SIZES.forEach(({ raw, n, warm }, index) => {
        it(`times the state round trip at ${raw} raw bytes`, { timeout: TIMEOUT_MS }, async () => {
            const stater = rig().facts.state_instances[index] as string;
            const stored = 4 * Math.ceil(raw / 3) + WRAPPER_BYTES;
            const generated = generatedState(raw);
            const command = `state ${raw}`;

            // Create, then warm up; every iteration's checksum is checked, so a
            // figure is never taken from a run that carried the wrong bytes.
            let steps = 0;
            for (let i = 0; i < 1 + warm; i++) {
                const [, report] = await timed(rig(), stater, command);
                expect(report.results).toEqual([expectedState(generated, steps)]);
                steps += 1;
            }

            const stateSamples: number[] = [];
            const results: string[] = [];
            for (let i = 0; i < n; i++) {
                const [elapsed, report] = await timed(rig(), stater, command);
                stateSamples.push(elapsed);
                results.push(...report.results);
            }
            expect(results).toEqual(
                Array.from({ length: n }, (_, i) => expectedState(generated, steps + i)),
            );

            const noopSamples: number[] = [];
            for (let i = 0; i < warm + n; i++) {
                const [elapsed, report] = await timed(rig(), stater, "noop");
                expect(report.results).toEqual(["ok"]);
                expect(report.windows).toEqual([]);
                if (i >= warm) {
                    noopSamples.push(elapsed);
                }
            }

            lines.push(
                `raw ${raw} B, stored ${stored} B, N ${n}: ` +
                    `state ${ms(figures(stateSamples))}; ` +
                    `noop on ${stater} ${ms(figures(noopSamples))}`,
            );
        });
    });
});
