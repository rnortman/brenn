import { readFileSync } from "node:fs";
import * as path from "node:path";
import { expect, test, type Page } from "@playwright/test";
import {
  expectedState,
  fnv1a64,
  generatedState,
  withCounter,
} from "../../frontend/src/page-rig/state";
import { type BytesReport, parseEntry, run } from "./bytes-helpers";
import { echoStubHost, openSurface } from "./echo-stub";

/**
 * The Chromium timing workload for the surface byte path, at the node rig's
 * sizes, on the `bytes` surface of `brenn.e2e.brenn`. Selected by its
 * `@measure` tag: `make e2e-measure` runs it alone on a fresh server, and the
 * default `make e2e` leaves it out. Timings are printed, never asserted; every
 * sample's checksum is.
 *
 * Each figure is the time from the driver's dispatched click to the driver's
 * scrollback gaining the expected reports, read from `performance.now()` in
 * the page:
 *
 *  - floor: `noop` on `stater` — the driver's press activation, the fixture's
 *    activation and the driver's render activation;
 *  - binary state at rest: `state <size>` on `stater` (then `noop` with the
 *    state window filled) — the same chain, the fixture reading and
 *    publishing the state on its page-local `io` port;
 *  - binary row delivered: `emit out <size> <k>` on `streamer` — the chain,
 *    plus the row out over the page's websocket, the server, the row back in,
 *    the delivery activation and a second driver render.
 *
 * The page script only reads a clock, dispatches the click and counts added
 * scrollback entries; parsing and checking are done here, in node.
 */

/** Raw sizes with the timed iterations and the untimed warm-ups at each. */
const STATE_SIZES = [
  { raw: 49_149, n: 200, warm: 10 },
  { raw: 1_048_576, n: 40, warm: 5 },
  { raw: 3_000_000, n: 20, warm: 3 },
] as const;

/**
 * The delivered-row workload's sizes. Every emitted row is one `PublishBatch`
 * entry on a wire channel, which draws one token of the instance's attach
 * send budget (`SURFACE_SEND_BURST`, 256, refilled one per 15 s; the e2e
 * document sets no `send_burst` on `streamer`, so the default is its burst).
 * Past it the page's outbox retries once a second and the figures would
 * silently include those retries, so the smallest size runs 100 rows rather
 * than the node rig's 200, and the total here must stay under the burst.
 */
const DELIVERED_SIZES = [
  { raw: 49_149, n: 100, warm: 10 },
  { raw: 1_048_576, n: 40, warm: 5 },
  { raw: 3_000_000, n: 20, warm: 3 },
] as const;

/** The default attach send burst, read from the Rust source that defines it. */
function surfaceSendBurst(): number {
  const source = path.join(__dirname, "../../brenn-lib/src/messaging/config.rs");
  const found = /^pub const SURFACE_SEND_BURST: u32 = (\d+);$/m.exec(
    readFileSync(source, "utf8"),
  );
  if (found?.[1] === undefined) {
    throw new Error(`${source} no longer defines SURFACE_SEND_BURST as a u32 literal`);
  }
  return Number(found[1]);
}

const FLOOR_N = 500;
const FLOOR_WARM = 50;

/** How long one timed chain may take before the page gives up on it. */
const SAMPLE_TIMEOUT_MS = 60_000;

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

const lines: string[] = [];

function record(label: string, raw: number, samples: number[]): void {
  lines.push(`  ${label} ${raw} B, N ${samples.length}: ${ms(figures(samples))}`);
}

/**
 * Send `command` through `instance` and resolve once its scrollback has gained
 * `entries` reports: the elapsed page-clock milliseconds and the reports.
 */
async function timed(
  page: Page,
  instance: string,
  command: string,
  entries: number,
): Promise<[number, BytesReport[]]> {
  const [elapsed, texts] = await echoStubHost(page, instance).evaluate(
    (
      node: Element,
      [text, want, limit]: [string, number, number],
    ): Promise<[number, string[]]> =>
      new Promise((resolve, reject) => {
        const field = node.querySelector("[data-echo-input]");
        const send = node.querySelector("[data-echo-send-custom]");
        if (
          !(field instanceof HTMLInputElement) ||
          !(send instanceof HTMLElement)
        ) {
          reject(new Error("the echo-stub's custom-send controls are not rendered"));
          return;
        }
        const added: string[] = [];
        let start = 0;
        const timer = setTimeout(() => {
          observer.disconnect();
          reject(new Error(`${added.length} of ${want} entries after ${limit} ms`));
        }, limit);
        const observer = new MutationObserver((records) => {
          for (const r of records) {
            for (const n of Array.from(r.addedNodes)) {
              if (n instanceof Element && n.hasAttribute("data-echo-message")) {
                added.push(n.textContent ?? "");
              }
            }
          }
          if (added.length >= want) {
            const elapsed = performance.now() - start;
            observer.disconnect();
            clearTimeout(timer);
            resolve([elapsed, added]);
          }
        });
        observer.observe(node, { childList: true, subtree: true });
        field.value = text;
        start = performance.now();
        send.dispatchEvent(new MouseEvent("click", { bubbles: true }));
      }),
    [command, entries, SAMPLE_TIMEOUT_MS] as [string, number, number],
  );
  expect(texts, `${instance} gains exactly ${entries} reports for ${command}`).toHaveLength(
    entries,
  );
  return [elapsed, texts.map(parseEntry)];
}

/** A fresh `bytes` page whose `instance` has run one command. */
async function freshPage(page: Page, instance: string): Promise<void> {
  await openSurface(page, "bytes");
  await run(page, instance, "noop", (w) => w === "ok");
}

test.describe("@measure the surface byte path in Chromium, timed", () => {
  test.describe.configure({ mode: "serial", timeout: 600_000 });

  test.afterAll(() => {
    console.log(
      [
        `surface byte path in Chromium (${test.info().project.name}; page clock, driver click to driver render):`,
        ...lines,
      ].join("\n"),
    );
  });

  test("the floor: noop on the state holder", async ({ page }) => {
    await freshPage(page, "drive-stater");
    const samples: number[] = [];
    for (let i = 0; i < FLOOR_WARM + FLOOR_N; i++) {
      const [elapsed, [report]] = await timed(page, "drive-stater", "noop", 1);
      expect(report?.results).toEqual(["ok"]);
      if (i >= FLOOR_WARM) {
        samples.push(elapsed);
      }
    }
    record("floor noop", 0, samples);
  });

  test("binary state at rest on 0.2.0", async ({ page }) => {
    for (const { raw, n, warm } of STATE_SIZES) {
      // A fresh page empties the state ring, so one holder serves every size.
      await freshPage(page, "drive-stater");
      const pattern = generatedState(raw);
      const stateSamples: number[] = [];
      for (let i = 0; i < warm + n; i++) {
        const [elapsed, [report]] = await timed(page, "drive-stater", `state ${raw}`, 1);
        expect(report?.results).toEqual([expectedState(pattern, i)]);
        if (i >= warm) {
          stateSamples.push(elapsed);
        }
      }
      record("state", raw, stateSamples);

      const noopSamples: number[] = [];
      for (let i = 0; i < warm + n; i++) {
        const [elapsed, [report]] = await timed(page, "drive-stater", "noop", 1);
        expect(report?.results).toEqual(["ok"]);
        if (i >= warm) {
          noopSamples.push(elapsed);
        }
      }
      record("noop, state filled", raw, noopSamples);
    }
  });

  test("binary row delivered over the socket", async ({ page }) => {
    const planned = DELIVERED_SIZES.reduce((t, s) => t + s.n + s.warm, 0);
    expect(planned, "the planned rows fit the attach send burst").toBeLessThan(
      surfaceSendBurst(),
    );
    await freshPage(page, "drive-streamer");
    let counter = 0;
    for (const { raw, n, warm } of DELIVERED_SIZES) {
      const pattern = generatedState(raw);
      const samples: number[] = [];
      for (let i = 0; i < warm + n; i++) {
        counter += 1;
        const sum = fnv1a64(withCounter(pattern, counter));
        const [elapsed, added] = await timed(
          page,
          "drive-streamer",
          `emit out ${raw} ${counter}`,
          2,
        );
        // The kernel does not order the page-local half of a flush against the
        // wire half, so the two reports are found by content, not position.
        const emitted = added.filter((r) => r.results.length === 1);
        const delivered = added.filter((r) => r.results.length === 0);
        expect(emitted.map((r) => r.results[0])).toEqual([`emit:${raw}:${sum}`]);
        expect(delivered.map((r) => r.streams[0])).toEqual([
          expect.objectContaining({ count: 1, len: raw, fnv1a: sum }),
        ]);
        if (i >= warm) {
          samples.push(elapsed);
        }
      }
      record("delivered row", raw, samples);
    }
  });
});
