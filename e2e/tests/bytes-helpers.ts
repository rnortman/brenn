import { expect, type Page } from "@playwright/test";
import { CHAIN_TIMEOUT, echoStubHost, publishVia } from "./echo-stub";

/**
 * Shared by `bytes.spec.ts` and `bytes-measure.spec.ts`: how the specs reach
 * the `bytes` surface of `brenn.e2e.brenn`, where three
 * `processor-page-bytes-test` instances (`keeper`, `stater`, `streamer`) are
 * each driven by an echo-stub (`drive-keeper`, `drive-stater`,
 * `drive-streamer`). A driver's send publishes the fixture's command; its
 * scrollback shows the fixture's reports. Everything here reads the DOM and
 * computes in node; nothing compares bytes in the page.
 */

/** The `bytes` page's websocket path; the URL also carries a build query. */
export const BYTES_SOCKET_PATH = "/surface/bytes/ws";

/** One envelope as `processor-page-bytes-test` reports it. */
export interface BytesEnvelope {
  header: Record<string, unknown>;
  content_type: string | null;
  hex: string;
}

/** One report `processor-page-bytes-test` publishes on `report`. */
export interface BytesReport {
  windows: {
    port: string;
    new_from: number;
    dropped: number;
    envelopes: BytesEnvelope[];
  }[];
  streams: {
    port: string;
    new_from: number;
    dropped: number;
    count: number;
    len: number;
    fnv1a: string;
  }[];
  results: string[];
  sync: string | null;
}

/**
 * Parse one scrollback entry: the text-shape envelope JSON whose `body` string
 * is the fixture's report JSON.
 */
export function parseEntry(text: string): BytesReport {
  const envelope = JSON.parse(text) as { body?: unknown };
  if (typeof envelope.body !== "string") {
    throw new Error(`a scrollback entry carries no string body: ${text}`);
  }
  return JSON.parse(envelope.body) as BytesReport;
}

/** Every report in `instance`'s scrollback, oldest first. */
export async function reports(
  page: Page,
  instance: string,
): Promise<BytesReport[]> {
  const texts = await echoStubHost(page, instance)
    .locator("[data-echo-message]")
    .allTextContents();
  return texts.map(parseEntry);
}

/**
 * Poll `instance`'s scrollback until a report's `key` is `expected`, and return
 * it. On timeout the failure lists every key the scrollback showed.
 */
export async function reportWhere(
  page: Page,
  instance: string,
  what: string,
  key: (r: BytesReport) => string | undefined,
  expected: string,
): Promise<BytesReport> {
  let found: BytesReport | undefined;
  await expect
    .poll(
      async () => {
        const all = await reports(page, instance);
        found = all.find((r) => key(r) === expected);
        return all.map(key).filter((k) => k !== undefined);
      },
      { message: `${instance}'s scrollback shows ${what}`, timeout: CHAIN_TIMEOUT },
    )
    .toContain(expected);
  if (found === undefined) {
    throw new Error(`${instance}'s scrollback shows no ${what}`);
  }
  return found;
}

/** Send `command` through `instance` and wait for the report that ran it. */
export async function run(
  page: Page,
  instance: string,
  command: string,
  result: (word: string) => boolean,
): Promise<BytesReport> {
  const before = (await reports(page, instance)).length;
  await publishVia(page, instance, command);
  let found: BytesReport | undefined;
  await expect
    .poll(
      async () => {
        found = (await reports(page, instance))
          .slice(before)
          .find((r) => r.results.length === 1 && result(r.results[0] ?? ""));
        return found?.results;
      },
      { message: `${instance} runs ${command.slice(0, 60)}`, timeout: CHAIN_TIMEOUT },
    )
    .toBeDefined();
  if (found === undefined) {
    throw new Error(`${instance} did not run ${command.slice(0, 60)}`);
  }
  return found;
}

/** A report's window on `port`, or undefined. */
export function windowOf(
  report: BytesReport,
  port: string,
): BytesReport["windows"][number] | undefined {
  return report.windows.find((w) => w.port === port);
}

export function hex(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString("hex");
}
