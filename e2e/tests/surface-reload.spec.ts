import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { expect, test, type Locator, type Page } from "@playwright/test";

/**
 * Browser e2e for what a live page does when the surface under it is
 * reconfigured or retired by a config reload.
 *
 * The server the harness started is holding a *copy* of `brenn.e2e.brenn`
 * (`BRENN_E2E_CONFIG`, made by the `e2e` target). These specs rewrite that copy
 * in place, send the process `SIGUSR1`, and read the outcome back off the
 * retained `brenn:config.status` channel with `brenn config-status --db` — the
 * same three steps an installer takes on a real host. Nothing here reaches into
 * the process any other way.
 *
 * Two flows, in declaration order:
 *
 *  1. A surface whose resolved value moved (its skin) has its live pages closed
 *     with the reconfigured close code. The page reloads itself and comes back
 *     on the new skin — through the door's refreshing 503 document if it
 *     reloaded into the swap window.
 *  2. A surface removed from the document has its live pages closed with the
 *     retired close code. The page goes to its terminal state, does *not*
 *     navigate and does *not* reconnect, and the slug 404s from then on.
 *
 * These pages are fully mounted by the time they are retired, so the spec
 * asserts on the chrome banner's fatal state and the warning breadcrumb in the
 * console — not the pre-chrome connect indicator, which is gone once chrome has
 * mounted.
 *
 * Runs after `bar.spec.ts` (single worker, alphabetical file order) because it
 * mutates the document those specs' surfaces are declared in. The original
 * bytes are restored, and one more reload applied, in `test.afterAll` — so a
 * failure here leaves the harness's copy rewritten, but never the checked-in
 * document.
 */

const CHAIN_TIMEOUT = 20_000;

/**
 * How long to keep watching for a navigation or a reconnect that must not
 * happen. Long enough to cover the kernel's first reconnect backoff
 * (`ConnConfig.initial_backoff`), short enough not to dominate the suite.
 */
const QUIET_PERIOD = 3_000;

/** Read a harness variable the `e2e` target exports, or fail loudly. */
function fromEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} must be set; the \`make e2e\` target exports it.`);
  }
  return value;
}

const LIVE_CONFIG = fromEnv("BRENN_E2E_CONFIG");
const BRENN_BIN = fromEnv("BRENN_E2E_BIN");
const DB_PATH = fromEnv("BRENN_E2E_DB");
const SERVER_PID = Number(fromEnv("BRENN_E2E_SERVER_PID"));

/** The document as the server booted on it, captured before any rewrite. */
const BOOT_DOCUMENT = readFileSync(LIVE_CONFIG, "utf8");

/**
 * The retained outcome body, as much of it as these specs read. The full schema
 * is `ReloadStatus` in `brenn-messaging/src/config_reload.rs`; it is additive,
 * so naming the fields used here is safe.
 */
interface ReloadStatus {
  outcome: "applied" | "unchanged" | "refused";
  generation: number;
  at: string;
  refusals: string[];
  delta: {
    surfaces_added: string[];
    surfaces_removed: string[];
    surfaces_changed: string[];
  };
}

/** The retained outcome, read over a read-only connection to the live store. */
function readStatus(): ReloadStatus {
  const out = execFileSync(BRENN_BIN, ["config-status", "--db", DB_PATH], {
    encoding: "utf8",
  });
  return JSON.parse(out) as ReloadStatus;
}

/**
 * Rewrite the live document with `mutate`, ask the process to converge, and
 * return the outcome it published. Fails the caller if the outcome is not
 * `applied`, quoting the refusals — a refused reload leaves the process running
 * what it was, so every assertion after it would be about the old document.
 */
async function reloadWith(mutate: (doc: string) => string): Promise<ReloadStatus> {
  const before = readStatus();
  writeFileSync(LIVE_CONFIG, mutate(readFileSync(LIVE_CONFIG, "utf8")));
  process.kill(SERVER_PID, "SIGUSR1");

  const deadline = Date.now() + 60_000;
  for (;;) {
    const status = readStatus();
    if (status.at !== before.at || status.generation !== before.generation) {
      expect(
        status.outcome,
        `reload was ${status.outcome}: ${status.refusals.join(" | ")}`,
      ).toBe("applied");
      return status;
    }
    if (Date.now() > deadline) {
      throw new Error(
        `the reload outcome never advanced past ${before.at} (${before.outcome})`,
      );
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
}

/** Replace exactly one occurrence of `from`, or fail saying so. */
function replaceOnce(doc: string, from: string, to: string): string {
  const parts = doc.split(from);
  if (parts.length !== 2) {
    throw new Error(
      `expected exactly one ${JSON.stringify(from)} in the live document, found ${parts.length - 1}`,
    );
  }
  return parts.join(to);
}

/** The bar-pixel surface's skin, which is the cheapest value move it has. */
function retuneSkin(doc: string): string {
  return replaceOnce(doc, 'skin = "foundry"', 'skin = "bench"');
}

/**
 * Delete the marked region of the document — the `bar-feeder` surface and its
 * doc comment. The markers are in `brenn.e2e.brenn` and name this file.
 */
function retireFeeder(doc: string): string {
  const begin = doc.indexOf("// e2e:retire-block:begin");
  const endMarker = "// e2e:retire-block:end\n";
  const end = doc.indexOf(endMarker);
  if (begin < 0 || end < 0) {
    throw new Error("the retire-block markers are not in the live document");
  }
  // The comment block above the begin marker introduces it, so the deletion
  // starts at the head of that comment; leaving it would orphan a doc comment
  // in front of whatever follows, which does not parse.
  const head = doc.lastIndexOf("\n\n", begin);
  return doc.slice(0, head + 1) + doc.slice(end + endMarker.length);
}

/** Open a surface page and wait for its document to finish loading. */
async function openSurface(page: Page, slug: string): Promise<void> {
  await page.goto(`/surface/${slug}`, { waitUntil: "load" });
}

/** The `#surface-root` element that carries `data-layout` / `data-skin`. */
function surfaceRoot(page: Page): Locator {
  return page.locator("#surface-root");
}

/** Chrome's connection banner, whose `data-banner-state` is its link state. */
function banner(page: Page): Locator {
  return page.locator("[data-surface-banner]");
}

/** Count main-frame navigations from now on; the page reload is one of them. */
function countNavigations(page: Page): () => number {
  let seen = 0;
  page.on("framenavigated", (frame) => {
    if (frame === page.mainFrame()) {
      seen += 1;
    }
  });
  return () => seen;
}

/** Collect every console line the page writes from now on. */
function collectConsole(page: Page): () => string[] {
  const lines: string[] = [];
  page.on("console", (message) => lines.push(message.text()));
  return () => lines;
}

test.afterAll(async () => {
  // Through the same helper the cases use, so the suite does not end with a
  // reload in flight and a restore that was refused is a failure here rather
  // than a flake in whatever spec runs next.
  await reloadWith(() => BOOT_DOCUMENT);
});

test("a reconfigured surface closes its pages, which reload once onto the new value", async ({
  page,
}) => {
  await openSurface(page, "bar-pixel");
  await expect(surfaceRoot(page)).toHaveAttribute("data-skin", "foundry", {
    timeout: CHAIN_TIMEOUT,
  });
  const navigations = countNavigations(page);

  const status = await reloadWith(retuneSkin);
  expect(status.delta.surfaces_changed).toContain("bar-pixel");

  await expect(surfaceRoot(page)).toHaveAttribute("data-skin", "bench", {
    timeout: CHAIN_TIMEOUT,
  });
  await expect(banner(page)).toHaveAttribute("data-banner-state", "hidden", {
    timeout: CHAIN_TIMEOUT,
  });

  // Exactly one reload is expected; a second is acceptable only if the page
  // landed inside the swap window and got the door's refreshing 503 document.
  await page.waitForTimeout(QUIET_PERIOD);
  expect(navigations()).toBeGreaterThanOrEqual(1);
  expect(navigations()).toBeLessThanOrEqual(2);
});

test("a retired surface closes its pages terminally and its URL 404s", async ({
  page,
}) => {
  await openSurface(page, "bar-feeder");
  await expect(banner(page)).toHaveAttribute("data-banner-state", "hidden", {
    timeout: CHAIN_TIMEOUT,
  });
  const navigations = countNavigations(page);
  const consoleLines = collectConsole(page);

  const status = await reloadWith(retireFeeder);
  expect(status.delta.surfaces_removed).toContain("bar-feeder");

  await expect(banner(page)).toHaveAttribute("data-banner-state", "fatal", {
    timeout: CHAIN_TIMEOUT,
  });
  await expect
    .poll(() => consoleLines().join("\n"), { timeout: CHAIN_TIMEOUT })
    .toContain("this surface has been retired");

  await page.waitForTimeout(QUIET_PERIOD);
  expect(navigations()).toBe(0);
  await expect(banner(page)).toHaveAttribute("data-banner-state", "fatal");

  // The slug is not a surface any more, so the door answers 404 rather than the
  // 503 it answers while a surface is being replaced.
  const response = await page.request.get("/surface/bar-feeder");
  expect(response.status()).toBe(404);
});
