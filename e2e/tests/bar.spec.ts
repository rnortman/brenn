import { expect, test, type Locator, type Page } from "@playwright/test";

/**
 * Browser e2e for the bar surface: the instances + layout + skins world.
 * Exercises, end-to-end through the real WASM shell/component bundle and the
 * live server bus, the flows the bar surface must support:
 *
 *  1. Bar loads → config-synthesised default layout (three protobar instances →
 *     `columns-3`), three panels visible, no stale/gap hint in any panel (empty
 *     durable channels + the snapshot-subscribe gap suppression).
 *  2. `bar` and `bar-pixel` each carry their own skin stylesheet + `data-skin`.
 *  3. Content published to one instance's channel renders only in that panel.
 *  4. The LLM switches the layout at runtime through the layout channel — all
 *     four layout kinds exercised (`columns-3` is the default from spec 1;
 *     `single`, `columns-2`, `main-side` are driven here).
 *  5. A malformed layout doc is rejected and the last-good layout stays — never
 *     a blank screen.
 *  6. Reload restores the retained layout and panel content (durable snapshot).
 *
 * Runs single-worker in declaration order (playwright.config.ts) against the
 * `brenn.e2e.toml` bar surfaces (`bar`, `bar-pixel`, `bar-feeder`). The bar
 * channels are durable with `retain_depth = 1` (latest-wins) and persist for
 * the whole server run, so spec 1 must run first against pristine empty
 * channels; the later specs republish their own layout/content preconditions
 * so they do not depend on the exact retained state a prior spec left behind.
 *
 * Selectors are component/shell-authored marker attributes, never styling
 * hooks: the shell stamps `data-layout` on `#surface-root`, `data-panel` on an
 * assigned per-instance `section[data-instance]`, and renders the panel label
 * into a `header[data-panel-label]`; protobar writes `[data-protobar-message]`
 * and `[data-protobar-status]` (the latter carries the "gap — data may be
 * stale" hint when a real gap fires).
 */

// Cold wasm load + WS connect + mount + snapshot replay can outlast the 5s
// assertion default on a fresh navigation, so the chain assertions get a
// generous explicit budget.
const CHAIN_TIMEOUT = 20_000;

/** Open a surface page and wait for its document to finish loading. */
async function openSurface(page: Page, slug: string): Promise<void> {
  await page.goto(`/surface/${slug}`, { waitUntil: "load" });
}

/** The `#surface-root` element that carries `data-layout` / `data-skin`. */
function surfaceRoot(page: Page): Locator {
  return page.locator("#surface-root");
}

/** A bar instance's mount section (carries `data-panel` when assigned). */
function panel(page: Page, instance: string): Locator {
  return page.locator(`#surface-root > section[data-instance="${instance}"]`);
}

/** The protobar message node inside an instance's panel. */
function panelMessage(page: Page, instance: string): Locator {
  return panel(page, instance).locator("[data-protobar-message]");
}

/**
 * Publish `body` verbatim onto the channel bound to the feeder's `instance`
 * output by dispatching the designed component → shell `brenn-port-publish`
 * seam event on that mounted echo-stub element. This is exactly the event the
 * visible "send custom" button dispatches, so no gate is bypassed and no
 * test-only production code exists — the same reasoning the durabar PoC spec
 * used for `brenn-log`. It is used in preference to the button because the
 * feeder's `feed-layout` instance is the fourth echo-stub and thus unassigned
 * in the feeder's own config-default `columns-3` layout — headless and
 * `display:none`, so not clickable — while its output is the only path onto the
 * layout channel.
 */
async function publishVia(
  feeder: Page,
  instance: string,
  body: string,
): Promise<void> {
  const el = feeder.locator(`brenn-echo-stub[data-instance="${instance}"]`);
  await expect(el).toBeAttached({ timeout: CHAIN_TIMEOUT });
  await el.evaluate((node: Element, b: string) => {
    node.dispatchEvent(
      new CustomEvent("brenn-port-publish", {
        bubbles: true,
        composed: true,
        detail: { port: "out", body: b },
      }),
    );
  }, body);
}

test("bar loads the config-default layout with no stale hint on empty channels", async ({
  page,
}) => {
  // First spec, pristine run: the layout channel is empty, so the shell
  // synthesises the default from the three configured instances → columns-3,
  // with each instance's section assigned its slot and visible.
  await openSurface(page, "bar");
  await expect(surfaceRoot(page)).toHaveAttribute("data-layout", "columns-3", {
    timeout: CHAIN_TIMEOUT,
  });
  await expect(panel(page, "p1")).toHaveAttribute("data-panel", "a");
  await expect(panel(page, "p2")).toHaveAttribute("data-panel", "b");
  await expect(panel(page, "p3")).toHaveAttribute("data-panel", "c");

  // Empty content channels: every panel sits in its pre-first-message state and
  // — because the snapshot subscribe's self-inflicted BeyondRetained gap is
  // suppressed — none shows the stale/gap hint. Without suppression a
  // brand-new bar would carry a permanent false staleness warning.
  for (const instance of ["p1", "p2", "p3"]) {
    await expect(panelMessage(page, instance)).toHaveText("awaiting data", {
      timeout: CHAIN_TIMEOUT,
    });
    await expect(
      panel(page, instance).locator("[data-protobar-status]"),
    ).not.toContainText("gap");
  }
});

test("bar and bar-pixel each carry their own skin", async ({ context }) => {
  // The config-selectable skin swap: two surfaces, identical bindings, different
  // skin — proven by the emitted stylesheet link and the root's data-skin.
  const bench = await context.newPage();
  await openSurface(bench, "bar");
  await expect(surfaceRoot(bench)).toHaveAttribute("data-skin", "bench", {
    timeout: CHAIN_TIMEOUT,
  });
  await expect(
    bench.locator('link[rel="stylesheet"][href*="skins/bench.css"]'),
  ).toHaveCount(1);
  await expect(
    bench.locator('link[rel="stylesheet"][href*="skins/foundry.css"]'),
  ).toHaveCount(0);

  const foundry = await context.newPage();
  await openSurface(foundry, "bar-pixel");
  await expect(surfaceRoot(foundry)).toHaveAttribute("data-skin", "foundry", {
    timeout: CHAIN_TIMEOUT,
  });
  await expect(
    foundry.locator('link[rel="stylesheet"][href*="skins/foundry.css"]'),
  ).toHaveCount(1);
  await expect(
    foundry.locator('link[rel="stylesheet"][href*="skins/bench.css"]'),
  ).toHaveCount(0);

  await bench.close();
  await foundry.close();
});

test("content published to one channel renders only in its bound panel", async ({
  context,
}) => {
  // Bar connects and reaches its default layout before the publish, so this is
  // the live push chain (publish → ACL → fan-out → WS Deliver → client demux →
  // activation window → the p2 panel), not retained replay.
  const bar = await context.newPage();
  await openSurface(bar, "bar");
  await expect(surfaceRoot(bar)).toHaveAttribute("data-layout", "columns-3", {
    timeout: CHAIN_TIMEOUT,
  });

  const marker = "bar-content-only-in-p2";
  const feeder = await context.newPage();
  await openSurface(feeder, "bar-feeder");
  await publishVia(feeder, "feed-b", marker);

  // Renders in p2 (bound to brenn:bar-b)…
  await expect(panelMessage(bar, "p2")).toHaveText(marker, {
    timeout: CHAIN_TIMEOUT,
  });
  // …and nowhere else: this test's marker never reaches p1/p3. Asserting the
  // marker's absence (rather than the exact pristine "awaiting data" text) keeps
  // the isolation self-contained — it proves instance routing kept one channel's
  // content out of its siblings' panels without depending on bar-a/bar-c staying
  // globally empty across the whole suite run.
  await expect(panelMessage(bar, "p1")).not.toContainText(marker);
  await expect(panelMessage(bar, "p3")).not.toContainText(marker);

  await feeder.close();
  await bar.close();
});

test("the LLM switches the layout at runtime through the layout channel", async ({
  context,
}) => {
  const bar = await context.newPage();
  await openSurface(bar, "bar");
  await expect(surfaceRoot(bar)).toHaveAttribute("data-layout", "columns-3", {
    timeout: CHAIN_TIMEOUT,
  });

  const feeder = await context.newPage();
  await openSurface(feeder, "bar-feeder");

  // single: only slot a (p1) is assigned; the other instances stay mounted but
  // their sections lose data-panel and the base CSS hides them.
  await publishVia(
    feeder,
    "feed-layout",
    JSON.stringify({ v: 1, kind: "single", panels: { a: { instance: "p1" } } }),
  );
  await expect(surfaceRoot(bar)).toHaveAttribute("data-layout", "single", {
    timeout: CHAIN_TIMEOUT,
  });
  await expect(panel(bar, "p1")).toHaveAttribute("data-panel", "a");
  await expect(panel(bar, "p2")).toBeHidden();
  await expect(panel(bar, "p3")).toBeHidden();

  // columns-2: slots a (p1) and b (p2); p3 stays hidden.
  await publishVia(
    feeder,
    "feed-layout",
    JSON.stringify({
      v: 1,
      kind: "columns-2",
      panels: { a: { instance: "p1" }, b: { instance: "p2" } },
    }),
  );
  await expect(surfaceRoot(bar)).toHaveAttribute("data-layout", "columns-2", {
    timeout: CHAIN_TIMEOUT,
  });
  await expect(panel(bar, "p1")).toHaveAttribute("data-panel", "a");
  await expect(panel(bar, "p2")).toHaveAttribute("data-panel", "b");
  await expect(panel(bar, "p3")).toBeHidden();

  // main-side with labels + ratio: all three slots, plain-text labels rendered
  // into the panel-label header, and the ratio surfaced as the --surface-ratio
  // custom property on the root.
  await publishVia(
    feeder,
    "feed-layout",
    JSON.stringify({
      v: 1,
      kind: "main-side",
      panels: {
        a: { instance: "p1", label: "MAIN" },
        b: { instance: "p2", label: "TOP" },
        c: { instance: "p3", label: "BOTTOM" },
      },
      ratio: 0.6,
    }),
  );
  await expect(surfaceRoot(bar)).toHaveAttribute("data-layout", "main-side", {
    timeout: CHAIN_TIMEOUT,
  });
  await expect(panel(bar, "p1")).toHaveAttribute("data-panel", "a");
  await expect(panel(bar, "p2")).toHaveAttribute("data-panel", "b");
  await expect(panel(bar, "p3")).toHaveAttribute("data-panel", "c");
  await expect(
    panel(bar, "p1").locator("header[data-panel-label]"),
  ).toHaveText("MAIN");
  await expect(surfaceRoot(bar)).toHaveAttribute(
    "style",
    /--surface-ratio:\s*0\.6/,
  );

  await feeder.close();
  await bar.close();
});

test("a malformed layout doc is rejected and the last-good layout stays", async ({
  context,
}) => {
  const bar = await context.newPage();
  await openSurface(bar, "bar");

  const feeder = await context.newPage();
  await openSurface(feeder, "bar-feeder");

  // Establish a known-good layout first, so this spec does not depend on the
  // exact doc a prior spec left retained.
  await publishVia(
    feeder,
    "feed-layout",
    JSON.stringify({
      v: 1,
      kind: "columns-2",
      panels: { a: { instance: "p1" }, b: { instance: "p2" } },
    }),
  );
  await expect(surfaceRoot(bar)).toHaveAttribute("data-layout", "columns-2", {
    timeout: CHAIN_TIMEOUT,
  });

  // Arm a listener for the shell's rejection breadcrumb before publishing the bad
  // doc: on an invalid layout doc the shell drops it and logs `rejected layout
  // doc: <reason>` at warn on the bar page (ShellAction::Report → console.warn).
  // Awaiting that console message is a deterministic proof the malformed doc was
  // delivered, processed, and rejected — a fixed sleep would pass vacuously
  // whenever the publish → ACL → durable fan-out → WS deliver chain outran it on a
  // loaded runner (the same slow chain CHAIN_TIMEOUT exists for).
  const rejection = bar.waitForEvent("console", {
    predicate: (msg) =>
      msg.type() === "warning" && msg.text().includes("rejected layout doc"),
    timeout: CHAIN_TIMEOUT,
  });

  // A doc that parses as JSON but fails validation (unknown layout kind). The
  // shell rejects the whole doc atomically and keeps the last-good layout — a
  // kiosk never blanks or partially applies on bad LLM output.
  await publishVia(
    feeder,
    "feed-layout",
    JSON.stringify({
      v: 1,
      kind: "not-a-real-kind",
      panels: { a: { instance: "p1" } },
    }),
  );

  // Wait for the rejection to be observed, then assert nothing changed: the
  // layout is still columns-2 and its panels are still on screen.
  await rejection;
  await expect(surfaceRoot(bar)).toHaveAttribute("data-layout", "columns-2");
  await expect(panel(bar, "p1")).toHaveAttribute("data-panel", "a");
  await expect(panel(bar, "p2")).toHaveAttribute("data-panel", "b");

  await feeder.close();
  await bar.close();
});

test("reload restores the retained layout and panel content", async ({
  context,
  page,
}) => {
  // Establish a distinctive retained layout + content, overwriting whatever the
  // malformed-doc spec left on the layout channel (retain_depth = 1, latest
  // wins). Publish while `bar` is connected so both arrive live and become the
  // retained latest; then reload and prove the durable snapshot path restores
  // them with no republish.
  const feeder = await context.newPage();
  await openSurface(feeder, "bar-feeder");

  await openSurface(page, "bar");
  await expect(surfaceRoot(page)).toHaveAttribute("data-layout", /.+/, {
    timeout: CHAIN_TIMEOUT,
  });

  const contentMarker = "bar-reload-restores-this";
  await publishVia(
    feeder,
    "feed-layout",
    JSON.stringify({
      v: 1,
      kind: "main-side",
      panels: {
        a: { instance: "p1", label: "RELOADED" },
        b: { instance: "p2" },
        c: { instance: "p3" },
      },
    }),
  );
  await publishVia(feeder, "feed-b", contentMarker);

  // Live application, pre-reload.
  await expect(surfaceRoot(page)).toHaveAttribute("data-layout", "main-side", {
    timeout: CHAIN_TIMEOUT,
  });
  await expect(
    panel(page, "p1").locator("header[data-panel-label]"),
  ).toHaveText("RELOADED");
  await expect(panelMessage(page, "p2")).toHaveText(contentMarker, {
    timeout: CHAIN_TIMEOUT,
  });

  // A full reload is a fresh WS session that snapshot-subscribes each durable
  // channel (last_seq: 0) and replays the retained latest: the layout and the
  // panel content come back with nothing published here.
  await page.reload({ waitUntil: "load" });
  await expect(surfaceRoot(page)).toHaveAttribute("data-layout", "main-side", {
    timeout: CHAIN_TIMEOUT,
  });
  await expect(
    panel(page, "p1").locator("header[data-panel-label]"),
  ).toHaveText("RELOADED", { timeout: CHAIN_TIMEOUT });
  await expect(panelMessage(page, "p2")).toHaveText(contentMarker, {
    timeout: CHAIN_TIMEOUT,
  });

  await feeder.close();
});
