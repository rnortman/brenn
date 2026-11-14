import { expect, type Locator, type Page } from "@playwright/test";

/**
 * Shared by the specs that open surfaces and drive an echo-stub instance: the
 * assertion budget, opening a surface page, finding an echo-stub, and
 * publishing through its "send custom" button.
 */

/**
 * Cold wasm load + WS connect + mount + snapshot replay can outlast the 5s
 * assertion default on a fresh navigation, so the chain assertions get a
 * generous explicit budget.
 */
export const CHAIN_TIMEOUT = 20_000;

/** Open a surface page and wait for its document to finish loading. */
export async function openSurface(page: Page, slug: string): Promise<void> {
  await page.goto(`/surface/${slug}`, { waitUntil: "load" });
}

/**
 * An echo-stub instance's kernel-owned wrapper: a div stamped with the kind and
 * the instance (canonical source: `mount_wrapper` in
 * `surface/kernel/src/dom.rs`). The component's own host element inside it
 * carries no identity, so this names exactly one element.
 */
export function echoStubHost(page: Page, instance: string): Locator {
  // TODO(e2e-tag-scheme-tie): these literals re-encode the Rust naming with
  // nothing mechanical tying the two together.
  return page.locator(
    `div[data-kind="echo-stub"][data-instance="${instance}"]`,
  );
}

/**
 * Publish `body` verbatim onto the channel bound to `instance`'s output by
 * driving that echo-stub's own "send custom" button: fill its free-form field,
 * then click it. The click is dispatched from page script rather than through
 * Playwright because an echo-stub left unassigned in its surface's default
 * layout is headless and `display:none`, so not clickable by a real pointer.
 *
 * Everything past the click is production: the component's gesture wiring asks
 * for a sync-call activation, and its entry publishes from inside one. No gate
 * is bypassed and no test-only production code exists.
 */
export async function publishVia(
  page: Page,
  instance: string,
  body: string,
): Promise<void> {
  const el = echoStubHost(page, instance);
  await expect(el).toBeAttached({ timeout: CHAIN_TIMEOUT });
  await el.evaluate((node: Element, b: string) => {
    const field = node.querySelector("[data-echo-input]");
    const send = node.querySelector("[data-echo-send-custom]");
    if (!(field instanceof HTMLInputElement) || !(send instanceof HTMLElement)) {
      throw new Error("the echo-stub's custom-send controls are not rendered");
    }
    field.value = b;
    send.dispatchEvent(new MouseEvent("click", { bubbles: true }));
  }, body);
}
