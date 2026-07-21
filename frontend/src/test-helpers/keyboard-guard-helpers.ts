import { vi } from "vitest";

/**
 * Capture every `brenn-tool-response` CustomEvent that bubbles to the
 * document. Returns the captured array and a cleanup function.
 */
export function captureToolResponses(): {
  events: CustomEvent[];
  dispose: () => void;
} {
  const events: CustomEvent[] = [];
  const handler = (e: Event) => {
    events.push(e as CustomEvent);
  };
  document.addEventListener("brenn-tool-response", handler);
  return {
    events,
    dispose: () => document.removeEventListener("brenn-tool-response", handler),
  };
}

/**
 * Advance time past the mount grace period so canInterceptKeyboard returns
 * true for any registered element. Uses a large margin (10 s) to avoid any
 * ordering sensitivity between spy installation and registerMount() firing.
 */
export function advancePastGrace(): void {
  vi.spyOn(Date, "now").mockReturnValue(Date.now() + 10_000);
}
