import { describe, it, expect, beforeEach, vi } from "vitest";
import {
  setChatHasText,
  registerMount,
  unregisterMount,
  canInterceptKeyboard,
  eventOriginatedInside,
} from "./keyboard-guard.js";

/**
 * Create a minimal Element-like object for testing.
 * WeakMap keys just need to be objects; we don't need real DOM elements.
 */
function fakeElement(): Element {
  return {} as unknown as Element;
}

beforeEach(() => {
  // Reset shared state between tests.
  setChatHasText(false);
});

describe("canInterceptKeyboard", () => {
  it("returns false for an unregistered element", () => {
    const el = fakeElement();
    expect(canInterceptKeyboard(el)).toBe(false);
  });

  it("returns false during the 500ms grace period", () => {
    const el = fakeElement();
    registerMount(el);
    expect(canInterceptKeyboard(el)).toBe(false);
  });

  it("returns true after the grace period with empty chat", () => {
    const el = fakeElement();

    // Mock Date.now to control time.
    const now = Date.now();
    vi.spyOn(Date, "now").mockReturnValue(now);

    registerMount(el);

    // Still in grace period.
    vi.spyOn(Date, "now").mockReturnValue(now + 499);
    expect(canInterceptKeyboard(el)).toBe(false);

    // Grace period elapsed.
    vi.spyOn(Date, "now").mockReturnValue(now + 500);
    expect(canInterceptKeyboard(el)).toBe(true);

    vi.restoreAllMocks();
  });

  it("returns false when chat has text, even after grace period", () => {
    const el = fakeElement();

    const now = Date.now();
    vi.spyOn(Date, "now").mockReturnValue(now);
    registerMount(el);

    // Advance past grace period.
    vi.spyOn(Date, "now").mockReturnValue(now + 1000);
    expect(canInterceptKeyboard(el)).toBe(true);

    // Chat gets text — now blocked.
    setChatHasText(true);
    expect(canInterceptKeyboard(el)).toBe(false);

    // Chat cleared — allowed again.
    setChatHasText(false);
    expect(canInterceptKeyboard(el)).toBe(true);

    vi.restoreAllMocks();
  });

  it("returns false after unregisterMount", () => {
    const el = fakeElement();

    const now = Date.now();
    vi.spyOn(Date, "now").mockReturnValue(now);
    registerMount(el);

    vi.spyOn(Date, "now").mockReturnValue(now + 1000);
    expect(canInterceptKeyboard(el)).toBe(true);

    unregisterMount(el);
    expect(canInterceptKeyboard(el)).toBe(false);

    vi.restoreAllMocks();
  });

  it("tracks multiple elements independently", () => {
    const el1 = fakeElement();
    const el2 = fakeElement();

    const now = Date.now();
    vi.spyOn(Date, "now").mockReturnValue(now);
    registerMount(el1);

    // el2 mounts 300ms later.
    vi.spyOn(Date, "now").mockReturnValue(now + 300);
    registerMount(el2);

    // At now+500: el1 is past grace, el2 is not.
    vi.spyOn(Date, "now").mockReturnValue(now + 500);
    expect(canInterceptKeyboard(el1)).toBe(true);
    expect(canInterceptKeyboard(el2)).toBe(false);

    // At now+800: both past grace.
    vi.spyOn(Date, "now").mockReturnValue(now + 800);
    expect(canInterceptKeyboard(el1)).toBe(true);
    expect(canInterceptKeyboard(el2)).toBe(true);

    vi.restoreAllMocks();
  });
});

describe("eventOriginatedInside", () => {
  /**
   * Build a minimal Event whose composedPath() returns the given nodes.
   * happy-dom populates composedPath() only for real dispatched events;
   * for unit-testing the helper we stub composedPath directly.
   */
  function makeEvent(path: EventTarget[]): Event {
    const e = new Event("keydown", { bubbles: true, composed: true });
    vi.spyOn(e, "composedPath").mockReturnValue(path);
    return e;
  }

  it("returns true when the event target is the host itself", () => {
    const host = document.createElement("div");
    const e = makeEvent([host]);
    expect(eventOriginatedInside(e, host)).toBe(true);
  });

  it("returns true when the event target is a descendant of the host", () => {
    const host = document.createElement("div");
    const child = document.createElement("span");
    host.appendChild(child);
    // composedPath goes innermost-first: child, host, body, html, document, window
    const e = makeEvent([child, host]);
    expect(eventOriginatedInside(e, host)).toBe(true);
  });

  it("returns false when the event target is a sibling outside the host", () => {
    const host = document.createElement("div");
    const sibling = document.createElement("textarea");
    // composedPath does not include host
    const e = makeEvent([sibling]);
    expect(eventOriginatedInside(e, host)).toBe(false);
  });

  it("returns true for a shadow-DOM-hosted descendant via real dispatch (composedPath crosses shadow boundary)", () => {
    // Attach host to document so composedPath() is populated by the real
    // (happy-dom) implementation, not a stub — this exercises the actual
    // shadow-DOM traversal rather than a hand-rolled fake path.
    const host = document.createElement("div");
    document.body.appendChild(host);
    const shadowRoot = host.attachShadow({ mode: "open" });
    const inner = document.createElement("button");
    shadowRoot.appendChild(inner);

    let result: boolean | undefined;
    // Capture the event at the document level where composedPath() is still live.
    const handler = (e: Event) => { result = eventOriginatedInside(e, host); };
    document.addEventListener("keydown", handler);
    try {
      inner.dispatchEvent(new KeyboardEvent("keydown", { bubbles: true, composed: true }));
    } finally {
      document.removeEventListener("keydown", handler);
      document.body.removeChild(host);
    }
    expect(result).toBe(true);
  });

  it("returns false for a synthetic event with an empty composedPath (fail-closed)", () => {
    const host = document.createElement("div");
    const e = makeEvent([]);
    expect(eventOriginatedInside(e, host)).toBe(false);
  });
});
