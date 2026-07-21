// @vitest-environment happy-dom
//
// Tests for the Debug UI viewport snapshot feature (design §2.2, §2.4):
//   - Capture shape: assembled snapshot has correct fields and derived booleans.
//   - Missing API: capture proceeds with null fields when visualViewport or
//     input element is absent; derived booleans are null (AC2, AC7).
//   - Toast gating: success toast + ws.send() when connected; failure toast only
//     (no ws.send()) when not connected; failure toast when capture throws (AC9, §2.4).
//   - Menu rendering: "Debug UI" item renders and its click invokes capture (AC1).

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import "./app.js";
import { BrennApp } from "./app.js";
import { MockWebSocket } from "../test-utils/mock-websocket.js";
import type { WsClientMessage } from "../generated/WsClientMessage.js";
import type { DebugViewportSnapshotData } from "../generated/DebugViewportSnapshotData.js";

/** Narrow accessor for private internals used in tests.
 *
 * Fields reflect the actual BrennApp / BrennWs names as of this version;
 * if any are renamed the test will fail at runtime (cast breaks) rather than
 * at compile time. If a private field is renamed, update this interface. */
interface AppInternals {
  _captureDebugSnapshot(): void;
  currentUsername: string;
  toastHost?: { push: (o: { text: string; ttlMs?: number }) => void } | null;
  // BrennWs.trySend(msg) replaces the old isConnected()+send() pattern.
  ws: { trySend(msg: WsClientMessage): boolean } | null;
}

const realWebSocket = globalThis.WebSocket;

beforeEach(() => {
  MockWebSocket.instances = [];
  (globalThis as unknown as { WebSocket: unknown }).WebSocket =
    MockWebSocket as unknown;
  const slugMeta = document.createElement("meta");
  slugMeta.setAttribute("name", "app-slug");
  slugMeta.setAttribute("content", "test");
  document.head.appendChild(slugMeta);
});

afterEach(() => {
  (globalThis as unknown as { WebSocket: unknown }).WebSocket = realWebSocket;
  document.body.replaceChildren();
  document.head
    .querySelectorAll('meta[name="app-slug"], meta[name="initial-conversation-id"]')
    .forEach((el) => el.remove());
  vi.restoreAllMocks();
});

async function mountConnectedApp(): Promise<{
  app: BrennApp;
  ws: MockWebSocket;
}> {
  const app = document.createElement("brenn-app") as BrennApp;
  document.body.appendChild(app);
  await app.updateComplete;
  expect(MockWebSocket.instances.length).toBe(1);
  const ws = MockWebSocket.instances[0]!;
  // Let the microtask-deferred open fire.
  await Promise.resolve();
  await app.updateComplete;
  return { app, ws };
}

// ---------------------------------------------------------------------------
// Capture shape
// ---------------------------------------------------------------------------

describe("_captureDebugSnapshot — capture shape when connected", () => {
  it("sends a DebugViewportSnapshot message with populated fields and derived booleans", async () => {
    const { app, ws } = await mountConnectedApp();
    const appI = app as unknown as AppInternals;

    const sentBefore = ws.sent.length;
    appI._captureDebugSnapshot();

    // Exactly one new message should have been sent.
    const newMsgs = ws.sent.slice(sentBefore);
    expect(newMsgs.length).toBe(1);

    const msg = JSON.parse(newMsgs[0]!) as WsClientMessage;
    expect(msg.type).toBe("DebugViewportSnapshot");

    // Snapshot must carry the snapshot field.
    const snap = (msg as Extract<WsClientMessage, { type: "DebugViewportSnapshot" }>).snapshot as DebugViewportSnapshotData;

    // Scalar fields from window — happy-dom provides these.
    expect(typeof snap.inner_width).toBe("number");
    expect(typeof snap.inner_height).toBe("number");
    expect(typeof snap.device_pixel_ratio).toBe("number");
    expect(typeof snap.scroll_x).toBe("number");
    expect(typeof snap.scroll_y).toBe("number");

    // build_id is a non-empty string.
    expect(typeof snap.build_id).toBe("string");
    expect(snap.build_id.length).toBeGreaterThan(0);

    // client_timestamp is an ISO 8601 string.
    expect(typeof snap.client_timestamp).toBe("string");
    expect(() => new Date(snap.client_timestamp)).not.toThrow();

    // user_agent is present.
    expect(typeof snap.user_agent).toBe("string");

    // visibility_state is present.
    expect(typeof snap.visibility_state).toBe("string");

    // display_mode_standalone and max_width_768 are booleans.
    expect(typeof snap.display_mode_standalone).toBe("boolean");
    expect(typeof snap.max_width_768).toBe("boolean");
  });

  it("computes derived booleans (false branch): input within layout viewport", async () => {
    const { app, ws } = await mountConnectedApp();
    const appI = app as unknown as AppInternals;

    // Add a fake #input element. In happy-dom getBoundingClientRect returns zeros,
    // so input.bottom (0) > innerHeight (≥0) is false — tests the false branch.
    const input = document.createElement("textarea");
    input.id = "input";
    document.body.appendChild(input);

    appI._captureDebugSnapshot();

    const newMsgs = ws.sent.slice(-1);
    expect(newMsgs.length).toBe(1);
    const msg = JSON.parse(newMsgs[0]!) as WsClientMessage;
    const snap = (msg as Extract<WsClientMessage, { type: "DebugViewportSnapshot" }>).snapshot as DebugViewportSnapshotData;

    expect(snap.input).not.toBeNull();
    // In happy-dom, bottom=0 and innerHeight≥0, so input_bottom_below_layout is false.
    expect(snap.input_bottom_below_layout).toBe(false);

    // input_bottom_below_visual_fold: null when visualViewport is null (no soft keyboard
    // in happy-dom), or the computed value.
    if (snap.visual_viewport === null) {
      expect(snap.input_bottom_below_visual_fold).toBeNull();
    } else {
      const expectedFold =
        snap.input!.bottom > snap.visual_viewport.offset_top + snap.visual_viewport.height;
      expect(snap.input_bottom_below_visual_fold).toBe(expectedFold);
    }

    document.body.removeChild(input);
  });

  it("computes derived booleans (true branch): input below layout viewport fold", async () => {
    const { app, ws } = await mountConnectedApp();
    const appI = app as unknown as AppInternals;

    const input = document.createElement("textarea");
    input.id = "input";
    document.body.appendChild(input);

    // Stub getBoundingClientRect on Element.prototype so the rect() helper in
    // _captureDebugSnapshot sees a large bottom value. Use a specific bottom
    // value (window.innerHeight + 1000) that is guaranteed to exceed the
    // layout viewport regardless of what happy-dom sets innerHeight to.
    const fakeBottom = window.innerHeight + 1000;
    vi.spyOn(Element.prototype, "getBoundingClientRect").mockReturnValue({
      top: fakeBottom - 50, left: 0, right: 390, bottom: fakeBottom, width: 390, height: 50,
      toJSON: () => ({}),
      x: 0, y: fakeBottom - 50,
    } as DOMRect);

    appI._captureDebugSnapshot();

    const newMsgs = ws.sent.slice(-1);
    const msg = JSON.parse(newMsgs[0]!) as WsClientMessage;
    const snap = (msg as Extract<WsClientMessage, { type: "DebugViewportSnapshot" }>).snapshot as DebugViewportSnapshotData;

    expect(snap.input).not.toBeNull();
    // input.bottom (innerHeight + 1000) > window.innerHeight — must be true.
    expect(snap.input_bottom_below_layout).toBe(true);

    document.body.removeChild(input);
  });
});

// ---------------------------------------------------------------------------
// Missing API
// ---------------------------------------------------------------------------

describe("_captureDebugSnapshot — missing API / missing elements", () => {
  it("proceeds with null fields when visualViewport is absent; derived booleans are null", async () => {
    const { app, ws } = await mountConnectedApp();
    const appI = app as unknown as AppInternals;

    // Remove visualViewport if present; define as undefined.
    const realVV = (window as unknown as { visualViewport: unknown }).visualViewport;
    Object.defineProperty(window, "visualViewport", {
      value: undefined,
      configurable: true,
    });

    // Remove #input if present.
    document.querySelector("#input")?.remove();

    let threw = false;
    try {
      appI._captureDebugSnapshot();
    } catch {
      threw = true;
    }
    expect(threw).toBe(false);

    const newMsgs = ws.sent.slice(-1);
    const msg = JSON.parse(newMsgs[0]!) as WsClientMessage;
    const snap = (msg as Extract<WsClientMessage, { type: "DebugViewportSnapshot" }>).snapshot as DebugViewportSnapshotData;

    // visualViewport must be null.
    expect(snap.visual_viewport).toBeNull();
    // input rect must be null (no #input element).
    expect(snap.input).toBeNull();
    // derived booleans must be null when their inputs are absent.
    expect(snap.input_bottom_below_visual_fold).toBeNull();
    expect(snap.input_bottom_below_layout).toBeNull();

    // Restore.
    Object.defineProperty(window, "visualViewport", {
      value: realVV,
      configurable: true,
    });
  });

  it("does not throw or crash the app when elements are absent", async () => {
    const { app, ws } = await mountConnectedApp();
    const appI = app as unknown as AppInternals;

    // Ensure no optional elements are present.
    for (const sel of [
      "#input",
      "brenn-input-bar",
      ".app-main",
      "brenn-pane-layout",
      "brenn-message-list",
      ".attachment-strip",
      ".chip-bar",
      ".presence-bar",
      "brenn-status-bar",
    ]) {
      document.querySelector(sel)?.remove();
    }

    const sentBefore = ws.sent.length;
    expect(() => { appI._captureDebugSnapshot(); }).not.toThrow();

    // A message was still sent.
    expect(ws.sent.length).toBeGreaterThan(sentBefore);
    const msg = JSON.parse(ws.sent[ws.sent.length - 1]!) as WsClientMessage;
    expect(msg.type).toBe("DebugViewportSnapshot");
    const snap = (msg as Extract<WsClientMessage, { type: "DebugViewportSnapshot" }>).snapshot as DebugViewportSnapshotData;
    // brenn-pane-layout and brenn-message-list removed above → computed-style fields null.
    expect(snap.pane_layout_min_height).toBeNull();
    expect(snap.pane_layout_height).toBeNull();
    expect(snap.message_list_min_height).toBeNull();
    expect(snap.message_list_height).toBeNull();
  });

  // New optional elements absent → rect and computed-style fields must be null.
  it("serializes header-band and mobile-slot-content fields as null when those elements are absent", async () => {
    const { app, ws } = await mountConnectedApp();
    const appI = app as unknown as AppInternals;

    // Ensure the new optional elements are not in DOM.
    for (const sel of [".mobile-slot-content", ".app-header", ".app-topbar", ".app-layout"]) {
      document.querySelector(sel)?.remove();
    }

    const sentBefore = ws.sent.length;
    expect(() => { appI._captureDebugSnapshot(); }).not.toThrow();

    const newMsgs = ws.sent.slice(sentBefore);
    expect(newMsgs.length).toBe(1);
    const msg = JSON.parse(newMsgs[0]!) as WsClientMessage;
    const snap = (msg as Extract<WsClientMessage, { type: "DebugViewportSnapshot" }>).snapshot as DebugViewportSnapshotData;

    // Elements absent → rect and computed-style fields must be null.
    expect(snap.mobile_slot_content_min_height).toBeNull();
    expect(snap.app_topbar).toBeNull();
    expect(snap.app_header).toBeNull();
    expect(snap.app_layout).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Mid-chain and header-band field shape (capture with stubs present)
// ---------------------------------------------------------------------------

describe("_captureDebugSnapshot — mid-chain and header-band fields", () => {
  it("populates mid-chain flex and header-band fields when elements are present in DOM", async () => {
    const { app, ws } = await mountConnectedApp();
    const appI = app as unknown as AppInternals;

    // Remove any pre-existing matching elements from the app's own light DOM so
    // document.querySelector reliably returns the stubs we're about to add.
    // BrennApp uses createRenderRoot() { return this; } (light DOM), so these may
    // already exist. Removing them first makes the test deterministic.
    for (const sel of [
      ".app-layout", ".app-topbar", ".app-header", ".mobile-slot-content",
      "brenn-pane-layout", "brenn-message-list", ".app-main",
    ]) {
      document.querySelector(sel)?.remove();
    }

    // Add stub elements.
    const stubs: HTMLElement[] = [];
    const addStub = (tag: string, cls?: string) => {
      const el = document.createElement(tag);
      if (cls) el.className = cls;
      document.body.appendChild(el);
      stubs.push(el);
    };
    addStub("div", "app-layout");
    addStub("div", "app-topbar");
    addStub("header", "app-header");
    addStub("div", "mobile-slot-content");
    const paneLayoutStub = document.createElement("brenn-pane-layout");
    document.body.appendChild(paneLayoutStub);
    stubs.push(paneLayoutStub);
    const messageListStub = document.createElement("brenn-message-list");
    document.body.appendChild(messageListStub);
    stubs.push(messageListStub);
    const appMainStub = document.createElement("div");
    appMainStub.className = "app-main";
    document.body.appendChild(appMainStub);
    stubs.push(appMainStub);

    // Spy on getComputedStyle so cs() receives a real value, confirming the call
    // was actually made and the field is populated with a string (not always null).
    const realGetComputedStyle = window.getComputedStyle.bind(window);
    vi.spyOn(window, "getComputedStyle").mockImplementation((el, pseudo) => {
      const style = realGetComputedStyle(el, pseudo ?? undefined);
      // Return a real CSSStyleDeclaration with getPropertyValue intercepted.
      return new Proxy(style, {
        get(target, prop) {
          if (prop === "getPropertyValue") {
            return (name: string) => {
              if (name === "min-height") return "0px";
              if (name === "height") return "100px";
              return target.getPropertyValue(name);
            };
          }
          return (target as unknown as Record<string, unknown>)[prop as string];
        },
      });
    });

    const sentBefore = ws.sent.length;
    appI._captureDebugSnapshot();

    const newMsgs = ws.sent.slice(sentBefore);
    expect(newMsgs.length).toBe(1);
    const msg = JSON.parse(newMsgs[0]!) as WsClientMessage;
    const snap = (msg as Extract<WsClientMessage, { type: "DebugViewportSnapshot" }>).snapshot as DebugViewportSnapshotData;

    // Rect fields for the new elements must be non-null objects (happy-dom returns zeros).
    expect(snap.app_topbar).not.toBeNull();
    expect(typeof snap.app_topbar!.height).toBe("number");
    expect(snap.app_header).not.toBeNull();
    expect(typeof snap.app_header!.height).toBe("number");
    expect(snap.app_layout).not.toBeNull();
    expect(typeof snap.app_layout!.height).toBe("number");

    // Computed-style fields: the spy returns "0px" for min-height — assert a real
    // string value to confirm cs() was called and the field is not spuriously null.
    expect(snap.pane_layout_min_height).toBe("0px");
    expect(snap.message_list_min_height).toBe("0px");
    expect(snap.mobile_slot_content_min_height).toBe("0px");
    expect(snap.pane_layout_height).toBe("100px");
    expect(snap.message_list_height).toBe("100px");
    expect(snap.app_main_height).toBe("100px");

    // document_element_offset_height is a number (documentElement is always present).
    expect(typeof snap.document_element_offset_height).toBe("number");

    // Cleanup.
    stubs.forEach((el) => el.remove());
  });
});

// ---------------------------------------------------------------------------
// Viewport-unit probe fields
// ---------------------------------------------------------------------------

describe("_captureDebugSnapshot — viewport-unit probe fields", () => {
  it("probe fields have correct values: 0 when supported in happy-dom, null when unsupported (test-1/test-2)", async () => {
    // In happy-dom, CSS.supports returns true for all four viewport height units and
    // getBoundingClientRect().height returns 0 for all elements (no layout engine).
    // The probe loop gates on CSS.supports before measuring; each supported unit
    // assigns the measured value (0) rather than leaving the variable null.
    // This test verifies the full value-assignment path (not just type or presence).
    const { app, ws } = await mountConnectedApp();
    const appI = app as unknown as AppInternals;

    const sentBefore = ws.sent.length;
    appI._captureDebugSnapshot();

    const newMsgs = ws.sent.slice(sentBefore);
    expect(newMsgs.length).toBe(1);
    const msg = JSON.parse(newMsgs[0]!) as WsClientMessage;
    const snap = (msg as Extract<WsClientMessage, { type: "DebugViewportSnapshot" }>).snapshot as DebugViewportSnapshotData;

    // All four units are supported in happy-dom; each probe measures 0 (layout engine
    // returns 0 for all rects). Assert the actual numeric value, not just presence/type —
    // this exercises the if/else assignment chain in app.ts and catches cross-assignment
    // or missed branches (test-1 fix).
    expect(snap.probe_100vh_px).toBe(0);
    expect(snap.probe_100svh_px).toBe(0);
    expect(snap.probe_100lvh_px).toBe(0);
    expect(snap.probe_100dvh_px).toBe(0);

    // screen_avail_height and window_outer_height are required non-Option numbers.
    expect(typeof snap.screen_avail_height).toBe("number");
    expect(typeof snap.window_outer_height).toBe("number");
  });

  it("probe element is removed from documentElement after capture (no DOM side effects, test-3)", async () => {
    // Probes are appended to document.documentElement, not document.body, to avoid
    // body overflow/transform affecting the containing block for position:absolute.
    // Spy on documentElement.appendChild to verify insertion happened, then verify
    // removal (net child count restored). This catches both "never inserted" and
    // "inserted but not removed" regressions.
    const { app } = await mountConnectedApp();
    const appI = app as unknown as AppInternals;

    const appendSpy = vi.spyOn(document.documentElement, "appendChild");
    const htmlChildCountBefore = document.documentElement.children.length;
    appI._captureDebugSnapshot();

    // At least 5 probes are inserted (1 safe-area + 4 viewport-unit).
    expect(appendSpy).toHaveBeenCalled();
    expect(appendSpy.mock.calls.length).toBeGreaterThanOrEqual(5);
    // All probes must be removed synchronously by the finally blocks.
    expect(document.documentElement.children.length).toBe(htmlChildCountBefore);
  });

  it("per-unit catch: capture completes when one probe getBoundingClientRect throws; other probes still capture (test-5)", async () => {
    // Verify the per-probe try/catch: if getBoundingClientRect throws for a probe element,
    // that unit's variable stays null, but capture completes without throwing and the
    // other three units are still measured.
    //
    // Strategy: track which elements are probe elements by intercepting
    // document.documentElement.appendChild. The first probe-element getBoundingClientRect
    // call throws; the others return the normal happy-dom zero rect.
    const { app, ws } = await mountConnectedApp();
    const appI = app as unknown as AppInternals;

    const probeElements = new Set<Element>();
    let probeThrowCount = 0;
    const origAppendChild = document.documentElement.appendChild.bind(document.documentElement);
    vi.spyOn(document.documentElement, "appendChild").mockImplementation(function <T extends Node>(node: T): T {
      // Track elements appended to documentElement during capture as probe candidates.
      if (node instanceof Element) probeElements.add(node);
      return origAppendChild(node);
    });

    const origGetBCR = Element.prototype.getBoundingClientRect;
    vi.spyOn(Element.prototype, "getBoundingClientRect").mockImplementation(function (this: Element) {
      if (probeElements.has(this)) {
        probeThrowCount++;
        if (probeThrowCount === 1) throw new Error("simulated probe failure");
      }
      return origGetBCR.call(this);
    });

    const sentBefore = ws.sent.length;
    // Capture must not throw even though one probe throws internally.
    expect(() => appI._captureDebugSnapshot()).not.toThrow();

    const newMsgs = ws.sent.slice(sentBefore);
    expect(newMsgs.length).toBe(1);
    const msg = JSON.parse(newMsgs[0]!) as WsClientMessage;
    const snap = (msg as Extract<WsClientMessage, { type: "DebugViewportSnapshot" }>).snapshot as DebugViewportSnapshotData;

    // First probe threw → null; remaining three measured → 0.
    const probeValues = [snap.probe_100vh_px, snap.probe_100svh_px, snap.probe_100lvh_px, snap.probe_100dvh_px];
    const nullCount = probeValues.filter((v) => v === null).length;
    const numericCount = probeValues.filter((v) => typeof v === "number").length;
    expect(nullCount).toBe(1);
    expect(numericCount).toBe(3);

    // The faulting unit must be recorded in probe_exception_units.
    expect(snap.probe_exception_units).not.toBeNull();
    expect((snap.probe_exception_units as string[]).length).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Toast gating
// ---------------------------------------------------------------------------

describe("_captureDebugSnapshot — toast gating", () => {
  it("shows success toast and sends ws message when connected (trySend returns true)", async () => {
    const { app } = await mountConnectedApp();
    const appI = app as unknown as AppInternals;

    const pushFn = vi.fn();
    Object.defineProperty(app, "toastHost", { get: () => ({ push: pushFn }), configurable: true });

    let capturedMsg: WsClientMessage | null = null;
    // Stub ws.trySend to return true (connected) and capture the message.
    appI.ws = { trySend: (msg: WsClientMessage) => { capturedMsg = msg; return true; } };

    appI._captureDebugSnapshot();

    expect(capturedMsg).not.toBeNull();
    expect((capturedMsg as unknown as WsClientMessage).type).toBe("DebugViewportSnapshot");

    expect(pushFn).toHaveBeenCalledTimes(1);
    const pushed = pushFn.mock.calls[0]![0] as { text: string; ttlMs?: number };
    expect(pushed.text).toContain("sent");
  });

  it("shows failure toast and does NOT call trySend when ws is null", async () => {
    const { app } = await mountConnectedApp();
    const appI = app as unknown as AppInternals;

    const pushFn = vi.fn();
    Object.defineProperty(app, "toastHost", { get: () => ({ push: pushFn }), configurable: true });

    // No ws (null) → optional-chain trySend returns undefined (falsy).
    appI.ws = null;

    appI._captureDebugSnapshot();

    // Exactly one failure toast.
    expect(pushFn).toHaveBeenCalledTimes(1);
    const pushed = pushFn.mock.calls[0]![0] as { text: string; ttlMs?: number };
    expect(pushed.text.toLowerCase()).toContain("failed");
    expect(pushed.ttlMs).toBeGreaterThanOrEqual(4000);
  });

  it("shows failure toast when trySend returns false (not connected at send time)", async () => {
    const { app } = await mountConnectedApp();
    const appI = app as unknown as AppInternals;

    const pushFn = vi.fn();
    Object.defineProperty(app, "toastHost", { get: () => ({ push: pushFn }), configurable: true });

    // trySend returns false → socket not OPEN at send time.
    appI.ws = { trySend: () => false };

    appI._captureDebugSnapshot();

    // Exactly one failure toast — no success toast, no double-toast.
    expect(pushFn).toHaveBeenCalledTimes(1);
    const pushed = pushFn.mock.calls[0]![0] as { text: string; ttlMs?: number };
    expect(pushed.text.toLowerCase()).toContain("failed");
    expect(pushed.ttlMs).toBeGreaterThanOrEqual(4000);
  });

  it("shows failure toast when DOM capture throws mid-body (AC7 — single toast only)", async () => {
    const { app } = await mountConnectedApp();
    const appI = app as unknown as AppInternals;

    const pushFn = vi.fn();
    Object.defineProperty(app, "toastHost", { get: () => ({ push: pushFn }), configurable: true });

    const trySendFn = vi.fn();
    appI.ws = { trySend: trySendFn };

    // Suppress console.error: reportClientError (called in the catch) logs there.
    const errorFn = vi.spyOn(console, "error").mockImplementation(() => {});

    // Stub document.createElement to throw when called with 'div' — this fires
    // inside the safe-area probe, mid-body of the try block, before trySend.
    vi.spyOn(document, "createElement").mockImplementation((tag: string) => {
      if (tag === "div") throw new Error("forced DOM error");
      // For other tags use the real implementation via the HTMLDocument prototype.
      return Object.getPrototypeOf(document).createElement.call(document, tag) as HTMLElement;
    });

    appI._captureDebugSnapshot();

    // trySend must NOT have been called (throw happened before send).
    expect(trySendFn).not.toHaveBeenCalled();
    // Exactly one failure toast (the catch branch).
    expect(pushFn).toHaveBeenCalledTimes(1);
    const pushed = pushFn.mock.calls[0]![0] as { text: string };
    expect(pushed.text.toLowerCase()).toContain("failed");
    // reportClientError (and hence console.error) must have been called.
    expect(errorFn).toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// Menu rendering
// ---------------------------------------------------------------------------

describe("_renderUserMenu — Debug UI item", () => {
  it("renders 'Debug UI' button in the user menu when username is set", async () => {
    const { app, ws } = await mountConnectedApp();
    const appI = app as unknown as AppInternals;

    // Set currentUsername so the menu renders (it guards on this being truthy).
    appI.currentUsername = "testuser";

    // Open the menu so the dropdown is rendered.
    (appI as unknown as { userMenuOpen: boolean }).userMenuOpen = true;
    await app.updateComplete;

    // The "Debug UI" button must be in the rendered shadow DOM / light DOM.
    // BrennApp uses LitElement; renderRoot is shadowRoot or 'this' depending on
    // createRenderRoot. The buttons end up in the shadow root.
    const root = app.shadowRoot ?? app;
    const buttons = Array.from(root.querySelectorAll("button.user-menu-item"));
    const debugBtn = buttons.find((b) => b.textContent?.trim() === "Debug UI");
    expect(debugBtn).toBeDefined();

    ws; // suppress unused warning
  });

  it("clicking 'Debug UI' closes the menu and invokes _captureDebugSnapshot", async () => {
    const { app } = await mountConnectedApp();
    const appI = app as unknown as AppInternals;

    appI.currentUsername = "testuser";
    (appI as unknown as { userMenuOpen: boolean }).userMenuOpen = true;
    await app.updateComplete;

    // Spy on _captureDebugSnapshot before the click.
    const captureSpy = vi.spyOn(
      app as unknown as { _captureDebugSnapshot(): void },
      "_captureDebugSnapshot",
    ).mockImplementation(() => {});

    const root = app.shadowRoot ?? app;
    const buttons = Array.from(root.querySelectorAll("button.user-menu-item"));
    const debugBtn = buttons.find((b) => b.textContent?.trim() === "Debug UI") as HTMLElement | undefined;
    expect(debugBtn).toBeDefined();
    debugBtn!.click();

    await app.updateComplete;

    expect(captureSpy).toHaveBeenCalledTimes(1);
    // Menu should be closed after the click.
    expect((appI as unknown as { userMenuOpen: boolean }).userMenuOpen).toBe(false);
  });
});
