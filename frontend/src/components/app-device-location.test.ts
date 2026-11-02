// @vitest-environment happy-dom
//
// The user-menu location toggle: rendered only when Welcome says the app may
// use location; enabling sends one DeviceLocationShare with the browser's
// fix; the server's DeviceLocationSharing drives the label and the reporter,
// whose sends are DeviceLocationFix; disabling sends DeviceLocationClear; a
// denied permission shows the error and sends nothing; a Share the server
// refuses shows an error; a disconnect stops the reporter; a changed browser
// zone is re-sent before the next fix.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import "./app.js";
import { BrennApp } from "./app.js";
import { MockWebSocket } from "../test-utils/mock-websocket.js";
import type { WsClientMessage } from "../generated/WsClientMessage.js";
import { REPORT_INTERVAL_MS } from "../location.js";

interface AppInternals {
  userMenuOpen: boolean;
}

/** Spy on the message list's appendError, where the app reports failures. */
function spyAppendError() {
  const proto = customElements.get("brenn-message-list")!.prototype as {
    appendError(text: string): void;
  };
  return vi.spyOn(proto, "appendError").mockImplementation(() => {});
}

const realWebSocket = globalThis.WebSocket;

/** The zone the test environment's browser reports, which every fix carries. */
const ZONE = Intl.DateTimeFormat().resolvedOptions().timeZone;

type Coords = { latitude: number; longitude: number; accuracy: number };
type Outcome = { coords: Coords } | { code: number } | "pending";
let outcome: Outcome;
let fixRequests: number;
/** Resolves the last request made while `outcome` was "pending". */
let resolvePending: ((coords: Coords) => void) | null;

beforeEach(() => {
  MockWebSocket.instances = [];
  (globalThis as unknown as { WebSocket: unknown }).WebSocket = MockWebSocket as unknown;
  const slugMeta = document.createElement("meta");
  slugMeta.setAttribute("name", "app-slug");
  slugMeta.setAttribute("content", "test");
  document.head.appendChild(slugMeta);
  outcome = { coords: { latitude: 51.5, longitude: -0.25, accuracy: 35 } };
  fixRequests = 0;
  resolvePending = null;
  Object.defineProperty(navigator, "geolocation", {
    value: {
      getCurrentPosition(
        success: (p: GeolocationPosition) => void,
        error: (e: GeolocationPositionError) => void,
      ): void {
        fixRequests++;
        if (outcome === "pending") {
          resolvePending = (coords) =>
            success({ coords, timestamp: 0 } as unknown as GeolocationPosition);
        } else if ("coords" in outcome) {
          success({ coords: outcome.coords, timestamp: 0 } as unknown as GeolocationPosition);
        } else {
          error({
            code: outcome.code,
            message: "fake",
            PERMISSION_DENIED: 1,
            POSITION_UNAVAILABLE: 2,
            TIMEOUT: 3,
          } as GeolocationPositionError);
        }
      },
    },
    configurable: true,
  });
});

afterEach(() => {
  (globalThis as unknown as { WebSocket: unknown }).WebSocket = realWebSocket;
  document.body.replaceChildren();
  document.head
    .querySelectorAll('meta[name="app-slug"], meta[name="initial-conversation-id"]')
    .forEach((el) => el.remove());
  delete (navigator as unknown as { geolocation?: unknown }).geolocation;
  vi.useRealTimers();
  vi.restoreAllMocks();
});

async function mountWithWelcome(deviceLocationEnabled: boolean): Promise<{ app: BrennApp; ws: MockWebSocket }> {
  const app = document.createElement("brenn-app") as BrennApp;
  document.body.appendChild(app);
  await app.updateComplete;
  const ws = MockWebSocket.instances[0]!;
  await Promise.resolve();
  await app.updateComplete;
  ws.deliver({
    type: "Welcome",
    username: "alice",
    user_id: 1,
    multiuser: false,
    singleton: true,
    available_models: [],
    default_model: "sonnet",
    attachment_targets: [],
    pwa_push_enabled: false,
    device_location_enabled: deviceLocationEnabled,
  });
  ws.deliver({ type: "SetLayout", layout: { type: "SinglePane" } });
  await app.updateComplete;
  await app.updateComplete;
  return { app, ws };
}

async function openMenu(app: BrennApp): Promise<void> {
  (app as unknown as AppInternals).userMenuOpen = true;
  await app.updateComplete;
}

function menuButton(app: BrennApp, label: string): HTMLButtonElement | undefined {
  const root = app.shadowRoot ?? app;
  return Array.from(root.querySelectorAll<HTMLButtonElement>("button.user-menu-item")).find(
    (b) => b.textContent?.trim() === label,
  );
}

function sentOfType<T extends WsClientMessage["type"]>(
  ws: MockWebSocket,
  type: T,
  from = 0,
): Extract<WsClientMessage, { type: T }>[] {
  return ws.sent
    .slice(from)
    .map((s) => JSON.parse(s) as WsClientMessage)
    .filter((m): m is Extract<WsClientMessage, { type: T }> => m.type === type);
}

async function settle(app: BrennApp): Promise<void> {
  for (let i = 0; i < 5; i++) await Promise.resolve();
  await app.updateComplete;
}

describe("location menu item", () => {
  it("is absent when the app may not use location", async () => {
    const { app } = await mountWithWelcome(false);
    await openMenu(app);
    expect(menuButton(app, "Share location from this device")).toBeUndefined();
    expect(menuButton(app, "Stop sharing location from this device")).toBeUndefined();
  });

  it("enables, follows the server's state, reports fixes and disables", async () => {
    vi.useFakeTimers();
    const { app, ws } = await mountWithWelcome(true);
    await openMenu(app);
    const share = menuButton(app, "Share location from this device");
    expect(share).toBeDefined();

    const before = ws.sent.length;
    share!.click();
    await settle(app);
    expect((app as unknown as AppInternals).userMenuOpen).toBe(false);
    expect(sentOfType(ws, "DeviceLocationShare", before)).toEqual([
      { type: "DeviceLocationShare", latitude: 51.5, longitude: -0.25, accuracy_m: 35, time_zone: ZONE },
    ]);
    expect(sentOfType(ws, "DeviceLocationFix", before)).toHaveLength(0);

    // The server confirms: label flips and the reporter starts.
    const beforeSharing = ws.sent.length;
    ws.deliver({ type: "DeviceLocationSharing", enabled: true });
    await settle(app);
    expect(sentOfType(ws, "DeviceLocationFix", beforeSharing)).toHaveLength(1);
    outcome = { coords: { latitude: 48.75, longitude: 2.25, accuracy: 12.5 } };
    await vi.advanceTimersByTimeAsync(REPORT_INTERVAL_MS);
    expect(sentOfType(ws, "DeviceLocationFix", beforeSharing)).toEqual([
      { type: "DeviceLocationFix", latitude: 51.5, longitude: -0.25, accuracy_m: 35, time_zone: ZONE },
      { type: "DeviceLocationFix", latitude: 48.75, longitude: 2.25, accuracy_m: 12.5, time_zone: ZONE },
    ]);

    await openMenu(app);
    const stop = menuButton(app, "Stop sharing location from this device");
    expect(stop).toBeDefined();
    const beforeStop = ws.sent.length;
    stop!.click();
    await settle(app);
    expect(sentOfType(ws, "DeviceLocationClear", beforeStop)).toHaveLength(1);

    // The server confirms: the reporter stops.
    ws.deliver({ type: "DeviceLocationSharing", enabled: false });
    await settle(app);
    const afterClear = ws.sent.length;
    await vi.advanceTimersByTimeAsync(3 * REPORT_INTERVAL_MS);
    expect(sentOfType(ws, "DeviceLocationFix", afterClear)).toHaveLength(0);
    await openMenu(app);
    expect(menuButton(app, "Share location from this device")).toBeDefined();
  });

  it("shows the error and sends nothing when permission is denied", async () => {
    const { app, ws } = await mountWithWelcome(true);
    const errorSpy = spyAppendError();
    outcome = { code: 1 };
    await openMenu(app);
    const before = ws.sent.length;
    menuButton(app, "Share location from this device")!.click();
    await settle(app);
    expect(ws.sent.slice(before)).toHaveLength(0);
    expect(errorSpy).toHaveBeenCalledWith(
      "Location permission was denied. Allow location for this site in browser settings to share it.",
    );
  });

  it("shows an error when the server refuses a Share, and not after a Clear", async () => {
    const { app, ws } = await mountWithWelcome(true);
    const errorSpy = spyAppendError();
    await openMenu(app);
    const before = ws.sent.length;
    menuButton(app, "Share location from this device")!.click();
    await settle(app);
    expect(sentOfType(ws, "DeviceLocationShare", before)).toHaveLength(1);

    ws.deliver({ type: "DeviceLocationSharing", enabled: false });
    await settle(app);
    expect(errorSpy.mock.calls).toEqual([["Location sharing is not available in this app right now."]]);
    await openMenu(app);
    expect(menuButton(app, "Share location from this device")).toBeDefined();

    // A Clear's answer is not a refusal.
    errorSpy.mockClear();
    ws.deliver({ type: "DeviceLocationSharing", enabled: true });
    await settle(app);
    await openMenu(app);
    menuButton(app, "Stop sharing location from this device")!.click();
    await settle(app);
    expect(sentOfType(ws, "DeviceLocationClear", before)).toHaveLength(1);
    ws.deliver({ type: "DeviceLocationSharing", enabled: false });
    await settle(app);
    expect(errorSpy).not.toHaveBeenCalled();
  });

  it("does not report a Share dropped on a closed socket as a refusal on the next connect", async () => {
    vi.useFakeTimers();
    const { app, ws } = await mountWithWelcome(true);
    const errorSpy = spyAppendError();
    outcome = "pending";
    await openMenu(app);
    menuButton(app, "Share location from this device")!.click();
    await settle(app);
    expect(resolvePending).not.toBeNull();

    ws.close();
    await settle(app);
    resolvePending!({ latitude: 51.5, longitude: -0.25, accuracy: 35 });
    await settle(app);
    expect(sentOfType(ws, "DeviceLocationShare")).toHaveLength(0);

    await vi.advanceTimersByTimeAsync(REPORT_INTERVAL_MS);
    const reconnected = MockWebSocket.instances[MockWebSocket.instances.length - 1]!;
    expect(reconnected).not.toBe(ws);
    reconnected.deliver({ type: "DeviceLocationSharing", enabled: false });
    await settle(app);
    expect(errorSpy.mock.calls).toEqual([["Not connected — message not sent. Reconnecting..."]]);
  });

  it("clears and reports when the reporter finds the permission withdrawn", async () => {
    const { app, ws } = await mountWithWelcome(true);
    const errorSpy = spyAppendError();
    outcome = { code: 1 };
    const before = ws.sent.length;
    ws.deliver({ type: "DeviceLocationSharing", enabled: true });
    await settle(app);
    expect(sentOfType(ws, "DeviceLocationClear", before)).toHaveLength(1);
    expect(sentOfType(ws, "DeviceLocationFix", before)).toHaveLength(0);
    expect(errorSpy).toHaveBeenCalledTimes(1);
  });

  it("stops the reporter on disconnect; the next connect's sharing state starts a fresh one", async () => {
    vi.useFakeTimers();
    const { app, ws } = await mountWithWelcome(true);
    ws.deliver({ type: "DeviceLocationSharing", enabled: true });
    await settle(app);
    expect(sentOfType(ws, "DeviceLocationFix")).toHaveLength(1);

    ws.close();
    await settle(app);
    const requestsAtClose = fixRequests;
    // Past the reconnect delay and a full report interval, with a
    // visibility change on the way: the stopped reporter asks for nothing.
    await vi.advanceTimersByTimeAsync(REPORT_INTERVAL_MS + 1);
    document.dispatchEvent(new Event("visibilitychange"));
    await settle(app);
    expect(fixRequests).toBe(requestsAtClose);

    const reconnected = MockWebSocket.instances[MockWebSocket.instances.length - 1]!;
    expect(reconnected).not.toBe(ws);
    expect(sentOfType(reconnected, "DeviceLocationFix")).toHaveLength(0);
    reconnected.deliver({ type: "DeviceLocationSharing", enabled: true });
    await settle(app);
    expect(sentOfType(reconnected, "DeviceLocationFix")).toHaveLength(1);
  });

  it("re-sends SetTimezone before a fix when the browser's zone has changed", async () => {
    vi.useFakeTimers();
    let zone = "Europe/Berlin";
    const RealDateTimeFormat = Intl.DateTimeFormat;
    vi.spyOn(Intl, "DateTimeFormat").mockImplementation(function (
      ...args: ConstructorParameters<typeof Intl.DateTimeFormat>
    ) {
      const format = new RealDateTimeFormat(...args);
      const real = format.resolvedOptions.bind(format);
      format.resolvedOptions = () => ({ ...real(), timeZone: zone });
      return format;
    } as typeof Intl.DateTimeFormat);
    const { app, ws } = await mountWithWelcome(true);
    expect(sentOfType(ws, "SetTimezone")).toEqual([{ type: "SetTimezone", timezone: "Europe/Berlin" }]);

    ws.deliver({ type: "DeviceLocationSharing", enabled: true });
    await settle(app);
    // Unchanged zone: the fix goes alone.
    expect(sentOfType(ws, "SetTimezone")).toHaveLength(1);
    expect(sentOfType(ws, "DeviceLocationFix")).toHaveLength(1);

    zone = "Asia/Tokyo";
    const before = ws.sent.length;
    await vi.advanceTimersByTimeAsync(REPORT_INTERVAL_MS);
    const types = ws.sent.slice(before).map((s) => (JSON.parse(s) as WsClientMessage).type);
    expect(types).toEqual(["SetTimezone", "DeviceLocationFix"]);
    expect(sentOfType(ws, "SetTimezone", before)).toEqual([{ type: "SetTimezone", timezone: "Asia/Tokyo" }]);
    // The fix carries the zone the browser reported with it.
    expect(sentOfType(ws, "DeviceLocationFix", before)).toEqual([
      { type: "DeviceLocationFix", latitude: 51.5, longitude: -0.25, accuracy_m: 35, time_zone: "Asia/Tokyo" },
    ]);
  });
});
