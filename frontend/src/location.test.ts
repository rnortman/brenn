// @vitest-environment happy-dom
//
// requestFix against a faked navigator.geolocation (happy-dom has none), and
// LocationReporter's schedule under fake timers: an immediate fix, one per
// interval while visible, none while hidden, one on becoming visible, never
// two in flight, and a full stop on permission denial or stop().

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { FIX_TIMEOUT_MS, LocationReporter, REPORT_INTERVAL_MS, requestFix, type Fix } from "./location.js";

const PERMISSION_DENIED = 1;
const POSITION_UNAVAILABLE = 2;
const TIMEOUT = 3;

/** The zone the test environment's browser reports, which every fix carries. */
const ZONE = Intl.DateTimeFormat().resolvedOptions().timeZone;

type Outcome = { coords: { latitude: number; longitude: number; accuracy: number } } | { code: number };

/** A fake Geolocation whose calls are recorded and answered from `outcome`. */
class FakeGeolocation {
  outcome: Outcome = { coords: { latitude: 51.5, longitude: -0.25, accuracy: 35 } };
  /** When true, calls are held until `resolvePending()`. */
  hold = false;
  calls: PositionOptions[] = [];
  private held: (() => void)[] = [];

  getCurrentPosition(
    success: (p: GeolocationPosition) => void,
    error: (e: GeolocationPositionError) => void,
    options: PositionOptions,
  ): void {
    this.calls.push(options);
    const answer = (): void => {
      const o = this.outcome;
      if ("coords" in o) {
        success({ coords: o.coords, timestamp: 0 } as unknown as GeolocationPosition);
      } else {
        error({
          code: o.code,
          message: "fake",
          PERMISSION_DENIED,
          POSITION_UNAVAILABLE,
          TIMEOUT,
        } as GeolocationPositionError);
      }
    };
    if (this.hold) {
      this.held.push(answer);
    } else {
      answer();
    }
  }

  resolvePending(): void {
    const held = this.held;
    this.held = [];
    for (const answer of held) answer();
  }
}

let geo: FakeGeolocation;
let visibility: DocumentVisibilityState;

function installGeolocation(): void {
  geo = new FakeGeolocation();
  Object.defineProperty(navigator, "geolocation", { value: geo, configurable: true });
}

function setVisibility(state: DocumentVisibilityState): void {
  visibility = state;
  document.dispatchEvent(new Event("visibilitychange"));
}

/** Let the promise chain behind a reportOnce() settle. */
async function settle(): Promise<void> {
  for (let i = 0; i < 5; i++) await Promise.resolve();
}

beforeEach(() => {
  installGeolocation();
  visibility = "visible";
  Object.defineProperty(document, "visibilityState", {
    get: () => visibility,
    configurable: true,
  });
});

afterEach(() => {
  delete (navigator as unknown as { geolocation?: unknown }).geolocation;
  delete (document as unknown as { visibilityState?: unknown }).visibilityState;
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("requestFix", () => {
  it("returns the coordinates and accuracy, with the reporter's options", async () => {
    const result = await requestFix(1234);
    expect(result).toEqual({
      ok: true,
      fix: { latitude: 51.5, longitude: -0.25, accuracy_m: 35, time_zone: ZONE },
    });
    expect(geo.calls).toEqual([{ enableHighAccuracy: false, maximumAge: 1234, timeout: FIX_TIMEOUT_MS }]);
  });

  it.each([
    [PERMISSION_DENIED, "permission_denied"],
    [POSITION_UNAVAILABLE, "unavailable"],
    [TIMEOUT, "timeout"],
  ])("maps error code %i to %s", async (code, reason) => {
    geo.outcome = { code };
    const result = await requestFix(0);
    expect(result).toMatchObject({ ok: false, reason });
  });

  it("returns not_supported without the Geolocation API", async () => {
    delete (navigator as unknown as { geolocation?: unknown }).geolocation;
    expect(await requestFix(0)).toEqual({ ok: false, reason: "not_supported" });
  });
});

describe("LocationReporter", () => {
  function reporter(): { r: LocationReporter; sent: Fix[]; denied: { count: number } } {
    const sent: Fix[] = [];
    const denied = { count: 0 };
    const r = new LocationReporter(
      (fix) => sent.push(fix),
      () => {
        denied.count++;
      },
    );
    return { r, sent, denied };
  }

  it("sends once on start and again after each interval", async () => {
    vi.useFakeTimers();
    const { r, sent } = reporter();
    r.start();
    await settle();
    expect(sent).toHaveLength(1);
    expect(geo.calls[0]!.maximumAge).toBe(REPORT_INTERVAL_MS);

    await vi.advanceTimersByTimeAsync(REPORT_INTERVAL_MS);
    expect(sent).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(REPORT_INTERVAL_MS);
    expect(sent).toHaveLength(3);
    r.stop();
  });

  it("skips ticks while hidden and reports once on becoming visible", async () => {
    vi.useFakeTimers();
    const { r, sent } = reporter();
    r.start();
    await settle();
    expect(sent).toHaveLength(1);

    setVisibility("hidden");
    await vi.advanceTimersByTimeAsync(3 * REPORT_INTERVAL_MS);
    expect(sent).toHaveLength(1);

    setVisibility("visible");
    await settle();
    expect(sent).toHaveLength(2);
    r.stop();
  });

  it("never has two requests in flight", async () => {
    vi.useFakeTimers();
    geo.hold = true;
    const { r, sent } = reporter();
    r.start();
    setVisibility("visible");
    await vi.advanceTimersByTimeAsync(REPORT_INTERVAL_MS);
    expect(geo.calls).toHaveLength(1);

    geo.resolvePending();
    await settle();
    expect(sent).toHaveLength(1);
    r.stop();
  });

  it("does not send on unavailable or timeout and keeps running", async () => {
    vi.useFakeTimers();
    geo.outcome = { code: POSITION_UNAVAILABLE };
    const { r, sent, denied } = reporter();
    r.start();
    await settle();
    geo.outcome = { code: TIMEOUT };
    await vi.advanceTimersByTimeAsync(REPORT_INTERVAL_MS);
    expect(sent).toHaveLength(0);
    expect(denied.count).toBe(0);

    geo.outcome = { coords: { latitude: 1, longitude: 2, accuracy: 3 } };
    await vi.advanceTimersByTimeAsync(REPORT_INTERVAL_MS);
    expect(sent).toEqual([{ latitude: 1, longitude: 2, accuracy_m: 3, time_zone: ZONE }]);
    r.stop();
  });

  it("stops and calls onDenied once when permission is denied", async () => {
    vi.useFakeTimers();
    geo.outcome = { code: PERMISSION_DENIED };
    const { r, sent, denied } = reporter();
    r.start();
    await settle();
    expect(denied.count).toBe(1);
    expect(sent).toHaveLength(0);

    const callsAfterDenial = geo.calls.length;
    await vi.advanceTimersByTimeAsync(3 * REPORT_INTERVAL_MS);
    setVisibility("visible");
    await settle();
    expect(geo.calls).toHaveLength(callsAfterDenial);
    expect(denied.count).toBe(1);
  });

  it("stop() clears the interval and the visibility listener", async () => {
    vi.useFakeTimers();
    const removeSpy = vi.spyOn(document, "removeEventListener");
    const { r, sent } = reporter();
    r.start();
    await settle();
    r.stop();
    r.stop();
    expect(removeSpy.mock.calls.filter(([type]) => type === "visibilitychange")).toHaveLength(1);

    await vi.advanceTimersByTimeAsync(3 * REPORT_INTERVAL_MS);
    setVisibility("visible");
    await settle();
    expect(sent).toHaveLength(1);
    expect(geo.calls).toHaveLength(1);
  });

  it("drops a fix that resolves after stop()", async () => {
    geo.hold = true;
    const { r, sent } = reporter();
    r.start();
    r.stop();
    geo.resolvePending();
    await settle();
    expect(sent).toHaveLength(0);
  });
});
