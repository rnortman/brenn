/**
 * Device location: one-shot fixes from the browser's Geolocation API and the
 * reporter that sends them while this device shares location.
 *
 * Caller responsibilities:
 *  - Call `requestFix` from a user gesture the first time, so the browser's
 *    permission prompt is allowed to appear.
 *  - Run a `LocationReporter` only while the server says this (device, user)
 *    pair shares location and the WebSocket is connected.
 *
 * Nothing is cached here beyond the browser's own `maximumAge` reuse; no
 * history is kept. `watchPosition` is never used.
 */

/**
 * One fix as sent to the server, with the browser's IANA zone at the time of
 * the fix. The browser's own timestamp is not sent.
 */
export type Fix = { latitude: number; longitude: number; accuracy_m: number; time_zone: string };

/** Outcome of one fix request. */
export type FixResult =
  | { ok: true; fix: Fix }
  | {
      ok: false;
      reason: "permission_denied" | "not_supported" | "unavailable" | "timeout";
      detail?: string;
    };

/**
 * Interval between fixes while the document is visible, and the `maximumAge`
 * the reporter accepts. The backend's `STALE_AFTER_SECONDS` (900) in
 * `brenn-device-location` is set from this value.
 */
export const REPORT_INTERVAL_MS = 5 * 60_000;

/** How long one fix request may take before the browser gives up. */
export const FIX_TIMEOUT_MS = 30_000;

/**
 * One fix from the browser. A cached fix up to `maximumAgeMs` old is
 * accepted. Never throws.
 */
export function requestFix(maximumAgeMs: number): Promise<FixResult> {
  // Absent in some browsers and null in others (and in happy-dom).
  const geolocation = (navigator as { geolocation?: Geolocation | null }).geolocation;
  if (!geolocation) {
    return Promise.resolve({ ok: false, reason: "not_supported" });
  }
  return new Promise<FixResult>((resolve) => {
    geolocation.getCurrentPosition(
      (position) => {
        resolve({
          ok: true,
          fix: {
            latitude: position.coords.latitude,
            longitude: position.coords.longitude,
            accuracy_m: position.coords.accuracy,
            time_zone: Intl.DateTimeFormat().resolvedOptions().timeZone,
          },
        });
      },
      (error) => {
        switch (error.code) {
          case error.PERMISSION_DENIED:
            resolve({ ok: false, reason: "permission_denied", detail: error.message });
            break;
          case error.TIMEOUT:
            resolve({ ok: false, reason: "timeout", detail: error.message });
            break;
          default:
            resolve({ ok: false, reason: "unavailable", detail: error.message });
        }
      },
      { enableHighAccuracy: false, maximumAge: maximumAgeMs, timeout: FIX_TIMEOUT_MS },
    );
  });
}

/**
 * Sends a fix now and then every `REPORT_INTERVAL_MS` while the document is
 * visible, plus one whenever it becomes visible. Stops itself and calls
 * `onDenied` when the browser's permission has been withdrawn; an unavailable
 * or timed-out fix is skipped and the next tick tries again.
 */
export class LocationReporter {
  private interval: ReturnType<typeof setInterval> | null = null;
  private visibilityHandler: (() => void) | null = null;
  private pending = false;

  constructor(
    private readonly send: (fix: Fix) => void,
    private readonly onDenied: () => void,
  ) {}

  /** Send one fix now, then every REPORT_INTERVAL_MS while the document is visible. */
  start(): void {
    if (this.interval !== null) return;
    const whenVisible = (): void => {
      if (document.visibilityState === "visible") {
        void this.reportOnce();
      }
    };
    this.visibilityHandler = whenVisible;
    document.addEventListener("visibilitychange", whenVisible);
    this.interval = setInterval(whenVisible, REPORT_INTERVAL_MS);
    void this.reportOnce();
  }

  /** Stop the timer and remove the visibility listener. Idempotent. */
  stop(): void {
    if (this.interval !== null) {
      clearInterval(this.interval);
      this.interval = null;
    }
    if (this.visibilityHandler !== null) {
      document.removeEventListener("visibilitychange", this.visibilityHandler);
      this.visibilityHandler = null;
    }
  }

  /** One fix now. Coalesces: a request in flight is not duplicated. */
  async reportOnce(): Promise<void> {
    if (this.pending) return;
    this.pending = true;
    try {
      const result = await requestFix(REPORT_INTERVAL_MS);
      if (result.ok) {
        // A fix that resolves after stop() is dropped: the socket it would
        // go to may be closed, and sharing may have been turned off.
        if (this.interval !== null) {
          this.send(result.fix);
        }
      } else if (result.reason === "permission_denied") {
        const wasRunning = this.interval !== null;
        this.stop();
        if (wasRunning) {
          this.onDenied();
        }
      }
    } finally {
      this.pending = false;
    }
  }
}
