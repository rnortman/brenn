// Pure utility functions shared between sw.ts (WebWorker context) and tests.
// No WebWorker or DOM globals — safe to import from either environment.

// isFenixUserAgent: returns true iff the given UA string identifies
// Firefox-Android (the Fenix family). Substring check is intentional —
// no complex parsing; we only need to gate on the well-known Fenix UA.
export function isFenixUserAgent(ua: string): boolean {
    return ua.includes("Firefox") && ua.includes("Android");
}
