/**
 * Vitest global setup: fail tests on unexpected console.error.
 *
 * Import this via vitest.config.ts `test.setupFiles`. It installs a
 * beforeEach/afterEach pair that intercepts console.error. Any call that
 * is not "consumed" by expectConsoleError() causes the test to fail.
 */

import { beforeEach, afterEach, expect } from "vitest";

// ---------------------------------------------------------------------------
// Per-test state
// ---------------------------------------------------------------------------

type ErrorRecord = { args: unknown[] };
type Matcher = string | RegExp | ((args: unknown[]) => boolean);

/** console.error calls captured during the current test. */
let recordedErrors: ErrorRecord[] = [];

/** Matchers registered by expectConsoleError() during the current test. */
let pendingMatchers: Matcher[] = [];

/** The real console.error, saved before the override. */
const realConsoleError: (...args: unknown[]) => void = console.error.bind(console);

// ---------------------------------------------------------------------------
// Matcher helpers
// ---------------------------------------------------------------------------

function matcherMatches(matcher: Matcher, record: ErrorRecord): boolean {
    if (typeof matcher === "string") {
        return record.args.some(
            (a) => typeof a === "string" && a.includes(matcher),
        );
    }
    if (matcher instanceof RegExp) {
        return record.args.some(
            (a) => typeof a === "string" && matcher.test(a),
        );
    }
    return matcher(record.args);
}

function formatArgs(args: unknown[]): string {
    return args
        .map((a) => {
            if (typeof a === "string") return a;
            try {
                return JSON.stringify(a);
            } catch {
                return String(a);
            }
        })
        .join(" ");
}

function formatMatcher(matcher: Matcher): string {
    if (typeof matcher === "string") return JSON.stringify(matcher);
    if (matcher instanceof RegExp) return matcher.toString();
    return "(predicate function)";
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Declare that this test expects one console.error call matching `matcher`.
 *
 * - string: matches if any arg contains the string as a substring.
 * - RegExp: matches if any arg is a string matching the regex.
 * - function: receives the full args array and returns true for a match.
 *
 * Multiple calls are allowed (one per expected error). Order does not
 * matter — each matcher is consumed against the recorded calls at
 * afterEach time (not eagerly).
 */
export function expectConsoleError(matcher: Matcher): void {
    pendingMatchers.push(matcher);
}

/**
 * Run the finalization logic that afterEach normally runs.
 *
 * Exported for use in self-tests that need to assert the failure paths
 * without actually triggering a real afterEach failure. Always clears
 * the module-level state so afterEach does not double-report.
 */
export function __finalizeForTest(): void {
    // Snapshot and clear state eagerly so afterEach sees nothing even
    // when this function throws.
    const errors = recordedErrors;
    const matchers = pendingMatchers;
    recordedErrors = [];
    pendingMatchers = [];

    const remaining = [...errors];

    // For each pending matcher, find and consume the first matching record.
    for (const matcher of matchers) {
        const idx = remaining.findIndex((r) => matcherMatches(matcher, r));
        if (idx === -1) {
            // No matching error was recorded for this matcher.
            throw new Error(
                `Expected console.error matching ${formatMatcher(matcher)} but none occurred`,
            );
        }
        remaining.splice(idx, 1);
    }

    if (remaining.length > 0) {
        const details = remaining
            .map((r, i) => `  [${i}] ${formatArgs(r.args)}`)
            .join("\n");
        throw new Error(
            `Unexpected console.error during test (${remaining.length} call(s)):\n${details}`,
        );
    }
}

// ---------------------------------------------------------------------------
// Vitest hooks
// ---------------------------------------------------------------------------

beforeEach(() => {
    recordedErrors = [];
    pendingMatchers = [];

    console.error = (...args: unknown[]): void => {
        recordedErrors.push({ args });
        // Forward to the real console.error so the output still appears
        // in the test reporter (useful for debugging failures).
        realConsoleError(...args);
    };
});

afterEach(() => {
    // Restore first so any error thrown below doesn't suppress subsequent
    // tests' beforeEach override.
    console.error = realConsoleError;

    const errors = recordedErrors;
    const matchers = pendingMatchers;
    recordedErrors = [];
    pendingMatchers = [];

    const remaining = [...errors];

    for (const matcher of matchers) {
        const idx = remaining.findIndex((r) => matcherMatches(matcher, r));
        if (idx === -1) {
            expect.fail(
                `Expected console.error matching ${formatMatcher(matcher)} but none occurred`,
            );
            return;
        }
        remaining.splice(idx, 1);
    }

    if (remaining.length > 0) {
        const details = remaining
            .map((r, i) => `  [${i}] ${formatArgs(r.args)}`)
            .join("\n");
        expect.fail(
            `Unexpected console.error during test (${remaining.length} call(s)):\n${details}`,
        );
    }
});
