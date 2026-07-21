/**
 * Unit tests for global-error-handlers.ts.
 *
 * The module is a side-effect module; importing it registers the two window
 * listeners. We mock reportClientError at the module boundary via vi.mock
 * so we don't touch console or WS state.
 *
 * happy-dom ships ErrorEvent but not PromiseRejectionEvent. For the
 * unhandledrejection test we synthesise a plain Event and graft the
 * required properties onto it.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock error-reporter before importing the side-effect module so the listeners
// pick up the mock on registration.
vi.mock("./error-reporter.js", () => ({
    reportClientError: vi.fn(),
    describeReason: vi.fn((value: unknown) => {
        // Minimal real implementation for the handler tests.
        // Matches the shape-only policy: Error instances get name+message+stack;
        // strings and primitives pass through; objects get shape-only (constructor+keys,
        // no JSON.stringify, no values).
        if (value instanceof Error) {
            return `${value.name}: ${value.message}\n${value.stack ?? ""}`.trimEnd();
        }
        if (typeof value === "string") return value;
        if (
            value === null ||
            value === undefined ||
            typeof value === "number" ||
            typeof value === "boolean" ||
            typeof value === "bigint" ||
            typeof value === "symbol"
        ) {
            return String(value);
        }
        // Shape-only fallback (no JSON.stringify, no values).
        const name = (value as { constructor?: { name?: string } }).constructor?.name ?? "[unknown]";
        const keys = Object.keys(value as object);
        const body = keys.length > 0 ? `{ ${keys.join(", ")} }` : "{}";
        return `${name} ${body}`;
    }),
}));

// Import the side-effect module. The listeners are registered on window.
import "./global-error-handlers.js";

// Import the mock so we can assert on it.
import { reportClientError } from "./error-reporter.js";

const mockReport = reportClientError as ReturnType<typeof vi.fn>;

beforeEach(() => {
    mockReport.mockClear();
});

// ---------------------------------------------------------------------------
// unhandledrejection
// ---------------------------------------------------------------------------

describe("unhandledrejection listener", () => {
    it("calls reportClientError with 'Unhandled promise rejection: ' prefix", () => {
        // happy-dom doesn't export PromiseRejectionEvent; synthesise it.
        const ev = new Event("unhandledrejection") as Event & {
            reason: unknown;
            promise: Promise<void>;
        };
        ev.reason = new Error("boom");
        ev.promise = Promise.resolve();
        window.dispatchEvent(ev);

        expect(mockReport).toHaveBeenCalledTimes(1);
        const arg = mockReport.mock.calls[0][0] as string;
        expect(arg).toMatch(/^Unhandled promise rejection: Error: boom\n/);
    });

    it("handles ev.reason === undefined (bare Promise.reject())", () => {
        const ev = new Event("unhandledrejection") as Event & {
            reason: unknown;
            promise: Promise<void>;
        };
        ev.reason = undefined;
        ev.promise = Promise.resolve();
        window.dispatchEvent(ev);

        expect(mockReport).toHaveBeenCalledTimes(1);
        expect(mockReport.mock.calls[0][0]).toBe("Unhandled promise rejection: undefined");
    });

    it("does not throw even when reportClientError itself throws", () => {
        mockReport.mockImplementationOnce(() => { throw new Error("reporter failed"); });
        const ev = new Event("unhandledrejection") as Event & {
            reason: unknown;
            promise: Promise<void>;
        };
        ev.reason = new Error("original");
        ev.promise = Promise.resolve();
        expect(() => window.dispatchEvent(ev)).not.toThrow();
    });
});

// ---------------------------------------------------------------------------
// error listener
// ---------------------------------------------------------------------------

describe("error listener — with Error object", () => {
    it("calls reportClientError with 'Uncaught error: ' prefix", () => {
        const ev = new ErrorEvent("error", {
            error: new Error("oops"),
            message: "oops",
            filename: "f",
            lineno: 1,
            colno: 1,
        });
        window.dispatchEvent(ev);

        expect(mockReport).toHaveBeenCalledTimes(1);
        const arg = mockReport.mock.calls[0][0] as string;
        expect(arg).toMatch(/^Uncaught error: Error: oops\n/);
    });
});

describe("error listener — ev.error is null", () => {
    it("formats fallback as 'message at filename:lineno:colno' string", () => {
        const ev = new ErrorEvent("error", {
            error: null,
            message: "script error",
            filename: "app.js",
            lineno: 42,
            colno: 7,
        });
        window.dispatchEvent(ev);

        expect(mockReport).toHaveBeenCalledTimes(1);
        const arg = mockReport.mock.calls[0][0] as string;
        expect(arg).toMatch(/^Uncaught error: /);
        // Change 4: fallback is a formatted string, not an object — hits the
        // string branch in describeReason, preserving full diagnostic content.
        expect(arg).toContain("script error");
        expect(arg).toContain("app.js");
        expect(arg).toContain("42");
        expect(arg).toContain("7");
        expect(arg).toContain(" at ");
    });

    it("ev.error is undefined — same formatted string fallback", () => {
        const ev = new ErrorEvent("error", {
            error: undefined,
            message: "another error",
            filename: "main.js",
            lineno: 10,
            colno: 3,
        });
        window.dispatchEvent(ev);

        expect(mockReport).toHaveBeenCalledTimes(1);
        const arg = mockReport.mock.calls[0][0] as string;
        expect(arg).toContain("another error");
        expect(arg).toContain("main.js");
        expect(arg).toContain("10");
        expect(arg).toContain("3");
    });
});

describe("error listener — exception suppression", () => {
    it("does not throw even when reportClientError itself throws", () => {
        mockReport.mockImplementationOnce(() => { throw new Error("reporter failed"); });
        const ev = new ErrorEvent("error", {
            error: new Error("original"),
            message: "original",
            filename: "f",
            lineno: 1,
            colno: 1,
        });
        expect(() => window.dispatchEvent(ev)).not.toThrow();
    });
});
