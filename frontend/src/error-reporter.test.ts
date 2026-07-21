/**
 * Unit tests for error-reporter.ts.
 *
 * Tests cover: describeReason type dispatch, reportClientError console
 * logging, WS delivery, truncation, and exception suppression.
 */

import { describe, it, expect, vi, afterEach } from "vitest";
import {
    describeReason,
    reportClientError,
    setReporterTarget,
    _clearReporterTargetForTest,
    MAX_MESSAGE_LEN,
} from "./error-reporter.js";
import { expectConsoleError } from "./test-setup.js";

// ---------------------------------------------------------------------------
// Helper to reset the module-level activeWs to null between tests.
// ---------------------------------------------------------------------------
function clearTarget(): void {
    _clearReporterTargetForTest();
}

// ---------------------------------------------------------------------------
// describeReason
// ---------------------------------------------------------------------------

describe("describeReason — Error", () => {
    it("formats Error with name, message, and stack", () => {
        const err = new Error("x");
        const result = describeReason(err);
        expect(result).toMatch(/^Error: x\n/);
        // trimEnd applied — no trailing whitespace
        expect(result).not.toMatch(/\s$/);
    });
});

describe("describeReason — string", () => {
    it("returns the string unchanged", () => {
        expect(describeReason("hi")).toBe("hi");
    });
});

describe("describeReason — primitives", () => {
    it("null → 'null'", () => {
        expect(describeReason(null)).toBe("null");
    });

    it("undefined → 'undefined'", () => {
        expect(describeReason(undefined)).toBe("undefined");
    });

    it("BigInt(1) → '1'", () => {
        expect(describeReason(BigInt(1))).toBe("1");
    });

    it("Symbol('s') → 'Symbol(s)'", () => {
        expect(describeReason(Symbol("s"))).toBe("Symbol(s)");
    });
});

describe("describeReason — plain object", () => {
    it("emits shape only: constructor name present", () => {
        const result = describeReason({ a: 1 });
        expect(result).toContain("Object");
    });

    it("emits shape only: key name present, no value", () => {
        const result = describeReason({ token: "secret123", endpoint: "https://example.com/push" });
        expect(result).toContain("token");
        expect(result).toContain("endpoint");
        expect(result).not.toContain("secret123");
        expect(result).not.toContain("https://example.com/push");
    });

    it("emits all key names for multi-key object", () => {
        const result = describeReason({ message: "foo", filename: "bar.ts", lineno: 1, colno: 2 });
        expect(result).toContain("message");
        expect(result).toContain("filename");
        expect(result).toContain("lineno");
        expect(result).toContain("colno");
    });

    it("emits empty braces for empty object", () => {
        const result = describeReason({});
        expect(result).toContain("Object");
        expect(result).toContain("{}");
    });

    it("emits exact shape format: 'Object { a }'", () => {
        expect(describeReason({ a: 1 })).toBe("Object { a }");
    });

    it("emits custom constructor name", () => {
        class Foo { x = 1; }
        const result = describeReason(new Foo());
        expect(result).toContain("Foo");
        expect(result).toContain("x");
    });

    it("null-prototype object — constructor fallback is [unknown]", () => {
        const obj = Object.create(null) as Record<string, unknown>;
        obj["k"] = "v";
        const result = describeReason(obj);
        expect(result).toMatch(/\[unknown\]/);
        expect(result).toContain("k");
        expect(result).not.toContain("v");
    });

    it("exotic: Proxy with throwing get — returns a string, does not throw", () => {
        const p = new Proxy({}, { get() { throw new Error("no get"); } });
        const result = describeReason(p);
        expect(typeof result).toBe("string");
        expect(result.length).toBeGreaterThan(0);
        // Constructor access fires the get trap → name = "[unknown]"; backing {} has no keys.
        expect(result).toMatch(/\[unknown\]/);
        expect(result).toContain("{}");
    });

    it("exotic: Proxy with throwing ownKeys — returns a string, does not throw, no key names", () => {
        // Use a target with real keys so non-leakage is non-vacuous: if Object.keys were
        // called on the target directly, "token" would appear in output.
        const p = new Proxy({ token: "secret" }, { ownKeys() { throw new Error("no keys"); } });
        const result = describeReason(p);
        expect(typeof result).toBe("string");
        expect(result.length).toBeGreaterThan(0);
        // Constructor name present (or [unknown] fallback), no key names leaked.
        expect(result).toMatch(/Object|\[unknown\]/);
        // Enumeration failed — neither key name nor value should appear.
        expect(result).not.toContain("token");
        expect(result).not.toContain("secret");
    });
});

describe("describeReason — pathological objects", () => {
    it("object with a throwing getter: Object.keys succeeds, key name present", () => {
        const obj = {
            get x(): string {
                throw new Error("nope");
            },
        };
        const result = describeReason(obj);
        expect(typeof result).toBe("string");
        expect(result.length).toBeGreaterThan(0);
        // Object.keys does not invoke getters — key name 'x' is present in shape output.
        expect(result).toContain("x");
    });

    it("cyclic object: Object.keys succeeds, key name present", () => {
        const obj: Record<string, unknown> = {};
        obj["self"] = obj;
        const result = describeReason(obj);
        expect(typeof result).toBe("string");
        expect(result).toContain("self");
    });
});

// ---------------------------------------------------------------------------
// reportClientError — no target registered
// ---------------------------------------------------------------------------

describe("reportClientError — no target", () => {
    it("logs to console.error and does not throw", () => {
        clearTarget();
        expectConsoleError("no-target-msg");
        expect(() => reportClientError("no-target-msg")).not.toThrow();
    });

    it("does not invoke sendClientError when target is null", () => {
        clearTarget();
        const spy = vi.fn();
        // Register a target so we can observe calls, then immediately clear it.
        setReporterTarget({ sendClientError: spy } as unknown as Parameters<typeof setReporterTarget>[0]);
        clearTarget();

        expectConsoleError("null-target");
        reportClientError("null-target");

        expect(spy).not.toHaveBeenCalled();
    });
});

// ---------------------------------------------------------------------------
// reportClientError — with target
// ---------------------------------------------------------------------------

describe("reportClientError — with registered target", () => {
    afterEach(() => {
        clearTarget();
    });

    it("fires console.error AND sendClientError exactly once", () => {
        const recorded: string[] = [];
        const fakeWs = {
            sendClientError: (m: string) => {
                recorded.push(m);
            },
        };
        setReporterTarget(fakeWs as unknown as Parameters<typeof setReporterTarget>[0]);

        expectConsoleError("msg");
        reportClientError("msg");

        expect(recorded).toHaveLength(1);
        expect(recorded[0]).toBe("msg");
    });

    it("suppresses exceptions thrown by sendClientError", () => {
        const fakeWs = {
            sendClientError: () => {
                throw new DOMException("Connection closed", "InvalidStateError");
            },
        };
        setReporterTarget(fakeWs as unknown as Parameters<typeof setReporterTarget>[0]);

        expectConsoleError("err-msg");
        expect(() => reportClientError("err-msg")).not.toThrow();
    });
});

// ---------------------------------------------------------------------------
// Truncation
// ---------------------------------------------------------------------------

describe("reportClientError — truncation", () => {
    afterEach(() => {
        clearTarget();
    });

    it("truncates message longer than MAX_MESSAGE_LEN code units", () => {
        const TRUNCATION_SUFFIX = "…[truncated]";
        const long = "a".repeat(20_000);
        let capturedSendArg = "";

        const fakeWs = {
            sendClientError: (m: string) => {
                capturedSendArg = m;
            },
        };
        setReporterTarget(fakeWs as unknown as Parameters<typeof setReporterTarget>[0]);

        expectConsoleError((args: unknown[]) => {
            const s = args[0];
            return (
                typeof s === "string" &&
                s.length <= MAX_MESSAGE_LEN &&
                s.endsWith(TRUNCATION_SUFFIX)
            );
        });

        reportClientError(long);

        // sendClientError is called synchronously during reportClientError, so
        // we can assert on capturedSendArg immediately.
        expect(capturedSendArg.length).toBeLessThanOrEqual(MAX_MESSAGE_LEN);
        expect(capturedSendArg.endsWith(TRUNCATION_SUFFIX)).toBe(true);
        // Verify the retained prefix is the head of the original string.
        const prefixLen = capturedSendArg.length - TRUNCATION_SUFFIX.length;
        expect(capturedSendArg.startsWith("a".repeat(prefixLen))).toBe(true);
    });

    it("preserves surrogate pair at truncation boundary — does not split a pair", () => {
        // Build a string where the last char before the cut point would be a
        // high surrogate if we used a plain emoji (4 bytes = 2 code units).
        // Place a supplementary-plane char (U+1F600 = 2 code units: 0xD83D 0xDE00)
        // so that its high surrogate sits exactly at the cut boundary.
        const TRUNCATION_SUFFIX = "…[truncated]";
        const cutAt = MAX_MESSAGE_LEN - TRUNCATION_SUFFIX.length;
        // Fill up to cutAt-1 with ASCII, then place the emoji pair at cutAt-1..cutAt+1.
        const prefix = "a".repeat(cutAt - 1);
        const emoji = "\u{1F600}"; // 2 code units: high surrogate at cutAt-1, low at cutAt
        const long = prefix + emoji + "b".repeat(20_000);

        let capturedSendArg = "";
        const fakeWs = { sendClientError: (m: string) => { capturedSendArg = m; } };
        setReporterTarget(fakeWs as unknown as Parameters<typeof setReporterTarget>[0]);

        expectConsoleError((args: unknown[]) => typeof args[0] === "string");
        reportClientError(long);

        // Result must be well-formed UTF-16: no lone surrogates.
        // encodeURIComponent would throw on a lone surrogate; alternatively
        // check that the last char before the suffix is not a high surrogate.
        const prefixPart = capturedSendArg.slice(0, capturedSendArg.length - TRUNCATION_SUFFIX.length);
        if (prefixPart.length > 0) {
            const lastCode = prefixPart.charCodeAt(prefixPart.length - 1);
            const isHighSurrogate = lastCode >= 0xd800 && lastCode <= 0xdbff;
            expect(isHighSurrogate).toBe(false);
        }
        expect(capturedSendArg.endsWith(TRUNCATION_SUFFIX)).toBe(true);
    });
});
