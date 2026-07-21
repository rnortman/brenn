/**
 * Self-tests for test-setup.ts: verify that unexpected console.error
 * causes test failures, and that expectConsoleError() works correctly.
 */

import { describe, it, expect } from "vitest";
import { expectConsoleError, __finalizeForTest } from "./test-setup.js";

describe("test-setup console.error guard", () => {
    it("string matcher: expectConsoleError consumes a matching error", () => {
        console.error("expected boom", { code: 42 });
        expectConsoleError("expected boom");
        // afterEach verifies the matcher was satisfied — no residual error.
    });

    it("regex matcher: expectConsoleError consumes a matching error", () => {
        console.error("expected boom", { code: 42 });
        expectConsoleError(/boom/);
    });

    it("predicate matcher: expectConsoleError consumes a matching error", () => {
        console.error("expected boom", { code: 42 });
        expectConsoleError((args) => args[0] === "expected boom");
    });

    it("baseline: no console.error and no expectConsoleError — passes", () => {
        // Clean test with no errors and no matchers.
        expect(1 + 1).toBe(2);
    });

    it("failure path (negative POC): unmatched console.error causes finalization to throw", () => {
        // Emit an error WITHOUT registering a matcher, then call
        // __finalizeForTest() directly. It should throw, and because it
        // clears state before throwing, afterEach sees nothing to report.
        console.error("an error nobody expected");
        expect(() => __finalizeForTest()).toThrow(
            "Unexpected console.error during test",
        );
        // __finalizeForTest cleared state; afterEach is now a no-op.
    });

    it("failure path (negative POC): expectConsoleError without matching error causes finalization to throw", () => {
        // Register a matcher but emit NO console.error.
        // __finalizeForTest() should throw for the unmatched matcher.
        expectConsoleError("foo");
        // No console.error("foo") fires.
        expect(() => __finalizeForTest()).toThrow(
            "Expected console.error matching",
        );
        // __finalizeForTest cleared state; afterEach is now a no-op.
    });
});
