import { describe, it, expect } from "vitest";
import { parseSameOriginUrl } from "./url-util.js";

/**
 * Unit tests for parseSameOriginUrl.
 *
 * This function is the sole XSS/cross-origin guard for navigation URLs
 * received over the WebSocket. Testing it directly pins the full contract
 * (not just the callers' current inputs), so edge-case regressions are caught
 * here before they can reach nav-on-message.ts or app.ts.
 */

const ORIGIN = "https://example.com";

describe("parseSameOriginUrl", () => {
    it("accepts a same-origin absolute URL", () => {
        const result = parseSameOriginUrl("https://example.com/path", ORIGIN);
        expect(result).not.toBeNull();
        expect(result!.pathname).toBe("/path");
    });

    it("accepts a same-origin relative URL (resolved against origin)", () => {
        const result = parseSameOriginUrl("/relative/path", ORIGIN);
        expect(result).not.toBeNull();
        expect(result!.pathname).toBe("/relative/path");
    });

    it("rejects a cross-origin URL", () => {
        const result = parseSameOriginUrl("https://evil.com/steal", ORIGIN);
        expect(result).toBeNull();
    });

    it("rejects a javascript: URL", () => {
        // javascript: URIs have origin "null" which never equals the app origin.
        const result = parseSameOriginUrl("javascript:alert(1)", ORIGIN);
        expect(result).toBeNull();
    });

    it("rejects a malformed URL (invalid host bracket syntax)", () => {
        // new URL("https://[invalid]", base) throws — parseSameOriginUrl must
        // catch the exception and return null, not propagate.
        const result = parseSameOriginUrl("https://[invalid]", ORIGIN);
        expect(result).toBeNull();
    });
});
