// @vitest-environment happy-dom
/**
 * Unit tests for nav-on-message.ts — the global NavigateTo handler
 * for non-app-shell Brenn pages.
 *
 * Tests use the exported `initNavOnMessage(doc, nav, win)` to inject fake
 * globals, allowing both the handler logic and the module-level gate to be
 * exercised in isolation.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";
import { initNavOnMessage } from "./nav-on-message.js";
import type { NavDocument, NavNavigator, NavWindow } from "./nav-on-message.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const ORIGIN = "https://brenn.example";

type NavAddEventListener = (type: "message", handler: (event: MessageEvent) => void) => void;

interface FakeServiceWorker {
    addEventListener: NavAddEventListener;
    _fire: (data: unknown) => void;
}

/** Build a fake navigator with a controllable SW message bus. */
function fakeNavigator(): {
    nav: NavNavigator & { serviceWorker: FakeServiceWorker };
    addEventListenerSpy: ReturnType<typeof vi.fn<NavAddEventListener>>;
} {
    let captured: ((event: MessageEvent) => void) | null = null;
    const addEventListenerSpy = vi.fn<NavAddEventListener>(
        (_type: string, handler: (event: MessageEvent) => void) => {
            captured = handler;
        },
    );
    const sw: FakeServiceWorker = {
        addEventListener: addEventListenerSpy,
        _fire(data: unknown) {
            if (captured) captured({ data } as MessageEvent);
        },
    };
    return {
        nav: { serviceWorker: sw },
        addEventListenerSpy,
    };
}

/** Build a fake document with optional brenn-app presence. */
function fakeDoc(hasBrennApp: boolean): NavDocument {
    return {
        querySelector: vi.fn(() => (hasBrennApp ? {} as Element : null)),
    };
}

type AssignFn = (url: string) => void;

/** Build a fake window with a spy on location.assign. */
function fakeWin(): {
    win: NavWindow;
    assignSpy: ReturnType<typeof vi.fn<AssignFn>>;
} {
    const assignSpy = vi.fn<AssignFn>();
    return {
        win: { location: { origin: ORIGIN, assign: assignSpy } },
        assignSpy,
    };
}

// ---------------------------------------------------------------------------
// Gate tests
// ---------------------------------------------------------------------------

describe("nav-on-message gate", () => {
    beforeEach(() => {
        vi.restoreAllMocks();
    });

    it("<brenn-app> absent + SW present → addEventListener called", () => {
        const doc = fakeDoc(false);
        const { nav, addEventListenerSpy } = fakeNavigator();
        const { win } = fakeWin();
        initNavOnMessage(doc, nav, win);
        expect(addEventListenerSpy).toHaveBeenCalledOnce();
    });

    it("<brenn-app> present → addEventListener NOT called", () => {
        const doc = fakeDoc(true);
        const { nav, addEventListenerSpy } = fakeNavigator();
        const { win } = fakeWin();
        initNavOnMessage(doc, nav, win);
        expect(addEventListenerSpy).not.toHaveBeenCalled();
    });

    it("serviceWorker absent → no error, addEventListener NOT called", () => {
        const doc = fakeDoc(false);
        const navNoSW = {} as { serviceWorker?: undefined };
        const { win } = fakeWin();
        // Should not throw.
        expect(() => initNavOnMessage(doc, navNoSW, win)).not.toThrow();
    });
});

// ---------------------------------------------------------------------------
// Handler logic tests
// ---------------------------------------------------------------------------

describe("nav-on-message handler logic", () => {
    beforeEach(() => {
        vi.restoreAllMocks();
    });

    it("same-origin /app/ URL → window.location.assign called once", () => {
        const doc = fakeDoc(false);
        const { nav } = fakeNavigator();
        const { win, assignSpy } = fakeWin();
        initNavOnMessage(doc, nav, win);
        nav.serviceWorker._fire({ type: "NavigateTo", url: "/app/graf/c/42" });
        expect(assignSpy).toHaveBeenCalledTimes(1);
        expect(assignSpy).toHaveBeenCalledWith(`${ORIGIN}/app/graf/c/42`);
    });

    it("same-origin absolute /app/ URL → assign called with parsed href", () => {
        const doc = fakeDoc(false);
        const { nav } = fakeNavigator();
        const { win, assignSpy } = fakeWin();
        initNavOnMessage(doc, nav, win);
        nav.serviceWorker._fire({
            type: "NavigateTo",
            url: `${ORIGIN}/app/graf/c/5`,
        });
        expect(assignSpy).toHaveBeenCalledTimes(1);
        expect(assignSpy).toHaveBeenCalledWith(`${ORIGIN}/app/graf/c/5`);
    });

    it("cross-origin URL → no assign, console.warn", () => {
        const doc = fakeDoc(false);
        const { nav } = fakeNavigator();
        const { win, assignSpy } = fakeWin();
        const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
        initNavOnMessage(doc, nav, win);
        nav.serviceWorker._fire({
            type: "NavigateTo",
            url: "https://evil.example/steal",
        });
        expect(assignSpy).not.toHaveBeenCalled();
        expect(warnSpy).toHaveBeenCalledOnce();
    });

    it("non-string url → no assign, console.warn", () => {
        const doc = fakeDoc(false);
        const { nav } = fakeNavigator();
        const { win, assignSpy } = fakeWin();
        const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
        initNavOnMessage(doc, nav, win);
        nav.serviceWorker._fire({ type: "NavigateTo", url: 42 });
        expect(assignSpy).not.toHaveBeenCalled();
        expect(warnSpy).toHaveBeenCalledOnce();
    });

    it("malformed url string (throws on parse) → no assign, console.warn", () => {
        const doc = fakeDoc(false);
        const { nav } = fakeNavigator();
        const { win, assignSpy } = fakeWin();
        const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
        initNavOnMessage(doc, nav, win);
        // "http://:80" throws in new URL() even with a base.
        nav.serviceWorker._fire({ type: "NavigateTo", url: "http://:80" });
        expect(assignSpy).not.toHaveBeenCalled();
        expect(warnSpy).toHaveBeenCalledOnce();
    });

    it("missing url field → no assign, console.warn (non-string)", () => {
        const doc = fakeDoc(false);
        const { nav } = fakeNavigator();
        const { win, assignSpy } = fakeWin();
        const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
        initNavOnMessage(doc, nav, win);
        nav.serviceWorker._fire({ type: "NavigateTo" });
        expect(assignSpy).not.toHaveBeenCalled();
        expect(warnSpy).toHaveBeenCalledOnce();
    });

    it("non-NavigateTo message type → ignored entirely, no assign, no warn", () => {
        const doc = fakeDoc(false);
        const { nav } = fakeNavigator();
        const { win, assignSpy } = fakeWin();
        initNavOnMessage(doc, nav, win);
        nav.serviceWorker._fire({ type: "SomethingElse", url: "/app/graf/c/1" });
        expect(assignSpy).not.toHaveBeenCalled();
    });

    it("null data → ignored, no assign, no warn", () => {
        const doc = fakeDoc(false);
        const { nav } = fakeNavigator();
        const { win, assignSpy } = fakeWin();
        initNavOnMessage(doc, nav, win);
        nav.serviceWorker._fire(null);
        expect(assignSpy).not.toHaveBeenCalled();
    });

    it("non-object data → ignored, no assign, no warn", () => {
        const doc = fakeDoc(false);
        const { nav } = fakeNavigator();
        const { win, assignSpy } = fakeWin();
        initNavOnMessage(doc, nav, win);
        nav.serviceWorker._fire("NavigateTo");
        expect(assignSpy).not.toHaveBeenCalled();
    });

    it("same-origin non-/app/ path → no assign, console.warn (security-1)", () => {
        const doc = fakeDoc(false);
        const { nav } = fakeNavigator();
        const { win, assignSpy } = fakeWin();
        const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
        initNavOnMessage(doc, nav, win);
        nav.serviceWorker._fire({ type: "NavigateTo", url: "/auth/logout" });
        expect(assignSpy).not.toHaveBeenCalled();
        expect(warnSpy).toHaveBeenCalledOnce();
    });
});
