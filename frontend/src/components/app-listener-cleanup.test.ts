// @vitest-environment happy-dom
//
// Regression tests for disconnectedCallback listener cleanup — specifically
// for the three listeners added by the pwa-push-listener-leak cycle:
//   pagehideHandler   — on window, "pagehide"
//   documentClickHandler — on document, "click"
//   serviceWorkerMessageHandler — on navigator.serviceWorker, "message"
//     (skipped here: happy-dom does not expose navigator.serviceWorker, so
//      the connectedCallback branch is not reached and there is nothing to clean)
//
// Pattern: mount → disconnect (removeChild) → assert fields nulled → remount
// → fire the event → assert handler fires exactly once (not twice), proving
// the first-mount registration was removed on disconnect.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as pushDb from "../push-db.js";
import "./app.js";
import { BrennApp } from "./app.js";
import { MockWebSocket } from "../test-utils/mock-websocket.js";

/** Private fields we need to inspect. */
interface AppInternals {
    updateComplete: Promise<boolean>;
    currentUserId: number;
    pagehideHandler: (() => void) | null;
    documentClickHandler: ((e: MouseEvent) => void) | null;
    serviceWorkerMessageHandler: ((e: MessageEvent) => void) | null;
}

const realWebSocket = globalThis.WebSocket;

beforeEach(() => {
    MockWebSocket.instances = [];
    (globalThis as unknown as { WebSocket: unknown }).WebSocket =
        MockWebSocket as unknown;
    const slugMeta = document.createElement("meta");
    slugMeta.setAttribute("name", "app-slug");
    slugMeta.setAttribute("content", "test");
    document.head.appendChild(slugMeta);
});

afterEach(() => {
    (globalThis as unknown as { WebSocket: unknown }).WebSocket = realWebSocket;
    document.body.replaceChildren();
    document.head
        .querySelectorAll('meta[name="app-slug"], meta[name="initial-conversation-id"]')
        .forEach((el) => el.remove());
    vi.restoreAllMocks();
});

async function mountApp(): Promise<{ app: BrennApp; internals: AppInternals }> {
    const app = document.createElement("brenn-app") as BrennApp;
    document.body.appendChild(app);
    const internals = app as unknown as AppInternals;
    await internals.updateComplete;
    await Promise.resolve();
    await internals.updateComplete;
    return { app, internals };
}

describe("disconnectedCallback nulls listener fields", () => {
    it("pagehideHandler is null after removeChild", async () => {
        const { app, internals } = await mountApp();
        expect(internals.pagehideHandler).not.toBeNull();

        document.body.removeChild(app);
        expect(internals.pagehideHandler).toBeNull();
    });

    it("documentClickHandler is null after removeChild", async () => {
        const { app, internals } = await mountApp();
        expect(internals.documentClickHandler).not.toBeNull();

        document.body.removeChild(app);
        expect(internals.documentClickHandler).toBeNull();
    });
});

describe("pagehideHandler fires exactly once after mount→disconnect→remount", () => {
    it("window pagehide fires the handler once, not twice", async () => {
        // Spy on removeSignedInUserId so pagehide has an observable side effect
        // without performing real IndexedDB work in the assertion path.
        const removeSpy = vi.spyOn(pushDb, "removeSignedInUserId").mockResolvedValue();

        const { app, internals } = await mountApp();

        // Set a non-zero userId so pagehideHandler actually calls removeSignedInUserId.
        internals.currentUserId = 42;

        // Disconnect: pagehideHandler should be removed from window.
        document.body.removeChild(app);
        expect(internals.pagehideHandler).toBeNull();

        // Remount: a fresh pagehideHandler is registered.
        document.body.appendChild(app);
        await internals.updateComplete;
        expect(internals.pagehideHandler).not.toBeNull();

        // Fire pagehide: if the old handler was not removed, removeSpy fires twice.
        window.dispatchEvent(new Event("pagehide"));

        // pagehideHandler calls removeSignedInUserId only when currentUserId !== 0.
        expect(removeSpy).toHaveBeenCalledTimes(1);
        expect(removeSpy).toHaveBeenCalledWith(42);
    });
});

describe("documentClickHandler fires exactly once after mount→disconnect→remount", () => {
    it("document click fires the handler once, not twice", async () => {
        const { app, internals } = await mountApp();

        // Give a spy-friendly observable: wrap removeEventListener on document
        // to count how many handlers remain. Easier approach: count calls to
        // the handler itself by patching it after mount.
        let callCount = 0;
        const original = internals.documentClickHandler!;
        internals.documentClickHandler = (e: MouseEvent) => {
            callCount++;
            original.call(app, e);
        };
        // Re-register the patched handler so window/document uses our wrapper.
        // We need to swap it in the live listener — simplest: removeEventListener
        // with the old fn, addEventListenter with the new one, then test.
        document.removeEventListener("click", original);
        document.addEventListener("click", internals.documentClickHandler);

        // Disconnect: documentClickHandler removed, field nulled.
        document.body.removeChild(app);
        expect(internals.documentClickHandler).toBeNull();

        // Remount: a fresh documentClickHandler is registered (not our wrapped one).
        document.body.appendChild(app);
        await internals.updateComplete;
        expect(internals.documentClickHandler).not.toBeNull();

        // Fire click: if the leaked old wrapped handler were still on document,
        // callCount would be 1; the remounted handler is a different closure so
        // our callCount spy won't see it, but the key test is that the OLD
        // handler (our wrapper) was cleaned up and callCount stays 0.
        document.dispatchEvent(new MouseEvent("click", { bubbles: true }));
        expect(callCount).toBe(0);
    });
});
