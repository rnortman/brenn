// @vitest-environment happy-dom
//
// Covers the app-startup image-config path:
//   - <meta name="max-image-long-edge"> present → maxLongEdge set, imageAttachmentsDisabled false
//   - <meta name="max-image-long-edge"> absent  → imageAttachmentsDisabled true, configErrorToast queued

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import "./app.js";
import { BrennApp } from "./app.js";
import { MockWebSocket } from "../test-utils/mock-websocket.js";

interface AppInternals {
    maxLongEdge: number;
    imageAttachmentsDisabled: boolean;
    configErrorToast: string | null;
    toastHost?: { push: (o: { text: string; ttlMs?: number }) => void };
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
        .querySelectorAll(
            'meta[name="app-slug"], meta[name="initial-conversation-id"], meta[name="max-image-long-edge"]',
        )
        .forEach((el) => el.remove());
});

describe("app startup — max-image-long-edge meta tag present", () => {
    it("reads maxLongEdge from meta and leaves imageAttachmentsDisabled false", async () => {
        const meta = document.createElement("meta");
        meta.setAttribute("name", "max-image-long-edge");
        meta.setAttribute("content", "1024");
        document.head.appendChild(meta);

        const app = document.createElement("brenn-app") as BrennApp;
        document.body.appendChild(app);
        await app.updateComplete;

        const internals = app as unknown as AppInternals;
        expect(internals.maxLongEdge).toBe(1024);
        expect(internals.imageAttachmentsDisabled).toBe(false);
        expect(internals.configErrorToast).toBeNull();
    });
});

describe("app startup — max-image-long-edge meta tag absent", () => {
    it("sets imageAttachmentsDisabled true and pushes config-error toast to toastHost", async () => {
        // No max-image-long-edge meta installed.
        const app = document.createElement("brenn-app") as BrennApp;
        // Override the @query toastHost getter per-instance so firstUpdated
        // sees a mock toastHost (the real @query getter is on the prototype).
        const pushFn = vi.fn();
        const mockToastHost = { push: pushFn };
        Object.defineProperty(app, "toastHost", {
            get: () => mockToastHost,
            configurable: true,
        });
        document.body.appendChild(app);
        await app.updateComplete;

        const internals = app as unknown as AppInternals;
        expect(internals.imageAttachmentsDisabled).toBe(true);
        // configErrorToast must be cleared after firstUpdated flushes it.
        expect(internals.configErrorToast).toBeNull();
        // The toast must have been pushed with the expected message and a long TTL.
        expect(pushFn).toHaveBeenCalledTimes(1);
        const pushed = pushFn.mock.calls[0]![0] as { text: string; ttlMs?: number };
        expect(pushed.text).toMatch(/image attachment disabled/i);
        expect(pushed.ttlMs).toBeGreaterThanOrEqual(30_000);
    });

    it("queues a config-error toast before firstUpdated, flushes it after render", async () => {
        // The constructor sets configErrorToast to the error message;
        // firstUpdated pushes it to toastHost and clears the field.
        const app = document.createElement("brenn-app") as BrennApp;
        // After createElement the constructor has run but the element is not
        // yet connected; configErrorToast is set before firstUpdated.
        const internals = app as unknown as AppInternals;
        expect(internals.configErrorToast).toMatch(/image attachment disabled/i);

        document.body.appendChild(app);
        await app.updateComplete;
        // After firstUpdated, configErrorToast is cleared (flushed to toastHost).
        expect(internals.configErrorToast).toBeNull();
    });
});
