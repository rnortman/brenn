// @vitest-environment happy-dom
import { describe, it, expect, afterEach, beforeEach, vi } from "vitest";
import "./app.js";
import { _clearReporterTargetForTest } from "../error-reporter.js";
import { MockWebSocket } from "../test-utils/mock-websocket.js";

/**
 * Tests for _handleNavigateTo — the SW→page NavigateTo message handler
 * added to handle push notification click deep-links.
 *
 * Strategy: mount a BrennApp with app-slug="graf", inject a mock WebSocket,
 * open a WS connection so ws.send is available, then call _handleNavigateTo
 * directly and assert on sent messages or location.assign.
 */

/** Exposes the private fields/methods we need for testing via unknown cast. */
interface AppInternals {
    updateComplete: Promise<boolean>;
    currentUserId: number;
    currentConversationId: number | null;
    _handleNavigateTo(url: string): void;
}

describe("_handleNavigateTo", () => {
    const realWebSocket = globalThis.WebSocket;

    beforeEach(() => {
        MockWebSocket.instances = [];
        (globalThis as unknown as { WebSocket: unknown }).WebSocket =
            MockWebSocket as unknown;
        const slugMeta = document.createElement("meta");
        slugMeta.setAttribute("name", "app-slug");
        slugMeta.setAttribute("content", "graf");
        document.head.appendChild(slugMeta);
    });

    afterEach(() => {
        (globalThis as unknown as { WebSocket: unknown }).WebSocket = realWebSocket;
        document.body.replaceChildren();
        document.head
            .querySelectorAll('meta[name="app-slug"], meta[name="initial-conversation-id"]')
            .forEach((el) => el.remove());
        _clearReporterTargetForTest();
        vi.restoreAllMocks();
    });

    async function mountApp(): Promise<{ app: AppInternals; ws: MockWebSocket }> {
        const rawApp = document.createElement("brenn-app");
        document.body.appendChild(rawApp);
        const app = rawApp as unknown as AppInternals;
        await app.updateComplete;
        const ws = MockWebSocket.instances[0]!;
        await Promise.resolve();
        await app.updateComplete;
        return { app, ws };
    }

    /**
     * Helper: simulate a signed-in user so routing tests don't hit the
     * pre-Welcome silent-drop path (currentUserId === 0).
     */
    function signIn(app: AppInternals, userId: number = 1): void {
        app.currentUserId = userId;
    }

    it("sends SwitchConversation for same-app URL with /c/{id}", async () => {
        const { app, ws } = await mountApp();
        signIn(app);

        app._handleNavigateTo(`${window.location.origin}/app/graf/c/42`);
        await app.updateComplete;

        expect(ws.sent.length).toBeGreaterThan(0);
        const last = JSON.parse(ws.sent[ws.sent.length - 1]!);
        expect(last.type).toBe("SwitchConversation");
        expect(last.conversation_id).toBe(42);
    });

    it("sends SwitchConversation for same-app path-only URL", async () => {
        const { app, ws } = await mountApp();
        signIn(app);

        app._handleNavigateTo("/app/graf/c/42");
        await app.updateComplete;

        const last = JSON.parse(ws.sent[ws.sent.length - 1]!);
        expect(last.type).toBe("SwitchConversation");
        expect(last.conversation_id).toBe(42);
    });

    it("sends NewConversation for /app/graf (no /c/) when currentConversationId is set", async () => {
        const { app, ws } = await mountApp();
        signIn(app);

        // Simulate a known conversation by mutating the private field.
        app.currentConversationId = 7;

        app._handleNavigateTo("/app/graf");
        await app.updateComplete;

        const last = JSON.parse(ws.sent[ws.sent.length - 1]!);
        expect(last.type).toBe("NewConversation");
    });

    it("no ws.send for /app/graf (no /c/) when currentConversationId is null", async () => {
        const { app, ws } = await mountApp();
        signIn(app);

        // Ensure detached (default).
        app.currentConversationId = null;
        const sentBefore = ws.sent.length;

        app._handleNavigateTo("/app/graf");
        await app.updateComplete;

        // No new sends.
        expect(ws.sent.length).toBe(sentBefore);
    });

    it("calls location.assign for different app URL", async () => {
        const { app } = await mountApp();
        signIn(app);
        const assignSpy = vi.spyOn(window.location, "assign").mockImplementation(() => {});

        app._handleNavigateTo("/app/pfin/c/3");
        await app.updateComplete;

        expect(assignSpy).toHaveBeenCalledWith(`${window.location.origin}/app/pfin/c/3`);
    });

    it("ignores non-/app/ same-origin path with console.warn", async () => {
        const { app, ws } = await mountApp();
        signIn(app);
        const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
        const assignSpy = vi.spyOn(window.location, "assign").mockImplementation(() => {});
        const sentBefore = ws.sent.length;

        app._handleNavigateTo("/login");
        await app.updateComplete;

        expect(warnSpy).toHaveBeenCalled();
        expect(ws.sent.length).toBe(sentBefore);
        expect(assignSpy).not.toHaveBeenCalled();
    });

    it("ignores cross-origin URL with console.warn", async () => {
        const { app, ws } = await mountApp();
        signIn(app);
        const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
        const assignSpy = vi.spyOn(window.location, "assign").mockImplementation(() => {});
        const sentBefore = ws.sent.length;

        app._handleNavigateTo("https://evil.example/app/graf/c/1");
        await app.updateComplete;

        expect(warnSpy).toHaveBeenCalled();
        expect(ws.sent.length).toBe(sentBefore);
        expect(assignSpy).not.toHaveBeenCalled();
    });

    it("ignores malformed URL with console.warn", async () => {
        const { app, ws } = await mountApp();
        signIn(app);
        const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
        const assignSpy = vi.spyOn(window.location, "assign").mockImplementation(() => {});
        const sentBefore = ws.sent.length;

        // Pass a genuinely malformed string so new URL("http://", origin) throws,
        // exercising the catch branch in _handleNavigateTo.
        app._handleNavigateTo("http://");
        await app.updateComplete;

        expect(warnSpy).toHaveBeenCalled();
        expect(ws.sent.length).toBe(sentBefore);
        expect(assignSpy).not.toHaveBeenCalled();
    });

    it("calls location.assign for /app/ with no slug (edge: empty slug)", async () => {
        // /app/ matches the startsWith("/app/") prefix but fails the full regex
        // (requires at least one non-/ char for the slug), so full-navigates.
        const { app } = await mountApp();
        signIn(app);
        const assignSpy = vi.spyOn(window.location, "assign").mockImplementation(() => {});

        app._handleNavigateTo(`${window.location.origin}/app/`);
        await app.updateComplete;

        expect(assignSpy).toHaveBeenCalledWith(`${window.location.origin}/app/`);
    });

    it("§4.2 pre-Welcome race: currentUserId === 0 → full navigation, no WS message, no reportClientError", async () => {
        // pre-Welcome: currentUserId === 0 → falls back to window.location.assign
        // (same end-state as the pre-rip openWindow path, no duplicate tab).
        const { app, ws } = await mountApp();
        // Do NOT call signIn — app starts with currentUserId === 0.
        const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
        const assignSpy = vi.spyOn(window.location, "assign").mockImplementation(() => {});
        const sentBefore = ws.sent.length;

        app._handleNavigateTo("/app/graf/c/7");
        await app.updateComplete;

        expect(ws.sent.length).toBe(sentBefore);
        expect(assignSpy).toHaveBeenCalledOnce();
        expect(assignSpy).toHaveBeenCalledWith("http://localhost:3000/app/graf/c/7");
        expect(errorSpy).not.toHaveBeenCalled();
    });

    it("same-conversation no-op: already on the target conversation → no SwitchConversation sent", async () => {
        // Receive-side same-conversation guard (app.ts) — preserve post-change per design §3.
        const { app, ws } = await mountApp();
        signIn(app, 1);
        app.currentConversationId = 42;
        const sentBefore = ws.sent.length;

        app._handleNavigateTo("/app/graf/c/42");
        await app.updateComplete;

        // No new sends — same conversation, no-op.
        expect(ws.sent.length).toBe(sentBefore);
    });
});
