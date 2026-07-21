// @vitest-environment happy-dom
//
// Pins the stale-client auto-reload path in `frontend/src/ws.ts`.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { expectConsoleError } from "../test-setup.js";
import {
    BrennWs,
    STALE_CLIENT_CLOSE_CODE,
    STALE_RELOAD_COUNT_KEY,
} from "../ws.js";
import type { WsServerMessage } from "../generated/WsServerMessage.js";
import "./message-list.js";
import "./app.js";
import type { BrennMessageList } from "./message-list.js";
import { BrennApp } from "./app.js";
import { MockWebSocket } from "../test-utils/mock-websocket.js";

describe("stale-client close → location.reload (3-strike loop guard)", () => {
    const realWebSocket = globalThis.WebSocket;
    let reloadSpy: ReturnType<typeof vi.spyOn>;

    beforeEach(() => {
        MockWebSocket.instances = [];
        (globalThis as unknown as { WebSocket: unknown }).WebSocket =
            MockWebSocket as unknown;
        sessionStorage.clear();
        // happy-dom exposes `window.location` as a real object but
        // does not pre-stub `reload`. Install a spy per-test and tear
        // it down via `restoreAllMocks` in afterEach.
        reloadSpy = vi
            .spyOn(window.location, "reload")
            .mockImplementation(() => {});
        vi.useFakeTimers();
    });

    afterEach(() => {
        vi.useRealTimers();
        vi.restoreAllMocks();
        (globalThis as unknown as { WebSocket: unknown }).WebSocket =
            realWebSocket;
        sessionStorage.clear();
    });

    function openWs(): {
        ws: BrennWs;
        mockWs: MockWebSocket;
        messages: WsServerMessage[];
        statuses: boolean[];
    } {
        const messages: WsServerMessage[] = [];
        const statuses: boolean[] = [];
        const priorCount = MockWebSocket.instances.length;
        const ws = new BrennWs(
            "test-slug",
            (msg) => messages.push(msg),
            (connected) => statuses.push(connected),
            () => "Compact",
        );
        ws.connect();
        // Exactly one additional mock instance was created by the
        // synchronous `connect()` call.
        expect(MockWebSocket.instances.length).toBe(priorCount + 1);
        const mockWs = MockWebSocket.instances[priorCount]!;
        return { ws, mockWs, messages, statuses };
    }

    it("first 3001 close: counter → 1, reload called once, no reconnect scheduled", () => {
        const { mockWs, messages } = openWs();
        mockWs.closeWithCode(STALE_CLIENT_CLOSE_CODE, "deployed-sha");

        expect(sessionStorage.getItem(STALE_RELOAD_COUNT_KEY)).toBe("1");
        expect(reloadSpy).toHaveBeenCalledTimes(1);
        // No in-transcript error on the first (or second) reload.
        expect(messages.filter((m) => m.type === "Error").length).toBe(0);

        // Critical: the reconnect timer path must NOT run. If it did,
        // advancing fake timers past MAX_RECONNECT_DELAY would
        // construct a second MockWebSocket instance.
        const instancesBeforeAdvance = MockWebSocket.instances.length;
        vi.advanceTimersByTime(120_000);
        expect(MockWebSocket.instances.length).toBe(instancesBeforeAdvance);
    });

    it("second 3001 close in same tab-session: counter → 2, reload called twice total", () => {
        const first = openWs();
        first.mockWs.closeWithCode(STALE_CLIENT_CLOSE_CODE, "deployed-sha");
        expect(sessionStorage.getItem(STALE_RELOAD_COUNT_KEY)).toBe("1");
        expect(reloadSpy).toHaveBeenCalledTimes(1);

        // A real browser would now reload; the test simulates the
        // post-reload reconnect by creating a fresh BrennWs against
        // the same sessionStorage (scope is per-tab, survives reload).
        const second = openWs();
        second.mockWs.closeWithCode(STALE_CLIENT_CLOSE_CODE, "deployed-sha");
        expect(sessionStorage.getItem(STALE_RELOAD_COUNT_KEY)).toBe("2");
        expect(reloadSpy).toHaveBeenCalledTimes(2);
    });

    it("third 3001 close hits cap: no further reload, in-transcript Error surfaced, no reconnect", () => {
        // Pre-seed the counter to the cap (3) directly — same as what
        // the first two reloads would have produced. Avoids shuffling
        // three BrennWs lifetimes for the same check.
        sessionStorage.setItem(STALE_RELOAD_COUNT_KEY, "3");
        // handleStaleClient() logs the cap-hit to console.error — expected.
        expectConsoleError(/stale-client reload cap reached/);
        const { mockWs, messages } = openWs();
        mockWs.closeWithCode(STALE_CLIENT_CLOSE_CODE, "deployed-sha");

        // Counter not bumped above the cap (stop, don't burn CPU).
        expect(sessionStorage.getItem(STALE_RELOAD_COUNT_KEY)).toBe("3");
        expect(reloadSpy).not.toHaveBeenCalled();

        // Error message routed through onMessage, user-visible text.
        const errors = messages.filter(
            (m): m is WsServerMessage & { type: "Error"; message: string } =>
                m.type === "Error",
        );
        expect(errors.length).toBe(1);
        expect(errors[0]!.message).toContain("outdated version");
        expect(errors[0]!.message).toContain("close and reopen");

        // Still no reconnect scheduled — at the cap we want the tab
        // to stay dead so the user closes it.
        const instancesBeforeAdvance = MockWebSocket.instances.length;
        vi.advanceTimersByTime(120_000);
        expect(MockWebSocket.instances.length).toBe(instancesBeforeAdvance);
    });

    it("onopen clears the stale-reload counter so a later transient mismatch re-arms the 3-strike budget", async () => {
        sessionStorage.setItem(STALE_RELOAD_COUNT_KEY, "2");
        openWs();
        // Flush the queueMicrotask-deferred onopen from MockWebSocket.
        vi.useRealTimers();
        await Promise.resolve();
        await Promise.resolve();
        expect(sessionStorage.getItem(STALE_RELOAD_COUNT_KEY)).toBeNull();
    });

    it("BrennApp routes stale-client Error → messageList.appendError (guards against F3-style drift)", async () => {
        // The cap-path surfaces failure through `onMessage({ type:
        // "Error", ... })`. That envelope must land in
        // `messageList.appendError`, NOT in `toastHost.push`, because
        // the design's UX promise is an in-transcript red block (the
        // same surface the server uses for "Session stolen"). This
        // test pins that routing end-to-end by mounting a real
        // <brenn-app>, driving its boot sequence through to a
        // rendered <brenn-message-list>, then firing a Close(3001)
        // at the cap and observing that appendError is what got
        // called (not toastHost.push).
        //
        // Note: the pre-seeded `brenn.stale-reload-count=3` must be
        // re-applied AFTER the mock socket's `onopen` fires (the
        // onopen handler clears the counter — the whole point of
        // clearing is to re-arm the 3-strike budget after a clean
        // handshake). So we seed after open-settle, then fire the
        // close.
        vi.useRealTimers();

        const slugMeta = document.createElement("meta");
        slugMeta.setAttribute("name", "app-slug");
        slugMeta.setAttribute("content", "test-slug");
        document.head.appendChild(slugMeta);

        try {
            const app = document.createElement("brenn-app") as BrennApp;
            document.body.appendChild(app);
            await app.updateComplete;

            // <brenn-app>'s own connect() constructed a MockWebSocket.
            // Wait for the queueMicrotask-deferred onopen to fire and
            // then feed the authoritative server sequence through to
            // SetLayout so <brenn-message-list> mounts.
            await Promise.resolve();
            await Promise.resolve();
            expect(MockWebSocket.instances.length).toBeGreaterThanOrEqual(1);
            const mockWs =
                MockWebSocket.instances[MockWebSocket.instances.length - 1]!;
            mockWs.onmessage?.(
                new MessageEvent("message", {
                    data: JSON.stringify({
                        type: "Welcome",
                        username: "alice",
                        user_id: 0,
                        multiuser: false,
                        singleton: true,
                        available_models: [],
                        default_model: "sonnet",
                        attachment_targets: [],
                        pwa_push_enabled: false,
                    } satisfies WsServerMessage),
                }),
            );
            mockWs.onmessage?.(
                new MessageEvent("message", {
                    data: JSON.stringify({
                        type: "SetLayout",
                        layout: { type: "SinglePane" },
                    } satisfies WsServerMessage),
                }),
            );
            await app.updateComplete;
            await app.updateComplete;

            // BrennApp uses light DOM (createRenderRoot returns
            // `this`), so the child list is visible to
            // `querySelector` on the app element itself.
            const list = app.querySelector(
                "brenn-message-list",
            ) as BrennMessageList | null;
            expect(list).toBeTruthy();
            const appendErrorSpy = vi.spyOn(list!, "appendError");

            // Seed the cap NOW — after onopen cleared it, before the
            // stale close fires. Simulates "this tab has already been
            // through 3 reload attempts in this tab-session".
            sessionStorage.setItem(STALE_RELOAD_COUNT_KEY, "3");
            // handleStaleClient() logs the cap-hit to console.error — expected.
            expectConsoleError(/stale-client reload cap reached/);

            mockWs.closeWithCode(STALE_CLIENT_CLOSE_CODE, "deployed-sha");
            // Let the close handler run and any resulting Lit render
            // settle before asserting.
            await app.updateComplete;

            expect(appendErrorSpy).toHaveBeenCalledTimes(1);
            const firstArg = appendErrorSpy.mock.calls[0]![0];
            expect(firstArg).toContain("outdated version");
            expect(firstArg).toContain("close and reopen");
        } finally {
            document.body.replaceChildren();
            document.head
                .querySelectorAll('meta[name="app-slug"]')
                .forEach((el) => el.remove());
        }
    });
});
