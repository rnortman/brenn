// @vitest-environment happy-dom
import { describe, it, expect, afterEach, beforeEach, vi } from "vitest";
import "./app.js";
import { BrennApp } from "./app.js";
import { MockWebSocket } from "../test-utils/mock-websocket.js";

/**
 * Tests for the §5 risk-1 runtime guard on `_handleTodoSnooze`.
 *
 * `TodoItem.effective_date` is non-nullable by wire contract, but the
 * guard catches a wire-level regression serving null/malformed
 * `effective_date`. When the guard trips, the handler `console.warn`s
 * and returns early — it does not send a `TodoSchedule` message with
 * a garbage target date.
 *
 * Setup mirrors `app-layout-transition.test.ts`'s initial-connect
 * fixture: stub `WebSocket`, inject `<meta name="app-slug">`, mount
 * `<brenn-app>`. Once the WS is open the handler is reachable via a
 * cast — the private access is intentional; this test exercises
 * exactly the guard, not the public onSnooze plumbing.
 */

/** Narrow accessor for the private handler without `any` pollution.
 *
 * Signature: `(path, repo?, effectiveDate, days)`. The runtime guard
 * against malformed `effectiveDate` is what these tests exercise; `unknown`
 * on that param models the wire-level regression (backend serves null /
 * garbage in violation of the non-nullable schema). */
interface SnoozeHandle {
  _handleTodoSnooze(
    path: string,
    repo: string | undefined,
    effectiveDate: unknown,
    days: number,
  ): void;
  todoTasks: {
    path: string;
    repo?: string | null;
    effective_date: string;
    tldr: string;
  }[];
}

describe("_handleTodoSnooze runtime guard (§5 risk #1)", () => {
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
        'meta[name="app-slug"], meta[name="initial-conversation-id"]',
      )
      .forEach((el) => el.remove());
    vi.restoreAllMocks();
  });

  async function mountApp(): Promise<{
    app: BrennApp;
    ws: MockWebSocket;
    warn: ReturnType<typeof vi.spyOn>;
  }> {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const app = document.createElement("brenn-app") as BrennApp;
    document.body.appendChild(app);
    await app.updateComplete;
    expect(MockWebSocket.instances.length).toBe(1);
    const ws = MockWebSocket.instances[0]!;
    // Let the microtask-deferred `open` fire so `sendRaw` sees OPEN
    // rather than dropping the message into the "Not connected" branch.
    await Promise.resolve();
    await app.updateComplete;
    // Reset the spy after mount: happy-dom has no `indexedDB`, so the
    // `firstUpdated` → `checkPendingShares` path emits an unrelated
    // `console.warn("Failed to check pending shares:", ...)`. Clear
    // those pre-test calls so each assertion sees only snooze-guard
    // calls.
    warn.mockClear();
    return { app, ws, warn };
  }

  it("null effective_date triggers warn, no TodoSchedule is sent", async () => {
    const { app, ws, warn } = await mountApp();
    const sentBefore = ws.sent.length;

    // `effectiveDate` is typed `string` but we pass `null` cast through
    // `unknown` to simulate the exact wire-level regression the guard
    // defends against (backend serving null in violation of the schema).
    (app as unknown as SnoozeHandle)._handleTodoSnooze(
      "todo/foo.md",
      undefined,
      null,
      1,
    );

    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn.mock.calls[0]![0]).toMatch(/effective_date/);
    expect(
      ws.sent.slice(sentBefore).some((s) => s.includes('"TodoSchedule"')),
    ).toBe(false);
  });

  it("malformed effective_date triggers warn, no TodoSchedule is sent", async () => {
    const { app, ws, warn } = await mountApp();
    const sentBefore = ws.sent.length;

    (app as unknown as SnoozeHandle)._handleTodoSnooze(
      "todo/foo.md",
      undefined,
      "not-a-date",
      1,
    );

    expect(warn).toHaveBeenCalledTimes(1);
    expect(
      ws.sent.slice(sentBefore).some((s) => s.includes('"TodoSchedule"')),
    ).toBe(false);
  });

  it("valid effective_date in the past sends TodoSchedule with today+1", async () => {
    vi.useFakeTimers();
    // Fix system time to a stable local "today" so the assertion is
    // exact rather than relying on the test-runner clock.
    vi.setSystemTime(new Date("2026-04-22T12:00:00"));

    const { app, ws, warn } = await mountApp();
    const appH = app as unknown as SnoozeHandle;
    // Seed the live task so the dispatch can snapshot its tldr into
    // the pending slot (post-slot-redesign requirement).
    appH.todoTasks = [
      {
        path: "todo/foo.md",
        repo: "life",
        effective_date: "2026-04-20",
        tldr: "foo",
      },
    ];
    const sentBefore = ws.sent.length;

    appH._handleTodoSnooze(
      "todo/foo.md",
      "life",
      "2026-04-20",
      1,
    );

    expect(warn).not.toHaveBeenCalled();
    const schedMsg = ws.sent
      .slice(sentBefore)
      .map((s) => JSON.parse(s))
      .find((m) => m.type === "TodoSchedule");
    expect(schedMsg).toBeDefined();
    expect(schedMsg).toMatchObject({
      type: "TodoSchedule",
      path: "todo/foo.md",
      repo: "life",
      date: "2026-04-23", // today+1 because effective < today
    });

    vi.useRealTimers();
  });
});
