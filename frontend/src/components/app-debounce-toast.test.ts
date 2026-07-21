// @vitest-environment happy-dom
import { describe, it, expect, afterEach, beforeEach, vi } from "vitest";
import "./app.js";
import { BrennApp } from "./app.js";
import { MockWebSocket } from "../test-utils/mock-websocket.js";

/**
 * Phase 4 coverage: per-row debounce (§6) and toast routing (§5.3).
 *
 * Debounce: `_armDebounce` gates duplicate sends within 400ms of the
 * first tap, on a per-key basis. A different key is unaffected.
 *
 * Toast routing: `_handleTodoActionResult` fires a toast only on
 * `done` success, with branches for recurring-continuation, terminal,
 * and non-recurring. snooze / reorder responses produce no toast.
 */

import type { TodoItem } from "../generated/TodoItem.js";
import type {
  SlotState,
  TaskGroup,
} from "./todo-list.js";

interface AppInternals {
  _handleTodoDone(path: string, repo?: string): void;
  _handleTodoSnooze(
    path: string,
    repo: string | undefined,
    effectiveDate: string,
    days: number,
  ): void;
  _handleTodoReorder(target: {
    path: string;
    repo?: string;
    after: { path: string; repo?: string } | null;
    before: { path: string; repo?: string } | null;
    targetGroupDate: null;
    selectedKeys: null;
  }): void;
  _handleMultiReorder(target: {
    path: string;
    repo?: string;
    after: { path: string; repo?: string } | null;
    before: { path: string; repo?: string } | null;
    targetGroupDate: null;
    selectedKeys: string[];
  }): void;
  _handleTodoSchedule(target: {
    path: string;
    repo?: string;
    date: string;
    selectedKeys: string[] | null;
  }): void;
  _handleTodoActionResult(
    path: string,
    success: boolean,
    error: string | null,
    nextCheckInDate: string | null,
    nextDueDate: string | null,
    terminal: boolean | null,
    repo?: string | null,
  ): void;
  _handleTodoState(tasks: unknown[], todayStr: string): void;
  _handleTodoRefresh(): void;
  _handleDismissSettled(key: string): void;
  _primeForReplay(conversationId: number | null): void;
  _freezeListIfNeeded(): void;
  _thawFrozenList(): void;
  // Render-pipeline access for the F2 end-to-end test. The app's
  // private `handleMessage` is the only way to reach the SetLayout +
  // TodoState handlers without faking the WS layer at a deeper level.
  handleMessage(msg: unknown): void;
  todoSlotState: Map<string, SlotState>;
  todoFrozenSnapshot: { groups: TaskGroup[]; todayStr: string } | null;
  todoSlotWatchdogs: Map<string, ReturnType<typeof setTimeout>>;
  todoErrorKeys: Map<string, string>;
  todoRefreshPending: boolean;
  todoRefreshQueued: boolean;
  todoTasks: {
    path: string;
    repo?: string | null;
    effective_date: string;
    tldr: string;
    sort_order?: number | null;
    due_date?: string | null;
  }[];
  todoTodayStr: string;
  todoReorderSnapshots: Map<string, { tasks: AppInternals["todoTasks"]; selectedTasks: Map<string, unknown> }>;
  selectedTasks: Map<string, unknown>;
  toastHost?: { push: (o: { text: string; ttlMs?: number }) => void };
}

/** Seed a pending slot for a given path/repo so
 *  `_handleTodoActionResult` has something to match against.
 *
 *  Inserted directly into `todoSlotState` to bypass the dispatch's WS
 *  send and watchdog arming — tests that care about the WS traffic
 *  or watchdog timing seed through the real `_handleTodoDone` /
 *  `_handleTodoSnooze` instead.
 *
 *  Snapshot capture is delegated to the real `_freezeListIfNeeded` so
 *  the snapshot shape can't drift from the production code. We also
 *  seed `todoTasks` with a stub row so the freeze hook's empty-
 *  todoTasks invariant guard doesn't fire. */
function seedPending(
  app: AppInternals,
  path: string,
  repo: string | undefined,
  action: "done" | "snooze" | "reorder" | "schedule",
  targetEffectiveDate?: string,
): void {
  const key = repo ? `${repo}:${path}` : `:${path}`;
  // Ensure `todoTasks` has at least one entry so `_freezeListIfNeeded`
  // doesn't trip its empty-list invariant guard.
  if (app.todoTasks.length === 0) {
    app.todoTasks = [
      {
        path,
        repo: repo ?? null,
        tldr: "seed fixture",
        effective_date: "2026-04-22",
      },
    ];
    app.todoTodayStr = "2026-04-22";
  }
  // Delegate snapshot capture to the production code path.
  app._freezeListIfNeeded();
  app.todoSlotState.set(key, {
    kind: "pending",
    action,
    startedAt: Date.now(),
    path,
    repo,
    taskTldr: "seed fixture",
    targetEffectiveDate,
  });
}

describe("per-row debounce + toast routing (Phase 4)", () => {
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

  async function mountApp(): Promise<{ app: BrennApp; ws: MockWebSocket }> {
    const app = document.createElement("brenn-app") as BrennApp;
    document.body.appendChild(app);
    await app.updateComplete;
    const ws = MockWebSocket.instances[0]!;
    // Allow the open event to fire.
    await Promise.resolve();
    await app.updateComplete;
    return { app, ws };
  }

  /** Seed the live task list with fixture entries so dispatch handlers
   *  (which snapshot `tldr` from `todoTasks`) can resolve the row. */
  function seedLiveTasks(
    app: AppInternals,
    entries: Array<{ path: string; repo?: string; tldr?: string }>,
  ): void {
    app.todoTasks = entries.map((e) => ({
      path: e.path,
      repo: e.repo ?? null,
      effective_date: "2026-04-22",
      tldr: e.tldr ?? e.path,
    }));
  }

  it("blocks a second TodoDone within 400ms on the same key", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-22T12:00:00"));
    const { app, ws } = await mountApp();
    const appI = app as unknown as AppInternals;
    seedLiveTasks(appI, [{ path: "todo/foo.md", repo: "life" }]);
    const sentBefore = ws.sent.length;

    appI._handleTodoDone("todo/foo.md", "life");
    appI._handleTodoDone("todo/foo.md", "life");

    const sends = ws.sent
      .slice(sentBefore)
      .map((s) => JSON.parse(s))
      .filter((m) => m.type === "TodoDone");
    expect(sends).toHaveLength(1);
    vi.useRealTimers();
  });

  it("allows a second TodoDone on a different key immediately", async () => {
    const { app, ws } = await mountApp();
    const appI = app as unknown as AppInternals;
    seedLiveTasks(appI, [
      { path: "todo/foo.md", repo: "life" },
      { path: "todo/bar.md", repo: "life" },
    ]);
    const sentBefore = ws.sent.length;
    appI._handleTodoDone("todo/foo.md", "life");
    appI._handleTodoDone("todo/bar.md", "life");
    const sends = ws.sent
      .slice(sentBefore)
      .map((s) => JSON.parse(s))
      .filter((m) => m.type === "TodoDone");
    expect(sends).toHaveLength(2);
  });

  it("allows a retry after the 400ms window expires", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-22T12:00:00"));
    const { app, ws } = await mountApp();
    const appI = app as unknown as AppInternals;
    seedLiveTasks(appI, [{ path: "todo/foo.md", repo: "life" }]);
    const sentBefore = ws.sent.length;
    appI._handleTodoDone("todo/foo.md", "life");
    vi.advanceTimersByTime(500);
    appI._handleTodoDone("todo/foo.md", "life");
    const sends = ws.sent
      .slice(sentBefore)
      .map((s) => JSON.parse(s))
      .filter((m) => m.type === "TodoDone");
    expect(sends).toHaveLength(2);
    vi.useRealTimers();
  });

  async function toastInterceptor(app: BrennApp): Promise<string[]> {
    // Stub the toast host with a capture function so we don't depend
    // on the real DOM insertion ordering inside Lit.
    await app.updateComplete;
    const calls: string[] = [];
    const host = (app as unknown as AppInternals).toastHost;
    if (host) {
      host.push = (o: { text: string }) => {
        calls.push(o.text);
      };
    } else {
      // Fall back: stub via Object.defineProperty
      Object.defineProperty(app, "toastHost", {
        configurable: true,
        value: { push: (o: { text: string }) => calls.push(o.text) },
      });
    }
    return calls;
  }

  it("fires 'Done. Next: <date>' on recurring-continuation", async () => {
    const { app } = await mountApp();
    const calls = await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    seedPending(appI, "todo/foo.md", undefined, "done");
    appI._handleTodoActionResult(
      "todo/foo.md",
      true,
      null,
      "2026-04-29",
      null,
      false,
    );
    expect(calls.length).toBe(1);
    expect(calls[0]).toMatch(/^Done\. Next: /);
  });

  it("fires 'Done. That was the last one.' on terminal completion", async () => {
    const { app } = await mountApp();
    const calls = await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    seedPending(appI, "todo/foo.md", undefined, "done");
    appI._handleTodoActionResult("todo/foo.md", true, null, null, null, true);
    expect(calls).toEqual(["Done. That was the last one."]);
  });

  it("fires plain 'Done.' for non-recurring tasks", async () => {
    const { app } = await mountApp();
    const calls = await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    seedPending(appI, "todo/foo.md", undefined, "done");
    appI._handleTodoActionResult("todo/foo.md", true, null, null, null, null);
    expect(calls).toEqual(["Done."]);
  });

  it("does NOT toast on snooze success", async () => {
    const { app } = await mountApp();
    const calls = await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    seedPending(appI, "todo/foo.md", undefined, "snooze", "2026-04-29");
    appI._handleTodoActionResult(
      "todo/foo.md",
      true,
      null,
      "2026-04-29",
      null,
      false,
    );
    expect(calls).toEqual([]);
  });

  it("does NOT toast on reorder success", async () => {
    const { app } = await mountApp();
    const calls = await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    seedPending(appI, "todo/foo.md", undefined, "reorder");
    appI._handleTodoActionResult("todo/foo.md", true, null, null, null, null);
    expect(calls).toEqual([]);
  });

  it("does NOT toast on an error response (chat inject handles it)", async () => {
    const { app } = await mountApp();
    const calls = await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    seedPending(appI, "todo/foo.md", undefined, "done");
    appI._handleTodoActionResult(
      "todo/foo.md",
      false,
      "stale_anchor: …",
      null,
      null,
      null,
    );
    expect(calls).toEqual([]);
  });

  it("PRD-done §8: falls back to next_due_date when next_check_in_date is absent", async () => {
    const { app } = await mountApp();
    const calls = await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    // Task recurs on due_date only (no check_in_date). graf returns
    // next_due_date but next_check_in_date is null.
    seedPending(appI, "todo/foo.md", undefined, "done");
    appI._handleTodoActionResult(
      "todo/foo.md",
      true,
      null,
      null,
      "2026-04-29",
      false,
    );
    expect(calls.length).toBe(1);
    expect(calls[0]).toMatch(/^Done\. Next: /);
  });

  // --- Slot transitions on success (design §3.4.2) ---------------------

  it("_handleTodoActionResult on success/done transitions slot to settled", async () => {
    const { app } = await mountApp();
    await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    seedPending(appI, "todo/foo.md", undefined, "done");
    appI._handleTodoActionResult(
      "todo/foo.md",
      true,
      null,
      "2026-04-29",
      null,
      false,
    );
    const entry = appI.todoSlotState.get(":todo/foo.md");
    expect(entry).toBeDefined();
    if (entry && entry.kind === "settled") {
      expect(entry.action).toBe("done");
      expect(entry.tileText).toMatch(/^Next: /);
    } else {
      throw new Error("expected settled entry");
    }
  });

  it("_handleTodoActionResult on success/snooze sets tileText from targetEffectiveDate", async () => {
    const { app } = await mountApp();
    await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    seedPending(appI, "todo/foo.md", undefined, "snooze", "2026-04-29");
    appI._handleTodoActionResult(
      "todo/foo.md",
      true,
      null,
      null,
      null,
      false,
    );
    const entry = appI.todoSlotState.get(":todo/foo.md");
    expect(entry).toBeDefined();
    if (entry && entry.kind === "settled") {
      expect(entry.action).toBe("snooze");
      expect(entry.tileText).toMatch(/^Snoozed to /);
    } else {
      throw new Error("expected settled entry");
    }
  });

  it("_handleTodoActionResult on success/reorder clears the slot outright", async () => {
    const { app } = await mountApp();
    await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    seedPending(appI, "todo/foo.md", undefined, "reorder");
    expect(appI.todoSlotState.has(":todo/foo.md")).toBe(true);
    appI._handleTodoActionResult("todo/foo.md", true, null, null, null, null);
    expect(appI.todoSlotState.has(":todo/foo.md")).toBe(false);
  });

  // --- TodoState reconcile (§3.4.3) ------------------------------------

  it("_handleTodoState preserves a still-pending key for a task still in the refresh", async () => {
    // Regression test for `todo-pending-state-lost-on-list-refresh`:
    // B's pending state must survive A's TodoState refresh.
    const { app } = await mountApp();
    const appI = app as unknown as AppInternals;
    seedPending(appI, "todo/bar.md", undefined, "snooze", "2026-04-29");
    // B is still in the refresh.
    appI._handleTodoState(
      [
        {
          path: "todo/bar.md",
          tldr: "bar",
          effective_date: "2026-04-29",
        },
      ],
      "2026-04-22",
    );
    const entry = appI.todoSlotState.get(":todo/bar.md");
    expect(entry).toBeDefined();
    expect(entry!.kind).toBe("pending");
  });

  it("_handleTodoState implicitly settles a pending key absent from the refresh (done)", async () => {
    // Defensive branch for §3.4.3: WS reconnect may lose the
    // TodoActionResult; the follow-up TodoState transitions the slot
    // to settled with a generic tile.
    const { app } = await mountApp();
    const appI = app as unknown as AppInternals;
    seedPending(appI, "todo/foo.md", undefined, "done");
    // foo.md absent from the refresh.
    appI._handleTodoState([], "2026-04-22");
    const entry = appI.todoSlotState.get(":todo/foo.md");
    expect(entry).toBeDefined();
    expect(entry!.kind).toBe("settled");
  });

  it("_handleTodoState drops a pending reorder whose task is absent (no settled tile — §3.6)", async () => {
    const { app } = await mountApp();
    const appI = app as unknown as AppInternals;
    seedPending(appI, "todo/foo.md", undefined, "reorder");
    appI._handleTodoState([], "2026-04-22");
    expect(appI.todoSlotState.has(":todo/foo.md")).toBe(false);
  });

  // --- Idle-timer dismissal (§3.5) -------------------------------------

  it("idle timer dismisses settled tiles after 5s", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-22T12:00:00"));
    const { app } = await mountApp();
    await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    seedPending(appI, "todo/foo.md", undefined, "done");
    appI._handleTodoActionResult("todo/foo.md", true, null, null, null, null);
    // Settled — tile present.
    expect(appI.todoSlotState.get(":todo/foo.md")?.kind).toBe("settled");
    vi.advanceTimersByTime(4900);
    expect(appI.todoSlotState.get(":todo/foo.md")?.kind).toBe("settled");
    vi.advanceTimersByTime(200); // now >5s
    expect(appI.todoSlotState.has(":todo/foo.md")).toBe(false);
    vi.useRealTimers();
  });

  it("idle timer resets on a new mutation dispatch", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-22T12:00:00"));
    const { app } = await mountApp();
    await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    // Settle A at t=0.
    seedPending(appI, "todo/foo.md", undefined, "done");
    appI._handleTodoActionResult("todo/foo.md", true, null, null, null, null);
    expect(appI.todoSlotState.get(":todo/foo.md")?.kind).toBe("settled");
    // At t=3s, dispatch done on B — idle timer should be cancelled.
    vi.advanceTimersByTime(3000);
    appI.todoTasks = [
      {
        path: "todo/bar.md",
        repo: "life",
        effective_date: "2026-04-22",
        tldr: "bar",
      },
    ];
    appI._handleTodoDone("todo/bar.md", "life");
    // At t=3s + 5s = 8s, settle B.
    vi.advanceTimersByTime(5000);
    // A tile still present — timer was cancelled and re-armed when B settled.
    expect(appI.todoSlotState.get(":todo/foo.md")?.kind).toBe("settled");
    appI._handleTodoActionResult("todo/bar.md", true, null, null, null, null);
    // A and B both settled; idle timer armed at t=8s.
    vi.advanceTimersByTime(5000);
    // At t=13s: both tiles collapsed together.
    expect(appI.todoSlotState.has(":todo/foo.md")).toBe(false);
    expect(appI.todoSlotState.has("life:todo/bar.md")).toBe(false);
    vi.useRealTimers();
  });

  it("idle timer re-arms after a pending slot is cleared via error (§3.5)", async () => {
    // Regression test for deep-review F1: the old code only
    // cancelled the idle timer in `_collapseSettledSlot`; it never
    // re-armed it. With A settled and B pending, B erroring should
    // cause A's tile to collapse after 5s of idle.
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-22T12:00:00"));
    const { app } = await mountApp();
    await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    // Settle A.
    seedPending(appI, "todo/foo.md", undefined, "done");
    appI._handleTodoActionResult("todo/foo.md", true, null, null, null, null);
    expect(appI.todoSlotState.get(":todo/foo.md")?.kind).toBe("settled");
    // Start B pending (which cancels the idle timer — "active triage").
    vi.advanceTimersByTime(1000);
    seedPending(appI, "todo/bar.md", undefined, "done");
    // B errors: slot cleared, A still settled.
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    appI._handleTodoActionResult(
      "todo/bar.md",
      false,
      "stale_anchor: …",
      null,
      null,
      null,
    );
    warn.mockRestore();
    expect(appI.todoSlotState.has(":todo/bar.md")).toBe(false);
    expect(appI.todoSlotState.get(":todo/foo.md")?.kind).toBe("settled");
    // 5s after B's error, A should collapse. Without the re-arm, A
    // would sit forever.
    vi.advanceTimersByTime(5_100);
    expect(appI.todoSlotState.has(":todo/foo.md")).toBe(false);
    vi.useRealTimers();
  });

  it("idle timer re-arms after a pending slot is cleared via reorder-success (§3.5)", async () => {
    // Same as above but the pending clears via reorder-success
    // (another path that used to leak — no settled tile, just a
    // slot drop).
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-22T12:00:00"));
    const { app } = await mountApp();
    await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    seedPending(appI, "todo/foo.md", undefined, "done");
    appI._handleTodoActionResult("todo/foo.md", true, null, null, null, null);
    vi.advanceTimersByTime(1000);
    seedPending(appI, "todo/bar.md", undefined, "reorder");
    appI._handleTodoActionResult("todo/bar.md", true, null, null, null, null);
    expect(appI.todoSlotState.has(":todo/bar.md")).toBe(false);
    vi.advanceTimersByTime(5_100);
    expect(appI.todoSlotState.has(":todo/foo.md")).toBe(false);
    vi.useRealTimers();
  });

  // --- Pending-slot watchdog (§3.4.4) ----------------------------------

  it("pending watchdog drops the slot after 30s of silence", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-22T12:00:00"));
    const { app } = await mountApp();
    const appI = app as unknown as AppInternals;
    appI.todoTasks = [
      {
        path: "todo/foo.md",
        repo: "life",
        effective_date: "2026-04-22",
        tldr: "foo",
      },
    ];
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    appI._handleTodoDone("todo/foo.md", "life");
    expect(appI.todoSlotState.get("life:todo/foo.md")?.kind).toBe("pending");
    vi.advanceTimersByTime(29_000);
    expect(appI.todoSlotState.get("life:todo/foo.md")?.kind).toBe("pending");
    vi.advanceTimersByTime(2_000);
    expect(appI.todoSlotState.has("life:todo/foo.md")).toBe(false);
    expect(warn).toHaveBeenCalled();
    warn.mockRestore();
    vi.useRealTimers();
  });

  it("idle timer re-arms after watchdog-fire clears the last pending (§3.5 F2)", async () => {
    // Regression test for deep-review F2: the old
    // `_onSlotWatchdogFire` did hand-rolled cleanup and never
    // re-armed the idle timer.
    //
    // Uses the real `_handleTodoDone` dispatch for B so that the
    // watchdog actually arms (seedPending skips the watchdog-arm).
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-22T12:00:00"));
    const { app } = await mountApp();
    await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    // Settle A.
    seedPending(appI, "todo/foo.md", undefined, "done");
    appI._handleTodoActionResult("todo/foo.md", true, null, null, null, null);
    // Dispatch B pending via the real handler (arms watchdog).
    vi.advanceTimersByTime(1000);
    appI.todoTasks = [
      {
        path: "todo/bar.md",
        repo: "life",
        effective_date: "2026-04-22",
        tldr: "bar",
      },
    ];
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    appI._handleTodoDone("todo/bar.md", "life");
    // Watchdog fires 30s from dispatch.
    vi.advanceTimersByTime(31_000);
    warn.mockRestore();
    expect(appI.todoSlotState.has("life:todo/bar.md")).toBe(false);
    // A's idle timer re-armed after B was cleared; fires 5s later.
    vi.advanceTimersByTime(5_100);
    expect(appI.todoSlotState.has(":todo/foo.md")).toBe(false);
    vi.useRealTimers();
  });

  // --- Refresh serialization (§3.5) ------------------------------------

  it("refresh click with a pending slot: drops settled, queues refresh, does not send", async () => {
    const { app, ws } = await mountApp();
    await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    // One settled slot (from a previous done) + one pending slot.
    seedPending(appI, "todo/foo.md", undefined, "done");
    appI._handleTodoActionResult("todo/foo.md", true, null, null, null, null);
    seedPending(appI, "todo/bar.md", undefined, "snooze", "2026-04-29");
    const sentBefore = ws.sent.length;
    appI._handleTodoRefresh();
    // Settled slot dropped; pending kept.
    expect(appI.todoSlotState.has(":todo/foo.md")).toBe(false);
    expect(appI.todoSlotState.get(":todo/bar.md")?.kind).toBe("pending");
    expect(appI.todoRefreshQueued).toBe(true);
    // No TodoRefresh sent yet.
    const refreshSends = ws.sent
      .slice(sentBefore)
      .map((s) => JSON.parse(s))
      .filter((m) => m.type === "TodoRefresh");
    expect(refreshSends).toHaveLength(0);
    // Settling the pending slot triggers the queued refresh.
    appI._handleTodoActionResult(
      "todo/bar.md",
      true,
      null,
      null,
      null,
      false,
    );
    const refreshSendsAfter = ws.sent
      .slice(sentBefore)
      .map((s) => JSON.parse(s))
      .filter((m) => m.type === "TodoRefresh");
    expect(refreshSendsAfter).toHaveLength(1);
    expect(appI.todoRefreshQueued).toBe(false);
  });

  it("mutation attempt short-circuits while todoRefreshQueued is true", async () => {
    const { app, ws } = await mountApp();
    await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    seedPending(appI, "todo/bar.md", undefined, "snooze", "2026-04-29");
    appI.todoTasks = [
      {
        path: "todo/foo.md",
        repo: "life",
        effective_date: "2026-04-22",
        tldr: "foo",
      },
    ];
    appI._handleTodoRefresh();
    expect(appI.todoRefreshQueued).toBe(true);
    const sentBefore = ws.sent.length;
    appI._handleTodoDone("todo/foo.md", "life");
    const doneSends = ws.sent
      .slice(sentBefore)
      .map((s) => JSON.parse(s))
      .filter((m) => m.type === "TodoDone");
    expect(doneSends).toHaveLength(0);
  });

  it("refresh queued: watchdog fire unblocks queued refresh", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-22T12:00:00"));
    const { app, ws } = await mountApp();
    await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    appI.todoTasks = [
      {
        path: "todo/foo.md",
        repo: "life",
        effective_date: "2026-04-22",
        tldr: "foo",
      },
    ];
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    appI._handleTodoDone("todo/foo.md", "life");
    appI._handleTodoRefresh();
    expect(appI.todoRefreshQueued).toBe(true);
    const sentBefore = ws.sent.length;
    vi.advanceTimersByTime(31_000);
    // Watchdog fired → queued refresh fired.
    const refreshSends = ws.sent
      .slice(sentBefore)
      .map((s) => JSON.parse(s))
      .filter((m) => m.type === "TodoRefresh");
    expect(refreshSends).toHaveLength(1);
    expect(appI.todoRefreshQueued).toBe(false);
    warn.mockRestore();
    vi.useRealTimers();
  });

  // --- Reorder-watchdog → TodoRefresh (app-ts-watchdog-reorder-tests) ---

  it("reorder watchdog fires → sends one TodoRefresh", async () => {
    // Covers: _onSlotWatchdogFire wasReorder branch (cycle 24 addition).
    // Dispatch a reorder, advance fake timers 31s past the 30s watchdog,
    // assert exactly one TodoRefresh appears in the WS-mock output.
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-22T12:00:00"));
    const { app, ws } = await mountApp();
    const appI = app as unknown as AppInternals;
    appI.todoTasks = [
      { path: "todo/foo.md", repo: "life", effective_date: "2026-04-22", tldr: "foo" },
      { path: "todo/bar.md", repo: "life", effective_date: "2026-04-22", tldr: "bar" },
    ];
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const sentBefore = ws.sent.length;
    appI._handleTodoReorder({
      path: "todo/foo.md",
      repo: "life",
      after: { path: "todo/bar.md", repo: "life" },
      before: null,
      targetGroupDate: null,
      selectedKeys: null,
    });
    expect(appI.todoSlotState.get("life:todo/foo.md")?.kind).toBe("pending");
    vi.advanceTimersByTime(31_000);
    // Slot must be gone (watchdog dropped it).
    expect(appI.todoSlotState.has("life:todo/foo.md")).toBe(false);
    // Exactly one TodoRefresh must have been sent.
    const refreshSends = ws.sent
      .slice(sentBefore)
      .map((s) => JSON.parse(s))
      .filter((m: { type: string }) => m.type === "TodoRefresh");
    expect(refreshSends).toHaveLength(1);
    warn.mockRestore();
    vi.useRealTimers();
  });

  it("done-action watchdog fires → no TodoRefresh sent", async () => {
    // The wasReorder branch must be action-specific: a done/snooze slot
    // timing out must not trigger a TodoRefresh.
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-22T12:00:00"));
    const { app, ws } = await mountApp();
    const appI = app as unknown as AppInternals;
    appI.todoTasks = [
      { path: "todo/foo.md", repo: "life", effective_date: "2026-04-22", tldr: "foo" },
    ];
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const sentBefore = ws.sent.length;
    appI._handleTodoDone("todo/foo.md", "life");
    vi.advanceTimersByTime(31_000);
    expect(appI.todoSlotState.has("life:todo/foo.md")).toBe(false);
    const refreshSends = ws.sent
      .slice(sentBefore)
      .map((s) => JSON.parse(s))
      .filter((m: { type: string }) => m.type === "TodoRefresh");
    expect(refreshSends).toHaveLength(0);
    warn.mockRestore();
    vi.useRealTimers();
  });

  it("reorder watchdog: refresh already pending at fire time → no double-send", async () => {
    // If todoRefreshPending is true exactly when the reorder watchdog fires,
    // _handleTodoRefresh returns early (rapid-double-click guard at line 3260)
    // and no second TodoRefresh is sent.
    // Strategy: dispatch the reorder, advance to 29s (watchdog not yet fired),
    // then manually set todoRefreshPending = true (simulating a late-arriving
    // refresh that started just before the watchdog deadline), then advance 2s
    // so the watchdog fires while todoRefreshPending is true.
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-22T12:00:00"));
    const { app, ws } = await mountApp();
    const appI = app as unknown as AppInternals;
    appI.todoTasks = [
      { path: "todo/foo.md", repo: "life", effective_date: "2026-04-22", tldr: "foo" },
      { path: "todo/bar.md", repo: "life", effective_date: "2026-04-22", tldr: "bar" },
    ];
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    appI._handleTodoReorder({
      path: "todo/foo.md",
      repo: "life",
      after: { path: "todo/bar.md", repo: "life" },
      before: null,
      targetGroupDate: null,
      selectedKeys: null,
    });
    // Advance to just before the watchdog fires.
    vi.advanceTimersByTime(29_000);
    // Simulate a refresh that arrived just before the watchdog deadline.
    appI.todoRefreshPending = true;
    const sentBefore = ws.sent.length;
    // Watchdog fires now — todoRefreshPending is true so _handleTodoRefresh
    // must return early and must NOT send a second TodoRefresh.
    vi.advanceTimersByTime(2_000);
    const refreshSends = ws.sent
      .slice(sentBefore)
      .map((s) => JSON.parse(s))
      .filter((m: { type: string }) => m.type === "TodoRefresh");
    expect(refreshSends).toHaveLength(0);
    warn.mockRestore();
    vi.useRealTimers();
  });

  // --- Null-matchedKey snapshot race (app-ts-snapshot-race-tests) -------

  it("null-matchedKey success ack cleans up reorder snapshot via scan", async () => {
    // Covers: _handleTodoActionResult null-matchedKey else branch (cycle 24).
    // Simulate: reorder dispatched, watchdog fires and clears the slot,
    // then success ack arrives with matchedKey=null → snapshot must be gone.
    const { app } = await mountApp();
    const appI = app as unknown as AppInternals;
    // Seed tasks so _handleTodoReorder can snapshot and find the key.
    appI.todoTasks = [
      { path: "todo/foo.md", repo: "life", effective_date: "2026-04-22", tldr: "foo" },
      { path: "todo/bar.md", repo: "life", effective_date: "2026-04-22", tldr: "bar" },
    ];
    // Dispatch a real reorder to populate todoReorderSnapshots.
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    appI._handleTodoReorder({
      path: "todo/foo.md",
      repo: "life",
      after: { path: "todo/bar.md", repo: "life" },
      before: null,
      targetGroupDate: null,
      selectedKeys: null,
    });
    expect(appI.todoReorderSnapshots.size).toBeGreaterThan(0);
    // Simulate the watchdog racing ahead: manually clear todoSlotState
    // so matchedKey will be null on the action result.
    appI.todoSlotState.clear();
    // Deliver the success ack — matchedKey resolves to null because the slot
    // was already cleared (no match in todoSlotState).
    appI._handleTodoActionResult(
      "todo/foo.md",
      true,
      null,
      null,
      null,
      null,
      "life",
    );
    // The snapshot must have been cleaned up by the suffix/exact scan.
    expect(appI.todoReorderSnapshots.size).toBe(0);
    warn.mockRestore();
  });

  // --- Manual dismiss (×) ---------------------------------------------

  it("manual _handleDismissSettled transitions just that slot to dismissed", async () => {
    // Under the freeze-the-list design, × on a tile while other tiles
    // are still showing transitions the slot to `dismissed` (a same-
    // height placeholder) rather than removing it outright — surviving
    // tiles must not shift up. The dismissed entry is collapsed on
    // thaw.
    const { app } = await mountApp();
    await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    seedPending(appI, "todo/foo.md", undefined, "done");
    appI._handleTodoActionResult("todo/foo.md", true, null, null, null, null);
    seedPending(appI, "todo/bar.md", undefined, "done");
    appI._handleTodoActionResult("todo/bar.md", true, null, null, null, null);
    appI._handleDismissSettled(":todo/foo.md");
    const fooEntry = appI.todoSlotState.get(":todo/foo.md");
    expect(fooEntry).toBeDefined();
    expect(fooEntry!.kind).toBe("dismissed");
    expect(appI.todoSlotState.get(":todo/bar.md")?.kind).toBe("settled");
  });

  it("manual dismiss does not reset the idle timer for surviving tiles (§3.5 rule 2)", async () => {
    // Regression test for round-2 F14: the list-level idle timer
    // must keep its accumulated elapsed time when a user clicks `×`
    // on one tile. An unconditional re-arm in `_collapseSettledSlot`
    // would give the surviving tiles a fresh 5s instead of the
    // remaining ~1s they should have had.
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-22T12:00:00"));
    const { app } = await mountApp();
    await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    // Settle A and B at t=0 — idle timer arms for 5s.
    seedPending(appI, "todo/foo.md", undefined, "done");
    appI._handleTodoActionResult("todo/foo.md", true, null, null, null, null);
    seedPending(appI, "todo/bar.md", undefined, "done");
    appI._handleTodoActionResult("todo/bar.md", true, null, null, null, null);
    // At t=3s, dismiss A manually.
    vi.advanceTimersByTime(3000);
    appI._handleDismissSettled(":todo/foo.md");
    // B should still be present (dismiss only drops one slot).
    expect(appI.todoSlotState.has(":todo/bar.md")).toBe(true);
    // At t=5.1s (2.1s after dismiss), B's original idle timer
    // should fire — not a fresh timer that would keep B alive
    // until t=8s.
    vi.advanceTimersByTime(2200);
    expect(appI.todoSlotState.has(":todo/bar.md")).toBe(false);
    vi.useRealTimers();
  });

  // --- _primeForReplay slot-preservation (§3.4.3) ----------------------

  it("_primeForReplay clears slots + watchdogs + snapshot", async () => {
    // _primeForReplay always clears todo state.
    const { app } = await mountApp();
    const appI = app as unknown as AppInternals;
    seedPending(appI, "todo/foo.md", undefined, "done");
    expect(appI.todoSlotState.size).toBe(1);
    expect(appI.todoFrozenSnapshot).not.toBeNull();
    appI._primeForReplay(42);
    expect(appI.todoSlotState.size).toBe(0);
    expect(appI.todoFrozenSnapshot).toBeNull();
    expect(appI.todoSlotWatchdogs.size).toBe(0);
    expect(appI.todoRefreshQueued).toBe(false);
  });

  // --- Entry-point debounce coverage (Phase 4 §6) -----------------------

  it("debounce blocks a second TodoSnooze within 400ms on the same key", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-22T12:00:00"));
    const { app, ws } = await mountApp();
    const appI = app as unknown as AppInternals;
    seedLiveTasks(appI, [{ path: "todo/foo.md", repo: "life" }]);
    const sentBefore = ws.sent.length;
    appI._handleTodoSnooze("todo/foo.md", "life", "2026-04-22", 1);
    appI._handleTodoSnooze("todo/foo.md", "life", "2026-04-22", 1);
    const sends = ws.sent
      .slice(sentBefore)
      .map((s) => JSON.parse(s))
      .filter((m) => m.type === "TodoSchedule");
    expect(sends).toHaveLength(1);
    vi.useRealTimers();
  });

  it("debounce blocks a second single-row reorder within 400ms on the same key", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-22T12:00:00"));
    const { app, ws } = await mountApp();
    const appI = app as unknown as AppInternals;
    // Seed tasks with `bar` acting as the `after` anchor so
    // `_resolveInsertionIndex` resolves before `_armDebounce` fires.
    appI.todoTasks = [
      { path: "todo/foo.md", repo: "life", effective_date: "2026-04-22", tldr: "f" },
      { path: "todo/bar.md", repo: "life", effective_date: "2026-04-22", tldr: "b" },
    ];
    const sentBefore = ws.sent.length;
    const target = {
      path: "todo/foo.md",
      repo: "life",
      after: { path: "todo/bar.md", repo: "life" },
      before: null,
      targetGroupDate: null,
      selectedKeys: null,
    };
    appI._handleTodoReorder(target);
    appI._handleTodoReorder(target);
    const sends = ws.sent
      .slice(sentBefore)
      .map((s) => JSON.parse(s))
      .filter((m) => m.type === "TodoReorder");
    expect(sends).toHaveLength(1);
    vi.useRealTimers();
  });

  it("debounce blocks a second multi-select reorder within 400ms on the same keys", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-22T12:00:00"));
    const { app, ws } = await mountApp();
    const appI = app as unknown as AppInternals;
    appI.todoTasks = [
      { path: "todo/foo.md", repo: "life", effective_date: "2026-04-22", tldr: "f" },
      { path: "todo/bar.md", repo: "life", effective_date: "2026-04-22", tldr: "b" },
      { path: "todo/baz.md", repo: "life", effective_date: "2026-04-22", tldr: "z" },
    ];
    const sentBefore = ws.sent.length;
    const target = {
      path: "todo/foo.md",
      repo: "life",
      after: { path: "todo/baz.md", repo: "life" },
      before: null,
      targetGroupDate: null,
      selectedKeys: ["life:todo/foo.md", "life:todo/bar.md"],
    };
    appI._handleMultiReorder(target);
    appI._handleMultiReorder(target);
    // Each successful multi-reorder dispatches N (=2) messages; a
    // debounced-short-circuited second call adds nothing.
    const sends = ws.sent
      .slice(sentBefore)
      .map((s) => JSON.parse(s))
      .filter((m) => m.type === "TodoReorder");
    expect(sends).toHaveLength(2);
    vi.useRealTimers();
  });

  it("error branch clears debounce so an immediate retry proceeds", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-22T12:00:00"));
    const { app, ws } = await mountApp();
    const appI = app as unknown as AppInternals;
    // Seed the live task so `_handleTodoDone` can snapshot tldr.
    appI.todoTasks = [
      {
        path: "todo/foo.md",
        repo: "life",
        effective_date: "2026-04-22",
        tldr: "foo",
      },
    ];
    appI._handleTodoDone("todo/foo.md", "life");
    const sentAfterFirst = ws.sent.filter((s) =>
      s.includes('"TodoDone"'),
    ).length;
    expect(sentAfterFirst).toBe(1);
    // Error result arrives — `_clearDebounce` should fire on the error branch.
    // Slot is already pending from the dispatch above; no preseed needed.
    appI._handleTodoActionResult(
      "todo/foo.md",
      false,
      "stale_anchor: …",
      null,
      null,
      null,
    );
    // Immediate retry (still well inside the 400ms window from the
    // first tap) should proceed now that the window is cleared.
    appI._handleTodoDone("todo/foo.md", "life");
    const sentAfterRetry = ws.sent.filter((s) =>
      s.includes('"TodoDone"'),
    ).length;
    expect(sentAfterRetry).toBe(2);
    vi.useRealTimers();
  });

  // --- Row-error badge auto-clear (Phase 4 §7.2) ------------------------

  it("error badge auto-clears after 6s", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-22T12:00:00"));
    const { app } = await mountApp();
    const appI = app as unknown as AppInternals;
    seedPending(appI, "todo/foo.md", undefined, "done");
    appI._handleTodoActionResult(
      "todo/foo.md",
      false,
      "stale_anchor: …",
      null,
      null,
      null,
    );
    expect(appI.todoErrorKeys.size).toBe(1);
    vi.advanceTimersByTime(7000);
    expect(appI.todoErrorKeys.size).toBe(0);
    vi.useRealTimers();
  });

  // --- todoFrozenSnapshot lifecycle (freeze-the-list design) -----------
  //
  // The snapshot is captured at first-pending and cleared on thaw. All
  // three thaw paths below must null it out; mid-session acks must not
  // retake it (L2).

  /** Build a minimal TodoItem for the `todoTasks` seeding below. */
  function mkTask(
    path: string,
    tldr: string,
    effective_date: string,
    sort_order?: number,
    opts?: Partial<TodoItem>,
  ): TodoItem {
    return {
      path,
      tldr,
      effective_date,
      ...(sort_order !== undefined ? { sort_order } : {}),
      ...opts,
    };
  }

  it("L1: snapshot captured on first pending", async () => {
    const { app } = await mountApp();
    const appI = app as unknown as AppInternals;
    appI.todoTodayStr = "2026-04-12";
    const tasks: TodoItem[] = [
      mkTask("a.md", "A", "2026-04-12", 0),
      mkTask("b.md", "B", "2026-04-12", 1),
      mkTask("c.md", "C", "2026-04-12", 2),
    ];
    appI.todoTasks = tasks;
    expect(appI.todoFrozenSnapshot).toBeNull();
    appI._handleTodoDone("b.md", undefined);
    expect(appI.todoFrozenSnapshot).not.toBeNull();
    expect(appI.todoFrozenSnapshot!.todayStr).toBe("2026-04-12");
    // Pre-dispatch tasks shape preserved.
    const flat = appI.todoFrozenSnapshot!.groups.flatMap((g) => g.tasks);
    expect(flat.map((t) => t.path)).toEqual(["a.md", "b.md", "c.md"]);
  });

  it("L2: snapshot NOT retaken on a second pending", async () => {
    const { app } = await mountApp();
    const appI = app as unknown as AppInternals;
    appI.todoTodayStr = "2026-04-12";
    appI.todoTasks = [
      mkTask("a.md", "A", "2026-04-12", 0),
      mkTask("b.md", "B", "2026-04-12", 1),
    ];
    appI._handleTodoDone("a.md", undefined);
    const first = appI.todoFrozenSnapshot;
    expect(first).not.toBeNull();
    appI._handleTodoDone("b.md", undefined);
    // Same object identity — idempotent hook, no retake.
    expect(appI.todoFrozenSnapshot).toBe(first);
  });

  it("L3: snapshot cleared on idle-timer fire", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-22T12:00:00"));
    const { app } = await mountApp();
    await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    seedPending(appI, "todo/foo.md", undefined, "done");
    appI._handleTodoActionResult("todo/foo.md", true, null, null, null, null);
    expect(appI.todoFrozenSnapshot).not.toBeNull();
    vi.advanceTimersByTime(5_100);
    expect(appI.todoFrozenSnapshot).toBeNull();
    vi.useRealTimers();
  });

  it("L4: snapshot cleared on zero-pending-zero-settled collapse (error path)", async () => {
    const { app } = await mountApp();
    const appI = app as unknown as AppInternals;
    seedPending(appI, "todo/foo.md", undefined, "done");
    expect(appI.todoFrozenSnapshot).not.toBeNull();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    appI._handleTodoActionResult(
      "todo/foo.md",
      false,
      "stale_anchor: …",
      null,
      null,
      null,
    );
    warn.mockRestore();
    // Error path collapses the slot → zero pending + zero settled → thaw.
    expect(appI.todoFrozenSnapshot).toBeNull();
  });

  it("L5: refresh-click with no pending thaws via collapse path", async () => {
    const { app, ws } = await mountApp();
    await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    seedPending(appI, "todo/foo.md", undefined, "done");
    appI._handleTodoActionResult("todo/foo.md", true, null, null, null, null);
    expect(appI.todoFrozenSnapshot).not.toBeNull();
    const sentBefore = ws.sent.length;
    appI._handleTodoRefresh();
    // Refresh click drops settled; zero-pending-zero-settled → thaw.
    expect(appI.todoFrozenSnapshot).toBeNull();
    const refreshSends = ws.sent
      .slice(sentBefore)
      .map((s) => JSON.parse(s))
      .filter((m) => m.type === "TodoRefresh");
    expect(refreshSends).toHaveLength(1);
  });

  it("L6: refresh-click with pending queues; snapshot survives until idle fires later", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-22T12:00:00"));
    const { app, ws } = await mountApp();
    await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    seedPending(appI, "todo/foo.md", undefined, "done");
    const sentBefore = ws.sent.length;
    appI._handleTodoRefresh();
    expect(appI.todoRefreshQueued).toBe(true);
    expect(appI.todoFrozenSnapshot).not.toBeNull();
    // Ack pending → settled; queued refresh fires; snapshot still
    // non-null because the settled tile now holds the row position.
    appI._handleTodoActionResult("todo/foo.md", true, null, null, null, null);
    expect(appI.todoFrozenSnapshot).not.toBeNull();
    const refreshSends = ws.sent
      .slice(sentBefore)
      .map((s) => JSON.parse(s))
      .filter((m) => m.type === "TodoRefresh");
    expect(refreshSends).toHaveLength(1);
    // Idle-timer fire (5s later) thaws.
    vi.advanceTimersByTime(5_100);
    expect(appI.todoFrozenSnapshot).toBeNull();
    expect(appI.todoSlotState.has(":todo/foo.md")).toBe(false);
    vi.useRealTimers();
  });

  it("× on last settled tile → dismissed → thaw immediately", async () => {
    // Covers the _handleDismissSettled path explicitly: user dismisses
    // the only settled tile left; no pending / no settled / just a
    // dismissed placeholder → thaw immediately, which drops the
    // dismissed entry too.
    const { app } = await mountApp();
    await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    seedPending(appI, "todo/foo.md", undefined, "done");
    appI._handleTodoActionResult("todo/foo.md", true, null, null, null, null);
    expect(appI.todoFrozenSnapshot).not.toBeNull();
    appI._handleDismissSettled(":todo/foo.md");
    expect(appI.todoFrozenSnapshot).toBeNull();
    expect(appI.todoSlotState.has(":todo/foo.md")).toBe(false);
  });

  it("× on one of two settled tiles leaves a dismissed placeholder; snapshot survives", async () => {
    // Two settled tiles; × on one of them. Snapshot must remain
    // non-null (surviving tile still holds a position) and the ×'d
    // slot transitions to dismissed (not removed outright).
    const { app } = await mountApp();
    await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    seedPending(appI, "todo/foo.md", undefined, "done");
    appI._handleTodoActionResult("todo/foo.md", true, null, null, null, null);
    seedPending(appI, "todo/bar.md", undefined, "done");
    appI._handleTodoActionResult("todo/bar.md", true, null, null, null, null);
    appI._handleDismissSettled(":todo/foo.md");
    const entry = appI.todoSlotState.get(":todo/foo.md");
    expect(entry).toBeDefined();
    expect(entry!.kind).toBe("dismissed");
    expect(appI.todoSlotState.get(":todo/bar.md")?.kind).toBe("settled");
    expect(appI.todoFrozenSnapshot).not.toBeNull();
  });

  it("_freezeListIfNeeded throws on empty todoTasks (invariant guard)", async () => {
    const { app } = await mountApp();
    const appI = app as unknown as AppInternals;
    appI.todoTasks = [];
    expect(() => appI._freezeListIfNeeded()).toThrow(
      /refusing to snapshot empty/i,
    );
  });

  // F1: pin the reorder freeze placement. The design's call-site
  // contract requires `_freezeListIfNeeded()` to run BEFORE the
  // optimistic splice mutates `todoTasks`. A regression that moves
  // the freeze call below the splice (or removes it and relies on
  // `_setSlotState`'s call) would silently snapshot the post-splice
  // list. These tests fail in that case.

  it("reorder: snapshot reflects pre-splice order, not optimistic post-splice", async () => {
    const { app } = await mountApp();
    const appI = app as unknown as AppInternals;
    appI.todoTodayStr = "2026-04-12";
    const tasks: TodoItem[] = [
      mkTask("a.md", "A", "2026-04-12", 0),
      mkTask("b.md", "B", "2026-04-12", 1),
      mkTask("c.md", "C", "2026-04-12", 2),
    ];
    appI.todoTasks = tasks;
    // Drag B to the end (after C).
    appI._handleTodoReorder({
      path: "b.md",
      repo: undefined,
      after: { path: "c.md", repo: undefined },
      before: null,
      targetGroupDate: null,
      selectedKeys: null,
    });
    // Optimistic splice landed on todoTasks: A C B.
    expect(appI.todoTasks.map((t) => t.path)).toEqual(["a.md", "c.md", "b.md"]);
    // Snapshot reflects pre-splice order: A B C.
    expect(appI.todoFrozenSnapshot).not.toBeNull();
    const snap = appI.todoFrozenSnapshot!.groups.flatMap((g) => g.tasks);
    expect(snap.map((t) => t.path)).toEqual(["a.md", "b.md", "c.md"]);
  });

  it("multi-reorder: snapshot reflects pre-splice order", async () => {
    const { app } = await mountApp();
    const appI = app as unknown as AppInternals;
    appI.todoTodayStr = "2026-04-12";
    const tasks: TodoItem[] = [
      mkTask("a.md", "A", "2026-04-12", 0),
      mkTask("b.md", "B", "2026-04-12", 1),
      mkTask("c.md", "C", "2026-04-12", 2),
      mkTask("d.md", "D", "2026-04-12", 3),
      mkTask("e.md", "E", "2026-04-12", 4),
    ];
    appI.todoTasks = tasks;
    // Drag B and D after E.
    appI._handleMultiReorder({
      path: "b.md",
      repo: undefined,
      after: { path: "e.md", repo: undefined },
      before: null,
      targetGroupDate: null,
      selectedKeys: [":b.md", ":d.md"],
    });
    // Optimistic splice: A C E B D.
    expect(appI.todoTasks.map((t) => t.path)).toEqual([
      "a.md", "c.md", "e.md", "b.md", "d.md",
    ]);
    // Snapshot is pre-splice: A B C D E.
    expect(appI.todoFrozenSnapshot).not.toBeNull();
    const snap = appI.todoFrozenSnapshot!.groups.flatMap((g) => g.tasks);
    expect(snap.map((t) => t.path)).toEqual([
      "a.md", "b.md", "c.md", "d.md", "e.md",
    ]);
  });

  // Snap-back: on reorder !success, todoTasks and selectedTasks revert to
  // the pre-splice snapshot captured before the optimistic update.

  it("single-reorder snap-back: !success reverts todoTasks to pre-splice order", async () => {
    const { app } = await mountApp();
    const appI = app as unknown as AppInternals;
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    appI.todoTodayStr = "2026-04-12";
    const tasks: TodoItem[] = [
      mkTask("a.md", "A", "2026-04-12", 0),
      mkTask("b.md", "B", "2026-04-12", 1),
      mkTask("c.md", "C", "2026-04-12", 2),
    ];
    appI.todoTasks = tasks;
    // Drag B to the end.
    appI._handleTodoReorder({
      path: "b.md",
      repo: undefined,
      after: { path: "c.md", repo: undefined },
      before: null,
      targetGroupDate: null,
      selectedKeys: null,
    });
    // Optimistic order: A C B.
    expect(appI.todoTasks.map((t) => t.path)).toEqual(["a.md", "c.md", "b.md"]);
    // Error ack — snap-back should revert to A B C.
    appI._handleTodoActionResult("b.md", false, "stale_anchor", null, null, null);
    expect(appI.todoTasks.map((t) => t.path)).toEqual(["a.md", "b.md", "c.md"]);
    // Snapshot entry consumed (deleted).
    expect(appI.todoReorderSnapshots.size).toBe(0);
    warn.mockRestore();
  });

  it("multi-reorder snap-back: !success on non-primary task reverts todoTasks", async () => {
    const { app } = await mountApp();
    const appI = app as unknown as AppInternals;
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    appI.todoTodayStr = "2026-04-12";
    const tasks: TodoItem[] = [
      mkTask("a.md", "A", "2026-04-12", 0),
      mkTask("b.md", "B", "2026-04-12", 1),
      mkTask("c.md", "C", "2026-04-12", 2),
      mkTask("d.md", "D", "2026-04-12", 3),
    ];
    appI.todoTasks = tasks;
    // Drag B and D after C as a multi-select (primary=b, selected=[b,d]).
    appI._handleMultiReorder({
      path: "b.md",
      repo: undefined,
      after: { path: "c.md", repo: undefined },
      before: null,
      targetGroupDate: null,
      selectedKeys: [":b.md", ":d.md"],
    });
    // Optimistic order: A C B D.
    expect(appI.todoTasks.map((t) => t.path)).toEqual(["a.md", "c.md", "b.md", "d.md"]);
    // Error ack for the non-primary task (d.md) — snap-back should revert.
    appI._handleTodoActionResult("d.md", false, "stale_anchor", null, null, null);
    expect(appI.todoTasks.map((t) => t.path)).toEqual(["a.md", "b.md", "c.md", "d.md"]);
    warn.mockRestore();
  });

  // F5: out-of-order dispatch with interleaved acks — the snapshot
  // must survive every ack (settled doesn't retake the snapshot).
  // Pins F10's "snapshot survives across acks" semantic.

  it("out-of-order dispatch + acks: snapshot identity preserved across all acks", async () => {
    const { app } = await mountApp();
    await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    appI.todoTodayStr = "2026-04-12";
    const tasks: TodoItem[] = [
      mkTask("a.md", "A", "2026-04-12", 0),
      mkTask("b.md", "B", "2026-04-12", 1),
      mkTask("c.md", "C", "2026-04-12", 2),
      mkTask("d.md", "D", "2026-04-12", 3),
      mkTask("e.md", "E", "2026-04-12", 4),
    ];
    appI.todoTasks = tasks;
    // Dispatch D first, then B, then E.
    appI._handleTodoDone("d.md", undefined);
    const snap = appI.todoFrozenSnapshot;
    expect(snap).not.toBeNull();
    appI._handleTodoActionResult("d.md", true, null, null, null, null);
    expect(appI.todoFrozenSnapshot).toBe(snap);
    appI._handleTodoDone("b.md", undefined);
    expect(appI.todoFrozenSnapshot).toBe(snap);
    appI._handleTodoActionResult("b.md", true, null, null, null, null);
    expect(appI.todoFrozenSnapshot).toBe(snap);
    appI._handleTodoDone("e.md", undefined);
    expect(appI.todoFrozenSnapshot).toBe(snap);
    appI._handleTodoActionResult("e.md", true, null, null, null, null);
    expect(appI.todoFrozenSnapshot).toBe(snap);
    // Snapshot still contains the original 5-task order.
    const snapPaths = snap!.groups.flatMap((g) => g.tasks).map((t) => t.path);
    expect(snapPaths).toEqual(["a.md", "b.md", "c.md", "d.md", "e.md"]);
  });

  // F6: round-2 added a `console.warn` for the case where
  // `_handleDismissSettled` runs against a non-settled entry. Pin
  // both branches (warn-on-wrong-kind, silent-on-missing).

  it("_handleDismissSettled warns when slot is pending (wrong kind)", async () => {
    const { app } = await mountApp();
    const appI = app as unknown as AppInternals;
    seedPending(appI, "todo/foo.md", undefined, "done");
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      appI._handleDismissSettled(":todo/foo.md");
      const matching = warn.mock.calls.filter(
        (c) => typeof c[0] === "string" && /expected settled/.test(c[0]),
      );
      expect(matching.length).toBeGreaterThanOrEqual(1);
      // Slot was NOT mutated.
      expect(appI.todoSlotState.get(":todo/foo.md")?.kind).toBe("pending");
    } finally {
      warn.mockRestore();
    }
  });

  it("_handleDismissSettled silent on missing entry (benign UI race)", async () => {
    const { app } = await mountApp();
    const appI = app as unknown as AppInternals;
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      appI._handleDismissSettled(":todo/never-existed.md");
      const matching = warn.mock.calls.filter(
        (c) => typeof c[0] === "string" && /_handleDismissSettled/.test(c[0]),
      );
      expect(matching.length).toBe(0);
    } finally {
      warn.mockRestore();
    }
  });

  // F2: end-to-end integration of the mandatory regression. Drives
  // <brenn-app> through SetLayout + TodoState, fires Done on rows
  // 2/4/6 of a 7-row Today section, simulates the acks, and asserts
  // the rendered DOM (not just `slotState`) shows tombstones in-place
  // at indices 1/3/5. Catches a regression where the
  // `.frozenSnapshot=${this.todoFrozenSnapshot}` binding at
  // `app.ts:2564` is dropped — the renderer test (todo-list.test.ts F1)
  // and the lifecycle tests (L1) both still pass in that case, but
  // the user-visible behavior breaks.

  it("F2 end-to-end: 7 tasks, Done on rows 2/4/6 → tombstones in-place at indices 1/3/5", async () => {
    const { app } = await mountApp();
    const appI = app as unknown as AppInternals;
    // 1. SetLayout to enable rendering.
    appI.handleMessage({ type: "SetLayout", layout: { type: "TwoColumn" } });
    await app.updateComplete;
    // 2. TodoState seeds 7 tasks in Today and flips hasTodoList.
    appI.handleMessage({
      type: "TodoState",
      tasks: [
        { path: "a.md", repo: null, tldr: "A", effective_date: "2026-04-12", sort_order: 0 },
        { path: "b.md", repo: null, tldr: "B", effective_date: "2026-04-12", sort_order: 1 },
        { path: "c.md", repo: null, tldr: "C", effective_date: "2026-04-12", sort_order: 2 },
        { path: "d.md", repo: null, tldr: "D", effective_date: "2026-04-12", sort_order: 3 },
        { path: "e.md", repo: null, tldr: "E", effective_date: "2026-04-12", sort_order: 4 },
        { path: "f.md", repo: null, tldr: "F", effective_date: "2026-04-12", sort_order: 5 },
        { path: "g.md", repo: null, tldr: "G", effective_date: "2026-04-12", sort_order: 6 },
      ],
      today: "2026-04-12",
    });
    await app.updateComplete;
    // 3. Click Done on B / D / F (rows at positions 2 / 4 / 6).
    appI._handleTodoDone("b.md", undefined);
    appI._handleTodoActionResult("b.md", true, null, null, null, null);
    appI._handleTodoDone("d.md", undefined);
    appI._handleTodoActionResult("d.md", true, null, null, null, null);
    appI._handleTodoDone("f.md", undefined);
    appI._handleTodoActionResult("f.md", true, null, null, null, null);
    await app.updateComplete;
    // 4. Inspect the actual rendered DOM in the brenn-todo-list shadow.
    const todoList = app.querySelector("brenn-todo-list") as HTMLElement | null;
    expect(todoList).not.toBeNull();
    const rows = Array.from(
      todoList!.shadowRoot!.querySelectorAll(".task-row"),
    );
    expect(rows).toHaveLength(7);
    const keys = rows.map((r) => r.getAttribute("data-task-key"));
    expect(keys).toEqual([
      ":a.md", ":b.md", ":c.md", ":d.md", ":e.md", ":f.md", ":g.md",
    ]);
    // 5. B (index 1), D (index 3), F (index 5) carry the settled class.
    expect(rows[0].classList.contains("settled")).toBe(false);
    expect(rows[1].classList.contains("settled")).toBe(true);
    expect(rows[2].classList.contains("settled")).toBe(false);
    expect(rows[3].classList.contains("settled")).toBe(true);
    expect(rows[4].classList.contains("settled")).toBe(false);
    expect(rows[5].classList.contains("settled")).toBe(true);
    expect(rows[6].classList.contains("settled")).toBe(false);
  });

  // --- Heading-drop schedule (empty-day-buckets-heading-drop §5) ------

  it("debounce blocks a second TodoSchedule within 400ms on the same key", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-22T12:00:00"));
    const { app, ws } = await mountApp();
    const appI = app as unknown as AppInternals;
    seedLiveTasks(appI, [{ path: "todo/foo.md", repo: "life" }]);
    const sentBefore = ws.sent.length;
    appI._handleTodoSchedule({
      path: "todo/foo.md",
      repo: "life",
      date: "2026-04-25",
      selectedKeys: null,
    });
    appI._handleTodoSchedule({
      path: "todo/foo.md",
      repo: "life",
      date: "2026-04-25",
      selectedKeys: null,
    });
    const sends = ws.sent
      .slice(sentBefore)
      .map((s) => JSON.parse(s))
      .filter((m) => m.type === "TodoSchedule");
    expect(sends).toHaveLength(1);
    vi.useRealTimers();
  });

  it("schedule short-circuits while todoRefreshQueued is true", async () => {
    const { app, ws } = await mountApp();
    await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    seedPending(appI, "todo/bar.md", undefined, "snooze", "2026-04-29");
    seedLiveTasks(appI, [
      { path: "todo/bar.md" },
      { path: "todo/foo.md", repo: "life" },
    ]);
    appI._handleTodoRefresh();
    expect(appI.todoRefreshQueued).toBe(true);
    const sentBefore = ws.sent.length;
    appI._handleTodoSchedule({
      path: "todo/foo.md",
      repo: "life",
      date: "2026-04-25",
      selectedKeys: null,
    });
    const sends = ws.sent
      .slice(sentBefore)
      .map((s) => JSON.parse(s))
      .filter((m) => m.type === "TodoSchedule");
    expect(sends).toHaveLength(0);
  });

  it("settlement: schedule success transitions slot to settled with 'Scheduled for ...'", async () => {
    const { app } = await mountApp();
    await toastInterceptor(app);
    const appI = app as unknown as AppInternals;
    seedLiveTasks(appI, [{ path: "todo/foo.md", repo: "life" }]);
    appI._handleTodoSchedule({
      path: "todo/foo.md",
      repo: "life",
      date: "2026-04-25",
      selectedKeys: null,
    });
    appI._handleTodoActionResult(
      "todo/foo.md",
      true,
      null,
      null,
      null,
      false,
    );
    const entry = appI.todoSlotState.get("life:todo/foo.md");
    expect(entry).toBeDefined();
    if (entry && entry.kind === "settled") {
      expect(entry.action).toBe("schedule");
      expect(entry.tileText).toMatch(/^Scheduled for /);
    } else {
      throw new Error("expected settled entry");
    }
  });

  it("reconcile-loop settlement: TodoState refresh dropping a scheduled task settles 'Scheduled for ...'", async () => {
    const { app } = await mountApp();
    const appI = app as unknown as AppInternals;
    seedPending(
      appI,
      "todo/foo.md",
      undefined,
      "schedule",
      "2026-04-25",
    );
    // foo.md absent from refresh.
    appI._handleTodoState([], "2026-04-22");
    const entry = appI.todoSlotState.get(":todo/foo.md");
    expect(entry).toBeDefined();
    if (entry && entry.kind === "settled") {
      expect(entry.action).toBe("schedule");
      expect(entry.tileText).toMatch(/^Scheduled for /);
    } else {
      throw new Error("expected settled entry");
    }
  });

  it("multi-select schedule: dispatches one TodoSchedule per selected task with the same date", async () => {
    // F6: also asserts `(path, repo)` pairs land on the wire — pins
    // that `_sendTodoSchedule` carries each task's identity, not just
    // the primary's, and that all 3 messages share the target date.
    const { app, ws } = await mountApp();
    const appI = app as unknown as AppInternals;
    appI.todoTasks = [
      {
        path: "todo/foo.md",
        repo: "life",
        effective_date: "2026-04-22",
        tldr: "f",
      },
      {
        path: "todo/bar.md",
        repo: "life",
        effective_date: "2026-04-22",
        tldr: "b",
      },
      {
        path: "todo/baz.md",
        repo: "life",
        effective_date: "2026-04-22",
        tldr: "z",
      },
    ];
    const sentBefore = ws.sent.length;
    appI._handleTodoSchedule({
      path: "todo/foo.md",
      repo: "life",
      date: "2026-04-25",
      selectedKeys: ["life:todo/foo.md", "life:todo/bar.md", "life:todo/baz.md"],
    });
    const sends = ws.sent
      .slice(sentBefore)
      .map((s) => JSON.parse(s))
      .filter((m) => m.type === "TodoSchedule");
    expect(sends).toHaveLength(3);
    const idents = sends.map((m) => ({
      path: m.path,
      repo: m.repo,
      date: m.date,
    }));
    expect(idents).toEqual([
      { path: "todo/foo.md", repo: "life", date: "2026-04-25" },
      { path: "todo/bar.md", repo: "life", date: "2026-04-25" },
      { path: "todo/baz.md", repo: "life", date: "2026-04-25" },
    ]);
  });

  // F6 (continued): single-task schedule with `repo: undefined` at the
  // call site sends `repo: null` on the wire — pins the
  // `repo ?? null` normalization in `_sendTodoSchedule`.
  it("single-task schedule normalizes repo: undefined → null on the wire", async () => {
    const { app, ws } = await mountApp();
    const appI = app as unknown as AppInternals;
    appI.todoTasks = [
      {
        path: "todo/foo.md",
        repo: null,
        effective_date: "2026-04-22",
        tldr: "f",
      },
    ];
    const sentBefore = ws.sent.length;
    appI._handleTodoSchedule({
      path: "todo/foo.md",
      // repo intentionally omitted (undefined).
      date: "2026-04-25",
      selectedKeys: null,
    });
    const sends = ws.sent
      .slice(sentBefore)
      .map((s) => JSON.parse(s))
      .filter((m) => m.type === "TodoSchedule");
    expect(sends).toHaveLength(1);
    expect(sends[0].path).toBe("todo/foo.md");
    expect(sends[0].repo).toBeNull();
    expect(sends[0].date).toBe("2026-04-25");
  });

  // F7: schedule does NOT optimistically mutate `todoTasks[i].effective_date`.
  // Design §5 explicitly pins this contract ("No optimistic
  // `effective_date` mutation … mirrors `_handleTodoSnooze`"). The
  // renderer reads from the frozen snapshot during the triage
  // session, so any local mutation would be invisible AND drift
  // would surface on thaw. Snooze deliberately skips this; schedule
  // must too.
  it("schedule does not optimistically mutate todoTasks[i].effective_date (§5)", async () => {
    const { app } = await mountApp();
    const appI = app as unknown as AppInternals;
    appI.todoTasks = [
      {
        path: "todo/foo.md",
        repo: "life",
        effective_date: "2026-04-22",
        tldr: "f",
      },
    ];
    const beforeDate = appI.todoTasks[0].effective_date;
    appI._handleTodoSchedule({
      path: "todo/foo.md",
      repo: "life",
      date: "2026-04-25",
      selectedKeys: null,
    });
    expect(appI.todoTasks[0].effective_date).toBe(beforeDate);
    expect(appI.todoTasks[0].effective_date).toBe("2026-04-22");
  });

  // F9: mixed-source multi-select. 2 of 3 tasks already on the
  // target date; the third is not. Per design §4.6 the brenn-app
  // handler dispatches all 3 messages — graf is idempotent on
  // same-date and per-task no-op filtering at the brenn layer is
  // unnecessary. A future "optimization" that filters per-task
  // would fail here.
  it("mixed-source multi-select schedule: 2 already-on-target + 1 not = 3 wire messages (§4.6)", async () => {
    const { app, ws } = await mountApp();
    const appI = app as unknown as AppInternals;
    appI.todoTasks = [
      {
        path: "todo/foo.md",
        repo: "life",
        effective_date: "2026-04-25", // already on target
        tldr: "f",
      },
      {
        path: "todo/bar.md",
        repo: "life",
        effective_date: "2026-04-25", // already on target
        tldr: "b",
      },
      {
        path: "todo/baz.md",
        repo: "life",
        effective_date: "2026-04-22", // not on target
        tldr: "z",
      },
    ];
    const sentBefore = ws.sent.length;
    appI._handleTodoSchedule({
      path: "todo/baz.md",
      repo: "life",
      date: "2026-04-25",
      selectedKeys: ["life:todo/foo.md", "life:todo/bar.md", "life:todo/baz.md"],
    });
    const sends = ws.sent
      .slice(sentBefore)
      .map((s) => JSON.parse(s))
      .filter((m) => m.type === "TodoSchedule");
    expect(sends).toHaveLength(3);
    for (const m of sends) {
      expect(m.date).toBe("2026-04-25");
    }
  });

  // F10: schedule freezes the list before marking pending. Design §5
  // calls out "Freeze BEFORE marking pending" as the freeze-invariant
  // contract; existing reorder/snooze tests already pin this.
  it("schedule freezes the list before marking pending (§5)", async () => {
    const { app } = await mountApp();
    const appI = app as unknown as AppInternals;
    appI.todoTasks = [
      {
        path: "todo/foo.md",
        repo: "life",
        effective_date: "2026-04-22",
        tldr: "f",
      },
    ];
    expect(appI.todoFrozenSnapshot).toBeNull();
    appI._handleTodoSchedule({
      path: "todo/foo.md",
      repo: "life",
      date: "2026-04-25",
      selectedKeys: null,
    });
    expect(appI.todoFrozenSnapshot).not.toBeNull();
    // Pending slot exists — confirms freeze ran BEFORE mark-pending
    // (the freeze is no-op if already snapshot, so the order matters
    // only on first-pending; this is first-pending).
    expect(appI.todoSlotState.get("life:todo/foo.md")?.kind).toBe("pending");
  });

  // --- exact-key (repo != null) branch -----------------------------------

  it("exact-key match transitions only the matching repo slot", async () => {
    // Two pending slots for the same path but different repos. Firing
    // _handleTodoActionResult with repo="life" must transition only the
    // "life" slot; the "work" slot stays pending.
    const { app } = await mountApp();
    await toastInterceptor(app);
    const appI = app as unknown as AppInternals;

    // Seed two pending done slots: same path, different repos.
    seedPending(appI, "todo/shared.md", "life", "done");
    // seedPending uses app.todoTasks[0] as the freeze anchor; inject a
    // second task entry so the "work" slot gets a live counterpart.
    appI.todoTasks.push({
      path: "todo/shared.md",
      repo: "work",
      effective_date: "2026-04-22",
      tldr: "seed fixture work",
    });
    appI.todoSlotState.set("work:todo/shared.md", {
      kind: "pending",
      action: "done",
      startedAt: Date.now(),
      path: "todo/shared.md",
      repo: "work",
      taskTldr: "seed fixture work",
    });

    // Fire result for "life" repo only.
    appI._handleTodoActionResult(
      "todo/shared.md",
      true,
      null,
      "2026-04-29",
      null,
      false,
      "life",
    );

    // "life" slot must have transitioned (settled or absent).
    const lifeEntry = appI.todoSlotState.get("life:todo/shared.md");
    expect(lifeEntry?.kind).not.toBe("pending");

    // "work" slot must still be pending — exact-key match must not bleed.
    const workEntry = appI.todoSlotState.get("work:todo/shared.md");
    expect(workEntry).toBeDefined();
    expect(workEntry!.kind).toBe("pending");
  });

  it("suffix-scan fallback fires when repo is null", async () => {
    // When the backend sends repo=null (single-repo config), the
    // suffix-scan path must still resolve the slot and transition it.
    const { app } = await mountApp();
    await toastInterceptor(app);
    const appI = app as unknown as AppInternals;

    // Seed a pending slot keyed as single-repo (repo undefined → "").
    seedPending(appI, "todo/nullrepo.md", undefined, "done");

    appI._handleTodoActionResult(
      "todo/nullrepo.md",
      true,
      null,
      "2026-04-29",
      null,
      false,
      null, // repo == null → suffix-scan path
    );

    const entry = appI.todoSlotState.get(":todo/nullrepo.md");
    expect(entry?.kind).not.toBe("pending");
  });
});
