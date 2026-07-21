// @vitest-environment happy-dom
import { describe, it, expect, afterEach, vi } from "vitest";
import {
  todoKey,
  groupTasksByDate,
  buildSlotGroups,
  isDroppable,
  hitTestDrop,
  BrennTodoList,
  type TodoPendingAction,
  type ScheduleTarget,
  type SlotState,
  type TaskGroup,
  type HitTestRow,
} from "./todo-list.js";
import { localTodayStr } from "../date-util.js";
import type { TodoItem } from "../generated/TodoItem.js";

/** Narrow accessor for the private `_pendingLabel` dispatch. */
interface TodoListInternals {
  _pendingLabel(key: string): string;
}

/** Build a minimal TodoItem with just the fields we need.
 *
 * `effective_date` is required by graf's contract — every TodoItem has a
 * concrete date (null-date rows get partitioned into `lint_errors` at the
 * graf layer, per `docs/designs/todo-section-order-and-dated-invariant.md`). */
function task(
  path: string,
  tldr: string,
  effective_date: string,
  opts?: Partial<TodoItem>,
): TodoItem {
  return { path, tldr, effective_date, ...opts };
}

describe("todoKey", () => {
  it("produces repo:path when repo is provided", () => {
    expect(todoKey("todo/dentist.md", "life")).toBe("life:todo/dentist.md");
  });

  it("produces :path when repo is null", () => {
    expect(todoKey("todo/dentist.md", null)).toBe(":todo/dentist.md");
  });

  it("produces :path when repo is undefined", () => {
    expect(todoKey("todo/dentist.md")).toBe(":todo/dentist.md");
  });

  it("distinguishes same path in different repos", () => {
    const a = todoKey("todo/review.md", "life");
    const b = todoKey("todo/review.md", "eng");
    expect(a).not.toBe(b);
  });
});

describe("localTodayStr", () => {
  it("returns a YYYY-MM-DD string", () => {
    const result = localTodayStr();
    expect(result).toMatch(/^\d{4}-\d{2}-\d{2}$/);
  });
});

describe("groupTasksByDate", () => {
  // Use a fixed "today" for all tests: 2026-04-12 (a Sunday).
  const today = "2026-04-12";

  /** Drop the always-emitted empty TODAY/TOMORROW/WEEKDAY placeholders
   *  (design.md §3 — the 7-day window is seeded regardless of which
   *  days are populated). Most pre-existing tests reason only about
   *  populated buckets; this filter restores that view without losing
   *  the empty-bucket behavior coverage in the dedicated tests below. */
  function nonEmpty(groups: TaskGroup[]): TaskGroup[] {
    return groups.filter((g) => g.tasks.length > 0);
  }

  it("groups tasks with past due_date as Overdue", () => {
    const tasks = [
      task("a.md", "Task A", "2026-04-10", { due_date: "2026-04-10" }),
      task("b.md", "Task B", "2026-04-11", { due_date: "2026-04-11" }),
    ];
    const groups = nonEmpty(groupTasksByDate(tasks, today));

    expect(groups).toHaveLength(1);
    expect(groups[0].header).toBe("Overdue");
    expect(groups[0].headerClass).toBe("overdue");
    expect(groups[0].tasks).toHaveLength(2);
  });

  it("puts tasks with past effective_date but no due_date in Earlier", () => {
    const tasks = [
      task("a.md", "Task A", "2026-04-10"),
      task("b.md", "Task B", "2026-04-11"),
    ];
    const groups = nonEmpty(groupTasksByDate(tasks, today));

    expect(groups).toHaveLength(1);
    expect(groups[0].header).toBe("Pulled forward");
    expect(groups[0].headerClass).toBe("earlier");
    expect(groups[0].tasks).toHaveLength(2);
  });

  it("identifies today's tasks", () => {
    const tasks = [task("a.md", "Task A", "2026-04-12")];
    const groups = nonEmpty(groupTasksByDate(tasks, today));

    expect(groups).toHaveLength(1);
    expect(groups[0].header).toBe("Today");
    expect(groups[0].headerClass).toBe("today");
  });

  it("identifies tomorrow's tasks", () => {
    const tasks = [task("a.md", "Task A", "2026-04-13")];
    const groups = nonEmpty(groupTasksByDate(tasks, today));

    expect(groups).toHaveLength(1);
    expect(groups[0].header).toBe("Tomorrow");
    expect(groups[0].headerClass).toBe("tomorrow");
  });

  it("uses weekday names for days 2-6 ahead", () => {
    const tasks = [
      task("a.md", "Task A", "2026-04-14"), // Monday (2 days ahead)
      task("b.md", "Task B", "2026-04-17"), // Thursday (5 days ahead)
    ];
    const groups = nonEmpty(groupTasksByDate(tasks, today));

    expect(groups).toHaveLength(2);
    expect(groups[0].headerClass).toBe("weekday");
    expect(groups[1].headerClass).toBe("weekday");
    // Don't assert exact weekday names since locale may vary, but
    // verify they're strings (not "Overdue", "Today", etc.)
    expect(groups[0].header).not.toBe("Today");
    expect(groups[0].header).not.toBe("Tomorrow");
  });

  it("uses short date format for tasks 7+ days out", () => {
    const tasks = [task("a.md", "Task A", "2026-04-19")]; // 7 days ahead
    const groups = nonEmpty(groupTasksByDate(tasks, today));

    expect(groups).toHaveLength(1);
    expect(groups[0].headerClass).toBe("future");
    // Locale-dependent format, just check it's not a relative label
    expect(groups[0].header).not.toBe("Today");
    expect(groups[0].header).not.toBe("Tomorrow");
    expect(groups[0].header).not.toBe("Overdue");
  });

  it("emits sections in fixed enum order", () => {
    const tasks = [
      task("a.md", "Overdue", "2026-04-10", { due_date: "2026-04-10" }),
      task("b.md", "Earlier", "2026-04-11"),
      task("c.md", "Due today", "2026-04-12", { due_date: "2026-04-12" }),
      task("d.md", "Today", "2026-04-12"),
      task("e.md", "Tomorrow", "2026-04-13"),
      task("f.md", "Future", "2026-04-25"),
    ];
    const groups = groupTasksByDate(tasks, today);
    const headerClasses = groups.map((g) => g.headerClass);

    expect(headerClasses[0]).toBe("overdue");
    expect(headerClasses[1]).toBe("due-today");
    expect(headerClasses[2]).toBe("earlier");
    expect(headerClasses[3]).toBe("today");
    expect(headerClasses[4]).toBe("tomorrow");
    expect(headerClasses[headerClasses.length - 1]).toBe("future");
  });

  it("OVERDUE renders first even when input puts it last", () => {
    const tasks = [
      task("a.md", "Past check-in 1", "2025-06-01"),
      task("b.md", "Past check-in 2", "2025-07-01"),
      task("c.md", "Past check-in 3", "2025-08-01"),
      task("d.md", "Overdue late", "2025-12-01", { due_date: "2025-12-01" }),
      task("e.md", "Overdue later", "2026-02-01", { due_date: "2026-02-01" }),
    ];
    const groups = groupTasksByDate(tasks, today);
    const headerClasses = groups.map((g) => g.headerClass);

    expect(headerClasses[0]).toBe("overdue");
    expect(headerClasses[1]).toBe("earlier");
  });

  it("returns the 7-day always-visible window for no tasks", () => {
    // Empty input now seeds today + next 6 days as empty placeholders
    // (design.md §3). All 7 are date-section buckets (TODAY, TOMORROW,
    // 5 WEEKDAYs). OVERDUE / DUE_TODAY / EARLIER / FUTURE are NOT
    // forced and absent here.
    const groups = groupTasksByDate([], today);
    expect(groups).toHaveLength(7);
    for (const g of groups) {
      expect(g.tasks).toHaveLength(0);
      expect(g.canonicalDate).not.toBeNull();
    }
    expect(groups[0].headerClass).toBe("today");
    expect(groups[1].headerClass).toBe("tomorrow");
    for (let i = 2; i < 7; i++) {
      expect(groups[i].headerClass).toBe("weekday");
    }
  });

  it("merges multiple past due_date tasks into one Overdue group", () => {
    const tasks = [
      task("a.md", "A", "2026-04-01", { due_date: "2026-04-01" }),
      task("b.md", "B", "2026-04-05", { due_date: "2026-04-05" }),
      task("c.md", "C", "2026-04-11", { due_date: "2026-04-11" }),
    ];
    const groups = nonEmpty(groupTasksByDate(tasks, today));

    expect(groups).toHaveLength(1);
    expect(groups[0].header).toBe("Overdue");
    expect(groups[0].tasks).toHaveLength(3);
  });

  it("merges multiple past tentative dates into Earlier", () => {
    const tasks = [
      task("a.md", "A", "2026-04-01"),
      task("b.md", "B", "2026-04-05"),
      task("c.md", "C", "2026-04-11"),
    ];
    const groups = nonEmpty(groupTasksByDate(tasks, today));

    expect(groups).toHaveLength(1);
    expect(groups[0].header).toBe("Pulled forward");
    expect(groups[0].headerClass).toBe("earlier");
    expect(groups[0].tasks).toHaveLength(3);
  });

  it("separates different weekdays into distinct groups", () => {
    const tasks = [
      task("a.md", "Monday", "2026-04-14"),
      task("b.md", "Wednesday", "2026-04-16"),
    ];
    const groups = nonEmpty(groupTasksByDate(tasks, today));

    expect(groups).toHaveLength(2);
    expect(groups[0].headerClass).toBe("weekday");
    expect(groups[1].headerClass).toBe("weekday");
    expect(groups[0].header).not.toBe(groups[1].header);
  });

  it("separates different future dates into distinct groups", () => {
    const tasks = [
      task("a.md", "Later", "2026-04-25"),
      task("b.md", "Much later", "2026-05-10"),
    ];
    const groups = nonEmpty(groupTasksByDate(tasks, today));

    expect(groups).toHaveLength(2);
    expect(groups[0].headerClass).toBe("future");
    expect(groups[1].headerClass).toBe("future");
  });

  it("handles tasks with all date categories in sorted input", () => {
    const tasks = [
      task("a.md", "Past due", "2026-03-01", { due_date: "2026-03-01" }),
      task("b.md", "Way past tentative", "2026-03-15"),
      task("c.md", "Yesterday", "2026-04-11"),
      task("d.md", "Due today", "2026-04-12", { due_date: "2026-04-12" }),
      task("e.md", "Today task", "2026-04-12"),
      task("f.md", "Tomorrow task", "2026-04-13"),
      task("g.md", "This week", "2026-04-15"),
      task("h.md", "Next week", "2026-04-20"),
    ];
    const groups = groupTasksByDate(tasks, today);

    expect(groups.length).toBeGreaterThanOrEqual(6);
    expect(groups[0].header).toBe("Overdue");
    expect(groups[0].tasks).toHaveLength(1);
    expect(groups[1].header).toBe("Due today");
    expect(groups[1].tasks).toHaveLength(1);
    expect(groups[2].header).toBe("Pulled forward");
    expect(groups[2].tasks).toHaveLength(2);
    expect(groups[3].header).toBe("Today");
    expect(groups[3].tasks).toHaveLength(1);
    expect(groups[4].header).toBe("Tomorrow");
  });

  it("due_date equal to today is Due today, not Overdue", () => {
    const tasks = [
      task("a.md", "Due today", "2026-04-12", { due_date: "2026-04-12" }),
    ];
    const groups = nonEmpty(groupTasksByDate(tasks, today));

    expect(groups).toHaveLength(1);
    expect(groups[0].header).toBe("Due today");
    expect(groups[0].headerClass).toBe("due-today");
  });

  it("past effective_date with future due_date is Earlier, not Overdue", () => {
    const tasks = [
      task("a.md", "Rescheduled", "2026-04-08", { due_date: "2026-04-20" }),
    ];
    const groups = nonEmpty(groupTasksByDate(tasks, today));

    expect(groups).toHaveLength(1);
    expect(groups[0].header).toBe("Pulled forward");
  });

  it("future effective_date with past due_date is Overdue", () => {
    const tasks = [
      task("a.md", "Snoozed past deadline", "2026-05-01", {
        due_date: "2026-04-01",
      }),
    ];
    const groups = nonEmpty(groupTasksByDate(tasks, today));

    expect(groups).toHaveLength(1);
    expect(groups[0].header).toBe("Overdue");
    expect(groups[0].headerClass).toBe("overdue");
  });

  it("future due_date on future task uses normal grouping", () => {
    const tasks = [
      task("a.md", "Future", "2026-04-15", { due_date: "2026-04-20" }),
    ];
    const groups = nonEmpty(groupTasksByDate(tasks, today));

    expect(groups).toHaveLength(1);
    expect(groups[0].headerClass).toBe("weekday");
  });

  it("boundary: task dated exactly 6 days ahead is weekday", () => {
    const tasks = [task("a.md", "Saturday", "2026-04-18")];
    const groups = nonEmpty(groupTasksByDate(tasks, today));

    expect(groups).toHaveLength(1);
    expect(groups[0].headerClass).toBe("weekday");
  });

  it("boundary: task dated exactly 7 days ahead is future", () => {
    const tasks = [task("a.md", "Next Sunday", "2026-04-19")];
    const groups = nonEmpty(groupTasksByDate(tasks, today));

    expect(groups).toHaveLength(1);
    expect(groups[0].headerClass).toBe("future");
  });

  it("canonicalDate is todayStr for Today group", () => {
    const tasks = [task("a.md", "Today task", "2026-04-12")];
    const groups = nonEmpty(groupTasksByDate(tasks, today));

    expect(groups[0].header).toBe("Today");
    expect(groups[0].canonicalDate).toBe("2026-04-12");
  });

  it("canonicalDate is null for past tentative dates in Earlier group", () => {
    const tasks = [task("a.md", "Past task", "2026-04-05")];
    const groups = nonEmpty(groupTasksByDate(tasks, today));

    expect(groups[0].header).toBe("Pulled forward");
    expect(groups[0].canonicalDate).toBeNull();
  });

  it("canonicalDate is tomorrowStr for Tomorrow group", () => {
    const tasks = [task("a.md", "Tomorrow task", "2026-04-13")];
    const groups = nonEmpty(groupTasksByDate(tasks, today));

    expect(groups[0].header).toBe("Tomorrow");
    expect(groups[0].canonicalDate).toBe("2026-04-13");
  });

  it("canonicalDate is the specific date for weekday groups", () => {
    const tasks = [task("a.md", "Wednesday task", "2026-04-15")];
    const groups = nonEmpty(groupTasksByDate(tasks, today));

    expect(groups[0].headerClass).toBe("weekday");
    expect(groups[0].canonicalDate).toBe("2026-04-15");
  });

  it("canonicalDate is the specific date for future groups", () => {
    const tasks = [task("a.md", "Future task", "2026-04-25")];
    const groups = nonEmpty(groupTasksByDate(tasks, today));

    expect(groups[0].headerClass).toBe("future");
    expect(groups[0].canonicalDate).toBe("2026-04-25");
  });

  it("canonicalDate is null for Overdue group", () => {
    const tasks = [
      task("a.md", "Overdue", "2026-04-10", { due_date: "2026-04-10" }),
    ];
    const groups = nonEmpty(groupTasksByDate(tasks, today));

    expect(groups[0].header).toBe("Overdue");
    expect(groups[0].canonicalDate).toBeNull();
  });

  it("orders multiple WEEKDAY groups chronologically", () => {
    const tasks = [
      task("a.md", "Thursday task", "2026-04-16"),
      task("b.md", "Monday task", "2026-04-14"),
      task("c.md", "Wednesday task", "2026-04-15"),
    ];
    const groups = nonEmpty(groupTasksByDate(tasks, today));

    expect(groups).toHaveLength(3);
    expect(groups[0].canonicalDate).toBe("2026-04-14");
    expect(groups[1].canonicalDate).toBe("2026-04-15");
    expect(groups[2].canonicalDate).toBe("2026-04-16");
  });

  it("orders multiple FUTURE groups chronologically", () => {
    const tasks = [
      task("a.md", "Much later", "2026-06-01"),
      task("b.md", "Later", "2026-04-25"),
    ];
    const groups = nonEmpty(groupTasksByDate(tasks, today));

    expect(groups).toHaveLength(2);
    expect(groups[0].canonicalDate).toBe("2026-04-25");
    expect(groups[1].canonicalDate).toBe("2026-06-01");
  });

  // --- today-earlier-bucket split (design.md §6) ---

  it("Due-today renders above Earlier and Today", () => {
    const tasks = [
      task("a.md", "Earlier P3", "2026-04-05", { sort_order: 30 }),
      task("b.md", "Due-today P2", "2026-04-12", {
        due_date: "2026-04-12",
        sort_order: 20,
      }),
      task("c.md", "Today P1", "2026-04-12", { sort_order: 10 }),
    ];
    const groups = nonEmpty(groupTasksByDate(tasks, today));
    expect(groups.map((g) => g.header)).toEqual([
      "Due today",
      "Pulled forward",
      "Today",
    ]);
  });

  it("Earlier and Today are separated; Today's P1 doesn't sort below Earlier's P3", () => {
    const tasks = [
      task("a.md", "Today P1", "2026-04-12", { sort_order: 10 }),
      task("b.md", "Earlier P3", "2026-04-05", { sort_order: 30 }),
    ];
    const groups = nonEmpty(groupTasksByDate(tasks, today));
    expect(groups).toHaveLength(2);
    expect(groups[0].header).toBe("Pulled forward");
    expect(groups[0].tasks).toHaveLength(1);
    expect(groups[0].tasks[0].path).toBe("b.md");
    expect(groups[1].header).toBe("Today");
    expect(groups[1].tasks).toHaveLength(1);
    expect(groups[1].tasks[0].path).toBe("a.md");
  });

  it("due_date in past keeps a task in Overdue, not Earlier or Due-today", () => {
    const tasks = [
      task("a.md", "Past due", "2026-04-10", { due_date: "2026-04-10" }),
    ];
    const groups = nonEmpty(groupTasksByDate(tasks, today));
    expect(groups).toHaveLength(1);
    expect(groups[0].header).toBe("Overdue");
  });

  it("due_date today + effective_date today is Due-today, not Today", () => {
    const tasks = [
      task("a.md", "Due today", "2026-04-12", { due_date: "2026-04-12" }),
    ];
    const groups = nonEmpty(groupTasksByDate(tasks, today));
    expect(groups).toHaveLength(1);
    expect(groups[0].header).toBe("Due today");
    expect(groups[0].headerClass).toBe("due-today");
  });

  it("due_date today + effective_date in the past is Due-today (not Earlier)", () => {
    // Pins the §2 cascade decision: DUE_TODAY wins over EARLIER when both
    // apply. If the policy ever flips, this test flips with it.
    const tasks = [
      task("a.md", "Rolled forward, due today", "2026-04-09", {
        due_date: "2026-04-12",
      }),
    ];
    const groups = nonEmpty(groupTasksByDate(tasks, today));
    expect(groups).toHaveLength(1);
    expect(groups[0].header).toBe("Due today");
    expect(groups[0].headerClass).toBe("due-today");
  });

  it("Due-today canonicalDate is null", () => {
    const tasks = [
      task("a.md", "Due today", "2026-04-12", { due_date: "2026-04-12" }),
    ];
    const groups = nonEmpty(groupTasksByDate(tasks, today));
    expect(groups[0].canonicalDate).toBeNull();
  });

  it("Earlier canonicalDate is null", () => {
    const tasks = [task("a.md", "Earlier", "2026-04-08")];
    const groups = nonEmpty(groupTasksByDate(tasks, today));
    expect(groups[0].canonicalDate).toBeNull();
  });

  // --- Always-visible 7-day window (design.md §3) ---

  it("emits empty TODAY/TOMORROW/WEEKDAY buckets when no tasks fall in the next 7 days", () => {
    // Tasks only in OVERDUE + FUTURE — the 7 forced date headers
    // appear between them in section/chronological order.
    const tasks = [
      task("a.md", "Overdue", "2026-04-10", { due_date: "2026-04-10" }),
      task("b.md", "Future", "2026-04-25"),
    ];
    const groups = groupTasksByDate(tasks, today);
    // Expect 1 OVERDUE + 7 forced + 1 FUTURE = 9.
    expect(groups).toHaveLength(9);
    expect(groups[0].headerClass).toBe("overdue");
    expect(groups[1].headerClass).toBe("today");
    expect(groups[2].headerClass).toBe("tomorrow");
    for (let i = 3; i < 8; i++) {
      expect(groups[i].headerClass).toBe("weekday");
    }
    expect(groups[8].headerClass).toBe("future");
    // Forced placeholders are empty.
    for (let i = 1; i < 8; i++) {
      expect(groups[i].tasks).toHaveLength(0);
    }
  });

  it("merges empty placeholders with populated buckets (single task on today+3)", () => {
    // Single task on today+3 (Wednesday) — should populate that one
    // bucket and seed 6 empty placeholders for the remaining 6 days.
    const tasks = [task("w.md", "Wednesday task", "2026-04-15")];
    const groups = groupTasksByDate(tasks, today);
    expect(groups).toHaveLength(7);
    let populated = 0;
    for (const g of groups) {
      if (g.tasks.length > 0) populated++;
    }
    expect(populated).toBe(1);
    // The populated bucket sits at the today+3 position
    // (chronological: TODAY, TOMORROW, +2, +3, +4, +5, +6).
    expect(groups[3].canonicalDate).toBe("2026-04-15");
    expect(groups[3].tasks).toHaveLength(1);
    expect(groups[3].tasks[0].path).toBe("w.md");
  });

  it("does not emit empty placeholders for OVERDUE / DUE_TODAY / EARLIER / FUTURE", () => {
    // Totally empty input: result is exactly the 7 forced date
    // buckets. OVERDUE / DUE_TODAY / EARLIER / FUTURE stay absent.
    const groups = groupTasksByDate([], today);
    expect(groups).toHaveLength(7);
    for (const g of groups) {
      expect(["overdue", "due-today", "earlier", "future"]).not.toContain(
        g.headerClass,
      );
    }
  });
});

/** Phase 4 §5.4: `_pendingLabel` picks a label matching the in-flight
 * action — `completing…` / `snoozing…` / `reordering…`, with
 * `working…` as a defensive fallback. */
describe("_pendingLabel dispatch (Phase 4 §5.4)", () => {
  function makeList(
    entries: [string, TodoPendingAction][],
  ): TodoListInternals {
    const el = document.createElement("brenn-todo-list") as BrennTodoList;
    const slotState = new Map<string, SlotState>();
    for (const [key, action] of entries) {
      const colon = key.indexOf(":");
      const repo = key.slice(0, colon) || undefined;
      const path = key.slice(colon + 1);
      slotState.set(key, {
        kind: "pending",
        action,
        startedAt: Date.now(),
        path,
        repo,
        taskTldr: "fixture",
      });
    }
    el.slotState = slotState;
    return el as unknown as TodoListInternals;
  }

  it('"done" → "completing…"', () => {
    const el = makeList([[":foo.md", "done"]]);
    expect(el._pendingLabel(":foo.md")).toBe("completing…");
  });

  it('"snooze" → "snoozing…"', () => {
    const el = makeList([[":foo.md", "snooze"]]);
    expect(el._pendingLabel(":foo.md")).toBe("snoozing…");
  });

  it('"reorder" → "reordering…"', () => {
    const el = makeList([[":foo.md", "reorder"]]);
    expect(el._pendingLabel(":foo.md")).toBe("reordering…");
  });

  it('map miss → "working…" fallback', () => {
    const el = makeList([]);
    expect(el._pendingLabel(":foo.md")).toBe("working…");
  });

  it('settled entry → "working…" (label only applies while pending)', () => {
    const el = document.createElement("brenn-todo-list") as BrennTodoList;
    el.slotState = new Map<string, SlotState>([
      [
        ":foo.md",
        {
          kind: "settled",
          action: "done",
          settledAt: Date.now(),
          tileText: "Done.",
          path: "foo.md",
          repo: undefined,
          taskTldr: "fixture",
        },
      ],
    ]);
    expect((el as unknown as TodoListInternals)._pendingLabel(":foo.md")).toBe(
      "working…",
    );
  });
});

/** `buildSlotGroups` is a thin wrapper under the freeze-the-list design
 *  — it either renders from the frozen snapshot or from the live task
 *  list via `groupTasksByDate`. All slots emitted are `kind: "live"`;
 *  pending/settled/dismissed rendering happens in `_renderTask` keyed
 *  off `slotState`. */
describe("buildSlotGroups", () => {
  const today = "2026-04-12";

  it("no snapshot: delegates to groupTasksByDate and emits live slots", () => {
    const tasks: TodoItem[] = [
      task("a.md", "Today A", "2026-04-12"),
      task("b.md", "Tomorrow B", "2026-04-13"),
    ];
    const slotGroups = buildSlotGroups(tasks, today, null);
    const legacy = groupTasksByDate(tasks, today);
    expect(slotGroups.map((g) => g.header)).toEqual(
      legacy.map((g) => g.header),
    );
    for (const g of slotGroups) {
      for (const s of g.slots) expect(s.kind).toBe("live");
    }
    expect(
      slotGroups.flatMap((g) => g.slots).length,
    ).toBe(tasks.length);
  });

  it("empty tasks + no snapshot → 7 always-visible date buckets", () => {
    // Mirror of `groupTasksByDate([], today)` in §3 above — the
    // always-visible 7-day window seeds 7 empty buckets even with zero
    // live tasks, and `buildSlotGroups` is a thin wrapper around it.
    const groups = buildSlotGroups([], today, null);
    expect(groups).toHaveLength(7);
    for (const g of groups) {
      expect(g.slots).toHaveLength(0);
      expect(g.canonicalDate).not.toBeNull();
    }
  });

  it("frozen snapshot: renders from snapshot's groups, ignores live tasks prop", () => {
    // Snapshot captured [A, B, C]; live refresh delivers a disjoint
    // set [X, Y]. Render must reflect the snapshot.
    const snapshot: { groups: TaskGroup[]; todayStr: string } = {
      todayStr: today,
      groups: groupTasksByDate(
        [
          task("a.md", "A", "2026-04-12"),
          task("b.md", "B", "2026-04-12"),
          task("c.md", "C", "2026-04-12"),
        ],
        today,
      ),
    };
    const liveTasks: TodoItem[] = [
      task("x.md", "X", "2026-04-12"),
      task("y.md", "Y", "2026-04-12"),
    ];
    const groups = buildSlotGroups(liveTasks, today, snapshot);
    const keys = groups.flatMap((g) =>
      g.slots.map((s) => todoKey(s.task.path, s.task.repo)),
    );
    expect(keys).toEqual([":a.md", ":b.md", ":c.md"]);
  });

  it("frozen snapshot: pins section ordering against a later todayStr", () => {
    // Snapshot captured on 2026-04-12; a midnight rollover changes the
    // `today` prop to 2026-04-13 (Tomorrow task would now be Today).
    // Render must still reflect the snapshot's sectioning. The snapshot
    // also carries the always-visible 7-day window seeded at capture
    // time (TODAY / TOMORROW / 5 WEEKDAYs), so the result is 7 buckets
    // with the populated TOMORROW bucket among them.
    const snapshot: { groups: TaskGroup[]; todayStr: string } = {
      todayStr: "2026-04-12",
      groups: groupTasksByDate(
        [task("a.md", "Tomorrow's task", "2026-04-13")],
        "2026-04-12",
      ),
    };
    const groups = buildSlotGroups([], "2026-04-13", snapshot);
    expect(groups).toHaveLength(7);
    const tomorrow = groups.find((g) => g.headerClass === "tomorrow");
    expect(tomorrow).toBeDefined();
    expect(tomorrow!.slots).toHaveLength(1);
    expect(tomorrow!.slots[0].task.path).toBe("a.md");
  });
});

/** Frozen-list rendering: the full render pipeline under a snapshot.
 *
 *  These are the regression tests that motivated the rewrite. Click
 *  Done on rows 2, 4, 6 of the same section — tombstones must land
 *  in place (not stacked, not at section bottom, not any fragile
 *  splice computation). */
describe("frozen list rendering", () => {
  const today = "2026-04-12";

  function settledState(
    path: string,
    taskTldr: string,
    action: "done" | "snooze",
    tileText: string,
    targetEffectiveDate?: string,
  ): SlotState {
    return {
      kind: "settled",
      action,
      settledAt: Date.now(),
      tileText,
      path,
      repo: undefined,
      taskTldr,
      targetEffectiveDate,
    };
  }

  function mkSnapshot(tasks: TodoItem[], todayStr: string) {
    return { todayStr, groups: groupTasksByDate(tasks, todayStr) };
  }

  async function mountList(
    tasks: TodoItem[],
    slotState: Map<string, SlotState>,
    snapshot: { groups: TaskGroup[]; todayStr: string } | null,
  ): Promise<BrennTodoList> {
    const el = document.createElement("brenn-todo-list") as BrennTodoList;
    el.visible = true;
    el.todayStr = today;
    el.tasks = tasks;
    el.slotState = slotState;
    el.frozenSnapshot = snapshot;
    document.body.appendChild(el);
    await el.updateComplete;
    return el;
  }

  it("F1: 7 tasks in Today, done on 2/4/6; tombstones stay in place", async () => {
    // A B C D E F G all in Today. Click Done on B, D, F.
    const allTasks: TodoItem[] = [
      task("a.md", "A", "2026-04-12", { sort_order: 0 }),
      task("b.md", "B", "2026-04-12", { sort_order: 1 }),
      task("c.md", "C", "2026-04-12", { sort_order: 2 }),
      task("d.md", "D", "2026-04-12", { sort_order: 3 }),
      task("e.md", "E", "2026-04-12", { sort_order: 4 }),
      task("f.md", "F", "2026-04-12", { sort_order: 5 }),
      task("g.md", "G", "2026-04-12", { sort_order: 6 }),
    ];
    // Snapshot captured at first-pending (click on B): pre-dispatch
    // list is A B C D E F G. Subsequent dones on D and F do NOT
    // retake the snapshot.
    const snapshot = mkSnapshot(allTasks, today);
    // After all three acks, slotState has B/D/F settled. The live
    // `tasks` prop may have had those removed server-side; the
    // rendered list is immutable under the frozen snapshot.
    const slotState = new Map<string, SlotState>();
    slotState.set(":b.md", settledState("b.md", "B", "done", "Done."));
    slotState.set(":d.md", settledState("d.md", "D", "done", "Done."));
    slotState.set(":f.md", settledState("f.md", "F", "done", "Done."));
    const el = await mountList(
      [allTasks[0], allTasks[2], allTasks[4], allTasks[6]], // A C E G only in live refresh
      slotState,
      snapshot,
    );
    const rows = Array.from(
      el.shadowRoot!.querySelectorAll(".task-row"),
    );
    expect(rows).toHaveLength(7);
    const keys = rows.map((r) => r.getAttribute("data-task-key"));
    expect(keys).toEqual([
      ":a.md",
      ":b.md",
      ":c.md",
      ":d.md",
      ":e.md",
      ":f.md",
      ":g.md",
    ]);
    // B D F are settled rows; A C E G are live.
    expect(rows[0].classList.contains("settled")).toBe(false);
    expect(rows[1].classList.contains("settled")).toBe(true);
    expect(rows[2].classList.contains("settled")).toBe(false);
    expect(rows[3].classList.contains("settled")).toBe(true);
    expect(rows[4].classList.contains("settled")).toBe(false);
    expect(rows[5].classList.contains("settled")).toBe(true);
    expect(rows[6].classList.contains("settled")).toBe(false);
    document.body.removeChild(el);
  });

  it("F2: mid-triage refresh does not reshape the list", async () => {
    // Six-task section A B C D E F. Done on C (pending). A refresh
    // delivers [A, B, D, E, F, H] — C gone, H added. Render must still
    // show A B C(pending) D E F; H is absent.
    const preTasks: TodoItem[] = [
      task("a.md", "A", "2026-04-12", { sort_order: 0 }),
      task("b.md", "B", "2026-04-12", { sort_order: 1 }),
      task("c.md", "C", "2026-04-12", { sort_order: 2 }),
      task("d.md", "D", "2026-04-12", { sort_order: 3 }),
      task("e.md", "E", "2026-04-12", { sort_order: 4 }),
      task("f.md", "F", "2026-04-12", { sort_order: 5 }),
    ];
    const snapshot = mkSnapshot(preTasks, today);
    const slotState = new Map<string, SlotState>();
    slotState.set(":c.md", {
      kind: "pending",
      action: "done",
      startedAt: Date.now(),
      path: "c.md",
      repo: undefined,
      taskTldr: "C",
    });
    // Live tasks prop is the refreshed list (C gone, H added).
    const refreshedTasks: TodoItem[] = [
      preTasks[0], preTasks[1], preTasks[3], preTasks[4], preTasks[5],
      task("h.md", "H", "2026-04-12", { sort_order: 7 }),
    ];
    const el = await mountList(refreshedTasks, slotState, snapshot);
    const rows = Array.from(
      el.shadowRoot!.querySelectorAll(".task-row"),
    );
    expect(rows).toHaveLength(6);
    const keys = rows.map((r) => r.getAttribute("data-task-key"));
    expect(keys).toEqual([
      ":a.md",
      ":b.md",
      ":c.md",
      ":d.md",
      ":e.md",
      ":f.md",
    ]);
    // H is NOT in the DOM.
    expect(keys.includes(":h.md")).toBe(false);
    // C renders as pending.
    expect(rows[2].classList.contains("pending")).toBe(true);
    document.body.removeChild(el);
  });

  it("F3: thawed list reshapes to the buffered refresh", async () => {
    // Continuation of F2 conceptually: idle fires, snapshot cleared,
    // render reflects current `todoTasks`.
    const refreshedTasks: TodoItem[] = [
      task("a.md", "A", "2026-04-12", { sort_order: 0 }),
      task("b.md", "B", "2026-04-12", { sort_order: 1 }),
      task("d.md", "D", "2026-04-12", { sort_order: 3 }),
      task("e.md", "E", "2026-04-12", { sort_order: 4 }),
      task("f.md", "F", "2026-04-12", { sort_order: 5 }),
      task("h.md", "H", "2026-04-12", { sort_order: 7 }),
    ];
    const el = await mountList(refreshedTasks, new Map(), null);
    const rows = Array.from(
      el.shadowRoot!.querySelectorAll(".task-row"),
    );
    const keys = rows.map((r) => r.getAttribute("data-task-key"));
    expect(keys).toEqual([
      ":a.md",
      ":b.md",
      ":d.md",
      ":e.md",
      ":f.md",
      ":h.md",
    ]);
    document.body.removeChild(el);
  });

  it("F9: × on one tile of several leaves a dismissed placeholder", async () => {
    // A B C D E in Today. Done on B, Done on D; both settle. × on B
    // → B transitions to dismissed. Render must still be 5 rows, in
    // original positions.
    const allTasks: TodoItem[] = [
      task("a.md", "A", "2026-04-12", { sort_order: 0 }),
      task("b.md", "B", "2026-04-12", { sort_order: 1 }),
      task("c.md", "C", "2026-04-12", { sort_order: 2 }),
      task("d.md", "D", "2026-04-12", { sort_order: 3 }),
      task("e.md", "E", "2026-04-12", { sort_order: 4 }),
    ];
    const snapshot = mkSnapshot(allTasks, today);
    const slotState = new Map<string, SlotState>();
    slotState.set(":b.md", {
      kind: "dismissed",
      action: "done",
      dismissedAt: Date.now(),
      path: "b.md",
      repo: undefined,
      taskTldr: "B",
    });
    slotState.set(":d.md", settledState("d.md", "D", "done", "Done."));
    const el = await mountList(
      [allTasks[0], allTasks[2], allTasks[4]],
      slotState,
      snapshot,
    );
    const rows = Array.from(
      el.shadowRoot!.querySelectorAll(".task-row"),
    );
    expect(rows).toHaveLength(5);
    // Live and settled rows carry data-task-key; the dismissed
    // placeholder at index 1 intentionally omits it (no handlers,
    // structurally unaddressable).
    expect(rows[0].getAttribute("data-task-key")).toBe(":a.md");
    expect(rows[1].getAttribute("data-task-key")).toBeNull();
    expect(rows[2].getAttribute("data-task-key")).toBe(":c.md");
    expect(rows[3].getAttribute("data-task-key")).toBe(":d.md");
    expect(rows[4].getAttribute("data-task-key")).toBe(":e.md");
    expect(rows[1].classList.contains("dismissed")).toBe(true);
    expect(rows[1].getAttribute("aria-hidden")).toBe("true");
    expect(rows[3].classList.contains("settled")).toBe(true);
    document.body.removeChild(el);
  });

  it("settled-tile glyph/text/class (done)", async () => {
    const allTasks: TodoItem[] = [task("a.md", "Todd's birthday", "2026-04-12")];
    const snapshot = mkSnapshot(allTasks, today);
    const slotState = new Map<string, SlotState>();
    slotState.set(
      ":a.md",
      settledState("a.md", "Todd's birthday", "done", "Next: Apr 29"),
    );
    const el = await mountList([], slotState, snapshot);
    const tile = el.shadowRoot!.querySelector(".task-row.settled")!;
    expect(tile).not.toBeNull();
    expect(tile.getAttribute("role")).toBe("status");
    expect(tile.getAttribute("aria-live")).toBe("polite");
    expect(tile.querySelector(".settled-glyph")!.textContent).toBe("✓");
    expect(tile.querySelector(".settled-title")!.textContent).toBe(
      "Todd's birthday",
    );
    expect(tile.querySelector(".settled-outcome")!.textContent).toBe(
      "Next: Apr 29",
    );
    const dismiss = tile.querySelector(".settled-dismiss")!;
    expect(dismiss).not.toBeNull();
    expect(dismiss.getAttribute("aria-label")).toBe("Dismiss");
    expect(tile.textContent).not.toMatch(/undo/i);
    document.body.removeChild(el);
  });

  it("pending row: emits .task-row.pending with pending-label, omits data-task-idx and action buttons", async () => {
    // F5: the pending branch of `_renderTask` produces a same-outer
    // `.task-row` with the pending class, a `.pending-label`, no
    // `.task-actions`, and no `data-task-idx` / `data-group-idx`
    // (drag hit-test relies on attribute absence + class exclusion).
    const allTasks: TodoItem[] = [
      task("a.md", "A", "2026-04-12", { sort_order: 0 }),
      task("b.md", "B", "2026-04-12", { sort_order: 1 }),
    ];
    const snapshot = mkSnapshot(allTasks, today);
    const slotState = new Map<string, SlotState>();
    slotState.set(":b.md", {
      kind: "pending",
      action: "done",
      startedAt: Date.now(),
      path: "b.md",
      repo: undefined,
      taskTldr: "B",
    });
    const el = await mountList(allTasks, slotState, snapshot);
    const rows = Array.from(
      el.shadowRoot!.querySelectorAll<HTMLElement>(".task-row"),
    );
    expect(rows).toHaveLength(2);
    // Row A is live; row B is pending.
    const liveRow = rows[0];
    const pendingRow = rows[1];
    expect(liveRow.classList.contains("pending")).toBe(false);
    expect(pendingRow.classList.contains("pending")).toBe(true);
    // Pending row has the pending label and no action buttons.
    expect(pendingRow.querySelector(".pending-label")!.textContent).toBe(
      "completing…",
    );
    expect(pendingRow.querySelector(".task-actions")).toBeNull();
    // Pending row omits drag-hit-test data-attrs (uniform F4 rule).
    expect(pendingRow.getAttribute("data-task-idx")).toBeNull();
    expect(pendingRow.getAttribute("data-group-idx")).toBeNull();
    // Live row does emit them.
    expect(liveRow.getAttribute("data-task-idx")).toBe("0");
    expect(liveRow.getAttribute("data-group-idx")).toBe("0");
    // Pending row keeps `data-task-key` so click handlers can route.
    expect(pendingRow.getAttribute("data-task-key")).toBe(":b.md");
    // Trailing badges (errorText/select-dot/priority/recurring/repo)
    // share the same code path between branches; spot-check that the
    // task-meta wrapper exists on both.
    expect(pendingRow.querySelector(".task-meta")).not.toBeNull();
    expect(liveRow.querySelector(".task-meta")).not.toBeNull();
    document.body.removeChild(el);
  });

  it("settled-tile glyph/class (snooze)", async () => {
    const allTasks: TodoItem[] = [task("a.md", "Review docs", "2026-04-12")];
    const snapshot = mkSnapshot(allTasks, today);
    const slotState = new Map<string, SlotState>();
    slotState.set(
      ":a.md",
      settledState(
        "a.md",
        "Review docs",
        "snooze",
        "Snoozed to Apr 20",
        "2026-04-20",
      ),
    );
    const el = await mountList([], slotState, snapshot);
    const tile = el.shadowRoot!.querySelector(".task-row.settled")!;
    expect(tile.classList.contains("settled-snooze")).toBe(true);
    expect(tile.querySelector(".settled-glyph")!.textContent).toBe("💤");
    document.body.removeChild(el);
  });

  it("settled tile dismiss button dispatches onDismissSettled with the slot key", async () => {
    const allTasks: TodoItem[] = [task("a.md", "A", "2026-04-12")];
    const snapshot = mkSnapshot(allTasks, today);
    const slotState = new Map<string, SlotState>();
    slotState.set(":a.md", settledState("a.md", "A", "done", "Done."));
    const el = await mountList([], slotState, snapshot);
    const calls: string[] = [];
    el.onDismissSettled = (key: string) => calls.push(key);
    const dismiss = el.shadowRoot!.querySelector(
      ".settled-dismiss",
    ) as HTMLButtonElement;
    dismiss.click();
    expect(calls).toEqual([":a.md"]);
    document.body.removeChild(el);
  });
});

/** `interactionsDisabled` gates `_onDragStart` — round-1 review
 *  flagged the handler-level short-circuit as tested but the drag-
 *  start entry point as untested. */
describe("_onDragStart interactionsDisabled gate", () => {
  interface DragInternals {
    _onDragStart(
      e: PointerEvent,
      t: TodoItem,
      gi: number,
      ti: number,
    ): void;
    _drag: unknown;
  }

  function makeList(disabled: boolean): BrennTodoList {
    const el = document.createElement("brenn-todo-list") as BrennTodoList;
    el.visible = true;
    el.todayStr = "2026-04-12";
    el.tasks = [task("a.md", "A", "2026-04-12")];
    el.slotState = new Map();
    el.frozenSnapshot = null;
    el.interactionsDisabled = disabled;
    return el;
  }

  it("does not start a drag when interactionsDisabled is true", async () => {
    const el = makeList(true);
    document.body.appendChild(el);
    await el.updateComplete;
    const handle = el.shadowRoot!.querySelector(".drag-handle") as HTMLElement;
    handle.setPointerCapture = () => {};
    const drag = el as unknown as DragInternals;
    const evt = new PointerEvent("pointerdown", {
      bubbles: true,
      button: 0,
      pointerId: 1,
      clientX: 0,
      clientY: 0,
    });
    Object.defineProperty(evt, "currentTarget", { value: handle });
    drag._onDragStart(evt, el.tasks[0], 0, 0);
    expect(drag._drag).toBeNull();
    document.body.removeChild(el);
  });

  it("starts a drag when interactionsDisabled is false", async () => {
    const el = makeList(false);
    document.body.appendChild(el);
    await el.updateComplete;
    const handle = el.shadowRoot!.querySelector(".drag-handle") as HTMLElement;
    handle.setPointerCapture = () => {};
    const drag = el as unknown as DragInternals;
    const evt = new PointerEvent("pointerdown", {
      bubbles: true,
      button: 0,
      pointerId: 1,
      clientX: 0,
      clientY: 0,
    });
    Object.defineProperty(evt, "currentTarget", { value: handle });
    drag._onDragStart(evt, el.tasks[0], 0, 0);
    expect(drag._drag).not.toBeNull();
    document.body.removeChild(el);
  });
});

/** Drop-side hit-test predicate for the today-earlier-bucket split.
 *  See `todo-list.ts::isDroppable` and design.md §4. */
describe("isDroppable predicate (today-earlier-bucket §4)", () => {
  it("rejects cross-bucket drop into OVERDUE", () => {
    expect(isDroppable("overdue", 3, 0)).toBe(false);
  });
  it("rejects cross-bucket drop into EARLIER", () => {
    expect(isDroppable("earlier", 3, 1)).toBe(false);
  });
  it("rejects cross-bucket drop into DUE_TODAY", () => {
    expect(isDroppable("due-today", 3, 2)).toBe(false);
  });
  it("allows same-source-group drop into EARLIER (within-bucket reorder)", () => {
    expect(isDroppable("earlier", 1, 1)).toBe(true);
  });
  it("allows same-source-group drop into OVERDUE (predicate-symmetric)", () => {
    expect(isDroppable("overdue", 0, 0)).toBe(true);
  });
  it("allows same-source-group drop into DUE_TODAY (predicate-symmetric)", () => {
    expect(isDroppable("due-today", 2, 2)).toBe(true);
  });
  it("allows drop into TODAY from any source", () => {
    expect(isDroppable("today", 1, 3)).toBe(true);
    expect(isDroppable("today", 3, 3)).toBe(true);
  });
  it("allows drop into TOMORROW / WEEKDAY / FUTURE from any source", () => {
    expect(isDroppable("tomorrow", 0, 4)).toBe(true);
    expect(isDroppable("weekday", 1, 5)).toBe(true);
    expect(isDroppable("future", 2, 6)).toBe(true);
  });
});

/** Drag-start gate for OVERDUE and DUE_TODAY (today-earlier-bucket §4).
 *  Both buckets are keyed off `due_date`, which TodoReorder cannot
 *  change; allowing drag-out would cause snap-back on next render.
 *  EARLIER is *not* gated — drag-out of EARLIER is the legal
 *  cross-bucket move from a pseudo-bucket. */
describe("_onDragStart pseudo-bucket gate (today-earlier-bucket §4)", () => {
  interface DragInternals {
    _onDragStart(
      e: PointerEvent,
      t: TodoItem,
      gi: number,
      ti: number,
    ): void;
    _drag: { sourceGroupIdx: number } | null;
  }

  async function mountWith(tasks: TodoItem[]): Promise<BrennTodoList> {
    const el = document.createElement("brenn-todo-list") as BrennTodoList;
    el.visible = true;
    el.todayStr = "2026-04-12";
    el.tasks = tasks;
    el.slotState = new Map();
    el.frozenSnapshot = null;
    document.body.appendChild(el);
    await el.updateComplete;
    return el;
  }

  function fakeEvent(): PointerEvent {
    const handle = document.createElement("div");
    handle.setPointerCapture = () => {};
    const evt = new PointerEvent("pointerdown", {
      bubbles: true,
      button: 0,
      pointerId: 1,
      clientX: 0,
      clientY: 0,
    });
    Object.defineProperty(evt, "currentTarget", { value: handle });
    return evt;
  }

  it("rejects drag from OVERDUE", async () => {
    const el = await mountWith([
      task("a.md", "Overdue", "2026-04-10", { due_date: "2026-04-10" }),
    ]);
    const drag = el as unknown as DragInternals;
    drag._onDragStart(fakeEvent(), el.tasks[0], 0, 0);
    expect(drag._drag).toBeNull();
    document.body.removeChild(el);
  });

  it("rejects drag from DUE_TODAY", async () => {
    const el = await mountWith([
      task("a.md", "Due today", "2026-04-12", { due_date: "2026-04-12" }),
    ]);
    const drag = el as unknown as DragInternals;
    drag._onDragStart(fakeEvent(), el.tasks[0], 0, 0);
    expect(drag._drag).toBeNull();
    document.body.removeChild(el);
  });

  it("allows drag from EARLIER", async () => {
    const el = await mountWith([
      task("a.md", "Earlier", "2026-04-08"),
    ]);
    const drag = el as unknown as DragInternals;
    drag._onDragStart(fakeEvent(), el.tasks[0], 0, 0);
    expect(drag._drag).not.toBeNull();
    expect(drag._drag!.sourceGroupIdx).toBe(0);
    document.body.removeChild(el);
  });

  it("allows drag from TODAY", async () => {
    const el = await mountWith([
      task("a.md", "Today", "2026-04-12"),
    ]);
    const drag = el as unknown as DragInternals;
    drag._onDragStart(fakeEvent(), el.tasks[0], 0, 0);
    expect(drag._drag).not.toBeNull();
    document.body.removeChild(el);
  });
});

// `.action-btn` needs `touch-action: manipulation` for reliable tap
// activation inside a scrollable ancestor, and its `:hover` rules must
// be gated on `(hover: hover)` so touch devices don't get sticky hover.
describe("action-btn CSS invariants", () => {
  it("declares touch-action: manipulation inside .action-btn block", () => {
    const css = BrennTodoList.styles.cssText;
    const match = css.match(/\.action-btn\s*\{[^}]*\}/);
    expect(match).not.toBeNull();
    expect(match![0]).toContain("touch-action: manipulation");
  });

  it("gates .action-btn :hover rules on (hover: hover)", () => {
    const css = BrennTodoList.styles.cssText;
    const open = css.match(/@media\s*\(\s*hover:\s*hover\s*\)\s*\{/);
    expect(open).not.toBeNull();
    const start = open!.index! + open![0].length;
    let depth = 1;
    let end = start;
    while (end < css.length && depth > 0) {
      const ch = css[end];
      if (ch === "{") depth++;
      else if (ch === "}") depth--;
      if (depth === 0) break;
      end++;
    }
    expect(depth).toBe(0);
    const body = css.slice(start, end);
    expect(body).toMatch(/\.action-btn:hover\s*\{[^}]*color:\s*#4a6fa5/);
    expect(body).toMatch(/\.action-btn\.done-btn:hover\s*\{[^}]*color:\s*#3ddc84/);
    const outsideMedia = css.slice(0, open!.index!) + css.slice(end + 1);
    expect(outsideMedia).not.toMatch(/\.action-btn:hover\s*\{/);
    expect(outsideMedia).not.toMatch(/\.action-btn\.done-btn:hover\s*\{/);
  });
});

/** Pure unit tests for the heading-drop hit-test (design.md §4.3).
 *
 *  Synthetic `HitTestRow[]` arrays exercise the helper without DOM
 *  mounting: heading ownership bands, populated vs. empty buckets,
 *  pseudo-bucket rejection, `isDroppable` rejection, out-of-bounds
 *  pointer positions, and mixed populated + empty layouts. */
describe("hitTestDrop (heading-drop §4.3)", () => {
  function header(
    gi: number,
    top: number,
    bottom: number,
    headerClass: string,
    canonicalDate: string | null,
  ): HitTestRow {
    return { kind: "header", gi, top, bottom, headerClass, canonicalDate };
  }
  function row(
    gi: number,
    ti: number,
    top: number,
    bottom: number,
  ): HitTestRow {
    return { kind: "task", gi, ti, top, bottom };
  }

  it("empty-bucket landing pad: pointer in TOMORROW band targets TOMORROW", () => {
    // Empty TOMORROW heading at [100, 120) followed by an empty
    // WEDNESDAY heading at [120, 140). TOMORROW owns y=110 and y=119;
    // WEDNESDAY owns y=120 (band is half-open [top, bottom)).
    const rows: HitTestRow[] = [
      header(0, 100, 110, "tomorrow", "2026-04-13"),
      header(1, 120, 130, "weekday", "2026-04-14"),
    ];
    expect(hitTestDrop(rows, 110, 99)).toEqual({
      mode: "schedule",
      gi: 0,
    });
    expect(hitTestDrop(rows, 119, 99)).toEqual({
      mode: "schedule",
      gi: 0,
    });
    // y=120 lands in the second header's band.
    expect(hitTestDrop(rows, 125, 99)).toEqual({
      mode: "schedule",
      gi: 1,
    });
  });

  it("pointer well inside an empty heading's band returns the empty heading, not the next one", () => {
    // Heading at [100, 110); next heading at [200, 210). The first
    // heading owns the entire gap up to y=199.
    const rows: HitTestRow[] = [
      header(0, 100, 110, "tomorrow", "2026-04-13"),
      header(1, 200, 210, "weekday", "2026-04-14"),
    ];
    // y=144 is far below the first heading's bottom but still inside
    // its ownership band.
    expect(hitTestDrop(rows, 144, 99)).toEqual({
      mode: "schedule",
      gi: 0,
    });
  });

  it("populated bucket: heading band ends at first row's top", () => {
    // Heading at [100, 110); first task at [120, 160). The heading
    // owns [100, 120); the row owns its midpoint at 140.
    const rows: HitTestRow[] = [
      header(0, 100, 110, "today", "2026-04-12"),
      row(0, 0, 120, 160),
    ];
    // y=119 → schedule (still in the heading band).
    expect(hitTestDrop(rows, 119, 99)).toEqual({
      mode: "schedule",
      gi: 0,
    });
    // y=120 → reorder via row pass (above the row's midpoint=140 →
    // insertIdx=0).
    expect(hitTestDrop(rows, 120, 99)).toEqual({
      mode: "reorder",
      gi: 0,
      insertIdx: 0,
    });
    // y=141 → reorder, below midpoint, insertIdx=1.
    expect(hitTestDrop(rows, 141, 99)).toEqual({
      mode: "reorder",
      gi: 0,
      insertIdx: 1,
    });
  });

  it("rejects pseudo-bucket headings (canonicalDate === null)", () => {
    // EARLIER heading with no canonical date — the pointer falls
    // through the heading pass to the row pass.
    const rows: HitTestRow[] = [
      header(0, 100, 110, "earlier", null),
      row(0, 0, 120, 160),
    ];
    // y=105 inside the EARLIER heading band: heading is rejected,
    // row pass picks the row (above midpoint=140 → insertIdx=0).
    expect(hitTestDrop(rows, 105, 0)).toEqual({
      mode: "reorder",
      gi: 0,
      insertIdx: 0,
    });
  });

  it("rejects headings filtered by isDroppable", () => {
    // EARLIER heading with a real canonicalDate would still be
    // rejected for cross-bucket drops if we got there, but EARLIER
    // is filtered earlier by canonicalDate === null. Use an
    // OVERDUE heading with a non-null canonicalDate to exercise the
    // isDroppable branch (synthetic — production OVERDUE has null
    // canonicalDate).
    const rows: HitTestRow[] = [
      header(0, 100, 110, "overdue", "2026-04-10"),
      row(0, 0, 120, 160),
    ];
    // sourceGi=99 (different bucket) → cross-bucket drop into
    // pseudo-bucket → isDroppable rejects → fall through to row
    // pass.
    expect(hitTestDrop(rows, 105, 99)).toEqual({
      mode: "reorder",
      gi: 0,
      insertIdx: 0,
    });
  });

  it("returns null when pointer is above the first heading's top", () => {
    const rows: HitTestRow[] = [
      header(0, 100, 110, "today", "2026-04-12"),
      row(0, 0, 120, 160),
    ];
    expect(hitTestDrop(rows, 50, 99)).toBeNull();
  });

  it("returns null when no heading band claims and no row qualifies", () => {
    // Heading at [100, 110) and nothing below it — pointer at 200
    // falls in the heading's band (which extends to +Infinity) so
    // this is a schedule. But if the heading is a pseudo-bucket
    // and there are no rows at all, we return null.
    const rows: HitTestRow[] = [
      header(0, 100, 110, "earlier", null),
    ];
    expect(hitTestDrop(rows, 200, 0)).toBeNull();
  });

  it("mixed populated + empty: pointer inside an empty bucket returns its gi", () => {
    // [TODAY heading + 1 row, empty TOMORROW heading, empty
    // WEDNESDAY heading + 1 row].
    const rows: HitTestRow[] = [
      header(0, 0, 20, "today", "2026-04-12"),
      row(0, 0, 20, 60), // TODAY's task at midpoint=40
      header(1, 60, 80, "tomorrow", "2026-04-13"), // empty
      header(2, 80, 100, "weekday", "2026-04-14"),
      row(2, 0, 100, 140), // WED's task at midpoint=120
    ];
    // y=70 lands in the empty TOMORROW heading's band [60, 80).
    expect(hitTestDrop(rows, 70, 0)).toEqual({
      mode: "schedule",
      gi: 1,
    });
  });
});

/** Heading-drop dispatch via `_onDragEnd` (design.md §4.6). Mounts a
 *  `<brenn-todo-list>`, simulates the drag state by reaching into the
 *  internals (`_drag`, `_cachedGroups`), and asserts that
 *  `_onDragEnd` fires `onSchedule` with the right `ScheduleTarget`. */
describe("heading-drop dispatch (§4.6)", () => {
  interface DragEndInternals {
    _drag: {
      sourceKey: string;
      sourceTask: TodoItem;
      sourceGroupIdx: number;
      sourceTaskIdx: number;
      ghostEl: null;
      handleEl: HTMLElement;
      taskListEl: HTMLElement;
      pointerId: number;
      startY: number;
      lastClientY: number;
      active: boolean;
      dropMode: "reorder" | "schedule";
      dropGroupIdx: number;
      dropInsertIdx: number;
      scrollRaf: null;
      selectedKeys: Set<string> | null;
    } | null;
    _onDragEnd(e: PointerEvent): void;
    _cachedGroups: TaskGroup[];
  }

  async function mountWith(tasks: TodoItem[]): Promise<BrennTodoList> {
    const el = document.createElement("brenn-todo-list") as BrennTodoList;
    el.visible = true;
    el.todayStr = "2026-04-12";
    el.tasks = tasks;
    el.slotState = new Map();
    el.frozenSnapshot = null;
    document.body.appendChild(el);
    await el.updateComplete;
    return el;
  }

  function fakeUpEvent(): PointerEvent {
    const evt = new PointerEvent("pointerup", {
      bubbles: true,
      button: 0,
      pointerId: 1,
      clientX: 0,
      clientY: 0,
    });
    return evt;
  }

  it("schedule-mode drop fires onSchedule with the bucket's canonicalDate", async () => {
    const el = await mountWith([task("a.md", "A", "2026-04-12")]);
    const calls: ScheduleTarget[] = [];
    el.onSchedule = (t) => calls.push(t);
    const inst = el as unknown as DragEndInternals;
    // Drag from gi=0 (TODAY); target is one of the empty WEEKDAY
    // buckets — pick the bucket whose canonicalDate is 2026-04-14.
    const targetGi = inst._cachedGroups.findIndex(
      (g) => g.canonicalDate === "2026-04-14",
    );
    expect(targetGi).toBeGreaterThan(0);
    inst._drag = {
      sourceKey: ":a.md",
      sourceTask: el.tasks[0],
      sourceGroupIdx: 0,
      sourceTaskIdx: 0,
      ghostEl: null,
      handleEl: document.createElement("div"),
      taskListEl: document.createElement("div"),
      pointerId: 1,
      startY: 0,
      lastClientY: 0,
      active: true,
      dropMode: "schedule",
      dropGroupIdx: targetGi,
      dropInsertIdx: 0,
      scrollRaf: null,
      selectedKeys: null,
    };
    inst._onDragEnd(fakeUpEvent());
    expect(calls).toHaveLength(1);
    expect(calls[0].path).toBe("a.md");
    expect(calls[0].date).toBe("2026-04-14");
    expect(calls[0].selectedKeys).toBeNull();
    document.body.removeChild(el);
  });

  it("same-bucket drop is a no-op (single-task)", async () => {
    // Drag from TODAY onto the TODAY heading: targetDate ===
    // task.effective_date → no dispatch.
    const el = await mountWith([task("a.md", "A", "2026-04-12")]);
    const calls: ScheduleTarget[] = [];
    el.onSchedule = (t) => calls.push(t);
    const inst = el as unknown as DragEndInternals;
    const targetGi = inst._cachedGroups.findIndex(
      (g) => g.canonicalDate === "2026-04-12",
    );
    inst._drag = {
      sourceKey: ":a.md",
      sourceTask: el.tasks[0],
      sourceGroupIdx: 0,
      sourceTaskIdx: 0,
      ghostEl: null,
      handleEl: document.createElement("div"),
      taskListEl: document.createElement("div"),
      pointerId: 1,
      startY: 0,
      lastClientY: 0,
      active: true,
      dropMode: "schedule",
      dropGroupIdx: targetGi,
      dropInsertIdx: 0,
      scrollRaf: null,
      selectedKeys: null,
    };
    inst._onDragEnd(fakeUpEvent());
    expect(calls).toHaveLength(0);
    document.body.removeChild(el);
  });

  it("multi-select schedule fires onSchedule with the ordered keys", async () => {
    const el = await mountWith([
      task("a.md", "A", "2026-04-12"),
      task("b.md", "B", "2026-04-12"),
    ]);
    const calls: ScheduleTarget[] = [];
    el.onSchedule = (t) => calls.push(t);
    const inst = el as unknown as DragEndInternals;
    const targetGi = inst._cachedGroups.findIndex(
      (g) => g.canonicalDate === "2026-04-15",
    );
    inst._drag = {
      sourceKey: ":a.md",
      sourceTask: el.tasks[0],
      sourceGroupIdx: 0,
      sourceTaskIdx: 0,
      ghostEl: null,
      handleEl: document.createElement("div"),
      taskListEl: document.createElement("div"),
      pointerId: 1,
      startY: 0,
      lastClientY: 0,
      active: true,
      dropMode: "schedule",
      dropGroupIdx: targetGi,
      dropInsertIdx: 0,
      scrollRaf: null,
      selectedKeys: new Set([":a.md", ":b.md"]),
    };
    inst._onDragEnd(fakeUpEvent());
    expect(calls).toHaveLength(1);
    expect(calls[0].date).toBe("2026-04-15");
    expect(calls[0].selectedKeys).toEqual([":a.md", ":b.md"]);
    document.body.removeChild(el);
  });

  // F8: `selectedKeys` arrives as a `Set<string>` (insertion-ordered
  // but unrelated to display order). The dispatch must project it
  // into display order via `_orderSelectedKeysByDisplay`. Seed the
  // Set with insertion order [B, A] (the reverse of display order)
  // and assert the dispatched array is [A, B] — pins the helper
  // specifically; would fail if the dispatch returned
  // `Array.from(drag.selectedKeys)` directly.
  it("multi-select schedule projects selectedKeys into display order, not insertion order", async () => {
    const el = await mountWith([
      task("a.md", "A", "2026-04-12"),
      task("b.md", "B", "2026-04-12"),
      task("c.md", "C", "2026-04-12"),
    ]);
    const calls: ScheduleTarget[] = [];
    el.onSchedule = (t) => calls.push(t);
    const inst = el as unknown as DragEndInternals;
    const targetGi = inst._cachedGroups.findIndex(
      (g) => g.canonicalDate === "2026-04-15",
    );
    // Insertion order [C, A, B] disagrees with display order [A, B, C].
    const insertionOrdered = new Set<string>([":c.md", ":a.md", ":b.md"]);
    inst._drag = {
      sourceKey: ":c.md",
      sourceTask: el.tasks[2],
      sourceGroupIdx: 0,
      sourceTaskIdx: 2,
      ghostEl: null,
      handleEl: document.createElement("div"),
      taskListEl: document.createElement("div"),
      pointerId: 1,
      startY: 0,
      lastClientY: 0,
      active: true,
      dropMode: "schedule",
      dropGroupIdx: targetGi,
      dropInsertIdx: 0,
      scrollRaf: null,
      selectedKeys: insertionOrdered,
    };
    inst._onDragEnd(fakeUpEvent());
    expect(calls).toHaveLength(1);
    expect(calls[0].selectedKeys).toEqual([":a.md", ":b.md", ":c.md"]);
    document.body.removeChild(el);
  });
});

/** Empty-input render contract (§3.3 flip): zero tasks + null
 *  snapshot now renders 7 group headers, not the retired
 *  `.empty-state` placeholder. */
describe("empty-input rendering (§3.3 flip)", () => {
  it("renders 7 group headers and no .empty-state for tasks=[] + frozenSnapshot=null", async () => {
    const el = document.createElement("brenn-todo-list") as BrennTodoList;
    el.visible = true;
    el.todayStr = "2026-04-12";
    el.tasks = [];
    el.slotState = new Map();
    el.frozenSnapshot = null;
    document.body.appendChild(el);
    await el.updateComplete;
    const headers = el.shadowRoot!.querySelectorAll(".group-header");
    expect(headers.length).toBe(7);
    expect(el.shadowRoot!.querySelector(".empty-state")).toBeNull();
    document.body.removeChild(el);
  });
});

/** Settled-tile rendering for the new schedule action (§5.1 #3). */
describe("settled tile (schedule)", () => {
  it("renders 📅 glyph + .settled-schedule class with 'Scheduled for ...' text", async () => {
    const el = document.createElement("brenn-todo-list") as BrennTodoList;
    el.visible = true;
    el.todayStr = "2026-04-12";
    el.tasks = [task("a.md", "A", "2026-04-12")];
    const snap = {
      todayStr: "2026-04-12",
      groups: groupTasksByDate(el.tasks, "2026-04-12"),
    };
    el.frozenSnapshot = snap;
    el.slotState = new Map<string, SlotState>([
      [
        ":a.md",
        {
          kind: "settled",
          action: "schedule",
          settledAt: Date.now(),
          tileText: "Scheduled for Apr 22",
          path: "a.md",
          repo: undefined,
          taskTldr: "A",
          targetEffectiveDate: "2026-04-22",
        },
      ],
    ]);
    document.body.appendChild(el);
    await el.updateComplete;
    const tile = el.shadowRoot!.querySelector(".task-row.settled");
    expect(tile).not.toBeNull();
    expect(tile!.classList.contains("settled-schedule")).toBe(true);
    expect(tile!.querySelector(".settled-glyph")!.textContent).toBe("📅");
    expect(tile!.querySelector(".settled-outcome")!.textContent).toBe(
      "Scheduled for Apr 22",
    );
    document.body.removeChild(el);
  });
});

/** Schedule-target heading highlight (§4.4): `.group-header.drop-target`
 *  is retired; the new affordance is `.group-header.schedule-target`,
 *  applied while `dropMode === "schedule"`. */
describe("schedule-target CSS retirement (§4.4)", () => {
  it("CSS does not declare a .group-header.drop-target rule", () => {
    const css = BrennTodoList.styles.cssText;
    expect(css).not.toMatch(/\.group-header\.drop-target/);
  });

  it("CSS declares a .group-header.schedule-target rule", () => {
    const css = BrennTodoList.styles.cssText;
    expect(css).toMatch(/\.group-header\.schedule-target\s*\{/);
  });
});

/** `_pendingLabel` for the new "schedule" action (§5.1 #4). */
describe("_pendingLabel schedule (§5.1 #4)", () => {
  it('"schedule" → "scheduling…"', () => {
    const el = document.createElement("brenn-todo-list") as BrennTodoList;
    const slotState = new Map<string, SlotState>();
    slotState.set(":foo.md", {
      kind: "pending",
      action: "schedule",
      startedAt: Date.now(),
      path: "foo.md",
      repo: undefined,
      taskTldr: "fixture",
      targetEffectiveDate: "2026-04-22",
    });
    el.slotState = slotState;
    expect((el as unknown as TodoListInternals)._pendingLabel(":foo.md")).toBe(
      "scheduling…",
    );
  });
});

/** Heading-drop integration tests (design.md §7) — exercise the wiring
 *  of `hitTestDrop` into `_updateDropTarget`, `_renderDropVisuals`'s
 *  visual-mutual-exclusion contract (§4.4), and the `_onDragEnd`
 *  branching from a real mounted component. happy-dom does not lay
 *  out elements, so each test stubs `getBoundingClientRect` on the
 *  task-list children to give the hit-test predictable geometry. */
describe("heading-drop integration (§7)", () => {
  interface DragInternals {
    _drag: {
      sourceKey: string;
      sourceTask: TodoItem;
      sourceGroupIdx: number;
      sourceTaskIdx: number;
      ghostEl: HTMLElement | null;
      handleEl: HTMLElement;
      taskListEl: HTMLElement;
      pointerId: number;
      startY: number;
      lastClientY: number;
      active: boolean;
      dropMode: "reorder" | "schedule";
      dropGroupIdx: number;
      dropInsertIdx: number;
      scrollRaf: number | null;
      selectedKeys: Set<string> | null;
    } | null;
    _onDragStart(
      e: PointerEvent,
      t: TodoItem,
      gi: number,
      ti: number,
    ): void;
    _onDragEnd(e: PointerEvent): void;
    _updateDropTarget(clientY: number): void;
    _cachedGroups: TaskGroup[];
  }

  /** Per-element rect specs for `stubRects`. `top` and `bottom` are
   *  what `getBoundingClientRect()` returns; the helper fills in
   *  `height = bottom - top` and zero left/right (we only care about Y). */
  interface RectSpec {
    el: Element;
    top: number;
    bottom: number;
  }

  /** Override `getBoundingClientRect` on each element in `specs` so
   *  `_updateDropTarget`'s rect math is deterministic in happy-dom. */
  function stubRects(specs: RectSpec[]): void {
    for (const s of specs) {
      const top = s.top;
      const bottom = s.bottom;
      Object.defineProperty(s.el, "getBoundingClientRect", {
        configurable: true,
        value: () =>
          ({
            top,
            bottom,
            height: bottom - top,
            left: 0,
            right: 0,
            width: 0,
            x: 0,
            y: top,
            toJSON: () => ({}),
          }) as DOMRect,
      });
    }
  }

  async function mountWith(tasks: TodoItem[]): Promise<BrennTodoList> {
    const el = document.createElement("brenn-todo-list") as BrennTodoList;
    el.visible = true;
    el.todayStr = "2026-04-12";
    el.tasks = tasks;
    el.slotState = new Map();
    el.frozenSnapshot = null;
    document.body.appendChild(el);
    await el.updateComplete;
    return el;
  }

  /** Build a minimal active-drag state on an already-rendered element.
   *  The `taskListEl` is read from the shadow DOM so `_updateDropTarget`'s
   *  `querySelectorAll` finds the same nodes the user would see. */
  function primeDrag(
    el: BrennTodoList,
    sourceKey: string,
    sourceTask: TodoItem,
    sourceGroupIdx: number,
    sourceTaskIdx: number,
    initialMode: "reorder" | "schedule" = "reorder",
  ): DragInternals {
    const taskList = el.shadowRoot!.querySelector(
      ".task-list",
    ) as HTMLElement;
    const inst = el as unknown as DragInternals;
    inst._drag = {
      sourceKey,
      sourceTask,
      sourceGroupIdx,
      sourceTaskIdx,
      ghostEl: null,
      handleEl: document.createElement("div"),
      taskListEl: taskList,
      pointerId: 1,
      startY: 0,
      lastClientY: 0,
      active: true,
      dropMode: initialMode,
      dropGroupIdx: sourceGroupIdx,
      dropInsertIdx: sourceTaskIdx,
      scrollRaf: null,
      selectedKeys: null,
    };
    return inst;
  }

  function fakeUpEvent(): PointerEvent {
    return new PointerEvent("pointerup", {
      bubbles: true,
      button: 0,
      pointerId: 1,
      clientX: 0,
      clientY: 0,
    });
  }

  function fakeDownEvent(handle: HTMLElement): PointerEvent {
    handle.setPointerCapture = () => {};
    const evt = new PointerEvent("pointerdown", {
      bubbles: true,
      button: 0,
      pointerId: 1,
      clientX: 0,
      clientY: 0,
    });
    Object.defineProperty(evt, "currentTarget", { value: handle });
    return evt;
  }

  /** Stub every `.group-header` and `.task-row` in `el`'s shadow DOM
   *  so the always-visible 7-day window's empty trailing headers
   *  cannot accidentally claim a stray `clientY` via their `+Infinity`
   *  band-end (the last header in DOM order owns `[bandTop, +Infinity)`).
   *
   *  Caller passes per-element overrides for the buckets they care
   *  about; everything else is parked in a far-below band that won't
   *  match realistic test pointer Y values. */
  function stubAllRowsAndHeaders(
    el: BrennTodoList,
    overrides: Map<HTMLElement, { top: number; bottom: number }>,
  ): void {
    const all = Array.from(
      el.shadowRoot!.querySelectorAll<HTMLElement>(
        ".task-row, .group-header",
      ),
    );
    const farBelowTop = 100_000;
    let parkY = farBelowTop;
    const specs: RectSpec[] = [];
    for (const node of all) {
      const ov = overrides.get(node);
      if (ov) {
        specs.push({ el: node, top: ov.top, bottom: ov.bottom });
      } else {
        // Park each unstubbed element in its own non-overlapping
        // 10-px band starting at 100_000. Realistic test pointer Ys
        // (0..1000) never reach this region.
        specs.push({ el: node, top: parkY, bottom: parkY + 10 });
        parkY += 10;
      }
    }
    stubRects(specs);
  }

  /** Geometry helper: stub the TODAY heading + its single task row, then
   *  pin the empty TOMORROW heading directly below.
   *
   *  Layout (Y coords):
   *    [0,   20)   TODAY heading
   *    [20,  60)   TODAY task row (midY=40)
   *    [60,  80)   TOMORROW heading (empty bucket)
   *    [80, 100)   WED heading
   *    [100_000+)  remaining empty WEEKDAYs parked far below */
  function stubTodayTomorrowGeometry(el: BrennTodoList): void {
    const todayHeader = el.shadowRoot!.querySelector(
      '.group-header[data-group-idx="0"]',
    ) as HTMLElement;
    const todayRow = el.shadowRoot!.querySelector(
      '.task-row[data-group-idx="0"][data-task-idx="0"]',
    ) as HTMLElement;
    const tomorrowHeader = el.shadowRoot!.querySelector(
      '.group-header[data-group-idx="1"]',
    ) as HTMLElement;
    const wedHeader = el.shadowRoot!.querySelector(
      '.group-header[data-group-idx="2"]',
    ) as HTMLElement;
    expect(todayHeader).not.toBeNull();
    expect(todayRow).not.toBeNull();
    expect(tomorrowHeader).not.toBeNull();
    expect(wedHeader).not.toBeNull();
    const overrides = new Map<HTMLElement, { top: number; bottom: number }>();
    overrides.set(todayHeader, { top: 0, bottom: 20 });
    overrides.set(todayRow, { top: 20, bottom: 60 });
    overrides.set(tomorrowHeader, { top: 60, bottom: 80 });
    overrides.set(wedHeader, { top: 80, bottom: 100 });
    stubAllRowsAndHeaders(el, overrides);
  }

  // F1: row → header flip at equal `(gi, insertIdx)` triggers
  // `_renderDropVisuals` because the change-detection guard at §4.2
  // includes `dropMode`. Pointer over TODAY's first task (reorder,
  // gi=TODAY, insertIdx=0) → pointer flips into TODAY's heading band
  // (schedule, gi=TODAY) — DOM must transition: drop-indicator gone,
  // schedule-target present.

  it("row → heading flip at equal (gi, insertIdx) re-renders visuals (§4.2)", async () => {
    // Two-task TODAY bucket so the source is at ti=1 — the row hit
    // at gi=0/ti=0 is a real change relative to source, forcing
    // `_renderDropVisuals` to fire on the first `_updateDropTarget`.
    const el = await mountWith([
      task("a.md", "A", "2026-04-12"),
      task("b.md", "B", "2026-04-12"),
    ]);
    const todayHeader = el.shadowRoot!.querySelector(
      '.group-header[data-group-idx="0"]',
    ) as HTMLElement;
    const rowA = el.shadowRoot!.querySelector(
      '.task-row[data-group-idx="0"][data-task-idx="0"]',
    ) as HTMLElement;
    const rowB = el.shadowRoot!.querySelector(
      '.task-row[data-group-idx="0"][data-task-idx="1"]',
    ) as HTMLElement;
    const tomorrowHeader = el.shadowRoot!.querySelector(
      '.group-header[data-group-idx="1"]',
    ) as HTMLElement;
    const overrides = new Map<HTMLElement, { top: number; bottom: number }>();
    overrides.set(todayHeader, { top: 0, bottom: 20 });
    overrides.set(rowA, { top: 20, bottom: 60 }); // midY=40
    overrides.set(rowB, { top: 60, bottom: 100 }); // midY=80
    overrides.set(tomorrowHeader, { top: 100, bottom: 120 });
    stubAllRowsAndHeaders(el, overrides);
    // Source = task B at gi=0, ti=1. Pointer y=25 over rowA above its
    // midpoint=40 → reorder, gi=0, insertIdx=0. (Different from
    // source's initial dropInsertIdx=1, so _renderDropVisuals fires.)
    const inst = primeDrag(el, ":b.md", el.tasks[1], 0, 1);
    inst._updateDropTarget(25);
    expect(inst._drag!.dropMode).toBe("reorder");
    expect(inst._drag!.dropGroupIdx).toBe(0);
    expect(inst._drag!.dropInsertIdx).toBe(0);
    // Reorder visuals: exactly one .drop-indicator, no .schedule-target.
    expect(el.shadowRoot!.querySelectorAll(".drop-indicator")).toHaveLength(1);
    expect(
      el.shadowRoot!.querySelectorAll(".group-header.schedule-target"),
    ).toHaveLength(0);

    // Flip the pointer up into TODAY's heading band (y=10). Same gi,
    // same insertIdx (0) — only `dropMode` changes. The change-
    // detection guard at §4.2 includes `dropMode`, so visuals must
    // re-render: drop-indicator gone, schedule-target appears.
    inst._updateDropTarget(10);
    expect(inst._drag!.dropMode).toBe("schedule");
    expect(inst._drag!.dropGroupIdx).toBe(0);
    expect(el.shadowRoot!.querySelectorAll(".drop-indicator")).toHaveLength(0);
    const targetHeaders = el.shadowRoot!.querySelectorAll(
      ".group-header.schedule-target",
    );
    expect(targetHeaders).toHaveLength(1);
    expect(
      (targetHeaders[0] as HTMLElement).dataset.groupIdx,
    ).toBe("0");
    document.body.removeChild(el);
  });

  it("heading → row reverse flip at equal (gi, insertIdx) re-renders visuals (§4.2)", async () => {
    // Two-task TODAY bucket. Source is task B at ti=1; the schedule
    // hit and the row hit both differ from the source's initial
    // dropInsertIdx, so `_renderDropVisuals` fires on each call.
    const el = await mountWith([
      task("a.md", "A", "2026-04-12"),
      task("b.md", "B", "2026-04-12"),
    ]);
    const todayHeader = el.shadowRoot!.querySelector(
      '.group-header[data-group-idx="0"]',
    ) as HTMLElement;
    const rowA = el.shadowRoot!.querySelector(
      '.task-row[data-group-idx="0"][data-task-idx="0"]',
    ) as HTMLElement;
    const rowB = el.shadowRoot!.querySelector(
      '.task-row[data-group-idx="0"][data-task-idx="1"]',
    ) as HTMLElement;
    const tomorrowHeader = el.shadowRoot!.querySelector(
      '.group-header[data-group-idx="1"]',
    ) as HTMLElement;
    const overrides = new Map<HTMLElement, { top: number; bottom: number }>();
    overrides.set(todayHeader, { top: 0, bottom: 20 });
    overrides.set(rowA, { top: 20, bottom: 60 }); // midY=40
    overrides.set(rowB, { top: 60, bottom: 100 }); // midY=80
    overrides.set(tomorrowHeader, { top: 100, bottom: 120 });
    stubAllRowsAndHeaders(el, overrides);
    const inst = primeDrag(el, ":b.md", el.tasks[1], 0, 1);
    // First land in TODAY's heading band (y=10) → schedule on gi=0.
    inst._updateDropTarget(10);
    expect(inst._drag!.dropMode).toBe("schedule");
    expect(
      el.shadowRoot!.querySelectorAll(".group-header.schedule-target"),
    ).toHaveLength(1);
    expect(el.shadowRoot!.querySelectorAll(".drop-indicator")).toHaveLength(0);
    // Now move down into rowA, above midY=40 → reorder, gi=0,
    // insertIdx=0. Mode flipped from schedule → reorder; visuals
    // must re-render even though gi is unchanged (insertIdx
    // wasn't tracked under schedule, but `dropMode` is the
    // discriminator that forces the render per §4.2).
    inst._updateDropTarget(25);
    expect(inst._drag!.dropMode).toBe("reorder");
    expect(inst._drag!.dropGroupIdx).toBe(0);
    expect(inst._drag!.dropInsertIdx).toBe(0);
    expect(el.shadowRoot!.querySelectorAll(".drop-indicator")).toHaveLength(1);
    expect(
      el.shadowRoot!.querySelectorAll(".group-header.schedule-target"),
    ).toHaveLength(0);
    document.body.removeChild(el);
  });

  // F2: visual-indicator mutual exclusion (DOM-state). The static CSS
  // check covers "rule absent"; these two cover "DOM never has both".

  it("on heading hit: zero .drop-indicator, exactly one .schedule-target (§7)", async () => {
    const el = await mountWith([task("a.md", "A", "2026-04-12")]);
    stubTodayTomorrowGeometry(el);
    const inst = primeDrag(el, ":a.md", el.tasks[0], 0, 0);
    // y=70 lands in the empty TOMORROW heading band [60, 80).
    inst._updateDropTarget(70);
    expect(inst._drag!.dropMode).toBe("schedule");
    expect(el.shadowRoot!.querySelectorAll(".drop-indicator")).toHaveLength(0);
    expect(
      el.shadowRoot!.querySelectorAll(".group-header.schedule-target"),
    ).toHaveLength(1);
    // .group-header.drop-target (the retired class) is never present.
    expect(
      el.shadowRoot!.querySelectorAll(".group-header.drop-target"),
    ).toHaveLength(0);
    document.body.removeChild(el);
  });

  it("on row hit: exactly one .drop-indicator, zero .schedule-target (§7)", async () => {
    // Two-task TODAY bucket so a row hit can target ti>0 cleanly. The
    // user drags task A (gi=0, ti=0) and the pointer lands on task B's
    // bottom half (insertIdx=2 — past the end of the visible group).
    const el = await mountWith([
      task("a.md", "A", "2026-04-12"),
      task("b.md", "B", "2026-04-12"),
    ]);
    const todayHeader = el.shadowRoot!.querySelector(
      '.group-header[data-group-idx="0"]',
    ) as HTMLElement;
    const rowA = el.shadowRoot!.querySelector(
      '.task-row[data-group-idx="0"][data-task-idx="0"]',
    ) as HTMLElement;
    const rowB = el.shadowRoot!.querySelector(
      '.task-row[data-group-idx="0"][data-task-idx="1"]',
    ) as HTMLElement;
    const tomorrowHeader = el.shadowRoot!.querySelector(
      '.group-header[data-group-idx="1"]',
    ) as HTMLElement;
    const overrides = new Map<HTMLElement, { top: number; bottom: number }>();
    overrides.set(todayHeader, { top: 0, bottom: 20 });
    overrides.set(rowA, { top: 20, bottom: 60 }); // midY=40
    overrides.set(rowB, { top: 60, bottom: 100 }); // midY=80
    overrides.set(tomorrowHeader, { top: 100, bottom: 120 });
    stubAllRowsAndHeaders(el, overrides);
    const inst = primeDrag(el, ":a.md", el.tasks[0], 0, 0);
    // y=90: inside row B, below midY=80 → reorder, gi=0, insertIdx=2.
    inst._updateDropTarget(90);
    expect(inst._drag!.dropMode).toBe("reorder");
    expect(el.shadowRoot!.querySelectorAll(".drop-indicator")).toHaveLength(1);
    expect(
      el.shadowRoot!.querySelectorAll(".group-header.schedule-target"),
    ).toHaveLength(0);
    expect(
      el.shadowRoot!.querySelectorAll(".group-header.drop-target"),
    ).toHaveLength(0);
    document.body.removeChild(el);
  });

  // F3: EARLIER heading from a TODAY source → no schedule-target
  // highlight, no schedule dispatch (rejected by canonicalDate ===
  // null at the hit-test level).

  it("EARLIER heading hit from TODAY source: no schedule visual, no schedule dispatch (§4.1)", async () => {
    // Build a list with one EARLIER task and one TODAY task. The
    // resulting groups are: [EARLIER (gi=0), TODAY (gi=1), TOMORROW
    // (gi=2), 5 weekdays...]. EARLIER's canonicalDate === null, so the
    // hit-test rejects it as a schedule candidate.
    const el = await mountWith([
      task("e.md", "Earlier", "2026-04-08"),
      task("t.md", "Today", "2026-04-12"),
    ]);
    const earlierHeader = el.shadowRoot!.querySelector(
      '.group-header[data-group-idx="0"]',
    ) as HTMLElement;
    const earlierRow = el.shadowRoot!.querySelector(
      '.task-row[data-group-idx="0"][data-task-idx="0"]',
    ) as HTMLElement;
    const todayHeader = el.shadowRoot!.querySelector(
      '.group-header[data-group-idx="1"]',
    ) as HTMLElement;
    const todayRow = el.shadowRoot!.querySelector(
      '.task-row[data-group-idx="1"][data-task-idx="0"]',
    ) as HTMLElement;
    const overrides = new Map<HTMLElement, { top: number; bottom: number }>();
    overrides.set(earlierHeader, { top: 0, bottom: 20 });
    overrides.set(earlierRow, { top: 20, bottom: 60 }); // midY=40
    overrides.set(todayHeader, { top: 60, bottom: 80 });
    overrides.set(todayRow, { top: 80, bottom: 120 }); // midY=100
    stubAllRowsAndHeaders(el, overrides);
    const calls: ScheduleTarget[] = [];
    el.onSchedule = (t) => calls.push(t);
    // Drag from TODAY (gi=1, ti=0).
    const inst = primeDrag(el, ":t.md", el.tasks[1], 1, 0);
    // y=10: inside EARLIER heading band [0, 20). Heading rejected
    // (canonicalDate=null), row pass picks earlierRow's gi=0. But
    // EARLIER row is excluded from candidates by the caller's
    // pseudo-bucket pre-filter (isDroppable rejects cross-source
    // drops INTO earlier). Result: no candidate → drag state
    // unchanged (still reorder/sourceGi/sourceTi).
    inst._updateDropTarget(10);
    expect(inst._drag!.dropMode).toBe("reorder");
    expect(inst._drag!.dropGroupIdx).toBe(1);
    // No schedule-target on any header.
    expect(
      el.shadowRoot!.querySelectorAll(".group-header.schedule-target"),
    ).toHaveLength(0);
    // End the drag — schedule dispatch must NOT fire.
    inst._onDragEnd(fakeUpEvent());
    expect(calls).toHaveLength(0);
    document.body.removeChild(el);
  });

  // F4: pre-existing OVERDUE drag-start gate (DRAG_LOCKED_SOURCE_HEADER_CLASSES)
  // — design listed this as a sanity-check regression. A pointerdown
  // on the drag handle of an OVERDUE row must not begin a drag.

  it("OVERDUE drag-start gate: pointerdown on handle does not begin a drag (§7 regression)", async () => {
    const el = await mountWith([
      task("a.md", "Overdue", "2026-04-10", { due_date: "2026-04-10" }),
    ]);
    const inst = el as unknown as DragInternals;
    expect(inst._drag).toBeNull();
    const handle = el.shadowRoot!.querySelector(
      ".drag-handle",
    ) as HTMLElement;
    inst._onDragStart(fakeDownEvent(handle), el.tasks[0], 0, 0);
    expect(inst._drag).toBeNull();
    document.body.removeChild(el);
  });

  // F5: pointer over a task row in a populated bucket → reorder
  // dispatch (not schedule). Verifies the not-taken branch of
  // `_onDragEnd`'s new dropMode switch still routes through onReorder.

  it("row hit: drop fires onReorder (not onSchedule) (§7)", async () => {
    const el = await mountWith([
      task("a.md", "A", "2026-04-12"),
      task("b.md", "B", "2026-04-12"),
    ]);
    const todayHeader = el.shadowRoot!.querySelector(
      '.group-header[data-group-idx="0"]',
    ) as HTMLElement;
    const rowA = el.shadowRoot!.querySelector(
      '.task-row[data-group-idx="0"][data-task-idx="0"]',
    ) as HTMLElement;
    const rowB = el.shadowRoot!.querySelector(
      '.task-row[data-group-idx="0"][data-task-idx="1"]',
    ) as HTMLElement;
    const tomorrowHeader = el.shadowRoot!.querySelector(
      '.group-header[data-group-idx="1"]',
    ) as HTMLElement;
    const overrides = new Map<HTMLElement, { top: number; bottom: number }>();
    overrides.set(todayHeader, { top: 0, bottom: 20 });
    overrides.set(rowA, { top: 20, bottom: 60 }); // midY=40
    overrides.set(rowB, { top: 60, bottom: 100 }); // midY=80
    overrides.set(tomorrowHeader, { top: 100, bottom: 120 });
    stubAllRowsAndHeaders(el, overrides);
    const reorderCalls: { path: string; targetGroupDate: string | null }[] = [];
    const scheduleCalls: ScheduleTarget[] = [];
    el.onReorder = (t) => reorderCalls.push({
      path: t.path,
      targetGroupDate: t.targetGroupDate,
    });
    el.onSchedule = (t) => scheduleCalls.push(t);
    // Drag A (gi=0, ti=0). Move pointer over B's bottom half →
    // reorder, gi=0, insertIdx=2 (after B).
    const inst = primeDrag(el, ":a.md", el.tasks[0], 0, 0);
    inst._updateDropTarget(90);
    expect(inst._drag!.dropMode).toBe("reorder");
    inst._onDragEnd(fakeUpEvent());
    expect(scheduleCalls).toHaveLength(0);
    expect(reorderCalls).toHaveLength(1);
    expect(reorderCalls[0].path).toBe("a.md");
    expect(reorderCalls[0].targetGroupDate).toBe("2026-04-12");
    document.body.removeChild(el);
  });
});

/** F6: mixed-source multi-select schedule. Two of three selected tasks
 *  already live on the target date; the third does not. Per design
 *  §4.6 the brenn-app handler dispatches all 3 messages — graf is
 *  idempotent on the same-date cases, and per-task no-op filtering
 *  at the brenn layer is unnecessary. This test pins that decision so
 *  a future "optimization" that filters per-task in the brenn-app
 *  handler fails here. */
describe("mixed-source multi-select schedule (§4.6)", () => {
  // Lives in this file rather than app-debounce-toast.test.ts because
  // the assertion is about the brenn-todo-list dispatch (`onSchedule`
  // gets called once with all selected keys); the brenn-app handler's
  // 3-message expansion is already covered in app-debounce-toast.

  interface DragEndInternals {
    _drag: {
      sourceKey: string;
      sourceTask: TodoItem;
      sourceGroupIdx: number;
      sourceTaskIdx: number;
      ghostEl: HTMLElement | null;
      handleEl: HTMLElement;
      taskListEl: HTMLElement;
      pointerId: number;
      startY: number;
      lastClientY: number;
      active: boolean;
      dropMode: "reorder" | "schedule";
      dropGroupIdx: number;
      dropInsertIdx: number;
      scrollRaf: number | null;
      selectedKeys: Set<string> | null;
    } | null;
    _onDragEnd(e: PointerEvent): void;
    _cachedGroups: TaskGroup[];
  }

  async function mountWith(tasks: TodoItem[]): Promise<BrennTodoList> {
    const el = document.createElement("brenn-todo-list") as BrennTodoList;
    el.visible = true;
    el.todayStr = "2026-04-12";
    el.tasks = tasks;
    el.slotState = new Map();
    el.frozenSnapshot = null;
    document.body.appendChild(el);
    await el.updateComplete;
    return el;
  }

  function fakeUpEvent(): PointerEvent {
    return new PointerEvent("pointerup", {
      bubbles: true,
      button: 0,
      pointerId: 1,
      clientX: 0,
      clientY: 0,
    });
  }

  it("mixed source dates: dispatch fires for all selected keys (no per-task filter)", async () => {
    // 3 selected tasks: A on 2026-04-12, B on 2026-04-12, C on
    // 2026-04-13. Drop target is 2026-04-12 (TODAY's heading) — A and
    // B are already there, C is not. Design contract: dispatch fires
    // once with all 3 keys; the brenn-app handler then sends 3
    // TodoSchedule messages, relying on graf idempotency for A and B.
    const el = await mountWith([
      task("a.md", "A", "2026-04-12"),
      task("b.md", "B", "2026-04-12"),
      task("c.md", "C", "2026-04-13"),
    ]);
    const calls: ScheduleTarget[] = [];
    el.onSchedule = (t) => calls.push(t);
    const inst = el as unknown as DragEndInternals;
    const todayGi = inst._cachedGroups.findIndex(
      (g) => g.canonicalDate === "2026-04-12",
    );
    expect(todayGi).toBeGreaterThanOrEqual(0);
    // Drag captured by the pointer-task is C — the task NOT already
    // on the target. selectedKeys covers all three. Note: when
    // sourceTask.effective_date !== targetDate, the single-task
    // no-op gate is bypassed. selectedKeys non-null means the multi-
    // select code path runs; targetDate equality with sourceTask is
    // not checked in that branch.
    inst._drag = {
      sourceKey: ":c.md",
      sourceTask: el.tasks[2],
      sourceGroupIdx: 0,
      sourceTaskIdx: 0,
      ghostEl: null,
      handleEl: document.createElement("div"),
      taskListEl: document.createElement("div"),
      pointerId: 1,
      startY: 0,
      lastClientY: 0,
      active: true,
      dropMode: "schedule",
      dropGroupIdx: todayGi,
      dropInsertIdx: 0,
      scrollRaf: null,
      selectedKeys: new Set([":a.md", ":b.md", ":c.md"]),
    };
    inst._onDragEnd(fakeUpEvent());
    expect(calls).toHaveLength(1);
    expect(calls[0].date).toBe("2026-04-12");
    expect(calls[0].selectedKeys).toEqual([":a.md", ":b.md", ":c.md"]);
    document.body.removeChild(el);
  });
});

// ---------------------------------------------------------------------------
// Snooze split-button DOM tests (phase-4-split-button-dom-tests).
// ---------------------------------------------------------------------------
//
// Drive the caret + menu interactions: keyboard open/close, outside-click
// dismiss, aria-expanded state. `_positionSnoozeMenu` reads
// `getBoundingClientRect()` (stubbed) and `_mobileMenuSheet` reads
// `window.matchMedia`. Stubs keep the tests environment-independent.

describe("snooze split-button DOM interactions", () => {
  /** Minimal task fixture for rendering a split-button row. */
  function singleTask(): BrennTodoList {
    const el = document.createElement("brenn-todo-list") as BrennTodoList;
    el.visible = true;
    el.todayStr = "2026-04-12";
    el.tasks = [task("a.md", "Alpha task", "2026-04-12")];
    el.slotState = new Map();
    el.frozenSnapshot = null;
    document.body.appendChild(el);
    return el;
  }

  afterEach(() => {
    document.body.replaceChildren();
    // Restore matchMedia if overridden.
    // We always override per-test rather than globally.
  });

  /** Stub matchMedia to return a coarse-pointer match (mobile sheet). */
  function stubCoarsePointer(coarse: boolean): void {
    Object.defineProperty(window, "matchMedia", {
      writable: true,
      configurable: true,
      value: (q: string) => ({
        matches: q === "(pointer: coarse)" ? coarse : false,
        media: q,
        addEventListener: () => {},
        removeEventListener: () => {},
        dispatchEvent: () => false,
      }),
    });
  }

  /** Stub getBoundingClientRect to return a non-zero rect so positioning
   *  doesn't try to clamp a zero-area caret. */
  function stubBoundingRect(el: Element): void {
    (el as HTMLElement).getBoundingClientRect = () => ({
      top: 100, left: 100, right: 200, bottom: 120,
      width: 100, height: 20, x: 100, y: 100,
      toJSON: () => "{}",
    });
  }

  it("caret click opens the menu (aria-expanded = true, menu present)", async () => {
    stubCoarsePointer(false);
    const el = singleTask();
    await el.updateComplete;

    const root = el.shadowRoot!;
    const caret = root.querySelector(".snooze-caret") as HTMLButtonElement | null;
    expect(caret).toBeTruthy();
    expect(caret!.getAttribute("aria-expanded")).toBe("false");

    // Stub bounding rect before clicking so _positionSnoozeMenu won't throw.
    stubBoundingRect(caret!);

    caret!.click();
    await el.updateComplete;
    // Wait for the updateComplete.then() inside _openSnoozeMenu.
    await el.updateComplete;

    expect(caret!.getAttribute("aria-expanded")).toBe("true");
    expect(root.querySelector(".snooze-menu")).toBeTruthy();
  });

  it("caret click again closes the menu (toggle behavior)", async () => {
    stubCoarsePointer(false);
    const el = singleTask();
    await el.updateComplete;

    const root = el.shadowRoot!;
    const caret = root.querySelector(".snooze-caret") as HTMLButtonElement;
    stubBoundingRect(caret);
    caret.click();
    await el.updateComplete;
    await el.updateComplete;

    expect(caret.getAttribute("aria-expanded")).toBe("true");

    caret.click();
    await el.updateComplete;

    expect(caret.getAttribute("aria-expanded")).toBe("false");
    expect(root.querySelector(".snooze-menu")).toBeFalsy();
  });

  it("ArrowDown on caret opens the menu", async () => {
    stubCoarsePointer(false);
    const el = singleTask();
    await el.updateComplete;

    const root = el.shadowRoot!;
    const caret = root.querySelector(".snooze-caret") as HTMLButtonElement;
    stubBoundingRect(caret);

    caret.dispatchEvent(
      new KeyboardEvent("keydown", { key: "ArrowDown", bubbles: true }),
    );
    await el.updateComplete;
    await el.updateComplete;

    expect(caret.getAttribute("aria-expanded")).toBe("true");
  });

  it("Escape on caret (when menu is open) closes the menu", async () => {
    stubCoarsePointer(false);
    const el = singleTask();
    await el.updateComplete;

    const root = el.shadowRoot!;
    const caret = root.querySelector(".snooze-caret") as HTMLButtonElement;
    stubBoundingRect(caret);
    caret.click();
    await el.updateComplete;
    await el.updateComplete;

    caret.dispatchEvent(
      new KeyboardEvent("keydown", { key: "Escape", bubbles: true }),
    );
    await el.updateComplete;

    expect(caret.getAttribute("aria-expanded")).toBe("false");
    expect(root.querySelector(".snooze-menu")).toBeFalsy();
  });

  it("mobile sheet breakpoint: menu renders with mobile-sheet class", async () => {
    stubCoarsePointer(true); // coarse pointer → _mobileMenuSheet() returns true
    const el = singleTask();
    await el.updateComplete;

    const root = el.shadowRoot!;
    const caret = root.querySelector(".snooze-caret") as HTMLButtonElement;
    stubBoundingRect(caret);
    caret.click();
    await el.updateComplete;
    await el.updateComplete;

    const menu = root.querySelector(".snooze-menu");
    expect(menu).toBeTruthy();
    // When _snoozeMenuAsSheet is true, the menu gets the sheet CSS class.
    expect(menu!.classList.contains("snooze-menu-sheet")).toBe(true);
  });

  it("outside-click on document closes the menu", async () => {
    stubCoarsePointer(false);
    const el = singleTask();
    await el.updateComplete;

    const root = el.shadowRoot!;
    const caret = root.querySelector(".snooze-caret") as HTMLButtonElement;
    stubBoundingRect(caret);
    caret.click();
    await el.updateComplete;
    await el.updateComplete;

    expect(caret.getAttribute("aria-expanded")).toBe("true");

    // Fire a pointerdown outside the menu (on document.body).
    document.body.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true }));
    await el.updateComplete;

    expect(caret.getAttribute("aria-expanded")).toBe("false");
    expect(root.querySelector(".snooze-menu")).toBeFalsy();
  });

  // _onMenuKeydown tests (snooze-menu-keydown-tests).
  // ArrowDown/Up focus traversal, Enter/Space select+close, Tab close.

  it("ArrowDown in open menu moves focus to the next menu item", async () => {
    stubCoarsePointer(false);
    const el = singleTask();
    await el.updateComplete;

    const root = el.shadowRoot!;
    const caret = root.querySelector(".snooze-caret") as HTMLButtonElement;
    stubBoundingRect(caret);
    caret.click();
    await el.updateComplete;
    await el.updateComplete;

    // Menu is open; get items and focus the first explicitly.
    const menu = root.querySelector(".snooze-menu") as HTMLElement;
    expect(menu).toBeTruthy();
    const items = Array.from(menu.querySelectorAll<HTMLElement>('[role="menuitem"]'));
    expect(items.length).toBeGreaterThan(1);
    items[0].focus();

    // Dispatch ArrowDown from items[0] — it bubbles to the <ul> where
    // @keydown is attached. _onMenuKeydown reads data-menu-idx from e.target
    // (the <li>), so idx=0 → _focusMenuItem(1) → items[1] gets focus.
    items[0].dispatchEvent(
      new KeyboardEvent("keydown", { key: "ArrowDown", bubbles: true }),
    );
    await el.updateComplete;

    // happy-dom: focused element within shadow root is visible via
    // shadowRoot.activeElement.
    expect(root.activeElement).toBe(items[1]);
  });

  it("Enter on a menu item selects it and closes the menu", async () => {
    stubCoarsePointer(false);
    const el = singleTask();
    await el.updateComplete;

    // Attach an onSnooze spy so we can confirm the selection landed.
    const snoozeSpy = vi.fn();
    el.onSnooze = snoozeSpy;

    const root = el.shadowRoot!;
    const caret = root.querySelector(".snooze-caret") as HTMLButtonElement;
    stubBoundingRect(caret);
    caret.click();
    await el.updateComplete;
    await el.updateComplete;

    const menu = root.querySelector(".snooze-menu") as HTMLElement;
    const items = Array.from(menu.querySelectorAll<HTMLElement>('[role="menuitem"]'));
    // Focus item 0 (data-menu-idx="0" → SNOOZE_MENU_ENTRIES[0].days).
    items[0].focus();

    // Dispatch from items[0] so e.target carries data-menu-idx="0" and
    // _onMenuKeydown's idx resolution works correctly.
    items[0].dispatchEvent(
      new KeyboardEvent("keydown", { key: "Enter", bubbles: true }),
    );
    await el.updateComplete;

    // Menu must be closed after Enter.
    expect(root.querySelector(".snooze-menu")).toBeFalsy();
    // onSnooze must have been called.
    expect(snoozeSpy).toHaveBeenCalled();
  });

  it("Tab in open menu closes the menu without triggering onSnooze", async () => {
    stubCoarsePointer(false);
    const el = singleTask();
    await el.updateComplete;

    const snoozeSpy = vi.fn();
    el.onSnooze = snoozeSpy;

    const root = el.shadowRoot!;
    const caret = root.querySelector(".snooze-caret") as HTMLButtonElement;
    stubBoundingRect(caret);
    caret.click();
    await el.updateComplete;
    await el.updateComplete;

    const menu = root.querySelector(".snooze-menu") as HTMLElement;
    menu.dispatchEvent(
      new KeyboardEvent("keydown", { key: "Tab", bubbles: true }),
    );
    await el.updateComplete;

    expect(root.querySelector(".snooze-menu")).toBeFalsy();
    expect(snoozeSpy).not.toHaveBeenCalled();
  });
});
