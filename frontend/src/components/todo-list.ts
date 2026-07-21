/**
 * <brenn-todo-list> — Task list pane for the graf todo integration.
 *
 * Shadow DOM for style encapsulation. Displays tasks grouped by effective
 * date. Done and snooze buttons dispatch callbacks to brenn-app which
 * sends WS mutations.
 *
 * Pure renderer — parent owns state, component renders and calls callbacks.
 */

import { LitElement, html, css, nothing } from "lit";
import { customElement, property, state } from "lit/decorators.js";
import type { TodoItem } from "../generated/TodoItem.js";
import type { TodoAnchor } from "../generated/TodoAnchor.js";
import {
  localTodayStr,
  addDays,
  shortDate,
  snoozeTargetDate,
} from "../date-util.js";
import { MenuController } from "./menu-controller.js";

/** Compute a unique key for a task (handles multi-repo). */
export function todoKey(path: string, repo?: string | null): string {
  return `${repo ?? ""}:${path}`;
}

/** A group of tasks sharing a date header. */
export interface TaskGroup {
  header: string;
  headerClass: string;
  /** Canonical date for this group (YYYY-MM-DD). Null for OVERDUE (it's
   * a bucket of heterogeneous past due dates, so no single canonical
   * value). All other sections have a concrete date. */
  canonicalDate: string | null;
  tasks: TodoItem[];
}

/** Info about the target bucket for a heading-drop schedule. Heading-drop
 *  drops fire `TodoSchedule` instead of `TodoReorder`: they pin the
 *  task to a date but do not write `sort_order`, so the task ranks
 *  naturally in its new bucket via `COALESCE(sort_order, priority,
 *  UNRANKED)` (see graf's `todo_schedule`). See design.md §4. */
export interface ScheduleTarget {
  /** Path of the primary task (for single-task drag, the dragged task;
   *  for multi-select, the pointer-captured task). */
  path: string;
  repo?: string;
  /** Target bucket's canonical date. Never null (heading drops on
   *  null-canonicalDate buckets are filtered by hit-test §4.1). */
  date: string;
  /** Display-ordered selected keys for multi-select schedule. Null for
   *  single-task drag. Caller iterates this for actual dispatches. */
  selectedKeys: string[] | null;
}

/** Info about the target position for a drop. */
export interface ReorderTarget {
  /** Path of the primary (pointer-captured) task. */
  path: string;
  /** Repo of the primary task. */
  repo?: string;
  /** The neighbor above the drop position, or null if dropping at start of group. */
  after: TodoAnchor | null;
  /** The neighbor below the drop position, or null if dropping at end of group. */
  before: TodoAnchor | null;
  /** Canonical date of the target group (for optimistic effective_date update). */
  targetGroupDate: string | null;
  /** Keys of all selected tasks in display order. Null for single-task drag.
   *  When non-null, `path`/`repo` are the primary task (used for no-op
   *  detection) but the caller iterates `selectedKeys` for the actual
   *  reorder operations. */
  selectedKeys: string[] | null;
}

/** Get the weekday name for a YYYY-MM-DD string. */
function weekdayName(dateStr: string): string {
  const d = new Date(dateStr + "T00:00:00");
  return d.toLocaleDateString(undefined, { weekday: "long" });
}

/** Phase 4 snooze-menu offsets (+3d / +1w / +1m). Single source of truth
 * shared by the menu render and the keyboard-select handler; adding or
 * reordering entries here automatically updates both. */
export const SNOOZE_MENU_ENTRIES: readonly {
  days: number;
  label: string;
}[] = [
  { days: 3, label: "+3 days" },
  { days: 7, label: "+1 week" },
  { days: 30, label: "+1 month" },
] as const;

/** Settled-tile glyph + class lookup for the three settled action
 *  variants. Centralized so adding a new variant lands in one place
 *  (CSS rule + this table + the `SlotState.action` union). */
export const SETTLED_TILE_LOOKS: Readonly<{
  done: { glyph: string; className: string };
  snooze: { glyph: string; className: string };
  schedule: { glyph: string; className: string };
}> = {
  done: { glyph: "✓", className: "settled-done" },
  snooze: { glyph: "💤", className: "settled-snooze" },
  schedule: { glyph: "📅", className: "settled-schedule" },
} as const;

/** Discriminated set of in-flight action kinds on a todo row.
 *
 * Referenced by both `<brenn-app>` (as the `todoSlotState` pending variant)
 * and `<brenn-todo-list>` (as the `_pendingLabel` switch). Central
 * definition so a new action kind lands in one place. */
export type TodoPendingAction = "done" | "snooze" | "reorder" | "schedule";

/** Snapshot fields captured at dispatch time and carried through the
 *  pending → settled transition unchanged. Kept verbatim on both variants
 *  so the renderer never has to re-find the task in `todoTasks` (the task
 *  may have been removed from the live list by the time we render the
 *  settled tile). */
export interface SlotSnapshot {
  /** Path + repo stored directly (not derived from the map key) so we
   *  never need to reverse `todoKey`'s encoding (which isn't
   *  unambiguously reversible when `path` contains a `:`). */
  path: string;
  repo: string | undefined;
  /** Task's tldr at dispatch, used for the settled-tile text. */
  taskTldr: string;
  /** For snooze: the date the mutation asked graf to land on — used
   *  by the settled-tile text ("Snoozed to MM/DD"). Undefined for
   *  done/reorder. */
  targetEffectiveDate?: string;
}

/** Per-slot state tracking a row from dispatch through settlement.
 *
 * A slot is keyed by `todoKey(path, repo)` — identical to the existing
 * pending-key scheme. The discriminated union lets `<brenn-todo-list>`
 * render either a greyed pending row (action buttons hidden, label
 * visible) or a confirmation tile (replaces the row in-place after a
 * successful done/snooze ack). */
export type SlotState =
  | ({ kind: "pending"; action: TodoPendingAction; startedAt: number }
     & SlotSnapshot)
  | ({ kind: "settled"; action: "done" | "snooze" | "schedule"; settledAt: number;
       /** For done: "Next: MM/DD" | "That was the last one." | "Done.".
        *  For snooze: "Snoozed to MM/DD".
        *  For schedule: "Scheduled for MM/DD".
        *  Always present (no omission — rendering can rely on it). */
       tileText: string;
     }
     & SlotSnapshot)
  | ({ kind: "dismissed"; action: "done" | "snooze" | "schedule"; dismissedAt: number }
     & SlotSnapshot);

/** Fixed section ordering for the grouped task display. The enum's
 * numeric values drive sort order; within WEEKDAY and FUTURE, multiple
 * groups are ordered by their canonical date (ISO YYYY-MM-DD is lex-sortable).
 *
 * Order policy (extends `docs/designs/todo-section-order-and-dated-invariant.md`
 * with the today-earlier-bucket split):
 * OVERDUE → DUE_TODAY → EARLIER → TODAY → TOMORROW → WEEKDAY (days 2–6
 * chronologically) → FUTURE (chronologically). The enum is the single
 * source of truth for section order; rendering never depends on input
 * array order. */
enum Section {
  OVERDUE = 0,
  DUE_TODAY = 1,
  EARLIER = 2,
  TODAY = 3,
  TOMORROW = 4,
  WEEKDAY = 5,
  FUTURE = 6,
}

export interface SectionAssignment {
  section: Section;
  key: string;
  header: string;
  headerClass: string;
  canonicalDate: string | null;
}

/** Header classes for the three pseudo-buckets — sections whose
 *  membership is keyed off something other than `effective_date == bucket
 *  date`, so dropping a task into one has no defensible optimistic
 *  assignment (`canonicalDate` is null for all three). The drop hit-test
 *  filters cross-bucket drops INTO any of these. See design.md §4. */
export const PSEUDO_BUCKET_HEADER_CLASSES: ReadonlySet<string> = new Set([
  "overdue",
  "earlier",
  "due-today",
]);

/** Header classes for source buckets where drag-start is blocked
 *  entirely. Both keyed off `due_date`; `TodoReorder` only rewrites
 *  `effective_date`, so dragging out would let the user appear to move
 *  the task only to have the next render snap it back. EARLIER is
 *  excluded — it's the one pseudo-bucket where drag-out is meaningful
 *  (rebasing `effective_date` to the target bucket's canonical date is
 *  exactly what the user wants). See design.md §4. */
export const DRAG_LOCKED_SOURCE_HEADER_CLASSES: ReadonlySet<string> = new Set([
  "overdue",
  "due-today",
]);

/** Pure predicate for the drop-side hit-test rule. The drop is allowed
 *  iff:
 *
 *  - The target is a real date bucket (TODAY/TOMORROW/WEEKDAY/FUTURE),
 *    regardless of source; or
 *  - The target is a pseudo-bucket (OVERDUE/EARLIER/DUE_TODAY), but the
 *    source group is the same group (within-bucket reorder).
 *
 *  Cross-bucket drops INTO any pseudo-bucket are rejected. The drag-out
 *  rule (blocking drag-start from OVERDUE/DUE_TODAY) is separate; see
 *  `DRAG_LOCKED_SOURCE_HEADER_CLASSES`.
 *
 *  Same-source-group drops into OVERDUE / DUE_TODAY return `true` for
 *  symmetry — the predicate would allow within-bucket reorder there if
 *  the drag-start gate were relaxed. In practice the drag-start gate
 *  prevents drags from initiating in those buckets at all.
 */
export function isDroppable(
  targetHeaderClass: string,
  sourceGroupIdx: number,
  targetGroupIdx: number,
): boolean {
  if (!PSEUDO_BUCKET_HEADER_CLASSES.has(targetHeaderClass)) return true;
  return sourceGroupIdx === targetGroupIdx;
}

/** Per-row geometry input to the hit-test. One entry per element in
 *  the `.task-row, .group-header` query, in DOM order. */
export interface HitTestRow {
  kind: "header" | "task";
  /** Group index (`data-group-idx`). */
  gi: number;
  /** Task index within group (`data-task-idx`); only set when
   *  `kind === "task"`. */
  ti?: number;
  /** Header class string (used by `isDroppable`); only set when
   *  `kind === "header"`. */
  headerClass?: string;
  /** Bucket canonicalDate (used by §4.1); only set when
   *  `kind === "header"`. */
  canonicalDate?: string | null;
  /** Vertical extent in viewport coordinates (`getBoundingClientRect`
   *  top/bottom). */
  top: number;
  bottom: number;
}

export interface HitTestResult {
  mode: "reorder" | "schedule";
  gi: number;
  /** Set when `mode === "reorder"`. */
  insertIdx?: number;
}

/** Pure hit-test for a drag pointer position against the rendered list
 *  of headers + task rows. Two-pass:
 *
 *  1. Heading pass: each header owns the vertical band from its own
 *     top to the next row's top (or `+Infinity` if last). If `clientY`
 *     falls in a heading's band AND the heading is a valid schedule
 *     candidate (real-date bucket per §4.1, droppable per §4.1), return
 *     `{ mode: "schedule", gi }`. Bands are non-overlapping by
 *     construction so the first match is the only match.
 *
 *  2. Row pass: closest-midpoint hit-test on each `task` row, filtered
 *     by `isDroppable`. `clientY ≤ midY` → `insertIdx = ti`,
 *     `clientY > midY` → `insertIdx = ti + 1`.
 *
 *  Returns `null` if neither pass yields a candidate (caller leaves the
 *  previous drop target unchanged).
 *
 *  See design.md §4.3. */
export function hitTestDrop(
  rows: HitTestRow[],
  clientY: number,
  sourceGi: number,
): HitTestResult | null {
  if (rows.length === 0) return null;
  // Pointer above the entire list area: no candidate. Caller keeps
  // the previous drop target unchanged. See design.md §4.3 step 3.
  if (clientY < rows[0].top) return null;

  // Heading pass.
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    if (r.kind !== "header") continue;
    const bandTop = r.top;
    const bandBottom = i + 1 < rows.length ? rows[i + 1].top : Infinity;
    if (clientY < bandTop || clientY >= bandBottom) continue;
    // Reject headings without a concrete date (§4.1) — pseudo-buckets
    // OVERDUE / EARLIER / DUE_TODAY have no concrete date to write
    // into `tentative_date`, so they are never schedule candidates.
    if (r.canonicalDate == null) continue;
    if (r.headerClass == null) continue;
    if (!isDroppable(r.headerClass, sourceGi, r.gi)) continue;
    return { mode: "schedule", gi: r.gi };
  }

  // Row pass: closest-midpoint hit-test on each task row. The caller
  // pre-filters pseudo-bucket task rows out of `rows`, so any task
  // row reaching here is droppable.
  let bestGi = -1;
  let bestInsertIdx = -1;
  let minDist = Infinity;
  for (const r of rows) {
    if (r.kind !== "task") continue;
    if (r.ti == null) continue;
    const midY = r.top + (r.bottom - r.top) / 2;
    const dist = Math.abs(clientY - midY);
    if (clientY <= midY && dist < minDist) {
      minDist = dist;
      bestGi = r.gi;
      bestInsertIdx = r.ti;
    } else if (clientY > midY && dist < minDist) {
      minDist = dist;
      bestGi = r.gi;
      bestInsertIdx = r.ti + 1;
    }
  }
  if (bestGi !== -1 && bestInsertIdx !== -1) {
    return { mode: "reorder", gi: bestGi, insertIdx: bestInsertIdx };
  }
  return null;
}

/** A single render entry for `buildSlotGroups` output. Under the
 *  freeze-the-list design every slot is a live-row placeholder — the
 *  pending / settled / dismissed rendering branches live inside
 *  `_renderTask`, keyed off `slotState`. The type is kept (rather than
 *  reducing to `TodoItem`) to leave room for future slot variants
 *  without re-plumbing the render pipeline. */
export type TodoSlot = { kind: "live"; task: TodoItem };

/** A group of `TodoSlot`s sharing a date header. Parallel to `TaskGroup`
 *  but produced by `buildSlotGroups`, which respects the freeze snapshot
 *  when one is active. */
export interface SlotGroup {
  header: string;
  headerClass: string;
  canonicalDate: string | null;
  slots: TodoSlot[];
}

/** Assign a date string to its date-section (TODAY / TOMORROW /
 *  WEEKDAY / FUTURE). Used by both `sectionOf` (after the OVERDUE /
 *  DUE_TODAY / EARLIER short-circuits) and `groupTasksByDate` (to seed
 *  the always-visible 7-day window of empty placeholder buckets — see
 *  design.md §3). */
export function sectionForDate(
  date: string,
  todayStr: string,
  tomorrowStr: string,
  weekEndStr: string,
): SectionAssignment {
  if (date === todayStr) {
    return {
      section: Section.TODAY,
      key: "today",
      header: "Today",
      headerClass: "today",
      canonicalDate: todayStr,
    };
  }
  if (date === tomorrowStr) {
    return {
      section: Section.TOMORROW,
      key: "tomorrow",
      header: "Tomorrow",
      headerClass: "tomorrow",
      canonicalDate: tomorrowStr,
    };
  }
  if (date < weekEndStr) {
    return {
      section: Section.WEEKDAY,
      key: `weekday-${date}`,
      header: weekdayName(date),
      headerClass: "weekday",
      canonicalDate: date,
    };
  }
  return {
    section: Section.FUTURE,
    key: `date-${date}`,
    header: shortDate(date),
    headerClass: "future",
    canonicalDate: date,
  };
}

/** Assign a task to its section. Returns the bucket key plus the info
 * needed to create a new `TaskGroup` on first encounter. */
export function sectionOf(
  task: TodoItem,
  todayStr: string,
  tomorrowStr: string,
  weekEndStr: string,
): SectionAssignment {
  // OVERDUE: tasks with an actual due_date in the past. Membership keyed
  // off `due_date`, not `effective_date`.
  const isOverdue = task.due_date != null && task.due_date < todayStr;
  if (isOverdue) {
    return {
      section: Section.OVERDUE,
      key: "overdue",
      header: "Overdue",
      headerClass: "overdue",
      canonicalDate: null,
    };
  }
  const date = task.effective_date;
  // DUE_TODAY: real deadline today, not overdue. Membership keyed off
  // `due_date`, not `effective_date`, so canonicalDate is null
  // (membership keyed off `due_date`; `effective_date` may be `< today`
  // or `== today` — the section is homogeneous in `due_date` only).
  // Sits before EARLIER by design — a task with both `due_date == today`
  // and `effective_date < today` lands here so the morning-scan headline
  // surfaces what's actually due today (see design.md §2).
  if (task.due_date === todayStr) {
    return {
      section: Section.DUE_TODAY,
      key: "due-today",
      header: "Due today",
      headerClass: "due-today",
      canonicalDate: null,
    };
  }
  // EARLIER: past effective_date, not overdue, not due-today.
  // Heterogeneous past dates (rolled-forward backlog) collapse into one
  // bucket. Display label is "Pulled forward"; the code identifier
  // (Section.EARLIER, key "earlier", headerClass "earlier") stays for
  // continuity with existing CSS and drag/drop sets.
  if (date < todayStr) {
    return {
      section: Section.EARLIER,
      key: "earlier",
      header: "Pulled forward",
      headerClass: "earlier",
      canonicalDate: null,
    };
  }
  // TODAY / TOMORROW / WEEKDAY / FUTURE: delegate to the date-only
  // helper so the always-visible-7-day-window seeding logic in
  // `groupTasksByDate` shares this exact branch.
  return sectionForDate(date, todayStr, tomorrowStr, weekEndStr);
}

/** Group tasks into a fixed-order series of date sections.
 *
 * Section order is determined by the `Section` enum (not by input
 * array order). Within WEEKDAY and FUTURE, multiple groups are ordered
 * chronologically by their canonical date. Within each group, tasks
 * preserve the order graf returned them in (effective_date,
 * sort_order/priority, tldr — see graf's SQL ORDER BY).
 *
 * Note: `TodoItem.effective_date` is non-null by contract (graf's
 * `todo_query` partitions null-date rows into `lint_errors`). There is
 * no "Undated" bucket. */
export function groupTasksByDate(tasks: TodoItem[], todayStr: string): TaskGroup[] {
  const tomorrowStr = addDays(todayStr, 1);
  const weekEndStr = addDays(todayStr, 7); // 6 days ahead (today + 1..6)

  // Track section per key so we can sort groups at the end. Section is
  // the primary sort key; `group.canonicalDate` is the intra-section
  // tiebreaker (only fires for WEEKDAY/FUTURE, since the other sections
  // each have a single fixed key).
  interface Bucket {
    section: Section;
    group: TaskGroup;
  }
  const buckets: Map<string, Bucket> = new Map();

  for (const task of tasks) {
    const a = sectionOf(task, todayStr, tomorrowStr, weekEndStr);
    let b = buckets.get(a.key);
    if (!b) {
      b = {
        section: a.section,
        group: {
          header: a.header,
          headerClass: a.headerClass,
          canonicalDate: a.canonicalDate,
          tasks: [],
        },
      };
      buckets.set(a.key, b);
    }
    b.group.tasks.push(task);
  }

  // Always-visible 7-day window: seed empty buckets for today + next 6
  // days (TODAY, TOMORROW, plus 5 WEEKDAYs at today+2..today+6). The
  // user's morning view should preserve the weekday rhythm regardless
  // of which days are populated, and empty buckets serve as
  // schedule-drop landing pads (see design.md §3, §4). OVERDUE,
  // DUE_TODAY, EARLIER, and FUTURE are NOT seeded — they appear only
  // when populated, exactly as before.
  for (let i = 0; i < 7; i++) {
    const date = addDays(todayStr, i);
    const a = sectionForDate(date, todayStr, tomorrowStr, weekEndStr);
    if (!buckets.has(a.key)) {
      buckets.set(a.key, {
        section: a.section,
        group: {
          header: a.header,
          headerClass: a.headerClass,
          canonicalDate: a.canonicalDate,
          tasks: [],
        },
      });
    }
  }

  // Sort by (section, canonicalDate). ISO YYYY-MM-DD is lexically sortable.
  // The intra-section compare only fires when two buckets share a section.
  // Under the current enum, only WEEKDAY and FUTURE can have multiple
  // buckets, and both produce non-null canonicalDate values — hence the
  // non-null assertion. OVERDUE / EARLIER / DUE_TODAY each have a single
  // fixed key (and canonicalDate: null), so they never reach the
  // tiebreaker. If a future section is added with null canonicalDate
  // and multiple keys, define intra-section ordering explicitly.
  const sorted = Array.from(buckets.values()).sort((x, y) => {
    if (x.section !== y.section) return x.section - y.section;
    const xd = x.group.canonicalDate!;
    const yd = y.group.canonicalDate!;
    return xd < yd ? -1 : xd > yd ? 1 : 0;
  });

  return sorted.map((b) => b.group);
}

/** Build the render list for the todo pane under the freeze-the-list
 *  contract (`docs/designs/todo-tombstone-regression/design.md`).
 *
 *  When `frozenSnapshot` is non-null, rendering is driven by the
 *  snapshot's pre-captured groups: every slot is a live-row
 *  placeholder, and the pending / settled / dismissed branches of
 *  `<brenn-todo-list>._renderTask` paint the in-place status via
 *  `slotState.get(key)`. The snapshot is pinned at first-pending and
 *  cleared only on thaw, so the rendered order is immutable through
 *  a triage session.
 *
 *  When `frozenSnapshot` is null, this collapses to a thin wrapper
 *  over `groupTasksByDate` — no multi-pass splicing, no suppression,
 *  no orphan-section logic.
 */
export function buildSlotGroups(
  tasks: TodoItem[],
  todayStr: string,
  frozenSnapshot: { groups: TaskGroup[]; todayStr: string } | null,
): SlotGroup[] {
  const sourceGroups = frozenSnapshot
    ? frozenSnapshot.groups
    : groupTasksByDate(tasks, todayStr);
  return sourceGroups.map((g) => ({
    header: g.header,
    headerClass: g.headerClass,
    canonicalDate: g.canonicalDate,
    slots: g.tasks.map((t) => ({ kind: "live", task: t })),
  }));
}

@customElement("brenn-todo-list")
export class BrennTodoList extends LitElement {
  @property({ attribute: false }) tasks: TodoItem[] = [];
  /** Per-row slot state covering both in-flight (pending) and completed
   *  (settled) rows. Replaces the old `pendingKeys` + `pendingAction`
   *  pair — one map keyed by `todoKey(path, repo)`.
   *
   *  - `kind: "pending"` → render the greyed row with the action-label
   *    matching `action`.
   *  - `kind: "settled"` → render the in-place confirmation tile. The
   *    live row for the same key (if any) is suppressed by
   *    `buildSlotGroups` (§3.2 key precedence rule).
   *
   *  Parent owns mutation; we only read. Mutated in place on the parent
   *  with a matching `requestUpdate()` so Lit re-reads the reference —
   *  see `todo-list-ui-state-preservation.md` §3.10. */
  @property({ attribute: false }) slotState: Map<string, SlotState> = new Map();
  /** Frozen render snapshot taken at first-pending (see the design
   *  doc's "How the freeze is represented" section). When non-null,
   *  `buildSlotGroups` renders from these pre-captured groups rather
   *  than from the current `tasks` prop, so incoming TodoState
   *  refreshes never change what the user sees mid-triage. Parent
   *  owns mutation; we only read. */
  @property({ attribute: false }) frozenSnapshot:
    | { groups: TaskGroup[]; todayStr: string }
    | null = null;
  /** Per-row transient error badge: key → error message. Phase 4: parent
   *  owns the auto-clear timer; the list component only reads. */
  @property({ attribute: false }) errorKeys: Map<string, string> = new Map();
  @property({ attribute: false }) todayStr: string = localTodayStr();
  @property({ type: Boolean, reflect: true }) visible = false;
  @property({ attribute: false }) onDone:
    | ((path: string, repo?: string) => void)
    | null = null;
  /**
   * Callback for a snooze action. The face button sends `days=1`; the
   * menu entries send `3`, `7`, or `30`. Parent receives
   * `effective_date` directly — everything else about the task is
   * already in parent state.
   */
  @property({ attribute: false }) onSnooze:
    | ((
        path: string,
        repo: string | undefined,
        effectiveDate: string,
        days: number,
      ) => void)
    | null = null;
  @property({ attribute: false }) onReorder:
    | ((target: ReorderTarget) => void)
    | null = null;
  /** Heading-drop schedule callback. Fired when the user drops on a
   *  date-bucket heading rather than between two task rows. The brenn-
   *  app handler dispatches `TodoSchedule` (one per task for multi-
   *  select) — see design.md §5. */
  @property({ attribute: false }) onSchedule:
    | ((target: ScheduleTarget) => void)
    | null = null;
  @property({ attribute: false }) onRefresh: (() => void) | null = null;
  @property({ attribute: false }) onCollapse: (() => void) | null = null;
  /** Manual dismiss (`×`) on a settled tile. Parent drops the slot. */
  @property({ attribute: false }) onDismissSettled:
    | ((key: string) => void)
    | null = null;
  @property({ type: Boolean }) refreshPending = false;
  /** True when the parent has put the list into a "no new mutations"
   *  state (currently: while a click-refresh is queued waiting on
   *  pending slots to settle — design §3.5). Gates `_onDragStart` so
   *  the drag UI doesn't start an optimistic reorder whose dispatch
   *  the parent will short-circuit. Dispatch-level gates live on the
   *  parent (`_handleTodoDone` / `_handleTodoSnooze` /
   *  `_handleTodoReorder` / `_handleMultiReorder`); this prop
   *  extends the same gate to the drag-handle entry point that's
   *  owned by this component. */
  @property({ type: Boolean }) interactionsDisabled = false;
  /** Keys of currently selected tasks (parent-owned). */
  @property({ attribute: false }) selectedKeys: Set<string> = new Set();
  /** Callback when selection changes. */
  @property({ attribute: false }) onSelectionChange:
    | ((keys: Set<string>) => void)
    | null = null;

  // --- Drag state (not Lit reactive — managed via direct DOM manipulation) ---

  /** Active drag info, or null when not dragging. */
  private _drag: {
    /** Key of the dragged task. */
    sourceKey: string;
    /** The task being dragged. */
    sourceTask: TodoItem;
    /** Group index of the source task at drag start. */
    sourceGroupIdx: number;
    /** Index within source group at drag start. */
    sourceTaskIdx: number;
    /** Ghost element floating with pointer, created on first active move. */
    ghostEl: HTMLElement | null;
    /** The handle element that initiated this drag (stored for listener cleanup). */
    handleEl: HTMLElement;
    /** Cached reference to .task-list scroll container (avoids repeated queries). */
    taskListEl: HTMLElement;
    /** The pointerId we captured. */
    pointerId: number;
    /** Y at pointerdown. */
    startY: number;
    /** Last known clientY, used by auto-scroll to update drop target. */
    lastClientY: number;
    /** Whether we've passed the dead zone and are in active drag mode. */
    active: boolean;
    /** Current drop target info (group index + insertion index within
     *  group). `dropInsertIdx` is meaningful only when `dropMode ===
     *  "reorder"`; under `"schedule"` the heading owns the entire
     *  bucket so the insert index is irrelevant (graf's `todo_schedule`
     *  removes any prior `sort_order`, so the task ranks naturally
     *  in its new bucket — see design.md §4.6). */
    dropMode: "reorder" | "schedule";
    dropGroupIdx: number;
    dropInsertIdx: number;
    /** requestAnimationFrame id for auto-scroll, or null. */
    scrollRaf: number | null;
    /** Keys of all selected tasks participating in this drag, or null for single-task. */
    selectedKeys: Set<string> | null;
  } | null = null;

  /** Bound handlers stored for cleanup. */
  private _boundDragMove: ((e: PointerEvent) => void) | null = null;
  private _boundDragEnd: ((e: PointerEvent) => void) | null = null;
  private _boundDragKeydown: ((e: KeyboardEvent) => void) | null = null;

  // --- Selection state ---

  /** Last toggled key for shift+click range selection. */
  private _lastToggledKey: string | null = null;

  /** Long-press timer for selection entry. */
  private _longPressTimer: ReturnType<typeof setTimeout> | null = null;
  /** Pointer position at pointerdown, for cancelling long-press on move. */
  private _longPressStart: { x: number; y: number; key: string } | null = null;
  /** Key that is currently in long-press hold state (for CSS feedback). */
  private _holdKey: string | null = null;
  /** Set when long-press fires — suppresses the subsequent click event. */
  private _longPressConsumed = false;

  // --- Snooze split-button menu state (Phase 4) ---

  /** Key of the row whose snooze menu is currently open, or null if none. */
  @state() private _snoozeMenuKey: string | null = null;
  /** Shared menu lifecycle for the snooze dropdown: outside-click + Escape.
   *  Uses `pointerdown` and `composedPath()` so shadow-DOM clicks inside
   *  the menu are correctly detected as in-menu (the re-targeted `target`
   *  at the document level is the host element, not the internal node). */
  private _snoozeMenuController = new MenuController(
    (e) => {
      // composedPath() walks through shadow roots, giving us the real
      // chain of elements the pointer traversed inside the shadow DOM.
      for (const node of e.composedPath()) {
        if (node instanceof Element) {
          if (
            node.classList?.contains("snooze-menu") ||
            node.classList?.contains("snooze-caret")
          ) {
            return true;
          }
        }
      }
      return false;
    },
    () => this._closeSnoozeMenu(false),
    { eventType: "pointerdown" },
  );
  /** Per-row mobile-sheet flag: true when the menu was opened on a
   *  coarse-pointer viewport and should render as a centered sheet.
   *  `@state` so a transition between desktop / mobile breakpoints
   *  while the menu is closed doesn't leave the class mapping stale. */
  @state() private _snoozeMenuAsSheet = false;

  static styles = css`
    :host {
      container-type: inline-size;
      flex: 1;
      flex-direction: column;
      min-width: 0;
      min-height: 0;
    }

    :host(:not([visible])) {
      display: none !important;
    }

    :host([visible]) {
      display: flex;
    }

    /* --- Header --- */

    .todo-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0.5rem 1rem;
      border-bottom: 1px solid #2a2a40;
      background: #161628;
      flex-shrink: 0;
    }

    .todo-title {
      font-size: 0.85rem;
      color: #a0a0b8;
      font-weight: 500;
    }

    .header-actions {
      display: flex;
      gap: 0.25rem;
      align-items: center;
    }

    .header-btn {
      background: none;
      border: 1px solid #3a3a50;
      color: #a0a0b8;
      font-size: 1rem;
      cursor: pointer;
      padding: 0.15rem 0.5rem;
      border-radius: 3px;
      flex-shrink: 0;
      line-height: 1;
    }

    .header-btn:hover {
      background: #2a2a40;
      color: #d0d0d8;
    }

    @keyframes brenn-todo-refresh-spin {
      from { transform: rotate(0deg); }
      to   { transform: rotate(360deg); }
    }

    .header-btn.refresh-btn {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 1.75rem;
    }

    .refresh-icon {
      display: inline-block;
      line-height: 1;
    }

    .header-btn.refresh-btn.is-refreshing {
      color: #4a6fa5;
      border-color: #4a6fa5;
    }

    .header-btn.refresh-btn.is-refreshing:hover {
      /* Kill the hover restyle while busy so it doesn't look clickable. */
      background: none;
      color: #4a6fa5;
    }

    .header-btn.refresh-btn[disabled] {
      opacity: 0.6;
    }

    /* Spin the icon, not the button, so the focus outline stays put. */
    .header-btn.refresh-btn.is-refreshing .refresh-icon {
      animation: brenn-todo-refresh-spin 0.9s linear infinite;
    }

    @media (prefers-reduced-motion: reduce) {
      .header-btn.refresh-btn.is-refreshing .refresh-icon {
        animation: none;
      }
    }

    /* --- Task list --- */

    .task-list {
      flex: 1;
      overflow-y: auto;
      padding: 0;
      scrollbar-color: #2a2a40 transparent;
      scrollbar-width: thin;
    }

    /* --- Date group headers --- */

    .group-header {
      font-size: 0.7rem;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: #808098;
      padding: 0.6rem 1rem 0.3rem;
      border-bottom: 1px solid #1e1e34;
    }

    .group-header.overdue {
      color: #e94560;
    }

    .group-header.earlier {
      /* Dimmer than .today — communicates "backlog, not the headline". */
      color: #8c8ca8;
    }

    .group-header.due-today {
      /* The morning-scan headline. Warmer / more attention-grabbing than
       * .today, but not the alarm color of .overdue. */
      color: #d4a44a;
    }

    .group-header.today {
      color: #4a6fa5;
    }

    /* --- Task row --- */

    .task-row {
      display: flex;
      align-items: flex-start;
      gap: 0.25rem;
      padding: 0.4rem 0.5rem;
      border-bottom: 1px solid #1a1a2e;
    }

    .task-row:hover {
      background: #1a1a2e;
    }

    .task-row.pending {
      opacity: 0.45;
    }

    /* --- Settled tile (parks the slot after a successful done/snooze
     * so the user's next click target doesn't shift — see
     * docs/designs/todo-list-ui-state-preservation.md §3.3). --- */

    .task-row.settled {
      background: #151c2e;
      border-left: 3px solid #3ddc84;
      padding-left: calc(0.5rem - 3px);
      color: #d0d0d8;
    }

    /* --- Dismissed placeholder (manual × on a tile mid-triage). --- */
    /* Purely a spacer — same outer box, no children, matches the live-
     * row vertical footprint so surviving tiles don't shift up. Min-height
     * mirrors the live-row's content-height (two line-boxes + padding). */
    .task-row.dismissed {
      min-height: calc(0.85rem * 1.2 + 0.8rem);
      opacity: 0;
      pointer-events: none;
    }

    .task-row.settled.settled-snooze {
      border-left-color: #4a6fa5;
    }

    .task-row.settled.settled-schedule {
      /* Warm amber matches the .due-today header color; communicates
       * "scheduled, has a date attached" rather than the cool-blue
       * snooze accent. See design.md §5.1 #3. */
      border-left-color: #d4a44a;
    }

    .task-row.settled .settled-glyph {
      flex-shrink: 0;
      width: 1.25em;
      text-align: center;
      font-size: 0.9rem;
    }

    .task-row.settled .settled-title {
      flex: 1;
      min-width: 0;
      font-size: 0.85rem;
      color: #d0d0d8;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .task-row.settled .settled-outcome {
      color: #808098;
      font-size: 0.75rem;
      flex-shrink: 0;
      margin-left: 0.25rem;
    }

    .task-row.settled .settled-dismiss {
      background: none;
      border: none;
      color: #5a5a78;
      font-size: 0.9rem;
      cursor: pointer;
      padding: 0.25rem;
      min-width: 32px;
      min-height: 32px;
      display: flex;
      align-items: center;
      justify-content: center;
      line-height: 1;
      touch-action: manipulation;
    }

    @media (pointer: coarse) {
      .task-row.settled .settled-dismiss {
        min-width: 48px;
        min-height: 48px;
      }
    }

    /* --- Layout ordering (wide) ---
     * DOM order: drag-handle, task-title, task-meta (contents).
     * Visual order via CSS order so actions precede the title. */

    .task-meta {
      display: contents;
    }

    .drag-handle    { order: 0; }
    .task-actions   { order: 1; }
    .pending-label  { order: 1; }
    .select-dot     { order: 2; }
    .priority       { order: 3; }
    .task-title     { order: 4; }
    .recurring      { order: 5; }
    .repo-badge     { order: 6; }

    /* --- Action buttons --- */

    .task-actions {
      display: flex;
      gap: 0.15rem;
      flex-shrink: 0;
    }

    .action-btn {
      background: none;
      border: none;
      color: #5a5a78;
      font-size: 0.85rem;
      cursor: pointer;
      padding: 0.25rem;
      min-width: 32px;
      min-height: 32px;
      display: flex;
      align-items: center;
      justify-content: center;
      line-height: 1;
      touch-action: manipulation;
    }

    @media (pointer: coarse) {
      .action-btn {
        min-width: 48px;
        min-height: 48px;
      }
    }

    @media (hover: hover) {
      .action-btn:hover {
        color: #4a6fa5;
        background: #22223a;
      }

      .action-btn.done-btn:hover {
        color: #3ddc84;
      }

      .task-row.settled .settled-dismiss:hover {
        color: #d0d0d8;
        background: #22223a;
      }
    }

    /* --- Snooze split button (Phase 4) --- */

    .snooze-split {
      display: inline-flex;
      align-items: stretch;
      position: relative;
    }

    /* Collapse the shared border so face + caret read as one control. */
    .snooze-face {
      padding-right: 0.15rem;
    }

    .snooze-caret {
      padding-left: 0.1rem;
      padding-right: 0.1rem;
      min-width: 16px;
      font-size: 0.7rem;
    }

    @media (pointer: coarse) {
      .snooze-caret {
        min-width: 28px;
      }
    }

    /* --- Snooze menu popup ---
     * position:fixed escapes the scrollable .task-list clipping rect.
     * Coordinates are set inline on open (see _positionSnoozeMenu). */

    .snooze-menu {
      list-style: none;
      margin: 0;
      padding: 0.2rem 0;
      position: fixed;
      min-width: 11rem;
      background: #1e1e34;
      border: 1px solid #3a3a50;
      border-radius: 4px;
      box-shadow: 0 6px 16px rgba(0, 0, 0, 0.5);
      z-index: 9000;
    }

    .snooze-menu [role="menuitem"] {
      display: flex;
      justify-content: space-between;
      gap: 1rem;
      padding: 0.4rem 0.75rem;
      font-size: 0.85rem;
      color: #d0d0d8;
      cursor: pointer;
      outline: none;
    }

    .snooze-menu [role="menuitem"]:hover,
    .snooze-menu [role="menuitem"]:focus {
      background: #22223a;
      color: #fff;
    }

    .snooze-menu-label {
      font-weight: 500;
    }

    .snooze-menu-date {
      color: #808098;
      font-variant-numeric: tabular-nums;
    }

    /* Mobile breakpoint: render as a centered fixed sheet. */
    .snooze-menu.snooze-menu-sheet {
      left: 50% !important;
      top: 50% !important;
      transform: translate(-50%, -50%);
      min-width: 16rem;
    }

    /* --- Row error badge (Phase 4 §7.2) --- */

    .row-error {
      color: #e94560;
      font-size: 0.75rem;
      flex-shrink: 0;
      padding: 0.15rem 0.4rem;
      border: 1px solid #e94560;
      border-radius: 3px;
      background: rgba(233, 69, 96, 0.08);
      cursor: help;
    }

    /* --- Pending label --- */

    .pending-label {
      font-size: 0.75rem;
      color: #5a5a78;
      font-style: italic;
      flex-shrink: 0;
      padding: 0.25rem 0.5rem;
    }

    /* --- Priority --- */

    .priority {
      font-size: 0.7rem;
      font-weight: 700;
      flex-shrink: 0;
      min-width: 1.4em;
      text-align: center;
    }

    .priority.p1 {
      color: #e94560;
    }

    .priority.p2 {
      color: #c47a3a;
    }

    .priority.p3 {
      color: #808098;
    }

    /* --- Task title --- */

    .task-title {
      flex: 1;
      min-width: 0;
      font-size: 0.85rem;
      color: #d0d0d8;
    }

    /* --- Recurring indicator --- */

    .recurring {
      color: #5a5a78;
      font-size: 0.75rem;
      flex-shrink: 0;
      margin-left: 0.15rem;
    }

    /* --- Repo badge --- */

    .repo-badge {
      font-size: 0.7rem;
      color: #5a5a78;
      flex-shrink: 0;
      margin-left: 0.5rem;
    }

    /* --- Drag handle --- */

    .drag-handle {
      cursor: grab;
      color: #3a3a50;
      font-size: 0.85rem;
      flex-shrink: 0;
      padding: 0.25rem;
      min-width: 32px;
      min-height: 32px;
      display: flex;
      align-items: center;
      justify-content: center;
      user-select: none;
      touch-action: none;
      line-height: 1;
    }

    @media (pointer: coarse) {
      .drag-handle {
        min-width: 48px;
        min-height: 48px;
      }
    }

    .drag-handle:hover {
      color: #808098;
    }

    /* --- Drag ghost (appended to shadow root) --- */

    .drag-ghost {
      position: fixed;
      pointer-events: none;
      opacity: 0.75;
      z-index: 1000;
      background: #1e1e34;
      border: 1px solid #4a6fa5;
      border-radius: 4px;
      box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4);
      padding: 0.4rem 0.75rem;
      font-size: 0.85rem;
      color: #d0d0d8;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      max-width: 300px;
    }

    /* --- Drop indicator line --- */

    .drop-indicator {
      height: 2px;
      background: #4a6fa5;
      margin: 0 0.5rem;
      border-radius: 1px;
      pointer-events: none;
    }

    /* --- Source row placeholder during drag --- */

    .task-row.dragging {
      height: 2px;
      padding: 0;
      overflow: hidden;
      background: #4a6fa5;
      opacity: 0.4;
      border-bottom: none;
    }

    .task-row.dragging > * {
      display: none;
    }

    /* --- Schedule-target heading highlight (heading-drop) ---
     * Mutually exclusive with .drop-indicator: a reorder hit shows
     * a 2-px bar between rows; a schedule hit shows this filled,
     * outlined block on the heading. The graphic gestalt of the two
     * cues is deliberately distinct (line vs. block). See
     * design.md §4.4. */

    .group-header.schedule-target {
      outline: 2px solid #4a6fa5;
      outline-offset: -2px;
      background: #1f2c46;
      color: #e0e8f5;
      /* Empty-bucket case needs visible thickness; pad to ~ a row's
       * height so an empty heading is a generous landing pad rather
       * than a one-line strip. */
      padding-top: 0.7rem;
      padding-bottom: 0.7rem;
    }

    /* --- Selection state --- */

    .task-row.selected {
      background: #1e2a3e;
      border-left: 3px solid #4a6fa5;
      padding-left: calc(0.5rem - 3px);
    }

    .task-row.selected:hover {
      background: #22304a;
    }

    .select-dot {
      width: 6px;
      height: 6px;
      border-radius: 50%;
      background: #4a6fa5;
      flex-shrink: 0;
      margin-right: 0.25rem;
    }

    .task-row.holding {
      background: #1a2235;
      transition: background 0.15s ease;
    }

    /* --- Narrow (compact) layout --- */

    @container (max-width: 500px) {
      .task-row {
        display: grid;
        grid-template-columns: auto 1fr;
        grid-template-rows: auto auto;
        gap: 0.15rem 0.35rem;
        align-items: start;
      }

      .drag-handle {
        grid-row: 1 / -1;
        align-self: stretch;
      }

      .task-title {
        grid-column: 2;
        grid-row: 1;
      }

      .task-meta {
        grid-column: 2;
        grid-row: 2;
        display: flex;
        align-items: center;
        gap: 0.35rem;
      }
    }

  `;

  render() {
    return html`
      <div class="todo-header">
        <span class="todo-title">Tasks</span>
        <div class="header-actions">
          <button
            class="header-btn refresh-btn ${this.refreshPending ? "is-refreshing" : ""}"
            @click=${this._handleRefresh}
            ?disabled=${this.refreshPending}
            aria-busy=${this.refreshPending ? "true" : "false"}
            aria-label="Refresh tasks"
          ><span class="refresh-icon">↻</span></button>
          <button
            class="header-btn"
            @click=${this._handleCollapse}
            title="Close task panel"
          >&times;</button>
        </div>
      </div>
      <div class="task-list">
        ${this._renderGroups()}
      </div>
    `;
  }

  private _renderGroups() {
    const groups = buildSlotGroups(
      this.tasks,
      this.todayStr,
      this.frozenSnapshot,
    );
    // Single pass per group: build the rendered Lit template *and*
    // populate `_cachedGroups` (used by drag hit-test + shift-click
    // range selection) from the same `slotState.get(key)` lookup. This
    // collapses three previously parallel encodings of the "live vs
    // non-live" predicate into one (review F4):
    //   - the cachedGroups filter (was: separate map().filter()),
    //   - _renderSlots' branch (was: separate slotState.get() in
    //     `_renderSlots`),
    //   - _renderTask's templating (was: a third slotState.get() inside
    //     `_renderTask`).
    // Now every row's `entry` is computed once here and threaded
    // through to `_renderTask` as a parameter.
    const cached: TaskGroup[] = [];
    const rendered = groups.map((g, gi) => {
      const liveOnly: TodoItem[] = [];
      let liveIdx = 0;
      const rows = g.slots.map((s) => {
        const key = todoKey(s.task.path, s.task.repo);
        const entry = this.slotState.get(key);
        if (entry === undefined) {
          // Live row: gets a real `data-task-idx`, contributes to
          // `_cachedGroups`, drag hit-test sees it.
          const out = this._renderTask(s.task, key, undefined, gi, liveIdx);
          liveOnly.push(s.task);
          liveIdx++;
          return out;
        }
        // Non-live (pending / settled / dismissed): no data-task-idx,
        // not in `_cachedGroups`, drop selector excludes it.
        return this._renderTask(s.task, key, entry, gi, null);
      });
      cached.push({
        header: g.header,
        headerClass: g.headerClass,
        canonicalDate: g.canonicalDate,
        tasks: liveOnly,
      });
      return html`
        <div
          class="group-header ${g.headerClass}"
          data-group-idx="${gi}"
        >${g.header}</div>
        ${rows}
      `;
    });
    this._cachedGroups = cached;
    return rendered;
  }

  /** Cached groups from last render, used by drag hit-testing. */
  private _cachedGroups: TaskGroup[] = [];

  /** Render one row. The visual mode is driven by the `entry` argument:
   *
   *  - `entry === undefined` → live-row template (full interactive
   *    controls). `taskIdx` MUST be a number (drag hit-test index).
   *  - `entry.kind === "pending"` → live-row outer with greyed-row
   *    class and an action-label (no buttons). `taskIdx === null`.
   *  - `entry.kind === "settled"` → in-place confirmation tile (same
   *    outer `.task-row`, swapped contents). `taskIdx === null`.
   *  - `entry.kind === "dismissed"` → empty placeholder of matching
   *    height so surviving tiles don't shift up. `taskIdx === null`.
   *
   *  Caller (`_renderGroups`) computes `key` + `entry` once per row
   *  and threads them in here, so this function never re-reads
   *  `slotState`. Non-live branches OMIT `data-task-idx` /
   *  `data-group-idx` (uniform rule, assigning `nothing` to an
   *  attribute removes it from the DOM in Lit) — drag hit-test
   *  relies on the attribute's *absence* AND on class-list
   *  exclusion. */
  private _renderTask(
    task: TodoItem,
    key: string,
    entry: SlotState | undefined,
    groupIdx: number,
    taskIdx: number | null,
  ) {
    // Settled and dismissed are structurally distinct rows (different
    // children, different roles) — they stay as small early returns.
    if (entry?.kind === "settled") {
      const settledLook = SETTLED_TILE_LOOKS[entry.action];
      const glyph = settledLook.glyph;
      const classes = `task-row settled ${settledLook.className}`;
      return html`
        <div
          class="${classes}"
          data-task-key="${key}"
          role="status"
          aria-live="polite"
        >
          <span class="settled-glyph" aria-hidden="true">${glyph}</span>
          <span class="settled-title">${entry.taskTldr}</span>
          <span class="settled-outcome">${entry.tileText}</span>
          <button
            type="button"
            class="settled-dismiss"
            @click=${(e: MouseEvent) => {
              e.stopPropagation();
              this.onDismissSettled?.(key);
            }}
            title="Dismiss"
            aria-label="Dismiss"
          >&times;</button>
        </div>
      `;
    }

    if (entry?.kind === "dismissed") {
      // Placeholder row: preserves the vertical footprint while other
      // settled tiles in the same triage session are still on screen.
      // Collapsed on thaw. No `data-task-key` — the row is structurally
      // unaddressable (no handlers, aria-hidden), so a key would just
      // be future false-positive surface for "select-by-key" helpers.
      return html`
        <div class="task-row dismissed" aria-hidden="true"></div>
      `;
    }

    // Live row vs pending row share most of the row chrome (the outer
    // `.task-row`, drag handle, title, meta-trailing badges); the
    // meaningful differences are the data-attrs, the drag handler on
    // the handle, and the action-cluster vs pending-label slot. Branch
    // small things, share the rest.
    const isPending = entry?.kind === "pending";

    // Live-row contract: caller threads a real index for the live
    // branch. The runtime guard catches a state-machine bug rather
    // than silently emitting a sentinel.
    if (!isPending && taskIdx === null) {
      throw new Error(
        `_renderTask: live row for key=${key} requires a taskIdx (caller contract violation)`,
      );
    }

    const isDragging = this._drag?.active === true && (
      this._drag.sourceKey === key ||
      (this._drag.selectedKeys?.has(key) ?? false)
    );
    const isSelected = this.selectedKeys.has(key);
    const isHolding = this._holdKey === key;
    const errorText = this.errorKeys.get(key) ?? null;

    // Trailing-badges block — identical between live and pending. Pulled
    // out so a future change lands in one place.
    const trailingBadges = html`
      ${errorText
        ? html`<span class="row-error" title="${errorText}">Error: see chat</span>`
        : nothing}
      ${isSelected ? html`<span class="select-dot"></span>` : nothing}
      ${task.priority != null
        ? html`<span class="priority ${this._priorityClass(task.priority)}"
            >P${task.priority}</span
          >`
        : nothing}
      ${task.rrule ? html`<span class="recurring">↻</span>` : nothing}
      ${task.repo
        ? html`<span class="repo-badge">${task.repo}</span>`
        : nothing}
    `;

    return html`
      <div
        class="task-row ${isPending ? "pending" : ""} ${!isPending && isDragging ? "dragging" : ""} ${isSelected ? "selected" : ""} ${isHolding ? "holding" : ""}"
        data-group-idx="${isPending ? nothing : groupIdx}"
        data-task-idx="${isPending ? nothing : (taskIdx as number)}"
        data-task-key="${key}"
        @click=${(e: MouseEvent) => this._onRowClick(e, key)}
        @pointerdown=${(e: PointerEvent) => this._onRowPointerDown(e, key)}
        @pointermove=${(e: PointerEvent) => this._onRowPointerMove(e)}
        @pointerup=${() => this._cancelLongPress()}
        @pointercancel=${() => this._cancelLongPress()}
      >
        <div
          class="drag-handle"
          @pointerdown=${isPending
            ? nothing
            : (e: PointerEvent) =>
                this._onDragStart(e, task, groupIdx, taskIdx as number)}
        >⠿</div>
        <span class="task-title">${task.tldr}</span>
        <div class="task-meta">
          ${isPending
            ? html`<span class="pending-label">${this._pendingLabel(key)}</span>`
            : html`
                <div class="task-actions">
                  <button
                    type="button"
                    class="action-btn done-btn"
                    @click=${(e: MouseEvent) => { e.stopPropagation(); this._handleDone(task); }}
                    title="Mark done"
                    aria-label="Mark done"
                  >✓</button>
                  ${this._renderSnoozeSplit(task, groupIdx, taskIdx as number, key)}
                </div>
              `}
          ${trailingBadges}
        </div>
      </div>
    `;
  }

  /** Render the split-button snooze control (face + caret + menu). */
  private _renderSnoozeSplit(
    task: TodoItem,
    groupIdx: number,
    taskIdx: number,
    key: string,
  ) {
    const menuOpen = this._snoozeMenuKey === key;
    const caretId = `snooze-caret-${groupIdx}-${taskIdx}`;
    const menuId = `snooze-menu-${groupIdx}-${taskIdx}`;
    const snoozeFaceDate = this._snoozeTargetDate(task, 1);
    const faceTitle = `Snooze to ${shortDate(snoozeFaceDate)}`;
    return html`
      <div class="snooze-split">
        <button
          type="button"
          class="action-btn snooze-face"
          @click=${(e: MouseEvent) => { e.stopPropagation(); this._handleSnooze(task, 1); }}
          title="${faceTitle}"
          aria-label="${faceTitle}"
        >💤</button>
        <button
          type="button"
          id="${caretId}"
          class="action-btn snooze-caret"
          @click=${(e: MouseEvent) => this._onCaretClick(e, key)}
          @keydown=${(e: KeyboardEvent) => this._onCaretKeydown(e, key)}
          aria-label="More snooze options"
          aria-haspopup="menu"
          aria-expanded="${menuOpen ? "true" : "false"}"
          aria-controls="${menuId}"
        >▾</button>
        ${menuOpen ? this._renderSnoozeMenu(task, caretId, menuId) : nothing}
      </div>
    `;
  }

  /** Resolve the date the snooze action will land on for a given
   *  `days` offset. Thin wrapper around the shared `snoozeTargetDate`
   *  helper so the tooltip / menu labels show exactly the date the
   *  dispatch in `_handleTodoSnooze` (app.ts) will send. */
  private _snoozeTargetDate(task: TodoItem, days: number): string {
    return snoozeTargetDate(task.effective_date, this.todayStr, days);
  }

  /** Render the snooze caret's dropdown menu (+3 days / +1 week / +1 month). */
  private _renderSnoozeMenu(task: TodoItem, caretId: string, menuId: string) {
    const menuClass = this._snoozeMenuAsSheet
      ? "snooze-menu snooze-menu-sheet"
      : "snooze-menu";
    return html`
      <ul
        id="${menuId}"
        class="${menuClass}"
        role="menu"
        aria-labelledby="${caretId}"
        @click=${(e: MouseEvent) => e.stopPropagation()}
        @keydown=${(e: KeyboardEvent) => this._onMenuKeydown(e, task)}
      >
        ${SNOOZE_MENU_ENTRIES.map(
          (entry, i) => html`
            <li
              role="menuitem"
              tabindex="-1"
              data-menu-idx="${i}"
              @click=${(e: MouseEvent) => {
                e.stopPropagation();
                this._selectMenuEntry(task, entry.days);
              }}
            >
              <span class="snooze-menu-label">${entry.label}</span>
              <span class="snooze-menu-date">${shortDate(
                this._snoozeTargetDate(task, entry.days),
              )}</span>
            </li>
          `,
        )}
      </ul>
    `;
  }

  // --- Selection ---
  /** Long-press threshold in ms. */
  private static readonly LONG_PRESS_MS = 500;
  /** Movement threshold to cancel long-press (px). */
  private static readonly LONG_PRESS_MOVE_PX = 5;

  /** Handle pointerdown on a task row (starts long-press timer). */
  private _onRowPointerDown(e: PointerEvent, key: string): void {
    // Only primary button.
    if (e.button !== 0) return;
    // Don't start long-press from drag handle or action buttons.
    const target = e.target as HTMLElement;
    if (target.closest(".drag-handle") || target.closest(".action-btn")) return;

    this._longPressStart = { x: e.clientX, y: e.clientY, key };
    this._longPressTimer = setTimeout(() => {
      this._longPressTimer = null;
      this._longPressStart = null;
      this._holdKey = null;
      this._longPressConsumed = true;
      this._toggleSelection(key);
      // Haptic feedback on mobile if available.
      if (navigator.vibrate) {
        navigator.vibrate(10);
      }
      this.requestUpdate();
    }, BrennTodoList.LONG_PRESS_MS);

    // Show hold feedback.
    this._holdKey = key;
    this.requestUpdate();
  }

  /** Cancel long-press if pointer moves too far. */
  private _onRowPointerMove(e: PointerEvent): void {
    if (!this._longPressStart) return;
    const dx = e.clientX - this._longPressStart.x;
    const dy = e.clientY - this._longPressStart.y;
    if (Math.abs(dx) > BrennTodoList.LONG_PRESS_MOVE_PX ||
        Math.abs(dy) > BrennTodoList.LONG_PRESS_MOVE_PX) {
      this._cancelLongPress();
    }
  }

  /** Cancel any pending long-press. */
  private _cancelLongPress(): void {
    if (this._longPressTimer) {
      clearTimeout(this._longPressTimer);
      this._longPressTimer = null;
    }
    this._longPressStart = null;
    if (this._holdKey) {
      this._holdKey = null;
      this.requestUpdate();
    }
  }

  /** Handle click on a task row. */
  private _onRowClick(e: MouseEvent, key: string): void {
    // Suppress the click that follows a long-press selection.
    if (this._longPressConsumed) {
      this._longPressConsumed = false;
      return;
    }

    // Don't handle if click came from drag handle or buttons.
    const target = e.target as HTMLElement;
    if (target.closest(".drag-handle") || target.closest(".action-btn")) return;

    // Ctrl/Cmd+click: toggle selection immediately (enter selection mode).
    if (e.ctrlKey || e.metaKey) {
      e.preventDefault();
      this._toggleSelection(key);
      return;
    }

    // Shift+click: range select.
    if (e.shiftKey && this._lastToggledKey && this.selectedKeys.size > 0) {
      e.preventDefault();
      this._selectRange(this._lastToggledKey, key);
      return;
    }

    // Plain click in selection mode: toggle.
    if (this.selectedKeys.size > 0) {
      this._toggleSelection(key);
      return;
    }

    // Plain click not in selection mode: reserved for future detail view.
  }

  /** Toggle a single key's selection state. */
  private _toggleSelection(key: string): void {
    const next = new Set(this.selectedKeys);
    if (next.has(key)) {
      next.delete(key);
    } else {
      next.add(key);
    }
    this._lastToggledKey = key;
    this.selectedKeys = next;
    this.onSelectionChange?.(next);
  }

  /** Select all tasks in the range between key1 and key2 (inclusive). */
  private _selectRange(fromKey: string, toKey: string): void {
    // Build a flat ordered list of all keys from cached groups.
    const allKeys: string[] = [];
    for (const group of this._cachedGroups) {
      for (const task of group.tasks) {
        allKeys.push(todoKey(task.path, task.repo));
      }
    }
    const fromIdx = allKeys.indexOf(fromKey);
    const toIdx = allKeys.indexOf(toKey);
    if (fromIdx === -1 || toIdx === -1) return;

    const start = Math.min(fromIdx, toIdx);
    const end = Math.max(fromIdx, toIdx);

    const next = new Set(this.selectedKeys);
    for (let i = start; i <= end; i++) {
      next.add(allKeys[i]);
    }
    this._lastToggledKey = toKey;
    this.selectedKeys = next;
    this.onSelectionChange?.(next);
  }

  /** Clear all selections. */
  clearSelection(): void {
    if (this.selectedKeys.size === 0) return;
    this.selectedKeys = new Set();
    this._lastToggledKey = null;
    this.onSelectionChange?.(this.selectedKeys);
  }

  /** Reconcile selection on tasks change — drop keys for removed tasks. */
  updated(changedProperties: Map<string, unknown>): void {
    super.updated(changedProperties);
    if (changedProperties.has("tasks") && this.selectedKeys.size > 0) {
      const currentKeys = new Set(this.tasks.map(t => todoKey(t.path, t.repo)));
      let changed = false;
      const next = new Set<string>();
      for (const key of this.selectedKeys) {
        if (currentKeys.has(key)) {
          next.add(key);
        } else {
          changed = true;
        }
      }
      if (changed) {
        // Clear _lastToggledKey if it no longer exists.
        if (this._lastToggledKey && !currentKeys.has(this._lastToggledKey)) {
          this._lastToggledKey = null;
        }
        this.selectedKeys = next;
        this.onSelectionChange?.(next);
      }
    }
  }

  connectedCallback(): void {
    super.connectedCallback();
    this._boundEscapeHandler = this._onEscape.bind(this);
    document.addEventListener("keydown", this._boundEscapeHandler);
  }

  disconnectedCallback(): void {
    super.disconnectedCallback();
    if (this._boundEscapeHandler) {
      document.removeEventListener("keydown", this._boundEscapeHandler);
      this._boundEscapeHandler = null;
    }
    // Clean up any in-progress drag.
    if (this._drag) {
      this._cleanupDrag();
    }
    // Drop the outside-click + viewport listeners if a menu was still open.
    this._removeOutsideClick();
    this._removeViewportListeners();
  }

  private _boundEscapeHandler: ((e: KeyboardEvent) => void) | null = null;

  private _onEscape(e: KeyboardEvent): void {
    if (e.key !== "Escape") return;
    // Menu close wins over selection clear — user's most recent action
    // was opening the menu, not toggling selection.
    if (this._snoozeMenuKey !== null) {
      this._closeSnoozeMenu(true);
      return;
    }
    if (this.selectedKeys.size > 0) {
      this.clearSelection();
    }
  }

  // --- Drag-and-drop reorder ---
  /** Dead zone in px before entering active drag mode. */
  private static readonly DRAG_DEAD_ZONE = 5;
  /** Distance from scroll container edge to trigger auto-scroll. */
  private static readonly SCROLL_EDGE_PX = 40;
  /** Max auto-scroll speed in px per frame. */
  private static readonly SCROLL_SPEED = 8;

  private _onDragStart(e: PointerEvent, task: TodoItem, groupIdx: number, taskIdx: number): void {
    // Only primary button (left click / single touch).
    if (e.button !== 0) return;
    // Design §3.5: while interactions are parent-disabled (currently a
    // queued refresh waiting on pending slots), skip dragging entirely
    // so the user doesn't start an optimistic reorder whose dispatch
    // the parent is going to short-circuit anyway.
    if (this.interactionsDisabled) return;
    // Don't start a drag on a pending task.
    const existing = this.slotState.get(todoKey(task.path, task.repo));
    if (existing?.kind === "pending") return;

    // Drag-out gate for OVERDUE and DUE_TODAY (design.md §4). Both
    // buckets are keyed off `due_date`, which the TodoReorder wire op
    // cannot change. Allowing drag-out would let the user appear to
    // move the task to e.g. TOMORROW only to have it snap back into
    // its source bucket on the next render. Block drag entirely from
    // these buckets; the user changes due_date elsewhere in the UI.
    // (This also blocks within-bucket reorder for these buckets — a
    // deliberate simplification; see design.md §4.)
    const sourceGroup = this._cachedGroups[groupIdx];
    if (
      sourceGroup &&
      DRAG_LOCKED_SOURCE_HEADER_CLASSES.has(sourceGroup.headerClass)
    ) {
      return;
    }

    const handle = e.currentTarget as HTMLElement;
    handle.setPointerCapture(e.pointerId);

    const taskListEl = this.shadowRoot!.querySelector(".task-list") as HTMLElement;
    if (!taskListEl) return;

    const taskKey = todoKey(task.path, task.repo);

    // Multi-select drag: if the dragged task is selected, all selected tasks
    // participate. If it's not selected, clear the selection (standard
    // file-manager behavior: dragging an unselected item clears selection).
    let dragSelectedKeys: Set<string> | null = null;
    if (this.selectedKeys.has(taskKey) && this.selectedKeys.size > 1) {
      dragSelectedKeys = new Set(this.selectedKeys);
    } else if (this.selectedKeys.size > 0 && !this.selectedKeys.has(taskKey)) {
      this.clearSelection();
    }

    this._drag = {
      sourceKey: taskKey,
      sourceTask: task,
      sourceGroupIdx: groupIdx,
      sourceTaskIdx: taskIdx,
      ghostEl: null,
      handleEl: handle,
      taskListEl,
      pointerId: e.pointerId,
      startY: e.clientY,
      lastClientY: e.clientY,
      active: false,
      dropMode: "reorder",
      dropGroupIdx: groupIdx,
      dropInsertIdx: taskIdx,
      scrollRaf: null,
      selectedKeys: dragSelectedKeys,
    };

    this._boundDragMove = (ev: PointerEvent) => this._onDragMove(ev);
    this._boundDragEnd = (ev: PointerEvent) => this._onDragEnd(ev);
    this._boundDragKeydown = (ev: KeyboardEvent) => {
      if (ev.key === "Escape") this._cancelDrag();
    };

    handle.addEventListener("pointermove", this._boundDragMove);
    handle.addEventListener("pointerup", this._boundDragEnd);
    handle.addEventListener("pointercancel", this._boundDragEnd);
    document.addEventListener("keydown", this._boundDragKeydown);

    e.preventDefault();
  }

  private _onDragMove(e: PointerEvent): void {
    const drag = this._drag;
    if (!drag) return;

    const dy = Math.abs(e.clientY - drag.startY);

    // Dead zone check.
    if (!drag.active) {
      if (dy < BrennTodoList.DRAG_DEAD_ZONE) return;
      // Enter active drag mode.
      drag.active = true;
      this._createGhost(e.clientX, e.clientY);
      // Re-render to apply .dragging class to the source row(s).
      this.requestUpdate();
    }

    // Update ghost position.
    if (drag.ghostEl) {
      drag.ghostEl.style.left = `${e.clientX + 12}px`;
      drag.ghostEl.style.top = `${e.clientY - 16}px`;
    }

    // Track last Y for auto-scroll drop target updates.
    drag.lastClientY = e.clientY;

    // Compute drop target.
    this._updateDropTarget(e.clientY);

    // Auto-scroll.
    this._handleAutoScroll(e.clientY);
  }

  private _onDragEnd(e: PointerEvent): void {
    const drag = this._drag;
    if (!drag) return;

    if (drag.active) {
      if (drag.dropMode === "schedule") {
        this._dispatchScheduleDrop(drag);
        this._cleanupDrag();
        return;
      }
      // Check for no-op: same group and same index (based on primary task).
      const isNoop =
        drag.dropGroupIdx === drag.sourceGroupIdx &&
        (drag.dropInsertIdx === drag.sourceTaskIdx ||
         drag.dropInsertIdx === drag.sourceTaskIdx + 1);

      if (!isNoop && this.onReorder) {
        const targetGroup = this._cachedGroups[drag.dropGroupIdx];
        if (targetGroup) {
          const groupTasks = targetGroup.tasks;
          const insertIdx = drag.dropInsertIdx;

          // The set of keys to skip when finding anchors: all participating
          // tasks (multi-select) or just the source (single-task).
          const skipKeys = drag.selectedKeys ?? new Set([drag.sourceKey]);

          // Determine anchors: the tasks immediately above and below the
          // insertion point, excluding all participating tasks.
          let afterTask: TodoItem | null = null;
          let beforeTask: TodoItem | null = null;

          // Walk backward to find a non-participating task above.
          for (let i = insertIdx - 1; i >= 0; i--) {
            if (!skipKeys.has(todoKey(groupTasks[i].path, groupTasks[i].repo))) {
              afterTask = groupTasks[i];
              break;
            }
          }
          // Walk forward to find a non-participating task below.
          for (let i = insertIdx; i < groupTasks.length; i++) {
            if (!skipKeys.has(todoKey(groupTasks[i].path, groupTasks[i].repo))) {
              beforeTask = groupTasks[i];
              break;
            }
          }

          // At least one anchor must be present (guaranteed since we checked
          // for no-op and the group has non-participating tasks).
          if (afterTask || beforeTask) {
            const orderedSelectedKeys = this._orderSelectedKeysByDisplay(
              drag.selectedKeys,
            );

            this.onReorder({
              path: drag.sourceTask.path,
              repo: drag.sourceTask.repo ?? undefined,
              after: afterTask
                ? { path: afterTask.path, repo: afterTask.repo }
                : null,
              before: beforeTask
                ? { path: beforeTask.path, repo: beforeTask.repo }
                : null,
              targetGroupDate: targetGroup.canonicalDate,
              selectedKeys: orderedSelectedKeys,
            });
          }
        }
      }
    }

    this._cleanupDrag();
  }

  /** Project a participating-keys set into a display-ordered array
   *  by walking `_cachedGroups`. Returns `null` when the input is
   *  `null` (single-task drag) so callers can pass the result
   *  straight through to the wire-shape `selectedKeys: string[] |
   *  null`. Used by both the reorder branch of `_onDragEnd` and
   *  `_dispatchScheduleDrop`. */
  private _orderSelectedKeysByDisplay(
    selectedKeys: Set<string> | null,
  ): string[] | null {
    if (selectedKeys === null) return null;
    const ordered: string[] = [];
    for (const group of this._cachedGroups) {
      for (const t of group.tasks) {
        const k = todoKey(t.path, t.repo);
        if (selectedKeys.has(k)) ordered.push(k);
      }
    }
    return ordered;
  }

  /** Build and dispatch a `ScheduleTarget` for a heading-drop drag.
   *  Used by `_onDragEnd` when `dropMode === "schedule"`.
   *
   *  No-op detection: a same-bucket schedule drop (target group's
   *  canonicalDate equals the dragged task's `effective_date`) is
   *  filtered out here. For multi-select with mixed source dates,
   *  the dispatch fires for the full set; per-task no-op filtering
   *  is unnecessary because graf's `todo_schedule` is idempotent on
   *  a same-date schedule. See design.md §4.6. */
  private _dispatchScheduleDrop(drag: NonNullable<typeof this._drag>): void {
    if (!this.onSchedule) return;
    const targetGroup = this._cachedGroups[drag.dropGroupIdx];
    if (!targetGroup) {
      // Invariant violation: `dropGroupIdx` is set by `_updateDropTarget`
      // from `_cachedGroups[gi]`, so a missing entry here means the
      // cached groups changed under us between hit-test and dispatch.
      // Surface for visibility per CLAUDE.md "better dead than wrong".
      console.warn(
        `_dispatchScheduleDrop: dropGroupIdx=${drag.dropGroupIdx} not in _cachedGroups (length=${this._cachedGroups.length}); dropping dispatch`,
      );
      return;
    }
    const targetDate = targetGroup.canonicalDate;
    if (targetDate === null) {
      // Invariant violation: hit-test rejects pseudo-bucket headings
      // (`canonicalDate === null`) before they can become a schedule
      // target. Reaching this branch means either a state-machine
      // bug or a future regression in `hitTestDrop`. Warn loudly.
      console.warn(
        `_dispatchScheduleDrop: targetGroup.canonicalDate is null (headerClass=${targetGroup.headerClass}); hit-test should have rejected — dropping dispatch`,
      );
      return;
    }

    // Project the participating-keys set into display order for
    // multi-select schedule. `null` passes through to the brenn-app
    // handler's "single-task" path.
    const orderedSelectedKeys = this._orderSelectedKeysByDisplay(
      drag.selectedKeys,
    );

    // Single-task no-op (same-bucket drop): pointer captured a single
    // task whose `effective_date` already matches the target. Bail out
    // — graf would treat this as a redundant write.
    if (
      orderedSelectedKeys === null &&
      drag.sourceTask.effective_date === targetDate
    ) {
      return;
    }

    this.onSchedule({
      path: drag.sourceTask.path,
      repo: drag.sourceTask.repo ?? undefined,
      date: targetDate,
      selectedKeys: orderedSelectedKeys,
    });
  }

  private _cancelDrag(): void {
    this._cleanupDrag();
  }

  private _cleanupDrag(): void {
    const drag = this._drag;
    if (!drag) return;

    // Remove ghost.
    if (drag.ghostEl) {
      drag.ghostEl.remove();
    }

    // Cancel auto-scroll.
    if (drag.scrollRaf != null) {
      cancelAnimationFrame(drag.scrollRaf);
    }

    // Strip every drop-visual artifact (reorder bar + schedule-target
    // heading highlight). One call covers both cues.
    this._clearDropVisuals();

    // Clean up event listeners on the original handle element (stored ref,
    // not re-queried — survives Lit re-renders that replace DOM nodes).
    const handle = drag.handleEl;
    if (this._boundDragMove) {
      handle.removeEventListener("pointermove", this._boundDragMove);
    }
    if (this._boundDragEnd) {
      handle.removeEventListener("pointerup", this._boundDragEnd);
      handle.removeEventListener("pointercancel", this._boundDragEnd);
    }
    if (this._boundDragKeydown) {
      document.removeEventListener("keydown", this._boundDragKeydown);
    }

    this._boundDragMove = null;
    this._boundDragEnd = null;
    this._boundDragKeydown = null;
    this._drag = null;

    // Re-render to remove .dragging class.
    this.requestUpdate();
  }

  private _createGhost(x: number, y: number): void {
    const drag = this._drag!;
    const count = drag.selectedKeys?.size ?? 1;
    const ghost = document.createElement("div");
    ghost.className = "drag-ghost";
    ghost.textContent = count > 1
      ? `${drag.sourceTask.tldr} (+${count - 1})`
      : drag.sourceTask.tldr;
    ghost.style.left = `${x + 12}px`;
    ghost.style.top = `${y - 16}px`;
    this.shadowRoot!.appendChild(ghost);
    drag.ghostEl = ghost;
  }

  /** Find the drop target for the given pointer Y coordinate.
   *
   *  Thin wrapper: build `HitTestRow[]` from the current DOM + cached
   *  groups, delegate the hit-test to the pure `hitTestDrop` function,
   *  and apply the change-detection guard before re-rendering visuals.
   *  See design.md §4.3. */
  private _updateDropTarget(clientY: number): void {
    const drag = this._drag;
    if (!drag) return;

    const taskList = drag.taskListEl;
    // Exclude every non-live variant — settled / dismissed / pending
    // rows share the `.task-row` class but either carry no
    // `data-group-idx` / `data-task-idx` attrs (settled, dismissed,
    // pending) or represent an in-flight mutation not available for
    // drop targeting. Including them would cause `parseInt(undefined)`
    // to silently vote `(gi=0, ti=0)` and corrupt drop-target scoring.
    // Pending is also filtered at drag-start (`_onDragStart` returns
    // early), but excluding in the drop-selector too is cheaper and
    // explicit.
    const elements = Array.from(
      taskList.querySelectorAll<HTMLElement>(
        ".task-row:not(.settled):not(.dismissed):not(.pending), .group-header",
      ),
    );

    const rows: HitTestRow[] = [];
    for (const el of elements) {
      const rect = el.getBoundingClientRect();
      if (el.classList.contains("group-header")) {
        const gi = parseInt(el.dataset.groupIdx ?? "0", 10);
        const tg = this._cachedGroups[gi];
        rows.push({
          kind: "header",
          gi,
          headerClass: tg?.headerClass,
          canonicalDate: tg?.canonicalDate ?? null,
          top: rect.top,
          bottom: rect.bottom,
        });
      } else {
        const gi = parseInt(el.dataset.groupIdx ?? "0", 10);
        const ti = parseInt(el.dataset.taskIdx ?? "0", 10);
        const tg = this._cachedGroups[gi];
        // Pre-filter pseudo-bucket task rows here so the row-pass in
        // `hitTestDrop` doesn't need to know about `_cachedGroups` —
        // the helper stays a pure function over its inputs.
        if (tg && !isDroppable(tg.headerClass, drag.sourceGroupIdx, gi)) {
          continue;
        }
        rows.push({
          kind: "task",
          gi,
          ti,
          top: rect.top,
          bottom: rect.bottom,
        });
      }
    }

    const result = hitTestDrop(rows, clientY, drag.sourceGroupIdx);
    if (result === null) {
      // No candidate (e.g., pointer above first heading or below last
      // row in a non-droppable region). Keep the previous drop target.
      return;
    }

    // Change-detection guard: a row→header flip at equal `(gi,
    // insertIdx)` (e.g., pointer over "above the first task in TODAY"
    // versus "on the TODAY heading") would not trigger
    // `_renderDropVisuals` without the `dropMode` term — leaving
    // stale visuals on screen. The `mode === "reorder"` guard on
    // `insertIdx` reflects that schedule mode has no insert index
    // (graf removes any prior `sort_order` on schedule).
    const newMode = result.mode;
    const newGi = result.gi;
    const newInsertIdx = result.insertIdx ?? -1;
    const changed =
      newMode !== drag.dropMode ||
      newGi !== drag.dropGroupIdx ||
      (newMode === "reorder" && newInsertIdx !== drag.dropInsertIdx);
    drag.dropMode = newMode;
    drag.dropGroupIdx = newGi;
    if (newMode === "reorder") {
      drag.dropInsertIdx = newInsertIdx;
    }
    if (!changed) return;

    if (newMode === "reorder") {
      this._renderDropVisuals("reorder", newGi, newInsertIdx);
    } else {
      this._renderDropVisuals("schedule", newGi);
    }
  }

  /** Render the drop visuals for the current drag state. Mutually
   *  exclusive: reorder shows the 2-px bar between rows; schedule
   *  shows the heading-block highlight on the target heading.
   *
   *  Always clears prior visuals first, so calling this on every
   *  state change is safe — see design.md §4.4 / §4.5. */
  private _renderDropVisuals(
    mode: "reorder" | "schedule",
    groupIdx: number,
    insertIdx?: number,
  ): void {
    this._clearDropVisuals();
    const taskList = this._drag?.taskListEl;
    if (!taskList) return;

    if (mode === "reorder") {
      // Insert the 2-px bar between rows. If insertIdx === 0, the
      // bar goes right after the group header; otherwise it goes
      // after the (insertIdx - 1)th task in the group.
      let refElement: Element | null = null;
      if (insertIdx === 0 || insertIdx === undefined) {
        const header = taskList.querySelector(
          `.group-header[data-group-idx="${groupIdx}"]`,
        );
        refElement = header?.nextElementSibling ?? null;
      } else {
        const row = taskList.querySelector(
          `.task-row[data-group-idx="${groupIdx}"][data-task-idx="${insertIdx - 1}"]`,
        );
        refElement = row?.nextElementSibling ?? null;
      }
      const indicator = document.createElement("div");
      indicator.className = "drop-indicator";
      if (refElement) {
        taskList.insertBefore(indicator, refElement);
      } else {
        taskList.appendChild(indicator);
      }
      return;
    }

    // Schedule mode: highlight the target heading.
    const header = taskList.querySelector(
      `.group-header[data-group-idx="${groupIdx}"]`,
    );
    if (header) header.classList.add("schedule-target");
  }

  /** Strip every drop-visual artifact (the 2-px reorder bar and any
   *  `.schedule-target` heading highlight). Called from `_cleanupDrag`
   *  and at the top of `_renderDropVisuals` so a state transition
   *  always paints a clean slate. */
  private _clearDropVisuals(): void {
    const indicators = this.shadowRoot?.querySelectorAll(".drop-indicator");
    if (indicators) {
      for (const el of indicators) el.remove();
    }
    const headers = this.shadowRoot?.querySelectorAll(
      ".group-header.schedule-target",
    );
    if (headers) {
      for (const el of headers) el.classList.remove("schedule-target");
    }
  }

  /** Auto-scroll when pointer is near the edge of the scroll container. */
  private _handleAutoScroll(clientY: number): void {
    const drag = this._drag;
    if (!drag) return;

    const taskList = drag.taskListEl;
    const rect = taskList.getBoundingClientRect();
    const edge = BrennTodoList.SCROLL_EDGE_PX;
    const maxSpeed = BrennTodoList.SCROLL_SPEED;

    let scrollDelta = 0;
    if (clientY < rect.top + edge) {
      // Near top edge — scroll up.
      const proximity = Math.max(0, rect.top + edge - clientY);
      scrollDelta = -(proximity / edge) * maxSpeed;
    } else if (clientY > rect.bottom - edge) {
      // Near bottom edge — scroll down.
      const proximity = Math.max(0, clientY - (rect.bottom - edge));
      scrollDelta = (proximity / edge) * maxSpeed;
    }

    // Cancel any existing scroll animation.
    if (drag.scrollRaf != null) {
      cancelAnimationFrame(drag.scrollRaf);
      drag.scrollRaf = null;
    }

    if (scrollDelta !== 0) {
      const doScroll = () => {
        if (!this._drag) return;
        taskList.scrollTop += scrollDelta;
        // Rows shifted under the pointer — update drop target using last known Y.
        this._updateDropTarget(this._drag.lastClientY);
        this._drag.scrollRaf = requestAnimationFrame(doScroll);
      };
      drag.scrollRaf = requestAnimationFrame(doScroll);
    }
  }

  private _priorityClass(p: number): string {
    if (p === 1) return "p1";
    if (p === 2) return "p2";
    return "p3";
  }

  /** Phase 4 §5.4: pick a pending label that matches the in-flight
   *  action. Falls back to "working…" when the slot entry is absent
   *  or not pending (defensive — a live row only renders when
   *  `kind === "pending"`, so this should not fire in practice). */
  private _pendingLabel(key: string): string {
    const entry = this.slotState.get(key);
    const action: TodoPendingAction | undefined =
      entry?.kind === "pending" ? entry.action : undefined;
    switch (action) {
      case "done":
        return "completing…";
      case "snooze":
        return "snoozing…";
      case "reorder":
        return "reordering…";
      case "schedule":
        return "scheduling…";
      default:
        return "working…";
    }
  }

  private _handleDone(task: TodoItem): void {
    if (this.onDone) {
      this.onDone(task.path, task.repo ?? undefined);
    }
  }

  private _handleSnooze(task: TodoItem, days = 1): void {
    if (this.onSnooze) {
      this.onSnooze(task.path, task.repo ?? undefined, task.effective_date, days);
    }
  }

  // --- Snooze menu handlers (Phase 4) ---

  /** Mobile breakpoint (coarse pointer) — use the centered-sheet fallback. */
  private _mobileMenuSheet(): boolean {
    return window.matchMedia("(pointer: coarse)").matches;
  }

  /** Open the snooze menu for a row. `focusFirst` lands focus on the first
   *  menuitem (keyboard / click both land on first-item). */
  private _openSnoozeMenu(key: string): void {
    // Close any existing menu first (only one open at a time).
    if (this._snoozeMenuKey === key) return;
    this._snoozeMenuAsSheet = this._mobileMenuSheet();
    this._snoozeMenuKey = key;
    this._installOutsideClick();
    this._installViewportListeners();
    // After render, position the menu under the caret (fixed-positioned
    // so it escapes the task-list clipping rect) and move focus to the
    // first menu item.
    this.updateComplete.then(() => {
      this._positionSnoozeMenu();
      this._focusMenuItem(0);
    });
  }

  /** While the menu is open, any scroll (of the list, the window, or an
   *  ancestor) or resize moves the caret relative to the menu's fixed
   *  viewport coordinates. Matching native `<select>` behavior, close
   *  the menu rather than chase the caret with a reposition — cheap,
   *  robust, and matches the design §10 risk acknowledgement. */
  private _boundViewportClose: (() => void) | null = null;
  private _installViewportListeners(): void {
    if (this._boundViewportClose) return;
    this._boundViewportClose = () => {
      this._closeSnoozeMenu(false);
    };
    // Capture + passive on scroll catches ancestor scrolls (e.g. the
    // inner `.task-list`) that don't bubble to window; passive since
    // we never call preventDefault.
    window.addEventListener("scroll", this._boundViewportClose, {
      capture: true,
      passive: true,
    });
    window.addEventListener("resize", this._boundViewportClose);
  }

  private _removeViewportListeners(): void {
    if (!this._boundViewportClose) return;
    window.removeEventListener("scroll", this._boundViewportClose, {
      capture: true,
    });
    window.removeEventListener("resize", this._boundViewportClose);
    this._boundViewportClose = null;
  }

  /** Position the desktop menu just below the caret, clamped so it
   *  doesn't overflow the viewport. On the mobile-sheet breakpoint this
   *  is a no-op — CSS handles the centering via `!important`. */
  private _positionSnoozeMenu(): void {
    if (this._snoozeMenuAsSheet) return;
    const root = this.shadowRoot;
    if (!root) return;
    const menu = root.querySelector(".snooze-menu") as HTMLElement | null;
    if (!menu) return;
    const caret = root.querySelector(
      `[aria-expanded="true"].snooze-caret`,
    ) as HTMLElement | null;
    if (!caret) return;
    const cr = caret.getBoundingClientRect();
    const menuRect = menu.getBoundingClientRect();
    // Default: anchor menu's right edge to caret's right edge, placing
    // it just below the caret.
    let left = cr.right - menuRect.width;
    let top = cr.bottom + 2;
    const pad = 4;
    const vw = window.innerWidth;
    const vh = window.innerHeight;
    if (left < pad) left = pad;
    if (left + menuRect.width > vw - pad) left = vw - pad - menuRect.width;
    if (top + menuRect.height > vh - pad) {
      // Flip above the caret if no room below.
      const aboveTop = cr.top - menuRect.height - 2;
      if (aboveTop >= pad) top = aboveTop;
    }
    menu.style.left = `${left}px`;
    menu.style.top = `${top}px`;
  }

  /** Close the snooze menu. If `returnFocusToCaret` is true, focus the
   *  originating caret button (Esc / selection path). */
  private _closeSnoozeMenu(returnFocusToCaret: boolean): void {
    if (this._snoozeMenuKey === null) return;
    const caretId = this._caretIdForKey(this._snoozeMenuKey);
    this._snoozeMenuKey = null;
    this._removeOutsideClick();
    this._removeViewportListeners();
    if (returnFocusToCaret && caretId) {
      this.updateComplete.then(() => {
        const el = this.shadowRoot?.getElementById(caretId) as
          | HTMLButtonElement
          | null;
        el?.focus();
      });
    }
  }

  /** Resolve the caret id for a menu-owning row key by walking the rendered
   *  rows. Needed because ids are derived from groupIdx/taskIdx. */
  private _caretIdForKey(key: string): string | null {
    const row = this.shadowRoot?.querySelector(
      `.task-row[data-task-key="${CSS.escape(key)}"]`,
    ) as HTMLElement | null;
    if (!row) return null;
    const caret = row.querySelector(".snooze-caret") as HTMLElement | null;
    return caret?.id ?? null;
  }

  private _installOutsideClick(): void {
    this._snoozeMenuController.open();
  }

  private _removeOutsideClick(): void {
    this._snoozeMenuController.close();
  }

  private _onCaretClick(e: MouseEvent, key: string): void {
    e.stopPropagation();
    if (this._snoozeMenuKey === key) {
      this._closeSnoozeMenu(false);
    } else {
      this._openSnoozeMenu(key);
    }
  }

  private _onCaretKeydown(e: KeyboardEvent, key: string): void {
    // APG Menu Button pattern: Enter/Space/ArrowDown open the menu and
    // focus the first item. (Browsers already fire a click on Enter/
    // Space by default; intercept ArrowDown too.)
    if (e.key === "ArrowDown" || e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      e.stopPropagation();
      this._openSnoozeMenu(key);
    } else if (e.key === "Escape") {
      if (this._snoozeMenuKey === key) {
        e.preventDefault();
        this._closeSnoozeMenu(true);
      }
    }
  }

  /** Focus the Nth menu item (0-based, wraps). */
  private _focusMenuItem(idx: number): void {
    const menu = this.shadowRoot?.querySelector(".snooze-menu");
    if (!menu) return;
    const items = Array.from(
      menu.querySelectorAll<HTMLElement>('[role="menuitem"]'),
    );
    if (items.length === 0) return;
    const wrapped = ((idx % items.length) + items.length) % items.length;
    items[wrapped].focus();
  }

  private _onMenuKeydown(e: KeyboardEvent, task: TodoItem): void {
    const key = e.key;
    if (key === "Escape") {
      e.preventDefault();
      e.stopPropagation();
      this._closeSnoozeMenu(true);
      return;
    }
    if (key === "Tab") {
      // Let focus flow out — close the menu without stealing tab.
      this._closeSnoozeMenu(false);
      return;
    }
    const target = e.target as HTMLElement | null;
    const idxStr = target?.getAttribute("data-menu-idx") ?? null;
    const idx = idxStr !== null ? parseInt(idxStr, 10) : -1;
    if (key === "ArrowDown") {
      e.preventDefault();
      this._focusMenuItem(idx + 1);
    } else if (key === "ArrowUp") {
      e.preventDefault();
      this._focusMenuItem(idx - 1);
    } else if (key === "Home") {
      e.preventDefault();
      this._focusMenuItem(0);
    } else if (key === "End") {
      e.preventDefault();
      this._focusMenuItem(-1); // wraps to last
    } else if (key === "Enter" || key === " ") {
      e.preventDefault();
      e.stopPropagation();
      if (idx >= 0 && idx < SNOOZE_MENU_ENTRIES.length) {
        this._selectMenuEntry(task, SNOOZE_MENU_ENTRIES[idx].days);
      }
    }
  }

  private _selectMenuEntry(task: TodoItem, days: number): void {
    this._closeSnoozeMenu(false);
    this._handleSnooze(task, days);
  }

  private _handleRefresh(): void {
    // Button is `?disabled=${this.refreshPending}` so clicks can't fire while
    // busy; the parent owns the authoritative re-entrancy guard.
    if (this.onRefresh) {
      this.onRefresh();
    }
  }

  private _handleCollapse(): void {
    if (this.onCollapse) {
      this.onCollapse();
    }
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "brenn-todo-list": BrennTodoList;
  }
}
