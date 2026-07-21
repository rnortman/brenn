/**
 * Date helpers shared across components.
 *
 * Small browser-local-TZ helpers for YYYY-MM-DD math. These power the
 * todo list's section headers as well as the snooze button's target-date
 * computation (Phase 3: `max(today, effective_date) + 1`). Phase 2's
 * done button reuses the same helpers.
 */

/** Format a `Date` as YYYY-MM-DD in the browser's local timezone. */
function formatDateStr(d: Date): string {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

/** Format today's date as YYYY-MM-DD in the browser's local timezone. */
export function localTodayStr(): string {
  return formatDateStr(new Date());
}

/**
 * Add N days to a YYYY-MM-DD string, return YYYY-MM-DD.
 *
 */
export function addDays(dateStr: string, n: number): string {
  const d = new Date(dateStr + "T00:00:00");
  d.setDate(d.getDate() + n);
  return formatDateStr(d);
}

/**
 * Format a YYYY-MM-DD string as a short, locale-aware month+day label
 * (e.g. "Apr 18").
 *
 * Shared by the todo list's future-date section headers, the snooze
 * split-button tooltip / menu labels, and the Phase 4 "Next: MM/DD"
 * toast. Parsed as local-time so the label matches the user's wall
 * clock, same convention as the rest of the frontend.
 */
export function shortDate(dateStr: string): string {
  const d = new Date(dateStr + "T00:00:00");
  return d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

/**
 * Compute the target date a snooze action will land on:
 * `addDays(max(todayStr, effectiveDate), days)`.
 *
 * Mirrors the rule in `prd-snooze.md` §2-3: the snooze caret changes only
 * `N` in `max(today, effective_date) + N`. A stale `tentative_date` that
 * pushed `effective_date` into the future still compounds correctly —
 * snoozing a task already surfaced next week lands it `days` after that,
 * not `days` after today.
 *
 * Used by both `_handleTodoSnooze` (app.ts) and the snooze menu label
 * renderer (todo-list.ts) so the tooltip, the menu's right-column date,
 * and the actual dispatched `TodoSchedule.date` cannot drift apart.
 */
export function snoozeTargetDate(
  effectiveDate: string,
  todayStr: string,
  days: number,
): string {
  const base = effectiveDate > todayStr ? effectiveDate : todayStr;
  return addDays(base, days);
}
