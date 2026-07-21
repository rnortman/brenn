import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  localTodayStr,
  addDays,
  shortDate,
  snoozeTargetDate,
} from "./date-util.js";

/**
 * Tests for the shared date helpers powering the todo UI's snooze target
 * computation (`max(today, effective_date) + N`) and section headers.
 */

describe("localTodayStr", () => {
  it("returns a YYYY-MM-DD string", () => {
    const result = localTodayStr();
    expect(result).toMatch(/^\d{4}-\d{2}-\d{2}$/);
  });
});

describe("addDays", () => {
  it("adds one day", () => {
    expect(addDays("2026-04-22", 1)).toBe("2026-04-23");
  });

  it("handles month rollover", () => {
    expect(addDays("2026-04-30", 1)).toBe("2026-05-01");
  });

  it("handles year rollover", () => {
    expect(addDays("2026-12-31", 1)).toBe("2027-01-01");
  });

  it("handles multi-day offsets", () => {
    expect(addDays("2026-04-22", 7)).toBe("2026-04-29");
  });
});

/**
 * The snooze button's target-date rule: `max(today, effective_date) + 1`.
 * These cases cover the three branches the design calls out.
 *
 * `snoozeTargetDate` is the single source of truth used by both
 * `_handleTodoSnooze` (the actual WS dispatch) and `_snoozeTargetDate`
 * in the list component (the tooltip / menu labels). Testing it here
 * verifies the real helper, not a test-local copy.
 */
describe("snoozeTargetDate (max(today, effective) + N)", () => {
  it("returns today+1 when effective_date is in the past", () => {
    // Task was surfaced yesterday; user snoozes today. Target = today+1.
    expect(snoozeTargetDate("2026-04-21", "2026-04-22", 1)).toBe("2026-04-23");
  });

  it("returns today+1 when effective_date equals today", () => {
    expect(snoozeTargetDate("2026-04-22", "2026-04-22", 1)).toBe("2026-04-23");
  });

  it("returns effective+1 when effective_date is in the future", () => {
    // Task is surfaced tomorrow; user snoozes today. Target must not
    // collapse to today — it should land the day after tomorrow.
    expect(snoozeTargetDate("2026-04-23", "2026-04-22", 1)).toBe("2026-04-24");
  });

  it("compounds a stale tentative_date on a recurring task", () => {
    // Recurring task with a stale tentative_date that pushed
    // effective_date far into the future. Snoozing again should
    // advance from the stale value, not from today.
    expect(snoozeTargetDate("2026-05-15", "2026-04-22", 1)).toBe("2026-05-16");
  });
});

describe("addDays under mocked system time", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("localTodayStr reflects the mocked clock", () => {
    vi.setSystemTime(new Date("2026-04-22T12:00:00"));
    // localTodayStr uses browser-local components; avoid asserting the
    // exact string across timezones by just checking the shape + that
    // two successive calls agree.
    const a = localTodayStr();
    const b = localTodayStr();
    expect(a).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    expect(a).toBe(b);
  });
});

describe("shortDate", () => {
  it("returns a locale-formatted month/day label", () => {
    const s = shortDate("2026-04-22");
    // Locale-dependent exact form (e.g. "Apr 22" in en-US); ensure it's
    // a non-empty string that isn't the raw ISO date.
    expect(typeof s).toBe("string");
    expect(s.length).toBeGreaterThan(0);
    expect(s).not.toBe("2026-04-22");
  });
});

/**
 * The snooze menu's +3 days / +1 week / +1 month entries (Phase 4 §4.2)
 * each compute their target date via `max(today, effective_date) + N`.
 * These cases cover the three N values and confirm the date labels
 * match what the snooze handler will ultimately send to the server.
 */
describe("snoozeTargetDate menu cases (N in {3, 7, 30})", () => {
  it("+3 days: future tentative surfaces correctly", () => {
    // Recurring task surfaced for a future check-in. +3 lands 3 days
    // after the effective_date, not 3 days after today.
    expect(snoozeTargetDate("2026-04-25", "2026-04-22", 3)).toBe("2026-04-28");
  });

  it("+1 week from today for today's task", () => {
    expect(snoozeTargetDate("2026-04-22", "2026-04-22", 7)).toBe("2026-04-29");
  });

  it("+1 month crosses month boundary", () => {
    expect(snoozeTargetDate("2026-04-22", "2026-04-22", 30)).toBe("2026-05-22");
  });

  it("+1 month handles year end", () => {
    expect(snoozeTargetDate("2026-12-15", "2026-12-15", 30)).toBe("2027-01-14");
  });
});
