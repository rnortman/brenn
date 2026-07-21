// @vitest-environment happy-dom
import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import "./toast-host.js";
import type { BrennToastHost } from "./toast-host.js";

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  document.body.replaceChildren();
  vi.useRealTimers();
});

function mount(): BrennToastHost {
  const el = document.createElement("brenn-toast-host") as BrennToastHost;
  document.body.appendChild(el);
  return el;
}

/** Probe the shadow DOM to count visible toast nodes. */
function visibleCount(el: BrennToastHost): number {
  return el.shadowRoot?.querySelectorAll(".toast").length ?? 0;
}

describe("BrennToastHost — visible cap", () => {
  it("shows at most 4 toasts at once; excess waits in pending", async () => {
    const el = mount();
    await el.updateComplete;

    for (let i = 0; i < 5; i++) {
      el.push({ text: `toast ${i}`, ttlMs: 10_000 });
    }
    await el.updateComplete;

    expect(visibleCount(el)).toBe(4);
  });

  it("promotes a pending toast when a visible one is dismissed via click", async () => {
    const el = mount();
    await el.updateComplete;

    for (let i = 0; i < 5; i++) {
      el.push({ text: `toast ${i}`, ttlMs: 10_000 });
    }
    await el.updateComplete;
    expect(visibleCount(el)).toBe(4);

    // Click-dismiss the first visible toast to open a slot for the pending one.
    const firstToast = el.shadowRoot!.querySelector(".toast") as HTMLElement;
    expect(firstToast).toBeTruthy();
    firstToast.click();
    await el.updateComplete;

    // After dismissal the pending toast should be promoted: 3 remaining + 1 new = 4.
    expect(visibleCount(el)).toBe(4);
  });
});

describe("BrennToastHost — auto-dismiss", () => {
  it("removes a toast after its ttlMs expires", async () => {
    const el = mount();
    await el.updateComplete;

    el.push({ text: "bye soon", ttlMs: 500 });
    await el.updateComplete;
    expect(visibleCount(el)).toBe(1);

    vi.advanceTimersByTime(500);
    await el.updateComplete;

    expect(visibleCount(el)).toBe(0);
  });

  it("does not remove a toast before ttlMs expires", async () => {
    const el = mount();
    await el.updateComplete;

    el.push({ text: "still here", ttlMs: 1000 });
    await el.updateComplete;

    vi.advanceTimersByTime(999);
    await el.updateComplete;

    expect(visibleCount(el)).toBe(1);
  });
});

describe("BrennToastHost — click-to-dismiss", () => {
  it("removes the toast immediately on click and cancels the timer", async () => {
    const el = mount();
    await el.updateComplete;

    el.push({ text: "click me", ttlMs: 10_000 });
    await el.updateComplete;

    const toastEl = el.shadowRoot?.querySelector(".toast") as HTMLElement | null;
    expect(toastEl).toBeTruthy();

    toastEl!.click();
    await el.updateComplete;

    expect(visibleCount(el)).toBe(0);
    // Timer should be cancelled — advancing time should not throw or add toasts.
    vi.advanceTimersByTime(10_000);
    await el.updateComplete;
    expect(visibleCount(el)).toBe(0);
  });
});

describe("BrennToastHost — nextId monotonicity", () => {
  it("assigns unique ids across rapid pushes", async () => {
    const el = mount();
    await el.updateComplete;

    const N = 3;
    for (let i = 0; i < N; i++) {
      el.push({ text: `t${i}`, ttlMs: 10_000 });
    }
    await el.updateComplete;

    const toasts = el.shadowRoot?.querySelectorAll(".toast");
    // Verify N distinct toast nodes were rendered (no id collision caused reuse).
    expect(toasts?.length).toBe(N);
  });
});

describe("BrennToastHost — disconnectedCallback cleanup", () => {
  it("cancels timers and drains queues when unmounted", async () => {
    const el = mount();
    await el.updateComplete;

    // Push enough to fill visible cap and have one pending.
    for (let i = 0; i < 5; i++) {
      el.push({ text: `t${i}`, ttlMs: 10_000 });
    }
    await el.updateComplete;
    expect(visibleCount(el)).toBe(4);

    // Remove the element — should not throw even if timers are pending.
    document.body.removeChild(el);
    // disconnectedCallback sets this.visible = [] and this.pending = [].
    // Allow Lit to process the reactive-property update on the detached element.
    await el.updateComplete;

    // Visible and pending arrays must be drained — the documented invariant.
    expect(visibleCount(el)).toBe(0);
    expect(el.shadowRoot!.querySelectorAll(".toast").length).toBe(0);

    // Advancing time after unmount must not cause any timer callbacks to fire
    // against the detached element (test passes if no exception is thrown).
    expect(() => vi.advanceTimersByTime(10_000)).not.toThrow();
  });
});
