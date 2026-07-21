// @vitest-environment happy-dom
import { describe, it, expect, afterEach, vi } from "vitest";
import "./batch-assign-table.js";
import type { BrennPfinBatchAssignTable } from "./batch-assign-table.js";
import { setChatHasText } from "../../keyboard-guard.js";
import {
  captureToolResponses,
  advancePastGrace,
} from "../../test-helpers/keyboard-guard-helpers.js";

afterEach(() => {
  document.body.replaceChildren();
  setChatHasText(false);
  vi.restoreAllMocks();
});

/**
 * Build a `<brenn-pfin-batch-assign-table>` with N `.pfin-batch-assign-row`
 * children, each having accept/reject buttons so the component wires up click
 * handlers in `connectedCallback`.
 */
function mountBatchAssignTable(rowCount: number = 2): BrennPfinBatchAssignTable {
  const el = document.createElement(
    "brenn-pfin-batch-assign-table",
  ) as BrennPfinBatchAssignTable;

  for (let i = 0; i < rowCount; i++) {
    const row = document.createElement("div");
    row.className = "pfin-batch-assign-row";
    row.dataset.index = String(i);

    const acceptBtn = document.createElement("button");
    acceptBtn.className = "pfin-batch-accept";
    acceptBtn.textContent = "Accept";

    const rejectBtn = document.createElement("button");
    rejectBtn.className = "pfin-batch-reject";
    rejectBtn.textContent = "Reject";

    row.appendChild(acceptBtn);
    row.appendChild(rejectBtn);
    el.appendChild(row);
  }

  document.body.appendChild(el);
  return el;
}

/**
 * Drive all rows to "accepted" via click on each row's accept button,
 * simulating real user interaction rather than touching private state.
 */
function acceptAllRows(el: BrennPfinBatchAssignTable): void {
  const acceptBtns = el.querySelectorAll<HTMLButtonElement>(".pfin-batch-accept");
  for (const btn of acceptBtns) {
    btn.click();
  }
}

describe("brenn-pfin-batch-assign-table keyboard interception", () => {
  it("grace period blocks Enter on the host before advancePastGrace() is called", async () => {
    const el = mountBatchAssignTable(2);
    await el.updateComplete;
    // Do NOT call advancePastGrace() — mount grace period is still active.

    const captured = captureToolResponses();
    try {
      el.dispatchEvent(
        new KeyboardEvent("keydown", { key: "Enter", bubbles: true, composed: true }),
      );
      expect(captured.events).toHaveLength(0);
    } finally {
      captured.dispose();
    }
  });

  it("Enter on a sibling outside the host does not trigger submit when all rows decided", async () => {
    const el = mountBatchAssignTable(2);
    await el.updateComplete;
    advancePastGrace();

    acceptAllRows(el);
    await el.updateComplete;

    const sibling = document.createElement("textarea");
    document.body.appendChild(sibling);

    const captured = captureToolResponses();
    sibling.dispatchEvent(
      new KeyboardEvent("keydown", { key: "Enter", bubbles: true, composed: true }),
    );

    expect(captured.events).toHaveLength(0);
    captured.dispose();
  });

  it("Enter dispatched on the host itself triggers submit when all rows decided", async () => {
    const el = mountBatchAssignTable(2);
    await el.updateComplete;
    advancePastGrace();

    acceptAllRows(el);
    await el.updateComplete;

    const captured = captureToolResponses();
    el.dispatchEvent(
      new KeyboardEvent("keydown", { key: "Enter", bubbles: true, composed: true }),
    );

    expect(captured.events).toHaveLength(1);
    const detail = captured.events[0].detail as { decisions: { index: number; accepted: boolean }[] };
    expect(detail.decisions).toEqual(
      expect.arrayContaining([
        { index: 0, accepted: true },
        { index: 1, accepted: true },
      ]),
    );
    captured.dispose();
  });

  it("'A' key from a sibling outside the host does not populate decisions (no submit on subsequent Enter from host)", async () => {
    const el = mountBatchAssignTable(2);
    await el.updateComplete;
    advancePastGrace();

    const sibling = document.createElement("textarea");
    document.body.appendChild(sibling);

    // Dispatch 'A' from outside — origin check must block decision mutation.
    sibling.dispatchEvent(
      new KeyboardEvent("keydown", { key: "A", bubbles: true, composed: true }),
    );

    // Now dispatch Enter from host — if decisions were populated by the sibling
    // 'A' above, submit() would fire.
    const captured = captureToolResponses();
    el.dispatchEvent(
      new KeyboardEvent("keydown", { key: "Enter", bubbles: true, composed: true }),
    );

    expect(captured.events).toHaveLength(0);
    captured.dispose();
  });

  it("'A' key dispatched on the host itself accepts all remaining rows (Enter then submits)", async () => {
    const el = mountBatchAssignTable(2);
    await el.updateComplete;
    advancePastGrace();

    // Dispatch 'A' from host — should accept all rows.
    el.dispatchEvent(
      new KeyboardEvent("keydown", { key: "A", bubbles: true, composed: true }),
    );

    // All rows now decided; Enter from host should submit.
    const captured = captureToolResponses();
    el.dispatchEvent(
      new KeyboardEvent("keydown", { key: "Enter", bubbles: true, composed: true }),
    );

    expect(captured.events).toHaveLength(1);
    const detail = captured.events[0].detail as { decisions: { index: number; accepted: boolean }[] };
    expect(detail.decisions).toEqual(
      expect.arrayContaining([
        { index: 0, accepted: true },
        { index: 1, accepted: true },
      ]),
    );
    captured.dispose();
  });

  it("Enter on host when not all rows decided does not trigger submit", async () => {
    const el = mountBatchAssignTable(2);
    await el.updateComplete;
    advancePastGrace();

    // Accept only the first row (not all rows decided).
    const firstAccept = el.querySelector<HTMLButtonElement>(".pfin-batch-accept")!;
    firstAccept.click();
    await el.updateComplete;

    const captured = captureToolResponses();
    el.dispatchEvent(
      new KeyboardEvent("keydown", { key: "Enter", bubbles: true, composed: true }),
    );

    expect(captured.events).toHaveLength(0);
    captured.dispose();
  });

  it("chat-input repro: Enter on sibling after send() clears chatHasText does not submit", async () => {
    const el = mountBatchAssignTable(1);
    await el.updateComplete;
    advancePastGrace();

    acceptAllRows(el);
    await el.updateComplete;

    setChatHasText(true);
    const chatTextarea = document.createElement("textarea");
    chatTextarea.value = "hello";
    document.body.appendChild(chatTextarea);

    const captured = captureToolResponses();

    // Simulate input-bar.send() clearing chatHasText mid-event.
    chatTextarea.addEventListener("keydown", () => {
      setChatHasText(false);
    });

    chatTextarea.dispatchEvent(
      new KeyboardEvent("keydown", { key: "Enter", bubbles: true, composed: true }),
    );

    expect(captured.events).toHaveLength(0);
    captured.dispose();
  });
});
