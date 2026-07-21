// @vitest-environment happy-dom
import { describe, it, expect, afterEach, vi } from "vitest";
import "./propose.js";
import type { BrennPfinPropose } from "./propose.js";
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
 * Build a `<brenn-pfin-propose>` with the minimum DOM the component reads
 * in `connectedCallback` — at least one `.pfin-proposal` child so the
 * component has something to focus.
 */
function mountPropose(): BrennPfinPropose {
  const el = document.createElement(
    "brenn-pfin-propose",
  ) as BrennPfinPropose;
  const card = document.createElement("div");
  card.className = "pfin-proposal";
  card.dataset.index = "0";
  card.textContent = "Option A";
  el.appendChild(card);
  document.body.appendChild(el);
  return el;
}

describe("brenn-pfin-propose deny button flow", () => {
  it("clicking the Deny… button enters deny mode (input visible)", async () => {
    const el = mountPropose();
    await el.updateComplete;

    // Initially: action bar with Deny entry button, no input.
    expect(el.querySelector(".pfin-propose-deny-input")).toBeNull();
    const denyBtn = el.querySelector<HTMLButtonElement>(
      ".pfin-propose-deny-btn",
    );
    expect(denyBtn).not.toBeNull();

    denyBtn!.click();
    await el.updateComplete;

    // Now: input is rendered and the Deny entry button is gone.
    const input = el.querySelector<HTMLInputElement>(
      ".pfin-propose-deny-input",
    );
    expect(input).not.toBeNull();
    expect(el.querySelector(".pfin-propose-deny-btn")).toBeNull();
  });

  it("Cancel inside deny mode returns to choose mode without dispatching", async () => {
    const el = mountPropose();
    await el.updateComplete;
    const captured = captureToolResponses();

    el.querySelector<HTMLButtonElement>(".pfin-propose-deny-btn")!.click();
    await el.updateComplete;
    expect(el.querySelector(".pfin-propose-deny-input")).not.toBeNull();

    // Cancel is the first button in the deny-actions bar.
    const cancelBtn = el.querySelector<HTMLButtonElement>(
      ".pfin-propose-deny-actions button:first-child",
    );
    expect(cancelBtn?.textContent).toBe("Cancel");
    cancelBtn!.click();
    await el.updateComplete;

    // Back to choose mode: input gone, Deny entry button restored.
    expect(el.querySelector(".pfin-propose-deny-input")).toBeNull();
    expect(el.querySelector(".pfin-propose-deny-btn")).not.toBeNull();

    // No event was dispatched — Cancel must never deny.
    expect(captured.events).toHaveLength(0);
    captured.dispose();
  });

  it("Send with feedback text dispatches { deny: true, reason }", async () => {
    const el = mountPropose();
    await el.updateComplete;
    const captured = captureToolResponses();

    el.querySelector<HTMLButtonElement>(".pfin-propose-deny-btn")!.click();
    await el.updateComplete;

    const input = el.querySelector<HTMLInputElement>(
      ".pfin-propose-deny-input",
    )!;
    input.value = "wrong category";
    input.dispatchEvent(new Event("input"));
    await el.updateComplete;

    const sendBtn = el.querySelector<HTMLButtonElement>(
      ".pfin-propose-deny-actions button:last-child",
    );
    expect(sendBtn?.textContent).toBe("Send");
    sendBtn!.click();

    expect(captured.events).toHaveLength(1);
    expect(captured.events[0].detail).toEqual({
      deny: true,
      reason: "wrong category",
    });
    captured.dispose();
  });

  it("Send with empty input dispatches { deny: true } without a reason key", async () => {
    const el = mountPropose();
    await el.updateComplete;
    const captured = captureToolResponses();

    el.querySelector<HTMLButtonElement>(".pfin-propose-deny-btn")!.click();
    await el.updateComplete;

    el.querySelector<HTMLButtonElement>(
      ".pfin-propose-deny-actions button:last-child",
    )!.click();

    expect(captured.events).toHaveLength(1);
    // `reason` must be absent (not just undefined) — the dispatch path uses
    // `reason ? { deny, reason } : { deny }` to keep the wire payload clean.
    expect(captured.events[0].detail).toEqual({ deny: true });
    expect("reason" in (captured.events[0].detail as object)).toBe(false);
    captured.dispose();
  });

  it("Send trims whitespace; pure-whitespace input still has no reason", async () => {
    const el = mountPropose();
    await el.updateComplete;
    const captured = captureToolResponses();

    el.querySelector<HTMLButtonElement>(".pfin-propose-deny-btn")!.click();
    await el.updateComplete;

    const input = el.querySelector<HTMLInputElement>(
      ".pfin-propose-deny-input",
    )!;
    input.value = "   ";
    input.dispatchEvent(new Event("input"));
    await el.updateComplete;

    el.querySelector<HTMLButtonElement>(
      ".pfin-propose-deny-actions button:last-child",
    )!.click();

    expect(captured.events).toHaveLength(1);
    expect(captured.events[0].detail).toEqual({ deny: true });
    captured.dispose();
  });
});

describe("brenn-pfin-propose keyboard interception", () => {
  it("grace period blocks Enter on the host before advancePastGrace() is called", async () => {
    const el = mountPropose();
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

  it("spurious accept blocked when Enter targets a sibling outside the host", async () => {
    const el = mountPropose();
    await el.updateComplete;
    advancePastGrace();

    const sibling = document.createElement("textarea");
    document.body.appendChild(sibling);

    const captured = captureToolResponses();
    sibling.dispatchEvent(
      new KeyboardEvent("keydown", { key: "Enter", bubbles: true, composed: true }),
    );

    expect(captured.events).toHaveLength(0);
    captured.dispose();
  });

  it("self-Enter (Enter dispatched on the host element) still accepts", async () => {
    const el = mountPropose();
    await el.updateComplete;
    advancePastGrace();

    const captured = captureToolResponses();
    el.dispatchEvent(
      new KeyboardEvent("keydown", { key: "Enter", bubbles: true, composed: true }),
    );

    expect(captured.events).toHaveLength(1);
    expect(captured.events[0].detail).toEqual({ selected: 0 });
    captured.dispose();
  });

  it("Enter on a descendant card inside the host still accepts", async () => {
    const el = mountPropose();
    await el.updateComplete;
    advancePastGrace();

    const card = el.querySelector<HTMLElement>(".pfin-proposal")!;
    const captured = captureToolResponses();
    card.dispatchEvent(
      new KeyboardEvent("keydown", { key: "Enter", bubbles: true, composed: true }),
    );

    expect(captured.events).toHaveLength(1);
    expect(captured.events[0].detail).toEqual({ selected: 0 });
    captured.dispose();
  });

  it("chat-input repro: Enter on sibling textarea after send() clears chatHasText does not accept", async () => {
    const el = mountPropose();
    await el.updateComplete;
    advancePastGrace();

    // Simulate chat textarea with text (canInterceptKeyboard blocked by chatHasText=true).
    setChatHasText(true);
    const chatTextarea = document.createElement("textarea");
    chatTextarea.value = "hello";
    document.body.appendChild(chatTextarea);

    const captured = captureToolResponses();

    // Simulate what input-bar.send() does: clears chatHasText synchronously before
    // the event finishes bubbling. The origin check must block the accept regardless.
    chatTextarea.addEventListener("keydown", () => {
      setChatHasText(false); // send() clears this during the same event
    });

    chatTextarea.dispatchEvent(
      new KeyboardEvent("keydown", { key: "Enter", bubbles: true, composed: true }),
    );

    expect(captured.events).toHaveLength(0);
    captured.dispose();
  });

  it("empty-chat Enter on sibling does not accept the propose dialog", async () => {
    const el = mountPropose();
    await el.updateComplete;
    advancePastGrace();

    setChatHasText(false); // chat is empty — canInterceptKeyboard would now allow

    const sibling = document.createElement("textarea");
    document.body.appendChild(sibling);

    const captured = captureToolResponses();
    sibling.dispatchEvent(
      new KeyboardEvent("keydown", { key: "Enter", bubbles: true, composed: true }),
    );

    expect(captured.events).toHaveLength(0);
    captured.dispose();
  });

  it("number key '1' on sibling does not accept the propose dialog", async () => {
    const el = mountPropose();
    await el.updateComplete;
    advancePastGrace();

    const sibling = document.createElement("div");
    document.body.appendChild(sibling);

    const captured = captureToolResponses();
    sibling.dispatchEvent(
      new KeyboardEvent("keydown", { key: "1", bubbles: true, composed: true }),
    );

    expect(captured.events).toHaveLength(0);
    captured.dispose();
  });

  it("number key '1' on the host itself still accepts", async () => {
    const el = mountPropose();
    await el.updateComplete;
    advancePastGrace();

    const captured = captureToolResponses();
    el.dispatchEvent(
      new KeyboardEvent("keydown", { key: "1", bubbles: true, composed: true }),
    );

    expect(captured.events).toHaveLength(1);
    expect(captured.events[0].detail).toEqual({ selected: 0 });
    captured.dispose();
  });
});
