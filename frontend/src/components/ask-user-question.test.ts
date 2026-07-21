// @vitest-environment happy-dom
import { describe, it, expect, afterEach, vi } from "vitest";
import "./ask-user-question.js";
import type { BrennAskUserQuestion } from "./ask-user-question.js";
import { setChatHasText } from "../keyboard-guard.js";
import {
  captureToolResponses,
  advancePastGrace,
} from "../test-helpers/keyboard-guard-helpers.js";

afterEach(() => {
  document.body.replaceChildren();
  setChatHasText(false);
  vi.restoreAllMocks();
});

/**
 * Minimal valid AuqPayload for a single single-select question with one option.
 * Single-select single-question means isQuickSubmit=true: pressing the option
 * (or number key '1') auto-submits, which is what we test for number-key dispatch.
 */
const SINGLE_SELECT_PAYLOAD = JSON.stringify({
  questions: [
    {
      question: "Pick one?",
      header: "Category",
      options: [{ label: "Groceries", description: "" }],
      multiSelect: false,
    },
  ],
  rendered: {
    questions: [
      {
        question_html: "<p>Pick one?</p>",
        options: [{ label_html: "Groceries", description_html: "" }],
      },
    ],
  },
  enter_sends: true,
});

/**
 * Minimal valid AuqPayload for a single multi-select question with one option.
 * multi-select so clicking an option button doesn't auto-submit (isQuickSubmit
 * requires single-select); that lets us test Enter submission separately.
 */
const MULTI_SELECT_PAYLOAD = JSON.stringify({
  questions: [
    {
      question: "What category?",
      header: "Category",
      options: [{ label: "Groceries", description: "" }],
      multiSelect: true,
    },
  ],
  rendered: {
    questions: [
      {
        question_html: "<p>What category?</p>",
        options: [{ label_html: "Groceries", description_html: "" }],
      },
    ],
  },
  enter_sends: true,
});

/**
 * Mount a `<brenn-ask-user-question>` with an embedded
 * `<script type="application/json">` child holding the given JSON payload.
 */
function mountAuq(payload: string = MULTI_SELECT_PAYLOAD): BrennAskUserQuestion {
  const el = document.createElement(
    "brenn-ask-user-question",
  ) as BrennAskUserQuestion;

  const script = document.createElement("script");
  script.type = "application/json";
  script.textContent = payload;
  el.appendChild(script);

  document.body.appendChild(el);
  return el;
}



/**
 * Click the first option button inside AUQ's shadow root to drive `allAnswered`
 * to true via the public click path (not private state mutation).
 * Asserts that the button was found and the selected class was applied, so the
 * test fails loudly if mount/render hasn't completed.
 */
async function selectFirstOption(el: BrennAskUserQuestion): Promise<void> {
  await el.updateComplete;
  const btn = el.shadowRoot?.querySelector<HTMLButtonElement>(".auq-option-btn");
  expect(btn, "auq-option-btn must exist after updateComplete — check mountAuq payload and connectedCallback").not.toBeNull();
  btn!.click();
  await el.updateComplete;
  expect(
    btn!.getAttribute("aria-pressed"),
    "option button must have aria-pressed='true' after click — check that allAnswered will be true",
  ).toBe("true");
}

describe("brenn-ask-user-question keyboard interception", () => {
  it("grace period blocks Enter on the host before advancePastGrace() is called", async () => {
    const el = mountAuq();
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
    const el = mountAuq();
    await selectFirstOption(el);
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

  it("self-Enter (Enter dispatched on the host element) accepts when allAnswered", async () => {
    const el = mountAuq();
    await selectFirstOption(el);
    advancePastGrace();

    const captured = captureToolResponses();
    el.dispatchEvent(
      new KeyboardEvent("keydown", { key: "Enter", bubbles: true, composed: true }),
    );

    expect(captured.events).toHaveLength(1);
    const detail = captured.events[0].detail as { questions: unknown; answers: Record<string, string> };
    expect(detail.answers).toBeDefined();
    captured.dispose();
  });

  it("chat-input repro: Enter on sibling after send() clears chatHasText does not accept", async () => {
    const el = mountAuq();
    await selectFirstOption(el);
    advancePastGrace();

    // Simulate chat textarea with text.
    setChatHasText(true);
    const chatTextarea = document.createElement("textarea");
    chatTextarea.value = "hello";
    document.body.appendChild(chatTextarea);

    const captured = captureToolResponses();

    // Simulate input-bar.send() clearing chatHasText synchronously mid-event.
    chatTextarea.addEventListener("keydown", () => {
      setChatHasText(false);
    });

    chatTextarea.dispatchEvent(
      new KeyboardEvent("keydown", { key: "Enter", bubbles: true, composed: true }),
    );

    expect(captured.events).toHaveLength(0);
    captured.dispose();
  });

  it("empty-chat Enter on sibling does not accept the AUQ dialog", async () => {
    const el = mountAuq();
    await selectFirstOption(el);
    advancePastGrace();

    // Chat is empty — canInterceptKeyboard would now allow without origin check.
    setChatHasText(false);

    const sibling = document.createElement("textarea");
    document.body.appendChild(sibling);

    const captured = captureToolResponses();
    sibling.dispatchEvent(
      new KeyboardEvent("keydown", { key: "Enter", bubbles: true, composed: true }),
    );

    expect(captured.events).toHaveLength(0);
    captured.dispose();
  });

  it("number key '1' dispatched on a sibling outside the host does not dispatch brenn-tool-response", async () => {
    // Use single-select so pressing '1' would auto-submit if allowed through.
    const el = mountAuq(SINGLE_SELECT_PAYLOAD);
    await el.updateComplete;
    advancePastGrace();

    const sibling = document.createElement("textarea");
    document.body.appendChild(sibling);

    const captured = captureToolResponses();
    sibling.dispatchEvent(
      new KeyboardEvent("keydown", { key: "1", bubbles: true, composed: true }),
    );

    expect(captured.events).toHaveLength(0);
    captured.dispose();
  });

  it("number key '1' dispatched on the AUQ host itself dispatches brenn-tool-response", async () => {
    // Single-select single-question: pressing '1' auto-submits via isQuickSubmit.
    const el = mountAuq(SINGLE_SELECT_PAYLOAD);
    await el.updateComplete;
    advancePastGrace();

    const captured = captureToolResponses();
    el.dispatchEvent(
      new KeyboardEvent("keydown", { key: "1", bubbles: true, composed: true }),
    );

    expect(captured.events).toHaveLength(1);
    const detail = captured.events[0].detail as { answers: Record<string, string> };
    expect(detail.answers).toBeDefined();
    captured.dispose();
  });
});
