// @vitest-environment happy-dom
//
// Exercises the seq plausibility bound in BrennApp.handleMessage — defense
// against a poisoned high seq silencing future renders.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import "./message-list.js";
import "./app.js";
import type { BrennMessageList } from "./message-list.js";
import { BrennApp, SEQ_JUMP_THRESHOLD } from "./app.js";
import { MockWebSocket } from "../test-utils/mock-websocket.js";
import type { BrennWs } from "../ws.js";

/** Expose private fields needed by bound-check tests. */
interface AppInternals {
  messageList: BrennMessageList;
  lastSeq: number | null;
  ws: BrennWs;
}

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

/** Mount the app and advance past the WS open microtask. */
async function mountApp(): Promise<{ app: BrennApp; ws: MockWebSocket }> {
  const app = document.createElement("brenn-app") as BrennApp;
  document.body.appendChild(app);
  await app.updateComplete;
  const ws = MockWebSocket.instances[0]!;
  await Promise.resolve(); // WS open microtask
  await app.updateComplete;
  return { app, ws };
}

/**
 * Drive the app to a state where layoutReady=true and lastSeq=null.
 * Delivers: Welcome → SetLayout → ConversationSwitched (no history).
 */
async function primeApp(app: BrennApp, ws: MockWebSocket): Promise<void> {
  ws.deliver({
    type: "Welcome",
    username: "alice",
    user_id: 0,
    multiuser: false,
    singleton: true,
    available_models: [],
    default_model: "sonnet",
    attachment_targets: [],
    pwa_push_enabled: false,
  });
  ws.deliver({ type: "SetLayout", layout: { type: "SinglePane" } });
  await app.updateComplete;
  await app.updateComplete;
  ws.deliver({
    type: "ConversationSwitched",
    conversation_id: 1,
    state: "Idle",
    is_owner: true,
    shared: false,
    reload: false,
  });
  await app.updateComplete;
}

/** Deliver an AssistantMessage at the given seq to advance lastSeq. */
async function advanceLastSeq(
  app: BrennApp,
  ws: MockWebSocket,
  seq: number,
): Promise<void> {
  ws.deliver({ type: "AssistantMessage", content: "<p>_</p>", seq });
  await app.updateComplete;
}

describe("seq bound check — AC1: out-of-bound rejection", () => {
  it("accepts frame at exactly prevLastSeq + THRESHOLD (boundary pin: <= not <)", async () => {
    const { app, ws } = await mountApp();
    await primeApp(app, ws);
    const internals = app as unknown as AppInternals;

    await advanceLastSeq(app, ws, 10);
    expect(internals.lastSeq).toBe(10);

    // Exactly at threshold — must be accepted.
    const atThresholdSeq = 10 + SEQ_JUMP_THRESHOLD;
    ws.deliver({
      type: "AssistantMessage",
      content: "<p>at-threshold</p>",
      seq: atThresholdSeq,
    });
    await app.updateComplete;

    expect(internals.lastSeq).toBe(atThresholdSeq);
  });

  it("drops frame and does not advance lastSeq when seq > prevLastSeq + THRESHOLD", async () => {
    const { app, ws } = await mountApp();
    await primeApp(app, ws);
    const internals = app as unknown as AppInternals;

    // Advance lastSeq to 10.
    await advanceLastSeq(app, ws, 10);
    expect(internals.lastSeq).toBe(10);

    const setLastSeqSpy = vi.spyOn(internals.ws, "setLastSeq");
    const appendSystemMessageSpy = vi.spyOn(
      internals.messageList,
      "appendSystemMessage",
    );
    const consoleErrorSpy = vi
      .spyOn(console, "error")
      .mockImplementation(() => {});

    const outOfBoundSeq = 10 + SEQ_JUMP_THRESHOLD + 1;
    ws.deliver({
      type: "SystemMessageBroadcast",
      rendered_html: "<p>system</p>",
      category: "EventDrain",
      timestamp: new Date().toISOString(),
      seq: outOfBoundSeq,
    });
    await app.updateComplete;

    // lastSeq must not advance.
    expect(internals.lastSeq).toBe(10);
    // setLastSeq must not be called for the rejected frame.
    expect(setLastSeqSpy).not.toHaveBeenCalled();
    // Render must not fire.
    expect(appendSystemMessageSpy).toHaveBeenCalledTimes(0);
    // console.error must be called exactly once.
    expect(consoleErrorSpy).toHaveBeenCalledTimes(1);
    // The error message must contain all four required fields.
    // Join all arguments so a structured-args format change (e.g.
    // console.error(msg, {seq, prev, threshold})) is still detected.
    const callArgs = consoleErrorSpy.mock.calls[0]!;
    expect(callArgs.length).toBeGreaterThanOrEqual(1);
    const errMsg = callArgs.map(String).join(" ");
    expect(errMsg).toContain(String(outOfBoundSeq));
    expect(errMsg).toContain("prevLastSeq=10");
    expect(errMsg).toContain(String(SEQ_JUMP_THRESHOLD));
    expect(errMsg).toContain("SystemMessageBroadcast");
  });
});

describe("seq bound check — AC2: tab not silenced after rejection", () => {
  it("accepts and renders a normal +1 frame immediately after a rejected frame", async () => {
    const { app, ws } = await mountApp();
    await primeApp(app, ws);
    const internals = app as unknown as AppInternals;

    await advanceLastSeq(app, ws, 10);

    // Deliver out-of-bound frame (rejected).
    vi.spyOn(console, "error").mockImplementation(() => {});
    ws.deliver({
      type: "SystemMessageBroadcast",
      rendered_html: "<p>bad</p>",
      category: "EventDrain",
      timestamp: new Date().toISOString(),
      seq: 10 + SEQ_JUMP_THRESHOLD + 1,
    });
    await app.updateComplete;
    expect(internals.lastSeq).toBe(10);

    // Now deliver a normal in-bound frame.
    const spy = vi.spyOn(internals.messageList, "appendSystemMessage");
    ws.deliver({
      type: "SystemMessageBroadcast",
      rendered_html: "<p>ok</p>",
      category: "EventDrain",
      timestamp: new Date().toISOString(),
      seq: 11,
    });
    await app.updateComplete;

    expect(internals.lastSeq).toBe(11);
    expect(spy).toHaveBeenCalledTimes(1);
  });
});

describe("seq bound check — AC3: normal +1 sequence", () => {
  it("advances lastSeq and renders each consecutive +1 message", async () => {
    const { app, ws } = await mountApp();
    await primeApp(app, ws);
    const internals = app as unknown as AppInternals;

    const spy = vi.spyOn(internals.messageList, "appendAssistantMessage");

    for (let seq = 1; seq <= 5; seq++) {
      ws.deliver({ type: "AssistantMessage", content: `<p>${seq}</p>`, seq });
      await app.updateComplete;
      expect(internals.lastSeq).toBe(seq);
    }

    expect(spy).toHaveBeenCalledTimes(5);
  });
});

describe("seq bound check — AC4: per-message bound, not cumulative", () => {
  it("accepts THRESHOLD consecutive +1 frames from a baseline", async () => {
    const { app, ws } = await mountApp();
    await primeApp(app, ws);
    const internals = app as unknown as AppInternals;

    // Start from baseline seq 1000.
    await advanceLastSeq(app, ws, 1000);

    const spy = vi.spyOn(internals.messageList, "appendAssistantMessage");

    for (let i = 1; i <= SEQ_JUMP_THRESHOLD; i++) {
      ws.deliver({
        type: "AssistantMessage",
        content: `<p>${i}</p>`,
        seq: 1000 + i,
      });
      await app.updateComplete;
    }

    expect(internals.lastSeq).toBe(1000 + SEQ_JUMP_THRESHOLD);
    expect(spy).toHaveBeenCalledTimes(SEQ_JUMP_THRESHOLD);
  });

  it("accepts a single frame jumping the full THRESHOLD from baseline", async () => {
    // Verifies the bound is per-message (one big step is fine up to THRESHOLD),
    // not cumulative (only small steps allowed). This directly validates the
    // per-message claim — consecutive +1 steps alone cannot distinguish a
    // threshold of 1 from a threshold of 100.
    const { app, ws } = await mountApp();
    await primeApp(app, ws);
    const internals = app as unknown as AppInternals;

    await advanceLastSeq(app, ws, 500);
    expect(internals.lastSeq).toBe(500);

    const spy = vi.spyOn(internals.messageList, "appendAssistantMessage");

    ws.deliver({
      type: "AssistantMessage",
      content: "<p>big jump</p>",
      seq: 500 + SEQ_JUMP_THRESHOLD,
    });
    await app.updateComplete;

    expect(internals.lastSeq).toBe(500 + SEQ_JUMP_THRESHOLD);
    expect(spy).toHaveBeenCalledTimes(1);
  });
});

describe("seq bound check — AC5: dedup overlap unchanged", () => {
  it("seq equal to prevLastSeq does not advance lastSeq (Math.max path)", async () => {
    const { app, ws } = await mountApp();
    await primeApp(app, ws);
    const internals = app as unknown as AppInternals;

    await advanceLastSeq(app, ws, 7);

    const spy = vi.spyOn(internals.messageList, "appendUserMessage");

    // seq = prevLastSeq — Math.max(7,7)=7; shouldDrop fires downstream.
    ws.deliver({
      type: "UserMessageEcho",
      text: "hi",
      username: "alice",
      timestamp: new Date().toISOString(),
      seq: 7,
    });
    await app.updateComplete;

    expect(internals.lastSeq).toBe(7);
    expect(spy).toHaveBeenCalledTimes(0);

    // seq < prevLastSeq — Math.max(7,6)=7; shouldDrop fires.
    ws.deliver({
      type: "UserMessageEcho",
      text: "hi2",
      username: "alice",
      timestamp: new Date().toISOString(),
      seq: 6,
    });
    await app.updateComplete;

    expect(internals.lastSeq).toBe(7);
    expect(spy).toHaveBeenCalledTimes(0);
  });
});

describe("seq bound check — AC6: null baseline accepts any non-negative magnitude", () => {
  it("accepts a very large seq as the first baseline and evaluates the next frame against it", async () => {
    const { app, ws } = await mountApp();
    await primeApp(app, ws);
    const internals = app as unknown as AppInternals;

    // lastSeq must be null after primeApp (no seq-bearing frames yet).
    expect(internals.lastSeq).toBe(null);

    const spy = vi.spyOn(internals.messageList, "appendAssistantMessage");

    // Deliver a very large seq — must be accepted as the baseline.
    const bigSeq = 5_000_000_000;
    ws.deliver({ type: "AssistantMessage", content: "<p>big</p>", seq: bigSeq });
    await app.updateComplete;

    expect(internals.lastSeq).toBe(bigSeq);
    expect(spy).toHaveBeenCalledTimes(1);

    // Follow-up +1 frame — evaluated against the new baseline; must advance.
    ws.deliver({
      type: "AssistantMessage",
      content: "<p>next</p>",
      seq: bigSeq + 1,
    });
    await app.updateComplete;

    expect(internals.lastSeq).toBe(bigSeq + 1);
    expect(spy).toHaveBeenCalledTimes(2);
  });
});

describe("seq bound check — negative seq rejection (design §2.2 step 2)", () => {
  it("rejects negative seq from null baseline: no advance, no render, console.error", async () => {
    const { app, ws } = await mountApp();
    await primeApp(app, ws);
    const internals = app as unknown as AppInternals;

    expect(internals.lastSeq).toBe(null);

    const consoleErrorSpy = vi
      .spyOn(console, "error")
      .mockImplementation(() => {});
    const spy = vi.spyOn(internals.messageList, "appendAssistantMessage");

    ws.deliver({ type: "AssistantMessage", content: "<p>neg</p>", seq: -1 });
    await app.updateComplete;

    expect(internals.lastSeq).toBe(null);
    expect(spy).toHaveBeenCalledTimes(0);
    expect(consoleErrorSpy).toHaveBeenCalledTimes(1);
    // Verify diagnostic format: all four required fields present.
    const negCallArgs = consoleErrorSpy.mock.calls[0]!;
    expect(negCallArgs.length).toBeGreaterThanOrEqual(1);
    const negErrMsg = negCallArgs.map(String).join(" ");
    expect(negErrMsg).toContain("-1");
    expect(negErrMsg).toContain("prevLastSeq=null");
    expect(negErrMsg).toContain(String(SEQ_JUMP_THRESHOLD));
    expect(negErrMsg).toContain("AssistantMessage");
  });

  it("rejects negative seq from non-null baseline: no advance, no render, console.error", async () => {
    const { app, ws } = await mountApp();
    await primeApp(app, ws);
    const internals = app as unknown as AppInternals;

    await advanceLastSeq(app, ws, 5);
    expect(internals.lastSeq).toBe(5);

    const consoleErrorSpy = vi
      .spyOn(console, "error")
      .mockImplementation(() => {});
    const spy = vi.spyOn(internals.messageList, "appendAssistantMessage");

    ws.deliver({ type: "AssistantMessage", content: "<p>neg</p>", seq: -1 });
    await app.updateComplete;

    expect(internals.lastSeq).toBe(5);
    expect(spy).toHaveBeenCalledTimes(0);
    expect(consoleErrorSpy).toHaveBeenCalledTimes(1);
    // Verify diagnostic format: all four required fields present.
    const negCallArgs = consoleErrorSpy.mock.calls[0]!;
    expect(negCallArgs.length).toBeGreaterThanOrEqual(1);
    const negErrMsg = negCallArgs.map(String).join(" ");
    expect(negErrMsg).toContain("-1");
    expect(negErrMsg).toContain("prevLastSeq=5");
    expect(negErrMsg).toContain(String(SEQ_JUMP_THRESHOLD));
    expect(negErrMsg).toContain("AssistantMessage");
  });
});
