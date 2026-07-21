// @vitest-environment happy-dom
//
// Pins the SystemMessageBroadcast dedup rule introduced in Part B.3.
//
// The bug: a live broadcast and a history-replayed row carrying the same DB seq
// would both render, producing a double system card. The fix: the
// SystemMessageBroadcast handler drops any broadcast whose seq is ≤ prevLastSeq
// (the lastSeq value captured *before* this message's seq update), where
// prevLastSeq reflects what history replay already consumed.
//
// Three scenarios:
//   1. Live broadcast with seq > prevLastSeq — must render (no false drop).
//   2. Same broadcast delivered again after lastSeq already includes that seq
//      (history-replay path covered it) — must be dropped (no double-render).
//   3. Live broadcast with seq = null — must render regardless of lastSeq
//      (null seq means "no dedup" — legacy path; must not be silently eaten).

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import "./message-list.js";
import "./app.js";
import type { BrennMessageList } from "./message-list.js";
import { BrennApp } from "./app.js";
import { MockWebSocket } from "../test-utils/mock-websocket.js";

/** Private field accessor for the message-list child and dedup state. */
interface AppInternals {
  messageList: BrennMessageList;
  lastSeq: number | null;
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
    .querySelectorAll('meta[name="app-slug"], meta[name="initial-conversation-id"]')
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
 * Drive the app to a state where layoutReady=true and lastSeq=0.
 * Delivers: Welcome → SetLayout → ConversationSwitched (no history).
 */
async function primeApp(
  app: BrennApp,
  ws: MockWebSocket,
): Promise<void> {
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

const SYSTEM_BROADCAST_SEQ_5 = {
  type: "SystemMessageBroadcast",
  rendered_html:
    '<details class="brenn-system brenn-system-event-drain"><summary>Events</summary></details>',
  category: "EventDrain",
  timestamp: new Date().toISOString(),
  seq: 5,
} as const;

describe("SystemMessageBroadcast dedup (B.3 — wake-spawn race fix)", () => {
  it("genuine new broadcast (seq > prevLastSeq) is rendered", async () => {
    const { app, ws } = await mountApp();
    await primeApp(app, ws);

    const internals = app as unknown as AppInternals;
    const spy = vi.spyOn(internals.messageList, "appendSystemMessage");

    // lastSeq starts at null (no history frames yet).
    ws.deliver(SYSTEM_BROADCAST_SEQ_5);
    await app.updateComplete;

    expect(spy).toHaveBeenCalledTimes(1);
  });

  it("duplicate broadcast after history covers same seq is dropped", async () => {
    const { app, ws } = await mountApp();
    await primeApp(app, ws);

    const internals = app as unknown as AppInternals;

    // Simulate history replay: deliver an AssistantMessage with seq=5 so
    // lastSeq advances to 5 (same as the upcoming live broadcast).
    ws.deliver({ type: "AssistantMessage", content: "<p>hi</p>", seq: 5 });
    await app.updateComplete;

    const spy = vi.spyOn(internals.messageList, "appendSystemMessage");

    // Now deliver the live broadcast with seq=5 — history already covered it.
    // prevLastSeq was 5 when this message arrives; must be dropped.
    ws.deliver(SYSTEM_BROADCAST_SEQ_5);
    await app.updateComplete;

    expect(spy).toHaveBeenCalledTimes(0);
  });

  it("wake-spawn race ordering: broadcast before history, then history replay, then buffered broadcast fires — single render", async () => {
    // Reproduces the actual production race:
    //   1. Live SystemMessageBroadcast arrives in WS buffer BEFORE history replay.
    //   2. History frames including the same seq arrive (lastSeq advances).
    //   3. The buffered broadcast fires again after history drain — must be dropped.
    //
    // The previous "duplicate broadcast" test verified the dedup result but not
    // this ordering: it advanced lastSeq via AssistantMessage first, then
    // delivered the broadcast. Here the broadcast arrives first (renders once),
    // then history covers the same seq, then the broadcast fires again (dropped).
    const { app, ws } = await mountApp();
    await primeApp(app, ws);

    const internals = app as unknown as AppInternals;
    const spy = vi.spyOn(internals.messageList, "appendSystemMessage");

    // Step 1: live broadcast arrives before history (lastSeq=null → 5). Renders.
    ws.deliver(SYSTEM_BROADCAST_SEQ_5);
    await app.updateComplete;

    expect(spy).toHaveBeenCalledTimes(1);

    // Step 2: history replay covers the same seq.
    ws.deliver({ type: "AssistantMessage", content: "<p>hi</p>", seq: 5 });
    await app.updateComplete;

    // After Step 2, lastSeq must be exactly 5 — this pins the mechanism:
    // the drop in Step 3 must be "prevLastSeq=5 >= seq=5", not some other predicate.
    expect((app as unknown as AppInternals).lastSeq).toBe(5);

    // Step 3: the buffered broadcast fires again after history drain.
    // prevLastSeq is now 5; seq=5 <= prevLastSeq=5 → must be dropped.
    ws.deliver(SYSTEM_BROADCAST_SEQ_5);
    await app.updateComplete;

    expect(spy).toHaveBeenCalledTimes(1);
  });

  it("broadcast with seq=null is never dropped regardless of lastSeq", async () => {
    const { app, ws } = await mountApp();
    await primeApp(app, ws);

    const internals = app as unknown as AppInternals;

    // Advance lastSeq to 10.
    ws.deliver({ type: "AssistantMessage", content: "<p>hi</p>", seq: 10 });
    await app.updateComplete;

    const spy = vi.spyOn(internals.messageList, "appendSystemMessage");

    // seq=null means no dedup: must render even though lastSeq=10.
    ws.deliver({ ...SYSTEM_BROADCAST_SEQ_5, seq: null });
    await app.updateComplete;

    expect(spy).toHaveBeenCalledTimes(1);
  });
});

describe("shouldDrop — new call sites (AssistantMessage, UserMessageEcho, ToolUseSummary, TargetResult)", () => {
  let app: BrennApp;
  let ws: MockWebSocket;
  let internals: AppInternals;

  beforeEach(async () => {
    ({ app, ws } = await mountApp());
    await primeApp(app, ws);
    internals = app as unknown as AppInternals;
  });

  /** Advance lastSeq by delivering an AssistantMessage at the given seq. */
  async function advanceLastSeq(seq: number): Promise<void> {
    ws.deliver({ type: "AssistantMessage", content: "<p>_</p>", seq });
    await app.updateComplete;
  }

  it("AssistantMessage with seq <= prevLastSeq is dropped", async () => {
    // Advance lastSeq to 5 via a prior AssistantMessage.
    await advanceLastSeq(5);

    const spy = vi.spyOn(internals.messageList, "appendAssistantMessage");

    // seq=5 again — prevLastSeq=5, seq<=prevLastSeq → must be dropped.
    ws.deliver({ type: "AssistantMessage", content: "<p>dup</p>", seq: 5 });
    await app.updateComplete;

    expect(spy).toHaveBeenCalledTimes(0);
  });

  it("AssistantMessage with seq > prevLastSeq is rendered", async () => {
    const spy = vi.spyOn(internals.messageList, "appendAssistantMessage");

    ws.deliver({ type: "AssistantMessage", content: "<p>new</p>", seq: 3 });
    await app.updateComplete;

    expect(spy).toHaveBeenCalledTimes(1);
  });

  it("UserMessageEcho with seq <= prevLastSeq is dropped", async () => {
    // Advance lastSeq to 7.
    await advanceLastSeq(7);

    const spy = vi.spyOn(internals.messageList, "appendUserMessage");

    ws.deliver({
      type: "UserMessageEcho",
      text: "hello",
      username: "alice",
      timestamp: new Date().toISOString(),
      seq: 7,
    });
    await app.updateComplete;

    expect(spy).toHaveBeenCalledTimes(0);
  });

  it("UserMessageEcho with seq > prevLastSeq is rendered", async () => {
    const spy = vi.spyOn(internals.messageList, "appendUserMessage");

    ws.deliver({
      type: "UserMessageEcho",
      text: "hello",
      username: "alice",
      timestamp: new Date().toISOString(),
      seq: 2,
    });
    await app.updateComplete;

    expect(spy).toHaveBeenCalledTimes(1);
  });

  it("ToolUseSummary with seq <= prevLastSeq is dropped", async () => {
    // Advance lastSeq to 4.
    await advanceLastSeq(4);

    const spy = vi.spyOn(internals.messageList, "appendToolUseSummary");

    ws.deliver({
      type: "ToolUseSummary",
      tool_name: "Bash",
      rendered_summary: "<p>ran bash</p>",
      seq: 4,
    });
    await app.updateComplete;

    expect(spy).toHaveBeenCalledTimes(0);
  });

  it("ToolUseSummary with seq > prevLastSeq is rendered", async () => {
    const spy = vi.spyOn(internals.messageList, "appendToolUseSummary");

    ws.deliver({
      type: "ToolUseSummary",
      tool_name: "Bash",
      rendered_summary: "<p>ran bash</p>",
      seq: 6,
    });
    await app.updateComplete;

    expect(spy).toHaveBeenCalledTimes(1);
  });

  it("TargetResult with seq <= prevLastSeq is dropped", async () => {
    // Advance lastSeq to 8.
    await advanceLastSeq(8);

    const spy = vi.spyOn(internals.messageList, "appendTargetResult");

    ws.deliver({
      type: "TargetResult",
      target: "import",
      success: true,
      summary: "done",
      files: [],
      seq: 8,
    });
    await app.updateComplete;

    expect(spy).toHaveBeenCalledTimes(0);
  });

  it("TargetResult with seq > prevLastSeq is rendered", async () => {
    const spy = vi.spyOn(internals.messageList, "appendTargetResult");

    ws.deliver({
      type: "TargetResult",
      target: "import",
      success: true,
      summary: "done",
      files: [],
      seq: 9,
    });
    await app.updateComplete;

    expect(spy).toHaveBeenCalledTimes(1);
  });
});
