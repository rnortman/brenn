// @vitest-environment happy-dom
import { describe, it, expect, afterEach, beforeEach } from "vitest";
import { LitElement, html } from "lit";
import { query } from "lit/decorators.js";
// Side-effect import registers <brenn-message-list>; the type-only
// import gives the tests a concrete type for the cast without dragging
// the value reference into bundler tree-shaking consideration.
// Matches the pattern in app.ts.
import "./message-list.js";
import "./app.js";
import type { BrennMessageList, MessageBatchItem } from "./message-list.js";
import { BrennApp } from "./app.js";
import type { WsServerMessage } from "../generated/WsServerMessage.js";
import { MockWebSocket } from "../test-utils/mock-websocket.js";

afterEach(() => {
  document.body.replaceChildren();
  document.head.querySelectorAll('meta[name="app-slug"], meta[name="initial-conversation-id"]').forEach((el) => el.remove());
});

describe("brenn-message-list clear safety", () => {
  it("clear() does not throw before first render", () => {
    const el = document.createElement(
      "brenn-message-list",
    ) as BrennMessageList;
    document.body.appendChild(el);
    // Pin the precondition: before updateComplete resolves, the
    // .message-scroll query target does not yet exist in the
    // element's shadow root. This is the exact state the
    // subtree-swap race puts a freshly-mounted message-list in;
    // without this assertion, the "no throw" test could become a
    // tautology if happy-dom's scheduling changed.
    expect(el.shadowRoot?.querySelector(".message-scroll")).toBeFalsy();
    expect(() => el.clear()).not.toThrow();
  });

  it("clear() still works after first render", async () => {
    const el = document.createElement(
      "brenn-message-list",
    ) as BrennMessageList;
    document.body.appendChild(el);
    await el.updateComplete;
    el.appendAssistantMessage("hello");
    // Sanity check: message was actually appended into the shadow
    // DOM's scroll container before we clear.
    const container = el.shadowRoot?.querySelector(".message-scroll");
    expect(container).toBeTruthy();
    expect(container!.querySelectorAll(".msg-assistant").length).toBe(1);
    expect(() => el.clear()).not.toThrow();
    // The `if (this.container)` null-safety guard must not regress
    // the clear-when-rendered path: when container exists, content
    // must actually be gone.
    expect(container!.children.length).toBe(0);
  });
});

class Parent extends LitElement {
  static properties = { variant: {} };
  variant: "a" | "b" = "a";
  @query("brenn-message-list") messageList!: BrennMessageList;

  override createRenderRoot() {
    return this;
  } // light DOM

  render() {
    return this.variant === "a"
      ? html`<div class="a"><brenn-message-list></brenn-message-list></div>`
      : html`<section class="b"><brenn-message-list></brenn-message-list></section>`;
  }
}
customElements.define("x-parent", Parent);

describe("layout transition ordering (mid-session resize path)", () => {
  // Post-rewrite, the subtree-swap-during-replay race is only reachable
  // through the mid-session viewport-change code path in _handleSetLayout
  // (user rotates phone / drags window across 768px with the WS already up).
  // The initial-connect path is now race-free because the server emits
  // SetLayout before any history frame — see the integration test below.
  // This test keeps the resize path honest: clear must be safe against a
  // freshly-mounted message-list whose shadow DOM hasn't rendered yet.
  it("swap + await + clear + append yields exactly one message", async () => {
    const p = document.createElement("x-parent") as Parent;
    document.body.appendChild(p);
    await p.updateComplete;
    const oldList = p.messageList;
    // Seed the old list with pre-swap content — mimics an in-flight
    // replay landing in the old layout's message-list right as the
    // mid-session resize swaps the subtree underneath it.
    await oldList.updateComplete;
    oldList.appendAssistantMessage("pre-swap-from-old");

    p.variant = "b";
    await p.updateComplete;

    // Old element is unmounted by the subtree swap — no longer in
    // the DOM tree, so anything targeting it by @query cannot
    // resolve to it.
    expect(oldList.isConnected).toBe(false);

    const newList = p.messageList;
    expect(newList).not.toBe(oldList);
    // Critical: clear must be safe even if newList's own shadow DOM
    // hasn't rendered yet after this single parent-updateComplete.
    expect(() => newList.clear()).not.toThrow();

    await newList.updateComplete;
    newList.appendAssistantMessage("post-swap-from-new");

    // Only one <brenn-message-list> in the DOM.
    expect(document.querySelectorAll("brenn-message-list").length).toBe(1);
    // The user-visible symptom of the bug was a duplicated
    // transcript, not merely two message-list elements. The
    // surviving list must contain exactly the post-swap content —
    // no leak-through from the pre-swap element.
    const messages = newList.shadowRoot!.querySelectorAll(".msg-assistant");
    expect(messages.length).toBe(1);
    expect(messages[0]!.textContent).toBe("post-swap-from-new");
  });
});

// ---------------------------------------------------------------------------
// Integration-style test for the initial-connect startup flow.
// ---------------------------------------------------------------------------
//
// Stubs `WebSocket` globally, mounts <brenn-app>, captures the server-side
// sink, and feeds the authoritative message sequence:
//
//   Welcome, SetLayout{SinglePane}, ConversationSwitched,
//   [3 history frames], HistoryComplete, ArtifactIndex
//
// Asserts: exactly one <brenn-message-list> exists, it contains exactly 3
// messages, and <brenn-file-viewer> is not visible. This pins the property
// that the render gate + replay queue deliver a single correct replay pass
// with no viewer overlay, which is the closure criterion for the
// mobile-firefox-reopen and tab-switch-back symptom tickets.

describe("initial-connect startup flow (integration)", () => {
  const realWebSocket = globalThis.WebSocket;

  beforeEach(() => {
    MockWebSocket.instances = [];
    // happy-dom ships a real WebSocket that would reach for the network;
    // swap it for the mock. Cast-through-unknown because the shapes match
    // for our use but MockWebSocket doesn't implement the full spec.
    (globalThis as unknown as { WebSocket: unknown }).WebSocket =
      MockWebSocket as unknown;

    // <brenn-app>'s constructor reads these meta tags.
    const slugMeta = document.createElement("meta");
    slugMeta.setAttribute("name", "app-slug");
    slugMeta.setAttribute("content", "test");
    document.head.appendChild(slugMeta);
  });

  afterEach(() => {
    (globalThis as unknown as { WebSocket: unknown }).WebSocket = realWebSocket;
  });

  it(
    "Welcome → SetLayout{SinglePane} → ConversationSwitched → 3 history frames → HistoryComplete → ArtifactIndex yields one message-list with 3 messages and no visible viewer",
    async () => {
      const app = document.createElement("brenn-app") as BrennApp;
      document.body.appendChild(app);
      await app.updateComplete;

      // Mock WS instance created by the app on connect.
      expect(MockWebSocket.instances.length).toBe(1);
      const ws = MockWebSocket.instances[0]!;
      expect(ws.url).toContain("viewport=");
      // The force-refresh-stale-browser-tabs handshake attaches the
      // build string on every connect. Vitest's `define` rewrites the
      // `globalThis.__BRENN_BUILD_ID__` read in `build-info.ts` to
      // `"test-build"` at test-bundle time, so this literal is the
      // exact value the connect URL carries.
      expect(ws.url).toContain("build=test-build");

      // Wait for the microtask-deferred `open` to fire and the app to
      // observe connected=true. No SetViewportClass goes on the wire:
      // viewport is now encoded in the connect URL.
      await Promise.resolve();
      await app.updateComplete;
      expect(
        ws.sent.filter((s) => s.includes('"SetViewportClass"')).length,
      ).toBe(0);

      // ---- Drive the authoritative server sequence. ----
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

      // Before SetLayout, renderable frames are queued — feed a probe
      // assistant message to pin the gate.
      ws.deliver({
        type: "AssistantMessage",
        content: "<p>pre-layout probe</p>",
        seq: 1,
      });

      ws.deliver({ type: "SetLayout", layout: { type: "SinglePane" } });

      // The SetLayout handler awaits this.updateComplete before flushing.
      // Let Lit commit and the flush run.
      await app.updateComplete;
      await app.updateComplete;

      ws.deliver({
        type: "ConversationSwitched",
        conversation_id: 42,
        state: "Idle",
        is_owner: true,
        shared: false,
        reload: false,
      });
      // _primeForReplay runs on ConversationSwitched. Let it settle; at
      // this point the probe from before SetLayout has already flushed.
      await app.updateComplete;

      // Three history frames — two assistant, one user echo.
      ws.deliver({
        type: "AssistantMessage",
        content: "<p>first</p>",
        seq: 2,
      });
      ws.deliver({
        type: "UserMessageEcho",
        text: "hello",
        username: "alice",
        timestamp: new Date().toISOString(),
        seq: 3,
      });
      ws.deliver({
        type: "AssistantMessage",
        content: "<p>third</p>",
        seq: 4,
      });
      ws.deliver({
        type: "HistoryComplete",
        oldest_loaded_seq: null,
      });
      ws.deliver({ type: "ArtifactIndex", files: [] });
      await app.updateComplete;

      // Invariants:
      // Exactly one <brenn-message-list> instance (no subtree-swap dup).
      const lists = document.querySelectorAll("brenn-message-list");
      expect(lists.length).toBe(1);
      const list = lists[0] as BrennMessageList;
      await list.updateComplete;

      // _primeForReplay(conversationId=42) clears the message-list when
      // ConversationSwitched arrives. The three history frames after
      // that yield exactly three messages. The pre-SetLayout probe was
      // flushed into the list before ConversationSwitched cleared it, so
      // it does NOT leak through.
      const container = list.shadowRoot!.querySelector(".message-scroll")!;
      // Assistant messages carry .msg-assistant; the user echo carries
      // .msg-user. Count their sum against the batch count.
      const assistantMsgs = container.querySelectorAll(".msg-assistant");
      const userMsgs = container.querySelectorAll(".msg-user");
      expect(assistantMsgs.length + userMsgs.length).toBe(3);
      expect(assistantMsgs.length).toBe(2);
      expect(userMsgs.length).toBe(1);

      // File viewer is mounted (SinglePane template includes it), but
      // on fresh replay there is no path for it to be shown: the slot
      // stays at { type: "Chat" }, which binds `.visible=false` on the
      // viewer. The `visible` property is reflected as an attribute
      // (see BrennFileViewer), which happy-dom honors.
      const viewer = document.querySelector("brenn-file-viewer");
      expect(viewer).toBeTruthy();
      // Reflected boolean: viewer.hasAttribute("visible") is true only
      // when .visible === true. Replay must not flip it on.
      expect(viewer!.hasAttribute("visible")).toBe(false);
    },
  );

  // ---------------------------------------------------------------------------
  // Cross-breakpoint resize: message-list element identity invariant.
  // ---------------------------------------------------------------------------
  //
  // Pins the load-bearing property of the unified `_renderPaneLayout`:
  // a SetLayout flip across the breakpoint must reuse the existing
  // <brenn-message-list> and must not send a synthetic Reconnect. If a
  // future regression splits the unified template back into two separate
  // `html\`...\`` literals, the element-identity assertion fails.
  async function runResizeKeepsMessageListTest(opts: {
    initial: "SinglePane" | "TwoColumn";
    target: "SinglePane" | "TwoColumn";
  }): Promise<void> {
    const app = document.createElement("brenn-app") as BrennApp;
    document.body.appendChild(app);
    await app.updateComplete;

    expect(MockWebSocket.instances.length).toBe(1);
    const ws = MockWebSocket.instances[0]!;
    await Promise.resolve();
    await app.updateComplete;

    // Initial connect sequence with 3 history frames.
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
    ws.deliver({ type: "SetLayout", layout: { type: opts.initial } });
    await app.updateComplete;
    await app.updateComplete;
    ws.deliver({
      type: "ConversationSwitched",
      conversation_id: 42,
      state: "Idle",
      is_owner: true,
      shared: false,
      reload: false,
    });
    await app.updateComplete;
    ws.deliver({ type: "AssistantMessage", content: "<p>one</p>", seq: 1 });
    ws.deliver({ type: "AssistantMessage", content: "<p>two</p>", seq: 2 });
    ws.deliver({ type: "AssistantMessage", content: "<p>three</p>", seq: 3 });
    ws.deliver({ type: "HistoryComplete", oldest_loaded_seq: null });
    ws.deliver({ type: "ArtifactIndex", files: [] });
    await app.updateComplete;

    const before = document.querySelector("brenn-message-list");
    expect(before).toBeTruthy();
    const beforeContainer = before!.shadowRoot!.querySelector(
      ".message-scroll",
    ) as HTMLElement;
    const beforeChildren = beforeContainer.children.length;
    expect(beforeChildren).toBe(3);

    const appI = app as unknown as {
      ccState: string;
      currentLayout: string;
      layoutReady: boolean;
    };
    const ccStateBefore = appI.ccState;

    // Only count post-swap traffic.
    ws.sent = [];

    ws.deliver({ type: "SetLayout", layout: { type: opts.target } });
    await app.updateComplete;
    await app.updateComplete;

    // Same <brenn-message-list> instance before and after — Lit reused
    // it across the swap, so no replay was needed.
    const after = document.querySelector("brenn-message-list");
    expect(after).toBe(before);
    const afterContainer = after!.shadowRoot!.querySelector(
      ".message-scroll",
    ) as HTMLElement;
    expect(afterContainer.children.length).toBe(beforeChildren);

    // No synthetic Reconnect was sent. Parse and discriminate on
    // message type rather than substring-matching the JSON wire form.
    const reconnects = ws.sent
      .map((s) => JSON.parse(s) as { type?: string })
      .filter((m) => m.type === "Reconnect");
    expect(reconnects.length).toBe(0);

    expect(appI.currentLayout).toBe(opts.target);
    expect(appI.layoutReady).toBe(true);
    // No spurious "Connecting" flicker.
    expect(appI.ccState).toBe(ccStateBefore);
  }

  it("narrow→wide resize keeps message-list element and sends no Reconnect", async () => {
    await runResizeKeepsMessageListTest({
      initial: "SinglePane",
      target: "TwoColumn",
    });
  });

  it("wide→narrow resize keeps message-list element and sends no Reconnect", async () => {
    await runResizeKeepsMessageListTest({
      initial: "TwoColumn",
      target: "SinglePane",
    });
  });
});

// ---------------------------------------------------------------------------
// bulkAppend single-DOM-attach invariant.
// ---------------------------------------------------------------------------
//
// The mobile-history-replay-too-expensive ticket closure depends on
// `bulkAppend` building a single DocumentFragment and attaching it to
// the container with a single `appendChild`. If a future regression
// reverted to per-message `container.appendChild`, the count test in
// the integration suite above would still pass — message count is
// identical either way. This test pins the *batching mechanism* itself
// by spying on the live container's `appendChild`.
describe("bulkAppend single-DOM-attach invariant", () => {
  it("bulkAppend with N items hits container.appendChild exactly once with a DocumentFragment", async () => {
    const list = document.createElement("brenn-message-list") as BrennMessageList;
    document.body.appendChild(list);
    await list.updateComplete;

    // The container is the @query'd `.message-scroll` descendant inside
    // the shadow root. Reach into it via the shadow DOM rather than the
    // private field.
    const container = list.shadowRoot!.querySelector(".message-scroll") as HTMLElement;
    expect(container).toBeTruthy();

    // Spy on container.appendChild from JS. Note: when a DocumentFragment
    // is attached, the runtime moves each child individually — happy-dom
    // (and real browsers per spec) may do this by re-entering appendChild,
    // so we'll see one DOCUMENT_FRAGMENT_NODE call followed by per-child
    // ELEMENT_NODE calls. The load-bearing invariant is the fragment call:
    // if a regression reverted to per-message attach (no fragment), the
    // fragment call disappears and we'd only see ELEMENT_NODE calls.
    const original = container.appendChild.bind(container);
    const calls: { nodeType: number; nodeName: string }[] = [];
    container.appendChild = ((node: Node) => {
      calls.push({ nodeType: node.nodeType, nodeName: node.nodeName });
      return original(node);
    }) as typeof container.appendChild;

    const items: MessageBatchItem[] = [
      { kind: "assistant", content: "<p>one</p>" },
      { kind: "assistant", content: "<p>two</p>" },
      {
        kind: "user",
        text: "three",
        username: "alice",
        timestamp: new Date().toISOString(),
        isSelf: true,
        attachments: [],
        selectedTasks: [],
      },
      { kind: "assistant", content: "<p>four</p>" },
      { kind: "error", message: "five" },
    ];
    list.bulkAppend(items);

    // Exactly one DocumentFragment-typed call: the bulkAppend single attach.
    const fragCalls = calls.filter((c) => c.nodeType === 11);
    expect(fragCalls.length).toBe(1);

    // Sanity: the messages did land in the container.
    expect(container.querySelectorAll(".msg").length).toBe(5);
  });

  it("bulkAppend with empty batch is a no-op (no DOM attach, no throw)", async () => {
    const list = document.createElement("brenn-message-list") as BrennMessageList;
    document.body.appendChild(list);
    await list.updateComplete;

    const container = list.shadowRoot!.querySelector(".message-scroll") as HTMLElement;
    const original = container.appendChild.bind(container);
    let calls = 0;
    container.appendChild = ((node: Node) => {
      calls++;
      return original(node);
    }) as typeof container.appendChild;

    list.bulkAppend([]);
    expect(calls).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// Render-gate frame classifier.
// ---------------------------------------------------------------------------
//
// `BrennApp.classifyFrame` is the single source of truth for the gate
// (which frames must wait for SetLayout) and the flush split (which go
// into the bulkAppend batch vs. re-dispatch through handleMessage). The
// integration test above exercises only AssistantMessage. Pin the rest
// of the table here so a regression that drops or mis-classifies a
// variant fails fast in CI.
describe("BrennApp.classifyFrame render-gate classifier", () => {
  // Helper to build a minimally-valid frame of the requested type.
  // Only `type` is read by the classifier, but the WsServerMessage union
  // requires the per-variant required fields so TS accepts the literal.
  function frame(type: string): WsServerMessage {
    // The classifier reads only msg.type. Use a permissive cast for
    // the test fixture rather than constructing every variant by hand.
    return { type } as unknown as WsServerMessage;
  }

  it("returns 'list' for message-list-bound variants", () => {
    for (const t of [
      "StreamToken",
      "ThinkingToken",
      "AssistantMessage",
      "UserMessageEcho",
      "ToolUseSummary",
      "TargetResult",
      "Error",
    ]) {
      expect(BrennApp.classifyFrame(frame(t))).toBe("list");
    }
  });

  it("returns 'side' for non-message-list render-touching variants", () => {
    for (const t of ["ArtifactContent", "AppBusy", "SessionStolen"]) {
      expect(BrennApp.classifyFrame(frame(t))).toBe("side");
    }
  });

  it("returns null for state-only / non-render frames", () => {
    // Representative state-only frames that handleMessage routes via
    // reactive @state updates (safe to run pre-layout).
    for (const t of [
      "Welcome",
      "ConversationSwitched",
      "ConversationList",
      "ArtifactIndex",
      "TodoState",
      "HistoryComplete",
      "HistoryPage",
      "PermissionRequest",
      "PermissionResolved",
      "PermissionCancelled",
      "Status",
    ]) {
      expect(BrennApp.classifyFrame(frame(t))).toBeNull();
    }
  });
});

// ---------------------------------------------------------------------------
// Tool-use grouping cross-batch invariant.
// ---------------------------------------------------------------------------
//
// `_appendToolUseItem` in `bulkAppend` must promote a single tool-use node
// into a group when a second tool-use follows it — covering both the
// in-fragment promotion (both items in the same batch) and the
// container→fragment cross-boundary promotion (first item already in the
// container, second arrives in a new batch).
describe("bulkAppend tool-use grouping", () => {
  it("promotes two consecutive tool-uses in the same batch to a group", async () => {
    const list = document.createElement("brenn-message-list") as BrennMessageList;
    document.body.appendChild(list);
    await list.updateComplete;

    list.bulkAppend([
      { kind: "toolUse", toolName: "Read", renderedSummary: "read file.txt", detailHtml: null },
      { kind: "toolUse", toolName: "Write", renderedSummary: "write result.txt", detailHtml: null },
    ]);

    const container = list.shadowRoot!.querySelector(".message-scroll")!;
    // Both tool-uses should be promoted into a single group node.
    const groups = container.querySelectorAll(".tool-use-group");
    const singles = container.querySelectorAll(".tool-use-single");
    expect(groups.length).toBe(1);
    expect(singles.length).toBe(0);
    // Group contains two items.
    expect(groups[0]!.querySelectorAll(".tool-use-item").length).toBe(2);
    expect(groups[0]!.querySelector("summary")?.textContent).toBe("2 tool uses");
  });

  it("promotes single→group when second tool-use arrives in a subsequent batch (container→fragment boundary)", async () => {
    const list = document.createElement("brenn-message-list") as BrennMessageList;
    document.body.appendChild(list);
    await list.updateComplete;

    // First batch: one tool-use lands in the container as a single.
    list.bulkAppend([
      { kind: "toolUse", toolName: "Read", renderedSummary: "read file.txt", detailHtml: null },
    ]);

    const container = list.shadowRoot!.querySelector(".message-scroll")!;
    expect(container.querySelectorAll(".tool-use-single").length).toBe(1);
    expect(container.querySelectorAll(".tool-use-group").length).toBe(0);

    // Second batch: a second tool-use follows immediately — the existing
    // container single must be promoted into a group (cross-boundary case).
    list.bulkAppend([
      { kind: "toolUse", toolName: "Write", renderedSummary: "write result.txt", detailHtml: null },
    ]);

    expect(container.querySelectorAll(".tool-use-single").length).toBe(0);
    expect(container.querySelectorAll(".tool-use-group").length).toBe(1);
    expect(container.querySelectorAll(".tool-use-item").length).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// bulkAppend StreamToken / ThinkingToken fallback path.
// ---------------------------------------------------------------------------
//
// Stream tokens in a replay batch are degenerate (CC sends AssistantMessage
// in history, not per-token streams).  `bulkAppend` falls back to the live
// streaming path: it sets `_suspendAutoScroll`, calls `appendStreamToken` /
// `appendThinkingToken` (which appends directly to `this.container`, NOT the
// fragment), then re-syncs `lastInFrag = this.container.lastElementChild`.
// This ensures a subsequent tool-use item in the same batch can still read
// the correct "previous sibling" for group-promotion decisions.
describe("bulkAppend streamToken / thinkingToken fallback", () => {
  it("streamToken in a batch appends text to the container via the live path", async () => {
    const list = document.createElement("brenn-message-list") as BrennMessageList;
    document.body.appendChild(list);
    await list.updateComplete;

    list.bulkAppend([{ kind: "streamToken", token: "hello world" }]);

    const container = list.shadowRoot!.querySelector(".message-scroll")!;
    // appendStreamToken opens a streaming element; text content must appear.
    expect(container.textContent).toContain("hello world");
  });

  it("thinkingToken in a batch appends text via the thinking-element path", async () => {
    const list = document.createElement("brenn-message-list") as BrennMessageList;
    document.body.appendChild(list);
    await list.updateComplete;

    list.bulkAppend([{ kind: "thinkingToken", token: "inner reasoning" }]);

    const container = list.shadowRoot!.querySelector(".message-scroll")!;
    // appendThinkingToken creates a .msg-thinking child inside the streaming element.
    expect(container.querySelector(".msg-thinking")).not.toBeNull();
    expect(container.querySelector(".msg-thinking")!.textContent).toContain("inner reasoning");
  });

  it("streamToken followed by assistant in the same batch both land in the container", async () => {
    const list = document.createElement("brenn-message-list") as BrennMessageList;
    document.body.appendChild(list);
    await list.updateComplete;

    list.bulkAppend([
      { kind: "streamToken", token: "streamed" },
      { kind: "assistant", content: "<p>full message</p>" },
    ]);

    const container = list.shadowRoot!.querySelector(".message-scroll")!;
    // appendStreamToken creates a streaming .msg-assistant directly in the container.
    // The subsequent "assistant" item builds a second .msg-assistant via the fragment.
    // Both must be present.
    expect(container.textContent).toContain("streamed");
    expect(container.querySelectorAll(".msg-assistant").length).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// Side-batch dispatch (ArtifactContent / AppBusy / SessionStolen).
// ---------------------------------------------------------------------------
//
// `classifyFrame` returns "side" for these variants. The integration test
// above drives a flow with zero ArtifactContent frames; this test seeds a
// replay that contains one, asserts it routes to the fileViewer rather than
// the message-list, and checks that `loadingHistory` suppresses auto-nav.
describe("side-batch dispatch during replay", () => {
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
  });

  it("ArtifactContent in a replay batch routes to fileViewer without causing viewer auto-nav", async () => {
    const app = document.createElement("brenn-app") as BrennApp;
    document.body.appendChild(app);
    await app.updateComplete;

    const ws = MockWebSocket.instances[0]!;
    await Promise.resolve();
    await app.updateComplete;

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

    // Feed a replay batch that includes both a list frame and a side frame.
    ws.deliver({ type: "AssistantMessage", content: "<p>msg</p>", seq: 1 });
    ws.deliver({
      type: "ArtifactContent",
      file_path: "/repo/file.txt",
      rendered_html: "<pre>hello</pre>",
      raw_content: "hello",
      snapshot: null,
    });
    ws.deliver({ type: "HistoryComplete", oldest_loaded_seq: null });
    ws.deliver({ type: "ArtifactIndex", files: [] });
    await app.updateComplete;

    // The message-list must have the assistant message.
    const list = document.querySelector("brenn-message-list") as BrennMessageList;
    await list.updateComplete;
    const container = list.shadowRoot!.querySelector(".message-scroll")!;
    expect(container.querySelectorAll(".msg-assistant").length).toBe(1);

    // The file viewer must NOT be auto-navigated during loadingHistory
    // (the "no auto-nav during replay" invariant).
    const viewer = document.querySelector("brenn-file-viewer");
    expect(viewer).toBeTruthy();
    expect(viewer!.hasAttribute("visible")).toBe(false);
  });

  /** Access internal state fields for assertions without rendering. */
  interface AppInternals {
    showStealButton: boolean;
    currentConversationId: number | null;
  }

  it("AppBusy mid-replay shows the steal button via _handleMessage routing", async () => {
    const app = document.createElement("brenn-app") as BrennApp;
    document.body.appendChild(app);
    await app.updateComplete;

    const ws = MockWebSocket.instances[0]!;
    await Promise.resolve();
    await app.updateComplete;

    // Bootstrap the session.
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

    // Deliver AppBusy — should set showStealButton=true.
    ws.deliver({ type: "AppBusy", message: "Another session is active" });
    await app.updateComplete;

    const appI = app as unknown as AppInternals;
    expect(appI.showStealButton).toBe(true);
  });

  it("SessionStolen mid-replay clears conversation state via _handleMessage routing", async () => {
    const app = document.createElement("brenn-app") as BrennApp;
    document.body.appendChild(app);
    await app.updateComplete;

    const ws = MockWebSocket.instances[0]!;
    await Promise.resolve();
    await app.updateComplete;

    // Bootstrap the session.
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

    // First deliver AppBusy to set showStealButton=true, then SessionStolen
    // to verify it resets to false (and clears conversation state).
    ws.deliver({ type: "AppBusy", message: "Another session active" });
    await app.updateComplete;
    const appI = app as unknown as AppInternals;
    expect(appI.showStealButton).toBe(true);

    // Deliver SessionStolen — clears showStealButton and currentConversationId.
    ws.deliver({ type: "SessionStolen", message: "Session was taken by another client" });
    await app.updateComplete;

    expect(appI.showStealButton).toBe(false);
    expect(appI.currentConversationId).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// URL viewport value round-trip.
// ---------------------------------------------------------------------------
//
// The connect URL always includes `viewport=<value>`. The existing integration
// test asserts `viewport=` is present but not the value. This test pins that
// the default viewport (no matchMedia stub → window.matchMedia returns false →
// getViewportClass() returns "Wide") produces `viewport=Wide` in the URL.
describe("WS connect URL viewport value", () => {
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
  });

  it("encodes 'Wide' in the connect URL when matchMedia returns false (default in happy-dom)", async () => {
    const app = document.createElement("brenn-app") as BrennApp;
    document.body.appendChild(app);
    await app.updateComplete;

    const ws = MockWebSocket.instances[0]!;
    // happy-dom's matchMedia always returns false (no real viewport).
    // getViewportClass() → "Wide" → URL must contain "viewport=Wide".
    expect(ws.url).toContain("viewport=Wide");
  });
});
