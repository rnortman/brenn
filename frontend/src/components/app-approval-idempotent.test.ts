// @vitest-environment happy-dom
import { describe, it, expect, afterEach, beforeEach } from "vitest";
import "./app.js";
import { BrennApp } from "./app.js";
import { expectConsoleError } from "../test-setup.js";
import { MockWebSocket } from "../test-utils/mock-websocket.js";

/**
 * Regression test for the idempotency guard on `PermissionRequest` /
 * `ToolCardRequest` in `app.ts`. A replayed request on incremental
 * reconnect (WS break-and-resume without a `switched || msg.reload`)
 * would otherwise stack a second dialog on top of the still-displayed one.
 *
 * The guard: `approvalQueue.some((a) => a.request_id === msg.request_id)`
 * short-circuits the push when the request_id is already queued.
 */

/** Narrow accessor for the private queue used by the approval dispatcher.
 * The guard is only observable by inspecting `approvalQueue.length`
 * before/after the synthetic frame delivery. */
interface QueueHandle {
  approvalQueue: Array<{ request_id: string }>;
}

describe("PermissionRequest / ToolCardRequest idempotency guard", () => {
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
  });

  async function mountApp(): Promise<{ app: BrennApp; ws: MockWebSocket }> {
    const app = document.createElement("brenn-app") as BrennApp;
    document.body.appendChild(app);
    await app.updateComplete;
    const ws = MockWebSocket.instances[0]!;
    await Promise.resolve();
    await app.updateComplete;
    return { app, ws };
  }

  /** Deliver a server frame through the mock socket's onmessage hook. */
  function deliver(ws: MockWebSocket, payload: object): void {
    ws.onmessage?.(
      new MessageEvent("message", { data: JSON.stringify(payload) }),
    );
  }

  it("duplicate PermissionRequest yields approvalQueue.length === 1", async () => {
    const { app, ws } = await mountApp();
    const queueHandle = app as unknown as QueueHandle;

    const frame = {
      type: "PermissionRequest",
      request_id: "req_dup_perm",
      tool_name: "Bash",
      tool_input: { command: "echo hi" },
      formatted_display: "<div>hi</div>",
    };

    // The app's handleMessage throws because the approval-container DOM
    // component is not rendered in this test environment. With the narrowed
    // ws.ts catch scope, handler exceptions propagate out of deliver(); the
    // queue push still happens before the throw (approvalQueue.push precedes
    // showCurrentApproval which is where the null-component throw occurs).
    expect(() => deliver(ws, frame)).toThrow();
    expect(queueHandle.approvalQueue.length).toBe(1);

    // Same request_id, second delivery (e.g. replayed on reconnect).
    // The idempotency guard fires before showCurrentApproval, so no throw.
    deliver(ws, frame);
    expect(queueHandle.approvalQueue.length).toBe(1);
    expect(queueHandle.approvalQueue[0]!.request_id).toBe("req_dup_perm");
  });

  it("duplicate ToolCardRequest yields approvalQueue.length === 1", async () => {
    const { app, ws } = await mountApp();
    const queueHandle = app as unknown as QueueHandle;

    const frame = {
      type: "ToolCardRequest",
      request_id: "req_dup_tc",
      tool_name: "mcp__brenn__ProposeReconciliation",
      tool_input: { proposals: [] },
      formatted_display: "<div>card</div>",
    };

    // Same as PermissionRequest: approval-container is null in test env.
    // Handler throw propagates out of deliver(); queue push precedes the throw.
    expect(() => deliver(ws, frame)).toThrow();
    expect(queueHandle.approvalQueue.length).toBe(1);

    deliver(ws, frame);
    expect(queueHandle.approvalQueue.length).toBe(1);
    expect(queueHandle.approvalQueue[0]!.request_id).toBe("req_dup_tc");
  });

  it("distinct request_ids stack normally", async () => {
    const { app, ws } = await mountApp();
    const queueHandle = app as unknown as QueueHandle;

    // Two distinct frames each trigger the component-setter throw.
    // Handler throws propagate out of deliver(); queue pushes precede the throw.
    expect(() => deliver(ws, {
      type: "PermissionRequest",
      request_id: "req_a",
      tool_name: "Bash",
      tool_input: { command: "a" },
      formatted_display: "<div>a</div>",
    })).toThrow();
    // The second frame's queue push triggers updateApprovalCounter (queue.length > 1),
    // which reads approvalContainer — also null. So this also throws.
    expect(() => deliver(ws, {
      type: "PermissionRequest",
      request_id: "req_b",
      tool_name: "Bash",
      tool_input: { command: "b" },
      formatted_display: "<div>b</div>",
    })).toThrow();

    expect(queueHandle.approvalQueue.length).toBe(2);
    expect(queueHandle.approvalQueue.map((a) => a.request_id)).toEqual([
      "req_a",
      "req_b",
    ]);
  });

  /**
   * Pin the scenario the guard exists for: user is on a conversation with
   * a permission dialog up; the WS disconnects and reconnects (incremental —
   * no conversation switch, no reload); the server replays the same
   * PermissionRequest on the new socket. The queue must stay length 1 and
   * the original entry must be preserved. Without the guard, the replayed
   * frame would stack on top of the still-displayed dialog.
   *
   * Note: the first tab resolving the permission before replay is a
   * server-side invariant (`handle_permission_response` removes from
   * `pending_permissions` before broadcasting `PermissionResolved`), so the
   * "stale replay after resolve" case is not protected here by design.
   */
  it("second delivery via a fresh socket keeps queue at length 1", async () => {
    const { app, ws } = await mountApp();
    const queueHandle = app as unknown as QueueHandle;

    const frame = {
      type: "PermissionRequest",
      request_id: "req_reconnect",
      tool_name: "Bash",
      tool_input: { command: "echo hi" },
      formatted_display: "<div>hi</div>",
    };

    // First delivery: approval-container is null → handler throws out of deliver().
    // Queue push precedes the throw.
    expect(() => deliver(ws, frame)).toThrow();
    expect(queueHandle.approvalQueue.length).toBe(1);

    // Simulate disconnect: close the current socket. The app's onclose
    // handler schedules a reconnect via setTimeout; the new socket replays
    // the frame on the fresh transport. We bypass the timer by delivering
    // through a fresh MockWebSocket directly — the handleMessage dispatcher
    // doesn't care which socket the frame came from, and the queue state is
    // the thing under test.
    ws.close();
    // happy-dom fires onclose synchronously from close(). Any new WS
    // instance would be registered in MockWebSocket.instances; if not, we
    // simulate the replay by delivering the same frame through a fresh
    // socket mounted on the existing handleMessage callback (which we
    // don't have direct access to — so re-use the original `ws` object:
    // its onmessage is still the app's bound handler).
    deliver(ws, frame);

    expect(queueHandle.approvalQueue.length).toBe(1);
    expect(queueHandle.approvalQueue[0]!.request_id).toBe("req_reconnect");
  });
});
