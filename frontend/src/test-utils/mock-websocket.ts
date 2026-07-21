/**
 * Shared MockWebSocket for test files that need to drive brenn-app over a
 * synthetic WebSocket connection.
 *
 * Usage:
 *   import { MockWebSocket } from "../test-utils/mock-websocket.js";
 *   // or, from within the components/ subtree:
 *   import { MockWebSocket } from "../../test-utils/mock-websocket.js";
 *
 * In beforeEach, reset instances and install:
 *   MockWebSocket.instances = [];
 *   (globalThis as unknown as { WebSocket: unknown }).WebSocket = MockWebSocket as unknown;
 * In afterEach, restore the real constructor.
 */

import type { WsServerMessage } from "../generated/WsServerMessage.js";

export class MockWebSocket {
  static readonly CONNECTING = 0 as const;
  static readonly OPEN = 1 as const;
  static readonly CLOSING = 2 as const;
  static readonly CLOSED = 3 as const;
  readyState: number = MockWebSocket.CONNECTING;
  url: string;
  onopen: ((ev: Event) => void) | null = null;
  onmessage: ((ev: MessageEvent) => void) | null = null;
  onclose: ((ev: CloseEvent) => void) | null = null;
  onerror: ((ev: Event) => void) | null = null;
  /** Messages passed to send() — inspect in tests to assert protocol behaviour. */
  sent: string[] = [];
  static instances: MockWebSocket[] = [];

  constructor(url: string) {
    this.url = url;
    MockWebSocket.instances.push(this);
    // Defer open so the caller can wire handlers first.
    queueMicrotask(() => {
      this.readyState = MockWebSocket.OPEN;
      this.onopen?.(new Event("open"));
    });
  }

  send(data: string): void {
    this.sent.push(data);
  }

  close(): void {
    this.readyState = MockWebSocket.CLOSED;
    this.onclose?.(new CloseEvent("close"));
  }

  /** Feed a WsServerMessage into the app as if the server sent it. */
  deliver(msg: WsServerMessage): void {
    this.onmessage?.(
      new MessageEvent("message", { data: JSON.stringify(msg) }),
    );
  }

  /** Fire a close event with a specific code/reason — the stale-client trigger. */
  closeWithCode(code: number, reason: string): void {
    this.readyState = MockWebSocket.CLOSED;
    // happy-dom's CloseEvent constructor honours `code` and `reason`
    // directly. Cast because the lib.dom typing is stricter than the real
    // interface.
    this.onclose?.(new CloseEvent("close", { code, reason } as EventInit));
  }
}
