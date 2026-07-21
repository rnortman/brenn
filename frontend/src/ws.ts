/**
 * Typed WebSocket client for Brenn.
 * Handles connection lifecycle and message parsing.
 */

import type { WsClientMessage } from "./generated/WsClientMessage";
import type { WsServerMessage } from "./generated/WsServerMessage";
import type { ViewportClass } from "./generated/ViewportClass";
import { BUILD_ID } from "./build-info.js";
import { reportClientError } from "./error-reporter.js";

export type MessageHandler = (msg: WsServerMessage) => void;
export type StatusHandler = (connected: boolean) => void;
export type ViewportProvider = () => ViewportClass;

/** Max reconnect delay in ms. */
const MAX_RECONNECT_DELAY = 60_000;
/** Initial reconnect delay in ms. */
const INITIAL_RECONNECT_DELAY = 3_000;

/**
 * WebSocket close code (RFC 6455 §7.4.2 private range 3000-3999) that
 * the server uses to tell us "your bundle predates the deployed
 * server; reload to pick up the new JS." Kept in sync with the Rust
 * `STALE_CLIENT_CLOSE_CODE` in `brenn/src/routes/ws.rs`.
 */
export const STALE_CLIENT_CLOSE_CODE = 3001;

/**
 * sessionStorage key used to guard against reload loops. Per-tab scope
 * survives `location.reload()` and clears on tab close, which is
 * exactly the right granularity for the 3-strike check. Exported so
 * tests import the key rather than copy-pasting the literal.
 */
export const STALE_RELOAD_COUNT_KEY = "brenn.stale-reload-count";

/**
 * How many force-reloads we tolerate in a single tab-session before
 * giving up and surfacing an in-transcript error. A single deploy race
 * produces exactly one reload; two reloads is consistent with two
 * rapid deploys; three strongly suggests the reloaded bundle is itself
 * stale (CDN not updated, cache poisoning, bug in the handshake).
 */
const MAX_STALE_RELOADS = 3;

/**
 * Read the current stale-reload count from sessionStorage. Anything
 * that fails to parse as a non-negative integer is treated as zero —
 * we'd rather reload once spuriously than get stuck in a state where a
 * corrupt counter prevents a legitimate auto-refresh.
 */
function readStaleReloadCount(): number {
    const raw = sessionStorage.getItem(STALE_RELOAD_COUNT_KEY);
    if (raw === null) {
        return 0;
    }
    const n = Number.parseInt(raw, 10);
    return Number.isFinite(n) && n >= 0 ? n : 0;
}

/**
 * Handle a server-issued stale-client close. Increments the per-tab
 * sessionStorage counter and triggers `location.reload()`; at the cap
 * surfaces an error via `onError` instead, because a third reload in a
 * single tab-session means the reloaded bundle is itself stale.
 *
 * Exported for the unit tests.
 */
export function handleStaleClient(
    reason: string,
    onError: (message: string) => void,
): void {
    const count = readStaleReloadCount();
    if (count >= MAX_STALE_RELOADS) {
        onError(
            "This tab is running an outdated version of Brenn and couldn't " +
                "auto-refresh. Please close and reopen the tab.",
        );
        console.error(
            `stale-client reload cap reached (${count} reloads); ` +
                `server build was ${reason}`,
        );
        return;
    }
    sessionStorage.setItem(STALE_RELOAD_COUNT_KEY, String(count + 1));
    console.info(
        `stale-client close from server (build=${reason}); ` +
            `reloading (attempt ${count + 1}/${MAX_STALE_RELOADS})`,
    );
    location.reload();
}

export class BrennWs {
  private ws: WebSocket | null = null;
  private onMessage: MessageHandler;
  private onStatus: StatusHandler;
  private reconnectTimer: number | null = null;
  private reconnectDelay = INITIAL_RECONNECT_DELAY;
  private appSlug: string;
  /** Conversation to request on first connect. Cleared after use. */
  private initialConversationId: number | null = null;
  /** Conversation to reconnect to (set after first successful connect). */
  private currentConversationId: number | null = null;
  /** Highest DB seq seen, for incremental reconnect. */
  private lastSeq: number | null = null;
  /** Supplies the current viewport class to embed in every connect URL so
   *  the server can emit the correct SetLayout before any history frames. */
  private viewportProvider: ViewportProvider;
  /** Ephemeral message handlers registered via addMessageHandler(). */
  private extraHandlers: Set<MessageHandler> = new Set();

  constructor(
    appSlug: string,
    onMessage: MessageHandler,
    onStatus: StatusHandler,
    viewportProvider: ViewportProvider,
  ) {
    if (!appSlug) {
      throw new Error("BrennWs: appSlug is required (missing <meta name=\"app-slug\"> tag?)");
    }
    this.appSlug = appSlug;
    this.onMessage = onMessage;
    this.onStatus = onStatus;
    this.viewportProvider = viewportProvider;
  }

  /** Set the initial conversation to request on first connect. */
  setInitialConversation(id: number | null): void {
    this.initialConversationId = id;
  }

  /** Track the current conversation so reconnects go to the right place. */
  setCurrentConversation(id: number | null): void {
    this.currentConversationId = id;
  }

  /** Track the highest seq seen for incremental reconnect. */
  setLastSeq(seq: number | null): void {
    this.lastSeq = seq;
  }

  connect(): void {
    if (this.ws) {
      return;
    }

    const protocol = location.protocol === "https:" ? "wss:" : "ws:";
    const params = new URLSearchParams();

    // On first connect, pass the initial conversation ID so the server
    // skips auto-selection and goes straight to the right conversation.
    // On reconnect, pass the current conversation ID.
    const convId = this.initialConversationId ?? this.currentConversationId;
    if (convId !== null) {
      params.set("conv", String(convId));
      // Only send lastSeq on reconnects (not initial connect).
      if (this.initialConversationId === null && this.lastSeq !== null) {
        params.set("seq", String(this.lastSeq));
      }
    }
    // Always include viewport — the server uses it to emit SetLayout BEFORE
    // any history frame, so the client can mount the correct DOM shape up-front.
    params.set("viewport", this.viewportProvider());
    // Always include the build identifier. Same-build handshake lets
    // the server-side `ws_handler` green-light this connect; a
    // mismatch (or missing param — never happens post-handshake
    // bundle) triggers a Close(3001) reload.
    params.set("build", BUILD_ID);
    // Clear initial — subsequent reconnects use currentConversationId.
    this.initialConversationId = null;

    const qs = params.toString();
    const url = `${protocol}//${location.host}/app/${this.appSlug}/ws?${qs}`;

    this.ws = new WebSocket(url);

    this.ws.onopen = () => {
      this.onStatus(true);
      this.reconnectDelay = INITIAL_RECONNECT_DELAY;
      if (this.reconnectTimer !== null) {
        clearTimeout(this.reconnectTimer);
        this.reconnectTimer = null;
      }
      // Successful handshake — clear the stale-reload counter so a
      // later transient mismatch (e.g. a second deploy landing in
      // this tab's lifetime) doesn't inherit prior reload attempts.
      sessionStorage.removeItem(STALE_RELOAD_COUNT_KEY);
    };

    this.ws.onmessage = (event: MessageEvent) => {
      // Separate parse from dispatch so that handler exceptions are not
      // misattributed as parse failures.
      let parsed: Record<string, unknown>;
      try {
        parsed = JSON.parse(event.data as string) as Record<string, unknown>;
      } catch (e) {
        // Parse failure is an invariant violation — our server sent bad data.
        // Surface visibly and report back to backend for logging/alerting.
        const errorMsg = `Failed to parse server message: ${e}`;
        reportClientError(errorMsg);
        this.onMessage({ type: "Error", message: errorMsg });
        return;
      }

      // Validate that the parsed object has a string `type` field.
      // The exhaustive switch in the app component catches unknown type values,
      // but this catches structurally invalid messages (missing type, non-object, etc.)
      if (typeof parsed !== "object" || parsed === null || typeof parsed.type !== "string") {
        const errorMsg = `Server message missing 'type' field: ${event.data as string}`;
        reportClientError(errorMsg);
        this.onMessage({ type: "Error", message: errorMsg });
        return;
      }

      const msg = parsed as unknown as WsServerMessage;
      // Dispatch to ephemeral handlers first (e.g. VAPID key response).
      for (const h of this.extraHandlers) {
        h(msg);
      }
      this.onMessage(msg);
    };

    this.ws.onclose = (event: CloseEvent) => {
      this.ws = null;
      this.onStatus(false);
      if (event.code === STALE_CLIENT_CLOSE_CODE) {
        // Stale bundle: reload or surface an error at the cap. Never
        // reconnect on this path — reloading replaces the page, and
        // at the cap we want the user to close the tab.
        handleStaleClient(event.reason, (message) => {
          this.onMessage({ type: "Error", message });
        });
        return;
      }
      this.scheduleReconnect();
    };

    this.ws.onerror = () => {
      // onclose will fire after this, so we just log.
      console.error("WebSocket error");
    };
  }

  /**
   * Register an ephemeral message handler (e.g. for awaiting a VAPID key
   * response in the push-subscription flow).  The handler is called for
   * every incoming message until removed.  It is the caller's responsibility
   * to call removeMessageHandler() once the awaited response is received.
   */
  addMessageHandler(handler: MessageHandler): void {
    this.extraHandlers.add(handler);
  }

  /** Remove a previously registered ephemeral handler. */
  removeMessageHandler(handler: MessageHandler): void {
    this.extraHandlers.delete(handler);
  }

  send(msg: WsClientMessage): void {
    if (!this.sendRaw(msg)) {
      // Surface the failure visibly — user needs to know their message was lost.
      this.onMessage({
        type: "Error",
        message: "Not connected — message not sent. Reconnecting...",
      });
    }
  }

  /**
   * Send a ClientError message directly via sendRaw (not through send(),
   * which would surface a user-visible toast on disconnect).
   * Best-effort: no-op if the socket is not OPEN.
   */
  public sendClientError(message: string): void {
    this.sendRaw({ type: "ClientError", message });
  }

  /**
   * Send a message if connected. Returns true if the message was accepted by
   * the socket (i.e. the socket was OPEN at send time), false if not connected.
   * Unlike `send()`, does NOT surface a user-visible "Not connected" toast on
   * failure — the caller is responsible for its own feedback.
   */
  trySend(msg: WsClientMessage): boolean {
    return this.sendRaw(msg);
  }

  /** Send a message if connected. Returns false if not connected. */
  private sendRaw(msg: WsClientMessage): boolean {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg));
      return true;
    }
    return false;
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer !== null) {
      return;
    }
    this.reconnectTimer = window.setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, this.reconnectDelay);
    // Exponential backoff with cap.
    this.reconnectDelay = Math.min(this.reconnectDelay * 2, MAX_RECONNECT_DELAY);
  }
}
