/**
 * <brenn-app> — Top-level application shell.
 *
 * Owns the WS connection, CC state machine, user settings, and routes
 * messages to child components. All state flows downward via method calls
 * and property updates.
 */

import { LitElement, html, nothing } from "lit";
import { customElement, state, query } from "lit/decorators.js";
import type { WsServerMessage } from "../generated/WsServerMessage";
import type { CcState } from "../generated/CcState";
import type { ConversationSummary } from "../generated/ConversationSummary";
import type { ArtifactFileInfo } from "../generated/ArtifactFileInfo.js";
import type { AttachmentMeta } from "../generated/AttachmentMeta.js";
import type { AttachmentRef } from "../generated/AttachmentRef.js";
import type { SnapshotMetadata } from "../generated/SnapshotMetadata.js";
import type { ViewportClass } from "../generated/ViewportClass.js";
import type { PaneLayout } from "../generated/PaneLayout.js";
import type { ModelInfo } from "../generated/ModelInfo.js";
import type { TargetInfo } from "../generated/TargetInfo.js";
import type { RuleScope } from "../generated/RuleScope.js";
import type { PushClickTraceEvent } from "../generated/PushClickTraceEvent.js";
import type { SwToAppMessage } from "../generated/SwToAppMessage.js";
import type { TodoItem } from "../generated/TodoItem.js";
import type { SelectedTask } from "../generated/SelectedTask.js";

/**
 * Frontend-local selection state: the wire `SelectedTask` is `{ref}` only,
 * but the chip bar needs `tldr` for display. We keep `tldr` in FE state,
 * captured at selection time from the TodoItem, and strip it when sending
 * (see `handleUserSubmit`). This decouples the UX-nicety display text from
 * the wire protocol, which stays minimal to avoid self-rejection bugs when
 * backend-emitted tldrs exceed validator caps.
 */
interface LocalSelectedTask {
  ref: string;
  tldr: string;
}
import type { WsClientMessage } from "../generated/WsClientMessage.js";
import type { DebugViewportSnapshotData } from "../generated/DebugViewportSnapshotData.js";
import type { RectData } from "../generated/RectData.js";
import type { VisualViewportData } from "../generated/VisualViewportData.js";
import { BUILD_ID } from "../build-info.js";
import { BrennWs } from "../ws.js";
import { readMaxImageLongEdge, MissingClientConfigError } from "../image-resize.js";
import { setReporterTarget, reportClientError } from "../error-reporter.js";
import { openShareDb } from "../share-db.js";
import type { ShareData } from "../share-db.js";
import { UserSettings } from "../settings.js";
import { addSignedInUserId, removeSignedInUserId } from "../push-db.js";
import { enablePush, disablePush } from "../push.js";
import {
  CHAT_AND_FILE,
  CHAT_ONLY,
  copyBundleSlots,
  slotNavigate,
  slotBack,
  slotReset,
  slotType,
} from "../panes.js";
import type { BrennMessageList, MessageBatchItem } from "./message-list.js";
import type { BrennInputBar } from "./input-bar.js";
import type { BrennApprovalContainer } from "./approval-container.js";
import type { BrennConversationList } from "./conversation-list.js";
import type { BrennFileViewer } from "./file-viewer.js";
import type { BrennFilePicker } from "./file-picker.js";
import type { TodoAnchor } from "../generated/TodoAnchor.js";
import {
  todoKey,
  groupTasksByDate,
  type ReorderTarget,
  type ScheduleTarget,
  type SlotState,
  type TaskGroup,
} from "./todo-list.js";
import { localTodayStr, shortDate, snoozeTargetDate } from "../date-util.js";

/** Detail type for brenn-tool-response events dispatched by embedded components. */
type ToolResponseDetail =
  | { deny: true; reason?: string }
  | { always_allow: true; patterns: string[]; scope: RuleScope }
  | Record<string, unknown>;

/** Shape of history.state entries managed by this component. */
interface HistoryState {
  conversationId: number | null;
  /** Non-null when a SinglePane pane overlay is active. */
  pane?: "FilePicker" | "FileViewer" | "TodoList";
}

// Import components to register them.
import "./message-list.js";
import "./approval-container.js";
import "./tool-approve.js";
import "./ask-user-question.js";
import "./pfin/propose.js";
import "./pfin/batch-table.js";
import "./pfin/batch-swipe.js";
import "./pfin/batch-assign-table.js";
import "./pfin/batch-assign-swipe.js";
import "./status-bar.js";
import type { PermissionModeState } from "./status-bar.js";
import "./input-bar.js";
import "./conversation-list.js";
import "./file-viewer.js";
import "./file-picker.js";
import "./todo-list.js";
import "./pane-layout.js";
import "./toast-host.js";
import type { BrennToastHost } from "./toast-host.js";
import { parseSameOriginUrl } from "../url-util.js";


/** A queued request waiting for user action. Both permission requests
 *  (synchronous CC blocking) and tool card requests (async, CC continued)
 *  go through the same visual approval queue. The `type` field discriminates. */
type QueuedApproval =
  | Extract<WsServerMessage, { type: "PermissionRequest" }>
  | Extract<WsServerMessage, { type: "ToolCardRequest" }>;

/** Single source of truth for settled-tile text per action. Both
 *  `_handleTodoActionResult` (real ack path via TodoDoneResult/TodoMutationResult) and `_handleTodoState`
 *  (implicit-settlement reconcile path) call through here so a copy
 *  change lands in one place — mirrors `SETTLED_TILE_LOOKS`'s
 *  glyph/class table in `todo-list.ts`. The `"reorder"` branch
 *  returns `""` because reorder never produces a settled tile
 *  (design §3.6) — callers must not invoke this for reorder. */
function settledTileText(
  action: "done" | "snooze" | "schedule",
  targetDate: string | undefined,
): string {
  switch (action) {
    case "done":
      return "Done.";
    case "snooze":
      return targetDate ? `Snoozed to ${shortDate(targetDate)}` : "Snoozed.";
    case "schedule":
      return targetDate
        ? `Scheduled for ${shortDate(targetDate)}`
        : "Scheduled.";
  }
}

/**
 * Max permitted jump from prior `lastSeq` to a new `msg.seq`. Defense-in-depth
 * against a poisoned seq silencing the dedup gate. Far above any legitimate +1
 * burst under current hosting (backend writes seqs +1 per append_message under a
 * per-conversation lock), far below Number.MAX_SAFE_INTEGER.
 */
export const SEQ_JUMP_THRESHOLD = 100;

@customElement("brenn-app")
export class BrennApp extends LitElement {
  // Light DOM — styled by app.css.
  createRenderRoot(): HTMLElement {
    return this;
  }

  @state() private ccState: CcState = "Idle";
  @state() private connected = false;
  @state() private contextUsage: {
    usagePct: number;
    currentTokens: number;
    reminderPct: number;
    redPct: number;
    reminderTokens: number | null;
    redTokens: number | null;
  } | null = null;
  @state() private permissionMode: PermissionModeState = { status: "unseen" };
  @state() private costUsage: {
    lastTurnUsd: number;
    sinceLastCompactionUsd: number;
    last24hUsd: number;
  } | null = null;
  @state() private approvalVisible = false;
  @state() private sidebarVisible = false;
  @state() private currentConversationId: number | null = null;
  @state() private conversations: ConversationSummary[] = [];
  @state() private loadingHistory = false;
  @state() private enterSends = true;
  @state() private showStealButton = false;

  /** Highest DB seq seen, for incremental reconnect. */
  private lastSeq: number | null = null;

  /** Current user's username (from Welcome message). */
  private currentUsername = "";
  /** Whether this app is multiuser (from Welcome message). */
  private isMultiuser = false;
  /** Whether this app is singleton (one conversation per user, no conversation list). */
  private isSingleton = false;
  /** Whether the current user owns the active conversation. */
  @state() private currentIsOwner = true;
  /** Whether the active conversation is shared. */
  @state() private currentConversationShared = false;
  /** Users currently present in the active conversation (excluding self). */
  @state() private presenceUsers: string[] = [];

  /** Available models from backend. Empty until first CC spawn. */
  @state() private availableModels: ModelInfo[] = [];
  /** The app's configured default model. */
  private defaultModel = "";
  /** App-defined attachment targets (e.g. "Import bank export"). */
  private attachmentTargets: TargetInfo[] = [];
  /** Numeric user id (from Welcome message). Used for push IDB set and PushSubscribe. */
  private currentUserId = 0;
  /** Whether the active app has pwa_push.enabled = true (from Welcome). */
  private pwaPushEnabled = false;
  /** Whether this (device, user) pair currently has an active push subscription (from PushEnabled). */
  @state() private pushSubscribed = false;
  /** Whether a push enable/disable operation is in progress. */
  @state() private pushPending = false;
  /** Timer for deferred IDB set cleanup on WS close (5s grace period per §2.6.3). */
  private pushCleanupTimer: ReturnType<typeof setTimeout> | null = null;
  /** Whether the user dropdown menu is open. */
  @state() private userMenuOpen = false;
  /** The effective model: user preference or app default. */
  @state() private currentModel = "";

  /** Pane slot states — immutably replaced on navigation so Lit detects changes. */
  @state() private slots = copyBundleSlots(CHAT_AND_FILE);
  /** Whether the secondary slot is visible (false when no artifacts). TwoColumn only. */
  @state() private secondarySlotVisible = false;
  /** Split ratio between panes, persisted to UserSettings. */
  @state() private paneSplitRatio: number = 0.5;
  /** Current pane layout, driven by backend SetLayout messages. */
  @state() private currentLayout: PaneLayout["type"] = "TwoColumn";
  /** False until the first SetLayout arrives from the backend. */
  @state() private layoutReady = false;
  /** Frames that mutate message-list / file-viewer DOM, buffered until the
   *  first SetLayout has been processed and `<brenn-message-list>` is
   *  mounted. Flushed once in `_handleSetLayout` via `bulkAppend`.
   *  Structurally empty on the happy path (server emits SetLayout first);
   *  defensive buffer for any timing glitch. */
  private _pendingReplay: WsServerMessage[] = [];

  /** Messages we've echoed locally but haven't seen the server echo for yet.
   *  Used to suppress duplicate UserMessageEcho display. Queue because the
   *  user can send multiple messages quickly (steering). */
  private pendingEchoes: string[] = [];

  /** Cached artifact index for the current conversation. Reactive because
   *  the Files button (SinglePane) and secondary slot (TwoColumn) depend on it. */
  @state() private artifactIndex: ArtifactFileInfo[] = [];

  /** Whether this app has a todo list (set on first TodoState). */
  @state() private hasTodoList = false;
  /** Current todo tasks from the last TodoState message. */
  @state() private todoTasks: TodoItem[] = [];
  /** Per-row slot state covering both in-flight (pending) and
   *  just-settled (confirmation tile) rows. See
   *  `docs/designs/todo-list-ui-state-preservation.md` for the design.
   *
   *  Lit reactivity pattern: mutate-in-place + `requestUpdate()` (the
   *  same pattern `todoPendingAction` used before this design).
   *  Identity-swap would allocate on every dispatch; the explicit
   *  requestUpdate keeps allocation off the hot path. The shared map
   *  reference is passed to `<brenn-todo-list>` via @property and
   *  re-read on each update. */
  @state() private todoSlotState: Map<string, SlotState> = new Map();
  /**
   * Frozen render snapshot of the todo list (see
   * `docs/designs/todo-tombstone-regression/design.md`). Non-null
   * whenever any slot is pending, settled, or dismissed — i.e.
   * whenever the user is mid-triage or the idle timer hasn't yet
   * dismissed the last tile. While non-null, the todo list renders
   * from this snapshot; incoming TodoState refreshes update
   * `todoTasks` (for dispatch resolution like `_findLiveTask`) but
   * do NOT change what the user sees until the idle timer fires
   * and `_thawFrozenList()` clears the snapshot.
   *
   * Captured at the moment the first slot enters the pending state
   * (see `_freezeListIfNeeded`). The snapshot pins both the
   * `TaskGroup[]` structure and the `todayStr` used, so a midnight
   * rollover mid-triage doesn't re-section anything either.
   */
  @state() private todoFrozenSnapshot: {
    groups: TaskGroup[];
    todayStr: string;
  } | null = null;
  /** Per-slot watchdog timers (design §3.4.4). Fire at 30s if a pending
   *  slot's TodoDoneResult/TodoMutationResult never arrives — drop the slot so
   *  the row becomes interactive again. Not `@state` — rendering doesn't read
   *  timer handles. */
  private todoSlotWatchdogs: Map<string, ReturnType<typeof setTimeout>> =
    new Map();
  /** Single list-level idle timer (design §3.5). Armed when at least
   *  one settled slot exists and no slot is pending. Fires after 5s,
   *  collapses all settled slots at once. Cancelled on any new
   *  mutation dispatch so "active triage" keeps the tiles alive. */
  private todoIdleTimer: ReturnType<typeof setTimeout> | null = null;
  /** Per-row error message (Phase 4 §7.2): key → "Error: see chat" body.
   *  Cleared after 6s via `todoErrorTimers` or on next mutation. */
  @state() private todoErrorKeys: Map<string, string> = new Map();
  /** Per-row debounce timers (Phase 4 §6). Keyed identically to the
   *  slot map. A timer present means the row is in its 400ms lockout. */
  private todoDebounceTimers: Map<string, ReturnType<typeof setTimeout>> =
    new Map();
  /** Auto-clear timers for the per-row error badge. */
  private todoErrorTimers: Map<string, ReturnType<typeof setTimeout>> =
    new Map();
  /** Pre-splice snapshots for reorder snap-back on error.
   *  For single-reorder: keyed by the task's own matchedKey.
   *  For multi-reorder: keyed by EACH task's key (same snapshot object stored
   *  under each), so per-key success/failure cleanup in _handleTodoActionResult
   *  correctly finds the snapshot regardless of which task's ack arrives first.
   *  Entries: { tasks, selectedTasks } captured before the optimistic splice.
   *  Consumed (and removed) in the !success branch of `_handleTodoActionResult`. */
  private todoReorderSnapshots: Map<
    string,
    { tasks: TodoItem[]; selectedTasks: Map<string, LocalSelectedTask> }
  > = new Map();
  /** Whether a TodoRefresh is in flight (click → TodoState inbound). */
  @state() private todoRefreshPending = false;
  private _todoRefreshTimeoutId: ReturnType<typeof setTimeout> | null = null;
  /** True while a user-clicked refresh is queued waiting on in-flight
   *  mutations to settle (design §3.5). Distinct from
   *  `todoRefreshPending` — that tracks the click → TodoState
   *  round-trip; this tracks the client-side wait before we even send
   *  the `TodoRefresh`. UI surfaces both as a single continuous busy
   *  state on the refresh button. */
  @state() private todoRefreshQueued = false;
  /** Currently selected tasks (key → LocalSelectedTask).
   *
   * Frontend-only shape (carries `tldr` for chip display). Stripped to
   * `SelectedTask` (`{ref}` only) when sending over the wire. */
  @state() private selectedTasks: Map<string, LocalSelectedTask> = new Map();

  /** Cursor for backward history pagination. Non-null when older history is available. */
  private oldestLoadedSeq: number | null = null;
  /** Debounce flag for backward pagination requests. */
  private loadingMoreHistory = false;
  /** Today's date string for the todo list grouping (YYYY-MM-DD). */
  @state() private todoTodayStr: string = localTodayStr();

  /** Max long-edge pixels for client-side image resize; read from server-injected meta tag.
   * Written once in the constructor; not reactive state. */
  private maxLongEdge = 2576;
  /** True when max-image-long-edge meta tag is absent or invalid at startup.
   * Written once in the constructor; not reactive state. */
  private imageAttachmentsDisabled = false;
  /** Deferred toast message from constructor-time config error, shown in firstUpdated. */
  private configErrorToast: string | null = null;

  private settings: UserSettings;

  private appSlug: string;
  private initialNavigation = true;
  private popstateHandler: ((e: PopStateEvent) => void) | null = null;
  private viewportMql: MediaQueryList | null = null;
  private viewportMqlHandler: ((e: MediaQueryListEvent) => void) | null = null;
  private pagehideHandler: (() => void) | null = null;
  private serviceWorkerMessageHandler: ((event: MessageEvent) => void) | null = null;
  private documentClickHandler: ((e: MouseEvent) => void) | null = null;

  @query("brenn-message-list") private messageList!: BrennMessageList;
  @query("brenn-input-bar") private inputBar!: BrennInputBar;
  @query("brenn-approval-container") private approvalContainer!: BrennApprovalContainer;
  @query("brenn-conversation-list")
  private conversationList!: BrennConversationList;
  @query("brenn-file-viewer") private fileViewer!: BrennFileViewer;
  @query("brenn-file-picker") private filePicker!: BrennFilePicker;
  @query("brenn-toast-host") private toastHost?: BrennToastHost;

  private ws: BrennWs;
  /** Queue of pending approval requests. Head (index 0) is currently displayed. */
  private approvalQueue: QueuedApproval[] = [];

  constructor() {
    super();
    this.settings = new UserSettings();
    this.enterSends = this.settings.enterSends;
    this.paneSplitRatio = this.settings.paneSplitRatio;
    // Model preference will be resolved once Welcome arrives with default_model.

    // Read the app slug from the server-injected meta tag.
    const slugMeta = document.querySelector('meta[name="app-slug"]');
    this.appSlug = slugMeta?.getAttribute("content") ?? "";
    if (!this.appSlug) {
      throw new Error("Missing <meta name=\"app-slug\"> — cannot determine app");
    }

    // Read the initial conversation ID from the URL (server-injected meta tag).
    // Passed to BrennWs so it's included as ?conv= in the WS connect URL.
    // The server uses this to skip auto-selection and go straight to the
    // requested conversation, avoiding a race with client-side SwitchConversation.
    const convMeta = document.querySelector('meta[name="initial-conversation-id"]');
    const initialConvStr = convMeta?.getAttribute("content") ?? "";
    const initialConversationId = initialConvStr ? parseInt(initialConvStr, 10) : null;

    this.ws = new BrennWs(
      this.appSlug,
      (msg) => this.handleMessage(msg),
      (connected) => this.handleConnectionStatus(connected),
      () => this.getViewportClass(),
    );
    this.ws.setInitialConversation(initialConversationId);
    setReporterTarget(this.ws);

    // Read the image resize cap from the server-injected meta tag.
    try {
      this.maxLongEdge = readMaxImageLongEdge();
    } catch (err) {
      if (err instanceof MissingClientConfigError) {
        this.imageAttachmentsDisabled = true;
        this.configErrorToast =
          "Image attachment disabled — server config error; reload page";
      } else {
        console.error(
          "BrennApp: unexpected error reading max-image-long-edge config",
          err,
        );
        throw err;
      }
    }
  }

  /** The slot index where file content lives — 0 on SinglePane, 1 on TwoColumn. */
  private get fileSlot(): number {
    return this.currentLayout === "SinglePane" ? 0 : 1;
  }

  /** Get the current viewport class based on screen width. */
  private getViewportClass(): ViewportClass {
    return window.matchMedia("(max-width: 768px)").matches ? "Compact" : "Wide";
  }

  protected firstUpdated(): void {
    // Check for pending shares after first render so @query elements are available.
    this.checkPendingShares();
    // Push deferred config-error toast (toastHost unavailable pre-render).
    if (this.configErrorToast) {
      // Use a long ttlMs (60 s) to make this effectively persistent for the
      // duration of the page session — the user needs to act (reload) to fix it.
      if (this.toastHost) {
        this.toastHost.push({ text: this.configErrorToast, ttlMs: 60_000 });
      } else {
        // toastHost should always be present — if it's missing, the template or
        // component wiring is broken. Log so the failure is diagnosable.
        console.error(
          "BrennApp: toastHost not available in firstUpdated; config error toast dropped:",
          this.configErrorToast,
        );
      }
      this.configErrorToast = null;
    }
  }

  connectedCallback(): void {
    super.connectedCallback();
    this.ws.connect();
    // Listen for backward history pagination requests from the message list.
    this.addEventListener("load-more", () => {
      if (this.loadingMoreHistory || this.oldestLoadedSeq === null) return;
      this.loadingMoreHistory = true;
      this.ws.send({
        type: "LoadMoreHistory",
        before_seq: this.oldestLoadedSeq,
      });
    });

    // Listen for artifact re-open requests from tool-use summaries in the message list.
    this.addEventListener("artifact-reopen", ((e: CustomEvent) => {
      const filePath = e.detail?.filePath as string | undefined;
      if (filePath) {
        this.handleArtifactReopen(filePath);
      }
    }) as EventListener);

    // Listen for brenn-tool-response events from embedded approval components.
    document.addEventListener("brenn-tool-response", ((e: CustomEvent<ToolResponseDetail>) => {
      const d = e.detail;
      if (d && "always_allow" in d && d.always_allow) {
        this.handleAlwaysAllow(
          (d as { patterns: string[]; scope: RuleScope }).patterns,
          (d as { patterns: string[]; scope: RuleScope }).scope,
        );
      } else if (d && "deny" in d && d.deny) {
        const reason = "reason" in d && typeof d.reason === "string" ? d.reason : undefined;
        this.handleApprovalDecision(false, undefined, reason);
      } else {
        this.handleApprovalDecision(true, d as Record<string, unknown>);
      }
    }) as EventListener);

    // On pagehide (tab close / navigation away), clean up signed_in_user_ids.
    // This is a best-effort cleanup; crash leaves a residue (accepted per §2.6.3).
    this.pagehideHandler = () => {
      if (this.currentUserId !== 0) {
        removeSignedInUserId(this.currentUserId).catch((err: unknown) =>
          reportClientError(`removeSignedInUserId failed: ${String(err)}`)
        );
      }
    };
    window.addEventListener("pagehide", this.pagehideHandler);

    // Listen for messages from the service worker.
    if ("serviceWorker" in navigator) {
      this.serviceWorkerMessageHandler = (event: MessageEvent) => {
        const raw: unknown = event.data;
        if (raw === null || typeof raw !== "object") return;
        const data = raw as SwToAppMessage;

        if (data.type === "PushSubscriptionChanged" && this.pwaPushEnabled) {
          void this._handleEnablePush();
          return;
        }

        if (data.type === "NavigateTo") {
          if (typeof data.url !== "string") {
            reportClientError(`NavigateTo: url is not a string (${typeof data.url})`);
            return;
          }
          this._handleNavigateTo(data.url);
          return;
        }

        if (data.type === "PushClickTrace") {
          if (data.event === null || typeof data.event !== "object") {
            reportClientError(`PushClickTrace: event is not an object (${String(data.event)})`);
            return;
          }
          if (data.user_id !== null && typeof data.user_id !== "number") {
            reportClientError(`PushClickTrace: user_id is not a number or null (${typeof data.user_id})`);
            return;
          }
          this._handlePushClickTrace(data.user_id, data.event);
          return;
        }
      };
      navigator.serviceWorker.addEventListener("message", this.serviceWorkerMessageHandler);
    }

    // Close the user dropdown when clicking outside it.
    this.documentClickHandler = (e: MouseEvent) => {
      if (this.userMenuOpen) {
        const wrapper = this.renderRoot.querySelector(".user-menu-wrapper");
        if (wrapper && !wrapper.contains(e.target as Node)) {
          this.userMenuOpen = false;
        }
      }
    };
    document.addEventListener("click", this.documentClickHandler);

    // Viewport class change listener (device rotation, window resize).
    this.viewportMql = window.matchMedia("(max-width: 768px)");
    this.viewportMqlHandler = () => {
      // Report new viewport class to backend. The backend will respond
      // with SetLayout if the layout should change.
      this.ws.send({
        type: "SetViewportClass",
        viewport_class: this.getViewportClass(),
      });
    };
    this.viewportMql.addEventListener("change", this.viewportMqlHandler);

    // Browser back/forward navigation.
    this.popstateHandler = (e: PopStateEvent) => {
      const state = e.state as HistoryState | null;
      const conversationId = state?.conversationId ?? null;
      const pane = state?.pane ?? null;

      if (conversationId !== null && conversationId !== this.currentConversationId) {
        this.ws.send({ type: "SwitchConversation", conversation_id: conversationId });
        return;
      }
      if (conversationId === null && this.currentConversationId !== null) {
        // NewConversation does NOT create a DB record — it just detaches from
        // the current conversation and puts the app in empty state. A conversation
        // is only created when the user sends a message.
        this.ws.send({ type: "NewConversation" });
        return;
      }

      // Same conversation — handle SinglePane pane navigation.
      if (this.currentLayout === "SinglePane") {
        this._handlePanePopstate(pane);
      }
    };
    window.addEventListener("popstate", this.popstateHandler);
  }

  disconnectedCallback(): void {
    super.disconnectedCallback();
    if (this.popstateHandler) {
      window.removeEventListener("popstate", this.popstateHandler);
      this.popstateHandler = null;
    }
    if (this.viewportMql && this.viewportMqlHandler) {
      this.viewportMql.removeEventListener("change", this.viewportMqlHandler);
      this.viewportMql = null;
      this.viewportMqlHandler = null;
    }
    if (this.pagehideHandler) {
      window.removeEventListener("pagehide", this.pagehideHandler);
      this.pagehideHandler = null;
    }
    if (this.serviceWorkerMessageHandler && "serviceWorker" in navigator) {
      navigator.serviceWorker.removeEventListener("message", this.serviceWorkerMessageHandler);
      this.serviceWorkerMessageHandler = null;
    }
    if (this.documentClickHandler) {
      document.removeEventListener("click", this.documentClickHandler);
      this.documentClickHandler = null;
    }
    this._clearTodoRefreshPending();
    // Cancel any pending push cleanup timer.
    if (this.pushCleanupTimer !== null) {
      clearTimeout(this.pushCleanupTimer);
      this.pushCleanupTimer = null;
    }
    // Cancel any in-flight per-row debounce / error-badge / watchdog /
    // idle timers so they can't fire against a detached host (tests
    // mount/unmount between cases; production hosts live for the page
    // lifetime but the explicit cleanup keeps the contract clean).
    for (const timer of this.todoDebounceTimers.values()) {
      clearTimeout(timer);
    }
    this.todoDebounceTimers.clear();
    for (const timer of this.todoErrorTimers.values()) {
      clearTimeout(timer);
    }
    this.todoErrorTimers.clear();
    for (const timer of this.todoSlotWatchdogs.values()) {
      clearTimeout(timer);
    }
    this.todoSlotWatchdogs.clear();
    this._cancelIdleTimer();
  }

  /** Clear the todo-refresh-pending indicator and any armed fallback timer.
   * Called from the TodoState handler (success path), the fallback timer
   * itself, and disconnect. Keeps the boolean and the timer id in sync. */
  private _clearTodoRefreshPending(): void {
    this.todoRefreshPending = false;
    if (this._todoRefreshTimeoutId != null) {
      clearTimeout(this._todoRefreshTimeoutId);
      this._todoRefreshTimeoutId = null;
    }
  }

  render() {
    const canType = this.connected && !this.loadingHistory;
    const isWorking =
      this.ccState === "Thinking" ||
      this.ccState === "AwaitingApproval" ||
      this.ccState === "Compacting";

    const inputPlaceholder = !this.connected
      ? "Reconnecting\u2026"
      : this.loadingHistory
        ? "Restoring history\u2026"
        : this.currentConversationId !== null
          ? "Continue conversation\u2026"
          : "Start a new conversation\u2026";

    return html`
      <div class="app-layout">
        ${this.isSingleton ? nothing : html`<brenn-conversation-list
          .conversations=${this.conversations}
          .currentConversationId=${this.currentConversationId}
          .visible=${this.sidebarVisible}
          .multiuser=${this.isMultiuser}
          .onSelect=${(id: number) => this.handleConversationSelect(id)}
          .onNew=${() => this.handleNewConversation()}
          .onClose=${() => {
            this.sidebarVisible = false;
          }}
        ></brenn-conversation-list>`}
        <main class="app-main">
          <div class="app-topbar">
            ${this.isSingleton ? nothing : html`<button
              class="sidebar-toggle"
              @click=${() => {
                this.sidebarVisible = !this.sidebarVisible;
              }}
              title="Toggle conversation list"
            >
              ☰
            </button>
            <button
              class="new-conv-btn"
              @click=${() => this.handleNewConversation()}
              title="New conversation"
            >
              +
            </button>`}
            ${this.currentLayout === "SinglePane" &&
            this.hasTodoList &&
            slotType(this.slots, 0) === "Chat"
              ? html`<button
                  class="tasks-toggle"
                  @click=${() => this._handleTodoToggle()}
                  title="Show tasks"
                >
                  Tasks
                </button>`
              : nothing}
            ${this.currentLayout === "SinglePane" &&
            this.artifactIndex.length > 0 &&
            slotType(this.slots, 0) === "Chat"
              ? html`<button
                  class="files-toggle"
                  @click=${() => this._handleFilesToggle()}
                  title="Show files"
                >
                  Files
                </button>`
              : nothing}
            ${this._renderPrivacyToggle()}
            ${this._renderUserMenu()}
          </div>
          ${this.presenceUsers.length > 0
            ? html`<div class="presence-bar">
                <span class="presence-dot"></span>
                ${this.presenceUsers.join(", ")}
              </div>`
            : null}
          ${this.layoutReady ? this._renderPaneLayout() : nothing}
          ${this.showStealButton
            ? html`<div class="steal-bar">
                <button
                  class="steal-btn"
                  @click=${() => this.handleStealApp()}
                >
                  Force Close Existing Session
                </button>
              </div>`
            : null}
          <brenn-status-bar
            .ccState=${this.ccState}
            .connected=${this.connected}
            .contextUsage=${this.contextUsage}
            .costUsage=${this.costUsage}
            .permissionMode=${this.permissionMode}
            @request-compaction=${() => this.sendCompactionRequest()}
          ></brenn-status-bar>
          ${this.selectedTasks.size > 0 ? this._renderChipBar() : nothing}
          <brenn-input-bar
            .enabled=${canType}
            .isWorking=${isWorking}
            .placeholder=${inputPlaceholder}
            .enterSends=${this.enterSends}
            .appSlug=${this.appSlug}
            .availableModels=${this.availableModels}
            .currentModel=${this.currentModel}
            .onSubmit=${(text: string, attachments: AttachmentRef[], meta: AttachmentMeta[]) => this.handleUserSubmit(text, attachments, meta)}
            .onStop=${() => this.handleStopRequest()}
            .onToggleEnterSends=${() => this.handleToggleEnterSends()}
            .onModelChange=${(model: string) => this.handleModelChange(model)}
            .singleton=${this.isSingleton}
            .onOpenConversations=${() => { this.sidebarVisible = !this.sidebarVisible; }}
            .onNewConversation=${() => this.handleNewConversation()}
            .onNavigateHome=${() => { window.location.href = "/"; }}
            .attachmentTargets=${this.attachmentTargets}
            .onTargetUploaded=${(target: string, uploadIds: string[]) => {
              // SYNC: wire shape pinned by Rust test run_target_deserializes_frontend_wire_shape
              // (brenn-lib/src/ws_types/tests/client.rs). Renaming a key requires a
              // matching update to WsClientMessage::RunTarget and that test.
              this.ws.send({ type: "RunTarget", target, upload_ids: uploadIds });
            }}
            .onError=${(message: string) => { this.messageList.appendError(message); }}
            .onResized=${(text: string) => { this.toastHost?.push({ text }); }}
            .maxLongEdge=${this.maxLongEdge}
            .imageAttachmentsDisabled=${this.imageAttachmentsDisabled}
          ></brenn-input-bar>
        </main>
        <brenn-toast-host></brenn-toast-host>
      </div>
    `;
  }

  /** Single source of truth for which frame types touch the message-list
   *  or file-viewer DOM and so must be gated until the first SetLayout.
   *  `"list"` = message-list-bound (batched into a single DocumentFragment
   *  via bulkAppend). `"side"` = file-viewer / status-bar / overlay
   *  (re-dispatched individually through handleMessage on flush). Anything
   *  else updates reactive `@state` and is safe to run pre-layout. */
  private static readonly RENDERABLE_FRAMES: Readonly<
    Record<string, "list" | "side">
  > = {
    StreamToken: "list",
    ThinkingToken: "list",
    AssistantMessage: "list",
    UserMessageEcho: "list",
    SystemMessageBroadcast: "list",
    ToolUseSummary: "list",
    TargetResult: "list",
    Error: "list",
    ArtifactContent: "side",
    AppBusy: "side",
    SessionStolen: "side",
  };

  /** Returns the render kind for `msg`, or null if it is a non-render
   *  (state-only) frame that may run pre-layout. Public for tests. */
  static classifyFrame(msg: WsServerMessage): "list" | "side" | null {
    return BrennApp.RENDERABLE_FRAMES[msg.type] ?? null;
  }

  /**
   * Dedup guard for live broadcasts that may overlap an incremental history
   * re-replay (wake-spawn race fix, BridgeSpawned handler). Returns true if
   * the message should be dropped because its seq is already covered by a
   * prior replay batch.
   *
   * `prevLastSeq` must be captured *before* updating `this.lastSeq` for the
   * current message, so a genuine new broadcast (seq > prevLastSeq) is not
   * incorrectly suppressed.
   */
  private static shouldDrop(
    seq: number | null | undefined,
    prevLastSeq: number | null,
    frameType?: string,
  ): boolean {
    // Log a warning if seq is absent on a frame type that the protocol
    // requires to carry a seq — dedup is silently disabled without it.
    // A backend regression emitting seq: null on AssistantMessage,
    // UserMessageEcho, SystemMessageBroadcast, ToolUseSummary, or
    // TargetResult would otherwise cause silent duplicate renders.
    if ((seq === null || seq === undefined) && frameType !== undefined) {
      console.warn(
        `[brenn] shouldDrop: seq is ${seq} for frame type ${frameType} — dedup disabled`,
      );
    }
    return typeof seq === "number" && prevLastSeq !== null && seq <= prevLastSeq;
  }

  /** Translate a list-bound `WsServerMessage` into a typed `MessageBatchItem`
   *  for `BrennMessageList.bulkAppend`. Mirrors the per-message translation
   *  in `handleMessage`. Returns null for streaming-token frames so the
   *  caller can re-dispatch them through the live path (the streaming
   *  element is stateful and must not be batched into a fragment). */
  private _toBatchItem(msg: WsServerMessage): MessageBatchItem | null {
    switch (msg.type) {
      case "StreamToken":
        return { kind: "streamToken", token: msg.token };
      case "ThinkingToken":
        return { kind: "thinkingToken", token: msg.token };
      case "AssistantMessage":
        return { kind: "assistant", content: msg.content };
      case "UserMessageEcho":
        return {
          kind: "user",
          text: msg.text,
          username: msg.username,
          timestamp: msg.timestamp,
          isSelf: msg.username === this.currentUsername,
          attachments: msg.attachments ?? [],
          selectedTasks: msg.selected_tasks ?? [],
        };
      case "SystemMessageBroadcast":
        return {
          kind: "system",
          renderedHtml: msg.rendered_html,
          category: msg.category,
        };
      case "ToolUseSummary":
        return {
          kind: "toolUse",
          toolName: msg.tool_name,
          renderedSummary: msg.rendered_summary,
          detailHtml: msg.detail_html ?? null,
        };
      case "TargetResult":
        return {
          kind: "targetResult",
          target: msg.target,
          success: msg.success,
          summary: msg.summary,
          files: msg.files,
          detail: msg.detail ?? null,
        };
      case "Error":
        return { kind: "error", message: msg.message };
      default:
        // Caller guarantees msg is list-classified by classifyFrame, so
        // any other type is a bug.
        throw new Error(`_toBatchItem: unsupported list-frame type ${msg.type}`);
    }
  }

  /** Flush queued replay frames into the now-mounted message-list and
   *  file-viewer. Called exactly once from `_handleSetLayout` on the
   *  initial transition, after `await this.updateComplete` has resolved
   *  so `@query` targets are live. */
  private _flushPendingReplay(): void {
    if (this._pendingReplay.length === 0) return;
    const batch = this._pendingReplay;
    this._pendingReplay = [];
    // Split: message-list frames batch into one DocumentFragment via
    // bulkAppend (one layout, one paint — addresses replay flicker).
    // Side-channel frames (ArtifactContent, SessionStolen, AppBusy)
    // dispatch individually because they touch other components.
    //
    // Seq cursor for dedup within the flush batch — mirrors the shouldDrop
    // guard on the live path. handleMessage already updated lastSeq for every
    // queued frame; track a per-flush cursor to catch duplicate seqs within
    // the batch itself, defending against future ordering changes that might
    // queue the same broadcast twice.
    // Applied to all seq-bearing list types (AssistantMessage, UserMessageEcho,
    // SystemMessageBroadcast, ToolUseSummary, TargetResult), not just
    // SystemMessageBroadcast — symmetric with the live-path shouldDrop guard.
    let flushLastSeq: number | null = null;
    const listBatch: MessageBatchItem[] = [];
    const sideBatch: WsServerMessage[] = [];
    for (const m of batch) {
      const kind = BrennApp.classifyFrame(m);
      if (kind === "list") {
        // Dedup all seq-bearing list frames within the queued batch using the
        // same rule as the live path: drop if seq <= already-seen cursor.
        const mSeq = "seq" in m && typeof m.seq === "number" ? m.seq : null;
        if (mSeq !== null && flushLastSeq !== null && mSeq <= flushLastSeq) {
          continue;
        }
        if (mSeq !== null) {
          flushLastSeq = Math.max(flushLastSeq ?? 0, mSeq);
        }
        listBatch.push(this._toBatchItem(m)!);
      } else if (kind === "side") {
        sideBatch.push(m);
      } else {
        // Defensive: the gate only queues classified frames, so anything
        // else here is a bug. Panic-equivalent for frontend.
        throw new Error(`_flushPendingReplay: unexpected queued frame type ${m.type}`);
      }
    }
    if (listBatch.length > 0) {
      this.messageList.bulkAppend(listBatch);
    }
    for (const m of sideBatch) {
      // Re-dispatch through handleMessage; layoutReady is now true so
      // these take the direct path. loadingHistory is still true during
      // replay, so ArtifactContent will not auto-navigate.
      // Pass skipBoundCheck=true: the frame's seq was valid relative to
      // lastSeq when it was buffered; by flush time lastSeq may reflect
      // list frames that advanced the cursor further, making the original
      // seq appear out-of-bound — a false positive from flush ordering.
      this.handleMessage(m, /* skipBoundCheck */ true);
    }
  }

  /** Reset client state that a full history replay is about to rebuild.
   * `conversationId === null` means empty-state (no replay) and suppresses
   * the loadingHistory flip. The server's `handle_switch_conversation` sends
   * TodoState after attaching, so the sidebar is authoritative immediately.
   * The only caller is the ConversationSwitched handler (a true conversation
   * switch), which clears todo state unconditionally. */
  private _primeForReplay(conversationId: number | null): void {
    this.messageList?.clear();
    this.lastSeq = null;
    this.ws.setLastSeq(null);
    this.pendingEchoes = [];
    this.approvalQueue = [];
    this.approvalVisible = false;
    this.presenceUsers = [];
    this.artifactIndex = [];
    this.oldestLoadedSeq = null;
    this.loadingMoreHistory = false;
    this.secondarySlotVisible = false;
    this.selectedTasks = new Map();
    // Drop any frames buffered behind the render gate from a prior
    // pre-layout window. Empty in the post-rewrite happy path (the
    // server emits SetLayout strictly before any renderable frame); kept
    // for parity with the other replay-mirroring queues above.
    this._pendingReplay = [];
    this.hasTodoList = false;
    this.todoTasks = [];
    // Design §3.4.3: drop all slot state + associated timers on a
    // true conversation switch.
    this.todoSlotState.clear();
    this.todoFrozenSnapshot = null;
    for (const timer of this.todoSlotWatchdogs.values()) {
      clearTimeout(timer);
    }
    this.todoSlotWatchdogs.clear();
    this.todoReorderSnapshots.clear();
    this._cancelIdleTimer();
    this.todoRefreshQueued = false;
    // Only flip loadingHistory true when there *is* a conversation
    // to load — otherwise the empty-state UI would be stuck on
    // "Restoring history…" forever.
    this.loadingHistory = conversationId !== null;
    this.fileViewer?.clear();
    this.inputBar?.clearAttachments();
  }

  private handleMessage(
    msg: WsServerMessage,
    // When true, skip the out-of-bound rejection check. Used only by
    // _flushPendingReplay when re-dispatching side-channel frames (e.g.
    // ArtifactContent) that were buffered pre-SetLayout. By flush time
    // lastSeq may reflect later list frames, making the buffered frame's
    // seq appear out-of-bound — a flush-ordering false positive, not an
    // actual poisoned-seq attack.
    skipBoundCheck = false,
  ): void {
    // Capture prevLastSeq before updating: SystemMessageBroadcast dedup
    // must compare against what was already seen, not the post-update value.
    const prevLastSeq = this.lastSeq;
    // Track the highest seq for incremental reconnect, with a plausibility
    // bound to prevent a poisoned large seq from silencing future renders.
    if ("seq" in msg && typeof msg.seq === "number") {
      const seq = msg.seq;
      if (seq < 0) {
        console.error(
          `[brenn] seq-monotonicity-frontend: rejected msg.seq=${seq}, prevLastSeq=${prevLastSeq}, threshold=${SEQ_JUMP_THRESHOLD}, type=${msg.type}`,
        );
        return;
      } else if (prevLastSeq === null) {
        // Null baseline: accept any non-negative seq as the starting cursor.
        this.lastSeq = seq;
        this.ws.setLastSeq(this.lastSeq);
      } else if (skipBoundCheck || seq <= prevLastSeq + SEQ_JUMP_THRESHOLD) {
        // In-bound (or flushed side-channel bypassing the bound check):
        // advance only when seq strictly exceeds prevLastSeq — overlap frames
        // leave lastSeq unchanged; shouldDrop handles them downstream.
        if (seq > prevLastSeq) {
          this.lastSeq = seq;
          this.ws.setLastSeq(this.lastSeq);
        }
      } else {
        // Out-of-bound jump: log and drop. Do not advance lastSeq, do not
        // render. Tab continues normally for subsequent in-bound messages.
        console.error(
          `[brenn] seq-monotonicity-frontend: rejected msg.seq=${seq}, prevLastSeq=${prevLastSeq}, threshold=${SEQ_JUMP_THRESHOLD}, type=${msg.type}`,
        );
        return;
      }
    }

    // Render gate: until the first SetLayout lands, <brenn-message-list>
    // and <brenn-file-viewer> are not mounted. Queue any frame that would
    // mutate their DOM; non-render frames (state updates, conversation
    // metadata) still run immediately so the rest of the app catches up.
    // The queue is flushed once in `_handleSetLayout`.
    //
    if (!this.layoutReady && BrennApp.classifyFrame(msg) !== null) {
      this._pendingReplay.push(msg);
      return;
    }

    switch (msg.type) {
      case "StreamToken":
        this.messageList.appendStreamToken(msg.token);
        break;
      case "ThinkingToken":
        this.messageList.appendThinkingToken(msg.token);
        break;
      case "AssistantMessage":
        if (BrennApp.shouldDrop(msg.seq, prevLastSeq, "AssistantMessage")) {
          break;
        }
        this.messageList.appendAssistantMessage(msg.content);
        break;
      case "PermissionRequest":
      case "ToolCardRequest": {
        // Idempotency: drop duplicates on replay. Required for incremental
        // reconnect, which does not reset `approvalQueue`.
        if (
          this.approvalQueue.some((a) => a.request_id === msg.request_id)
        ) {
          break;
        }
        this.approvalQueue.push(msg);
        // If this is the only entry, show it now. Otherwise the dialog is
        // already showing the head — just update the counter.
        if (this.approvalQueue.length === 1) {
          this.showCurrentApproval();
        } else {
          this.updateApprovalCounter();
        }
        break;
      }
      case "PermissionCancelled":
        this.removeFromApprovalQueue(msg.request_id);
        break;
      case "PermissionResolved":
        // Another tab resolved the permission, or AlwaysAllow succeeded.
        this.removeFromApprovalQueue(msg.request_id);
        break;
      case "ToolCardResolved":
        // Another tab resolved the tool card.
        this.removeFromApprovalQueue(msg.request_id);
        break;
      case "ApprovalRuleError":
        // Invalid pattern in AlwaysAllow — dispatch to embedded component.
        // Includes request_id so the component can ignore stale errors.
        document.dispatchEvent(
          new CustomEvent("brenn-rule-error", {
            detail: { request_id: msg.request_id, error: msg.error },
          }),
        );
        break;
      case "Status":
        this.ccState = msg.state;
        break;
      case "Error":
        this.messageList.appendError(msg.message);
        break;
      case "ConversationList":
        this.conversations = msg.conversations;
        break;
      case "ConversationSwitched": {
        const switched = msg.conversation_id !== this.currentConversationId;
        this.currentConversationId = msg.conversation_id;
        // Track in WS so reconnects go to the right conversation.
        this.ws.setCurrentConversation(msg.conversation_id);
        this.ccState = msg.state;
        this.currentIsOwner = msg.is_owner;
        this.currentConversationShared = msg.shared;
        this.showStealButton = false;
        if (switched || msg.reload) {
          this._primeForReplay(msg.conversation_id);
          // Reset pane slots to bundle defaults — ArtifactIndex will
          // populate the secondary slot if there are artifacts.
          this.slots = copyBundleSlots(
            this.currentLayout === "SinglePane" ? CHAT_ONLY : CHAT_AND_FILE,
          );
        }

        this.updateUrl(msg.conversation_id);
        this.focusInput();
        break;
      }
      case "HistoryComplete":
        this.loadingHistory = false;
        this.oldestLoadedSeq = msg.oldest_loaded_seq ?? null;
        if (this.oldestLoadedSeq !== null) {
          this.messageList.showLoadMoreSentinel();
        }
        this.messageList.scrollToBottomNow();
        this.focusInput();
        break;
      case "HistoryPage":
        // Guard: ignore stale responses after conversation switch.
        if (this.oldestLoadedSeq === null) break;
        this.messageList.prependMessages(msg.messages);
        if (msg.has_more && msg.messages.length > 0) {
          this.oldestLoadedSeq = msg.messages[0].seq;
          this.messageList.showLoadMoreSentinel();
        } else {
          this.oldestLoadedSeq = null;
          this.messageList.hideLoadMoreSentinel();
        }
        this.loadingMoreHistory = false;
        break;
      case "UserMessageEcho": {
        if (BrennApp.shouldDrop(msg.seq, prevLastSeq, "UserMessageEcho")) {
          break;
        }
        // Suppress echoes of messages we sent locally (single-tab case).
        // In multi-tab, this shows messages from other tabs.
        // During history replay (loadingHistory), always show — the local
        // echo was cleared on conversation switch.
        const echoIdx = this.pendingEchoes.indexOf(msg.text);
        if (this.loadingHistory || echoIdx === -1) {
          this.messageList.appendUserMessage({
            text: msg.text,
            username: msg.username,
            timestamp: msg.timestamp,
            isSelf: msg.username === this.currentUsername,
            attachments: msg.attachments ?? [],
            selectedTasks: msg.selected_tasks ?? [],
          });
        } else {
          this.pendingEchoes.splice(echoIdx, 1);
        }
        break;
      }
      case "SystemMessageBroadcast": {
        // System-origin messages never go through pendingEchoes — they are
        // not typed by a human and cannot collide with local tab sends.
        //
        // Dedup: if this broadcast carries a DB seq that is already covered by
        // history replay (or an earlier live forward), drop it. This prevents
        // double-rendering when a live broadcast and a history-replay row arrive
        // for the same DB row (B.3 — wake-spawn race fix).
        if (BrennApp.shouldDrop(msg.seq, prevLastSeq, "SystemMessageBroadcast")) {
          break;
        }
        this.messageList.appendSystemMessage({
          renderedHtml: msg.rendered_html,
          category: msg.category,
        });
        break;
      }
      case "ArtifactContent":
        // During history replay, don't auto-navigate to the viewer — the
        // ArtifactIndex (which follows HistoryComplete) will set the pane
        // to picker mode, giving the user an overview of all files.
        // Live artifacts (CC just displayed a file) do switch to viewer.
        this.showArtifactContent(
          msg.file_path,
          msg.rendered_html,
          msg.raw_content,
          msg.snapshot ?? null,
          !this.loadingHistory,
        );
        break;
      case "ArtifactIndex":
        this.artifactIndex = msg.files;
        if (this.currentLayout === "TwoColumn") {
          // If we have artifacts and the secondary slot is hidden, show it
          // with the picker. (This happens after history replay.)
          // If the viewer is showing, don't switch to picker — the user is
          // actively viewing something.
          if (msg.files.length > 0 && !this.secondarySlotVisible) {
            this.secondarySlotVisible = true;
            this.slots = slotReset(this.slots, 1, {
              current: { type: "FilePicker" },
              parent: null,
            });
          } else if (msg.files.length === 0 && !this.hasTodoList) {
            this.secondarySlotVisible = false;
            this.fileViewer?.clear();
          } else if (msg.files.length === 0 && this.hasTodoList) {
            // Artifacts cleared but todo list is available — fall back to it.
            this.fileViewer?.clear();
            if (slotType(this.slots, 1) === "FilePicker" || slotType(this.slots, 1) === "FileViewer") {
              this.slots = slotReset(this.slots, 1, {
                current: { type: "TodoList" },
                parent: null,
              });
            }
          }
        } else {
          // SinglePane: no visible change. Cache the index; the Files button
          // appears in the topbar because render() checks artifactIndex.length.
          // If artifacts are cleared, reset to chat if we were in files.
          if (msg.files.length === 0 && slotType(this.slots, 0) !== "Chat") {
            this.slots = slotReset(this.slots, 0, {
              current: { type: "Chat" },
              parent: null,
            });
            this.fileViewer?.clear();
          }
        }
        // If viewer is showing, update the version list for the current file.
        if (
          slotType(this.slots, this.fileSlot) === "FileViewer" &&
          this.fileViewer?.filePath
        ) {
          this.updateViewerVersions(this.fileViewer.filePath);
        }
        break;
      case "ToolUseSummary":
        if (BrennApp.shouldDrop(msg.seq, prevLastSeq, "ToolUseSummary")) {
          break;
        }
        this.messageList.appendToolUseSummary(
          msg.tool_name,
          msg.rendered_summary,
          msg.detail_html ?? null,
        );
        break;
      case "SessionStolen":
        this.messageList.appendError(`Session stolen: ${msg.message}`);
        this.currentConversationId = null;
        this.ccState = "Idle";
        this.showStealButton = false;
        break;
      case "AppBusy":
        this.messageList.appendNotice(msg.message);
        this.showStealButton = true;
        break;
      case "Welcome":
        this.currentUsername = msg.username;
        this.isMultiuser = msg.multiuser;
        this.isSingleton = msg.singleton;
        this.defaultModel = msg.default_model;
        if (msg.available_models.length > 0) {
          this.availableModels = msg.available_models;
        }
        this.attachmentTargets = msg.attachment_targets ?? [];
        this.resolveCurrentModel();
        this.currentUserId = msg.user_id;
        this.pwaPushEnabled = msg.pwa_push_enabled;
        // Maintain the signed_in_user_ids IDB set (§2.6.3).
        // Cancel any pending cleanup timer from a prior WS close.
        if (this.pushCleanupTimer !== null) {
          clearTimeout(this.pushCleanupTimer);
          this.pushCleanupTimer = null;
        }
        addSignedInUserId(msg.user_id).catch((err: unknown) =>
          reportClientError(`addSignedInUserId failed: ${String(err)}`)
        );
        break;
      case "TargetResult":
        if (BrennApp.shouldDrop(msg.seq, prevLastSeq, "TargetResult")) {
          break;
        }
        this.messageList.appendTargetResult(
          msg.target,
          msg.success,
          msg.summary,
          msg.files,
          msg.detail ?? null,
        );
        break;
      case "ModelsAvailable":
        this.availableModels = msg.available_models;
        this.resolveCurrentModel();
        break;
      case "PresenceUpdate":
        // Filter out self, store other usernames.
        this.presenceUsers = msg.users
          .map((u) => u.username)
          .filter((name) => name !== this.currentUsername);
        break;
      case "SetLayout":
        void this._handleSetLayout(msg.layout);
        break;
      case "PrivacyChanged":
        if (msg.conversation_id === this.currentConversationId) {
          this.currentConversationShared = msg.shared;
          if (!msg.shared && !this.currentIsOwner) {
            // Non-owner kicked from now-private conversation.
            this.handleNewConversation();
          }
        }
        break;
      case "ContextUsage":
        // Only update (and trigger re-render) if usage actually changed.
        // We track usagePct AND currentTokens because absolute-token
        // thresholds can flip the pill colour without a percentage change.
        if (
          !this.contextUsage ||
          this.contextUsage.usagePct !== msg.usage_pct ||
          this.contextUsage.currentTokens !== msg.current_tokens
        ) {
          this.contextUsage = {
            usagePct: msg.usage_pct,
            currentTokens: msg.current_tokens,
            reminderPct: msg.reminder_pct,
            redPct: msg.red_pct,
            reminderTokens: msg.reminder_tokens,
            redTokens: msg.red_tokens,
          };
        }
        break;
      case "PermissionMode":
        // msg.mode is `"auto" | "other" | null` per the generated schema;
        // null means CC omitted the field. The type is a closed string union
        // matching the ts-rs override on WsServerMessage.PermissionMode.mode.
        this.permissionMode =
          msg.mode === null
            ? { status: "missing" }
            : { status: "seen", mode: msg.mode };
        break;
      case "TodoState":
        this._handleTodoState(msg.tasks, msg.today);
        break;
      case "TodoDoneResult":
        this._handleTodoActionResult(
          msg.path,
          msg.success,
          msg.error ?? null,
          msg.next_check_in_date ?? null,
          msg.next_due_date ?? null,
          msg.terminal ?? null,
          msg.repo ?? null,
        );
        break;
      case "TodoMutationResult":
        this._handleTodoActionResult(
          msg.path,
          msg.success,
          msg.error ?? null,
          null, // nextCheckInDate — not on this variant
          null, // nextDueDate — not on this variant
          null, // terminal — not on this variant
          msg.repo ?? null,
        );
        break;
      case "CostUsage":
        this.costUsage = {
          lastTurnUsd: msg.last_turn_usd,
          sinceLastCompactionUsd: msg.since_last_compaction_usd,
          last24hUsd: msg.last_24h_usd,
        };
        break;
      case "PushVapidKey":
        // Handled by the ephemeral handler in fetchVapidKey() (push.ts).
        break;
      case "PushEnabled":
        this.pushSubscribed = msg.enabled;
        break;
      default: {
        const _exhaustive: never = msg;
        this.messageList.appendError(
          `Unknown message type from server: ${(msg as { type: string }).type}`,
        );
      }
    }
  }

  /** Show artifact content in the file viewer.
   *  @param navigate - if true, switch to viewer mode (live artifacts).
   *    If false, just update the viewer content without changing pane mode
   *    (history replay — the pane stays hidden until ArtifactIndex arrives). */
  private showArtifactContent(
    filePath: string,
    renderedHtml: string,
    rawContent: string,
    snapshot: SnapshotMetadata | null,
    navigate: boolean,
  ): void {
    const versions = this.getVersionsForFile(filePath);
    this.fileViewer.show(filePath, renderedHtml, rawContent, snapshot, versions);
    if (navigate) {
      if (this.currentLayout === "TwoColumn") {
        // Desktop: always show viewer in secondary slot (non-disruptive).
        this.secondarySlotVisible = true;
        this.slots = slotReset(this.slots, 1, {
          current: { type: "FileViewer" },
          parent: { type: "FilePicker" },
        });
      } else {
        // SinglePane: navigate to viewer for live artifacts (CC just called
        // DisplayFile) and user-initiated file opens. History replay is
        // already excluded — navigate is false during loadingHistory.
        this.slots = slotNavigate(this.slots, 0, { type: "FileViewer" });
        this._pushPaneState("FileViewer");
      }
    }
  }

  /** Look up version list for a file from the cached artifact index. */
  private getVersionsForFile(filePath: string): ArtifactFileInfo["versions"] | null {
    const file = this.artifactIndex.find((f) => f.file_path === filePath);
    return file?.versions ?? null;
  }

  /** Update the viewer's version list when the index changes. */
  private updateViewerVersions(filePath: string): void {
    const versions = this.getVersionsForFile(filePath);
    if (this.fileViewer) {
      this.fileViewer.versions = versions;
    }
  }

  /** Handle artifact reopen from inline chat links. */
  private handleArtifactReopen(filePath: string): void {
    // Try to resolve from cached index first (load from DB).
    const file = this.artifactIndex.find((f) => f.file_path === filePath);
    if (file && file.versions.length > 0) {
      const latest = file.versions[file.versions.length - 1];
      this.ws.send({
        type: "LoadArtifactSnapshot",
        message_id: latest.message_id,
      });
    } else {
      // Fall back to disk read.
      this.ws.send({
        type: "ReopenArtifact",
        file_path: filePath,
        message_id: null,
      });
    }
  }

  /** User clicked a file in the picker. */
  private handleFilePickerSelect(filePath: string, latestMessageId: number): void {
    this.ws.send({
      type: "LoadArtifactSnapshot",
      message_id: latestMessageId,
    });
  }

  /** User clicked the back arrow in the viewer. */
  private handleFileViewerBack(): void {
    if (this.currentLayout === "SinglePane") {
      // Pop the history entry we pushed when opening the viewer.
      // The popstate handler sets the slot state and clears the viewer.
      history.back();
    } else {
      this.slots = slotBack(this.slots, this.fileSlot);
      this.fileViewer.clear();
    }
  }

  /** User selected a different version in the viewer dropdown. */
  private handleVersionSelect(messageId: number): void {
    this.ws.send({
      type: "LoadArtifactSnapshot",
      message_id: messageId,
    });
  }

  /** User closed the file pane (picker close button). */
  private handleRightPaneCollapse(): void {
    if (this.currentLayout === "TwoColumn") {
      this.secondarySlotVisible = false;
      this.fileViewer.clear();
    } else {
      // SinglePane: pop the history entry we pushed when opening the picker.
      // The popstate handler sets the slot state and clears the viewer.
      history.back();
    }
  }

  private handleConnectionStatus(connected: boolean): void {
    this.connected = connected;
    if (!connected && this.currentUserId !== 0) {
      // WS closed — schedule deferred IDB set cleanup (5s grace period per §2.6.3).
      // The grace period lets an in-flight push arrive before the set shrinks,
      // preventing a race between a push dispatched just before disconnect and the
      // SW's signed_in_user_ids check.  Cancel any prior pending timer first.
      if (this.pushCleanupTimer !== null) {
        clearTimeout(this.pushCleanupTimer);
      }
      const userId = this.currentUserId;
      this.pushCleanupTimer = setTimeout(() => {
        this.pushCleanupTimer = null;
        removeSignedInUserId(userId).catch((err: unknown) =>
          reportClientError(`removeSignedInUserId failed: ${String(err)}`)
        );
      }, 5_000);
    }
    if (connected) {
      // SetViewportClass is only for mid-session viewport changes — see
      // viewportMqlHandler. On open, the viewport is already in the WS
      // URL and the server emits SetLayout before any history frame.

      // Report browser timezone for message attribution.
      const tz = Intl.DateTimeFormat().resolvedOptions().timeZone;
      if (tz) {
        this.ws.send({ type: "SetTimezone", timezone: tz });
      }

      // Report device info (UA, platform, screen dimensions) for device identity.
      const platform: string =
        (navigator as { userAgentData?: { platform?: string } }).userAgentData?.platform ??
        navigator.platform ??
        "";
      this.ws.send({
        type: "SetDeviceInfo",
        user_agent: navigator.userAgent ?? "",
        platform,
        screen_width: screen.width ?? 0,
        screen_height: screen.height ?? 0,
      });

      // Conversation selection is handled server-side via the ?conv= query
      // parameter in the WS URL (set by BrennWs from initialConversationId
      // or currentConversationId). No need to send SwitchConversation here.
    }
  }

  /**
   * Update the browser URL to reflect the current conversation.
   * Uses replaceState when the URL isn't actually changing (reconnects,
   * initial load) and pushState for user-driven conversation switches.
   */
  private updateUrl(conversationId: number | null): void {
    const path = conversationId !== null
      ? `/app/${this.appSlug}/c/${conversationId}`
      : `/app/${this.appSlug}`;

    // replaceState when: initial navigation, or URL isn't changing.
    // pushState when: user-driven conversation switch (URL is changing).
    const currentPath = window.location.pathname;
    if (this.initialNavigation || currentPath === path) {
      this.initialNavigation = false;
      history.replaceState({ conversationId }, "", path);
    } else {
      history.pushState({ conversationId }, "", path);
    }
  }

  /**
   * Handle a `NavigateTo` message from the service worker (push notification
   * click). Validates same-origin, then either switches conversation in-app
   * (same app slug) or performs a full navigation (different app / outside
   * /app/...).  Cross-origin or malformed URLs are ignored with a console.warn.
   */
  _handleNavigateTo(url: string): void {
    const parsed = parseSameOriginUrl(url, window.location.origin);
    if (parsed === null) {
      console.warn("NavigateTo: malformed or cross-origin URL, ignoring", url);
      return;
    }

    // Pre-Welcome race: currentUserId === 0 means not-yet-welcomed. The WS
    // session is not yet established, so smooth in-app routing is impossible.
    // Fall back to a full navigation — same end-state as the SW opening a new
    // window on the old code, minus the duplicate tab. Do NOT call
    // reportClientError here; this is a normal timing edge case on slow
    // connections.
    if (this.currentUserId === 0) {
      window.location.assign(parsed.href);
      return;
    }

    // Match /app/{slug} or /app/{slug}/c/{id}
    const match = parsed.pathname.match(/^\/app\/([^/]+)(?:\/c\/(\d{1,15}))?\/?$/);
    if (match && match[1] === this.appSlug) {
      // Same app — switch conversation in-app via WS.
      if (match[2] !== undefined) {
        const conversationId = parseInt(match[2], 10);
        if (conversationId === this.currentConversationId) {
          // Already viewing this conversation — no-op (focus/foreground is
          // handled by the caller; emitting SwitchConversation would cause
          // the backend to reject it as a singleton-app error and surface a
          // spurious error toast on push-click against an open tab).
          return;
        }
        this.ws.send({ type: "SwitchConversation", conversation_id: conversationId });
      } else {
        // No /c/ in path — navigate to "no conversation" state.
        if (this.currentConversationId !== null) {
          this.ws.send({ type: "NewConversation" });
        }
        // else: already detached; focus alone is the user-visible action.
      }
    } else {
      // Defense-in-depth: only navigate to /app/* paths. The SW derives
      // targetPath from a backend-validated push payload (validate_app_path
      // restricts to /app/...), so this check is normally redundant. It
      // guards against a future code path that could post an unexpected path.
      if (!parsed.pathname.startsWith("/app/")) {
        console.warn("NavigateTo: target is not an /app/ path, ignoring", url);
        return;
      }
      // Different app — full navigation to the target app.
      window.location.assign(parsed.href);
    }
  }

  /**
   * Handle a `PushClickTrace` message from the service worker.
   *
   * Forwards the typed `PushClickTraceEvent` to the backend via WS as a
   * `PushClickTrace` message. The backend logs each event at INFO with
   * structured fields. Best-effort: if WS is not connected the event is lost.
   */
  _handlePushClickTrace(userId: number | null, event: PushClickTraceEvent): void {
    // Exhaustiveness guard: the switch covers every PushClickTraceEvent variant.
    // Adding a new variant to the Rust enum (and regenerating TS types) causes
    // a compile-time error here, forcing this function to be updated in lockstep.
    // Capture event.type before the switch so the default branch can use it in the
    // error message without casting from `never` (which would be misleading idiom).
    const eventType = (event as { type: string }).type;
    switch (event.type) {
      case "HandlerEntry":
      case "MatchAllResult":
      case "BrennClientsFilter":
      case "T1Chosen":
      case "T1Skipped":
      case "OpenWindowCalled":
      case "OpenWindowResult":
      case "FenixCascadeSkipped":
      case "Terminal":
        break;
      default: {
        // TypeScript narrows `event` to `never` here if all cases are covered.
        const _exhaustive: never = event;
        void _exhaustive;
        reportClientError(
          `PushClickTrace: unknown event type (${eventType})`,
        );
        return;
      }
    }
    if (userId === null) {
      // The push payload had no numeric user_id — the SW legitimately emits
      // null here. This is not a client error; drop the trace silently.
      // If this becomes common it indicates a backend push-payload bug.
      console.warn("PushClickTrace: user_id is null; dropping trace event");
      return;
    }
    this.ws.send({ type: "PushClickTrace", user_id: userId, event });
  }

  /**
   * Push a history entry for a SinglePane pane overlay (FilePicker or
   * FileViewer).  The URL stays the same — we're just adding a history
   * entry so browser-back closes the overlay instead of navigating away.
   */
  private _pushPaneState(pane: "FilePicker" | "FileViewer" | "TodoList"): void {
    // Don't stack duplicate entries (e.g., CC sends multiple DisplayFile
    // messages in a row — the user should only need one back-press).
    const current = history.state as HistoryState | null;
    if (current?.pane === pane) return;

    const state: HistoryState = {
      conversationId: this.currentConversationId,
      pane,
    };
    history.pushState(state, "", window.location.pathname);
  }

  /**
   * Handle popstate for SinglePane pane navigation.  Called when the user
   * presses browser-back (or forward) and the conversation hasn't changed.
   */
  private _handlePanePopstate(pane: string | null): void {
    if (pane === "FileViewer") {
      // Forward-nav back into the viewer — viewer may still have cached content.
      this.slots = slotReset(this.slots, 0, {
        current: { type: "FileViewer" },
        parent: { type: "Chat" },
      });
    } else if (pane === "FilePicker") {
      // Back from FileViewer to FilePicker, or forward into FilePicker.
      this.slots = slotReset(this.slots, 0, {
        current: { type: "FilePicker" },
        parent: { type: "Chat" },
      });
      this.fileViewer.clear();
    } else if (pane === "TodoList") {
      // Forward-nav back into the todo list.
      this.slots = slotReset(this.slots, 0, {
        current: { type: "TodoList" },
        parent: { type: "Chat" },
      });
    } else {
      // Back to chat.
      this.slots = slotReset(this.slots, 0, {
        current: { type: "Chat" },
        parent: null,
      });
      this.fileViewer.clear();
    }
  }

  private handleUserSubmit(text: string, attachments: AttachmentRef[], meta: AttachmentMeta[]): void {
    // Snapshot selected tasks before clearing. Strip FE-only `tldr` — the wire
    // `SelectedTask` is `{ref}` only.
    const selectedSnapshot: SelectedTask[] = this.selectedTasks.size > 0
      ? [...this.selectedTasks.values()].map(t => ({ ref: t.ref }))
      : [];

    // Show the message locally immediately for responsiveness.
    // Construct a full attributed echo (username + timestamp) so the
    // optimistic bubble renders identically to server-echoed messages.
    // The backend will also broadcast a UserMessageEcho, but we
    // suppress it for messages we sent ourselves (see handleMessage).
    this.pendingEchoes.push(text);
    this.messageList.appendUserMessage({
      text,
      username: this.currentUsername,
      timestamp: new Date().toISOString(),
      isSelf: true,
      attachments: meta,
      selectedTasks: selectedSnapshot,
    });

    // Clear selection after snapshotting.
    if (selectedSnapshot.length > 0) {
      this.selectedTasks = new Map();
    }

    // Include model override only if it differs from the app default.
    const model = this.currentModel !== this.defaultModel ? this.currentModel : null;
    this.ws.send({
      type: "SendMessage",
      text,
      attachments,
      model,
      selected_tasks: selectedSnapshot,
    });
    this.focusInput();
  }

  private handleStopRequest(): void {
    this.ws.send({ type: "StopRequest" });
  }

  /** Resolve the effective current model from user preference + app default. */
  private resolveCurrentModel(): void {
    const pref = this.settings.preferredModel;
    // Use preference if it's in the available list, otherwise fall back to default.
    if (pref && this.availableModels.some(m => m.value === pref)) {
      this.currentModel = pref;
    } else {
      this.currentModel = this.defaultModel;
    }
  }

  private handleModelChange(model: string): void {
    this.settings.preferredModel = model;
    this.currentModel = model;
  }

  private handleApprovalDecision(
    allow: boolean,
    updatedInput?: Record<string, unknown>,
    denyReason?: string,
  ): void {
    if (this.approvalQueue.length === 0) return;

    const current = this.approvalQueue[0];
    const decision = allow
      ? ({ decision: "Allow" as const, updated_input: updatedInput ?? null })
      : ({ decision: "Deny" as const, reason: denyReason ?? null });

    if (current.type === "PermissionRequest") {
      this.ws.send({
        type: "PermissionResponse",
        request_id: current.request_id,
        decision,
      });
    } else {
      this.ws.send({
        type: "ToolCardResponse",
        request_id: current.request_id,
        decision,
      });
    }

    // Remove the head and advance to next (or hide dialog).
    this.approvalQueue.shift();
    this.advanceApprovalQueue();
  }

  private handleAlwaysAllow(patterns: string[], scope: RuleScope): void {
    if (this.approvalQueue.length === 0) return;

    const current = this.approvalQueue[0];
    // AlwaysAllow is a permission concept — tool cards don't support it.
    if (current.type !== "PermissionRequest") return;

    this.ws.send({
      type: "PermissionResponse",
      request_id: current.request_id,
      decision: {
        decision: "AlwaysAllow",
        patterns,
        scope,
        tool_name: current.tool_name,
      },
    });

    // Don't dismiss yet — wait for PermissionResolved (success) or
    // ApprovalRuleError (invalid pattern, dialog stays open).
  }

  /** Show the approval container for the current head of the queue. */
  private showCurrentApproval(): void {
    if (this.approvalQueue.length === 0) return;
    const head = this.approvalQueue[0];

    this.approvalContainer.formattedDisplay = head.formatted_display;
    this.approvalContainer.queuePosition = 1;
    this.approvalContainer.queueTotal = this.approvalQueue.length;
    this.approvalVisible = true;
    // Scroll the message list so the approval card is visible.
    this.messageList?.scrollToBottomNow();
  }

  /** Update the "1 of N" counter on the approval container without re-showing. */
  private updateApprovalCounter(): void {
    if (this.approvalQueue.length === 0) return;
    this.approvalContainer.queuePosition = 1;
    this.approvalContainer.queueTotal = this.approvalQueue.length;
  }

  /** After resolving the head, show next approval or hide all dialogs. */
  private advanceApprovalQueue(): void {
    if (this.approvalQueue.length > 0) {
      this.showCurrentApproval();
    } else {
      this.approvalVisible = false;
      this.focusInput();
    }
  }

  /**
   * Remove an approval from the queue by request_id (cancelled or resolved
   * externally). If it was the head, advance to the next.
   */
  private removeFromApprovalQueue(requestId: string): void {
    const idx = this.approvalQueue.findIndex(
      (a) => a.request_id === requestId,
    );
    if (idx === -1) return;

    this.approvalQueue.splice(idx, 1);

    if (idx === 0) {
      // Was the currently displayed approval — advance.
      this.advanceApprovalQueue();
    } else {
      // Was a queued-but-not-displayed entry — just update counter.
      this.updateApprovalCounter();
    }
  }

  private handleConversationSelect(id: number): void {
    this.ws.send({ type: "SwitchConversation", conversation_id: id });
    this.sidebarVisible = false;
  }

  private handleNewConversation(): void {
    this.ws.send({ type: "NewConversation" });
    this.sidebarVisible = false;
  }

  private handleStealApp(): void {
    this.ws.send({ type: "StealApp" });
    this.showStealButton = false;
  }

  private sendCompactionRequest(): void {
    this.ws.send({ type: "RequestCompaction" });
  }

  /** Render the privacy toggle button (multiuser + owner only). */
  private _renderPrivacyToggle() {
    if (
      !this.isMultiuser ||
      this.currentConversationId === null
    ) {
      return nothing;
    }
    if (this.currentIsOwner) {
      // Owner sees a clickable toggle.
      const icon = this.currentConversationShared ? "\u{1F517}" : "\u{1F512}";
      const label = this.currentConversationShared ? "shared" : "private";
      const title = this.currentConversationShared
        ? "Click to make private"
        : "Click to make shared";
      return html`<button
        class="privacy-toggle"
        @click=${() => this.handleTogglePrivacy()}
        title=${title}
      >
        ${icon} ${label}
      </button>`;
    } else {
      // Non-owner sees a read-only badge.
      const icon = this.currentConversationShared ? "\u{1F517}" : "\u{1F512}";
      const label = this.currentConversationShared ? "shared" : "private";
      return html`<span class="privacy-badge">${icon} ${label}</span>`;
    }
  }

  private handleTogglePrivacy(): void {
    if (this.currentConversationId === null) return;
    this.ws.send({
      type: "SetConversationPrivacy",
      conversation_id: this.currentConversationId,
      shared: !this.currentConversationShared,
    });
  }

  private handleToggleEnterSends(): void {
    this.settings.toggleEnterSends();
    this.enterSends = this.settings.enterSends;
  }

  // --- Todo handlers ---

  /** Handle a TodoState message from the backend.
   *
   * `todayStr` is the server's authoritative "today" in its resolved
   * timezone — use it directly for sectioning instead of
   * `localTodayStr()` (browser's local TZ), which can disagree when the
   * server resolved a different zone. See
   * `docs/designs/graf-user-tz.md`. */
  private _handleTodoState(tasks: TodoItem[], todayStr: string): void {
    const wasFirst = !this.hasTodoList;
    this.hasTodoList = true;
    this.todoTasks = tasks;
    this.todoTodayStr = todayStr;

    // Design §3.4.3: reconcile slot state against the fresh task list
    // rather than wholesale-clearing it. A pending slot whose task is
    // gone from the refresh has been implicitly settled by the server
    // (the TodoDoneResult/TodoMutationResult may arrive later, or may have been
    // lost on a WS reconnect — either way the slot must transition so the row
    // isn't stuck greyed forever).
    const livePathKeys = new Set<string>();
    for (const t of tasks) livePathKeys.add(todoKey(t.path, t.repo));
    const keysToDrop: string[] = [];
    for (const [key, entry] of this.todoSlotState) {
      if (entry.kind !== "pending") continue;
      if (livePathKeys.has(key)) continue;
      // Task absent from refresh — implicit settlement path. Exhaustive
      // switch on `entry.action` so a new `TodoPendingAction` variant
      // fails compilation here rather than silently mis-attributing
      // the tile (sibling `_handleTodoActionResult` uses the same
      // pattern). done / snooze / schedule tile text is conservative
      // — no ack fields available; on a later real TodoDoneResult/TodoMutationResult
      // the idempotency check (`kind === "pending"` only) makes this
      // transition terminal.
      const action = entry.action;
      if (action === "reorder") {
        // Reorder never has a settled tile (§3.6). Drop outright.
        keysToDrop.push(key);
      } else if (
        action === "done" ||
        action === "snooze" ||
        action === "schedule"
      ) {
        this._settleSlot(
          key,
          action,
          settledTileText(action, entry.targetEffectiveDate),
        );
      } else {
        const _exhaustive: never = action;
        void _exhaustive;
      }
    }
    for (const key of keysToDrop) {
      this._collapseSettledSlot(key);
    }

    // No correlation id between TodoRefresh and TodoState — any TodoState
    // from any source means the list is fresh, so drop the indicator.
    this._clearTodoRefreshPending();

    // Any queued refresh may now be unblocked if the reconcile above
    // cleared the last pending slot.
    this._maybeFireQueuedRefresh();

    // On first TodoState in TwoColumn: show the todo list in the secondary
    // slot if no artifacts are occupying it.
    if (wasFirst && this.currentLayout === "TwoColumn") {
      if (
        this.artifactIndex.length === 0 &&
        slotType(this.slots, 1) === "FilePicker"
      ) {
        this.slots = slotReset(this.slots, 1, {
          current: { type: "TodoList" },
          parent: null,
        });
      }
      this.secondarySlotVisible = true;
    }
  }

  /** Render the chip bar showing selected tasks above the input. */
  private _renderChipBar() {
    const chips = [...this.selectedTasks.entries()];
    return html`
      <div class="chip-bar">
        ${chips.map(([key, task]) => html`
          <span class="task-chip">
            <span class="chip-text">${task.tldr}</span>
            <button class="chip-remove" @click=${() => this._removeSelectedTask(key)}>&times;</button>
          </span>
        `)}
        ${chips.length >= 2 ? html`
          <button class="chip-clear" @click=${() => this._clearSelectedTasks()}>clear all</button>
        ` : nothing}
      </div>
    `;
  }

  /** Remove a single task from selection (chip × click). */
  private _removeSelectedTask(key: string): void {
    const next = new Map(this.selectedTasks);
    next.delete(key);
    this.selectedTasks = next;
  }

  /** Clear all selected tasks (chip bar "clear all" button). */
  private _clearSelectedTasks(): void {
    this.selectedTasks = new Map();
  }

  /** Handle selection changes from the todo list component. */
  /** Build a key→task index from the current task list. */
  private _buildTaskIndex(): Map<string, TodoItem> {
    const index = new Map<string, TodoItem>();
    for (const t of this.todoTasks) {
      index.set(todoKey(t.path, t.repo), t);
    }
    return index;
  }

  /**
   * Find the insertion index in a task array based on after/before anchors.
   * Returns null if no anchors are provided.
   */
  private _resolveInsertionIndex(
    tasks: TodoItem[],
    after: TodoAnchor | null,
    before: TodoAnchor | null,
  ): number | null {
    if (after) {
      const afterKey = todoKey(after.path, after.repo);
      const afterIdx = tasks.findIndex(
        (t) => todoKey(t.path, t.repo) === afterKey,
      );
      return afterIdx !== -1 ? afterIdx + 1 : tasks.length;
    }
    if (before) {
      const beforeKey = todoKey(before.path, before.repo);
      const beforeIdx = tasks.findIndex(
        (t) => todoKey(t.path, t.repo) === beforeKey,
      );
      return beforeIdx !== -1 ? beforeIdx : 0;
    }
    return null;
  }

  /** Send a TodoReorder WS message, normalizing anchor repo fields to null. */
  private _sendTodoReorder(
    path: string,
    repo: string | null | undefined,
    after: TodoAnchor | null,
    before: TodoAnchor | null,
  ): void {
    this.ws.send({
      type: "TodoReorder",
      path,
      repo: repo ?? null,
      after: after
        ? { path: after.path, repo: after.repo ?? null }
        : null,
      before: before
        ? { path: before.path, repo: before.repo ?? null }
        : null,
    });
  }

  private _handleSelectionChange(keys: Set<string>): void {
    const tasksByKey = this._buildTaskIndex();

    // Sync the selectedTasks map: add new keys, remove stale ones.
    const next = new Map<string, LocalSelectedTask>();
    for (const key of keys) {
      const existing = this.selectedTasks.get(key);
      if (existing) {
        next.set(key, existing);
      } else {
        const task = tasksByKey.get(key);
        if (task) {
          const ref = task.repo ? `${task.repo}:${task.path}` : task.path;
          next.set(key, { ref, tldr: task.tldr });
        }
      }
    }
    this.selectedTasks = next;
  }

  /** Handle a TodoDoneResult or TodoMutationResult message from the backend. */
  private _handleTodoActionResult(
    path: string,
    success: boolean,
    error: string | null,
    nextCheckInDate: string | null,
    nextDueDate: string | null,
    terminal: boolean | null,
    repo?: string | null,
  ): void {
    // Use exact key match when the backend supplies `repo`; fall back to
    // path-suffix scan for older wire messages without the field.
    let matchedKey: string | null = null;
    let matchedEntry: SlotState | null = null;
    if (repo != null) {
      const exactKey = todoKey(path, repo);
      const entry = this.todoSlotState.get(exactKey);
      if (entry?.kind === "pending") {
        matchedKey = exactKey;
        matchedEntry = entry;
      }
    }
    // Fall back to path-suffix scan: covers single-repo configs (repo == null
    // on the wire) and guards against key-mismatch races when repo is supplied
    // but the exact key was not found (e.g. double-delivery, client race).
    if (matchedKey === null) {
      const fallbackKey = this._findKeyBySuffix(this.todoSlotState, path);
      if (fallbackKey !== undefined) {
        const fallbackEntry = this.todoSlotState.get(fallbackKey);
        if (fallbackEntry?.kind === "pending") {
          matchedKey = fallbackKey;
          matchedEntry = fallbackEntry;
        }
      }
    }
    const action =
      matchedEntry?.kind === "pending" ? matchedEntry.action : null;

    if (!success) {
      if (matchedKey) {
        // Drop the slot entirely (no tile for errors — the row stays
        // interactive for retry). §6: also clear the debounce so the
        // user can retry immediately.
        this._collapseSettledSlot(matchedKey);
        this._clearDebounce(matchedKey);
      }
      // PRD-snooze §2 snap-back: revert the optimistic reorder so that
      // the post-thaw render (once all pending slots settle) shows the
      // original order rather than the optimistically-spliced order.
      // During the frozen-pending phase the renderer paints from
      // todoFrozenSnapshot (captured pre-splice), so snap-back has no
      // visible effect until thaw — this guard is for the post-thaw window.
      if (action === "reorder" && matchedKey) {
        const snap = this.todoReorderSnapshots.get(matchedKey);
        if (snap) {
          this.todoTasks = snap.tasks;
          this.selectedTasks = snap.selectedTasks;
          this.todoReorderSnapshots.delete(matchedKey);
        }
      }
      if (matchedKey) {
        // §7.2: surface a row-level badge. Toast is suppressed for errors
        // per §5.3 / §7.2 — the chat inject + badge is the only surface.
        this._showRowError(matchedKey, error ?? "see chat");
      }
      return;
    }
    // On success, clean up any reorder snapshot (superseded by server state).
    // Normal path: matchedKey is non-null and we delete by exact key.
    // Race path: matchedKey is null (slot cleared by watchdog before the
    // ack arrived). Try exact key via repo first; fall back to suffix scan.
    if (matchedKey) {
      this.todoReorderSnapshots.delete(matchedKey);
    } else {
      // Attempt exact match when repo is known (avoids ambiguity in multi-repo
      // deployments where two tasks may share the same path across repos).
      const snapshotKey =
        repo != null
          ? this.todoReorderSnapshots.has(todoKey(path, repo))
            ? todoKey(path, repo)
            : this._findKeyBySuffix(this.todoReorderSnapshots, path)
          : this._findKeyBySuffix(this.todoReorderSnapshots, path);
      if (snapshotKey !== undefined) {
        this.todoReorderSnapshots.delete(snapshotKey);
      } else {
        console.warn(
          `Todo reorder snapshot not found for path=${path} repo=${repo ?? "null"} on success ack; snapshot may already be cleared by watchdog`,
        );
      }
    }

    // Success path:
    //  - `done` / `snooze` transition to settled (in-place tile) AND
    //    still fire the toast — design §3.4.2 F6 keeps the toast.
    //  - `reorder` clears the slot outright (no tile — §3.6).
    //  - `action === null` (no matched pending slot): action for a
    //    row this client never dispatched (another tab / server
    //    synthesized ack). Nothing to settle; warn for visibility.
    if (action === null) {
      console.warn(
        `Todo action success for unmatched path=${path}; no pending slot on this client`,
      );
    } else if (action === "done") {
      // PRD-done §8: the "Next: …" label is driven by
      // `next_check_in_date` primarily, with `next_due_date` as a
      // fallback for tasks whose recurrence is anchored on `due_date`
      // (graf returns the check-in date for anchored tasks and the
      // due date for pure-deadline tasks). Tile text and toast text
      // share the same outcome discriminator so a future case
      // (e.g. terminal-with-date) can't land in one without the
      // other.
      const nextAnchor = nextCheckInDate ?? nextDueDate;
      let tileText: string;
      if (nextAnchor && !terminal) {
        tileText = `Next: ${shortDate(nextAnchor)}`;
      } else if (terminal === true) {
        tileText = "That was the last one.";
      } else {
        tileText = "Done.";
      }
      const toastText =
        tileText === "Done." ? "Done." : `Done. ${tileText}`;
      this.toastHost?.push({ text: toastText });
      if (matchedKey) {
        this._settleSlot(matchedKey, "done", tileText);
      }
    } else if (action === "snooze") {
      if (matchedKey && matchedEntry?.kind === "pending") {
        this._settleSlot(
          matchedKey,
          "snooze",
          settledTileText("snooze", matchedEntry.targetEffectiveDate),
        );
      }
    } else if (action === "schedule") {
      if (matchedKey && matchedEntry?.kind === "pending") {
        this._settleSlot(
          matchedKey,
          "schedule",
          settledTileText("schedule", matchedEntry.targetEffectiveDate),
        );
      }
    } else if (action === "reorder") {
      if (matchedKey) {
        this._collapseSettledSlot(matchedKey);
      }
    } else {
      // Exhaustiveness guard — a future TodoPendingAction addition
      // fails here at compile time rather than silently falling
      // through.
      const _exhaustive: never = action;
      void _exhaustive;
    }

    // A successful mutation implies any previous error on this row is
    // stale — clear the badge immediately.
    if (matchedKey) {
      this._clearRowError(matchedKey);
    }
  }

  /** Record a slot's state (at dispatch or on settlement). Mutates the
   *  Map in place + calls `requestUpdate()` — standard Lit pattern for
   *  mutable reactive state. On pending dispatch, arms the 30s
   *  watchdog so a dropped TodoDoneResult/TodoMutationResult doesn't leave the row
   *  stuck greyed forever (design §3.4.4).
   *
   *  Lit coalesces consecutive synchronous `requestUpdate()` calls via
   *  its `isUpdatePending` guard (see
   *  `@lit/reactive-element/development/reactive-element.js` — the
   *  second call short-circuits when the first hasn't rendered yet),
   *  so callers that issue N updates in a single pass (e.g.
   *  `_handleMultiReorder`) still pay exactly one render. No explicit
   *  batch flag needed. */
  private _setSlotState(state: SlotState): void {
    const key = todoKey(state.path, state.repo);
    // Universal freeze hook for done/snooze: the done/snooze dispatch
    // paths do NOT mutate `todoTasks`, so capturing the snapshot here
    // records the pre-pending view. Reorder dispatch does mutate
    // `todoTasks` before reaching this point, so both reorder handlers
    // call `_freezeListIfNeeded` explicitly at the top of the handler
    // BEFORE their splice. `_freezeListIfNeeded` is idempotent, so the
    // later call here is a no-op on the reorder path.
    if (state.kind === "pending") {
      this._freezeListIfNeeded();
    }
    this.todoSlotState.set(key, state);
    if (state.kind === "pending") {
      this._armSlotWatchdog(key, state.startedAt);
    }
    this.requestUpdate();
  }

  /** Capture a snapshot of the current todo list grouping if one isn't
   *  already captured. Idempotent — repeated calls within a triage
   *  session return immediately, which is the contract the reorder
   *  handlers rely on (they call explicitly at entry, pre-splice, and
   *  the later `_setSlotState` call falls through as a no-op).
   *
   *  Per CLAUDE.md's "better dead than wrong / panic rather than tolerate a
   *  bug" posture applied to client-side invariants: capturing against
   *  an empty `todoTasks` would produce an empty snapshot that the
   *  renderer would interpret as "frozen and empty" — a state that
   *  cannot naturally occur (a pending dispatch implies the user
   *  clicked a visible row). Throw rather than silently degrading. */
  private _freezeListIfNeeded(): void {
    if (this.todoFrozenSnapshot !== null) return;
    if (this.todoTasks.length === 0) {
      throw new Error(
        "_freezeListIfNeeded: refusing to snapshot empty todoTasks (invariant: freeze only happens on first-pending, which implies ≥1 visible row)",
      );
    }
    this.todoFrozenSnapshot = {
      groups: groupTasksByDate(this.todoTasks, this.todoTodayStr),
      todayStr: this.todoTodayStr,
    };
  }

  /** Drop all settled / dismissed slots, clear the frozen snapshot,
   *  and request a re-render. The list reshapes to match current
   *  `todoTasks` on the next render.
   *
   *  Called from exactly three places: `_onIdleTimerFire` (idle
   *  timeout), `_collapseSettledSlot`'s zero-pending-zero-settled
   *  branch (triage ended without arming idle), and
   *  `_handleDismissSettled` when the last × leaves zero pending +
   *  zero settled.
   *
   *  Pending entries are NOT dropped here — a pending slot outside a
   *  frozen window is an impossible state. Defensive: warn and leave
   *  it alone rather than silently dropping and hiding a state-machine
   *  bug. */
  private _thawFrozenList(): void {
    const toDrop: string[] = [];
    for (const [key, state] of this.todoSlotState) {
      if (state.kind === "settled" || state.kind === "dismissed") {
        toDrop.push(key);
      } else if (state.kind === "pending") {
        console.warn(
          `_thawFrozenList: pending slot ${key} encountered during thaw; leaving alone (state-machine bug suspected)`,
        );
      }
    }
    for (const key of toDrop) {
      this.todoSlotState.delete(key);
      this._clearSlotWatchdog(key);
    }
    this.todoFrozenSnapshot = null;
    this.requestUpdate();
  }

  /** Transition a slot from pending to settled. Carries the
   *  `SlotSnapshot` fields through unchanged. Cancels the pending
   *  watchdog and arms the list-level idle timer if no pending slots
   *  remain. Design §3.4.2. */
  private _settleSlot(
    key: string,
    action: "done" | "snooze" | "schedule",
    tileText: string,
  ): void {
    const entry = this.todoSlotState.get(key);
    if (!entry || entry.kind !== "pending") {
      // Defensive: already settled (e.g. the TodoDoneResult/TodoMutationResult
      // arrived after a TodoState refresh already settled the slot). Idempotent.
      return;
    }
    const settled: SlotState = {
      kind: "settled",
      action,
      settledAt: Date.now(),
      tileText,
      path: entry.path,
      repo: entry.repo,
      taskTldr: entry.taskTldr,
      targetEffectiveDate: entry.targetEffectiveDate,
    };
    this.todoSlotState.set(key, settled);
    this._clearSlotWatchdog(key);
    this.requestUpdate();
    this._armIdleTimerIfNeeded();
    // Settling a slot may have been the last pending — check if a
    // queued refresh can now fire.
    this._maybeFireQueuedRefresh();
  }

  /** Drop a slot entirely (manual `×` dismiss, error, reorder success,
   *  watchdog fire, bulk idle-fire, etc.). Cancels the slot's watchdog
   *  if any.
   *
   *  Design §3.5 idle-timer contract has two sub-rules that interact
   *  here:
   *
   *  1. "The timer is re-armed when the last pending slot transitions
   *     to settled (or to cleared / errored), provided at least one
   *     settled slot remains." → re-arm on pending→cleared.
   *  2. "Manual `×` dismiss on a tile drops just that one slot; the
   *     list-level idle timer keeps running for the others." → do NOT
   *     re-arm on settled→cleared (the currently-armed timer must
   *     keep its accumulated elapsed time, not be reset to a fresh
   *     5s).
   *
   *  The pre-delete `prev.kind` check discriminates: we only re-arm
   *  when the slot we just removed was pending. `_armIdleTimerIfNeeded`
   *  starts a fresh timer (`setTimeout`), so an unconditional re-arm
   *  would violate rule 2 above — round-2 review caught the regression. */
  private _collapseSettledSlot(key: string): void {
    const prev = this.todoSlotState.get(key);
    const hadEntry = this.todoSlotState.delete(key);
    this._clearSlotWatchdog(key);
    if (!hadEntry) return;
    const counts = this._slotKindCounts();
    if (!counts.anySettled) {
      this._cancelIdleTimer();
    } else if (prev?.kind === "pending") {
      // Pending → cleared (error / reorder-success / watchdog) is a
      // "new action just finished" transition per §3.5 rule 1 —
      // re-arm with fresh 5s for the surviving settled tiles.
      this._armIdleTimerIfNeeded();
    }
    // Settled → cleared (manual × / idle fire) intentionally leaves
    // the existing idle timer alone (§3.5 rule 2).
    this.requestUpdate();
    this._maybeFireQueuedRefresh();
    // Freeze-thaw hook: if this collapse leaves zero pending + zero
    // settled, the triage session is done — thaw the list so the
    // buffered `todoTasks` becomes visible again. Cases reaching this
    // branch: (a) reorder-only session (no settled tile ever), (b)
    // error / watchdog on last pending, (c) refresh-click's
    // synchronous settled drain followed by this collapse. The idle
    // timer handles the done/snooze-with-tile case separately via
    // `_onIdleTimerFire`.
    this._maybeThawIfTriageEnded();
  }

  /** Arm the 30s watchdog for a pending slot (design §3.4.4). */
  private _armSlotWatchdog(key: string, startedAt: number): void {
    this._clearSlotWatchdog(key);
    // Elapsed since dispatch may be nonzero on implicit-settlement
    // paths that re-enter pending — clamp the timer to a positive
    // remaining interval so it fires ~30s after `startedAt`.
    const elapsed = Date.now() - startedAt;
    const remaining = Math.max(0, BrennApp.TODO_SLOT_WATCHDOG_MS - elapsed);
    const timer = setTimeout(() => {
      this._onSlotWatchdogFire(key);
    }, remaining);
    this.todoSlotWatchdogs.set(key, timer);
  }

  /** Cancel a slot's watchdog timer. */
  private _clearSlotWatchdog(key: string): void {
    const t = this.todoSlotWatchdogs.get(key);
    if (t !== undefined) {
      clearTimeout(t);
      this.todoSlotWatchdogs.delete(key);
    }
  }

  /** Watchdog fired — the pending slot never got its TodoDoneResult/TodoMutationResult.
   *  Drop the slot (no tile), clear debounce so retry works, log for
   *  visibility (CLAUDE.md "better dead than wrong" — this is a protocol-level
   *  anomaly we want to surface).
   *
   *  Delegates the actual slot removal + idle-timer / queued-refresh
   *  bookkeeping to `_collapseSettledSlot` so the four exit paths
   *  (manual ×, idle fire, error, watchdog) share the same cleanup.
   *  Reorder-watchdog note: the row has been optimistically reordered
   *  in `todoTasks` and will render at that position until the next
   *  TodoState refresh; the warn names the action so a stuck reorder
   *  is distinguishable in devtools from a stuck done/snooze.
   *  For stuck reorders specifically, force a TodoRefresh so the client
   *  re-syncs immediately rather than waiting for the next natural refresh. */
  private _onSlotWatchdogFire(key: string): void {
    const entry = this.todoSlotState.get(key);
    if (!entry || entry.kind !== "pending") return;
    console.warn(
      `Todo slot watchdog fired — TodoDoneResult/TodoMutationResult never arrived for key=${key} action=${entry.action}`,
    );
    const wasReorder = entry.action === "reorder";
    this._clearDebounce(key);
    this._collapseSettledSlot(key);
    // Drop any reorder snapshot stashed for this key so it doesn't
    // accumulate across many timeout-lost reorders within a session.
    this.todoReorderSnapshots.delete(key);
    // Force re-sync: a stuck reorder leaves todoTasks optimistically
    // spliced until the next natural TodoState. Route through
    // _handleTodoRefresh so the pending/queued split is handled correctly:
    // if another slot is still pending, _handleTodoRefresh queues the
    // refresh until that slot settles; if a refresh is already in flight,
    // the early-return guard prevents double-send.
    if (wasReorder) {
      if (this.todoRefreshPending || this.todoRefreshQueued) {
        console.warn(
          `Todo reorder watchdog: refresh already in flight or queued; re-sync deferred (key=${key})`,
        );
      }
      this._handleTodoRefresh();
    }
  }

  /** Arm the list-level idle timer if at least one settled slot exists
   *  and no slot is pending. Cancels any currently-armed idle timer
   *  first; design §3.5 uses a single timer. */
  private _armIdleTimerIfNeeded(): void {
    this._cancelIdleTimer();
    const counts = this._slotKindCounts();
    if (counts.anyPending) return; // active triage; don't arm.
    if (!counts.anySettled) return;
    this.todoIdleTimer = setTimeout(() => {
      this._onIdleTimerFire();
    }, BrennApp.TODO_IDLE_DISMISS_MS);
  }

  /** Cancel the list-level idle timer if armed. */
  private _cancelIdleTimer(): void {
    if (this.todoIdleTimer !== null) {
      clearTimeout(this.todoIdleTimer);
      this.todoIdleTimer = null;
    }
  }

  /** Idle-dismiss fire: thaw the frozen list, which drops every
   *  settled + dismissed slot at once and clears the snapshot. The
   *  list reshapes to current `todoTasks` on the next render. */
  private _onIdleTimerFire(): void {
    this.todoIdleTimer = null;
    this._thawFrozenList();
    // The old code's per-key `_collapseSettledSlot` loop also fired
    // `_maybeFireQueuedRefresh` transitively. With `_thawFrozenList`
    // owning the cleanup, call it once explicitly in case a refresh
    // was queued waiting for settled tiles to clear.
    this._maybeFireQueuedRefresh();
  }

  /** Whether any slot is currently pending. Tight inline loop — the
   *  more general `_slotKindCounts` would also compute `anySettled`
   *  and allocate an object; this caller (and `_maybeFireQueuedRefresh`)
   *  only needs the one bit. */
  private _hasPendingSlots(): boolean {
    for (const s of this.todoSlotState.values()) {
      if (s.kind === "pending") return true;
    }
    return false;
  }

  /** Return the first key in `map` whose suffix matches `":${path}"`.
   *  Used as a fallback scan when the exact `todoKey(path, repo)` is
   *  unavailable (single-repo wire messages without repo, or race
   *  paths where the slot was already cleared). Both
   *  `_handleTodoActionResult` (slot scan and snapshot scan) use this
   *  pattern; centralising it means a key-format change (e.g. escaping,
   *  multi-segment repos) only needs one edit. */
  private _findKeyBySuffix<V>(
    map: Map<string, V>,
    path: string,
  ): string | undefined {
    const suffix = `:${path}`;
    for (const k of map.keys()) {
      if (k.endsWith(suffix)) return k;
    }
    return undefined;
  }

  /** One pass over `slotState` returning whether any pending or
   *  settled slots exist. Replaces near-identical hand-rolled loops
   *  in `_collapseSettledSlot`, `_handleDismissSettled`, and
   *  `_armIdleTimerIfNeeded`. The render path doesn't read this so
   *  it's not memoized — `slotState` is small (≤ tens of rows mid-
   *  triage) and these calls are off the render hot path. */
  private _slotKindCounts(): { anyPending: boolean; anySettled: boolean } {
    let anyPending = false;
    let anySettled = false;
    for (const s of this.todoSlotState.values()) {
      if (s.kind === "pending") anyPending = true;
      else if (s.kind === "settled") anySettled = true;
      if (anyPending && anySettled) break;
    }
    return { anyPending, anySettled };
  }

  /** Common triage-end gate: if zero pending + zero settled remain
   *  AND a frozen snapshot is active, thaw. Both
   *  `_collapseSettledSlot` and `_handleDismissSettled` end with the
   *  same condition; this central gate keeps the predicate from
   *  drifting.
   *
   *  Recomputes counts internally rather than trusting a caller-
   *  computed struct, so a future caller that mutates `slotState`
   *  between its own count and this gate doesn't fire against stale
   *  data. The recompute is one cheap pass over a small map (off the
   *  render hot path). */
  private _maybeThawIfTriageEnded(): void {
    if (this.todoFrozenSnapshot === null) return;
    const counts = this._slotKindCounts();
    if (!counts.anyPending && !counts.anySettled) {
      this._thawFrozenList();
    }
  }

  /** Called after every slot transition that may have cleared the
   *  last pending entry. If a refresh is queued and no pending slots
   *  remain, fire the refresh now (design §3.5). */
  private _maybeFireQueuedRefresh(): void {
    if (!this.todoRefreshQueued) return;
    if (this._hasPendingSlots()) return;
    this._fireRefreshNow();
  }

  /** Manual `×` dismiss on a settled tile.
   *
   *  Under the freeze-the-list design the tile's row is still holding
   *  a position in the rendered snapshot; dropping the slot outright
   *  would let surrounding rows shift up (other surviving tiles plus
   *  the never-moved live rows would re-flow). Transition the slot
   *  to `dismissed` instead — `_renderTask` paints a same-height
   *  placeholder so the surrounding rows stay put. The placeholder is
   *  collapsed on thaw.
   *
   *  If this dismissal leaves zero pending + zero settled (only
   *  dismissed rows remaining), the triage session is effectively
   *  over — thaw immediately. The dismissed placeholders' only job
   *  was to hold positions for surviving tiles that no longer exist. */
  private _handleDismissSettled(key: string): void {
    const entry = this.todoSlotState.get(key);
    // Two distinct early returns to keep the posture consistent with
    // surrounding code (`_thawFrozenList` warns on unexpected pending;
    // we surface the same way here):
    // - missing entry → benign UI race with thaw / fast double-click.
    //   Silent.
    // - non-settled entry (pending / dismissed) → click landed on a
    //   row that doesn't have a × button right now. Surface in
    //   devtools rather than swallowing.
    if (!entry) return;
    if (entry.kind !== "settled") {
      console.warn(
        `_handleDismissSettled: slot ${key} is ${entry.kind}, expected settled; ignoring`,
      );
      return;
    }
    const dismissed: SlotState = {
      kind: "dismissed",
      action: entry.action,
      dismissedAt: Date.now(),
      path: entry.path,
      repo: entry.repo,
      taskTldr: entry.taskTldr,
      targetEffectiveDate: entry.targetEffectiveDate,
    };
    this.todoSlotState.set(key, dismissed);
    // Settled → dismissed transition parallels settled → cleared in
    // terms of the idle timer: leave the currently-armed timer alone
    // (§3.5 rule 2 — manual × doesn't reset the timer for other
    // tiles). Check whether any settled remain; if not, cancel.
    const counts = this._slotKindCounts();
    if (!counts.anySettled) {
      this._cancelIdleTimer();
    }
    this.requestUpdate();
    // Zero-pending-zero-settled: thaw immediately. Dismissed-only is
    // no longer a useful state; the placeholders were holding positions
    // for tiles that no longer exist.
    this._maybeThawIfTriageEnded();
    this._maybeFireQueuedRefresh();
  }

  /** Per-row debounce guard (§6). Returns true if the row is currently
   *  locked out and the caller must short-circuit. Otherwise arms the
   *  400ms timer and returns false. */
  private _armDebounce(path: string, repo?: string, ms = 400): boolean {
    const key = todoKey(path, repo);
    if (this.todoDebounceTimers.has(key)) {
      return true;
    }
    const timer = setTimeout(() => {
      this.todoDebounceTimers.delete(key);
    }, ms);
    this.todoDebounceTimers.set(key, timer);
    return false;
  }

  /** Clear the debounce window for a row (used after an error so a
   *  deliberate retry lands immediately). */
  private _clearDebounce(key: string): void {
    const timer = this.todoDebounceTimers.get(key);
    if (timer !== undefined) {
      clearTimeout(timer);
      this.todoDebounceTimers.delete(key);
    }
  }

  /** Show a row-level error badge with 6s auto-dismiss (§7.2). */
  private _showRowError(key: string, text: string): void {
    const existing = this.todoErrorTimers.get(key);
    if (existing !== undefined) clearTimeout(existing);
    this.todoErrorKeys.set(key, text);
    this.requestUpdate();
    const timer = setTimeout(() => {
      this._clearRowError(key);
    }, 6000);
    this.todoErrorTimers.set(key, timer);
  }

  /** Clear a row-level error badge (auto-dismiss or on next mutation). */
  private _clearRowError(key: string): void {
    const timer = this.todoErrorTimers.get(key);
    if (timer !== undefined) {
      clearTimeout(timer);
      this.todoErrorTimers.delete(key);
    }
    if (this.todoErrorKeys.delete(key)) {
      this.requestUpdate();
    }
  }

  /** User tapped done on a task. */
  private _handleTodoDone(path: string, repo?: string): void {
    // §3.5: a queued refresh is a hard gate on new mutations — the
    // client has promised to send nothing until pending slots drain.
    if (this.todoRefreshQueued) return;
    if (this._armDebounce(path, repo)) return;
    const task = this._findLiveTask(path, repo);
    if (!task) return; // defensive — the UI only surfaces live rows.
    // New action: cancel any idle-dismiss so tiles stay alive while
    // the user is actively triaging.
    this._cancelIdleTimer();
    this._setSlotState({
      kind: "pending",
      action: "done",
      startedAt: Date.now(),
      path,
      repo,
      taskTldr: task.tldr,
    });
    this._clearRowError(todoKey(path, repo));
    // Backend requires `completion_date` — it refuses to guess, so the
    // browser sources its local today. Server-clock fallback was removed
    // in Phase 2 review; see `handle_todo_done` in `routes/ws.rs`.
    this.ws.send({
      type: "TodoDone",
      path,
      repo: repo ?? null,
      completion_date: localTodayStr(),
    });
  }

  /** Find the live `TodoItem` for a given path/repo, or null if absent.
   *  Used by the dispatch handlers to snapshot `taskTldr` into the
   *  pending slot (so the settled tile can render its title even after
   *  the task is gone from `todoTasks`). */
  private _findLiveTask(path: string, repo: string | undefined): TodoItem | null {
    const key = todoKey(path, repo);
    for (const t of this.todoTasks) {
      if (todoKey(t.path, t.repo) === key) return t;
    }
    return null;
  }

  /** User tapped snooze on a task — either the face (+1 day) or a menu
   *  entry (+3 / +7 / +30). */
  private _handleTodoSnooze(
    path: string,
    repo: string | undefined,
    effectiveDate: string,
    days: number,
  ): void {
    // `TodoItem.effective_date` is non-nullable by wire contract
    // (`ws_types.rs` — non-`Option` `NaiveDate`). Static types cover
    // statically-known callers but do not catch a wire-level regression
    // that serves a row with null/malformed `effective_date`. Warn and
    // bail rather than silently snoozing to a garbage date.
    if (
      typeof effectiveDate !== "string" ||
      !/^\d{4}-\d{2}-\d{2}$/.test(effectiveDate)
    ) {
      console.warn(
        "TodoSnooze: missing or malformed effective_date; refusing to snooze",
        { path, repo, effective_date: effectiveDate },
      );
      return;
    }
    if (!Number.isFinite(days) || days < 1) {
      console.warn("TodoSnooze: invalid days; refusing to snooze", {
        path,
        repo,
        days,
      });
      return;
    }
    // §3.5: queued-refresh short-circuit (same gate as `_armDebounce`).
    if (this.todoRefreshQueued) return;
    if (this._armDebounce(path, repo)) return;
    // Shared helper encodes `max(today, effective_date) + days` — a stale
    // `tentative_date` compounds correctly here (snoozing a recurring
    // task already surfaced for next week lands it N days after that,
    // not N days after today).
    const target = snoozeTargetDate(effectiveDate, localTodayStr(), days);
    const task = this._findLiveTask(path, repo);
    if (!task) return; // defensive — the UI only surfaces live rows.
    this._cancelIdleTimer();
    this._setSlotState({
      kind: "pending",
      action: "snooze",
      startedAt: Date.now(),
      path,
      repo,
      taskTldr: task.tldr,
      targetEffectiveDate: target,
    });
    this._clearRowError(todoKey(path, repo));
    this._sendTodoSchedule(path, repo, target);
  }

  /** Send a TodoSchedule WS message, normalizing the repo field to null.
   *  Shared by `_handleTodoSnooze` (snooze writes a `tentative_date`)
   *  and `_handleTodoSchedule` (heading-drop writes a `tentative_date`).
   *  Both wire ops are identical at the WS layer; the dispatch handlers
   *  diverge in their pending-slot bookkeeping (snooze action vs.
   *  schedule action) and in the post-action settled-tile copy. */
  private _sendTodoSchedule(
    path: string,
    repo: string | null | undefined,
    date: string,
  ): void {
    this.ws.send({
      type: "TodoSchedule",
      path,
      repo: repo ?? null,
      date,
    });
  }

  /** User dragged one or more tasks onto a date-bucket heading.
   *  Heading-drop fires `TodoSchedule` for each task: graf sets the
   *  `tentative_date` and removes any prior `sort_order`, so the task
   *  ranks naturally in its new bucket via
   *  `COALESCE(sort_order, priority, UNRANKED)`. See design.md §4 / §5. */
  private _handleTodoSchedule(target: ScheduleTarget): void {
    // §3.5: queued-refresh short-circuit (same gate as
    // `_handleTodoSnooze` / `_handleTodoReorder` / `_handleMultiReorder`).
    if (this.todoRefreshQueued) return;

    const targetDate = target.date;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(targetDate)) {
      console.warn(
        "TodoSchedule: malformed date; refusing to dispatch",
        { target },
      );
      return;
    }

    const primaryKey = todoKey(target.path, target.repo);
    const tasksByKey = this._buildTaskIndex();
    // Resolve participating keys: multi-select (selectedKeys) or just
    // the primary task. Display-ordered already by `<brenn-todo-list>`'s
    // ordered-keys builder.
    const keys: string[] = target.selectedKeys ?? [primaryKey];

    // Resolve to live tasks. Skip any that no longer exist (dispatch-
    // time race: TodoState refresh removed them between drop and now).
    const movedTasks: TodoItem[] = [];
    for (const k of keys) {
      const task = tasksByKey.get(k);
      if (task) movedTasks.push(task);
    }
    if (movedTasks.length === 0) {
      console.warn(
        "_handleTodoSchedule: no tasks resolved; dropping dispatch",
      );
      return;
    }

    // Per-task debounce: gate the primary entry-point first (matches
    // multi-reorder's primary-task gate). If the primary is debounced,
    // short-circuit the whole batch.
    if (this._armDebounce(target.path, target.repo)) return;

    // Freeze BEFORE marking pending. The renderer uses the snapshot
    // during the triage session so the user sees "scheduling…" labels
    // on rows in their pre-drop positions until thaw. Idempotent.
    // (Schedule does NOT mutate `todoTasks` optimistically — see
    // design.md §5; we mirror snooze's "no optimistic mutation"
    // behavior since the renderer reads from the snapshot anyway.)
    this._freezeListIfNeeded();

    this._cancelIdleTimer();
    const startedAt = Date.now();
    // Single pass: mark each slot pending, arm the per-task debounce
    // for non-primary keys, clear any row-error badge, and send the
    // wire op. Order is immaterial across tasks — schedule does not
    // write `sort_order`, so the N tasks all land in the target
    // bucket and rank by `COALESCE(sort_order, priority, UNRANKED)`
    // regardless of which one's mutation finishes first.
    for (const task of movedTasks) {
      const key = todoKey(task.path, task.repo);
      this._setSlotState({
        kind: "pending",
        action: "schedule",
        startedAt,
        path: task.path,
        repo: task.repo ?? undefined,
        taskTldr: task.tldr,
        targetEffectiveDate: targetDate,
      });
      this._clearRowError(key);
      if (key !== primaryKey) {
        this._armDebounce(task.path, task.repo ?? undefined);
      }
      this._sendTodoSchedule(task.path, task.repo, targetDate);
    }

    // Multi-select schedule consumes the selection, mirroring
    // `_handleMultiReorder`.
    if (target.selectedKeys !== null) {
      this.selectedTasks = new Map();
    }
  }

  /** User dragged a task to a new position. Optimistic local reorder + WS mutation. */
  private _handleTodoReorder(target: ReorderTarget): void {
    if (target.selectedKeys && target.selectedKeys.length > 1) {
      this._handleMultiReorder(target);
      return;
    }

    // §3.5: queued-refresh short-circuit.
    if (this.todoRefreshQueued) return;
    // §6: gate duplicate sends at the entry point, same shape as
    // `_handleTodoDone` / `_handleTodoSnooze`.
    if (this._armDebounce(target.path, target.repo)) return;

    const key = todoKey(target.path, target.repo);

    // Optimistic update: reorder the local task array.
    const tasks = [...this.todoTasks];
    const srcIdx = tasks.findIndex(
      (t) => todoKey(t.path, t.repo) === key,
    );
    if (srcIdx === -1) {
      // Task not in live list — dispatch-time race (TodoState
      // refresh removed it between render and drop). Warn rather
      // than silently dropping so the race is visible in devtools.
      console.warn(
        `_handleTodoReorder: dragged task ${key} not found in todoTasks; dropping dispatch`,
      );
      return;
    }

    // Freeze BEFORE the optimistic splice. The snapshot must reflect
    // the pre-drag order: the user sees "reordering…" labels on rows
    // in their old positions, and the new order snaps into place on
    // thaw. Idempotent, so no-op if a prior done/snooze already froze
    // the list.
    this._freezeListIfNeeded();

    // Stash pre-splice state for snap-back on error (PRD-snooze §2).
    // Store the reference to the current arrays — no clone needed because:
    //  • the next line assigns a fresh array to this.todoTasks (tasks.splice +
    //    reassign), leaving the original reference immutable for snap-back.
    //  • this.selectedTasks is always replaced (new Map(…)) at every write
    //    site (lines 1028/1786/2092/2097/2173/2235/2941/3173) — never mutated
    //    in place — so the reference captured here remains stable.
    this.todoReorderSnapshots.set(key, {
      tasks: this.todoTasks,
      selectedTasks: this.selectedTasks,
    });

    // Remove from current position and shallow-clone the task so we don't
    // mutate the object still referenced in the previous todoTasks array.
    const movedTask = { ...tasks.splice(srcIdx, 1)[0] };
    this._applyCrossGroupDate(movedTask, target.targetGroupDate);

    const insertIdx = this._resolveInsertionIndex(tasks, target.after, target.before);
    if (insertIdx === null) {
      // Neither `after` nor `before` anchor was provided — the
      // caller (drag code in `todo-list.ts`) always supplies at
      // least one. Warn and drop rather than silently no-oping.
      console.warn(
        `_handleTodoReorder: no anchor supplied for key=${key}; dropping dispatch`,
      );
      this.todoReorderSnapshots.delete(key);
      return;
    }

    tasks.splice(insertIdx, 0, movedTask);
    this.todoTasks = tasks;

    this._cancelIdleTimer();
    this._setSlotState({
      kind: "pending",
      action: "reorder",
      startedAt: Date.now(),
      path: target.path,
      repo: target.repo,
      taskTldr: movedTask.tldr,
    });
    // Design-fidelity §7.2: any row-level error badge is stale the
    // moment the user retries — match done/snooze behavior by clearing
    // it on dispatch rather than waiting for the success ack.
    this._clearRowError(todoKey(target.path, target.repo));
    this._sendTodoReorder(target.path, target.repo, target.after, target.before);
  }

  /**
   * Apply the optimistic `effective_date` update for a cross-group drag.
   *
   * Dropping into a concrete-dated section (TODAY/TOMORROW/WEEKDAY/FUTURE)
   * updates the task's `effective_date` to match so grouping renders
   * correctly before the server confirms. The `targetGroupDate === null`
   * branch is defense-in-depth — under §4 of the today-earlier-bucket
   * design, drops into pseudo-buckets (OVERDUE / EARLIER / DUE_TODAY,
   * all of which have `canonicalDate: null`) are filtered out at the
   * hit-test level. If one slips through, leave `effective_date` alone.
   * The backend recomputes the authoritative `effective_date` on next
   * query.
   */
  private _applyCrossGroupDate(
    task: TodoItem,
    targetGroupDate: string | null,
  ): void {
    if (targetGroupDate !== null && targetGroupDate !== task.effective_date) {
      task.effective_date = targetGroupDate;
    }
  }

  /**
   * Multi-select reorder: move all selected tasks as a group to the drop position.
   * Each task gets a separate TodoReorder message with chained anchors to preserve
   * relative order.
   */
  private _handleMultiReorder(target: ReorderTarget): void {
    // §3.5: queued-refresh short-circuit.
    if (this.todoRefreshQueued) return;
    // §6: gate on the primary task's key — if any of the selected
    // tasks is still inside its 400ms window, short-circuit the
    // whole batch so the operation is atomic (we don't want half a
    // multi-select going through).
    if (this._armDebounce(target.path, target.repo)) return;

    const selectedKeys = target.selectedKeys!;
    const tasksByKey = this._buildTaskIndex();

    // Resolve selected keys to tasks in display order.
    const movedTasks: TodoItem[] = [];
    for (const key of selectedKeys) {
      const task = tasksByKey.get(key);
      if (task) movedTasks.push({ ...task });
    }
    if (movedTasks.length === 0) {
      console.warn(
        "_handleMultiReorder: no selected tasks resolved; dropping dispatch",
      );
      return;
    }

    // Freeze BEFORE the optimistic splice so the snapshot captures
    // the pre-drag order. Idempotent wrt a prior triage-session
    // freeze. New order appears only on thaw.
    this._freezeListIfNeeded();

    // Stash pre-splice state for snap-back on error (PRD-snooze §2).
    // Store the snapshot under EACH task's key (not just the primary) so that
    // _handleTodoActionResult can find it regardless of which task's ack arrives
    // first or fails first. All N keys point at the same snapshot object; the
    // first snap-back that runs restores state, and subsequent deletes on other
    // keys are no-ops against a map that already had the entry removed.
    const primaryKey = todoKey(target.path, target.repo);
    const snap = {
      tasks: this.todoTasks,
      selectedTasks: this.selectedTasks,
    };
    // selectedKeys includes primaryKey; add any task in selectedKeys not yet mapped.
    for (const k of selectedKeys) {
      this.todoReorderSnapshots.set(k, snap);
    }
    // Belt-and-suspenders: always include primaryKey even if not in selectedKeys.
    this.todoReorderSnapshots.set(primaryKey, snap);

    // Optimistic update: extract all selected tasks, insert at drop position.
    const selectedSet = new Set(selectedKeys);
    const tasks = this.todoTasks.filter(
      (t) => !selectedSet.has(todoKey(t.path, t.repo)),
    );

    for (const task of movedTasks) {
      this._applyCrossGroupDate(task, target.targetGroupDate);
    }

    const insertIdx = this._resolveInsertionIndex(tasks, target.after, target.before);
    if (insertIdx === null) {
      console.warn(
        "_handleMultiReorder: no anchor supplied; dropping dispatch",
      );
      // Clear all per-task snapshot keys we just installed.
      for (const k of selectedKeys) {
        this.todoReorderSnapshots.delete(k);
      }
      this.todoReorderSnapshots.delete(primaryKey);
      return;
    }

    tasks.splice(insertIdx, 0, ...movedTasks);
    this.todoTasks = tasks;

    // Mark all as pending. Cancel the list-level idle timer once —
    // this whole batch is a single "new action". The N
    // `requestUpdate()` calls from `_setSlotState` are coalesced to
    // one render by Lit's `isUpdatePending` guard
    // (@lit/reactive-element's `requestUpdate` short-circuits when an
    // update is already queued), so no manual batching is needed.
    this._cancelIdleTimer();
    const startedAt = Date.now();
    for (const task of movedTasks) {
      const key = todoKey(task.path, task.repo);
      this._setSlotState({
        kind: "pending",
        action: "reorder",
        startedAt,
        path: task.path,
        repo: task.repo ?? undefined,
        taskTldr: task.tldr,
      });
      // Design-fidelity §7.2: match done/snooze dispatch behavior —
      // clear any existing row-error badge on retry.
      this._clearRowError(key);
      // Arm per-task timer too so a subsequent done/snooze on any
      // selected row is still gated (primary key was armed at entry).
      if (key !== todoKey(target.path, target.repo)) {
        this._armDebounce(task.path, task.repo ?? undefined);
      }
    }

    // Send chained TodoReorder messages. Each task after the first uses the
    // previous task as its --after anchor, preserving relative order. All
    // tasks share the same `before` anchor (target.before) — intermediate
    // tasks can't reference later tasks whose sort_order hasn't been written yet.
    for (let i = 0; i < movedTasks.length; i++) {
      const task = movedTasks[i];
      const after = i === 0
        ? target.after
        : { path: movedTasks[i - 1].path, repo: movedTasks[i - 1].repo };
      this._sendTodoReorder(task.path, task.repo, after, target.before);
    }

    this.selectedTasks = new Map();
  }

  /** How long we'll wait for a TodoState response before clearing the
   * refreshing indicator anyway. Generous — the list fetch is usually sub-
   * second, but graf can be slow to start. Purely a UX safety net; the
   * refresh itself keeps going on the backend. */
  private static readonly TODO_REFRESH_TIMEOUT_MS = 10_000;

  /** Per-pending-slot watchdog threshold (design §3.4.4). Longer than
   *  any reasonable server round-trip, short enough that a stuck row
   *  recovers within ~a minute. */
  private static readonly TODO_SLOT_WATCHDOG_MS = 30_000;

  /** List-level idle timer for settled-slot dismissal (design §3.5).
   *  Long enough for the user to read the tile; short enough that they
   *  don't have to click `×` on every one after a triage session. */
  private static readonly TODO_IDLE_DISMISS_MS = 5_000;

  /** User clicked the refresh button in the todo header.
   *
   *  §3.5: serialize behind in-flight mutations. Drop all settled
   *  tiles up front (the user has asked for a fresh view — the tiles'
   *  purpose is served). If any slot is still pending, set
   *  `todoRefreshQueued` and defer the TodoRefresh send until every
   *  pending slot settles (or is dropped by watchdog / error). */
  private _handleTodoRefresh(): void {
    // Rapid double-click: ignore a second click while a refresh is in
    // flight or queued. The first click's state transition still stands.
    if (this.todoRefreshPending || this.todoRefreshQueued) return;

    // Drop all settled slots immediately — and cancel the list-level
    // idle timer (only meaningful while settled slots exist).
    const toCollapse: string[] = [];
    for (const [key, state] of this.todoSlotState) {
      if (state.kind === "settled") toCollapse.push(key);
    }
    for (const key of toCollapse) this._collapseSettledSlot(key);

    if (this._hasPendingSlots()) {
      this.todoRefreshQueued = true;
      this.requestUpdate();
      return;
    }

    this._fireRefreshNow();
  }

  /** Actually send the TodoRefresh + arm the UX-safety-net timeout.
   *  Extracted so `_handleTodoRefresh` and `_maybeFireQueuedRefresh`
   *  share the same code path. */
  private _fireRefreshNow(): void {
    this.todoRefreshQueued = false;
    this.todoRefreshPending = true;
    this.ws.send({ type: "TodoRefresh" });
    // No prior timer possible — `todoRefreshPending` gates entry.
    this._todoRefreshTimeoutId = setTimeout(() => {
      this._clearTodoRefreshPending();
    }, BrennApp.TODO_REFRESH_TIMEOUT_MS);
    this.requestUpdate();
  }

  /** User tapped "Tasks" button (SinglePane only). */
  private _handleTodoToggle(): void {
    this.slots = slotReset(this.slots, 0, {
      current: { type: "TodoList" },
      parent: { type: "Chat" },
    });
    this._pushPaneState("TodoList");
  }

  /** Switch the secondary slot between TodoList and FilePicker/FileViewer. */
  private _handleSecondaryTabSwitch(
    target: "TodoList" | "FilePicker",
  ): void {
    if (target === "TodoList" && slotType(this.slots, 1) !== "TodoList") {
      // Switching to todo list — clear file viewer if it was showing.
      if (slotType(this.slots, 1) === "FileViewer") {
        this.fileViewer?.clear();
      }
      this.slots = slotReset(this.slots, 1, {
        current: { type: "TodoList" },
        parent: null,
      });
    } else if (
      target === "FilePicker" &&
      slotType(this.slots, 1) !== "FilePicker" &&
      slotType(this.slots, 1) !== "FileViewer"
    ) {
      this.slots = slotReset(this.slots, 1, {
        current: { type: "FilePicker" },
        parent: null,
      });
    }
  }

  /** Render the todo list component with the given visibility and collapse handler. */
  private _renderTodoList(visible: boolean, onCollapse: () => void) {
    return html`
      <brenn-todo-list
        .visible=${visible}
        .tasks=${this.todoTasks}
        .slotState=${this.todoSlotState}
        .frozenSnapshot=${this.todoFrozenSnapshot}
        .errorKeys=${this.todoErrorKeys}
        .refreshPending=${this.todoRefreshPending || this.todoRefreshQueued}
        .interactionsDisabled=${this.todoRefreshQueued}
        .todayStr=${this.todoTodayStr}
        .selectedKeys=${new Set(this.selectedTasks.keys())}
        .onSelectionChange=${(keys: Set<string>) => this._handleSelectionChange(keys)}
        .onDone=${(path: string, repo?: string) =>
          this._handleTodoDone(path, repo)}
        .onSnooze=${(
          path: string,
          repo: string | undefined,
          effectiveDate: string,
          days: number,
        ) => this._handleTodoSnooze(path, repo, effectiveDate, days)}
        .onReorder=${(target: ReorderTarget) =>
          this._handleTodoReorder(target)}
        .onSchedule=${(target: ScheduleTarget) =>
          this._handleTodoSchedule(target)}
        .onRefresh=${() => this._handleTodoRefresh()}
        .onCollapse=${onCollapse}
        .onDismissSettled=${(key: string) => this._handleDismissSettled(key)}
      ></brenn-todo-list>
    `;
  }

  /** Focus the main input bar after a short delay (allows DOM to settle). */
  private focusInput(): void {
    requestAnimationFrame(() => {
      this.inputBar?.focus();
    });
  }

  /**
   * Render the pane layout — single template literal across both layouts so
   * Lit reuses `<brenn-pane-layout>`, the slot-0 wrapper, and
   * `<brenn-message-list>` across a layout swap. The subtrees that genuinely
   * differ (single's primary-slot overlays vs two-column's slot-1 content)
   * are gated by `isSingle ? … : nothing`. The message-list element survives
   * the swap, so a cross-breakpoint resize does NOT need a history replay
   * round-trip — see `_handleSetLayout`.
   *
   * Invariant: at most one `<brenn-file-viewer>` and at most one
   * `<brenn-file-picker>` exist in the rendered tree at any moment. The
   * other half of each conditional renders `nothing`.
   * `@query("brenn-file-viewer")` / `@query("brenn-file-picker")` in this
   * component return the first match and depend on this invariant. Future
   * edits to this template must preserve it.
   */
  private _renderPaneLayout() {
    const isSingle = this.currentLayout === "SinglePane";
    const primMode = slotType(this.slots, 0);
    const secMode = slotType(this.slots, 1);
    const showTabs = this.hasTodoList && this.artifactIndex.length > 0;
    return html`
      <brenn-pane-layout
        .layout=${this.currentLayout}
        .secondaryVisible=${!isSingle && this.secondarySlotVisible}
        .splitRatio=${this.paneSplitRatio}
        @split-ratio-changed=${this._handleSplitRatioChanged}
      >
        <div slot="slot-0" class="mobile-slot-content">
          <brenn-message-list
            .visible=${!isSingle || primMode === "Chat"}
            .appSlug=${this.appSlug}
          >
            <brenn-approval-container
              .visible=${this.approvalVisible}
            ></brenn-approval-container>
          </brenn-message-list>
          ${isSingle
            ? html`
                <brenn-file-picker
                  .visible=${primMode === "FilePicker"}
                  .files=${this.artifactIndex}
                  .onFileSelect=${(filePath: string, latestMessageId: number) =>
                    this.handleFilePickerSelect(filePath, latestMessageId)}
                  .onCollapse=${() => this.handleRightPaneCollapse()}
                ></brenn-file-picker>
                <brenn-file-viewer
                  .visible=${primMode === "FileViewer"}
                  .onBack=${() => this.handleFileViewerBack()}
                  .onVersionSelect=${(messageId: number) =>
                    this.handleVersionSelect(messageId)}
                ></brenn-file-viewer>
                ${this._renderTodoList(primMode === "TodoList", () => {
                  history.back();
                })}
              `
            : nothing}
        </div>
        ${isSingle
          ? nothing
          : html`
              <div slot="slot-1" class="secondary-content">
                ${showTabs
                  ? html`
                      <div class="secondary-tabs">
                        <button
                          class="secondary-tab"
                          aria-selected=${secMode === "TodoList"
                            ? "true"
                            : "false"}
                          @click=${() =>
                            this._handleSecondaryTabSwitch("TodoList")}
                        >
                          Tasks
                        </button>
                        <button
                          class="secondary-tab"
                          aria-selected=${secMode === "FilePicker" ||
                          secMode === "FileViewer"
                            ? "true"
                            : "false"}
                          @click=${() =>
                            this._handleSecondaryTabSwitch("FilePicker")}
                        >
                          Files
                        </button>
                      </div>
                    `
                  : nothing}
                ${this._renderTodoList(secMode === "TodoList", () =>
                  this.handleRightPaneCollapse(),
                )}
                <brenn-file-picker
                  .visible=${secMode === "FilePicker"}
                  .files=${this.artifactIndex}
                  .onFileSelect=${(filePath: string, latestMessageId: number) =>
                    this.handleFilePickerSelect(filePath, latestMessageId)}
                  .onCollapse=${() => this.handleRightPaneCollapse()}
                ></brenn-file-picker>
                <brenn-file-viewer
                  .visible=${secMode === "FileViewer"}
                  .onBack=${() => this.handleFileViewerBack()}
                  .onVersionSelect=${(messageId: number) =>
                    this.handleVersionSelect(messageId)}
                ></brenn-file-viewer>
              </div>
            `}
      </brenn-pane-layout>
    `;
  }

  /** Handle SetLayout from the backend. */
  private async _handleSetLayout(layout: PaneLayout): Promise<void> {
    const newType = layout.type;
    // Initial-path interaction: `currentLayout` defaults to TwoColumn
    // (line ~135). On first connect with `layoutReady=false`, the
    // `&& this.layoutReady` clause keeps this guard from firing even
    // when the server's first SetLayout matches the default — we still
    // need to flip the gate and flush the queue. Only mid-session
    // identity SetLayouts hit this and short-circuit.
    if (newType === this.currentLayout && this.layoutReady) {
      return; // No change.
    }

    const wasReady = this.layoutReady;
    this.currentLayout = newType;
    this.layoutReady = true;

    // Reset slots to the appropriate bundle for the new layout.
    if (newType === "SinglePane") {
      this.slots = copyBundleSlots(CHAT_ONLY);
    } else {
      this.slots = copyBundleSlots(CHAT_AND_FILE);
      // If artifacts exist, show the secondary pane.
      if (this.artifactIndex.length > 0) {
        this.secondarySlotVisible = true;
      }
    }

    if (!wasReady) {
      // Initial path. Await the child's first commit so `@query` resolves,
      // then drain anything that was queued under the render gate.
      await this.updateComplete;
      this._flushPendingReplay();
      this.messageList?.scrollToBottomNow();
      return;
    }
    // Mid-session cross-breakpoint resize: the unified `_renderPaneLayout`
    // template keeps `<brenn-message-list>` stable across the swap, so the
    // reactive `currentLayout` / `slots` updates above are all the work.
  }

  /** User tapped "Files" button (SinglePane only). */
  private _handleFilesToggle(): void {
    this.slots = slotReset(this.slots, 0, {
      current: { type: "FilePicker" },
      parent: { type: "Chat" },
    });
    this._pushPaneState("FilePicker");
  }

  /** Check IndexedDB for pending shares stashed by the service worker. */
  private async checkPendingShares(): Promise<void> {
    try {
      const db = await openShareDb();
      const tx = db.transaction("pending", "readwrite");
      const store = tx.objectStore("pending");

      const all: ShareData[] = await new Promise((resolve, reject) => {
        const req = store.getAll();
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
      });

      for (const share of all) {
        if (share.file) {
          const file = new File([share.file.data], share.file.name, {
            type: share.file.type,
          });
          this.inputBar?.uploadExternalFile(file);
        }
        const textParts = [share.title, share.text, share.url].filter(Boolean);
        if (textParts.length > 0) {
          this.inputBar?.prefillText(textParts.join("\n"));
        }
        store.delete(share.id);
      }

      db.close();
    } catch (e) {
      // IndexedDB unavailable (e.g., private browsing in some browsers).
      // Share target won't work, but the app is otherwise fine.
      console.warn("Failed to check pending shares:", e);
    }
  }

  /** Handle split ratio changes from the pane layout resize handle. */
  private _handleSplitRatioChanged(
    e: CustomEvent<{ ratio: number }>,
  ): void {
    this.paneSplitRatio = e.detail.ratio;
    this.settings.paneSplitRatio = e.detail.ratio;
  }

  /** Logout form submit: clean up signed_in_user_ids before navigating away. */
  private _handleLogout(): void {
    if (this.currentUserId !== 0) {
      removeSignedInUserId(this.currentUserId).catch((err: unknown) =>
        reportClientError(`removeSignedInUserId failed: ${String(err)}`)
      );
    }
  }

  /** User clicked "Enable push on this device". */
  private async _handleEnablePush(): Promise<void> {
    if (this.pushPending) return;
    this.pushPending = true;
    try {
      const result = await enablePush(this.ws);
      if (!result.ok) {
        if (result.reason === "permission_denied") {
          this.messageList?.appendError(
            "Push notification permission was denied. Enable notifications in browser settings to use push.",
          );
        } else if (result.reason === "not_supported") {
          this.messageList?.appendError(
            "Push notifications are not supported in this browser.",
          );
        } else {
          this.messageList?.appendError(
            `Failed to enable push notifications: ${result.detail ?? "unknown error"}`,
          );
        }
      }
      // pushSubscribed is updated by the PushEnabled WS response from the backend.
    } finally {
      this.pushPending = false;
    }
  }

  /** User clicked "Disable push on this device". */
  private async _handleDisablePush(): Promise<void> {
    if (this.pushPending) return;
    this.pushPending = true;
    try {
      await disablePush(this.ws);
      // pushSubscribed is updated by the PushEnabled WS response from the backend.
    } finally {
      this.pushPending = false;
    }
  }

  /**
   * Capture a debug viewport/layout snapshot and send it to the backend over WS.
   * Invoked from the "Debug UI" user menu item.
   *
   * The entire body is wrapped in try/catch — a capture error must never crash
   * the app (AC7, Constraints). On error: failure toast + console.error.
   */
  private _captureDebugSnapshot(): void {
    try {
      // --- helper: getBoundingClientRect or null ---
      const rect = (el: Element | null): RectData | null => {
        if (!el) return null;
        const r = el.getBoundingClientRect();
        return { top: r.top, left: r.left, right: r.right, bottom: r.bottom, width: r.width, height: r.height };
      };

      // --- element lookups ---
      const inputEl = document.querySelector<HTMLElement>("#input");
      const inputBarEl = document.querySelector<HTMLElement>("brenn-input-bar");
      const appMainEl = document.querySelector<HTMLElement>(".app-main");
      const paneLayoutEl = document.querySelector<HTMLElement>("brenn-pane-layout");
      const messageListEl = document.querySelector<HTMLElement>("brenn-message-list");
      const attachmentStripEl = document.querySelector<HTMLElement>(".attachment-strip");
      const chipBarEl = document.querySelector<HTMLElement>(".chip-bar");
      const presenceBarEl = document.querySelector<HTMLElement>(".presence-bar");
      const stealBarEl = document.querySelector<HTMLElement>(".steal-bar");
      const statusBarEl = document.querySelector<HTMLElement>("brenn-status-bar");
      // Elements for the internal-header band geometry check.
      const appTopbarEl = document.querySelector<HTMLElement>(".app-topbar");
      const appHeaderEl = document.querySelector<HTMLElement>(".app-header");
      const appLayoutEl = document.querySelector<HTMLElement>(".app-layout");
      // null in TwoColumn path and before layoutReady.
      const mobileSlotContentEl = document.querySelector<HTMLElement>(".mobile-slot-content");

      // --- visual viewport ---
      const vv = window.visualViewport ?? null;
      const visualViewport: VisualViewportData | null = vv
        ? { width: vv.width, height: vv.height, offset_top: vv.offsetTop, offset_left: vv.offsetLeft, page_top: vv.pageTop, page_left: vv.pageLeft, scale: vv.scale }
        : null;

      // --- element rects ---
      const inputRect = rect(inputEl);
      const messageListScrollTop = messageListEl?.scrollTop ?? null;
      const messageListScrollHeight = messageListEl?.scrollHeight ?? null;
      const messageListClientHeight = messageListEl?.clientHeight ?? null;

      // --- derived booleans (AC6) ---
      const inputBottomBelowVisualFold: boolean | null =
        inputRect !== null && visualViewport !== null
          ? inputRect.bottom > (visualViewport.offset_top + visualViewport.height)
          : null;
      const inputBottomBelowLayout: boolean | null =
        inputRect !== null
          ? inputRect.bottom > window.innerHeight
          : null;

      // --- computed styles ---
      const cs = (el: Element | null, prop: string): string | null => {
        if (!el) return null;
        try { return getComputedStyle(el).getPropertyValue(prop).trim() || null; } catch (err) { console.warn("BrennApp: getComputedStyle failed", err); return null; }
      };

      const htmlEl = document.documentElement;
      const bodyEl = document.body;

      // --- transient-DOM-probe helper ---
      // Creates a zero-width, visibility:hidden div with the given cssText, appends it
      // to documentElement (so body overflow/transform cannot affect the containing
      // block), measures its getBoundingClientRect().height, and removes it in finally.
      // Returns null on exception; the caller handles the unsupported-unit check before
      // calling so an unsupported unit is never passed here.
      const measureCssHeight = (cssText: string): number | null => {
        const el = document.createElement("div");
        try {
          el.style.cssText = cssText;
          document.documentElement.appendChild(el);
          return el.getBoundingClientRect().height;
        } catch (err) {
          return null;
        } finally {
          el.remove();
        }
      };

      // --- safe-area probe ---
      // Appended to documentElement (not body) so body overflow/transform cannot
      // affect the containing block for position:absolute probes.
      let safeTop: string | null = null;
      let safeRight: string | null = null;
      let safeBottom: string | null = null;
      let safeLeft: string | null = null;
      const probe = document.createElement("div");
      try {
        probe.style.cssText = "position:absolute;top:-9999px;left:-9999px;padding:env(safe-area-inset-top) env(safe-area-inset-right) env(safe-area-inset-bottom) env(safe-area-inset-left);pointer-events:none;visibility:hidden";
        document.documentElement.appendChild(probe);
        const pcs = getComputedStyle(probe);
        safeTop = pcs.paddingTop || null;
        safeRight = pcs.paddingRight || null;
        safeBottom = pcs.paddingBottom || null;
        safeLeft = pcs.paddingLeft || null;
      } catch (err) { console.warn("BrennApp: safe-area probe failed", err); /* safeTop/Right/Bottom/Left remain null */ }
      finally { probe.remove(); }

      // --- viewport-unit probes ---
      // Each probe is a transient absolutely-positioned, zero-width,
      // visibility:hidden element sized with a single unit. Inserted and removed
      // synchronously so it cannot perturb the geometry being measured.
      // Appended to documentElement so body overflow/transform cannot change the
      // containing block for position:absolute (quality-3).
      // `null` when CSS.supports reports the unit is unsupported; this is distinct
      // from an exception (see probe_exception_units below).
      let probe100vhPx: number | null = null;
      let probe100svhPx: number | null = null;
      let probe100lvhPx: number | null = null;
      let probe100dvhPx: number | null = null;
      const probeExceptionUnits: string[] = [];
      const vpUnits: string[] = ["100vh", "100svh", "100lvh", "100dvh"];
      for (const unit of vpUnits) {
        // Leave variable null (unsupported) rather than assigning 0, which is
        // indistinguishable from a real zero-height measurement (correctness-1).
        if (typeof CSS !== "undefined" && !CSS.supports("height", unit)) continue;
        const cssText = `position:absolute;top:-9999px;left:-9999px;width:0;height:${unit};visibility:hidden;pointer-events:none`;
        const h = measureCssHeight(cssText);
        if (h === null) {
          console.warn(`BrennApp: viewport-unit probe (${unit}) threw`);
          probeExceptionUnits.push(unit); // surfaced in snapshot for backend triage
        } else {
          if (unit === "100vh")  probe100vhPx  = h;
          else if (unit === "100svh") probe100svhPx = h;
          else if (unit === "100lvh") probe100lvhPx = h;
          else if (unit === "100dvh") probe100dvhPx = h;
        }
      }

      // --- UA data ---
      const nav = navigator as Navigator & { userAgentData?: { brands?: Array<{ brand: string; version: string }>; mobile?: boolean } };
      const uaBrands: string[] | null = nav.userAgentData?.brands
        ? nav.userAgentData.brands.map((b) => `${b.brand}/${b.version}`)
        : null;
      const uaMobile: boolean | null = nav.userAgentData?.mobile ?? null;

      // --- active element ---
      const activeEl = document.activeElement;
      const activeElementTag: string | null = activeEl ? activeEl.tagName.toLowerCase() : null;
      const activeElementId: string | null = activeEl?.id || null;

      // --- scrolling element ---
      const scrollEl = document.scrollingElement;
      const scrollingElementScrollTop: number | null = scrollEl?.scrollTop ?? null;
      const scrollingElementScrollLeft: number | null = scrollEl?.scrollLeft ?? null;

      // --- assemble snapshot ---
      const snapshot: DebugViewportSnapshotData = {
        inner_width: window.innerWidth,
        inner_height: window.innerHeight,
        document_element_client_width: htmlEl.clientWidth,
        document_element_client_height: htmlEl.clientHeight,
        document_element_scroll_height: htmlEl.scrollHeight,
        scroll_x: window.scrollX,
        scroll_y: window.scrollY,
        scrolling_element_scroll_top: scrollingElementScrollTop,
        scrolling_element_scroll_left: scrollingElementScrollLeft,
        device_pixel_ratio: window.devicePixelRatio,
        screen_width: screen.width,
        screen_height: screen.height,
        screen_orientation_type: screen.orientation?.type ?? null,
        display_mode_standalone: window.matchMedia("(display-mode: standalone)").matches,
        max_width_768: window.matchMedia("(max-width: 768px)").matches,
        visual_viewport: visualViewport,
        input: inputRect,
        input_bar: rect(inputBarEl),
        app_main: rect(appMainEl),
        pane_layout: rect(paneLayoutEl),
        message_list: rect(messageListEl),
        attachment_strip: rect(attachmentStripEl),
        chip_bar: rect(chipBarEl),
        presence_bar: rect(presenceBarEl),
        steal_bar: rect(stealBarEl),
        status_bar: rect(statusBarEl),
        body: rect(bodyEl),
        document_element: rect(htmlEl),
        message_list_scroll_top: messageListScrollTop,
        message_list_scroll_height: messageListScrollHeight,
        message_list_client_height: messageListClientHeight,
        input_bottom_below_visual_fold: inputBottomBelowVisualFold,
        input_bottom_below_layout: inputBottomBelowLayout,
        html_height: cs(htmlEl, "height"),
        body_height: cs(bodyEl, "height"),
        body_overflow: cs(bodyEl, "overflow"),
        input_bar_position: cs(inputBarEl, "position"),
        input_bar_flex_shrink: cs(inputBarEl, "flex-shrink"),
        app_main_min_height: cs(appMainEl, "min-height"),
        // computed styles — mid-chain flex nodes.
        pane_layout_min_height: cs(paneLayoutEl, "min-height"),
        pane_layout_height: cs(paneLayoutEl, "height"),
        message_list_min_height: cs(messageListEl, "min-height"),
        message_list_height: cs(messageListEl, "height"),
        mobile_slot_content_min_height: cs(mobileSlotContentEl, "min-height"),
        app_main_height: cs(appMainEl, "height"),
        // bounding rects — internal header elements.
        app_topbar: rect(appTopbarEl),
        app_header: rect(appHeaderEl),
        app_layout: rect(appLayoutEl),
        // root scalar.
        document_element_offset_height: htmlEl.offsetHeight,
        safe_area_inset_top: safeTop,
        safe_area_inset_right: safeRight,
        safe_area_inset_bottom: safeBottom,
        safe_area_inset_left: safeLeft,
        // viewport-unit probes: document which CSS unit produces which px value.
        probe_100vh_px: probe100vhPx,
        probe_100svh_px: probe100svhPx,
        probe_100lvh_px: probe100lvhPx,
        probe_100dvh_px: probe100dvhPx,
        // units that threw during probing (distinct from unsupported = null via CSS.supports).
        probe_exception_units: probeExceptionUnits.length > 0 ? probeExceptionUnits : null,
        // window-bounds scalars to pin the dvh-vs-innerHeight mechanism.
        screen_avail_height: screen.availHeight,
        window_outer_height: window.outerHeight,
        user_agent: navigator.userAgent,
        ua_brands: uaBrands,
        ua_mobile: uaMobile,
        active_element_tag: activeElementTag,
        active_element_id: activeElementId,
        visibility_state: document.visibilityState,
        client_timestamp: new Date().toISOString(),
        build_id: BUILD_ID,
      };

      // --- send or show failure toast ---
      // Use trySend() (returns bool) rather than isConnected()+send() to close
      // the TOCTOU window: if the socket closes between check and send, the
      // boolean result is authoritative and no double-toast can occur.
      const msg: WsClientMessage = { type: "DebugViewportSnapshot", snapshot };
      if (this.ws?.trySend(msg)) {
        this.toastHost?.push({ text: "Debug snapshot sent", ttlMs: 3000 });
      } else {
        this.toastHost?.push({ text: "Debug capture failed: not connected", ttlMs: 4000 });
      }
    } catch (err) {
      reportClientError(`BrennApp: _captureDebugSnapshot failed: ${String(err)}`);
      this.toastHost?.push({ text: "Debug capture failed", ttlMs: 4000 });
    }
  }

  /** Read CSRF token from <meta name="csrf-token"> for the logout form. */
  private _getCsrfToken(): string {
    const meta = document.querySelector<HTMLMetaElement>('meta[name="csrf-token"]');
    return meta?.content ?? "";
  }

  /** Render the user dropdown menu (username trigger + push toggle + logout). */
  private _renderUserMenu() {
    if (!this.currentUsername) return nothing;
    const csrfToken = this._getCsrfToken();
    const pushItem = this.pwaPushEnabled
      ? this.pushPending
        ? html`<button class="user-menu-item" disabled>Push…</button>`
        : this.pushSubscribed
          ? html`<button
              class="user-menu-item"
              @click=${() => { this.userMenuOpen = false; void this._handleDisablePush(); }}
            >Disable push on this device</button>`
          : html`<button
              class="user-menu-item"
              @click=${() => { this.userMenuOpen = false; void this._handleEnablePush(); }}
            >Enable push on this device</button>`
      : nothing;
    return html`
      <div class="user-menu-wrapper">
        <button
          class="user-menu-trigger"
          @click=${() => { this.userMenuOpen = !this.userMenuOpen; }}
          aria-haspopup="true"
          aria-expanded=${this.userMenuOpen}
        >${this.currentUsername} ▾</button>
        ${this.userMenuOpen ? html`
          <div class="user-menu-dropdown">
            ${pushItem}
            <button class="user-menu-item"
              @click=${() => { this.userMenuOpen = false; this._captureDebugSnapshot(); }}
            >Debug UI</button>
            <form method="post" action="/logout" class="logout-form"
              @submit=${() => { this._handleLogout(); }}>
              <input type="hidden" name="csrf_token" .value=${csrfToken}>
              <button type="submit" class="user-menu-item">Log out</button>
            </form>
          </div>` : nothing}
      </div>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "brenn-app": BrennApp;
  }
}
