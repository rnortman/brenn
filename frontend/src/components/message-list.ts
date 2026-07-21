/**
 * <brenn-message-list> — Scrollable message display with smart scroll.
 *
 * Uses shadow DOM for style encapsulation — markdown-rendered HTML (headings,
 * code blocks, tables, etc.) is scoped here and can't leak into the rest of the UI.
 *
 * Uses imperative methods for message appending (accumulation pattern,
 * not reactive re-render). Manages streaming state internally.
 */

import { LitElement, html, css } from "lit";
import { customElement, property, query } from "lit/decorators.js";
import { markdownStyles } from "../styles/markdown.js";
import type { AttachmentMeta } from "../generated/AttachmentMeta.js";
import type { SelectedTask } from "../generated/SelectedTask.js";
import type { HistoryPageMessage } from "../generated/HistoryPageMessage.js";
import type { SystemMessageCategory } from "../generated/SystemMessageCategory.js";

/**
 * Typed batch item for `BrennMessageList.bulkAppend`. The wire-format
 * dispatch lives in `<brenn-app>`'s `_flushPendingReplay`, where it
 * mirrors the per-message translation that `handleMessage` already does
 * (wire frames → presentation-component method calls). This type keeps
 * the message-list component free of `WsServerMessage` knowledge.
 */
/**
 * Input to `appendUserMessage` / `_buildUserMessageEl` and the `kind:"user"`
 * arm of `MessageBatchItem`. Carries only chat-input-origin fields — system
 * messages use `SystemMessageInput` instead.
 */
export interface UserMessageInput {
  text: string;
  username: string;
  timestamp: string;
  isSelf: boolean;
  attachments: AttachmentMeta[];
  selectedTasks: SelectedTask[];
}

/**
 * Input to `appendSystemMessage` / `_buildSystemCard` and the `kind:"system"`
 * arm of `MessageBatchItem`. Only the fields actually consumed by the render
 * method are included — `timestamp` and `seq` are tracked at the wire level
 * in `app.ts` before dispatch and are not needed by the renderer itself.
 */
export interface SystemMessageInput {
  renderedHtml: string;
  category: SystemMessageCategory;
}

export type MessageBatchItem =
  | { kind: "assistant"; content: string }
  | ({ kind: "user" } & UserMessageInput)
  | ({ kind: "system" } & SystemMessageInput)
  | { kind: "toolUse"; toolName: string; renderedSummary: string; detailHtml: string | null }
  | {
      kind: "targetResult";
      target: string;
      success: boolean;
      summary: string;
      files: string[];
      detail: string | null;
    }
  | { kind: "error"; message: string }
  | { kind: "streamToken"; token: string }
  | { kind: "thinkingToken"; token: string };

@customElement("brenn-message-list")
export class BrennMessageList extends LitElement {
  // Shadow DOM (Lit default) — styles are encapsulated.

  @property({ type: Boolean, reflect: true }) visible = true;
  /** App slug for building attachment URLs. */
  @property({ type: String }) appSlug = "";

  static styles = [
    markdownStyles,
    css`
      /* === Scroll viewport === */

      .scroll-outer {
        flex: 1;
        overflow-y: auto;
        overflow-x: hidden;
        min-height: 0;

        scrollbar-color: #2a2a40 transparent;
        scrollbar-width: thin;
      }

      /* === Message content container === */

      .message-scroll {
        padding: 1rem;
        display: flex;
        flex-direction: column;
        gap: 0.25rem;
      }

      :host {
        display: flex;
        flex-direction: column;
        flex: 1;
        min-height: 0;
        min-width: 0;
      }

      :host(:not([visible])) {
        display: none !important;
      }

      /* === Base message styles === */

      .msg {
        padding: 0.375rem 0.75rem;
        max-width: 80ch;
        min-width: 0;
        white-space: pre-wrap;
        word-wrap: break-word;
        overflow-wrap: break-word;
        line-height: 1.6;
        font-size: 1rem;
        color: #d0d0d8;
      }

      /* User messages: plain text, subtle left border. */
      .msg-user {
        color: #c0c0d0;
        border-left: 3px solid #4a6fa5;
        background: #1e1e35;
        padding-left: 0.75rem;
        margin-top: 0.75rem;
      }

      /* Other users' messages: different accent color. */
      .msg-user-other {
        border-left-color: #7a5fa5;
      }

      /* System-origin chat cards (collapsed by default via <details>).
         Default: no colored frame — visually like a tool-use details block. */
      details.brenn-system {
        background: transparent;
        padding: 0.4rem 0.75rem;
        margin-top: 0.5rem;
        border-radius: 4px;
        /* No border-left at all in the default case. */
      }

      details.brenn-system > summary {
        cursor: pointer;
        list-style: none;
        color: #7a7a90;
        font-size: 0.85rem;
      }
      details.brenn-system > summary::before { content: "▶ "; font-size: 0.75rem; }
      details.brenn-system[open] > summary::before { content: "▼ "; }

      /* R7: UI-error and graf-error exception — red border + expanded by default.
         Both categories share the same visual treatment (prominently flagged errors)
         but remain semantically distinct: brenn-system-ui-error is a user-attempted
         UI tool call; brenn-system-graf-error is a graf subprocess query failure.
         The selector targets the inner-class form (set by wrap_system_details
         inside the rendered HTML) rather than the outer wrapper class. */
      details.brenn-system.brenn-system-ui-error,
      details.brenn-system.brenn-system-graf-error {
        border-left: 3px solid #e94560;
        background: #2d1a24;
        padding: 0.5rem 0.75rem;
      }

      /* Inner per-message sub-cards inside a brenn-system-body, OR inside a
         tool-use-item (the brenn-message-sent case).
         Lightweight separator only — not a full-strength frame. */
      details.brenn-message {
        background: rgba(255, 255, 255, 0.02);
        padding: 0.3rem 0.5rem;
        margin: 0.3rem 0;
        border-radius: 3px;
        border-left: 2px solid #4a4a60;
      }
      details.brenn-message > summary {
        cursor: pointer;
        list-style: none;
        color: #8888a0;
        font-size: 0.85rem;
      }
      details.brenn-message > summary::before { content: "▶ "; font-size: 0.75rem; }
      details.brenn-message[open] > summary::before { content: "▼ "; }

      .brenn-system-body,
      .brenn-msg-body {
        margin-top: 0.4rem;
      }
      .brenn-msg-from,
      .brenn-msg-sender,
      .brenn-msg-wake {
        margin-right: 0.5rem;
      }
      .brenn-msg-status-ok {
        color: #a3d9a5;
      }
      .brenn-msg-status-err {
        color: #ff8a9e;
      }
      .brenn-msg-status-pending {
        color: #f0c674;
      }
      .brenn-event-list,
      .brenn-idle-hook-repos {
        margin: 0.25rem 0;
        padding-left: 1.2rem;
      }

      /* Attribution line (username + timestamp) above message text. */
      .msg-attribution {
        font-size: 0.8rem;
        color: #707088;
        margin-bottom: 2px;
        display: flex;
        gap: 0.4em;
        align-items: baseline;
      }
      .msg-username {
        font-weight: 600;
        color: #9090a8;
      }
      .msg-timestamp {
        color: #585870;
      }

      /* Assistant messages: rendered HTML, normal whitespace.
         Also carries .md-content for shared markdown styles. */
      .msg-assistant {
        white-space: normal;
      }

      /* Message-list heading sizes are slightly smaller than full-document contexts. */
      .msg-assistant h1 {
        font-size: 1.2rem;
      }
      .msg-assistant h2 {
        font-size: 1.1rem;
      }
      .msg-assistant h3 {
        font-size: 1rem;
      }

      /* Streaming: subtle indicator that content is still arriving. */
      .msg-streaming {
        border-left: 2px solid #e94560;
        padding-left: 0.75rem;
        opacity: 0.9;
      }

      /* Thinking blocks during streaming: muted, italic. */
      .msg-thinking {
        color: #707088;
        font-style: italic;
        font-size: 0.9rem;
        border-left: 2px solid #3a3a50;
        padding-left: 0.5rem;
        margin: 0.25rem 0;
        white-space: pre-wrap;
      }

      /* Error messages */
      .msg-error {
        color: #ff8a9e;
        background: #2d1a24;
        border-left: 3px solid #e94560;
        padding-left: 0.75rem;
        font-size: 0.9rem;
      }

      /* Informational notices (e.g., AppBusy) */
      .msg-notice {
        color: #f0c674;
        background: #2d2a1a;
        border-left: 3px solid #e0a830;
        padding-left: 0.75rem;
        font-size: 0.9rem;
      }

      /* Target handler results (e.g. import results) */
      .msg-target-result {
        padding: 0.75rem;
        border-radius: 6px;
        font-size: 0.9rem;
      }
      .msg-target-result.target-success {
        color: #a3d9a5;
        background: #1a2d1e;
        border-left: 3px solid #4caf50;
      }
      .msg-target-result.target-failure {
        color: #ff8a9e;
        background: #2d1a24;
        border-left: 3px solid #e94560;
      }
      .target-result-header {
        font-weight: 600;
        margin-bottom: 0.25rem;
        font-size: 0.85rem;
        opacity: 0.8;
      }
      .target-result-body {
        white-space: pre-wrap;
      }
      /* Expandable detail in target results (uses <details>/<summary>) */
      details.msg-target-result > summary {
        cursor: pointer;
        list-style: none;
      }
      details.msg-target-result > summary::before {
        content: "▶ ";
        font-size: 0.75rem;
      }
      details.msg-target-result[open] > summary::before {
        content: "▼ ";
      }
      .target-result-detail {
        margin-top: 0.5rem;
        padding: 0.5rem;
        background: rgba(0, 0, 0, 0.2);
        border-radius: 4px;
        font-size: 0.8rem;
        white-space: pre-wrap;
        word-break: break-word;
        max-height: 400px;
        overflow-y: auto;
      }

      /* === Tool-use summaries === */

      .tool-use-group {
        font-size: 0.85rem;
        color: #707088;
        margin: 0.125rem 0;
        flex-shrink: 0;
        min-width: 0;
        overflow: hidden;
      }

      .tool-use-group summary {
        cursor: pointer;
        user-select: none;
        padding: 0.125rem 0.25rem;
        list-style: none;
        font-family: "JetBrains Mono", "Fira Code", "Cascadia Code", monospace;
      }

      .tool-use-group summary::before {
        content: "▸ ";
      }

      .tool-use-group[open] summary::before {
        content: "▾ ";
      }

      .tool-use-group summary:hover {
        color: #a0a0b8;
      }

      .tool-use-item {
        padding: 0.125rem 0.25rem 0.125rem 1.25rem;
        font-family: "JetBrains Mono", "Fira Code", "Cascadia Code", monospace;
        font-size: 0.8rem;
        color: #606078;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        max-width: 100%;
        flex-shrink: 0;
      }

      /* Single tool-use summary (not in a group yet) */
      .tool-use-single {
        font-size: 0.85rem;
        color: #707088;
        padding: 0.125rem 0.25rem;
        font-family: "JetBrains Mono", "Fira Code", "Cascadia Code", monospace;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        max-width: 100%;
        margin: 0.125rem 0;
        flex-shrink: 0;
      }

      /* Shared styles for tool-summary inner elements (items and singles). */
      .tool-use-item .ts-label,
      .tool-use-single .ts-label {
        color: #585870;
        min-width: 4ch;
        display: inline-block;
      }

      .tool-use-item .ts-file,
      .tool-use-single .ts-file,
      .tool-use-item .ts-cmd,
      .tool-use-single .ts-cmd,
      .tool-use-item .ts-pattern,
      .tool-use-single .ts-pattern {
        color: #8888a0;
      }

      .tool-use-item .ts-artifact,
      .tool-use-single .ts-artifact {
        cursor: pointer;
      }

      .tool-use-item .ts-artifact:hover,
      .tool-use-single .ts-artifact:hover {
        color: #b0b0c8;
        text-decoration: underline;
      }

      .tool-use-item .ts-denied,
      .tool-use-single .ts-denied {
        color: #e94560;
      }

      .tool-use-item .ts-answer,
      .tool-use-single .ts-answer {
        color: #6aaa6a;
      }

      .tool-use-item .ts-denied-note,
      .tool-use-single .ts-denied-note {
        color: #e94560;
        font-style: italic;
      }

      .tool-use-item .ts-question,
      .tool-use-single .ts-question {
        margin-bottom: 0.125rem;
      }

      /* Expandable tool-use items (details/summary) */
      details.tool-use-item {
        padding: 0;
      }

      details.tool-use-item > summary {
        padding: 0.125rem 0.25rem;
        cursor: pointer;
        user-select: none;
        list-style: none;
      }

      details.tool-use-item > summary::-webkit-details-marker {
        display: none;
      }

      details.tool-use-item > summary::before {
        content: "▸ ";
        color: #585870;
      }

      details.tool-use-item[open] > summary::before {
        content: "▾ ";
        color: #585870;
      }

      details.tool-use-item > summary:hover {
        color: #a0a0b8;
      }

      /* Detail body (expanded view) */
      .td-body {
        padding: 0.25rem 0.5rem 0.5rem 1.25rem;
        font-size: 0.8rem;
        border-left: 2px solid #2a2a40;
        margin-left: 0.5rem;
        margin-top: 0.125rem;
      }

      .td-approval {
        margin-bottom: 0.5rem;
        color: #8888a0;
      }

      .td-auto {
        color: #6a9a6a;
      }

      .td-manual {
        color: #9a9a6a;
      }

      .td-denied {
        color: #e94560;
      }

      .td-approval code {
        background: #1a1a2e;
        padding: 0.1rem 0.3rem;
        border-radius: 2px;
        font-size: 0.75rem;
      }

      .td-heading {
        color: #585870;
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 0.25rem;
      }

      .td-section {
        margin-bottom: 0.5rem;
      }

      .td-section:last-child {
        margin-bottom: 0;
      }

      .td-json {
        background: #0d0d1a;
        border: 1px solid #1a1a2e;
        padding: 0.375rem 0.5rem;
        font-family: "JetBrains Mono", "Fira Code", "Cascadia Code", monospace;
        font-size: 0.75rem;
        color: #8888a0;
        overflow-x: auto;
        white-space: pre-wrap;
        word-wrap: break-word;
        max-height: 20rem;
        overflow-y: auto;
      }

      /* === Attachments === */

      .msg-attachments {
        display: flex;
        flex-wrap: wrap;
        gap: 0.5rem;
        margin-top: 0.4rem;
      }

      .msg-attachment-thumb-link {
        display: block;
      }

      .msg-attachment-thumb {
        max-width: 200px;
        max-height: 150px;
        border-radius: 4px;
        border: 1px solid #3a3a50;
      }

      .msg-attachment-chip {
        display: inline-block;
        padding: 0.25rem 0.5rem;
        background: #1a1a30;
        border: 1px solid #3a3a50;
        border-radius: 4px;
        color: #a0a0b8;
        font-size: 0.85rem;
        text-decoration: none;
      }
      .msg-attachment-chip:hover {
        background: #2a2a40;
        color: #d0d0d8;
      }

      /* === Selected task chips (in message echo) === */

      .msg-selected-tasks {
        display: flex;
        flex-wrap: wrap;
        gap: 0.25rem;
        margin-bottom: 0.3rem;
      }

      .msg-task-chip {
        display: inline-flex;
        align-items: center;
        background: #1e2a3e;
        border: 1px solid #2a3a50;
        border-radius: 3px;
        padding: 0.1rem 0.35rem;
        font-size: 0.75rem;
        color: #8090a8;
        max-width: 200px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
      }

      /* === Load-more sentinel (backward pagination) === */

      .load-more-sentinel {
        display: flex;
        justify-content: center;
        padding: 0.5rem;
        color: #585870;
        font-size: 0.85rem;
      }

      .load-more-sentinel.hidden {
        display: none;
      }

      .history-seam {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        padding: 0.75rem 1rem;
        color: #585870;
        font-size: 0.8rem;
        font-style: italic;
      }

      .history-seam::before,
      .history-seam::after {
        content: '';
        flex: 1;
        height: 1px;
        background: #3a3a50;
      }

      .load-more-spinner {
        width: 16px;
        height: 16px;
        border: 2px solid #3a3a50;
        border-top-color: #707088;
        border-radius: 50%;
        animation: spin 0.8s linear infinite;
      }

      @keyframes spin {
        to { transform: rotate(360deg); }
      }

      /* === Mobile === */

      @media (max-width: 600px) {
        .message-scroll {
          padding: 0.5rem;
        }

        .msg {
          max-width: 100%;
        }

        .msg-attribution {
          font-size: 0.9rem;
        }

        .msg-attachment-thumb {
          max-width: 150px;
          max-height: 100px;
        }
      }
    `,
  ];

  @query(".scroll-outer") private scrollEl!: HTMLElement;
  @query(".message-scroll") private container!: HTMLElement;

  /** Current streaming element (accumulates tokens during CC response). */
  private streamingEl: HTMLElement | null = null;

  /** Current thinking element within the streaming area. */
  private thinkingEl: HTMLElement | null = null;

  /** Whether we're currently accumulating thinking tokens. */
  private inThinking = false;

  /** Whether user is scrolled near the bottom. */
  private isAtBottom = true;

  /** Suspends per-append scroll work during `bulkAppend`. When set, the
   *  `maybeScroll` helper is a no-op — the caller issues one
   *  `scrollToBottomNow` after the whole batch commits. Also skips the
   *  layout-forcing scroll-geometry read each append would otherwise do. */
  private _suspendAutoScroll = false;

  /** Threshold in px for "at bottom" detection. */
  private static readonly SCROLL_THRESHOLD = 50;

  protected firstUpdated(): void {
    this.scrollEl.addEventListener("scroll", () => {
      this.updateScrollPosition();
    });
    // Delegate clicks on artifact re-open elements.
    this.scrollEl.addEventListener("click", (e) => {
      const target = (e.target as HTMLElement).closest("[data-artifact-path]");
      if (target) {
        const path = (target as HTMLElement).dataset.artifactPath;
        if (path) {
          this.dispatchEvent(
            new CustomEvent("artifact-reopen", {
              detail: { filePath: path },
              bubbles: true,
              composed: true,
            }),
          );
        }
      }
    });
  }

  /** Sentinel element at the top of the message list for backward pagination. */
  private sentinelEl: HTMLElement | null = null;
  /** IntersectionObserver watching the sentinel. */
  private sentinelObserver: IntersectionObserver | null = null;
  /** Divider between simplified (backward-paginated) and full-fidelity messages. */
  private seamIndicatorEl: HTMLElement | null = null;

  render() {
    return html`<div class="scroll-outer">
      <div class="message-scroll"></div>
      <slot></slot>
    </div>`;
  }

  override disconnectedCallback(): void {
    // Explicitly disconnect the IntersectionObserver so it doesn't hold a
    // reference to the detached element after a layout swap. GC would reclaim
    // it eventually, but the explicit disconnect makes the contract clear and
    // is belt-and-suspenders for any future code that might retain a reference.
    this.sentinelObserver?.disconnect();
    this.sentinelObserver = null;
    // Also null sentinelEl so showLoadMoreSentinel() recreates the full
    // observer+element pair on reconnect rather than silently re-showing
    // a sentinel with no observer attached (breaking infinite scroll).
    this.sentinelEl = null;
    super.disconnectedCallback();
  }

  // --- Public API ---

  appendUserMessage(input: UserMessageInput): void {
    this.container.appendChild(this._buildUserMessageEl(input));
    // Always scroll on user action — they want to see the response.
    this.scrollToBottom();
  }

  /** Append a system-origin message card (collapsed details block). */
  appendSystemMessage(input: SystemMessageInput): void {
    // Clean up any in-flight streaming element before inserting the system
    // card — mirrors the pattern in appendAssistantMessage. Idempotent when
    // no stream is in progress. Keeps DOM order correct if a system broadcast
    // races with a live assistant stream.
    this.cleanupStreaming();
    this.container.appendChild(this._buildSystemCard(input));
    this.maybeScroll();
  }

  appendAssistantMessage(content: string): void {
    // Clean up streaming state if present (idempotent).
    this.cleanupStreaming();
    this.container.appendChild(this._buildAssistantMessageEl(content));
    this.maybeScroll();
  }

  appendError(message: string): void {
    this.container.appendChild(this._buildErrorEl(message));
    this.maybeScroll();
  }

  /** Append an informational notice (non-error, e.g., AppBusy). */
  appendNotice(message: string): void {
    const el = document.createElement("div");
    el.className = "msg msg-notice";
    el.textContent = message;
    this.container.appendChild(el);
    this.maybeScroll();
  }

  /** Append a target handler result (e.g. import result). */
  appendTargetResult(
    target: string,
    success: boolean,
    summary: string,
    files: string[],
    detail: string | null,
  ): void {
    this.container.appendChild(
      this._buildTargetResultEl(target, success, summary, files, detail),
    );
    this.maybeScroll();
  }

  /** Append a compact tool-use summary. Consecutive summaries auto-collapse. */
  appendToolUseSummary(
    toolName: string,
    renderedSummary: string,
    detailHtml: string | null,
  ): void {
    const item = this.buildToolItem(toolName, renderedSummary, detailHtml);
    this._appendToolUseItem(this.container, this.container.lastElementChild, item);
    this.maybeScroll();
  }

  /** Build a tool-use item element, optionally expandable with detail. */
  private buildToolItem(
    toolName: string,
    renderedSummary: string,
    detailHtml: string | null,
  ): HTMLElement {
    if (detailHtml) {
      const details = document.createElement("details");
      details.className = "tool-use-item";
      const summary = document.createElement("summary");
      summary.innerHTML = `<span class="ts-label">${this.escapeHtml(toolName)}</span> ${renderedSummary}`;
      details.appendChild(summary);
      const body = document.createElement("div");
      body.className = "td-body";
      body.innerHTML = detailHtml;
      details.appendChild(body);
      return details;
    }
    const div = document.createElement("div");
    div.className = "tool-use-item";
    div.innerHTML = `<span class="ts-label">${this.escapeHtml(toolName)}</span> ${renderedSummary}`;
    return div;
  }

  appendStreamToken(token: string): void {
    if (this.inThinking) {
      // Switched from thinking to text — close thinking block.
      this.inThinking = false;
      this.thinkingEl = null;
    }
    this.ensureStreamingEl();
    // Append as text node — O(n) per token, not O(n²).
    this.streamingEl!.appendChild(document.createTextNode(token));
    this.maybeScroll();
  }

  appendThinkingToken(token: string): void {
    this.ensureStreamingEl();
    if (!this.thinkingEl) {
      this.thinkingEl = document.createElement("div");
      this.thinkingEl.className = "msg-thinking";
      this.streamingEl!.appendChild(this.thinkingEl);
      this.inThinking = true;
    }
    this.thinkingEl.appendChild(document.createTextNode(token));
    this.maybeScroll();
  }

  /** Prepend a batch of simplified messages from backward pagination. */
  prependMessages(messages: HistoryPageMessage[]): void {
    if (messages.length === 0) return;

    const scrollHeight = this.scrollEl.scrollHeight;
    const scrollTop = this.scrollEl.scrollTop;

    // On first backward pagination, insert a seam indicator between
    // simplified and full-fidelity messages.
    if (!this.seamIndicatorEl) {
      this.seamIndicatorEl = document.createElement("div");
      this.seamIndicatorEl.className = "history-seam";
      this.seamIndicatorEl.textContent =
        "Older messages shown in simplified view";
      const firstFullFidelity = this.sentinelEl
        ? this.sentinelEl.nextSibling
        : this.container.firstChild;
      this.container.insertBefore(this.seamIndicatorEl, firstFullFidelity);
    }

    // Must follow seam-indicator insertion — correct ordering depends on
    // the seam being in the DOM before we read sentinel.nextSibling.
    const insertionPoint = this.sentinelEl
      ? this.sentinelEl.nextSibling
      : this.container.firstChild;

    for (const msg of messages) {
      let el: HTMLElement;
      if (msg.category) {
        // System-origin row — route through the same builder the live path uses.
        el = this._buildSystemCard({
          renderedHtml: msg.rendered_html,
          category: msg.category,
        });
      } else if (msg.role === "assistant") {
        el = document.createElement("div");
        el.className = "msg msg-assistant md-content";
        el.innerHTML = msg.rendered_html;
      } else {
        el = document.createElement("div");
        el.className = "msg msg-user";
        el.innerHTML = msg.rendered_html;
        this._renderAttachments(el, msg.attachments ?? []);
      }
      this.container.insertBefore(el, insertionPoint);
    }

    // Preserve scroll position — content was added above the viewport.
    this.scrollEl.scrollTop =
      scrollTop + (this.scrollEl.scrollHeight - scrollHeight);
  }

  /** Show the "load more" sentinel at the top of the message list. */
  showLoadMoreSentinel(): void {
    if (!this.sentinelEl) {
      this.sentinelEl = document.createElement("div");
      this.sentinelEl.className = "load-more-sentinel";
      this.sentinelEl.innerHTML = '<div class="load-more-spinner"></div>';
      this.container.prepend(this.sentinelEl);

      // Set up IntersectionObserver.
      this.sentinelObserver = new IntersectionObserver(
        (entries) => {
          for (const entry of entries) {
            if (entry.isIntersecting) {
              this.dispatchEvent(
                new CustomEvent("load-more", { bubbles: true, composed: true }),
              );
            }
          }
        },
        { root: this.scrollEl, threshold: 0.1 },
      );
      this.sentinelObserver.observe(this.sentinelEl);
    } else {
      // Already exists — just make visible and show spinner.
      this.sentinelEl.classList.remove("hidden");
      this.sentinelEl.innerHTML = '<div class="load-more-spinner"></div>';
    }
  }

  /** Hide the sentinel (no more history to load). */
  hideLoadMoreSentinel(): void {
    if (this.sentinelEl) {
      this.sentinelEl.classList.add("hidden");
    }
  }

  /**
   * Append a batch of pre-translated message items in one pass. Used by the
   * render gate in `<brenn-app>` once the first `SetLayout` has committed:
   * any frames buffered under the gate are built into a single
   * DocumentFragment here, then attached to `this.container` in a single
   * appendChild — one layout, one paint for the whole history batch
   * (addresses the mobile-history-replay-too-expensive flicker ticket).
   *
   * The wire-format dispatch lives in `BrennApp._flushPendingReplay`, which
   * translates `WsServerMessage` variants into `MessageBatchItem`s — same
   * pattern as `handleMessage` translating singletons into `appendXxx`
   * calls. Keeps this component free of `WsServerMessage` knowledge.
   *
   * Streaming tokens in a replay batch are degenerate (CC sends
   * AssistantMessage in history, not per-token streams) — they fall back
   * to the live path so the streaming-element invariant is preserved.
   */
  bulkAppend(items: MessageBatchItem[]): void {
    if (items.length === 0) return;

    // Match `appendAssistantMessage`: close any in-progress streaming
    // element so the batch doesn't land after a half-open stream.
    this.cleanupStreaming();

    const frag = document.createDocumentFragment();
    // Tool-use grouping reads the "last appended" node to decide
    // single→group promotion. During a batch build, the fragment has no
    // layout and `this.container` hasn't seen our new nodes yet, so we
    // track the last appended element on a local variable.
    let lastInFrag: Element | null = this.container.lastElementChild;

    for (const item of items) {
      switch (item.kind) {
        case "streamToken":
        case "thinkingToken":
          // Degenerate in replay; delegate to the live path. The
          // streaming element becomes a child of `this.container`, NOT
          // the fragment — the fragment is attached below, so late
          // streaming state stays coherent with the live-stream path.
          this._suspendAutoScroll = true;
          try {
            if (item.kind === "streamToken") this.appendStreamToken(item.token);
            else this.appendThinkingToken(item.token);
          } finally {
            this._suspendAutoScroll = false;
          }
          lastInFrag = this.container.lastElementChild;
          break;
        case "assistant": {
          const el = this._buildAssistantMessageEl(item.content);
          frag.appendChild(el);
          lastInFrag = el;
          break;
        }
        case "user": {
          const el = this._buildUserMessageEl(item);
          frag.appendChild(el);
          lastInFrag = el;
          break;
        }
        case "system": {
          const el = this._buildSystemCard(item);
          frag.appendChild(el);
          lastInFrag = el;
          break;
        }
        case "toolUse": {
          const el = this.buildToolItem(item.toolName, item.renderedSummary, item.detailHtml);
          lastInFrag = this._appendToolUseItem(frag, lastInFrag, el);
          break;
        }
        case "targetResult": {
          const el = this._buildTargetResultEl(
            item.target,
            item.success,
            item.summary,
            item.files,
            item.detail,
          );
          frag.appendChild(el);
          lastInFrag = el;
          break;
        }
        case "error": {
          const el = this._buildErrorEl(item.message);
          frag.appendChild(el);
          lastInFrag = el;
          break;
        }
        default: {
          // Exhaustiveness check — adding a new MessageBatchItem variant
          // without updating this switch is a compile error.
          const _exhaustive: never = item;
          throw new Error(
            `bulkAppend: unsupported batch item ${(_exhaustive as { kind: string }).kind}`,
          );
        }
      }
    }

    // Single DOM op — browser does one reflow / one paint.
    this.container.appendChild(frag);
  }

  // --- Pure builders used by bulkAppend (no this.container writes). ---

  private _buildAssistantMessageEl(content: string): HTMLElement {
    const el = this.createMessageEl("assistant");
    el.innerHTML = content;
    return el;
  }

  private _buildErrorEl(message: string): HTMLElement {
    const el = document.createElement("div");
    el.className = "msg msg-error";
    el.textContent = `Error: ${message}`;
    return el;
  }

  /** Build the outer wrapper for a system-origin message card.
   *
   * The wrapper carries no `.msg-user` class (which would apply the blue
   * left border). The system-card styling comes from `details.brenn-system`
   * rules inside the rendered HTML plus the per-category marker class on
   * the wrapper. */
  private _buildSystemCard(input: SystemMessageInput): HTMLElement {
    const card = document.createElement("div");
    const klass = categoryClassMap[input.category];
    card.className = `msg msg-system msg-system-${klass}`;
    card.innerHTML = input.renderedHtml;
    return card;
  }

  private _buildUserMessageEl(input: UserMessageInput): HTMLElement {
    const {
      text,
      username,
      timestamp,
      isSelf,
      attachments,
      selectedTasks,
    } = input;

    // Human-authored messages: flat-bubble path.
    const el = this.createMessageEl("user");
    if (!isSelf) {
      el.classList.add("msg-user-other");
    }

    const attr = document.createElement("div");
    attr.className = "msg-attribution";

    if (!isSelf && username) {
      const nameSpan = document.createElement("span");
      nameSpan.className = "msg-username";
      nameSpan.textContent = username;
      attr.appendChild(nameSpan);
    }

    const timeSpan = document.createElement("span");
    timeSpan.className = "msg-timestamp";
    const date = new Date(timestamp);
    timeSpan.textContent = date.toLocaleTimeString(undefined, {
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    });
    timeSpan.title = date.toLocaleString();
    attr.appendChild(timeSpan);
    el.appendChild(attr);

    if (selectedTasks.length > 0) {
      const chipsEl = document.createElement("div");
      chipsEl.className = "msg-selected-tasks";
      for (const task of selectedTasks) {
        // Wire `SelectedTask` is `{ref}` only; no tldr available in echo.
        // Display the ref itself — this path only runs for live echoes
        // (history replay emits empty selected_tasks), so users see the
        // very ref they just selected.
        const chip = document.createElement("span");
        chip.className = "msg-task-chip";
        chip.textContent = task.ref;
        chip.title = task.ref;
        chipsEl.appendChild(chip);
      }
      el.appendChild(chipsEl);
    }

    if (text) {
      const textEl = document.createElement("div");
      textEl.className = "msg-text";
      textEl.textContent = text;
      el.appendChild(textEl);
    }

    this._renderAttachments(el, attachments);

    return el;
  }

  /** Render attachment thumbnails/chips into `container`. Shared by
   * `_buildUserMessageEl` (live path) and `prependMessages` (history path). */
  private _renderAttachments(
    container: HTMLElement,
    attachments: AttachmentMeta[],
  ): void {
    if (attachments.length === 0) return;

    const attachEl = document.createElement("div");
    attachEl.className = "msg-attachments";
    for (const att of attachments) {
      if (att.media_type.startsWith("image/")) {
        const link = document.createElement("a");
        link.href = `/app/${this.appSlug}/attachment/${att.upload_id}/${encodeURIComponent(att.filename)}`;
        link.target = "_blank";
        link.className = "msg-attachment-thumb-link";
        const img = document.createElement("img");
        img.className = "msg-attachment-thumb";
        img.src = link.href;
        img.alt = att.filename;
        img.loading = "lazy";
        link.appendChild(img);
        attachEl.appendChild(link);
      } else {
        const chip = document.createElement("a");
        chip.href = `/app/${this.appSlug}/attachment/${att.upload_id}/${encodeURIComponent(att.filename)}`;
        chip.className = "msg-attachment-chip";
        chip.textContent = `\u{1F4C4} ${att.filename} (${humanFileSize(att.size)})`;
        chip.target = "_blank";
        attachEl.appendChild(chip);
      }
    }
    container.appendChild(attachEl);
  }

  private _buildTargetResultEl(
    target: string,
    success: boolean,
    summary: string,
    files: string[],
    detail: string | null,
  ): HTMLElement {
    const statusClass = success ? "target-success" : "target-failure";
    const headerText = files.length > 0 ? files.join(", ") : target;

    if (detail) {
      const firstLine = summary.split("\n")[0];
      const details = document.createElement("details");
      details.className = `msg msg-target-result ${statusClass}`;

      const summaryEl = document.createElement("summary");
      summaryEl.className = "target-result-header";
      summaryEl.textContent = `${headerText} — ${firstLine}`;
      details.appendChild(summaryEl);

      const summaryBody = document.createElement("div");
      summaryBody.className = "target-result-body";
      summaryBody.textContent = summary;
      details.appendChild(summaryBody);

      const detailBody = document.createElement("pre");
      detailBody.className = "target-result-detail";
      detailBody.textContent = detail;
      details.appendChild(detailBody);
      return details;
    }
    const el = document.createElement("div");
    el.className = `msg msg-target-result ${statusClass}`;
    const header = document.createElement("div");
    header.className = "target-result-header";
    header.textContent = headerText;
    el.appendChild(header);
    const body = document.createElement("div");
    body.className = "target-result-body";
    body.textContent = summary;
    el.appendChild(body);
    return el;
  }

  /**
   * Tool-use grouping logic, parameterized on parent + last-appended element
   * so it works for both the live append path (parent = `this.container`,
   * lastChild = `this.container.lastElementChild`) and the batched fragment
   * path (parent = a `DocumentFragment`, lastChild may live in the fragment
   * OR be a pre-existing container child for cross-boundary promotion).
   *
   * Returns the new "last element" — the live caller can ignore the return
   * since it reads from `this.container.lastElementChild` on the next call;
   * the batch caller threads it through a local variable.
   */
  private _appendToolUseItem(
    parent: ParentNode,
    lastChild: Element | null,
    item: HTMLElement,
  ): Element {
    if (lastChild?.classList.contains("tool-use-group")) {
      // Already a group — append and update count.
      const summary = lastChild.querySelector(":scope > summary");
      lastChild.appendChild(item);
      if (summary) {
        const count = lastChild.querySelectorAll(":scope > .tool-use-item").length;
        summary.textContent = `${count} tool uses`;
      }
      return lastChild;
    }
    if (lastChild?.classList.contains("tool-use-single")) {
      // Convert single → group.
      const details = document.createElement("details");
      details.className = "tool-use-group";
      const summaryEl = document.createElement("summary");
      summaryEl.textContent = "2 tool uses";
      details.appendChild(summaryEl);

      // Move the existing single's child item into the group.
      // The single wrapper has exactly one child: a tool-use-item element.
      const prevItem = lastChild.firstElementChild;
      if (prevItem) {
        details.appendChild(prevItem);
      }
      details.appendChild(item);

      // Swap the single for the new group wherever the single lives — in
      // the fragment, or in the container (when the tail element is
      // pre-existing from a prior batch/live append).
      const singleParent = lastChild.parentNode;
      if (singleParent) {
        singleParent.replaceChild(details, lastChild);
      } else {
        // Defensive: orphaned single with no parent is a caller-bug,
        // but fail loud rather than silently drop the item.
        throw new Error(
          "appendToolUseItem: tool-use-single had no parent during group promotion",
        );
      }
      return details;
    }
    // First (standalone) summary — render as single, append into parent.
    const single = document.createElement("div");
    single.className = "tool-use-single";
    single.appendChild(item);
    parent.appendChild(single);
    return single;
  }

  clear(): void {
    this.cleanupStreaming();
    // Clean up sentinel and observer.
    if (this.sentinelObserver) {
      this.sentinelObserver.disconnect();
      this.sentinelObserver = null;
    }
    this.sentinelEl = null;
    this.seamIndicatorEl = null;
    // `container` is @query(".message-scroll") on this element's own
    // shadow root. If the element hasn't rendered yet (e.g. just
    // mounted by a layout-driven subtree swap), container is undefined
    // and there is no content to clear. No-op in that case.
    if (this.container) {
      this.container.innerHTML = "";
    }
  }

  scrollToBottomNow(): void {
    this.scrollToBottom();
  }

  // --- Private helpers ---

  private createMessageEl(role: "user" | "assistant"): HTMLElement {
    const el = document.createElement("div");
    // Assistant messages get md-content for shared markdown styles.
    el.className =
      role === "assistant" ? "msg msg-assistant md-content" : "msg msg-user";
    return el;
  }

  private ensureStreamingEl(): void {
    if (!this.streamingEl) {
      this.streamingEl = document.createElement("div");
      this.streamingEl.className = "msg msg-assistant md-content msg-streaming";
      this.container.appendChild(this.streamingEl);
    }
  }

  private cleanupStreaming(): void {
    if (this.streamingEl) {
      this.streamingEl.remove();
      this.streamingEl = null;
      this.thinkingEl = null;
      this.inThinking = false;
    }
  }

  private updateScrollPosition(): void {
    const { scrollTop, scrollHeight, clientHeight } = this.scrollEl;
    this.isAtBottom =
      scrollHeight - scrollTop - clientHeight < BrennMessageList.SCROLL_THRESHOLD;
  }

  private maybeScroll(): void {
    if (this._suspendAutoScroll) return;
    if (this.isAtBottom) {
      this.scrollToBottom();
    }
  }

  private scrollToBottom(): void {
    // Use requestAnimationFrame to ensure DOM has updated.
    requestAnimationFrame(() => {
      this.scrollEl.scrollTop = this.scrollEl.scrollHeight;
    });
  }

  /** Minimal HTML escape for tool names (which are short, trusted strings
   *  from the backend, but we escape out of principle). */
  private escapeHtml(s: string): string {
    return s
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }
}

function humanFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

/**
 * Map SystemMessageCategory wire strings to kebab-case CSS class
 * suffixes used on the `msg-system-<klass>` marker class on system
 * cards. The marker class is a hook for tests / future per-category
 * styling; the visible card style is provided by the `.brenn-system`
 * rules above plus per-category classes inside the rendered HTML.
 *
 * Typed as `Record<SystemMessageCategory, string>` so the TS compiler
 * enforces exhaustiveness — adding a variant on the Rust side without
 * an entry here is a compile error, eliminating the silent drift the
 * regex-based version had.
 */
const categoryClassMap: Record<SystemMessageCategory, string> = {
  MessagesReceived: "messages-received",
  EventDrain: "event-drain",
  CompactionReminder: "compaction-reminder",
  CompactionHardTrigger: "compaction-hard-trigger",
  CompactionIdlePrompt: "compaction-idle-prompt",
  IdleHook: "idle-hook",
  CompactionUserRequest: "compaction-user-request",
  UiError: "ui-error",
  DeviceSlugReminder: "device-slug-reminder",
  GrafError: "graf-error",
  CompactionFailed: "compaction-failed",
  DebugSnapshot: "debug-snapshot",
};

declare global {
  interface HTMLElementTagNameMap {
    "brenn-message-list": BrennMessageList;
  }
}
