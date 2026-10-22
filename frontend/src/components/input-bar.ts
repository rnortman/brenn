/**
 * <brenn-input-bar> — Compact auto-resizing message input with send/stop dual-mode.
 *
 * Starts at one line, grows to max-height on input.
 * Enter/Ctrl+Enter/Shift+Enter behavior controlled by `enterSends` property.
 *
 * Button behavior depends on `isWorking` and whether the textarea has text:
 * - Idle + empty: Send arrow (disabled)
 * - Idle + text: Send arrow (enabled)
 * - Working + empty: Stop button (square icon)
 * - Working + text: Send arrow (enabled) — sends as steering message
 *
 * File attachments: supports file picker button, paste-to-attach, and drag-and-drop.
 * Files are uploaded via POST /app/{slug}/upload, tracked as pending attachments,
 * and included in the SendMessage when the user sends.
 *
 * Snippet composer: an inline panel whose text is attached as a text file. It
 * opens from the menu, or pre-filled when a large text paste lands in the main
 * box. Its Secret box, and the "Attach secret file" menu item, mark the
 * upload secret: the server delivers it to the agent only, and the chip shows
 * a lock instead of a preview.
 */

import { LitElement, html, nothing } from "lit";
import { customElement, property, query, state } from "lit/decorators.js";
import type { AttachmentMeta } from "../generated/AttachmentMeta";
import type { AttachmentRef } from "../generated/AttachmentRef";
import type { ModelInfo } from "../generated/ModelInfo";
import type { TargetInfo } from "../generated/TargetInfo";
import type { TargetUploadResponse } from "../generated/TargetUploadResponse";
import type { UploadResponse } from "../generated/UploadResponse";
import { setChatHasText } from "../keyboard-guard.js";
import { maybeResizeImage, type ResizeResult } from "../image-resize.js";
import { describeReason, describeReasonShort, reportClientError } from "../error-reporter.js";
import { MenuController } from "./menu-controller.js";
import { SECRET_ATTACHMENT_TITLE } from "./secret-attachment.js";

/** Max textarea height in px before scrollbar appears. */
const MAX_HEIGHT = 120;

/** A main-box text paste at least this many characters opens the snippet composer. */
export const SNIPPET_OFFER_CHARS = 2000;
/** A main-box text paste at least this many lines opens the snippet composer. */
export const SNIPPET_OFFER_LINES = 20;
/** Name given to a snippet whose name field is left empty. */
const DEFAULT_SNIPPET_NAME = "snippet.txt";

/** A caret range in the main box. */
interface CaretRange {
  start: number;
  end: number;
}

/** One open snippet composer session. */
interface ComposerState {
  /** Current text of the composer textarea. */
  text: string;
  /** Current value of the name field. */
  name: string;
  /** Whether the Secret box is checked. */
  secret: boolean;
  /** Whether the user has unmasked a secret composer's text. */
  show: boolean;
  /**
   * Caret range in the main box recorded when a large paste was diverted into
   * the composer; non-null while "Paste inline" is offered. Any later change
   * to the main box withdraws it, since the range no longer means the same
   * place.
   */
  pasteOffer: CaretRange | null;
}

/** A file being uploaded or ready to attach. */
interface PendingAttachment {
  /** Local key for matching during async updates. */
  localKey: string;
  /** UUID from the upload endpoint. */
  uploadId: string;
  /** Original filename. */
  filename: string;
  /** Validated media type from server. */
  mediaType: string;
  /** File size in bytes. */
  size: number;
  /** Local object URL for image preview (revoked on removal). */
  previewUrl: string | null;
  /** Delivered to the agent only; never previewed, resized or served back. */
  secret: boolean;
  /** Upload state. */
  status: "uploading" | "ready" | "error";
  /** Error message if status is "error". */
  errorMessage?: string;
}

@customElement("brenn-input-bar")
export class BrennInputBar extends LitElement {
  // Light DOM — styled by app.css.
  createRenderRoot(): HTMLElement {
    return this;
  }

  @property({ type: Boolean }) enabled = true;
  /** Whether CC is currently working (Thinking or AwaitingApproval). */
  @property({ type: Boolean }) isWorking = false;
  @property({ type: String }) placeholder = "Message...";
  @property({ type: Boolean }) enterSends = true;
  /** App slug for upload endpoint URL. Set by parent. */
  @property({ type: String }) appSlug = "";
  /** Available models for the picker. Empty = no picker shown. */
  @property({ attribute: false }) availableModels: ModelInfo[] = [];
  /** Currently selected model value. */
  @property({ type: String }) currentModel = "";

  /** Callback invoked with message text, attachment refs, and metadata on submit. Set by parent. */
  onSubmit: ((text: string, attachments: AttachmentRef[], meta: AttachmentMeta[]) => void) | null = null;
  /** Callback invoked when the stop button is clicked. Set by parent. */
  onStop: (() => void) | null = null;
  /** Callback invoked when user toggles enter-sends. Set by parent. */
  onToggleEnterSends: (() => void) | null = null;
  /** Callback invoked when user cycles the model. Set by parent. */
  onModelChange: ((model: string) => void) | null = null;
  /** Callback invoked when the conversations panel should toggle. Set by parent. */
  onOpenConversations: (() => void) | null = null;
  /** Callback invoked to start a new conversation. Set by parent. */
  onNewConversation: (() => void) | null = null;
  /** Whether this app is singleton (hides conversation management in menu). */
  @property({ type: Boolean }) singleton = false;
  /** Callback invoked to navigate home. Set by parent. */
  onNavigateHome: (() => void) | null = null;
  /** App-defined attachment targets (e.g. "Import bank export"). */
  @property({ attribute: false }) attachmentTargets: TargetInfo[] = [];
  /** Callback invoked when target files are uploaded and ready for RunTarget. */
  onTargetUploaded: ((target: string, uploadIds: string[]) => void) | null = null;
  /** Callback invoked when an error occurs (displayed in message list). */
  onError: ((message: string) => void) | null = null;
  /** Callback invoked when an image was resized before upload. Set by parent. */
  onResized: ((text: string) => void) | null = null;
  /** Maximum long-edge pixel cap for image resize. Set by parent from meta tag. */
  @property({ type: Number }) maxLongEdge = 2576;
  /** When true, image uploads are disabled (server config error at startup). */
  @property({ type: Boolean }) imageAttachmentsDisabled = false;

  /** Track whether the textarea has content (for button mode switching). */
  @state() private hasText = false;
  /** Pending file attachments. */
  @state() private pendingAttachments: PendingAttachment[] = [];
  /** Whether the bottom menu popup is open. */
  @state() private menuOpen = false;
  /** The open snippet composer session; null while the panel is closed. */
  @state() private composer: ComposerState | null = null;

  @query("#input") private inputEl!: HTMLTextAreaElement;
  @query("#file-input") private fileInputEl!: HTMLInputElement;
  @query("#camera-input") private cameraInputEl!: HTMLInputElement;
  @query("#target-input") private targetInputEl!: HTMLInputElement;
  @query(".snippet-text") private composerTextEl!: HTMLTextAreaElement | null;

  /** Name of the target being uploaded to (set before opening file picker). */
  private pendingTargetName = "";

  /**
   * Whether the next #file-input `change` uploads secret. Both "Attach file"
   * menu items write it before opening the picker; handleFileSelect resets it.
   */
  private nextFileIsSecret = false;

  private get composerHasText(): boolean {
    return (this.composer?.text.length ?? 0) > 0;
  }

  /** Publish "some chat box holds text" to the keyboard guard. */
  private syncKeyboardGuard(): void {
    setChatHasText(this.hasText || this.composerHasText);
  }

  /** Whether we have content to send (text or attachments). */
  private get hasContent(): boolean {
    return this.hasText || this.pendingAttachments.some(a => a.status === "ready");
  }

  /** Shared menu lifecycle: outside-click → close. */
  private _menuController = new MenuController(
    (e) => {
      const target = e.target as Element | null;
      return (
        target?.closest(".menu-btn") !== null ||
        target?.closest(".bottom-menu-popup") !== null
      );
    },
    () => {
      this.menuOpen = false;
      this._menuController.close();
    },
    { eventType: "click" },
  );

  /** Escape closes the bottom menu. Stored so we can remove it on disconnect. */
  private _boundMenuEscapeHandler: ((e: KeyboardEvent) => void) | null = null;

  connectedCallback(): void {
    super.connectedCallback();
    this._boundMenuEscapeHandler = (e: KeyboardEvent) => {
      if (e.key === "Escape" && this.menuOpen) {
        this.menuOpen = false;
        this._menuController.close();
      }
    };
    document.addEventListener("keydown", this._boundMenuEscapeHandler);
    // Re-arm the controller if the component is reconnected while the menu
    // was open (edge case: element moved in DOM). In practice the menu is
    // always closed on disconnect, but this is belt-and-suspenders.
    if (this.menuOpen) this._menuController.open();
  }

  disconnectedCallback(): void {
    super.disconnectedCallback();
    // Always close on disconnect to prevent document-listener leaks.
    this._menuController.close();
    this.menuOpen = false;
    if (this._boundMenuEscapeHandler) {
      document.removeEventListener("keydown", this._boundMenuEscapeHandler);
      this._boundMenuEscapeHandler = null;
    }
  }

  protected firstUpdated(): void {
    // Auto-focus on initial render.
    this.inputEl?.focus();
    // Sync initial text state to keyboard guard (covers restored drafts).
    this.syncKeyboardGuard();
  }

  protected updated(changed: Map<string, unknown>): void {
    // Re-focus when the input becomes enabled (e.g., CC finishes thinking).
    // Disabled textareas can't hold focus, so the browser blurs them.
    if (changed.has("enabled") && this.enabled) {
      requestAnimationFrame(() => this.inputEl?.focus());
    }
  }

  render() {
    const hint = this.enterSends
      ? "Enter sends \u00b7 Shift+Enter for newline"
      : "Enter for newline \u00b7 Ctrl/Cmd+Enter sends";

    // Button mode: show stop (■) when working and input is empty;
    // otherwise show send arrow.
    const showStop = this.isWorking && !this.hasContent;
    const btnDisabled = !this.enabled || (!showStop && !this.hasContent);
    const btnLabel = showStop ? "\u25A0" : "\u2192";
    const btnTitle = showStop ? "Stop" : "Send";
    const btnClass = showStop ? "send-btn stop-mode" : "send-btn";

    return html`
      <form class="input-form"
            @submit=${this.handleSubmit}
            @dragover=${this.handleDragOver}
            @dragleave=${this.handleDragLeave}
            @drop=${this.handleDrop}>
        ${this.pendingAttachments.length > 0
          ? html`<div class="attachment-strip">${this.renderAttachments()}</div>`
          : nothing}
        ${this.composer ? this.renderComposer(this.composer) : nothing}
        <div class="input-row">
          <div class="menu-anchor">
            <button type="button"
                    class="menu-btn"
                    title="Menu"
                    aria-haspopup="true"
                    aria-expanded=${this.menuOpen}
                    @click=${this.handleMenuToggle}>
              \u22EE
            </button>
            ${this.menuOpen ? this.renderMenu() : nothing}
          </div>
          <textarea
            id="input"
            placeholder=${this.placeholder}
            rows="1"
            @keydown=${this.handleKeydown}
            @input=${this.handleInput}
            @paste=${this.handlePaste}
          ></textarea>
          <button type="submit" class=${btnClass} ?disabled=${btnDisabled} title=${btnTitle}>${btnLabel}</button>
        </div>
        <div class="input-hint-row">
          <span class="input-hint" @click=${this.handleToggle}>${hint}</span>
          ${this.renderModelPicker()}
        </div>
        <input type="file" id="file-input" multiple class="hidden-file-input"
               @change=${this.handleFileSelect}>
        <input type="file" id="camera-input"
               accept="image/*" capture="environment"
               class="hidden-file-input"
               ?disabled=${this.imageAttachmentsDisabled}
               @change=${this.handleCameraCapture}>
        <input type="file" id="target-input" multiple class="hidden-file-input"
               @change=${this.handleTargetFileSelect}>
      </form>
    `;
  }

  /** Focus the input textarea. */
  focus(): void {
    this.inputEl?.focus();
  }

  /** Upload a file programmatically (used by share target pickup). */
  uploadExternalFile(file: File): void {
    this.uploadFile(file).catch(e => reportClientError(`uploadFile escaped: ${String(e)}`));
  }

  /** Pre-fill the textarea with text (used by share target pickup). */
  prefillText(text: string): void {
    if (!this.inputEl) return;
    this.withdrawPasteOffer();
    this.inputEl.value = text;
    this.hasText = text.trim().length > 0;
    this.syncKeyboardGuard();
    this.autoResize();
  }

  /** Clear pending attachments (called after send or conversation switch). */
  clearAttachments(): void {
    for (const att of this.pendingAttachments) {
      if (att.previewUrl) URL.revokeObjectURL(att.previewUrl);
    }
    this.pendingAttachments = [];
  }

  // --- Attachment rendering ---

  private renderAttachments() {
    return this.pendingAttachments.map(
      (att, idx) => html`
        <div class="attachment-chip ${att.status} ${att.secret ? "secret" : ""}"
             title=${att.secret ? SECRET_ATTACHMENT_TITLE : nothing}>
          ${att.secret
            ? html`<span class="attachment-icon">\u{1F512}</span>`
            : att.previewUrl
              ? html`<img class="attachment-thumb" src=${att.previewUrl} alt=${att.filename}>`
              : html`<span class="attachment-icon">\u{1F4C4}</span>`}
          <span class="attachment-name">${att.filename}</span>
          ${att.status === "uploading"
            ? html`<span class="attachment-uploading">\u2026</span>`
            : nothing}
          ${att.status === "error"
            ? html`<span class="attachment-error">\u26A0 ${att.errorMessage ?? "Upload failed"}</span>`
            : nothing}
          <button type="button"
                  class="attachment-remove"
                  title="Remove"
                  @click=${() => this.removeAttachment(idx)}>
            \u00D7
          </button>
        </div>
      `,
    );
  }

  // --- Menu ---

  private renderMenu() {
    return html`
      <div class="bottom-menu-popup" role="menu">
        ${this.attachmentTargets.map(
          (target) => html`
            <button type="button" class="menu-item" role="menuitem"
                    @click=${() => this.handleMenuTarget(target)}>
              <span class="menu-item-icon">\u{1F4E5}</span> ${target.label}
            </button>
          `,
        )}
        <button type="button" class="menu-item attach-file-menu-item" role="menuitem"
                @click=${this.handleMenuAttach}>
          <span class="menu-item-icon">\u{1F4CE}</span> Attach file
        </button>
        <button type="button" class="menu-item attach-secret-menu-item" role="menuitem"
                @click=${this.handleMenuAttachSecret}>
          <span class="menu-item-icon">\u{1F512}</span> Attach secret file
        </button>
        <button type="button" class="menu-item snippet-menu-item" role="menuitem"
                @click=${this.handleMenuSnippet}>
          <span class="menu-item-icon">\u{1F4DD}</span> Snippet
        </button>
        <button type="button" class="menu-item camera-menu-item" role="menuitem"
                @click=${this.handleMenuCamera}>
          <span class="menu-item-icon">\u{1F4F7}</span> Camera
        </button>
        ${this.singleton ? nothing : html`<button type="button" class="menu-item" role="menuitem"
                @click=${this.handleMenuConversations}>
          <span class="menu-item-icon">\u2630</span> Conversations
        </button>
        <button type="button" class="menu-item" role="menuitem"
                @click=${this.handleMenuNewConversation}>
          <span class="menu-item-icon">+</span> New conversation
        </button>`}
        <button type="button" class="menu-item" role="menuitem"
                @click=${this.handleMenuHome}>
          <span class="menu-item-icon">\u{1F3E0}</span> Home
        </button>
      </div>
    `;
  }

  /** Set menu open state and drive the controller's listener lifecycle. */
  private _setMenuOpen(open: boolean): void {
    this.menuOpen = open;
    if (open) {
      this._menuController.open();
    } else {
      this._menuController.close();
    }
  }

  private handleMenuToggle(e: Event): void {
    e.stopPropagation();
    this._setMenuOpen(!this.menuOpen);
  }

  private handleMenuAttach(): void {
    this._setMenuOpen(false);
    this.nextFileIsSecret = false;
    this.fileInputEl?.click();
  }

  private handleMenuAttachSecret(): void {
    this._setMenuOpen(false);
    this.nextFileIsSecret = true;
    this.fileInputEl?.click();
  }

  private handleMenuSnippet(): void {
    this._setMenuOpen(false);
    this.openComposer("", null);
  }

  private renderComposer(c: ComposerState) {
    const masked = c.secret && !c.show;
    const canAttach = c.text.trim().length > 0;
    return html`
      <div class="snippet-composer ${c.secret ? "secret" : ""}"
           @dragover=${this.handleComposerDragOver}
           @drop=${this.handleComposerDrop}>
        <textarea
          class="snippet-text ${masked ? "masked" : ""}"
          placeholder="Paste or type the snippet"
          rows="3"
          autocomplete="off"
          autocapitalize="off"
          autocorrect="off"
          spellcheck="false"
          data-lpignore="true"
          data-1p-ignore
          data-bwignore
          .value=${c.text}
          @input=${this.handleComposerInput}
          @keydown=${this.handleComposerKeydown}
          @paste=${this.handleComposerPaste}
        ></textarea>
        <div class="snippet-controls">
          <input type="text"
                 class="snippet-name"
                 placeholder=${DEFAULT_SNIPPET_NAME}
                 aria-label="Snippet file name"
                 autocomplete="off"
                 spellcheck="false"
                 .value=${c.name}
                 @input=${this.handleComposerNameInput}
                 @keydown=${this.handleComposerKeydown}>
          <label class="snippet-secret">
            <input type="checkbox"
                   class="snippet-secret-box"
                   .checked=${c.secret}
                   @change=${this.handleComposerSecretChange}>
            \u{1F512} Secret
          </label>
          ${c.secret
            ? html`<button type="button" class="snippet-btn snippet-show"
                           aria-pressed=${c.show}
                           @click=${this.handleComposerShowToggle}>
                     ${c.show ? "Hide" : "Show"}
                   </button>`
            : nothing}
          <span class="snippet-spacer"></span>
          ${c.pasteOffer
            ? html`<button type="button" class="snippet-btn snippet-paste-inline"
                           @click=${this.handlePasteInline}>Paste inline</button>`
            : nothing}
          <button type="button" class="snippet-btn snippet-cancel"
                  @click=${this.closeComposer}>Cancel</button>
          <button type="button" class="snippet-btn snippet-attach"
                  ?disabled=${!canAttach}
                  @click=${this.attachSnippet}>Attach</button>
        </div>
      </div>
    `;
  }

  /** Open the composer holding `text`; `offer` enables "Paste inline" at that range. */
  private openComposer(text: string, offer: CaretRange | null): void {
    this.composer = {
      text,
      name: DEFAULT_SNIPPET_NAME,
      secret: false,
      show: false,
      pasteOffer: offer,
    };
    this.syncKeyboardGuard();
    this.updateComplete
      .then(() => {
        const el = this.composerTextEl;
        if (!el) return;
        this.autoResize(el);
        el.focus();
      })
      .catch(e => reportClientError(`composer focus escaped: ${String(e)}`));
  }

  private closeComposer(): void {
    this.composer = null;
    this.syncKeyboardGuard();
    this.inputEl?.focus();
  }

  /** Replace the open composer session with `patch` applied; no-op while closed. */
  private updateComposer(patch: Partial<ComposerState>): void {
    if (this.composer) this.composer = { ...this.composer, ...patch };
  }

  private handleComposerInput(e: Event): void {
    const el = e.target as HTMLTextAreaElement;
    this.updateComposer({ text: el.value });
    this.syncKeyboardGuard();
    this.autoResize(el);
  }

  private handleComposerNameInput(e: Event): void {
    this.updateComposer({ name: (e.target as HTMLInputElement).value });
  }

  private handleComposerSecretChange(e: Event): void {
    this.updateComposer({ secret: (e.target as HTMLInputElement).checked, show: false });
  }

  private handleComposerShowToggle(): void {
    if (this.composer) this.updateComposer({ show: !this.composer.show });
  }

  private handleComposerKeydown(e: KeyboardEvent): void {
    if (e.key === "Escape") {
      e.preventDefault();
      e.stopPropagation();
      this.closeComposer();
    } else if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) {
      e.preventDefault();
      e.stopPropagation();
      this.attachSnippet();
    } else if (e.key === "Enter") {
      // Plain Enter is a newline in the composer and never sends the message.
      e.stopPropagation();
      if (e.target instanceof HTMLInputElement) e.preventDefault();
    }
  }

  private handleComposerPaste(e: ClipboardEvent): void {
    const files = e.clipboardData?.files;
    if (files && files.length > 0) {
      e.preventDefault();
      this.uploadFiles(files, this.composer?.secret ?? false);
    }
    // Text paste: the browser inserts it into the composer.
  }

  private handleComposerDragOver(e: DragEvent): void {
    // Accept the drop here and keep the form's drop handler and highlight
    // out of it.
    e.preventDefault();
    e.stopPropagation();
  }

  /**
   * Files dropped on the composer upload with its Secret flag, as pasted ones
   * do; a drop without files is left to the browser.
   */
  private handleComposerDrop(e: DragEvent): void {
    e.stopPropagation();
    this.querySelector("form")?.classList.remove("drag-over");
    const files = e.dataTransfer?.files;
    if (files && files.length > 0) {
      e.preventDefault();
      this.uploadFiles(files, this.composer?.secret ?? false);
    }
  }

  /** Attach the composer text as a text file, then clear and close the composer. */
  private attachSnippet(): void {
    if (!this.composer) return;
    const { text, secret } = this.composer;
    if (text.trim().length === 0) return;
    const name = this.composer.name.trim() || DEFAULT_SNIPPET_NAME;
    const file = new File([text], name, { type: "text/plain" });
    this.closeComposer();
    this.uploadFile(file, secret).catch(e => reportClientError(`uploadFile escaped: ${String(e)}`));
  }

  /** Drop "Paste inline": its recorded range is stale once the main box changes. */
  private withdrawPasteOffer(): void {
    if (this.composer?.pasteOffer) this.updateComposer({ pasteOffer: null });
  }

  /** Insert the diverted paste into the main box at its recorded caret range. */
  private handlePasteInline(): void {
    const offer = this.composer?.pasteOffer;
    if (!this.composer || !offer || !this.inputEl) return;
    this.inputEl.setRangeText(this.composer.text, offer.start, offer.end, "end");
    this.closeComposer();
    this.handleInput();
  }

  private handleMenuCamera(): void {
    this._setMenuOpen(false);
    if (this.imageAttachmentsDisabled) {
      // cameraInputEl is disabled when imageAttachmentsDisabled, so .click()
      // would silently no-op. Surface the error toast explicitly instead.
      this.onError?.("Image attachment disabled — server config error; reload page");
      return;
    }
    this.cameraInputEl?.click();
  }

  private handleMenuConversations(): void {
    this._setMenuOpen(false);
    this.onOpenConversations?.();
  }

  private handleMenuNewConversation(): void {
    this._setMenuOpen(false);
    this.onNewConversation?.();
  }

  private handleMenuTarget(target: TargetInfo): void {
    this._setMenuOpen(false);
    this.pendingTargetName = target.name;
    const input = this.targetInputEl;
    if (!input) return;
    input.accept = target.accept.join(",");
    input.multiple = target.multi;
    input.value = "";
    input.click();
  }

  private async handleTargetFileSelect(): Promise<void> {
    const input = this.targetInputEl;
    const files = input?.files;
    if (!files || files.length === 0) return;

    const targetName = this.pendingTargetName;
    if (!targetName) return;

    const formData = new FormData();
    formData.append("target", targetName);
    // Resize all files in parallel so multi-image uploads don't block serially.
    const resizeResults = await Promise.all(
      Array.from(files).map((f) => maybeResizeImage(f, this.maxLongEdge)),
    );
    for (const resizeResult of resizeResults) {
      if (resizeResult.resized) {
        const { from, to } = resizeResult;
        this.onResized?.(
          `Image resized to fit (was ${from.w}x${from.h}, now ${to.w}x${to.h})`,
        );
      }
      formData.append("file", resizeResult.file);
    }

    try {
      const resp = await this.postUpload(formData);

      if (!resp.ok) {
        const body = await resp.text().catch(() => "");
        this.onError?.(body || `Upload failed (${resp.status})`);
        return;
      }

      const data: TargetUploadResponse = await resp.json();
      // Phase 2: send RunTarget via WS to execute the command.
      this.onTargetUploaded?.(targetName, data.upload_ids);
    } catch (err) {
      this.onError?.(describeReasonShort(err));
    }
  }

  private handleMenuHome(): void {
    this._setMenuOpen(false);
    this.onNavigateHome?.();
  }

  // --- Event handlers ---

  private handleSubmit(e: Event): void {
    e.preventDefault();
    if (!this.enabled) return;
    // If in stop mode (working + empty input), send stop instead.
    if (this.isWorking && !this.hasContent) {
      this.onStop?.();
      return;
    }
    this.send();
  }

  private send(): void {
    if (!this.enabled) return;
    const text = this.inputEl.value.trim();
    const readyAttachments = this.pendingAttachments.filter(a => a.status === "ready");
    if (!text && readyAttachments.length === 0) return;

    const refs: AttachmentRef[] = readyAttachments.map(a => ({ upload_id: a.uploadId }));
    const meta: AttachmentMeta[] = readyAttachments.map(a => ({
      upload_id: a.uploadId,
      filename: a.filename,
      media_type: a.mediaType,
      size: a.size,
      secret: a.secret,
    }));
    this.onSubmit?.(text, refs, meta);

    this.withdrawPasteOffer();
    this.inputEl.value = "";
    this.hasText = false;
    this.syncKeyboardGuard();
    this.clearAttachments();
    this.autoResize();
  }

  private handleKeydown(e: KeyboardEvent): void {
    if (!this.enabled) return;
    if (e.key !== "Enter") return;

    const isModified = e.ctrlKey || e.metaKey;
    if (isModified) {
      // Ctrl/Cmd+Enter always sends.
      e.preventDefault();
      e.stopPropagation();
      this.send();
    } else if (e.shiftKey) {
      // Shift+Enter always newline (browser default).
    } else if (this.enterSends) {
      // Plain Enter sends when toggle is on.
      e.preventDefault();
      e.stopPropagation();
      this.send();
    }
    // Otherwise plain Enter adds newline (browser default).
  }

  private handleInput(): void {
    this.withdrawPasteOffer();
    this.hasText = (this.inputEl?.value.trim().length ?? 0) > 0;
    this.syncKeyboardGuard();
    this.autoResize();
  }

  private handlePaste(e: ClipboardEvent): void {
    const files = e.clipboardData?.files;
    if (files && files.length > 0) {
      e.preventDefault();
      this.uploadFiles(files);
      return;
    }
    // A large text paste is offered to the composer; the browser inserts the
    // rest. An open composer already holding text is left alone.
    const text = e.clipboardData?.getData("text/plain") ?? "";
    if (!isLargePaste(text) || this.composerHasText || !this.inputEl) return;
    e.preventDefault();
    this.openComposer(text, {
      start: this.inputEl.selectionStart,
      end: this.inputEl.selectionEnd,
    });
  }

  private handleCameraCapture(): void {
    const files = this.cameraInputEl?.files;
    if (files && files.length > 0) {
      this.uploadFiles(files);
      this.cameraInputEl.value = "";
    }
  }

  private handleFileSelect(): void {
    const secret = this.nextFileIsSecret;
    this.nextFileIsSecret = false;
    const files = this.fileInputEl?.files;
    if (files && files.length > 0) {
      this.uploadFiles(files, secret);
      // Reset so the same file can be selected again.
      this.fileInputEl.value = "";
    }
  }

  private handleDragOver(e: DragEvent): void {
    e.preventDefault();
    e.stopPropagation();
    (e.currentTarget as HTMLElement).classList.add("drag-over");
  }

  private handleDragLeave(e: DragEvent): void {
    e.preventDefault();
    (e.currentTarget as HTMLElement).classList.remove("drag-over");
  }

  private handleDrop(e: DragEvent): void {
    e.preventDefault();
    e.stopPropagation();
    (e.currentTarget as HTMLElement).classList.remove("drag-over");
    const files = e.dataTransfer?.files;
    if (files && files.length > 0) {
      this.uploadFiles(files);
    }
  }

  // --- Upload logic ---

  private async uploadFiles(files: FileList, secret = false): Promise<void> {
    for (const file of Array.from(files)) {
      await this.uploadFile(file, secret);
    }
  }

  /**
   * Upload one chat attachment. A secret upload is sent byte-for-byte: no
   * resize (so the image-disabled guard, which protects resize, does not
   * apply) and no preview URL.
   */
  private async uploadFile(file: File, secret = false): Promise<void> {
    // Reject image uploads when config delivery failed at startup.
    if (!secret && this.imageAttachmentsDisabled && file.type.startsWith("image/")) {
      this.onError?.("Image attachment disabled — server config error; reload page");
      return;
    }

    // --- Instant placeholder: visible before resize completes ---
    const localKey = `${Date.now()}-${Math.random().toString(36).slice(2)}`;
    const makePreviewUrl = (f: File): string | null =>
      f.type.startsWith("image/") ? URL.createObjectURL(f) : null;
    const mimeOrDefault = (f: File): string =>
      f.type || "application/octet-stream";

    const previewUrl = secret ? null : makePreviewUrl(file);

    const placeholder: PendingAttachment = {
      localKey,
      uploadId: "",
      filename: file.name,
      mediaType: mimeOrDefault(file),
      size: file.size,
      previewUrl,
      secret,
      status: "uploading",
    };

    this.pendingAttachments = [...this.pendingAttachments, placeholder];

    // --- Resize (after placeholder is visible) ---
    const resizeResult: ResizeResult = secret
      ? { resized: false, file }
      : await maybeResizeImage(file, this.maxLongEdge);
    if (resizeResult.resized) {
      const { from, to } = resizeResult;
      this.onResized?.(
        `Image resized to fit (was ${from!.w}x${from!.h}, now ${to!.w}x${to!.h})`,
      );
    }
    const fileToUpload = resizeResult.file;

    // Bail if user removed the placeholder while resize was running.
    // `previewUrl` (the original blob URL) is revoked here; the unconditional
    // revoke below only executes on the non-bail path.
    if (!this.pendingAttachments.some(a => a.localKey === localKey)) {
      if (previewUrl) URL.revokeObjectURL(previewUrl);
      return;
    }

    // When the image was actually resized, swap the preview URL to the resized
    // file (different blob) and update all metadata. When not resized,
    // fileToUpload === file, so the blob is the same and there is nothing to
    // swap — keep the existing previewUrl and only update scalar fields.
    let newPreviewUrl: string | null = previewUrl;
    if (resizeResult.resized) {
      if (previewUrl) URL.revokeObjectURL(previewUrl);
      newPreviewUrl = makePreviewUrl(fileToUpload);
    }

    this.pendingAttachments = this.pendingAttachments.map(a =>
      a.localKey === localKey
        ? {
            ...a,
            previewUrl: newPreviewUrl,
            filename: fileToUpload.name,
            mediaType: mimeOrDefault(fileToUpload),
            size: fileToUpload.size,
          }
        : a,
    );

    try {
      const formData = new FormData();
      formData.append("file", fileToUpload);
      if (secret) formData.append("secret", "1");

      const resp = await this.postUpload(formData);

      if (!resp.ok) {
        const body = await resp.text().catch(() => "");
        const msg = resp.status === 413
          ? resizeResult.resized
            ? "File too large after resize (server limit reached)"
            : "File too large (server limit reached)"
          : body || `Upload failed (${resp.status})`;
        throw new Error(msg);
      }

      const data: UploadResponse = await resp.json();

      // Update the placeholder with server response (match by localKey, not index).
      this.pendingAttachments = this.pendingAttachments.map(a =>
        a.localKey === localKey
          ? {
              ...a,
              uploadId: data.upload_id,
              filename: data.filename,
              mediaType: data.media_type,
              size: data.size,
              status: "ready" as const,
            }
          : a,
      );
    } catch (err) {
      // Forward upload failures (including JSON parse errors on the response)
      // to the backend log so on-call has a signal beyond the client's UI chip.
      reportClientError(`upload failed for ${file.name}: ${describeReason(err)}`);
      // Revoke the preview URL on error — thumbnail not useful in error state,
      // and keeping it alive until removeAttachment would be a leak.
      const existing = this.pendingAttachments.find(a => a.localKey === localKey);
      if (existing?.previewUrl) URL.revokeObjectURL(existing.previewUrl);
      // Mark as errored.
      this.pendingAttachments = this.pendingAttachments.map(a =>
        a.localKey === localKey
          ? {
              ...a,
              previewUrl: null,
              status: "error" as const,
              errorMessage: describeReasonShort(err),
            }
          : a,
      );
    }
  }

  private removeAttachment(idx: number): void {
    const att = this.pendingAttachments[idx];
    if (att.previewUrl) URL.revokeObjectURL(att.previewUrl);
    this.pendingAttachments = this.pendingAttachments.filter((_, i) => i !== idx);
  }

  /**
   * POST a FormData payload to the upload endpoint for this app.
   * Returns the raw Response; callers handle ok/error/413 logic themselves
   * since the two upload paths have different semantics for those cases.
   */
  private postUpload(formData: FormData): Promise<Response> {
    return fetch(`/app/${this.appSlug}/upload`, {
      method: "POST",
      body: formData,
      credentials: "same-origin",
    });
  }

  // --- Resize ---

  /** Fit a textarea (the main box by default) to its content, up to MAX_HEIGHT. */
  private autoResize(el: HTMLTextAreaElement | null = this.inputEl): void {
    if (!el) return;
    // Reset to auto to measure natural scrollHeight.
    el.style.height = "auto";
    el.style.overflowY = "hidden";
    if (el.scrollHeight > MAX_HEIGHT) {
      el.style.height = `${MAX_HEIGHT}px`;
      el.style.overflowY = "auto";
    } else {
      el.style.height = `${el.scrollHeight}px`;
    }
  }

  private handleToggle(): void {
    this.onToggleEnterSends?.();
  }

  private get currentModelLabel(): string {
    const m = this.availableModels.find(m => m.value === this.currentModel);
    return m?.display_name ?? this.currentModel;
  }

  private renderModelPicker() {
    // TODO(model-picker-lock-heuristic): one model offered is treated as one
    // model allowed. When the server's list is narrowed by a stale cache
    // rather than by the app's allow-list, the lone entry may not be the
    // model in effect, and the label then names a model the list does not
    // carry.
    if (this.availableModels.length === 0) return nothing;
    const title = this.availableModels.find(m => m.value === this.currentModel)?.description ?? "";
    if (this.availableModels.length === 1) {
      return html`<span class="model-picker model-picker-locked"
                        title=${title}>${this.currentModelLabel}</span>`;
    }
    return html`<span class="model-picker" @click=${this.cycleModel}
                      title=${title}>${this.currentModelLabel} ⇌</span>`;
  }

  private cycleModel(): void {
    if (this.availableModels.length < 2) return;
    const idx = this.availableModels.findIndex(m => m.value === this.currentModel);
    const nextIdx = (idx + 1) % this.availableModels.length;
    this.onModelChange?.(this.availableModels[nextIdx].value);
  }
}

/** Whether a main-box text paste is large enough to offer the snippet composer. */
export function isLargePaste(text: string): boolean {
  if (text.length >= SNIPPET_OFFER_CHARS) return true;
  return text.split("\n").length >= SNIPPET_OFFER_LINES;
}

declare global {
  interface HTMLElementTagNameMap {
    "brenn-input-bar": BrennInputBar;
  }
}
