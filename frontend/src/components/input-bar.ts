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
import { maybeResizeImage } from "../image-resize.js";
import { describeReason, describeReasonShort, reportClientError } from "../error-reporter.js";
import { MenuController } from "./menu-controller.js";

/** Max textarea height in px before scrollbar appears. */
const MAX_HEIGHT = 120;

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

  @query("#input") private inputEl!: HTMLTextAreaElement;
  @query("#file-input") private fileInputEl!: HTMLInputElement;
  @query("#camera-input") private cameraInputEl!: HTMLInputElement;
  @query("#target-input") private targetInputEl!: HTMLInputElement;

  /** Name of the target being uploaded to (set before opening file picker). */
  private pendingTargetName = "";

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
    setChatHasText(this.hasText);
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
          ${this.availableModels.length > 1
            ? html`<span class="model-picker" @click=${this.cycleModel}
                         title=${this.availableModels.find(m => m.value === this.currentModel)?.description ?? ""}>${this.currentModelLabel} ⇌</span>`
            : nothing}
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
    this.inputEl.value = text;
    this.hasText = text.trim().length > 0;
    setChatHasText(this.hasText);
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
        <div class="attachment-chip ${att.status}">
          ${att.previewUrl
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
        <button type="button" class="menu-item" role="menuitem"
                @click=${this.handleMenuAttach}>
          <span class="menu-item-icon">\u{1F4CE}</span> Attach file
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
    this.fileInputEl?.click();
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
    }));
    this.onSubmit?.(text, refs, meta);

    this.inputEl.value = "";
    this.hasText = false;
    setChatHasText(false);
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
    this.hasText = (this.inputEl?.value.trim().length ?? 0) > 0;
    setChatHasText(this.hasText);
    this.autoResize();
  }

  private handlePaste(e: ClipboardEvent): void {
    const files = e.clipboardData?.files;
    if (files && files.length > 0) {
      e.preventDefault();
      this.uploadFiles(files);
    }
    // Text paste: let browser handle it normally.
  }

  private handleCameraCapture(): void {
    const files = this.cameraInputEl?.files;
    if (files && files.length > 0) {
      this.uploadFiles(files);
      this.cameraInputEl.value = "";
    }
  }

  private handleFileSelect(): void {
    const files = this.fileInputEl?.files;
    if (files && files.length > 0) {
      this.uploadFiles(files);
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

  private async uploadFiles(files: FileList): Promise<void> {
    for (const file of Array.from(files)) {
      await this.uploadFile(file);
    }
  }

  private async uploadFile(file: File): Promise<void> {
    // Reject image uploads when config delivery failed at startup.
    if (this.imageAttachmentsDisabled && file.type.startsWith("image/")) {
      this.onError?.("Image attachment disabled — server config error; reload page");
      return;
    }

    // --- Instant placeholder: visible before resize completes ---
    const localKey = `${Date.now()}-${Math.random().toString(36).slice(2)}`;
    const makePreviewUrl = (f: File): string | null =>
      f.type.startsWith("image/") ? URL.createObjectURL(f) : null;
    const mimeOrDefault = (f: File): string =>
      f.type || "application/octet-stream";

    const previewUrl = makePreviewUrl(file);

    const placeholder: PendingAttachment = {
      localKey,
      uploadId: "",
      filename: file.name,
      mediaType: mimeOrDefault(file),
      size: file.size,
      previewUrl,
      status: "uploading",
    };

    this.pendingAttachments = [...this.pendingAttachments, placeholder];

    // --- Resize (after placeholder is visible) ---
    const resizeResult = await maybeResizeImage(file, this.maxLongEdge);
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

  private autoResize(): void {
    const el = this.inputEl;
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

  private cycleModel(): void {
    if (this.availableModels.length < 2) return;
    const idx = this.availableModels.findIndex(m => m.value === this.currentModel);
    const nextIdx = (idx + 1) % this.availableModels.length;
    this.onModelChange?.(this.availableModels[nextIdx].value);
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "brenn-input-bar": BrennInputBar;
  }
}
