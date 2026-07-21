/**
 * <brenn-file-viewer> -- Single-file rendered content display pane.
 *
 * Shadow DOM for style encapsulation. Receives pre-rendered HTML from the
 * backend and displays it with a header bar containing navigation and version
 * controls.
 */

import { LitElement, html, css, nothing } from "lit";
import { customElement, property, state } from "lit/decorators.js";
import { unsafeHTML } from "lit/directives/unsafe-html.js";
import { markdownStyles, frontmatterStyles } from "../styles/markdown.js";
import type { SnapshotMetadata } from "../generated/SnapshotMetadata.js";
import type { ArtifactVersionInfo } from "../generated/ArtifactVersionInfo.js";

/** Transient state of the Copy button, used to flash "Copied!" / "Failed". */
type CopyState = "idle" | "copied" | "failed";

@customElement("brenn-file-viewer")
export class BrennFileViewer extends LitElement {
  @property({ type: Boolean, reflect: true }) visible = false;
  @property() filePath = "";
  @property() renderedHtml = "";
  @property() rawContent = "";
  @property({ attribute: false }) snapshot: SnapshotMetadata | null = null;
  @property({ attribute: false }) versions: ArtifactVersionInfo[] | null = null;
  @property({ attribute: false }) onBack: (() => void) | null = null;
  @property({ attribute: false }) onVersionSelect:
    | ((messageId: number) => void)
    | null = null;

  @state() private copyState: CopyState = "idle";
  private copyResetTimer: number | null = null;

  static styles = [
    markdownStyles,
    frontmatterStyles,
    css`
      :host {
        flex: 1;
        flex-direction: column;
        min-width: 0;
        min-height: 0;
      }

      :host(:not([visible])) {
        display: none !important;
      }

      :host([visible]) {
        display: flex;
      }

      .file-header {
        display: flex;
        align-items: center;
        gap: 0.5rem;
        padding: 0.5rem 1rem;
        border-bottom: 1px solid #2a2a40;
        background: #161628;
        flex-shrink: 0;
      }

      .back-btn,
      .copy-btn {
        background: none;
        border: 1px solid #3a3a50;
        color: #a0a0b8;
        font-size: 0.9rem;
        cursor: pointer;
        padding: 0.15rem 0.5rem;
        border-radius: 3px;
        flex-shrink: 0;
      }

      .back-btn:hover,
      .copy-btn:hover {
        background: #2a2a40;
        color: #d0d0d8;
      }

      .copy-btn {
        font-size: 0.8rem;
      }

      .copy-btn[disabled] {
        cursor: default;
      }

      .file-path {
        font-family: "JetBrains Mono", "Fira Code", "Cascadia Code", monospace;
        font-size: 0.85rem;
        color: #a0a0b8;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        flex: 1;
        min-width: 0;
      }

      .stable-link {
        color: #6a6a8a;
        text-decoration: none;
        font-size: 0.8rem;
        flex-shrink: 0;
      }

      .stable-link:hover {
        color: #a0a0b8;
      }

      .version-select {
        background: #1a1a2e;
        color: #a0a0b8;
        border: 1px solid #3a3a50;
        border-radius: 3px;
        font-size: 0.8rem;
        padding: 0.1rem 0.3rem;
        cursor: pointer;
        flex-shrink: 0;
      }

      .version-select:hover {
        border-color: #5a5a70;
      }

      .file-content {
        flex: 1;
        overflow-y: auto;
        overflow-x: hidden;
        min-width: 0;
        padding: 1rem;
        line-height: 1.6;
        font-size: 1rem;
        color: #d0d0d8;

        scrollbar-color: #2a2a40 transparent;
        scrollbar-width: thin;
      }

    `,
  ];

  render() {
    const hasVersions =
      this.versions !== null && this.versions.length > 1 && this.snapshot;

    const copyLabel =
      this.copyState === "copied"
        ? "Copied!"
        : this.copyState === "failed"
          ? "Failed"
          : "Copy";

    return html`
      <div class="file-header">
        ${this.onBack
          ? html`<button
              class="back-btn"
              @click=${this._handleBack}
              title="Back to file list"
            >
              &#x2190;
            </button>`
          : nothing}
        <span class="file-path">${this.filePath}</span>
        ${hasVersions ? this._renderVersionSelect() : nothing}
        <button
          class="copy-btn"
          @click=${this._handleCopy}
          title="Copy file source to clipboard"
          ?disabled=${this.copyState !== "idle"}
        >
          ${copyLabel}
        </button>
        ${this.snapshot?.stable_url
          ? html`<a
              class="stable-link"
              href=${this.snapshot.stable_url}
              target="_blank"
              title="Open file in new tab"
              >&#x2197;</a
            >`
          : nothing}
      </div>
      <div class="file-content md-content">
        ${unsafeHTML(this.renderedHtml)}
      </div>
    `;
  }

  private _renderVersionSelect() {
    if (!this.versions || !this.snapshot) return nothing;
    return html`
      <select
        class="version-select"
        @change=${this._handleVersionChange}
        title="Select version"
      >
        ${this.versions.map(
          (v) => html`
            <option
              value=${v.message_id}
              ?selected=${v.message_id === this.snapshot!.message_id}
            >
              v${v.version}
            </option>
          `,
        )}
      </select>
    `;
  }

  /** Update displayed content. Visibility is controlled by the parent via
   *  the `visible` property binding — do NOT set `visible` here. */
  show(
    filePath: string,
    renderedHtml: string,
    rawContent: string,
    snapshot: SnapshotMetadata | null,
    versions: ArtifactVersionInfo[] | null,
  ): void {
    this.filePath = filePath;
    this.renderedHtml = renderedHtml;
    this.rawContent = rawContent;
    this.snapshot = snapshot;
    this.versions = versions;
    this._resetCopyState();
  }

  /** Clear displayed content. Visibility is controlled by the parent. */
  clear(): void {
    this.filePath = "";
    this.renderedHtml = "";
    this.rawContent = "";
    this.snapshot = null;
    this.versions = null;
    this._resetCopyState();
  }

  private _handleBack(): void {
    if (this.onBack) {
      this.onBack();
    }
  }

  private _handleVersionChange(e: Event): void {
    const select = e.target as HTMLSelectElement;
    const messageId = parseInt(select.value, 10);
    if (this.onVersionSelect && !isNaN(messageId)) {
      this.onVersionSelect(messageId);
    }
  }

  private async _handleCopy(): Promise<void> {
    // Defensive: button is disabled when not idle, but guard anyway.
    if (this.copyState !== "idle") return;
    const text = this.rawContent;
    const ok = await copyToClipboard(text);
    this.copyState = ok ? "copied" : "failed";
    if (this.copyResetTimer !== null) {
      window.clearTimeout(this.copyResetTimer);
    }
    this.copyResetTimer = window.setTimeout(() => {
      this.copyState = "idle";
      this.copyResetTimer = null;
    }, 2000);
  }

  private _resetCopyState(): void {
    this.copyState = "idle";
    if (this.copyResetTimer !== null) {
      window.clearTimeout(this.copyResetTimer);
      this.copyResetTimer = null;
    }
  }

  disconnectedCallback(): void {
    super.disconnectedCallback();
    if (this.copyResetTimer !== null) {
      window.clearTimeout(this.copyResetTimer);
      this.copyResetTimer = null;
    }
  }
}

/** Copy text to the clipboard. Returns true on success, false on failure.
 *  Tries the async Clipboard API first (requires a secure context) and falls
 *  back to `document.execCommand('copy')` for plain-http dev servers. */
async function copyToClipboard(text: string): Promise<boolean> {
  if (navigator.clipboard && navigator.clipboard.writeText) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch (err) {
      // Fall through to execCommand fallback.
      console.warn("clipboard API failed, falling back to execCommand:", err);
    }
  }
  const ta = document.createElement("textarea");
  ta.value = text;
  ta.style.position = "fixed";
  ta.style.opacity = "0";
  document.body.appendChild(ta);
  try {
    ta.select();
    return document.execCommand("copy");
  } catch (err) {
    console.error("copy fallback failed:", err);
    return false;
  } finally {
    // Always remove the textarea, even if execCommand threw.
    document.body.removeChild(ta);
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "brenn-file-viewer": BrennFileViewer;
  }
}
