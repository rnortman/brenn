/**
 * <brenn-file-picker> -- File list pane for browsing conversation artifacts.
 *
 * Shadow DOM for style encapsulation. Displays all artifact files in the
 * conversation with version counts. Click a file to open it in the viewer.
 */

import { LitElement, html, css, nothing } from "lit";
import { customElement, property } from "lit/decorators.js";
import type { ArtifactFileInfo } from "../generated/ArtifactFileInfo.js";

@customElement("brenn-file-picker")
export class BrennFilePicker extends LitElement {
  @property({ type: Boolean, reflect: true }) visible = false;
  @property({ attribute: false }) files: ArtifactFileInfo[] = [];
  @property({ attribute: false }) onFileSelect:
    | ((filePath: string, latestMessageId: number) => void)
    | null = null;
  @property({ attribute: false }) onCollapse: (() => void) | null = null;

  static styles = css`
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

    .picker-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0.5rem 1rem;
      border-bottom: 1px solid #2a2a40;
      background: #161628;
      flex-shrink: 0;
    }

    .picker-title {
      font-size: 0.85rem;
      color: #a0a0b8;
      font-weight: 500;
    }

    .collapse-btn {
      background: none;
      border: 1px solid #3a3a50;
      color: #a0a0b8;
      font-size: 1rem;
      cursor: pointer;
      padding: 0.15rem 0.5rem;
      border-radius: 3px;
      flex-shrink: 0;
    }

    .collapse-btn:hover {
      background: #2a2a40;
      color: #d0d0d8;
    }

    .file-list {
      flex: 1;
      overflow-y: auto;
      padding: 0.5rem 0;

      scrollbar-color: #2a2a40 transparent;
      scrollbar-width: thin;
    }

    .file-entry {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0.5rem 1rem;
      cursor: pointer;
      border-bottom: 1px solid #1a1a2e;
    }

    .file-entry:hover {
      background: #1a1a2e;
    }

    .file-name {
      font-family: "JetBrains Mono", "Fira Code", "Cascadia Code", monospace;
      font-size: 0.85rem;
      color: #d0d0d8;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      min-width: 0;
      flex: 1;
    }

    .version-badge {
      font-size: 0.75rem;
      color: #6a6a8a;
      flex-shrink: 0;
      margin-left: 0.5rem;
    }

    .empty-state {
      padding: 2rem 1rem;
      text-align: center;
      color: #6a6a8a;
      font-size: 0.9rem;
    }
  `;

  render() {
    return html`
      <div class="picker-header">
        <span class="picker-title">Files</span>
        <button
          class="collapse-btn"
          @click=${this._handleCollapse}
          title="Close file panel"
        >
          &times;
        </button>
      </div>
      <div class="file-list">
        ${this.files.length > 0
          ? this.files.map((f) => this._renderFileEntry(f))
          : html`<div class="empty-state">No files displayed yet</div>`}
      </div>
    `;
  }

  private _renderFileEntry(file: ArtifactFileInfo) {
    const versionCount = file.versions.length;
    if (versionCount === 0) return nothing;
    const latestVersion = file.versions[versionCount - 1];
    return html`
      <div
        class="file-entry"
        @click=${() => this._handleFileClick(file.file_path, latestVersion.message_id)}
      >
        <span class="file-name">${file.file_path}</span>
        ${versionCount > 1
          ? html`<span class="version-badge"
              >${versionCount} versions</span
            >`
          : nothing}
      </div>
    `;
  }

  private _handleFileClick(filePath: string, latestMessageId: number): void {
    if (this.onFileSelect) {
      this.onFileSelect(filePath, latestMessageId);
    }
  }

  private _handleCollapse(): void {
    if (this.onCollapse) {
      this.onCollapse();
    }
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "brenn-file-picker": BrennFilePicker;
  }
}
