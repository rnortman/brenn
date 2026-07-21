/**
 * <brenn-conversation-list> — Sidebar showing conversation history.
 *
 * Desktop: left sidebar, ~250px wide.
 * Mobile: overlay triggered by hamburger menu.
 */

import { LitElement, html, css, nothing } from "lit";
import { customElement, property } from "lit/decorators.js";
import type { ConversationSummary } from "../generated/ConversationSummary";
import type { ConversationListStatus } from "../generated/ConversationListStatus";

@customElement("brenn-conversation-list")
export class BrennConversationList extends LitElement {
  static styles = css`
    :host {
      display: block;
    }

    .sidebar {
      width: 260px;
      height: 100%;
      background: #141427;
      border-right: 1px solid #2a2a40;
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }

    .sidebar-header {
      padding: 0.75rem 1rem;
      border-bottom: 1px solid #2a2a40;
      display: flex;
      align-items: center;
      justify-content: space-between;
    }

    .sidebar-title {
      font-size: 0.85rem;
      font-weight: 600;
      color: #a0a0b0;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }

    .close-btn {
      background: none;
      border: none;
      color: #707088;
      font-size: 1.2rem;
      cursor: pointer;
      padding: 0.25rem;
      display: none;
    }

    .conv-list {
      flex: 1;
      overflow-y: auto;
      padding: 0.5rem 0;
      scrollbar-color: #2a2a40 transparent;
      scrollbar-width: thin;
    }

    .conv-item {
      padding: 0.6rem 1rem;
      cursor: pointer;
      border-left: 3px solid transparent;
      transition: background 0.1s;
    }

    .conv-item:hover {
      background: #1e1e35;
    }

    .conv-item.active {
      background: #1e1e35;
      border-left-color: #4a6fa5;
    }

    .conv-title {
      font-size: 0.9rem;
      color: #d0d0d8;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .conv-meta {
      display: flex;
      align-items: center;
      gap: 0.5rem;
      margin-top: 0.2rem;
      font-size: 0.75rem;
      color: #707088;
    }

    .status-dot {
      width: 6px;
      height: 6px;
      border-radius: 50%;
      display: inline-block;
      flex-shrink: 0;
    }

    .status-dot.active {
      background: #4caf50;
    }
    .status-dot.completed {
      background: #707088;
    }
    .status-dot.error {
      background: #e94560;
    }

    .empty-state {
      padding: 2rem 1rem;
      text-align: center;
      color: #707088;
      font-size: 0.85rem;
    }

    .private-badge {
      font-size: 0.75rem;
      margin-right: 0.3em;
    }

    .conv-owner {
      color: #9090a8;
      font-weight: 500;
    }

    /* Mobile: overlay */
    @media (max-width: 768px) {
      .close-btn {
        display: block;
      }

      .sidebar {
        width: 280px;
      }
    }
  `;

  @property({ type: Array }) conversations: ConversationSummary[] = [];
  @property({ type: Number }) currentConversationId: number | null = null;
  @property({ type: Boolean }) visible = false;
  @property({ type: Boolean }) multiuser = false;
  @property({ attribute: false }) onSelect: ((id: number) => void) | null =
    null;
  @property({ attribute: false }) onNew: (() => void) | null = null;
  @property({ attribute: false }) onClose: (() => void) | null = null;

  render() {
    if (!this.visible) return nothing;

    return html`
      <div class="sidebar">
        <div class="sidebar-header">
          <span class="sidebar-title">Conversations</span>
          <button class="close-btn" @click=${() => this.onClose?.()}>✕</button>
        </div>
        <div class="conv-list">
          ${this.conversations.length === 0
            ? html`<div class="empty-state">No conversations yet</div>`
            : this.conversations.map((conv) => this.renderConvItem(conv))}
        </div>
      </div>
    `;
  }

  private renderConvItem(conv: ConversationSummary) {
    const isActive = conv.id === this.currentConversationId;
    const statusClass = this.statusToClass(conv.status);
    const title = conv.title ?? "Untitled";
    const timeAgo = this.formatTimeAgo(conv.updated_at);
    // In multiuser apps: show lock icon on private conversations.
    const privateBadge =
      this.multiuser && !conv.shared ? html`<span class="private-badge" title="Private">🔒</span>` : null;

    return html`
      <div
        class="conv-item ${isActive ? "active" : ""}"
        @click=${() => this.onSelect?.(conv.id)}
      >
        <div class="conv-title">${privateBadge}${title}</div>
        <div class="conv-meta">
          <span class="status-dot ${statusClass}"></span>
          ${conv.owner
            ? html`<span class="conv-owner">${conv.owner}</span><span>·</span>`
            : null}
          <span>${timeAgo}</span>
          <span>·</span>
          <span>${conv.message_count} msgs</span>
        </div>
      </div>
    `;
  }

  private statusToClass(status: ConversationListStatus): string {
    switch (status) {
      case "Active":
        return "active";
      case "Completed":
        return "completed";
      case "Error":
        return "error";
    }
  }

  private formatTimeAgo(isoDate: string): string {
    const date = new Date(isoDate);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMs / 3600000);
    const diffDays = Math.floor(diffMs / 86400000);

    if (diffMins < 1) return "just now";
    if (diffMins < 60) return `${diffMins}m ago`;
    if (diffHours < 24) return `${diffHours}h ago`;
    if (diffDays < 7) return `${diffDays}d ago`;
    return date.toLocaleDateString();
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "brenn-conversation-list": BrennConversationList;
  }
}
