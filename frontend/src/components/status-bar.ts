/**
 * <brenn-status-bar> — CC state, connection status, and context usage indicator.
 *
 * Shows CC state (Thinking, Compacting, etc.) when active.
 * Shows context usage percentage when available (from /context checks).
 * Context indicator turns yellow at reminder_pct (or reminder_tokens) and
 * red at red_pct (or red_tokens) — whichever gate fires first.
 * Click on context indicator dispatches "request-compaction" event.
 *
 * Renders nothing visible when idle+connected+no context data (min-height
 * on host element in app.css prevents layout shift).
 */

import { LitElement, html, nothing } from "lit";
import { customElement, property } from "lit/decorators.js";
import type { CcState } from "../generated/CcState";

export interface ContextUsageInfo {
  usagePct: number;
  currentTokens: number;
  reminderPct: number;
  redPct: number;
  reminderTokens: number | null;
  redTokens: number | null;
}

export interface CostUsageInfo {
  lastTurnUsd: number;
  sinceLastCompactionUsd: number;
  last24hUsd: number;
}

/** Wire type for CC's reported permission mode. `"auto"` is the expected
 *  value; `"other"` indicates any unrecognized mode (the backend alerts and
 *  logs the raw string). Matches the `#[ts(type)]` override on `PermissionMode.mode`
 *  in WsServerMessage. */
export type PermissionModeValue = "auto" | "other";

/** Tri-state for CC's permission_mode init field. `unseen` = no init
 *  frame seen yet; `missing` = CC omitted the field; `seen` = CC
 *  reported a value. Explicit discriminant avoids `if (!mode)` bugs
 *  (an empty string, null, and "not yet received" would all collapse). */
export type PermissionModeState =
  | { status: "unseen" }
  | { status: "missing" }
  | { status: "seen"; mode: PermissionModeValue };

@customElement("brenn-status-bar")
export class BrennStatusBar extends LitElement {
  // Light DOM.
  createRenderRoot(): HTMLElement {
    return this;
  }

  @property({ type: String }) ccState: CcState = "Idle";
  @property({ type: Boolean }) connected = true;
  @property({ attribute: false }) contextUsage: ContextUsageInfo | null = null;
  @property({ attribute: false }) costUsage: CostUsageInfo | null = null;
  @property({ attribute: false }) permissionMode: PermissionModeState = {
    status: "unseen",
  };

  render() {
    if (!this.connected) {
      return html`<div class="status status-disconnected">Disconnected — reconnecting...</div>`;
    }

    const labels: Record<CcState, string> = {
      Idle: "",
      Connecting: "Starting assistant...",
      Thinking: "Thinking...",
      AwaitingApproval: "Awaiting approval",
      Compacting: "Compacting context...",
      Error: "Error",
    };

    const stateText = labels[this.ccState];
    const permPart = this.renderPermissionMode();
    const costPart = this.renderCostUsage();
    const contextPart = this.renderContextUsage();

    // Collect non-empty parts in display order: state · perm · cost · context,
    // then interleave with separators. Needs to handle ≥4 parts cleanly.
    const parts: unknown[] = [];
    if (stateText) parts.push(stateText);
    if (permPart !== nothing) parts.push(permPart);
    if (costPart !== nothing) parts.push(costPart);
    if (contextPart !== nothing) parts.push(contextPart);
    if (parts.length === 0) {
      return nothing;
    }

    const joined = parts.flatMap((p, i) => (i === 0 ? [p] : [" · ", p]));
    const cls = this.ccState === "Error" ? "status status-error" : "status";
    return html`<div class=${cls}>${joined}</div>`;
  }

  private renderPermissionMode() {
    switch (this.permissionMode.status) {
      case "unseen":
        return nothing;
      case "missing":
        return html`<span
          class="perm-mode perm-mode-warn"
          title="CC init frame omitted permission_mode. See server logs."
          >(no mode)</span
        >`;
      case "seen": {
        const { mode } = this.permissionMode;
        const isOk = mode === "auto";
        const cls = isOk ? "perm-mode" : "perm-mode perm-mode-warn";
        const title = isOk
          ? "CC permission mode"
          : `CC permission mode unexpected (expected 'auto', got '${mode}'). See server logs.`;
        return html`<span class=${cls} title=${title}>${mode}</span>`;
      }
    }
  }

  private renderCostUsage() {
    if (!this.costUsage) return nothing;
    const { lastTurnUsd, sinceLastCompactionUsd, last24hUsd } = this.costUsage;
    const fmt = (v: number) => `$${v.toFixed(v < 1 ? 4 : 2)}`;
    const tip = `Last turn ${fmt(lastTurnUsd)} · since compact ${fmt(sinceLastCompactionUsd)} · 24h ${fmt(last24hUsd)}`;
    return html`<span class="cost-usage" title=${tip}
      >${fmt(lastTurnUsd)} · 24h ${fmt(last24hUsd)}</span
    >`;
  }

  private renderContextUsage() {
    if (!this.contextUsage) return nothing;

    const {
      usagePct,
      currentTokens,
      reminderPct,
      redPct,
      reminderTokens,
      redTokens,
    } = this.contextUsage;
    // "Whichever fires first" — pct OR (configured) absolute tokens.
    const isRed =
      usagePct >= redPct ||
      (redTokens !== null && currentTokens >= redTokens);
    const isYellow =
      usagePct >= reminderPct ||
      (reminderTokens !== null && currentTokens >= reminderTokens);
    let cls = "context-usage";
    if (isRed) {
      cls += " context-red";
    } else if (isYellow) {
      cls += " context-yellow";
    }

    return html`<span
      class=${cls}
      title="Context usage — click to compact"
      @click=${this.handleCompactClick}
      >Context: ${usagePct}%</span
    >`;
  }

  private handleCompactClick() {
    this.dispatchEvent(
      new CustomEvent("request-compaction", { bubbles: true, composed: true }),
    );
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "brenn-status-bar": BrennStatusBar;
  }
}
