/**
 * <brenn-tool-approve> — Generic approve/deny/always-allow component.
 *
 * Embedded in the backend's formatted_display HTML for CC's standard tools.
 * Reads tool name from the `tool-name` attribute and default patterns from
 * an embedded <script type="application/json"> child.
 *
 * Dispatches `brenn-tool-response` CustomEvent on itself (bubbles to app.ts):
 *   Allow:        { }
 *   Deny:         { deny: true }
 *   Deny+reason:  { deny: true, reason: "..." }
 *   Always Allow: { always_allow: true, patterns: [...], scope: "..." }
 *
 * Listens on `document` for `brenn-rule-error` to display validation errors
 * from the always-allow flow. Errors include `request_id` for stale-check.
 *
 * No global keyboard shortcuts — approve/deny require explicit click or
 * Tab-to-focus + native button activation. This prevents accidental approval
 * when the user is typing in the chat input.
 *
 * Light DOM (so children — tool content and script tag — render directly).
 */

import { LitElement, html, nothing } from "lit";
import { customElement, property, state } from "lit/decorators.js";
import type { RuleScope } from "../generated/RuleScope.js";

interface ToolApproveConfig {
  default_patterns?: string[];
}

/** Delay before the Deny button re-enables in the deny-reason panel (ms). */
const DENY_CONFIRM_DELAY_MS = 500;

@customElement("brenn-tool-approve")
export class BrennToolApprove extends LitElement {
  // Light DOM — children render directly.
  createRenderRoot(): HTMLElement {
    return this;
  }

  @property({ type: String, attribute: "tool-name" }) toolName = "";

  /** Whether the "Always Allow" expanded panel is showing. */
  @state() private alwaysAllowExpanded = false;

  /** User-edited patterns (initialized from config when panel opens). */
  @state() private editedPatterns: string[] = [];

  /** Validation error from always-allow attempt. */
  @state() private ruleError = "";

  /** Whether the deny-reason panel is showing. */
  @state() private denyExpanded = false;

  /** User-entered deny reason text. */
  @state() private denyReason = "";

  /** Whether the confirm-deny button is enabled (false during grace period). */
  @state() private denyConfirmEnabled = false;

  /** Default patterns loaded from embedded script tag. */
  private defaultPatterns: string[] = [];

  /** Timer for the deny-confirm button enable delay. */
  private denyConfirmTimer: ReturnType<typeof setTimeout> | null = null;

  /** Bound rule error handler (stored for removal). */
  private boundRuleErrorHandler = this.handleRuleError.bind(this);

  connectedCallback(): void {
    super.connectedCallback();

    // Read config from embedded <script type="application/json"> child.
    const scriptEl = Array.from(this.children).find(
      (el): el is HTMLScriptElement =>
        el instanceof HTMLScriptElement && el.type === "application/json",
    );
    if (scriptEl) {
      try {
        const config = JSON.parse(scriptEl.textContent ?? "{}") as ToolApproveConfig;
        this.defaultPatterns = config.default_patterns ?? [];
      } catch {
        this.defaultPatterns = [];
      }
    }

    // Reset state for fresh display.
    this.alwaysAllowExpanded = false;
    this.ruleError = "";
    this.editedPatterns = [];
    this.denyExpanded = false;
    this.denyReason = "";
    this.denyConfirmEnabled = false;

    document.addEventListener(
      "brenn-rule-error",
      this.boundRuleErrorHandler as EventListener,
    );

    // No focus steal — the chat textarea keeps focus.
  }

  disconnectedCallback(): void {
    super.disconnectedCallback();
    document.removeEventListener(
      "brenn-rule-error",
      this.boundRuleErrorHandler as EventListener,
    );
    if (this.denyConfirmTimer !== null) {
      clearTimeout(this.denyConfirmTimer);
      this.denyConfirmTimer = null;
    }
  }

  render() {
    return html`
      <div class="approval-header">Tool: ${this.toolName}</div>
      ${this.denyExpanded
        ? this.renderDenyReasonPanel()
        : this.alwaysAllowExpanded
          ? this.renderAlwaysAllowPanel()
          : this.renderButtons()}
    `;
  }

  private renderButtons() {
    return html`
      <div class="approval-actions">
        <button class="approval-allow" @click=${this.handleAllow}>
          Allow
        </button>
        <button
          class="approval-always-allow"
          @click=${this.handleAlwaysAllowClick}
        >
          Always Allow
        </button>
        <button class="approval-deny" @click=${this.handleDenyClick}>Deny</button>
      </div>
    `;
  }

  private renderDenyReasonPanel() {
    return html`
      <div class="approval-deny-panel">
        <div class="approval-deny-input-row">
          <input
            class="approval-deny-input"
            type="text"
            placeholder="Reason (optional)"
            .value=${this.denyReason}
            @input=${this.handleDenyReasonInput}
            @keydown=${this.handleDenyReasonKeydown}
          />
          <button
            class="approval-deny-submit-icon"
            title="Confirm deny"
            @click=${this.submitDeny}
          >\u2192</button>
        </div>
        <div class="approval-actions">
          <button
            class="approval-deny ${this.denyConfirmEnabled ? "" : "approval-deny-disabled"}"
            ?disabled=${!this.denyConfirmEnabled}
            @click=${this.submitDeny}
          >Deny</button>
          <button class="approval-cancel" @click=${this.handleCancelDeny}>
            Cancel
          </button>
        </div>
      </div>
    `;
  }

  private renderAlwaysAllowPanel() {
    return html`
      <div class="approval-always-panel">
        ${this.editedPatterns.map(
          (pattern, idx) => html`
            <div class="approval-pattern-row">
              <label
                class="approval-pattern-label"
                for="approval-pattern-input-${idx}"
                >${this.editedPatterns.length > 1 ? `Pattern ${idx + 1}:` : "Pattern:"}</label
              >
              <input
                id="approval-pattern-input-${idx}"
                class="approval-pattern-input"
                type="text"
                .value=${pattern}
                @input=${(e: Event) => this.handlePatternInput(e, idx)}
              />
            </div>
          `,
        )}
        ${this.ruleError
          ? html`<div class="approval-rule-error">${this.ruleError}</div>`
          : nothing}
        <div class="approval-actions">
          <button
            class="approval-scope-btn"
            @click=${() => this.handleScopeSelect("Conversation")}
          >
            This Conversation
          </button>
          <button
            class="approval-scope-btn approval-scope-permanent"
            @click=${() => this.handleScopeSelect("Permanent")}
          >
            Permanent
          </button>
          <button class="approval-cancel" @click=${this.handleCancelAlwaysAllow}>
            Cancel
          </button>
        </div>
      </div>
    `;
  }

  // --- Action handlers ---

  private handleAllow(): void {
    this.dispatchEvent(
      new CustomEvent("brenn-tool-response", {
        bubbles: true,
        composed: true,
        detail: {},
      }),
    );
  }

  private handleDenyClick(): void {
    this.denyExpanded = true;
    this.denyReason = "";
    this.denyConfirmEnabled = false;

    // Enable the confirm button after a delay to prevent accidental double-click.
    this.denyConfirmTimer = setTimeout(() => {
      this.denyConfirmEnabled = true;
      this.denyConfirmTimer = null;
    }, DENY_CONFIRM_DELAY_MS);

    // Focus the reason input after render.
    requestAnimationFrame(() => {
      const input = this.querySelector<HTMLInputElement>(".approval-deny-input");
      input?.focus();
    });
  }

  private submitDeny(): void {
    const reason = this.denyReason.trim();
    this.dispatchEvent(
      new CustomEvent("brenn-tool-response", {
        bubbles: true,
        composed: true,
        detail: reason ? { deny: true, reason } : { deny: true },
      }),
    );
  }

  private handleCancelDeny(): void {
    this.denyExpanded = false;
    this.denyReason = "";
    if (this.denyConfirmTimer !== null) {
      clearTimeout(this.denyConfirmTimer);
      this.denyConfirmTimer = null;
    }
  }

  private handleDenyReasonInput(e: Event): void {
    this.denyReason = (e.target as HTMLInputElement).value;
  }

  private handleDenyReasonKeydown(e: KeyboardEvent): void {
    if (e.key === "Enter") {
      e.preventDefault();
      this.submitDeny();
    } else if (e.key === "Escape") {
      e.preventDefault();
      this.handleCancelDeny();
    }
  }

  private handleAlwaysAllowClick(): void {
    this.alwaysAllowExpanded = true;
    this.editedPatterns = [...this.defaultPatterns];
    this.ruleError = "";
    // Focus the pattern input — user explicitly clicked "Always Allow".
    requestAnimationFrame(() => {
      const input = this.querySelector<HTMLInputElement>(
        "#approval-pattern-input-0",
      );
      if (input) {
        input.focus();
        input.select();
      }
    });
  }

  private handlePatternInput(e: Event, idx: number): void {
    const input = e.target as HTMLInputElement;
    const updated = [...this.editedPatterns];
    updated[idx] = input.value;
    this.editedPatterns = updated;
  }

  private handleScopeSelect(scope: RuleScope): void {
    this.ruleError = "";
    this.dispatchEvent(
      new CustomEvent("brenn-tool-response", {
        bubbles: true,
        composed: true,
        detail: { always_allow: true, patterns: this.editedPatterns, scope },
      }),
    );
  }

  private handleCancelAlwaysAllow(): void {
    this.alwaysAllowExpanded = false;
    this.ruleError = "";
  }

  private handleRuleError(e: CustomEvent<{ request_id: string; error: string }>): void {
    // We don't have access to request_id here, but if this component is visible
    // and receives the event, it's for the current approval (queue head).
    this.ruleError = e.detail?.error ?? "Unknown error";
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "brenn-tool-approve": BrennToolApprove;
  }
}
