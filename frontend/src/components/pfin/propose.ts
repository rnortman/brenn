/**
 * <brenn-pfin-propose> — Interactive multi-choice proposal selector for pfin.
 *
 * Backend renders proposal cards as light DOM children. This component adds
 * interactivity: click/tap/number key/Enter to choose, Escape or the Deny
 * button to deny with optional feedback text.
 *
 * Dispatches `brenn-tool-response` CustomEvent:
 *   Choose:  { selected: N }
 *   Deny:    { deny: true, reason?: "user feedback" }
 *
 * Light DOM — backend-rendered cards appear directly.
 */

import { LitElement, html } from "lit";
import { customElement, state } from "lit/decorators.js";
import { registerMount, unregisterMount, canInterceptKeyboard, eventOriginatedInside } from "../../keyboard-guard.js";

@customElement("brenn-pfin-propose")
export class BrennPfinPropose extends LitElement {
  // Light DOM so backend-rendered children appear directly.
  // Set renderBefore to null so Lit appends its output (the deny input)
  // *after* the backend-rendered proposal cards, not before them.
  // (Lit's default for light DOM is renderBefore=firstChild, which would
  // put Lit's output above the backend content.)
  createRenderRoot(): HTMLElement {
    this.renderOptions.renderBefore = null;
    return this;
  }

  /** Index of the currently focused proposal (-1 = none). */
  @state() private focusedIndex = 0;

  /** Whether the deny feedback input is showing. */
  @state() private denyMode = false;

  /** Text in the deny feedback input. */
  @state() private denyText = "";

  /** Cached proposal elements (set in connectedCallback). */
  private proposals: HTMLElement[] = [];

  /** Bound keyboard handler. */
  private boundKeyHandler = this.handleKeydown.bind(this);

  connectedCallback(): void {
    super.connectedCallback();

    // Discover proposal children from the DOM.
    this.proposals = Array.from(
      this.querySelectorAll<HTMLElement>(".pfin-proposal"),
    );

    // Reset state.
    this.focusedIndex = 0;
    this.denyMode = false;
    this.denyText = "";

    // Attach click handlers to each proposal card.
    for (const proposal of this.proposals) {
      proposal.addEventListener("click", this.handleProposalClick);
    }

    // Update visual focus.
    this.updateFocusVisual();

    // No focus steal — the chat textarea keeps focus.
    // Keyboard nav works via document-level listener + keyboard guard.

    registerMount(this);
    document.addEventListener("keydown", this.boundKeyHandler);
  }

  disconnectedCallback(): void {
    super.disconnectedCallback();
    unregisterMount(this);
    for (const proposal of this.proposals) {
      proposal.removeEventListener("click", this.handleProposalClick);
    }
    document.removeEventListener("keydown", this.boundKeyHandler);
    this.proposals = [];
  }

  render() {
    if (this.denyMode) {
      return html`
        <div class="pfin-propose-deny">
          <input
            class="pfin-propose-deny-input"
            type="text"
            placeholder="Feedback (optional) — Enter to send"
            .value=${this.denyText}
            @input=${this.handleDenyInput}
            @keydown=${this.handleDenyKeydown}
          />
          <div class="pfin-propose-deny-actions">
            <button
              type="button"
              class="pfin-batch-btn"
              @click=${this.cancelDeny}
            >Cancel</button>
            <button
              type="button"
              class="pfin-batch-btn pfin-batch-submit"
              @click=${this.submitDeny}
            >Send</button>
          </div>
        </div>
      `;
    }
    // Lit appends its render output *after* the backend-rendered proposal cards
    // (renderBefore=null). The Deny button sits below the proposals so it's
    // visible on every device — desktop users still have Escape, mobile users
    // need the button.
    return html`
      <div class="pfin-propose-actions">
        <button
          type="button"
          class="pfin-batch-btn pfin-propose-deny-btn"
          @click=${this.enterDenyMode}
        >Deny…</button>
      </div>
    `;
  }

  updated(): void {
    if (this.denyMode) {
      // Focus the deny input after render.
      requestAnimationFrame(() => {
        const input = this.querySelector<HTMLInputElement>(
          ".pfin-propose-deny-input",
        );
        input?.focus();
      });
    }
  }

  private handleProposalClick = (e: Event): void => {
    const target = (e.currentTarget as HTMLElement);
    const index = parseInt(target.dataset.index ?? "", 10);
    if (!isNaN(index)) {
      this.choose(index);
    }
  };

  private handleKeydown(e: KeyboardEvent): void {
    // When deny input is focused, don't intercept keys here.
    if (this.denyMode) return;

    // Don't intercept keyboard when the user is typing in chat or during grace period.
    if (!canInterceptKeyboard(this)) return;

    if (e.key === "ArrowDown" || e.key === "ArrowUp") {
      e.preventDefault();
      const delta = e.key === "ArrowDown" ? 1 : -1;
      const len = this.proposals.length;
      if (len === 0) return;
      this.focusedIndex = ((this.focusedIndex + delta) % len + len) % len;
      this.updateFocusVisual();
    } else if (e.key === "Enter") {
      if (!eventOriginatedInside(e, this)) return;
      e.preventDefault();
      if (this.focusedIndex >= 0 && this.focusedIndex < this.proposals.length) {
        this.choose(this.focusedIndex);
      }
    } else if (e.key === "Escape") {
      e.preventDefault();
      this.denyMode = true;
    } else if (e.key >= "1" && e.key <= "9") {
      if (!eventOriginatedInside(e, this)) return;
      const idx = parseInt(e.key, 10) - 1;
      if (idx < this.proposals.length) {
        e.preventDefault();
        this.choose(idx);
      }
    }
  }

  private handleDenyInput(e: Event): void {
    this.denyText = (e.target as HTMLInputElement).value;
  }

  private handleDenyKeydown(e: KeyboardEvent): void {
    if (e.key === "Enter") {
      e.preventDefault();
      e.stopPropagation();
      this.submitDeny();
    } else if (e.key === "Escape") {
      e.preventDefault();
      e.stopPropagation();
      this.cancelDeny();
    }
  }

  private enterDenyMode = (): void => {
    this.denyMode = true;
  };

  private cancelDeny = (): void => {
    this.denyMode = false;
    this.updateFocusVisual();
  };

  private submitDeny = (): void => {
    const reason = this.denyText.trim() || undefined;
    this.dispatchEvent(
      new CustomEvent("brenn-tool-response", {
        bubbles: true,
        composed: true,
        detail: reason ? { deny: true, reason } : { deny: true },
      }),
    );
  };

  private choose(index: number): void {
    this.dispatchEvent(
      new CustomEvent("brenn-tool-response", {
        bubbles: true,
        composed: true,
        detail: { selected: index },
      }),
    );
  }

  private updateFocusVisual(): void {
    for (let i = 0; i < this.proposals.length; i++) {
      this.proposals[i].classList.toggle(
        "pfin-proposal-focused",
        i === this.focusedIndex,
      );
    }
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "brenn-pfin-propose": BrennPfinPropose;
  }
}
