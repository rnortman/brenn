/**
 * <brenn-pfin-batch-table> — Desktop batch reconciliation table.
 *
 * Backend renders a table with one row per batch item. This component adds
 * interactivity: ✓/✗ toggle buttons per row, keyboard navigation, and
 * accept/reject remaining + submit controls.
 *
 * Subclassed by `<brenn-pfin-batch-assign-table>` — only `rowSelector()`
 * differs between the two.
 *
 * Dispatches `brenn-tool-response` CustomEvent:
 *   Submit: { decisions: [{ index: N, accepted: bool }, ...] }
 *
 * Light DOM — backend-rendered table appears directly.
 */

import { LitElement, html, nothing } from "lit";
import { customElement, state } from "lit/decorators.js";
import { registerMount, unregisterMount, canInterceptKeyboard, eventOriginatedInside } from "../../keyboard-guard.js";

@customElement("brenn-pfin-batch-table")
export class BrennPfinBatchTable extends LitElement {
  createRenderRoot(): HTMLElement {
    this.renderOptions.renderBefore = null;
    return this;
  }

  /** Per-row decisions: index → accepted. Undefined = undecided. */
  @state() protected decisions: Map<number, boolean> = new Map();

  /** Currently focused row index. */
  @state() private focusedRow = 0;

  /** Cached row elements. */
  protected rows: HTMLElement[] = [];

  /** Bound keyboard handler. */
  private boundKeyHandler = this.handleKeydown.bind(this);

  /** CSS selector for row elements. Subclasses override to change the class. */
  protected rowSelector(): string {
    return ".pfin-batch-row";
  }

  connectedCallback(): void {
    super.connectedCallback();

    this.rows = Array.from(
      this.querySelectorAll<HTMLElement>(this.rowSelector()),
    );
    this.decisions = new Map();
    this.focusedRow = 0;

    // Attach click handlers to accept/reject buttons.
    for (const row of this.rows) {
      const acceptBtn = row.querySelector<HTMLElement>(".pfin-batch-accept");
      const rejectBtn = row.querySelector<HTMLElement>(".pfin-batch-reject");
      if (acceptBtn) {
        acceptBtn.addEventListener("click", (e) => {
          e.stopPropagation();
          this.setDecision(row, true);
        });
      }
      if (rejectBtn) {
        rejectBtn.addEventListener("click", (e) => {
          e.stopPropagation();
          this.setDecision(row, false);
        });
      }
    }

    this.updateFocusVisual();

    // No focus steal — the chat textarea keeps focus.
    // Keyboard nav works via document-level listener + keyboard guard.

    registerMount(this);
    document.addEventListener("keydown", this.boundKeyHandler);
  }

  disconnectedCallback(): void {
    super.disconnectedCallback();
    unregisterMount(this);
    document.removeEventListener("keydown", this.boundKeyHandler);
    this.rows = [];
  }

  render() {
    const total = this.rows.length;
    const decided = this.decisions.size;
    const accepted = [...this.decisions.values()].filter((v) => v).length;
    const rejected = decided - accepted;
    const allDecided = decided === total;

    return html`
      <div class="pfin-batch-controls">
        <span class="pfin-batch-tally">
          ${decided}/${total} decided
          ${accepted > 0 ? html` · <span class="pfin-batch-tally-accept">${accepted} accepted</span>` : nothing}
          ${rejected > 0 ? html` · <span class="pfin-batch-tally-reject">${rejected} rejected</span>` : nothing}
        </span>
        <div class="pfin-batch-buttons">
          <button
            class="pfin-batch-btn pfin-batch-accept-remaining"
            @click=${this.acceptRemaining}
            ?disabled=${allDecided}
          >Accept remaining</button>
          <button
            class="pfin-batch-btn pfin-batch-reject-remaining"
            @click=${this.rejectRemaining}
            ?disabled=${allDecided}
          >Reject remaining</button>
          <button
            class="pfin-batch-btn pfin-batch-submit"
            @click=${this.submit}
            ?disabled=${!allDecided}
          >Submit</button>
        </div>
      </div>
    `;
  }

  private setDecision(row: HTMLElement, accepted: boolean): void {
    const index = parseInt(row.dataset.index ?? "", 10);
    if (isNaN(index)) return;

    const current = this.decisions.get(index);
    if (current === accepted) {
      // Toggle off — go back to undecided.
      this.decisions.delete(index);
    } else {
      this.decisions.set(index, accepted);
    }
    // Trigger re-render.
    this.decisions = new Map(this.decisions);
    this.updateRowVisual(row, index);
  }

  private updateRowVisual(row: HTMLElement, index: number): void {
    const decision = this.decisions.get(index);
    row.classList.toggle("pfin-batch-accepted", decision === true);
    row.classList.toggle("pfin-batch-rejected", decision === false);
  }

  private updateFocusVisual(): void {
    for (let i = 0; i < this.rows.length; i++) {
      this.rows[i].classList.toggle(
        "pfin-batch-focused",
        i === this.focusedRow,
      );
    }
  }

  private handleKeydown(e: KeyboardEvent): void {
    // Don't intercept keyboard when the user is typing in chat or during grace period.
    if (!canInterceptKeyboard(this)) return;

    if (e.key === "ArrowDown" || e.key === "ArrowUp") {
      e.preventDefault();
      const delta = e.key === "ArrowDown" ? 1 : -1;
      const len = this.rows.length;
      if (len === 0) return;
      this.focusedRow = ((this.focusedRow + delta) % len + len) % len;
      this.updateFocusVisual();
    } else if (e.key === "a") {
      if (!eventOriginatedInside(e, this)) return;
      e.preventDefault();
      if (this.focusedRow < this.rows.length) {
        this.setDecision(this.rows[this.focusedRow], true);
      }
    } else if (e.key === "r") {
      if (!eventOriginatedInside(e, this)) return;
      e.preventDefault();
      if (this.focusedRow < this.rows.length) {
        this.setDecision(this.rows[this.focusedRow], false);
      }
    } else if (e.key === "A") {
      if (!eventOriginatedInside(e, this)) return;
      e.preventDefault();
      this.acceptRemaining();
    } else if (e.key === "R") {
      if (!eventOriginatedInside(e, this)) return;
      e.preventDefault();
      this.rejectRemaining();
    } else if (e.key === "Enter") {
      if (!eventOriginatedInside(e, this)) return;
      e.preventDefault();
      if (this.decisions.size === this.rows.length) {
        this.submit();
      }
    }
  }

  private acceptRemaining(): void {
    for (const row of this.rows) {
      const index = parseInt(row.dataset.index ?? "", 10);
      if (!isNaN(index) && !this.decisions.has(index)) {
        this.decisions.set(index, true);
        this.updateRowVisual(row, index);
      }
    }
    this.decisions = new Map(this.decisions);
  }

  private rejectRemaining(): void {
    for (const row of this.rows) {
      const index = parseInt(row.dataset.index ?? "", 10);
      if (!isNaN(index) && !this.decisions.has(index)) {
        this.decisions.set(index, false);
        this.updateRowVisual(row, index);
      }
    }
    this.decisions = new Map(this.decisions);
  }

  private submit(): void {
    const decisions = [...this.decisions.entries()].map(
      ([index, accepted]) => ({ index, accepted }),
    );
    this.dispatchEvent(
      new CustomEvent("brenn-tool-response", {
        bubbles: true,
        composed: true,
        detail: { decisions },
      }),
    );
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "brenn-pfin-batch-table": BrennPfinBatchTable;
  }
}
