/**
 * <brenn-pfin-batch-swipe> — Mobile batch reconciliation with swipe gestures.
 *
 * Backend renders stacked cards with reveal layers. This component adds
 * pointer event handling for swipe-to-accept/reject interaction.
 *
 * Swipe right → Accept (green reveal with ✓ + "Accept" label)
 * Swipe left  → Reject (red reveal with ✗ + "Reject" label)
 *
 * Continuous feedback during drag: color intensity and label proportional
 * to drag distance, threshold snap at ~30% card width.
 *
 * Subclassed by `<brenn-pfin-batch-assign-swipe>` — only `cardSelector()`
 * differs between the two.
 *
 * Dispatches `brenn-tool-response` CustomEvent:
 *   Submit: { decisions: [{ index: N, accepted: bool }, ...] }
 *
 * Light DOM — backend-rendered cards appear directly.
 */

import { LitElement, html, nothing } from "lit";
import { customElement, state } from "lit/decorators.js";

/** Fraction of card width required to trigger accept/reject. */
const SWIPE_THRESHOLD = 0.3;
/** Fraction at which the label text appears. */
const LABEL_THRESHOLD = 0.15;

@customElement("brenn-pfin-batch-swipe")
export class BrennPfinBatchSwipe extends LitElement {
  createRenderRoot(): HTMLElement {
    this.renderOptions.renderBefore = null;
    return this;
  }

  /** Per-item decisions: original_index → accepted. */
  @state() protected decisions: Map<number, boolean> = new Map();

  /** All swipe item elements. */
  protected items: HTMLElement[] = [];

  /** Tracking state for active drag. */
  private dragState: {
    item: HTMLElement;
    card: HTMLElement;
    startX: number;
    width: number;
  } | null = null;

  // Bound handlers for cleanup.
  private boundPointerMove = this.onPointerMove.bind(this);
  private boundPointerUp = this.onPointerUp.bind(this);

  /** CSS selector for the card element within each swipe item. Subclasses override. */
  protected cardSelector(): string {
    return ".pfin-batch-card";
  }

  connectedCallback(): void {
    super.connectedCallback();

    this.items = Array.from(
      this.querySelectorAll<HTMLElement>(".pfin-batch-swipe-item"),
    );
    this.decisions = new Map();

    // Attach pointer handlers to each card.
    for (const item of this.items) {
      const card = item.querySelector<HTMLElement>(this.cardSelector());
      if (card) {
        card.addEventListener("pointerdown", this.onPointerDown.bind(this));
        // Prevent horizontal scroll from interfering.
        card.style.touchAction = "pan-y";
      }
    }
  }

  disconnectedCallback(): void {
    super.disconnectedCallback();
    document.removeEventListener("pointermove", this.boundPointerMove);
    document.removeEventListener("pointerup", this.boundPointerUp);
    this.items = [];
    this.dragState = null;
  }

  render() {
    const total = this.items.length;
    const decided = this.decisions.size;
    const accepted = [...this.decisions.values()].filter((v) => v).length;
    const rejected = decided - accepted;
    const remaining = total - decided;
    const allDecided = decided === total;

    return html`
      <div class="pfin-batch-swipe-controls">
        <div class="pfin-batch-swipe-tally">
          ${remaining > 0 ? html`<span class="pfin-batch-swipe-remaining">${remaining} remaining</span>` : nothing}
          ${accepted > 0 ? html`<span class="pfin-batch-tally-accept">${accepted} accepted</span>` : nothing}
          ${rejected > 0 ? html`<span class="pfin-batch-tally-reject">${rejected} rejected</span>` : nothing}
        </div>
        <div class="pfin-batch-buttons">
          <button
            class="pfin-batch-btn pfin-batch-accept-remaining"
            @click=${this.acceptAll}
            ?disabled=${allDecided}
          >Accept all</button>
          <button
            class="pfin-batch-btn pfin-batch-reject-remaining"
            @click=${this.rejectAll}
            ?disabled=${allDecided}
          >Reject all</button>
          <button
            class="pfin-batch-btn pfin-batch-submit"
            @click=${this.submit}
            ?disabled=${!allDecided}
          >Submit</button>
        </div>
      </div>
    `;
  }

  // ---- Swipe gesture handling ----

  private onPointerDown(e: PointerEvent): void {
    const card = e.currentTarget as HTMLElement;
    const item = card.closest<HTMLElement>(".pfin-batch-swipe-item");
    if (!item) return;

    // Don't start drag on already-decided items.
    const index = parseInt(item.dataset.index ?? "", 10);
    if (isNaN(index) || this.decisions.has(index)) return;

    this.dragState = {
      item,
      card,
      startX: e.clientX,
      width: card.offsetWidth,
    };

    card.setPointerCapture(e.pointerId);
    document.addEventListener("pointermove", this.boundPointerMove);
    document.addEventListener("pointerup", this.boundPointerUp);
  }

  private onPointerMove(e: PointerEvent): void {
    if (!this.dragState) return;

    const deltaX = e.clientX - this.dragState.startX;
    const { card, item, width } = this.dragState;
    const progress = Math.abs(deltaX) / width;
    const isAccept = deltaX > 0;

    // Move the card.
    card.style.transform = `translateX(${deltaX}px)`;
    card.style.transition = "none";

    // Show appropriate reveal layer with opacity based on progress.
    const acceptReveal = item.querySelector<HTMLElement>(
      ".pfin-batch-reveal-accept",
    );
    const rejectReveal = item.querySelector<HTMLElement>(
      ".pfin-batch-reveal-reject",
    );

    if (acceptReveal && rejectReveal) {
      if (isAccept) {
        acceptReveal.style.opacity = String(Math.min(progress / SWIPE_THRESHOLD, 1));
        acceptReveal.style.display = "flex";
        rejectReveal.style.display = "none";
      } else {
        rejectReveal.style.opacity = String(Math.min(progress / SWIPE_THRESHOLD, 1));
        rejectReveal.style.display = "flex";
        acceptReveal.style.display = "none";
      }

      // Show label text past the label threshold.
      const activeReveal = isAccept ? acceptReveal : rejectReveal;
      const label = activeReveal.querySelector<HTMLElement>(
        ".pfin-batch-reveal-label",
      );
      if (label) {
        label.style.opacity = progress >= LABEL_THRESHOLD ? "1" : "0";
      }

      // Scale bump on icon when past threshold.
      const icon = activeReveal.querySelector<HTMLElement>(
        ".pfin-batch-reveal-icon",
      );
      if (icon) {
        icon.style.transform =
          progress >= SWIPE_THRESHOLD ? "scale(1.2)" : "scale(1)";
      }
    }

    // Border hint on the card when past threshold.
    if (progress >= SWIPE_THRESHOLD) {
      card.style.borderColor = isAccept
        ? "var(--pfin-batch-accept-color, #4a9)"
        : "var(--pfin-batch-reject-color, #e55)";
    } else {
      card.style.borderColor = "";
    }
  }

  private onPointerUp(e: PointerEvent): void {
    document.removeEventListener("pointermove", this.boundPointerMove);
    document.removeEventListener("pointerup", this.boundPointerUp);

    if (!this.dragState) return;

    const deltaX = e.clientX - this.dragState.startX;
    const { card, item, width } = this.dragState;
    const progress = Math.abs(deltaX) / width;
    const isAccept = deltaX > 0;
    const index = parseInt(item.dataset.index ?? "", 10);

    this.dragState = null;

    if (progress >= SWIPE_THRESHOLD && !isNaN(index)) {
      // Past threshold — animate off-screen and mark decided.
      const direction = isAccept ? 1 : -1;
      card.style.transition = "transform 0.3s ease-out, opacity 0.3s ease-out";
      card.style.transform = `translateX(${direction * width * 1.5}px)`;
      card.style.opacity = "0";

      // After animation, collapse the item.
      card.addEventListener(
        "transitionend",
        () => {
          item.style.transition = "max-height 0.2s ease-out, margin 0.2s ease-out, padding 0.2s ease-out";
          item.style.maxHeight = "0";
          item.style.overflow = "hidden";
          item.style.margin = "0";
          item.style.padding = "0";
        },
        { once: true },
      );

      this.decisions.set(index, isAccept);
      this.decisions = new Map(this.decisions);
    } else {
      // Snap back.
      card.style.transition = "transform 0.2s ease-out";
      card.style.transform = "translateX(0)";
      card.style.borderColor = "";

      // Reset reveal layers.
      const acceptReveal = item.querySelector<HTMLElement>(
        ".pfin-batch-reveal-accept",
      );
      const rejectReveal = item.querySelector<HTMLElement>(
        ".pfin-batch-reveal-reject",
      );
      if (acceptReveal) {
        acceptReveal.style.display = "none";
        acceptReveal.style.opacity = "0";
      }
      if (rejectReveal) {
        rejectReveal.style.display = "none";
        rejectReveal.style.opacity = "0";
      }
    }
  }

  // ---- Bulk actions ----

  private acceptAll(): void {
    for (const item of this.items) {
      const index = parseInt(item.dataset.index ?? "", 10);
      if (!isNaN(index) && !this.decisions.has(index)) {
        this.decisions.set(index, true);
        this.collapseItem(item, true);
      }
    }
    this.decisions = new Map(this.decisions);
  }

  private rejectAll(): void {
    for (const item of this.items) {
      const index = parseInt(item.dataset.index ?? "", 10);
      if (!isNaN(index) && !this.decisions.has(index)) {
        this.decisions.set(index, false);
        this.collapseItem(item, false);
      }
    }
    this.decisions = new Map(this.decisions);
  }

  private collapseItem(item: HTMLElement, accepted: boolean): void {
    const card = item.querySelector<HTMLElement>(this.cardSelector());
    if (!card) return;

    // Show the appropriate reveal briefly.
    const reveal = item.querySelector<HTMLElement>(
      accepted
        ? ".pfin-batch-reveal-accept"
        : ".pfin-batch-reveal-reject",
    );
    if (reveal) {
      reveal.style.display = "flex";
      reveal.style.opacity = "1";
    }

    // Collapse after a brief flash.
    const direction = accepted ? 1 : -1;
    card.style.transition = "transform 0.25s ease-out, opacity 0.25s ease-out";
    card.style.transform = `translateX(${direction * card.offsetWidth}px)`;
    card.style.opacity = "0";

    card.addEventListener(
      "transitionend",
      () => {
        item.style.transition =
          "max-height 0.15s ease-out, margin 0.15s ease-out";
        item.style.maxHeight = "0";
        item.style.overflow = "hidden";
        item.style.margin = "0";
        item.style.padding = "0";
      },
      { once: true },
    );
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
    "brenn-pfin-batch-swipe": BrennPfinBatchSwipe;
  }
}
