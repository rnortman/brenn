/**
 * <brenn-pfin-batch-assign-swipe> — Mobile batch assignment with swipe gestures.
 *
 * Subclasses `<brenn-pfin-batch-swipe>` (reconciliation). The only structural
 * difference is the card CSS class (`.pfin-batch-assign-card`). Swipe-item,
 * reveal, and gesture mechanics are all inherited unchanged.
 *
 * Swipe right → Accept (green reveal with ✓ + "Accept" label)
 * Swipe left  → Reject (red reveal with ✗ + "Reject" label)
 *
 * Dispatches `brenn-tool-response` CustomEvent:
 *   Submit: { decisions: [{ index: N, accepted: bool }, ...] }
 *
 * Light DOM — backend-rendered cards appear directly.
 */

import { customElement } from "lit/decorators.js";
import { BrennPfinBatchSwipe } from "./batch-swipe.js";

@customElement("brenn-pfin-batch-assign-swipe")
export class BrennPfinBatchAssignSwipe extends BrennPfinBatchSwipe {
  /** Card selector override — assign cards use a different CSS class. */
  protected override cardSelector(): string {
    return ".pfin-batch-assign-card";
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "brenn-pfin-batch-assign-swipe": BrennPfinBatchAssignSwipe;
  }
}
