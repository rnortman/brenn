/**
 * <brenn-pfin-batch-assign-table> — Desktop batch assignment table.
 *
 * Subclasses `<brenn-pfin-batch-table>` (reconciliation). The only structural
 * differences are the custom-element tag and the row CSS class
 * (`.pfin-batch-assign-row`). All interaction logic — ✓/✗ toggle, keyboard
 * navigation, accept/reject remaining, submit — is inherited unchanged.
 *
 * Dispatches `brenn-tool-response` CustomEvent:
 *   Submit: { decisions: [{ index: N, accepted: bool }, ...] }
 *
 * Light DOM — backend-rendered table appears directly.
 */

import { customElement } from "lit/decorators.js";
import { BrennPfinBatchTable } from "./batch-table.js";

@customElement("brenn-pfin-batch-assign-table")
export class BrennPfinBatchAssignTable extends BrennPfinBatchTable {
  /** Row selector override — assign rows use a different CSS class. */
  protected override rowSelector(): string {
    return ".pfin-batch-assign-row";
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "brenn-pfin-batch-assign-table": BrennPfinBatchAssignTable;
  }
}
