/**
 * <brenn-approval-container> — Thin shell for tool approval display.
 *
 * Renders the queue counter header and the backend-provided `formattedDisplay`
 * HTML via unsafeHTML. All interaction logic lives in embedded components
 * (e.g. <brenn-tool-approve>, <brenn-ask-user-question>) that dispatch
 * `brenn-tool-response` events.
 *
 * This component has no buttons, no keyboard handling, no interaction logic.
 */

import { LitElement, html, nothing } from "lit";
import { customElement, property } from "lit/decorators.js";
import { unsafeHTML } from "lit/directives/unsafe-html.js";

@customElement("brenn-approval-container")
export class BrennApprovalContainer extends LitElement {
  // Light DOM.
  createRenderRoot(): HTMLElement {
    return this;
  }

  @property({ type: String }) formattedDisplay = "";
  @property({ type: Boolean, reflect: true }) visible = false;
  @property({ type: Number }) queuePosition = 1;
  @property({ type: Number }) queueTotal = 1;

  render() {
    if (!this.visible) {
      return html``;
    }
    const counter =
      this.queueTotal > 1
        ? html`<span class="approval-queue-counter"
            >${this.queuePosition} of ${this.queueTotal}</span
          >`
        : nothing;
    return html`
      <div class="approval-card">
        ${counter
          ? html`<div class="approval-header">${counter}</div>`
          : nothing}
        <div class="approval-detail">
          ${unsafeHTML(this.formattedDisplay)}
        </div>
      </div>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "brenn-approval-container": BrennApprovalContainer;
  }
}
