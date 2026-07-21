/**
 * <brenn-pane-layout> — Content-agnostic layout geometry with resize handle.
 *
 * Provides named slot containers (slot-0, slot-1) arranged according to the
 * layout type. The parent component decides what content goes in each slot.
 * This component only handles geometry, resize, and sizing.
 *
 * Shadow DOM for style encapsulation.
 */

import { LitElement, html, css, nothing } from "lit";
import { customElement, property, query, state } from "lit/decorators.js";
import type { Layout } from "../panes.js";

const MIN_PANE_PX = 200;
const DEFAULT_RATIO = 0.5;

@customElement("brenn-pane-layout")
export class BrennPaneLayout extends LitElement {
  /** Which geometry to use. Only set via property binding (.layout=). */
  @property() layout: Layout["type"] = "TwoColumn";

  /** Whether the secondary slot (slot-1) is visible. When false,
   *  primary fills the width and the resize handle is hidden. */
  @property({ type: Boolean }) secondaryVisible = true;

  /** Fraction of width allocated to the primary slot (0.0–1.0). */
  @property({ type: Number }) splitRatio = DEFAULT_RATIO;

  /** True while the user is dragging the resize handle. */
  @state() private dragging = false;

  /** Primary slot element, for setting its flex via the CSSOM (CSP-safe). */
  @query(".pane-primary") private primarySlot?: HTMLElement;

  // Bound handlers for document-level listeners during drag.
  private _onMouseMove: ((e: MouseEvent) => void) | null = null;
  private _onMouseUp: ((e: MouseEvent) => void) | null = null;
  private _onTouchMove: ((e: TouchEvent) => void) | null = null;
  private _onTouchEnd: (() => void) | null = null;

  static styles = css`
    :host {
      display: flex;
      flex: 1;
      min-height: 0;
      min-width: 0;
    }

    .pane-layout {
      display: flex;
      flex: 1;
      min-height: 0;
      min-width: 0;
    }

    .two-column {
      flex-direction: row;
    }

    .single-pane {
      flex-direction: row;
    }

    .pane-slot {
      min-width: 0;
      min-height: 0;
      overflow: hidden;
      display: flex;
      flex-direction: column;
    }

    /* Static fill used by the single-pane and secondary slots. Replaces an
       inline style="flex: 1" so the document works under CSP style-src 'self'. */
    .pane-fill {
      flex: 1;
    }

    /* Primary slot in two-column mode. The flex value is variable (depends on
       the drag ratio); it is supplied via the --pane-flex custom property, set
       through the per-property CSSOM API (which CSP does not gate), never via an
       inline style= attribute. */
    .pane-primary {
      flex: var(--pane-flex, 1);
    }

    .pane-resize-handle {
      flex: 0 0 4px;
      background: #2a2a40;
      cursor: col-resize;
      position: relative;
    }

    .pane-resize-handle:hover,
    .pane-resize-handle.dragging {
      background: #4a6fa5;
    }

    /* 24px hit area for comfortable targeting, 4px visual line.
       Biased rightward so it doesn't overlap the primary pane's scrollbar. */
    .pane-resize-handle::before {
      content: "";
      position: absolute;
      top: 0;
      bottom: 0;
      left: -2px;
      right: -18px;
    }

  `;

  render() {
    if (this.layout === "SinglePane") {
      return html`
        <div class="pane-layout single-pane">
          <div class="pane-slot pane-fill">
            <slot name="slot-0"></slot>
          </div>
        </div>
      `;
    }

    // TwoColumn
    const showSecondary = this.secondaryVisible;

    return html`
      <div class="pane-layout two-column">
        <div class="pane-slot pane-primary">
          <slot name="slot-0"></slot>
        </div>
        ${showSecondary
          ? html`
              <div
                class="pane-resize-handle ${this.dragging ? "dragging" : ""}"
                @mousedown=${this._handleMouseDown}
                @touchstart=${this._handleTouchStart}
                @dblclick=${this._handleDoubleClick}
              ></div>
              <div class="pane-slot pane-secondary pane-fill">
                <slot name="slot-1"></slot>
              </div>
            `
          : nothing}
      </div>
    `;
  }

  /**
   * Drive the primary slot's flex via the `--pane-flex` custom property using
   * the per-property CSSOM API. This is CSP-safe (style-src 'self' gates inline
   * `style=` attributes, not individual `style.setProperty` calls), so the
   * split ratio stays adjustable without `'unsafe-inline'`.
   */
  updated(changed: Map<PropertyKey, unknown>): void {
    // Only the flex inputs matter; skip re-renders driven by other reactive
    // state (e.g. `dragging`), which don't affect the ratio.
    if (
      !changed.has("layout") &&
      !changed.has("secondaryVisible") &&
      !changed.has("splitRatio")
    ) {
      return;
    }
    const el = this.primarySlot;
    if (!el) return;
    const flex =
      this.layout === "TwoColumn" && this.secondaryVisible
        ? `0 0 ${this.splitRatio * 100}%`
        : "1";
    el.style.setProperty("--pane-flex", flex);
  }

  disconnectedCallback(): void {
    super.disconnectedCallback();
    this._cleanupDrag();
  }

  // --- Mouse resize ---

  private _handleMouseDown(e: MouseEvent): void {
    e.preventDefault();
    this.dragging = true;
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";

    this._onMouseMove = (ev: MouseEvent) => this._updateRatio(ev.clientX);
    this._onMouseUp = () => this._endDrag();

    document.addEventListener("mousemove", this._onMouseMove);
    document.addEventListener("mouseup", this._onMouseUp);
  }

  // --- Touch resize ---

  private _handleTouchStart(e: TouchEvent): void {
    if (e.touches.length !== 1) return;
    e.preventDefault();
    this.dragging = true;

    this._onTouchMove = (ev: TouchEvent) => {
      if (ev.touches.length === 1) {
        this._updateRatio(ev.touches[0].clientX);
      }
    };
    this._onTouchEnd = () => this._endDrag();

    document.addEventListener("touchmove", this._onTouchMove, {
      passive: false,
    });
    document.addEventListener("touchend", this._onTouchEnd);
    document.addEventListener("touchcancel", this._onTouchEnd);
  }

  // --- Double-click reset ---

  private _handleDoubleClick(): void {
    this.splitRatio = DEFAULT_RATIO;
    this._emitRatioChanged();
  }

  // --- Drag internals ---

  private _updateRatio(clientX: number): void {
    const container = this.renderRoot.querySelector(
      ".pane-layout",
    ) as HTMLElement | null;
    if (!container) return;

    const rect = container.getBoundingClientRect();
    const totalWidth = rect.width;
    if (totalWidth <= 0) return;

    const minRatio = MIN_PANE_PX / totalWidth;
    const maxRatio = 1 - MIN_PANE_PX / totalWidth;

    let ratio = (clientX - rect.left) / totalWidth;
    ratio = Math.max(minRatio, Math.min(maxRatio, ratio));
    this.splitRatio = ratio;
  }

  private _endDrag(): void {
    this.dragging = false;
    document.body.style.cursor = "";
    document.body.style.userSelect = "";
    this._cleanupDrag();
    this._emitRatioChanged();
  }

  private _cleanupDrag(): void {
    if (this._onMouseMove) {
      document.removeEventListener("mousemove", this._onMouseMove);
      this._onMouseMove = null;
    }
    if (this._onMouseUp) {
      document.removeEventListener("mouseup", this._onMouseUp);
      this._onMouseUp = null;
    }
    if (this._onTouchMove) {
      document.removeEventListener("touchmove", this._onTouchMove);
      this._onTouchMove = null;
    }
    if (this._onTouchEnd) {
      document.removeEventListener("touchend", this._onTouchEnd);
      document.removeEventListener("touchcancel", this._onTouchEnd);
      this._onTouchEnd = null;
    }
  }

  private _emitRatioChanged(): void {
    this.dispatchEvent(
      new CustomEvent("split-ratio-changed", {
        detail: { ratio: this.splitRatio },
        bubbles: true,
        composed: true,
      }),
    );
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "brenn-pane-layout": BrennPaneLayout;
  }
}
