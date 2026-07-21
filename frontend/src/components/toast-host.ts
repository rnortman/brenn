/**
 * <brenn-toast-host> — Transient toast stack (Phase 4 of recurring-task UI).
 *
 * Minimal Lit element mounted once at the app root. Owns a queue of
 * toasts shown fixed-bottom-right with CSS transitions. Push via
 * `toastHost.push({ text, ttlMs })` — auto-dismiss via `setTimeout`,
 * click-to-dismiss, visible cap of 4 with excess queued.
 *
 * Hand-rolled rather than pulled from a toast dep: the catalog aesthetic
 * goal (see `CLAUDE.md`'s "Catalog, Not Generation") plus the small
 * feature surface (queue, cap, fixed stack, auto-dismiss) make a ~100
 * line Lit element cheaper than integrating + theming an external lib.
 */

import { LitElement, html, css, nothing } from "lit";
import { customElement, state } from "lit/decorators.js";
import { repeat } from "lit/directives/repeat.js";

interface Toast {
  id: number;
  text: string;
  /** Timeout handle so we can cancel on manual dismiss. */
  timer: ReturnType<typeof setTimeout>;
}

/** Max toasts visible at once. Excess items wait in the pending queue. */
const VISIBLE_CAP = 4;

@customElement("brenn-toast-host")
export class BrennToastHost extends LitElement {
  /** Currently visible toasts. */
  @state() private visible: Toast[] = [];
  /** Queued toasts waiting for a slot. */
  private pending: { text: string; ttlMs: number }[] = [];
  /** Monotonic id source (prevents DOM node recycle on rapid push). */
  private nextId = 1;

  static styles = css`
    :host {
      position: fixed;
      right: 1rem;
      bottom: 1rem;
      display: flex;
      flex-direction: column-reverse;
      gap: 0.5rem;
      pointer-events: none;
      z-index: 10000;
    }

    .toast {
      pointer-events: auto;
      background: #1e1e34;
      border: 1px solid #4a6fa5;
      color: #d0d0d8;
      padding: 0.5rem 0.9rem;
      border-radius: 4px;
      font-size: 0.85rem;
      box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4);
      max-width: 320px;
      cursor: pointer;
      animation: brenn-toast-in 0.15s ease-out;
    }

    @keyframes brenn-toast-in {
      from { opacity: 0; transform: translateY(8px); }
      to   { opacity: 1; transform: translateY(0); }
    }

    @media (prefers-reduced-motion: reduce) {
      .toast {
        animation: none;
      }
    }
  `;

  disconnectedCallback(): void {
    super.disconnectedCallback();
    // Cancel each in-flight auto-dismiss so the timer can't fire against
    // a detached host (test cases tear hosts down between runs; prod
    // hosts live for the page lifetime). Drop pending too — anything
    // queued is implicitly cancelled along with the component.
    for (const t of this.visible) {
      clearTimeout(t.timer);
    }
    this.visible = [];
    this.pending = [];
  }

  /** Enqueue a toast. If the visible cap is full, it waits until a slot frees. */
  push(opts: { text: string; ttlMs?: number }): void {
    const ttlMs = opts.ttlMs ?? 3000;
    if (this.visible.length >= VISIBLE_CAP) {
      this.pending.push({ text: opts.text, ttlMs });
      return;
    }
    this._spawn(opts.text, ttlMs);
  }

  private _spawn(text: string, ttlMs: number): void {
    const id = this.nextId++;
    const timer = setTimeout(() => this._dismiss(id), ttlMs);
    this.visible = [...this.visible, { id, text, timer }];
  }

  private _dismiss(id: number): void {
    const idx = this.visible.findIndex((t) => t.id === id);
    if (idx === -1) return;
    clearTimeout(this.visible[idx].timer);
    const next = [...this.visible];
    next.splice(idx, 1);
    this.visible = next;
    // Promote one pending toast if any waiting.
    const queued = this.pending.shift();
    if (queued) {
      this._spawn(queued.text, queued.ttlMs);
    }
  }

  render() {
    if (this.visible.length === 0) return nothing;
    // Key by toast id: dismissing a non-bottom toast should remove the
    // clicked DOM node rather than letting Lit recycle it for the toast
    // beneath (which also breaks the CSS enter animation on the wrong
    // element). The `nextId` monotonic counter enforces the invariant.
    return repeat(
      this.visible,
      (t) => t.id,
      (t) => html`
        <div
          class="toast"
          role="status"
          aria-live="polite"
          @click=${() => this._dismiss(t.id)}
        >${t.text}</div>
      `,
    );
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "brenn-toast-host": BrennToastHost;
  }
}
