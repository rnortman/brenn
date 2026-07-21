/**
 * MenuController — shared open/close + outside-click lifecycle.
 *
 * Encapsulates the ~40 LoC of duplicated popup-menu lifecycle that
 * `input-bar.ts` and `todo-list.ts` both implement:
 *   - boolean open state
 *   - install a document listener on open, remove on close
 *   - close on outside click (pointerdown / click)
 *
 * Callers supply:
 *   - `isInMenu(e: MouseEvent) => boolean` — returns true when the
 *     click event targets an element inside the menu or trigger; used
 *     to suppress outside-click close for in-menu interactions.
 *     The full event is passed so callers can use `composedPath()` for
 *     shadow-DOM components, or `event.target` / `closest()` for light DOM.
 *   - `onClose()` — called when the controller decides the menu should
 *     close; caller updates its own state.
 *
 * The caller is responsible for tracking `open` state and calling
 * `open()` / `close()` to drive the lifecycle. The controller itself
 * does not own state; it manages only the document listener lifecycle.
 *
 * Escape handling is intentionally NOT included. Escape close semantics
 * (focus-return behavior, which caller state to update) differ per
 * component and must be implemented by each caller. Including Escape here
 * would either impose the wrong focus-return policy or require another
 * callback, with no benefit over a simple `document.addEventListener`
 * inside the component's own Escape handler.
 *
 * Usage (light DOM):
 *   private _menu = new MenuController(
 *     (e) => (e.target as Element)?.closest(".my-menu") !== null,
 *     () => { this.menuOpen = false; this._menu.close(); },
 *     { eventType: "click" },
 *   );
 *
 * Usage (shadow DOM — use composedPath):
 *   private _menu = new MenuController(
 *     (e) => e.composedPath().some(
 *       n => n instanceof Element && n.classList.contains("my-menu"),
 *     ),
 *     () => this._close(),
 *   );
 */

export interface MenuControllerOptions {
  /** DOM event type for the outside-click listener. Default: "pointerdown". */
  eventType?: "pointerdown" | "click";
}

export class MenuController {
  private readonly _isInMenu: (e: MouseEvent) => boolean;
  private readonly _onClose: () => void;
  private readonly _eventType: "pointerdown" | "click";
  private _outsideHandler: ((e: Event) => void) | null = null;

  constructor(
    isInMenu: (e: MouseEvent) => boolean,
    onClose: () => void,
    opts: MenuControllerOptions = {},
  ) {
    this._isInMenu = isInMenu;
    this._onClose = onClose;
    this._eventType = opts.eventType ?? "pointerdown";
  }

  /** Install document outside-click listener. Call when the menu opens. */
  open(): void {
    if (this._outsideHandler) {
      console.warn("MenuController.open(): already installed — call close() before open()");
      return; // already installed
    }
    this._outsideHandler = (e: Event) => {
      if (this._isInMenu(e as MouseEvent)) return;
      this._onClose();
    };
    document.addEventListener(this._eventType, this._outsideHandler);
  }

  /** Remove document outside-click listener. Call when the menu closes,
   *  and also in the host's `disconnectedCallback` to prevent leaks. */
  close(): void {
    if (this._outsideHandler) {
      document.removeEventListener(this._eventType, this._outsideHandler);
      this._outsideHandler = null;
    }
  }
}
