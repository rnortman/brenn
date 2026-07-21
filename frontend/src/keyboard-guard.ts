/**
 * keyboard-guard.ts — Shared module for protecting keyboard shortcuts in
 * approval dialogs from interfering with the chat input.
 *
 * Rules:
 * 1. If the chat input has text, no dialog may intercept keyboard events.
 * 2. Dialogs must wait 500ms after mount before intercepting, even if chat is empty.
 *
 * Usage:
 * - input-bar.ts calls setChatHasText() whenever the input changes.
 * - Approval components call registerMount/unregisterMount in lifecycle hooks.
 * - Before handling a document-level keydown, components call canInterceptKeyboard().
 */

/** Grace period before a newly-mounted component may intercept keyboard. */
export const MOUNT_GRACE_MS = 500;

/** Whether the chat input currently has text. */
let chatHasText = false;

/** Per-component mount timestamps. WeakMap avoids leaks on disconnect. */
const mountTimes = new WeakMap<Element, number>();

/** Called by input-bar whenever its text content changes. */
export function setChatHasText(value: boolean): void {
  chatHasText = value;
}

/** Called in connectedCallback of approval components. */
export function registerMount(el: Element): void {
  mountTimes.set(el, Date.now());
}

/** Called in disconnectedCallback of approval components. */
export function unregisterMount(el: Element): void {
  mountTimes.delete(el);
}

/**
 * Returns true if the component is allowed to intercept keyboard events.
 * False if the chat input has text or the mount grace period hasn't elapsed.
 */
export function canInterceptKeyboard(el: Element): boolean {
  if (chatHasText) return false;
  const mounted = mountTimes.get(el);
  if (mounted === undefined) return false;
  return (Date.now() - mounted) >= MOUNT_GRACE_MS;
}

/**
 * Returns true if `e` originated inside `host`'s DOM subtree, traversing
 * shadow boundaries via `composedPath()`. Returns false (fail-closed) if the
 * path is empty (e.g. a synthetic event dispatched on a detached node).
 *
 * Works for both light-DOM and open-shadow-DOM hosts.
 */
export function eventOriginatedInside(e: Event, host: Element): boolean {
  const path = e.composedPath();
  if (path.length === 0) return false;
  return path.includes(host);
}
