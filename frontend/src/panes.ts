/**
 * Pane infrastructure types and default bundles.
 *
 * Three separable concepts:
 * - PaneContent: what a slot can display (Chat, FilePicker, FileViewer, TodoList)
 * - Layout: pure geometry (SinglePane, TwoColumn)
 * - LayoutBundle: a layout + pane assignment per slot
 *
 * The layout component is content-agnostic — it provides geometry via slots.
 * The app maps PaneContent types to actual components in its render method.
 */

/** What a pane slot can display. */
export type PaneContent =
  | { type: "Chat" }
  | { type: "FilePicker" }
  | { type: "FileViewer" }
  | { type: "TodoList" };

/** State of a single slot in the layout.
 *  Single-parent pointer, not a stack — a second slotNavigate overwrites
 *  the parent. Intentional for the current depth-2 use case. */
export interface SlotState {
  current: PaneContent;
  /** If set, "back" navigation returns to this pane type. */
  parent: PaneContent | null;
}

/** Pure geometry — how many slots, how they're arranged. */
export type Layout =
  | { type: "SinglePane" }
  | { type: "TwoColumn" };

/** A layout + pane assignment, packaged together. */
export interface LayoutBundle {
  layout: Layout;
  /** One SlotState per slot in the layout. Length must match:
   *  SinglePane → 1, TwoColumn → 2. */
  slots: SlotState[];
}

// --- Default bundles ---

export const CHAT_ONLY: LayoutBundle = {
  layout: { type: "SinglePane" },
  slots: [{ current: { type: "Chat" }, parent: null }],
};

export const CHAT_AND_FILE: LayoutBundle = {
  layout: { type: "TwoColumn" },
  slots: [
    { current: { type: "Chat" }, parent: null },
    { current: { type: "FilePicker" }, parent: null },
  ],
};

export const CHAT_AND_TODO: LayoutBundle = {
  layout: { type: "TwoColumn" },
  slots: [
    { current: { type: "Chat" }, parent: null },
    { current: { type: "TodoList" }, parent: null },
  ],
};

// --- Slot navigation (pure functions) ---

/** Navigate a slot to a new pane type, saving current as parent.
 *  Returns a new array (immutable update for Lit reactivity). */
export function slotNavigate(
  slots: SlotState[],
  slotIndex: number,
  to: PaneContent,
): SlotState[] {
  return slots.map((s, i) =>
    i === slotIndex ? { current: to, parent: { ...s.current } } : s,
  );
}

/** Navigate a slot back to its parent. Returns unchanged array if
 *  the slot has no parent. */
export function slotBack(
  slots: SlotState[],
  slotIndex: number,
): SlotState[] {
  return slots.map((s, i) =>
    i === slotIndex && s.parent
      ? { current: { ...s.parent }, parent: null }
      : s,
  );
}

/** Reset a slot to a specific state (e.g., bundle default). */
export function slotReset(
  slots: SlotState[],
  slotIndex: number,
  to: SlotState,
): SlotState[] {
  return slots.map((s, i) => (i === slotIndex ? { ...to } : s));
}

/** Get the current pane type of a slot, or null if index is out of range. */
export function slotType(
  slots: SlotState[],
  slotIndex: number,
): PaneContent["type"] | null {
  return slots[slotIndex]?.current.type ?? null;
}

/** Create a fresh deep copy of a bundle's slot states. */
export function copyBundleSlots(bundle: LayoutBundle): SlotState[] {
  return bundle.slots.map((s) => ({
    current: { ...s.current },
    parent: s.parent ? { ...s.parent } : null,
  }));
}
