import { describe, it, expect } from "vitest";
import {
  CHAT_AND_FILE,
  CHAT_AND_TODO,
  CHAT_ONLY,
  copyBundleSlots,
  slotNavigate,
  slotBack,
  slotReset,
  slotType,
  type SlotState,
} from "./panes.js";

describe("slotType", () => {
  it("returns the current type of a valid slot index", () => {
    const slots = copyBundleSlots(CHAT_AND_FILE);
    expect(slotType(slots, 0)).toBe("Chat");
    expect(slotType(slots, 1)).toBe("FilePicker");
  });

  it("returns null for out-of-range index", () => {
    const slots = copyBundleSlots(CHAT_AND_FILE);
    expect(slotType(slots, 5)).toBeNull();
    expect(slotType(slots, -1)).toBeNull();
  });

  it("returns null for empty array", () => {
    expect(slotType([], 0)).toBeNull();
  });
});

describe("slotNavigate", () => {
  it("sets current to the target and saves old current as parent", () => {
    const slots = copyBundleSlots(CHAT_AND_FILE);
    const result = slotNavigate(slots, 1, { type: "FileViewer" });

    expect(result[1].current.type).toBe("FileViewer");
    expect(result[1].parent?.type).toBe("FilePicker");
  });

  it("does not mutate the original array", () => {
    const slots = copyBundleSlots(CHAT_AND_FILE);
    const original1Type = slots[1].current.type;
    slotNavigate(slots, 1, { type: "FileViewer" });

    expect(slots[1].current.type).toBe(original1Type);
  });

  it("leaves other slots unchanged", () => {
    const slots = copyBundleSlots(CHAT_AND_FILE);
    const result = slotNavigate(slots, 1, { type: "FileViewer" });

    expect(result[0].current.type).toBe("Chat");
    expect(result[0].parent).toBeNull();
  });

  it("overwrites existing parent (depth-2, not a stack)", () => {
    const slots = copyBundleSlots(CHAT_AND_FILE);
    const step1 = slotNavigate(slots, 1, { type: "FileViewer" });
    // Navigate again — parent should be FileViewer now, not FilePicker
    const step2 = slotNavigate(step1, 1, { type: "Chat" });

    expect(step2[1].current.type).toBe("Chat");
    expect(step2[1].parent?.type).toBe("FileViewer");
  });
});

describe("slotBack", () => {
  it("restores parent as current and clears parent", () => {
    const slots = copyBundleSlots(CHAT_AND_FILE);
    const navigated = slotNavigate(slots, 1, { type: "FileViewer" });
    const result = slotBack(navigated, 1);

    expect(result[1].current.type).toBe("FilePicker");
    expect(result[1].parent).toBeNull();
  });

  it("returns unchanged array when slot has no parent", () => {
    const slots = copyBundleSlots(CHAT_AND_FILE);
    const result = slotBack(slots, 1);

    expect(result[1].current.type).toBe("FilePicker");
    expect(result[1].parent).toBeNull();
  });

  it("does not mutate the original array", () => {
    const slots = copyBundleSlots(CHAT_AND_FILE);
    const navigated = slotNavigate(slots, 1, { type: "FileViewer" });
    slotBack(navigated, 1);

    expect(navigated[1].current.type).toBe("FileViewer");
    expect(navigated[1].parent?.type).toBe("FilePicker");
  });

  it("leaves other slots unchanged", () => {
    const slots = copyBundleSlots(CHAT_AND_FILE);
    const navigated = slotNavigate(slots, 1, { type: "FileViewer" });
    const result = slotBack(navigated, 1);

    expect(result[0].current.type).toBe("Chat");
    expect(result[0].parent).toBeNull();
  });
});

describe("slotReset", () => {
  it("replaces the slot at the given index", () => {
    const slots = copyBundleSlots(CHAT_AND_FILE);
    const navigated = slotNavigate(slots, 1, { type: "FileViewer" });
    const result = slotReset(navigated, 1, {
      current: { type: "FilePicker" },
      parent: null,
    });

    expect(result[1].current.type).toBe("FilePicker");
    expect(result[1].parent).toBeNull();
  });

  it("leaves other slots unchanged", () => {
    const slots = copyBundleSlots(CHAT_AND_FILE);
    const result = slotReset(slots, 1, {
      current: { type: "Chat" },
      parent: null,
    });

    expect(result[0].current.type).toBe("Chat");
  });

  it("does not share references with the input state", () => {
    const resetTo: SlotState = {
      current: { type: "FilePicker" },
      parent: null,
    };
    const slots = copyBundleSlots(CHAT_AND_FILE);
    const result = slotReset(slots, 1, resetTo);

    // Mutating the input should not affect the result.
    resetTo.current = { type: "Chat" };
    expect(result[1].current.type).toBe("FilePicker");
  });
});

describe("copyBundleSlots", () => {
  it("produces a deep copy — mutations do not affect the bundle", () => {
    const slots = copyBundleSlots(CHAT_AND_FILE);
    slots[0].current = { type: "FileViewer" };

    expect(CHAT_AND_FILE.slots[0].current.type).toBe("Chat");
  });

  it("copies parent when present", () => {
    // Construct a bundle with a parent to verify deep copy.
    const bundle = {
      layout: { type: "TwoColumn" as const },
      slots: [
        {
          current: { type: "FileViewer" as const },
          parent: { type: "FilePicker" as const },
        },
      ],
    };
    const slots = copyBundleSlots(bundle);
    slots[0].parent = { type: "Chat" };

    expect(bundle.slots[0].parent?.type).toBe("FilePicker");
  });
});

describe("default bundles", () => {
  it("CHAT_ONLY has one slot with Chat", () => {
    expect(CHAT_ONLY.layout.type).toBe("SinglePane");
    expect(CHAT_ONLY.slots).toHaveLength(1);
    expect(CHAT_ONLY.slots[0].current.type).toBe("Chat");
  });

  it("CHAT_AND_FILE has two slots: Chat and FilePicker", () => {
    expect(CHAT_AND_FILE.layout.type).toBe("TwoColumn");
    expect(CHAT_AND_FILE.slots).toHaveLength(2);
    expect(CHAT_AND_FILE.slots[0].current.type).toBe("Chat");
    expect(CHAT_AND_FILE.slots[1].current.type).toBe("FilePicker");
  });

  it("CHAT_AND_TODO has two slots: Chat and TodoList", () => {
    expect(CHAT_AND_TODO.layout.type).toBe("TwoColumn");
    expect(CHAT_AND_TODO.slots).toHaveLength(2);
    expect(CHAT_AND_TODO.slots[0].current.type).toBe("Chat");
    expect(CHAT_AND_TODO.slots[1].current.type).toBe("TodoList");
  });
});

describe("full navigation sequence", () => {
  it("picker → viewer → back → picker", () => {
    let slots = copyBundleSlots(CHAT_AND_FILE);

    // Start: slot 1 is FilePicker
    expect(slotType(slots, 1)).toBe("FilePicker");

    // Navigate to viewer
    slots = slotNavigate(slots, 1, { type: "FileViewer" });
    expect(slotType(slots, 1)).toBe("FileViewer");
    expect(slots[1].parent?.type).toBe("FilePicker");

    // Back to picker
    slots = slotBack(slots, 1);
    expect(slotType(slots, 1)).toBe("FilePicker");
    expect(slots[1].parent).toBeNull();
  });

  it("navigate, navigate again, back goes to intermediate not original", () => {
    let slots = copyBundleSlots(CHAT_AND_FILE);

    slots = slotNavigate(slots, 1, { type: "FileViewer" });
    slots = slotNavigate(slots, 1, { type: "Chat" });

    // Parent is FileViewer (the intermediate), not FilePicker (the original)
    expect(slotType(slots, 1)).toBe("Chat");
    expect(slots[1].parent?.type).toBe("FileViewer");

    slots = slotBack(slots, 1);
    expect(slotType(slots, 1)).toBe("FileViewer");
    expect(slots[1].parent).toBeNull();
  });

  it("conversation switch resets everything", () => {
    let slots = copyBundleSlots(CHAT_AND_FILE);
    slots = slotNavigate(slots, 1, { type: "FileViewer" });

    // Simulate conversation switch: reset to bundle defaults
    slots = copyBundleSlots(CHAT_AND_FILE);
    expect(slotType(slots, 0)).toBe("Chat");
    expect(slotType(slots, 1)).toBe("FilePicker");
    expect(slots[1].parent).toBeNull();
  });

  it("todo list in secondary slot, switch to files and back", () => {
    let slots = copyBundleSlots(CHAT_AND_TODO);

    // Start: slot 1 is TodoList
    expect(slotType(slots, 1)).toBe("TodoList");

    // Switch to FilePicker via reset (tab switch)
    slots = slotReset(slots, 1, {
      current: { type: "FilePicker" },
      parent: null,
    });
    expect(slotType(slots, 1)).toBe("FilePicker");

    // Switch back to TodoList
    slots = slotReset(slots, 1, {
      current: { type: "TodoList" },
      parent: null,
    });
    expect(slotType(slots, 1)).toBe("TodoList");
  });

  it("SinglePane: navigate to TodoList and back", () => {
    let slots = copyBundleSlots(CHAT_ONLY);

    // Navigate to TodoList
    slots = slotNavigate(slots, 0, { type: "TodoList" });
    expect(slotType(slots, 0)).toBe("TodoList");
    expect(slots[0].parent?.type).toBe("Chat");

    // Back to Chat
    slots = slotBack(slots, 0);
    expect(slotType(slots, 0)).toBe("Chat");
    expect(slots[0].parent).toBeNull();
  });
});
