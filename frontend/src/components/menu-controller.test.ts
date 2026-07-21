// @vitest-environment happy-dom
//
// Unit tests for MenuController — covers the four branches called out in the
// class docstring: open idempotency, close-before-open no-op, isInMenu
// suppression, and listener teardown on close.

import { describe, it, expect, afterEach, vi } from "vitest";
import { MenuController } from "./menu-controller.js";

afterEach(() => {
  // Nothing to clean up at the module level; each test constructs its own
  // controller and explicitly calls close() where needed.
});

describe("MenuController — open idempotency", () => {
  it("calling open() twice installs the listener only once and warns on second call", () => {
    const addSpy = vi.spyOn(document, "addEventListener");
    const removeSpy = vi.spyOn(document, "removeEventListener");
    const warnSpy = vi.spyOn(console, "warn");

    const mc = new MenuController(
      () => false,
      () => {},
    );
    mc.open();
    mc.open(); // second call must warn and be a no-op

    // Exactly one addEventListener call for "pointerdown" (the default event type).
    const addCalls = addSpy.mock.calls.filter(([type]) => type === "pointerdown");
    expect(addCalls.length).toBe(1);

    // Second open() must emit a console.warn containing "already installed".
    expect(warnSpy).toHaveBeenCalledOnce();
    expect(warnSpy.mock.calls[0]![0]).toContain("already installed");

    // Clean up.
    mc.close();
    addSpy.mockRestore();
    removeSpy.mockRestore();
    warnSpy.mockRestore();
  });
});

describe("MenuController — close before open", () => {
  it("calling close() before open() is a no-op (does not throw)", () => {
    const removeSpy = vi.spyOn(document, "removeEventListener");

    const mc = new MenuController(
      () => false,
      () => {},
    );
    expect(() => mc.close()).not.toThrow();

    // No removeEventListener call was made.
    const removeCalls = removeSpy.mock.calls.filter(([type]) => type === "pointerdown");
    expect(removeCalls.length).toBe(0);

    removeSpy.mockRestore();
  });
});

describe("MenuController — isInMenu suppression", () => {
  it("onClose is NOT called when isInMenu returns true", () => {
    const onClose = vi.fn();
    const mc = new MenuController(
      () => true, // all clicks are "in menu"
      onClose,
    );
    mc.open();

    // Dispatch a pointerdown event on the document.
    document.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true }));

    expect(onClose).not.toHaveBeenCalled();
    mc.close();
  });

  it("onClose IS called when isInMenu returns false", () => {
    const onClose = vi.fn();
    const mc = new MenuController(
      () => false, // no click is "in menu"
      onClose,
    );
    mc.open();

    document.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true }));

    expect(onClose).toHaveBeenCalledOnce();
    mc.close();
  });
});

describe("MenuController — listener teardown on close", () => {
  it("close() removes the outside-click listener so further events do not fire onClose", () => {
    const onClose = vi.fn();
    const mc = new MenuController(
      () => false,
      onClose,
    );
    mc.open();
    mc.close();

    // After close, a pointerdown must not reach the handler.
    document.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true }));

    expect(onClose).not.toHaveBeenCalled();
  });

  it("click eventType option routes the listener to 'click', not 'pointerdown'", () => {
    const addSpy = vi.spyOn(document, "addEventListener");
    const onClose = vi.fn();

    const mc = new MenuController(() => false, onClose, { eventType: "click" });
    mc.open();

    const pointerCalls = addSpy.mock.calls.filter(([type]) => type === "pointerdown");
    const clickCalls = addSpy.mock.calls.filter(([type]) => type === "click");
    expect(pointerCalls.length).toBe(0);
    expect(clickCalls.length).toBe(1);

    // A click event triggers onClose.
    document.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    expect(onClose).toHaveBeenCalledOnce();

    mc.close();
    addSpy.mockRestore();
  });
});
