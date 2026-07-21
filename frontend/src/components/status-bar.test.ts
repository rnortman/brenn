// Component-level tests for BrennStatusBar.renderPermissionMode.
// The function is private; we exercise it by setting the permissionMode
// property and observing the rendered light-DOM output.

import { describe, it, expect, afterEach } from "vitest";
import "./status-bar.js";
import type { BrennStatusBar, PermissionModeState } from "./status-bar.js";

afterEach(() => {
    document.body.replaceChildren();
});

async function mount(permissionMode: PermissionModeState): Promise<BrennStatusBar> {
    const el = document.createElement("brenn-status-bar") as BrennStatusBar;
    // Leave ccState at default Idle; the unseen case relies on Idle + no perm
    // → empty render, while the other cases rely on permPart alone being
    // non-nothing to force the status div to render.
    el.permissionMode = permissionMode;
    document.body.appendChild(el);
    await el.updateComplete;
    return el;
}

describe("BrennStatusBar — renderPermissionMode", () => {
    it("unseen: renderPermissionMode returns nothing (no perm spans in status div)", async () => {
        const el = await mount({ status: "unseen" });
        // Force the status div to render via a non-empty ccState label so that
        // renderPermissionMode's return value is what keeps perm spans absent —
        // not the whole component being empty.
        el.ccState = "Thinking";
        await el.updateComplete;
        const div = el.querySelector(".status");
        expect(div).toBeTruthy();
        expect(div!.querySelector(".perm-mode")).toBeNull();
        expect(div!.querySelector(".perm-mode-warn")).toBeNull();
    });

    it("missing: renders (no mode) span with warning class", async () => {
        const el = await mount({ status: "missing" });
        const span = el.querySelector(".perm-mode-warn");
        expect(span).toBeTruthy();
        expect(span!.textContent?.trim()).toBe("(no mode)");
        expect(span!.getAttribute("title")).toContain("permission_mode");
    });

    it("seen + auto: renders 'auto' span without warning class", async () => {
        const el = await mount({
            status: "seen",
            mode: "auto",
        });
        const span = el.querySelector(".perm-mode");
        expect(span).toBeTruthy();
        expect(span!.textContent?.trim()).toBe("auto");
        expect(span!.classList.contains("perm-mode-warn")).toBe(false);
        expect(span!.getAttribute("title")).toBe("CC permission mode");
    });

    it("seen + other: renders 'other' with warning class and descriptive title", async () => {
        const el = await mount({
            status: "seen",
            mode: "other",
        });
        const span = el.querySelector(".perm-mode-warn");
        expect(span).toBeTruthy();
        expect(span!.textContent?.trim()).toBe("other");
        // Both base class and warn modifier must be present (CSS targets .perm-mode for shared styling).
        expect(span!.classList.contains("perm-mode")).toBe(true);
        const title = span!.getAttribute("title") ?? "";
        expect(title).toContain("unexpected");
        expect(title).toContain("other");
        expect(title).toContain("auto");
    });
});
