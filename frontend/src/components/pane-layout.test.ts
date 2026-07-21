// @vitest-environment happy-dom
//
// Pins the CSSOM flex path in BrennPaneLayout.
//
// Under CSP style-src 'self' the primary slot's flex ratio cannot be an inline
// `style=` attribute; it is driven through the `--pane-flex` custom property via
// `el.style.setProperty(...)` in `updated()`. These tests lock that mechanism:
// if `updated()` stops firing, is mis-gated, or the `@query(".pane-primary")`
// binding breaks, the splitter silently stops resizing.

import { afterEach, describe, expect, it } from "vitest";
import "./pane-layout.js";
import type { BrennPaneLayout } from "./pane-layout.js";

afterEach(() => {
    document.body.replaceChildren();
});

async function mount(
    props: Partial<
        Pick<BrennPaneLayout, "layout" | "secondaryVisible" | "splitRatio">
    >,
): Promise<BrennPaneLayout> {
    const el = document.createElement("brenn-pane-layout") as BrennPaneLayout;
    if (props.layout !== undefined) el.layout = props.layout;
    if (props.secondaryVisible !== undefined)
        el.secondaryVisible = props.secondaryVisible;
    if (props.splitRatio !== undefined) el.splitRatio = props.splitRatio;
    document.body.appendChild(el);
    await el.updateComplete;
    return el;
}

function primaryFlex(el: BrennPaneLayout): string {
    const primary = el.renderRoot.querySelector(
        ".pane-primary",
    ) as HTMLElement | null;
    expect(primary, ".pane-primary should exist in TwoColumn layout").not.toBeNull();
    return primary!.style.getPropertyValue("--pane-flex");
}

describe("BrennPaneLayout — --pane-flex CSSOM driving", () => {
    it("TwoColumn + secondaryVisible encodes the split ratio", async () => {
        const el = await mount({
            layout: "TwoColumn",
            secondaryVisible: true,
            splitRatio: 0.3,
        });
        expect(primaryFlex(el)).toBe("0 0 30%");
    });

    it("re-renders update --pane-flex when splitRatio changes (drag step)", async () => {
        const el = await mount({
            layout: "TwoColumn",
            secondaryVisible: true,
            splitRatio: 0.3,
        });
        expect(primaryFlex(el)).toBe("0 0 30%");
        el.splitRatio = 0.6;
        await el.updateComplete;
        expect(primaryFlex(el)).toBe("0 0 60%");
    });

    it("TwoColumn + secondary hidden falls back to flex 1", async () => {
        const el = await mount({
            layout: "TwoColumn",
            secondaryVisible: false,
            splitRatio: 0.3,
        });
        expect(primaryFlex(el)).toBe("1");
    });

    it("SinglePane has no .pane-primary and updated() does not throw", async () => {
        const el = await mount({ layout: "SinglePane" });
        // The guard in updated() (`if (!el) return`) makes this a no-op; mounting
        // without an exception is the assertion. There is no .pane-primary here.
        expect(el.renderRoot.querySelector(".pane-primary")).toBeNull();
        // Force another update cycle to exercise the guard explicitly.
        el.splitRatio = 0.7;
        await expect(el.updateComplete).resolves.not.toThrow();
    });
});
