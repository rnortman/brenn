// @vitest-environment happy-dom
import { describe, it, expect, afterEach } from "vitest";
import "./file-viewer.js";
import type { BrennFileViewer } from "./file-viewer.js";

afterEach(() => {
  document.body.replaceChildren();
});

describe("brenn-file-viewer frontmatter rendering", () => {
  it("includes the frontmatter element when renderedHtml carries .fm-block", async () => {
    const viewer = document.createElement(
      "brenn-file-viewer",
    ) as BrennFileViewer;
    viewer.show(
      "task.md",
      `<aside class="fm-block"><dl class="fm-list"><div class="fm-row"><dt>status</dt><dd>in_progress</dd></div></dl></aside><h1>Body</h1>`,
      "---\nstatus: in_progress\n---\n# Body\n",
      null,
      null,
    );
    viewer.visible = true;
    document.body.appendChild(viewer);
    await viewer.updateComplete;

    // The Shadow DOM should contain the .fm-block element rendered via
    // unsafeHTML, alongside the body's <h1>. We verify both by querying
    // through shadowRoot — this catches the `frontmatterStyles` import
    // being deleted from `file-viewer.ts` (the markup hooks would
    // disappear) but does NOT catch CSS drift between the Rust
    // FRONTMATTER_CSS and the generated TS template; that's enforced
    // by the build step (`make frontend-css` regenerates the file from
    // the same Rust constant).
    const fm = viewer.shadowRoot!.querySelector(".fm-block");
    expect(fm).toBeTruthy();
    const dt = viewer.shadowRoot!.querySelector(".fm-list dt");
    expect(dt?.textContent).toBe("status");
    const h1 = viewer.shadowRoot!.querySelector("h1");
    expect(h1?.textContent).toBe("Body");

    // Adopted stylesheets pin the wiring: Lit attaches each `css`
    // template (markdownStyles + frontmatterStyles + the inline css``)
    // to `shadowRoot.adoptedStyleSheets`. If the frontmatterStyles
    // import is dropped from `file-viewer.ts`, the count goes down.
    // We only assert "more than one" because the inline css`` block
    // and markdownStyles are also there.
    const sheets = viewer.shadowRoot!.adoptedStyleSheets;
    expect(sheets.length).toBeGreaterThanOrEqual(3);
  });
});
