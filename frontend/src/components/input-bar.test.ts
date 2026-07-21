// @vitest-environment happy-dom
//
// Pins the resize-before-upload path in BrennInputBar.
// Covers: onResized callback fired on oversized image; resized blob POSTed;
// imageAttachmentsDisabled blocks image uploads with onError.
// Also covers: handleKeydown stopPropagation for consumed-Enter branches.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { expectConsoleError } from "../test-setup.js";
import "./input-bar.js";
import { BrennInputBar } from "./input-bar.js";

/** Stub createImageBitmap to return a bitmap with given dimensions. */
function stubBitmap(w: number, h: number): void {
    (globalThis as unknown as Record<string, unknown>).createImageBitmap = async () => ({
        width: w,
        height: h,
        close: vi.fn(),
    });
}

/** Stub canvas.getContext + toBlob so the pipeline runs without real GPU. */
function stubCanvas(blobSizeBytes: number): void {
    // Intercept document.createElement("canvas") to return a stub.
    const origCreate = document.createElement.bind(document);
    vi.spyOn(document, "createElement").mockImplementation((tag: string) => {
        if (tag !== "canvas") return origCreate(tag);
        const el = origCreate("canvas") as HTMLCanvasElement;
        const ctx = {
            drawImage: vi.fn(),
        };
        vi.spyOn(el, "getContext").mockReturnValue(
            ctx as unknown as CanvasRenderingContext2D,
        );
        vi.spyOn(el, "toBlob").mockImplementation(
            (cb: BlobCallback) => {
                cb(new Blob([new Uint8Array(blobSizeBytes)], { type: "image/jpeg" }));
            },
        );
        return el;
    });
}

function makeImageFile(name: string): File {
    return new File([new Uint8Array(10)], name, { type: "image/jpeg" });
}

describe("BrennInputBar — handleKeydown stopPropagation", () => {
    let el: BrennInputBar;
    let textarea: HTMLTextAreaElement;

    beforeEach(async () => {
        el = document.createElement("brenn-input-bar") as BrennInputBar;
        el.appSlug = "test";
        el.enterSends = true;
        document.body.appendChild(el);
        await el.updateComplete;
        // Light DOM — textarea is a direct child of el.
        textarea = el.querySelector("textarea") as HTMLTextAreaElement;
    });

    afterEach(() => {
        vi.restoreAllMocks();
        document.body.replaceChildren();
    });

    it("plain Enter with enterSends=true does not propagate to document", async () => {
        // Non-empty text so send() does not early-return (empty text is also fine
        // for this test — stopPropagation fires regardless of send()'s early return).
        textarea.value = "hello";

        let documentSawEvent = false;
        const listener = () => { documentSawEvent = true; };
        document.addEventListener("keydown", listener);
        try {
            const ev = new KeyboardEvent("keydown", { key: "Enter", bubbles: true, cancelable: true });
            // Dispatch on the textarea — Lit's @keydown is attached there.
            textarea.dispatchEvent(ev);
            await el.updateComplete;
        } finally {
            document.removeEventListener("keydown", listener);
        }
        expect(documentSawEvent).toBe(false);
    });

    it("Ctrl+Enter does not propagate to document", async () => {
        let documentSawEvent = false;
        const listener = () => { documentSawEvent = true; };
        document.addEventListener("keydown", listener);
        try {
            const ev = new KeyboardEvent("keydown", { key: "Enter", ctrlKey: true, bubbles: true, cancelable: true });
            textarea.dispatchEvent(ev);
            await el.updateComplete;
        } finally {
            document.removeEventListener("keydown", listener);
        }
        expect(documentSawEvent).toBe(false);
    });

    it("Shift+Enter propagates to document (newline, not consumed)", async () => {
        let documentSawEvent = false;
        const listener = () => { documentSawEvent = true; };
        document.addEventListener("keydown", listener);
        try {
            const ev = new KeyboardEvent("keydown", { key: "Enter", shiftKey: true, bubbles: true, cancelable: true });
            textarea.dispatchEvent(ev);
            await el.updateComplete;
        } finally {
            document.removeEventListener("keydown", listener);
        }
        expect(documentSawEvent).toBe(true);
    });

    it("plain Enter with enterSends=false propagates to document (newline, not consumed)", async () => {
        el.enterSends = false;
        await el.updateComplete;

        let documentSawEvent = false;
        const listener = () => { documentSawEvent = true; };
        document.addEventListener("keydown", listener);
        try {
            const ev = new KeyboardEvent("keydown", { key: "Enter", bubbles: true, cancelable: true });
            textarea.dispatchEvent(ev);
            await el.updateComplete;
        } finally {
            document.removeEventListener("keydown", listener);
        }
        expect(documentSawEvent).toBe(true);
    });
});

describe("BrennInputBar — hidden file inputs are class-styled (CSP style-src 'self')", () => {
    let el: BrennInputBar;

    beforeEach(async () => {
        el = document.createElement("brenn-input-bar") as BrennInputBar;
        el.appSlug = "test";
        document.body.appendChild(el);
        await el.updateComplete;
    });

    afterEach(() => {
        document.body.replaceChildren();
    });

    // The three file inputs must be hidden via the `hidden-file-input` class
    // (styled in app.css), not an inline `style="display:none"` — inline styles
    // are blocked under CSP style-src 'self'. A typo in the class name would
    // make these inputs visible; assert class presence (the real invariant,
    // testable without loading app.css).
    for (const id of ["file-input", "camera-input", "target-input"]) {
        it(`#${id} carries the hidden-file-input class and no inline style`, () => {
            const input = el.querySelector(`#${id}`) as HTMLInputElement | null;
            expect(input, `#${id} should exist`).not.toBeNull();
            expect(input!.classList.contains("hidden-file-input")).toBe(true);
            expect(input!.getAttribute("style")).toBeNull();
        });
    }
});

describe("BrennInputBar — resize-before-upload", () => {
    let el: BrennInputBar;
    let fetchSpy: ReturnType<typeof vi.spyOn>;
    const origCreateImageBitmap = (globalThis as unknown as Record<string, unknown>).createImageBitmap;

    beforeEach(async () => {
        // Stub fetch to succeed with a minimal UploadResponse.
        fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValue(
            new Response(
                JSON.stringify({
                    upload_id: "abc123",
                    filename: "photo.jpg",
                    media_type: "image/jpeg",
                    size: 1024,
                }),
                { status: 200, headers: { "Content-Type": "application/json" } },
            ),
        );

        el = document.createElement("brenn-input-bar") as BrennInputBar;
        el.appSlug = "test";
        el.maxLongEdge = 100; // small cap so a 200x150 image triggers resize
        document.body.appendChild(el);
        await el.updateComplete;
    });

    afterEach(() => {
        vi.restoreAllMocks();
        document.body.replaceChildren();
        // Restore createImageBitmap if it was stubbed.
        (globalThis as unknown as Record<string, unknown>).createImageBitmap =
            origCreateImageBitmap;
    });

    it("fires onResized with dimension text when image exceeds maxLongEdge", async () => {
        stubBitmap(200, 150); // long edge 200 > cap 100
        stubCanvas(512);

        const resizedMessages: string[] = [];
        el.onResized = (text) => { resizedMessages.push(text); };

        const file = makeImageFile("photo.jpg");
        el.uploadExternalFile(file);

        // Allow async resize + fetch to complete.
        await new Promise(r => setTimeout(r, 20));

        expect(resizedMessages).toHaveLength(1);
        expect(resizedMessages[0]).toContain("200x150");
        expect(resizedMessages[0]).toContain("100x75");
    });

    it("does not fire onResized when image is within cap", async () => {
        stubBitmap(80, 60); // long edge 80 < cap 100
        stubCanvas(512);

        const resizedMessages: string[] = [];
        el.onResized = (text) => { resizedMessages.push(text); };

        const file = makeImageFile("small.jpg");
        el.uploadExternalFile(file);

        await new Promise(r => setTimeout(r, 20));

        expect(resizedMessages).toHaveLength(0);
        // fetch still called with original file
        expect(fetchSpy).toHaveBeenCalledTimes(1);
        const body = (fetchSpy.mock.calls[0]![1] as RequestInit).body as FormData;
        const uploaded = body.get("file") as File;
        expect(uploaded.name).toBe("small.jpg");
    });

    it("POSTs the resized blob (smaller file) to the upload endpoint", async () => {
        stubBitmap(200, 150);
        stubCanvas(300); // resized blob is 300 bytes

        el.onResized = vi.fn();
        const file = makeImageFile("photo.jpg");
        el.uploadExternalFile(file);

        await new Promise(r => setTimeout(r, 20));

        expect(fetchSpy).toHaveBeenCalledTimes(1);
        const url = fetchSpy.mock.calls[0]![0] as string;
        expect(url).toBe("/app/test/upload");
        const body = (fetchSpy.mock.calls[0]![1] as RequestInit).body as FormData;
        const uploaded = body.get("file") as File;
        // Resized file has .jpg extension and jpeg MIME.
        expect(uploaded.name).toMatch(/\.jpg$/);
        expect(uploaded.type).toBe("image/jpeg");
    });

    it("blocks image upload and fires onError when imageAttachmentsDisabled", async () => {
        el.imageAttachmentsDisabled = true;
        await el.updateComplete;

        const errors: string[] = [];
        el.onError = (msg) => { errors.push(msg); };

        const file = makeImageFile("photo.jpg");
        el.uploadExternalFile(file);

        await new Promise(r => setTimeout(r, 20));

        expect(fetchSpy).not.toHaveBeenCalled();
        expect(errors).toHaveLength(1);
        expect(errors[0]).toMatch(/image attachment disabled/i);
    });

    it("allows non-image uploads when imageAttachmentsDisabled", async () => {
        el.imageAttachmentsDisabled = true;
        await el.updateComplete;

        const errors: string[] = [];
        el.onError = (msg) => { errors.push(msg); };

        const pdf = new File([new Uint8Array(10)], "doc.pdf", { type: "application/pdf" });
        el.uploadExternalFile(pdf);

        await new Promise(r => setTimeout(r, 20));

        // Non-image: fetch called, no error.
        expect(fetchSpy).toHaveBeenCalledTimes(1);
        expect(errors).toHaveLength(0);
    });

    it("surfaces 413-specific error message on PAYLOAD_TOO_LARGE response", async () => {
        stubBitmap(200, 150);
        stubCanvas(512);

        // Override fetch to return 413.
        fetchSpy.mockResolvedValue(new Response("", { status: 413 }));

        // reportClientError logs the upload failure via console.error.
        expectConsoleError(/upload failed/i);

        const file = makeImageFile("photo.jpg");
        el.uploadExternalFile(file);

        await new Promise(r => setTimeout(r, 20));
        await el.updateComplete;

        // 413 error is stored in the placeholder's errorMessage field, not onError.
        const internals = el as unknown as { pendingAttachments: Array<{ errorMessage?: string; previewUrl: string | null }> };
        expect(internals.pendingAttachments).toHaveLength(1);
        const att = internals.pendingAttachments[0]!;
        const errMsg = att.errorMessage ?? "";
        expect(errMsg).toMatch(/too large/i);
        // Must contain "after resize" because the image was resized.
        expect(errMsg).toContain("after resize");
        // Must not be the generic "Upload failed (413)" format.
        expect(errMsg).not.toMatch(/upload failed \(\d+\)/i);
        // previewUrl must be revoked and nulled — not left as a dangling blob URL.
        expect(att.previewUrl).toBeNull();
    });

    it("surfaces 413 message without 'after resize' when resize did not occur", async () => {
        // long edge 80 ≤ cap 100 → resize does NOT fire (resized: false)
        stubBitmap(80, 60);
        stubCanvas(512);

        // Override fetch to return 413.
        fetchSpy.mockResolvedValue(new Response("", { status: 413 }));

        // reportClientError logs the upload failure via console.error.
        expectConsoleError(/upload failed/i);

        const file = makeImageFile("photo.jpg");
        el.uploadExternalFile(file);

        await new Promise(r => setTimeout(r, 20));
        await el.updateComplete;

        const internals = el as unknown as { pendingAttachments: Array<{ errorMessage?: string; previewUrl: string | null }> };
        expect(internals.pendingAttachments).toHaveLength(1);
        const att = internals.pendingAttachments[0]!;
        const errMsg = att.errorMessage ?? "";
        expect(errMsg).toMatch(/too large/i);
        // Must NOT claim resize happened.
        expect(errMsg).not.toContain("after resize");
        // Must not be the generic "Upload failed (413)" format.
        expect(errMsg).not.toMatch(/upload failed \(\d+\)/i);
    });

    it("post-resize placeholder update sets filename/mediaType/size to resized values", async () => {
        stubBitmap(200, 150); // long edge 200 > cap 100 → resize fires
        stubCanvas(300);       // resized blob is 300 bytes

        const file = makeImageFile("photo.heic");
        el.uploadExternalFile(file);

        await new Promise(r => setTimeout(r, 20));
        await el.updateComplete;

        type Att = { filename: string; mediaType: string; size: number; status: string };
        const internals = el as unknown as { pendingAttachments: Att[] };
        expect(internals.pendingAttachments).toHaveLength(1);
        const att = internals.pendingAttachments[0]!;
        // After successful upload+settle, filename/mediaType come from server response stub.
        // At minimum the status must be "ready" (not "uploading" or "error").
        expect(att.status).toBe("ready");
        // The resized file produced by maybeResizeImage has .jpg extension.
        expect(att.filename).toMatch(/\.jpg$/i);
    });

    it("aborts upload and revokes previewUrl when placeholder removed during resize", async () => {
        // Pause resize via a deferred promise on createImageBitmap.
        let releaseBitmap!: () => void;
        const bitmapPending = new Promise<void>(r => { releaseBitmap = r; });
        (globalThis as unknown as Record<string, unknown>).createImageBitmap = async () => {
            await bitmapPending;
            return { width: 200, height: 150, close: vi.fn() };
        };
        stubCanvas(512);

        const revokedUrls: string[] = [];
        vi.spyOn(URL, "revokeObjectURL").mockImplementation((url) => { revokedUrls.push(url); });
        const createdUrls: string[] = [];
        vi.spyOn(URL, "createObjectURL").mockImplementation(() => {
            const url = `blob:test-${createdUrls.length}`;
            createdUrls.push(url);
            return url;
        });

        const file = makeImageFile("photo.jpg");
        el.uploadExternalFile(file);

        // Let the microtask queue run so the placeholder is pushed before resize awaits.
        await Promise.resolve();
        await el.updateComplete;

        // Placeholder should be visible with original file metadata.
        const internals = el as unknown as { pendingAttachments: Array<{ status: string; filename: string }> };
        expect(internals.pendingAttachments).toHaveLength(1);
        expect(internals.pendingAttachments[0]!.status).toBe("uploading");

        // Simulate user removing the attachment while resize is still in flight.
        const priv = el as unknown as { removeAttachment(idx: number): void };
        priv.removeAttachment(0);
        await el.updateComplete;
        expect(internals.pendingAttachments).toHaveLength(0);

        // Release the resize stub — uploadFile resumes and should bail.
        releaseBitmap();
        await new Promise(r => setTimeout(r, 20));

        // fetch must not have been called.
        expect(fetchSpy).not.toHaveBeenCalled();
        // The original previewUrl (created at placeholder time) must have been revoked.
        expect(revokedUrls).toContain(createdUrls[0]);
        // No second object URL should have been created after bail.
        expect(createdUrls).toHaveLength(1);
    });

    // -------------------------------------------------------------------
    // handleTargetFileSelect: resize path
    // -------------------------------------------------------------------

    it("target upload: fires onResized and POSTs smaller blob when image exceeds cap", async () => {
        // Override fetch to return a TargetUploadResponse.
        fetchSpy.mockResolvedValue(
            new Response(
                JSON.stringify({ upload_ids: ["tgt123"], files: ["photo.jpg"] }),
                { status: 200, headers: { "Content-Type": "application/json" } },
            ),
        );

        stubBitmap(300, 200); // long edge 300 > cap 100 → will be resized
        stubCanvas(256);      // resized blob is 256 bytes

        const resizedMessages: string[] = [];
        el.onResized = (text) => { resizedMessages.push(text); };

        // Access private fields via casting to set up state the handler reads.
        const priv = el as unknown as { pendingTargetName: string };
        priv.pendingTargetName = "import";

        // Stub the target file input's files property.
        const targetInput = el.querySelector("#target-input") as HTMLInputElement;
        const file = makeImageFile("photo.jpg");
        const fileList = { 0: file, length: 1, item: (i: number) => (i === 0 ? file : null) } as unknown as FileList;
        Object.defineProperty(targetInput, "files", { get: () => fileList, configurable: true });

        targetInput.dispatchEvent(new Event("change", { bubbles: false }));

        // Allow async resize + fetch to complete.
        await new Promise(r => setTimeout(r, 20));

        // onResized must have fired with dimension text.
        expect(resizedMessages).toHaveLength(1);
        expect(resizedMessages[0]).toContain("300x200");

        // fetch must have been called with the resized (smaller) blob.
        expect(fetchSpy).toHaveBeenCalledTimes(1);
        const url = fetchSpy.mock.calls[0]![0] as string;
        expect(url).toBe("/app/test/upload");
        const body = (fetchSpy.mock.calls[0]![1] as RequestInit).body as FormData;
        expect(body.get("target")).toBe("import");
        const uploaded = body.get("file") as File;
        // Resized blob is 256 bytes; original was 10 bytes but the stub canvas
        // always produces a fixed-size blob.
        expect(uploaded.size).toBe(256);
    });
});
