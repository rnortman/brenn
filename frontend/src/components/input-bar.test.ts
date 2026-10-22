// @vitest-environment happy-dom
//
// Pins the resize-before-upload path in BrennInputBar.
// Covers: onResized callback fired on oversized image; resized blob POSTed;
// imageAttachmentsDisabled blocks image uploads with onError.
// Also covers: handleKeydown stopPropagation for consumed-Enter branches;
// the snippet composer, secret attachments and the large-paste offer.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { expectConsoleError } from "../test-setup.js";
import type { AttachmentMeta } from "../generated/AttachmentMeta";
import {
    MOUNT_GRACE_MS,
    canInterceptKeyboard,
    registerMount,
    setChatHasText,
    unregisterMount,
} from "../keyboard-guard.js";
import "./input-bar.js";
import {
    BrennInputBar,
    SNIPPET_OFFER_CHARS,
    SNIPPET_OFFER_LINES,
    isLargePaste,
} from "./input-bar.js";

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

describe("BrennInputBar — model picker states", () => {
    async function mount(
        models: { value: string; display_name: string; description: string }[],
        current: string,
    ): Promise<BrennInputBar> {
        const el = document.createElement("brenn-input-bar") as BrennInputBar;
        el.appSlug = "test";
        el.availableModels = models as unknown as BrennInputBar["availableModels"];
        el.currentModel = current;
        document.body.appendChild(el);
        await el.updateComplete;
        return el;
    }

    const OPUS = { value: "opus[1m]", display_name: "Opus 5 1M", description: "big" };
    const SONNET = { value: "sonnet", display_name: "Sonnet", description: "fast" };

    afterEach(() => {
        document.body.replaceChildren();
    });

    it("renders no picker when no models are offered", async () => {
        const el = await mount([], "opus[1m]");
        expect(el.querySelector(".model-picker")).toBeNull();
    });

    it("renders an inert label when exactly one model is offered", async () => {
        const changes: string[] = [];
        const el = await mount([OPUS], "opus[1m]");
        el.onModelChange = (m) => { changes.push(m); };
        await el.updateComplete;

        const picker = el.querySelector(".model-picker") as HTMLElement;
        expect(picker).not.toBeNull();
        expect(picker.classList.contains("model-picker-locked")).toBe(true);
        // Label shows the model; no cycle affordance.
        expect(picker.textContent).toContain("Opus 5 1M");
        expect(picker.textContent).not.toContain("⇌");

        // Clicking must not change the model.
        picker.click();
        await el.updateComplete;
        expect(changes).toEqual([]);
        expect(el.currentModel).toBe("opus[1m]");
    });

    it("renders an interactive picker that cycles when 2+ models are offered", async () => {
        const changes: string[] = [];
        const el = await mount([OPUS, SONNET], "opus[1m]");
        el.onModelChange = (m) => { changes.push(m); };
        await el.updateComplete;

        const picker = el.querySelector(".model-picker") as HTMLElement;
        expect(picker).not.toBeNull();
        expect(picker.classList.contains("model-picker-locked")).toBe(false);
        expect(picker.textContent).toContain("⇌");

        picker.click();
        await el.updateComplete;
        expect(changes).toEqual(["sonnet"]);
    });
});

type PendingView = {
    pendingAttachments: Array<{
        filename: string;
        status: string;
        secret: boolean;
        previewUrl: string | null;
    }>;
};

/** Minimal paste event carrying text and optional files. */
function pasteEvent(text: string, files: File[] = []): ClipboardEvent {
    const ev = new Event("paste", { bubbles: true, cancelable: true }) as ClipboardEvent;
    const fileList = Object.assign([...files], {
        item: (i: number) => files[i] ?? null,
    }) as unknown as FileList;
    Object.defineProperty(ev, "clipboardData", {
        value: {
            files: fileList,
            getData: (type: string) => (type === "text/plain" ? text : ""),
        },
    });
    return ev;
}

/** A bubbling `drop` event whose `dataTransfer.files` holds the given files. */
function dropEvent(files: File[]): DragEvent {
    const ev = new Event("drop", { bubbles: true, cancelable: true }) as DragEvent;
    const fileList = Object.assign([...files], {
        item: (i: number) => files[i] ?? null,
    }) as unknown as FileList;
    Object.defineProperty(ev, "dataTransfer", { value: { files: fileList } });
    return ev;
}

/** Make `input.files` report the given files, then fire `change`. */
function selectFiles(input: HTMLInputElement, files: File[]): void {
    const fileList = Object.assign([...files], {
        item: (i: number) => files[i] ?? null,
    }) as unknown as FileList;
    Object.defineProperty(input, "files", { get: () => fileList, configurable: true });
    input.dispatchEvent(new Event("change", { bubbles: false }));
}

describe("BrennInputBar — snippet composer and secret attachments", () => {
    let el: BrennInputBar;
    let fetchSpy: ReturnType<typeof vi.spyOn>;

    beforeEach(async () => {
        vi.useRealTimers();
        fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (_url, init) => {
            const file = ((init as RequestInit).body as FormData).get("file") as File;
            return new Response(
                JSON.stringify({
                    upload_id: `id-${file.name}`,
                    filename: file.name,
                    media_type: "text/plain",
                    size: file.size,
                }),
                { status: 200, headers: { "Content-Type": "application/json" } },
            );
        });
        el = document.createElement("brenn-input-bar") as BrennInputBar;
        el.appSlug = "test";
        document.body.appendChild(el);
        await el.updateComplete;
    });

    afterEach(() => {
        vi.useRealTimers();
        vi.unstubAllGlobals();
        vi.restoreAllMocks();
        document.body.replaceChildren();
        setChatHasText(false);
    });

    function q<T extends Element>(sel: string): T | null {
        return el.querySelector(sel) as T | null;
    }

    async function openMenuItem(sel: string): Promise<void> {
        q<HTMLButtonElement>(".menu-btn")!.click();
        await el.updateComplete;
        q<HTMLButtonElement>(sel)!.click();
        await el.updateComplete;
    }

    async function typeComposer(text: string): Promise<void> {
        const ta = q<HTMLTextAreaElement>(".snippet-text")!;
        ta.value = text;
        ta.dispatchEvent(new Event("input", { bubbles: true }));
        await el.updateComplete;
    }

    async function setSecretBox(checked: boolean): Promise<void> {
        const box = q<HTMLInputElement>(".snippet-secret-box")!;
        box.checked = checked;
        box.dispatchEvent(new Event("change", { bubbles: true }));
        await el.updateComplete;
    }

    function postedForms(): FormData[] {
        return fetchSpy.mock.calls.map((c: unknown[]) => (c[1] as RequestInit).body as FormData);
    }

    async function settle(): Promise<void> {
        await new Promise((r) => setTimeout(r, 20));
        await el.updateComplete;
    }

    it("Snippet menu item opens an empty composer with the default name", async () => {
        expect(q(".snippet-composer")).toBeNull();
        await openMenuItem(".snippet-menu-item");
        expect(q(".snippet-composer")).not.toBeNull();
        expect(q<HTMLTextAreaElement>(".snippet-text")!.value).toBe("");
        expect(q<HTMLInputElement>(".snippet-name")!.value).toBe("snippet.txt");
        expect(q<HTMLInputElement>(".snippet-secret-box")!.checked).toBe(false);
        expect(q<HTMLButtonElement>(".snippet-attach")!.disabled).toBe(true);
        expect(q(".snippet-paste-inline")).toBeNull();
    });

    it("Attach posts the text as snippet.txt with no secret field and closes the composer", async () => {
        await openMenuItem(".snippet-menu-item");
        await typeComposer("line one\nline two");
        q<HTMLButtonElement>(".snippet-attach")!.click();
        await settle();

        expect(fetchSpy).toHaveBeenCalledTimes(1);
        const form = postedForms()[0]!;
        const file = form.get("file") as File;
        expect(file.name).toBe("snippet.txt");
        expect(await file.text()).toBe("line one\nline two");
        expect(form.has("secret")).toBe(false);
        expect(q(".snippet-composer")).toBeNull();
        const att = (el as unknown as PendingView).pendingAttachments[0]!;
        expect(att.status).toBe("ready");
        expect(att.secret).toBe(false);
    });

    it("Attach uses the entered name and, with Secret checked, posts secret=1", async () => {
        await openMenuItem(".snippet-menu-item");
        await typeComposer("token-value");
        const name = q<HTMLInputElement>(".snippet-name")!;
        name.value = "oauth.json";
        name.dispatchEvent(new Event("input", { bubbles: true }));
        await setSecretBox(true);
        q<HTMLButtonElement>(".snippet-attach")!.click();
        await settle();

        const form = postedForms()[0]!;
        expect((form.get("file") as File).name).toBe("oauth.json");
        expect(form.get("secret")).toBe("1");
        const att = (el as unknown as PendingView).pendingAttachments[0]!;
        expect(att.secret).toBe(true);
        const chip = q<HTMLElement>(".attachment-chip.secret")!;
        expect(chip).not.toBeNull();
        expect(chip.textContent).toContain("\u{1F512}");
        expect(chip.title).toMatch(/secret/i);
        expect(chip.querySelector("img")).toBeNull();
    });

    it("Ctrl+Enter attaches, plain Enter does not, Escape cancels", async () => {
        await openMenuItem(".snippet-menu-item");
        await typeComposer("abc");
        const ta = q<HTMLTextAreaElement>(".snippet-text")!;

        const sends: string[] = [];
        el.onSubmit = (text) => { sends.push(text); };

        ta.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true, cancelable: true }));
        await el.updateComplete;
        expect(q(".snippet-composer")).not.toBeNull();
        expect(fetchSpy).not.toHaveBeenCalled();

        ta.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", ctrlKey: true, bubbles: true, cancelable: true }));
        await settle();
        expect(fetchSpy).toHaveBeenCalledTimes(1);
        expect(q(".snippet-composer")).toBeNull();
        expect(sends).toEqual([]);

        await openMenuItem(".snippet-menu-item");
        await typeComposer("discard me");
        q<HTMLTextAreaElement>(".snippet-text")!.dispatchEvent(
            new KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true }),
        );
        await settle();
        expect(q(".snippet-composer")).toBeNull();
        expect(fetchSpy).toHaveBeenCalledTimes(1);
    });

    it("files pasted into the composer upload with its Secret flag and leave its text alone", async () => {
        await openMenuItem(".snippet-menu-item");
        await typeComposer("keep");
        await setSecretBox(true);
        const file = new File(["{}"], "client.json", { type: "application/json" });
        const ev = pasteEvent("", [file]);
        q<HTMLTextAreaElement>(".snippet-text")!.dispatchEvent(ev);
        await settle();

        expect(ev.defaultPrevented).toBe(true);
        expect(fetchSpy).toHaveBeenCalledTimes(1);
        const form = postedForms()[0]!;
        expect((form.get("file") as File).name).toBe("client.json");
        expect(form.get("secret")).toBe("1");
        expect(q<HTMLTextAreaElement>(".snippet-text")!.value).toBe("keep");
    });

    it("files dropped on a secret composer upload as secret and skip the form's drop", async () => {
        await openMenuItem(".snippet-menu-item");
        await setSecretBox(true);
        const form = q<HTMLFormElement>("form")!;
        form.classList.add("drag-over");
        const file = new File(["{}"], "client_secret.json", { type: "application/json" });
        const ev = dropEvent([file]);
        q<HTMLTextAreaElement>(".snippet-text")!.dispatchEvent(ev);
        await settle();

        expect(ev.defaultPrevented).toBe(true);
        expect(fetchSpy).toHaveBeenCalledTimes(1);
        const posted = postedForms()[0]!;
        expect((posted.get("file") as File).name).toBe("client_secret.json");
        expect(posted.get("secret")).toBe("1");
        expect(form.classList.contains("drag-over")).toBe(false);
        expect((el as unknown as PendingView).pendingAttachments[0]!.secret).toBe(true);
    });

    it("files dropped on a non-secret composer upload as plain", async () => {
        await openMenuItem(".snippet-menu-item");
        q(".snippet-composer")!.dispatchEvent(
            dropEvent([new File(["a"], "notes.txt", { type: "text/plain" })]),
        );
        await settle();

        expect(fetchSpy).toHaveBeenCalledTimes(1);
        expect(postedForms()[0]!.has("secret")).toBe(false);
    });

    it("the masked class follows the Secret box and the Show toggle", async () => {
        await openMenuItem(".snippet-menu-item");
        const ta = () => q<HTMLTextAreaElement>(".snippet-text")!;
        expect(ta().classList.contains("masked")).toBe(false);
        expect(q(".snippet-show")).toBeNull();

        await setSecretBox(true);
        expect(ta().classList.contains("masked")).toBe(true);

        q<HTMLButtonElement>(".snippet-show")!.click();
        await el.updateComplete;
        expect(ta().classList.contains("masked")).toBe(false);

        q<HTMLButtonElement>(".snippet-show")!.click();
        await el.updateComplete;
        expect(ta().classList.contains("masked")).toBe(true);

        await setSecretBox(false);
        expect(ta().classList.contains("masked")).toBe(false);
    });

    it("a reopened composer carries nothing over from a cancelled secret session", async () => {
        await openMenuItem(".snippet-menu-item");
        await typeComposer("hunter2");
        const name = q<HTMLInputElement>(".snippet-name")!;
        name.value = "pw.txt";
        name.dispatchEvent(new Event("input", { bubbles: true }));
        await setSecretBox(true);
        q<HTMLButtonElement>(".snippet-show")!.click();
        await el.updateComplete;
        q<HTMLButtonElement>(".snippet-cancel")!.click();
        await el.updateComplete;

        await openMenuItem(".snippet-menu-item");
        expect(q<HTMLTextAreaElement>(".snippet-text")!.value).toBe("");
        expect(q<HTMLInputElement>(".snippet-name")!.value).toBe("snippet.txt");
        expect(q<HTMLInputElement>(".snippet-secret-box")!.checked).toBe(false);
        expect(q(".snippet-show")).toBeNull();
        expect(q(".snippet-composer.secret")).toBeNull();
    });

    it("composer textarea opts out of autocomplete, spellcheck and password managers", async () => {
        await openMenuItem(".snippet-menu-item");
        const ta = q<HTMLTextAreaElement>(".snippet-text")!;
        expect(ta.getAttribute("autocomplete")).toBe("off");
        expect(ta.getAttribute("autocapitalize")).toBe("off");
        expect(ta.getAttribute("autocorrect")).toBe("off");
        expect(ta.getAttribute("spellcheck")).toBe("false");
        expect(ta.getAttribute("data-lpignore")).toBe("true");
        expect(ta.hasAttribute("data-1p-ignore")).toBe(true);
        expect(ta.hasAttribute("data-bwignore")).toBe(true);
    });

    it("Attach secret file marks only the next file-input change as secret", async () => {
        const fileInput = q<HTMLInputElement>("#file-input")!;
        await openMenuItem(".attach-secret-menu-item");
        selectFiles(fileInput, [new File(["a"], "one.txt", { type: "text/plain" })]);
        await settle();
        selectFiles(fileInput, [new File(["b"], "two.txt", { type: "text/plain" })]);
        await settle();

        const forms = postedForms();
        expect(forms).toHaveLength(2);
        expect(forms[0]!.get("secret")).toBe("1");
        expect(forms[1]!.has("secret")).toBe(false);
    });

    it("a cancelled secret picker does not make the next Attach file secret", async () => {
        const fileInput = q<HTMLInputElement>("#file-input")!;
        await openMenuItem(".attach-secret-menu-item");
        // Picker cancelled: no change event.
        await openMenuItem(".attach-file-menu-item");
        selectFiles(fileInput, [new File(["a"], "plain.txt", { type: "text/plain" })]);
        await settle();

        const forms = postedForms();
        expect(forms).toHaveLength(1);
        expect(forms[0]!.has("secret")).toBe(false);
    });

    it("a secret image is posted unresized and without a preview URL, even with images disabled", async () => {
        const errors: string[] = [];
        el.onError = (msg) => { errors.push(msg); };
        const resizeSpy = vi.fn();
        vi.stubGlobal("createImageBitmap", resizeSpy);
        const createUrl = vi.spyOn(URL, "createObjectURL");
        el.maxLongEdge = 10;
        el.imageAttachmentsDisabled = true;
        await el.updateComplete;

        const fileInput = q<HTMLInputElement>("#file-input")!;
        await openMenuItem(".attach-secret-menu-item");
        const img = makeImageFile("photo.jpg");
        selectFiles(fileInput, [img]);
        await settle();

        expect(resizeSpy).not.toHaveBeenCalled();
        expect(createUrl).not.toHaveBeenCalled();
        const form = postedForms()[0]!;
        const posted = form.get("file") as File;
        expect(posted.name).toBe("photo.jpg");
        expect(posted.type).toBe("image/jpeg");
        expect(posted.size).toBe(img.size);
        expect(form.get("secret")).toBe("1");
        const att = (el as unknown as PendingView).pendingAttachments[0]!;
        expect(att.previewUrl).toBeNull();
        expect(q(".attachment-chip img")).toBeNull();
        expect(errors).toEqual([]);
    });

    it("send() carries secret in AttachmentMeta", async () => {
        await openMenuItem(".snippet-menu-item");
        await typeComposer("s3cr3t");
        await setSecretBox(true);
        q<HTMLButtonElement>(".snippet-attach")!.click();
        await settle();
        await openMenuItem(".snippet-menu-item");
        await typeComposer("public");
        q<HTMLButtonElement>(".snippet-attach")!.click();
        await settle();

        const metas: AttachmentMeta[][] = [];
        el.onSubmit = (_t, _r, meta) => { metas.push(meta); };
        (el as unknown as { send(): void }).send();
        await el.updateComplete;

        expect(metas).toHaveLength(1);
        expect(metas[0]!.map((m) => m.secret)).toEqual([true, false]);
    });

    describe("large pastes into the main box", () => {
        function mainBox(): HTMLTextAreaElement {
            return q<HTMLTextAreaElement>("#input")!;
        }

        it("a short paste is left to the browser", async () => {
            const ev = pasteEvent("short text");
            mainBox().dispatchEvent(ev);
            await el.updateComplete;
            expect(ev.defaultPrevented).toBe(false);
            expect(q(".snippet-composer")).toBeNull();
            expect(isLargePaste("x".repeat(SNIPPET_OFFER_CHARS - 1))).toBe(false);
            expect(isLargePaste("x\n".repeat(SNIPPET_OFFER_LINES - 2) + "x")).toBe(false);
        });

        for (const [label, text] of [
            ["characters", "x".repeat(SNIPPET_OFFER_CHARS)],
            ["lines", Array.from({ length: SNIPPET_OFFER_LINES }, (_, i) => `l${i}`).join("\n")],
        ] as const) {
            it(`a paste at the ${label} threshold opens the composer pre-filled`, async () => {
                mainBox().value = "before";
                const ev = pasteEvent(text);
                mainBox().dispatchEvent(ev);
                await el.updateComplete;
                expect(ev.defaultPrevented).toBe(true);
                expect(mainBox().value).toBe("before");
                expect(q<HTMLTextAreaElement>(".snippet-text")!.value).toBe(text);
                expect(q(".snippet-paste-inline")).not.toBeNull();
                expect(q<HTMLInputElement>(".snippet-secret-box")!.checked).toBe(false);
            });
        }

        it("Paste inline inserts the text at the recorded caret and closes the composer", async () => {
            const box = mainBox();
            box.value = "headtail";
            box.setSelectionRange(4, 4);
            const big = "x".repeat(SNIPPET_OFFER_CHARS);
            box.dispatchEvent(pasteEvent(big));
            await el.updateComplete;
            // The caret moves while the composer is open; the recorded one wins.
            box.setSelectionRange(0, 0);

            q<HTMLButtonElement>(".snippet-paste-inline")!.click();
            await el.updateComplete;
            expect(box.value).toBe(`head${big}tail`);
            expect(q(".snippet-composer")).toBeNull();
            expect(q<HTMLButtonElement>(".send-btn")!.disabled).toBe(false);
        });

        it("editing the main box withdraws Paste inline", async () => {
            const box = mainBox();
            box.value = "Please review: ";
            box.setSelectionRange(15, 15);
            box.dispatchEvent(pasteEvent("x".repeat(SNIPPET_OFFER_CHARS)));
            await el.updateComplete;
            expect(q(".snippet-paste-inline")).not.toBeNull();

            box.value = "Hi, Please review: ";
            box.dispatchEvent(new Event("input", { bubbles: true }));
            await el.updateComplete;
            expect(q(".snippet-paste-inline")).toBeNull();
            expect(q(".snippet-composer")).not.toBeNull();
            expect(box.value).toBe("Hi, Please review: ");
        });

        it("prefilling the main box withdraws Paste inline", async () => {
            mainBox().dispatchEvent(pasteEvent("x".repeat(SNIPPET_OFFER_CHARS)));
            await el.updateComplete;
            el.prefillText("replaced");
            await el.updateComplete;
            expect(q(".snippet-paste-inline")).toBeNull();
        });

        it("a large paste while the composer holds text goes to the main box", async () => {
            await openMenuItem(".snippet-menu-item");
            await typeComposer("draft");
            const ev = pasteEvent("x".repeat(SNIPPET_OFFER_CHARS));
            mainBox().dispatchEvent(ev);
            await el.updateComplete;
            expect(ev.defaultPrevented).toBe(false);
            expect(q<HTMLTextAreaElement>(".snippet-text")!.value).toBe("draft");
        });
    });

    describe("keyboard guard", () => {
        let probe: HTMLElement;

        beforeEach(() => {
            vi.useFakeTimers({ toFake: ["Date"] });
            probe = document.createElement("div");
            registerMount(probe);
            vi.setSystemTime(Date.now() + MOUNT_GRACE_MS + 1);
        });

        afterEach(() => {
            unregisterMount(probe);
        });

        async function typeMain(text: string): Promise<void> {
            const box = q<HTMLTextAreaElement>("#input")!;
            box.value = text;
            box.dispatchEvent(new Event("input", { bubbles: true }));
            await el.updateComplete;
        }

        it("stays blocked while the composer holds text and the main box is cleared", async () => {
            await openMenuItem(".snippet-menu-item");
            await typeComposer("pending");
            await typeMain("hello");
            await typeMain("");
            expect(canInterceptKeyboard(probe)).toBe(false);
        });

        it("stays blocked while the composer holds text and the main box is sent", async () => {
            await openMenuItem(".snippet-menu-item");
            await typeComposer("pending");
            await typeMain("hello");
            el.onSubmit = () => {};
            (el as unknown as { send(): void }).send();
            await el.updateComplete;
            expect(q<HTMLTextAreaElement>("#input")!.value).toBe("");
            expect(canInterceptKeyboard(probe)).toBe(false);
        });

        it("is released when the composer closes with the main box empty", async () => {
            await openMenuItem(".snippet-menu-item");
            await typeComposer("pending");
            expect(canInterceptKeyboard(probe)).toBe(false);
            q<HTMLButtonElement>(".snippet-cancel")!.click();
            await el.updateComplete;
            expect(canInterceptKeyboard(probe)).toBe(true);
        });
    });
});
