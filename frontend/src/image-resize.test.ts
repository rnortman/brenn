/**
 * Unit tests for image-resize.ts.
 *
 * happy-dom provides HTMLCanvasElement but no real 2D compositing.
 * We stub globalThis.createImageBitmap, canvas.getContext, and
 * canvas.toBlob so we can assert branching logic without pixel math.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { expectConsoleError } from "./test-setup.js";
import {
    MissingClientConfigError,
    readMaxImageLongEdge,
    maybeResizeImage,
} from "./image-resize.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Install a <meta name="max-image-long-edge"> tag and return a cleanup fn. */
function installMeta(content: string): () => void {
    const meta = document.createElement("meta");
    meta.name = "max-image-long-edge";
    meta.content = content;
    document.head.appendChild(meta);
    return () => meta.remove();
}

/** Create a minimal File with the given MIME type and name. */
function makeFile(name: string, type: string, sizeBytes = 100): File {
    const bytes = new Uint8Array(sizeBytes);
    return new File([bytes], name, { type });
}

/**
 * Returns a stub for createImageBitmap that resolves with a fake bitmap
 * of the given dimensions. Installs/uninstalls on globalThis.
 */
function stubCreateImageBitmap(
    width: number,
    height: number,
): () => void {
    const original = globalThis.createImageBitmap;
    globalThis.createImageBitmap = vi
        .fn()
        .mockResolvedValue({ width, height, close: vi.fn() }) as typeof createImageBitmap;
    return () => {
        globalThis.createImageBitmap = original;
    };
}

/**
 * Stubs createImageBitmap to throw. Returns cleanup fn.
 */
function stubCreateImageBitmapThrows(err: Error): () => void {
    const original = globalThis.createImageBitmap;
    globalThis.createImageBitmap = vi
        .fn()
        .mockRejectedValue(err) as typeof createImageBitmap;
    return () => {
        globalThis.createImageBitmap = original;
    };
}

/**
 * Stubs HTMLCanvasElement.prototype.getContext so that calls to
 * `canvas.getContext("2d")` return a minimal context with a stubbed
 * drawImage, and canvas.toBlob invokes its callback with the given blob.
 *
 * Returns cleanup fn.
 */
function stubCanvas(blobOrNull: Blob | null): () => void {
    const originalGetContext = HTMLCanvasElement.prototype.getContext;
    const originalToBlob = HTMLCanvasElement.prototype.toBlob;

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (HTMLCanvasElement.prototype as any).getContext = function (
        contextId: string,
    ) {
        if (contextId === "2d") {
            return { drawImage: vi.fn() };
        }
        return null;
    };

    HTMLCanvasElement.prototype.toBlob = function (
        callback: BlobCallback,
        _type?: string,
        _quality?: number,
    ): void {
        // Schedule async to match real browser behavior.
        setTimeout(() => callback(blobOrNull), 0);
    };

    return () => {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (HTMLCanvasElement.prototype as any).getContext = originalGetContext;
        HTMLCanvasElement.prototype.toBlob = originalToBlob;
    };
}

/** Stub getContext to return null (simulates context acquisition failure). */
function stubCanvasGetContextNull(): () => void {
    const originalGetContext = HTMLCanvasElement.prototype.getContext;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (HTMLCanvasElement.prototype as any).getContext = () => null;
    return () => {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (HTMLCanvasElement.prototype as any).getContext = originalGetContext;
    };
}

// ---------------------------------------------------------------------------
// readMaxImageLongEdge
// ---------------------------------------------------------------------------

describe("readMaxImageLongEdge", () => {
    let cleanup: () => void = () => {};
    afterEach(() => cleanup());

    it("returns the numeric value when meta tag is present and valid", () => {
        cleanup = installMeta("2576");
        expect(readMaxImageLongEdge()).toBe(2576);
    });

    it("throws MissingClientConfigError when meta tag is absent", () => {
        expect(() => readMaxImageLongEdge()).toThrow(MissingClientConfigError);
    });

    it("throws MissingClientConfigError when content is non-numeric", () => {
        cleanup = installMeta("not-a-number");
        expect(() => readMaxImageLongEdge()).toThrow(MissingClientConfigError);
    });

    it("throws MissingClientConfigError when content is zero", () => {
        cleanup = installMeta("0");
        expect(() => readMaxImageLongEdge()).toThrow(MissingClientConfigError);
    });

    it("throws MissingClientConfigError when content is negative", () => {
        cleanup = installMeta("-100");
        expect(() => readMaxImageLongEdge()).toThrow(MissingClientConfigError);
    });
});

// ---------------------------------------------------------------------------
// maybeResizeImage — non-image MIME
// ---------------------------------------------------------------------------

describe("maybeResizeImage — non-image MIME", () => {
    it("returns original file unchanged with resized: false", async () => {
        const file = makeFile("doc.pdf", "application/pdf");
        const result = await maybeResizeImage(file, 2576);
        expect(result.resized).toBe(false);
        expect(result.file).toBe(file);
    });
});

// ---------------------------------------------------------------------------
// maybeResizeImage — image within cap
// ---------------------------------------------------------------------------

describe("maybeResizeImage — image within long-edge cap", () => {
    let restore: () => void = () => {};
    afterEach(() => restore());

    it("returns original file unchanged when long edge equals cap", async () => {
        restore = stubCreateImageBitmap(2576, 1932);
        const file = makeFile("photo.jpg", "image/jpeg");
        const result = await maybeResizeImage(file, 2576);
        expect(result.resized).toBe(false);
        expect(result.file).toBe(file);
    });

    it("returns original file unchanged when long edge is below cap", async () => {
        restore = stubCreateImageBitmap(1024, 768);
        const file = makeFile("small.jpg", "image/jpeg");
        const result = await maybeResizeImage(file, 2576);
        expect(result.resized).toBe(false);
        expect(result.file).toBe(file);
    });
});

// ---------------------------------------------------------------------------
// maybeResizeImage — resize path (happy path)
// ---------------------------------------------------------------------------

describe("maybeResizeImage — image exceeds cap (resize)", () => {
    let restoreBitmap: () => void = () => {};
    let restoreCanvas: () => void = () => {};

    beforeEach(() => {
        // 4032×3024 portrait-style; long edge = 4032
        restoreBitmap = stubCreateImageBitmap(4032, 3024);
        restoreCanvas = stubCanvas(new Blob(["fake-jpeg"], { type: "image/jpeg" }));
    });
    afterEach(() => {
        restoreBitmap();
        restoreCanvas();
    });

    it("returns resized: true", async () => {
        const file = makeFile("camera.jpg", "image/jpeg");
        const result = await maybeResizeImage(file, 2576);
        expect(result.resized).toBe(true);
    });

    it("output file has .jpg extension", async () => {
        const file = makeFile("camera.heic", "image/jpeg");
        const result = await maybeResizeImage(file, 2576);
        expect((result.file as File).name).toMatch(/\.jpg$/);
    });

    it("output file has image/jpeg MIME type", async () => {
        const file = makeFile("camera.jpg", "image/jpeg");
        const result = await maybeResizeImage(file, 2576);
        expect(result.file.type).toBe("image/jpeg");
    });

    it("from dimensions match input bitmap dimensions", async () => {
        const file = makeFile("camera.jpg", "image/jpeg");
        const result = await maybeResizeImage(file, 2576);
        if (!result.resized) throw new Error("expected resized: true");
        expect(result.from).toEqual({ w: 4032, h: 3024 });
    });

    it("to.w equals maxLongEdge when width is the long edge", async () => {
        const file = makeFile("camera.jpg", "image/jpeg");
        const result = await maybeResizeImage(file, 2576);
        if (!result.resized) throw new Error("expected resized: true");
        expect(result.to.w).toBe(2576);
    });

    it("aspect ratio preserved within 1 px", async () => {
        const file = makeFile("camera.jpg", "image/jpeg");
        const result = await maybeResizeImage(file, 2576);
        if (!result.resized) throw new Error("expected resized: true");
        const fromRatio = result.from.w / result.from.h;
        const toRatio = result.to.w / result.to.h;
        expect(Math.abs(fromRatio - toRatio)).toBeLessThanOrEqual(
            1 / result.from.h + 1 / result.to.h,
        );
    });

    it("handles tall portrait images: long edge is height", async () => {
        restoreBitmap();
        restoreBitmap = stubCreateImageBitmap(3024, 4032); // portrait
        const file = makeFile("portrait.jpg", "image/jpeg");
        const result = await maybeResizeImage(file, 2576);
        expect(result.resized).toBe(true);
        if (!result.resized) throw new Error("expected resized: true");
        expect(result.to.h).toBe(2576);
    });
});

// ---------------------------------------------------------------------------
// maybeResizeImage — decode failure
// ---------------------------------------------------------------------------

describe("maybeResizeImage — decode failure", () => {
    let restore: () => void = () => {};
    afterEach(() => restore());

    it("returns original file unchanged on decode failure", async () => {
        restore = stubCreateImageBitmapThrows(new Error("unsupported format"));
        const file = makeFile("bad.jpg", "image/jpeg");
        expectConsoleError("decode failed");
        const result = await maybeResizeImage(file, 2576);
        expect(result.resized).toBe(false);
        expect(result.file).toBe(file);
    });

    it("reports decode failure via reportClientError (not console.warn)", async () => {
        restore = stubCreateImageBitmapThrows(new Error("unsupported format"));
        const file = makeFile("bad.jpg", "image/jpeg");
        // reportClientError logs via console.error; declare expected call to satisfy
        // the test-setup harness, then verify the message content.
        const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
        try {
            await maybeResizeImage(file, 2576);
            expect(errorSpy).toHaveBeenCalledTimes(1);
            expect(String(errorSpy.mock.calls[0]![0])).toMatch(/decode failed/i);
        } finally {
            errorSpy.mockRestore();
        }
    });
});

// ---------------------------------------------------------------------------
// maybeResizeImage — post-decode pipeline failure: getContext returns null
// ---------------------------------------------------------------------------

describe("maybeResizeImage — canvas.getContext returns null", () => {
    let restoreBitmap: () => void = () => {};
    let restoreCanvas: () => void = () => {};

    beforeEach(() => {
        restoreBitmap = stubCreateImageBitmap(4032, 3024);
        restoreCanvas = stubCanvasGetContextNull();
    });
    afterEach(() => {
        restoreBitmap();
        restoreCanvas();
    });

    it("returns original file unchanged", async () => {
        const file = makeFile("photo.jpg", "image/jpeg");
        // getContext returning null is a pipeline error — expect console.error
        expectConsoleError("canvas.getContext");
        const result = await maybeResizeImage(file, 2576);
        expect(result.resized).toBe(false);
        expect(result.file).toBe(file);
    });
});

// ---------------------------------------------------------------------------
// maybeResizeImage — post-decode pipeline failure: drawImage throws
// ---------------------------------------------------------------------------

describe("maybeResizeImage — drawImage throws", () => {
    let restoreBitmap: () => void = () => {};
    let restoreCanvas: () => void = () => {};

    beforeEach(() => {
        restoreBitmap = stubCreateImageBitmap(4032, 3024);
        const originalGetContext = HTMLCanvasElement.prototype.getContext;
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (HTMLCanvasElement.prototype as any).getContext = function (contextId: string) {
            if (contextId === "2d") {
                return { drawImage: vi.fn().mockImplementation(() => { throw new Error("OOM"); }) };
            }
            return null;
        };
        restoreCanvas = () => {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            (HTMLCanvasElement.prototype as any).getContext = originalGetContext;
        };
    });
    afterEach(() => {
        restoreBitmap();
        restoreCanvas();
    });

    it("returns original file unchanged when drawImage throws", async () => {
        const file = makeFile("photo.jpg", "image/jpeg");
        expectConsoleError("drawImage threw");
        const result = await maybeResizeImage(file, 2576);
        expect(result.resized).toBe(false);
        expect(result.file).toBe(file);
    });
});

// ---------------------------------------------------------------------------
// maybeResizeImage — post-decode pipeline failure: canvas.toBlob returns null
// ---------------------------------------------------------------------------

describe("maybeResizeImage — canvas.toBlob returns null", () => {
    let restoreBitmap: () => void = () => {};
    let restoreCanvas: () => void = () => {};

    beforeEach(() => {
        restoreBitmap = stubCreateImageBitmap(4032, 3024);
        restoreCanvas = stubCanvas(null); // null blob
    });
    afterEach(() => {
        restoreBitmap();
        restoreCanvas();
    });

    it("returns original file unchanged", async () => {
        const file = makeFile("photo.jpg", "image/jpeg");
        expectConsoleError("canvas.toBlob returned null");
        const result = await maybeResizeImage(file, 2576);
        expect(result.resized).toBe(false);
        expect(result.file).toBe(file);
    });
});
