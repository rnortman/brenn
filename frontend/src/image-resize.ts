/**
 * Client-side image resize before upload.
 *
 * Exported API:
 *   - MissingClientConfigError  — thrown by readMaxImageLongEdge when meta tag absent/invalid
 *   - readMaxImageLongEdge()    — reads <meta name="max-image-long-edge"> once
 *   - maybeResizeImage()        — conditionally downsizes an image file to fit long-edge cap
 */

import { reportClientError } from "./error-reporter.js";

/** Thrown when the server-supplied <meta name="max-image-long-edge"> is absent or non-numeric. */
export class MissingClientConfigError extends Error {
    constructor(message: string) {
        super(message);
        this.name = "MissingClientConfigError";
    }
}

/**
 * Read the max-image-long-edge cap from the app-shell meta tag.
 *
 * Throws MissingClientConfigError if the tag is absent or contains a
 * non-positive integer. Call once at app startup; cache the result.
 */
export function readMaxImageLongEdge(): number {
    const meta = document.querySelector<HTMLMetaElement>(
        'meta[name="max-image-long-edge"]',
    );
    if (!meta) {
        throw new MissingClientConfigError(
            'Required <meta name="max-image-long-edge"> is missing from app shell',
        );
    }
    // Use Number() rather than parseInt() so trailing non-digit characters
    // (e.g. "2576px") are rejected instead of silently truncated.
    const value = Number(meta.content.trim());
    if (!Number.isInteger(value) || value <= 0) {
        throw new MissingClientConfigError(
            `<meta name="max-image-long-edge"> has invalid content: ${JSON.stringify(meta.content)}`,
        );
    }
    return value;
}

/**
 * Discriminated union: when `resized` is false the image was returned unchanged;
 * when `resized` is true the image was downscaled and `from`/`to` are guaranteed
 * to be present (no `!` assertions at the call site).
 */
export type ResizeResult =
    | { resized: false; file: File }
    | { resized: true; file: File; from: { w: number; h: number }; to: { w: number; h: number } };

/**
 * Optionally downscale an image file so its long edge fits within maxLongEdge.
 *
 * - Non-image MIME: returned unchanged, resized: false.
 * - Image with long edge <= maxLongEdge: returned unchanged, resized: false.
 * - Decode failure: reportClientError, returned unchanged, resized: false.
 * - Post-decode pipeline failure: console.error, returned unchanged, resized: false.
 * - Otherwise: resized JPEG returned, resized: true, from/to populated.
 *
 * Never throws. All error paths fall back to the original file.
 */
export async function maybeResizeImage(
    file: File,
    maxLongEdge: number,
): Promise<ResizeResult> {
    // Step 1: non-image MIME — skip.
    if (!file.type.startsWith("image/")) {
        return { file, resized: false };
    }

    // Step 2: decode.
    let bitmap: ImageBitmap;
    try {
        bitmap = await createImageBitmap(file, { imageOrientation: "from-image" });
    } catch (err) {
        reportClientError(`image-resize: decode failed, uploading original: ${String(err)}`);
        return { file, resized: false };
    }

    // Step 3: check if resize needed.
    const longEdge = Math.max(bitmap.width, bitmap.height);
    if (longEdge <= maxLongEdge) {
        bitmap.close();
        return { file, resized: false };
    }

    // Step 4: compute scaled dimensions preserving aspect ratio.
    // Floor at 1 to guard against extreme aspect ratios where the short
    // edge rounds to 0 (e.g. a 10000×1 image at maxLongEdge=2576).
    const scale = maxLongEdge / longEdge;
    const outW = Math.max(1, Math.round(bitmap.width * scale));
    const outH = Math.max(1, Math.round(bitmap.height * scale));
    const from = { w: bitmap.width, h: bitmap.height };
    const to = { w: outW, h: outH };

    // Step 5: draw to canvas.
    const canvas = document.createElement("canvas");
    canvas.width = outW;
    canvas.height = outH;
    const ctx = canvas.getContext("2d");
    if (!ctx) {
        bitmap.close();
        reportClientError(
            "image-resize: canvas.getContext('2d') returned null — uploading original",
        );
        return { file, resized: false };
    }

    try {
        ctx.drawImage(bitmap, 0, 0, outW, outH);
    } catch (err) {
        bitmap.close();
        reportClientError(`image-resize: drawImage threw — uploading original: ${String(err)}`);
        return { file, resized: false };
    }

    // Release bitmap memory as soon as possible.
    bitmap.close();

    // Step 6: export as JPEG.
    const blob = await new Promise<Blob | null>((resolve) => {
        canvas.toBlob(resolve, "image/jpeg", 0.9);
    });

    if (!blob) {
        reportClientError(
            "image-resize: canvas.toBlob returned null — uploading original",
        );
        return { file, resized: false };
    }

    // Step 7: build a new File with original stem + .jpg.
    // Use a lookbehind to require at least one preceding char before the
    // final dot, so dotfile-style names like ".hidden" are left intact
    // rather than becoming ".jpg".  Fall back to "image" if stem is empty.
    const stem = file.name.replace(/(?<=.)\.[^.]*$/, "") || "image";
    const outFilename = `${stem}.jpg`;
    const outFile = new File([blob], outFilename, { type: "image/jpeg" });

    return { file: outFile, resized: true, from, to };
}
