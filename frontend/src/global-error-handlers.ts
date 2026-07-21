/**
 * Global window error listeners for Brenn.
 *
 * Side-effect module: import it first from main.ts. On evaluation it registers
 * two window listeners that funnel all unhandled rejections and uncaught errors
 * into reportClientError. Does not call event.preventDefault() on either.
 */

import { reportClientError, describeReason } from "./error-reporter.js";

window.addEventListener("unhandledrejection", (ev: PromiseRejectionEvent) => {
    try {
        reportClientError(
            "Unhandled promise rejection: " + describeReason(ev.reason),
        );
    } catch {
        /* swallow — must never raise from a listener */
    }
});

window.addEventListener("error", (ev: ErrorEvent) => {
    try {
        const v: unknown =
            ev.error !== null && ev.error !== undefined
                ? (ev.error as unknown)
                : `${ev.message} at ${ev.filename}:${ev.lineno}:${ev.colno}`;
        reportClientError("Uncaught error: " + describeReason(v));
    } catch {
        /* swallow — must never raise from a listener */
    }
});
