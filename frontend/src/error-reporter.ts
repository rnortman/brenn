/**
 * Frontend error-reporting primitive.
 *
 * Delivers a string payload to the backend via WsClientMessage::ClientError.
 * Also logs to console.error. Best-effort: drops silently if the socket is
 * not open. Never throws.
 */

import type { BrennWs } from "./ws.js";

/**
 * Maximum reported message length in JS string code units (UTF-16).
 * The final reported string (including truncation suffix) is at most this
 * many code units; the suffix fits within the cap, not appended after it.
 */
export const MAX_MESSAGE_LEN = 8192;

const TRUNCATION_SUFFIX = "…[truncated]";

/** Module-level reporter target. Null until BrennApp registers itself. */
let activeWs: BrennWs | null = null;

/** Register the active WS used for reporting. Called by BrennApp during construction. */
export function setReporterTarget(ws: BrennWs): void {
    activeWs = ws;
}

/**
 * Clear the reporter target. For test use only — do not call in production
 * code; callers that do so silently disable all error reporting.
 * @internal
 */
export function _clearReporterTargetForTest(): void {
    activeWs = null;
}

/**
 * Report an already-formatted error string to console + backend.
 * Best-effort: logs to console.error; if a target is registered and its
 * socket is OPEN, also sends WsClientMessage::ClientError. Never throws.
 */
export function reportClientError(message: string): void {
    // Truncate so the total output (including suffix) is at most MAX_MESSAGE_LEN
    // code units.
    if (message.length > MAX_MESSAGE_LEN) {
        let cutAt = MAX_MESSAGE_LEN - TRUNCATION_SUFFIX.length;
        // Avoid splitting a UTF-16 surrogate pair: if the char before the cut is
        // a high surrogate (U+D800–U+DBFF) back up one code unit so both halves
        // are dropped together.
        if ((message.charCodeAt(cutAt - 1) & 0xfc00) === 0xd800) {
            cutAt -= 1;
        }
        message = message.slice(0, cutAt) + TRUNCATION_SUFFIX;
    }

    console.error(message);

    if (activeWs !== null) {
        try {
            activeWs.sendClientError(message);
        } catch {
            // Swallow — e.g. InvalidStateError from WebSocket.send racing
            // against a close. The console line already landed.
        }
    }
}

/**
 * Produce a short, human-readable description of any value, suitable for UI
 * display. For Error instances returns only the message (no stack). Total:
 * never throws, never returns undefined.
 */
export function describeReasonShort(value: unknown): string {
    try {
        if (value instanceof Error) {
            return value.message || value.name;
        }
        if (typeof value === "string") {
            return value || "Unknown error";
        }
        return String(value);
    } catch {
        return "<undescribable>";
    }
}

/**
 * Produce a shape-only description of an object: constructor name and
 * own enumerable key names, with no property values. Defensive against
 * Proxy traps and null-prototype objects. Never throws.
 */
function describeObjectShape(value: object): string {
    let name: string;
    try {
        name = (value as { constructor?: { name?: string } }).constructor?.name ?? "[unknown]";
    } catch {
        name = "[unknown]";
    }

    let keys: string[];
    try {
        keys = Object.keys(value);
    } catch {
        keys = [];
    }

    const body = keys.length > 0 ? `{ ${keys.join(", ")} }` : "{}";
    // Early truncation here keeps Object.keys output bounded before it reaches
    // reportClientError's surrogate-safe truncation pass. Other describeReason
    // branches do not truncate; they rely solely on reportClientError.
    return `${name} ${body}`.slice(0, MAX_MESSAGE_LEN);
}

/**
 * Produce a string description of any value, suitable for the backend log.
 * Total: never throws, never returns undefined.
 */
export function describeReason(value: unknown): string {
    try {
        if (value instanceof Error) {
            return `${value.name}: ${value.message}\n${value.stack ?? ""}`.trimEnd();
        }
        if (typeof value === "string") {
            return value;
        }
        // Primitive types where String() is always safe and meaningful.
        if (
            value === null ||
            value === undefined ||
            typeof value === "number" ||
            typeof value === "boolean" ||
            typeof value === "bigint" ||
            typeof value === "symbol"
        ) {
            return String(value);
        }
        // Object: emit shape only (constructor + key names, no values) to avoid
        // leaking secrets into the backend log.
        return describeObjectShape(value as object);
    } catch {
        return "<undescribable>";
    }
}
