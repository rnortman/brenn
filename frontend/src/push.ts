/**
 * PWA push subscription helpers: enablePush() and disablePush().
 *
 * Caller responsibilities:
 *  - Pass a `BrennWs` instance that is currently connected.
 *  - Only call when the app has `pwa_push_enabled = true` (from Welcome).
 *
 * Design §2.6.3 — permission UI: gesture-gated, triggered from the
 * user-menu dropdown in the page header.
 */

import type { BrennWs } from "./ws.js";
import type { WsServerMessage } from "./generated/WsServerMessage.js";

/** Outcomes for enablePush(). */
export type EnablePushResult =
    | { ok: true }
    | { ok: false; reason: "permission_denied" | "not_supported" | "error"; detail?: string };

/**
 * Full enable-push flow (design §2.6.3):
 * 1. Register SW if not yet registered.
 * 2. Request Notification permission.
 * 3. Fetch VAPID public key via WS request/response.
 * 4. PushManager.subscribe().
 * 5. Send PushSubscribe over WS.
 *
 * Returns a result discriminant.  Never throws.
 */
export async function enablePush(ws: BrennWs): Promise<EnablePushResult> {
    if (!("serviceWorker" in navigator) || !("PushManager" in window)) {
        return { ok: false, reason: "not_supported" };
    }

    // (a) Register SW (idempotent — browser returns existing registration).
    let reg: ServiceWorkerRegistration;
    try {
        reg = await navigator.serviceWorker.register("/sw.js", { scope: "/" });
    } catch (e) {
        return { ok: false, reason: "error", detail: `SW registration failed: ${e}` };
    }

    // (b) Request notification permission (gesture-gated by browser).
    let permission: NotificationPermission;
    try {
        permission = await Notification.requestPermission();
    } catch (e) {
        return { ok: false, reason: "error", detail: `Permission request failed: ${e}` };
    }
    if (permission !== "granted") {
        return { ok: false, reason: "permission_denied" };
    }

    // (c) Fetch VAPID public key via WS.
    let vapidKey: string;
    try {
        vapidKey = await fetchVapidKey(ws);
    } catch (e) {
        return { ok: false, reason: "error", detail: `VAPID key fetch failed: ${e}` };
    }

    // (d) Subscribe via PushManager.
    let sub: PushSubscription;
    try {
        const appServerKey = urlBase64ToUint8Array(vapidKey);
        sub = await reg.pushManager.subscribe({
            userVisibleOnly: true,
            applicationServerKey: appServerKey.buffer as ArrayBuffer,
        });
    } catch (e) {
        // Some browsers reach here with NotAllowedError if the permission
        // check at step (b) was skipped or misreported (e.g. embedded views).
        if (e instanceof DOMException && e.name === "NotAllowedError") {
            return { ok: false, reason: "permission_denied" };
        }
        return { ok: false, reason: "error", detail: `PushManager.subscribe failed: ${e}` };
    }

    // (e) Send subscription to backend.
    const json = sub.toJSON();
    const p256dh = json.keys?.["p256dh"];
    const auth = json.keys?.["auth"];
    if (!json.endpoint || !p256dh || !auth) {
        return { ok: false, reason: "error", detail: "PushSubscription missing required fields" };
    }
    // SYNC: wire shape pinned by Rust test push_subscribe_deserializes_frontend_wire_shape
    // (brenn-lib/src/ws_types/tests/client.rs). Renaming a key here requires a
    // matching update to the WsClientMessage::PushSubscribe fields and that test.
    ws.send({ type: "PushSubscribe", endpoint: json.endpoint, p256dh, auth });

    return { ok: true };
}

/**
 * Disable-push flow (design §2.6.3):
 * 1. Unsubscribe via PushManager.
 * 2. Send PushUnsubscribe over WS.
 *
 * Never throws.
 */
export async function disablePush(ws: BrennWs): Promise<void> {
    try {
        if ("serviceWorker" in navigator) {
            const reg = await navigator.serviceWorker.getRegistration("/");
            if (reg) {
                const sub = await reg.pushManager.getSubscription();
                if (sub) {
                    await sub.unsubscribe();
                }
            }
        }
    } catch {
        // Unsubscribe failure is non-fatal: backend will still delete on next
        // 410/404 from the push service.
    }
    // Always tell the backend to delete the row.
    // SYNC: wire shape pinned by Rust test push_unsubscribe_deserializes_frontend_wire_shape
    // (brenn-lib/src/ws_types/tests/client.rs).
    ws.send({ type: "PushUnsubscribe" });
}

/**
 * Request the VAPID public key from the backend.  Sends `PushVapidKeyRequest`
 * and waits for the next `PushVapidKey` response.  Times out after 10s.
 */
function fetchVapidKey(ws: BrennWs): Promise<string> {
    return new Promise((resolve, reject) => {
        const timeout = window.setTimeout(() => {
            unregister();
            reject(new Error("Timed out waiting for PushVapidKey"));
        }, 10_000);

        function handler(msg: WsServerMessage): void {
            if (msg.type === "PushVapidKey") {
                unregister();
                resolve(msg.public_key_b64url);
            }
        }

        function unregister(): void {
            clearTimeout(timeout);
            ws.removeMessageHandler(handler);
        }

        ws.addMessageHandler(handler);
        // SYNC: wire shape pinned by Rust test push_vapid_key_request_deserializes_frontend_wire_shape
        // (brenn-lib/src/ws_types/tests/client.rs).
        ws.send({ type: "PushVapidKeyRequest" });
    });
}

/**
 * Convert a base64url-encoded VAPID public key to a Uint8Array suitable
 * for `applicationServerKey`.
 */
function urlBase64ToUint8Array(base64url: string): Uint8Array {
    // Pad to multiple of 4, replace URL-safe chars.
    const padded = base64url.replace(/-/g, "+").replace(/_/g, "/");
    const padding = "=".repeat((4 - (padded.length % 4)) % 4);
    const base64 = padded + padding;
    const raw = atob(base64);
    const bytes = new Uint8Array(raw.length);
    for (let i = 0; i < raw.length; i++) {
        bytes[i] = raw.charCodeAt(i);
    }
    return bytes;
}
