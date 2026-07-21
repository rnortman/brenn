/// <reference lib="webworker" />
// Service worker: share target + PWA push notification handling.
// No caching, no offline support.

import { openShareDb } from "./share-db.js";
import type { ShareData } from "./share-db.js";
import { readSignedInUserIds } from "./push-db.js";
import type { PushPayload } from "./generated/PushPayload.js";
import { handleNotificationClick, handlePushSubscriptionChange } from "./sw-handler.js";
import type { SwClient, SwGlobals } from "./sw-handler.js";

declare const self: ServiceWorkerGlobalScope;

self.addEventListener("install", () => {
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);

  if (url.pathname === "/share-target" && event.request.method === "POST") {
    event.respondWith(handleShareTarget(event.request));
    return;
  }

  // Everything else: pass through to network.
});

// ---------------------------------------------------------------------------
// Push notification handlers
// ---------------------------------------------------------------------------

self.addEventListener("push", (event) => {
    event.waitUntil(handlePushEvent(event));
});

async function handlePushEvent(event: PushEvent): Promise<void> {
    let payload: PushPayload | null = null;
    try {
        if (event.data) {
            payload = event.data.json() as PushPayload;
        }
    } catch {
        // Malformed JSON — fall through to generic notification (§ SW behavior).
    }

    if (payload !== null) {
        // Defense-in-depth: drop if user_id not in signed_in_user_ids set.
        // Server-side current-user check is the primary defense (§2.7.3 step 7).
        const signedIn = await readSignedInUserIds();
        if (!signedIn.includes(payload.user_id)) {
            // User logged out or push is for a different signed-in user — drop.
            return;
        }

        const options: NotificationOptions = {
            body: payload.body,
        };
        if (payload.icon) options.icon = payload.icon;
        if (payload.badge) options.badge = payload.badge;
        if (payload.tag) options.tag = payload.tag;
        // Carry through `data` (deep-link URL etc.) for notificationclick.
        // Merge user_id into data for potential SW-side use; never shown to OS.
        const data: Record<string, unknown> = { ...(payload.data ?? {}), user_id: payload.user_id };
        options.data = data;

        await self.registration.showNotification(payload.title, options);
    } else {
        // Missing or unparseable payload: show a generic notification only if
        // at least one user is signed in on this device. Without a valid payload
        // we can't check which user this push targets, so we fall back to
        // "any signed-in user" as the guard. This prevents spoofed malformed
        // pushes from showing notifications on idle devices (errhandling-8).
        const signedIn = await readSignedInUserIds();
        if (signedIn.length > 0) {
            await self.registration.showNotification("New message", {
                body: "You have a new notification.",
            });
        }
    }
}

// Adapt ServiceWorkerGlobalScope to SwGlobals without double-casting self.
// The two casts below are narrow and safe: matchAll({type:"window",...})
// returns WindowClient[] (a structural superset of SwClient[]) at runtime;
// openWindow returns WindowClient | null (superset of SwClient | null).
// TypeScript's conditional return type for matchAll doesn't resolve cleanly
// here, so we cast via unknown at the exact point of divergence.
function makeSwGlobals(): SwGlobals {
    return {
        location: self.location,
        navigator: self.navigator,
        clients: {
            matchAll: (opts) =>
                self.clients.matchAll(opts) as unknown as Promise<readonly SwClient[]>,
            openWindow: (url) =>
                self.clients.openWindow(url) as Promise<SwClient | null>,
        },
    };
}

self.addEventListener("notificationclick", (event) => {
    event.notification.close();
    event.waitUntil(handleNotificationClick(makeSwGlobals(), event));
});

self.addEventListener("pushsubscriptionchange", (event) => {
    // Firefox fires this when the push service rotates the subscription.
    // Re-subscribe and forward the new subscription to the backend via WS.
    // We post a message to all controlled clients; the app.ts handler
    // calls enablePush() which runs the full re-subscribe flow.
    event.waitUntil(handlePushSubscriptionChange(makeSwGlobals()));
});

// ---------------------------------------------------------------------------
// Share target handler
// ---------------------------------------------------------------------------

async function handleShareTarget(request: Request): Promise<Response> {
  const formData = await request.formData();

  const title = formData.get("title") as string | null;
  const text = formData.get("text") as string | null;
  const shareUrl = formData.get("url") as string | null;
  const file = formData.get("file") as File | null;

  const shareData: ShareData = {
    id: `${Date.now()}-${Math.random().toString(36).slice(2)}`,
    title,
    text,
    url: shareUrl,
    file: file
      ? { name: file.name, type: file.type, data: await file.arrayBuffer() }
      : null,
  };

  const db = await openShareDb();
  const tx = db.transaction("pending", "readwrite");
  tx.objectStore("pending").put(shareData);
  await new Promise<void>((resolve, reject) => {
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
  db.close();

  return Response.redirect("/", 303);
}
