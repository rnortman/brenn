// No WebWorker or DOM globals — safe to import from either environment.

import type { PushClickTraceEvent } from "./generated/PushClickTraceEvent.js";
import type { SwToAppMessage } from "./generated/SwToAppMessage.js";
import { isFenixUserAgent } from "./sw-util.js";

export interface SwClient {
    id: string;
    url: string;
    focused: boolean;
    visibilityState: DocumentVisibilityState;
    type: "window" | "worker" | "sharedworker" | "all";
    focus: () => Promise<SwClient>;
    postMessage: (msg: unknown) => void;
}

export interface SwGlobals {
    location: { origin: string };
    clients: {
        matchAll: (opts: { type: ClientTypes; includeUncontrolled: boolean }) => Promise<readonly SwClient[]>;
        openWindow: (url: string) => Promise<SwClient | null>;
    };
    navigator: { userAgent: string };
}

// postPushClickTrace: post a PushClickTrace event to an /app/ client.
// Best-effort: caller-filtered appClients; empty list is a no-op.
function postPushClickTrace(
    appClients: SwClient[],
    event: PushClickTraceEvent,
    userId: number | null,
): void {
    if (appClients.length === 0) {
        return;
    }
    const msg = {
        type: "PushClickTrace",
        user_id: userId,
        event,
    } satisfies SwToAppMessage;
    appClients[0]!.postMessage(msg);
}

export async function handlePushSubscriptionChange(globals: SwGlobals): Promise<void> {
    // Notify all active clients so they can re-subscribe via WS.
    const clients = await globals.clients.matchAll({ type: "window", includeUncontrolled: true });
    for (const client of clients) {
        // Receiver re-subscribes from scratch via `_handleEnablePush()`; forwarding
        // the SW's already-issued subscription would be redundant. If the receiver
        // ever short-circuits to `setApplicationServerKey`, this field needs to come back.
        try {
            client.postMessage({ type: "PushSubscriptionChanged" } satisfies SwToAppMessage);
        } catch (err) {
            // Client detached between matchAll and postMessage — platform fault, not recoverable.
            // Log and continue so remaining clients still receive the notification.
            console.warn("[brenn-sw] pushsubscriptionchange: postMessage failed",
                { client_id: client.id, err: String(err) });
        }
    }
}

export async function handleNotificationClick(
    globals: SwGlobals,
    event: { notification: { data: unknown } },
): Promise<void> {
    const data = event.notification.data as Record<string, unknown> | null;
    const origin = globals.location.origin;

    // Parse data.url once; derive both openWindowUrl (for openWindow)
    // and targetPath (bare path for T1's NavigateTo postMessage).
    // Falls back to origin root "/" on parse failure or cross-origin.
    let openWindowUrl = new URL("/", origin).href;
    let targetPath = "/";
    if (data && typeof data["url"] === "string") {
        try {
            const parsed = new URL(data["url"], origin);
            if (parsed.origin === origin) {
                openWindowUrl = parsed.href;
                // Compute targetPath:
                //   If path starts with /r/: read to= param; missing or cross-origin → "/"
                //   Otherwise: use parsed URL's path+query+fragment
                if (parsed.pathname.startsWith("/r/")) {
                    const to = parsed.searchParams.get("to");
                    if (to !== null) {
                        try {
                            const toParsed = new URL(to, origin);
                            if (toParsed.origin === origin) {
                                targetPath = toParsed.pathname + toParsed.search + toParsed.hash;
                            }
                        } catch {
                            // Malformed to= → fall back to "/".
                        }
                    }
                } else {
                    targetPath = parsed.pathname + parsed.search + parsed.hash;
                }
            }
        } catch {
            // Malformed URL — fall back to root "/".
        }
    }

    const payloadUserId: number | null =
        data && typeof data["user_id"] === "number" ? (data["user_id"] as number) : null;
    const convId: unknown = data ? data["conversation_id"] : undefined;

    // Fenix bypass: Firefox-Android focus() is a Gecko-level no-op that does not
    // bring the targeted tab to the foreground (Mozilla bug 1880000). Gate before
    // matchAll so clients.matchAll is genuinely skipped on Fenix.
    // Trade-off accepted: possible duplicate Brenn tab vs. invisible no-op focus.
    // Trace events here use an empty client list — no matchAll has run yet.
    if (isFenixUserAgent(globals.navigator.userAgent)) {
        const noClients: SwClient[] = [];
        postPushClickTrace(noClients, { type: "FenixCascadeSkipped" }, payloadUserId);
        // Jump directly to openWindow. Skipped: matchAll, HandlerEntry,
        // MatchAllResult, BrennClientsFilter, T1Chosen/T1Skipped,
        // focus(), NavigateTo postMessage.
        postPushClickTrace(noClients, { type: "OpenWindowCalled", url: openWindowUrl }, payloadUserId);
        let fenixClient: SwClient | null;
        try {
            fenixClient = await globals.clients.openWindow(openWindowUrl);
        } catch (err) {
            console.warn("[brenn-sw] Fenix: openWindow rejected", { err: String(err), conv_id: convId });
            postPushClickTrace(noClients, { type: "Terminal", branch: "open_window_rejected" }, payloadUserId);
            return;
        }
        postPushClickTrace(
            noClients,
            { type: "OpenWindowResult", opened_url: fenixClient ? fenixClient.url : null },
            payloadUserId,
        );
        if (fenixClient !== null) {
            postPushClickTrace(noClients, { type: "Terminal", branch: "t2_t3_opened" }, payloadUserId);
            console.log("[brenn-sw] Fenix T2/T3: opened new window", { conv_id: convId });
        } else {
            postPushClickTrace(noClients, { type: "Terminal", branch: "no_eligible_target" }, payloadUserId);
            console.log("[brenn-sw] Fenix: openWindow returned null; no-op", { conv_id: convId });
        }
        return;
    }

    // Enumerate all window clients.
    const allClients = await globals.clients.matchAll({ type: "window", includeUncontrolled: true });

    // Filter to same-origin Brenn clients (any Brenn page, not just /app/*).
    // Dropped clients: different_origin or url_parse_error only.
    const keptClients: SwClient[] = [];
    const droppedClients: { id: string; url: string; reason: string }[] = [];
    for (const c of allClients) {
        try {
            const cu = new URL(c.url);
            if (cu.origin === origin) {
                keptClients.push(c);
            } else {
                droppedClients.push({ id: c.id, url: c.url, reason: "different_origin" });
            }
        } catch {
            droppedClients.push({ id: c.id, url: c.url, reason: "url_parse_error" });
        }
    }
    const brennClients = keptClients;
    // Pre-filter to /app/* clients once; postPushClickTrace no longer re-parses
    // URLs on every call. Fenix branch uses noClients (empty) and is unaffected.
    const appClients = brennClients.filter((c) => {
        try {
            return new URL(c.url).pathname.startsWith("/app/");
        } catch {
            return false;
        }
    });
    // Diagnose the case where Brenn tabs exist but none are on /app/* (e.g. only
    // /admin/ or /auth/ tabs open). postPushClickTrace will fast-path on the
    // empty appClients list; without this warn the drop is silent.
    if (brennClients.length > 0 && appClients.length === 0) {
        console.warn("[brenn-sw] push-click: Brenn clients found but none on /app/* — trace dropped");
    }

    // Checkpoint 1: HandlerEntry.
    postPushClickTrace(
        appClients,
        {
            type: "HandlerEntry",
            target_user_id: payloadUserId,
            target_path: targetPath,
            redirector_url: openWindowUrl,
            payload_keys: data ? Object.keys(data) : [],
        },
        payloadUserId,
    );

    // Checkpoint 2: MatchAllResult.
    postPushClickTrace(
        appClients,
        {
            type: "MatchAllResult",
            clients: allClients.map((c) => ({
                id: c.id,
                url: c.url,
                focused: c.focused,
                visibility_state: c.visibilityState,
                type: c.type,
            })),
        },
        payloadUserId,
    );

    // Checkpoint 3: BrennClientsFilter.
    postPushClickTrace(
        appClients,
        {
            type: "BrennClientsFilter",
            kept: brennClients.map((c) => c.id),
            dropped_with_reason: droppedClients,
        },
        payloadUserId,
    );

    // ---------------------------------------------------------------------------
    // T1 — any same-origin Brenn client present.
    // Priority: focused > visible > first by matchAll order.
    // ---------------------------------------------------------------------------
    let chosen: SwClient | null = null;
    for (const c of brennClients) {
        if (c.focused) { chosen = c; break; }
    }
    if (!chosen) {
        for (const c of brennClients) {
            if (c.visibilityState === "visible") { chosen = c; break; }
        }
    }
    if (!chosen && brennClients.length > 0) {
        chosen = brennClients[0]!;
    }

    if (chosen) {
        let focusRejected = false;
        try {
            await chosen.focus();
        } catch (err) {
            // focus() rejected — log and continue. Still post NavigateTo and return.
            // Do NOT fall through to openWindow: on non-Fenix platforms focus() rejection
            // is rare; opening a duplicate tab is worse than relying on platform
            // notification-click semantics to surface the focused window.
            focusRejected = true;
            console.warn("[brenn-sw] T1: focus() rejected (platform may surface window anyway)",
                { err: String(err), conv_id: convId });
        }
        // Checkpoint: T1Chosen (after focus attempt so focus_rejected is known).
        postPushClickTrace(
            appClients,
            { type: "T1Chosen", client_id: chosen.id, target_path: targetPath, focus_rejected: focusRejected },
            payloadUserId,
        );
        chosen.postMessage({ type: "NavigateTo", url: targetPath } satisfies SwToAppMessage);
        // Checkpoint: Terminal.
        postPushClickTrace(appClients, { type: "Terminal", branch: "t1_posted" }, payloadUserId);
        console.log("[brenn-sw] T1: posted NavigateTo to existing client", { conv_id: convId });
        return;
    }

    // Checkpoint: T1Skipped — no same-origin Brenn clients.
    postPushClickTrace(
        appClients,
        { type: "T1Skipped", reason: "no_brenn_clients" },
        payloadUserId,
    );

    // ---------------------------------------------------------------------------
    // openWindow — no same-origin Brenn clients present.
    // ---------------------------------------------------------------------------
    postPushClickTrace(appClients, { type: "OpenWindowCalled", url: openWindowUrl }, payloadUserId);
    let newClient: SwClient | null;
    try {
        newClient = await globals.clients.openWindow(openWindowUrl);
    } catch (err) {
        // openWindow can reject (e.g. "requires user gesture" on some platforms/builds).
        console.warn("[brenn-sw] openWindow rejected", { err: String(err), conv_id: convId });
        postPushClickTrace(appClients, { type: "Terminal", branch: "open_window_rejected" }, payloadUserId);
        return;
    }
    postPushClickTrace(
        appClients,
        { type: "OpenWindowResult", opened_url: newClient ? newClient.url : null },
        payloadUserId,
    );
    if (newClient !== null) {
        postPushClickTrace(appClients, { type: "Terminal", branch: "t2_t3_opened" }, payloadUserId);
        console.log("[brenn-sw] T2/T3: opened new window", { conv_id: convId });
    } else {
        postPushClickTrace(appClients, { type: "Terminal", branch: "no_eligible_target" }, payloadUserId);
        console.log("[brenn-sw] openWindow returned null; no-op", { conv_id: convId });
    }
}
