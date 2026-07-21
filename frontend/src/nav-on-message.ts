/**
 * nav-on-message.ts — global NavigateTo handler for non-app-shell pages.
 *
 * Loaded on every Brenn HTML page (landing /, /auth/login, /auth/register,
 * /app/*\/file/*, etc.). On `/app/*` shell pages, `<brenn-app>` is present in
 * static HTML and the app shell handles NavigateTo itself; this script
 * no-ops via the gate below.
 *
 * The listener is registered synchronously (no serviceWorker.ready await) so
 * that messages posted immediately after the page finishes loading are not
 * lost. `navigator.serviceWorker.addEventListener` works regardless of
 * controller state — the handler fires once a SW message arrives.
 *
 * Only navigates to same-origin `/app/*` paths: any other NavigateTo target is
 * rejected, matching the backend's validate_app_path invariant.
 */

import type { SwToAppMessage } from "./generated/SwToAppMessage.js";
import { parseSameOriginUrl } from "./url-util.js";

/**
 * Minimal interfaces for the injected globals, allowing tests to pass fakes.
 * Exported so tests can import and use them for typed fakes.
 */
export interface NavDocument {
    querySelector(selector: string): Element | null;
}

export interface NavNavigator {
    serviceWorker?: {
        addEventListener(type: "message", handler: (event: MessageEvent) => void): void;
    };
}

export interface NavWindow {
    location: { origin: string; assign(url: string): void };
}

/**
 * Core initialisation: gates on `<brenn-app>` absence + SW availability, then
 * registers the NavigateTo listener. Exported so tests can inject fakes and
 * verify the gate and handler independently.
 *
 * The module top-level calls this with the real browser globals.
 */
export function initNavOnMessage(
    doc: NavDocument,
    nav: NavNavigator,
    win: NavWindow,
): void {
    // Gate: if the app shell is present, defer to its listener.
    // Also guard on serviceWorker support — absent in some private-browsing modes and webviews.
    if (doc.querySelector("brenn-app") !== null || !nav.serviceWorker) {
        return;
    }

    nav.serviceWorker.addEventListener("message", (event: MessageEvent) => {
        const raw: unknown = event.data;
        if (raw === null || typeof raw !== "object") {
            return;
        }
        const data = raw as SwToAppMessage;
        if (data.type !== "NavigateTo") {
            return;
        }
        if (typeof data.url !== "string") {
            console.warn("nav-on-message: NavigateTo url is not a string", typeof data.url);
            return;
        }
        const url = data.url;
        const origin = win.location.origin;
        const parsed = parseSameOriginUrl(url, origin);
        if (parsed === null) {
            console.warn("nav-on-message: NavigateTo url is invalid or cross-origin, ignoring", url);
            return;
        }
        // Defense-in-depth: only navigate to /app/* paths. The SW derives
        // targetPath from a backend-validated push payload (validate_app_path
        // restricts to /app/...), so this check is normally redundant. It
        // guards against a future code path that could post an unexpected path.
        if (!parsed.pathname.startsWith("/app/")) {
            console.warn("nav-on-message: NavigateTo target is not an /app/ path, ignoring", url);
            return;
        }
        win.location.assign(parsed.href);
    });
}

// Module-level side effect: wire up with real browser globals.
initNavOnMessage(document, navigator, window);
