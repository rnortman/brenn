// @vitest-environment happy-dom
/**
 * Unit tests for handleNotificationClick in sw-handler.ts.
 *
 * sw.ts declares `self: ServiceWorkerGlobalScope` and is not importable as a
 * module directly. sw-handler.ts extracts the handler with SW globals as
 * explicit parameters, following the pattern of sw-util.ts.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { isFenixUserAgent } from "./sw-util.js";
import { handleNotificationClick, handlePushSubscriptionChange } from "./sw-handler.js";
import type { SwGlobals, SwClient } from "./sw-handler.js";

// ---------------------------------------------------------------------------
// Fake types
// ---------------------------------------------------------------------------

interface FakeWindowClient extends SwClient {
    navigate: (url: string) => Promise<FakeWindowClient | null>;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const ORIGIN = "https://brenn.example";

let idCounter = 0;

function makeClient(
    pathname: string,
    opts: Partial<FakeWindowClient> = {},
): FakeWindowClient {
    return {
        id: `fake-client-${idCounter++}`,
        url: `${ORIGIN}${pathname}`,
        focused: false,
        visibilityState: "hidden",
        type: "window",
        focus: vi.fn(async function(this: FakeWindowClient) { return this; }),
        postMessage: vi.fn(),
        navigate: vi.fn(async () => null),
        ...opts,
    };
}

function makeSelf(
    clients: FakeWindowClient[],
    openWindow?: () => Promise<FakeWindowClient | null>,
    userAgent?: string,
): SwGlobals {
    return {
        location: { origin: ORIGIN },
        clients: {
            matchAll: vi.fn(async () => clients),
            openWindow: vi.fn(openWindow ?? (async () => null)),
        },
        navigator: { userAgent: userAgent ?? "" },
    };
}

function buildData(url: string | undefined, userId: number | null): Record<string, unknown> | null {
    if (url === undefined && userId === null) return null;
    const d: Record<string, unknown> = {};
    if (url !== undefined) d["url"] = url;
    if (userId !== null) d["user_id"] = userId;
    return d;
}

/**
 * Find the first PushClickTrace postMessage call on a mock client whose
 * event.type matches the given eventType, or null if none exists.
 * Pass undefined for eventType to match any PushClickTrace (including those
 * without an event field, e.g. the negative-assertion case).
 */
function findTrace(
    client: { postMessage: unknown },
    eventType?: string,
): unknown[] | null {
    const calls = (client.postMessage as ReturnType<typeof vi.fn>).mock.calls;
    const found = calls.find((args) => {
        const msg = args[0] as { type: string; event?: { type: string } };
        if (msg.type !== "PushClickTrace") return false;
        if (eventType === undefined) return true;
        return msg.event?.type === eventType;
    });
    return found ?? null;
}

beforeEach(() => { idCounter = 0; });

// ---------------------------------------------------------------------------
// Tests: §4 cascade tests (rewritten for simplified cascade)
// ---------------------------------------------------------------------------

describe("handleNotificationClick SW cascade", () => {

    // Test 1: T1 — single /app/* client.
    it("T1: focuses /app/* client and posts NavigateTo without user_id", async () => {
        const client = makeClient("/app/graf/c/17", { focused: true, visibilityState: "visible" });
        const fakeSelf = makeSelf([client]);

        await handleNotificationClick(fakeSelf, { notification: { data: buildData("/r/abc-nonce?to=/app/graf/c/9", 1) } });

        expect(client.focus).toHaveBeenCalled();
        expect(client.postMessage).toHaveBeenCalledWith({
            type: "NavigateTo",
            url: "/app/graf/c/9",
        });
        expect(fakeSelf.clients.openWindow).not.toHaveBeenCalled();
    });

    // Test 2: T1 with single non-/app/* Brenn client (e.g. /auth/login).
    // Confirms the /app/* filter is gone; auth-page tabs are now eligible for T1.
    it("T1: focuses /auth/login client — /app/* filter is gone", async () => {
        const client = makeClient("/auth/login", { focused: false, visibilityState: "visible" });
        const fakeSelf = makeSelf([client]);

        await handleNotificationClick(fakeSelf, { notification: { data: buildData("/r/abc?to=/app/graf/c/9", 1) } });

        expect(client.focus).toHaveBeenCalled();
        expect(client.postMessage).toHaveBeenCalledWith({
            type: "NavigateTo",
            url: "/app/graf/c/9",
        });
        expect(fakeSelf.clients.openWindow).not.toHaveBeenCalled();
    });

    // Test A (Gap 1): T1 with /auth/login chosen — no PushClickTrace posted; console.warn fires.
    it("T1 with non-/app/* chosen: no PushClickTrace posted; console.warn fires", async () => {
        const client = makeClient("/auth/login", { focused: false, visibilityState: "visible" });
        const fakeSelf = makeSelf([client]);
        const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

        await handleNotificationClick(fakeSelf, { notification: { data: buildData("/r/abc?to=/app/graf/c/9", 1) } });

        // T1 still posts NavigateTo to the chosen client.
        expect(client.postMessage).toHaveBeenCalledWith({
            type: "NavigateTo",
            url: "/app/graf/c/9",
        });
        // No PushClickTrace should have been posted — appClients is empty so
        // postPushClickTrace fast-paths out at sw-handler.ts:33-35.
        expect(findTrace(client)).toBeNull();
        // console.warn fires the "none on /app/*" diagnostic.
        expect(warnSpy).toHaveBeenCalledWith(
            expect.stringContaining("[brenn-sw] push-click: Brenn clients found but none on /app/*"),
        );

        warnSpy.mockRestore();
    });

    // Test 3: T1 with mixed /app/* and non-/app/* clients.
    // All are eligible; selection priority applies across the combined set.
    it("T1: mixed /app/* and non-/app/* clients — focused wins", async () => {
        const loginClient = makeClient("/auth/login", { focused: true, visibilityState: "visible" });
        const appClient = makeClient("/app/foo/c/1", { focused: false, visibilityState: "visible" });
        const fakeSelf = makeSelf([loginClient, appClient]);

        await handleNotificationClick(fakeSelf, { notification: { data: buildData("/r/n?to=/app/foo/c/9", 1) } });

        expect(loginClient.focus).toHaveBeenCalled();
        expect(appClient.focus).not.toHaveBeenCalled();
        expect(loginClient.postMessage).toHaveBeenCalledWith({
            type: "NavigateTo",
            url: "/app/foo/c/9",
        });
    });

    // Test 4: T1 with multiple clients — selection priority.
    it("T1: focused client wins over visible-but-not-focused; visible wins over hidden-first", async () => {
        const hidden = makeClient("/app/foo", { focused: false, visibilityState: "hidden" });
        const visible = makeClient("/app/bar", { focused: false, visibilityState: "visible" });
        const focused = makeClient("/app/baz", { focused: true, visibilityState: "visible" });
        const fakeSelf = makeSelf([hidden, visible, focused]);

        await handleNotificationClick(fakeSelf, { notification: { data: buildData("/r/n?to=/app/x/c/1", 1) } });

        expect(focused.focus).toHaveBeenCalled();
        expect(visible.focus).not.toHaveBeenCalled();
        expect(hidden.focus).not.toHaveBeenCalled();
    });

    it("T1: visible wins over hidden when no focused client", async () => {
        const hidden = makeClient("/app/foo", { focused: false, visibilityState: "hidden" });
        const visible = makeClient("/app/bar", { focused: false, visibilityState: "visible" });
        const fakeSelf = makeSelf([hidden, visible]);

        await handleNotificationClick(fakeSelf, { notification: { data: buildData("/app/x/c/1", 1) } });

        expect(visible.focus).toHaveBeenCalled();
        expect(hidden.focus).not.toHaveBeenCalled();
    });

    // Test 5: T1 with focus() rejecting — NavigateTo still posted; no openWindow.
    // Asserts focus_rejected=true in the T1Chosen trace event via postMessage.
    // The trace goes to appClients[0] — the first /app/* client in matchAll order,
    // which is the focused client itself (also at /app/*).
    it("T1: focus() rejected → NavigateTo still posted; no openWindow; focus_rejected=true in trace", async () => {
        // focusClient is at /app/* (so it's in appClients) and is the focused one.
        // appClients[0] = focusClient, so trace is posted to focusClient.postMessage.
        const focusClient = makeClient("/app/foo", { focused: true });
        focusClient.focus = vi.fn(async () => { throw new Error("no focus rights"); });
        const fakeSelf = makeSelf([focusClient], async () => ({ } as FakeWindowClient));
        const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

        await handleNotificationClick(fakeSelf, { notification: { data: buildData("/r/nonce?to=/app/foo/c/7", 2) } });

        expect(focusClient.postMessage).toHaveBeenCalledWith({
            type: "NavigateTo",
            url: "/app/foo/c/7",
        });
        // openWindow NOT called — focus() rejection does not fall through.
        expect(fakeSelf.clients.openWindow).not.toHaveBeenCalled();

        // T1Chosen trace event must have focus_rejected=true when focus() throws.
        // The trace is posted to appClients[0] (focusClient) via PushClickTrace postMessage.
        const t1ChosenCall = findTrace(focusClient, "T1Chosen");
        expect(t1ChosenCall).not.toBeNull();
        expect((t1ChosenCall![0] as { event: { focus_rejected: boolean } }).event.focus_rejected).toBe(true);

        warnSpy.mockRestore();
    });

    // Test 5b: T1 with focus() succeeding → focus_rejected=false in T1Chosen trace.
    it("T1: focus() succeeds → focus_rejected=false in T1Chosen trace", async () => {
        const focusClient = makeClient("/app/foo", { focused: true });
        const fakeSelf = makeSelf([focusClient]);

        await handleNotificationClick(fakeSelf, { notification: { data: buildData("/r/nonce?to=/app/foo/c/7", 2) } });

        const t1ChosenCall = findTrace(focusClient, "T1Chosen");
        expect(t1ChosenCall).not.toBeNull();
        expect((t1ChosenCall![0] as { event: { focus_rejected: boolean } }).event.focus_rejected).toBe(false);
    });

    // Test 6: No same-origin Brenn clients → openWindow(redirectorUrl).
    it("openWindow called when no same-origin Brenn clients exist", async () => {
        const fakeSelf = makeSelf([], async () => ({ } as FakeWindowClient));

        await handleNotificationClick(fakeSelf, { notification: { data: buildData("/r/abc?to=/app/graf/c/9", 1) } });

        expect(fakeSelf.clients.openWindow).toHaveBeenCalledWith(`${ORIGIN}/r/abc?to=/app/graf/c/9`);
    });

    // Test 7: Cross-origin clients only — filtered out; openWindow called.
    it("cross-origin clients only → filtered; openWindow called", async () => {
        const crossOrigin: FakeWindowClient = {
            id: "cross-1",
            url: "https://other.example/app/foo",
            focused: false,
            visibilityState: "visible" as const,
            type: "window",
            focus: vi.fn(async function(this: FakeWindowClient) { return this; }),
            postMessage: vi.fn(),
            navigate: vi.fn(async () => null),
        };
        const fakeSelf = makeSelf([crossOrigin], async () => ({ } as FakeWindowClient));

        await handleNotificationClick(fakeSelf, { notification: { data: buildData("/r/x?to=/app/x/c/1", 1) } });

        expect(crossOrigin.focus).not.toHaveBeenCalled();
        expect(fakeSelf.clients.openWindow).toHaveBeenCalled();
    });

    // Test 7b: url_parse_error client only — filtered out; openWindow called.
    // Covers the `catch { // url_parse_error — drop. }` branch (sw-handler.ts).
    it("url_parse_error client only → filtered; openWindow called", async () => {
        const malformedClient: FakeWindowClient = {
            id: "bad-url-1",
            url: "not-a-url",
            focused: false,
            visibilityState: "visible" as const,
            type: "window",
            focus: vi.fn(async function(this: FakeWindowClient) { return this; }),
            postMessage: vi.fn(),
            navigate: vi.fn(async () => null),
        };
        const fakeSelf = makeSelf([malformedClient], async () => ({ } as FakeWindowClient));

        await handleNotificationClick(fakeSelf, { notification: { data: buildData("/r/x?to=/app/x/c/1", 1) } });

        expect(malformedClient.focus).not.toHaveBeenCalled();
        expect(fakeSelf.clients.openWindow).toHaveBeenCalled();
    });

    // Test 8: openWindow returns null → logs no-op.
    it("openWindow returns null → no crash; no-op log", async () => {
        const fakeSelf = makeSelf([], async () => null);
        const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});

        await expect(
            handleNotificationClick(fakeSelf, { notification: { data: buildData("/r/n?to=/app/x/c/1", 1) } })
        ).resolves.toBeUndefined();

        expect(logSpy).toHaveBeenCalledWith(
            expect.stringContaining("[brenn-sw] openWindow returned null"),
            expect.anything(),
        );
        logSpy.mockRestore();
    });

    // Test 9: openWindow rejects → caught; no unhandled rejection; warn fires.
    it("openWindow rejects → caught; no unhandled rejection", async () => {
        const fakeSelf = makeSelf([], async () => { throw new Error("no user gesture"); });
        const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

        await expect(
            handleNotificationClick(fakeSelf, { notification: { data: buildData("/r/n?to=/app/x/c/1", 1) } })
        ).resolves.toBeUndefined();

        expect(warnSpy).toHaveBeenCalledWith(
            expect.stringContaining("[brenn-sw] openWindow rejected"),
            expect.anything(),
        );
        warnSpy.mockRestore();
    });

    // Test 10: targetPath decoding.
    it("/r/<nonce>?to=/app/x/c/9 → targetPath = /app/x/c/9", async () => {
        const client = makeClient("/app/foo", { focused: true });
        const fakeSelf = makeSelf([client]);

        await handleNotificationClick(fakeSelf, { notification: { data: buildData("/r/some-nonce?to=/app/x/c/9", 1) } });

        expect(client.postMessage).toHaveBeenCalledWith({
            type: "NavigateTo",
            url: "/app/x/c/9",
        });
    });

    it("bare /app/x/c/9 → targetPath = /app/x/c/9", async () => {
        const client = makeClient("/app/foo", { focused: true });
        const fakeSelf = makeSelf([client]);

        await handleNotificationClick(fakeSelf, { notification: { data: buildData("/app/x/c/9", 1) } });

        expect(client.postMessage).toHaveBeenCalledWith({
            type: "NavigateTo",
            url: "/app/x/c/9",
        });
    });

    it("cross-origin data.url → targetPath = '/', openWindowUrl = origin root", async () => {
        const fakeSelf = makeSelf([], async () => ({ } as FakeWindowClient));

        await handleNotificationClick(fakeSelf, { notification: { data: buildData("https://evil.example/steal", 1) } });

        expect(fakeSelf.clients.openWindow).toHaveBeenCalledWith(`${ORIGIN}/`);
    });

    it("cross-origin to= → targetPath = '/'", async () => {
        const client = makeClient("/app/foo", { focused: true });
        const fakeSelf = makeSelf([client]);

        await handleNotificationClick(fakeSelf, { notification: { data: buildData("/r/x?to=https://evil/steal", 1) } });

        expect(client.postMessage).toHaveBeenCalledWith({
            type: "NavigateTo",
            url: "/",
        });
    });

    // Test 12: logging — each terminal branch emits a [brenn-sw] log line.
    it("logging: T1 success emits [brenn-sw] T1 log", async () => {
        const client = makeClient("/app/foo", { focused: true });
        const fakeSelf = makeSelf([client]);
        const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});

        await handleNotificationClick(fakeSelf, { notification: { data: buildData("/r/n?to=/app/foo/c/1", 1) } });

        expect(logSpy).toHaveBeenCalledWith(expect.stringContaining("[brenn-sw] T1"), expect.anything());
        logSpy.mockRestore();
    });

    it("logging: T2/T3 emits [brenn-sw] T2/T3 log", async () => {
        const fakeSelf = makeSelf([], async () => ({ } as FakeWindowClient));
        const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});

        await handleNotificationClick(fakeSelf, { notification: { data: buildData("/r/n?to=/app/foo/c/1", 1) } });

        expect(logSpy).toHaveBeenCalledWith(expect.stringContaining("[brenn-sw] T2/T3"), expect.anything());
        logSpy.mockRestore();
    });

    // NavigateTo does NOT carry user_id (field dropped per §2.3).
    it("NavigateTo message has no user_id field", async () => {
        const client = makeClient("/auth/register", { focused: true });
        const fakeSelf = makeSelf([client]);

        await handleNotificationClick(fakeSelf, { notification: { data: buildData("/r/n?to=/app/foo/c/1", 42) } });

        const call = (client.postMessage as ReturnType<typeof vi.fn>).mock.calls.find(
            (args) => (args[0] as { type: string }).type === "NavigateTo"
        );
        expect(call).toBeDefined();
        expect(call![0]).not.toHaveProperty("user_id");
    });

    // Landing page "/" is eligible for T1.
    it("T1: landing page '/' client is eligible (any same-origin Brenn page)", async () => {
        const landingClient = makeClient("/", { focused: false, visibilityState: "visible" });
        const fakeSelf = makeSelf([landingClient]);

        await handleNotificationClick(fakeSelf, { notification: { data: buildData("/app/foo/c/1", 1) } });

        expect(landingClient.focus).toHaveBeenCalled();
        expect(landingClient.postMessage).toHaveBeenCalledWith({
            type: "NavigateTo",
            url: "/app/foo/c/1",
        });
        expect(fakeSelf.clients.openWindow).not.toHaveBeenCalled();
    });

    // payloadUserId=null: same-origin client still wins T1 (no null-guard like old cascade).
    it("payloadUserId=null: same-origin client still chosen for T1", async () => {
        const client = makeClient("/app/foo", { focused: true });
        const fakeSelf = makeSelf([client]);

        await handleNotificationClick(fakeSelf, { notification: { data: buildData("/app/foo/c/1", null) } });

        expect(client.focus).toHaveBeenCalled();
        expect(client.postMessage).toHaveBeenCalledWith({
            type: "NavigateTo",
            url: "/app/foo/c/1",
        });
        expect(fakeSelf.clients.openWindow).not.toHaveBeenCalled();
    });

    // "Wrong user" /app/* client: still chosen for T1 (no identity check anymore).
    it("T1: 'wrong user' /app/* client still chosen (no user_id filter)", async () => {
        const wrongUser = makeClient("/app/foo", { focused: false, visibilityState: "visible" });
        const fakeSelf = makeSelf([wrongUser]);

        await handleNotificationClick(fakeSelf, { notification: { data: buildData("/app/bar", 2) } });

        expect(wrongUser.focus).toHaveBeenCalled();
        expect(wrongUser.postMessage).toHaveBeenCalledWith({
            type: "NavigateTo",
            url: "/app/bar",
        });
        expect(fakeSelf.clients.openWindow).not.toHaveBeenCalled();
    });

    // Test B (Gap 2): explicit null notification data.
    // Passes { notification: { data: null } } directly (not via buildData).
    // Validates: targetPath="/", payloadUserId=null, payload_keys=[] end-to-end.
    it("data=null: targetPath='/', payloadUserId=null, payload_keys=[]", async () => {
        // /app/foo client keeps appClients non-empty so trace messages flow.
        const client = makeClient("/app/foo", { focused: true, visibilityState: "visible" });
        const fakeSelf = makeSelf([client]);

        await handleNotificationClick(fakeSelf, { notification: { data: null } });

        // NavigateTo must carry "/" — null data means targetPath = "/".
        expect(client.postMessage).toHaveBeenCalledWith({
            type: "NavigateTo",
            url: "/",
        });

        // HandlerEntry trace must reflect null data state:
        // target_path="/", payload_keys=[], target_user_id=null.
        const handlerEntryCall = findTrace(client, "HandlerEntry");
        expect(handlerEntryCall).not.toBeNull();
        if (handlerEntryCall === null) return; // type-narrow; not.toBeNull() above fails the test
        const handlerEntryMsg = handlerEntryCall[0] as {
            user_id: number | null;
            event: { target_path: string; payload_keys: string[]; target_user_id: number | null };
        };
        expect(handlerEntryMsg.user_id).toBeNull();
        expect(handlerEntryMsg.event.target_path).toBe("/");
        expect(handlerEntryMsg.event.payload_keys).toEqual([]);
        expect(handlerEntryMsg.event.target_user_id).toBeNull();
    });

    // data.url absent → targetPath = "/" and openWindowUrl = origin root.
    it("data.url absent → targetPath = '/', openWindowUrl = origin root", async () => {
        const client = makeClient("/app/foo", { focused: true });
        const fakeSelf = makeSelf([client]);

        await handleNotificationClick(fakeSelf, { notification: { data: buildData(undefined, 1) } });

        expect(client.postMessage).toHaveBeenCalledWith({
            type: "NavigateTo",
            url: "/",
        });
    });
});

// ---------------------------------------------------------------------------
// Tests: isFenixUserAgent predicate
// ---------------------------------------------------------------------------

describe("isFenixUserAgent", () => {
    it("returns true for a Fenix (Firefox-Android) UA string", () => {
        const fenixUa = "Mozilla/5.0 (Android 14; Mobile; rv:136.0) Gecko/136.0 Firefox/136.0";
        expect(isFenixUserAgent(fenixUa)).toBe(true);
    });

    it("returns false for Chrome on Android", () => {
        const chromeAndroidUa = "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36";
        expect(isFenixUserAgent(chromeAndroidUa)).toBe(false);
    });

    it("returns false for Firefox on desktop", () => {
        const desktopFirefoxUa = "Mozilla/5.0 (X11; Linux x86_64; rv:136.0) Gecko/20100101 Firefox/136.0";
        expect(isFenixUserAgent(desktopFirefoxUa)).toBe(false);
    });

    it("returns false for Safari on iOS", () => {
        const safariIosUa = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1";
        expect(isFenixUserAgent(safariIosUa)).toBe(false);
    });

    it("returns false for empty string", () => {
        expect(isFenixUserAgent("")).toBe(false);
    });
});

// ---------------------------------------------------------------------------
// Tests: handlePushSubscriptionChange
// ---------------------------------------------------------------------------

describe("handlePushSubscriptionChange", () => {
    it("broadcasts PushSubscriptionChanged to all clients", async () => {
        const c1 = makeClient("/app/foo");
        const c2 = makeClient("/app/bar");
        const c3 = makeClient("/");
        const fakeSelf = makeSelf([c1, c2, c3]);

        await handlePushSubscriptionChange(fakeSelf);

        for (const c of [c1, c2, c3]) {
            expect(c.postMessage).toHaveBeenCalledOnce();
            expect(c.postMessage).toHaveBeenCalledWith({ type: "PushSubscriptionChanged" });
        }
    });

    it("calls matchAll with { type: 'window', includeUncontrolled: true }", async () => {
        const fakeSelf = makeSelf([]);

        await handlePushSubscriptionChange(fakeSelf);

        expect(fakeSelf.clients.matchAll).toHaveBeenCalledWith({ type: "window", includeUncontrolled: true });
    });

    it("handles zero clients without error; matchAll still called", async () => {
        const fakeSelf = makeSelf([]);

        await expect(handlePushSubscriptionChange(fakeSelf)).resolves.toBeUndefined();
        expect(fakeSelf.clients.matchAll).toHaveBeenCalledOnce();
    });
});

// ---------------------------------------------------------------------------
// Tests: Fenix cascade bypass
// ---------------------------------------------------------------------------

describe("handleNotificationClick Fenix bypass", () => {
    const FENIX_UA = "Mozilla/5.0 (Android 14; Mobile; rv:136.0) Gecko/136.0 Firefox/136.0";
    const CHROME_ANDROID_UA = "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 Chrome/124.0.0.0 Mobile Safari/537.36";

    // Primary: Fenix UA → matchAll not called for T1; openWindow IS called against redirector URL.
    it("Fenix UA: skips T1 matchAll+focus and calls openWindow with redirector URL", async () => {
        const existingClient = makeClient("/app/graf/c/1", { focused: true });
        const newClient = makeClient("/app/graf/c/9");
        const fakeSelf = makeSelf([existingClient], async () => newClient, FENIX_UA);

        await handleNotificationClick(fakeSelf, { notification: { data: buildData("/r/abc?to=/app/graf/c/9", 1) } });

        // openWindow was called with the redirector URL.
        expect(fakeSelf.clients.openWindow).toHaveBeenCalledWith(`${ORIGIN}/r/abc?to=/app/graf/c/9`);
        // matchAll was NOT called — the Fenix gate fires before clients.matchAll.
        expect(fakeSelf.clients.matchAll).not.toHaveBeenCalled();
        // The existing client was NOT focused (T1 skipped entirely).
        expect(existingClient.focus).not.toHaveBeenCalled();
        // No NavigateTo posted to the existing client.
        expect(existingClient.postMessage).not.toHaveBeenCalledWith(
            expect.objectContaining({ type: "NavigateTo" }),
        );
    });

    // Regression: non-Fenix UA still runs T1 (existing-client focus path).
    it("non-Fenix UA: T1 still runs; existing client focused (regression check)", async () => {
        const existingClient = makeClient("/app/graf/c/1", { focused: true });
        const fakeSelf = makeSelf([existingClient], async () => null, CHROME_ANDROID_UA);

        await handleNotificationClick(fakeSelf, { notification: { data: buildData("/r/abc?to=/app/graf/c/9", 1) } });

        // T1 ran: focus was called on the matching client.
        expect(existingClient.focus).toHaveBeenCalled();
        // openWindow was NOT called (T1 succeeded).
        expect(fakeSelf.clients.openWindow).not.toHaveBeenCalled();
    });

    // No UA set (empty string): behaves as non-Fenix (T1 path).
    it("no UA (empty string): T1 still runs; existing client focused", async () => {
        const existingClient = makeClient("/app/graf/c/1", { focused: true });
        // makeSelf without userAgent arg → navigator.userAgent = "".
        const fakeSelf = makeSelf([existingClient]);

        await handleNotificationClick(fakeSelf, { notification: { data: buildData("/r/abc?to=/app/graf/c/9", 1) } });

        expect(existingClient.focus).toHaveBeenCalled();
        expect(fakeSelf.clients.openWindow).not.toHaveBeenCalled();
    });

    // Test C (Gap 3): Fenix UA with openWindow rejecting.
    // Covers the catch block at sw-handler.ts:104-107 (zero coverage before this test).
    it("Fenix UA: openWindow rejects → caught; console.warn fires", async () => {
        const fakeSelf = makeSelf([], async () => { throw new Error("simulated rejection"); }, FENIX_UA);
        const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

        // Must resolve (no unhandled rejection).
        await expect(
            handleNotificationClick(fakeSelf, { notification: { data: buildData("/r/abc?to=/app/graf/c/9", 1) } })
        ).resolves.toBeUndefined();

        // openWindow was called — confirms the Fenix try block was entered.
        expect(fakeSelf.clients.openWindow).toHaveBeenCalled();
        // console.warn fires the Fenix-specific rejection message.
        expect(warnSpy).toHaveBeenCalledWith(
            expect.stringContaining("Fenix: openWindow rejected"),
            expect.anything(),
        );

        warnSpy.mockRestore();
    });

    // Fenix + openWindow returns null → logs no-op, no navigate attempt.
    it("Fenix UA: openWindow returns null → no-op, no crash", async () => {
        const fakeSelf = makeSelf([], async () => null, FENIX_UA);
        const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});

        await handleNotificationClick(fakeSelf, { notification: { data: buildData("/r/abc?to=/app/graf/c/9", 1) } });

        expect(fakeSelf.clients.openWindow).toHaveBeenCalledWith(`${ORIGIN}/r/abc?to=/app/graf/c/9`);
        expect(logSpy).toHaveBeenCalledWith(
            expect.stringContaining("[brenn-sw] Fenix: openWindow returned null"),
            expect.anything(),
        );
        logSpy.mockRestore();
    });
});
