// @vitest-environment happy-dom
//
// Pins the stale-preference repair in BrennApp.resolveCurrentModel: a persisted
// preferredModel the server does not offer is dropped from localStorage, not
// merely overridden for the session.

import { afterEach, beforeEach, describe, expect, it } from "vitest";
import "./app.js";
import { BrennApp } from "./app.js";
import { MockWebSocket } from "../test-utils/mock-websocket.js";
import type { ModelInfo } from "../generated/ModelInfo.js";

const SETTINGS_KEY = "brenn-settings";

const OPUS: ModelInfo = {
    value: "opus[1m]",
    display_name: "Opus 5 1M",
    description: "big",
};
const SONNET: ModelInfo = {
    value: "sonnet",
    display_name: "Sonnet",
    description: "fast",
};

const realWebSocket = globalThis.WebSocket;

function storedPreference(): string | null | undefined {
    const raw = localStorage.getItem(SETTINGS_KEY);
    if (raw === null) return undefined;
    return (JSON.parse(raw) as { preferredModel?: string | null }).preferredModel;
}

async function mountWithPreference(
    pref: string | null,
    defaultModel: string,
): Promise<{ app: BrennApp; ws: MockWebSocket }> {
    localStorage.setItem(
        SETTINGS_KEY,
        JSON.stringify({ enterSends: true, paneSplitRatio: 0.5, preferredModel: pref }),
    );
    const app = document.createElement("brenn-app") as BrennApp;
    document.body.appendChild(app);
    await app.updateComplete;
    (app as unknown as { defaultModel: string }).defaultModel = defaultModel;
    const ws = MockWebSocket.instances[0]!;
    return { app, ws };
}

function currentModel(app: BrennApp): string {
    return (app as unknown as { currentModel: string }).currentModel;
}

beforeEach(() => {
    MockWebSocket.instances = [];
    (globalThis as unknown as { WebSocket: unknown }).WebSocket =
        MockWebSocket as unknown;
    localStorage.clear();
    const slugMeta = document.createElement("meta");
    slugMeta.setAttribute("name", "app-slug");
    slugMeta.setAttribute("content", "test");
    document.head.appendChild(slugMeta);
});

afterEach(() => {
    (globalThis as unknown as { WebSocket: unknown }).WebSocket = realWebSocket;
    document.body.replaceChildren();
    document.head.querySelectorAll('meta[name="app-slug"]').forEach((el) => el.remove());
    localStorage.clear();
});

describe("BrennApp — stale model preference repair", () => {
    it("clears a preference the incoming list excludes and falls back to the default", async () => {
        const { app, ws } = await mountWithPreference("sonnet", "opus[1m]");

        ws.deliver({ type: "ModelsAvailable", available_models: [OPUS] });
        await app.updateComplete;

        expect(currentModel(app)).toBe("opus[1m]");
        expect(storedPreference()).toBeNull();
    });

    it("leaves the preference untouched when the incoming list is empty", async () => {
        const { app, ws } = await mountWithPreference("sonnet", "opus[1m]");

        ws.deliver({ type: "ModelsAvailable", available_models: [] });
        await app.updateComplete;

        // Empty means "not yet known" — the session falls back, the stored
        // preference survives for the next list that does say something.
        expect(currentModel(app)).toBe("opus[1m]");
        expect(storedPreference()).toBe("sonnet");
    });

    it("keeps a preference the incoming list offers", async () => {
        const { app, ws } = await mountWithPreference("sonnet", "opus[1m]");

        ws.deliver({ type: "ModelsAvailable", available_models: [OPUS, SONNET] });
        await app.updateComplete;

        expect(currentModel(app)).toBe("sonnet");
        expect(storedPreference()).toBe("sonnet");
    });

    it("repairs on the Welcome frame too, which is the first list a client sees", async () => {
        const { app, ws } = await mountWithPreference("sonnet", "opus[1m]");

        ws.deliver({
            type: "Welcome",
            username: "alice",
            user_id: 0,
            multiuser: false,
            singleton: true,
            available_models: [OPUS],
            default_model: "opus[1m]",
            attachment_targets: [],
            pwa_push_enabled: false,
        });
        await app.updateComplete;

        expect(currentModel(app)).toBe("opus[1m]");
        expect(storedPreference()).toBeNull();
    });

    it("does nothing when no preference is set", async () => {
        const { app, ws } = await mountWithPreference(null, "opus[1m]");

        ws.deliver({ type: "ModelsAvailable", available_models: [OPUS] });
        await app.updateComplete;

        expect(currentModel(app)).toBe("opus[1m]");
        expect(storedPreference()).toBeNull();
    });
});
