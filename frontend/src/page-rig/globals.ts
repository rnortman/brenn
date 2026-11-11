// The stand-in browser the page rig runs the production kernel bundle and
// `surface.ts` in: exactly the globals the two of them touch, hand-written, in
// vitest's node environment.
//
// Node's own `CustomEvent`, `Event`, `crypto`, `performance`, `queueMicrotask`,
// `Response` and WebAssembly streaming functions are used as they are. Everything
// else the page reaches for is defined here: `Window` (with `globalThis`
// re-parented onto its prototype, which is what `web_sys::window()` checks),
// `document` and its elements, `location`, `sessionStorage`, the viewport facts,
// the input event classes, `fetch`, `WebSocket` and the four timer functions.
//
// Time is the rig's. A zero-delay timeout runs on a real macrotask, in arming
// order; every other timeout and every interval is recorded and never fires. So
// the kernel's connect deadline, liveness deadline and status interval cannot
// fire, the worker is not held open, and no case passes or fails on elapsed
// time. `Date.now`, `performance.now` and the random sources stay real.

import { existsSync, readFileSync } from "node:fs";
import { relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

/** The original `setImmediate`, captured before anything can replace it. */
const realSetImmediate = globalThis.setImmediate;
const realClearImmediate = globalThis.clearImmediate;

/** Yield one real macrotask. */
export function tick(): Promise<void> {
    return new Promise((done) => {
        realSetImmediate(done);
    });
}

/**
 * The rig's failure record. The first failure is the one reported: a later one
 * is usually a consequence of it.
 */
export class Failures {
    private first: Error | null = null;

    /** Record `message` as a failure and return the error to throw. */
    record(message: string): Error {
        const err = new Error(`page rig: ${message}`);
        if (this.first === null) {
            this.first = err;
        }
        return err;
    }

    /** Record and throw. */
    fatal(message: string): never {
        throw this.record(message);
    }

    /** Throw the first recorded failure, if any. */
    rethrow(): void {
        if (this.first !== null) {
            throw this.first;
        }
    }

    get failed(): boolean {
        return this.first !== null;
    }
}

/** A recorded timer that never fires. */
export interface ParkedTimer {
    handle: number;
    delay: number;
    repeating: boolean;
}

/** How many ticks `settle()` waits for zero-delay timers before naming a livelock. */
const SETTLE_TICK_CAP = 10_000;

type TimerCallback = (...args: unknown[]) => void;

/**
 * The stand-in timer functions. Handles are numbers, shared between zero-delay
 * and parked timers.
 */
export class RigTimers {
    private nextHandle = 1;
    private readonly due = new Map<number, ReturnType<typeof setImmediate>>();
    private readonly parkedTimers = new Map<number, ParkedTimer>();

    constructor(private readonly failures: Failures) {}

    /** Run `fn` on the next zero-delay slot, counted as outstanding until it runs. */
    soon(fn: () => void): number {
        const handle = this.nextHandle++;
        this.due.set(
            handle,
            realSetImmediate(() => {
                this.due.delete(handle);
                try {
                    fn();
                } catch (err) {
                    // A browser reports a throwing timer callback and carries on;
                    // here it is a failure the next settle rethrows.
                    this.failures.record(`a timer callback threw: ${describe(err)}`);
                }
            }),
        );
        return handle;
    }

    setTimeout(fn: TimerCallback, delay?: number, ...args: unknown[]): number {
        if (typeof fn !== "function") {
            this.failures.fatal("setTimeout was handed a string to evaluate");
        }
        if (delay === undefined || !(delay > 0)) {
            return this.soon(() => fn(...args));
        }
        return this.park(delay, false);
    }

    setInterval(fn: TimerCallback, delay?: number): number {
        if (typeof fn !== "function") {
            this.failures.fatal("setInterval was handed a string to evaluate");
        }
        return this.park(delay ?? 0, true);
    }

    clear(handle: number | undefined): void {
        if (handle === undefined) {
            return;
        }
        const immediate = this.due.get(handle);
        if (immediate !== undefined) {
            realClearImmediate(immediate);
            this.due.delete(handle);
        }
        this.parkedTimers.delete(handle);
    }

    /** Every timer recorded and not cleared, for diagnosing a page that never settles. */
    parked(): ParkedTimer[] {
        return [...this.parkedTimers.values()];
    }

    /** How many zero-delay timers are armed and have not run. */
    get outstanding(): number {
        return this.due.size;
    }

    /**
     * Yield real macrotasks until no zero-delay timer is outstanding, then
     * rethrow the first recorded failure if there is one.
     */
    async settle(): Promise<void> {
        for (let ticks = 0; ; ticks++) {
            this.failures.rethrow();
            if (this.due.size === 0) {
                // One more tick lets a microtask chain that arms nothing finish.
                await tick();
                if (this.due.size === 0) {
                    this.failures.rethrow();
                    return;
                }
            }
            if (ticks >= SETTLE_TICK_CAP) {
                this.failures.fatal(
                    `the page did not settle after ${SETTLE_TICK_CAP} ticks: a ` +
                        `livelock keeps ${this.due.size} zero-delay timer(s) armed`,
                );
            }
            await tick();
        }
    }

    private park(delay: number, repeating: boolean): number {
        const handle = this.nextHandle++;
        this.parkedTimers.set(handle, { handle, delay, repeating });
        return handle;
    }
}

/** Best-effort text of a thrown value, for failure messages. */
export function describe(err: unknown): string {
    if (err instanceof Error) {
        return err.stack ?? `${err.name}: ${err.message}`;
    }
    if (typeof err === "string") {
        return err;
    }
    try {
        return JSON.stringify(err) ?? String(err);
    } catch {
        return String(err);
    }
}

type Listener = (ev: Event) => void;

/** `addEventListener` / `dispatchEvent` for the stand-in `Window`, `document` and elements. */
class Target {
    private readonly listeners = new Map<string, Listener[]>();

    addEventListener(type: string, listener: Listener): void {
        const list = this.listeners.get(type) ?? [];
        list.push(listener);
        this.listeners.set(type, list);
    }

    removeEventListener(type: string, listener: Listener): void {
        const list = this.listeners.get(type);
        if (list !== undefined) {
            this.listeners.set(
                type,
                list.filter((l) => l !== listener),
            );
        }
    }

    dispatchEvent(ev: Event): boolean {
        for (const listener of [...(this.listeners.get(ev.type) ?? [])]) {
            try {
                listener(ev);
            } catch (err) {
                // A browser reports a throwing listener and goes on dispatching.
                rigFailures?.record(
                    `a '${ev.type}' listener threw: ${describe(err)}`,
                );
            }
        }
        return true;
    }
}

/** The failure record the stand-in event targets report a throwing listener to. */
let rigFailures: Failures | null = null;

/** A stand-in element: attributes, children and the handful of members the kernel reads. */
export class RigElement extends Target {
    readonly tagName: string;
    readonly attributes = new Map<string, string>();
    readonly children: RigElement[] = [];
    parentElement: RigElement | null = null;
    textContent = "";
    hidden = false;
    readonly style = {
        setProperty(): void {},
        removeProperty(): void {},
    };

    constructor(tag: string) {
        super();
        this.tagName = tag.toUpperCase();
    }

    get id(): string {
        return this.attributes.get("id") ?? "";
    }

    set id(value: string) {
        this.attributes.set("id", value);
    }

    getAttribute(name: string): string | null {
        return this.attributes.get(name) ?? null;
    }

    setAttribute(name: string, value: string): void {
        this.attributes.set(name, String(value));
    }

    removeAttribute(name: string): void {
        this.attributes.delete(name);
    }

    appendChild(child: RigElement): RigElement {
        child.remove();
        child.parentElement = this;
        this.children.push(child);
        return child;
    }

    insertBefore(child: RigElement, reference: RigElement | null): RigElement {
        child.remove();
        child.parentElement = this;
        const at = reference === null ? -1 : this.children.indexOf(reference);
        if (at < 0) {
            this.children.push(child);
        } else {
            this.children.splice(at, 0, child);
        }
        return child;
    }

    remove(): void {
        const parent = this.parentElement;
        if (parent !== null) {
            parent.children.splice(parent.children.indexOf(this), 1);
            this.parentElement = null;
        }
    }

    get firstElementChild(): RigElement | null {
        return this.children[0] ?? null;
    }

    contains(other: RigElement): boolean {
        return other === this || this.children.some((c) => c.contains(other));
    }

    getBoundingClientRect(): { left: number; top: number; width: number; height: number } {
        return { left: 0, top: 0, width: 0, height: 0 };
    }

    *walk(): Generator<RigElement> {
        yield this;
        for (const child of this.children) {
            yield* child.walk();
        }
    }
}

/** The stand-in `HTMLElement`; every element the stand-in document makes is one. */
export class RigHTMLElement extends RigElement {}

/** The stand-in `Window`, onto whose prototype `globalThis` is re-parented. */
class RigWindow extends Target {}

/** The stand-in `document`. */
export class RigDocument extends Target {
    readonly head = new RigHTMLElement("head");
    readonly body = new RigHTMLElement("body");

    createElement(tag: string): RigHTMLElement {
        return new RigHTMLElement(tag);
    }

    getElementById(id: string): RigElement | null {
        for (const root of [this.head, this.body]) {
            for (const el of root.walk()) {
                if (el.id === id) {
                    return el;
                }
            }
        }
        return null;
    }

    querySelector(selector: string): RigElement | null {
        const meta = /^meta\[name="([^"]+)"\]$/.exec(selector);
        if (meta !== null) {
            for (const el of this.head.walk()) {
                if (el.tagName === "META" && el.getAttribute("name") === meta[1]) {
                    return el;
                }
            }
            return null;
        }
        const id = /^#(.+)$/.exec(selector);
        if (id?.[1] !== undefined) {
            return this.getElementById(id[1]);
        }
        throw new Error(`the stand-in document does not support the selector ${selector}`);
    }
}

/** What a page states about itself: its three metas and its location. */
export interface PageFacts {
    slug: string;
    buildId: string;
    configChannel: string;
    location: { protocol: string; host: string };
}

/** What the installed globals hand back to the rig. */
export interface InstalledGlobals {
    document: RigDocument;
    timers: RigTimers;
}

/**
 * Map a URL the page fetches to a file under the staged tree: a
 * `/surface-static/<path>?v=<build>` URL (a kind's core) or a `file:` URL (the
 * kernel's own wasm, which its glue locates beside itself).
 */
function stagedPath(href: string, staticRoot: string, buildId: string): string | null {
    let path: string;
    if (href.startsWith("file:")) {
        path = fileURLToPath(href);
    } else if (href.startsWith("/surface-static/")) {
        const [rest, query] = href.slice("/surface-static/".length).split("?");
        if (rest === undefined || query !== `v=${buildId}`) {
            return null;
        }
        path = resolve(staticRoot, rest);
    } else {
        return null;
    }
    const inside = relative(staticRoot, path);
    if (inside === "" || inside.startsWith("..") || inside.startsWith(sep)) {
        return null;
    }
    return existsSync(path) ? path : null;
}

/**
 * Install the stand-in browser for one page. Called before `surface.ts` is
 * imported: the module reads `document` at evaluation.
 *
 * `webSocket` is the stand-in socket class the scripted peer owns.
 */
export function installGlobals(
    page: PageFacts,
    staticRoot: string,
    failures: Failures,
    webSocket: unknown,
): InstalledGlobals {
    // TODO(surface-byte-crossing-in-browser): the page's body crossing is gated
    // only here, on node's WebAssembly engine with these stand-ins; no test runs
    // it in a real browser.
    if (rigFailures !== null) {
        failures.fatal("the stand-in globals are already installed in this worker");
    }
    rigFailures = failures;
    const g = globalThis as unknown as Record<string, unknown>;

    Object.setPrototypeOf(globalThis, RigWindow.prototype);
    // The re-parented global has no `Target` fields of its own; give it the map
    // the constructor would have.
    Object.defineProperty(globalThis, "listeners", {
        value: new Map<string, Listener[]>(),
        enumerable: false,
    });

    const document = new RigDocument();
    for (const [name, content] of [
        ["surface-slug", page.slug],
        ["brenn-build-id", page.buildId],
        ["surface-config-channel", page.configChannel],
    ] as const) {
        const meta = new RigHTMLElement("meta");
        meta.setAttribute("name", name);
        meta.setAttribute("content", content);
        document.head.appendChild(meta);
    }
    const root = new RigHTMLElement("div");
    root.id = "surface-root";
    document.body.appendChild(root);

    const storage = new Map<string, string>();
    const timers = new RigTimers(failures);

    Object.assign(g, {
        Window: RigWindow,
        window: globalThis,
        document,
        Element: RigElement,
        HTMLElement: RigHTMLElement,
        location: {
            protocol: page.location.protocol,
            host: page.location.host,
            reload: (): never => failures.fatal("the page asked to reload"),
        },
        sessionStorage: {
            getItem: (key: string): string | null => storage.get(key) ?? null,
            setItem: (key: string, value: string): void => {
                storage.set(key, String(value));
            },
            removeItem: (key: string): void => {
                storage.delete(key);
            },
        },
        innerWidth: 1024,
        innerHeight: 768,
        devicePixelRatio: 1,
        matchMedia: (media: string) => ({ matches: false, media }),
        KeyboardEvent: class KeyboardEvent {},
        MouseEvent: class MouseEvent {},
        PointerEvent: class PointerEvent {},
        WebSocket: webSocket,
        fetch: (input: unknown): Promise<Response> => {
            const href =
                input instanceof URL
                    ? input.href
                    : typeof input === "string"
                      ? input
                      : input instanceof Request
                        ? input.url
                        : String(input);
            const path = stagedPath(href, staticRoot, page.buildId);
            if (path === null) {
                return Promise.reject(failures.record(`the page fetched an unmapped URL ${href}`));
            }
            return Promise.resolve(
                new Response(readFileSync(path), {
                    headers: { "content-type": "application/wasm" },
                }),
            );
        },
        setTimeout: (fn: TimerCallback, delay?: number, ...args: unknown[]) =>
            timers.setTimeout(fn, delay, ...args),
        clearTimeout: (handle?: number) => timers.clear(handle),
        setInterval: (fn: TimerCallback, delay?: number) => timers.setInterval(fn, delay),
        clearInterval: (handle?: number) => timers.clear(handle),
    });

    // Either seam event means the page gave up on itself; neither is ever a
    // state a case means to reach.
    for (const event of ["brenn-surface-reload", "brenn-surface-failure"]) {
        (globalThis as unknown as Target).addEventListener(event, (ev: Event) => {
            failures.record(
                `the kernel dispatched '${event}': ${describe((ev as CustomEvent).detail)}`,
            );
        });
    }

    return { document, timers };
}
