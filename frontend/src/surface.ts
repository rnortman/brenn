/**
 * Surface bootstrap — the permanent TypeScript floor for a Brenn surface page.
 *
 * The browser is a rendering layer; this file never parses a protocol frame and
 * imports no generated types. It loads and starts the wasm kernel, wiring the
 * kernel-to-bootstrap seam. Its build id and asset URLs come from the served
 * page (meta tags + the manifest JSON), not a build-time define.
 *
 * The `bootstrap()` sequence, in order: install the global error handlers, read
 * the page inputs, load the wasm modules, install the two kernel-to-bootstrap
 * seam listeners on `window`, then call the kernel's `start()` and record its
 * handle. It runs automatically on a real surface page (guarded on the manifest
 * `<script>`) and is exported so tests can drive it with an injected importer.
 */

/** DOM id of the kernel's root, mirrored from the backend-rendered page. */
const SURFACE_ROOT_ID = "surface-root";

/** DOM id of the manifest `<script>`, mirrored from the backend-rendered page. */
const MANIFEST_SCRIPT_ID = "brenn-surface-manifest";

/**
 * The slice of the wasm `KernelHandle` the bootstrap's error handlers need: the
 * post-kernel path to funnel an uncaught error over the surface connection as an
 * error-level `Log` frame. The bootstrap never parses a protocol frame, so this
 * is the only kernel surface it types.
 */
export interface KernelErrorSink {
    log_error(message: string, source: string): void;
}

/**
 * The live kernel handle once `start()` has returned one, else null. The global
 * error handlers branch on this: null ⇒ pre-kernel (render a static failure,
 * since no WS exists to report over), set ⇒ post-kernel (forward to
 * `log_error`). `bootstrap()` installs the handle via `setKernelHandle` once
 * `start()` returns.
 */
let kernelHandle: KernelErrorSink | null = null;

/**
 * Whether a terminal static-failure message is already on screen — set at the
 * reload cap and when the reload counter cannot be persisted. Once set, the
 * pre-kernel global error handler leaves that message in place rather than
 * overwriting it with a raw error dump: a synchronous panic out of `start()`
 * unwinds into the `unhandledrejection` handler immediately after the cap
 * message renders, and that actionable message is the only guidance an
 * unattended kiosk operator sees.
 */
let terminalFailureRendered = false;

/**
 * Record the kernel handle once `start()` returns it, switching the global error
 * handlers from the pre-kernel static-failure path to the post-kernel
 * `log_error` forwarding path. Passing null restores the clean pre-kernel
 * state (no handle, no terminal failure latched); production only ever passes a
 * real handle. Exported for the `start()` wiring and tests.
 */
export function setKernelHandle(handle: KernelErrorSink | null): void {
    kernelHandle = handle;
    if (handle === null) {
        terminalFailureRendered = false;
    }
}

/**
 * sessionStorage key guarding against reload loops. Per-tab scope survives
 * `location.reload()` and clears on tab close — the right granularity for the
 * 3-strike check. Exported so tests import the key rather than copy the literal.
 */
export const SURFACE_RELOAD_COUNT_KEY = "brenn.surface-reload-count";

/**
 * sessionStorage key for the *chrome-death* reload counter — kept separate from
 * `SURFACE_RELOAD_COUNT_KEY` and, unlike it, never cleared on
 * `brenn-surface-ready`. A chrome that dies deterministically *after* the page
 * reaches ready would otherwise loop forever: each reload reconnects, fires
 * `brenn-surface-ready`, resets the shared counter, then dies again — the shared
 * cap never accumulates. Counting chrome-death reloads on their own key that
 * `resetReloadCount` leaves alone makes that loop converge to the static
 * failure. Exported so tests import the key rather than copy the literal.
 */
export const SURFACE_CHROME_DEATH_COUNT_KEY =
    "brenn.surface-chrome-death-count";

/**
 * The reload reason the kernel sends when the singleton chrome instance dies
 * after mount (`InstanceFailed` for `chrome_instance`). Kept in step with the
 * kernel's `RequestReload { reason }` literal; a mismatch only means a
 * chrome-death reload counts on the shared key instead, still bounded, just via
 * the ready-resettable counter.
 */
const CHROME_DEATH_RELOAD_REASON = "chrome died";

/**
 * How many auto-reloads we tolerate in a single tab-session before giving up
 * and surfacing a static failure message. Every auto-reload trigger (stale
 * build, kernel panic, changed bindings, kernel load failure) funnels through
 * `cappedReload`, so a genuinely broken deploy converges to the static message
 * in at most this many reloads, while a long-lived kiosk that resets on each
 * successful connect never wedges.
 */
const MAX_SURFACE_RELOADS = 3;

/**
 * The sessionStorage key a reload with `reason` counts against: chrome-death
 * reloads use their own ready-immune counter (see
 * `SURFACE_CHROME_DEATH_COUNT_KEY`); everything else shares the main counter.
 */
function reloadCountKey(reason: string): string {
    return reason === CHROME_DEATH_RELOAD_REASON
        ? SURFACE_CHROME_DEATH_COUNT_KEY
        : SURFACE_RELOAD_COUNT_KEY;
}

/**
 * Read the current reload count for `key` from sessionStorage. Anything that
 * fails to parse as a non-negative integer — including storage being
 * unavailable (privacy modes) — is treated as zero: we'd rather reload once
 * spuriously than wedge the guard on a corrupt or absent counter.
 */
function readReloadCount(key: string): number {
    let raw: string | null;
    try {
        raw = sessionStorage.getItem(key);
    } catch {
        return 0;
    }
    if (raw === null) {
        return 0;
    }
    const n = Number.parseInt(raw, 10);
    return Number.isFinite(n) && n >= 0 ? n : 0;
}

/**
 * Render a last-resort static failure message into `#surface-root` as
 * `textContent` (never markup — publisher/error text is untrusted) and log it.
 * Used at the reload cap and for the unrecoverable pre-kernel error paths, where
 * no WS exists to report over.
 */
export function renderStaticFailure(message: string): void {
    console.error(`surface bootstrap failure: ${message}`);
    const root = document.getElementById(SURFACE_ROOT_ID);
    if (root !== null) {
        root.textContent = message;
    }
}

/**
 * The one shared auto-reload trigger. Increments the per-tab counter and calls
 * `location.reload()`; at the cap it renders the static failure message instead
 * of reloading, because a further reload of an already-broken deploy would just
 * loop.
 *
 * The increment is confirmed by read-back. If it cannot be persisted — storage
 * disabled by policy, or a zero-quota mode where every `setItem` is a no-op or
 * throws — the counter can never advance and an unconditional reload would loop
 * unbounded, defeating the cap. That case converges to the static failure
 * instead, keeping the "broken deploy ends in a bounded static message"
 * guarantee even when the counter has no backing store.
 *
 * Exported for the seam wiring and the unit tests.
 */
export function cappedReload(reason: string): void {
    const key = reloadCountKey(reason);
    const count = readReloadCount(key);
    if (count >= MAX_SURFACE_RELOADS) {
        terminalFailureRendered = true;
        renderStaticFailure(
            "This surface couldn't start after several attempts. " +
                "Please close and reopen the tab.",
        );
        console.error(
            `surface reload cap reached (${count} reloads); reason: ${reason}`,
        );
        return;
    }
    const next = count + 1;
    let persisted = false;
    try {
        sessionStorage.setItem(key, String(next));
        persisted = readReloadCount(key) === next;
    } catch {
        persisted = false;
    }
    if (!persisted) {
        terminalFailureRendered = true;
        renderStaticFailure(
            "This surface couldn't start, and this browser can't track " +
                "reload attempts. Please close and reopen the tab.",
        );
        console.error(
            `surface reload counter not persistable; reason: ${reason}`,
        );
        return;
    }
    console.info(
        `surface reload (reason=${reason}); ` +
            `attempt ${next}/${MAX_SURFACE_RELOADS}`,
    );
    location.reload();
}

/**
 * Reset the shared reload counter, called on the first successful connect
 * (`brenn-surface-ready`) so a kiosk surviving many deploys over its lifetime
 * never exhausts the cap. Tolerant of storage being unavailable.
 *
 * Deliberately leaves `SURFACE_CHROME_DEATH_COUNT_KEY` alone: a chrome that
 * dies after ready reaches this reset on every reload, so clearing that counter
 * here would defeat its cap and loop forever.
 *
 * Exported for the seam wiring and the unit tests.
 */
export function resetReloadCount(): void {
    try {
        sessionStorage.removeItem(SURFACE_RELOAD_COUNT_KEY);
    } catch {
        // Storage unavailable — nothing to reset.
    }
}

/**
 * Best-effort human-readable description of an uncaught value. A separate
 * implementation from the legacy `error-reporter.ts` (frozen with the legacy
 * protocol); it needs no shared code.
 */
function describeError(reason: unknown): string {
    if (reason instanceof Error) {
        return reason.stack ?? `${reason.name}: ${reason.message}`;
    }
    if (typeof reason === "string") {
        return reason;
    }
    try {
        return JSON.stringify(reason) ?? String(reason);
    } catch {
        return String(reason);
    }
}

/**
 * Route an uncaught error/rejection. Post-kernel (handle set), the kernel owns
 * the surface DOM and the WS connection, so forward for an error-level `Log`
 * frame and leave the surface running. Pre-kernel (no handle), no WS exists to report
 * over, so this is the last-resort static failure. Both branches are guarded so
 * the handler never throws.
 */
function handleGlobalError(message: string, source: string): void {
    const handle = kernelHandle;
    if (handle !== null) {
        // `log_error` is best-effort and silently dropped when the WS is
        // down or backpressured, so always leave a browser-console trace — the
        // WS outage is exactly when the server-side record is missing.
        console.error(`surface uncaught (post-kernel): ${message} [${source}]`);
        try {
            handle.log_error(message, source);
        } catch (err) {
            // Never propagate out of a listener, but do record the failure:
            // otherwise a dead-kernel report attempt leaves no breadcrumb at all.
            console.error(`surface log_error failed: ${describeError(err)}`);
        }
        return;
    }
    if (terminalFailureRendered) {
        // A terminal static-failure message is already on screen; leave it and
        // just trace, rather than overwriting the actionable message.
        console.error(
            `surface uncaught (pre-kernel, terminal): ${message} [${source}]`,
        );
        return;
    }
    try {
        renderStaticFailure(message);
    } catch {
        // A render failure must never propagate out of a listener.
    }
}

/**
 * Install the two global handlers (`error` + `unhandledrejection`). Called first
 * in the bootstrap sequence so an error anywhere in the rest of boot is caught.
 * Each listener body is fully guarded: a listener must never raise.
 *
 * Exported for the bootstrap sequence and the unit tests.
 */
export function installGlobalHandlers(): void {
    window.addEventListener(
        "unhandledrejection",
        (ev: PromiseRejectionEvent) => {
            try {
                handleGlobalError(
                    "Unhandled promise rejection: " + describeError(ev.reason),
                    "window.unhandledrejection",
                );
            } catch {
                // Swallow — must never raise from a listener.
            }
        },
    );
    window.addEventListener("error", (ev: ErrorEvent) => {
        try {
            const v: unknown =
                ev.error !== null && ev.error !== undefined
                    ? (ev.error as unknown)
                    : `${ev.message} at ${ev.filename}:${ev.lineno}:${ev.colno}`;
            handleGlobalError(
                "Uncaught error: " + describeError(v),
                "window.error",
            );
        } catch {
            // Swallow — must never raise from a listener.
        }
    });
}

/**
 * A component-module entry in the surface manifest: one declared **instance**,
 * its config `kind`, and the build-ID-stamped module URL. Shape mirrored from
 * `page.rs`'s `ManifestComponent` — backend-produced, bootstrap-consumed, never
 * a WS frame and never touched by ts-rs.
 *
 * One entry per instance, not per kind: sibling instances of one kind carry
 * distinct `module` URLs (they differ in an `instance=` query) precisely so the
 * browser evaluates each as its own module record with its own linear memory.
 */
export interface ManifestComponent {
    instance: string;
    kind: string;
    module: string;
    abi: string;
}

/** The `abi` value of a headless component-model instance (`page.rs`'s `Abi`). */
const ABI_PROCESSOR = "processor";

/**
 * The component-module manifest embedded in the page. Shape mirrored from
 * `page.rs`'s `SurfaceManifest`: `kernel` is the kernel module URL, `components`
 * lists each configured component's module URL.
 */
export interface SurfaceManifest {
    kernel: string;
    components: ManifestComponent[];
}

/** Structural guard: the parsed JSON matches the frozen manifest shape. */
function isSurfaceManifest(value: unknown): value is SurfaceManifest {
    if (typeof value !== "object" || value === null) {
        return false;
    }
    const obj = value as Record<string, unknown>;
    if (typeof obj.kernel !== "string" || !Array.isArray(obj.components)) {
        return false;
    }
    return obj.components.every((c): c is ManifestComponent => {
        if (typeof c !== "object" || c === null) {
            return false;
        }
        const entry = c as Record<string, unknown>;
        return (
            typeof entry.instance === "string" &&
            typeof entry.kind === "string" &&
            typeof entry.module === "string" &&
            typeof entry.abi === "string"
        );
    });
}

/** Parse the manifest `<script>`, or null if it is absent or not well-formed. */
function readManifest(): SurfaceManifest | null {
    const script = document.getElementById(MANIFEST_SCRIPT_ID);
    if (script === null || script.textContent === null) {
        return null;
    }
    let parsed: unknown;
    try {
        parsed = JSON.parse(script.textContent);
    } catch {
        return null;
    }
    return isSurfaceManifest(parsed) ? parsed : null;
}

/**
 * Read and validate the manifest the bootstrap needs before loading modules. A
 * served page always carries a well-formed manifest, so a missing or malformed
 * one means a broken deploy — render the last-resort static failure (no WS
 * exists to report over) and return null so the bootstrap aborts rather than
 * loading modules with no wiring.
 *
 * The `surface-slug` / `brenn-build-id` metas are on the page too, but they are
 * the kernel's inputs (it re-reads them in `start()`), not the bootstrap's.
 *
 * Exported for the bootstrap sequence and the unit tests.
 */
export function readSurfaceManifest(): SurfaceManifest | null {
    const manifest = readManifest();
    if (manifest === null) {
        renderStaticFailure(
            "This surface page is missing its component manifest. " +
                "This usually means a broken or incomplete deploy.",
        );
        return null;
    }
    return manifest;
}

/**
 * A wasm-bindgen `--target web` module: its default export is the async init
 * function that fetches, compiles, and instantiates the module's wasm. The
 * bootstrap never parses a frame, so it types only what it calls on a module.
 */
interface WasmModule {
    default(): Promise<unknown>;
}

/**
 * A component module: wasm-bindgen's `default` init plus the contract's
 * instance-bind export (`brenn_surface_contract::BIND_INSTANCE_EXPORT`). The
 * loader calls the bind immediately after init, handing the module the instance
 * id from the manifest entry it was loaded for.
 *
 * This is the identity channel because it is the only one available: Rust in a
 * `--target web` module cannot read the glue module's `import.meta.url`, so the
 * `instance=` query that forced the distinct module record is invisible in-module.
 * Moving one string from the manifest into the module it just loaded is loading —
 * the TS layer gains no message logic from it.
 */
interface ComponentModule extends WasmModule {
    brenn_bind_instance(instance: string): void;
}

/**
 * The kernel module additionally exports `start()`, which brings the kernel online
 * (owns the WS connection, mounts components) and returns the handle the
 * post-kernel error path forwards over. Returned by `loadModules` so the
 * bootstrap sequence can call `start()` after installing the seam listeners.
 */
export interface KernelModule extends WasmModule {
    start(): KernelErrorSink;
    /**
     * The processor host seam (`surface/kernel`'s `entry.rs`): the free functions
     * a headless instance's import shims delegate to. Every one takes the
     * instance id from the loader's own closure over the manifest entry — the
     * component never names itself.
     */
    brenn_processor_publish(
        instance: string,
        port: string,
        body: string,
        urgency: string | undefined,
    ): string;
    brenn_processor_log(instance: string, level: string, message: string): void;
    brenn_processor_alert(
        instance: string,
        severity: string,
        title: string,
        body: string,
    ): void;
    brenn_processor_config_get(
        instance: string,
        key: string,
    ): string | undefined;
    brenn_processor_register(
        instance: string,
        entry: (activation: string) => void,
    ): boolean;
    brenn_processor_load_failed(instance: string, detail: string): void;
}

/**
 * A `jco transpile --instantiation` module: instantiation-free at evaluation, so
 * one evaluation per *kind* is correct and sibling instances share the compiled
 * core modules through it. `getCoreModule` resolves the core wasm filenames the
 * glue asks for; `imports` is keyed by unversioned WIT interface name.
 */
interface ProcessorModule {
    instantiate(
        getCoreModule: (name: string) => Promise<WebAssembly.Module>,
        imports: Record<string, Record<string, unknown>>,
    ): Promise<ProcessorInstance>;
}

/** What a transpiled `world processor` exports: the activation entry point. */
interface ProcessorInstance {
    receive(activation: WitActivation): void;
}

/**
 * The `activation` record as the transpiled glue destructures it. The kernel
 * hands the shim its own JSON (snake-cased serde), so the only work at the
 * boundary is the field renaming the canonical ABI's lifting expects.
 */
interface WitActivation {
    ports: {
        port: string;
        envelopes: string[];
        newFrom: number;
        dropped: number;
    }[];
}

/** The same record as the kernel serializes it. */
interface KernelActivation {
    ports: {
        port: string;
        envelopes: string[];
        new_from: number;
        dropped: number;
    }[];
}

/** How the bootstrap dynamically imports a module URL; overridable in tests. */
export type ModuleImporter = (url: string) => Promise<unknown>;

const defaultImporter: ModuleImporter = (url) => import(/* @vite-ignore */ url);

/**
 * Load and initialize the wasm modules named in the manifest: the kernel first,
 * then one module per declared component instance in parallel (each is bound to
 * its instance and registers that instance's custom element). Returns the initialized kernel module — whose `start()` the bootstrap
 * sequence calls after installing the seam listeners — or null when the kernel
 * itself failed to load.
 *
 * Failure handling differs by module class:
 * - Kernel import/init failure (deploy-window fetch failure, 404, wasm init
 *   throw) routes through `cappedReload` — the transient/deploy-race class the
 *   reload guard exists for — and returns null. Without this a page loading
 *   during a deploy would wedge on a static error page.
 * - Component module load/init/bind failure is logged and does NOT abort the
 *   surface: the kernel error-cards any instance whose element never got defined,
 *   so partial availability beats a dead kiosk. One instance's failure is one
 *   instance's — its siblings have their own module records.
 *
 * Exported for the bootstrap sequence and the unit tests.
 */
export async function loadModules(
    manifest: SurfaceManifest,
    importModule: ModuleImporter = defaultImporter,
): Promise<KernelModule | null> {
    let kernel: KernelModule;
    try {
        kernel = (await importModule(manifest.kernel)) as KernelModule;
        await kernel.default();
    } catch (err) {
        console.error(
            `surface kernel module load failed: ${describeError(err)}`,
        );
        cappedReload("kernel load failed");
        return null;
    }
    await Promise.all(
        manifest.components
            // Processor instances are brought up later, on the kernel's
            // `brenn-processor-start`: their config map and bindings row arrive
            // with `Welcome`, which is after `start()`.
            .filter((component) => component.abi !== ABI_PROCESSOR)
            .map(async (component) => {
                try {
                    const mod = (await importModule(
                        component.module,
                    )) as ComponentModule;
                    await mod.default();
                    // Bind before the element is ever created: the module derives its
                    // instance-scoped tag from this, so it defines nothing until it
                    // knows which instance it is.
                    mod.brenn_bind_instance(component.instance);
                } catch (err) {
                    console.error(
                        `surface component instance '${component.instance}' ` +
                            `(kind '${component.kind}') module load failed: ` +
                            describeError(err),
                    );
                }
            }),
    );
    return kernel;
}

/**
 * Load a transpiled processor kind's module once and memoize it, along with the
 * compiled core `WebAssembly.Module`s it asks for. One compiled module per core
 * file per kind: sibling instances share the code and differ only in the
 * instantiation (and thus the linear memory) built on it.
 *
 * The core files are named relative to the glue, so they resolve against the
 * kind's module URL.
 */
class ProcessorKindCache {
    private readonly modules = new Map<string, Promise<ProcessorModule>>();
    private readonly cores = new Map<string, Promise<WebAssembly.Module>>();

    constructor(private readonly importModule: ModuleImporter) {}

    module(url: string): Promise<ProcessorModule> {
        let mod = this.modules.get(url);
        if (mod === undefined) {
            mod = this.importModule(url) as Promise<ProcessorModule>;
            this.modules.set(url, mod);
        }
        return mod;
    }

    coreLoader(
        moduleUrl: string,
    ): (name: string) => Promise<WebAssembly.Module> {
        return (name) => {
            const url = new URL(name, new URL(moduleUrl, document.baseURI))
                .href;
            let core = this.cores.get(url);
            if (core === undefined) {
                core = WebAssembly.compileStreaming(fetch(url));
                this.cores.set(url, core);
            }
            return core;
        };
    }
}

/**
 * Build one instance's WIT import object. Every entry is a thin arrow closed
 * over `instance`, delegating to the kernel's host seam — the TS layer holds no
 * policy, decides nothing, and cannot name an instance the kernel did not give
 * it.
 *
 * The transpiled glue keys imports by unversioned interface name and pulls only
 * the interfaces the artifact actually imports, so offering all four is safe for
 * any transpilable-profile component. `alert` is offered unconditionally for the
 * same reason: a kind that does not import it never reads the key, and a kind
 * that does had its surface's alert grant proven at boot (and the kernel's own
 * `brenn_processor_alert` re-checks the live grant regardless).
 *
 * A refused publish reaches the guest as the WIT `publish-error` variant: the
 * glue's `getErrorPayload` reads a thrown value's own `payload` property, so the
 * shim throws the variant rather than returning it.
 */
function processorImports(
    kernel: KernelModule,
    instance: string,
): Record<string, Record<string, unknown>> {
    const publish = (port: string, payload: string, urgency?: string): void => {
        const error = kernel.brenn_processor_publish(
            instance,
            port,
            payload,
            urgency,
        );
        if (error !== "") {
            throw { payload: { tag: error } };
        }
    };
    return {
        "brenn:processor/ports": {
            publish: (port: string, payload: string) => publish(port, payload),
            publishWithUrgency: (
                port: string,
                payload: string,
                urgency: string,
            ) => publish(port, payload, urgency),
        },
        "brenn:processor/log": {
            log: (level: string, message: string) =>
                kernel.brenn_processor_log(instance, level, message),
        },
        "brenn:processor/alert": {
            alert: (severity: string, title: string, body: string) =>
                kernel.brenn_processor_alert(instance, severity, title, body),
        },
        "brenn:processor/config": {
            get: (key: string) =>
                kernel.brenn_processor_config_get(instance, key),
        },
    };
}

/**
 * Wrap a transpiled instance's `receive` in the activation entry the kernel
 * registers: it takes the kernel's serialized activation, hands the component
 * the lifted record, and answers in the kernel's own vocabulary — return
 * `undefined` for ok, return an error string for err, throw for trap.
 *
 * The one discrimination rule of this seam lives here. jco lifts the `err` arm
 * of `receive`'s `result<_, receive-error>` into a **throw** (a `ComponentError`
 * carrying the variant on an own `payload` property), so at this boundary err
 * and trap both arrive as exceptions. They are told apart by that property
 * rather than by class, because the generated `ComponentError` is private to the
 * transpiled module and never exported. A component returning err keeps running
 * with its buffer discarded; anything else is a trap and is terminal for the
 * instance.
 */
function activationEntry(
    instance: ProcessorInstance,
): (activation: string) => void {
    return (json: string) => {
        const parsed = JSON.parse(json) as KernelActivation;
        const lifted: WitActivation = {
            ports: parsed.ports.map((window) => ({
                port: window.port,
                envelopes: window.envelopes,
                newFrom: window.new_from,
                dropped: window.dropped,
            })),
        };
        try {
            instance.receive(lifted);
        } catch (err) {
            if (
                typeof err === "object" &&
                err !== null &&
                Object.prototype.hasOwnProperty.call(err, "payload")
            ) {
                // The kernel reads a returned string as the err arm.
                return describeReceiveError(
                    (err as { payload: unknown }).payload,
                );
            }
            throw err;
        }
        return undefined;
    };
}

/**
 * The operator's account of a `receive-error`. The variant lifts to
 * `{ tag, val }`; both arms carry a string, and neither is ever parsed — this is
 * the diagnostic that reaches the failure record for that activation.
 */
function describeReceiveError(payload: unknown): string {
    const variant = payload as { tag?: unknown; val?: unknown } | null;
    const tag =
        typeof variant?.tag === "string" ? variant.tag : "receive failed";
    return typeof variant?.val === "string" ? `${tag}: ${variant.val}` : tag;
}

/**
 * Bring up one headless processor instance: instantiate the kind's transpiled
 * module with this instance's imports, then register its `receive` with the
 * kernel. Its own instantiation means its own linear memory — a trap poisons
 * this instance and no sibling.
 *
 * Every failure — import, instantiate, or a registration the kernel refuses —
 * is reported to the kernel, which marks the instance `failed` and reports the
 * death. There is no error card to render: a headless instance has no wrapper,
 * so the status row is the observable. One instance's failure is one
 * instance's; the surface keeps running.
 */
async function startProcessor(
    kernel: KernelModule,
    cache: ProcessorKindCache,
    component: ManifestComponent,
): Promise<void> {
    try {
        const mod = await cache.module(component.module);
        const instance = await mod.instantiate(
            cache.coreLoader(component.module),
            processorImports(kernel, component.instance),
        );
        if (
            !kernel.brenn_processor_register(
                component.instance,
                activationEntry(instance),
            )
        ) {
            // The kernel already reported why (unknown instance, wrong ABI, or a
            // duplicate registration) and marked nothing; say so and stop here
            // rather than leaving an instantiated-but-undelivered instance
            // looking live in the status table.
            kernel.brenn_processor_load_failed(
                component.instance,
                "activation registration refused",
            );
        }
    } catch (err) {
        const detail = describeError(err);
        console.error(
            `surface processor instance '${component.instance}' ` +
                `(kind '${component.kind}') failed to start: ${detail}`,
        );
        kernel.brenn_processor_load_failed(component.instance, detail);
    }
}

/**
 * Bring up the processor instances the kernel named, in parallel. Instances the
 * manifest does not carry are a broken deploy rather than a component fault —
 * reported to the kernel so the row goes `failed` like any other bring-up
 * failure.
 *
 * Exported for the unit tests.
 */
export async function startProcessors(
    kernel: KernelModule,
    manifest: SurfaceManifest,
    instances: string[],
    importModule: ModuleImporter = defaultImporter,
): Promise<void> {
    const cache = new ProcessorKindCache(importModule);
    await Promise.all(
        instances.map(async (instance) => {
            const component = manifest.components.find(
                (c) => c.instance === instance && c.abi === ABI_PROCESSOR,
            );
            if (component === undefined) {
                kernel.brenn_processor_load_failed(
                    instance,
                    "no processor module in the page manifest",
                );
                return;
            }
            await startProcessor(kernel, cache, component);
        }),
    );
}

/**
 * Read the instance-id list off a `brenn-processor-start` detail.
 *
 * A malformed detail (no `instances` array) starts nothing, leaving every
 * declared processor row `Pending` forever — a page that looks like it is still
 * coming up rather than one that broke. The detail is kernel-authored, so a
 * malformed one is kernel↔bootstrap glue drift, the failure mode a same-repo
 * build skew produces; `reportDrift` (wired to the backend error channel in
 * production) carries it to telemetry rather than leaving it in a devtools
 * console nobody watches. A legitimately empty `instances` array is *not* drift
 * and reports nothing.
 */
export function processorStartInstances(
    detail: unknown,
    reportDrift?: (message: string) => void,
): string[] {
    const d = detail as { instances?: unknown } | null | undefined;
    if (!Array.isArray(d?.instances)) {
        const message =
            `surface: brenn-processor-start detail has no instances array ` +
            `(got ${typeof d?.instances}); no processors will start`;
        console.error(message);
        reportDrift?.(message);
        return [];
    }
    const instances: string[] = [];
    for (const i of d.instances) {
        if (typeof i === "string") {
            instances.push(i);
        } else {
            console.error(
                `surface: brenn-processor-start instances contained a ` +
                    `non-string entry (${typeof i}); skipping it`,
            );
        }
    }
    return instances;
}

/**
 * The kernel-to-bootstrap seam events, dispatched by the kernel on `window`.
 * Mirrored from the frozen contract (`brenn_surface_proto::contract`); the
 * bootstrap imports no Rust or generated types, so the names live here as
 * literals kept in step with that contract.
 */
const SURFACE_RELOAD_EVENT = "brenn-surface-reload";
const SURFACE_READY_EVENT = "brenn-surface-ready";
const PROCESSOR_START_EVENT = "brenn-processor-start";

/**
 * The reason to pass to `cappedReload` for a `brenn-surface-reload` seam event.
 * The kernel supplies `detail = { reason }`, but the kernel is a separately
 * deployed wasm artifact, so a missing or non-string reason falls back rather
 * than throwing. Exported for the unit tests.
 */
export function seamReloadReason(detail: unknown): string {
    const d = detail as { reason?: unknown } | null | undefined;
    return typeof d?.reason === "string" ? d.reason : "kernel reload";
}

/**
 * Install the two kernel-to-bootstrap seam listeners on `window`:
 * `brenn-surface-reload` (`detail = { reason }`) funnels every kernel-requested
 * reload — stale build, kernel panic, changed bindings — through `cappedReload`;
 * `brenn-surface-ready` resets the reload counter on the first successful
 * connect.
 *
 * Installed by `bootstrap` *before* `start()`: DOM dispatch is synchronous and
 * reaches only listeners registered at dispatch time, and the kernel's panic
 * hook (installed inside `start()`) can dispatch `brenn-surface-reload`
 * synchronously during `start()` itself — that must land on an already-installed
 * listener or the capped-reload guard never fires for the startup-death case.
 */
function installSeamListeners(
    kernel: KernelModule,
    manifest: SurfaceManifest,
    importModule: ModuleImporter,
): void {
    window.addEventListener(SURFACE_RELOAD_EVENT, (ev: Event) => {
        cappedReload(seamReloadReason((ev as CustomEvent).detail));
    });
    window.addEventListener(SURFACE_READY_EVENT, () => {
        resetReloadCount();
    });
    // `brenn-processor-start` names the headless instances to bring up. Dispatch
    // is synchronous and bring-up is not, so the handler cannot await: it starts
    // the work and lets the kernel's event loop continue. Every failure inside
    // reports itself to the kernel, so nothing is owed to this promise.
    window.addEventListener(PROCESSOR_START_EVENT, (ev: Event) => {
        const instances = processorStartInstances(
            (ev as CustomEvent).detail,
            // A malformed detail is glue drift: route it through the same
            // post-kernel error channel an uncaught error uses, so it reaches
            // backend telemetry instead of dying in the console.
            (message) => handleGlobalError(message, PROCESSOR_START_EVENT),
        );
        void startProcessors(kernel, manifest, instances, importModule);
    });
}

/**
 * The bootstrap sequence. Runs the boot steps in order: global handlers first
 * (so an error anywhere in the rest of boot is caught), page-input reads, module
 * loading, seam-listener installation, then `start()`. A null return from
 * `readPageInputs` (broken deploy — static failure already rendered) or
 * `loadModules` (kernel load failed — `cappedReload` already fired) aborts the
 * sequence without calling `start()`.
 *
 * Exported so tests drive it with an injected importer; production calls it via
 * the guarded module-level invocation below.
 */
export async function bootstrap(
    importModule: ModuleImporter = defaultImporter,
): Promise<void> {
    installGlobalHandlers();
    const manifest = readSurfaceManifest();
    if (manifest === null) {
        return;
    }
    const kernel = await loadModules(manifest, importModule);
    if (kernel === null) {
        return;
    }
    installSeamListeners(kernel, manifest, importModule);
    setKernelHandle(kernel.start());
}

// Module-level side effect: bootstrap a real surface page. The served page
// carries the manifest `<script>`, and the deferred module script runs after it
// is parsed, so its presence marks a genuine entry — and keeps this module
// import side-effect-free under unit tests, which import the named exports.
if (document.getElementById(MANIFEST_SCRIPT_ID) !== null) {
    void bootstrap();
}
