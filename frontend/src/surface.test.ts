// @vitest-environment happy-dom
//
// Pins the surface bootstrap reload-loop guard in `frontend/src/surface.ts`.

import {
    afterEach,
    beforeAll,
    beforeEach,
    describe,
    expect,
    it,
    vi,
} from "vitest";
import { expectConsoleError } from "./test-setup.js";
import {
    bootstrap,
    cappedReload,
    installGlobalHandlers,
    loadKernel,
    type ManifestComponent,
    processorStartInstances,
    startProcessors,
    type ModuleImporter,
    readSurfaceManifest,
    renderStaticFailure,
    resetReloadCount,
    seamReloadReason,
    setKernelHandle,
    SURFACE_CHROME_DEATH_COUNT_KEY,
    SURFACE_RELOAD_COUNT_KEY,
    type SurfaceManifest,
} from "./surface.js";

/**
 * Runs `body` with `sessionStorage.setItem` replaced by `stub`, then restores
 * it. happy-dom's Storage is Proxy-backed, so a plain assignment or delete does
 * not restore the method; the restore installs a prototype passthrough via
 * defineProperty. Used to exercise the unpersistable-counter branch of
 * cappedReload (setItem that silently no-ops or throws).
 */
function withBrokenSetItem(stub: () => void, body: () => void): void {
    const proto = Object.getPrototypeOf(sessionStorage);
    Object.defineProperty(sessionStorage, "setItem", {
        configurable: true,
        writable: true,
        value: stub,
    });
    try {
        body();
    } finally {
        Object.defineProperty(sessionStorage, "setItem", {
            configurable: true,
            writable: true,
            value: function (this: Storage, key: string, val: string) {
                proto.setItem.call(this, key, val);
            },
        });
    }
}

describe("surface bootstrap capped-reload guard", () => {
    let reloadSpy: ReturnType<typeof vi.spyOn>;

    beforeEach(() => {
        sessionStorage.clear();
        document.body.innerHTML = '<div id="surface-root"></div>';
        // happy-dom exposes window.location but does not pre-stub reload.
        reloadSpy = vi
            .spyOn(window.location, "reload")
            .mockImplementation(() => {});
    });

    afterEach(() => {
        vi.restoreAllMocks();
        sessionStorage.clear();
        document.body.innerHTML = "";
        // Clears the terminal-failure latch the cap tests can set.
        setKernelHandle(null);
    });

    it("first reload: counter → 1, reload called once", () => {
        cappedReload("stale build");
        expect(sessionStorage.getItem(SURFACE_RELOAD_COUNT_KEY)).toBe("1");
        expect(reloadSpy).toHaveBeenCalledTimes(1);
    });

    it("increments across successive reloads up to the cap", () => {
        cappedReload("kernel load failed");
        cappedReload("kernel load failed");
        expect(sessionStorage.getItem(SURFACE_RELOAD_COUNT_KEY)).toBe("2");
        expect(reloadSpy).toHaveBeenCalledTimes(2);
    });

    it("at the cap: no reload, static failure rendered, counter not bumped", () => {
        sessionStorage.setItem(SURFACE_RELOAD_COUNT_KEY, "3");
        expectConsoleError(/surface bootstrap failure/);
        expectConsoleError(/surface reload cap reached/);

        cappedReload("kernel panic");

        expect(reloadSpy).not.toHaveBeenCalled();
        expect(sessionStorage.getItem(SURFACE_RELOAD_COUNT_KEY)).toBe("3");
        const root = document.getElementById("surface-root");
        expect(root?.textContent).toContain("couldn't start");
    });

    it("reset-on-ready clears the counter", () => {
        sessionStorage.setItem(SURFACE_RELOAD_COUNT_KEY, "2");
        resetReloadCount();
        expect(sessionStorage.getItem(SURFACE_RELOAD_COUNT_KEY)).toBeNull();
    });

    it("chrome-death reloads count on their own key, not the shared one", () => {
        cappedReload("chrome died");
        expect(sessionStorage.getItem(SURFACE_CHROME_DEATH_COUNT_KEY)).toBe(
            "1",
        );
        expect(sessionStorage.getItem(SURFACE_RELOAD_COUNT_KEY)).toBeNull();
        expect(reloadSpy).toHaveBeenCalledTimes(1);
    });

    it("reset-on-ready leaves the chrome-death counter alone (loop converges)", () => {
        // Each post-ready chrome-death reload reconnects, fires ready (reset),
        // then dies again. If the chrome-death counter reset here it would loop
        // forever; instead it accumulates to the cap and stops reloading.
        cappedReload("chrome died");
        resetReloadCount();
        cappedReload("chrome died");
        resetReloadCount();
        cappedReload("chrome died");
        resetReloadCount();
        expect(sessionStorage.getItem(SURFACE_CHROME_DEATH_COUNT_KEY)).toBe(
            "3",
        );
        expect(reloadSpy).toHaveBeenCalledTimes(3);

        // At the cap the next chrome death renders the static failure instead of
        // reloading.
        expectConsoleError(/surface bootstrap failure/);
        expectConsoleError(/surface reload cap reached/);
        cappedReload("chrome died");
        expect(reloadSpy).toHaveBeenCalledTimes(3);
        const root = document.getElementById("surface-root");
        expect(root?.textContent).toContain("couldn't start");
    });

    it("tolerant read: unparseable counter is treated as zero", () => {
        sessionStorage.setItem(SURFACE_RELOAD_COUNT_KEY, "not-a-number");
        cappedReload("stale build");
        // Treated as 0, so it increments to 1 and reloads (not stuck).
        expect(sessionStorage.getItem(SURFACE_RELOAD_COUNT_KEY)).toBe("1");
        expect(reloadSpy).toHaveBeenCalledTimes(1);
    });

    // setItem that never persists (silent no-op or throw) means the read-back
    // never confirms the increment, so the cap could never trip via the counter
    // and an unconditional reload would loop forever; cappedReload must instead
    // converge to the static failure. Both storage-failure shapes exercise the
    // same branch, so they share withBrokenSetItem and one assertion body.
    for (const [label, stub] of [
        ["silent no-op", () => {}],
        [
            "setItem throws",
            () => {
                throw new Error("storage disabled");
            },
        ],
    ] as const) {
        it(`unpersistable counter (${label}): converges to static failure, does not reload`, () => {
            expectConsoleError(/surface bootstrap failure/);
            expectConsoleError(/surface reload counter not persistable/);
            withBrokenSetItem(stub, () => {
                cappedReload("kernel load failed");
                expect(reloadSpy).not.toHaveBeenCalled();
                const root = document.getElementById("surface-root");
                expect(root?.textContent).toContain("close and reopen");
            });
        });
    }

    it("renderStaticFailure writes textContent, not markup", () => {
        expectConsoleError(/surface bootstrap failure/);
        renderStaticFailure("<b>boom</b>");
        const root = document.getElementById("surface-root");
        expect(root?.textContent).toBe("<b>boom</b>");
        expect(root?.querySelector("b")).toBeNull();
    });
});

describe("surface bootstrap global error handlers", () => {
    // The window listeners are installed once for the module's lifetime; each
    // test resets the module handle to null in beforeEach, so every test starts
    // in the pre-kernel state regardless of order (post-kernel tests set their own
    // stub handle).
    beforeAll(() => {
        installGlobalHandlers();
    });

    beforeEach(() => {
        document.body.innerHTML = '<div id="surface-root"></div>';
        sessionStorage.clear();
        setKernelHandle(null);
    });

    afterEach(() => {
        vi.restoreAllMocks();
        document.body.innerHTML = "";
        sessionStorage.clear();
        setKernelHandle(null);
    });

    it("pre-kernel: uncaught error renders a static failure into #surface-root", () => {
        expectConsoleError(/surface bootstrap failure/);
        window.dispatchEvent(
            new ErrorEvent("error", {
                message: "pre-kernel-boom",
                error: new Error("pre-kernel-boom"),
            }),
        );
        const root = document.getElementById("surface-root");
        expect(root?.textContent).toContain("pre-kernel-boom");
    });

    it("pre-kernel: unhandled rejection renders a static failure", () => {
        expectConsoleError(/surface bootstrap failure/);
        const rejection = new Event(
            "unhandledrejection",
        ) as unknown as PromiseRejectionEvent;
        Object.defineProperty(rejection, "reason", {
            value: new Error("rejected-boom"),
        });
        window.dispatchEvent(rejection);
        const root = document.getElementById("surface-root");
        expect(root?.textContent).toContain("rejected-boom");
    });

    it("post-kernel: forwards to log_error, logs a console trace, leaves the DOM untouched", () => {
        // A console trace must always exist: log_error is best-effort and
        // silently dropped when the WS is down.
        expectConsoleError(/surface uncaught \(post-kernel\)/);
        const logError = vi.fn();
        setKernelHandle({ log_error: logError });
        window.dispatchEvent(
            new ErrorEvent("error", {
                message: "post-kernel-boom",
                error: new Error("post-kernel-boom"),
            }),
        );
        expect(logError).toHaveBeenCalledTimes(1);
        expect(logError.mock.calls[0]?.[0]).toContain("post-kernel-boom");
        expect(logError.mock.calls[0]?.[1]).toBe("window.error");
        // The kernel owns the surface DOM post-kernel; the handler must not clobber it.
        expect(document.getElementById("surface-root")?.textContent).toBe("");
    });

    it("post-kernel: a throwing log_error is logged, never propagates out of the listener", () => {
        expectConsoleError(/surface uncaught \(post-kernel\)/);
        expectConsoleError(/surface log_error failed/);
        setKernelHandle({
            log_error: () => {
                throw new Error("report failed");
            },
        });
        expect(() =>
            window.dispatchEvent(
                new ErrorEvent("error", {
                    message: "x",
                    error: new Error("x"),
                }),
            ),
        ).not.toThrow();
    });

    it("pre-kernel: a non-Error object rejection is described as JSON", () => {
        expectConsoleError(/surface bootstrap failure/);
        const rejection = new Event(
            "unhandledrejection",
        ) as unknown as PromiseRejectionEvent;
        Object.defineProperty(rejection, "reason", { value: { code: 1 } });
        window.dispatchEvent(rejection);
        expect(document.getElementById("surface-root")?.textContent).toContain(
            '"code":1',
        );
    });

    it("pre-kernel: a circular rejection falls back to String() without throwing", () => {
        expectConsoleError(/surface bootstrap failure/);
        const circular: Record<string, unknown> = {};
        circular.self = circular;
        const rejection = new Event(
            "unhandledrejection",
        ) as unknown as PromiseRejectionEvent;
        Object.defineProperty(rejection, "reason", { value: circular });
        expect(() => window.dispatchEvent(rejection)).not.toThrow();
        expect(document.getElementById("surface-root")?.textContent).toContain(
            "[object Object]",
        );
    });

    it("pre-kernel: an ErrorEvent with no error uses the message/filename fallback", () => {
        expectConsoleError(/surface bootstrap failure/);
        window.dispatchEvent(
            new ErrorEvent("error", {
                message: "no-error-obj",
                filename: "surface.js",
                lineno: 7,
                colno: 3,
                error: null,
            }),
        );
        const text = document.getElementById("surface-root")?.textContent;
        expect(text).toContain("no-error-obj");
        expect(text).toContain("surface.js:7:3");
    });

    it("pre-kernel: a terminal cap message is not overwritten by a later error", () => {
        // At the cap, cappedReload renders the actionable message and latches;
        // a subsequent uncaught error (e.g. the wasm unwind out of start()) must
        // not clobber it with a raw error dump.
        expectConsoleError(/surface bootstrap failure/);
        expectConsoleError(/surface reload cap reached/);
        expectConsoleError(/surface uncaught \(pre-kernel, terminal\)/);
        sessionStorage.setItem(SURFACE_RELOAD_COUNT_KEY, "3");

        cappedReload("kernel panic");
        window.dispatchEvent(
            new ErrorEvent("error", {
                message: "late-boom",
                error: new Error("late-boom"),
            }),
        );

        const text = document.getElementById("surface-root")?.textContent;
        expect(text).toContain("couldn't start");
        expect(text).not.toContain("late-boom");
    });
});

describe("surface bootstrap page-input reads", () => {
    beforeEach(() => {
        document.body.innerHTML = '<div id="surface-root"></div>';
        document.head.innerHTML = "";
    });

    afterEach(() => {
        vi.restoreAllMocks();
        document.body.innerHTML = "";
        document.head.innerHTML = "";
    });

    function setPage(metasAndManifest: string): void {
        document.head.innerHTML = metasAndManifest;
    }

    const METAS =
        '<meta name="surface-slug" content="deskbar">' +
        '<meta name="brenn-build-id" content="build-xyz">';

    function manifestScript(json: string): string {
        return `<script type="application/json" id="brenn-surface-manifest">${json}</script>`;
    }

    it("reads a well-formed manifest", () => {
        setPage(
            METAS +
                manifestScript(
                    JSON.stringify({
                        kernel: "/surface-static/brenn_surface_kernel.js?v=build-xyz",
                        components: [
                            {
                                instance: "echo-stub",
                                kind: "echo-stub",
                                module: "/surface-static/brenn_echo_stub.js?v=build-xyz&instance=echo-stub",
                            },
                        ],
                    }),
                ),
        );
        const manifest = readSurfaceManifest();
        expect(manifest).not.toBeNull();
        expect(manifest?.kernel).toBe(
            "/surface-static/brenn_surface_kernel.js?v=build-xyz",
        );
        expect(manifest?.components).toEqual([
            {
                instance: "echo-stub",
                kind: "echo-stub",
                module: "/surface-static/brenn_echo_stub.js?v=build-xyz&instance=echo-stub",
            },
        ]);
    });

    it("missing manifest script → static failure, returns null", () => {
        expectConsoleError(/surface bootstrap failure/);
        setPage(METAS);
        expect(readSurfaceManifest()).toBeNull();
        expect(document.getElementById("surface-root")?.textContent).toContain(
            "manifest",
        );
    });

    it("malformed manifest JSON → static failure, returns null", () => {
        expectConsoleError(/surface bootstrap failure/);
        setPage(METAS + manifestScript("{not valid json"));
        expect(readSurfaceManifest()).toBeNull();
    });

    it("manifest of the wrong top-level shape → static failure, returns null", () => {
        expectConsoleError(/surface bootstrap failure/);
        setPage(
            METAS +
                manifestScript(
                    JSON.stringify({ kernel: 123, components: "nope" }),
                ),
        );
        expect(readSurfaceManifest()).toBeNull();
    });

    it("manifest with a malformed component entry → static failure, returns null", () => {
        expectConsoleError(/surface bootstrap failure/);
        setPage(
            METAS +
                manifestScript(
                    JSON.stringify({
                        kernel: "/surface-static/brenn_surface_kernel.js?v=b",
                        components: [{ instance: "i", kind: 1, module: "x" }],
                    }),
                ),
        );
        expect(readSurfaceManifest()).toBeNull();
    });
});

describe("surface bootstrap module loading", () => {
    let reloadSpy: ReturnType<typeof vi.spyOn>;

    beforeEach(() => {
        sessionStorage.clear();
        document.body.innerHTML = '<div id="surface-root"></div>';
        reloadSpy = vi
            .spyOn(window.location, "reload")
            .mockImplementation(() => {});
    });

    afterEach(() => {
        vi.restoreAllMocks();
        sessionStorage.clear();
        document.body.innerHTML = "";
    });

    const KERNEL_URL = "/surface-static/brenn_surface_kernel.js?v=b";

    function manifest(components: ManifestComponent[]): SurfaceManifest {
        return { kernel: KERNEL_URL, components };
    }

    it("kernel import failure → cappedReload, returns null, components not loaded", async () => {
        expectConsoleError(/kernel module load failed/);
        const importModule = vi.fn(async () => {
            throw new Error("404");
        });
        const kernel = await loadKernel(
            manifest([
                {
                    instance: "echo-stub",
                    kind: "echo-stub",
                    module: "/x.js",
                },
            ]).kernel,
            importModule,
        );
        expect(kernel).toBeNull();
        expect(reloadSpy).toHaveBeenCalledTimes(1);
        // Only the kernel import was attempted; components are skipped.
        expect(importModule).toHaveBeenCalledTimes(1);
    });

    it("kernel init (wasm) throw → cappedReload, returns null", async () => {
        expectConsoleError(/kernel module load failed/);
        const importModule = vi.fn(async (url: string) => {
            if (url === KERNEL_URL) {
                return {
                    default: async () => {
                        throw new Error("wasm init");
                    },
                    start: vi.fn(),
                };
            }
            return { default: async () => {} };
        });
        const kernel = await loadKernel(manifest([]).kernel, importModule);
        expect(kernel).toBeNull();
        expect(reloadSpy).toHaveBeenCalledTimes(1);
    });

});

// Placed last: bootstrap() calls installGlobalHandlers(), adding window
// error/unhandledrejection listeners that persist for the module lifetime.
// These tests dispatch only the seam events, so the extra listeners are inert,
// but keeping the block last avoids perturbing the error-handler tests above.
describe("surface bootstrap orchestration", () => {
    let reloadSpy: ReturnType<typeof vi.spyOn>;

    const KERNEL_URL = "/surface-static/brenn_surface_kernel.js?v=b";

    beforeEach(() => {
        sessionStorage.clear();
        document.head.innerHTML = `<script type="application/json" id="brenn-surface-manifest">${JSON.stringify(
            { kernel: KERNEL_URL, components: [] },
        )}</script>`;
        document.body.innerHTML = '<div id="surface-root"></div>';
        reloadSpy = vi
            .spyOn(window.location, "reload")
            .mockImplementation(() => {});
    });

    afterEach(() => {
        vi.restoreAllMocks();
        sessionStorage.clear();
        document.head.innerHTML = "";
        document.body.innerHTML = "";
    });

    /** A stub importer whose kernel module runs `onStart` inside `start()`. */
    function kernelImporter(onStart: () => void): ModuleImporter {
        return vi.fn(async (url: string) => {
            if (url === KERNEL_URL) {
                return {
                    default: async () => {},
                    start: vi.fn(() => {
                        onStart();
                        return { log_error: vi.fn() };
                    }),
                };
            }
            return { default: async () => {} };
        });
    }

    it("installs seam listeners before start(): a synchronous reload dispatch is caught", async () => {
        // start() synchronously dispatches brenn-surface-reload — the panic-hook
        // startup-death case. The listener must already be installed.
        const start = vi.fn(() => {
            window.dispatchEvent(
                new CustomEvent("brenn-surface-reload", {
                    detail: { reason: "kernel panic" },
                }),
            );
            return { log_error: vi.fn() };
        });
        const importModule: ModuleImporter = vi.fn(async (url: string) => {
            if (url === KERNEL_URL) {
                return { default: async () => {}, start };
            }
            return { default: async () => {} };
        });

        await bootstrap(importModule);

        expect(start).toHaveBeenCalledTimes(1);
        // The seam listener funnelled the synchronous reload through cappedReload.
        expect(reloadSpy).toHaveBeenCalledTimes(1);
        expect(sessionStorage.getItem(SURFACE_RELOAD_COUNT_KEY)).toBe("1");
    });

    it("brenn-surface-ready resets the reload counter", async () => {
        sessionStorage.setItem(SURFACE_RELOAD_COUNT_KEY, "2");
        await bootstrap(
            kernelImporter(() => {
                window.dispatchEvent(new CustomEvent("brenn-surface-ready"));
            }),
        );
        expect(sessionStorage.getItem(SURFACE_RELOAD_COUNT_KEY)).toBeNull();
    });

    it("aborts before start() when the page has no manifest", async () => {
        expectConsoleError(/surface bootstrap failure/);
        document.head.innerHTML = "";
        const start = vi.fn(() => ({ log_error: vi.fn() }));
        const importModule: ModuleImporter = vi.fn(async () => ({
            default: async () => {},
            start,
        }));

        await bootstrap(importModule);

        expect(importModule).not.toHaveBeenCalled();
        expect(start).not.toHaveBeenCalled();
    });

    it("aborts before start() when the kernel module fails to load", async () => {
        expectConsoleError(/kernel module load failed/);
        const start = vi.fn(() => ({ log_error: vi.fn() }));
        const importModule: ModuleImporter = vi.fn(async (url: string) => {
            if (url === KERNEL_URL) {
                throw new Error("404");
            }
            return { default: async () => {}, start };
        });

        await bootstrap(importModule);

        expect(start).not.toHaveBeenCalled();
        // loadKernel routed the kernel failure through cappedReload.
        expect(reloadSpy).toHaveBeenCalledTimes(1);
    });
});

describe("surface seam reload reason", () => {
    it("uses a well-formed string reason", () => {
        expect(seamReloadReason({ reason: "stale build" })).toBe("stale build");
    });

    it("falls back to 'kernel reload' for missing/non-string detail", () => {
        expect(seamReloadReason(undefined)).toBe("kernel reload");
        expect(seamReloadReason(null)).toBe("kernel reload");
        expect(seamReloadReason({})).toBe("kernel reload");
        expect(seamReloadReason({ reason: 5 })).toBe("kernel reload");
    });
});

describe("surface processor start detail", () => {
    it("reads a well-formed instance list", () => {
        expect(processorStartInstances({ instances: ["a", "b"] })).toEqual([
            "a",
            "b",
        ]);
    });

    it("yields nothing for a missing or non-array detail, and skips non-strings — loudly", () => {
        expect(processorStartInstances(undefined)).toEqual([]);
        expect(processorStartInstances(null)).toEqual([]);
        expect(processorStartInstances({})).toEqual([]);
        expect(processorStartInstances({ instances: "a" })).toEqual([]);
        expect(processorStartInstances({ instances: ["a", 5] })).toEqual(["a"]);
        // A malformed same-page kernel detail is a bug, not a quiet no-op:
        // four missing/non-array cases plus the one non-string entry.
        expectConsoleError(/no instances array/);
        expectConsoleError(/no instances array/);
        expectConsoleError(/no instances array/);
        expectConsoleError(/no instances array/);
        expectConsoleError(/non-string entry/);
    });

    it("routes a malformed detail to the drift reporter, but not a legit-empty one", () => {
        const reportDrift = vi.fn();
        expect(processorStartInstances({}, reportDrift)).toEqual([]);
        expect(reportDrift).toHaveBeenCalledTimes(1);
        expect(reportDrift).toHaveBeenCalledWith(
            expect.stringMatching(/no instances array/),
        );
        expectConsoleError(/no instances array/);

        // A legitimately empty instance list is not drift: nothing reported.
        reportDrift.mockClear();
        expect(processorStartInstances({ instances: [] }, reportDrift)).toEqual(
            [],
        );
        expect(reportDrift).not.toHaveBeenCalled();
    });
});

describe("surface processor bring-up", () => {
    /** A kernel host seam recording what the loader asked of it. */
    function fakeKernel() {
        return {
            default: async () => {},
            start: vi.fn(),
            brenn_processor_publish: vi.fn(
                (_i: string, _p: string, _b: string, _u?: string) => "",
            ),
            brenn_processor_publish_deferred: vi.fn(
                (_i: string, _p: string, _b: string, _d: bigint) => "",
            ),
            brenn_processor_defer_cancel: vi.fn(
                (_i: string, _p: string, _x: number) => "",
            ),
            brenn_processor_defer_edit: vi.fn(
                (
                    _i: string,
                    _p: string,
                    _x: number,
                    _b?: string,
                    _d?: bigint,
                ) => "",
            ),
            brenn_processor_log: vi.fn(),
            brenn_processor_alert: vi.fn(),
            brenn_processor_config_get: vi.fn(
                (_i: string, _k: string) => undefined,
            ),
            brenn_processor_register: vi.fn(
                (_i: string, _e: (a: string) => unknown) => true,
            ),
            brenn_processor_load_failed: vi.fn((_i: string, _d: string) => {}),
            brenn_dom_root: vi.fn((_i: string) => 1n),
            brenn_dom_create_element: vi.fn((_i: string, _t: string) => 1n),
            brenn_dom_set_attribute: vi.fn(
                (_i: string, _n: bigint, _k: string, _v: string) => {},
            ),
            brenn_dom_remove_attribute: vi.fn(
                (_i: string, _n: bigint, _k: string) => {},
            ),
            brenn_dom_set_text: vi.fn(
                (_i: string, _n: bigint, _t: string) => {},
            ),
            brenn_dom_set_style_property: vi.fn(
                (_i: string, _n: bigint, _k: string, _v: string) => {},
            ),
            brenn_dom_remove_style_property: vi.fn(
                (_i: string, _n: bigint, _k: string) => {},
            ),
            brenn_dom_append: vi.fn((_i: string, _p: bigint, _c: bigint) => {}),
            brenn_dom_insert_before: vi.fn(
                (_i: string, _p: bigint, _c: bigint, _r?: bigint) => {},
            ),
            brenn_dom_remove: vi.fn((_i: string, _n: bigint) => {}),
            brenn_dom_value: vi.fn((_i: string, _n: bigint) => ""),
            brenn_dom_set_value: vi.fn(
                (_i: string, _n: bigint, _v: string) => {},
            ),
            brenn_dom_listen: vi.fn(
                (_i: string, _n: bigint, _e: string, _p: string) => {},
            ),
            brenn_dom_utc_offset_minutes: vi.fn(
                (_i: string, _e: bigint) => 0,
            ),
            brenn_dom_page_root: vi.fn((_i: string) => 1n),
            brenn_dom_page_body: vi.fn((_i: string) => 1n),
            brenn_dom_instance_wrapper: vi.fn(
                (_i: string, _of: string) => undefined,
            ),
            brenn_dom_parent: vi.fn((_i: string, _n: bigint) => undefined),
        };
    }

    const PROC_MODULE = "/surface-static/processor/counter/counter.js?v=b";

    function manifest(components: ManifestComponent[]): SurfaceManifest {
        return {
            kernel: "/surface-static/brenn_surface_kernel.js?v=b",
            components,
        };
    }

    function processorEntry(instance: string): ManifestComponent {
        return {
            instance,
            kind: "counter",
            module: PROC_MODULE,
        };
    }

    it("evaluates the kind's module once and instantiates once per instance", async () => {
        const kernel = fakeKernel();
        const receive = vi.fn();
        const instantiate = vi.fn(async () => ({ receive }));
        const importModule = vi.fn(async () => ({ instantiate }));

        await startProcessors(
            kernel as unknown as Parameters<typeof startProcessors>[0],
            manifest([processorEntry("p1"), processorEntry("p2")]),
            ["p1", "p2"],
            importModule as unknown as ModuleImporter,
        );

        // One evaluation per kind (siblings share the URL), one instantiation
        // per instance — its own linear memory.
        expect(importModule).toHaveBeenCalledTimes(1);
        expect(instantiate).toHaveBeenCalledTimes(2);
        expect(
            kernel.brenn_processor_register.mock.calls.map((c) => c[0]),
        ).toEqual(["p1", "p2"]);
        expect(kernel.brenn_processor_load_failed).not.toHaveBeenCalled();
    });

    it("closes each instance's imports over its own instance id", async () => {
        const kernel = fakeKernel();
        let captured: Record<string, Record<string, unknown>> | undefined;
        const importModule = vi.fn(async () => ({
            instantiate: async (
                _core: unknown,
                imports: Record<string, Record<string, unknown>>,
            ) => {
                captured = imports;
                return { receive: vi.fn() };
            },
        }));

        await startProcessors(
            kernel as unknown as Parameters<typeof startProcessors>[0],
            manifest([processorEntry("p1")]),
            ["p1"],
            importModule as unknown as ModuleImporter,
        );

        const ports = captured?.["brenn:processor/ports"] as {
            publish: (port: string, payload: string) => void;
        };
        ports.publish("out", "{}");
        expect(kernel.brenn_processor_publish).toHaveBeenCalledWith(
            "p1",
            "out",
            "{}",
            undefined,
        );

        const config = captured?.["brenn:processor/config"] as {
            get: (key: string) => unknown;
        };
        config.get("greeting");
        expect(kernel.brenn_processor_config_get).toHaveBeenCalledWith(
            "p1",
            "greeting",
        );
    });

    // Every `brenn:processor/dom` and `brenn:processor/page-dom` shim, as
    // [interface, method, the component's arguments, the kernel export it
    // forwards to]. Pure argument plumbing is exactly the code that fails by
    // transposition, and the instance the closure supplies — the whole of the
    // confinement — is a leading argument no component can reach.
    const DOM_SHIMS: ReadonlyArray<
        readonly [string, string, readonly unknown[], string]
    > = [
        ["brenn:processor/dom", "root", [], "brenn_dom_root"],
        [
            "brenn:processor/dom",
            "createElement",
            ["div"],
            "brenn_dom_create_element",
        ],
        [
            "brenn:processor/dom",
            "setAttribute",
            [3n, "data-x", "v"],
            "brenn_dom_set_attribute",
        ],
        [
            "brenn:processor/dom",
            "removeAttribute",
            [3n, "data-x"],
            "brenn_dom_remove_attribute",
        ],
        ["brenn:processor/dom", "setText", [3n, "hi"], "brenn_dom_set_text"],
        [
            "brenn:processor/dom",
            "setStyleProperty",
            [3n, "color", "red"],
            "brenn_dom_set_style_property",
        ],
        [
            "brenn:processor/dom",
            "removeStyleProperty",
            [3n, "color"],
            "brenn_dom_remove_style_property",
        ],
        ["brenn:processor/dom", "append", [3n, 4n], "brenn_dom_append"],
        [
            "brenn:processor/dom",
            "insertBefore",
            [3n, 4n, 5n],
            "brenn_dom_insert_before",
        ],
        ["brenn:processor/dom", "remove", [3n], "brenn_dom_remove"],
        ["brenn:processor/dom", "value", [3n], "brenn_dom_value"],
        [
            "brenn:processor/dom",
            "setValue",
            [3n, "typed"],
            "brenn_dom_set_value",
        ],
        [
            "brenn:processor/dom",
            "listen",
            [3n, "click", "send"],
            "brenn_dom_listen",
        ],
        [
            "brenn:processor/dom",
            "utcOffsetMinutes",
            [1700000000000n],
            "brenn_dom_utc_offset_minutes",
        ],
        ["brenn:processor/page-dom", "pageRoot", [], "brenn_dom_page_root"],
        ["brenn:processor/page-dom", "pageBody", [], "brenn_dom_page_body"],
        [
            "brenn:processor/page-dom",
            "instanceWrapper",
            ["chrome"],
            "brenn_dom_instance_wrapper",
        ],
        ["brenn:processor/page-dom", "parent", [3n], "brenn_dom_parent"],
    ];

    async function capturedImports(
        kernel: ReturnType<typeof fakeKernel>,
        instance = "p1",
    ): Promise<Record<string, Record<string, unknown>>> {
        let captured: Record<string, Record<string, unknown>> | undefined;
        const importModule = vi.fn(async () => ({
            instantiate: async (
                _core: unknown,
                imports: Record<string, Record<string, unknown>>,
            ) => {
                captured = imports;
                return { receive: vi.fn() };
            },
        }));
        await startProcessors(
            kernel as unknown as Parameters<typeof startProcessors>[0],
            manifest([processorEntry(instance)]),
            [instance],
            importModule as unknown as ModuleImporter,
        );
        if (captured === undefined) {
            throw new Error("the instance was never instantiated");
        }
        return captured;
    }

    it("forwards every dom and page-dom call with the closure's instance first", async () => {
        const kernel = fakeKernel();
        const captured = await capturedImports(kernel, "p1");

        for (const [iface, method, args, exportName] of DOM_SHIMS) {
            const shim = captured[iface]?.[method] as
                | ((...a: unknown[]) => unknown)
                | undefined;
            if (typeof shim !== "function") {
                throw new Error(`${iface}#${method} is not offered`);
            }
            // Arity is the negative case: the component supplies its own
            // arguments and has no parameter to name an instance with, so it
            // cannot address the kernel as anybody else.
            expect(shim.length).toBe(args.length);
            shim(...args);
            const forwarded = kernel[
                exportName as keyof typeof kernel
            ] as unknown as ReturnType<typeof vi.fn>;
            expect(forwarded).toHaveBeenCalledWith("p1", ...args);
        }
    });

    it("omits an absent insert-before reference rather than inventing one", async () => {
        const kernel = fakeKernel();
        const captured = await capturedImports(kernel, "p1");
        const dom = captured["brenn:processor/dom"] as {
            insertBefore: (
                parent: bigint,
                child: bigint,
                reference: bigint | undefined,
            ) => void;
        };
        dom.insertBefore(1n, 2n, undefined);
        expect(kernel.brenn_dom_insert_before).toHaveBeenCalledWith(
            "p1",
            1n,
            2n,
            undefined,
        );
    });

    it("delegates the deferred family and throws its refusals as defer-error", async () => {
        const kernel = fakeKernel();
        kernel.brenn_processor_defer_cancel = vi.fn(
            (_i: string, _p: string, _x: number) => "out-of-range",
        );
        let captured: Record<string, Record<string, unknown>> | undefined;
        const importModule = vi.fn(async () => ({
            instantiate: async (
                _core: unknown,
                imports: Record<string, Record<string, unknown>>,
            ) => {
                captured = imports;
                return { receive: vi.fn() };
            },
        }));

        await startProcessors(
            kernel as unknown as Parameters<typeof startProcessors>[0],
            manifest([processorEntry("p1")]),
            ["p1"],
            importModule as unknown as ModuleImporter,
        );

        const ports = captured?.["brenn:processor/ports"] as {
            publishDeferred: (
                port: string,
                payload: string,
                deliverAfter: bigint,
            ) => void;
            deferCancel: (port: string, index: number) => void;
            deferEdit: (
                port: string,
                index: number,
                payload: string | undefined,
                deliverAfter: bigint | undefined,
            ) => void;
        };

        ports.publishDeferred("out", "{}", 1_700_000_060_000n);
        expect(kernel.brenn_processor_publish_deferred).toHaveBeenCalledWith(
            "p1",
            "out",
            "{}",
            1_700_000_060_000n,
        );

        // Both `option` arguments of an edit, in both states: `undefined` is the
        // guest asking to leave that half alone, and must not become a value.
        ports.deferEdit("out", 1, "next", 1_700_000_120_000n);
        expect(kernel.brenn_processor_defer_edit).toHaveBeenCalledWith(
            "p1",
            "out",
            1,
            "next",
            1_700_000_120_000n,
        );
        ports.deferEdit("out", 1, undefined, undefined);
        expect(kernel.brenn_processor_defer_edit).toHaveBeenLastCalledWith(
            "p1",
            "out",
            1,
            undefined,
            undefined,
        );

        // A refusal from the control family carries a `defer-error` tag, which the
        // publish vocabulary has no spelling for — proof the shim forwards the
        // kernel's answer rather than a fixed set of its own.
        expect(() => ports.deferCancel("out", 7)).toThrowError(
            expect.objectContaining({
                payload: { tag: "out-of-range" },
            }) as Error,
        );
    });

    it("throws the publish-error variant so the glue lifts the err arm", async () => {
        const kernel = fakeKernel();
        kernel.brenn_processor_publish = vi.fn(
            (_i: string, _p: string, _b: string, _u?: string) =>
                "quota-exceeded",
        );
        let captured: Record<string, Record<string, unknown>> | undefined;
        const importModule = vi.fn(async () => ({
            instantiate: async (
                _core: unknown,
                imports: Record<string, Record<string, unknown>>,
            ) => {
                captured = imports;
                return { receive: vi.fn() };
            },
        }));

        await startProcessors(
            kernel as unknown as Parameters<typeof startProcessors>[0],
            manifest([processorEntry("p1")]),
            ["p1"],
            importModule as unknown as ModuleImporter,
        );

        const ports = captured?.["brenn:processor/ports"] as {
            publish: (port: string, payload: string) => void;
        };
        // The glue's `getErrorPayload` reads an own `payload` property; the tag
        // is the WIT variant name the guest sees.
        expect(() => ports.publish("out", "{}")).toThrowError(
            expect.objectContaining({
                payload: { tag: "quota-exceeded" },
            }) as Error,
        );
    });

    it("lifts the kernel's activation and tells err from trap at the shim", async () => {
        const kernel = fakeKernel();
        let entry: ((activation: string) => unknown) | undefined;
        kernel.brenn_processor_register = vi.fn(
            (_i: string, e: (a: string) => unknown) => {
                entry = e;
                return true;
            },
        ) as unknown as typeof kernel.brenn_processor_register;

        const seen: unknown[] = [];
        let behavior: "ok" | "reply" | "err" | "trap" = "ok";
        const receive = (activation: unknown): string | undefined => {
            seen.push(activation);
            if (behavior === "reply") {
                return '{"cancel":true}';
            }
            if (behavior === "err") {
                // What jco's ComponentError looks like at this boundary.
                const e = new Error("component error");
                Object.defineProperty(e, "payload", {
                    value: { tag: "processing-failed", val: "bad row" },
                });
                throw e;
            }
            if (behavior === "trap") {
                throw new Error("unreachable");
            }
            return undefined;
        };
        const importModule = vi.fn(async () => ({
            instantiate: async () => ({ receive }),
        }));

        await startProcessors(
            kernel as unknown as Parameters<typeof startProcessors>[0],
            manifest([processorEntry("p1")]),
            ["p1"],
            importModule as unknown as ModuleImporter,
        );

        const activation = JSON.stringify({
            ports: [{ port: "in", envelopes: ["{}"], new_from: 1, dropped: 2 }],
            deferred: [
                {
                    port: "out",
                    entries: [
                        {
                            index: 0,
                            payload: "ping",
                            deliver_after: 1_700_000_060_000,
                        },
                    ],
                },
            ],
            now: 1_700_000_000_000,
            sync: null,
        });

        // ok: the kernel reads undefined as "flush the buffer".
        expect(entry?.(activation)).toBeUndefined();
        // Every field, with the u64s as BigInts: the canonical ABI traps on a
        // missing field, not a smaller activation. Strict, so that a `sync` key
        // omitted rather than set to `undefined` fails here — the two are the
        // same to `toEqual` and are not the same to the lowering.
        expect(seen[0]).toStrictEqual({
            ports: [{ port: "in", envelopes: ["{}"], newFrom: 1, dropped: 2 }],
            deferred: [
                {
                    port: "out",
                    entries: [
                        {
                            index: 0,
                            payload: "ping",
                            deliverAfter: 1_700_000_060_000n,
                        },
                    ],
                },
            ],
            now: 1_700_000_000_000n,
            sync: undefined,
        });

        // Absent `now` serializes as `null`; the shim must lower it to
        // `undefined`, not to 0n. Same for the sync port's name.
        expect(
            entry?.(
                JSON.stringify({
                    ports: [],
                    deferred: [],
                    now: null,
                    sync: null,
                }),
            ),
        ).toBeUndefined();
        expect(seen[1]).toStrictEqual({
            ports: [],
            deferred: [],
            now: undefined,
            sync: undefined,
        });

        // A sync-call activation carries its port's name, and the guest's
        // `ok(some(reply))` comes back as the reply object the kernel reads.
        // Whether a reply was allowed is the kernel's call, not this seam's.
        behavior = "reply";
        const syncActivation = JSON.stringify({
            ports: [
                { port: "brenn:mount", envelopes: ["{}"], new_from: 0, dropped: 0 },
            ],
            deferred: [],
            now: null,
            sync: "brenn:mount",
        });
        expect(entry?.(syncActivation)).toEqual({ reply: '{"cancel":true}' });
        expect(seen[2]).toStrictEqual({
            ports: [
                { port: "brenn:mount", envelopes: ["{}"], newFrom: 0, dropped: 0 },
            ],
            deferred: [],
            now: undefined,
            sync: "brenn:mount",
        });

        // err: a returned string — buffer discarded, instance lives.
        behavior = "err";
        expect(entry?.(activation)).toBe("processing-failed: bad row");

        // trap: rethrown — terminal for the instance.
        behavior = "trap";
        expect(() => entry?.(activation)).toThrowError("unreachable");
    });

    it("reports import, instantiate, and registration failures to the kernel", async () => {
        const kernel = fakeKernel();
        kernel.brenn_processor_register = vi.fn(
            (instance: string) => instance !== "refused",
        ) as unknown as typeof kernel.brenn_processor_register;
        const importModule = vi.fn(async (url: string) => {
            if (url.includes("missing")) {
                throw new Error("404");
            }
            return {
                instantiate: async (
                    _core: unknown,
                    imports: Record<string, Record<string, unknown>>,
                ) => {
                    const config = imports["brenn:processor/config"] as {
                        get: (key: string) => unknown;
                    };
                    if (
                        config.get("__boom") === undefined &&
                        url.includes("bad")
                    ) {
                        throw new Error("bad core wasm");
                    }
                    return { receive: vi.fn() };
                },
            };
        });

        expectConsoleError(/instance 'gone'.*failed to start/);
        expectConsoleError(/instance 'bad'.*failed to start/);
        {
            await startProcessors(
                kernel as unknown as Parameters<typeof startProcessors>[0],
                manifest([
                    {
                        instance: "gone",
                        kind: "counter",
                        module: "/missing.js",
                    },
                    {
                        instance: "bad",
                        kind: "counter",
                        module: "/bad.js",
                    },
                    processorEntry("refused"),
                ]),
                ["gone", "bad", "refused", "undeclared"],
                importModule as unknown as ModuleImporter,
            );
        }

        const reported = kernel.brenn_processor_load_failed.mock.calls.map(
            (c) => c[0],
        );
        expect(reported.sort()).toEqual([
            "bad",
            "gone",
            "refused",
            "undeclared",
        ]);
    });
});
