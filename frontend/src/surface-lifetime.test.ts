// @vitest-environment happy-dom
//
// One rule, on the one host whose answer to it is TypeScript: the page's loader
// gives a component one instance per *activation*.
//
// The Rust host-conformance suite's surface adapter instantiates per call from
// its first line, so what it pins is the contract, not this loader — the page's
// lifetime policy lives in `startProcessor` and nothing under wasmtime can see
// it. This file is the other half: the same rule, asserted against the
// production bring-up path, so a loader that quietly went back to one instance
// per instance would fail here rather than in an out-of-tree component months
// later.

import { beforeAll, describe, expect, it, vi } from "vitest";
import {
    type KernelActivation,
    type ManifestComponent,
    type ModuleImporter,
    startProcessors,
    type SurfaceManifest,
} from "./surface.js";

const KIND_MODULE = "/surface-static/processor/probe/probe.js?v=b";

/** An activation over one empty port: the shape, not the content, is the point. */
function activation(): string {
    const record: KernelActivation = {
        ports: [{ port: "in", envelopes: [], new_from: 0, dropped: 0 }],
        deferred: [],
        now: 0,
        sync: null,
    };
    return JSON.stringify(record);
}

function manifest(): SurfaceManifest {
    const component: ManifestComponent = {
        instance: "probe",
        kind: "probe",
        module: KIND_MODULE,
        // The core-URL path is pinned in `surface.test.ts`.
        cores: [],
    };
    return {
        kernel: "/surface-static/brenn_surface_kernel.js?v=b",
        components: [component],
        withheld: [],
    };
}

/**
 * Only what the loader touches on the bring-up path. The fake guest calls no
 * import, so the rest of the host seam never runs.
 */
function registeringKernel(): {
    kernel: unknown;
    entry: () => (a: string) => unknown;
} {
    let registered: ((a: string) => unknown) | undefined;
    const kernel = {
        brenn_processor_register: vi.fn(
            (_i: string, e: (a: string) => unknown) => {
                registered = e;
                return true;
            },
        ),
        brenn_processor_load_failed: vi.fn((_i: string, _d: string) => {}),
        brenn_processor_withheld: vi.fn((_i: string, _r: string) => {}),
    };
    return {
        kernel,
        entry: () => {
            if (registered === undefined) {
                throw new Error("the loader registered no activation entry");
            }
            return registered;
        },
    };
}

interface Driven {
    importModule: ReturnType<typeof vi.fn>;
    instantiate: ReturnType<typeof vi.fn>;
    /** One entry per instantiation: how many activations that memory saw. */
    counters: number[];
}

/** Bring one kind up through the production loader and activate it twice. */
async function driveTwoActivations(): Promise<Driven> {
    const { kernel, entry } = registeringKernel();
    // A guest whose memory is its own closure: a fresh one per instantiation
    // counts from zero, a reused one keeps counting.
    const counters: number[] = [];
    // Synchronous, as jco's `sync` instantiation mode is and as a sync-call
    // activation needs — the entry runs inside a gesture's dispatch and has
    // nothing to await with.
    const instantiate = vi.fn(() => {
        const mine = counters.push(0) - 1;
        return {
            receive: (_a: unknown) => {
                counters[mine] += 1;
                return undefined;
            },
        };
    });
    const importModule = vi.fn(async () => ({ instantiate }));

    await startProcessors(
        kernel as unknown as Parameters<typeof startProcessors>[0],
        manifest(),
        ["probe"],
        importModule as unknown as ModuleImporter,
    );

    entry()(activation());
    entry()(activation());

    return { importModule, instantiate, counters };
}

describe("page loader instance lifetime", () => {
    // Driven once for both tests. A bring-up failure fails here, loudly, rather
    // than showing up as a confusing assertion further down.
    let driven: Driven;
    beforeAll(async () => {
        driven = await driveTwoActivations();
    });

    it("instantiates the kind's compiled module once per activation", () => {
        expect(driven.importModule).toHaveBeenCalledTimes(1);
        expect(driven.instantiate).toHaveBeenCalledTimes(2);
        expect(driven.counters).toEqual([1, 1]);
    });

    it("instantiates nothing at bring-up", async () => {
        // Bring-up buys compilation, not a guest: an instance the page never
        // activates never runs a start function, and a kind's statics cannot
        // be initialized ahead of the activation that reads them.
        const { kernel } = registeringKernel();
        const instantiate = vi.fn(() => ({ receive: () => undefined }));
        const importModule = vi.fn(async () => ({ instantiate }));
        await startProcessors(
            kernel as unknown as Parameters<typeof startProcessors>[0],
            manifest(),
            ["probe"],
            importModule as unknown as ModuleImporter,
        );
        expect(importModule).toHaveBeenCalledTimes(1);
        expect(instantiate).not.toHaveBeenCalled();
    });
});
