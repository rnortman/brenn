// The loading must be identical across suites for their claims to be about the
// same hosting: the same `--instantiation sync` entry point, core modules read
// from the same directory, and a missing tree failing with the build command.
//
// TODO(processor-transplant-browser-engine): this resolves the transpiled tree
// by filesystem path — it dynamic-imports a `file://` URL and reads the core
// wasm bytes with `readFileSync` — so the guest runs under node's WebAssembly
// engine, not a browser one.

import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import type { ProcessorInstance } from "../surface.js";

/** The transpiled module's `--instantiation sync` entry point. */
type Instantiate = (
    getCoreModule: (name: string) => WebAssembly.Module,
    imports: Record<string, Record<string, unknown>>,
) => ProcessorInstance;

/**
 * The repo root. vitest runs with its config root (`frontend/`) as cwd, so the
 * root is one level up; the build artifacts these suites read live outside the
 * frontend tree.
 */
export const REPO_ROOT = resolve(process.cwd(), "..");

/** Where the transpiled tree of one processor kind is served from. */
export function transpiledDir(kind: string): string {
    return resolve(REPO_ROOT, "surface/dist/processor", kind);
}

/**
 * Fail naming the command that builds the tree, rather than skipping.
 *
 * A silently skipped test asserts its invariant nowhere and reports green.
 */
export function requireTranspiledTree(kind: string): void {
    const dir = transpiledDir(kind);
    if (!existsSync(resolve(dir, `${kind}.js`))) {
        throw new Error(
            `the transpiled ${kind} tree is missing at ${dir} — ` +
                "build it with `make surface-transpile`",
        );
    }
}

/**
 * The glue evaluation and the compiled core modules of one kind, both kept for
 * the process. Compiling is the expensive half and does not vary per
 * activation — which is also the split the page loader has: one compiled module
 * per kind, one instance per activation.
 */
const compiled = new Map<string, Promise<Compiled>>();

interface Compiled {
    instantiate: Instantiate;
    /** Core module per file name, compiled on first ask and kept. */
    cores: Map<string, WebAssembly.Module>;
}

function compileTranspiled(kind: string): Promise<Compiled> {
    const existing = compiled.get(kind);
    if (existing !== undefined) {
        return existing;
    }
    const dir = transpiledDir(kind);
    const loading = (async (): Promise<Compiled> => {
        const { instantiate } = (await import(
            /* @vite-ignore */ pathToFileURL(resolve(dir, `${kind}.js`)).href
        )) as { instantiate: Instantiate };
        return { instantiate, cores: new Map() };
    })();
    compiled.set(kind, loading);
    return loading;
}

/**
 * Instantiate the transpiled guest of `kind` against `imports`.
 *
 * Cheap after the first call for a kind: the glue is evaluated once and each
 * core module is compiled once, so a caller that instantiates per activation
 * pays for instantiation and not for compilation.
 */
export async function instantiateTranspiled(
    kind: string,
    imports: Record<string, Record<string, unknown>>,
): Promise<ProcessorInstance> {
    const dir = transpiledDir(kind);
    const { instantiate, cores } = await compileTranspiled(kind);
    // Synchronous, as the sync-instantiation glue requires.
    return instantiate((name) => {
        const held = cores.get(name);
        if (held !== undefined) {
            return held;
        }
        const module = new WebAssembly.Module(readFileSync(resolve(dir, name)));
        cores.set(name, module);
        return module;
    }, imports);
}
