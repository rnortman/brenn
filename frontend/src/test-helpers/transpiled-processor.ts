// The loading must be identical across suites for their claims to be about the
// same hosting: the same `--instantiation async` entry point, core modules read
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

/** The transpiled module's `--instantiation async` entry point. */
type Instantiate = (
    getCoreModule: (name: string) => Promise<WebAssembly.Module>,
    imports: Record<string, Record<string, unknown>>,
) => Promise<ProcessorInstance>;

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

/** Instantiate the transpiled guest of `kind` against `imports`. */
export async function instantiateTranspiled(
    kind: string,
    imports: Record<string, Record<string, unknown>>,
): Promise<ProcessorInstance> {
    const dir = transpiledDir(kind);
    const { instantiate } = (await import(
        /* @vite-ignore */ pathToFileURL(resolve(dir, `${kind}.js`)).href
    )) as { instantiate: Instantiate };
    return instantiate(
        (name) => WebAssembly.compile(readFileSync(resolve(dir, name))),
        imports,
    );
}
