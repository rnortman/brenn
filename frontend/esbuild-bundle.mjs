/**
 * Bundler entry point for the Bazel lane: esbuild's JS API over one entry
 * point, with the build id taken from Bazel's stable status file.
 *
 * The make lane injects the build id from its own environment. An environment
 * variable read by a Bazel action is part of that action's cache key, so a
 * per-build value there would bust the cache on exactly the builds that most
 * need it; the stamp file is the mechanism that keeps it out of dev keys.
 */

import { readFileSync, realpathSync, writeFileSync } from "node:fs";
import { basename, resolve } from "node:path";
import { build } from "esbuild";
import { DEFINE_NAME, parseArgs, resolveBuildId, treeRootOf } from "./esbuild-bundle-opts.mjs";

const opts = parseArgs(process.argv.slice(2));

const define = {};
if (opts.buildId) {
    define[DEFINE_NAME] = JSON.stringify(
        resolveBuildId(process.env, (path) => readFileSync(path, "utf8"), opts.requireStamp),
    );
}

// Every module name esbuild writes into the bundle — the banner comments and
// the source map's `sources` — is a path from the output file to the module's
// resolved location. Under Bazel that location is in the output base while the
// output file is in a sandbox whose path carries a per-action segment, so
// letting esbuild write where Bazel wants the bytes would bake a path of
// machine- and run-dependent depth into a shipped artifact. Instead it bundles
// to a notional path inside the resolved source tree, exactly where the make
// lane's output sits, and the bytes are written to Bazel's path afterwards.
const root = treeRootOf(realpathSync(resolve(opts.root, opts.entry)), opts.entry);
const outfile = resolve(opts.outfile);
const notional = `${root}/dist/${basename(outfile)}`;

const result = await build({
    absWorkingDir: root,
    entryPoints: [opts.entry],
    outfile: notional,
    write: false,
    bundle: true,
    format: "esm",
    sourcemap: opts.sourcemap,
    define,
    logLevel: "warning",
});

const expected = new Set([notional, opts.sourcemap ? `${notional}.map` : null].filter(Boolean));
const produced = new Set(result.outputFiles.map((f) => f.path));
if (expected.size !== produced.size || [...expected].some((p) => !produced.has(p))) {
    throw new Error(
        `esbuild produced ${[...produced].join(", ")}, expected ${[...expected].join(", ")}`,
    );
}
for (const file of result.outputFiles) {
    writeFileSync(file.path === notional ? outfile : `${outfile}.map`, file.contents);
}
