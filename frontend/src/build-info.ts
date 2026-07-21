/**
 * Build identifier shared with the Rust backend for the stale-tab
 * force-refresh handshake.
 *
 * Substituted at bundle time via esbuild's `--define:globalThis.__BRENN_BUILD_ID__`
 * (Makefile in production, vitest.config.ts in tests). The `globalThis`-
 * scoped form lets a single `define` work for both the production
 * bundler and vitest.
 */
const raw = globalThis.__BRENN_BUILD_ID__;
if (typeof raw !== "string" || raw.length === 0) {
    throw new Error(
        "__BRENN_BUILD_ID__ is not defined - esbuild --define or " +
            "vitest.config define is missing",
    );
}
export const BUILD_ID: string = raw;
