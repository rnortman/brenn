import { defineConfig } from "vitest/config";

// Mirrors the Makefile's production esbuild --define so
// `build-info.ts` resolves to `BUILD_ID === "test-build"` in tests.
export default defineConfig({
    define: {
        "globalThis.__BRENN_BUILD_ID__": '"test-build"',
    },
    test: {
        environment: "happy-dom",
        setupFiles: ["fake-indexeddb/auto", "./src/test-setup.ts"],
        server: {
            deps: {
                // The jco-transpiled processor trees under surface/dist are build
                // output living outside this project root, and the transplant
                // parity test must load them exactly as a browser would — plain
                // ESM, untransformed. Externalizing hands them to node's own
                // loader instead of vite's resolver, which cannot reach outside
                // the root anyway.
                external: [/[\\/]surface[\\/]dist[\\/]processor[\\/]/],
            },
        },
    },
});
