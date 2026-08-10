import { defineConfig } from "vitest/config";

// Mirrors the Makefile's production esbuild --define so
// `build-info.ts` resolves to `BUILD_ID === "test-build"` in tests.
export default defineConfig({
    define: {
        "globalThis.__BRENN_BUILD_ID__": '"test-build"',
    },
    server: {
        fs: {
            // The Bazel lane runs this suite out of a runfiles tree, where every
            // source is a symlink into the build output and so resolves outside
            // the project root, which the root check refuses to load. Relaxed
            // only there: `JS_BINARY__EXECROOT` is set by the rules_js launcher
            // and by nothing else. Everywhere else the check stays on, because
            // this same config governs `vitest --ui`, which does start an HTTP
            // server whose `/@fs/` endpoint would then serve any path on disk.
            strict: process.env.JS_BINARY__EXECROOT === undefined,
        },
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
