import { describe, expect, it } from "vitest";
import {
    DEFINE_NAME,
    parseArgs,
    resolveBuildId,
    STAMP_KEY,
    statusFilePath,
    stampValue,
    treeRootOf,
    UNSTAMPED_BUILD_ID,
} from "./esbuild-bundle-opts.mjs";

/** A reader that fails if the caller reaches for a path it was not given. */
function reader(contents) {
    return (path) => {
        if (!(path in contents)) {
            throw new Error(`unexpected read of ${path}`);
        }
        return contents[path];
    };
}

describe("stampValue", () => {
    it("reads the value after the first space", () => {
        expect(stampValue(`${STAMP_KEY} v1.2.3\n`, STAMP_KEY)).toBe("v1.2.3");
    });

    it("does not confuse a key that is a prefix of another", () => {
        const text = `${STAMP_KEY}_EXTRA nope\n${STAMP_KEY} yes\n`;
        expect(stampValue(text, STAMP_KEY)).toBe("yes");
    });

    it("returns null when the key is absent", () => {
        expect(stampValue("OTHER_KEY 1\n", STAMP_KEY)).toBeNull();
    });
});

describe("statusFilePath", () => {
    it("is null when the build is unstamped", () => {
        expect(statusFilePath({})).toBeNull();
    });

    it("joins an execroot-relative path onto the execroot", () => {
        expect(
            statusFilePath({
                BAZEL_STABLE_STATUS_FILE: "bazel-out/stable-status.txt",
                JS_BINARY__EXECROOT: "/x/execroot/_main",
            }),
        ).toBe("/x/execroot/_main/bazel-out/stable-status.txt");
    });

    it("leaves an absolute path alone", () => {
        expect(statusFilePath({ BAZEL_STABLE_STATUS_FILE: "/abs/status.txt" })).toBe(
            "/abs/status.txt",
        );
    });

    it("fails on a relative path with no execroot rather than reading the wrong file", () => {
        expect(() => statusFilePath({ BAZEL_STABLE_STATUS_FILE: "s.txt" })).toThrow("EXECROOT");
    });
});

describe("resolveBuildId", () => {
    const env = { BAZEL_STABLE_STATUS_FILE: "/status.txt" };

    it("takes the placeholder when the build is unstamped", () => {
        expect(resolveBuildId({}, reader({}))).toBe(UNSTAMPED_BUILD_ID);
        // The placeholder is what the unstamped Rust binary reports, so the two
        // halves of the handshake still agree in a dev build.
        expect(UNSTAMPED_BUILD_ID).toBe(`{${STAMP_KEY}}`);
    });

    it("takes the stamped value when the status file carries the key", () => {
        const files = { "/status.txt": `${STAMP_KEY} abc1234\n` };
        expect(resolveBuildId(env, reader(files))).toBe("abc1234");
    });

    it("fails a stamped build whose status file lost the key", () => {
        const files = { "/status.txt": "SOMETHING_ELSE 1\n" };
        expect(() => resolveBuildId(env, reader(files))).toThrow(STAMP_KEY);
    });

    it("fails a stamped build whose key is present but empty", () => {
        const files = { "/status.txt": `${STAMP_KEY} \n` };
        expect(() => resolveBuildId(env, reader(files))).toThrow(STAMP_KEY);
    });

    it("fails when the build says it is stamped and the environment does not", () => {
        // The rules_js env contract renaming out from under us looks exactly
        // like a dev build; `--require-stamp` is what tells the two apart.
        expect(() => resolveBuildId({}, reader({}), true)).toThrow("stamped build");
    });

    it("still takes the stamped value when both agree", () => {
        const files = { "/status.txt": `${STAMP_KEY} abc1234\n` };
        expect(resolveBuildId(env, reader(files), true)).toBe("abc1234");
    });
});

describe("parseArgs", () => {
    const minimal = ["--root", "tree", "--entry", "a.ts", "--outfile", "a.js"];

    it("parses the full form", () => {
        expect(parseArgs([...minimal, "--sourcemap", "--build-id", "--require-stamp"])).toEqual({
            root: "tree",
            entry: "a.ts",
            outfile: "a.js",
            sourcemap: true,
            buildId: true,
            requireStamp: true,
        });
    });

    it("defaults every flag off", () => {
        const opts = parseArgs(minimal);
        expect(opts.sourcemap).toBe(false);
        expect(opts.buildId).toBe(false);
        expect(opts.requireStamp).toBe(false);
    });

    it("rejects an unrecognised argument rather than ignoring it", () => {
        expect(() => parseArgs([...minimal, "--minify"])).toThrow("--minify");
    });

    it("rejects a missing required argument", () => {
        expect(() => parseArgs(["--root", "tree", "--entry", "a.ts"])).toThrow("--outfile");
    });

    it("rejects a value-taking argument with no value", () => {
        expect(() => parseArgs(["--root", "tree", "--entry"])).toThrow("--entry");
    });
});

describe("treeRootOf", () => {
    it("strips the entry from its resolved path", () => {
        expect(treeRootOf("/out/bin/frontend/ts_src/src/main.ts", "src/main.ts")).toBe(
            "/out/bin/frontend/ts_src",
        );
    });

    it("fails when the resolved path does not end in the entry", () => {
        expect(() => treeRootOf("/elsewhere/main.ts", "src/main.ts")).toThrow("src/main.ts");
    });
});

describe("DEFINE_NAME", () => {
    it("is the globalThis-scoped form both lanes substitute", () => {
        // `build-info.ts` reads the global under exactly this name, and
        // `vitest.config.ts` defines the same one for tests.
        expect(DEFINE_NAME.startsWith("globalThis.")).toBe(true);
    });
});
