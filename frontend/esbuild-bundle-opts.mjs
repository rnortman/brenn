/**
 * Argument parsing and build-id resolution for the frontend bundler.
 *
 * Kept apart from the bundler itself so it can be exercised without esbuild:
 * everything here is pure but for the file reader handed to it.
 */

/** The stable status key carrying the build id. */
export const STAMP_KEY = "STABLE_BRENN_BUILD_ID";

/** The global the bundle substitutes; `build-info.ts` reads it. */
export const DEFINE_NAME = "globalThis.__BRENN_BUILD_ID__";

/**
 * What an unstamped build bakes in: the key in braces, byte for byte what the
 * unstamped Rust binary reports, so a dev browser and a dev backend agree and
 * the stale-tab handshake does not fire against itself.
 */
export const UNSTAMPED_BUILD_ID = `{${STAMP_KEY}}`;

/** The value of `key` in a Bazel status file, or null if the file has no such line. */
export function stampValue(statusText, key) {
    for (const line of statusText.split("\n")) {
        const sep = line.indexOf(" ");
        if (sep === -1) {
            continue;
        }
        if (line.slice(0, sep) === key) {
            return line.slice(sep + 1).trim();
        }
    }
    return null;
}

/**
 * Where the stable status file is, or null on an unstamped build.
 *
 * Bazel names it relative to the execroot while the tool runs in the bin
 * directory, so the two have to be put back together.
 */
export function statusFilePath(env) {
    const statusFile = env.BAZEL_STABLE_STATUS_FILE;
    if (!statusFile) {
        return null;
    }
    if (statusFile.startsWith("/")) {
        return statusFile;
    }
    const execroot = env.JS_BINARY__EXECROOT;
    if (!execroot) {
        throw new Error(
            `${statusFile} is execroot-relative and JS_BINARY__EXECROOT is unset, so the ` +
                "stamped build id cannot be read",
        );
    }
    return `${execroot}/${statusFile}`;
}

/**
 * The build id this bundle bakes in.
 *
 * Bazel exports the status file path only on stamped builds, so its absence is
 * the dev case and takes the placeholder — unless `requireStamp` says the build
 * is stamped, in which case the absence is the environment contract having
 * changed underneath us and the placeholder would ship in a release. Presence
 * without the key is a stamped release whose workspace status broke. Both fail.
 */
export function resolveBuildId(env, readFile, requireStamp = false) {
    const statusFile = statusFilePath(env);
    if (!statusFile) {
        if (requireStamp) {
            throw new Error(
                "this is a stamped build but no status file is named in the environment, so " +
                    "the bundle would bake in the unstamped placeholder",
            );
        }
        return UNSTAMPED_BUILD_ID;
    }
    const value = stampValue(readFile(statusFile), STAMP_KEY);
    if (value === null || value === "") {
        throw new Error(
            `${statusFile} carries no ${STAMP_KEY}: a stamped build whose ` +
                "workspace status lost the key would ship an unidentifiable bundle",
        );
    }
    return value;
}

/**
 * The directory `entry` sits under, given the entry's resolved path.
 *
 * Bazel stages a generated tree as a real directory of symlinks to the output
 * base, so the tree's own path and the path its files resolve to are different
 * directories. esbuild names modules by their resolved paths, so the second is
 * the one the bundle has to be anchored on.
 */
export function treeRootOf(resolvedEntry, entry) {
    const suffix = `/${entry}`;
    if (!resolvedEntry.endsWith(suffix)) {
        throw new Error(
            `${entry} resolves to ${resolvedEntry}, which does not end in ${suffix}: ` +
                "the bundle's module names would not be relative to the source tree",
        );
    }
    return resolvedEntry.slice(0, -suffix.length);
}

/** Arguments taking a value, mapped to the option they set. */
const VALUED = { "--root": "root", "--entry": "entry", "--outfile": "outfile" };

/**
 * Parse the bundler's argv tail.
 *
 * `--root`, `--entry` and `--outfile` are required; `--sourcemap`,
 * `--build-id` and `--require-stamp` are flags. An unrecognised argument is an
 * error rather than something ignored, because a silently dropped flag is a
 * bundle built with the wrong options.
 */
export function parseArgs(argv) {
    const opts = {
        root: null,
        entry: null,
        outfile: null,
        sourcemap: false,
        buildId: false,
        requireStamp: false,
    };
    for (let i = 0; i < argv.length; i++) {
        const arg = argv[i];
        if (arg === "--sourcemap") {
            opts.sourcemap = true;
        } else if (arg === "--build-id") {
            opts.buildId = true;
        } else if (arg === "--require-stamp") {
            opts.requireStamp = true;
        } else if (arg in VALUED) {
            const value = argv[i + 1];
            if (value === undefined) {
                throw new Error(`${arg} takes a value`);
            }
            opts[VALUED[arg]] = value;
            i++;
        } else {
            throw new Error(`unrecognised argument ${arg}`);
        }
    }
    for (const required of Object.values(VALUED)) {
        if (opts[required] === null) {
            throw new Error(`--${required} is required`);
        }
    }
    return opts;
}
