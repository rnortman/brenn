/**
 * url-util.ts — shared URL parsing utilities.
 *
 * No DOM or SW globals required; importable from both app-shell and SW contexts.
 */

/**
 * Parse `url` and verify it is same-origin relative to `origin`.
 * Returns the parsed URL on success, null if the URL is malformed or
 * cross-origin.
 */
export function parseSameOriginUrl(url: string, origin: string): URL | null {
    let parsed: URL;
    try {
        parsed = new URL(url, origin);
    } catch {
        return null;
    }
    if (parsed.origin !== origin) {
        return null;
    }
    return parsed;
}
