/** Shared IndexedDB helper for the share target flow.
 *
 * Used by both sw.ts (stash) and app.ts (pickup). esbuild bundles this
 * into both output files — duplication is intentional (~20 lines).
 * indexedDB is available in both WebWorker and DOM lib typings.
 */

/** Shape of a pending share stashed in IDB by the service worker. */
export interface ShareData {
  id: string;
  title: string | null;
  text: string | null;
  url: string | null;
  file: { name: string; type: string; data: ArrayBuffer } | null;
}

const DB_NAME = "brenn-share";
const DB_VERSION = 1;
const STORE_NAME = "pending";

export function openShareDb(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = () => {
      req.result.createObjectStore(STORE_NAME, { keyPath: "id" });
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}
