/**
 * IndexedDB helper for the PWA push `signed_in_user_ids` set.
 *
 * The set holds every user id known to be signed in on any active tab in
 * this browser profile.  Both the main app (DOM context) and the service
 * worker read/write it; each context opens its own short-lived handle.
 *
 * Store layout
 *   DB name: "brenn-session"
 *   Object store: "signed_in_user_ids"
 *   Single record: key = "signed_in_user_ids", value = number[]
 *
 * Design §2.6.3: the SW checks `payload.user_id ∈ stored_set` before
 * showing a notification.  The main app inserts on Welcome and removes on
 * logout / WS close (with a 5-second grace period).
 */

/** Name of the IndexedDB database shared between main thread and SW. */
export const SESSION_DB_NAME = "brenn-session";
const SESSION_DB_VERSION = 1;
const SESSION_STORE = "signed_in_user_ids";
const SESSION_KEY = "signed_in_user_ids";

export function openSessionDb(): Promise<IDBDatabase> {
    return new Promise((resolve, reject) => {
        const req = indexedDB.open(SESSION_DB_NAME, SESSION_DB_VERSION);
        req.onupgradeneeded = () => {
            req.result.createObjectStore(SESSION_STORE);
        };
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
    });
}

/** Read the current set.  Returns an empty array if absent or on error. */
export async function readSignedInUserIds(): Promise<number[]> {
    let db: IDBDatabase;
    try {
        db = await openSessionDb();
    } catch {
        return [];
    }
    return new Promise<number[]>((resolve) => {
        const tx = db.transaction(SESSION_STORE, "readonly");
        const req = tx.objectStore(SESSION_STORE).get(SESSION_KEY);
        req.onsuccess = () => {
            db.close();
            const val = req.result as number[] | undefined;
            resolve(Array.isArray(val) ? val : []);
        };
        req.onerror = () => {
            db.close();
            resolve([]);
        };
    });
}

/**
 * Open the session DB and apply `mutate` to the stored `number[]` in a single
 * readwrite transaction.  Resolves on commit; rejects on open-failure or
 * transaction error.
 */
async function modifySignedInUserIds(mutate: (ids: number[]) => number[]): Promise<void> {
    const db = await openSessionDb();
    await new Promise<void>((resolve, reject) => {
        const tx = db.transaction(SESSION_STORE, "readwrite");
        const store = tx.objectStore(SESSION_STORE);
        const req = store.get(SESSION_KEY);
        req.onsuccess = () => {
            const val = req.result as number[] | undefined;
            const next = mutate(Array.isArray(val) ? val : []);
            store.put(next, SESSION_KEY);
        };
        tx.oncomplete = () => { db.close(); resolve(); };
        tx.onerror = () => { db.close(); reject(tx.error); };
    });
}

/** Add `userId` to the set (idempotent). Single readwrite transaction. */
export function addSignedInUserId(userId: number): Promise<void> {
    return modifySignedInUserIds((ids) => {
        if (!ids.includes(userId)) {
            ids.push(userId);
        }
        return ids;
    });
}

/** Remove `userId` from the set (idempotent). Single readwrite transaction. */
export function removeSignedInUserId(userId: number): Promise<void> {
    return modifySignedInUserIds((ids) => ids.filter((id) => id !== userId));
}
