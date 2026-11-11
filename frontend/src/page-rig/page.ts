// Bringing a rig page up the page's way: the stand-in browser, then the
// production `surface.ts`, then its own `bootstrap` with the manifest a served
// page carries, against the production kernel bundle and the staged kind.
//
// The kernel is one wasm instance with page-lifetime state and no shutdown, so
// a page belongs to a test file: `openPage` refuses a second call in one worker,
// and vitest gives each file its own.

import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { REPO_ROOT } from "../test-helpers/transpiled-processor.js";
import { Failures, type ParkedTimer, type RigTimers, installGlobals, tick } from "./globals.js";
import { type Kit, type KitPage, Peer } from "./peer.js";

const STATIC_ROOT = resolve(REPO_ROOT, "surface/page-rig-dist");
const KIT_PATH = resolve(REPO_ROOT, "surface/test-fixtures/peer/page_rig_kit.json");

/** One report the fixture kind published, attributed to its instance. */
export interface Report {
    instance: string;
    windows: {
        port: string;
        new_from: number;
        dropped: number;
        /** Hex of the UTF-8 bytes of each envelope the guest was handed. */
        envelopes: string[];
    }[];
    deferred: {
        port: string;
        entries: { index: number; hex: string; deliver_after: number }[];
    }[];
    results: string[];
    sync: string | null;
}

/** Parse one report body, naming the instance and the body when it is not JSON. */
function parseReport(instance: string, body: string): Report {
    let parsed: Omit<Report, "instance">;
    try {
        parsed = JSON.parse(body) as Omit<Report, "instance">;
    } catch (err) {
        throw new Error(
            `'${instance}' published a report that is not JSON (${String(err)}): ` +
                JSON.stringify(body.slice(0, 200)),
        );
    }
    return { instance, ...parsed };
}

/**
 * The one report `instance` produced among `reports`, failing when it produced
 * none or several and naming every instance that did report.
 */
export function only(reports: Report[], instance: string): Report {
    const mine = reports.filter((r) => r.instance === instance);
    if (mine.length !== 1) {
        throw new Error(
            `expected one report from '${instance}', got ${mine.length}; ` +
                `reports came from [${reports.map((r) => `'${r.instance}'`).join(", ")}]`,
        );
    }
    return mine[0] as Report;
}

/** One instance's row in the kernel's status document. */
export interface StatusRow {
    instance: string;
    state: string;
    reason: string | null;
}

/** A page brought up by `openPage`. */
export interface Page {
    readonly kit: Kit;
    readonly facts: KitPage;
    /** Deliver `lines` as one command on `instance`'s `in` channel, settle, and return the reports produced. */
    command(instance: string, ...lines: string[]): Promise<Report[]>;
    /** Deliver `body` on `channel`, settle, and return the reports produced. */
    deliver(channel: string, body: string): Promise<Report[]>;
    /** Every report `instance` has published, oldest first. */
    reports(instance: string): Report[];
    /** The rows of the newest status document the kernel published. */
    status(): StatusRow[];
    /** Wait until the page is quiescent; rethrow the first recorded failure. */
    settle(): Promise<void>;
    /** Every timer the page armed with a delay, none of which fires. */
    parkedTimers(): ParkedTimer[];
    /** Rethrow the first failure recorded since bring-up. */
    finish(): void;
}

/** The kit, read from the staged tree, failing naming the target that stages it. */
function readKit(): Kit {
    for (const path of [KIT_PATH, resolve(STATIC_ROOT, "brenn_surface_kernel.js")]) {
        if (!existsSync(path)) {
            throw new Error(
                `${path} is missing — the page rig runs only as ` +
                    "`bazel test //frontend:vitest_page_rig`, which stages it",
            );
        }
    }
    return JSON.parse(readFileSync(KIT_PATH, "utf8")) as Kit;
}

/** The fields of a staged kind's `manifest.json` the page manifest is built from. */
interface KindRecord {
    kind: string;
    files: string[];
}

/** One staged kind's `manifest.json`. */
function kindRecord(kind: string): KindRecord {
    const path = resolve(STATIC_ROOT, "processor", kind, "manifest.json");
    const record = JSON.parse(readFileSync(path, "utf8")) as KindRecord;
    if (record.kind !== kind || !Array.isArray(record.files)) {
        throw new Error(`${path} is not the staged record of kind '${kind}'`);
    }
    return record;
}

/**
 * The page manifest in the shape the backend serves it: the kernel URL, and per
 * instance its kind's module and every core the transpile emitted other than
 * the component artifact itself, each stamped with the build id.
 */
function pageManifest(page: KitPage): string {
    const stamp = `?v=${page.build_id}`;
    const records = new Map<string, KindRecord>();
    const components = page.instances.map(({ instance, kind }) => {
        let record = records.get(kind);
        if (record === undefined) {
            record = kindRecord(kind);
            records.set(kind, record);
        }
        const dir = `/surface-static/processor/${kind}/`;
        return {
            instance,
            kind,
            module: `${dir}${kind}.js${stamp}`,
            cores: record.files
                .filter((f) => f.endsWith(".wasm") && f !== `${kind}.component.wasm`)
                .map((f) => `${dir}${f}${stamp}`),
        };
    });
    return JSON.stringify({
        kernel: `/surface-static/brenn_surface_kernel.js${stamp}`,
        components,
        withheld: [],
    });
}

/** Import a `/surface-static/…` URL from the staged tree, natively. */
function importStatic(url: string): Promise<unknown> {
    const [path, query] = url.split("?");
    if (path === undefined || !path.startsWith("/surface-static/") || query === undefined) {
        throw new Error(`the page imported an unmapped URL ${url}`);
    }
    const file = resolve(STATIC_ROOT, path.slice("/surface-static/".length));
    return import(/* @vite-ignore */ pathToFileURL(file).href);
}

/** How long bring-up may take before the instances still unreported are named. */
const BRING_UP_DEADLINE_MS = 4_000;

let opened = false;

/**
 * Bring up the kit's page `name`. Call inside a test, so that a failure during
 * bring-up — a kernel panic, a refused registration, a `surface.ts` failure
 * path — lands in that test's `console.error` gate.
 */
export async function openPage(name: string): Promise<Page> {
    if (opened) {
        throw new Error("openPage was already called in this file; a page belongs to one file");
    }
    opened = true;
    const kit = readKit();
    const facts = kit.pages[name];
    if (facts === undefined) {
        throw new Error(`the kit has no page '${name}'`);
    }
    const failures = new Failures();
    let timers: RigTimers | null = null;
    const peer = new Peer(kit, facts, failures, () => {
        if (timers === null) {
            throw new Error("the peer ran before the stand-in timers were installed");
        }
        return timers;
    });
    const installed = installGlobals(
        {
            slug: facts.slug,
            buildId: facts.build_id,
            configChannel: facts.config_channel,
            location: facts.location,
        },
        STATIC_ROOT,
        failures,
        peer.WebSocket,
    );
    timers = installed.timers;
    const rigTimers = installed.timers;

    // After the import: with the manifest present at evaluation, the module's
    // own guarded `bootstrap()` would run a second time with the default importer.
    const surface = await import("../surface.js");
    const script = installed.document.createElement("script");
    script.id = "brenn-surface-manifest";
    script.setAttribute("type", "application/json");
    script.textContent = pageManifest(facts);
    installed.document.head.appendChild(script);

    await surface.bootstrap(importStatic);

    const reportsOf = (instance: string): Report[] =>
        peer.published
            .filter((p) => p.channel === facts.report_channel && p.attribution === instance)
            .map((p) => parseReport(instance, p.body));
    const reportsSince = (order: number): Report[] =>
        peer.published
            .filter((p) => p.order >= order && p.channel === facts.report_channel)
            .map((p) => {
                if (p.attribution === null) {
                    throw new Error(`an unattributed report reached the peer: ${p.body}`);
                }
                return parseReport(p.attribution, p.body);
            });

    const latestStatus = (): StatusRow[] | null => {
        const all = peer.published.filter((p) => p.channel === facts.status_channel);
        const latest = all[all.length - 1];
        return latest === undefined
            ? null
            : (JSON.parse(latest.body) as { instances: StatusRow[] }).instances;
    };

    // Bring-up is the one stretch of real asynchrony — dynamic import, streaming
    // compile — so it is waited out a macrotask at a time until every instance
    // has published its registration report, never by sleeping. An instance the
    // kernel marked failed ends the wait naming it; one that neither reports nor
    // fails ends it at the deadline, which sits inside vitest's default test
    // timeout so the named failure is the one reported.
    const deadline = performance.now() + BRING_UP_DEADLINE_MS;
    for (;;) {
        failures.rethrow();
        const failed = (latestStatus() ?? []).filter((row) => row.state === "failed");
        if (failed.length > 0) {
            throw new Error(
                "bring-up: " +
                    failed
                        .map((row) => `'${row.instance}' failed (${String(row.reason)})`)
                        .join(", "),
            );
        }
        const pending = facts.instances
            .map(({ instance }) => instance)
            .filter((instance) => reportsOf(instance).length === 0);
        if (pending.length === 0) {
            break;
        }
        if (performance.now() > deadline) {
            throw new Error(
                `bring-up: no registration report after ${BRING_UP_DEADLINE_MS} ms from ` +
                    pending.map((instance) => `'${instance}'`).join(", "),
            );
        }
        await tick();
    }
    await rigTimers.settle();

    const nextOrder = (): number => peer.published.length;
    const page: Page = {
        kit,
        facts,
        async command(instance, ...lines) {
            const entry = facts.instances.find((i) => i.instance === instance);
            if (entry === undefined) {
                throw new Error(`page '${name}' has no instance '${instance}'`);
            }
            return page.deliver(entry.command_channel, lines.join("\n"));
        },
        async deliver(channel, body) {
            const from = nextOrder();
            peer.deliver(channel, body);
            await rigTimers.settle();
            return reportsSince(from);
        },
        reports: reportsOf,
        status() {
            const rows = latestStatus();
            if (rows === null) {
                throw new Error("the kernel has published no status document");
            }
            return rows;
        },
        settle: () => rigTimers.settle(),
        parkedTimers: () => rigTimers.parked(),
        finish: () => failures.rethrow(),
    };
    return page;
}
