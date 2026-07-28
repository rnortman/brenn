// The transplant test, surface half.
//
// The same artifact the wasmtime half drives (`brenn-wasm/tests/
// processor_transplant.rs`), jco-transpiled, driven through the same scripted
// activation sequence in `transplant.json` and reduced to that file's canonical
// transcript. Equality of the two transcripts is the executable form of the
// invariant: any component runs on any host that can satisfy its imports, and
// the component cannot tell which host it got.
//
// This half hosts the *real* transpiled tree — the emitted glue and its core
// wasm modules, instantiated through the `--instantiation async` entry point the
// bootstrap loader uses — and lifts each activation through the production
// `activationEntry` from `surface.ts`, the same function the kernel registers.
// So the `deferred` windows and `now` a scripted activation names cross into the
// guest by the code path a real page uses, field renaming and `bigint` widening
// included; a lift that lost either would fail here. What the harness still
// supplies itself is the surrounding contract the kernel supplies in the
// browser: the four surface imports, and buffer-during-receive /
// flush-iff-ok. The kernel's own implementation of that contract is pinned
// separately (`surface/kernel/src/logic.rs` core tests and the loader cases in
// `surface.test.ts`); what is under test here is the guest on the transpiled
// hosting, which is the half the wasmtime run cannot reach.
//
// Wire class: the script is `brenn:`-bound throughout. That is an owner scoping
// decision, not doctrine — backend WASM consumers cannot bind `ephemeral:`
// channels yet (a registry fork, never a decision), and closing that gap is its
// own design and implementation effort. The `ephemeral:`-bound variant of this
// fixture is that effort's standing obligation and extends this criterion with
// no further ratification. Nothing in this harness is class-aware, so the
// deferral costs the criterion nothing beyond coverage of the backend hosting.
//
// TODO(processor-transplant-browser-engine): this harness resolves the
// transpiled tree by filesystem path — it dynamic-imports a `file://` URL and
// reads the core wasm bytes with `readFileSync` — so the guest runs under
// node's WebAssembly engine, not a browser one.

import { readFileSync, existsSync } from "node:fs";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { beforeAll, describe, expect, it } from "vitest";
import {
    activationEntry,
    type KernelActivation,
    type ProcessorInstance,
} from "./surface.js";

// vitest runs with its config root (`frontend/`) as cwd, so the repo root is one
// level up. Both inputs are build/source artifacts outside the frontend tree:
// the transpiled output the surface deploys, and the fixture's shared script.
const REPO_ROOT = resolve(process.cwd(), "..");
const DIST = resolve(REPO_ROOT, "surface/dist/processor/processor-transplant");
const SCRIPT = resolve(
    REPO_ROOT,
    "brenn-wasm/components/processor-transplant/transplant.json",
);

/** One port's window as the script states it, before envelope expansion. */
interface ScriptPort {
    port: string;
    envelopes: { id: string; body: string }[];
    new_from: number;
    dropped: number;
}

/**
 * One scripted activation: the kernel's own field spelling for everything except
 * the input envelopes, which the script names by identity and each harness
 * expands.
 */
interface ScriptActivation {
    ports: ScriptPort[];
    deferred: KernelActivation["deferred"];
    now: number | null;
}

/** One deferred publish as the transcript reduces it. */
interface DeferredPublish {
    body: string;
    deliver_after: number;
}

/** One buffered control op as the transcript reduces it. */
interface TranscriptOp {
    op: "cancel" | "edit";
    port: string;
    index: number;
    payload?: string | null;
    deliver_after?: number | null;
}

interface TranscriptEntry {
    outcome: "ok" | "err" | "trap";
    publishes: string[];
    deferred_publishes: DeferredPublish[];
    ops: TranscriptOp[];
}

interface Script {
    envelope_template: Record<string, unknown>;
    config: Record<string, string>;
    activations: ScriptActivation[];
    transcript: TranscriptEntry[];
}

/** The transpiled module's `--instantiation async` entry point. */
type Instantiate = (
    getCoreModule: (name: string) => Promise<WebAssembly.Module>,
    imports: Record<string, Record<string, unknown>>,
) => Promise<ProcessorInstance>;

const script: Script = JSON.parse(readFileSync(SCRIPT, "utf8"));

/**
 * Own-property test. The same form the transpiled glue's own `getErrorPayload`
 * uses, and deliberately not `Object.hasOwn` — this project's lib target
 * predates it, and widening the target for a test would be the tail wagging the
 * dog.
 */
function hasOwn(target: object, key: string): boolean {
    return Object.prototype.hasOwnProperty.call(target, key);
}

/**
 * Expand an `(id, body)` pair into the canonical `MessageEnvelope` JSON using
 * the script's fixed non-identifying fields. The wasmtime half performs the
 * identical expansion — the script names the identity, both harnesses supply
 * the same frame around it.
 */
function envelope(pair: { id: string; body: string }): string {
    return JSON.stringify({
        ...script.envelope_template,
        message_id: pair.id,
        body: pair.body,
    });
}

/**
 * Everything one activation buffered, in call order per class. Reset before each
 * activation and reported only on ok: keeping it iff `receive` returns ok is the
 * flush-on-ok / discard-on-err contract reduced to what a transcript can see,
 * and it covers deferral — a parked schedule and a control op are discarded with
 * the publishes.
 */
interface Buffer {
    publishes: string[];
    deferredPublishes: DeferredPublish[];
    ops: TranscriptOp[];
}

function emptyBuffer(): Buffer {
    return { publishes: [], deferredPublishes: [], ops: [] };
}

/**
 * Drive the whole script against one instance and return the transcript.
 *
 * Err vs trap is discriminated by the production `activationEntry`, not here: it
 * answers in the kernel's vocabulary — `undefined` for ok, a diagnostic string
 * for the err arm, a rethrow for a trap — so this harness reads the outcome off
 * the same seam the page reads it off.
 */
async function runScript(): Promise<TranscriptEntry[]> {
    const { instantiate } = (await import(
        /* @vite-ignore */ pathToFileURL(
            resolve(DIST, "processor-transplant.js"),
        ).href
    )) as { instantiate: Instantiate };

    let buffer = emptyBuffer();
    const instance = await instantiate(
        (name) => WebAssembly.compile(readFileSync(resolve(DIST, name))),
        {
            "brenn:processor/config": {
                get: (key: string): string | undefined =>
                    hasOwn(script.config, key) ? script.config[key] : undefined,
            },
            // The log plane has no class-blind canonical form (tracing
            // backend-side, the kernel component-log plane browser-side), so it
            // is absent from the transcript. The import must still be satisfied:
            // an unsatisfiable `log` would fail instantiation outright.
            "brenn:processor/log": { log: () => {} },
            "brenn:processor/ports": {
                publish: (_port: string, payload: string) => {
                    buffer.publishes.push(payload);
                },
                publishDeferred: (
                    _port: string,
                    payload: string,
                    deliverAfter: bigint,
                ) => {
                    buffer.deferredPublishes.push({
                        body: payload,
                        deliver_after: Number(deliverAfter),
                    });
                },
                deferCancel: (port: string, index: number) => {
                    buffer.ops.push({ op: "cancel", port, index });
                },
                deferEdit: (
                    port: string,
                    index: number,
                    payload: string | undefined,
                    deliverAfter: bigint | undefined,
                ) => {
                    buffer.ops.push({
                        op: "edit",
                        port,
                        index,
                        // `undefined` is the absent half; the transcript spells
                        // absence `null`, which is what JSON can carry.
                        payload: payload === undefined ? null : payload,
                        deliver_after:
                            deliverAfter === undefined
                                ? null
                                : Number(deliverAfter),
                    });
                },
            },
        },
    );
    const entry = activationEntry(instance);

    return script.activations.map((activation) => {
        buffer = emptyBuffer();
        const record: KernelActivation = {
            ports: activation.ports.map((p) => ({
                port: p.port,
                envelopes: p.envelopes.map(envelope),
                new_from: p.new_from,
                dropped: p.dropped,
            })),
            deferred: activation.deferred,
            now: activation.now,
        };
        let outcome: TranscriptEntry["outcome"];
        try {
            outcome = entry(JSON.stringify(record)) === undefined ? "ok" : "err";
        } catch {
            outcome = "trap";
        }
        if (outcome !== "ok") {
            return {
                outcome,
                publishes: [],
                deferred_publishes: [],
                ops: [],
            };
        }
        return {
            outcome,
            publishes: buffer.publishes,
            deferred_publishes: buffer.deferredPublishes,
            ops: buffer.ops,
        };
    });
}

describe("processor transplant — surface hosting", () => {
    let transcript: TranscriptEntry[];

    beforeAll(async () => {
        // Mirrors the wasmtime half's posture on a missing artifact: fail with
        // the command that fixes it, never skip. A silently skipped parity test
        // asserts the invariant on one host while reporting green.
        if (!existsSync(resolve(DIST, "processor-transplant.js"))) {
            throw new Error(
                `the transpiled processor-transplant tree is missing at ${DIST} — ` +
                    "build it with `make surface-transpile`",
            );
        }
        transcript = await runScript();
    });

    it("produces the canonical transcript", () => {
        expect(transcript).toEqual(script.transcript);
    });

    it("survives err and dies on trap", () => {
        // The transcript's shape is itself the contract for err vs trap: an err
        // activation flushes nothing yet is followed by an ok activation, and the
        // trap activation flushes nothing and is last. Asserted separately from
        // the equality above so a regenerated transcript cannot quietly lose it —
        // the same independent pin the wasmtime half carries.
        // Properties, not the literal sequence: extending the fixture with a
        // legitimate activation must not break this pin, or the cheapest repair
        // is to paste the new sequence back in — which re-derives the assertion
        // from the fixture and dissolves the independence.
        const outcomes = transcript.map((e) => e.outcome);
        const errAt = outcomes.indexOf("err");
        expect(errAt).toBeGreaterThanOrEqual(0);
        expect(outcomes.indexOf("trap")).toBe(outcomes.length - 1);
        expect(outcomes.slice(errAt + 1)).toContain("ok");
        transcript.forEach((entry, i) => {
            // Every buffered class is discarded together: publishes, deferred
            // schedules, and control ops.
            const flushed =
                entry.publishes.length +
                entry.deferred_publishes.length +
                entry.ops.length;
            if (outcomes[i] === "ok") {
                expect(flushed).toBeGreaterThan(0);
            } else {
                expect(flushed).toBe(0);
            }
        });
    });

    it("carries the deferred windows and the clock into the guest", () => {
        // The lift is the production one, so this is the pin on `deferred`/`now`
        // reaching a real guest: the script's clock and parked-message windows
        // are only observable in the transcript because the fixture read them
        // back out of the activation it was handed. Derived from the script, so
        // it follows the fixture rather than freezing a literal.
        const scripted = script.activations.map((a) => ({
            deferred: a.deferred,
            now: a.now,
        }));
        const reported = transcript.map((entry, i) => {
            if (entry.outcome !== "ok") {
                return scripted[i];
            }
            const summary = JSON.parse(entry.publishes[0]) as {
                deferred: KernelActivation["deferred"];
                now: number | null;
            };
            return { deferred: summary.deferred, now: summary.now };
        });
        expect(reported).toEqual(scripted);
        // At least one activation carries a clock and at least one carries none:
        // a script that lost either case would make the pin vacuous.
        expect(scripted.some((a) => a.now !== null)).toBe(true);
        expect(scripted.some((a) => a.now === null)).toBe(true);
    });
});
