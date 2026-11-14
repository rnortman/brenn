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
// bootstrap loader uses — and hands each activation through the production
// `activationEntryBytes` from `surface.ts`, the same function the kernel
// registers for a `brenn:processor@0.2.0` kind. The kernel builds that record
// itself in the browser; here the script is written in the same shape — the
// glue's camel-cased fields, `bigint` clocks, bodies as a content type and a
// `Uint8Array` — so the `deferred` windows and `now` a scripted activation
// names cross into the guest through the glue's own lift. What the harness
// still supplies itself is the surrounding contract the kernel supplies in the
// browser: the four surface imports, and buffer-during-receive /
// flush-iff-ok. The kernel's own implementation of that contract is pinned
// separately (`surface/kernel/src/logic.rs` core tests and the loader cases in
// `surface.test.ts`); what is under test here is the guest on the transpiled
// hosting, which is the half the wasmtime run cannot reach.
//
// The envelopes here are fabricated headers and bodies, not records the kernel
// produced. The kernel's own crossing is run by the page rig, against real
// 0.2.0 guests.
//
// Wire class: the script is `brenn:`-bound throughout. That is an owner scoping
// decision, not doctrine — backend WASM consumers cannot bind `ephemeral:`
// channels yet (a registry fork, never a decision), and closing that gap is its
// own design and implementation effort. The `ephemeral:`-bound variant of this
// fixture is that effort's standing obligation and extends this criterion with
// no further ratification. Nothing in this harness is class-aware, so the
// deferral costs the criterion nothing beyond coverage of the backend hosting.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { beforeAll, describe, expect, it } from "vitest";
import {
    activationEntryBytes,
    type Body,
    type BytesProcessorInstance,
    type KernelActivation,
} from "./surface.js";
import {
    REPO_ROOT,
    instantiateTranspiled,
    requireTranspiledTree,
} from "./test-helpers/transpiled-processor.js";

const KIND = "processor-transplant";
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

/** One immediate publish as the transcript reduces it. */
interface Publish {
    port: string;
    body: string;
}

/** One deferred publish as the transcript reduces it. */
interface DeferredPublish {
    port: string;
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
    publishes: Publish[];
    deferred_publishes: DeferredPublish[];
    ops: TranscriptOp[];
}

interface Script {
    envelope_template: Record<string, unknown>;
    config: Record<string, string>;
    activations: ScriptActivation[];
    transcript: TranscriptEntry[];
}

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

/** The glue's record for an envelope: its header JSON and its body. */
interface GlueEnvelope {
    header: string;
    body: Body;
}

/** The activation record the 0.2.0 glue lifts into the guest. */
interface GlueActivation {
    ports: {
        port: string;
        envelopes: GlueEnvelope[];
        newFrom: number;
        dropped: number;
    }[];
    deferred: {
        port: string;
        entries: { index: number; body: Body; deliverAfter: bigint }[];
    }[];
    now: bigint | undefined;
    sync: string | undefined;
}

/** An untyped text body, as every body in the script is. */
function text(body: string): Body {
    return { contentType: undefined, bytes: new TextEncoder().encode(body) };
}

/**
 * The text of a body the guest handed over. Every body this probe publishes
 * is untyped text, so anything else is the fixture or the seam having changed.
 */
function textOf(body: Body): string {
    if (body.contentType !== undefined && body.contentType !== null) {
        throw new Error(`the probe handed over a typed body (${body.contentType})`);
    }
    return new TextDecoder("utf-8", { fatal: true }).decode(body.bytes);
}

/**
 * Expand an `(id, body)` pair into an envelope using the script's fixed
 * non-identifying fields: the header JSON, and the body as untyped text. The
 * wasmtime half performs the same expansion — the script names the identity,
 * both harnesses supply the same frame around it.
 */
function envelope(
    pair: { id: string; body: string },
    overrides: Record<string, unknown> = {},
): GlueEnvelope {
    return {
        header: JSON.stringify({
            ...script.envelope_template,
            message_id: pair.id,
            ...overrides,
        }),
        body: text(pair.body),
    };
}

/** The script's deferred windows, in the glue's record shape. */
function deferred(windows: KernelActivation["deferred"]): GlueActivation["deferred"] {
    return windows.map((window) => ({
        port: window.port,
        entries: window.entries.map((entry) => ({
            index: entry.index,
            body: text(entry.payload),
            deliverAfter: BigInt(entry.deliver_after),
        })),
    }));
}

/**
 * Everything one activation buffered, in call order per class. Reset before each
 * activation and reported only on ok: keeping it iff `receive` returns ok is the
 * flush-on-ok / discard-on-err contract reduced to what a transcript can see,
 * and it covers deferral — a parked schedule and a control op are discarded with
 * the publishes.
 */
interface Buffer {
    publishes: Publish[];
    deferredPublishes: DeferredPublish[];
    ops: TranscriptOp[];
}

function emptyBuffer(): Buffer {
    return { publishes: [], deferredPublishes: [], ops: [] };
}

/**
 * One instantiated guest behind the production `activationEntryBytes`, with
 * the buffer its publishes land in.
 *
 * Shared by the transcript run and the reply pin, so both drive the same real
 * transpiled tree through the same seam.
 */
interface Harness {
    entry: (activation: GlueActivation) => string | { reply: Body } | undefined;
    reset: () => void;
    buffer: () => Buffer;
}

/**
 * Instantiate the transpiled guest and wrap it in the production entry.
 *
 * Err vs trap is discriminated by that entry, not here: it answers in the
 * kernel's vocabulary — `undefined` for ok, a reply object for an answered sync
 * call, a diagnostic string for the err arm, a rethrow for a trap — so callers
 * read the outcome off the same seam the page reads it off.
 */
async function harness(): Promise<Harness> {
    let buffer = emptyBuffer();
    const instance = await instantiateTranspiled<BytesProcessorInstance>("surface/dist", KIND, {
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
            publish: (port: string, body: Body) => {
                buffer.publishes.push({ port, body: textOf(body) });
            },
            publishDeferred: (port: string, body: Body, deliverAfter: bigint) => {
                buffer.deferredPublishes.push({
                    port,
                    body: textOf(body),
                    deliver_after: Number(deliverAfter),
                });
            },
            deferCancel: (port: string, index: number) => {
                buffer.ops.push({ op: "cancel", port, index });
            },
            deferEdit: (
                port: string,
                index: number,
                body: Body | undefined,
                deliverAfter: bigint | undefined,
            ) => {
                buffer.ops.push({
                    op: "edit",
                    port,
                    index,
                    // `undefined` is the absent half; the transcript spells
                    // absence `null`, which is what JSON can carry.
                    payload: body === undefined ? null : textOf(body),
                    deliver_after:
                        deliverAfter === undefined
                            ? null
                            : Number(deliverAfter),
                });
            },
        },
    });
    return {
        entry: activationEntryBytes(instance),
        reset: () => {
            buffer = emptyBuffer();
        },
        buffer: () => buffer,
    };
}

/**
 * Drive the whole script and return the transcript.
 *
 * One instance per activation: linear memory is activation-scoped on every
 * host, and the report's `counter` reads that rule off the guest. Reusing one
 * instance across the script would make this half disagree with the wasmtime
 * half on the counter alone, which is the divergence the rule closes rather
 * than a property of the transpiled seam.
 */
async function runScript(): Promise<TranscriptEntry[]> {
    const transcript: TranscriptEntry[] = [];
    for (const activation of script.activations) {
        const { entry, reset, buffer } = await harness();
        reset();
        const record: GlueActivation = {
            ports: activation.ports.map((p) => ({
                port: p.port,
                envelopes: p.envelopes.map((pair) => envelope(pair)),
                newFrom: p.new_from,
                dropped: p.dropped,
            })),
            deferred: deferred(activation.deferred),
            // option<u64>: null is absence, not a zero clock.
            now: activation.now === null ? undefined : BigInt(activation.now),
            // The transcript scripts asynchronous deliveries only: nothing on
            // the backend side of the parity can be a sync call, so there is
            // nothing for the surface side to mirror.
            sync: undefined,
        };
        let outcome: TranscriptEntry["outcome"];
        let answer: string | { reply: Body } | undefined;
        let trapped = false;
        try {
            answer = entry(record);
        } catch {
            trapped = true;
        }
        if (trapped) {
            outcome = "trap";
        } else if (answer === undefined) {
            outcome = "ok";
        } else if (typeof answer === "string") {
            // The kernel reads a returned string as the err arm.
            outcome = "err";
        } else {
            // A reply is neither outcome, and folding it into one would report
            // green on an invariant nothing asserted: the script carries no sync
            // call, so a reply here is the fixture or the seam having changed.
            throw new Error(
                `scripted activation answered a reply: ${textOf(answer.reply)}`,
            );
        }
        if (outcome !== "ok") {
            transcript.push({
                outcome,
                publishes: [],
                deferred_publishes: [],
                ops: [],
            });
            continue;
        }
        transcript.push({
            outcome,
            publishes: buffer().publishes,
            deferred_publishes: buffer().deferredPublishes,
            ops: buffer().ops,
        });
    }
    return transcript;
}

describe("processor transplant — surface hosting", () => {
    let transcript: TranscriptEntry[];

    beforeAll(async () => {
        // Mirrors the wasmtime half's posture on a missing artifact: fail with
        // the command that fixes it, never skip. A silently skipped parity test
        // asserts the invariant on one host while reporting green.
        requireTranspiledTree("surface/dist", KIND);
        transcript = await runScript();
    });

    it("produces the canonical transcript", () => {
        expect(transcript).toEqual(script.transcript);
    });

    it("carries a sync port in and a reply back out of a real guest", async () => {
        // The transcript cannot cover this: no backend activation is a sync
        // call, so the scripted half of the parity has nothing to mirror. What
        // is under test is jco's own lift in both directions — the world's
        // `option<string>` `sync` reaching the guest as a port name, and
        // `ok(some(body))` arriving as the reply object the kernel reads — on
        // the real transpiled tree rather than a hand-written mock of it.
        //
        // The activation carries an ordinary delivery beside the request, which
        // is what makes the guest SDK's sync accessors observable: the request
        // is windowed on its own port and excluded from the delivered windows,
        // so a fold over deliveries never sees it. Every migrated UI kind's
        // handler is built from those accessors, and this is the only place
        // they run.
        const { entry, reset } = await harness();
        reset();
        const port = "brenn:mount";
        const answered = entry({
            ports: [
                {
                    port,
                    // A request is attributed to its own target, by the bare
                    // name the page knows the instance by — the shape the
                    // kernel mints, which the guest reports back.
                    envelopes: [envelope({
                        id: "9a1d0a3e-0000-4000-8000-00000000f1ed",
                        body: "__reply__",
                    }, { source: "transplant", sender: "transplant" })],
                    newFrom: 0,
                    dropped: 0,
                },
                {
                    port: "in",
                    envelopes: [envelope({
                        id: "9a1d0a3e-0000-4000-8000-00000000f1ee",
                        body: "delivered",
                    })],
                    newFrom: 0,
                    dropped: 0,
                },
            ],
            deferred: [],
            now: undefined,
            sync: port,
        });
        if (answered === undefined || typeof answered === "string") {
            throw new Error(`the sync call answered no reply: ${answered}`);
        }
        expect(textOf(answered.reply)).toBe(
            `replied:${port}:mount=true:classified=undeclared:` +
                `request=__reply__:` +
                `bare-identity=true:delivered=[in]`,
        );

        // And the other arm from the same instance: an activation naming no
        // sync port answers nothing at all, which is a bare `undefined` and not
        // a reply object carrying a null.
        reset();
        expect(
            entry({ ports: [], deferred: [], now: undefined, sync: undefined }),
        ).toBeUndefined();
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
            const summary = JSON.parse(entry.publishes[0].body) as {
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
