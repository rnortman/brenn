// The scripted attach peer the page rig's kernel talks to, through a stand-in
// `WebSocket`.
//
// Every frame the peer sends is one the kit emitted from the Rust types: a
// page's opening ladder whole, everything else a template filled through its
// named slots. The peer knows no field name of a frame it sends. What it reads
// of the kernel's frames is read in one place, `readClientFrame`, which throws
// naming the frame and the field on anything absent.
//
// At the surface route's attach version a body-carrying frame is a binary
// message: a big-endian `u32` header length, the header JSON, then each body's
// bytes in descriptor order. `assemble` and `split` are the only code here that
// knows that layout; the kit's server samples pin it byte for byte against
// messages the Rust codec encoded.
//
// It answers what the kernel needs to attach and run, and nothing else: any
// other client frame, a frame of the wrong message type for its kind, a second
// socket, or a delivery on a channel the kernel never subscribed is a failure.
//
// It is the deferral authority of every transportable channel, as the server
// is: a batch entry with a release instant is parked under a minted id for its
// `(channel, attribution)`, a control op edits or cancels it, and every set a
// batch touched is restated as a `DeferredView` ahead of the batch's answer.
// Nothing parked is ever released.

import { type Failures, type RigTimers, describe } from "./globals.js";

/** A JSON value as the kit carries it. */
export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };

/** A frame and the JSON pointers of each of its variable fields. */
export interface Template {
    frame: Json;
    slots: Record<string, string[]>;
}

/**
 * A binary frame's header exactly as the codec wrote it, the JSON pointers of
 * its variable fields, and the pointer of each body descriptor in body order.
 */
export interface BinaryTemplate {
    header: string;
    slots: Record<string, string[]>;
    bodies: string[];
}

/** One body as it crosses the wire: its bytes and its content type, if typed. */
export interface RigBody {
    bytes: Uint8Array;
    contentType: string | null;
}

/** One binary message the Rust codec encoded, with what produced it. */
export interface ServerSample {
    /** `deliver`, or `deferred_view` for the view template of the sample's size. */
    template: "deliver" | "deferred_view";
    values: Record<string, Json>;
    bodies: { hex: string; content_type: string | null }[];
    hex: string;
}

/** One client frame as the page writes it: a text message, or a binary one as hex. */
export type ClientSample = { text: string } | { hex: string };

/** One instance a page runs. */
export interface KitInstance {
    instance: string;
    kind: string;
    /** The contract `kind` is built against, as the server would state it. */
    contract: string;
    command_channel: string;
}

/** One named page of the kit. */
export interface KitPage {
    slug: string;
    build_id: string;
    config_channel: string;
    location: { protocol: string; host: string };
    instances: KitInstance[];
    report_channel: string;
    feed_channel: string;
    /** The wire channel `b-keeper`'s `feed` port reads, on a page that runs it. */
    bytes_feed_channel: string;
    /** The text-only wire channel `b-keeper`'s `out` port is bound to. */
    bytes_out_channel: string;
    /** The wire channel, declared `non_text`, `b-keeper`'s `bout` port is bound to. */
    bytes_wire_channel: string;
    /** The wire channel, declared `non_text`, that wakes `b-stream` on the `measure` page. */
    stream_channel: string;
    /** The page-local channel `b-keeper` keeps typed bodies on and `plain` reads. */
    shared_channel: string;
    /** The page-local channel, declared `non_text`, behind `b-keeper`'s `timer` port. */
    bytes_timer_channel: string;
    /**
     * The `brenn:processor@0.1.0` instances with a `state` io port, each on a
     * page-local channel of its own.
     */
    state_instances: string[];
    /** The same, on `brenn:processor@0.2.0`. */
    bytes_state_instances: string[];
    /**
     * The `processor-sdk-bytes-test` instances, whose binary state the guest
     * SDK keeps on a `non_text` page-local channel of its own.
     */
    sdk_state_instances: string[];
    status_channel: string;
    max_body_bytes: number;
    ladder: { hello: Json; welcome: Json; config_deliver: { header: string; body: string } };
}

/** The kit `//surface/test-fixtures/peer:page_rig_kit` prints. */
export interface Kit {
    pages: Record<string, KitPage>;
    answers: {
        subscribe_result: Template;
        publish_result: Template;
        publish_batch_result: Template;
    };
    deliver: BinaryTemplate;
    /** `DeferredView` templates, the one at index `n` holding `n` entries. */
    deferred_views: BinaryTemplate[];
    server_samples: ServerSample[];
    client_samples: ClientSample[];
}

/** The template a server sample is rebuilt from. */
export function sampleTemplate(kit: Kit, sample: ServerSample): BinaryTemplate {
    if (sample.template === "deliver") {
        return kit.deliver;
    }
    const template = kit.deferred_views[sample.bodies.length];
    if (template === undefined) {
        throw new Error(`the kit has no DeferredView template of ${sample.bodies.length} entries`);
    }
    return template;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });

/** Untyped text as a body. */
export function textBody(text: string): RigBody {
    return { bytes: encoder.encode(text), contentType: null };
}

/** A body's bytes as text, failing on bytes that are not UTF-8. */
export function textOf(body: RigBody): string {
    return decoder.decode(body.bytes);
}

/** Lowercase hex of `bytes`. */
export function hexOf(bytes: Uint8Array): string {
    return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

/** The bytes `hex` spells. */
export function bytesOfHex(hex: string): Uint8Array {
    if (hex.length % 2 !== 0 || !/^[0-9a-f]*$/.test(hex)) {
        throw new Error(`not lowercase hex: ${hex.slice(0, 40)}`);
    }
    const bytes = new Uint8Array(hex.length / 2);
    for (let i = 0; i < bytes.length; i++) {
        bytes[i] = parseInt(hex.slice(2 * i, 2 * i + 2), 16);
    }
    return bytes;
}

/** One binary message: the header's length, big-endian, the header, the bodies. */
export function assemble(header: string, bodies: Uint8Array[]): Uint8Array {
    const head = encoder.encode(header);
    const total = 4 + head.length + bodies.reduce((n, b) => n + b.length, 0);
    const out = new Uint8Array(total);
    new DataView(out.buffer).setUint32(0, head.length, false);
    out.set(head, 4);
    let at = 4 + head.length;
    for (const body of bodies) {
        out.set(body, at);
        at += body.length;
    }
    return out;
}

/** A binary message cut at its header: the header's text and the body section. */
export function split(message: Uint8Array): { header: string; rest: Uint8Array } {
    if (message.length < 4) {
        throw new Error(`a binary message of ${message.length} bytes has no header length`);
    }
    const len = new DataView(message.buffer, message.byteOffset, 4).getUint32(0, false);
    if (4 + len > message.length) {
        throw new Error(`a header of ${len} bytes runs past a ${message.length}-byte message`);
    }
    return {
        header: decoder.decode(message.subarray(4, 4 + len)),
        rest: message.subarray(4 + len),
    };
}

/**
 * Fill a binary `template` with `values`, write a descriptor for each of
 * `bodies` and join them into the message the peer sends.
 */
export function fillBinary(
    template: BinaryTemplate,
    values: Record<string, Json>,
    bodies: RigBody[],
): Uint8Array {
    const header = JSON.parse(template.header) as Json;
    fillSlots(header, template.slots, values);
    if (bodies.length !== template.bodies.length) {
        throw new Error(
            `the template carries ${template.bodies.length} bodies and was handed ${bodies.length}`,
        );
    }
    template.bodies.forEach((pointer, i) => {
        const body = bodies[i] as RigBody;
        const descriptor: { [key: string]: Json } = { len: body.bytes.length };
        if (body.contentType !== null) {
            descriptor["content_type"] = body.contentType;
        }
        setPointer(header, pointer, descriptor);
    });
    return assemble(
        JSON.stringify(header),
        bodies.map((b) => b.bytes),
    );
}

/**
 * Fill `template` with `values`, one per slot. A slot left unfilled, or a value
 * for a slot the template does not have, is an error: either means the rig and
 * the kit disagree about the frame.
 */
export function fill(template: Template, values: Record<string, Json>): string {
    const frame = structuredClone(template.frame);
    fillSlots(frame, template.slots, values);
    return JSON.stringify(frame);
}

function fillSlots(frame: Json, slots: Record<string, string[]>, values: Record<string, Json>): void {
    const names = Object.keys(slots);
    for (const name of Object.keys(values)) {
        if (!names.includes(name)) {
            throw new Error(`the template has no slot '${name}' (it has ${names.join(", ")})`);
        }
    }
    for (const name of names) {
        const value = values[name];
        if (value === undefined) {
            throw new Error(`slot '${name}' was not filled`);
        }
        for (const pointer of slots[name] ?? []) {
            setPointer(frame, pointer, value);
        }
    }
}

function setPointer(root: Json, pointer: string, value: Json): void {
    const path = pointer
        .split("/")
        .slice(1)
        .map((p) => p.replace(/~1/g, "/").replace(/~0/g, "~"));
    const last = path.pop();
    if (last === undefined) {
        throw new Error(`slot pointer '${pointer}' names the whole frame`);
    }
    let at: Json = root;
    for (const key of path) {
        const next: Json | undefined = child(at, key);
        if (next === undefined) {
            throw new Error(`slot pointer '${pointer}' names nothing in the frame`);
        }
        at = next;
    }
    if (Array.isArray(at)) {
        at[Number(last)] = value;
    } else if (at !== null && typeof at === "object") {
        at[last] = value;
    } else {
        throw new Error(`slot pointer '${pointer}' names inside a scalar`);
    }
}

function child(at: Json, key: string): Json | undefined {
    if (Array.isArray(at)) {
        return at[Number(key)];
    }
    if (at !== null && typeof at === "object") {
        return at[key];
    }
    return undefined;
}

/** One publish inside a `PublishBatch`. */
export interface BatchPublish {
    channel: string;
    body: RigBody;
    deliverAfter: number | null;
}

/** One control op inside a `PublishBatch`. */
export interface BatchOp {
    channel: string;
    messageId: string;
    kind: "cancel" | "edit";
    /** An edit's new body, or null for a cancel and an edit that keeps its body. */
    body: RigBody | null;
    deliverAfter: number | null;
}

/** The client frames the peer reads, reduced to what it reads of them. */
export type ClientFrame =
    | { type: "Hello" }
    | { type: "Subscribe"; channel: string }
    | {
          type: "Publish";
          attribution: string | null;
          channel: string;
          body: RigBody;
          correlation: number | null;
      }
    | {
          type: "PublishBatch";
          attribution: string | null;
          correlation: number;
          publishes: BatchPublish[];
          deferredOps: BatchOp[];
      };

/** The client kinds that travel as binary messages. */
const BINARY_KINDS = new Set(["Publish", "PublishBatch"]);

/**
 * Read a client frame by field name: a text message as its JSON, a binary
 * message as its header with each body cut from the body section in
 * descriptor order. The one place the peer names a field of a frame the kernel
 * sends; a field it needs that is absent or of the wrong type, a kind sent as
 * the wrong message type, or a body section that disagrees with its
 * descriptors throws naming the frame.
 */
export function readClientFrame(data: string | Uint8Array): ClientFrame {
    const binary = typeof data !== "string";
    const { header: text, rest } = binary ? split(data) : { header: data, rest: new Uint8Array() };
    const raw: unknown = JSON.parse(text);
    if (raw === null || typeof raw !== "object" || Array.isArray(raw)) {
        throw new Error(`client frame is not an object: ${text.slice(0, 200)}`);
    }
    const frame = raw as Record<string, unknown>;
    const type = frame["type"];
    if (typeof type !== "string") {
        throw new Error(`client frame has no 'type': ${text.slice(0, 200)}`);
    }
    if (BINARY_KINDS.has(type) !== binary) {
        throw new Error(
            `client frame ${type} arrived as a ${binary ? "binary" : "text"} message: ${text.slice(0, 200)}`,
        );
    }
    const field = <T>(
        from: Record<string, unknown>,
        name: string,
        check: (v: unknown) => v is T,
        what: string,
    ): T => {
        const value = from[name];
        if (!check(value)) {
            throw new Error(
                value === undefined
                    ? `client frame ${what} has no '${name}'`
                    : `client frame ${what} has a '${name}' of the wrong type: ${describe(value)}`,
            );
        }
        return value;
    };
    const optionalOf = <T>(
        from: Record<string, unknown>,
        name: string,
        check: (v: unknown) => v is T,
        what: string,
    ): T | null => (from[name] === undefined ? null : field(from, name, check, what));
    const optional = <T>(name: string, check: (v: unknown) => v is T): T | null =>
        optionalOf(frame, name, check, type);
    // The body section, consumed in descriptor order.
    let at = 0;
    const body = (descriptor: unknown, what: string): RigBody => {
        const d = (descriptor ?? {}) as Record<string, unknown>;
        const len = field(d, "len", isNumber, what);
        const contentType = optionalOf(d, "content_type", isString, what);
        if (at + len > rest.length) {
            throw new Error(`client frame ${what} describes ${len} bytes past the body section`);
        }
        const bytes = rest.slice(at, at + len);
        at += len;
        return { bytes, contentType };
    };
    let read: ClientFrame;
    switch (type) {
        case "Hello":
            read = { type };
            break;
        case "Subscribe":
            read = { type, channel: field(frame, "channel", isString, type) };
            break;
        case "Publish":
            read = {
                type,
                attribution: optional("attribution", isString),
                channel: field(frame, "channel", isString, type),
                body: body(frame["body"], "Publish.body"),
                correlation: optional("correlation", isNumber),
            };
            break;
        case "PublishBatch": {
            const what = "PublishBatch.publishes[]";
            const publishes = field(frame, "publishes", isArray, type).map((entry: unknown) => {
                const p = (entry ?? {}) as Record<string, unknown>;
                return {
                    channel: field(p, "channel", isString, what),
                    body: body(p["body"], what),
                    deliverAfter: optionalOf(p, "deliver_after", isNumber, what),
                };
            });
            const opWhat = "PublishBatch.deferred_ops[]";
            // Omitted on the wire when empty.
            const deferredOps = (optional("deferred_ops", isArray) ?? []).map((entry: unknown) => {
                const o = (entry ?? {}) as Record<string, unknown>;
                const op = field(o, "op", isObject, opWhat);
                const kind = field(op, "kind", isString, opWhat);
                if (kind !== "cancel" && kind !== "edit") {
                    throw new Error(`client frame ${opWhat} has an op of kind '${kind}'`);
                }
                return {
                    channel: field(o, "channel", isString, opWhat),
                    messageId: field(o, "message_id", isString, opWhat),
                    kind,
                    body: op["body"] === undefined ? null : body(op["body"], opWhat),
                    deliverAfter: optionalOf(op, "deliver_after", isNumber, opWhat),
                } satisfies BatchOp;
            });
            read = {
                type,
                attribution: optional("attribution", isString),
                correlation: field(frame, "correlation", isNumber, type),
                publishes,
                deferredOps,
            };
            break;
        }
        default:
            throw new Error(`the peer has no answer for a client frame of type '${type}': ${text.slice(0, 300)}`);
    }
    if (at !== rest.length) {
        throw new Error(`client frame ${type} leaves ${rest.length - at} bytes after its last body`);
    }
    return read;
}

function parkKey(channel: string, attribution: string): string {
    return JSON.stringify([channel, attribution]);
}

/** `set` soonest release first, ties in the order parked, as a view states it. */
function releaseOrder(set: Parked[]): Parked[] {
    return set
        .map((m, i) => ({ m, i }))
        .sort((a, b) => a.m.deliverAfter - b.m.deliverAfter || a.i - b.i)
        .map(({ m }) => ({ ...m }));
}

function isString(v: unknown): v is string {
    return typeof v === "string";
}

function isNumber(v: unknown): v is number {
    return typeof v === "number";
}

function isArray(v: unknown): v is unknown[] {
    return Array.isArray(v);
}

function isObject(v: unknown): v is Record<string, unknown> {
    return v !== null && typeof v === "object" && !Array.isArray(v);
}

/** One body the kernel published to the wire, in arrival order. */
export interface WirePublish {
    /** Position among everything the peer received, from 0. */
    order: number;
    channel: string;
    body: RigBody;
    /** The publishing instance the frame names, or null for the page's own. */
    attribution: string | null;
    /** The client frame that carried it. */
    frame: "Publish" | "PublishBatch";
}

/** One message the peer holds parked, as a `DeferredView` states it. */
export interface Parked {
    messageId: string;
    body: RigBody;
    deliverAfter: number;
}

/** The handlers and members of a browser `WebSocket` the kernel uses. */
export interface RigSocket {
    readonly url: string;
    binaryType: string;
    onopen: ((ev: unknown) => void) | null;
    onmessage: ((ev: { data: unknown }) => void) | null;
    onclose: ((ev: unknown) => void) | null;
    onerror: ((ev: unknown) => void) | null;
    readonly readyState: number;
    send(data: unknown): void;
    close(code?: number, reason?: string): void;
}

/** The scripted peer of one page. */
export class Peer {
    /** Everything the kernel published to the wire, in arrival order. */
    readonly published: WirePublish[] = [];
    private socket: RigSocket | null = null;
    private readonly subscribed = new Set<string>();
    private readonly seqs = new Map<string, number>();
    /** Each sender's parked set, keyed by `parkKey`, in the order parked. */
    private readonly parkedSets = new Map<string, Parked[]>();
    private helloSeen = false;
    private nextOrder = 0;
    private readonly socketClass: new (url: string) => RigSocket;

    constructor(
        private readonly kit: Kit,
        private readonly page: KitPage,
        private readonly failures: Failures,
        private readonly timers: () => RigTimers,
    ) {
        // The stand-in global the kernel constructs. It closes over this peer.
        const peer = this;
        this.socketClass = class implements RigSocket {
            binaryType = "blob";
            onopen: ((ev: unknown) => void) | null = null;
            onmessage: ((ev: { data: unknown }) => void) | null = null;
            onclose: ((ev: unknown) => void) | null = null;
            onerror: ((ev: unknown) => void) | null = null;
            readyState = 0;

            constructor(readonly url: string) {
                peer.connect(this);
            }

            // A string is a text message. Binary data is copied before this
            // returns: the kernel passes a view over wasm memory, which does
            // not outlive the call.
            send(data: unknown): void {
                if (typeof data === "string") {
                    peer.receive(data);
                } else if (data instanceof ArrayBuffer) {
                    peer.receive(new Uint8Array(data.slice(0)));
                } else if (ArrayBuffer.isView(data)) {
                    peer.receive(
                        new Uint8Array(data.buffer, data.byteOffset, data.byteLength).slice(),
                    );
                } else {
                    peer.failures.fatal(`the kernel sent neither text nor bytes (${describe(data)})`);
                }
            }

            close(code?: number, reason?: string): void {
                peer.failures.fatal(`the kernel closed its socket (${String(code)} ${String(reason)})`);
            }
        };
    }

    /** The class to install as the global `WebSocket`. */
    get WebSocket(): new (url: string) => RigSocket {
        return this.socketClass;
    }

    /** What `attribution` has parked on `channel`, soonest release first. */
    parked(channel: string, attribution: string): Parked[] {
        return releaseOrder(this.parkedSets.get(parkKey(channel, attribution)) ?? []);
    }

    /** The socket the kernel opened, once it has. */
    get connected(): boolean {
        return this.socket !== null;
    }

    /**
     * Deliver `body` on `channel` to the kernel, as one `Deliver` row: the
     * channel's next `seq`, a fresh cursor and message id. A string is
     * untyped text. Only `ephemeral:` channels: the kit's template stamps every
     * row's envelope `ephemeral`.
     */
    deliver(channel: string, body: string | RigBody): void {
        if (!this.subscribed.has(channel)) {
            this.failures.fatal(`delivery on '${channel}', which the kernel never subscribed`);
        }
        if (!channel.startsWith("ephemeral:")) {
            this.failures.fatal(
                `delivery on '${channel}': the kit's deliver template types every envelope ` +
                    "'ephemeral', so it carries only ephemeral: channels",
            );
        }
        const seq = (this.seqs.get(channel) ?? 0) + 1;
        this.seqs.set(channel, seq);
        this.send(
            fillBinary(
                this.kit.deliver,
                { channel, seq, cursor: `rig-${seq}`, message_id: crypto.randomUUID() },
                [typeof body === "string" ? textBody(body) : body],
            ),
        );
    }

    private connect(socket: RigSocket): void {
        if (this.socket !== null) {
            this.failures.fatal(`the kernel opened a second socket (${socket.url}): a reconnect`);
        }
        const path = `/surface/${this.page.slug}/ws?build=${this.page.build_id}`;
        if (!socket.url.endsWith(path)) {
            this.failures.fatal(`the kernel opened ${socket.url}, not a URL ending ${path}`);
        }
        this.socket = socket;
        this.timers().soon(() => {
            (socket as { readyState: number }).readyState = 1;
            socket.onopen?.({ type: "open" });
        });
    }

    /**
     * A string as a text message; bytes as a binary one, handed over as a
     * fresh `ArrayBuffer` — which a browser does only when the page has set
     * `binaryType` to `"arraybuffer"`, and otherwise hands over a `Blob`.
     */
    private send(message: string | Uint8Array): void {
        const socket = this.socket;
        if (socket === null) {
            this.failures.fatal("the peer has no socket to send on");
        }
        if (typeof message === "string") {
            socket.onmessage?.({ data: message });
            return;
        }
        if (socket.binaryType !== "arraybuffer") {
            this.failures.fatal(
                `a binary frame on a socket whose binaryType is '${socket.binaryType}': ` +
                    "a browser would hand the page a Blob",
            );
        }
        socket.onmessage?.({ data: message.slice().buffer });
    }

    private sendSoon(...frames: (string | Uint8Array)[]): void {
        this.timers().soon(() => {
            for (const frame of frames) {
                this.send(frame);
            }
        });
    }

    private receive(data: string | Uint8Array): void {
        let frame: ClientFrame;
        try {
            frame = readClientFrame(data);
        } catch (err) {
            this.failures.fatal(err instanceof Error ? err.message : describe(err));
        }
        const ladder = this.page.ladder;
        const answers = this.kit.answers;
        switch (frame.type) {
            case "Hello":
                if (this.helloSeen) {
                    this.failures.fatal("the kernel sent a second Hello");
                }
                this.helloSeen = true;
                this.sendSoon(JSON.stringify(ladder.hello), JSON.stringify(ladder.welcome));
                return;
            case "Subscribe": {
                this.subscribed.add(frame.channel);
                if (frame.channel === this.page.config_channel) {
                    this.seqs.set(frame.channel, 1);
                    this.sendSoon(
                        fill(answers.subscribe_result, { channel: frame.channel, replay_count: 1 }),
                        assemble(ladder.config_deliver.header, [
                            encoder.encode(ladder.config_deliver.body),
                        ]),
                    );
                } else {
                    this.sendSoon(
                        fill(answers.subscribe_result, { channel: frame.channel, replay_count: 0 }),
                    );
                }
                return;
            }
            case "Publish":
                this.record(frame.type, frame.channel, frame.body, frame.attribution);
                if (frame.correlation !== null) {
                    this.sendSoon(fill(answers.publish_result, { correlation: frame.correlation }));
                }
                return;
            case "PublishBatch": {
                // The server's order: the control ops, then the publishes, then
                // a view of every set either touched, then the answer.
                const touched: string[] = [];
                const attribution = frame.attribution;
                const parks = frame.publishes.some((p) => p.deliverAfter !== null);
                if ((parks || frame.deferredOps.length > 0) && attribution === null) {
                    this.failures.fatal(
                        "an unattributed PublishBatch parks or edits: the page's own sender parks nothing",
                    );
                }
                for (const op of frame.deferredOps) {
                    this.applyOp(attribution as string, op);
                    touched.push(op.channel);
                }
                for (const p of frame.publishes) {
                    if (p.deliverAfter === null) {
                        this.record(frame.type, p.channel, p.body, attribution);
                    } else {
                        this.park(p.channel, attribution as string, p.body, p.deliverAfter);
                        touched.push(p.channel);
                    }
                }
                const views = [...new Set(touched)].map((channel) =>
                    this.view(channel, attribution as string),
                );
                this.sendSoon(
                    ...views,
                    fill(answers.publish_batch_result, { correlation: frame.correlation }),
                );
                return;
            }
        }
    }

    private park(channel: string, attribution: string, body: RigBody, deliverAfter: number): void {
        const key = parkKey(channel, attribution);
        const set = this.parkedSets.get(key) ?? [];
        set.push({ messageId: crypto.randomUUID(), body, deliverAfter });
        this.parkedSets.set(key, set);
    }

    /**
     * Edit or cancel the message `op` names in `attribution`'s set. An id the
     * set does not hold is a failure: a conforming page names only what a view
     * showed it, and the rig never releases.
     */
    private applyOp(attribution: string, op: BatchOp): void {
        const set = this.parkedSets.get(parkKey(op.channel, attribution)) ?? [];
        const at = set.findIndex((m) => m.messageId === op.messageId);
        const held = set[at];
        if (held === undefined) {
            this.failures.fatal(
                `a ${op.kind} of ${op.messageId} on '${op.channel}', which '${attribution}' has not parked`,
            );
        }
        if (op.kind === "cancel") {
            set.splice(at, 1);
            return;
        }
        if (op.body !== null) {
            held.body = op.body;
        }
        if (op.deliverAfter !== null) {
            held.deliverAfter = op.deliverAfter;
        }
    }

    /** `attribution`'s parked set on `channel` as one `DeferredView` message. */
    private view(channel: string, attribution: string): Uint8Array {
        const entries = this.parked(channel, attribution);
        const template = this.kit.deferred_views[entries.length];
        if (template === undefined) {
            this.failures.fatal(
                `'${attribution}' has ${entries.length} messages parked on '${channel}', ` +
                    `more than the kit's largest DeferredView template holds`,
            );
        }
        const values: Record<string, Json> = { channel, attribution };
        entries.forEach((m, i) => {
            values[`message_id_${i}`] = m.messageId;
            values[`deliver_after_${i}`] = m.deliverAfter;
        });
        return fillBinary(
            template,
            values,
            entries.map((m) => m.body),
        );
    }

    private record(
        frame: WirePublish["frame"],
        channel: string,
        body: RigBody,
        attribution: string | null,
    ): void {
        this.published.push({ order: this.nextOrder++, channel, body, attribution, frame });
    }
}
