// The scripted attach peer the page rig's kernel talks to, through a stand-in
// `WebSocket`.
//
// Every frame the peer sends is one the kit emitted from the Rust types: a
// page's opening ladder whole, everything else a template filled through its
// named slots. The peer knows no field name of a frame it sends. What it reads
// of the kernel's frames is read in one place, `readClientFrame`, which throws
// naming the frame and the field on anything absent.
//
// It answers what the kernel needs to attach and run, and nothing else: any
// other client frame, a binary frame, a second socket, or a delivery on a
// channel the kernel never subscribed is a failure.

import { type Failures, type RigTimers, describe } from "./globals.js";

/** A JSON value as the kit carries it. */
export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };

/** A frame and the JSON pointers of each of its variable fields. */
export interface Template {
    frame: Json;
    slots: Record<string, string[]>;
}

/** One instance a page runs. */
export interface KitInstance {
    instance: string;
    kind: string;
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
    /** The instances with a `state` io port, each on a page-local channel of its own. */
    state_instances: string[];
    status_channel: string;
    max_body_bytes: number;
    ladder: { hello: Json; welcome: Json; config_deliver: Json };
}

/** The kit `//surface/test-fixtures/peer:page_rig_kit` prints. */
export interface Kit {
    pages: Record<string, KitPage>;
    answers: {
        subscribe_result: Template;
        publish_result: Template;
        publish_batch_result: Template;
    };
    deliver: Template;
    client_samples: unknown[];
}

/**
 * Fill `template` with `values`, one per slot. A slot left unfilled, or a value
 * for a slot the template does not have, is an error: either means the rig and
 * the kit disagree about the frame.
 */
export function fill(template: Template, values: Record<string, Json>): string {
    const frame = structuredClone(template.frame);
    const names = Object.keys(template.slots);
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
        for (const pointer of template.slots[name] ?? []) {
            setPointer(frame, pointer, value);
        }
    }
    return JSON.stringify(frame);
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
    body: string;
}

/** The client frames the peer reads, reduced to what it reads of them. */
export type ClientFrame =
    | { type: "Hello" }
    | { type: "Subscribe"; channel: string }
    | {
          type: "Publish";
          attribution: string | null;
          channel: string;
          body: string;
          correlation: number | null;
      }
    | {
          type: "PublishBatch";
          attribution: string | null;
          correlation: number;
          publishes: BatchPublish[];
          deferredOps: unknown[];
      };

/**
 * Read a client frame by field name. The one place the peer names a field of a
 * frame the kernel sends; a field it needs that is absent or of the wrong type
 * throws naming the frame and the field.
 */
export function readClientFrame(text: string): ClientFrame {
    const raw: unknown = JSON.parse(text);
    if (raw === null || typeof raw !== "object" || Array.isArray(raw)) {
        throw new Error(`client frame is not an object: ${text.slice(0, 200)}`);
    }
    const frame = raw as Record<string, unknown>;
    const type = frame["type"];
    if (typeof type !== "string") {
        throw new Error(`client frame has no 'type': ${text.slice(0, 200)}`);
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
    const optional = <T>(
        name: string,
        check: (v: unknown) => v is T,
    ): T | null => (frame[name] === undefined ? null : field(frame, name, check, type));
    switch (type) {
        case "Hello":
            return { type };
        case "Subscribe":
            return { type, channel: field(frame, "channel", isString, type) };
        case "Publish":
            return {
                type,
                attribution: optional("attribution", isString),
                channel: field(frame, "channel", isString, type),
                body: field(frame, "body", isString, type),
                correlation: optional("correlation", isNumber),
            };
        case "PublishBatch":
            return {
                type,
                attribution: optional("attribution", isString),
                correlation: field(frame, "correlation", isNumber, type),
                publishes: field(frame, "publishes", isArray, type).map(
                    (entry: unknown) => {
                        const p = (entry ?? {}) as Record<string, unknown>;
                        return {
                            channel: field(p, "channel", isString, "PublishBatch.publishes[]"),
                            body: field(p, "body", isString, "PublishBatch.publishes[]"),
                        };
                    },
                ),
                // Omitted on the wire when empty.
                deferredOps: optional("deferred_ops", isArray) ?? [],
            };
        default:
            throw new Error(`the peer has no answer for a client frame of type '${type}': ${text.slice(0, 300)}`);
    }
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

/** One body the kernel published to the wire, in arrival order. */
export interface WirePublish {
    /** Position among everything the peer received, from 0. */
    order: number;
    channel: string;
    body: string;
    /** The publishing instance the frame names, or null for the page's own. */
    attribution: string | null;
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

            send(data: unknown): void {
                peer.receive(data);
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

    /** The socket the kernel opened, once it has. */
    get connected(): boolean {
        return this.socket !== null;
    }

    /**
     * Deliver `body` on `channel` to the kernel, as one `Deliver` row: the
     * channel's next `seq`, a fresh cursor and message id. Only `ephemeral:`
     * channels: the kit's template stamps every row's envelope `ephemeral`.
     */
    deliver(channel: string, body: string): void {
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
            fill(this.kit.deliver, {
                channel,
                body,
                seq,
                cursor: `rig-${seq}`,
                message_id: crypto.randomUUID(),
            }),
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

    private send(text: string): void {
        const socket = this.socket;
        if (socket === null) {
            this.failures.fatal("the peer has no socket to send on");
        }
        socket.onmessage?.({ data: text });
    }

    private sendSoon(...frames: string[]): void {
        this.timers().soon(() => {
            for (const frame of frames) {
                this.send(frame);
            }
        });
    }

    private receive(data: unknown): void {
        if (typeof data !== "string") {
            this.failures.fatal(`the kernel sent a binary frame (${describe(data)})`);
        }
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
                        JSON.stringify(ladder.config_deliver),
                    );
                } else {
                    this.sendSoon(
                        fill(answers.subscribe_result, { channel: frame.channel, replay_count: 0 }),
                    );
                }
                return;
            }
            case "Publish":
                this.record(frame.channel, frame.body, frame.attribution);
                if (frame.correlation !== null) {
                    this.sendSoon(fill(answers.publish_result, { correlation: frame.correlation }));
                }
                return;
            case "PublishBatch":
                if (frame.deferredOps.length > 0) {
                    this.failures.fatal(
                        `a PublishBatch from '${String(frame.attribution)}' carries deferred ops; ` +
                            "the rig parks only on page-local channels",
                    );
                }
                for (const p of frame.publishes) {
                    this.record(p.channel, p.body, frame.attribution);
                }
                this.sendSoon(
                    fill(answers.publish_batch_result, { correlation: frame.correlation }),
                );
                return;
        }
    }

    private record(channel: string, body: string, attribution: string | null): void {
        this.published.push({ order: this.nextOrder++, channel, body, attribution });
    }
}
