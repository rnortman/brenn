// The fixture kinds' state workload, computed on the test's side: the bytes
// `state <raw-len>` generates, advanced by its increments, and the result word
// the guest reports for them. Every fixture kind with a `state` command runs the
// same workload: the two raw kinds and the guest-SDK kind.

/** FNV-1a 64 of `bytes`, as 16 hex digits, in two 32-bit halves. */
export function fnv1a64(bytes: Uint8Array): string {
    let hi = 0xcbf29ce4;
    let lo = 0x84222325;
    for (const b of bytes) {
        lo = (lo ^ b) >>> 0;
        // Multiply by the prime 2^40 + 0x1b3: `lo * 0x1b3` is exact in a double,
        // and the 2^40 term moves `lo` eight bits into the high half.
        const low = lo * 0x1b3;
        const carry = Math.floor(low / 0x1_0000_0000);
        hi = (Math.imul(hi, 0x1b3) + carry + ((lo << 8) >>> 0)) >>> 0;
        lo = low >>> 0;
    }
    return hi.toString(16).padStart(8, "0") + lo.toString(16).padStart(8, "0");
}

/** The bytes every fixture generates for `state <len>` when it holds no state. */
export function generatedState(len: number): Uint8Array {
    const bytes = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
        bytes[i] = (i * 31 + 7) % 256;
    }
    return bytes;
}

/**
 * A fresh copy of `bytes`, at least four long, with `counter` as the
 * little-endian `u32` in bytes 0..4.
 */
export function withCounter(bytes: Uint8Array, counter: number): Uint8Array {
    const out = new Uint8Array(bytes);
    new DataView(out.buffer).setUint32(0, counter >>> 0, true);
    return out;
}

/**
 * The result word every fixture reports for the state of `generated.length`
 * bytes, advanced `steps` times: each step adds one to the little-endian `u32`
 * in the first four bytes.
 */
export function expectedState(generated: Uint8Array, steps: number): string {
    const view = new DataView(generated.buffer, generated.byteOffset, generated.byteLength);
    const bytes = withCounter(generated, view.getUint32(0, true) + steps);
    return `state:${bytes.length}:${fnv1a64(bytes)}`;
}
