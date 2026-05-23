function bytesToBase64(bytes) {
    if (typeof Buffer !== 'undefined') {
        return Buffer.from(bytes).toString('base64');
    }
    let binary = '';
    for (const byte of bytes) {
        binary += String.fromCharCode(byte);
    }
    return btoa(binary);
}
function base64ToBytes(value) {
    if (typeof Buffer !== 'undefined') {
        return new Uint8Array(Buffer.from(value, 'base64'));
    }
    const binary = atob(value);
    const out = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
        out[i] = binary.charCodeAt(i);
    }
    return out;
}
function encodePayload(message) {
    const wire = { ...message };
    if (message.type === 'data') {
        wire['bytes'] = bytesToBase64(message.bytes);
    }
    return new TextEncoder().encode(JSON.stringify(wire));
}
function decodePayload(bytes) {
    const wire = JSON.parse(new TextDecoder().decode(bytes));
    if (wire.type === 'data' && typeof wire['bytes'] === 'string') {
        return {
            type: 'data',
            object: wire['object'],
            bytes: base64ToBytes(wire['bytes']),
        };
    }
    return wire;
}
/** Length-prefixed JSON frames (browser and Node safe). */
export function encodeFrame(message) {
    const body = encodePayload(message);
    const frame = new Uint8Array(4 + body.length);
    const view = new DataView(frame.buffer);
    view.setUint32(0, body.length, false);
    frame.set(body, 4);
    return frame;
}
export function createFrameDecoder(onMessage) {
    let buffer = new Uint8Array(0);
    const append = (chunk) => {
        const next = new Uint8Array(buffer.length + chunk.length);
        next.set(buffer, 0);
        next.set(chunk, buffer.length);
        buffer = next;
    };
    return (chunk) => {
        append(chunk);
        while (buffer.length >= 4) {
            const size = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength).getUint32(0, false);
            if (buffer.length < 4 + size) {
                return;
            }
            const body = buffer.slice(4, 4 + size);
            buffer = buffer.slice(4 + size);
            onMessage(decodePayload(body));
        }
    };
}
//# sourceMappingURL=codec.js.map