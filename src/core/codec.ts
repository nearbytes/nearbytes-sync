import type { SyncMessage } from './types.js';

function bytesToBase64(bytes: Uint8Array): string {
  if (typeof Buffer !== 'undefined') {
    return Buffer.from(bytes).toString('base64');
  }
  let binary = '';
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary);
}

function base64ToBytes(value: string): Uint8Array {
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

type WireMessage = Record<string, unknown> & { type: string };

function encodePayload(message: SyncMessage): Uint8Array {
  const wire: WireMessage = { ...message } as WireMessage;
  if (message.type === 'data') {
    wire['bytes'] = bytesToBase64(message.bytes);
  }
  return new TextEncoder().encode(JSON.stringify(wire));
}

function decodePayload(bytes: Uint8Array): SyncMessage {
  const wire = JSON.parse(new TextDecoder().decode(bytes)) as WireMessage;
  if (wire.type === 'data' && typeof wire['bytes'] === 'string') {
    return {
      type: 'data',
      object: wire['object'] as SyncMessage extends { type: 'data' } ? never : never,
      bytes: base64ToBytes(wire['bytes']),
    } as SyncMessage;
  }
  return wire as SyncMessage;
}

/** Length-prefixed JSON frames (browser and Node safe). */
export function encodeFrame(message: SyncMessage): Uint8Array {
  const body = encodePayload(message);
  const frame = new Uint8Array(4 + body.length);
  const view = new DataView(frame.buffer);
  view.setUint32(0, body.length, false);
  frame.set(body, 4);
  return frame;
}

export function createFrameDecoder(
  onMessage: (message: SyncMessage) => void,
): (chunk: Uint8Array) => void {
  let buffer = new Uint8Array(0);

  const append = (chunk: Uint8Array): void => {
    const next = new Uint8Array(buffer.length + chunk.length);
    next.set(buffer, 0);
    next.set(chunk, buffer.length);
    buffer = next;
  };

  return (chunk: Uint8Array) => {
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
