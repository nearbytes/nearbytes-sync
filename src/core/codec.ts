import { createHash } from 'node:crypto';
import { bytesToHex, hexToBytes } from 'nearbytes-crypto';
import type { SyncMessage } from './types.js';

/** Control plane: compact JSON (no embedded binary). */
export const FRAME_KIND_CONTROL = 0;
/** Small event payload (channel + hash + raw bytes). */
export const FRAME_KIND_EVENT = 2;
/** Block stream opener: hash + total, then `total` raw bytes on the wire (no per-chunk framing). */
export const FRAME_KIND_BLOCK_STREAM_BEGIN = 3;
/** Block chunk payload for transports that need multiplex-safe block delivery. */
export const FRAME_KIND_BLOCK_CHUNK = 4;

const STREAM_BEGIN_BODY_BYTES = 32 + 8;
const BLOCK_CHUNK_HEADER_BYTES = 32 + 8 + 8 + 4;

/** Pump slice when falling back to in-memory block bytes. */
export const BLOCK_STREAM_WRITE_SLICE_BYTES = 4 * 1024 * 1024;

function readU32BE(view: DataView, offset: number): number {
  return view.getUint32(offset, false);
}

function readU64BE(view: DataView, offset: number): number {
  const hi = view.getUint32(offset, false);
  const lo = view.getUint32(offset + 4, false);
  return hi * 0x1_0000_0000 + lo;
}

function writeU64BE(view: DataView, offset: number, value: number): void {
  const hi = Math.floor(value / 0x1_0000_0000);
  const lo = value >>> 0;
  view.setUint32(offset, hi, false);
  view.setUint32(offset + 4, lo, false);
}

function writeU32BE(view: DataView, offset: number, value: number): void {
  view.setUint32(offset, value >>> 0, false);
}

function wrapLengthPrefixed(body: Uint8Array): Uint8Array {
  const frame = new Uint8Array(4 + body.length);
  new DataView(frame.buffer).setUint32(0, body.length, false);
  frame.set(body, 4);
  return frame;
}

function encodeControl(message: SyncMessage): Uint8Array {
  if (message.type === 'data') {
    throw new Error('control frame cannot carry data messages');
  }
  const json = new TextEncoder().encode(JSON.stringify(message));
  const body = new Uint8Array(1 + json.length);
  body[0] = FRAME_KIND_CONTROL;
  body.set(json, 1);
  return wrapLengthPrefixed(body);
}

/** After `want`, sender writes this once then pumps `total` raw bytes (continuous stream). */
export function encodeBlockStreamBegin(hashHex: string, total: number): Uint8Array {
  const hashBytes = hexToBytes(hashHex);
  if (hashBytes.length !== 32) {
    throw new Error(`block hash must be 32 bytes, got ${hashBytes.length}`);
  }
  const body = new Uint8Array(1 + STREAM_BEGIN_BODY_BYTES);
  body[0] = FRAME_KIND_BLOCK_STREAM_BEGIN;
  body.set(hashBytes, 1);
  writeU64BE(new DataView(body.buffer, body.byteOffset, body.byteLength), 33, total);
  return wrapLengthPrefixed(body);
}

function encodeEventData(message: Extract<SyncMessage, { type: 'data' }>): Uint8Array {
  if (message.object.kind !== 'event') {
    throw new Error('encodeEventData requires event object');
  }
  const channelBytes = new TextEncoder().encode(message.object.channel);
  const hashBytes = hexToBytes(message.object.hash);
  if (hashBytes.length !== 32) {
    throw new Error(`event hash must be 32 bytes, got ${hashBytes.length}`);
  }
  const payload = message.bytes;
  const body = new Uint8Array(1 + 2 + channelBytes.length + 32 + 4 + payload.byteLength);
  let o = 0;
  body[o++] = FRAME_KIND_EVENT;
  body[o++] = (channelBytes.length >> 8) & 0xff;
  body[o++] = channelBytes.length & 0xff;
  body.set(channelBytes, o);
  o += channelBytes.length;
  body.set(hashBytes, o);
  o += 32;
  writeU32BE(new DataView(body.buffer, body.byteOffset, body.byteLength), o, payload.byteLength);
  o += 4;
  body.set(payload, o);
  return wrapLengthPrefixed(body);
}

function encodeBlockChunk(message: Extract<SyncMessage, { type: 'data' }>): Uint8Array {
  if (message.object.kind !== 'block') {
    throw new Error('encodeBlockChunk requires block object');
  }
  if (message.offset === undefined || message.total === undefined) {
    throw new Error('block chunk requires offset and total');
  }
  const hashBytes = hexToBytes(message.object.hash);
  if (hashBytes.length !== 32) {
    throw new Error(`block hash must be 32 bytes, got ${hashBytes.length}`);
  }
  const payload = message.bytes;
  const body = new Uint8Array(1 + BLOCK_CHUNK_HEADER_BYTES + payload.byteLength);
  body[0] = FRAME_KIND_BLOCK_CHUNK;
  body.set(hashBytes, 1);
  const view = new DataView(body.buffer, body.byteOffset, body.byteLength);
  writeU64BE(view, 33, message.offset);
  writeU64BE(view, 41, message.total);
  writeU32BE(view, 49, payload.byteLength);
  body.set(payload, 53);
  return wrapLengthPrefixed(body);
}

/** Length-prefixed control / event frames only. Blocks use stream-begin + raw pump. */
export function encodeFrame(message: SyncMessage): Uint8Array {
  if (message.type === 'data') {
    if (message.object.kind !== 'event') {
      return encodeBlockChunk(message);
    }
    return encodeEventData(message);
  }
  return encodeControl(message);
}

export interface WireDecoderHandlers {
  readonly onMessage: (message: SyncMessage) => void;
  /** @deprecated Legacy path; prefer begin + bytes hooks. */
  readonly onBlockStream?: (hash: string, bytes: Uint8Array) => void;
  readonly onBlockStreamBegin?: (hash: string, total: number) => void;
  readonly onBlockStreamBytes?: (chunk: Uint8Array) => void;
  /** When true, raw bytes after stream-begin are delivered only via peer bulk routing (not feedStream). */
  readonly peerBulkBlockStream?: boolean;
}

/**
 * Control frames are length-prefixed; block bodies are a continuous `total`-byte run (SYNC-33).
 */
export interface WireDecoder {
  (chunk: Uint8Array): void;
  /** Reset block-stream parser state after the receiver has consumed `total` bytes. */
  endBlockStream(): void;
}

export function createWireDecoder(handlers: WireDecoderHandlers): WireDecoder {
  let frameBuf = new Uint8Array(0);
  let streamTotal = 0;
  let streamReceived = 0;

  const appendFrames = (chunk: Uint8Array): void => {
    if (frameBuf.length === 0) {
      frameBuf = new Uint8Array(chunk);
    } else {
      const next = new Uint8Array(frameBuf.length + chunk.length);
      next.set(frameBuf, 0);
      next.set(chunk, frameBuf.length);
      frameBuf = next;
    }
  };

  const decodeControl = (body: Uint8Array): SyncMessage => {
    const json = body.subarray(1);
    return JSON.parse(new TextDecoder().decode(json)) as SyncMessage;
  };

  const decodeEvent = (body: Uint8Array): SyncMessage => {
    if (body.length < 1 + 2 + 32 + 4) {
      throw new Error('event frame too short');
    }
    const channelLen = (body[1]! << 8) | body[2]!;
    const channelStart = 3;
    const hashStart = channelStart + channelLen;
    const payloadLenStart = hashStart + 32;
    const view = new DataView(body.buffer, body.byteOffset, body.byteLength);
    const payloadLen = readU32BE(view, payloadLenStart);
    const payloadStart = payloadLenStart + 4;
    if (body.length < payloadStart + payloadLen) {
      throw new Error('event frame truncated');
    }
    const channel = new TextDecoder().decode(body.subarray(channelStart, hashStart));
    const hash = bytesToHex(body.subarray(hashStart, hashStart + 32));
    const bytes = body.subarray(payloadStart, payloadStart + payloadLen);
    return { type: 'data', object: { kind: 'event', channel, hash }, bytes };
  };

  const decodeBlockChunk = (body: Uint8Array): SyncMessage => {
    if (body.length < 1 + BLOCK_CHUNK_HEADER_BYTES) {
      throw new Error('block chunk frame too short');
    }
    const view = new DataView(body.buffer, body.byteOffset, body.byteLength);
    const hash = bytesToHex(body.subarray(1, 33));
    const offset = readU64BE(view, 33);
    const total = readU64BE(view, 41);
    const payloadLen = readU32BE(view, 49);
    const payloadStart = 53;
    if (body.length < payloadStart + payloadLen) {
      throw new Error('block chunk frame truncated');
    }
    return {
      type: 'data',
      object: { kind: 'block', hash },
      bytes: body.subarray(payloadStart, payloadStart + payloadLen),
      offset,
      total,
    };
  };

  const parseFrames = (): void => {
    while (frameBuf.length >= 4) {
      const size = new DataView(frameBuf.buffer, frameBuf.byteOffset, frameBuf.byteLength).getUint32(
        0,
        false,
      );
      if (frameBuf.length < 4 + size) {
        return;
      }
      const body = frameBuf.subarray(4, 4 + size);
      const tail = frameBuf.subarray(4 + size);
      frameBuf = tail.length === 0 ? new Uint8Array(0) : new Uint8Array(tail);
      const kind = body[0];
      if (kind === FRAME_KIND_BLOCK_STREAM_BEGIN) {
        if (body.length < 1 + STREAM_BEGIN_BODY_BYTES) {
          throw new Error('block stream begin truncated');
        }
        const hash = bytesToHex(body.subarray(1, 33));
        const total = readU64BE(new DataView(body.buffer, body.byteOffset, body.byteLength), 33);
        if (handlers.onBlockStreamBytes) {
          streamTotal = total;
          streamReceived = 0;
          handlers.onBlockStreamBegin?.(hash, streamTotal);
          if (frameBuf.length > 0) {
            const pending = frameBuf;
            frameBuf = new Uint8Array(0);
            let rest = pending;
            while (rest.length > 0 && streamReceived < streamTotal) {
              const need = streamTotal - streamReceived;
              const take = Math.min(need, rest.length);
              handlers.onBlockStreamBytes(rest.subarray(0, take));
              streamReceived += take;
              rest = rest.subarray(take);
            }
            if (rest.length > 0) {
              frameBuf = new Uint8Array(rest);
            }
          }
        } else {
          streamTotal = total;
          streamReceived = 0;
          handlers.onBlockStreamBegin?.(hash, streamTotal);
        }
        continue;
      }
      if (kind === FRAME_KIND_EVENT) {
        handlers.onMessage(decodeEvent(body));
        continue;
      }
      if (kind === FRAME_KIND_BLOCK_CHUNK) {
        handlers.onMessage(decodeBlockChunk(body));
        continue;
      }
      if (kind === FRAME_KIND_CONTROL) {
        handlers.onMessage(decodeControl(body));
        continue;
      }
      throw new Error(`unknown frame kind: ${kind}`);
    }
  };

  const feedStream = (chunk: Uint8Array): void => {
    let rest = chunk;
    while (rest.length > 0 && streamReceived < streamTotal) {
      const need = streamTotal - streamReceived;
      const take = Math.min(need, rest.length);
      const slice = rest.subarray(0, take);
      handlers.onBlockStreamBytes?.(slice);
      streamReceived += take;
      rest = rest.subarray(take);
    }
    if (rest.length > 0) {
      appendFrames(rest);
      parseFrames();
    }
  };

  const endBlockStream = (): void => {
    streamTotal = 0;
    streamReceived = 0;
  };

  const decode = (chunk: Uint8Array): void => {
    if (streamTotal > 0 && streamReceived < streamTotal) {
      feedStream(chunk);
      return;
    }
    appendFrames(chunk);
    parseFrames();
  };

  return Object.assign(decode, { endBlockStream });
}

/** Handshake and other control-only paths (buffers complete block in memory). */
export function createFrameDecoder(onMessage: (message: SyncMessage) => void): (chunk: Uint8Array) => void {
  let active: {
    hash: string;
    total: number;
    received: number;
    buffer: Uint8Array;
    hasher: ReturnType<typeof createHash>;
  } | null = null;

  return createWireDecoder({
    onMessage,
    onBlockStreamBegin: (hash, total) => {
      active = {
        hash,
        total,
        received: 0,
        buffer: new Uint8Array(total),
        hasher: createHash('sha256'),
      };
    },
    onBlockStreamBytes: (slice) => {
      if (!active) {
        return;
      }
      active.hasher.update(slice);
      active.buffer.set(slice, active.received);
      active.received += slice.byteLength;
      if (active.received === active.total) {
        const digest = active.hasher.digest('hex');
        if (digest !== active.hash.toLowerCase()) {
          throw new Error('block stream hash mismatch');
        }
        onMessage({ type: 'data', object: { kind: 'block', hash: active.hash }, bytes: active.buffer });
        active = null;
      }
    },
  });
}
