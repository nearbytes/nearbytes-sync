import type { SyncMessage } from './types.js';
/** Control plane: compact JSON (no embedded binary). */
export declare const FRAME_KIND_CONTROL = 0;
/** Small event payload (channel + hash + raw bytes). */
export declare const FRAME_KIND_EVENT = 2;
/** Block stream opener: hash + total, then `total` raw bytes on the wire (no per-chunk framing). */
export declare const FRAME_KIND_BLOCK_STREAM_BEGIN = 3;
/** Pump slice when falling back to in-memory block bytes. */
export declare const BLOCK_STREAM_WRITE_SLICE_BYTES: number;
/** After `want`, sender writes this once then pumps `total` raw bytes (continuous stream). */
export declare function encodeBlockStreamBegin(hashHex: string, total: number): Uint8Array;
/** Length-prefixed control / event frames only. Blocks use stream-begin + raw pump. */
export declare function encodeFrame(message: SyncMessage): Uint8Array;
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
export declare function createWireDecoder(handlers: WireDecoderHandlers): WireDecoder;
/** Handshake and other control-only paths (buffers complete block in memory). */
export declare function createFrameDecoder(onMessage: (message: SyncMessage) => void): (chunk: Uint8Array) => void;
//# sourceMappingURL=codec.d.ts.map