import type { SyncMessage } from './types.js';
/** Length-prefixed JSON frames (browser and Node safe). */
export declare function encodeFrame(message: SyncMessage): Uint8Array;
export declare function createFrameDecoder(onMessage: (message: SyncMessage) => void): (chunk: Uint8Array) => void;
//# sourceMappingURL=codec.d.ts.map