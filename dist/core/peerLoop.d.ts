import type { Log } from 'nearbytes-log';
import type { Subject } from './types.js';
export interface DuplexPeer {
    write(chunk: Uint8Array): void;
    onData(handler: (chunk: Uint8Array) => void): void;
    close(): void;
}
export declare function attachPeerSession(log: Log, subject: Subject, peer: DuplexPeer): void;
//# sourceMappingURL=peerLoop.d.ts.map