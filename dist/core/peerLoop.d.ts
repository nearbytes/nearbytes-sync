import type { Log } from 'nearbytes-log';
import type { Subject } from './types.js';
export interface DuplexPeer {
    write(chunk: Uint8Array): void;
    onData(handler: (chunk: Uint8Array) => void): void;
    close(): void;
    onClose?(handler: () => void): void;
}
/**
 * Attaches anti-entropy on an association that already completed {@code hello}.
 * {@code subject} MUST be the remote friend's profile subject (SYNC-07).
 */
export declare function attachPeerSession(log: Log, subject: Subject, peer: DuplexPeer): () => void;
//# sourceMappingURL=peerLoop.d.ts.map