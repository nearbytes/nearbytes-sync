import type { DuplexPeer } from './peerLoop.js';
import type { Subject } from './types.js';
export interface FriendHandshakeOptions {
    readonly localProfilePublicKey: string;
    readonly subject: Subject;
    readonly allowedRemoteProfiles: ReadonlySet<string>;
    readonly timeoutMs?: number;
}
/**
 * Exchanges {@code hello} on a new duplex before anti-entropy.
 * Resolves with the verified remote profile public key (lower-case hex).
 */
export declare function exchangeFriendHandshake(peer: DuplexPeer, options: FriendHandshakeOptions): Promise<string>;
//# sourceMappingURL=handshake.d.ts.map