import type { Log } from 'nearbytes-log';
import { type AttachPeerSessionOptions, type DuplexPeer } from './peerLoop.js';
export interface FriendSessionEntry {
    readonly remoteProfilePublicKey: string;
    readonly transportLabel: string;
    readonly stop: () => void;
    close(): void;
    isAlive(): boolean;
}
/**
 * One active framed sync association per remote friend profile key (SYNC-06).
 * Duplicate inbound connections are dropped so Hyperswarm flaps do not replace a live mDNS session.
 */
export declare class FriendSessionRegistry {
    private readonly sessions;
    attach(log: Log, remoteProfilePublicKey: string, peer: DuplexPeer, sessionOptions?: AttachPeerSessionOptions, transportLabel?: string): {
        readonly entry: FriendSessionEntry;
        readonly created: boolean;
    };
    closeAll(): void;
}
//# sourceMappingURL=friendSessions.d.ts.map