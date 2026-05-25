import type { Log } from 'nearbytes-log';
export interface StartOptions {
    /** Lower-case hex profile public keys this node serves; see `requirements/sync-protocol-v1.md` SYNC-00. */
    readonly serveProfilePublicKeys?: readonly string[];
    /** The served profile used as initiator/follower identity per `sync-discovery-v1.md` DISC-12/24. MUST be in `serveProfilePublicKeys`. */
    readonly activeProfilePublicKey?: string;
    /** Log data directory (`…/data`) for fs block streaming (Node). */
    readonly blockStorageRoot?: string;
    /** `mdns` = LAN TCP only (max throughput on localhost). Default `all` (mDNS + Hyperswarm). */
    readonly discoveryTransport?: 'mdns' | 'all';
}
export interface SyncHandle {
    readonly friends: readonly string[];
    readonly serveProfilePublicKeys: readonly string[];
    stop(): Promise<void>;
}
export declare function start(log: Log, friends: readonly string[], options?: StartOptions): Promise<SyncHandle>;
//# sourceMappingURL=start.d.ts.map