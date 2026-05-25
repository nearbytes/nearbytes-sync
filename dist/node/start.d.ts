import type { Log } from 'nearbytes-log';
export interface StartOptions {
    /** Join this profile subject so followers can sync with you (your public key hex). */
    readonly serveProfilePublicKey?: string;
    /** Log data directory (`…/data`) for fs block streaming (Node). */
    readonly blockStorageRoot?: string;
    /** `mdns` = LAN TCP only (max throughput on localhost). Default `all` (mDNS + Hyperswarm). */
    readonly discoveryTransport?: 'mdns' | 'all';
}
export interface SyncHandle {
    readonly friends: readonly string[];
    stop(): Promise<void>;
}
export declare function start(log: Log, friends: readonly string[], options?: StartOptions): Promise<SyncHandle>;
//# sourceMappingURL=start.d.ts.map