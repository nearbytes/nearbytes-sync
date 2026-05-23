import type { Log } from 'nearbytes-log';
export interface StartOptions {
    /** Join this profile subject so followers can sync with you (your public key hex). */
    readonly serveProfilePublicKey?: string;
}
export interface SyncHandle {
    readonly friends: readonly string[];
    stop(): Promise<void>;
}
export declare function start(log: Log, friends: readonly string[], options?: StartOptions): Promise<SyncHandle>;
//# sourceMappingURL=start.d.ts.map