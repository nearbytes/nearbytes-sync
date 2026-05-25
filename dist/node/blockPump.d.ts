import type { DuplexPeer } from '../core/peerLoop.js';
export interface FromStoragePumpResult {
    readonly bytes: number;
    readonly pumpBeginAt: number;
    readonly pumpEndAt: number;
}
/** readSync + drain pump (same shape as node-tcp-pump-bench). */
export declare function pumpBlockFileFromStorage(dataDir: string, hash: string, peer: DuplexPeer): Promise<FromStoragePumpResult>;
//# sourceMappingURL=blockPump.d.ts.map