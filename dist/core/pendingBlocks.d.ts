import type { Log } from 'nearbytes-log';
export type PendingBlock = {
    readonly total: number;
    readonly parts: Map<number, Uint8Array>;
};
export declare function getPendingBlocks(log: Log): Map<string, PendingBlock>;
//# sourceMappingURL=pendingBlocks.d.ts.map