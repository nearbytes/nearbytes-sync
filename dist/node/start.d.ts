import type { Log } from 'nearbytes-log';
export interface SyncHandle {
    readonly friends: readonly string[];
    stop(): Promise<void>;
}
export declare function start(log: Log, friends: readonly string[]): Promise<SyncHandle>;
//# sourceMappingURL=start.d.ts.map