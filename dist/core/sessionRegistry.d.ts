import type { ReceptionObjectRef } from 'nearbytes-log';
export interface LocalHaveAnnouncer {
    pushLocalHave(refs: readonly ReceptionObjectRef[]): void;
}
export declare function registerLocalHaveAnnouncer(announcer: LocalHaveAnnouncer): () => void;
export declare function broadcastLocalHave(refs: readonly ReceptionObjectRef[]): void;
/** After each local reception append, push {@code have} to every open peer session (SYNC-10). */
export declare function patchLogForReactiveHave(log: {
    reception: {
        appendReception: (ref: ReceptionObjectRef) => Promise<string>;
    };
}): void;
//# sourceMappingURL=sessionRegistry.d.ts.map