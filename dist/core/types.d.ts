/** Wire-level subject (sync v1). */
export type Subject = {
    readonly kind: 'hub';
    readonly publicKey: string;
} | {
    readonly kind: 'profile';
    readonly publicKey: string;
} | {
    readonly kind: 'dataset';
    readonly publicKey: string;
};
export type ObjectRef = {
    readonly kind: 'block';
    readonly hash: string;
    readonly size?: number;
} | {
    readonly kind: 'event';
    readonly channel: string;
    readonly hash: string;
    readonly blockRefs?: readonly string[];
};
export type SyncMessage = {
    readonly type: 'hello';
    readonly protocol: 'nearbytes.sync.v1';
    readonly subject: Subject;
    readonly sessionNonce: string;
    readonly senderProfile?: string;
} | {
    readonly type: 'delta';
    readonly subject: Subject;
    readonly mode: 'global' | 'hub';
    readonly cursor?: string;
    readonly heads?: readonly ObjectRef[];
    readonly limit?: number;
} | {
    readonly type: 'have';
    readonly subject: Subject;
    readonly fromCursor?: string;
    readonly nextCursor?: string;
    readonly objects: readonly ObjectRef[];
    readonly more: boolean;
} | {
    readonly type: 'want';
    readonly objects: readonly ObjectRef[];
} | {
    readonly type: 'data';
    readonly object: ObjectRef;
    readonly bytes: Uint8Array;
} | {
    readonly type: 'subscribe';
    readonly delta: Extract<SyncMessage, {
        type: 'delta';
    }>;
} | {
    readonly type: 'error';
    readonly code: string;
    readonly detail?: string;
};
//# sourceMappingURL=types.d.ts.map