import { type Log } from 'nearbytes-log';
import type { Subject } from './types.js';
export interface DuplexPeer {
    write(chunk: Uint8Array): void;
    /** When set (TCP), honors kernel backpressure instead of buffering unbounded in userspace. */
    writeAsync?(chunk: Uint8Array): Promise<void>;
    /** @returns Unsubscribe (required after handshake so block streams are not decoded twice). */
    onData(handler: (chunk: Uint8Array) => void): () => void;
    /** Node TCP: route raw block-stream bytes without framing (set null to restore). */
    setBulkInbound?(handler: ((chunk: Uint8Array) => void) | null): void;
    /** Node TCP: takes over socket `data` until block stream completes (fast path). */
    setExclusiveInbound?(handler: ((chunk: Uint8Array) => void) | null): void;
    pauseInbound?(): void;
    resumeInbound?(): void;
    close(): void;
    onClose?(handler: () => void): void;
}
/** Wall-clock phase markers (ms since epoch) for a disk block stream. */
export interface DiskBlockStreamPhases {
    /** First byte received from the wire (start of receive). */
    readonly firstByteAt: number | null;
    /** Last byte received from the wire (pure-recv end). */
    readonly lastByteAt: number | null;
    /** All async fs.write() callbacks completed (disk drain end). */
    readonly diskDrainDoneAt: number | null;
    /** Hash digest finalized. */
    readonly hashDoneAt: number;
    /** Tmp→final rename completed. */
    readonly renameDoneAt: number;
}
export interface DiskBlockStreamFinishResult {
    readonly outcome: 'stored' | 'invalid';
    readonly phases: DiskBlockStreamPhases;
}
/** Node-only: stream inbound blocks to disk (see `createNodeDiskBlockStreamFactory`). */
export interface DiskBlockStreamSink {
    readonly total: number;
    readonly received: number;
    ingest(chunk: Uint8Array): void;
    finish(): Promise<DiskBlockStreamFinishResult>;
}
export interface DiskBlockStreamSinkFactory {
    create(hash: string, total: number): DiskBlockStreamSink;
}
export interface AttachPeerSessionOptions {
    /** Filesystem log root (`…/data`) for zero-copy block pump from disk. */
    readonly blockStorageRoot?: string;
    /** When set with {@link blockStorageRoot}, inbound blocks stream to disk instead of RAM. */
    readonly diskBlockStream?: DiskBlockStreamSinkFactory;
}
/**
 * Attaches anti-entropy on an association that already completed {@code hello}.
 * {@code subject} MUST be the remote friend's profile subject (SYNC-07).
 */
export declare function attachPeerSession(log: Log, subject: Subject, peer: DuplexPeer, onPeerClose?: () => void, options?: AttachPeerSessionOptions): () => void;
//# sourceMappingURL=peerLoop.d.ts.map