import type { Socket } from 'net';
import type { DuplexPeer } from '../core/peerLoop.js';
/** Node TCP peer with direct socket access for nc-style bulk block I/O. */
export type TcpDuplexPeer = DuplexPeer & {
    readonly tcpSocket: Socket;
};
export declare function tuneTcpSocket(socket: Socket): void;
export declare function drainSocket(socket: Socket): Promise<void>;
/** Returns true when the chunk is fully queued in the kernel (no drain wait needed). */
export declare function tryWriteSocket(socket: Socket, chunk: Uint8Array): boolean;
export declare function writeSocket(socket: Socket, chunk: Uint8Array): Promise<void>;
/** Pump many chunks; awaits drain only when the send buffer fills. */
export declare function writeSocketChunks(socket: Socket, chunks: readonly Uint8Array[]): Promise<void>;
export declare function isTcpDuplexPeer(peer: DuplexPeer): peer is TcpDuplexPeer;
export declare function duplexFromTcpSocket(socket: Socket): TcpDuplexPeer;
//# sourceMappingURL=netDuplex.d.ts.map