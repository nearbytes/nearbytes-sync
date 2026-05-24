import type { DuplexPeer } from '../core/peerLoop.js';
/** Normalized peer endpoint from any discovery backend. */
export type DiscoveredPeer = {
    readonly transport: 'duplex';
    readonly label: string;
    readonly connect: () => Promise<DuplexPeer>;
    /** Set when discovery already knows the remote profile key (e.g. mDNS TXT). */
    readonly profilePublicKey?: string;
} | {
    readonly transport: 'tcp';
    readonly label: string;
    readonly host: string;
    readonly port: number;
    readonly profilePublicKey: string;
};
export interface PeerDiscovery {
    start(): Promise<void>;
    onPeer(handler: (peer: DiscoveredPeer) => void): void;
    stop(): Promise<void>;
}
//# sourceMappingURL=types.d.ts.map