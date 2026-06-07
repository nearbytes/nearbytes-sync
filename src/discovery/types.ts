import type { DuplexPeer } from '../core/peerLoop.js';

/**
 * Normalized peer endpoint from any discovery backend.
 *
 * `associationProfile` is the lower-case hex profile public key whose sync
 * topic produced this connection — see `requirements/sync-protocol-v1.md`
 * SYNC-07 and `requirements/sync-discovery-v1.md` DISC-12/24. Per SYNC-00 a
 * node may serve $K \ge 0$ local profiles; `associationProfile` is what
 * lets the upper layer decide which local profile to authenticate as for
 * this specific connection.
 */
export type DiscoveredPeer =
  | {
      readonly transport: 'duplex';
      readonly label: string;
      readonly connect: () => Promise<DuplexPeer>;
      /**
       * True when this process initiated the Hyperswarm connection (outbound
       * dial). False for inbound accepts. Used to keep the inbound leg when
       * NAT would cause our outbound handshake to the same peer to fail.
       */
      readonly locallyInitiated?: boolean;
      /** Set when discovery already knows the remote profile key (e.g. mDNS TXT). */
      readonly profilePublicKey?: string;
      /** Profile owning the topic this connection is on (lower-case hex). */
      readonly associationProfile?: string;
    }
  | {
      readonly transport: 'tcp';
      readonly label: string;
      readonly host: string;
      readonly port: number;
      readonly profilePublicKey: string;
      /** Profile owning the topic this connection is on (lower-case hex). */
      readonly associationProfile: string;
      /**
       * Per-dataDir instance public key of the remote endpoint, learned from
       * the LAN announcement (`sync-discovery-v1.md` DISC-26/27). Used by the
       * sibling-aware TCP dial tiebreaker.
       */
      readonly remotePeerId: string;
      readonly remoteInstancePublicKey: string;
    };

export interface PeerDiscovery {
  start(): Promise<void>;
  onPeer(handler: (peer: DiscoveredPeer) => void): void;
  stop(): Promise<void>;
  /** mDNS only (DISC-24.1): allow outbound re-dial after association close. */
  forgetTcpPeer?(profilePublicKey: string, instancePublicKey: string): void;
}
