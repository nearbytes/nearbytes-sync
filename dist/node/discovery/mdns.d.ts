import type { PeerDiscovery } from '../../discovery/types.js';
/**
 * mDNS / DNS-SD discovery for friend carriage with multi-profile support.
 *
 * Per `requirements/sync-discovery-v1.md` DISC-23, a node serving $K \ge 2$
 * local profiles publishes $K$ records with distinct `prof` values, each
 * bound to its own TCP listener so an inbound socket unambiguously identifies
 * the targeted local profile (the `associationProfile` from `DiscoveredPeer`).
 *
 * Outbound dials (DISC-24) target the advertiser's announced `syncPort` and
 * sign the handshake with the **active** served profile.
 */
export declare function createMdnsDiscovery(options: {
    readonly peerId: string;
    readonly localProfilePublicKeys: readonly string[];
    readonly activeProfilePublicKey: string;
    readonly friendProfileKeys: ReadonlySet<string>;
}): PeerDiscovery;
//# sourceMappingURL=mdns.d.ts.map