import type { PeerDiscovery } from '../../discovery/types.js';
/**
 * Hyperswarm discovery for friend carriage.
 *
 * `topicToAssociationProfile` maps every joined topic (hex) to the profile
 * whose `topic(profile(p))` produced it — both served local profiles and
 * configured friends are entries in this map. On connection, we read
 * `peerInfo.topics` (the topic intersection with the remote) and map the
 * first match to its associated profile, so the upper layer knows which
 * profile owns this association per `sync-discovery-v1.md` DISC-12.
 *
 * If no topic in the intersection maps to a known profile we fall back to
 * `fallbackAssociationProfile`, which `start.ts` sets to the active served
 * profile so we can still talk on connections we initiated as a follower.
 */
export declare function createHyperswarmDiscovery(options: {
    readonly topics: readonly Uint8Array[];
    readonly topicToAssociationProfile: ReadonlyMap<string, string>;
    readonly fallbackAssociationProfile: string;
}): PeerDiscovery;
//# sourceMappingURL=hyperswarm.d.ts.map