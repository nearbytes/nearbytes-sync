import type { PeerDiscovery } from '../../discovery/types.js';
export declare function createMdnsDiscovery(options: {
    readonly peerId: string;
    readonly profilePublicKey: string;
    readonly friendProfileKeys: ReadonlySet<string>;
}): PeerDiscovery;
//# sourceMappingURL=mdns.d.ts.map