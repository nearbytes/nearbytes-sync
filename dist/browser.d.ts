export type { Subject, ObjectRef, SyncMessage } from './core/types.js';
export { syncTopic, profileSubject } from './core/topic.js';
export { encodeFrame, createFrameDecoder } from './core/codec.js';
export { acceptData, type AcceptResult } from './core/acceptData.js';
export type { DuplexPeer } from './core/peerLoop.js';
export { attachPeerSession } from './core/peerLoop.js';
export { LAN_DISCOVERY_PROTOCOL_VERSION, LAN_TRANSPORT_PROFILE_ID, buildLanDiscoveryTxtRecord, parseLanDiscoveryTxtRecord, } from './discovery/lanProfile.js';
export type { DiscoveredPeer, PeerDiscovery } from './discovery/types.js';
export { createCompositeDiscovery } from './discovery/composite.js';
//# sourceMappingURL=browser.d.ts.map