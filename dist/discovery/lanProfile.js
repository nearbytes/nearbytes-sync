/** Browser-safe LAN discovery TXT profile (aligned with nearbytes-app LAN transport). */
export const LAN_DISCOVERY_SERVICE_TYPE = 'nearbytes';
export const LAN_DISCOVERY_SERVICE_PROTOCOL = 'udp';
export const LAN_DISCOVERY_PROTOCOL_VERSION = '0.4';
export const LAN_TRANSPORT_PROFILE_ID = 'nearbytes-sync-v1';
export const LAN_TXT_MAX_RECOMMENDED_BYTES = 400;
export const LAN_MULTICAST_GROUP = '239.255.40.41';
export const LAN_MULTICAST_PORT = 40441;
export const LAN_MULTICAST_ANNOUNCE_MS = 5_000;
export function buildLanDiscoveryTxtRecord(input) {
    const caps = (input.capabilities ?? ['sync-v1', 'global-delta']).join(',');
    return {
        pv: LAN_DISCOVERY_PROTOCOL_VERSION,
        peer: input.peerId.trim(),
        alpn: LAN_TRANSPORT_PROFILE_ID,
        caps,
        syncPort: String(input.syncPort),
        prof: input.profilePublicKey.trim().toLowerCase(),
    };
}
export function parseLanDiscoveryTxtRecord(value) {
    const peerId = typeof value.peer === 'string' ? value.peer.trim() : '';
    const alpn = typeof value.alpn === 'string' ? value.alpn.trim() : '';
    const syncPortRaw = typeof value.syncPort === 'string' ? value.syncPort.trim() : '';
    const profilePublicKey = typeof value.prof === 'string' ? value.prof.trim().toLowerCase() : '';
    const syncPort = Number.parseInt(syncPortRaw, 10);
    if (peerId === '' || alpn === '' || !Number.isFinite(syncPort) || profilePublicKey === '') {
        return null;
    }
    return { peerId, syncPort, alpn, profilePublicKey };
}
//# sourceMappingURL=lanProfile.js.map