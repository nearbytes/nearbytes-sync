/** Browser-safe LAN discovery TXT profile (aligned with nearbytes-app LAN transport). */

export const LAN_DISCOVERY_SERVICE_TYPE = 'nearbytes';
export const LAN_DISCOVERY_SERVICE_PROTOCOL = 'udp' as const;
export const LAN_DISCOVERY_PROTOCOL_VERSION = '0.4';
export const LAN_TRANSPORT_PROFILE_ID = 'nearbytes-sync-v1';
export const LAN_TXT_MAX_RECOMMENDED_BYTES = 200;
export const LAN_MULTICAST_GROUP = '239.255.40.41';
export const LAN_MULTICAST_PORT = 40441;
export const LAN_MULTICAST_ANNOUNCE_MS = 5_000;

export interface LanDiscoveryTxtRecord {
  readonly pv: typeof LAN_DISCOVERY_PROTOCOL_VERSION;
  readonly peer: string;
  readonly alpn: typeof LAN_TRANSPORT_PROFILE_ID;
  readonly caps: string;
  readonly syncPort?: string;
  readonly head?: string;
  readonly addr?: string;
}

export function buildLanDiscoveryTxtRecord(input: {
  readonly peerId: string;
  readonly syncPort: number;
  readonly capabilities?: readonly string[];
}): LanDiscoveryTxtRecord {
  const caps = (input.capabilities ?? ['sync-v1', 'global-delta']).join(',');
  return {
    pv: LAN_DISCOVERY_PROTOCOL_VERSION,
    peer: input.peerId.trim(),
    alpn: LAN_TRANSPORT_PROFILE_ID,
    caps,
    syncPort: String(input.syncPort),
  };
}

export function parseLanDiscoveryTxtRecord(
  value: Record<string, unknown>,
): { peerId: string; syncPort: number; alpn: string } | null {
  const peerId = typeof value.peer === 'string' ? value.peer.trim() : '';
  const alpn = typeof value.alpn === 'string' ? value.alpn.trim() : '';
  const syncPortRaw = typeof value.syncPort === 'string' ? value.syncPort.trim() : '';
  const syncPort = Number.parseInt(syncPortRaw, 10);
  if (peerId === '' || alpn === '' || !Number.isFinite(syncPort)) {
    return null;
  }
  return { peerId, syncPort, alpn };
}
