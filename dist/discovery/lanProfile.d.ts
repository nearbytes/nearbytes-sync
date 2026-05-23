/** Browser-safe LAN discovery TXT profile (aligned with nearbytes-app LAN transport). */
export declare const LAN_DISCOVERY_SERVICE_TYPE = "nearbytes";
export declare const LAN_DISCOVERY_SERVICE_PROTOCOL: "udp";
export declare const LAN_DISCOVERY_PROTOCOL_VERSION = "0.4";
export declare const LAN_TRANSPORT_PROFILE_ID = "nearbytes-sync-v1";
export declare const LAN_TXT_MAX_RECOMMENDED_BYTES = 200;
export declare const LAN_MULTICAST_GROUP = "239.255.40.41";
export declare const LAN_MULTICAST_PORT = 40441;
export declare const LAN_MULTICAST_ANNOUNCE_MS = 5000;
export interface LanDiscoveryTxtRecord {
    readonly pv: typeof LAN_DISCOVERY_PROTOCOL_VERSION;
    readonly peer: string;
    readonly alpn: typeof LAN_TRANSPORT_PROFILE_ID;
    readonly caps: string;
    readonly syncPort?: string;
    readonly head?: string;
    readonly addr?: string;
}
export declare function buildLanDiscoveryTxtRecord(input: {
    readonly peerId: string;
    readonly syncPort: number;
    readonly capabilities?: readonly string[];
}): LanDiscoveryTxtRecord;
export declare function parseLanDiscoveryTxtRecord(value: Record<string, unknown>): {
    peerId: string;
    syncPort: number;
    alpn: string;
} | null;
//# sourceMappingURL=lanProfile.d.ts.map