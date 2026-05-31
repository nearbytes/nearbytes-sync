import dgram from 'dgram';
import { createServer, type Server, type Socket } from 'net';
import Bonjour from 'bonjour-service';
import {
  LAN_DISCOVERY_SERVICE_PROTOCOL,
  LAN_DISCOVERY_SERVICE_TYPE,
  LAN_MULTICAST_ANNOUNCE_MS,
  LAN_MULTICAST_GROUP,
  LAN_MULTICAST_PORT,
  LAN_TRANSPORT_PROFILE_ID,
  buildLanDiscoveryTxtRecord,
  parseLanDiscoveryTxtRecord,
} from '../../discovery/lanProfile.js';
import type { DuplexPeer } from '../../core/peerLoop.js';
import type { DiscoveredPeer, PeerDiscovery } from '../../discovery/types.js';
import { logPeerSocketError } from '../../logSyncError.js';
import { duplexFromTcpSocket } from '../netDuplex.js';
import { shouldInitiateSyncTcp } from '../tcpBulk.js';

const LAN_MULTICAST_ANNOUNCE_MS_LOCAL = LAN_MULTICAST_ANNOUNCE_MS;

function duplexFromNetSocket(socket: Socket): DuplexPeer {
  const peer = duplexFromTcpSocket(socket);
  socket.on('error', (err) => {
    logPeerSocketError(`mdns-tcp:${socket.remoteAddress}:${socket.remotePort}`, err);
  });
  return peer;
}

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
export function createMdnsDiscovery(options: {
  readonly peerId: string;
  readonly instancePublicKey: string;
  readonly localProfilePublicKeys: readonly string[];
  readonly activeProfilePublicKey: string;
  readonly friendProfileKeys: ReadonlySet<string>;
}): PeerDiscovery {
  const localProfiles = options.localProfilePublicKeys.map((p) => p.toLowerCase());
  const localProfileSet = new Set(localProfiles);
  const activeProfile = options.activeProfilePublicKey.toLowerCase();
  if (!localProfileSet.has(activeProfile)) {
    throw new Error('mDNS: activeProfilePublicKey must be one of localProfilePublicKeys');
  }
  let peerHandler: ((peer: DiscoveredPeer) => void) | null = null;
  let bonjour: InstanceType<typeof Bonjour> | null = null;
  let browser: ReturnType<InstanceType<typeof Bonjour>['find']> | null = null;
  const tcpServers: { profile: string; server: Server; port: number }[] = [];
  let multicastSocket: dgram.Socket | null = null;
  let multicastTimer: ReturnType<typeof setInterval> | null = null;
  const seenTcp = new Set<string>();

  const isAuthorizedRemoteProfile = (profilePublicKey: string): boolean => {
    const key = profilePublicKey.toLowerCase();
    return options.friendProfileKeys.has(key) || localProfileSet.has(key);
  };

  const startListenerForProfile = async (profile: string): Promise<{ server: Server; port: number }> => {
    const server = createServer((socket) => {
      if (!peerHandler) {
        socket.destroy();
        return;
      }
      const duplex = duplexFromNetSocket(socket);
      peerHandler({
        transport: 'duplex',
        label: `mdns-tcp:${socket.remoteAddress}:${socket.remotePort}->${profile.slice(0, 12)}`,
        connect: async () => duplex,
        associationProfile: profile,
      });
    });
    const port = await new Promise<number>((resolve, reject) => {
      server.listen(0, '0.0.0.0', () => {
        const address = server.address();
        if (typeof address === 'object' && address) {
          resolve(address.port);
          return;
        }
        reject(new Error('mDNS: TCP listener address unavailable'));
      });
      server.on('error', reject);
    });
    return { server, port };
  };

  return {
    async start(): Promise<void> {
      for (const profile of localProfiles) {
        const { server, port } = await startListenerForProfile(profile);
        tcpServers.push({ profile, server, port });
      }

      bonjour = new Bonjour();
      for (const entry of tcpServers) {
        const txt = buildLanDiscoveryTxtRecord({
          peerId: options.peerId,
          instancePublicKey: options.instancePublicKey,
          syncPort: entry.port,
          profilePublicKey: entry.profile,
        });
        bonjour.publish({
          name: `nearbytes-${options.peerId.slice(0, 8)}-${entry.profile.slice(0, 8)}`,
          type: LAN_DISCOVERY_SERVICE_TYPE,
          protocol: LAN_DISCOVERY_SERVICE_PROTOCOL,
          port: entry.port,
          txt: txt as unknown as Record<string, string>,
        });
      }

      browser = bonjour.find({
        type: LAN_DISCOVERY_SERVICE_TYPE,
        protocol: LAN_DISCOVERY_SERVICE_PROTOCOL,
      });

      browser.on('up', (service: { addresses?: string[]; txt?: Record<string, unknown> }) => {
        if (!peerHandler || !service.addresses?.length) {
          return;
        }
        const parsed = parseLanDiscoveryTxtRecord((service.txt ?? {}) as Record<string, unknown>);
        if (!parsed || parsed.alpn !== LAN_TRANSPORT_PROFILE_ID) {
          return;
        }
        if (parsed.instancePublicKey === options.instancePublicKey) {
          return;
        }
        if (!isAuthorizedRemoteProfile(parsed.profilePublicKey)) {
          return;
        }
        const isSibling = localProfileSet.has(parsed.profilePublicKey);
        const dialAsProfile = isSibling ? parsed.profilePublicKey : activeProfile;
        if (
          !shouldInitiateSyncTcp(
            dialAsProfile,
            parsed.profilePublicKey,
            options.instancePublicKey,
            parsed.instancePublicKey,
          )
        ) {
          return;
        }
        const host = service.addresses.find((a: string) => !a.includes(':')) ?? service.addresses[0];
        const key = `${parsed.profilePublicKey}:${parsed.instancePublicKey}:${host}:${parsed.syncPort}`;
        if (seenTcp.has(key)) {
          return;
        }
        seenTcp.add(key);
        peerHandler({
          transport: 'tcp',
          label: `mdns:${parsed.peerId}->${parsed.profilePublicKey.slice(0, 12)}`,
          host,
          port: parsed.syncPort,
          profilePublicKey: parsed.profilePublicKey,
          associationProfile: parsed.profilePublicKey,
          remotePeerId: parsed.peerId,
          remoteInstancePublicKey: parsed.instancePublicKey,
        });
      });

      multicastSocket = dgram.createSocket({ type: 'udp4', reuseAddr: true });
      await new Promise<void>((resolve, reject) => {
        multicastSocket!.once('error', reject);
        multicastSocket!.bind(LAN_MULTICAST_PORT, () => resolve());
      });
      multicastSocket.setMulticastTTL(1);
      const payloads = tcpServers.map((entry) => {
        const announcement = JSON.stringify({
          pv: '0.4',
          peer: options.peerId,
          inst: options.instancePublicKey,
          port: entry.port,
          alpn: LAN_TRANSPORT_PROFILE_ID,
          prof: entry.profile,
        });
        return new TextEncoder().encode(announcement);
      });
      multicastTimer = setInterval(() => {
        if (!multicastSocket) return;
        for (const payload of payloads) {
          multicastSocket.send(payload, LAN_MULTICAST_PORT, LAN_MULTICAST_GROUP);
        }
      }, LAN_MULTICAST_ANNOUNCE_MS_LOCAL);
    },

    onPeer(handler): void {
      peerHandler = handler;
    },

    async stop(): Promise<void> {
      if (multicastTimer) {
        clearInterval(multicastTimer);
        multicastTimer = null;
      }
      if (multicastSocket) {
        await new Promise<void>((resolve) => multicastSocket!.close(() => resolve()));
        multicastSocket = null;
      }
      browser?.stop();
      browser = null;
      bonjour?.destroy();
      bonjour = null;
      while (tcpServers.length > 0) {
        const entry = tcpServers.pop()!;
        await new Promise<void>((resolve, reject) => {
          entry.server.close((err) => (err ? reject(err) : resolve()));
        });
      }
    },
  };
}
