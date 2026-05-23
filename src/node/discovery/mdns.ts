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

const LAN_MULTICAST_ANNOUNCE_MS_LOCAL = LAN_MULTICAST_ANNOUNCE_MS;

function duplexFromNetSocket(socket: Socket): DuplexPeer {
  const handlers = new Set<(chunk: Uint8Array) => void>();
  socket.on('data', (buf: Buffer) => {
    const chunk = buf instanceof Uint8Array ? buf : new Uint8Array(buf);
    for (const handler of handlers) {
      handler(chunk);
    }
  });
  return {
    write: (chunk) => socket.write(chunk),
    onData: (cb) => handlers.add(cb),
    close: () => socket.destroy(),
  };
}

export function createMdnsDiscovery(options: { readonly peerId: string }): PeerDiscovery {
  let peerHandler: ((peer: DiscoveredPeer) => void) | null = null;
  let bonjour: InstanceType<typeof Bonjour> | null = null;
  let browser: ReturnType<InstanceType<typeof Bonjour>['find']> | null = null;
  let tcpServer: Server | null = null;
  let syncPort = 0;
  let multicastSocket: dgram.Socket | null = null;
  let multicastTimer: ReturnType<typeof setInterval> | null = null;
  const seenTcp = new Set<string>();

  return {
    async start(): Promise<void> {
      tcpServer = createServer((socket) => {
        if (!peerHandler) {
          socket.destroy();
          return;
        }
        const duplex = duplexFromNetSocket(socket);
        peerHandler({
          transport: 'duplex',
          label: `mdns-tcp:${socket.remoteAddress}:${socket.remotePort}`,
          connect: async () => duplex,
        });
      });

      await new Promise<void>((resolve, reject) => {
        tcpServer!.listen(0, '0.0.0.0', () => {
          const address = tcpServer!.address();
          if (typeof address === 'object' && address) {
            syncPort = address.port;
          }
          resolve();
        });
        tcpServer!.on('error', reject);
      });

      bonjour = new Bonjour();
      const txt = buildLanDiscoveryTxtRecord({ peerId: options.peerId, syncPort });
      bonjour.publish({
        name: `nearbytes-${options.peerId.slice(0, 8)}`,
        type: LAN_DISCOVERY_SERVICE_TYPE,
        protocol: LAN_DISCOVERY_SERVICE_PROTOCOL,
        port: syncPort,
        txt: txt as unknown as Record<string, string>,
      });

      browser = bonjour.find({
        type: LAN_DISCOVERY_SERVICE_TYPE,
        protocol: LAN_DISCOVERY_SERVICE_PROTOCOL,
      });

      browser.on('up', (service: { addresses?: string[]; txt?: Record<string, unknown> }) => {
        if (!peerHandler || !service.addresses?.length) {
          return;
        }
        const txt = parseLanDiscoveryTxtRecord((service.txt ?? {}) as Record<string, unknown>);
        if (!txt || txt.alpn !== LAN_TRANSPORT_PROFILE_ID) {
          return;
        }
        const host = service.addresses.find((a: string) => !a.includes(':')) ?? service.addresses[0];
        const key = `${host}:${txt.syncPort}`;
        if (seenTcp.has(key)) {
          return;
        }
        seenTcp.add(key);
        peerHandler({
          transport: 'tcp',
          label: `mdns:${txt.peerId}`,
          host,
          port: txt.syncPort,
        });
      });

      multicastSocket = dgram.createSocket({ type: 'udp4', reuseAddr: true });
      await new Promise<void>((resolve, reject) => {
        multicastSocket!.once('error', reject);
        multicastSocket!.bind(LAN_MULTICAST_PORT, () => resolve());
      });
      multicastSocket.setMulticastTTL(1);
      const announcement = JSON.stringify({
        pv: '0.4',
        peer: options.peerId,
        port: syncPort,
        alpn: LAN_TRANSPORT_PROFILE_ID,
      });
      const payload = new TextEncoder().encode(announcement);
      multicastTimer = setInterval(() => {
        multicastSocket?.send(payload, LAN_MULTICAST_PORT, LAN_MULTICAST_GROUP);
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
      await new Promise<void>((resolve, reject) => {
        if (!tcpServer) {
          resolve();
          return;
        }
        tcpServer.close((err) => (err ? reject(err) : resolve()));
      });
      tcpServer = null;
    },
  };
}
