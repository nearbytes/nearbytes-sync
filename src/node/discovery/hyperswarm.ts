import Hyperswarm from 'hyperswarm';
import { Socket } from 'net';
import type { DuplexPeer } from '../../core/peerLoop.js';
import type { DiscoveredPeer, PeerDiscovery } from '../../discovery/types.js';
import { logSyncError } from '../../logSyncError.js';
import { duplexFromTcpSocket } from '../netDuplex.js';

function duplexFromSocket(socket: {
  on(event: string, cb: (...args: unknown[]) => void): void;
  write(chunk: Uint8Array): void;
  end(): void;
}): DuplexPeer {
  if (socket instanceof Socket) {
    return duplexFromTcpSocket(socket);
  }
  const handlers = new Set<(chunk: Uint8Array) => void>();
  const closeHandlers = new Set<() => void>();
  socket.on('data', (buf: unknown) => {
    const raw = buf as Buffer;
    const chunk = raw instanceof Uint8Array ? raw : new Uint8Array(raw);
    for (const handler of handlers) {
      handler(chunk);
    }
  });
  socket.on('error', (err: unknown) => {
    logSyncError('hyperswarm socket', err);
  });
  socket.on('close', () => {
    for (const handler of closeHandlers) {
      handler();
    }
  });
  return {
    write: (chunk) => socket.write(chunk),
    onData: (cb) => {
      handlers.add(cb);
      return () => handlers.delete(cb);
    },
    setBulkInbound: undefined,
    close: () => socket.end(),
    onClose: (cb) => closeHandlers.add(cb),
  };
}

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
export function createHyperswarmDiscovery(options: {
  readonly topics: readonly Uint8Array[];
  readonly topicToAssociationProfile: ReadonlyMap<string, string>;
  readonly fallbackAssociationProfile: string;
}): PeerDiscovery {
  const swarm = new Hyperswarm();
  let peerHandler: ((peer: DiscoveredPeer) => void) | null = null;

  const pickAssociationProfile = (peerTopics: readonly Buffer[] | undefined): string => {
    if (peerTopics) {
      for (const topic of peerTopics) {
        const hex = topic.toString('hex');
        const profile = options.topicToAssociationProfile.get(hex);
        if (profile) {
          return profile;
        }
      }
    }
    return options.fallbackAssociationProfile;
  };

  swarm.on(
    'connection',
    (
      socket: { on(event: string, cb: (...args: unknown[]) => void): void; write(chunk: Uint8Array): void; end(): void },
      peerInfo: { publicKey: Buffer; topics?: readonly Buffer[] },
    ) => {
      if (!peerHandler) {
        return;
      }
      socket.on('error', (err: unknown) => {
        logSyncError(`hyperswarm peer:${peerInfo.publicKey.toString('hex').slice(0, 12)}`, err);
      });
      const duplex = duplexFromSocket(socket);
      peerHandler({
        transport: 'duplex',
        label: `hyperswarm:${peerInfo.publicKey.toString('hex').slice(0, 12)}`,
        connect: async () => duplex,
        associationProfile: pickAssociationProfile(peerInfo.topics),
      });
    },
  );

  return {
    async start(): Promise<void> {
      for (const topic of options.topics) {
        await swarm.join(Buffer.from(topic), { client: true, server: true });
      }
    },
    onPeer(handler): void {
      peerHandler = handler;
    },
    async stop(): Promise<void> {
      await swarm.destroy();
    },
  };
}
