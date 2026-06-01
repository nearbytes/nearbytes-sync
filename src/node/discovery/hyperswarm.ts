import Hyperswarm from 'hyperswarm';
import { Socket } from 'net';
import type { DuplexPeer } from '../../core/peerLoop.js';
import type { DiscoveredPeer, PeerDiscovery } from '../../discovery/types.js';
import { logPeerSocketError } from '../../logSyncError.js';
import { duplexFromTcpSocket } from '../netDuplex.js';
import { dhtTransportLabel, waitForDhtTransportLabel } from './transportLabel.js';

/**
 * NOTE: socket `'error'` is intentionally NOT handled here — the
 * `Hyperswarm.on('connection')` listener in `createHyperswarmDiscovery`
 * already installs an error handler with the richer `peer:<pubkey>`
 * scope. Installing a second handler here would double every log line
 * for the hyperswarm transport (one with `peer:<hex>` and one with the
 * generic `socket` scope), which is the source of the duplicated
 * `connection timed out` spam observed in the REPL.
 */
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
  let closed = false;
  const emitClose = (): void => {
    if (closed) {
      return;
    }
    closed = true;
    for (const handler of closeHandlers) {
      handler();
    }
  };
  socket.on('data', (buf: unknown) => {
    const raw = buf as Buffer;
    const chunk = raw instanceof Uint8Array ? raw : new Uint8Array(raw);
    for (const handler of handlers) {
      handler(chunk);
    }
  });
  socket.on('close', emitClose);
  socket.on('error', emitClose);
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
      peerInfo: { publicKey: Buffer; topics?: readonly Buffer[]; client?: boolean },
    ) => {
      if (!peerHandler) {
        return;
      }
      socket.on('error', (err: unknown) => {
        const tag = dhtTransportLabel(socket).replace(/^dht:/, '');
        logPeerSocketError(`dht ${tag}`, err);
      });
      const duplex = duplexFromSocket(socket);
      void (async () => {
        let label = dhtTransportLabel(socket);
        if (label === 'dht:unknown') {
          label = await waitForDhtTransportLabel(socket);
        }
        if (!peerHandler) {
          return;
        }
        peerHandler({
          transport: 'duplex',
          label,
          connect: async () => duplex,
          locallyInitiated: peerInfo.client === true,
          associationProfile: pickAssociationProfile(peerInfo.topics),
        });
      })();
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
      /**
       * `Hyperswarm.destroy()` performs DHT `leave` RPCs against every topic
       * we joined and waits for ACKs from bootstrap nodes; that wait is
       * unbounded in practice — we have observed it block indefinitely when
       * peers were flaky or the local DHT routing table was being torn down
       * mid-handshake. The CLI shutdown contract is that
       * `SyncHandle.stop()` returns in bounded time so the dataDir lock can
       * be released; we honour that contract here by racing destroy against
       * a hard budget and abandoning the DHT leaves on timeout.
       *
       * Abandoning leaves is safe: the network already treats every peer
       * disconnect (graceful or not) the same way — DHT entries time out
       * server-side, and any open transport sockets are closed by the OS
       * when the process exits. The only cost of timing out here is one
       * less informative leave RPC; correctness is unaffected.
       */
      const DESTROY_BUDGET_MS = 3000;
      let timer: NodeJS.Timeout | null = null;
      const timeout = new Promise<void>((resolve) => {
        timer = setTimeout(() => {
          process.stderr.write(
            `[nearbytes-sync:hyperswarm] swarm.destroy() did not finish within ` +
              `${DESTROY_BUDGET_MS}ms; abandoning DHT leaves and continuing shutdown\n`,
          );
          resolve();
        }, DESTROY_BUDGET_MS);
        timer.unref();
      });
      try {
        await Promise.race([swarm.destroy(), timeout]);
      } finally {
        if (timer !== null) clearTimeout(timer);
      }
    },
  };
}
