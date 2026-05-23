import Hyperswarm from 'hyperswarm';
import type { DuplexPeer } from '../../core/peerLoop.js';
import type { DiscoveredPeer, PeerDiscovery } from '../../discovery/types.js';

function duplexFromSocket(socket: { on(event: 'data', cb: (buf: Buffer) => void): void; write(chunk: Uint8Array): void; end(): void }): DuplexPeer {
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
    close: () => socket.end(),
  };
}

export function createHyperswarmDiscovery(topics: readonly Uint8Array[]): PeerDiscovery {
  const swarm = new Hyperswarm();
  let peerHandler: ((peer: DiscoveredPeer) => void) | null = null;

  swarm.on('connection', (socket: { on(event: 'data', cb: (buf: Buffer) => void): void; write(chunk: Uint8Array): void; end(): void }, peerInfo: { publicKey: Buffer }) => {
    if (!peerHandler) {
      return;
    }
    const duplex = duplexFromSocket(socket);
    peerHandler({
      transport: 'duplex',
      label: `hyperswarm:${peerInfo.publicKey.toString('hex').slice(0, 12)}`,
      connect: async () => duplex,
    });
  });

  return {
    async start(): Promise<void> {
      for (const topic of topics) {
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
