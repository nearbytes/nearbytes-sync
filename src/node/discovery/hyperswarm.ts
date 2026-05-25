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

export function createHyperswarmDiscovery(topics: readonly Uint8Array[]): PeerDiscovery {
  const swarm = new Hyperswarm();
  let peerHandler: ((peer: DiscoveredPeer) => void) | null = null;

  swarm.on('connection', (socket: { on(event: string, cb: (...args: unknown[]) => void): void; write(chunk: Uint8Array): void; end(): void }, peerInfo: { publicKey: Buffer }) => {
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
