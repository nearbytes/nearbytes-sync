import { connect as netConnect, type Socket } from 'net';
import type { DuplexPeer } from '../core/peerLoop.js';
import type { DiscoveredPeer } from '../discovery/types.js';

function duplexFromNetSocket(socket: Socket): DuplexPeer {
  const handlers = new Set<(chunk: Uint8Array) => void>();
  const closeHandlers = new Set<() => void>();
  socket.on('data', (buf: Buffer) => {
    const chunk = buf instanceof Uint8Array ? buf : new Uint8Array(buf);
    for (const handler of handlers) {
      handler(chunk);
    }
  });
  socket.on('close', () => {
    for (const handler of closeHandlers) {
      handler();
    }
  });
  return {
    write: (chunk) => socket.write(chunk),
    onData: (cb) => handlers.add(cb),
    close: () => socket.destroy(),
    onClose: (cb) => closeHandlers.add(cb),
  };
}

export async function connectDiscoveredPeer(peer: DiscoveredPeer): Promise<DuplexPeer> {
  if (peer.transport === 'duplex') {
    return peer.connect();
  }
  const socket = await new Promise<Socket>((resolve, reject) => {
    const s = netConnect({ host: peer.host, port: peer.port }, () => resolve(s));
    s.on('error', reject);
  });
  return duplexFromNetSocket(socket);
}
