import { connect as netConnect, type Socket } from 'net';
import type { DuplexPeer } from '../core/peerLoop.js';
import type { DiscoveredPeer } from '../discovery/types.js';
import { duplexFromTcpSocket } from './netDuplex.js';

export async function connectDiscoveredPeer(peer: DiscoveredPeer): Promise<DuplexPeer> {
  if (peer.transport === 'duplex') {
    return peer.connect();
  }
  const socket = await new Promise<Socket>((resolve, reject) => {
    const s = netConnect({ host: peer.host, port: peer.port }, () => resolve(s));
    s.on('error', reject);
  });
  return duplexFromTcpSocket(socket);
}
