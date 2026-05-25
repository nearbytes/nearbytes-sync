import { connect as netConnect } from 'net';
import { duplexFromTcpSocket } from './netDuplex.js';
export async function connectDiscoveredPeer(peer) {
    if (peer.transport === 'duplex') {
        return peer.connect();
    }
    const socket = await new Promise((resolve, reject) => {
        const s = netConnect({ host: peer.host, port: peer.port }, () => resolve(s));
        s.on('error', reject);
    });
    return duplexFromTcpSocket(socket);
}
//# sourceMappingURL=connect.js.map