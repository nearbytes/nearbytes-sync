import { connect as netConnect } from 'net';
function duplexFromNetSocket(socket) {
    const handlers = new Set();
    const closeHandlers = new Set();
    socket.on('data', (buf) => {
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
export async function connectDiscoveredPeer(peer) {
    if (peer.transport === 'duplex') {
        return peer.connect();
    }
    const socket = await new Promise((resolve, reject) => {
        const s = netConnect({ host: peer.host, port: peer.port }, () => resolve(s));
        s.on('error', reject);
    });
    return duplexFromNetSocket(socket);
}
//# sourceMappingURL=connect.js.map