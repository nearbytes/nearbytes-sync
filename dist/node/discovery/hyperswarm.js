import Hyperswarm from 'hyperswarm';
import { Socket } from 'net';
import { logSyncError } from '../../logSyncError.js';
import { duplexFromTcpSocket } from '../netDuplex.js';
function duplexFromSocket(socket) {
    if (socket instanceof Socket) {
        return duplexFromTcpSocket(socket);
    }
    const handlers = new Set();
    const closeHandlers = new Set();
    socket.on('data', (buf) => {
        const raw = buf;
        const chunk = raw instanceof Uint8Array ? raw : new Uint8Array(raw);
        for (const handler of handlers) {
            handler(chunk);
        }
    });
    socket.on('error', (err) => {
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
export function createHyperswarmDiscovery(topics) {
    const swarm = new Hyperswarm();
    let peerHandler = null;
    swarm.on('connection', (socket, peerInfo) => {
        if (!peerHandler) {
            return;
        }
        socket.on('error', (err) => {
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
        async start() {
            for (const topic of topics) {
                await swarm.join(Buffer.from(topic), { client: true, server: true });
            }
        },
        onPeer(handler) {
            peerHandler = handler;
        },
        async stop() {
            await swarm.destroy();
        },
    };
}
//# sourceMappingURL=hyperswarm.js.map