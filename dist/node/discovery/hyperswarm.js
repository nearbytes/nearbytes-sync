import Hyperswarm from 'hyperswarm';
function duplexFromSocket(socket) {
    const handlers = new Set();
    socket.on('data', (buf) => {
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
export function createHyperswarmDiscovery(topics) {
    const swarm = new Hyperswarm();
    let peerHandler = null;
    swarm.on('connection', (socket, peerInfo) => {
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