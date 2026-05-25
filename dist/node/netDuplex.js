export function tuneTcpSocket(socket) {
    socket.setNoDelay(true);
    try {
        const tunable = socket;
        tunable.setSendBufferSize?.(16 * 1024 * 1024);
        tunable.setReceiveBufferSize?.(16 * 1024 * 1024);
    }
    catch {
        /* optional on some platforms */
    }
}
function chunkToBuffer(chunk) {
    if (Buffer.isBuffer(chunk)) {
        return chunk;
    }
    return Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength);
}
export function drainSocket(socket) {
    return new Promise((resolve, reject) => {
        const onDrain = () => {
            socket.off('error', onError);
            resolve();
        };
        const onError = (err) => {
            socket.off('drain', onDrain);
            reject(err);
        };
        socket.once('drain', onDrain);
        socket.once('error', onError);
    });
}
/** Returns true when the chunk is fully queued in the kernel (no drain wait needed). */
export function tryWriteSocket(socket, chunk) {
    return socket.write(chunkToBuffer(chunk));
}
export function writeSocket(socket, chunk) {
    return new Promise((resolve, reject) => {
        const buf = chunkToBuffer(chunk);
        const flushed = socket.write(buf, (err) => {
            if (err) {
                reject(err);
            }
        });
        if (flushed) {
            resolve();
            return;
        }
        drainSocket(socket).then(resolve, reject);
    });
}
/** Pump many chunks; awaits drain only when the send buffer fills. */
export async function writeSocketChunks(socket, chunks) {
    for (const chunk of chunks) {
        if (!tryWriteSocket(socket, chunk)) {
            await drainSocket(socket);
        }
    }
}
export function isTcpDuplexPeer(peer) {
    return 'tcpSocket' in peer && peer.tcpSocket !== undefined;
}
export function duplexFromTcpSocket(socket) {
    tuneTcpSocket(socket);
    const handlers = new Set();
    const closeHandlers = new Set();
    let bulkInbound = null;
    let exclusiveInbound = null;
    socket.on('data', (buf) => {
        if (exclusiveInbound) {
            exclusiveInbound(buf);
            return;
        }
        if (bulkInbound) {
            bulkInbound(buf);
            return;
        }
        for (const handler of handlers) {
            handler(buf);
        }
    });
    socket.on('close', () => {
        for (const handler of closeHandlers) {
            handler();
        }
    });
    return {
        tcpSocket: socket,
        write: (chunk) => {
            void writeSocket(socket, chunk).catch(() => {
                socket.destroy();
            });
        },
        writeAsync: (chunk) => writeSocket(socket, chunk),
        onData: (cb) => {
            handlers.add(cb);
            return () => handlers.delete(cb);
        },
        setBulkInbound: (handler) => {
            bulkInbound = handler;
        },
        setExclusiveInbound: (handler) => {
            exclusiveInbound = handler;
        },
        pauseInbound: () => socket.pause(),
        resumeInbound: () => socket.resume(),
        close: () => socket.destroy(),
        onClose: (cb) => closeHandlers.add(cb),
    };
}
//# sourceMappingURL=netDuplex.js.map