import type { Socket } from 'net';
import type { DuplexPeer } from '../core/peerLoop.js';

/** Node TCP peer with direct socket access for nc-style bulk block I/O. */
export type TcpDuplexPeer = DuplexPeer & {
  readonly tcpSocket: Socket;
};

export function tuneTcpSocket(socket: Socket): void {
  socket.setNoDelay(true);
  try {
    const tunable = socket as Socket & {
      setSendBufferSize?(size: number): void;
      setReceiveBufferSize?(size: number): void;
    };
    tunable.setSendBufferSize?.(16 * 1024 * 1024);
    tunable.setReceiveBufferSize?.(16 * 1024 * 1024);
  } catch {
    /* optional on some platforms */
  }
}

function chunkToBuffer(chunk: Uint8Array): Buffer {
  if (Buffer.isBuffer(chunk)) {
    return chunk;
  }
  return Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength);
}

export function drainSocket(socket: Socket): Promise<void> {
  return new Promise((resolve, reject) => {
    const onDrain = (): void => {
      socket.off('error', onError);
      resolve();
    };
    const onError = (err: Error): void => {
      socket.off('drain', onDrain);
      reject(err);
    };
    socket.once('drain', onDrain);
    socket.once('error', onError);
  });
}

/** Returns true when the chunk is fully queued in the kernel (no drain wait needed). */
export function tryWriteSocket(socket: Socket, chunk: Uint8Array): boolean {
  return socket.write(chunkToBuffer(chunk));
}

export function writeSocket(socket: Socket, chunk: Uint8Array): Promise<void> {
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
export async function writeSocketChunks(socket: Socket, chunks: readonly Uint8Array[]): Promise<void> {
  for (const chunk of chunks) {
    if (!tryWriteSocket(socket, chunk)) {
      await drainSocket(socket);
    }
  }
}

export function isTcpDuplexPeer(peer: DuplexPeer): peer is TcpDuplexPeer {
  return 'tcpSocket' in peer && peer.tcpSocket !== undefined;
}

export function duplexFromTcpSocket(socket: Socket): TcpDuplexPeer {
  tuneTcpSocket(socket);
  const handlers = new Set<(chunk: Uint8Array) => void>();
  const closeHandlers = new Set<() => void>();
  let bulkInbound: ((chunk: Uint8Array) => void) | null = null;
  let exclusiveInbound: ((chunk: Uint8Array) => void) | null = null;
  socket.on('data', (buf: Buffer) => {
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
