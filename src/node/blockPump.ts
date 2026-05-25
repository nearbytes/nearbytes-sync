import { closeSync, openSync, readSync } from 'fs';
import { stat } from 'fs/promises';
import { join } from 'path';
import type { Hash } from 'nearbytes-crypto';
import { blockPath } from 'nearbytes-log';
import { BLOCK_STREAM_WRITE_SLICE_BYTES, encodeBlockStreamBegin } from '../core/codec.js';
import type { DuplexPeer } from '../core/peerLoop.js';
import { isTcpDuplexPeer } from './netDuplex.js';
import { drainSocket, tryWriteSocket } from './netDuplex.js';

const SLICE = BLOCK_STREAM_WRITE_SLICE_BYTES;

async function writePeerChunk(peer: DuplexPeer, chunk: Uint8Array): Promise<void> {
  if (isTcpDuplexPeer(peer)) {
    if (!tryWriteSocket(peer.tcpSocket, chunk)) {
      await drainSocket(peer.tcpSocket);
    }
    return;
  }
  if (peer.writeAsync) {
    await peer.writeAsync(chunk);
  } else {
    peer.write(chunk);
  }
}

export interface FromStoragePumpResult {
  readonly bytes: number;
  readonly pumpBeginAt: number;
  readonly pumpEndAt: number;
}

/** readSync + drain pump (same shape as node-tcp-pump-bench). */
export async function pumpBlockFileFromStorage(
  dataDir: string,
  hash: string,
  peer: DuplexPeer,
): Promise<FromStoragePumpResult> {
  const abs = join(dataDir, blockPath(hash as Hash));
  const size = (await stat(abs)).size;
  await writePeerChunk(peer, encodeBlockStreamBegin(hash, size));

  const fd = openSync(abs, 'r');
  const buf = Buffer.allocUnsafe(SLICE);
  const pumpBeginAt = Date.now();
  try {
    let sent = 0;
    while (sent < size) {
      const need = Math.min(SLICE, size - sent);
      readSync(fd, buf, 0, need, sent);
      await writePeerChunk(peer, buf.subarray(0, need));
      sent += need;
    }
  } finally {
    closeSync(fd);
  }
  const pumpEndAt = Date.now();
  return { bytes: size, pumpBeginAt, pumpEndAt };
}
