import { closeSync, openSync, readSync } from 'node:fs';
import { stat } from 'node:fs/promises';
import { join } from 'path';
import type { Socket } from 'net';
import type { Hash } from 'nearbytes-crypto';
import { blockPath } from 'nearbytes-log';
import { BLOCK_STREAM_WRITE_SLICE_BYTES, encodeBlockStreamBegin } from '../core/codec.js';
import { drainSocket, tryWriteSocket } from './netDuplex.js';

const SLICE = BLOCK_STREAM_WRITE_SLICE_BYTES;
/** Files at/under this size are read fully into memory before writing to the socket. */
const SMALL_FILE_INLINE_BYTES = 256 * 1024 * 1024;

/**
 * Decides which side dials TCP for an LAN-discovered peer.
 *
 * For cross-identity friend pairs (`local.profile != remote.profile`) the
 * lower profile hex dials (same rule as before). For same-identity sibling
 * pairs (`local.profile == remote.profile`, `sync-discovery-v1.md` DISC-26)
 * profile equality cannot break the tie, so we fall through to the
 * `peerId` lex order, which is always distinct because `peerId` is a
 * per-process random 16-byte hex string.
 */
export function shouldInitiateSyncTcp(
  localProfileHex: string,
  remoteProfileHex: string,
  localPeerId: string,
  remotePeerId: string,
): boolean {
  const localProfile = localProfileHex.toLowerCase();
  const remoteProfile = remoteProfileHex.toLowerCase();
  if (localProfile !== remoteProfile) {
    return localProfile < remoteProfile;
  }
  return localPeerId.toLowerCase() < remotePeerId.toLowerCase();
}

export interface PumpResult {
  readonly bytes: number;
  readonly pumpBeginAt: number;
  readonly pumpEndAt: number;
}

/** stream-begin + readSync(16 MiB) + tryWrite/drain (nc-shape, fastest on localhost). */
export async function pumpBlockFileOverSocket(
  socket: Socket,
  dataDir: string,
  hash: string,
): Promise<PumpResult> {
  const abs = join(dataDir, blockPath(hash as Hash));
  const fileSize = (await stat(abs)).size;
  const begin = encodeBlockStreamBegin(hash, fileSize);
  const readFd = openSync(abs, 'r');
  const pumpBeginAt = Date.now();
  try {
    if (!tryWriteSocket(socket, begin)) {
      await drainSocket(socket);
    }
    if (fileSize <= SMALL_FILE_INLINE_BYTES) {
      // Single bulk read avoids per-slice readSync overhead for small/mid files.
      // Cork the socket so begin + body land in the same TCP send batch.
      socket.cork();
      const full = Buffer.allocUnsafe(fileSize);
      let off = 0;
      while (off < fileSize) {
        off += readSync(readFd, full, off, fileSize - off, off);
      }
      if (!tryWriteSocket(socket, full)) {
        socket.uncork();
        await drainSocket(socket);
      } else {
        socket.uncork();
      }
    } else {
      const buf = Buffer.allocUnsafe(SLICE);
      let sent = 0;
      while (sent < fileSize) {
        const need = Math.min(SLICE, fileSize - sent);
        readSync(readFd, buf, 0, need, sent);
        if (!tryWriteSocket(socket, buf.subarray(0, need))) {
          await drainSocket(socket);
        }
        sent += need;
      }
    }
  } finally {
    closeSync(readFd);
  }
  const pumpEndAt = Date.now();
  return { bytes: fileSize, pumpBeginAt, pumpEndAt };
}
