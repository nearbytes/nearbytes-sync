import { closeSync, openSync, readSync } from 'node:fs';
import { stat } from 'node:fs/promises';
import { join } from 'path';
import { blockPath } from 'nearbytes-log';
import { BLOCK_STREAM_WRITE_SLICE_BYTES, encodeBlockStreamBegin } from '../core/codec.js';
import { drainSocket, tryWriteSocket } from './netDuplex.js';
const SLICE = BLOCK_STREAM_WRITE_SLICE_BYTES;
/** Files at/under this size are read fully into memory before writing to the socket. */
const SMALL_FILE_INLINE_BYTES = 256 * 1024 * 1024;
/** Lower profile hex initiates outbound TCP (one session per friend pair). */
export function shouldInitiateSyncTcp(localProfileHex, remoteProfileHex) {
    return localProfileHex.toLowerCase() < remoteProfileHex.toLowerCase();
}
/** stream-begin + readSync(16 MiB) + tryWrite/drain (nc-shape, fastest on localhost). */
export async function pumpBlockFileOverSocket(socket, dataDir, hash) {
    const abs = join(dataDir, blockPath(hash));
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
            }
            else {
                socket.uncork();
            }
        }
        else {
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
    }
    finally {
        closeSync(readFd);
    }
    const pumpEndAt = Date.now();
    return { bytes: fileSize, pumpBeginAt, pumpEndAt };
}
//# sourceMappingURL=tcpBulk.js.map