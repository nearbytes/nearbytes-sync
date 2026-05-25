import { close, mkdirSync, open, write } from 'node:fs';
import { rename, unlink } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { acquireSha256Stream } from 'nearbytes-crypto';
import { blockPath } from 'nearbytes-log';
/**
 * Unified ingest batch: one pre-allocated Buffer per batch, into which each
 * socket chunk is copied exactly once on the main thread. The same memory is
 * then used both for the parallel `pwrite` (by reference) and for the hash
 * worker (via a transferable clone). On Apple Silicon, pushing toward a single
 * memcpy via SAB or worker-side I/O paid an equivalent amount in page faults
 * and fresh allocations, so two memcpies on pooled `allocUnsafe` memory is
 * optimal.
 */
const BATCH_BYTES = 4 << 20;
function openWriteAsync(path) {
    return new Promise((resolve, reject) => {
        open(path, 'w', (err, fd) => (err ? reject(err) : resolve(fd)));
    });
}
function closeAsync(fd) {
    return new Promise((resolve, reject) => {
        close(fd, (err) => (err ? reject(err) : resolve()));
    });
}
function writeFdAsync(fd, buf, length, position) {
    return new Promise((resolve, reject) => {
        write(fd, buf, 0, length, position, (err) => (err ? reject(err) : resolve()));
    });
}
/**
 * Async fs.write queue + off-thread streaming sha256.
 *
 * Wire bytes are pushed in three directions for each chunk:
 *  - copied into a 4 MiB hash batch buffer, transferred to a streaming
 *    sha256 hasher (off-main-thread via `acquireSha256Stream`) when full
 *  - enqueued as a parallel pwrite (fs.write with explicit position) on the
 *    tmp fd
 *  - counted toward the stream total
 *
 * The hash, the parallel writes, and the rename all complete asynchronously,
 * so the wall clock is wire + max(drain, hashTail) + rename. The streaming
 * hasher is transparently dispatched to one of K long-lived worker threads
 * managed inside `nearbytes-crypto`; when K block streams finalize at once,
 * K SHA-256s run on K cores in parallel.
 */
function createAsyncSink(dataDir, hash, total) {
    const finalPath = join(dataDir, blockPath(hash));
    const tmpPath = `${finalPath}.tmp`;
    mkdirSync(dirname(finalPath), { recursive: true });
    const fdPromise = openWriteAsync(tmpPath);
    let received = 0;
    let firstByteAt = null;
    let lastByteAt = null;
    let firstError = null;
    let inflight = 0;
    let writesDoneResolve = null;
    let allWritesEnqueued = false;
    const writesDonePromise = new Promise((resolve) => {
        writesDoneResolve = resolve;
    });
    /**
     * The streaming hasher acquire is async; chunks may arrive before the
     * worker is checked out. We buffer them as transferable `ArrayBuffer`s
     * and drain on arrival, then continue streaming straight through. In
     * the steady state the acquire resolves in a single microtask, so
     * `pendingChunks` rarely holds more than one batch.
     */
    const hasherPromise = acquireSha256Stream();
    let hasher = null;
    const pendingChunks = [];
    let finalizeRequested = false;
    let digestResolve = null;
    let digestReject = null;
    const digestPromise = new Promise((resolve, reject) => {
        digestResolve = resolve;
        digestReject = reject;
    });
    const driveHasher = (h) => {
        for (const chunk of pendingChunks) {
            h.updateTransfer(chunk);
        }
        pendingChunks.length = 0;
        if (finalizeRequested) {
            h.finalize().then((hex) => digestResolve?.(hex), (err) => digestReject?.(err instanceof Error ? err : new Error(String(err))));
        }
    };
    hasherPromise.then((h) => {
        hasher = h;
        driveHasher(h);
    }, (err) => {
        firstError = err instanceof Error ? err : new Error(String(err));
        digestReject?.(firstError);
    });
    let batchBuf = Buffer.allocUnsafe(BATCH_BYTES);
    let batchUsed = 0;
    let batchPosition = 0;
    const tryResolveDone = () => {
        if (allWritesEnqueued && inflight === 0 && writesDoneResolve) {
            writesDoneResolve();
            writesDoneResolve = null;
        }
    };
    const flushBatch = () => {
        if (batchUsed === 0)
            return;
        const buf = batchBuf;
        const len = batchUsed;
        const pos = batchPosition;
        const hashCopy = new ArrayBuffer(len);
        new Uint8Array(hashCopy).set(buf.subarray(0, len));
        if (hasher) {
            hasher.updateTransfer(hashCopy);
        }
        else {
            pendingChunks.push(hashCopy);
        }
        inflight++;
        void (async () => {
            try {
                const fd = await fdPromise;
                await writeFdAsync(fd, buf, len, pos);
            }
            catch (err) {
                firstError = err instanceof Error ? err : new Error(String(err));
            }
            finally {
                inflight--;
                tryResolveDone();
            }
        })();
        batchPosition += len;
        batchUsed = 0;
        batchBuf = Buffer.allocUnsafe(BATCH_BYTES);
    };
    const ingest = (chunk) => {
        if (received >= total || firstError) {
            return;
        }
        if (firstByteAt === null) {
            firstByteAt = Date.now();
        }
        const chunkLen = chunk.byteLength;
        const remaining = total - received;
        const usable = remaining < chunkLen ? remaining : chunkLen;
        const space = BATCH_BYTES - batchUsed;
        if (usable <= space) {
            batchBuf.set(usable === chunkLen ? chunk : chunk.subarray(0, usable), batchUsed);
            batchUsed += usable;
            received += usable;
            if (batchUsed === BATCH_BYTES) {
                flushBatch();
            }
        }
        else {
            batchBuf.set(chunk.subarray(0, space), batchUsed);
            batchUsed += space;
            flushBatch();
            const tail = usable - space;
            batchBuf.set(chunk.subarray(space, space + tail), 0);
            batchUsed += tail;
            received += usable;
        }
        if (received >= total) {
            lastByteAt = Date.now();
            flushBatch();
            finalizeRequested = true;
            if (hasher) {
                hasher.finalize().then((hex) => digestResolve?.(hex), (err) => digestReject?.(err instanceof Error ? err : new Error(String(err))));
            }
            allWritesEnqueued = true;
            tryResolveDone();
        }
    };
    const finish = async () => {
        const [, digest] = await Promise.all([writesDonePromise, digestPromise]);
        if (firstError)
            throw firstError;
        const fd = await fdPromise;
        await closeAsync(fd);
        const diskDrainDoneAt = Date.now();
        const hashDoneAt = Date.now();
        if (digest !== hash.toLowerCase()) {
            await unlink(tmpPath).catch(() => undefined);
            return {
                outcome: 'invalid',
                phases: {
                    firstByteAt,
                    lastByteAt,
                    diskDrainDoneAt,
                    hashDoneAt,
                    renameDoneAt: hashDoneAt,
                },
            };
        }
        await rename(tmpPath, finalPath);
        const renameDoneAt = Date.now();
        return {
            outcome: 'stored',
            phases: {
                firstByteAt,
                lastByteAt,
                diskDrainDoneAt,
                hashDoneAt,
                renameDoneAt,
            },
        };
    };
    return {
        total,
        get received() {
            return received;
        },
        ingest,
        finish,
    };
}
export function createDiskBlockStreamSink(dataDir, hash, total) {
    return createAsyncSink(dataDir, hash, total);
}
export function createNodeDiskBlockStreamFactory(dataDir) {
    return {
        create: (hash, total) => createDiskBlockStreamSink(dataDir, hash, total),
    };
}
//# sourceMappingURL=blockReceive.js.map