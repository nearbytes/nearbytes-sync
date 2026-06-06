import { close, existsSync, mkdirSync, open, write } from 'node:fs';
import { rename, unlink } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { randomBytes } from 'node:crypto';
import type { Hash, Sha256Stream } from 'nearbytes-crypto';
import { acquireSha256Stream } from 'nearbytes-crypto';
import { blockPath } from 'nearbytes-log';
import type {
  DiskBlockStreamFinishResult,
  DiskBlockStreamSink,
  DiskBlockStreamSinkFactory,
} from '../core/peerLoop.js';

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

function openWriteAsync(path: string): Promise<number> {
  return new Promise((resolve, reject) => {
    open(path, 'w', (err, fd) => (err ? reject(err) : resolve(fd)));
  });
}

function closeAsync(fd: number): Promise<void> {
  return new Promise((resolve, reject) => {
    close(fd, (err) => (err ? reject(err) : resolve()));
  });
}

function writeFdAsync(fd: number, buf: Buffer, length: number, position: number): Promise<void> {
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
function createAsyncSink(dataDir: string, hash: string, total: number): DiskBlockStreamSink {
  const finalPath = join(dataDir, blockPath(hash as Hash));
  /**
   * Unique scratch path per in-flight stream. Content-addressed blocks are
   * CRDT-trivial (bytes ARE the hash) so two concurrent deliveries of the
   * same hash are correct by construction; the only requirement is that
   * neither writer trashes the other's tmp file. With a stable `.tmp` suffix
   * both writers shared the same path, causing `O_CREAT|O_TRUNC` to clobber
   * each other and the second rename to ENOENT once the first stole the
   * dir entry. A random suffix isolates each stream's scratch space.
   */
  const tmpPath = `${finalPath}.${randomBytes(8).toString('hex')}.tmp`;
  mkdirSync(dirname(finalPath), { recursive: true });
  const fdPromise: Promise<number> = openWriteAsync(tmpPath);
  let received = 0;
  let firstByteAt: number | null = null;
  let lastByteAt: number | null = null;
  let firstError: Error | null = null;
  let inflight = 0;
  let writesDoneResolve: (() => void) | null = null;
  let allWritesEnqueued = false;
  const writesDonePromise = new Promise<void>((resolve) => {
    writesDoneResolve = resolve;
  });

  /**
   * The streaming hasher acquire is async; chunks may arrive before the
   * worker is checked out. We buffer them as transferable `ArrayBuffer`s
   * and drain on arrival, then continue streaming straight through. In
   * the steady state the acquire resolves in a single microtask, so
   * `pendingChunks` rarely holds more than one batch.
   */
  const hasherPromise: Promise<Sha256Stream> = acquireSha256Stream();
  let hasher: Sha256Stream | null = null;
  const pendingChunks: ArrayBuffer[] = [];
  let finalizeRequested = false;
  let digestResolve: ((hex: string) => void) | null = null;
  let digestReject: ((err: Error) => void) | null = null;
  const digestPromise = new Promise<string>((resolve, reject) => {
    digestResolve = resolve;
    digestReject = reject;
  });

  const driveHasher = (h: Sha256Stream): void => {
    for (const chunk of pendingChunks) {
      h.updateTransfer(chunk);
    }
    pendingChunks.length = 0;
    if (finalizeRequested) {
      h.finalize().then(
        (hex) => digestResolve?.(hex),
        (err) => digestReject?.(err instanceof Error ? err : new Error(String(err))),
      );
    }
  };

  hasherPromise.then(
    (h) => {
      hasher = h;
      driveHasher(h);
    },
    (err) => {
      firstError = err instanceof Error ? err : new Error(String(err));
      digestReject?.(firstError);
    },
  );

  let batchBuf = Buffer.allocUnsafe(BATCH_BYTES);
  let batchUsed = 0;
  let batchPosition = 0;

  const tryResolveDone = (): void => {
    if (allWritesEnqueued && inflight === 0 && writesDoneResolve) {
      writesDoneResolve();
      writesDoneResolve = null;
    }
  };

  const flushBatch = (): void => {
    if (batchUsed === 0) return;
    const buf = batchBuf;
    const len = batchUsed;
    const pos = batchPosition;
    const hashCopy = new ArrayBuffer(len);
    new Uint8Array(hashCopy).set(buf.subarray(0, len));
    if (hasher) {
      hasher.updateTransfer(hashCopy);
    } else {
      pendingChunks.push(hashCopy);
    }
    inflight++;
    void (async () => {
      try {
        const fd = await fdPromise;
        await writeFdAsync(fd, buf, len, pos);
      } catch (err) {
        firstError = err instanceof Error ? err : new Error(String(err));
      } finally {
        inflight--;
        tryResolveDone();
      }
    })();
    batchPosition += len;
    batchUsed = 0;
    batchBuf = Buffer.allocUnsafe(BATCH_BYTES);
  };

  const ingest = (chunk: Uint8Array): void => {
    if (received >= total || firstError) {
      return;
    }
    if (firstByteAt === null) {
      firstByteAt = Date.now();
    }
    const chunkLen = chunk.byteLength;
    const remaining = total - received;
    const usable = remaining < chunkLen ? remaining : chunkLen;
    let offset = 0;
    while (offset < usable) {
      const space = BATCH_BYTES - batchUsed;
      const take = Math.min(space, usable - offset);
      batchBuf.set(chunk.subarray(offset, offset + take), batchUsed);
      batchUsed += take;
      received += take;
      offset += take;
      if (batchUsed === BATCH_BYTES) {
        flushBatch();
      }
    }
    if (received >= total) {
      lastByteAt = Date.now();
      flushBatch();
      finalizeRequested = true;
      if (hasher) {
        hasher.finalize().then(
          (hex) => digestResolve?.(hex),
          (err) => digestReject?.(err instanceof Error ? err : new Error(String(err))),
        );
      }
      allWritesEnqueued = true;
      tryResolveDone();
    }
  };

  const finish = async (): Promise<DiskBlockStreamFinishResult> => {
    const [, digest] = await Promise.all([writesDonePromise, digestPromise]);
    if (firstError) throw firstError;
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
    /**
     * Idempotent commit. Three race outcomes are all "stored":
     *  (a) Final path empty → rename succeeds atomically.
     *  (b) Final path already populated by a concurrent stream → drop our
     *      verified scratch (same hash, same bytes; CRDT merge is identity).
     *  (c) Rename loses a race after the existsSync check → POSIX rename
     *      overwrites atomically with byte-identical content, so success is
     *      still correctness; ENOENT only happens if our own tmp got unlinked
     *      out from under us, which is impossible with a unique tmp suffix
     *      but is tolerated defensively when the final exists.
     */
    if (existsSync(finalPath)) {
      await unlink(tmpPath).catch(() => undefined);
      return {
        outcome: 'stored',
        phases: {
          firstByteAt,
          lastByteAt,
          diskDrainDoneAt,
          hashDoneAt,
          renameDoneAt: hashDoneAt,
        },
      };
    }
    try {
      await rename(tmpPath, finalPath);
    } catch (err) {
      const code = (err as NodeJS.ErrnoException).code;
      if ((code === 'ENOENT' || code === 'EEXIST') && existsSync(finalPath)) {
        await unlink(tmpPath).catch(() => undefined);
      } else {
        throw err;
      }
    }
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

export function createDiskBlockStreamSink(
  dataDir: string,
  hash: string,
  total: number,
): DiskBlockStreamSink {
  return createAsyncSink(dataDir, hash, total);
}

export function createNodeDiskBlockStreamFactory(dataDir: string): DiskBlockStreamSinkFactory {
  return {
    create: (hash, total) => createDiskBlockStreamSink(dataDir, hash, total),
  };
}
