import { close, mkdirSync, open, write } from 'node:fs';
import { rename, unlink } from 'node:fs/promises';
import { availableParallelism } from 'node:os';
import { dirname, join } from 'node:path';
import type { Hash, HashWorkerPool, StreamingHasher } from 'nearbytes-crypto';
import { createHashWorkerPool } from 'nearbytes-crypto';
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

/**
 * Process-wide pool of long-lived streaming SHA-256 workers. Sized to the host
 * core count by default; a single pool serves every association so K
 * simultaneously-finalizing block streams reuse K warm workers without paying
 * any spawn cost. The pool is created lazily on the first inbound block stream
 * so non-sync consumers of `nearbytes-sync` do not eagerly spawn workers.
 *
 * The pool's capacity is the upper bound on inbound block streams that hash
 * concurrently in this process. Additional acquires queue on the pool's FIFO
 * and resume as workers release.
 */
let hashPool: HashWorkerPool | null = null;
let hashPoolCapacity: number = Math.max(2, availableParallelism());

export function configureHashWorkerPoolCapacity(capacity: number): void {
  if (!Number.isInteger(capacity) || capacity < 1) {
    throw new Error(`hash worker pool capacity must be a positive integer, got ${capacity}`);
  }
  if (hashPool) {
    throw new Error('hash worker pool already initialized; configure before first inbound stream');
  }
  hashPoolCapacity = capacity;
}

function getHashPool(): HashWorkerPool {
  if (!hashPool) {
    hashPool = createHashWorkerPool({ capacity: hashPoolCapacity });
  }
  return hashPool;
}

export async function shutdownHashWorkerPool(): Promise<void> {
  const p = hashPool;
  hashPool = null;
  if (p) await p.close();
}

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
 * Async fs.write queue + pooled worker-thread sha256.
 *
 * Wire bytes are pushed in three directions for each chunk:
 *  - copied into a 4 MiB hash batch buffer, transferred to a pooled streaming
 *    hasher when full
 *  - enqueued as a parallel pwrite (fs.write with explicit position) on the
 *    tmp fd
 *  - counted toward the stream total
 *
 * The hash worker, the parallel writes, and the rename all complete
 * asynchronously, so the wall clock is wire + max(drain, hashTail) + rename.
 * When K block streams are in flight simultaneously, each one holds an
 * independent worker from the pool, so K SHA-256s run in parallel on K cores.
 */
function createAsyncSink(dataDir: string, hash: string, total: number): DiskBlockStreamSink {
  const finalPath = join(dataDir, blockPath(hash as Hash));
  const tmpPath = `${finalPath}.tmp`;
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
   * The pool acquire is async; chunks may arrive before the worker is
   * checked out. We buffer them as transferable `ArrayBuffer`s and drain on
   * arrival, then continue streaming straight through. In the steady state
   * the pool is pre-warmed and acquire resolves in a single microtask, so
   * `pendingChunks` rarely holds more than one batch.
   */
  const hasherPromise: Promise<StreamingHasher> = getHashPool().acquire();
  let hasher: StreamingHasher | null = null;
  const pendingChunks: ArrayBuffer[] = [];
  let finalizeRequested = false;
  let digestResolve: ((hex: string) => void) | null = null;
  let digestReject: ((err: Error) => void) | null = null;
  const digestPromise = new Promise<string>((resolve, reject) => {
    digestResolve = resolve;
    digestReject = reject;
  });

  const driveHasher = (h: StreamingHasher): void => {
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
    const space = BATCH_BYTES - batchUsed;
    if (usable <= space) {
      batchBuf.set(usable === chunkLen ? chunk : chunk.subarray(0, usable), batchUsed);
      batchUsed += usable;
      received += usable;
      if (batchUsed === BATCH_BYTES) {
        flushBatch();
      }
    } else {
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
