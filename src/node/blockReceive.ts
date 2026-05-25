import { close, mkdirSync, open, write } from 'node:fs';
import { rename, unlink } from 'node:fs/promises';
import { dirname, join } from 'path';
import { Worker } from 'node:worker_threads';
import { fileURLToPath } from 'node:url';
import type { Hash } from 'nearbytes-crypto';
import { blockPath } from 'nearbytes-log';
import type {
  DiskBlockStreamFinishResult,
  DiskBlockStreamSink,
  DiskBlockStreamSinkFactory,
} from '../core/peerLoop.js';

const WORKER_URL = new URL('./hashWorker.js', import.meta.url);
/**
 * Unified ingest batch: one pre-allocated Buffer per batch, into which each socket
 * chunk is copied exactly once on the main thread. The same memory is then used both
 * for the parallel \texttt{pwrite} (by reference) and for the hash worker (via a
 * transferable clone). On Apple Silicon, attempts to push to a single memcpy via SAB
 * or worker-side I/O paid an equivalent amount in page faults / fresh allocations,
 * so 2 memcpies on pooled \texttt{allocUnsafe} memory turns out to be optimal.
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
 * Idle hash workers ready to receive chunks. Spawning a Node worker costs ~5–10 ms;
 * we keep one pre-warmed so the first inbound block stream pays zero spawn cost.
 */
const idleWorkers: Worker[] = [];
let warmupRequested = false;

function warmHashWorkers(): void {
  if (warmupRequested) return;
  warmupRequested = true;
  // Pre-spawn a single idle worker; additional workers are created on demand.
  idleWorkers.push(new Worker(fileURLToPath(WORKER_URL)));
}

function takeOrSpawnWorker(): Worker {
  const cached = idleWorkers.pop();
  if (cached) {
    // Refill the pool for the next stream.
    idleWorkers.push(new Worker(fileURLToPath(WORKER_URL)));
    return cached;
  }
  return new Worker(fileURLToPath(WORKER_URL));
}

function spawnHashWorker(): { worker: Worker; digest: Promise<string> } {
  warmHashWorkers();
  const worker = takeOrSpawnWorker();
  const digest = new Promise<string>((resolve, reject) => {
    worker.once('message', (msg: { type: 'digest'; hex: string }) => {
      if (msg && msg.type === 'digest') resolve(msg.hex);
      else reject(new Error(`unexpected worker message ${JSON.stringify(msg)}`));
    });
    worker.once('error', reject);
    worker.once('exit', (code) => {
      if (code !== 0 && code !== 1) reject(new Error(`hashWorker exited ${code}`));
    });
  });
  return { worker, digest };
}

/**
 * Async fs.write queue + worker-thread sha256.
 *
 * Wire bytes are pushed in three directions for each chunk:
 *  - copied into a 1 MiB hash batch buffer, transferred to the hash worker when full
 *  - enqueued as a parallel pwrite (fs.write with explicit position) on the tmp fd
 *  - counted toward the stream total
 *
 * The hash worker, the parallel writes, and the rename all complete asynchronously, so
 * the wall clock is wire + max(drain, hashTail) + rename.
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
  const { worker, digest: digestPromise } = spawnHashWorker();

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
    // Hash worker needs its own detachable ArrayBuffer; copy once.
    const hashCopy = new Uint8Array(len);
    hashCopy.set(buf.subarray(0, len));
    worker.postMessage({ type: 'chunk', buf: hashCopy.buffer }, [hashCopy.buffer]);
    // Disk write: parallel pwrite reusing the batch buffer (kept alive by the closure).
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
    // `chunk` may be either a Node `Buffer` (TCP socket fast path) or a plain
    // `Uint8Array` (wrapped duplex). `Uint8Array#set` works for both and is the
    // memmove the JIT compiles into SIMD-aware byte copy.
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
      worker.postMessage({ type: 'finalize' });
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
