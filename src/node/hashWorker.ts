/**
 * Worker thread: incremental sha256 over transferable chunks.
 *
 * Messages from main:
 *   { type: 'chunk', buf: ArrayBuffer }
 *   { type: 'finalize' }
 *
 * Messages to main:
 *   { type: 'digest', hex: string }
 */
import { createHash } from 'node:crypto';
import { parentPort } from 'node:worker_threads';

if (!parentPort) {
  throw new Error('hashWorker must be loaded as a worker');
}

const hasher = createHash('sha256');

parentPort.on('message', (msg: { type: 'chunk'; buf: ArrayBuffer } | { type: 'finalize' }) => {
  if (msg.type === 'chunk') {
    hasher.update(Buffer.from(msg.buf));
    return;
  }
  if (msg.type === 'finalize') {
    parentPort!.postMessage({ type: 'digest', hex: hasher.digest('hex') });
    parentPort!.close();
  }
});
