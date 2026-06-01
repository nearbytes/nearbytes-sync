import { access } from 'node:fs/promises';
import { join } from 'node:path';
import type { Hash } from 'nearbytes-crypto';
import { blockPath, type Log } from 'nearbytes-log';

/** True when a block blob exists in the log store or on disk under `storageRoot`. */
export async function blockReadable(
  log: Log,
  storageRoot: string | undefined,
  hash: Hash,
): Promise<boolean> {
  if (await log.blocks.has(hash)) {
    return true;
  }
  if (storageRoot === undefined) {
    return false;
  }
  try {
    await access(join(storageRoot, blockPath(hash)));
    return true;
  } catch {
    return false;
  }
}
