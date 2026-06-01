import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import type { Log } from 'nearbytes-log';
import type { ReceptionListResult } from 'nearbytes-log';
import {
  RECEPTION_ATTACH_TAIL,
  RECEPTION_PAGE_LIMIT,
  RECEPTION_RESUME_PAGE,
} from './syncConstants.js';

export async function readLocalReceptionMaxSeq(storageRoot: string | undefined): Promise<number> {
  if (storageRoot === undefined) {
    return 0;
  }
  try {
    const raw = await readFile(join(storageRoot, 'sync', 'reception.jsonl'), 'utf8');
    const lines = raw.trim().split('\n').filter((line) => line.length > 0);
    if (lines.length === 0) {
      return 0;
    }
    const parsed = JSON.parse(lines[lines.length - 1]!) as { seq?: unknown };
    return typeof parsed.seq === 'number' ? parsed.seq : Number.parseInt(String(parsed.seq), 10) || 0;
  } catch {
    return 0;
  }
}

/** Page of this instance's reception journal (linear `seq` order). */
export async function listLocalReceptionPage(
  log: Log,
  storageRoot: string | undefined,
  cursor?: string,
  limit = RECEPTION_PAGE_LIMIT,
): Promise<ReceptionListResult> {
  const maxSeq = await readLocalReceptionMaxSeq(storageRoot);
  if (cursor !== undefined && cursor !== '') {
    const parsed = Number.parseInt(cursor, 10);
    if (!Number.isNaN(parsed) && parsed >= maxSeq) {
      return { refs: [], more: false, next: String(maxSeq) };
    }
  }
  return log.reception.listAfter(cursor, limit);
}

/** On connect: announce recent tail of our journal only. */
export async function listLocalReceptionForConnect(
  log: Log,
  storageRoot: string | undefined,
): Promise<ReceptionListResult> {
  const maxSeq = await readLocalReceptionMaxSeq(storageRoot);
  const start = Math.max(-1, maxSeq - RECEPTION_ATTACH_TAIL);
  return log.reception.listAfter(String(start), RECEPTION_ATTACH_TAIL);
}

export { RECEPTION_RESUME_PAGE };
