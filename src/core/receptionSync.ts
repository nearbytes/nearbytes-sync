import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import type { Log } from 'nearbytes-log';
import type { ReceptionListResult } from 'nearbytes-log';
import {
  RECEPTION_ATTACH_TAIL,
  RECEPTION_PAGE_LIMIT,
  useInstantReceptionSync,
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
): Promise<ReceptionListResult> {
  const maxSeq = await readLocalReceptionMaxSeq(storageRoot);
  let parsedCursor = -1;
  if (cursor !== undefined && cursor !== '') {
    parsedCursor = Number.parseInt(cursor, 10);
    if (!Number.isNaN(parsedCursor) && parsedCursor >= maxSeq) {
      return { refs: [], more: false, next: String(maxSeq) };
    }
  }
  if (useInstantReceptionSync(maxSeq)) {
    const start = cursor === undefined || cursor === '' ? '-1' : cursor;
    const limit =
      start === '-1'
        ? Math.min(RECEPTION_PAGE_LIMIT, maxSeq + 1)
        : Math.min(RECEPTION_PAGE_LIMIT, maxSeq - parsedCursor + 1);
    return log.reception.listAfter(start, limit);
  }
  return log.reception.listAfter(cursor, RECEPTION_PAGE_LIMIT);
}

/** On connect: advertise everything we have (full journal or spec tail). */
export async function listLocalReceptionForConnect(
  log: Log,
  storageRoot: string | undefined,
): Promise<ReceptionListResult> {
  const maxSeq = await readLocalReceptionMaxSeq(storageRoot);
  if (useInstantReceptionSync(maxSeq)) {
    return log.reception.listAfter('-1', maxSeq + 1);
  }
  const start = Math.max(-1, maxSeq - RECEPTION_ATTACH_TAIL);
  return log.reception.listAfter(String(start), RECEPTION_ATTACH_TAIL);
}
