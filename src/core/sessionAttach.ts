import type { Subject, SyncMessage } from './types.js';
import { RECEPTION_PAGE_LIMIT } from './syncConstants.js';

/** Resume this remote instance's reception journal from our persisted cursor. */
export function buildResumeDelta(
  subject: Subject,
  cursor: string | undefined,
): Extract<SyncMessage, { type: 'delta' }> {
  return {
    type: 'delta',
    subject,
    mode: 'global',
    limit: RECEPTION_PAGE_LIMIT,
    ...(cursor !== undefined && cursor !== '' ? { cursor } : {}),
  };
}

export function buildResumeSubscribe(
  subject: Subject,
  cursor: string | undefined,
): Extract<SyncMessage, { type: 'subscribe' }> {
  return {
    type: 'subscribe',
    delta: buildResumeDelta(subject, cursor),
  };
}
