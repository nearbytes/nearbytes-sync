import type { ReceptionObjectRef } from 'nearbytes-log';

export interface LocalHaveAnnouncer {
  pushLocalHave(refs: readonly ReceptionObjectRef[]): void;
}

const announcers = new Set<LocalHaveAnnouncer>();

const patchedLogs = new WeakSet<object>();

export function registerLocalHaveAnnouncer(announcer: LocalHaveAnnouncer): () => void {
  announcers.add(announcer);
  return () => announcers.delete(announcer);
}

export function broadcastLocalHave(refs: readonly ReceptionObjectRef[]): void {
  if (refs.length === 0) {
    return;
  }
  for (const announcer of announcers) {
    announcer.pushLocalHave(refs);
  }
}

/** After each local reception append, push {@code have} to every open peer session (SYNC-10). */
export function patchLogForReactiveHave(log: {
  reception: { appendReception: (ref: ReceptionObjectRef) => Promise<string> };
}): void {
  if (patchedLogs.has(log)) {
    return;
  }
  patchedLogs.add(log);
  const append = log.reception.appendReception.bind(log.reception);
  log.reception.appendReception = async (ref) => {
    const cursor = await append(ref);
    broadcastLocalHave([ref]);
    return cursor;
  };
}
