import type { ReceptionObjectRef } from 'nearbytes-log';

export interface LocalHaveAnnouncer {
  pushLocalHave(refs: readonly ReceptionObjectRef[]): void | Promise<void>;
}

const announcers = new Set<LocalHaveAnnouncer>();

/** Refs written while no peer session was attached; drained on {@link registerLocalHaveAnnouncer}. */
let pendingBroadcast: ReceptionObjectRef[] = [];

const patchedLogs = new WeakSet<object>();

function refKey(ref: ReceptionObjectRef): string {
  return ref.kind === 'block'
    ? `block:${ref.hash.toLowerCase()}`
    : `event:${ref.channel.toLowerCase()}:${ref.hash.toLowerCase()}`;
}

function mergePendingBroadcast(refs: readonly ReceptionObjectRef[]): void {
  if (refs.length === 0) {
    return;
  }
  const seen = new Set(pendingBroadcast.map(refKey));
  for (const ref of refs) {
    const key = refKey(ref);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    pendingBroadcast.push(ref);
  }
}

async function drainPendingBroadcast(): Promise<void> {
  if (pendingBroadcast.length === 0 || announcers.size === 0) {
    return;
  }
  const batch = pendingBroadcast;
  pendingBroadcast = [];
  if (process.env.NBF_PROP_TRACE === '1') {
    console.error(
      `[nearbytes-sync] drainPendingHave n=${batch.length} announcers=${announcers.size}`,
    );
  }
  await Promise.all([...announcers].map((announcer) => Promise.resolve(announcer.pushLocalHave(batch))));
}

export async function broadcastLocalHaveAwait(refs: readonly ReceptionObjectRef[]): Promise<void> {
  if (refs.length === 0) {
    return;
  }
  if (announcers.size === 0) {
    mergePendingBroadcast(refs);
    return;
  }
  await Promise.all([...announcers].map((announcer) => Promise.resolve(announcer.pushLocalHave(refs))));
}

export function registerLocalHaveAnnouncer(announcer: LocalHaveAnnouncer): () => void {
  announcers.add(announcer);
  if (process.env.NBF_PROP_TRACE === '1') {
    console.error(`[nearbytes-sync] registerLocalHaveAnnouncer announcers=${announcers.size}`);
  }
  drainPendingBroadcast();
  return () => {
    announcers.delete(announcer);
    if (process.env.NBF_PROP_TRACE === '1') {
      console.error(`[nearbytes-sync] unregisterLocalHaveAnnouncer announcers=${announcers.size}`);
    }
  };
}

export function broadcastLocalHave(refs: readonly ReceptionObjectRef[]): void {
  if (refs.length === 0) {
    return;
  }
  if (process.env.NBF_PROP_TRACE === '1') {
    console.error(`[nearbytes-sync] broadcastLocalHave n=${refs.length} announcers=${announcers.size}`);
  }
  if (announcers.size === 0) {
    mergePendingBroadcast(refs);
    if (process.env.NBF_PROP_TRACE === '1') {
      console.error(`[nearbytes-sync] broadcastLocalHave queued pending=${pendingBroadcast.length}`);
    }
    return;
  }
  for (const announcer of announcers) {
    announcer.pushLocalHave(refs);
  }
}

type ReceptionWithFlush = {
  appendReception: (ref: ReceptionObjectRef) => Promise<string>;
  appendReceptionRaw?: (ref: ReceptionObjectRef) => Promise<string>;
  flushLocalHave?: () => void;
};

/** After each local reception append, push {@code have} to every open peer session (SYNC-10). */
export function patchLogForReactiveHave(log: {
  reception: { appendReception: (ref: ReceptionObjectRef) => Promise<string> };
}): void {
  if (patchedLogs.has(log)) {
    return;
  }
  patchedLogs.add(log);
  const append = log.reception.appendReception.bind(log.reception);
  let pending: ReceptionObjectRef[] = [];
  const flushPending = (): void => {
    if (pending.length === 0) {
      return;
    }
    const batch = pending;
    pending = [];
    if (process.env.NBF_PROP_TRACE === '1') {
      console.error(`[nearbytes-sync] flushLocalHave n=${batch.length} kinds=${batch.map((r) => r.kind).join(',')}`);
    }
    broadcastLocalHave(batch);
  };
  log.reception.appendReception = async (ref) => {
    const cursor = await append(ref);
    pending.push(ref);
    return cursor;
  };
  const reception = log.reception as ReceptionWithFlush;
  reception.flushLocalHave = flushPending;
  reception.appendReceptionRaw = append;
}
