import type { Log } from 'nearbytes-log';

export type PendingBlock = { readonly total: number; readonly parts: Map<number, Uint8Array> };

/** Survives peer reconnects — partial large blocks must not be lost on session stop. */
const pendingByLog = new WeakMap<Log, Map<string, PendingBlock>>();

export function getPendingBlocks(log: Log): Map<string, PendingBlock> {
  let map = pendingByLog.get(log);
  if (map === undefined) {
    map = new Map();
    pendingByLog.set(log, map);
  }
  return map;
}
