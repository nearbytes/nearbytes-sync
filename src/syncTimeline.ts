/**
 * Sync connect timeline. Enable via `configureSyncTimeline`
 * (typically from `nbf --debug timeline` through `installSyncDebugBridge`).
 *
 * Session key `session`: ms since REPL/sync boot (discovery startup, peer search).
 * Per-peer keys: ms since first `discovered` for that peer (connect, hello, data).
 */
import { formatSyncTimestamp } from './syncDebugLog.js';

export type SyncTimelineSink = (line: string) => void;

/** Global boot clock — REPL start through first peer sighting. */
export const SYNC_TIMELINE_SESSION = 'session';

let enabled = false;
let sink: SyncTimelineSink | undefined;

const origins = new Map<string, number>();

/** Enable connect-phase timeline lines; optional sink overrides stderr (e.g. nbf `--debug timeline`). */
export function configureSyncTimeline(options: {
  readonly enabled: boolean;
  readonly sink?: SyncTimelineSink;
}): void {
  enabled = options.enabled;
  sink = options.enabled ? options.sink : undefined;
}

export function isSyncTimelineEnabled(): boolean {
  return enabled;
}

/** Start the session clock; first line is `phase` at +0ms. Safe to call once per process. */
export function syncTimelineBeginSession(phase = 'boot'): void {
  if (!enabled) {
    return;
  }
  const now = Date.now();
  if (!origins.has(SYNC_TIMELINE_SESSION)) {
    origins.set(SYNC_TIMELINE_SESSION, now);
  }
  emitMark(SYNC_TIMELINE_SESSION, now, phase);
}

export function syncTimelineMarkSession(phase: string, detail = ''): void {
  syncTimelineMark(SYNC_TIMELINE_SESSION, phase, detail);
}

export function syncTimelineSessionMs(): number | undefined {
  const t0 = origins.get(SYNC_TIMELINE_SESSION);
  if (t0 === undefined) {
    return undefined;
  }
  return Date.now() - t0;
}

export function syncTimelineKey(profile: string, instance: string): string {
  return `${profile.toLowerCase().slice(0, 8)}|${instance.toLowerCase().slice(0, 8)}`;
}

/** Carry t0 from discovery label to stable profile|instance key after hello. */
export function syncTimelineHandoff(
  fromKey: string,
  profile: string,
  instance: string,
): string {
  const toKey = syncTimelineKey(profile, instance);
  const t0 = origins.get(fromKey) ?? origins.get(toKey);
  if (t0 !== undefined) {
    origins.set(toKey, t0);
    if (fromKey !== toKey) {
      origins.delete(fromKey);
    }
  }
  return toKey;
}

export function syncTimelineMark(key: string, phase: string, detail = ''): void {
  if (!enabled) {
    return;
  }
  const now = Date.now();
  let t0 = origins.get(key);
  if (t0 === undefined) {
    t0 = now;
    origins.set(key, t0);
  }
  emitMark(key, now, phase, detail, t0);
}

export function syncTimelineClear(key: string): void {
  origins.delete(key);
}

function emitMark(
  key: string,
  now: number,
  phase: string,
  detail = '',
  t0 = origins.get(key) ?? now,
): void {
  const delta = now - t0;
  const ms = String(delta).padStart(6, ' ');
  const tail = detail.length > 0 ? ` ${detail}` : '';
  const body = `[+${ms}ms] ${phase}${tail} (${key})`;
  if (sink !== undefined) {
    sink(body);
    return;
  }
  console.error(`[${formatSyncTimestamp(now)}] [nearbytes-sync:timeline] ${body}`);
}
