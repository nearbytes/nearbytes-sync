/**
 * Clean per-peer sync connect timeline. Enable via `configureSyncTimeline`
 * (typically from `nbf --debug timeline` through `installSyncDebugBridge`).
 *
 * One line per phase, ms since first event for that peer (discovery or connect).
 * Wire have/want spam stays on `--debug sync` only.
 */
import { formatSyncTimestamp } from './syncDebugLog.js';

export type SyncTimelineSink = (line: string) => void;

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

export function syncTimelineClear(key: string): void {
  origins.delete(key);
}
