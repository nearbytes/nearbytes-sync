/**
 * Optional verbose sync tracing (`configureSyncDebug`) and timestamped stderr.
 */

export type SyncDebugSink = (scope: string, line: string) => void;

let verboseEnabled = false;
let sink: SyncDebugSink | undefined;

/** Compact wall time for terminal debug (`14:17:23.255`). */
export function formatSyncTimestamp(now = Date.now()): string {
  const d = new Date(now);
  const h = String(d.getHours()).padStart(2, '0');
  const m = String(d.getMinutes()).padStart(2, '0');
  const s = String(d.getSeconds()).padStart(2, '0');
  const ms = String(d.getMilliseconds()).padStart(3, '0');
  return `${h}:${m}:${s}.${ms}`;
}

/** Enable wire-level trace lines; optional sink overrides stderr (e.g. nbf `--debug sync`). */
export function configureSyncDebug(options: {
  readonly enabled: boolean;
  readonly sink?: SyncDebugSink;
}): void {
  verboseEnabled = options.enabled;
  sink = options.enabled ? options.sink : undefined;
}

export function isSyncVerboseDebugEnabled(): boolean {
  return verboseEnabled;
}

/** Timestamped stderr for sync (errors and coalesced connect lines). */
export function syncEmit(scope: string, line: string): void {
  const formatted = `[${formatSyncTimestamp()}] [nearbytes-sync:${scope}] ${line}`;
  if (verboseEnabled && sink !== undefined) {
    sink(scope, line);
    return;
  }
  console.error(formatted);
}

/** Verbose protocol trace; no-op unless `configureSyncDebug({ enabled: true })`. */
export function syncDebugLine(scope: string, line: string): void {
  if (!verboseEnabled) {
    return;
  }
  if (sink !== undefined) {
    sink(scope, line);
    return;
  }
  console.error(`[${formatSyncTimestamp()}] [nearbytes-sync:${scope}] ${line}`);
}
