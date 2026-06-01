/**
 * Optional verbose sync tracing (`configureSyncDebug`) and timestamped stderr.
 */

export type SyncDebugSink = (scope: string, line: string) => void;

let verboseEnabled = false;
let sink: SyncDebugSink | undefined;

export function formatSyncTimestamp(now = Date.now()): string {
  return new Date(now).toISOString();
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
