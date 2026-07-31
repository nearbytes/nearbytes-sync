/**
 * Optional verbose sync tracing (`configureSyncDebug`) and timestamped stderr.
 *
 * Levels (highest→lowest severity): error, warn, info, debug, trace.
 *   error — should never happen; a bug or data corruption.
 *   warn  — expected-but-notable failure (handshake reject, timeout, missing block).
 *   info  — protocol milestones (hello, subscribe, attach, connect/disconnect).
 *   debug — per-page anti-entropy traffic (have/want/delta).
 *   trace — everything else (dedup/suppression, internal bookkeeping).
 */

export type SyncDebugLevel = 'error' | 'warn' | 'info' | 'debug' | 'trace';

export const SYNC_DEBUG_LEVEL_ORDER: Record<SyncDebugLevel, number> = {
  error: 0,
  warn: 1,
  info: 2,
  debug: 3,
  trace: 4,
};

export type SyncDebugSink = (scope: string, level: SyncDebugLevel, line: string) => void;

/**
 * A trace emitter threaded by reference through `StartOptions.trace` and down
 * into every layer that logs (TRACE-04). Distinct from `SyncDebugSink` only in
 * spirit: this is the value actually called at emit sites, already resolved
 * against a sink + level filter — never a module-global lookup.
 */
export type TraceEmit = (scope: string, level: SyncDebugLevel, line: string) => void;

/**
 * Stable identity of *this loaded copy* of the module (TRACE-05). Two
 * physical copies in node_modules (e.g. hoisted vs nested under a stale
 * transitive pin) each get their own `MODULE_ID`, generated once at import
 * time — this is what makes duplicate-copy bugs detectable instead of
 * silently dropping frames (see TRACE-06 and the rationale in
 * `sync-tracing-v1.md` §1).
 */
// Web Crypto (`globalThis.crypto.randomUUID`) rather than `node:crypto` —
// this module is reachable from the browser-safe entry point (`browser.js`
// re-exports `peerLoop.js`, which imports this file) via `attachPeerSession`.
export const MODULE_ID: string = globalThis.crypto.randomUUID();

/**
 * A trace destination the caller can mutate after `start()` has already
 * returned — `sink`/`minLevel` are read fresh on every emission (TRACE-01:
 * tracing must be activatable at runtime without restarting the engine).
 * Hold onto the same object passed as `StartOptions.trace` and flip
 * `.sink` between a function and `undefined` to toggle capture on/off with
 * no engine restart.
 */
export interface TraceDestination {
  sink?: SyncDebugSink;
  minLevel?: SyncDebugLevel;
}

/**
 * Resolve the `TraceEmit` a given `start()` call should use: reads the
 * caller's own by-reference destination (TRACE-04) live on every call, so
 * toggling `trace.sink` later takes effect without restarting the engine
 * (TRACE-01), and guards so a throwing sink can never reach the wire path
 * (TRACE-07). Falls back to the legacy module-global `configureSyncDebug`
 * path only when no `trace` destination is supplied at all — kept for
 * existing consumers (e.g. `nbf --debug`) that predate per-instance sinks.
 */
export function resolveTraceEmit(trace?: TraceDestination): TraceEmit {
  if (trace === undefined) {
    return syncDebugLine;
  }
  return (scope, level, line) => {
    const sink = trace.sink;
    if (sink === undefined) return;
    const minLevel = trace.minLevel ?? 'trace';
    if (SYNC_DEBUG_LEVEL_ORDER[level] > SYNC_DEBUG_LEVEL_ORDER[minLevel]) return;
    try {
      sink(scope, level, line);
    } catch (err) {
      // TRACE-07: enabling tracing MUST NOT affect the wire path — a
      // throwing sink is a consumer bug, not a sync-engine failure.
      console.error(`[nearbytes-sync] trace sink threw for ${scope}:`, err);
    }
  };
}

let verboseEnabled = false;
let minLevel: SyncDebugLevel = 'trace';
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

/**
 * Enable wire-level trace lines; optional sink overrides stderr (e.g. `nbf
 * --debug sync`). `minLevel` filters what's forwarded — defaults to `trace`
 * (everything) for back-compat; pass e.g. `'debug'` to drop per-message
 * `trace` chatter while keeping protocol milestones and warnings.
 */
export function configureSyncDebug(options: {
  readonly enabled: boolean;
  readonly minLevel?: SyncDebugLevel;
  readonly sink?: SyncDebugSink;
}): void {
  verboseEnabled = options.enabled;
  minLevel = options.minLevel ?? 'trace';
  sink = options.enabled ? options.sink : undefined;
}

export function isSyncVerboseDebugEnabled(): boolean {
  return verboseEnabled;
}

function passesLevel(level: SyncDebugLevel): boolean {
  return SYNC_DEBUG_LEVEL_ORDER[level] <= SYNC_DEBUG_LEVEL_ORDER[minLevel];
}

/** Timestamped stderr for sync (errors and coalesced connect lines). Always `warn`+ visible, level filter still applies when verbose. */
export function syncEmit(scope: string, line: string, level: SyncDebugLevel = 'warn'): void {
  const formatted = `[${formatSyncTimestamp()}] [nearbytes-sync:${scope}] ${line}`;
  if (verboseEnabled && sink !== undefined) {
    if (passesLevel(level)) sink(scope, level, line);
    return;
  }
  console.error(formatted);
}

/** Verbose protocol trace; no-op unless `configureSyncDebug({ enabled: true })` and level passes `minLevel`. */
export function syncDebugLine(scope: string, level: SyncDebugLevel, line: string): void {
  if (!verboseEnabled || !passesLevel(level)) {
    return;
  }
  if (sink !== undefined) {
    sink(scope, level, line);
    return;
  }
  console.error(`[${formatSyncTimestamp()}] [nearbytes-sync:${scope}] [${level}] ${line}`);
}
