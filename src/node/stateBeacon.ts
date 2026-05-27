/**
 * Sync-state beacon.
 *
 * The sync daemon (`nbsync daemon`) owns the dataDir lock and runs the
 * wire protocol; any other process attached to the same dataDir is in
 * writer-only mode (DISC-27, split form) and therefore has no peers of
 * its own to inspect. So that observability tooling — `nbf monitor`,
 * `nbsync status`, GUIs, etc. — can still answer "what is the daemon
 * doing right now?" without needing an IPC channel, the daemon
 * periodically serialises a small JSON state file into the dataDir
 * root. Readers poll it.
 *
 * Design choices:
 *
 *  - Path: `<dataDir>/.nearbytes-sync.state.json`. Dot-prefixed so it
 *    sorts away from user-visible files; sibling to (not under) the
 *    block / channel directories so it can never be misinterpreted as
 *    a content-addressed object.
 *  - Atomic publish: write `<...>.state.tmp`, then `rename(2)` over the
 *    final name. Readers see a fully-formed JSON document or nothing.
 *  - Cadence: fixed 500 ms. At that rate the I/O cost is negligible
 *    (one stat-sized write + one rename per tick) and human readers
 *    feel "live". Writes are skipped when the serialised payload is
 *    byte-identical to the previous one, so a quiescent daemon does
 *    nothing after the first publish until peer state actually
 *    changes — plus a heartbeat every `HEARTBEAT_MS` to refresh the
 *    `updatedAt` timestamp so readers can distinguish "stable state"
 *    from "dead daemon".
 *  - Cleanup: removed on `stop()`. If the daemon crashes without
 *    running stop(), the file is left behind; readers detect this via
 *    `updatedAt` staleness (and, if needed, the `pid` field).
 *
 * The file format is a versioned JSON object (`version: 1`). Future
 * schema changes must bump the version field; readers ignore beacons
 * whose `version` they do not recognise.
 */

import { unlink, writeFile, rename, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import type { ConnectedPeer, SyncHandle, SyncSnapshot } from './start.js';
import type { SyncEvent, SyncStats } from '../core/syncEvents.js';

export const STATE_BEACON_FILENAME = '.nearbytes-sync.state.json';
export const STATE_BEACON_TMP_SUFFIX = '.nearbytes-sync.state.tmp';

const TICK_MS = 500;
const HEARTBEAT_MS = 5_000;

export interface SyncStateBeaconPayload {
  readonly version: 1;
  readonly pid: number;
  readonly dataDir: string;
  /** ISO-8601 UTC. */
  readonly updatedAt: string;
  /**
   * Per-`dataDir` node identity (DISC-26). Optional for backward
   * compatibility with beacons written before this field existed.
   */
  readonly peerId?: string;
  /**
   * Active served profile public key (hex). Optional for the same
   * back-compat reason as `peerId`.
   */
  readonly activeProfilePublicKey?: string;
  readonly snapshot: SyncSnapshot;
  readonly peers: ReadonlyArray<{
    readonly remoteProfilePublicKey: string;
    readonly remotePeerId: string;
    readonly transportLabel: string;
    readonly localAssociationProfile: string;
    /** ISO-8601 UTC. */
    readonly connectedAt: string;
    readonly role: 'sibling' | 'friend';
  }>;
  /**
   * Most-recent wire-level events as exposed by `SyncHandle.recentEvents()`.
   * Optional for backward compatibility: a beacon written by a daemon
   * that predates the event bus omits this field, and readers MUST
   * treat it as an empty list (not as an error).
   *
   * Events are oldest-first. Timestamps are epoch ms (the same shape
   * the in-process `SyncEvent` carries), so a writer-only consumer can
   * format them with no conversion drift relative to a live consumer.
   */
  readonly events?: readonly SyncEvent[];
  /**
   * Cumulative + windowed throughput counters from
   * `SyncHandle.stats()`. Optional: older daemons that predate the
   * stats accumulator omit it, and readers MUST treat absence as
   * zeroed counters (not as an error).
   */
  readonly stats?: SyncStats;
}

export interface StateBeaconHandle {
  /** Stop publishing and remove the beacon file. */
  stop(): Promise<void>;
}

/**
 * Start a periodic state beacon publishing into `<dataDir>/.nearbytes-sync.state.json`.
 * Caller passes in a `SyncHandle`; we call `snapshot()` + `peers()` on it
 * each tick. The returned handle's `stop()` is idempotent.
 */
export function startStateBeacon(opts: {
  readonly dataDir: string;
  readonly sync: SyncHandle;
  /** Override default tick interval (default 500 ms). */
  readonly tickMs?: number;
}): StateBeaconHandle {
  const tickMs = opts.tickMs ?? TICK_MS;
  const finalPath = join(opts.dataDir, STATE_BEACON_FILENAME);
  const tmpPath = join(opts.dataDir, STATE_BEACON_TMP_SUFFIX);

  let stopped = false;
  let writing: Promise<void> = Promise.resolve();
  let lastSerialised: string | null = null;
  let lastWriteAt = 0;

  const buildPayload = (): SyncStateBeaconPayload => {
    const peers = opts.sync.peers();
    return {
      version: 1,
      pid: process.pid,
      dataDir: opts.dataDir,
      updatedAt: new Date().toISOString(),
      peerId: opts.sync.peerId,
      activeProfilePublicKey: opts.sync.activeProfilePublicKey,
      snapshot: opts.sync.snapshot(),
      peers: peers.map((p) => ({
        remoteProfilePublicKey: p.remoteProfilePublicKey,
        remotePeerId: p.remotePeerId,
        transportLabel: p.transportLabel,
        localAssociationProfile: p.localAssociationProfile,
        connectedAt: p.connectedAt.toISOString(),
        role: p.role,
      })),
      events: opts.sync.recentEvents(),
      stats: opts.sync.stats(),
    };
  };

  /**
   * The serialiser strips `updatedAt` so two snapshots that differ ONLY
   * in their timestamp compare equal. That is what makes the "skip
   * write on identical payload" optimisation actually skip during
   * quiescence — the wall-clock would otherwise tick every iteration.
   */
  const dedupKey = (payload: SyncStateBeaconPayload): string => {
    const { updatedAt: _ignored, ...rest } = payload;
    return JSON.stringify(rest);
  };

  const publish = async (): Promise<void> => {
    if (stopped) return;
    const payload = buildPayload();
    const key = dedupKey(payload);
    const now = Date.now();
    const skipBecauseUnchanged =
      key === lastSerialised && now - lastWriteAt < HEARTBEAT_MS;
    if (skipBecauseUnchanged) return;
    const body = JSON.stringify(payload, null, 2);
    await writeFile(tmpPath, body, 'utf8');
    await rename(tmpPath, finalPath);
    lastSerialised = key;
    lastWriteAt = now;
  };

  const tick = (): void => {
    if (stopped) return;
    writing = writing
      .then(publish)
      .catch((err) => {
        process.stderr.write(
          `[nearbytes-sync:beacon] publish failed (continuing): ${String(err)}\n`,
        );
      })
      .finally(() => {
        if (!stopped) {
          const t = setTimeout(tick, tickMs);
          t.unref();
        }
      });
  };

  /**
   * Fire the first publish on the next macrotask (not synchronously) so
   * callers can `startStateBeacon(...)` and immediately return without
   * triggering a write before they have set up their own error handlers.
   */
  const initial = setTimeout(tick, 0);
  initial.unref();

  return {
    async stop(): Promise<void> {
      stopped = true;
      await writing.catch(() => {});
      await unlink(finalPath).catch(() => {});
      await unlink(tmpPath).catch(() => {});
    },
  };
}

// ── reader ────────────────────────────────────────────────────────────────

export interface ReadBeaconResult {
  /** ISO-8601 UTC of the snapshot we read. */
  readonly updatedAt: string;
  /** Milliseconds since the snapshot was written (now − updatedAt). */
  readonly ageMs: number;
  readonly payload: SyncStateBeaconPayload;
}

/**
 * Read and parse the beacon file. Returns `null` if the file does not
 * exist, is malformed, or carries an unknown `version`. Callers
 * interpret freshness from `ageMs` (e.g. >5_000 ms = stale → daemon
 * may be hung).
 */
export async function readSyncStateBeacon(dataDir: string): Promise<ReadBeaconResult | null> {
  const path = join(dataDir, STATE_BEACON_FILENAME);
  let body: string;
  try {
    body = await readFile(path, 'utf8');
  } catch {
    return null;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch {
    return null;
  }
  if (
    typeof parsed !== 'object' ||
    parsed === null ||
    (parsed as { version?: unknown }).version !== 1
  ) {
    return null;
  }
  const payload = parsed as SyncStateBeaconPayload;
  if (
    typeof payload.updatedAt !== 'string' ||
    typeof payload.pid !== 'number' ||
    typeof payload.dataDir !== 'string' ||
    typeof payload.snapshot !== 'object' ||
    !Array.isArray(payload.peers)
  ) {
    return null;
  }
  // The `events` field is *optional* (older daemons did not publish one),
  // so its absence is fine; presence with the wrong shape is a hard reject
  // because rendering garbage events would be worse than rendering none.
  if (payload.events !== undefined && !Array.isArray(payload.events)) {
    return null;
  }
  // Same back-compat policy for `stats`: optional, but if present it
  // must be an object (we do not deep-validate every numeric field
  // because the renderer already coerces sane defaults).
  if (
    payload.stats !== undefined &&
    (typeof payload.stats !== 'object' || payload.stats === null)
  ) {
    return null;
  }
  const updatedAtMs = Date.parse(payload.updatedAt);
  if (Number.isNaN(updatedAtMs)) return null;
  return {
    updatedAt: payload.updatedAt,
    ageMs: Math.max(0, Date.now() - updatedAtMs),
    payload,
  };
}
