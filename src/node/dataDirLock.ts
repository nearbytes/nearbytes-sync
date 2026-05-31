/**
 * Sync-singleton lock for a Nearbytes `dataDir`.
 *
 * Spec reference: `requirements/sync-discovery-v1.md` DISC-27.
 *
 * ## The invariant
 *
 * **At most one process may run the Nearbytes sync engine against a given
 * `dataDir` at any time.** "Sync engine" here means discovery (mDNS +
 * Hyperswarm joins, DHT announcements), peer-loop ingest/egress, and the
 * outbound block-pump scheduler. The append-only log itself is *not* lock
 * protected — content-addressed events and blocks are CRDT-trivial under
 * concurrent writers (same hash ⇒ same bytes ⇒ atomic-link publish wins
 * exactly once), so multiple processes MAY write into the same dataDir.
 *
 * What we are protecting against is *duplicate sync*: two daemons joining
 * the same Hyperswarm topic from the same instance identity would announce the
 * same instance public key twice, double the friend-session bookkeeping at each peer,
 * waste bandwidth, and confuse the DISC-26 sibling carriage logic that
 * relies on instance identity to dedupe loopback.
 *
 * ## On-disk shape
 *
 * The lock is `<dataDir>/.nearbytes-sync.lock` — a one-line text file
 * containing the holder's pid plus a creation timestamp:
 *
 * ```
 * 12345 2026-05-26T09:00:00.000Z
 * ```
 *
 * The pid alone would be enough for liveness; the timestamp is exposed in
 * `probeSyncLock()` so callers can render friendly "held since" messages
 * without parsing `/proc` or `ps`.
 *
 * ## Acquisition semantics
 *
 *  - We create the file with `O_EXCL` (`flag: 'wx'`). If it already exists
 *    and the holder pid is alive we throw {@link SyncAlreadyRunningError}.
 *  - If the holder pid is dead (the previous process crashed without
 *    unlinking) we treat the lock as stale and silently replace it.
 *  - On clean shutdown the lock is unlinked. A `process.on('exit')` hook
 *    fires the same unlink so a forgotten `stop()` still releases it.
 *
 * ## Why pid-file instead of `flock(2)`
 *
 * `flock(2)` is the textbook "kernel releases on FD close" lock and would
 * be marginally more robust against crashes. Node has no built-in for it
 * (`fs.flock` does not exist); reaching for `flock` means a native module
 * dependency (`fs-ext`) on every install on every platform. The pid-file
 * approach has one weakness — pid reuse — which `isPidAlive` only mitigates
 * probabilistically. In a single-user desktop context the failure mode is
 * "a tiny window after a crash where a re-used pid blocks startup with a
 * misleading message", which is acceptable for this codebase's scope.
 * Migration to `flock` later is one file change away.
 */

import {
  closeSync,
  existsSync,
  mkdirSync,
  openSync,
  readFileSync,
  statSync,
  unlinkSync,
  writeFileSync,
} from 'node:fs';
import { join } from 'node:path';

export const DATADIR_LOCK_FILENAME = '.nearbytes-sync.lock';

/**
 * Inspection result for {@link probeSyncLock}. The `running: false` variant
 * is a positive answer ("no daemon is here"), not a "couldn't tell" — the
 * probe only returns `running: false` after confirming the lock file is
 * absent or its holder pid is dead.
 */
export type SyncLockStatus =
  | { readonly running: false }
  | {
      readonly running: true;
      readonly holderPid: number;
      readonly lockPath: string;
      /**
       * Wall-clock time the lock file was created (filesystem `birthtime`,
       * falling back to `mtime` on filesystems that do not record btime).
       * Local clock; see `application/file-events-v0.4.md` §2.5 for the
       * trade-offs of trusting unsynchronised wall clocks.
       */
      readonly heldSince: Date;
    };

/**
 * Thrown by {@link acquireSyncLock} when another live process already holds
 * the sync-singleton lock for this `dataDir`. The status field carries the
 * same shape {@link probeSyncLock} returns, so error consumers can present
 * a uniform "held by pid X since Y" message without re-probing.
 */
export class SyncAlreadyRunningError extends Error {
  readonly status: Extract<SyncLockStatus, { running: true }>;

  constructor(status: Extract<SyncLockStatus, { running: true }>) {
    super(
      `nearbytes-sync: dataDir is already in use by pid ${status.holderPid} ` +
        `(lock at ${status.lockPath}, held since ${status.heldSince.toISOString()}). ` +
        `Stop the other process or remove the stale lock file.`,
    );
    this.name = 'SyncAlreadyRunningError';
    this.status = status;
  }
}

/**
 * Returns true iff `pid` corresponds to a process this OS user can signal.
 * `process.kill(pid, 0)` performs no signal delivery but still raises
 * EPERM/ESRCH for missing pids, which is exactly the existence check we
 * want for stale-lock detection.
 */
function isPidAlive(pid: number): boolean {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    // EPERM means the process exists but we can't signal it — still alive.
    return code === 'EPERM';
  }
}

interface ParsedLock {
  readonly pid: number;
  readonly heldSince: Date;
}

function parseLockContents(raw: string, lockPath: string): ParsedLock | null {
  const trimmed = raw.trim();
  if (trimmed.length === 0) return null;
  const [pidStr, isoStr] = trimmed.split(/\s+/, 2);
  const pid = Number.parseInt(pidStr ?? '', 10);
  if (!Number.isInteger(pid) || pid <= 0) return null;
  let heldSince: Date;
  if (typeof isoStr === 'string' && isoStr.length > 0) {
    heldSince = new Date(isoStr);
    if (Number.isNaN(heldSince.getTime())) {
      heldSince = filesystemBirthTime(lockPath);
    }
  } else {
    heldSince = filesystemBirthTime(lockPath);
  }
  return { pid, heldSince };
}

function filesystemBirthTime(path: string): Date {
  try {
    const stats = statSync(path);
    const birth = stats.birthtimeMs > 0 ? stats.birthtimeMs : stats.mtimeMs;
    return new Date(birth);
  } catch {
    return new Date(0);
  }
}

/**
 * Non-blocking, side-effect-free probe. Returns `{ running: false }` when:
 *   - the lock file does not exist, OR
 *   - the lock file exists but its holder pid is dead (stale).
 *
 * In the stale case the file is NOT removed here — that is the job of the
 * next {@link acquireSyncLock} call (which has writer intent). A probe
 * never mutates the filesystem.
 *
 * Returns `{ running: true, ... }` when the lock file exists and its
 * holder pid is alive.
 */
export function probeSyncLock(dataDir: string): SyncLockStatus {
  const lockPath = join(dataDir, DATADIR_LOCK_FILENAME);
  if (!existsSync(lockPath)) return { running: false };
  let raw: string;
  try {
    raw = readFileSync(lockPath, 'utf8');
  } catch {
    return { running: false };
  }
  const parsed = parseLockContents(raw, lockPath);
  if (parsed === null) return { running: false };
  if (!isPidAlive(parsed.pid)) return { running: false };
  return {
    running: true,
    holderPid: parsed.pid,
    lockPath,
    heldSince: parsed.heldSince,
  };
}

/**
 * Acquires the sync-singleton lock for `dataDir`. Throws
 * {@link SyncAlreadyRunningError} if another live process holds it. A stale
 * lock (holder pid dead) is silently replaced.
 *
 * The returned function releases the lock; a `process.on('exit')` hook is
 * also registered so a clean Node shutdown unlinks the file even if the
 * caller forgot to `stop()`.
 *
 * Pass `undefined` for `dataDir` to opt out entirely (purely in-memory
 * deployments with no on-disk storage — there is nothing to protect).
 */
export function acquireSyncLock(dataDir: string | undefined): () => void {
  if (dataDir === undefined) return () => {};
  mkdirSync(dataDir, { recursive: true });
  const lockPath = join(dataDir, DATADIR_LOCK_FILENAME);

  const takeLock = (): void => {
    const fd = openSync(lockPath, 'wx');
    writeFileSync(fd, `${process.pid} ${new Date().toISOString()}\n`, 'utf8');
    closeSync(fd);
  };

  try {
    takeLock();
  } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code !== 'EEXIST') throw err;
    const status = probeSyncLock(dataDir);
    if (status.running) {
      throw new SyncAlreadyRunningError(status);
    }
    // Stale lock from a crashed previous run — replace it.
    unlinkSync(lockPath);
    takeLock();
  }

  let released = false;
  const release = (): void => {
    if (released) return;
    released = true;
    try {
      if (!existsSync(lockPath)) return;
      const raw = readFileSync(lockPath, 'utf8');
      const parsed = parseLockContents(raw, lockPath);
      if (parsed !== null && parsed.pid === process.pid) {
        unlinkSync(lockPath);
      }
    } catch {
      // best-effort
    }
  };
  process.once('exit', release);
  return release;
}
