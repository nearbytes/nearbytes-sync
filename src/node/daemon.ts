/**
 * Nearbytes sync daemon (`nbsync daemon`).
 *
 * Long-running process that owns the DISC-27 sync-singleton lock for a
 * `dataDir`, runs the wire protocol, and watches both the config file
 * (live-reload of friends/profiles) and the on-disk dataDir (so events
 * authored by *other* processes against the same dataDir — typically the
 * file-cli — are picked up and announced to peers without restarting).
 *
 * ## Architecture
 *
 *  config.json  ─► fs.watch ─► debounce ─► reloadSync()
 *
 *  channels/<pk>/<hash>.bin  ─► chokidar 'add' ─► log.reception.appendReception()
 *  blocks/<hash>.bin         ─► chokidar 'add' ─► log.reception.appendReception()
 *
 *  SIGTERM / SIGINT          ─► flush, stop sync, release lock, exit 0
 *
 * Reception appends are idempotent in their *effect* but not in the
 * journal: the patched `appendReception` (`patchLogForReactiveHave`)
 * fires `broadcastLocalHave` on each call, so a duplicate append yields
 * a duplicate `have` frame to each peer. That is harmless (peer either
 * already has the object — no-op — or hasn't, in which case the SYNC-14
 * single-flight `want` dedupes anyway). We accept the small wire-level
 * redundancy in exchange for a much simpler dedup model: every new file
 * we discover is announced once, no per-process "did I already announce
 * this hash" bookkeeping required.
 */

import { mkdir } from 'node:fs/promises';
import { existsSync, statSync, watch as fsWatch, type FSWatcher } from 'node:fs';
import { stat } from 'node:fs/promises';
import { basename, join, relative, sep } from 'node:path';
import chokidar from 'chokidar';
import { createCryptoOperations, createSecret, bytesToHex } from 'nearbytes-crypto';
import {
  createFilesystemLog,
  normalizeHash,
  type Log,
  type ReceptionObjectRef,
} from 'nearbytes-log';
import { start, type SyncHandle } from './start.js';
import {
  configsEquivalent,
  readDaemonConfig,
  type SyncDaemonConfig,
} from './daemonConfig.js';
import { probeSyncLock } from './dataDirLock.js';
import {
  STATE_BEACON_FILENAME,
  STATE_BEACON_TMP_SUFFIX,
  startStateBeacon,
  type StateBeaconHandle,
} from './stateBeacon.js';

export interface DaemonOptions {
  /** Absolute path to the JSON config file. */
  readonly configPath: string;
  /**
   * Optional hook called after every successful config reload. Useful
   * for tests; production use just reads the stderr log line we emit.
   */
  readonly onReload?: (config: SyncDaemonConfig) => void;
}

const HASH_RE = /^[0-9a-f]{64}\.bin$/i;
const PUBKEY_DIR_RE = /^[0-9a-f]{130}$/i;
const CONFIG_RELOAD_DEBOUNCE_MS = 250;

async function ensureDataDirLayout(dataDir: string): Promise<void> {
  await mkdir(join(dataDir, 'blocks'), { recursive: true });
  await mkdir(join(dataDir, 'channels'), { recursive: true });
}

function parseReceptionRef(absPath: string, dataDir: string): ReceptionObjectRef | null {
  const rel = relative(dataDir, absPath);
  if (rel.startsWith('..')) return null;
  const segments = rel.split(sep);
  if (segments.length === 2 && segments[0] === 'blocks') {
    const name = segments[1]!;
    if (!HASH_RE.test(name)) return null;
    const hash = normalizeHash(name.slice(0, 64));
    if (hash === null) return null;
    return { kind: 'block', hash };
  }
  if (segments.length === 3 && segments[0] === 'channels') {
    const channel = segments[1]!.toLowerCase();
    const name = segments[2]!;
    if (!PUBKEY_DIR_RE.test(channel)) return null;
    if (!HASH_RE.test(name)) return null;
    const hash = normalizeHash(name.slice(0, 64));
    if (hash === null) return null;
    return { kind: 'event', channel, hash };
  }
  return null;
}

async function deriveProfilePublicKeys(
  profiles: readonly { secret: string }[],
): Promise<string[]> {
  const crypto = createCryptoOperations();
  const out: string[] = [];
  for (const p of profiles) {
    const keyPair = await crypto.deriveKeys(createSecret(p.secret));
    out.push(bytesToHex(keyPair.publicKey).toLowerCase());
  }
  return out;
}

async function startSyncFromConfig(log: Log, config: SyncDaemonConfig): Promise<SyncHandle> {
  if (config.profiles.length === 0 || config.activeProfile === null) {
    return inertHandle();
  }
  const servedPks = await deriveProfilePublicKeys(config.profiles);
  const activeIdx = config.profiles.findIndex((p) => p.name === config.activeProfile);
  if (activeIdx < 0) {
    throw new Error(`activeProfile "${config.activeProfile}" not in profiles[]`);
  }
  const discoveryTransport =
    process.env['NEARBYTES_SYNC_DISCOVERY'] === 'mdns' ? ('mdns' as const) : undefined;
  return start(log, config.friends, {
    serveProfilePublicKeys: servedPks,
    activeProfilePublicKey: servedPks[activeIdx]!,
    blockStorageRoot: config.dataDir,
    ...(discoveryTransport ? { discoveryTransport } : {}),
  });
}

function inertHandle(): SyncHandle {
  return {
    friends: [],
    serveProfilePublicKeys: [],
    snapshot: () => ({ inflightInbound: 0, inflightOutbound: 0, connectedPeers: 0 }),
    peers: () => [],
    onEvent: () => () => {},
    recentEvents: () => [],
    stop: async () => {},
  };
}

/**
 * Watches `configPath` for changes and invokes `onReload(nextConfig)`
 * when the parsed contents differ from `lastApplied`. `fs.watch` fires
 * 2–3 times per atomic rename on most platforms; we debounce by
 * comparing the post-event mtime to the last successfully-applied one
 * (the parse step is the real dedup — see `configsEquivalent`).
 *
 * Reload failures (parse error, invalid schema) are logged and DO NOT
 * abort the daemon: the previous config remains in effect and the
 * operator can fix the file and save again.
 */
function watchConfigFile(
  configPath: string,
  onReload: (next: SyncDaemonConfig) => Promise<void>,
): { close(): void } {
  let lastMtimeMs = existsSync(configPath) ? statSync(configPath).mtimeMs : 0;
  let chain: Promise<void> = Promise.resolve();
  let pending: NodeJS.Timeout | null = null;
  let watcher: FSWatcher | null = null;
  try {
    watcher = fsWatch(configPath, { persistent: true });
  } catch (err) {
    console.error('[nbsync] config watcher disabled (fs.watch failed):', err);
    return { close: () => {} };
  }
  watcher.on('error', (err) => {
    console.error('[nbsync] config watcher error:', err);
  });
  watcher.on('change', () => {
    if (pending !== null) clearTimeout(pending);
    pending = setTimeout(() => {
      pending = null;
      chain = chain.then(async () => {
        let mtimeMs: number;
        try {
          mtimeMs = (await stat(configPath)).mtimeMs;
        } catch {
          return;
        }
        if (mtimeMs === lastMtimeMs) return;
        lastMtimeMs = mtimeMs;
        try {
          const next = await readDaemonConfig(configPath);
          await onReload(next);
        } catch (err) {
          console.error('[nbsync] config reload failed (keeping previous):', err);
        }
      });
    }, CONFIG_RELOAD_DEBOUNCE_MS);
    if (typeof pending.unref === 'function') pending.unref();
  });
  return {
    close: () => {
      if (pending !== null) clearTimeout(pending);
      watcher?.close();
    },
  };
}

/**
 * Main entry point. Returns a promise that resolves only on shutdown
 * (SIGTERM, SIGINT). Throws at boot if the dataDir lock is already held
 * by another live process (DISC-27 violation).
 */
export async function runDaemon(options: DaemonOptions): Promise<void> {
  let config = await readDaemonConfig(options.configPath);
  console.error(`[nbsync] boot · config=${options.configPath} · dataDir=${config.dataDir}`);

  const status = probeSyncLock(config.dataDir);
  if (status.running) {
    throw new Error(
      `nbsync: another sync daemon is already running for ${config.dataDir} ` +
        `(pid ${status.holderPid}, held since ${status.heldSince.toISOString()}).`,
    );
  }

  await ensureDataDirLayout(config.dataDir);
  const log = createFilesystemLog(config.dataDir);
  let sync = await startSyncFromConfig(log, config);
  reportUp(config, sync);

  /**
   * Publish snapshot+peers state for out-of-process observers (e.g.
   * `nbf monitor` in writer-only mode). Lifetime matches the daemon
   * itself; restarted across config reloads to follow the new
   * SyncHandle, then torn down on shutdown.
   */
  let beacon: StateBeaconHandle = startStateBeacon({ dataDir: config.dataDir, sync });

  const dataWatcher = chokidar.watch(
    [join(config.dataDir, 'channels'), join(config.dataDir, 'blocks')],
    {
      ignored: (path) =>
        path.endsWith('.tmp') ||
        /\.[0-9a-f]{16}\.tmp$/i.test(path) ||
        path.endsWith(STATE_BEACON_FILENAME) ||
        path.endsWith(STATE_BEACON_TMP_SUFFIX),
      ignoreInitial: true,
      persistent: true,
      awaitWriteFinish: { stabilityThreshold: 100, pollInterval: 50 },
    },
  );
  dataWatcher.on('add', (absPath: string) => {
    const ref = parseReceptionRef(absPath, config.dataDir);
    if (ref === null) return;
    log.reception.appendReception(ref).catch((err) => {
      console.error(`[nbsync] reception append failed for ${basename(absPath)}:`, err);
    });
  });
  dataWatcher.on('error', (err: unknown) => {
    console.error('[nbsync] dataDir watcher error:', err);
  });

  let reloadInFlight: Promise<void> = Promise.resolve();
  const applyReload = async (next: SyncDaemonConfig): Promise<void> => {
    if (configsEquivalent(config, next)) return;
    if (next.dataDir !== config.dataDir) {
      console.error(
        `[nbsync] ignoring dataDir change (live reload not supported; restart the daemon to switch dataDir from ${config.dataDir} to ${next.dataDir})`,
      );
    }
    console.error(
      `[nbsync] reload · profiles ${config.profiles.length}→${next.profiles.length} · friends ${config.friends.length}→${next.friends.length}`,
    );
    await beacon.stop();
    await sync.stop();
    config = { ...next, dataDir: config.dataDir };
    sync = await startSyncFromConfig(log, config);
    beacon = startStateBeacon({ dataDir: config.dataDir, sync });
    reportUp(config, sync);
    options.onReload?.(config);
  };

  const configWatcher = watchConfigFile(options.configPath, async (next) => {
    reloadInFlight = reloadInFlight.then(() => applyReload(next));
    await reloadInFlight;
  });

  let stopping = false;
  const shutdown = async (sig: string): Promise<void> => {
    if (stopping) return;
    stopping = true;
    console.error(`[nbsync] ${sig} → stopping`);
    configWatcher.close();
    await dataWatcher.close();
    await reloadInFlight.catch(() => {});
    await beacon.stop();
    await sync.stop();
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGHUP', () => {
    // SIGHUP triggers a manual re-read (idiomatic for daemons that
    // pre-date inotify). We re-read the file and apply the same
    // reload path the watcher uses.
    void (async () => {
      try {
        const next = await readDaemonConfig(options.configPath);
        reloadInFlight = reloadInFlight.then(() => applyReload(next));
        await reloadInFlight;
      } catch (err) {
        console.error('[nbsync] SIGHUP reload failed (keeping previous):', err);
      }
    })();
  });

  // Keep the process alive for the lifetime of `sync` — the SyncHandle's
  // internal sockets / timers hold the event loop without any explicit
  // keepalive on our part. `runDaemon` resolves only when we exit.
  await new Promise<void>(() => {});
}

function reportUp(config: SyncDaemonConfig, sync: SyncHandle): void {
  const profileTag = config.activeProfile ?? '(none)';
  console.error(
    `[nbsync] up · pid=${process.pid} · profile=${profileTag} · served=${sync.serveProfilePublicKeys.length} · friends=${sync.friends.length}`,
  );
}
