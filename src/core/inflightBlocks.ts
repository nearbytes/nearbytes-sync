import { resolve } from 'node:path';
import type { Log } from 'nearbytes-log';

/**
 * Process-wide registries keyed by normalized block storage root.
 *
 * Yarn workspaces can resolve multiple physical copies of `nearbytes-sync`
 * (skeleton, engine, benchmarks each carry `node_modules/nearbytes-sync`).
 * Module-level Maps are NOT shared across those copies, so duplicate pumps
 * and duplicate wants slipped through on LAN runs. `globalThis` gives one
 * store per dataDir for the whole Node process.
 */
const OUTBOUND_SERVED_SYM = Symbol.for('@nearbytes/sync/outboundServed');
const INFLIGHT_BY_ROOT_SYM = Symbol.for('@nearbytes/sync/inflightByRoot');
const SETTLING_BY_ROOT_SYM = Symbol.for('@nearbytes/sync/settlingByRoot');

type GlobalSyncStores = typeof globalThis & {
  [OUTBOUND_SERVED_SYM]?: Map<string, Set<string>>;
  [INFLIGHT_BY_ROOT_SYM]?: Map<string, InflightBlockRegistry>;
  [SETTLING_BY_ROOT_SYM]?: Map<string, Set<string>>;
};

function normalizeStorageRoot(storageRoot: string): string {
  return resolve(storageRoot);
}

function globalMap<K, V>(sym: symbol): Map<K, V> {
  const store = globalThis as GlobalSyncStores & Record<symbol, Map<K, V> | undefined>;
  let map = store[sym];
  if (map === undefined) {
    map = new Map<K, V>();
    store[sym] = map;
  }
  return map;
}

/**
 * Per storage-root registry of in-flight `want(H)` block requests.
 *
 * Without this, two friend (or sibling) sessions that independently receive
 * `have(H)` from different peers will each emit `want(H)` and the same block
 * bytes will arrive twice over the wire.
 */
export class InflightBlockRegistry {
  private readonly inflight = new Set<string>();

  /** Returns true iff `hash` was not in-flight and is now reserved by the caller. */
  claim(hash: string): boolean {
    const key = hash.toLowerCase();
    if (process.env['NEARBYTES_OPT_INFLIGHT_DEDUP'] === '0') {
      this.inflight.add(key);
      return true;
    }
    if (this.inflight.has(key)) return false;
    this.inflight.add(key);
    return true;
  }

  release(hash: string): void {
    this.inflight.delete(hash.toLowerCase());
  }

  /** Number of hashes currently in flight (for `bye`-time quiesce snapshots). */
  size(): number {
    return this.inflight.size;
  }
}

/**
 * Per-Log counter of outbound block-stream pumps currently in `runOutbound`
 * across all open associations.
 */
export class OutboundBlockStreamCounter {
  private count = 0;

  begin(): void {
    this.count += 1;
  }

  end(): void {
    if (this.count > 0) this.count -= 1;
  }

  size(): number {
    return this.count;
  }
}

const outbound = new WeakMap<Log, OutboundBlockStreamCounter>();

export function outboundBlockStreamCounter(log: Log): OutboundBlockStreamCounter {
  let c = outbound.get(log);
  if (c === undefined) {
    c = new OutboundBlockStreamCounter();
    outbound.set(log, c);
  }
  return c;
}

const registries = new WeakMap<Log, InflightBlockRegistry>();

/** Legacy per-Log inbound registry (prefer {@link inflightBlockRegistryForStorage}). */
export function inflightBlockRegistry(log: Log): InflightBlockRegistry {
  let r = registries.get(log);
  if (r === undefined) {
    r = new InflightBlockRegistry();
    registries.set(log, r);
  }
  return r;
}

export function inflightBlockRegistryForStorage(storageRoot: string): InflightBlockRegistry {
  const root = normalizeStorageRoot(storageRoot);
  const map = globalMap<string, InflightBlockRegistry>(INFLIGHT_BY_ROOT_SYM);
  let r = map.get(root);
  if (r === undefined) {
    r = new InflightBlockRegistry();
    map.set(root, r);
  }
  return r;
}

function outboundServedSet(storageRoot: string): Set<string> {
  const root = normalizeStorageRoot(storageRoot);
  const map = globalMap<string, Set<string>>(OUTBOUND_SERVED_SYM);
  let s = map.get(root);
  if (s === undefined) {
    s = new Set();
    map.set(root, s);
  }
  return s;
}

/** Reserve `hash` for outbound pump; false if this storage root already serves/served it. */
export function claimOutboundServe(storageRoot: string, hash: string): boolean {
  const key = hash.toLowerCase();
  const served = outboundServedSet(storageRoot);
  if (served.has(key)) {
    return false;
  }
  served.add(key);
  return true;
}

export function releaseOutboundServe(storageRoot: string, hash: string): void {
  outboundServedSet(storageRoot).delete(hash.toLowerCase());
}

/** Block received on the wire; disk finalize not yet in the reception journal. */
export function markBlockSettling(storageRoot: string, hash: string): void {
  const root = normalizeStorageRoot(storageRoot);
  const map = globalMap<string, Set<string>>(SETTLING_BY_ROOT_SYM);
  let s = map.get(root);
  if (s === undefined) {
    s = new Set();
    map.set(root, s);
  }
  s.add(hash.toLowerCase());
}

export function clearBlockSettling(storageRoot: string, hash: string): void {
  const root = normalizeStorageRoot(storageRoot);
  globalMap<string, Set<string>>(SETTLING_BY_ROOT_SYM).get(root)?.delete(hash.toLowerCase());
}

export function isBlockSettling(storageRoot: string, hash: string): boolean {
  const root = normalizeStorageRoot(storageRoot);
  return globalMap<string, Set<string>>(SETTLING_BY_ROOT_SYM).get(root)?.has(hash.toLowerCase()) ?? false;
}
