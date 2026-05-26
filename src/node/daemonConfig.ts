/**
 * Configuration schema and loader for the Nearbytes sync daemon (`nbsync`).
 *
 * The on-disk format is a JSON file (default `~/.nearbytes/config.json`)
 * shared with `nearbytes-skeleton` — the daemon reads only the sync-
 * relevant subset (`dataDir`, `profiles[]`, `activeProfile`, `friends[]`)
 * and ignores any other keys (e.g. the file-cli's `volumes`).
 *
 * Loading is intentionally permissive: missing fields fall back to sane
 * defaults, malformed entries inside a list are silently skipped. The
 * only hard failures are "file unreadable" and "not valid JSON" — those
 * are operator misconfigurations the daemon MUST not paper over.
 */

import { readFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { homedir } from 'node:os';

export interface DaemonProfileConfig {
  /** Local name (unique within `profiles`), used by `profile use <name>`. */
  readonly name: string;
  /** Profile secret (`name:password`); not a volume secret. */
  readonly secret: string;
}

export interface SyncDaemonConfig {
  /** Storage root for blocks, channels, and the dataDir lock. */
  readonly dataDir: string;
  /** Friend profile public keys (hex) to carry sync for. */
  readonly friends: readonly string[];
  /** Local served profiles; empty ⇒ daemon idles (per SYNC-00). */
  readonly profiles: readonly DaemonProfileConfig[];
  /** Active profile name (one of `profiles[].name`); null when empty. */
  readonly activeProfile: string | null;
}

const DEFAULT_CONFIG_DIR = join(homedir(), '.nearbytes');
const DEFAULT_CONFIG_FILE = join(DEFAULT_CONFIG_DIR, 'config.json');
const DEFAULT_DATA_DIR = join(homedir(), 'nearbytes', 'local');

export function defaultDaemonConfigPath(): string {
  return process.env['NEARBYTES_CONFIG'] ?? DEFAULT_CONFIG_FILE;
}

export function defaultDaemonDataDir(): string {
  return process.env['NEARBYTES_STORAGE_DIR'] ?? DEFAULT_DATA_DIR;
}

function readStringField(obj: Record<string, unknown>, key: string, fallback: string): string {
  const v = obj[key];
  return typeof v === 'string' && v.trim().length > 0 ? v.trim() : fallback;
}

function readProfiles(obj: Record<string, unknown>): DaemonProfileConfig[] {
  const out: DaemonProfileConfig[] = [];
  const seen = new Set<string>();
  if (Array.isArray(obj['profiles'])) {
    for (const entry of obj['profiles']) {
      if (typeof entry !== 'object' || entry === null) continue;
      const e = entry as Record<string, unknown>;
      const name = typeof e['name'] === 'string' ? e['name'].trim() : '';
      const secret = typeof e['secret'] === 'string' ? e['secret'].trim() : '';
      if (name.length === 0 || secret.length === 0 || seen.has(name)) continue;
      seen.add(name);
      out.push({ name, secret });
    }
  }
  if (out.length === 0 && typeof obj['profileSecret'] === 'string') {
    const legacy = (obj['profileSecret'] as string).trim();
    if (legacy.length > 0) out.push({ name: 'default', secret: legacy });
  }
  return out;
}

function readActiveProfile(
  obj: Record<string, unknown>,
  profiles: readonly DaemonProfileConfig[],
): string | null {
  if (profiles.length === 0) return null;
  const names = new Set(profiles.map((p) => p.name));
  const raw = obj['activeProfile'];
  if (typeof raw === 'string' && names.has(raw)) return raw;
  return profiles[0]!.name;
}

function readFriends(obj: Record<string, unknown>): string[] {
  if (!Array.isArray(obj['friends'])) return [];
  const out: string[] = [];
  for (const f of obj['friends']) {
    if (typeof f === 'string' && f.trim().length > 0) {
      out.push(f.trim().toLowerCase());
    }
  }
  return out;
}

function mergeWithDefaults(raw: unknown): SyncDaemonConfig {
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
    throw new Error('Config must be a JSON object');
  }
  const obj = raw as Record<string, unknown>;
  const profiles = readProfiles(obj);
  return {
    dataDir: readStringField(obj, 'dataDir', defaultDaemonDataDir()),
    friends: readFriends(obj),
    profiles,
    activeProfile: readActiveProfile(obj, profiles),
  };
}

const EMPTY_CONFIG: SyncDaemonConfig = {
  dataDir: defaultDaemonDataDir(),
  friends: [],
  profiles: [],
  activeProfile: null,
};

/**
 * Read and parse the sync-daemon config file. Missing or empty files are
 * silently accepted (daemon idles); malformed JSON or non-object roots
 * throw with the file path in the message.
 */
export async function readDaemonConfig(configPath?: string): Promise<SyncDaemonConfig> {
  const file = configPath ?? defaultDaemonConfigPath();
  if (!existsSync(file)) return EMPTY_CONFIG;
  let raw: string;
  try {
    raw = await readFile(file, 'utf-8');
  } catch (err) {
    throw new Error(`nbsync: cannot read config file ${file}: ${String(err)}`);
  }
  if (raw.trim().length === 0) return EMPTY_CONFIG;
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error(`nbsync: config file ${file} is not valid JSON`);
  }
  return mergeWithDefaults(parsed);
}

/**
 * Reduce two configs to a minimal change vector. Used by the daemon's
 * config watcher to decide whether a file change is actually relevant
 * (avoiding spurious sync restarts on whitespace-only edits, mtime
 * touches, etc.).
 */
export function configsEquivalent(a: SyncDaemonConfig, b: SyncDaemonConfig): boolean {
  if (a.dataDir !== b.dataDir) return false;
  if (a.activeProfile !== b.activeProfile) return false;
  if (a.friends.length !== b.friends.length) return false;
  for (let i = 0; i < a.friends.length; i++) {
    if (a.friends[i] !== b.friends[i]) return false;
  }
  if (a.profiles.length !== b.profiles.length) return false;
  for (let i = 0; i < a.profiles.length; i++) {
    if (a.profiles[i]!.name !== b.profiles[i]!.name) return false;
    if (a.profiles[i]!.secret !== b.profiles[i]!.secret) return false;
  }
  return true;
}
