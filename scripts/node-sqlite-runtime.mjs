/**
 * node:sqlite is built-in from Node 22.5, but requires --experimental-sqlite on
 * 22.5–22.12; unflagged from 22.13 (nodejs/node#55890).
 */
import { spawnSync } from 'node:child_process';

const SQLITE_FLAG = '--experimental-sqlite';
const reexecFlag = 'NB_SQLITE_FLAG_APPLIED';

export function versionGte(actual, required) {
  const a = actual.split('.').map((n) => Number(n));
  const r = required.split('.').map((n) => Number(n));
  for (let i = 0; i < Math.max(a.length, r.length); i++) {
    const av = a[i] ?? 0;
    const rv = r[i] ?? 0;
    if (av > rv) return true;
    if (av < rv) return false;
  }
  return true;
}

export function needsExperimentalSqlite(version = process.versions.node) {
  return versionGte(version, '22.5.0') && !versionGte(version, '22.13.0');
}

export function envWithSqliteFlag(env = process.env) {
  if (!needsExperimentalSqlite()) return env;
  const opts = env.NODE_OPTIONS ?? '';
  if (opts.includes('experimental-sqlite')) return env;
  return { ...env, NODE_OPTIONS: `${opts} ${SQLITE_FLAG}`.trim() };
}

export function maybeReexecForSqliteFlag(entryScript, extraArgs = []) {
  if (!needsExperimentalSqlite() || process.env[reexecFlag] === '1') return;

  const env = envWithSqliteFlag(process.env);
  env[reexecFlag] = '1';
  const r = spawnSync(process.execPath, [entryScript, ...extraArgs], {
    cwd: process.cwd(),
    stdio: 'inherit',
    env,
  });
  process.exit(r.status ?? 0);
}
