import { randomBytes } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const NODE_ID_FILENAME = '.nearbytes-node-id';
const NODE_ID_RE = /^[0-9a-f]{32}$/;

export function loadOrCreateNodeId(dataDir: string | undefined): string {
  if (dataDir === undefined) return randomBytes(16).toString('hex');
  mkdirSync(dataDir, { recursive: true });
  const file = join(dataDir, NODE_ID_FILENAME);
  if (existsSync(file)) {
    const existing = readFileSync(file, 'utf8').trim().toLowerCase();
    if (NODE_ID_RE.test(existing)) return existing;
  }
  const fresh = randomBytes(16).toString('hex');
  writeFileSync(file, fresh, { encoding: 'utf8', flag: 'wx' });
  return fresh;
}

export function peekNodeId(dataDir: string): string {
  try {
    const file = join(dataDir, NODE_ID_FILENAME);
    if (!existsSync(file)) return '';
    const existing = readFileSync(file, 'utf8').trim().toLowerCase();
    return NODE_ID_RE.test(existing) ? existing : '';
  } catch {
    return '';
  }
}
