#!/usr/bin/env node
/**
 * One-shot deploy: pull, install, build, refresh the user systemd unit,
 * and start/restart nearbytes-syncd immediately (no login required).
 *
 *   yarn daemon:update
 */
import { existsSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { platform } from 'node:os';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const unitName = 'nearbytes-syncd.service';

function run(label, cmd, args, { optional = false } = {}) {
  console.log(`[daemon:update] ${label}`);
  const r = spawnSync(cmd, args, { cwd: root, stdio: 'inherit', shell: true, env: process.env });
  if (r.status !== 0 && !optional) process.exit(r.status ?? 1);
  return r.status ?? 0;
}

if (platform() !== 'linux') {
  console.error('[daemon:update] systemd deploy is Linux-only');
  process.exit(2);
}

if (existsSync(resolve(root, '.git'))) {
  run('git pull --ff-only', 'git', ['pull', '--ff-only']);
}

run('yarn install', 'yarn', ['install']);
run('yarn build', 'yarn', ['build']);
run('install systemd unit', 'node', [resolve(root, 'scripts/install-systemd.mjs'), 'install']);
run('systemctl daemon-reload', 'systemctl', ['--user', 'daemon-reload']);
run('enable and start', 'systemctl', ['--user', 'enable', '--now', unitName]);
run('restart', 'systemctl', ['--user', 'restart', unitName]);
run('status', 'systemctl', ['--user', 'status', unitName, '--no-pager']);

console.log('[daemon:update] done.');
