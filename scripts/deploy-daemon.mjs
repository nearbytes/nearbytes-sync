#!/usr/bin/env node
/**
 * Failsafe deploy: ensure Node >= 22.13 (node:sqlite), pull, build, refresh
 * the user systemd unit with that Node binary, restart, and verify health.
 *
 *   yarn daemon:update
 */
import { existsSync, readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { platform } from 'node:os';
import { maybeReexecNvmrc } from './maybe-reexec-nvmrc.mjs';
import {
  envWithSqliteFlag,
  maybeReexecForSqliteFlag,
  versionGte,
} from './node-sqlite-runtime.mjs';
import { pathWithNodeBin } from './local-node.mjs';
import { captureNode, runNode, runYarn } from './toolchain.mjs';

const entry = fileURLToPath(import.meta.url);
const root = resolve(dirname(entry), '..');
const unitName = 'nearbytes-syncd.service';
const minNode = '22.13.0';
const healthTimeoutMs = 20_000;
const healthPollMs = 500;

maybeReexecNvmrc(entry);
maybeReexecForSqliteFlag(entry);

const env = envWithSqliteFlag(pathWithNodeBin(process.execPath, process.env));

function fail(message) {
  console.error(`[daemon:update] ${message}`);
  process.exit(1);
}

function run(label, cmd, args, { optional = false } = {}) {
  console.log(`[daemon:update] ${label}`);
  const r = spawnSync(cmd, args, { cwd: root, stdio: 'inherit', shell: true, env });
  if (r.status !== 0 && !optional) fail(`${label} failed (exit ${r.status ?? 1})`);
  return r.status ?? 0;
}

function capture(label, cmd, args) {
  console.log(`[daemon:update] ${label}`);
  return spawnSync(cmd, args, { cwd: root, encoding: 'utf8', shell: true, env });
}

async function assertRuntime() {
  if (!versionGte(process.versions.node, minNode)) {
    fail(
      `Node ${process.versions.node} is too old; need >= ${minNode} (see .nvmrc).`,
    );
  }
  try {
    await import('node:sqlite');
  } catch (err) {
    const detail = err instanceof Error ? err.message : String(err);
    fail(`Node at ${process.execPath} cannot load node:sqlite (${detail})`);
  }
  console.log(
    `[daemon:update] runtime ok — node ${process.versions.node} (${process.execPath})`,
  );
}

function assertUnitUsesNode(nodeBin) {
  const unitPath = resolve(
    process.env.HOME ?? '',
    '.config/systemd/user',
    unitName,
  );
  if (!existsSync(unitPath)) return;
  const unit = readFileSync(unitPath, 'utf8');
  const execLine = unit.split('\n').find((line) => line.startsWith('ExecStart='));
  if (!execLine?.includes(nodeBin)) {
    fail(
      `systemd unit does not use deploy Node binary.\n` +
        `  expected: ${nodeBin}\n` +
        `  unit:     ${execLine ?? '(missing ExecStart)'}`,
    );
  }
}

function waitForActive() {
  const deadline = Date.now() + healthTimeoutMs;
  while (Date.now() < deadline) {
    const r = capture('check active', 'systemctl', [
      '--user',
      'is-active',
      unitName,
    ]);
    const state = (r.stdout ?? '').trim();
    if (state === 'active') return;
    spawnSync('sleep', [`${healthPollMs / 1000}`], { stdio: 'ignore' });
  }
  fail(`service did not reach active within ${healthTimeoutMs / 1000}s`);
}

function mainPid() {
  const r = capture('read MainPID', 'systemctl', [
    '--user',
    'show',
    unitName,
    '-p',
    'MainPID',
    '--value',
  ]);
  const pid = Number.parseInt((r.stdout ?? '').trim(), 10);
  if (!Number.isFinite(pid) || pid <= 0) {
    fail('could not read MainPID from systemd');
  }
  return pid;
}

function assertHealthy(pid) {
  spawnSync('sleep', ['1'], { stdio: 'ignore' });

  const journal = capture('journal for current pid', 'journalctl', [
    '--user',
    '-u',
    unitName,
    `_PID=${pid}`,
    '-n',
    '30',
    '--no-pager',
  ]);
  const log = `${journal.stdout ?? ''}\n${journal.stderr ?? ''}`;
  if (/ERR_UNKNOWN_BUILTIN_MODULE|Error \[ERR_/.test(log)) {
    console.error(log);
    fail(`service pid ${pid} logged a startup error (see above)`);
  }

  const status = captureNode(root, [resolve(root, 'bin/nbsync.mjs'), 'status'], env);
  if (status.status !== 0) {
    console.error(status.stderr ?? status.stdout);
    fail('nbsync status failed after restart');
  }
  const out = status.stdout ?? '';
  console.log(out.trimEnd());
  if (!/sync:\s+running\b/i.test(out)) {
    fail('nbsync reports sync is not running after restart');
  }
}

if (platform() !== 'linux') {
  fail('systemd deploy is Linux-only');
}

await assertRuntime();

if (existsSync(resolve(root, '.git'))) {
  const pull = capture('git pull --ff-only', 'git', ['pull', '--ff-only']);
  if (pull.status !== 0) {
    console.warn(
      '[daemon:update] git pull skipped (offline or diverged) — continuing with local tree',
    );
  }
}

runYarn(root, ['install'], env);
runYarn(root, ['build'], env);

env.NB_NODE_BIN = process.execPath;
runNode(root, [resolve(root, 'scripts/install-systemd.mjs'), 'install'], env);
assertUnitUsesNode(process.execPath);

run('systemctl daemon-reload', 'systemctl', ['--user', 'daemon-reload']);
run('enable and start', 'systemctl', ['--user', 'enable', '--now', unitName]);
run('restart', 'systemctl', ['--user', 'restart', unitName]);

waitForActive();
const pid = mainPid();
run('status', 'systemctl', ['--user', 'status', unitName, '--no-pager']);
assertHealthy(pid);

console.log('[daemon:update] done — nearbytes-syncd is active and healthy.');
