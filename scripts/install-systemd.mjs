#!/usr/bin/env node
/**
 * Install / uninstall the nbsync systemd user unit.
 *
 * Usage:
 *   node scripts/install-systemd.mjs install
 *   node scripts/install-systemd.mjs uninstall
 *
 * Modern Ubuntu (>= 18.04) ships systemd with user services enabled.
 * We install under `~/.config/systemd/user/` so no root is required;
 * the service runs as the invoking user, has access to the user's
 * `~/.nearbytes/config.json`, and starts on login.
 *
 * After install:
 *   systemctl --user daemon-reload
 *   systemctl --user enable  --now nearbytes-syncd.service
 *   systemctl --user status        nearbytes-syncd.service
 *   journalctl  --user      -u     nearbytes-syncd.service -f
 *
 * To pick up config changes mid-flight (without a full restart):
 *   systemctl --user reload nearbytes-syncd.service     # sends SIGHUP
 *
 * Or edit `~/.nearbytes/config.json` directly — the daemon's fs.watch
 * picks it up within 250 ms.
 */

import { mkdirSync, readFileSync, writeFileSync, existsSync, unlinkSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { homedir, platform } from 'node:os';

const HERE = dirname(fileURLToPath(import.meta.url));
const PACKAGE_ROOT = resolve(HERE, '..');
const TEMPLATE_PATH = join(PACKAGE_ROOT, 'systemd', 'nearbytes-syncd.service.tmpl');
const NBSYNC_JS_PATH = join(PACKAGE_ROOT, 'bin', 'nbsync.mjs');

const USER_UNIT_DIR = join(homedir(), '.config', 'systemd', 'user');
const UNIT_NAME = 'nearbytes-syncd.service';
const UNIT_PATH = join(USER_UNIT_DIR, UNIT_NAME);

const DEFAULT_CONFIG_PATH = join(homedir(), '.nearbytes', 'config.json');

function fail(message) {
  process.stderr.write(`install-systemd: ${message}\n`);
  process.exit(2);
}

function requireLinux() {
  if (platform() !== 'linux') {
    fail(
      `systemd installation only supported on Linux (detected ${platform()}). ` +
        `On macOS, use a launchd plist; see README.md.`,
    );
  }
}

function findNodeBinary() {
  const fromEnv = process.env.NB_NODE_BIN;
  if (typeof fromEnv === 'string' && fromEnv.length > 0 && existsSync(fromEnv)) {
    return fromEnv;
  }
  // process.execPath is the absolute path to the node binary running
  // this script — the most reliable source. Falls back to PATH lookup
  // only if execPath is somehow missing (it never is on a real Node).
  if (process.execPath && existsSync(process.execPath)) {
    return process.execPath;
  }
  const which = spawnSync('which', ['node'], { encoding: 'utf8' });
  if (which.status !== 0 || which.stdout.trim().length === 0) {
    fail('cannot locate the node binary (process.execPath empty, `which node` failed)');
  }
  return which.stdout.trim();
}

function ensureExists(path, what) {
  if (!existsSync(path)) {
    fail(`${what} missing at ${path} (did you run \`yarn build\` first?)`);
  }
}

function install(configPath) {
  requireLinux();
  ensureExists(TEMPLATE_PATH, 'systemd unit template');
  ensureExists(NBSYNC_JS_PATH, 'nbsync bin');

  const nodeBin = findNodeBinary();
  const template = readFileSync(TEMPLATE_PATH, 'utf8');
  const unit = template
    .replaceAll('__NODE_BIN__', nodeBin)
    .replaceAll('__NBSYNC_JS__', NBSYNC_JS_PATH)
    .replaceAll('__CONFIG_PATH__', configPath);

  mkdirSync(USER_UNIT_DIR, { recursive: true });
  writeFileSync(UNIT_PATH, unit, 'utf8');
  process.stdout.write(`installed unit: ${UNIT_PATH}\n`);

  const reload = spawnSync('systemctl', ['--user', 'daemon-reload'], { stdio: 'inherit' });
  if (reload.status !== 0) {
    fail('`systemctl --user daemon-reload` failed (is systemd --user available?)');
  }

  process.stdout.write('\n');
  process.stdout.write('next steps:\n');
  process.stdout.write(`  systemctl --user enable --now ${UNIT_NAME}\n`);
  process.stdout.write(`  systemctl --user status        ${UNIT_NAME}\n`);
  process.stdout.write(`  journalctl  --user      -u     ${UNIT_NAME} -f\n`);
  process.stdout.write('\n');
  process.stdout.write('to apply config edits without restart:\n');
  process.stdout.write(`  systemctl --user reload ${UNIT_NAME}\n`);
  process.stdout.write(`  (or just save ${configPath}; the daemon's fs.watch picks it up)\n`);
}

function uninstall() {
  requireLinux();
  // Best-effort: stop + disable + remove. Each step prints its result;
  // missing prerequisites are not failures (uninstall must be idempotent).
  spawnSync('systemctl', ['--user', 'disable', '--now', UNIT_NAME], { stdio: 'inherit' });
  if (existsSync(UNIT_PATH)) {
    unlinkSync(UNIT_PATH);
    process.stdout.write(`removed unit: ${UNIT_PATH}\n`);
  } else {
    process.stdout.write(`no unit at ${UNIT_PATH} (nothing to remove)\n`);
  }
  spawnSync('systemctl', ['--user', 'daemon-reload'], { stdio: 'inherit' });
}

function main() {
  const [, , cmd, ...rest] = process.argv;
  switch (cmd) {
    case 'install': {
      const configPath = rest[0] ?? DEFAULT_CONFIG_PATH;
      install(configPath);
      return;
    }
    case 'uninstall': {
      uninstall();
      return;
    }
    default:
      process.stderr.write(
        `usage: install-systemd.mjs install [config-path] | uninstall\n` +
          `  default config-path: ${DEFAULT_CONFIG_PATH}\n`,
      );
      process.exit(2);
  }
}

main();
