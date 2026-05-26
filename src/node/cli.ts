/**
 * `nbsync` CLI — sync daemon control surface.
 *
 * Subcommands:
 *
 *   nbsync daemon [--config <path>]
 *     Run the sync daemon in the foreground. Use this from systemd /
 *     launchd / `yarn daemon`. Exits 0 on SIGTERM, non-zero on
 *     unrecoverable boot error (lock contention, malformed config).
 *
 *   nbsync status [--config <path>]
 *     Read config, probe the dataDir lock, print one-line status.
 *     Exits 0 if not running, 0 if running, 2 if config invalid.
 *
 *   nbsync probe <dataDir>
 *     Probe the dataDir lock directly. Doesn't read any config.
 *     Exits 0 always.
 *
 *   nbsync help
 *     Print this list.
 */

import {
  defaultDaemonConfigPath,
  readDaemonConfig,
} from './daemonConfig.js';
import { runDaemon } from './daemon.js';
import { probeSyncLock } from './dataDirLock.js';

interface ParsedArgs {
  readonly command: string;
  readonly positional: readonly string[];
  readonly flags: Readonly<Record<string, string | true>>;
}

function parseArgs(argv: readonly string[]): ParsedArgs {
  const [command = 'help', ...rest] = argv;
  const positional: string[] = [];
  const flags: Record<string, string | true> = {};
  for (let i = 0; i < rest.length; i++) {
    const a = rest[i]!;
    if (a.startsWith('--')) {
      const key = a.slice(2);
      const next = rest[i + 1];
      if (next !== undefined && !next.startsWith('--')) {
        flags[key] = next;
        i++;
      } else {
        flags[key] = true;
      }
    } else {
      positional.push(a);
    }
  }
  return { command, positional, flags };
}

function getConfigPath(flags: Readonly<Record<string, string | true>>): string {
  const v = flags['config'];
  if (typeof v === 'string') return v;
  return defaultDaemonConfigPath();
}

function printHelp(): void {
  process.stdout.write(
    [
      'nbsync — Nearbytes sync daemon control',
      '',
      'Usage:',
      '  nbsync daemon [--config <path>]   Run the daemon in foreground',
      '  nbsync status [--config <path>]   Report config + lock state',
      '  nbsync probe  <dataDir>           Probe the sync-singleton lock',
      '  nbsync help                       Show this message',
      '',
      `Default config path: ${defaultDaemonConfigPath()}`,
      '(override via NEARBYTES_CONFIG, or --config)',
      '',
    ].join('\n'),
  );
}

async function runStatus(configPath: string): Promise<number> {
  let config;
  try {
    config = await readDaemonConfig(configPath);
  } catch (err) {
    process.stderr.write(`nbsync status: ${(err as Error).message}\n`);
    return 2;
  }
  const status = probeSyncLock(config.dataDir);
  process.stdout.write(`config:      ${configPath}\n`);
  process.stdout.write(`dataDir:     ${config.dataDir}\n`);
  process.stdout.write(`profiles:    ${config.profiles.length}\n`);
  process.stdout.write(`active:      ${config.activeProfile ?? '(none)'}\n`);
  process.stdout.write(`friends:     ${config.friends.length}\n`);
  if (status.running) {
    process.stdout.write(`sync:        running (pid ${status.holderPid}, since ${status.heldSince.toISOString()})\n`);
  } else {
    process.stdout.write('sync:        not running\n');
  }
  return 0;
}

function runProbe(dataDir: string): number {
  const status = probeSyncLock(dataDir);
  if (status.running) {
    process.stdout.write(
      `running pid=${status.holderPid} lockPath=${status.lockPath} heldSince=${status.heldSince.toISOString()}\n`,
    );
  } else {
    process.stdout.write('not-running\n');
  }
  return 0;
}

async function main(): Promise<number> {
  const argv = process.argv.slice(2);
  const { command, positional, flags } = parseArgs(argv);
  switch (command) {
    case 'daemon': {
      const configPath = getConfigPath(flags);
      try {
        await runDaemon({ configPath });
        return 0;
      } catch (err) {
        process.stderr.write(`nbsync daemon: ${(err as Error).message}\n`);
        return 1;
      }
    }
    case 'status': {
      const configPath = getConfigPath(flags);
      return runStatus(configPath);
    }
    case 'probe': {
      const dataDir = positional[0];
      if (dataDir === undefined) {
        process.stderr.write('nbsync probe: missing dataDir argument\n');
        return 2;
      }
      return runProbe(dataDir);
    }
    case 'help':
    case '--help':
    case '-h':
      printHelp();
      return 0;
    default:
      process.stderr.write(`nbsync: unknown command "${command}"\n\n`);
      printHelp();
      return 2;
  }
}

main().then(
  (code) => {
    // Don't process.exit(0) on success — let the daemon hold the
    // event loop open. For non-zero (or for non-daemon subcommands
    // that return 0), exit explicitly so the process doesn't linger.
    if (code !== 0 || process.argv[2] !== 'daemon') {
      process.exit(code);
    }
  },
  (err) => {
    process.stderr.write(`nbsync: ${(err as Error).message}\n`);
    process.exit(1);
  },
);
