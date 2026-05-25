#!/usr/bin/env node
// Bump every github:nearbytes/<pkg>#<sha> dep in package.json to the
// current HEAD of its `main` branch on GitHub, then refresh yarn.lock.
//
// After this completes, review the diff and commit package.json +
// yarn.lock. Repos are kept self-consistent by applying this command
// in topological dep order:
//   crypto -> log -> sync -> skeleton -> files -> benchmarks
// so each pinned transitive SHA references a commit that itself
// already pins fresh transitives.
//
// Pure Node (>=18); portable to macOS, Linux, and Windows.

import { execFileSync, spawnSync } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';

function lsRemoteHead(repo) {
  const out = execFileSync(
    'git',
    [
      'ls-remote',
      `https://github.com/nearbytes/${repo}.git`,
      'refs/heads/main',
    ],
    { encoding: 'utf8' },
  );
  return out.split(/\s+/)[0];
}

const pkg = JSON.parse(readFileSync('package.json', 'utf8'));
const re = /^github:nearbytes\/([^#]+)#/;
let touched = false;

for (const section of ['dependencies', 'devDependencies']) {
  const deps = pkg[section];
  if (!deps) continue;
  for (const [name, spec] of Object.entries(deps)) {
    if (typeof spec !== 'string') continue;
    const m = re.exec(spec);
    if (!m) continue;
    const repo = m[1];
    const sha = lsRemoteHead(repo);
    const next = `github:nearbytes/${repo}#${sha}`;
    if (deps[name] !== next) {
      const prev = spec.split('#')[1] ?? '?';
      console.log(`${name.padEnd(24)} ${prev.slice(0, 7)} -> ${sha.slice(0, 7)}`);
      deps[name] = next;
      touched = true;
    } else {
      console.log(`${name.padEnd(24)} unchanged (${sha.slice(0, 7)})`);
    }
  }
}

if (touched) {
  writeFileSync('package.json', JSON.stringify(pkg, null, 2) + '\n');
}

const r = spawnSync('yarn', ['install'], { stdio: 'inherit', shell: true });
process.exit(r.status ?? 0);
