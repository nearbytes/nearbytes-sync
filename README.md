# nearbytes-sync

Friend-to-friend sync (Hyperswarm + mDNS, `nearbytes.sync.v1`). Reactive `have`/`want` on open associations — no timer-driven delta polling. Block transfer uses **length-prefixed binary frames** (no base64/JSON wrapping of ciphertext).

Normative requirements: `nearbytes-specs/requirements/sync-discovery-v1.md`, `sync-protocol-v1.md`.

## Behavior (v0)

- `hello` handshake with configured friends before anti-entropy (SYNC-08; no profile signature on wire)
- One active association per remote `(profile, instance)` triple (SYNC-06)
- **Local resume walk** on attach: outbound `delta` pages drive catch-up; only resume `have` (`fromCursor` set) advances pagination (SYNC-21, SYNC-10c)
- Attach tail announce: unsolicited push `have` (no `fromCursor`) for last 256 journal entries (SYNC-21c)
- Live reception pushes: reactive `have`/`want` — no timer-driven delta polling (SYNC-10)
- LAN mDNS TXT `prof` = advertiser profile public key; only connects to configured friends
- Blocks-first `want` ordering (SYNC-12); event `have` includes `blockRefs` when available (SYNC-13)
- Per-association serialized framed I/O (no interleaved writes)

## Build

```sh
npm install
npm run build
```

## Testing

Integration tests live in `nearbytes-files` (`yarn e2e:local`, `yarn e2e:bidirectional:local`). This package is consumed via `nearbytes-skeleton` `start(log, friends, { serveProfilePublicKey })`.

Built output is `dist/` (gitignored); do not commit `dist/`.

## Daemon (`nbsync`)

Long-running sync daemon that owns the [DISC-27](https://github.com/nearbytes/nearbytes-specs/blob/main/requirements/sync-discovery-v1.md) sync-singleton lock and keeps friend carriage running 24/7. Cross-process writers (the file CLI, scripts) coexist by appending to the same dataDir; the daemon's `chokidar` watcher notices the new event/block files and pushes `have` to peers.

```sh
yarn build
node bin/nbsync.mjs daemon        # foreground, reads ~/.nearbytes/config.json
node bin/nbsync.mjs status        # report dataDir + lock state
node bin/nbsync.mjs probe <dir>   # just probe the lock
```

The daemon watches:

| What | Fires on | Action |
|---|---|---|
| `~/.nearbytes/config.json` | save (debounced 250 ms) | `reloadSync(friends, profiles)` if changed |
| `<dataDir>/channels/<pk>/*.bin` | new file | append reception → `have` to peers |
| `<dataDir>/blocks/*.bin` | new file | append reception → `have` to peers |
| `SIGTERM` / `SIGINT` | once | flush, release lock, exit 0 |
| `SIGHUP` | once | manual config re-read (same code path as fs.watch) |

### Install as a systemd user service (Ubuntu 18.04+)

```sh
yarn build
yarn install:systemd                          # writes ~/.config/systemd/user/nearbytes-syncd.service
systemctl --user enable --now nearbytes-syncd # autostart on login, start now
systemctl --user status        nearbytes-syncd
journalctl  --user  -u         nearbytes-syncd -f
```

To pull the latest code, rebuild, refresh the unit (pinning the Node 22+
binary from `.nvmrc`), restart immediately, and verify the service is healthy:

```sh
yarn daemon:update
```

Requires Node >= 22.13 (`node:sqlite` for `nearbytes-log`). The script
auto-installs from `.nvmrc` under `.local/toolchain` when fnm/nvm are absent.

To pick up config changes without a full restart, either save `~/.nearbytes/config.json` (the daemon's `fs.watch` fires within 250 ms) or `systemctl --user reload nearbytes-syncd` (sends `SIGHUP`).

To uninstall: `yarn uninstall:systemd`.

### macOS launchd

systemd is Linux-only. On macOS, drop the following plist at `~/Library/LaunchAgents/com.nearbytes.syncd.plist` and `launchctl load -w` it (template ships in `systemd/launchd.plist.tmpl` if you want a starting point; for now the file is small enough to write by hand).

### Coexistence with the CLI

Per DISC-27 (split form: singleton sync, plural writers), the lock only protects the *sync engine*. Multiple processes MAY write events into the same dataDir simultaneously — content-addressed naming makes that CRDT-trivial, and `nearbytes-log`'s `fsIo.writeFile` publishes via `link(2)` first-wins. The skeleton's `bootSync` checks `probeSyncLock()` and falls back to a writer-only handle when a daemon is active, so the file CLI works alongside the daemon without conflict.

### Config-file permissions

`~/.nearbytes/config.json` contains your profile and volume secrets in cleartext (those strings ARE the inputs to `crypto.deriveKeys`), so the daemon refuses to load it if it's readable by anyone other than you. Concretely:

- `readDaemonConfig()` (called by `nbsync daemon` and `nbsync status`) `stat`s the file and throws if it is not owned by your UID or if its POSIX mode has any group/world bits set (i.e. mode must be `0o600`).
- `nearbytes-skeleton`'s `writeConfig` always publishes the file atomically at mode `0o600` via unique tmp + rename, so newly-written configs are safe by construction.

If you wrote `~/.nearbytes/config.json` by hand (or were using an older version of these tools that didn't tighten on write), `nbsync daemon` will refuse to start with an error pointing you at the exact fix:

```sh
chmod 600 ~/.nearbytes/config.json
```

The check is POSIX-only and no-ops on Windows (where ACLs would be the correct mechanism; out of scope for v1).
