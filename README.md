# nearbytes-sync

Friend-to-friend sync (Hyperswarm + mDNS, `nearbytes.sync.v1`). Reactive `have`/`want` on open associations — no timer-driven delta polling. Block transfer uses **length-prefixed binary frames** (no base64/JSON wrapping of ciphertext).

Normative requirements: `nearbytes-specs/requirements/sync-discovery-v1.md`, `sync-protocol-v1.md`.

## Behavior (v0)

- `hello` handshake with configured friends before anti-entropy
- One active association per remote friend profile key
- LAN mDNS TXT `prof` = advertiser profile public key; only connects to configured friends
- Blocks-first `want` ordering; event `have` includes `blockRefs` when available
- Per-association serialized framed I/O (no interleaved writes)

## Build

```sh
npm install
npm run build
```

## Testing

Integration tests live in `nearbytes-files` (`yarn e2e:local`, `yarn e2e:bidirectional:local`). This package is consumed via `nearbytes-skeleton` `start(log, friends, { serveProfilePublicKey })`.

Built output is `dist/` (gitignored); do not commit `dist/`.
