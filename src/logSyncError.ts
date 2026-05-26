/** Log sync-layer failures to stderr (never swallow silently). */
export function logSyncError(scope: string, err: unknown): void {
  if (err instanceof Error) {
    console.error(`[nearbytes-sync:${scope}]`, err.stack ?? err.message);
    return;
  }
  console.error(`[nearbytes-sync:${scope}]`, err);
}

/**
 * P2P peer sockets disconnect routinely as a swarm churns: peers sleep,
 * roam between networks, restart, lose Wi-Fi, get NAT-rebound, or simply
 * close after our own profile-topic re-join. These produce TCP-level
 * `ECONNRESET` / `EPIPE` / `ETIMEDOUT` / `ENOTCONN` errors that the
 * surrounding discovery layer already handles by re-establishing the
 * connection on the next swarm event — they are not sync bugs and MUST
 * NOT be reported with a stack trace, which would mask real failures.
 *
 * Use this helper for `socket.on('error', ...)` on discovery-layer
 * sockets. Genuinely unexpected errors still surface via `logSyncError`.
 *
 * Two-channel matching (both are needed in practice):
 *
 * 1. **`.code`** — Node's built-in `net.Socket` errors set this, e.g.
 *    `ECONNRESET`, `EPIPE`. This covers TCP transports.
 *
 * 2. **`.message`** — Hyperswarm's UDX/secret-stream surface throws
 *    plain `new Error(<libuv-strerror>)` from the native binding without
 *    populating `.code` (see `udx-native/lib/stream.js#customError` for
 *    the rare code-bearing path — the on-close path doesn't go through
 *    it). We match the canonical libuv `strerror` strings explicitly.
 */
const EXPECTED_PEER_DISCONNECT_CODES = new Set([
  'ECONNRESET',
  'EPIPE',
  'ETIMEDOUT',
  'ENOTCONN',
  'ECANCELED',
  'ECONNABORTED',
  'ECONNREFUSED',
  'EHOSTUNREACH',
  'ENETUNREACH',
  'ENETDOWN',
  'ENETRESET',
  'EHOSTDOWN',
]);

/**
 * Maps libuv `strerror` messages (and Hyperswarm secret-stream's
 * `'Stream timed out'`) to a short tag printed in the disconnect line.
 * Matching is exact on `err.message` after trimming; libuv produces
 * these strings verbatim.
 */
const EXPECTED_PEER_DISCONNECT_MESSAGES = new Map<string, string>([
  ['connection reset by peer', 'econnreset'],
  ['connection timed out', 'etimedout'],
  ['connection refused', 'econnrefused'],
  ['connection aborted', 'econnaborted'],
  ['broken pipe', 'epipe'],
  ['transport endpoint is not connected', 'enotconn'],
  ['operation canceled', 'ecanceled'],
  ['host is unreachable', 'ehostunreach'],
  ['network is unreachable', 'enetunreach'],
  ['network is down', 'enetdown'],
  ['network dropped connection on reset', 'enetreset'],
  ['host is down', 'ehostdown'],
  ['software caused connection abort', 'econnaborted'],
  ['end of file', 'eof'],
  ['stream timed out', 'etimedout'],
]);

/**
 * Coalescing window for *repeated* expected disconnects from the same
 * (scope, tag) pair. Hyperswarm retries dead peers for tens of seconds
 * after they go away, so the same `connection timed out` from the same
 * `peer:<pubkey>` fingerprint can fire many times in a burst. We print
 * the first occurrence immediately, suppress identical lines within the
 * window, and emit a `— and N more in the last Xs` summary when the
 * window closes (or as soon as the next distinct event arrives for the
 * same key).
 */
const COALESCE_WINDOW_MS = 5_000;

interface CoalesceEntry {
  count: number;
  firstAt: number;
  timer: NodeJS.Timeout;
}

const coalesceState = new Map<string, CoalesceEntry>();

function emitExpectedDisconnect(scope: string, tag: string): void {
  const key = `${scope}|${tag}`;
  const existing = coalesceState.get(key);
  if (existing) {
    existing.count += 1;
    return;
  }
  console.error(`[nearbytes-sync:${scope}] disconnected (${tag})`);
  const timer = setTimeout(() => {
    const entry = coalesceState.get(key);
    coalesceState.delete(key);
    if (!entry || entry.count <= 1) {
      return;
    }
    const extra = entry.count - 1;
    const elapsedS = Math.max(1, Math.round((Date.now() - entry.firstAt) / 1000));
    console.error(
      `[nearbytes-sync:${scope}] disconnected (${tag}) — and ${extra} more in the last ${elapsedS}s`,
    );
  }, COALESCE_WINDOW_MS);
  if (typeof timer.unref === 'function') {
    timer.unref();
  }
  coalesceState.set(key, { count: 1, firstAt: Date.now(), timer });
}

export function logPeerSocketError(scope: string, err: unknown): void {
  const code = (err as { code?: unknown } | null)?.code;
  if (typeof code === 'string' && EXPECTED_PEER_DISCONNECT_CODES.has(code)) {
    emitExpectedDisconnect(scope, code.toLowerCase());
    return;
  }
  const message = (err as { message?: unknown } | null)?.message;
  if (typeof message === 'string') {
    const normalized = message.trim().toLowerCase();
    const tag = EXPECTED_PEER_DISCONNECT_MESSAGES.get(normalized);
    if (tag) {
      emitExpectedDisconnect(scope, tag);
      return;
    }
  }
  logSyncError(scope, err);
}
