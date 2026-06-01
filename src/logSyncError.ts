import { SyncHandshakeError, type SyncHandshakeFailureCode } from './core/handshake.js';
import { syncEmit } from './syncDebugLog.js';

/** Log sync-layer failures to stderr (never swallow silently). */
export function logSyncError(scope: string, err: unknown): void {
  if (err instanceof Error) {
    syncEmit(scope, err.stack ?? err.message);
    return;
  }
  syncEmit(scope, String(err));
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
  syncEmit(scope, `disconnected (${tag})`);
  const timer = setTimeout(() => {
    const entry = coalesceState.get(key);
    coalesceState.delete(key);
    if (!entry || entry.count <= 1) {
      return;
    }
    const extra = entry.count - 1;
    const elapsedS = Math.max(1, Math.round((Date.now() - entry.firstAt) / 1000));
    syncEmit(scope, `disconnected (${tag}) — and ${extra} more in the last ${elapsedS}s`);
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

export type FriendConnectFailureReason = SyncHandshakeFailureCode | 'transport-error' | 'unknown';

export interface ClassifiedFriendConnectError {
  readonly reason: FriendConnectFailureReason;
  /** Transient / policy — no stack trace on stderr. */
  readonly expected: boolean;
  /** Safe to re-dial after a short backoff (Hyperswarm may also retry). */
  readonly retryable: boolean;
  /** Suppress stderr entirely (loopback races). */
  readonly silent: boolean;
  /** Short tag for coalesced stderr + monitor events. */
  readonly tag: string;
}

const HANDSHAKE_REASON_TAG: Record<SyncHandshakeFailureCode, string> = {
  timeout: 'handshake-timeout',
  'unsupported-protocol': 'handshake-protocol',
  'duplicate-nonce': 'handshake-race',
  'unauthorized-profile': 'handshake-rejected',
  'instance-loopback': 'handshake-loopback',
};

export function classifyFriendConnectError(err: unknown): ClassifiedFriendConnectError {
  if (err instanceof SyncHandshakeError) {
    return {
      reason: err.code,
      expected: true,
      retryable: err.retryable,
      silent: err.code === 'instance-loopback',
      tag: HANDSHAKE_REASON_TAG[err.code],
    };
  }
  const code = (err as { code?: unknown } | null)?.code;
  if (typeof code === 'string' && EXPECTED_PEER_DISCONNECT_CODES.has(code)) {
    return {
      reason: 'transport-error',
      expected: true,
      retryable: true,
      silent: false,
      tag: code.toLowerCase(),
    };
  }
  const message = (err as { message?: unknown } | null)?.message;
  if (typeof message === 'string') {
    const normalized = message.trim().toLowerCase();
    const tag = EXPECTED_PEER_DISCONNECT_MESSAGES.get(normalized);
    if (tag) {
      return {
        reason: 'transport-error',
        expected: true,
        retryable: true,
        silent: false,
        tag,
      };
    }
    if (normalized === 'sync handshake timed out') {
      return {
        reason: 'timeout',
        expected: true,
        retryable: true,
        silent: false,
        tag: 'handshake-timeout',
      };
    }
  }
  return {
    reason: 'unknown',
    expected: false,
    retryable: false,
    silent: false,
    tag: 'error',
  };
}

function emitFriendConnectLine(scope: string, line: string): void {
  const key = `${scope}|${line}`;
  const existing = coalesceState.get(key);
  if (existing) {
    existing.count += 1;
    return;
  }
  syncEmit(scope, line);
  const timer = setTimeout(() => {
    const entry = coalesceState.get(key);
    coalesceState.delete(key);
    if (!entry || entry.count <= 1) {
      return;
    }
    const extra = entry.count - 1;
    const elapsedS = Math.max(1, Math.round((Date.now() - entry.firstAt) / 1000));
    syncEmit(scope, `${line} — and ${extra} more in the last ${elapsedS}s`);
  }, COALESCE_WINDOW_MS);
  if (typeof timer.unref === 'function') {
    timer.unref();
  }
  coalesceState.set(key, { count: 1, firstAt: Date.now(), timer });
}

/**
 * Log a failed friend/sibling association attempt. Expected failures
 * (handshake timeout, transport reset, dual-dial race) are one-line,
 * coalesced, and stack-free — the monitor shows `peer-connect-failed`
 * for the same fact.
 */
export function logFriendConnectError(scope: string, err: unknown): void {
  const c = classifyFriendConnectError(err);
  if (c.silent) {
    return;
  }
  if (c.expected) {
    emitFriendConnectLine(
      scope,
      `connect failed (${c.tag}) — discovery will retry`,
    );
    return;
  }
  logSyncError(scope, err);
}

/** Dim stderr hint while backing off before another handshake attempt. */
export function logFriendConnectRetry(
  scope: string,
  tag: string,
  attempt: number,
  maxAttempts: number,
  delayMs: number,
): void {
  emitFriendConnectLine(
    scope,
    `connect failed (${tag}), retry ${attempt}/${maxAttempts} in ${Math.round(delayMs / 1000)}s`,
  );
}
