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
 */
const EXPECTED_PEER_DISCONNECT_CODES = new Set([
  'ECONNRESET',
  'EPIPE',
  'ETIMEDOUT',
  'ENOTCONN',
  'ECANCELED',
  'ECONNABORTED',
]);

export function logPeerSocketError(scope: string, err: unknown): void {
  const code = (err as { code?: unknown } | null)?.code;
  if (typeof code === 'string' && EXPECTED_PEER_DISCONNECT_CODES.has(code)) {
    console.error(`[nearbytes-sync:${scope}] disconnected (${code.toLowerCase()})`);
    return;
  }
  logSyncError(scope, err);
}
