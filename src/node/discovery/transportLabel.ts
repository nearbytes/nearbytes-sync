import { Socket } from 'node:net';

/**
 * Normalised wire endpoint for observability labels.
 *
 * Label shapes (stable, parseable by the CLI):
 *
 *   `dht:<host>:<port>`       — Hyperswarm (UDX or TCP hole-punch)
 *   `mdns-tcp:<host>:<port>`  — LAN sync TCP (optional `-><profile>` suffix)
 *   `tcp:<host>:<port>`       — direct TCP
 *
 * IPv6 hosts are bracketed: `dht:[fe80::1]:53432`.
 */
export function formatEndpointLabel(
  kind: 'dht' | 'mdns-tcp' | 'tcp',
  host: string,
  port: number,
  profileSuffix?: string,
): string {
  const hostPart = host.includes(':') && !host.startsWith('[') ? `[${host}]` : host;
  const base = `${kind}:${hostPart}:${port}`;
  return profileSuffix !== undefined ? `${base}->${profileSuffix}` : base;
}

/**
 * Hyperswarm hands us a Noise secret-stream (or similar) wrapper; the
 * remote IP lives on the underlying UDX stream or plain TCP socket.
 */
export function unwrapTransportSocket(socket: unknown): unknown {
  if (socket instanceof Socket) {
    return socket;
  }
  if (socket === null || typeof socket !== 'object') {
    return socket;
  }
  const s = socket as {
    rawStream?: unknown;
    _rawStream?: unknown;
    stream?: unknown;
  };
  if (s.rawStream !== undefined && s.rawStream !== socket) {
    return unwrapTransportSocket(s.rawStream);
  }
  if (s._rawStream !== undefined && s._rawStream !== socket) {
    return unwrapTransportSocket(s._rawStream);
  }
  if (s.stream !== undefined && s.stream !== socket) {
    return unwrapTransportSocket(s.stream);
  }
  return socket;
}

/**
 * Best-effort remote endpoint from a Hyperswarm connection socket (TCP
 * `net.Socket` or UDX stream with `remoteHost` / `remotePort`).
 */
export function endpointFromSwarmSocket(socket: unknown): { host: string; port: number } | null {
  const inner = unwrapTransportSocket(socket);
  if (inner instanceof Socket) {
    const host = inner.remoteAddress;
    if (host === undefined || host === '') {
      return null;
    }
    return { host, port: inner.remotePort ?? 0 };
  }
  const udx = inner as { remoteHost?: string | null; remotePort?: number };
  if (typeof udx.remoteHost === 'string' && udx.remoteHost.length > 0) {
    return { host: udx.remoteHost, port: udx.remotePort ?? 0 };
  }
  return null;
}

export function dhtTransportLabel(socket: unknown): string {
  const ep = endpointFromSwarmSocket(socket);
  if (ep === null) {
    return 'dht:unknown';
  }
  return formatEndpointLabel('dht', ep.host, ep.port);
}

const DHT_LABEL_WAIT_MS = 2_500;

/**
 * When UDX has not learned the remote host yet (common on inbound
 * hole-punched streams), wait briefly for `connect` / `remote-changed`.
 */
export function waitForDhtTransportLabel(
  socket: unknown,
  timeoutMs = DHT_LABEL_WAIT_MS,
): Promise<string> {
  const immediate = dhtTransportLabel(socket);
  if (immediate !== 'dht:unknown') {
    return Promise.resolve(immediate);
  }
  const inner = unwrapTransportSocket(socket);
  if (inner === null || typeof inner !== 'object') {
    return Promise.resolve(immediate);
  }
  const stream = inner as {
    on?(event: string, cb: () => void): void;
    off?(event: string, cb: () => void): void;
    removeListener?(event: string, cb: () => void): void;
  };
  const on = stream.on;
  if (typeof on !== 'function') {
    return Promise.resolve(immediate);
  }
  return new Promise((resolve) => {
    const finish = (): void => {
      cleanup();
      resolve(dhtTransportLabel(socket));
    };
    const cleanup = (): void => {
      stream.off?.('connect', finish);
      stream.off?.('remote-changed', finish);
      stream.removeListener?.('connect', finish);
      stream.removeListener?.('remote-changed', finish);
      clearTimeout(timer);
    };
    on.call(stream, 'connect', finish);
    on.call(stream, 'remote-changed', finish);
    const timer = setTimeout(finish, timeoutMs);
    if (typeof timer.unref === 'function') {
      timer.unref();
    }
  });
}
