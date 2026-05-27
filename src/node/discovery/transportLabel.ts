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
 * Best-effort remote endpoint from a Hyperswarm connection socket (TCP
 * `net.Socket` or UDX stream with `remoteHost` / `remotePort`).
 */
export function endpointFromSwarmSocket(socket: unknown): { host: string; port: number } | null {
  if (socket instanceof Socket) {
    const host = socket.remoteAddress;
    if (host === undefined || host === '') {
      return null;
    }
    return { host, port: socket.remotePort ?? 0 };
  }
  const udx = socket as { remoteHost?: string | null; remotePort?: number };
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
