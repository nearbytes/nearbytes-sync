import type { Log } from 'nearbytes-log';
import { attachPeerSession, type AttachPeerSessionOptions, type DuplexPeer } from './peerLoop.js';
import { profileSubject } from './topic.js';

export interface FriendSessionEntry {
  readonly remoteProfilePublicKey: string;
  readonly transportLabel: string;
  readonly stop: () => void;
  close(): void;
  isAlive(): boolean;
}

/** Prefer LAN mDNS TCP over Hyperswarm for bulk throughput. */
function transportPreference(label: string): number {
  if (label.startsWith('mdns-tcp:') || label.startsWith('mdns:')) {
    return 0;
  }
  if (label.startsWith('tcp:')) {
    return 1;
  }
  return 10;
}

/**
 * One active framed sync association per remote friend profile key (SYNC-06).
 * Duplicate inbound connections are dropped so Hyperswarm flaps do not replace a live mDNS session.
 */
export class FriendSessionRegistry {
  private readonly sessions = new Map<string, FriendSessionEntry>();

  attach(
    log: Log,
    remoteProfilePublicKey: string,
    peer: DuplexPeer,
    sessionOptions: AttachPeerSessionOptions = {},
    transportLabel = 'unknown',
  ): { readonly entry: FriendSessionEntry; readonly created: boolean } {
    const remote = remoteProfilePublicKey.toLowerCase();
    const existing = this.sessions.get(remote);
    if (existing !== undefined && existing.isAlive()) {
      if (transportPreference(transportLabel) >= transportPreference(existing.transportLabel)) {
        peer.close();
        return { entry: existing, created: false };
      }
      existing.stop();
      existing.close();
      this.sessions.delete(remote);
    }
    if (existing !== undefined) {
      existing.stop();
      existing.close();
      this.sessions.delete(remote);
    }

    const subject = profileSubject(remote);
    let alive = true;
    let entry: FriendSessionEntry;
    const stop = attachPeerSession(log, subject, peer, () => {
      alive = false;
      if (this.sessions.get(remote) === entry) {
        this.sessions.delete(remote);
      }
    }, sessionOptions);
    entry = {
      remoteProfilePublicKey: remote,
      transportLabel,
      stop,
      close: () => peer.close(),
      isAlive: () => alive,
    };
    this.sessions.set(remote, entry);
    return { entry, created: true };
  }

  closeAll(): void {
    for (const entry of this.sessions.values()) {
      entry.stop();
      entry.close();
    }
    this.sessions.clear();
  }
}
