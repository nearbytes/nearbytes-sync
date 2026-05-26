import type { Log } from 'nearbytes-log';
import { attachPeerSession, type AttachPeerSessionOptions, type DuplexPeer } from './peerLoop.js';
import { profileSubject } from './topic.js';

export interface FriendSessionEntry {
  readonly remoteProfilePublicKey: string;
  /** Empty string for legacy peers that did not advertise a peerId (pre-DISC-26). */
  readonly remotePeerId: string;
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
 * Sessions are keyed by `(remoteProfile, remotePeerId)` (`sync-discovery-v1.md`
 * DISC-26) so two sibling devices that share the same identity each get their
 * own session entry. Duplicate connections to the **same** sibling are still
 * deduped: a higher-preference transport (mDNS TCP) wins over a lower one.
 */
export class FriendSessionRegistry {
  private readonly sessions = new Map<string, FriendSessionEntry>();

  private sessionKey(remoteProfile: string, remotePeerId: string): string {
    return `${remoteProfile.toLowerCase()}|${remotePeerId.toLowerCase()}`;
  }

  attach(
    log: Log,
    remoteProfilePublicKey: string,
    remotePeerId: string,
    peer: DuplexPeer,
    sessionOptions: AttachPeerSessionOptions = {},
    transportLabel = 'unknown',
  ): { readonly entry: FriendSessionEntry; readonly created: boolean } {
    const remote = remoteProfilePublicKey.toLowerCase();
    const remotePid = remotePeerId.toLowerCase();
    const key = this.sessionKey(remote, remotePid);
    const existing = this.sessions.get(key);
    if (existing !== undefined && existing.isAlive()) {
      if (transportPreference(transportLabel) >= transportPreference(existing.transportLabel)) {
        peer.close();
        return { entry: existing, created: false };
      }
      existing.stop();
      existing.close();
      this.sessions.delete(key);
    }
    if (existing !== undefined) {
      existing.stop();
      existing.close();
      this.sessions.delete(key);
    }

    const subject = profileSubject(remote);
    let alive = true;
    let entry: FriendSessionEntry;
    const stop = attachPeerSession(log, subject, peer, () => {
      alive = false;
      if (this.sessions.get(key) === entry) {
        this.sessions.delete(key);
      }
    }, sessionOptions);
    entry = {
      remoteProfilePublicKey: remote,
      remotePeerId: remotePid,
      transportLabel,
      stop,
      close: () => peer.close(),
      isAlive: () => alive,
    };
    this.sessions.set(key, entry);
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
