import type { Log } from 'nearbytes-log';
import { attachPeerSession, type DuplexPeer } from './peerLoop.js';
import { profileSubject } from './topic.js';

export interface FriendSessionEntry {
  readonly remoteProfilePublicKey: string;
  readonly stop: () => void;
  close(): void;
}

/**
 * One active framed sync association per remote friend profile key (SYNC-06).
 */
export class FriendSessionRegistry {
  private readonly sessions = new Map<string, FriendSessionEntry>();

  attach(log: Log, remoteProfilePublicKey: string, peer: DuplexPeer): FriendSessionEntry {
    const remote = remoteProfilePublicKey.toLowerCase();
    const existing = this.sessions.get(remote);
    if (existing !== undefined) {
      existing.stop();
      existing.close();
      this.sessions.delete(remote);
    }

    const subject = profileSubject(remote);
    const stop = attachPeerSession(log, subject, peer);
    const entry: FriendSessionEntry = {
      remoteProfilePublicKey: remote,
      stop,
      close: () => peer.close(),
    };
    this.sessions.set(remote, entry);
    return entry;
  }

  closeAll(): void {
    for (const entry of this.sessions.values()) {
      entry.stop();
      entry.close();
    }
    this.sessions.clear();
  }
}
