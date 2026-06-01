import type { Log } from 'nearbytes-log';
import { attachPeerSession, type AttachPeerSessionOptions, type DuplexPeer } from './peerLoop.js';
import { profileSubject } from './topic.js';
import type { PeerSessionEventEmitter, SyncEventBus } from './syncEvents.js';

export interface FriendSessionEntry {
  readonly remoteProfilePublicKey: string;
  readonly remoteInstancePublicKey: string;
  readonly remotePeerId: string;
  readonly transportLabel: string;
  /** Wall-clock when the session became alive (after handshake). */
  readonly connectedAt: Date;
  /**
   * Local profile under which this association is run. For sibling carriage
   * this equals `remoteProfilePublicKey`; for asymmetric follow it differs.
   */
  readonly localAssociationProfile: string;
  /** Hyperswarm outbound dial vs inbound accept (LAN TCP is always false). */
  readonly locallyInitiated: boolean;
  readonly stop: () => void;
  close(): void;
  isAlive(): boolean;
}

/**
 * On equal transport tier, keep the inbound accept and drop our own outbound
 * dial so we do not tear down a working NAT-punched leg (WAN siblings).
 */
function preferKeepExistingSession(
  existingLocallyInitiated: boolean,
  newLocallyInitiated: boolean,
): boolean {
  if (existingLocallyInitiated !== newLocallyInitiated) {
    return !existingLocallyInitiated;
  }
  return true;
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
 * Sessions are keyed by `(remoteProfile, remoteInstancePublicKey)`
 * (`sync-discovery-v1.md` DISC-26/27) so two sibling devices that share the same profile each get their
 * own session entry. Duplicate connections to the **same** sibling are still
 * deduped: a higher-preference transport (mDNS TCP) wins over a lower one.
 */
export class FriendSessionRegistry {
  private readonly sessions = new Map<string, FriendSessionEntry>();

  /**
   * Optional observability bus. When set, every `attach()` that creates
   * a new live session emits `peer-connected`, the close hook emits
   * `peer-disconnected`, and the per-session adapter forwards
   * peer-loop block/event activity (see `PeerSessionEventEmitter`).
   *
   * The bus is purely additive — a registry constructed without one is
   * indistinguishable on the wire from a registry constructed with a
   * silent bus. We accept the injection at construction time so the
   * `start()` orchestrator can own the bus lifecycle and feed both the
   * in-memory `SyncHandle.onEvent` listeners and the daemon beacon
   * publisher from a single source.
   */
  constructor(private readonly bus?: SyncEventBus) {}

  /**
   * Count of currently-alive sibling/friend sessions. Used by `SyncSnapshot`
   * so that CLI bye-time flushes can refuse to declare "drained" until at
   * least one peer has actually been seen — without this, fast one-shot
   * writes race past DHT bootstrap and exit before announcing any `have`.
   */
  get aliveCount(): number {
    let n = 0;
    for (const entry of this.sessions.values()) {
      if (entry.isAlive()) n++;
    }
    return n;
  }

  hasAliveSession(remoteProfile: string, remoteInstancePublicKey: string): boolean {
    const entry = this.sessions.get(this.sessionKey(remoteProfile, remoteInstancePublicKey));
    return entry !== undefined && entry.isAlive();
  }

  private sessionKey(remoteProfile: string, remoteInstancePublicKey: string): string {
    return `${remoteProfile.toLowerCase()}|${remoteInstancePublicKey.toLowerCase()}`;
  }

  /**
   * Build a per-session event emitter that bakes the remote identity
   * into every emission. We construct one of these *per attach* (not
   * per registry) so the peer-loop's hook sites can stay context-free
   * even though every emission must carry the remote profile/instance.
   */
  private makeSessionEmitter(
    remote: string,
    remotePeerId: string,
    remoteInstance: string,
  ): PeerSessionEventEmitter | undefined {
    const bus = this.bus;
    if (bus === undefined) return undefined;
    return {
      blockSent: (blockHash, bytes) => {
        bus.emit({
          kind: 'block-sent',
          at: Date.now(),
          blockHash,
          bytes,
          toProfile: remote,
          toPeerId: remotePeerId,
          toInstancePublicKey: remoteInstance,
        });
      },
      blockReceived: (blockHash, bytes) => {
        bus.emit({
          kind: 'block-received',
          at: Date.now(),
          blockHash,
          bytes,
          fromProfile: remote,
          fromPeerId: remotePeerId,
          fromInstancePublicKey: remoteInstance,
        });
      },
      eventReceived: (channel, eventHash, bytes) => {
        bus.emit({
          kind: 'event-received',
          at: Date.now(),
          eventHash,
          channel,
          bytes,
          fromProfile: remote,
          fromPeerId: remotePeerId,
          fromInstancePublicKey: remoteInstance,
        });
      },
    };
  }

  attach(
    log: Log,
    remoteProfilePublicKey: string,
    remotePeerId: string,
    remoteInstancePublicKey: string,
    peer: DuplexPeer,
    sessionOptions: AttachPeerSessionOptions = {},
    transportLabel = 'unknown',
    localAssociationProfile = '',
    locallyInitiated = false,
  ): { readonly entry: FriendSessionEntry; readonly created: boolean } {
    const remote = remoteProfilePublicKey.toLowerCase();
    const remotePid = remotePeerId.toLowerCase();
    const remoteInstance = remoteInstancePublicKey.toLowerCase();
    const key = this.sessionKey(remote, remoteInstance);
    const existing = this.sessions.get(key);
    if (existing !== undefined) {
      if (existing.isAlive()) {
        const existingTier = transportPreference(existing.transportLabel);
        const newTier = transportPreference(transportLabel);
        if (newTier > existingTier) {
          peer.close();
          return { entry: existing, created: false };
        }
        if (
          newTier === existingTier &&
          preferKeepExistingSession(existing.locallyInitiated, locallyInitiated)
        ) {
          peer.close();
          return { entry: existing, created: false };
        }
        existing.stop();
        existing.close();
      } else {
        existing.stop();
        existing.close();
      }
      this.sessions.delete(key);
    }

    const subject = profileSubject(remote);
    let alive = true;
    let entry: FriendSessionEntry;
    const sessionEmitter = this.makeSessionEmitter(remote, remotePid, remoteInstance);
    const optionsWithEvents: AttachPeerSessionOptions =
      sessionEmitter !== undefined
        ? { ...sessionOptions, events: sessionEmitter }
        : sessionOptions;
    const stop = attachPeerSession(log, subject, peer, () => {
      const wasAlive = alive;
      alive = false;
      if (this.sessions.get(key) === entry) {
        this.sessions.delete(key);
      }
      if (wasAlive && this.bus !== undefined) {
        this.bus.emit({
          kind: 'peer-disconnected',
          at: Date.now(),
          remoteProfilePublicKey: remote,
          remotePeerId: remotePid,
          remoteInstancePublicKey: remoteInstance,
          transportLabel,
        });
      }
    }, optionsWithEvents);
    entry = {
      remoteProfilePublicKey: remote,
      remoteInstancePublicKey: remoteInstance,
      remotePeerId: remotePid,
      transportLabel,
      connectedAt: new Date(),
      localAssociationProfile: localAssociationProfile.toLowerCase(),
      locallyInitiated,
      stop,
      close: () => peer.close(),
      isAlive: () => alive,
    };
    this.sessions.set(key, entry);
    return { entry, created: true };
  }

  /**
   * Frozen snapshot of currently-alive sessions. Used by `SyncHandle.peers()`
   * so that CLI consumers can render observability views ("where is this
   * block coming from?") without taking a reference to the live registry.
   */
  liveEntries(): readonly FriendSessionEntry[] {
    const out: FriendSessionEntry[] = [];
    for (const entry of this.sessions.values()) {
      if (entry.isAlive()) out.push(entry);
    }
    return out;
  }

  closeAll(): void {
    for (const entry of this.sessions.values()) {
      entry.stop();
      entry.close();
    }
    this.sessions.clear();
  }
}
