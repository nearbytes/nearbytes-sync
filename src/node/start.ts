import type { Log } from 'nearbytes-log';
import { profileSubject, syncTopic } from '../core/topic.js';
import { createCompositeDiscovery } from '../discovery/composite.js';
import type { DiscoveredPeer } from '../discovery/types.js';
import { connectDiscoveredPeer } from './connect.js';
import { createHyperswarmDiscovery } from './discovery/hyperswarm.js';
import { createMdnsDiscovery } from './discovery/mdns.js';
import { appendBenchMarker } from '../benchMarker.js';
import { syncDebugLine } from '../syncDebugLog.js';
import {
  isSyncTimelineEnabled,
  syncTimelineClear,
  syncTimelineHandoff,
  syncTimelineKey,
  syncTimelineMark,
} from '../syncTimeline.js';
import { patchLogForReactiveHave } from '../core/sessionRegistry.js';
import {
  logSyncError,
  logPeerSocketError,
  logFriendConnectError,
  logFriendConnectRetry,
  classifyFriendConnectError,
} from '../logSyncError.js';
import { exchangeFriendHandshake, SyncHandshakeError } from '../core/handshake.js';
import type { DuplexPeer } from '../core/peerLoop.js';
import { FriendSessionRegistry } from '../core/friendSessions.js';
import { createNodeDiskBlockStreamFactory } from './blockReceive.js';
import { inflightBlockRegistry, outboundBlockStreamCounter } from '../core/inflightBlocks.js';
import { acquireSyncLock } from './dataDirLock.js';
import { createFetchCursorStore } from './fetchCursors.js';
import { loadOrCreateInstanceIdentity, peekInstancePublicKey } from './instanceIdentity.js';
import { loadOrCreateNodeId, peekNodeId as peekStoredNodeId } from './nodeId.js';
import {
  SyncEventBuffer,
  SyncEventBus,
  SyncStatsAccumulator,
  type SyncEvent,
  type SyncStats,
} from '../core/syncEvents.js';

export interface StartOptions {
  /** Lower-case hex profile public keys this node serves; see `requirements/sync-protocol-v1.md` SYNC-00. */
  readonly serveProfilePublicKeys?: readonly string[];
  /** The served profile used as initiator/follower identity per `sync-discovery-v1.md` DISC-12/24. MUST be in `serveProfilePublicKeys`. */
  readonly activeProfilePublicKey?: string;
  /** Log data directory (`…/data`) for fs block streaming (Node). */
  readonly blockStorageRoot?: string;
  /** `mdns` = LAN TCP only (max throughput on localhost). Default `all` (mDNS + Hyperswarm). */
  readonly discoveryTransport?: 'mdns' | 'all';
}

export interface SyncSnapshot {
  /** Block hashes for which a `want` is outstanding (incoming streams not yet finished). */
  readonly inflightInbound: number;
  /** Block stream pumps currently queued or running on the outbound wire chain. */
  readonly inflightOutbound: number;
  /**
   * Number of currently-alive sibling/friend sessions (post-`hello`).
   * Bye-time flush logic in CLI consumers MUST refuse to declare "drained"
   * until this has been > 0 at some point — otherwise fast one-shot writes
   * (e.g. `nbf file add` against a fresh dataDir) exit before the swarm
   * has bootstrapped and the local `have` announcement is never made.
   */
  readonly connectedPeers: number;
}

export interface ConnectedPeer {
  /** Profile public key the remote signs under for this association. */
  readonly remoteProfilePublicKey: string;
  /**
   * Stable per-dataDir instance identity of the remote (DISC-26/27). Two
   * sibling devices that share the same `remoteProfilePublicKey` have different
   * `remoteInstancePublicKey`s and each appear as a distinct entry here.
   */
  readonly remoteInstancePublicKey: string;
  readonly remotePeerId: string;
  /**
   * Transport route taken by this association. Examples:
   *   `mdns-tcp:192.168.1.5:53432` — mDNS-discovered TCP on the LAN
   *   `mdns:<instance-prefix>`     — pre-TCP-handshake mDNS sighting
   *   `dht:<host>:<port>`          — DHT-routed (Hyperswarm UDX/TCP); legacy
   *   `hyperswarm:<short-pubkey>`  labels are still accepted by the CLI
   * The label is exactly what discovery emitted; it is the user-facing
   * answer to "where did this peer come from?".
   */
  readonly transportLabel: string;
  /**
   * Local profile under which this association is run. For sibling carriage
   * (`remoteProfilePublicKey === localAssociationProfile`) the remote is
   * another device of OURS; for asymmetric follow they differ — we are
   * tailing somebody else's profile log.
   */
  readonly localAssociationProfile: string;
  /** Wall-clock when the session became alive (post-handshake). */
  readonly connectedAt: Date;
  /**
   * `'sibling'` when `remoteProfilePublicKey` equals
   * `localAssociationProfile` (same profile = another of our devices),
   * `'friend'` otherwise.
   */
  readonly role: 'sibling' | 'friend';
}

export interface SyncHandle {
  readonly friends: readonly string[];
  readonly serveProfilePublicKeys: readonly string[];
  /**
   * Stable per-`dataDir` instance identity (DISC-27 loopback key, P-256
   * public key hex). Survives process restarts; identifies this storage
   * instance regardless of which profile it serves at any moment.
   *
   * Used by observability tooling to answer "who am I, on the wire?"
   * and to map peer-table rows to known machines. In inert mode (no
   * dataDir, no profile) or writer-only mode (a daemon owns the lock)
   * the value is the empty string — the daemon's beacon is the source
   * of truth for the *actual* engine's identity in that case.
   */
  readonly instancePublicKey: string;
  /** Short stable per-`dataDir` diagnostic/transport peer id (DISC-27). */
  readonly peerId: string;
  /**
   * Hex-encoded public key of the profile the engine is currently
   * authoring under. Always one of `serveProfilePublicKeys` when the
   * engine is live; empty string in inert / writer-only modes.
   */
  readonly activeProfilePublicKey: string;
  /**
   * Point-in-time view of sync activity, used by CLIs to implement a clean
   * `bye`/`quit` flush ("wait until quiet before exiting"). Cheap to call:
   * just reads two counters on the per-Log inflight registries.
   */
  snapshot(): SyncSnapshot;
  /**
   * Currently-alive sibling/friend sessions. Frozen snapshot — safe to
   * mutate the returned array; subsequent calls return a fresh list.
   * Cheap: O(N) over the in-memory session registry, no I/O. Used by
   * observability commands (`peers`, `monitor`) so the operator can
   * answer "is this block coming from the daemon next to me, from a
   * sibling on the LAN, or over the DHT?".
   */
  peers(): readonly ConnectedPeer[];
  /**
   * Subscribe to live wire-level events (`peer-connected`,
   * `peer-disconnected`, `block-sent`, `block-received`,
   * `event-received`). Returns an unsubscribe thunk.
   *
   * Used by `nbf monitor` and similar observability tooling when this
   * process is the active sync engine. In writer-only mode the handle
   * is a stub and `onEvent` returns immediately — the daemon's beacon
   * is the source of truth there (see `readSyncStateBeacon`).
   *
   * The handler MUST be non-blocking: emissions happen on the wire's
   * critical path (hot loop). A slow handler degrades throughput.
   */
  onEvent(handler: (event: SyncEvent) => void): () => void;
  /**
   * Snapshot of the most recent events still resident in the in-memory
   * ring buffer (oldest-first, capped at the buffer's capacity). Lets
   * an observability UI render a backlog on first paint without
   * waiting for new traffic.
   */
  recentEvents(): readonly SyncEvent[];
  /**
   * Cumulative counters (since `start()` ran) plus a short-window
   * throughput estimate. Used by the monitor UI to render bandwidth
   * (KB/s) and lifetime totals (blocks/events/bytes transferred).
   * The window length is part of the returned struct so the UI can
   * label the figure honestly ("over last 5 s") rather than imply
   * instantaneous-ness.
   */
  stats(): SyncStats;
  stop(): Promise<void>;
}

function normalizeKeySet(keys: readonly string[]): Set<string> {
  const set = new Set<string>();
  for (const pk of keys) {
    set.add(pk.toLowerCase());
  }
  return set;
}

/**
 * Read the persisted per-`dataDir` instance identity *without* creating one
 * if missing. Returns the empty string if the file does not exist, is
 * unreadable, or contains invalid JSON. Used by writer-only consumers (the skeleton's
 * writer-only sync stub) that need to know "what id is the daemon
 * using for this dataDir?" but must not race the daemon for file
 * creation.
 */
export function peekNodeId(dataDir: string): string {
  return peekStoredNodeId(dataDir);
}

export { peekInstancePublicKey };

export async function start(
  log: Log,
  friends: readonly string[],
  options: StartOptions = {},
): Promise<SyncHandle> {
  patchLogForReactiveHave(log);

  const friendSet = normalizeKeySet(friends);
  const servedSet = normalizeKeySet(options.serveProfilePublicKeys ?? []);
  if (servedSet.size === 0) {
    throw new Error('friend carriage requires at least one served profile (configure profiles[])');
  }
  const activeProfile =
    options.activeProfilePublicKey?.toLowerCase() ?? [...servedSet][0]!;
  if (!servedSet.has(activeProfile)) {
    throw new Error('activeProfilePublicKey must be one of serveProfilePublicKeys');
  }

  const marker = `nearbytes-sync start ${new Date().toISOString()} friends=${friends.length} serve=${servedSet.size} active=${activeProfile.slice(0, 12)}`;
  await log.sync.appendMarker(marker);

  const topics: Uint8Array[] = [];
  const topicHexes = new Set<string>();
  const topicToAssociationProfile = new Map<string, string>();

  const addTopicForProfile = async (profile: string): Promise<void> => {
    const subject = profileSubject(profile);
    const topic = await syncTopic(subject);
    const hex = Buffer.from(topic).toString('hex');
    if (!topicHexes.has(hex)) {
      topicHexes.add(hex);
      topics.push(topic);
      topicToAssociationProfile.set(hex, profile);
    }
  };

  for (const lp of servedSet) {
    await addTopicForProfile(lp);
  }
  for (const f of friendSet) {
    await addTopicForProfile(f);
  }

  // Discovery starts whenever we have at least one topic to advertise on,
  // which is true whenever we serve at least one profile (DISC-10). An
  // empty friend list is no longer a no-op: per `sync-discovery-v1.md`
  // DISC-26, sibling devices that share a served profile MUST be able to
  // discover and sync with each other without any friend setup.
  if (topics.length === 0) {
    return {
      friends,
      serveProfilePublicKeys: [...servedSet],
      instancePublicKey: '',
      peerId: '',
      activeProfilePublicKey: activeProfile,
      snapshot: () => ({ inflightInbound: 0, inflightOutbound: 0, connectedPeers: 0 }),
      peers: () => [],
      onEvent: () => () => {},
      recentEvents: () => [],
      stats: () => ({
        totalBytesIn: 0,
        totalBytesOut: 0,
        totalBlocksIn: 0,
        totalBlocksOut: 0,
        totalEventsIn: 0,
        bytesPerSecIn: 0,
        bytesPerSecOut: 0,
        windowMs: 5_000,
      }),
      async stop() {},
    };
  }

  const releaseDataDirLock = acquireSyncLock(options.blockStorageRoot);
  const peerId = loadOrCreateNodeId(options.blockStorageRoot);
  const instance = loadOrCreateInstanceIdentity(options.blockStorageRoot);
  const instancePublicKey = instance.publicKey;
  const fetchCursors = createFetchCursorStore(options.blockStorageRoot);
  const authorizedRemoteProfiles = new Set<string>([...servedSet, ...friendSet]);
  // Default to mDNS+Hyperswarm so peers find each other across both LAN and
  // WAN out of the box. Benchmarks that want LAN-only TCP for max throughput
  // (no Noise encryption, no UDX framing) explicitly opt out by setting
  // NEARBYTES_SYNC_DISCOVERY=mdns or passing `discoveryTransport: 'mdns'`.
  const transport =
    options.discoveryTransport ??
    process.env['NEARBYTES_SYNC_DISCOVERY'] ??
    'all';
  const backends = [
    createMdnsDiscovery({
      peerId,
      instancePublicKey,
      localProfilePublicKeys: [...servedSet],
      activeProfilePublicKey: activeProfile,
      friendProfileKeys: friendSet,
    }),
  ];
  if (transport === 'all') {
    backends.unshift(
      createHyperswarmDiscovery({
        topics,
        topicToAssociationProfile,
        fallbackAssociationProfile: activeProfile,
      }),
    );
  }
  const discovery = createCompositeDiscovery(backends);

  // Observability bus owned by start(): every wire-level event flows
  // through here, and `start()` is the single point that wires both
  // the in-process ring buffer (for `SyncHandle.recentEvents()`) and
  // any out-of-process publisher (the daemon's beacon) onto it.
  const eventBus = new SyncEventBus();
  const eventBuffer = new SyncEventBuffer();
  eventBus.onEvent((e) => eventBuffer.push(e));

  // Cumulative + windowed throughput counters. Driven by the same bus
  // as the ring buffer so a UI does not need a second subscription;
  // `sync.stats()` simply queries this accumulator at render time.
  const statsAccumulator = new SyncStatsAccumulator();
  eventBus.onEvent((e) => {
    switch (e.kind) {
      case 'block-sent':
        statsAccumulator.recordBlockSent(e.bytes);
        break;
      case 'block-received':
        statsAccumulator.recordBlockReceived(e.bytes);
        break;
      case 'event-received':
        statsAccumulator.recordEventReceived(e.bytes);
        break;
      case 'peer-connected':
      case 'peer-disconnected':
      case 'peer-connect-failed':
        // Peer transitions do not move bytes; nothing to accumulate.
        break;
    }
  });

  const timelineDataSeen = new Map<string, { event: boolean; block: boolean; blockCount: number }>();
  if (isSyncTimelineEnabled()) {
    eventBus.onEvent((e) => {
      switch (e.kind) {
        case 'event-received': {
          const key = syncTimelineKey(e.fromProfile, e.fromInstancePublicKey);
          let seen = timelineDataSeen.get(key);
          if (seen === undefined) {
            seen = { event: false, block: false, blockCount: 0 };
            timelineDataSeen.set(key, seen);
          }
          if (!seen.event) {
            seen.event = true;
            syncTimelineMark(key, 'event←', `hash=${e.eventHash.slice(0, 8)}`);
          }
          break;
        }
        case 'block-received': {
          const key = syncTimelineKey(e.fromProfile, e.fromInstancePublicKey);
          let seen = timelineDataSeen.get(key);
          if (seen === undefined) {
            seen = { event: false, block: false, blockCount: 0 };
            timelineDataSeen.set(key, seen);
          }
          seen.blockCount += 1;
          if (!seen.block) {
            seen.block = true;
            syncTimelineMark(key, 'block←', `hash=${e.blockHash.slice(0, 8)}`);
          } else if (seen.blockCount === 2) {
            syncTimelineMark(key, 'blocks←', 'more incoming…');
          }
          break;
        }
        case 'peer-disconnected': {
          const key = syncTimelineKey(e.remoteProfilePublicKey, e.remoteInstancePublicKey);
          const seen = timelineDataSeen.get(key);
          if (seen !== undefined && seen.blockCount > 1) {
            syncTimelineMark(key, 'blocks-done', `count=${seen.blockCount}`);
          }
          timelineDataSeen.delete(key);
          syncTimelineClear(key);
          break;
        }
        case 'peer-connect-failed':
          syncTimelineMark(e.transportLabel, 'connect-failed', `${e.reason} attempts=${e.attempts}`);
          syncTimelineClear(e.transportLabel);
          break;
        default:
          break;
      }
    });
  }

  const friendSessions = new FriendSessionRegistry(eventBus);
  const connectingPairs = new Set<string>();
  const handshakingDuplexes = new WeakSet<DuplexPeer>();

  const FRIEND_CONNECT_MAX_ATTEMPTS = 3;
  const FRIEND_CONNECT_RETRY_MS = 2_000;
  const PAIR_SLOT_WAIT_MS = 60_000;
  const DHT_HANDSHAKE_TIMEOUT_MS = 30_000;

  const acquirePairSlot = async (pairKey: string): Promise<void> => {
    const deadline = Date.now() + PAIR_SLOT_WAIT_MS;
    while (connectingPairs.has(pairKey)) {
      if (Date.now() >= deadline) {
        throw new SyncHandshakeError('timeout', 'sync pair slot busy', true);
      }
      await new Promise<void>((resolve) => {
        const t = setTimeout(resolve, 25);
        if (typeof t.unref === 'function') {
          t.unref();
        }
      });
    }
    connectingPairs.add(pairKey);
  };

  const releasePairSlot = (pairKey: string): void => {
    connectingPairs.delete(pairKey);
  };

  const openFriendAssociation = (
    discovered: DiscoveredPeer,
    associationProfile: string,
    expectedRemote?: string,
  ): void => {
    const localProfileForAssoc = servedSet.has(associationProfile)
      ? associationProfile
      : activeProfile;
    let remoteProfileHint = expectedRemote?.toLowerCase();
    let pairKeyOptimistic: string | null = null;
    const connectScope = `friend-connect:${discovered.label}`;
    const timelineLabelKey = discovered.label;
    void (async () => {
      for (let attempt = 1; attempt <= FRIEND_CONNECT_MAX_ATTEMPTS; attempt++) {
      let duplex: DuplexPeer | undefined;
      let timelineKey = timelineLabelKey;
      try {
        syncTimelineMark(timelineLabelKey, 'connect-start', `attempt=${attempt}`);
        duplex = await connectDiscoveredPeer(discovered);
        syncTimelineMark(timelineLabelKey, 'tcp-connected');
        if (handshakingDuplexes.has(duplex)) {
          return;
        }
        handshakingDuplexes.add(duplex);
        if (
          remoteProfileHint !== undefined &&
          !authorizedRemoteProfiles.has(remoteProfileHint)
        ) {
          duplex.close();
          return;
        }

        const {
          remoteProfile,
          remotePeerId,
          remoteInstancePublicKey,
          earlyMessages,
        } = await exchangeFriendHandshake(duplex, {
          localProfilePublicKey: localProfileForAssoc,
          localPeerId: peerId,
          localInstancePublicKey: instancePublicKey,
          subject: profileSubject(associationProfile),
          allowedRemoteProfiles: authorizedRemoteProfiles,
          timeoutMs:
            discovered.transport === 'duplex' ? DHT_HANDSHAKE_TIMEOUT_MS : undefined,
        });
        remoteProfileHint = remoteProfile;
        timelineKey = syncTimelineHandoff(timelineLabelKey, remoteProfile, remoteInstancePublicKey);
        syncTimelineMark(
          timelineKey,
          'hello-done',
          `profile=${remoteProfile.slice(0, 8)} inst=${remoteInstancePublicKey.slice(0, 8)}`,
        );

        // Pair key includes the remote instance so two sibling devices
        // (same profile, different dataDir instance) each get their own
        // connecting-pair slot and can run in parallel.
        const pairKey = `${localProfileForAssoc}:${remoteProfile}:${remoteInstancePublicKey}`;
        await acquirePairSlot(pairKey);
        pairKeyOptimistic = pairKey;
        try {
          if (friendSessions.hasAliveSession(remoteProfile, remoteInstancePublicKey)) {
            syncDebugLine(
              'wire',
              `duplicate connect dropped remote=${remoteProfile.slice(0, 12)} inst=${remoteInstancePublicKey.slice(0, 8)}`,
            );
            duplex.close();
            return;
          }

          await appendBenchMarker(log, 'peer-connected', {
            transport: discovered.transport,
            label: discovered.label.slice(0, 64),
          });

          const storageRoot = options.blockStorageRoot;
          const initialFetchCursor = await fetchCursors.get(
            remoteProfile,
            remoteInstancePublicKey,
          );
          syncTimelineMark(
            timelineKey,
            'cursor-loaded',
            initialFetchCursor ?? 'start',
          );
          const { created } = friendSessions.attach(
            log,
            remoteProfile,
            remotePeerId,
            remoteInstancePublicKey,
            duplex,
            {
              blockStorageRoot: storageRoot,
              initialFetchCursor,
              initialMessages: earlyMessages,
              timelineKey,
              onFetchCursorCheckpoint: (cursor) =>
                fetchCursors.put(remoteProfile, remoteInstancePublicKey, cursor).catch((err) =>
                  logSyncError('fetchCursorCheckpoint', err),
                ),
              ...(storageRoot
                ? { diskBlockStream: createNodeDiskBlockStreamFactory(storageRoot) }
                : {}),
            },
            discovered.label,
            localProfileForAssoc,
            discovered.transport === 'duplex' && discovered.locallyInitiated === true,
          );
          if (created) {
            syncTimelineMark(timelineKey, 'session-attached');
            await appendBenchMarker(log, 'friend-session-attached', {
              remote: remoteProfile.slice(0, 16),
              remotePeerId: remotePeerId.slice(0, 16),
              remoteInstance: remoteInstancePublicKey.slice(0, 16),
              localProfile: localProfileForAssoc.slice(0, 16),
              sibling: remoteProfile === localProfileForAssoc,
            });
            // The friend-session-attached marker above is journal-grade
            // and survives restarts; the bus emit below is live-grade
            // and feeds the in-process monitor (`SyncHandle.onEvent`)
            // and the daemon beacon. Both fire here because they answer
            // the same question — "we now have a live peer" — at
            // different latencies and durabilities. peer-disconnected
            // is emitted by `FriendSessionRegistry` itself when the
            // close hook fires; no symmetric call here.
            eventBus.emit({
              kind: 'peer-connected',
              at: Date.now(),
              remoteProfilePublicKey: remoteProfile,
              remoteInstancePublicKey,
              remotePeerId,
              transportLabel: discovered.label,
              role:
                remoteProfile === localProfileForAssoc
                  ? ('sibling' as const)
                  : ('friend' as const),
            });
          }
        } finally {
          releasePairSlot(pairKey);
          pairKeyOptimistic = null;
        }
        return;
      } catch (err) {
        const classified = classifyFriendConnectError(err);
        const canRetry =
          classified.retryable && attempt < FRIEND_CONNECT_MAX_ATTEMPTS;
        if (canRetry) {
          logFriendConnectRetry(
            connectScope,
            classified.tag,
            attempt + 1,
            FRIEND_CONNECT_MAX_ATTEMPTS,
            FRIEND_CONNECT_RETRY_MS,
          );
          await new Promise<void>((resolve) => {
            const t = setTimeout(resolve, FRIEND_CONNECT_RETRY_MS);
            if (typeof t.unref === 'function') {
              t.unref();
            }
          });
          continue;
        }
        logFriendConnectError(connectScope, err);
        eventBus.emit({
          kind: 'peer-connect-failed',
          at: Date.now(),
          transportLabel: discovered.label,
          reason: classified.tag,
          attempts: attempt,
          remoteProfilePublicKey: remoteProfileHint ?? '',
          remoteInstancePublicKey: '',
          remotePeerId: '',
        });
        if (pairKeyOptimistic !== null) {
          releasePairSlot(pairKeyOptimistic);
          pairKeyOptimistic = null;
        }
        return;
      } finally {
        if (duplex !== undefined) {
          handshakingDuplexes.delete(duplex);
        }
      }
      }
    })();
  };

  discovery.onPeer((discovered) => {
    syncTimelineMark(
      discovered.label,
      'discovered',
      `transport=${discovered.transport}`,
    );
    const association =
      discovered.transport === 'tcp'
        ? discovered.associationProfile
        : discovered.associationProfile ?? activeProfile;
    if (discovered.transport === 'tcp') {
      if (!authorizedRemoteProfiles.has(discovered.profilePublicKey)) {
        return;
      }
      openFriendAssociation(discovered, association!, discovered.profilePublicKey);
      return;
    }
    openFriendAssociation(discovered, association!);
  });

  try {
    await discovery.start();
  } catch (err) {
    releaseDataDirLock();
    throw err;
  }
  await appendBenchMarker(log, 'discovery-started', {
    topics: topics.length,
    friends: friends.length,
    serve: servedSet.size,
  });

  const inbound = inflightBlockRegistry(log);
  const outbound = outboundBlockStreamCounter(log);
  return {
    friends,
    serveProfilePublicKeys: [...servedSet],
    instancePublicKey,
    peerId,
    activeProfilePublicKey: activeProfile,
    snapshot: (): SyncSnapshot => ({
      inflightInbound: inbound.size(),
      inflightOutbound: outbound.size(),
      connectedPeers: friendSessions.aliveCount,
    }),
    peers: (): readonly ConnectedPeer[] =>
      friendSessions.liveEntries().map((entry) => ({
        remoteProfilePublicKey: entry.remoteProfilePublicKey,
        remoteInstancePublicKey: entry.remoteInstancePublicKey,
        remotePeerId: entry.remotePeerId,
        transportLabel: entry.transportLabel,
        localAssociationProfile: entry.localAssociationProfile,
        connectedAt: entry.connectedAt,
        role:
          entry.localAssociationProfile !== '' &&
          entry.localAssociationProfile === entry.remoteProfilePublicKey
            ? ('sibling' as const)
            : ('friend' as const),
      })),
    onEvent: (handler) => eventBus.onEvent(handler),
    recentEvents: () => eventBuffer.recent(),
    stats: () => statsAccumulator.snapshot(),
    async stop(): Promise<void> {
      /**
       * Teardown contract: every step is best-effort and the dataDir lock
       * MUST be released no matter what. The lock represents our claim on
       * the storage root; leaving it behind on partial failure would force
       * the next start to take the stale-lock recovery path, which is
       * correct but ugly. We therefore run each step in isolation and
       * release the lock in a `finally`.
       */
      try {
        friendSessions.closeAll();
      } catch (err) {
        process.stderr.write(
          `[nearbytes-sync] friendSessions.closeAll failed: ${String(err)}\n`,
        );
      }
      try {
        await discovery.stop();
      } catch (err) {
        process.stderr.write(`[nearbytes-sync] discovery.stop failed: ${String(err)}\n`);
      }
      try {
        await log.sync.appendMarker(`nearbytes-sync stop ${new Date().toISOString()}`);
      } catch {
        /* best-effort marker; not a correctness boundary */
      }
      releaseDataDirLock();
    },
  };
}
