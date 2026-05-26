import { randomBytes } from 'crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'fs';
import { join } from 'path';
import type { Log } from 'nearbytes-log';
import { profileSubject, syncTopic } from '../core/topic.js';
import { createCompositeDiscovery } from '../discovery/composite.js';
import type { DiscoveredPeer } from '../discovery/types.js';
import { connectDiscoveredPeer } from './connect.js';
import { createHyperswarmDiscovery } from './discovery/hyperswarm.js';
import { createMdnsDiscovery } from './discovery/mdns.js';
import { appendBenchMarker } from '../benchMarker.js';
import { patchLogForReactiveHave } from '../core/sessionRegistry.js';
import { logSyncError, logPeerSocketError } from '../logSyncError.js';
import { exchangeFriendHandshake } from '../core/handshake.js';
import { FriendSessionRegistry } from '../core/friendSessions.js';
import { createNodeDiskBlockStreamFactory } from './blockReceive.js';
import { inflightBlockRegistry, outboundBlockStreamCounter } from '../core/inflightBlocks.js';
import { acquireSyncLock } from './dataDirLock.js';

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
}

export interface SyncHandle {
  readonly friends: readonly string[];
  readonly serveProfilePublicKeys: readonly string[];
  /**
   * Point-in-time view of sync activity, used by CLIs to implement a clean
   * `bye`/`quit` flush ("wait until quiet before exiting"). Cheap to call:
   * just reads two counters on the per-Log inflight registries.
   */
  snapshot(): SyncSnapshot;
  stop(): Promise<void>;
}

function normalizeKeySet(keys: readonly string[]): Set<string> {
  const set = new Set<string>();
  for (const pk of keys) {
    set.add(pk.toLowerCase());
  }
  return set;
}

const NODE_ID_FILENAME = '.nearbytes-node-id';
const NODE_ID_RE = /^[0-9a-f]{32}$/;

/**
 * Returns the per-`dataDir` node identity used to filter sibling loopback
 * (`sync-discovery-v1.md` DISC-26). Two processes that share the same
 * `dataDir` are the *same* node and MUST NOT see each other as siblings;
 * persisting the id under the directory makes "same storage = same node"
 * the unambiguous loopback predicate, independent of OS process identity.
 *
 * For purely in-memory deployments (`dataDir` undefined) we fall back to a
 * per-process random id so the discovery layer still has something to key
 * loopback by, accepting that two such processes can never collide on
 * storage anyway.
 */
function loadOrCreateNodeId(dataDir: string | undefined): string {
  if (dataDir === undefined) return randomBytes(16).toString('hex');
  mkdirSync(dataDir, { recursive: true });
  const file = join(dataDir, NODE_ID_FILENAME);
  if (existsSync(file)) {
    const existing = readFileSync(file, 'utf8').trim().toLowerCase();
    if (NODE_ID_RE.test(existing)) return existing;
  }
  const fresh = randomBytes(16).toString('hex');
  writeFileSync(file, fresh, { encoding: 'utf8', flag: 'wx' });
  return fresh;
}

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
      snapshot: () => ({ inflightInbound: 0, inflightOutbound: 0 }),
      async stop() {},
    };
  }

  const releaseDataDirLock = acquireSyncLock(options.blockStorageRoot);
  // `peerId` here is the dataDir-derived node identity (DISC-26 loopback
  // key). Stable across process restarts; collides only when two processes
  // genuinely share the same on-disk log — which is also caught by the
  // dataDir lock above and therefore never reaches this point in practice.
  const peerId = loadOrCreateNodeId(options.blockStorageRoot);
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

  const friendSessions = new FriendSessionRegistry();
  const connectingPairs = new Set<string>();

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
    void (async () => {
      try {
        const duplex = await connectDiscoveredPeer(discovered);
        if (
          remoteProfileHint !== undefined &&
          !authorizedRemoteProfiles.has(remoteProfileHint)
        ) {
          duplex.close();
          return;
        }

        const { remoteProfile, remotePeerId } = await exchangeFriendHandshake(duplex, {
          localProfilePublicKey: localProfileForAssoc,
          localPeerId: peerId,
          subject: profileSubject(associationProfile),
          allowedRemoteProfiles: authorizedRemoteProfiles,
        });
        remoteProfileHint = remoteProfile;

        // Pair key now includes the remote peerId so two sibling devices
        // (same profile, different peerId, DISC-26) each get their own
        // connecting-pair slot and can run in parallel.
        const pairKey = `${localProfileForAssoc}:${remoteProfile}:${remotePeerId}`;
        if (connectingPairs.has(pairKey)) {
          duplex.close();
          return;
        }
        connectingPairs.add(pairKey);
        pairKeyOptimistic = pairKey;

        await appendBenchMarker(log, 'peer-connected', {
          transport: discovered.transport,
          label: discovered.label.slice(0, 64),
        });

        const storageRoot = options.blockStorageRoot;
        const { created } = friendSessions.attach(
          log,
          remoteProfile,
          remotePeerId,
          duplex,
          {
            blockStorageRoot: storageRoot,
            ...(storageRoot
              ? { diskBlockStream: createNodeDiskBlockStreamFactory(storageRoot) }
              : {}),
          },
          discovered.label,
        );
        if (created) {
          await appendBenchMarker(log, 'friend-session-attached', {
            remote: remoteProfile.slice(0, 16),
            remotePeerId: remotePeerId.slice(0, 16),
            localProfile: localProfileForAssoc.slice(0, 16),
            sibling: remoteProfile === localProfileForAssoc,
          });
        }

        connectingPairs.delete(pairKey);
        pairKeyOptimistic = null;
      } catch (err) {
        logPeerSocketError(`friend-connect:${discovered.label}`, err);
        if (pairKeyOptimistic !== null) {
          connectingPairs.delete(pairKeyOptimistic);
        }
      }
    })();
  };

  discovery.onPeer((discovered) => {
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
    snapshot: (): SyncSnapshot => ({
      inflightInbound: inbound.size(),
      inflightOutbound: outbound.size(),
    }),
    async stop(): Promise<void> {
      friendSessions.closeAll();
      await discovery.stop();
      await log.sync.appendMarker(`nearbytes-sync stop ${new Date().toISOString()}`);
      releaseDataDirLock();
    },
  };
}
