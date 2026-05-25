import { randomBytes } from 'crypto';
import type { Log } from 'nearbytes-log';
import { profileSubject, syncTopic } from '../core/topic.js';
import { createCompositeDiscovery } from '../discovery/composite.js';
import type { DiscoveredPeer } from '../discovery/types.js';
import { connectDiscoveredPeer } from './connect.js';
import { createHyperswarmDiscovery } from './discovery/hyperswarm.js';
import { createMdnsDiscovery } from './discovery/mdns.js';
import { appendBenchMarker } from '../benchMarker.js';
import { patchLogForReactiveHave } from '../core/sessionRegistry.js';
import { logSyncError } from '../logSyncError.js';
import { exchangeFriendHandshake } from '../core/handshake.js';
import { FriendSessionRegistry } from '../core/friendSessions.js';
import { createNodeDiskBlockStreamFactory } from './blockReceive.js';

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

export interface SyncHandle {
  readonly friends: readonly string[];
  readonly serveProfilePublicKeys: readonly string[];
  stop(): Promise<void>;
}

function normalizeKeySet(keys: readonly string[]): Set<string> {
  const set = new Set<string>();
  for (const pk of keys) {
    set.add(pk.toLowerCase());
  }
  return set;
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

  if (topics.length === 0 || friendSet.size === 0) {
    return {
      friends,
      serveProfilePublicKeys: [...servedSet],
      async stop() {},
    };
  }

  const peerId = randomBytes(16).toString('hex');
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
    let remoteHint = expectedRemote?.toLowerCase();
    void (async () => {
      try {
        const duplex = await connectDiscoveredPeer(discovered);
        if (remoteHint !== undefined && !friendSet.has(remoteHint)) {
          duplex.close();
          return;
        }

        const remoteProfile = await exchangeFriendHandshake(duplex, {
          localProfilePublicKey: localProfileForAssoc,
          subject: profileSubject(associationProfile),
          allowedRemoteProfiles: friendSet,
        });
        remoteHint = remoteProfile;

        const pairKey = `${localProfileForAssoc}:${remoteProfile}`;
        if (connectingPairs.has(pairKey)) {
          duplex.close();
          return;
        }
        connectingPairs.add(pairKey);

        await appendBenchMarker(log, 'peer-connected', {
          transport: discovered.transport,
          label: discovered.label.slice(0, 64),
        });

        const storageRoot = options.blockStorageRoot;
        const { created } = friendSessions.attach(
          log,
          remoteProfile,
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
            localProfile: localProfileForAssoc.slice(0, 16),
          });
        }

        connectingPairs.delete(pairKey);
      } catch (err) {
        logSyncError(`friend-connect:${discovered.label}`, err);
        if (remoteHint !== undefined) {
          connectingPairs.delete(`${localProfileForAssoc}:${remoteHint}`);
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
      if (!friendSet.has(discovered.profilePublicKey)) {
        return;
      }
      openFriendAssociation(discovered, association!, discovered.profilePublicKey);
      return;
    }
    openFriendAssociation(discovered, association!);
  });

  await discovery.start();
  await appendBenchMarker(log, 'discovery-started', {
    topics: topics.length,
    friends: friends.length,
    serve: servedSet.size,
  });

  return {
    friends,
    serveProfilePublicKeys: [...servedSet],
    async stop(): Promise<void> {
      friendSessions.closeAll();
      await discovery.stop();
      await log.sync.appendMarker(`nearbytes-sync stop ${new Date().toISOString()}`);
    },
  };
}
