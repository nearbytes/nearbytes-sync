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
  /** Join this profile subject so followers can sync with you (your public key hex). */
  readonly serveProfilePublicKey?: string;
  /** Log data directory (`…/data`) for fs block streaming (Node). */
  readonly blockStorageRoot?: string;
  /** `mdns` = LAN TCP only (max throughput on localhost). Default `all` (mDNS + Hyperswarm). */
  readonly discoveryTransport?: 'mdns' | 'all';
}

export interface SyncHandle {
  readonly friends: readonly string[];
  stop(): Promise<void>;
}

function normalizeFriendSet(friends: readonly string[]): Set<string> {
  const set = new Set<string>();
  for (const pk of friends) {
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

  const friendSet = normalizeFriendSet(friends);
  const localProfile = options.serveProfilePublicKey?.toLowerCase();
  if (!localProfile) {
    throw new Error('friend carriage requires serveProfilePublicKey (configure profileSecret)');
  }

  const marker = `nearbytes-sync start ${new Date().toISOString()} friends=${friends.length} serve=yes`;
  await log.sync.appendMarker(marker);

  const topics: Uint8Array[] = [];
  const topicHexes = new Set<string>();

  const addTopic = async (subject: ReturnType<typeof profileSubject>): Promise<void> => {
    const topic = await syncTopic(subject);
    const hex = Buffer.from(topic).toString('hex');
    if (!topicHexes.has(hex)) {
      topicHexes.add(hex);
      topics.push(topic);
    }
  };

  for (const pk of friends) {
    await addTopic(profileSubject(pk));
  }
  await addTopic(profileSubject(localProfile));

  if (topics.length === 0 || friendSet.size === 0) {
    return { friends, async stop() {} };
  }

  const peerId = randomBytes(16).toString('hex');
  const transport =
    options.discoveryTransport ??
    process.env['NEARBYTES_SYNC_DISCOVERY'] ??
    'mdns';
  const backends = [
    createMdnsDiscovery({
      peerId,
      profilePublicKey: localProfile,
      friendProfileKeys: friendSet,
    }),
  ];
  if (transport === 'all') {
    backends.unshift(createHyperswarmDiscovery(topics));
  }
  const discovery = createCompositeDiscovery(backends);

  const friendSessions = new FriendSessionRegistry();
  const connectingFriends = new Set<string>();

  const openFriendAssociation = (discovered: DiscoveredPeer, expectedRemote?: string): void => {
    void (async () => {
      let remoteHint = expectedRemote?.toLowerCase();
      try {
        const duplex = await connectDiscoveredPeer(discovered);
        if (remoteHint !== undefined && !friendSet.has(remoteHint)) {
          duplex.close();
          return;
        }

        const remoteProfile = await exchangeFriendHandshake(duplex, {
          localProfilePublicKey: localProfile,
          subject: profileSubject(remoteHint ?? localProfile),
          allowedRemoteProfiles: friendSet,
        });
        remoteHint = remoteProfile;

        if (connectingFriends.has(remoteProfile)) {
          duplex.close();
          return;
        }
        connectingFriends.add(remoteProfile);

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
          });
        }

        connectingFriends.delete(remoteProfile);
      } catch (err) {
        logSyncError(`friend-connect:${discovered.label}`, err);
        if (remoteHint !== undefined) {
          connectingFriends.delete(remoteHint);
        }
      }
    })();
  };

  discovery.onPeer((discovered) => {
    if (discovered.transport === 'tcp') {
      if (!friendSet.has(discovered.profilePublicKey)) {
        return;
      }
      openFriendAssociation(discovered, discovered.profilePublicKey);
      return;
    }
    openFriendAssociation(discovered);
  });

  await discovery.start();
  await appendBenchMarker(log, 'discovery-started', {
    topics: topics.length,
    friends: friends.length,
    serve: 1,
  });

  return {
    friends,
    async stop(): Promise<void> {
      friendSessions.closeAll();
      await discovery.stop();
      await log.sync.appendMarker(`nearbytes-sync stop ${new Date().toISOString()}`);
    },
  };
}
