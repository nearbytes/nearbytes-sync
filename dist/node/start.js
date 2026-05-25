import { randomBytes } from 'crypto';
import { profileSubject, syncTopic } from '../core/topic.js';
import { createCompositeDiscovery } from '../discovery/composite.js';
import { connectDiscoveredPeer } from './connect.js';
import { createHyperswarmDiscovery } from './discovery/hyperswarm.js';
import { createMdnsDiscovery } from './discovery/mdns.js';
import { appendBenchMarker } from '../benchMarker.js';
import { patchLogForReactiveHave } from '../core/sessionRegistry.js';
import { logSyncError } from '../logSyncError.js';
import { exchangeFriendHandshake } from '../core/handshake.js';
import { FriendSessionRegistry } from '../core/friendSessions.js';
import { createNodeDiskBlockStreamFactory } from './blockReceive.js';
function normalizeKeySet(keys) {
    const set = new Set();
    for (const pk of keys) {
        set.add(pk.toLowerCase());
    }
    return set;
}
export async function start(log, friends, options = {}) {
    patchLogForReactiveHave(log);
    const friendSet = normalizeKeySet(friends);
    const servedSet = normalizeKeySet(options.serveProfilePublicKeys ?? []);
    if (servedSet.size === 0) {
        throw new Error('friend carriage requires at least one served profile (configure profiles[])');
    }
    const activeProfile = options.activeProfilePublicKey?.toLowerCase() ?? [...servedSet][0];
    if (!servedSet.has(activeProfile)) {
        throw new Error('activeProfilePublicKey must be one of serveProfilePublicKeys');
    }
    const marker = `nearbytes-sync start ${new Date().toISOString()} friends=${friends.length} serve=${servedSet.size} active=${activeProfile.slice(0, 12)}`;
    await log.sync.appendMarker(marker);
    const topics = [];
    const topicHexes = new Set();
    const topicToAssociationProfile = new Map();
    const addTopicForProfile = async (profile) => {
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
            async stop() { },
        };
    }
    const peerId = randomBytes(16).toString('hex');
    const transport = options.discoveryTransport ??
        process.env['NEARBYTES_SYNC_DISCOVERY'] ??
        'mdns';
    const backends = [
        createMdnsDiscovery({
            peerId,
            localProfilePublicKeys: [...servedSet],
            activeProfilePublicKey: activeProfile,
            friendProfileKeys: friendSet,
        }),
    ];
    if (transport === 'all') {
        backends.unshift(createHyperswarmDiscovery({
            topics,
            topicToAssociationProfile,
            fallbackAssociationProfile: activeProfile,
        }));
    }
    const discovery = createCompositeDiscovery(backends);
    const friendSessions = new FriendSessionRegistry();
    const connectingPairs = new Set();
    const openFriendAssociation = (discovered, associationProfile, expectedRemote) => {
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
                const { created } = friendSessions.attach(log, remoteProfile, duplex, {
                    blockStorageRoot: storageRoot,
                    ...(storageRoot
                        ? { diskBlockStream: createNodeDiskBlockStreamFactory(storageRoot) }
                        : {}),
                }, discovered.label);
                if (created) {
                    await appendBenchMarker(log, 'friend-session-attached', {
                        remote: remoteProfile.slice(0, 16),
                        localProfile: localProfileForAssoc.slice(0, 16),
                    });
                }
                connectingPairs.delete(pairKey);
            }
            catch (err) {
                logSyncError(`friend-connect:${discovered.label}`, err);
                if (remoteHint !== undefined) {
                    connectingPairs.delete(`${localProfileForAssoc}:${remoteHint}`);
                }
            }
        })();
    };
    discovery.onPeer((discovered) => {
        const association = discovered.transport === 'tcp'
            ? discovered.associationProfile
            : discovered.associationProfile ?? activeProfile;
        if (discovered.transport === 'tcp') {
            if (!friendSet.has(discovered.profilePublicKey)) {
                return;
            }
            openFriendAssociation(discovered, association, discovered.profilePublicKey);
            return;
        }
        openFriendAssociation(discovered, association);
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
        async stop() {
            friendSessions.closeAll();
            await discovery.stop();
            await log.sync.appendMarker(`nearbytes-sync stop ${new Date().toISOString()}`);
        },
    };
}
//# sourceMappingURL=start.js.map