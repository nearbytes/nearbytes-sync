import { randomBytes } from 'crypto';
import { attachPeerSession } from '../core/peerLoop.js';
import { profileSubject, syncTopic } from '../core/topic.js';
import { createCompositeDiscovery } from '../discovery/composite.js';
import { connectDiscoveredPeer } from './connect.js';
import { createHyperswarmDiscovery } from './discovery/hyperswarm.js';
import { createMdnsDiscovery } from './discovery/mdns.js';
export async function start(log, friends, options = {}) {
    const marker = `nearbytes-sync start ${new Date().toISOString()} friends=${friends.length} serve=${options.serveProfilePublicKey ? 'yes' : 'no'}`;
    await log.sync.appendMarker(marker);
    const topics = [];
    const topicHexes = new Set();
    const addTopic = async (subject) => {
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
    if (options.serveProfilePublicKey) {
        await addTopic(profileSubject(options.serveProfilePublicKey));
    }
    if (topics.length === 0) {
        return { friends, async stop() { } };
    }
    const primarySubject = options.serveProfilePublicKey
        ? profileSubject(options.serveProfilePublicKey)
        : friends.length > 0
            ? profileSubject(friends[0])
            : profileSubject(options.serveProfilePublicKey);
    const peerId = randomBytes(16).toString('hex');
    const discovery = createCompositeDiscovery([
        createHyperswarmDiscovery(topics),
        createMdnsDiscovery({ peerId }),
    ]);
    const sessions = [];
    discovery.onPeer((discovered) => {
        void (async () => {
            try {
                const duplex = await connectDiscoveredPeer(discovered);
                // One framed session per transport; v0 uses the first configured friend subject on this duplex.
                attachPeerSession(log, primarySubject, duplex);
                sessions.push(duplex);
            }
            catch {
                // ignore unreachable LAN / swarm peers
            }
        })();
    });
    await discovery.start();
    return {
        friends,
        async stop() {
            for (const session of sessions) {
                session.close();
            }
            await discovery.stop();
            await log.sync.appendMarker(`nearbytes-sync stop ${new Date().toISOString()}`);
        },
    };
}
//# sourceMappingURL=start.js.map