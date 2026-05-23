import { randomBytes } from 'crypto';
import { attachPeerSession } from '../core/peerLoop.js';
import { profileSubject, syncTopic } from '../core/topic.js';
import { createCompositeDiscovery } from '../discovery/composite.js';
import { connectDiscoveredPeer } from './connect.js';
import { createHyperswarmDiscovery } from './discovery/hyperswarm.js';
import { createMdnsDiscovery } from './discovery/mdns.js';
export async function start(log, friends) {
    const marker = `nearbytes-sync start ${new Date().toISOString()} friends=${friends.length}`;
    await log.sync.appendMarker(marker);
    if (friends.length === 0) {
        return { friends, async stop() { } };
    }
    const topics = [];
    const subjects = friends.map((pk) => {
        const subject = profileSubject(pk);
        return subject;
    });
    for (const subject of subjects) {
        topics.push(await syncTopic(subject));
    }
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
                attachPeerSession(log, profileSubject(friends[0]), duplex);
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