import Hyperswarm from 'hyperswarm';
import { Socket } from 'net';
import { logSyncError } from '../../logSyncError.js';
import { duplexFromTcpSocket } from '../netDuplex.js';
function duplexFromSocket(socket) {
    if (socket instanceof Socket) {
        return duplexFromTcpSocket(socket);
    }
    const handlers = new Set();
    const closeHandlers = new Set();
    socket.on('data', (buf) => {
        const raw = buf;
        const chunk = raw instanceof Uint8Array ? raw : new Uint8Array(raw);
        for (const handler of handlers) {
            handler(chunk);
        }
    });
    socket.on('error', (err) => {
        logSyncError('hyperswarm socket', err);
    });
    socket.on('close', () => {
        for (const handler of closeHandlers) {
            handler();
        }
    });
    return {
        write: (chunk) => socket.write(chunk),
        onData: (cb) => {
            handlers.add(cb);
            return () => handlers.delete(cb);
        },
        setBulkInbound: undefined,
        close: () => socket.end(),
        onClose: (cb) => closeHandlers.add(cb),
    };
}
/**
 * Hyperswarm discovery for friend carriage.
 *
 * `topicToAssociationProfile` maps every joined topic (hex) to the profile
 * whose `topic(profile(p))` produced it — both served local profiles and
 * configured friends are entries in this map. On connection, we read
 * `peerInfo.topics` (the topic intersection with the remote) and map the
 * first match to its associated profile, so the upper layer knows which
 * profile owns this association per `sync-discovery-v1.md` DISC-12.
 *
 * If no topic in the intersection maps to a known profile we fall back to
 * `fallbackAssociationProfile`, which `start.ts` sets to the active served
 * profile so we can still talk on connections we initiated as a follower.
 */
export function createHyperswarmDiscovery(options) {
    const swarm = new Hyperswarm();
    let peerHandler = null;
    const pickAssociationProfile = (peerTopics) => {
        if (peerTopics) {
            for (const topic of peerTopics) {
                const hex = topic.toString('hex');
                const profile = options.topicToAssociationProfile.get(hex);
                if (profile) {
                    return profile;
                }
            }
        }
        return options.fallbackAssociationProfile;
    };
    swarm.on('connection', (socket, peerInfo) => {
        if (!peerHandler) {
            return;
        }
        socket.on('error', (err) => {
            logSyncError(`hyperswarm peer:${peerInfo.publicKey.toString('hex').slice(0, 12)}`, err);
        });
        const duplex = duplexFromSocket(socket);
        peerHandler({
            transport: 'duplex',
            label: `hyperswarm:${peerInfo.publicKey.toString('hex').slice(0, 12)}`,
            connect: async () => duplex,
            associationProfile: pickAssociationProfile(peerInfo.topics),
        });
    });
    return {
        async start() {
            for (const topic of options.topics) {
                await swarm.join(Buffer.from(topic), { client: true, server: true });
            }
        },
        onPeer(handler) {
            peerHandler = handler;
        },
        async stop() {
            await swarm.destroy();
        },
    };
}
//# sourceMappingURL=hyperswarm.js.map