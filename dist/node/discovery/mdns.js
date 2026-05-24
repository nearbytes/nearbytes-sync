import dgram from 'dgram';
import { createServer } from 'net';
import Bonjour from 'bonjour-service';
import { LAN_DISCOVERY_SERVICE_PROTOCOL, LAN_DISCOVERY_SERVICE_TYPE, LAN_MULTICAST_ANNOUNCE_MS, LAN_MULTICAST_GROUP, LAN_MULTICAST_PORT, LAN_TRANSPORT_PROFILE_ID, buildLanDiscoveryTxtRecord, parseLanDiscoveryTxtRecord, } from '../../discovery/lanProfile.js';
const LAN_MULTICAST_ANNOUNCE_MS_LOCAL = LAN_MULTICAST_ANNOUNCE_MS;
function duplexFromNetSocket(socket) {
    const handlers = new Set();
    socket.on('data', (buf) => {
        const chunk = buf instanceof Uint8Array ? buf : new Uint8Array(buf);
        for (const handler of handlers) {
            handler(chunk);
        }
    });
    return {
        write: (chunk) => socket.write(chunk),
        onData: (cb) => handlers.add(cb),
        close: () => socket.destroy(),
    };
}
export function createMdnsDiscovery(options) {
    const localProfile = options.profilePublicKey.toLowerCase();
    let peerHandler = null;
    let bonjour = null;
    let browser = null;
    let tcpServer = null;
    let syncPort = 0;
    let multicastSocket = null;
    let multicastTimer = null;
    const seenTcp = new Set();
    const isFriendProfile = (profilePublicKey) => options.friendProfileKeys.has(profilePublicKey.toLowerCase());
    return {
        async start() {
            tcpServer = createServer((socket) => {
                if (!peerHandler) {
                    socket.destroy();
                    return;
                }
                const duplex = duplexFromNetSocket(socket);
                peerHandler({
                    transport: 'duplex',
                    label: `mdns-tcp:${socket.remoteAddress}:${socket.remotePort}`,
                    connect: async () => duplex,
                });
            });
            await new Promise((resolve, reject) => {
                tcpServer.listen(0, '0.0.0.0', () => {
                    const address = tcpServer.address();
                    if (typeof address === 'object' && address) {
                        syncPort = address.port;
                    }
                    resolve();
                });
                tcpServer.on('error', reject);
            });
            bonjour = new Bonjour();
            const txt = buildLanDiscoveryTxtRecord({
                peerId: options.peerId,
                syncPort,
                profilePublicKey: localProfile,
            });
            bonjour.publish({
                name: `nearbytes-${options.peerId.slice(0, 8)}`,
                type: LAN_DISCOVERY_SERVICE_TYPE,
                protocol: LAN_DISCOVERY_SERVICE_PROTOCOL,
                port: syncPort,
                txt: txt,
            });
            browser = bonjour.find({
                type: LAN_DISCOVERY_SERVICE_TYPE,
                protocol: LAN_DISCOVERY_SERVICE_PROTOCOL,
            });
            browser.on('up', (service) => {
                if (!peerHandler || !service.addresses?.length) {
                    return;
                }
                const parsed = parseLanDiscoveryTxtRecord((service.txt ?? {}));
                if (!parsed || parsed.alpn !== LAN_TRANSPORT_PROFILE_ID) {
                    return;
                }
                if (parsed.peerId === options.peerId || parsed.profilePublicKey === localProfile) {
                    return;
                }
                if (!isFriendProfile(parsed.profilePublicKey)) {
                    return;
                }
                const host = service.addresses.find((a) => !a.includes(':')) ?? service.addresses[0];
                const key = `${parsed.profilePublicKey}:${host}:${parsed.syncPort}`;
                if (seenTcp.has(key)) {
                    return;
                }
                seenTcp.add(key);
                peerHandler({
                    transport: 'tcp',
                    label: `mdns:${parsed.peerId}`,
                    host,
                    port: parsed.syncPort,
                    profilePublicKey: parsed.profilePublicKey,
                });
            });
            multicastSocket = dgram.createSocket({ type: 'udp4', reuseAddr: true });
            await new Promise((resolve, reject) => {
                multicastSocket.once('error', reject);
                multicastSocket.bind(LAN_MULTICAST_PORT, () => resolve());
            });
            multicastSocket.setMulticastTTL(1);
            const announcement = JSON.stringify({
                pv: '0.4',
                peer: options.peerId,
                port: syncPort,
                alpn: LAN_TRANSPORT_PROFILE_ID,
                prof: localProfile,
            });
            const payload = new TextEncoder().encode(announcement);
            multicastTimer = setInterval(() => {
                multicastSocket?.send(payload, LAN_MULTICAST_PORT, LAN_MULTICAST_GROUP);
            }, LAN_MULTICAST_ANNOUNCE_MS_LOCAL);
        },
        onPeer(handler) {
            peerHandler = handler;
        },
        async stop() {
            if (multicastTimer) {
                clearInterval(multicastTimer);
                multicastTimer = null;
            }
            if (multicastSocket) {
                await new Promise((resolve) => multicastSocket.close(() => resolve()));
                multicastSocket = null;
            }
            browser?.stop();
            browser = null;
            bonjour?.destroy();
            bonjour = null;
            await new Promise((resolve, reject) => {
                if (!tcpServer) {
                    resolve();
                    return;
                }
                tcpServer.close((err) => (err ? reject(err) : resolve()));
            });
            tcpServer = null;
        },
    };
}
//# sourceMappingURL=mdns.js.map