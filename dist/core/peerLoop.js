import { publicKeyFromHex, serializeEvent } from 'nearbytes-log';
import { acceptData } from './acceptData.js';
import { createFrameDecoder, encodeFrame } from './codec.js';
function toWireRef(ref) {
    if (ref.kind === 'block') {
        return { kind: 'block', hash: ref.hash };
    }
    return { kind: 'event', channel: ref.channel, hash: ref.hash };
}
async function readLocalBytes(log, ref) {
    try {
        if (ref.kind === 'block') {
            return await log.blocks.retrieve(ref.hash);
        }
        const pk = publicKeyFromHex(ref.channel);
        if (!pk) {
            return null;
        }
        const event = await log.events.retrieveEvent(pk, ref.hash);
        return new TextEncoder().encode(JSON.stringify(serializeEvent(event)));
    }
    catch {
        return null;
    }
}
async function hasObject(log, ref) {
    if (ref.kind === 'block') {
        return log.blocks.has(ref.hash);
    }
    const pk = publicKeyFromHex(ref.channel);
    if (!pk) {
        return true;
    }
    const events = await log.events.listEvents(pk);
    return events.includes(ref.hash);
}
export function attachPeerSession(log, subject, peer) {
    const send = (message) => {
        peer.write(encodeFrame(message));
    };
    const onMessage = async (msg) => {
        if (msg.type === 'delta' && msg.mode === 'global') {
            const out = await log.reception.listAfter(msg.cursor, msg.limit);
            send({
                type: 'have',
                subject,
                fromCursor: msg.cursor,
                nextCursor: out.next,
                objects: out.refs.map(toWireRef),
                more: out.more,
            });
            return;
        }
        if (msg.type === 'have') {
            const wants = [];
            for (const ref of msg.objects) {
                if (!(await hasObject(log, ref))) {
                    wants.push(ref);
                }
            }
            if (wants.length > 0) {
                send({ type: 'want', objects: wants });
            }
            if (msg.more && msg.nextCursor) {
                send({
                    type: 'delta',
                    subject,
                    mode: 'global',
                    cursor: msg.nextCursor,
                    limit: 256,
                });
            }
            return;
        }
        if (msg.type === 'want') {
            for (const ref of msg.objects) {
                const bytes = await readLocalBytes(log, ref);
                if (bytes) {
                    send({ type: 'data', object: ref, bytes });
                }
            }
            return;
        }
        if (msg.type === 'data') {
            await acceptData(log, msg.object, msg.bytes);
        }
    };
    peer.onData(createFrameDecoder((message) => {
        void onMessage(message);
    }));
    send({
        type: 'delta',
        subject,
        mode: 'global',
        limit: 256,
    });
}
//# sourceMappingURL=peerLoop.js.map