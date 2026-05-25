import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { blockPath } from 'nearbytes-log';
import { publicKeyFromHex, serializeEvent } from 'nearbytes-log';
import { acceptData } from './acceptData.js';
import { BLOCK_STREAM_WRITE_SLICE_BYTES, createWireDecoder, encodeBlockStreamBegin, encodeFrame, } from './codec.js';
import { appendBenchMarker } from '../benchMarker.js';
import { logSyncError } from '../logSyncError.js';
import { registerLocalHaveAnnouncer } from './sessionRegistry.js';
/** In-memory RX buffer limit per block stream (512 MiB). */
const MAX_BLOCK_STREAM_BUFFER_BYTES = 512 * 1024 * 1024;
async function toWireRef(log, ref) {
    if (ref.kind === 'block') {
        return { kind: 'block', hash: ref.hash };
    }
    const pk = publicKeyFromHex(ref.channel);
    if (!pk) {
        return { kind: 'event', channel: ref.channel, hash: ref.hash };
    }
    try {
        const event = await log.events.retrieveEvent(pk, ref.hash);
        const blockRefs = event.envelope.blockRefs.map((h) => h);
        return {
            kind: 'event',
            channel: ref.channel,
            hash: ref.hash,
            ...(blockRefs.length > 0 ? { blockRefs } : {}),
        };
    }
    catch (err) {
        logSyncError('toWireRef', err);
        return { kind: 'event', channel: ref.channel, hash: ref.hash };
    }
}
async function readLocalBytes(log, ref) {
    try {
        if (ref.kind === 'block') {
            return await log.blocks.retrieve(ref.hash, { verifyIntegrity: false });
        }
        const pk = publicKeyFromHex(ref.channel);
        if (!pk) {
            return null;
        }
        const event = await log.events.retrieveEvent(pk, ref.hash);
        return new TextEncoder().encode(JSON.stringify(serializeEvent(event)));
    }
    catch (err) {
        logSyncError(`readLocalBytes:${ref.kind}:${ref.kind === 'block' ? ref.hash.slice(0, 16) : ref.hash.slice(0, 16)}`, err);
        return null;
    }
}
async function hasObject(log, ref) {
    if (ref.kind === 'block') {
        return log.blocks.has(ref.hash);
    }
    const pk = publicKeyFromHex(ref.channel);
    if (!pk) {
        return false;
    }
    const events = await log.events.listEvents(pk);
    return events.includes(ref.hash);
}
async function missingRefs(log, refs) {
    const missing = [];
    for (const ref of refs) {
        if (!(await hasObject(log, ref))) {
            missing.push(ref);
        }
    }
    return missing;
}
async function pumpBlockStream(peer, bytes) {
    const total = bytes.byteLength;
    const slice = BLOCK_STREAM_WRITE_SLICE_BYTES;
    for (let offset = 0; offset < total; offset += slice) {
        const chunk = bytes.subarray(offset, Math.min(offset + slice, total));
        if (peer.writeAsync) {
            await peer.writeAsync(chunk);
        }
        else {
            peer.write(chunk);
        }
    }
}
function sendBlockStream(log, runOutbound, peer, storageRoot, ref, bytes, totalBytes) {
    runOutbound(async () => {
        try {
            let pumped = null;
            if (storageRoot) {
                const { isTcpDuplexPeer } = await import('../node/netDuplex.js');
                if (isTcpDuplexPeer(peer)) {
                    const { pumpBlockFileOverSocket } = await import('../node/tcpBulk.js');
                    pumped = await pumpBlockFileOverSocket(peer.tcpSocket, storageRoot, ref.hash);
                }
                else {
                    const { pumpBlockFileFromStorage } = await import('../node/blockPump.js');
                    pumped = await pumpBlockFileFromStorage(storageRoot, ref.hash, peer);
                }
            }
            else {
                if (!bytes) {
                    throw new Error('sendBlockStream requires bytes when blockStorageRoot is unset');
                }
                const pumpBeginAt = Date.now();
                const begin = encodeBlockStreamBegin(ref.hash, totalBytes);
                if (peer.writeAsync) {
                    await peer.writeAsync(begin);
                }
                else {
                    peer.write(begin);
                }
                await pumpBlockStream(peer, bytes);
                pumped = { bytes: totalBytes, pumpBeginAt, pumpEndAt: Date.now() };
            }
            if (pumped) {
                await appendBenchMarker(log, 'bulk-send-phases', {
                    hash: ref.hash.slice(0, 16),
                    bytes: pumped.bytes,
                    pumpBeginAt: pumped.pumpBeginAt,
                    pumpEndAt: pumped.pumpEndAt,
                }).catch((err) => logSyncError('bulk-send-phases', err));
            }
        }
        catch (err) {
            logSyncError(`sendBlockStream:${ref.hash.slice(0, 16)}`, err);
        }
    });
}
/** SYNC-12: blocks before events in separate want messages. */
function partitionWantRefs(refs) {
    const blocks = [];
    const events = [];
    for (const ref of refs) {
        if (ref.kind === 'block') {
            blocks.push(ref);
        }
        else {
            events.push(ref);
        }
    }
    return { blocks, events };
}
/**
 * Attaches anti-entropy on an association that already completed {@code hello}.
 * {@code subject} MUST be the remote friend's profile subject (SYNC-07).
 */
export function attachPeerSession(log, subject, peer, onPeerClose, options = {}) {
    const storageRoot = options.blockStorageRoot;
    const diskBlockStream = options.diskBlockStream;
    let inboundWire = Promise.resolve();
    let outboundWire = Promise.resolve();
    const runInbound = (fn) => {
        inboundWire = inboundWire
            .then(async () => {
            await fn();
        })
            .catch((err) => {
            logSyncError('peerLoop inbound', err);
        });
    };
    const runOutbound = (fn) => {
        outboundWire = outboundWire
            .then(async () => {
            await fn();
        })
            .catch((err) => {
            logSyncError('peerLoop outbound', err);
        });
    };
    const send = (message) => {
        runOutbound(() => {
            peer.write(encodeFrame(message));
        });
    };
    const sendHave = async (refs, more = false, nextCursor) => {
        if (refs.length === 0 && !more) {
            return;
        }
        const objects = [];
        for (const ref of refs) {
            objects.push(await toWireRef(log, ref));
        }
        send({
            type: 'have',
            subject,
            objects,
            more,
            ...(nextCursor !== undefined ? { nextCursor } : {}),
        });
    };
    const requestGlobalDelta = (cursor) => {
        send({
            type: 'delta',
            subject,
            mode: 'global',
            ...(cursor !== undefined ? { cursor } : {}),
            limit: 256,
        });
    };
    const sendWants = (refs) => {
        const { blocks, events } = partitionWantRefs(refs);
        if (blocks.length > 0) {
            send({ type: 'want', objects: blocks });
        }
        if (events.length > 0) {
            send({ type: 'want', objects: events });
        }
    };
    const announcer = {
        pushLocalHave(refs) {
            void sendHave(refs, false);
        },
    };
    const unregister = registerLocalHaveAnnouncer(announcer);
    const onMessage = async (msg) => {
        if (msg.type === 'hello') {
            return;
        }
        if (msg.type === 'delta' && msg.mode === 'global') {
            const out = await log.reception.listAfter(msg.cursor, msg.limit);
            await sendHave(out.refs, out.more, out.next);
            return;
        }
        if (msg.type === 'subscribe' && msg.delta.mode === 'global') {
            const out = await log.reception.listAfter(msg.delta.cursor, msg.delta.limit ?? 256);
            await sendHave(out.refs, out.more, out.next);
            return;
        }
        if (msg.type === 'have') {
            const wants = await missingRefs(log, msg.objects);
            if (wants.length > 0) {
                sendWants(wants);
            }
            if (msg.more && msg.nextCursor) {
                requestGlobalDelta(msg.nextCursor);
            }
            return;
        }
        if (msg.type === 'want') {
            const { blocks, events } = partitionWantRefs(msg.objects);
            for (const ref of blocks) {
                if (ref.kind !== 'block') {
                    continue;
                }
                if (storageRoot) {
                    sendBlockStream(log, runOutbound, peer, storageRoot, ref, null, 0);
                    continue;
                }
                const bytes = await readLocalBytes(log, ref);
                if (bytes) {
                    sendBlockStream(log, runOutbound, peer, storageRoot, ref, bytes, bytes.byteLength);
                }
            }
            for (const ref of events) {
                const bytes = await readLocalBytes(log, ref);
                if (bytes) {
                    send({ type: 'data', object: ref, bytes });
                }
            }
            return;
        }
        if (msg.type === 'data') {
            if (msg.object.kind === 'block') {
                return;
            }
            const result = await acceptData(log, msg.object, msg.bytes);
            if (result === 'invalid') {
                logSyncError(`acceptData invalid event:${msg.object.channel.slice(0, 16)}`, new Error('acceptData returned invalid'));
            }
            if (result === 'stored') {
                await appendBenchMarker(log, 'inbound-stored', {
                    kind: 'event',
                    channel: msg.object.channel.slice(0, 16),
                    bytes: msg.bytes.byteLength,
                });
            }
        }
    };
    const onBlockStream = async (hash, bytes) => {
        const object = { kind: 'block', hash };
        const result = await acceptData(log, object, bytes, { verifyIntegrity: false });
        if (result === 'invalid') {
            logSyncError(`acceptData invalid block:${hash.slice(0, 16)}`, new Error('acceptData returned invalid'));
        }
        if (result === 'stored') {
            await appendBenchMarker(log, 'inbound-stored', {
                kind: 'block',
                hash: hash.slice(0, 16),
                bytes: bytes.byteLength,
            });
        }
    };
    let incoming = null;
    const blockStreamComplete = () => {
        if (!incoming) {
            return true;
        }
        if (incoming.mode === 'disk') {
            return incoming.sink.received >= incoming.sink.total;
        }
        if (incoming.mode === 'discard') {
            return incoming.received >= incoming.total;
        }
        return incoming.received >= incoming.total;
    };
    const clearBulkInbound = () => {
        peer.setBulkInbound?.(null);
        peer.setExclusiveInbound?.(null);
        decodeControl.endBlockStream();
    };
    const markBlockStored = async (hash, bytes) => {
        await log.reception.appendReception({ kind: 'block', hash: hash });
        await appendBenchMarker(log, 'inbound-stored', {
            kind: 'block',
            hash: hash.slice(0, 16),
            bytes,
        });
    };
    const finishIncomingStream = async () => {
        const stream = incoming;
        if (!stream) {
            return;
        }
        incoming = null;
        clearBulkInbound();
        if (stream.mode === 'discard') {
            return;
        }
        if (stream.mode === 'disk') {
            const result = await stream.sink.finish();
            if (result.outcome === 'invalid') {
                logSyncError(`block stream hash mismatch ${stream.hash.slice(0, 16)}`, new Error('disk block stream verify failed'));
                return;
            }
            await markBlockStored(stream.hash, stream.total);
            const phases = result.phases;
            const fields = {
                hash: stream.hash.slice(0, 16),
                bytes: stream.total,
                hashDoneAt: phases.hashDoneAt,
                renameDoneAt: phases.renameDoneAt,
            };
            if (phases.firstByteAt !== null) {
                fields['firstByteAt'] = phases.firstByteAt;
            }
            if (phases.lastByteAt !== null) {
                fields['lastByteAt'] = phases.lastByteAt;
            }
            if (phases.diskDrainDoneAt !== null) {
                fields['diskDrainDoneAt'] = phases.diskDrainDoneAt;
            }
            await appendBenchMarker(log, 'bulk-recv-phases', fields).catch((err) => logSyncError('bulk-recv-phases', err));
            await appendBenchMarker(log, 'block-stream-end', {
                hash: stream.hash.slice(0, 16),
                bytes: stream.total,
            }).catch((err) => logSyncError('block-stream-end', err));
            return;
        }
        const digest = stream.hasher.digest('hex');
        if (digest !== stream.hash.toLowerCase()) {
            logSyncError(`block stream hash mismatch ${stream.hash.slice(0, 16)}`, new Error(`expected ${stream.hash.slice(0, 16)} got ${digest.slice(0, 16)}`));
            return;
        }
        await onBlockStream(stream.hash, stream.buffer);
    };
    const ingestStreamBytes = (chunk) => {
        const stream = incoming;
        if (!stream) {
            return;
        }
        if (stream.mode === 'disk') {
            stream.sink.ingest(chunk);
            if (stream.sink.received >= stream.sink.total) {
                void finishIncomingStream().catch((err) => logSyncError('finishIncomingStream', err));
            }
            return;
        }
        if (stream.mode === 'discard') {
            stream.received = Math.min(stream.total, stream.received + chunk.byteLength);
            if (stream.received >= stream.total) {
                incoming = null;
                clearBulkInbound();
            }
            return;
        }
        if (stream.received >= stream.total) {
            return;
        }
        const need = stream.total - stream.received;
        const take = Math.min(need, chunk.byteLength);
        const slice = take === chunk.byteLength ? chunk : chunk.subarray(0, take);
        stream.hasher.update(slice);
        stream.buffer.set(slice, stream.received);
        stream.received += take;
        if (take < chunk.byteLength && stream.received < stream.total) {
            ingestStreamBytes(chunk.subarray(take));
        }
        if (stream.received === stream.total) {
            void finishIncomingStream().catch((err) => logSyncError('finishIncomingStream', err));
        }
    };
    const beginIncomingBlockStream = (hash, total) => {
        if (incoming !== null) {
            throw new Error('block stream already active');
        }
        if (storageRoot && diskBlockStream) {
            const abs = join(storageRoot, blockPath(hash));
            if (existsSync(abs)) {
                incoming = { mode: 'discard', hash, total, received: 0 };
                return;
            }
            const sink = diskBlockStream.create(hash, total);
            incoming = { mode: 'disk', hash, total, sink };
            // Fast path: bypass ingestStreamBytes; the sink is the only consumer here.
            // The exclusive-inbound stream can deliver a chunk that crosses the
            // stream boundary (i.e. last bytes of this stream + the next frame).
            // We pass only the in-stream bytes to the sink and re-dispatch any
            // leftover through the wire decoder so the codec returns to control
            // mode without losing the next `block-stream-begin` frame.
            const directDiskIngest = (chunk) => {
                const remaining = total - sink.received;
                const chunkLen = chunk.byteLength;
                const take = remaining < chunkLen ? remaining : chunkLen;
                sink.ingest(take === chunkLen ? chunk : chunk.subarray(0, take));
                if (sink.received >= total) {
                    void finishIncomingStream().catch((err) => logSyncError('finishIncomingStream', err));
                    if (take < chunkLen) {
                        decodeControl(chunk.subarray(take));
                    }
                }
            };
            if (peer.setExclusiveInbound) {
                peer.setExclusiveInbound(directDiskIngest);
            }
            else {
                peer.setBulkInbound?.(directDiskIngest);
            }
            return;
        }
        peer.setBulkInbound?.(ingestStreamBytes);
        if (total > MAX_BLOCK_STREAM_BUFFER_BYTES) {
            throw new Error(`block stream ${total} B exceeds in-memory limit ${MAX_BLOCK_STREAM_BUFFER_BYTES}`);
        }
        incoming = {
            mode: 'memory',
            hash,
            total,
            hasher: createHash('sha256'),
            buffer: new Uint8Array(total),
            received: 0,
        };
    };
    const decodeControl = createWireDecoder({
        peerBulkBlockStream: peer.setBulkInbound !== undefined || peer.setExclusiveInbound !== undefined,
        onMessage: (message) => {
            runInbound(() => onMessage(message));
        },
        onBlockStreamBegin: (hash, total) => {
            void appendBenchMarker(log, 'block-stream-begin', {
                hash: hash.slice(0, 16),
                bytes: total,
            }).catch(() => undefined);
            beginIncomingBlockStream(hash, total);
        },
        onBlockStreamBytes: ingestStreamBytes,
    });
    peer.onData(decodeControl);
    send({
        type: 'subscribe',
        delta: { type: 'delta', subject, mode: 'global', limit: 256 },
    });
    requestGlobalDelta();
    const stop = () => {
        unregister();
        onPeerClose?.();
    };
    if (peer.onClose) {
        peer.onClose(stop);
    }
    return stop;
}
//# sourceMappingURL=peerLoop.js.map