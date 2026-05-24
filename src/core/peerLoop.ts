import type { Hash } from 'nearbytes-crypto';
import type { Log, ReceptionObjectRef } from 'nearbytes-log';
import { publicKeyFromHex, serializeEvent } from 'nearbytes-log';
import { acceptData } from './acceptData.js';
import { createFrameDecoder, encodeFrame } from './codec.js';
import type { ObjectRef, Subject, SyncMessage } from './types.js';
import { appendBenchMarker } from '../benchMarker.js';
import { registerLocalHaveAnnouncer, type LocalHaveAnnouncer } from './sessionRegistry.js';

export interface DuplexPeer {
  write(chunk: Uint8Array): void;
  onData(handler: (chunk: Uint8Array) => void): void;
  close(): void;
  onClose?(handler: () => void): void;
}

async function toWireRef(log: Log, ref: ReceptionObjectRef): Promise<ObjectRef> {
  if (ref.kind === 'block') {
    return { kind: 'block', hash: ref.hash };
  }
  const pk = publicKeyFromHex(ref.channel);
  if (!pk) {
    return { kind: 'event', channel: ref.channel, hash: ref.hash };
  }
  try {
    const event = await log.events.retrieveEvent(pk, ref.hash as Hash);
    const blockRefs = event.envelope.blockRefs.map((h) => h as string);
    return {
      kind: 'event',
      channel: ref.channel,
      hash: ref.hash,
      ...(blockRefs.length > 0 ? { blockRefs } : {}),
    };
  } catch {
    return { kind: 'event', channel: ref.channel, hash: ref.hash };
  }
}

async function readLocalBytes(log: Log, ref: ObjectRef): Promise<Uint8Array | null> {
  try {
    if (ref.kind === 'block') {
      return await log.blocks.retrieve(ref.hash as Hash);
    }
    const pk = publicKeyFromHex(ref.channel);
    if (!pk) {
      return null;
    }
    const event = await log.events.retrieveEvent(pk, ref.hash as Hash);
    return new TextEncoder().encode(JSON.stringify(serializeEvent(event)));
  } catch {
    return null;
  }
}

async function hasObject(log: Log, ref: ObjectRef): Promise<boolean> {
  if (ref.kind === 'block') {
    return log.blocks.has(ref.hash as Hash);
  }
  const pk = publicKeyFromHex(ref.channel);
  if (!pk) {
    return false;
  }
  const events = await log.events.listEvents(pk);
  return events.includes(ref.hash as Hash);
}

async function missingRefs(log: Log, refs: readonly ObjectRef[]): Promise<ObjectRef[]> {
  const missing: ObjectRef[] = [];
  for (const ref of refs) {
    if (!(await hasObject(log, ref))) {
      missing.push(ref);
    }
  }
  return missing;
}

/** Keep JSON frames under ~3 MiB base64 on the wire. */
const MAX_SYNC_DATA_CHUNK_BYTES = 2 * 1024 * 1024;

type PendingBlock = { readonly total: number; readonly parts: Map<number, Uint8Array> };

function sendBlockData(
  send: (message: SyncMessage) => void,
  ref: Extract<ObjectRef, { kind: 'block' }>,
  bytes: Uint8Array,
): void {
  if (bytes.byteLength <= MAX_SYNC_DATA_CHUNK_BYTES) {
    send({ type: 'data', object: ref, bytes });
    return;
  }
  const total = bytes.byteLength;
  for (let offset = 0; offset < total; offset += MAX_SYNC_DATA_CHUNK_BYTES) {
    const end = Math.min(offset + MAX_SYNC_DATA_CHUNK_BYTES, total);
    send({
      type: 'data',
      object: ref,
      bytes: bytes.subarray(offset, end),
      offset,
      total,
    });
  }
}

async function acceptDataMessage(
  log: Log,
  msg: Extract<SyncMessage, { type: 'data' }>,
  pendingBlocks: Map<string, PendingBlock>,
): Promise<'stored' | 'duplicate' | 'invalid' | 'pending'> {
  if (msg.object.kind === 'block' && msg.total != null && msg.total > 0) {
    const key = msg.object.hash;
    let pending = pendingBlocks.get(key);
    if (!pending) {
      pending = { total: msg.total, parts: new Map() };
      pendingBlocks.set(key, pending);
    }
    pending.parts.set(msg.offset ?? 0, msg.bytes);
    let received = 0;
    for (const chunk of pending.parts.values()) {
      received += chunk.byteLength;
    }
    if (received < pending.total) {
      return 'pending';
    }
    const merged = new Uint8Array(pending.total);
    for (const [offset, chunk] of pending.parts) {
      merged.set(chunk, offset);
    }
    pendingBlocks.delete(key);
    return acceptData(log, msg.object, merged);
  }
  return acceptData(log, msg.object, msg.bytes);
}

/** SYNC-12: blocks before events in separate want messages. */
function partitionWantRefs(refs: readonly ObjectRef[]): {
  readonly blocks: ObjectRef[];
  readonly events: ObjectRef[];
} {
  const blocks: ObjectRef[] = [];
  const events: ObjectRef[] = [];
  for (const ref of refs) {
    if (ref.kind === 'block') {
      blocks.push(ref);
    } else {
      events.push(ref);
    }
  }
  return { blocks, events };
}

/**
 * Attaches anti-entropy on an association that already completed {@code hello}.
 * {@code subject} MUST be the remote friend's profile subject (SYNC-07).
 */
export function attachPeerSession(log: Log, subject: Subject, peer: DuplexPeer): () => void {
  const pendingBlocks = new Map<string, PendingBlock>();
  let wire = Promise.resolve();
  const runSerial = (fn: () => void | Promise<void>): void => {
    wire = wire
      .then(async () => {
        await fn();
      })
      .catch(() => {
        /* keep queue alive after handler errors */
      });
  };

  const send = (message: SyncMessage): void => {
    runSerial(() => {
      peer.write(encodeFrame(message));
    });
  };

  const sendHave = async (
    refs: readonly ReceptionObjectRef[],
    more = false,
    nextCursor?: string,
  ): Promise<void> => {
    if (refs.length === 0 && !more) {
      return;
    }
    const objects: ObjectRef[] = [];
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

  const requestGlobalDelta = (cursor?: string): void => {
    send({
      type: 'delta',
      subject,
      mode: 'global',
      ...(cursor !== undefined ? { cursor } : {}),
      limit: 256,
    });
  };

  const sendWants = (refs: readonly ObjectRef[]): void => {
    const { blocks, events } = partitionWantRefs(refs);
    if (blocks.length > 0) {
      send({ type: 'want', objects: blocks });
    }
    if (events.length > 0) {
      send({ type: 'want', objects: events });
    }
  };

  const announcer: LocalHaveAnnouncer = {
    pushLocalHave(refs) {
      void sendHave(refs, false);
    },
  };
  const unregister = registerLocalHaveAnnouncer(announcer);

  const onMessage = async (msg: SyncMessage): Promise<void> => {
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
        const bytes = await readLocalBytes(log, ref);
        if (bytes && ref.kind === 'block') {
          sendBlockData(send, ref, bytes);
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
      const result = await acceptDataMessage(log, msg, pendingBlocks);
      if (result === 'stored') {
        const size =
          msg.object.kind === 'block' && msg.total != null && msg.total > 0
            ? msg.total
            : msg.bytes.byteLength;
        if (msg.object.kind === 'block') {
          await appendBenchMarker(log, 'inbound-stored', {
            kind: 'block',
            hash: msg.object.hash.slice(0, 16),
            bytes: size,
          });
        } else {
          await appendBenchMarker(log, 'inbound-stored', {
            kind: 'event',
            channel: msg.object.channel.slice(0, 16),
            bytes: size,
          });
        }
      }
    }
  };

  peer.onData(
    createFrameDecoder((message) => {
      runSerial(() => onMessage(message));
    }),
  );

  send({
    type: 'subscribe',
    delta: { type: 'delta', subject, mode: 'global', limit: 256 },
  });
  requestGlobalDelta();

  const stop = (): void => {
    pendingBlocks.clear();
    unregister();
  };
  if ('onClose' in peer && typeof peer.onClose === 'function') {
    peer.onClose(stop);
  }
  return stop;
}
