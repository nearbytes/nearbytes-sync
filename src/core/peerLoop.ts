import type { Hash } from 'nearbytes-crypto';
import type { Log, ReceptionObjectRef } from 'nearbytes-log';
import { publicKeyFromHex, serializeEvent } from 'nearbytes-log';
import { acceptData } from './acceptData.js';
import { createFrameDecoder, encodeFrame } from './codec.js';
import type { ObjectRef, Subject, SyncMessage } from './types.js';

export interface DuplexPeer {
  write(chunk: Uint8Array): void;
  onData(handler: (chunk: Uint8Array) => void): void;
  close(): void;
}

function toWireRef(ref: ReceptionObjectRef): ObjectRef {
  if (ref.kind === 'block') {
    return { kind: 'block', hash: ref.hash };
  }
  return { kind: 'event', channel: ref.channel, hash: ref.hash };
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
    return true;
  }
  const events = await log.events.listEvents(pk);
  return events.includes(ref.hash as Hash);
}

export function attachPeerSession(log: Log, subject: Subject, peer: DuplexPeer): void {
  const send = (message: SyncMessage): void => {
    peer.write(encodeFrame(message));
  };

  const onMessage = async (msg: SyncMessage): Promise<void> => {
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
      const wants: ObjectRef[] = [];
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
