import type { EncryptedData, Hash } from 'nearbytes-crypto';
import type { Log } from 'nearbytes-log';
import {
  deserializeEvent,
  publicKeyFromHex,
  validateBlockBytes,
  validateEventBytes,
} from 'nearbytes-log';
import type { SerializedEvent } from 'nearbytes-crypto';
import type { ObjectRef } from './types.js';

export type AcceptResult = 'stored' | 'duplicate' | 'invalid';

export async function acceptData(
  log: Log,
  ref: ObjectRef,
  bytes: Uint8Array,
): Promise<AcceptResult> {
  if (ref.kind === 'block') {
    const validation = await validateBlockBytes(ref.hash, bytes);
    if (!validation.ok) {
      return 'invalid';
    }
    const hash = ref.hash as Hash;
    if (await log.blocks.has(hash)) {
      return 'duplicate';
    }
    await log.blocks.store(hash, bytes as EncryptedData, true);
    return 'stored';
  }

  const channelHex = ref.channel.toLowerCase();
  const publicKey = publicKeyFromHex(channelHex);
  if (!publicKey) {
    return 'invalid';
  }

  const validation = await validateEventBytes(channelHex, ref.hash, bytes);
  if (!validation.ok) {
    return 'invalid';
  }
  const serialized = JSON.parse(new TextDecoder().decode(bytes)) as SerializedEvent;
  const event = deserializeEvent(serialized);
  const eventHash = ref.hash as Hash;
  if (await log.events.listEvents(publicKey).then((h) => h.includes(eventHash))) {
    return 'duplicate';
  }
  await log.events.storeEvent(publicKey, event);
  return 'stored';
}
