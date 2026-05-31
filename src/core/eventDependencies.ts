import type { Hash } from 'nearbytes-crypto';
import type { SerializedEvent } from 'nearbytes-crypto';
import type { Log } from 'nearbytes-log';
import { publicKeyFromHex, publicKeyToHex, serializeEvent } from 'nearbytes-log';
import type { ObjectRef } from './types.js';

/**
 * Declared `blockRefs` on a stored FILES event mix prior event hashes (causal
 * head) with content block hashes. After accepting an event out-of-order, ask
 * the peer for anything we still lack so replay can link the chain.
 */
export async function missingInboundEventDependencies(
  log: Log,
  channelHex: string,
  bytes: Uint8Array,
): Promise<ObjectRef[]> {
  let blockRefs: string[];
  try {
    const serialized = JSON.parse(new TextDecoder().decode(bytes)) as SerializedEvent;
    blockRefs = serialized.envelope.blockRefs.map((h) => String(h).toLowerCase());
  } catch {
    return [];
  }
  if (blockRefs.length === 0) {
    return [];
  }

  const pk = publicKeyFromHex(channelHex);
  if (pk === null) {
    return [];
  }

  const knownEvents = new Set(
    (await log.events.listEvents(pk)).map((h) => h.toLowerCase()),
  );
  const missing: ObjectRef[] = [];
  const channel = channelHex.toLowerCase();

  const headRef = blockRefs[0]!;
  if (blockRefs.length === 1) {
    if (!(await log.blocks.has(headRef as Hash))) {
      missing.push({ kind: 'block', hash: headRef });
    }
  } else if (!knownEvents.has(headRef)) {
    missing.push({ kind: 'event', channel, hash: headRef });
  }

  for (const hash of blockRefs.slice(1)) {
    if (await log.blocks.has(hash as Hash)) {
      continue;
    }
    if (knownEvents.has(hash)) {
      continue;
    }
    missing.push({ kind: 'block', hash });
  }

  return missing;
}

/** Scan local channel events for orphaned heads and return parent/block wants. */
export async function repairMissingEventDependencyWants(log: Log): Promise<ObjectRef[]> {
  const channels = await log.events.listChannels();
  const wants: ObjectRef[] = [];
  for (const pk of channels) {
    const channelHex = publicKeyToHex(pk).toLowerCase();
    const hashes = await log.events.listEvents(pk);
    for (const eventHash of hashes) {
      try {
        const signed = await log.events.retrieveEvent(pk, eventHash as Hash);
        const bytes = new TextEncoder().encode(JSON.stringify(serializeEvent(signed)));
        wants.push(...(await missingInboundEventDependencies(log, channelHex, bytes)));
      } catch {
        continue;
      }
    }
  }
  return dedupeObjectRefs(wants);
}

function dedupeObjectRefs(refs: readonly ObjectRef[]): ObjectRef[] {
  const seen = new Set<string>();
  const out: ObjectRef[] = [];
  for (const ref of refs) {
    const key =
      ref.kind === 'block'
        ? `block:${ref.hash.toLowerCase()}`
        : `event:${ref.channel.toLowerCase()}:${ref.hash.toLowerCase()}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    out.push(ref);
  }
  return out;
}
