import { existsSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import type { Hash } from 'nearbytes-crypto';
import { blockPath, type Log, type ReceptionObjectRef } from 'nearbytes-log';
import { publicKeyFromHex, serializeEvent } from 'nearbytes-log';
import { acceptData } from './acceptData.js';
import {
  missingInboundEventDependencies,
  repairMissingEventDependencyWants,
} from './eventDependencies.js';
import {
  BLOCK_STREAM_WRITE_SLICE_BYTES,
  createWireDecoder,
  encodeBlockStreamBegin,
  encodeFrame,
} from './codec.js';
import type { ObjectRef, Subject, SyncMessage } from './types.js';
import { appendBenchMarker } from '../benchMarker.js';
import { logSyncError } from '../logSyncError.js';
import { syncDebugLine } from '../syncDebugLog.js';
import { registerLocalHaveAnnouncer, type LocalHaveAnnouncer } from './sessionRegistry.js';
import { inflightBlockRegistry, outboundBlockStreamCounter } from './inflightBlocks.js';
import {
  NOOP_PEER_SESSION_EVENT_EMITTER,
  type PeerSessionEventEmitter,
} from './syncEvents.js';

/** In-memory RX buffer limit per block stream (512 MiB). */
const MAX_BLOCK_STREAM_BUFFER_BYTES = 512 * 1024 * 1024;

export interface DuplexPeer {
  write(chunk: Uint8Array): void;
  /** When set (TCP), honors kernel backpressure instead of buffering unbounded in userspace. */
  writeAsync?(chunk: Uint8Array): Promise<void>;
  /** @returns Unsubscribe (required after handshake so block streams are not decoded twice). */
  onData(handler: (chunk: Uint8Array) => void): () => void;
  /** Node TCP: route raw block-stream bytes without framing (set null to restore). */
  setBulkInbound?(handler: ((chunk: Uint8Array) => void) | null): void;
  /** Node TCP: takes over socket `data` until block stream completes (fast path). */
  setExclusiveInbound?(handler: ((chunk: Uint8Array) => void) | null): void;
  pauseInbound?(): void;
  resumeInbound?(): void;
  close(): void;
  onClose?(handler: () => void): void;
}

/** Wall-clock phase markers (ms since epoch) for a disk block stream. */
export interface DiskBlockStreamPhases {
  /** First byte received from the wire (start of receive). */
  readonly firstByteAt: number | null;
  /** Last byte received from the wire (pure-recv end). */
  readonly lastByteAt: number | null;
  /** All async fs.write() callbacks completed (disk drain end). */
  readonly diskDrainDoneAt: number | null;
  /** Hash digest finalized. */
  readonly hashDoneAt: number;
  /** Tmp→final rename completed. */
  readonly renameDoneAt: number;
}

export interface DiskBlockStreamFinishResult {
  readonly outcome: 'stored' | 'invalid';
  readonly phases: DiskBlockStreamPhases;
}

/** Node-only: stream inbound blocks to disk (see `createNodeDiskBlockStreamFactory`). */
export interface DiskBlockStreamSink {
  readonly total: number;
  readonly received: number;
  ingest(chunk: Uint8Array): void;
  finish(): Promise<DiskBlockStreamFinishResult>;
}

export interface DiskBlockStreamSinkFactory {
  create(hash: string, total: number): DiskBlockStreamSink;
}

export interface AttachPeerSessionOptions {
  /** Filesystem log root (`…/data`) for zero-copy block pump from disk. */
  readonly blockStorageRoot?: string;
  /** When set with {@link blockStorageRoot}, inbound blocks stream to disk instead of RAM. */
  readonly diskBlockStream?: DiskBlockStreamSinkFactory;
  /** Stored cursor into this remote endpoint's reception stream. */
  readonly initialFetchCursor?: string;
  /** Called after a remote `have` page has been processed and can be checkpointed. */
  readonly onFetchCursorCheckpoint?: (cursor: string) => void | Promise<void>;
  /** Already-decoded control frames captured during the hello handshake. */
  readonly initialMessages?: readonly SyncMessage[];
  /**
   * Observability sink for wire-level activity on this session. The
   * caller (typically `FriendSessionRegistry.attach`) bakes the remote
   * peer's identity into the emitter so the peer-loop can stay
   * context-free. Defaults to a no-op so tests and in-memory harnesses
   * can omit it.
   */
  readonly events?: PeerSessionEventEmitter;
}

function isMissingLocalObjectError(err: unknown): boolean {
  if (err instanceof Error && 'code' in err) {
    return (err as NodeJS.ErrnoException).code === 'ENOENT';
  }
  if (err instanceof Error) {
    return err.message.includes('File not found') || err.message.includes('no such file');
  }
  return false;
}

async function toWireRef(log: Log, ref: ReceptionObjectRef): Promise<ObjectRef | null> {
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
  } catch (err) {
    if (isMissingLocalObjectError(err)) {
      return null;
    }
    logSyncError('toWireRef', err);
    return { kind: 'event', channel: ref.channel, hash: ref.hash };
  }
}

async function readLocalBytes(log: Log, ref: ObjectRef): Promise<Uint8Array | null> {
  try {
    if (ref.kind === 'block') {
      return await log.blocks.retrieve(ref.hash as Hash, { verifyIntegrity: false });
    }
    const pk = publicKeyFromHex(ref.channel);
    if (!pk) {
      return null;
    }
    const event = await log.events.retrieveEvent(pk, ref.hash as Hash);
    return new TextEncoder().encode(JSON.stringify(serializeEvent(event)));
  } catch (err) {
    logSyncError(`readLocalBytes:${ref.kind}:${ref.kind === 'block' ? ref.hash.slice(0, 16) : ref.hash.slice(0, 16)}`, err);
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

/** Reception journal may list objects evicted from blocks/ or channels/ — do not advertise them. */
async function receptionRefLocallyAvailable(log: Log, ref: ReceptionObjectRef): Promise<boolean> {
  if (ref.kind === 'block') {
    return log.blocks.has(ref.hash as Hash);
  }
  const pk = publicKeyFromHex(ref.channel);
  if (!pk) {
    return false;
  }
  try {
    await log.events.retrieveEvent(pk, ref.hash as Hash);
    return true;
  } catch {
    return false;
  }
}

async function readLocalReceptionMaxSeq(storageRoot: string | undefined): Promise<number> {
  if (storageRoot === undefined) {
    return 0;
  }
  try {
    const raw = await readFile(join(storageRoot, 'sync', 'reception.jsonl'), 'utf8');
    const lines = raw.trim().split('\n').filter((line) => line.length > 0);
    if (lines.length === 0) {
      return 0;
    }
    const parsed = JSON.parse(lines[lines.length - 1]!) as { seq?: unknown };
    return typeof parsed.seq === 'number' ? parsed.seq : Number.parseInt(String(parsed.seq), 10) || 0;
  } catch {
    return 0;
  }
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

async function pumpBlockStream(peer: DuplexPeer, bytes: Uint8Array): Promise<void> {
  const total = bytes.byteLength;
  const slice = BLOCK_STREAM_WRITE_SLICE_BYTES;
  for (let offset = 0; offset < total; offset += slice) {
    const chunk = bytes.subarray(offset, Math.min(offset + slice, total));
    if (peer.writeAsync) {
      await peer.writeAsync(chunk);
    } else {
      peer.write(chunk);
    }
  }
}

function sendBlockStream(
  log: Log,
  runOutbound: (fn: () => void | Promise<void>) => void,
  peer: DuplexPeer,
  storageRoot: string | undefined,
  ref: Extract<ObjectRef, { kind: 'block' }>,
  bytes: Uint8Array | null,
  totalBytes: number,
  sessionEvents: PeerSessionEventEmitter,
): void {
  const outboundCounter = outboundBlockStreamCounter(log);
  outboundCounter.begin();
  runOutbound(async () => {
    try {
      let pumped: { bytes: number; pumpBeginAt: number; pumpEndAt: number } | null = null;
      if (storageRoot) {
        const { isTcpDuplexPeer } = await import('../node/netDuplex.js');
        if (isTcpDuplexPeer(peer)) {
          const { pumpBlockFileOverSocket } = await import('../node/tcpBulk.js');
          pumped = await pumpBlockFileOverSocket(peer.tcpSocket, storageRoot, ref.hash);
        } else {
          const { pumpBlockFileFromStorage } = await import('../node/blockPump.js');
          pumped = await pumpBlockFileFromStorage(storageRoot, ref.hash, peer);
        }
      } else {
        if (!bytes) {
          throw new Error('sendBlockStream requires bytes when blockStorageRoot is unset');
        }
        const pumpBeginAt = Date.now();
        const begin = encodeBlockStreamBegin(ref.hash, totalBytes);
        if (peer.writeAsync) {
          await peer.writeAsync(begin);
        } else {
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
        sessionEvents.blockSent(ref.hash, pumped.bytes);
      }
    } catch (err) {
      if (isMissingLocalObjectError(err)) {
        return;
      }
      logSyncError(`sendBlockStream:${ref.hash.slice(0, 16)}`, err);
    } finally {
      outboundCounter.end();
    }
  });
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
export function attachPeerSession(
  log: Log,
  subject: Subject,
  peer: DuplexPeer,
  onPeerClose?: () => void,
  options: AttachPeerSessionOptions = {},
): () => void {
  const storageRoot = options.blockStorageRoot;
  const diskBlockStream = options.diskBlockStream;
  const sessionEvents: PeerSessionEventEmitter =
    options.events ?? NOOP_PEER_SESSION_EVENT_EMITTER;
  const checkpointFetchCursor = async (cursor: string): Promise<void> => {
    await options.onFetchCursorCheckpoint?.(cursor);
  };
  let inboundWire = Promise.resolve();
  let outboundWire = Promise.resolve();
  const runInbound = (fn: () => void | Promise<void>): void => {
    inboundWire = inboundWire
      .then(async () => {
        await fn();
      })
      .catch((err) => {
        logSyncError('peerLoop inbound', err);
      });
  };
  const runOutbound = (fn: () => void | Promise<void>): void => {
    outboundWire = outboundWire
      .then(async () => {
        await fn();
      })
      .catch((err) => {
        logSyncError('peerLoop outbound', err);
      });
  };

  const send = (message: SyncMessage): void => {
    runOutbound(() => {
      peer.write(encodeFrame(message));
    });
  };

  /**
   * Per-Log inflight block tracker (SYNC: at-most-one `want(H)` across sessions).
   *
   * `claimedHashes` mirrors what THIS session currently holds in the registry,
   * so a session-close can release exactly its own claims without disturbing
   * slots owned by sibling sessions. Slots are released either when the
   * incoming stream for that hash completes (any outcome) or on session stop.
   */
  const inflight = inflightBlockRegistry(log);
  const claimedHashes = new Set<string>();
  const claimBlockWant = (hash: string): boolean => {
    const key = hash.toLowerCase();
    if (claimedHashes.has(key)) return true;
    if (!inflight.claim(key)) return false;
    claimedHashes.add(key);
    return true;
  };
  const releaseBlockClaim = (hash: string): void => {
    const key = hash.toLowerCase();
    if (claimedHashes.delete(key)) {
      inflight.release(key);
    }
  };

  const sendHave = async (
    refs: readonly ReceptionObjectRef[],
    more = false,
    nextCursor?: string,
  ): Promise<void> => {
    if (refs.length === 0 && !more) {
      send({ type: 'have', subject, objects: [], more: false });
      return;
    }
    const objects: ObjectRef[] = [];
    let skippedUnavailable = 0;
    for (const ref of refs) {
      if (!(await receptionRefLocallyAvailable(log, ref))) {
        skippedUnavailable += 1;
        continue;
      }
      const wireRef = await toWireRef(log, ref);
      if (wireRef !== null) {
        objects.push(wireRef);
      }
    }
    if (skippedUnavailable > 0) {
      syncDebugLine(
        'wire',
        `have filter skipped ${skippedUnavailable}/${refs.length} reception ref(s) not on disk`,
      );
    }
    if (objects.length === 0 && refs.length > 0) {
      syncDebugLine(
        'wire',
        `have → page had ${refs.length} journal ref(s) but none are locally available`,
      );
      if (more && nextCursor !== undefined) {
        send({ type: 'have', subject, objects: [], more: true, nextCursor });
      }
      return;
    }
    const effectiveNext =
      objects.length === 0 && refs.length > 0 && !more ? undefined : nextCursor;
    send({
      type: 'have',
      subject,
      objects,
      more,
      ...(effectiveNext !== undefined ? { nextCursor: effectiveNext } : {}),
    });
    syncDebugLine(
      'wire',
      `have → objects=${objects.length} more=${more}` +
        (effectiveNext !== undefined ? ` next=${effectiveNext}` : ''),
    );
  };

  const requestGlobalDelta = (cursor?: string): void => {
    syncDebugLine('wire', `delta → global cursor=${cursor ?? '(start)'} limit=256`);
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
      syncDebugLine('wire', `want → blocks=${blocks.length}`);
      send({ type: 'want', objects: blocks });
    }
    if (events.length > 0) {
      syncDebugLine('wire', `want → events=${events.length}`);
      send({ type: 'want', objects: events });
    }
  };

  /**
   * After anti-entropy quiesces, pull causal parents for any events we already
   * hold but cannot chain (global reception pagination may never re-offer them).
   * Chains on the inbound serial queue so repair never races a half-handled frame.
   */
  let orphanRepairChain = Promise.resolve();
  /** Last durable fetch cursor for this remote instance (SYNC-19). */
  let lastFetchedCursor = options.initialFetchCursor;
  /** Page cursor to checkpoint only after outstanding `want`/`data` work finishes. */
  let pendingPageCursor: string | undefined;
  /** One-shot rewind when a persisted cursor yields empty pages but we may still be behind. */
  let emptyCatchupRewound = false;

  const flushPendingPageCursor = async (): Promise<void> => {
    if (pendingPageCursor === undefined) {
      return;
    }
    const cursor = pendingPageCursor;
    pendingPageCursor = undefined;
    await checkpointFetchCursor(cursor);
    lastFetchedCursor = cursor;
  };

  const scheduleOrphanRepair = (): void => {
    orphanRepairChain = orphanRepairChain
      .then(async () => {
        await inboundWire;
        const repair = await repairMissingEventDependencyWants(log);
        if (repair.length > 0) {
          const wants = await missingRefs(log, repair);
          if (wants.length > 0) {
            sendWants(wants);
          }
        }
        await flushPendingPageCursor();
      })
      .catch((err) => {
        logSyncError('orphanRepair', err);
      });
  };

  /**
   * When a peer attaches, advertise the tail of our reception journal so a
   * late joiner (or a peer with a stale fetch cursor) still sees recent writes
   * made while it was offline — mutual `delta` alone is not always enough.
   *
   * Runs immediately (not behind `inboundWire`) so a joiner's first `subscribe`
   * cannot win the race and advance a false fetch cursor before we announce.
   */
  const pushProactiveReceptionTail = (): void => {
    void (async () => {
      const maxSeq = await readLocalReceptionMaxSeq(storageRoot);
      if (maxSeq <= 0) {
        return;
      }
      const window = 256;
      const start = Math.max(-1, maxSeq - window);
      const out = await log.reception.listAfter(String(start), window);
      if (out.refs.length > 0) {
        await sendHave(out.refs, out.more, out.next);
      }
    })().catch((err) => {
      logSyncError('proactiveReceptionTail', err);
    });
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
      syncDebugLine(
        'wire',
        `have ← objects=${msg.objects.length} more=${msg.more}` +
          (msg.nextCursor !== undefined ? ` next=${msg.nextCursor}` : ''),
      );
      const candidates = await missingRefs(log, msg.objects);
      const wants: ObjectRef[] = [];
      for (const ref of candidates) {
        if (ref.kind === 'block') {
          if (claimBlockWant(ref.hash)) {
            wants.push(ref);
          }
        } else {
          wants.push(ref);
        }
      }
      if (wants.length > 0) {
        sendWants(wants);
        if (msg.nextCursor !== undefined) {
          pendingPageCursor = msg.nextCursor;
        }
        if (msg.more && msg.nextCursor) {
          orphanRepairChain = orphanRepairChain
            .then(async () => {
              await inboundWire;
              await flushPendingPageCursor();
              requestGlobalDelta(msg.nextCursor);
            })
            .catch((err) => {
              logSyncError('havePageContinue', err);
            });
        } else {
          scheduleOrphanRepair();
        }
        return;
      }
      if (msg.more && msg.nextCursor) {
        requestGlobalDelta(msg.nextCursor);
      } else {
        if (msg.objects.length === 0) {
          const storedCursor =
            options.initialFetchCursor !== undefined
              ? Number.parseInt(options.initialFetchCursor, 10)
              : undefined;
          const localMaxSeq = await readLocalReceptionMaxSeq(storageRoot);
          if (
            storedCursor !== undefined &&
            Number.isFinite(storedCursor) &&
            storedCursor > localMaxSeq
          ) {
            lastFetchedCursor = undefined;
            pendingPageCursor = undefined;
            syncDebugLine('wire', 'have ← empty page — stale fetch cursor, restart delta from start');
            requestGlobalDelta(undefined);
          } else if (
            !emptyCatchupRewound &&
            options.initialFetchCursor !== undefined &&
            options.initialFetchCursor !== ''
          ) {
            emptyCatchupRewound = true;
            lastFetchedCursor = undefined;
            pendingPageCursor = undefined;
            syncDebugLine('wire', 'have ← empty terminal page — rewind fetch cursor once');
            requestGlobalDelta(undefined);
          }
        } else if (msg.nextCursor) {
          await checkpointFetchCursor(msg.nextCursor);
          lastFetchedCursor = msg.nextCursor;
        }
        scheduleOrphanRepair();
      }
      return;
    }

    if (msg.type === 'want') {
      const { blocks, events } = partitionWantRefs(msg.objects);
      let blockServed = 0;
      let blockMissingLocal = 0;
      for (const ref of blocks) {
        if (ref.kind !== 'block') {
          continue;
        }
        if (!(await log.blocks.has(ref.hash as Hash))) {
          blockMissingLocal += 1;
          continue;
        }
        if (storageRoot) {
          blockServed += 1;
          sendBlockStream(log, runOutbound, peer, storageRoot, ref, null, 0, sessionEvents);
          continue;
        }
        const bytes = await readLocalBytes(log, ref);
        if (bytes) {
          blockServed += 1;
          sendBlockStream(log, runOutbound, peer, storageRoot, ref, bytes, bytes.byteLength, sessionEvents);
        }
      }
      syncDebugLine(
        'wire',
        `want ← blocks=${blocks.length} events=${events.length}` +
          (blocks.length > 0
            ? ` served=${blockServed} missing-local=${blockMissingLocal}`
            : ''),
      );
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
        logSyncError(
          `acceptData invalid event:${msg.object.channel.slice(0, 16)}`,
          new Error('acceptData returned invalid'),
        );
      }
      if (result === 'stored') {
        await appendBenchMarker(log, 'inbound-stored', {
          kind: 'event',
          channel: msg.object.channel.slice(0, 16),
          bytes: msg.bytes.byteLength,
        });
        sessionEvents.eventReceived(
          msg.object.channel,
          msg.object.hash,
          msg.bytes.byteLength,
        );
        const deps = await missingInboundEventDependencies(
          log,
          msg.object.channel,
          msg.bytes,
        );
        if (deps.length > 0) {
          const wants = await missingRefs(log, deps);
          if (wants.length > 0) {
            sendWants(wants);
          }
        }
        scheduleOrphanRepair();
      }
    }
  };

  const onBlockStream = async (hash: string, bytes: Uint8Array): Promise<void> => {
    const object = { kind: 'block' as const, hash };
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
      sessionEvents.blockReceived(hash, bytes.byteLength);
      scheduleOrphanRepair();
    }
  };

  type IncomingBlockStream =
    | {
        readonly mode: 'memory';
        readonly hash: string;
        readonly total: number;
        readonly hasher: ReturnType<typeof createHash>;
        readonly buffer: Uint8Array;
        received: number;
      }
    | {
        readonly mode: 'disk';
        readonly hash: string;
        readonly total: number;
        readonly sink: DiskBlockStreamSink;
      }
    | {
        readonly mode: 'discard';
        readonly hash: string;
        readonly total: number;
        received: number;
      };
  let incoming: IncomingBlockStream | null = null;

  const blockStreamComplete = (): boolean => {
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

  const clearBulkInbound = (): void => {
    peer.setBulkInbound?.(null);
    peer.setExclusiveInbound?.(null);
    decodeControl.endBlockStream();
  };

  const markBlockStored = async (hash: string, bytes: number): Promise<void> => {
    await log.reception.appendReception({ kind: 'block', hash: hash as Hash });
    await appendBenchMarker(log, 'inbound-stored', {
      kind: 'block',
      hash: hash.slice(0, 16),
      bytes,
    });
    sessionEvents.blockReceived(hash, bytes);
    scheduleOrphanRepair();
  };

  const finishIncomingStream = async (): Promise<void> => {
    const stream = incoming;
    if (!stream) {
      return;
    }
    incoming = null;
    clearBulkInbound();
    releaseBlockClaim(stream.hash);
    if (stream.mode === 'discard') {
      return;
    }
    if (stream.mode === 'disk') {
      const result = await stream.sink.finish();
      if (result.outcome === 'invalid') {
        logSyncError(
          `block stream hash mismatch ${stream.hash.slice(0, 16)}`,
          new Error('disk block stream verify failed'),
        );
        return;
      }
      await markBlockStored(stream.hash, stream.total);
      const phases = result.phases;
      const fields: Record<string, string | number | boolean> = {
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
      await appendBenchMarker(log, 'bulk-recv-phases', fields).catch((err) =>
        logSyncError('bulk-recv-phases', err),
      );
      await appendBenchMarker(log, 'block-stream-end', {
        hash: stream.hash.slice(0, 16),
        bytes: stream.total,
      }).catch((err) => logSyncError('block-stream-end', err));
      return;
    }
    const digest = stream.hasher.digest('hex');
    if (digest !== stream.hash.toLowerCase()) {
      logSyncError(
        `block stream hash mismatch ${stream.hash.slice(0, 16)}`,
        new Error(`expected ${stream.hash.slice(0, 16)} got ${digest.slice(0, 16)}`),
      );
      return;
    }
    await onBlockStream(stream.hash, stream.buffer);
  };

  const ingestStreamBytes = (chunk: Uint8Array): void => {
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
        void finishIncomingStream().catch((err) => logSyncError('finishIncomingStream', err));
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

  const beginIncomingBlockStream = (hash: string, total: number): void => {
    if (incoming !== null) {
      throw new Error('block stream already active');
    }
    if (storageRoot && diskBlockStream) {
      const abs = join(storageRoot, blockPath(hash as Hash));
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
      const directDiskIngest = (chunk: Uint8Array): void => {
        const remaining = total - sink.received;
        const chunkLen = chunk.byteLength;
        const take = remaining < chunkLen ? remaining : chunkLen;
        sink.ingest(take === chunkLen ? chunk : chunk.subarray(0, take));
        if (sink.received >= total) {
          void finishIncomingStream().catch((err) =>
            logSyncError('finishIncomingStream', err),
          );
          if (take < chunkLen) {
            decodeControl(chunk.subarray(take));
          }
        }
      };
      if (peer.setExclusiveInbound) {
        peer.setExclusiveInbound(directDiskIngest);
      } else {
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

  const decodeControl: import('./codec.js').WireDecoder = createWireDecoder({
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
  for (const message of options.initialMessages ?? []) {
    runInbound(() => onMessage(message));
  }
  peer.resumeInbound?.();

  pushProactiveReceptionTail();

  send({
    type: 'subscribe',
    delta: {
      type: 'delta',
      subject,
      mode: 'global',
      limit: 256,
      ...(options.initialFetchCursor !== undefined
        ? { cursor: options.initialFetchCursor }
        : {}),
    },
  });
  requestGlobalDelta(options.initialFetchCursor);
  scheduleOrphanRepair();

  const stop = (): void => {
    unregister();
    for (const key of claimedHashes) {
      inflight.release(key);
    }
    claimedHashes.clear();
    onPeerClose?.();
  };
  if (peer.onClose) {
    peer.onClose(stop);
  }
  return stop;
}
