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
import { blockReadable } from './blockReadable.js';
import {
  listLocalReceptionForConnect,
  listLocalReceptionPage,
  readLocalReceptionMaxSeq,
} from './receptionSync.js';
import { buildResumeDelta, buildResumeSubscribe } from './sessionAttach.js';
import { RECEPTION_RESUME_PAGE } from './syncConstants.js';
import {
  BLOCK_STREAM_WRITE_SLICE_BYTES,
  createWireDecoder,
  encodeBlockStreamBegin,
  encodeFrame,
} from './codec.js';
import type { ObjectRef, Subject, SyncMessage } from './types.js';
import { appendBenchMarker } from '../benchMarker.js';
import { logSyncError } from '../logSyncError.js';
import { resolveTraceEmit, type TraceEmit, type WireFrameInput } from '../syncDebugLog.js';

const defaultTrace = resolveTraceEmit();
import { syncTimelineMark } from '../syncTimeline.js';
import {
  broadcastLocalHave,
  registerLocalHaveAnnouncer,
  type LocalHaveAnnouncer,
} from './sessionRegistry.js';
import {
  claimOutboundServe,
  clearBlockSettling,
  inflightBlockRegistry,
  inflightBlockRegistryForStorage,
  markBlockSettling,
  outboundBlockStreamCounter,
  releaseOutboundServe,
} from './inflightBlocks.js';
import {
  SessionStallGuard,
  type StallReason,
} from './connectionHealth.js';
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
  /** Stable key for `--debug timeline` (`profile|instance` or discovery label). */
  readonly timelineKey?: string;
  /** Called before forced close when an in-flight stall or rotation fires. */
  readonly onSessionStall?: (reason: StallReason) => void;
  /** Epoch ms when the association became live (for session rotation). */
  readonly sessionConnectedAt?: number;
  /** Trace emitter threaded by reference from `StartOptions.trace` (TRACE-04). Defaults to the legacy global sink. */
  readonly trace?: TraceEmit;
  /** Association identity for trace frames (TRACE-12/13/16) — set by `FriendSessionRegistry.attach`. */
  readonly localProfile?: string;
  readonly remoteProfile?: string;
  readonly remoteInstance?: string;
  readonly assoc?: string;
  readonly transport?: string;
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

/** Read `blockRefs` for a `have` advertisement without full event crypto validation. */
async function blockRefsForEventHave(
  log: Log,
  storageRoot: string | undefined,
  channel: string,
  eventHash: string,
): Promise<string[] | undefined> {
  if (storageRoot === undefined) {
    return undefined;
  }
  try {
    const raw = await readFile(join(storageRoot, 'channels', channel, `${eventHash}.bin`), 'utf8');
    const parsed = JSON.parse(raw) as { envelope?: { blockRefs?: unknown } };
    if (!Array.isArray(parsed.envelope?.blockRefs)) {
      return [];
    }
    const blockRefs: string[] = [];
    for (const h of parsed.envelope.blockRefs) {
      const hash = String(h);
      if (await blockReadable(log, storageRoot, hash as Hash)) {
        blockRefs.push(hash);
      }
    }
    return blockRefs;
  } catch {
    return undefined;
  }
}

async function toWireRef(
  log: Log,
  ref: ReceptionObjectRef,
  storageRoot?: string,
): Promise<ObjectRef | null> {
  if (ref.kind === 'block') {
    return { kind: 'block', hash: ref.hash };
  }
  const channel = ref.channel;
  const hash = ref.hash;
  const peeked = await blockRefsForEventHave(log, storageRoot, channel, hash);
  if (peeked !== undefined) {
    return {
      kind: 'event',
      channel,
      hash,
      ...(peeked.length > 0 ? { blockRefs: peeked } : {}),
    };
  }
  const pk = publicKeyFromHex(channel);
  if (!pk) {
    return { kind: 'event', channel, hash };
  }
  try {
    const event = await log.events.retrieveEvent(pk, hash as Hash);
    const blockRefs: string[] = [];
    for (const h of event.envelope.blockRefs) {
      const blockHash = h as Hash;
      if (await blockReadable(log, storageRoot, blockHash)) {
        blockRefs.push(blockHash as string);
      }
    }
    return {
      kind: 'event',
      channel,
      hash,
      ...(blockRefs.length > 0 ? { blockRefs } : {}),
    };
  } catch (err) {
    if (isMissingLocalObjectError(err)) {
      return null;
    }
    logSyncError('toWireRef', err);
    return { kind: 'event', channel, hash };
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

/** Opaque cursor key for in-flight resume pagination (empty string = journal start). */
function resumeCursorKey(cursor: string | undefined): string {
  return cursor !== undefined && cursor !== '' ? cursor : '';
}

async function hasObject(
  log: Log,
  ref: ObjectRef,
  storageRoot?: string,
): Promise<boolean> {
  if (ref.kind === 'block') {
    return blockReadable(log, storageRoot, ref.hash as Hash);
  }
  const pk = publicKeyFromHex(ref.channel);
  if (!pk) {
    return false;
  }
  return log.events.hasEvent(pk, ref.hash as Hash);
}

/** Reception journal may list objects evicted from blocks/ or channels/ — do not advertise them. */
async function receptionRefLocallyAvailable(
  log: Log,
  ref: ReceptionObjectRef,
  storageRoot?: string,
): Promise<boolean> {
  return hasObject(
    log,
    ref.kind === 'block'
      ? { kind: 'block', hash: ref.hash }
      : { kind: 'event', channel: ref.channel, hash: ref.hash },
    storageRoot,
  );
}

async function missingRefs(
  log: Log,
  refs: readonly ObjectRef[],
  storageRoot?: string,
): Promise<ObjectRef[]> {
  const missing: ObjectRef[] = [];
  for (const ref of refs) {
    if (!(await hasObject(log, ref, storageRoot))) {
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
  stallGuard: SessionStallGuard | null,
): void {
  if (storageRoot !== undefined && !claimOutboundServe(storageRoot, ref.hash)) {
    return;
  }
  const outboundCounter = outboundBlockStreamCounter(log);
  outboundCounter.begin();
  stallGuard?.armOutbound();
  runOutbound(async () => {
    try {
      let pumped: { bytes: number; pumpBeginAt: number; pumpEndAt: number } | null = null;
      if (storageRoot) {
        const { isTcpDuplexPeer } = await import('../node/netDuplex.js');
        const tcpBulkOn = process.env['NEARBYTES_OPT_TCP_BULK'] !== '0';
        if (tcpBulkOn && isTcpDuplexPeer(peer)) {
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
      if (storageRoot !== undefined && isMissingLocalObjectError(err)) {
        releaseOutboundServe(storageRoot, ref.hash);
      }
      if (isMissingLocalObjectError(err)) {
        return;
      }
      logSyncError(`sendBlockStream:${ref.hash.slice(0, 16)}`, err);
    } finally {
      stallGuard?.clearOutbound();
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
  const trace = options.trace ?? defaultTrace;
  // Association identity (TRACE-12/13/16) is the same for every frame this
  // session emits — attach it once here instead of repeating it at each
  // call site below.
  const emit = (
    frame: Omit<WireFrameInput, 'localProfile' | 'remoteProfile' | 'remoteInstance' | 'assoc' | 'transport'>,
  ): void => {
    trace({
      localProfile: options.localProfile,
      remoteProfile: options.remoteProfile,
      remoteInstance: options.remoteInstance,
      assoc: options.assoc,
      transport: options.transport,
      ...frame,
    });
  };
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

  const send = (message: SyncMessage, urgent = false): void => {
    const write = (): void => {
      peer.write(encodeFrame(message));
    };
    runOutbound(write);
  };

  let resumeWalkInFlight: string | undefined;
  let incomingStreamActive = false;

  /**
   * Per-Log inflight block tracker (SYNC: at-most-one `want(H)` across sessions).
   *
   * `claimedHashes` mirrors what THIS session currently holds in the registry,
   * so a session-close can release exactly its own claims without disturbing
   * slots owned by sibling sessions. Slots are released either when the
   * incoming stream for that hash completes (any outcome) or on session stop.
   */
  const inflight =
    storageRoot !== undefined
      ? inflightBlockRegistryForStorage(storageRoot)
      : inflightBlockRegistry(log);
  const claimedHashes = new Set<string>();

  let stallGuard: SessionStallGuard | null = null;

  const missingRefsForWant = async (refs: readonly ObjectRef[]): Promise<ObjectRef[]> => {
    const missing: ObjectRef[] = [];
    const batchSize = 32;

    const refPresent = async (ref: ObjectRef): Promise<boolean> => {
      if (ref.kind === 'block') {
        return blockReadable(log, storageRoot, ref.hash as Hash);
      }
      const pk = publicKeyFromHex(ref.channel);
      if (pk === null) {
        return false;
      }
      return log.events.hasEvent(pk, ref.hash as Hash);
    };

    for (let i = 0; i < refs.length; i += batchSize) {
      const batch = refs.slice(i, i + batchSize);
      const checked = await Promise.all(
        batch.map(async (ref) => ((await refPresent(ref)) ? null : ref)),
      );
      for (const ref of checked) {
        if (ref !== null) {
          missing.push(ref);
        }
      }
    }
    return missing;
  };

  const claimBlockWant = (hash: string): boolean => {
    const key = hash.toLowerCase();
    if (claimedHashes.has(key)) return false;
    if (!inflight.claim(key)) return false;
    claimedHashes.add(key);
    return true;
  };
  const releaseBlockClaim = (hash: string): void => {
    const key = hash.toLowerCase();
    if (claimedHashes.delete(key)) {
      inflight.release(key);
      stallGuard?.clearWant(key);
    }
  };

  const sendHave = async (
    refs: readonly ReceptionObjectRef[],
    more = false,
    nextCursor?: string,
    urgent = false,
    fromCursor?: string,
  ): Promise<void> => {
    if (process.env.NBF_PROP_TRACE === '1') {
      console.error(`[nearbytes-sync] sendHave in n=${refs.length} urgent=${urgent}`);
    }
    const fromCursorField =
      fromCursor !== undefined && fromCursor !== ''
        ? { fromCursor }
        : fromCursor === ''
          ? { fromCursor: '' as const }
          : {};
    if (refs.length === 0 && !more) {
      send({ type: 'have', subject, objects: [], more: false, ...fromCursorField }, urgent);
      return;
    }
    const objects: ObjectRef[] = [];
    let skippedUnavailable = 0;
    const batchSize = 32;
    for (let i = 0; i < refs.length; i += batchSize) {
      const batch = refs.slice(i, i + batchSize);
      const built = await Promise.all(
        batch.map(async (ref) => {
          if (!(await receptionRefLocallyAvailable(log, ref, storageRoot))) {
            return { available: false as const, wire: null };
          }
          const wireRef = await toWireRef(log, ref, storageRoot);
          return { available: true as const, wire: wireRef };
        }),
      );
      for (const entry of built) {
        if (!entry.available) {
          skippedUnavailable += 1;
          continue;
        }
        if (entry.wire !== null) {
          objects.push(entry.wire);
        }
      }
    }
    if (skippedUnavailable > 0) {
      emit({
        layer: 'anti-entropy', level: 'warn', dir: 'local', msg: 'have',
        corrId: fromCursor, corrKind: 'cursor',
        data: { reason: 'refs-unavailable', skipped: skippedUnavailable, total: refs.length },
      });
    }
    if (process.env.NBF_PROP_TRACE === '1') {
      console.error(
        `[nearbytes-sync] sendHave built objects=${objects.length} skipped=${skippedUnavailable}`,
      );
    }
    if (objects.length === 0 && refs.length > 0) {
      if (process.env.NBF_PROP_TRACE === '1') {
        console.error(
          `[nearbytes-sync] have → dropped all ${refs.length} ref(s) (skippedUnavailable=${skippedUnavailable})`,
        );
      }
      emit({
        layer: 'anti-entropy', level: 'warn', dir: 'out', msg: 'have',
        corrId: fromCursor, corrKind: 'cursor',
        data: { reason: 'all-refs-unavailable', total: refs.length },
      });
      if (more && nextCursor !== undefined) {
        send(
          { type: 'have', subject, objects: [], more: true, nextCursor, ...fromCursorField },
          urgent,
        );
      }
      return;
    }
    const effectiveNext =
      objects.length === 0 && refs.length > 0 && !more ? undefined : nextCursor;
    send(
      {
        type: 'have',
        subject,
        objects,
        more,
        ...fromCursorField,
        ...(effectiveNext !== undefined ? { nextCursor: effectiveNext } : {}),
      },
      urgent,
    );
    emit({
      layer: 'anti-entropy', level: 'debug', dir: 'out', msg: 'have',
      corrId: fromCursor, corrKind: 'cursor',
      data: {
        objects: objects.length,
        more,
        ...(effectiveNext !== undefined ? { nextCursor: effectiveNext } : {}),
        hashes: objects.map((o) => o.hash),
      },
    });
  };

  const requestGlobalDelta = (cursor?: string, urgent = false): void => {
    emit({
      layer: 'anti-entropy', level: 'debug', dir: 'out', msg: 'delta',
      corrId: cursor, corrKind: 'cursor', data: { cursor: cursor ?? 'start', mode: 'global' },
    });
    send(buildResumeDelta(subject, cursor), urgent);
  };

  let timelineHaveInLogged = false;
  let timelineWantOutLogged = false;

  const sendWants = (refs: readonly ObjectRef[]): void => {
    const { blocks, events } = partitionWantRefs(refs);
    const tl = options.timelineKey;
    if (tl !== undefined && !timelineWantOutLogged && (blocks.length > 0 || events.length > 0)) {
      timelineWantOutLogged = true;
      syncTimelineMark(tl, 'want→', `blocks=${blocks.length} events=${events.length}`);
    }
    if (blocks.length > 0) {
      emit({
        layer: 'anti-entropy', level: 'debug', dir: 'out', msg: 'want',
        data: { blocks: blocks.length, hashes: blocks.map((r) => r.hash) },
      });
      for (const ref of blocks) {
        if (ref.kind === 'block') {
          stallGuard?.armWant(ref.hash);
          // TRACE-20 (block layer): the want is now outstanding. Pairs with
          // want-satisfied / want-timeout on the same `hash` corrId, which is
          // what makes a never-answered want visible instead of merely absent.
          emit({
            layer: 'block', level: 'debug', dir: 'local', msg: 'want-armed',
            corrId: ref.hash, corrKind: 'hash', data: { hash: ref.hash },
          });
        }
      }
      send({ type: 'want', objects: blocks });
    }
    if (events.length > 0) {
      emit({
        layer: 'anti-entropy', level: 'debug', dir: 'out', msg: 'want',
        data: { events: events.length, hashes: events.map((r) => r.hash) },
      });
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
        const repair = await repairMissingEventDependencyWants(log, storageRoot);
        if (repair.length > 0) {
          const wants = await missingRefs(log, repair, storageRoot);
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
   * Local resume walk (SYNC-19–SYNC-21): we send `delta` pages; inbound `have`
   * answers only advance the walk when tagged with `fromCursor`. Unsolicited
   * tail `have` (SYNC-21c) and live pushes (SYNC-10) may still drive `want`.
   */
  let resumeWalkDone = false;
  let resumeWalkRewound = false;
  /** Dedupe attach `delta` + `subscribe` to one journal page per cursor. */
  let lastResumeRespondedKey: string | undefined;

  const requestResumeWalkPage = (cursor: string | undefined): void => {
    if (resumeWalkDone) {
      return;
    }
    const key = resumeCursorKey(cursor);
    if (resumeWalkInFlight === key) {
      return;
    }
    resumeWalkInFlight = key;
    stallGuard?.armResume();
    const tl = options.timelineKey;
    if (tl !== undefined) {
      syncTimelineMark(tl, 'page→', `cursor=${cursor ?? 'start'}`);
    }
    requestGlobalDelta(cursor, true);
  };

  const finishResumeWalk = (): void => {
    resumeWalkDone = true;
    resumeWalkInFlight = undefined;
    stallGuard?.clearResume();
    scheduleOrphanRepair();
  };

  const respondToGlobalResume = async (
    cursor: string | undefined,
    limit: number,
  ): Promise<void> => {
    const key = resumeCursorKey(cursor);
    if (lastResumeRespondedKey === key) {
      emit({
        layer: 'anti-entropy', level: 'trace', dir: 'in', msg: 'delta',
        corrId: key, corrKind: 'cursor', data: { reason: 'deduped', cursor: cursor ?? 'start' },
      });
      return;
    }
    lastResumeRespondedKey = key;
    const out = await listLocalReceptionPage(log, storageRoot, cursor, limit);
    await sendHave(out.refs, out.more, out.next, true, key);
  };

  const onResumeWalkHave = (msg: Extract<SyncMessage, { type: 'have' }>): void => {
    if (msg.fromCursor === undefined) {
      return;
    }
    const pageKey = resumeCursorKey(msg.fromCursor === '' ? undefined : msg.fromCursor);
    if (resumeWalkInFlight !== undefined && pageKey !== resumeWalkInFlight) {
      emit({
        layer: 'anti-entropy', level: 'trace', dir: 'in', msg: 'have',
        corrId: msg.fromCursor, corrKind: 'cursor',
        data: { reason: 'stale-resume-page', inFlight: resumeWalkInFlight },
      });
      return;
    }
    resumeWalkInFlight = undefined;
    stallGuard?.clearResume();

    if (msg.objects.length === 0 && !msg.more) {
      void (async () => {
        if (msg.nextCursor !== undefined) {
          await checkpointFetchCursor(msg.nextCursor);
          lastFetchedCursor = msg.nextCursor;
        }
        const initial = options.initialFetchCursor;
        const initialNum =
          initial !== undefined && initial !== '' ? Number.parseInt(initial, 10) : Number.NaN;
        const localMax = await readLocalReceptionMaxSeq(storageRoot);
        if (!resumeWalkRewound && !Number.isNaN(initialNum) && initialNum > localMax) {
          resumeWalkRewound = true;
          emit({
            layer: 'anti-entropy', level: 'info', dir: 'in', msg: 'resume',
            data: { reason: 'stale-cursor-rewind', initialCursor: initial },
          });
          requestResumeWalkPage(undefined);
          return;
        }
        finishResumeWalk();
      })().catch((err) => logSyncError('resumeWalkEmpty', err));
      return;
    }

    if (msg.nextCursor !== undefined) {
      pendingPageCursor = msg.nextCursor;
    }

    if (!msg.more) {
      finishResumeWalk();
      return;
    }

    if (msg.nextCursor !== undefined) {
      requestResumeWalkPage(msg.nextCursor);
    }
  };

  const processHaveWants = async (msg: Extract<SyncMessage, { type: 'have' }>): Promise<void> => {
    const candidates = await missingRefsForWant(msg.objects);
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
      if (msg.fromCursor !== undefined && msg.nextCursor !== undefined) {
        pendingPageCursor = msg.nextCursor;
      }
      sendWants(wants);
      return;
    }
    if (
      msg.fromCursor !== undefined &&
      msg.nextCursor !== undefined &&
      pendingPageCursor === msg.nextCursor
    ) {
      await flushPendingPageCursor();
    }
  };

  /** On attach: local resume walk + mutual subscribe/delta + tail announce (SYNC-15, SYNC-21c). */
  const runAttachSync = (): void => {
    const resumeCursor = options.initialFetchCursor;
    const tl = options.timelineKey;
    emit({
      layer: 'anti-entropy', level: 'info', dir: 'out', msg: 'attach',
      corrId: resumeCursor, corrKind: 'cursor', data: { phase: 'resume', cursor: resumeCursor ?? 'start' },
    });
    if (tl !== undefined) {
      syncTimelineMark(tl, 'resume-sent', `cursor=${resumeCursor ?? 'start'}`);
    }
    requestResumeWalkPage(resumeCursor);
    emit({
      layer: 'anti-entropy', level: 'info', dir: 'out', msg: 'subscribe',
      corrId: resumeCursor, corrKind: 'cursor', data: { cursor: resumeCursor ?? 'start' },
    });
    send(buildResumeSubscribe(subject, resumeCursor), true);
    void (async () => {
      const out = await listLocalReceptionForConnect(log, storageRoot);
      if (out.refs.length > 0) {
        emit({
          layer: 'anti-entropy', level: 'info', dir: 'out', msg: 'attach',
          data: { phase: 'announce', objects: out.refs.length },
        });
        if (tl !== undefined) {
          syncTimelineMark(tl, 'announce-sent', `objects=${out.refs.length}`);
        }
        await sendHave(out.refs, out.more, out.next, true);
      }
    })().catch((err) => {
      logSyncError('attachAnnounce', err);
    });
  };

  const announcer: LocalHaveAnnouncer = {
    pushLocalHave(refs) {
      void sendHave(refs, false, undefined, true).catch((err) => {
        logSyncError('pushLocalHave', err);
      });
    },
  };
  const unregister = registerLocalHaveAnnouncer(announcer);

  const onMessage = async (msg: SyncMessage): Promise<void> => {
    if (msg.type === 'hello') {
      emit({
        layer: 'handshake', level: 'trace', dir: 'in', msg: 'hello',
        data: { reason: 'stray-post-handshake', suppressed: true },
      });
      return;
    }

    if (msg.type === 'delta' && msg.mode === 'global') {
      await respondToGlobalResume(msg.cursor, msg.limit ?? RECEPTION_RESUME_PAGE);
      return;
    }

    if (msg.type === 'subscribe' && msg.delta.mode === 'global') {
      emit({
        layer: 'anti-entropy', level: 'info', dir: 'in', msg: 'subscribe',
        corrId: msg.delta.cursor, corrKind: 'cursor', data: { cursor: msg.delta.cursor ?? 'start' },
      });
      await respondToGlobalResume(msg.delta.cursor, msg.delta.limit ?? RECEPTION_RESUME_PAGE);
      return;
    }

    if (msg.type === 'have') {
      const tl = options.timelineKey;
      const resumePage = msg.fromCursor !== undefined;
      if (tl !== undefined && !timelineHaveInLogged) {
        timelineHaveInLogged = true;
        syncTimelineMark(
          tl,
          resumePage ? 'have←' : 'have← push',
          `objects=${msg.objects.length} more=${msg.more}` +
            (resumePage ? ` from=${msg.fromCursor === '' ? 'start' : msg.fromCursor}` : ''),
        );
      }
      emit({
        layer: 'anti-entropy', level: 'debug', dir: 'in', msg: 'have',
        corrId: resumePage ? msg.fromCursor : undefined, corrKind: 'cursor',
        data: {
          objects: msg.objects.length,
          more: msg.more,
          ...(msg.nextCursor !== undefined ? { nextCursor: msg.nextCursor } : {}),
          push: !resumePage,
          hashes: msg.objects.map((o) => o.hash),
        },
      });
      onResumeWalkHave(msg);
      await processHaveWants(msg);
      return;
    }

    if (msg.type === 'want') {
      const wantBlocks = msg.objects.filter((o) => o.kind === 'block');
      const wantEvents = msg.objects.filter((o) => o.kind === 'event');
      emit({
        layer: 'anti-entropy', level: 'debug', dir: 'in', msg: 'want',
        data: {
          blocks: wantBlocks.length,
          events: wantEvents.length,
          hashes: msg.objects.map((o) => o.hash),
        },
      });
      const { blocks, events } = partitionWantRefs(msg.objects);
      let blockServed = 0;
      let blockMissingLocal = 0;
      for (const ref of blocks) {
        if (ref.kind !== 'block') {
          continue;
        }
        if (storageRoot === undefined) {
          blockMissingLocal += 1;
          continue;
        }
        if (!(await blockReadable(log, storageRoot, ref.hash as Hash))) {
          blockMissingLocal += 1;
          continue;
        }
        blockServed += 1;
        sendBlockStream(log, runOutbound, peer, storageRoot, ref, null, 0, sessionEvents, stallGuard);
      }
      emit({
        layer: 'block', level: blockMissingLocal > 0 ? 'warn' : 'debug', dir: 'local', msg: 'want-satisfied',
        data: {
          blocks: blocks.length,
          events: events.length,
          ...(blocks.length > 0 ? { served: blockServed, missingLocal: blockMissingLocal } : {}),
        },
      });
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
      }
      if (result === 'stored' || result === 'duplicate') {
        const deps = await missingInboundEventDependencies(
          log,
          msg.object.channel,
          msg.bytes,
          storageRoot,
        );
        if (deps.length > 0) {
          const wants = await missingRefs(log, deps, storageRoot);
          if (wants.length > 0) {
            sendWants(wants);
          }
        }
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
    const blockRef = { kind: 'block' as const, hash: hash as Hash };
    const rawAppend =
      (log.reception as { appendReceptionRaw?: (ref: ReceptionObjectRef) => Promise<string> })
        .appendReceptionRaw ?? log.reception.appendReception.bind(log.reception);
    await rawAppend(blockRef);
    broadcastLocalHave([blockRef]);
    await appendBenchMarker(log, 'inbound-stored', {
      kind: 'block',
      hash: hash.slice(0, 16),
      bytes,
    });
    sessionEvents.blockReceived(hash, bytes);
  };

  /** Drop the live `incoming` slot so the wire decoder can accept the next stream-begin in the same TCP chunk. */
  const detachIncomingStream = (): IncomingBlockStream | null => {
    const stream = incoming;
    if (!stream) {
      return null;
    }
    incoming = null;
    incomingStreamActive = false;
    stallGuard?.clearStream();
    clearBulkInbound();
    releaseBlockClaim(stream.hash);
    return stream;
  };

  const finalizeIncomingStream = async (stream: IncomingBlockStream): Promise<void> => {
    try {
      if (stream.mode === 'discard') {
        if (storageRoot !== undefined) {
          clearBlockSettling(storageRoot, stream.hash);
        }
        return;
      }
      if (stream.mode === 'disk') {
        const result = await stream.sink.finish();
        if (result.outcome === 'invalid') {
          logSyncError(
            `block stream hash mismatch ${stream.hash.slice(0, 16)}`,
            new Error('disk block stream verify failed'),
          );
          if (storageRoot !== undefined) {
            clearBlockSettling(storageRoot, stream.hash);
          }
          return;
        }
        await markBlockStored(stream.hash, stream.total);
        if (storageRoot !== undefined) {
          clearBlockSettling(storageRoot, stream.hash);
        }
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
        if (storageRoot !== undefined) {
          clearBlockSettling(storageRoot, stream.hash);
        }
        return;
      }
      await onBlockStream(stream.hash, stream.buffer);
      if (storageRoot !== undefined) {
        clearBlockSettling(storageRoot, stream.hash);
      }
    } catch (err) {
      logSyncError('finalizeIncomingStream', err);
      if (storageRoot !== undefined) {
        clearBlockSettling(storageRoot, stream.hash);
      }
    }
  };

  const scheduleIncomingStreamFinalize = (): void => {
    const stream = detachIncomingStream();
    if (!stream) {
      return;
    }
    void finalizeIncomingStream(stream).catch((err) =>
      logSyncError('finalizeIncomingStream', err),
    );
  };

  const ingestStreamBytes = (chunk: Uint8Array): void => {
    const stream = incoming;
    if (!stream) {
      if (decodeControl.blockStreamState() !== null) {
        throw new Error('block stream bytes without active incoming receiver state');
      }
      return;
    }
    if (stream.mode === 'disk') {
      stream.sink.ingest(chunk);
      if (stream.sink.received >= stream.sink.total) {
        scheduleIncomingStreamFinalize();
      }
      return;
    }
    if (stream.mode === 'discard') {
      stream.received = Math.min(stream.total, stream.received + chunk.byteLength);
      if (stream.received >= stream.total) {
        scheduleIncomingStreamFinalize();
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
      scheduleIncomingStreamFinalize();
    }
  };

  const beginIncomingBlockStream = (hash: string, total: number): void => {
    if (incoming !== null) {
      if (!blockStreamComplete()) {
        throw new Error('block stream already active');
      }
      scheduleIncomingStreamFinalize();
    }
    stallGuard?.clearWant(hash);
    incomingStreamActive = true;
    stallGuard?.armStream();
    if (storageRoot && diskBlockStream) {
      const abs = join(storageRoot, blockPath(hash as Hash));
      if (existsSync(abs)) {
        incoming = { mode: 'discard', hash, total, received: 0 };
        return;
      }
      if (storageRoot !== undefined) {
        markBlockSettling(storageRoot, hash);
      }
      const sink = diskBlockStream.create(hash, total);
      incoming = { mode: 'disk', hash, total, sink };
      // Keep the wire decoder on the socket so urgent `have` / event frames can
      // interleave with block-stream bytes (exclusive/bulk inbound swallowed them).
      return;
    }
    if (total > MAX_BLOCK_STREAM_BUFFER_BYTES) {
      throw new Error(`block stream ${total} B exceeds in-memory limit ${MAX_BLOCK_STREAM_BUFFER_BYTES}`);
    }
    if (storageRoot !== undefined) {
      markBlockSettling(storageRoot, hash);
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

  const isInboundBlockStreamActive = (): boolean => {
    return incoming !== null || decodeControl.blockStreamState() !== null;
  };

  const syncInboundStreamStallGuard = (): void => {
    if (stallGuard === null) {
      return;
    }
    if (isInboundBlockStreamActive()) {
      if (!incomingStreamActive) {
        incomingStreamActive = true;
        stallGuard.armStream();
      } else {
        stallGuard.touchStream();
      }
      return;
    }
    if (incomingStreamActive) {
      incomingStreamActive = false;
      stallGuard.clearStream();
    }
  };

  if (options.onSessionStall !== undefined) {
    const onStall = options.onSessionStall;
    stallGuard = new SessionStallGuard(
      (reason) => {
        // TRACE-20/22 (block layer): why the association is being torn down,
        // with the outstanding-want count that usually explains it. A
        // want-timeout with wantsPending > 0 is the deadlock signature.
        emit({
          layer: 'block',
          level: 'warn',
          dir: 'local',
          msg: 'want-timeout',
          data: {
            reason,
            wantsPending: claimedHashes.size,
            hashes: [...claimedHashes],
          },
        });
        onStall(reason);
      },
      () => ({
        wantsPending: claimedHashes.size,
        streamActive: isInboundBlockStreamActive(),
        outboundActive: stallGuard?.isOutboundActive() ?? false,
        resumeInFlight: !resumeWalkDone && resumeWalkInFlight !== undefined,
      }),
      options.sessionConnectedAt ?? Date.now(),
    );
  }

  peer.onData((chunk) => {
    try {
      decodeControl(chunk);
      syncInboundStreamStallGuard();
    } catch (err) {
      logSyncError('wire decode', err);
      peer.close();
    }
  });
  for (const message of options.initialMessages ?? []) {
    runInbound(() => onMessage(message));
  }
  peer.resumeInbound?.();

  runAttachSync();

  const stop = (): void => {
    stallGuard?.stop();
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
