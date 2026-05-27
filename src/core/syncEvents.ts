/**
 * In-process sync-event bus for live observability.
 *
 * The bus is the single source of truth for "what just happened on the
 * wire" from a CLI/UI perspective. Every interesting wire-level
 * transition — peer connect/disconnect, block sent, block received,
 * event received — is emitted onto a `SyncEventBus` owned by `start()`.
 *
 * Two consumer paths coexist:
 *
 *  1. In-process: `SyncHandle.onEvent(handler)` subscribes a callback
 *     to the bus directly; cheap, zero serialisation. Used by
 *     `nbf monitor` when this process is the active sync engine.
 *  2. Out-of-process: the daemon mirrors recent bus events into the
 *     state beacon (`<dataDir>/.nearbytes-sync.state.json`) via a
 *     bounded {@link SyncEventBuffer}. A writer-only CLI reads the
 *     beacon and renders the daemon's events as if they were its own.
 *     This is what makes `nbf monitor` informative even when the
 *     monitoring process is *not* the one talking to the network.
 *
 * Design choices:
 *
 *  - Events use `at: number` (epoch ms) instead of `Date` so the
 *    in-process and serialised shapes are identical — no marshalling
 *    boundary, no Date↔string conversion drift between LIVE and
 *    DAEMON modes in the monitor UI.
 *  - The discriminated union is closed and small (six kinds today).
 *    Adding a kind is a breaking change for exhaustive switches in
 *    consumers, which is intentional: a new kind without a UI label
 *    would be silently invisible to operators.
 *  - The bus never carries control information (no "you should stop
 *    sending"-style backpressure). It is purely observational; a slow
 *    or absent subscriber MUST NOT stall the wire protocol.
 */

import { EventEmitter } from 'node:events';

/** Wire-level peer became active (post-handshake, registered in `FriendSessionRegistry`). */
export interface PeerConnectedEvent {
  readonly kind: 'peer-connected';
  /** Epoch ms (`Date.now()` at emission). */
  readonly at: number;
  readonly remoteProfilePublicKey: string;
  readonly remotePeerId: string;
  readonly transportLabel: string;
  /** Local-profile classification: sibling = same profile as us, friend = different. */
  readonly role: 'sibling' | 'friend';
}

/** Wire-level peer torn down (socket close or session stop). */
export interface PeerDisconnectedEvent {
  readonly kind: 'peer-disconnected';
  readonly at: number;
  readonly remoteProfilePublicKey: string;
  readonly remotePeerId: string;
  readonly transportLabel: string;
}

/**
 * Association attempt failed before a session was registered (handshake
 * timeout, transport reset, policy reject). Emitted once per failed
 * attempt sequence (after retries are exhausted). The monitor renders
 * this instead of surfacing a stderr stack trace.
 */
export interface PeerConnectFailedEvent {
  readonly kind: 'peer-connect-failed';
  readonly at: number;
  readonly transportLabel: string;
  /** Short machine tag, e.g. `handshake-timeout`. */
  readonly reason: string;
  /** Handshake attempts made (1 = no retry). */
  readonly attempts: number;
  /**
   * Remote profile hex when known (post-hello or mDNS hint); empty when
   * the failure happened before identity was established.
   */
  readonly remoteProfilePublicKey: string;
  readonly remotePeerId: string;
}

/** A block stream finished pumping to a specific peer (outbound). */
export interface BlockSentEvent {
  readonly kind: 'block-sent';
  readonly at: number;
  readonly blockHash: string;
  readonly bytes: number;
  /** Remote profile this block was sent to. */
  readonly toProfile: string;
  readonly toPeerId: string;
}

/** A block was received from a specific peer and stored locally. */
export interface BlockReceivedEvent {
  readonly kind: 'block-received';
  readonly at: number;
  readonly blockHash: string;
  readonly bytes: number;
  readonly fromProfile: string;
  readonly fromPeerId: string;
}

/** A profile-log event (not a content block) was received and stored. */
export interface EventReceivedEvent {
  readonly kind: 'event-received';
  readonly at: number;
  readonly eventHash: string;
  /** Owning channel's profile-public-key hex (lower-case). */
  readonly channel: string;
  readonly bytes: number;
  readonly fromProfile: string;
  readonly fromPeerId: string;
}

export type SyncEvent =
  | PeerConnectedEvent
  | PeerDisconnectedEvent
  | PeerConnectFailedEvent
  | BlockSentEvent
  | BlockReceivedEvent
  | EventReceivedEvent;

export type SyncEventKind = SyncEvent['kind'];

const EVENT_CHANNEL = 'event';

/**
 * Tiny typed wrapper around Node's `EventEmitter`. The wrapper exists
 * exclusively so the typed `emit(SyncEvent)` / `onEvent(handler)`
 * surface cannot accidentally be used with the wrong string event name.
 */
export class SyncEventBus {
  private readonly inner = new EventEmitter();

  constructor(maxListeners = 64) {
    // 64 is well above the realistic number of attached monitors and
    // also high enough that internal subscribers (the in-process ring
    // buffer + the daemon's beacon publisher) never trigger Node's
    // memory-leak warning on a moderately busy node.
    this.inner.setMaxListeners(maxListeners);
  }

  emit(event: SyncEvent): void {
    this.inner.emit(EVENT_CHANNEL, event);
  }

  /** Subscribe a handler. Returns an unsubscribe thunk. */
  onEvent(handler: (event: SyncEvent) => void): () => void {
    this.inner.on(EVENT_CHANNEL, handler);
    return () => {
      this.inner.off(EVENT_CHANNEL, handler);
    };
  }
}

/**
 * Per-session adapter that captures the remote peer's identity once
 * (in `friendSessions`) and presents the wire-loop a context-free
 * surface to emit block/event activity against.
 *
 * Without this adapter the peer-loop would need to thread
 * `(remoteProfile, remotePeerId)` through every hook point. By baking
 * them into the emitter at session attach time we keep the peer-loop's
 * signature unchanged across the observability addition.
 */
export interface PeerSessionEventEmitter {
  blockSent(blockHash: string, bytes: number): void;
  blockReceived(blockHash: string, bytes: number): void;
  eventReceived(channel: string, eventHash: string, bytes: number): void;
}

/**
 * Sentinel no-op emitter used when no bus is wired in (tests, in-memory
 * harness, etc.). Avoids `events?.method()` null-guards at every hook
 * site in the peer-loop.
 */
export const NOOP_PEER_SESSION_EVENT_EMITTER: PeerSessionEventEmitter = {
  blockSent: () => {},
  blockReceived: () => {},
  eventReceived: () => {},
};

/**
 * Bounded ring buffer of recent events. Owned by `start()` and updated
 * via a self-subscription to the bus. The buffer is what the beacon
 * serialises out to disk so writer-only consumers (`nbf monitor`
 * against a daemon-owned dataDir) can observe activity they did not
 * generate themselves.
 *
 * The buffer is *not* a journal — events are best-effort and lost on
 * process restart. Persistent observability is the job of the existing
 * `nearbytes-log` bench markers, which keep their own append-only
 * record.
 */
export class SyncEventBuffer {
  private readonly buf: SyncEvent[] = [];

  constructor(private readonly capacity = 200) {
    if (capacity < 1) throw new Error('SyncEventBuffer capacity must be >= 1');
  }

  push(event: SyncEvent): void {
    this.buf.push(event);
    while (this.buf.length > this.capacity) {
      this.buf.shift();
    }
  }

  /** Frozen snapshot, newest-last, safe for the caller to retain. */
  recent(): readonly SyncEvent[] {
    return [...this.buf];
  }

  size(): number {
    return this.buf.length;
  }
}

// ── cumulative + windowed statistics ─────────────────────────────────────

/**
 * Process-lifetime totals plus a short-window throughput estimate.
 *
 * Totals are simple monotonic counters since `start()` ran. Throughput
 * is computed over a sliding window (5 s by default) so transient
 * spikes are smoothed but the figure still reacts within a few seconds
 * to a peer joining or leaving — what an `htop`-style display needs.
 *
 * The two `bytesPerSec*` fields are inferred at query time from the
 * underlying sample buffer; they are not a separately maintained
 * counter, so a long quiet period naturally decays the rate to zero
 * without an explicit "reset on idle" path.
 */
export interface SyncStats {
  /** Bytes received across all kinds (blocks + events) since start. */
  readonly totalBytesIn: number;
  readonly totalBytesOut: number;
  readonly totalBlocksIn: number;
  readonly totalBlocksOut: number;
  /** Profile-log events received and stored (events, not blocks). */
  readonly totalEventsIn: number;
  /** Bytes-per-second over the last `windowMs` (default 5 000). */
  readonly bytesPerSecIn: number;
  readonly bytesPerSecOut: number;
  /**
   * Window length used to compute the rate, surfaced so UIs can label
   * the figure honestly ("over last 5 s") instead of pretending it is
   * instantaneous.
   */
  readonly windowMs: number;
}

const DEFAULT_BANDWIDTH_WINDOW_MS = 5_000;
/**
 * Hard cap on the sample retention window. We keep at most twice the
 * default rate window so that a `.rate(longer)` call is bounded but
 * still useful. Samples older than this are pruned at insertion time.
 */
const BANDWIDTH_RETENTION_MS = 2 * DEFAULT_BANDWIDTH_WINDOW_MS;

/**
 * Cumulative counters + sliding-window byte rates. Self-contained so
 * unit tests can drive it directly without standing up a full sync
 * stack.
 *
 * Implementation notes:
 *
 *  - Each `record()` appends a single `(at, bytesIn, bytesOut)`
 *    sample and prunes anything outside the retention window. Pruning
 *    is amortised O(1) per call because samples leave the head of the
 *    array in chronological order.
 *
 *  - `snapshot()` is the only externally visible read path. It is the
 *    place where the rate window length is materialised, so callers
 *    cannot accidentally observe an inconsistent (totals,rate) pair.
 */
export class SyncStatsAccumulator {
  private bytesIn = 0;
  private bytesOut = 0;
  private blocksIn = 0;
  private blocksOut = 0;
  private eventsIn = 0;
  /**
   * Newest-last sliding sample log: each entry contributes to the
   * windowed rate. `bytesIn`/`bytesOut` track per-sample deltas (NOT
   * cumulative) so the rate is simply sum-divided-by-window.
   */
  private readonly samples: Array<{ at: number; in: number; out: number }> = [];

  constructor(private readonly windowMs = DEFAULT_BANDWIDTH_WINDOW_MS) {}

  /**
   * Record a block-sent event. Updates the outbound totals and the
   * sliding sample log. `bytes` MUST be non-negative; zero-byte
   * blocks are a contract violation upstream (a block-sent event
   * always carries the bytes that hit the wire).
   */
  recordBlockSent(bytes: number): void {
    this.bytesOut += bytes;
    this.blocksOut += 1;
    this.recordSample(0, bytes);
  }

  recordBlockReceived(bytes: number): void {
    this.bytesIn += bytes;
    this.blocksIn += 1;
    this.recordSample(bytes, 0);
  }

  recordEventReceived(bytes: number): void {
    this.bytesIn += bytes;
    this.eventsIn += 1;
    this.recordSample(bytes, 0);
  }

  private recordSample(bytesInDelta: number, bytesOutDelta: number): void {
    const now = Date.now();
    this.samples.push({ at: now, in: bytesInDelta, out: bytesOutDelta });
    const cutoff = now - BANDWIDTH_RETENTION_MS;
    while (this.samples.length > 0 && this.samples[0]!.at < cutoff) {
      this.samples.shift();
    }
  }

  snapshot(): SyncStats {
    const now = Date.now();
    const start = now - this.windowMs;
    let bin = 0;
    let bout = 0;
    for (const s of this.samples) {
      if (s.at >= start) {
        bin += s.in;
        bout += s.out;
      }
    }
    const seconds = this.windowMs / 1000;
    return {
      totalBytesIn: this.bytesIn,
      totalBytesOut: this.bytesOut,
      totalBlocksIn: this.blocksIn,
      totalBlocksOut: this.blocksOut,
      totalEventsIn: this.eventsIn,
      bytesPerSecIn: bin / seconds,
      bytesPerSecOut: bout / seconds,
      windowMs: this.windowMs,
    };
  }
}
