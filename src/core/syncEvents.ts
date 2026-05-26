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
 *  - The discriminated union is closed and small (five kinds today).
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
