/**
 * Transport health budgets (SYNC-63–SYNC-65, OBS-64).
 *
 * Three layers, all compatible with push-wait idle:
 *  - TCP keepalive (kernel) — half-open detection without app traffic
 *  - In-flight stall timers — only while want/stream/resume/pump is outstanding
 *  - Session rotation — quiescent associations closed after max age
 */

/** Kernel TCP keepalive initial delay (seconds). SYNC-63 reference. */
export const TCP_KEEPALIVE_INITIAL_DELAY_SEC = 60;

/** Block `want` awaiting `block-stream-begin`. SYNC-64 reference. */
export const STALL_WANT_MS = 120_000;

/** Partial inbound block stream with no bytes. SYNC-64 reference. */
export const STALL_BLOCK_STREAM_MS = 120_000;

/** Resume-walk page awaiting matching resume `have`. SYNC-64 reference. */
export const STALL_RESUME_PAGE_MS = 120_000;

/** Outbound block pump on this association. SYNC-64 reference. */
export const STALL_OUTBOUND_PUMP_MS = 300_000;

/** Quiescent session rotation age. SYNC-65 reference. */
export const SESSION_ROTATION_MAX_AGE_MS = 600_000;

/** Poll interval once max age is reached but the session is still busy. */
export const SESSION_ROTATION_POLL_MS = 30_000;

export type StallReason =
  | 'want-timeout'
  | 'stream-timeout'
  | 'resume-timeout'
  | 'outbound-timeout'
  | 'session-rotation';

export interface SessionQuiescence {
  readonly wantsPending: number;
  readonly streamActive: boolean;
  readonly outboundActive: boolean;
  readonly resumeInFlight: boolean;
}

export function isSessionQuiescent(state: SessionQuiescence): boolean {
  return (
    state.wantsPending === 0 &&
    !state.streamActive &&
    !state.outboundActive &&
    !state.resumeInFlight
  );
}

function unrefTimer(timer: NodeJS.Timeout): void {
  if (typeof timer.unref === 'function') {
    timer.unref();
  }
}

/**
 * Per-association stall + rotation guard. Arms timers only for outstanding
 * work; rotation fires only when {@link isSessionQuiescent} at expiry.
 */
export class SessionStallGuard {
  private stopped = false;
  private readonly wantTimers = new Map<string, NodeJS.Timeout>();
  private streamTimer: NodeJS.Timeout | null = null;
  private resumeTimer: NodeJS.Timeout | null = null;
  private outboundTimer: NodeJS.Timeout | null = null;
  private rotationTimer: NodeJS.Timeout | null = null;
  private streamActive = false;
  private outboundActive = false;

  constructor(
    private readonly onStall: (reason: StallReason) => void,
    private readonly quiescence: () => SessionQuiescence,
    private readonly connectedAt = Date.now(),
  ) {
    this.armRotation();
  }

  stop(): void {
    this.stopped = true;
    for (const timer of this.wantTimers.values()) {
      clearTimeout(timer);
    }
    this.wantTimers.clear();
    if (this.streamTimer !== null) clearTimeout(this.streamTimer);
    if (this.resumeTimer !== null) clearTimeout(this.resumeTimer);
    if (this.outboundTimer !== null) clearTimeout(this.outboundTimer);
    if (this.rotationTimer !== null) clearTimeout(this.rotationTimer);
    this.streamTimer = null;
    this.resumeTimer = null;
    this.outboundTimer = null;
    this.rotationTimer = null;
  }

  private stall(reason: StallReason): void {
    if (this.stopped) return;
    this.stopped = true;
    this.stop();
    this.onStall(reason);
  }

  armWant(hash: string): void {
    if (this.stopped) return;
    const key = hash.toLowerCase();
    const existing = this.wantTimers.get(key);
    if (existing !== undefined) clearTimeout(existing);
    const timer = setTimeout(() => this.stall('want-timeout'), STALL_WANT_MS);
    unrefTimer(timer);
    this.wantTimers.set(key, timer);
  }

  clearWant(hash: string): void {
    const key = hash.toLowerCase();
    const timer = this.wantTimers.get(key);
    if (timer !== undefined) {
      clearTimeout(timer);
      this.wantTimers.delete(key);
    }
  }

  armStream(): void {
    if (this.stopped) return;
    this.streamActive = true;
    this.resetStreamTimer();
  }

  touchStream(): void {
    if (this.stopped || !this.streamActive) return;
    this.resetStreamTimer();
  }

  clearStream(): void {
    this.streamActive = false;
    if (this.streamTimer !== null) {
      clearTimeout(this.streamTimer);
      this.streamTimer = null;
    }
  }

  private resetStreamTimer(): void {
    if (this.streamTimer !== null) clearTimeout(this.streamTimer);
    const timer = setTimeout(() => this.stall('stream-timeout'), STALL_BLOCK_STREAM_MS);
    unrefTimer(timer);
    this.streamTimer = timer;
  }

  armResume(): void {
    if (this.stopped) return;
    if (this.resumeTimer !== null) clearTimeout(this.resumeTimer);
    const timer = setTimeout(() => this.stall('resume-timeout'), STALL_RESUME_PAGE_MS);
    unrefTimer(timer);
    this.resumeTimer = timer;
  }

  clearResume(): void {
    if (this.resumeTimer !== null) {
      clearTimeout(this.resumeTimer);
      this.resumeTimer = null;
    }
  }

  armOutbound(): void {
    if (this.stopped) return;
    this.outboundActive = true;
    if (this.outboundTimer !== null) clearTimeout(this.outboundTimer);
    const timer = setTimeout(() => this.stall('outbound-timeout'), STALL_OUTBOUND_PUMP_MS);
    unrefTimer(timer);
    this.outboundTimer = timer;
  }

  clearOutbound(): void {
    this.outboundActive = false;
    if (this.outboundTimer !== null) {
      clearTimeout(this.outboundTimer);
      this.outboundTimer = null;
    }
  }

  isOutboundActive(): boolean {
    return this.outboundActive;
  }

  private armRotation(): void {
    const ageMs = Date.now() - this.connectedAt;
    const delayMs =
      ageMs >= SESSION_ROTATION_MAX_AGE_MS
        ? SESSION_ROTATION_POLL_MS
        : Math.max(0, SESSION_ROTATION_MAX_AGE_MS - ageMs);
    const timer = setTimeout(() => {
      if (isSessionQuiescent(this.quiescence())) {
        this.stall('session-rotation');
        return;
      }
      this.armRotation();
    }, delayMs);
    unrefTimer(timer);
    this.rotationTimer = timer;
  }
}
