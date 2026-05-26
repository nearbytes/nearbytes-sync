import type { Log } from 'nearbytes-log';

/**
 * Per-Log registry of in-flight `want(H)` block requests.
 *
 * Without this, two friend (or sibling) sessions that independently receive
 * `have(H)` from different peers will each emit `want(H)` and the same block
 * bytes will arrive twice over the wire. Two transports landing the same
 * content-addressed bytes on the same `<hash>.bin` path is correct in the CRDT
 * sense (the hash is the content) but wastes bandwidth and creates a race on
 * the tmp→final rename step. The registry forces at most one in-flight `want`
 * for any given hash across all active sessions sharing this Log.
 *
 * A slot is claimed right before `want(H)` is sent and released when the
 * incoming stream finishes (stored, hash-mismatch, or discarded as
 * already-local) or when the holding session closes. If the holding session
 * disconnects mid-stream, the slot is freed and the next session's `have(H)`
 * will be allowed to re-request.
 */
export class InflightBlockRegistry {
  private readonly inflight = new Set<string>();

  /** Returns true iff `hash` was not in-flight and is now reserved by the caller. */
  claim(hash: string): boolean {
    const key = hash.toLowerCase();
    if (this.inflight.has(key)) return false;
    this.inflight.add(key);
    return true;
  }

  release(hash: string): void {
    this.inflight.delete(hash.toLowerCase());
  }

  /** Number of hashes currently in flight (for `bye`-time quiesce snapshots). */
  size(): number {
    return this.inflight.size;
  }
}

/**
 * Per-Log counter of outbound block-stream pumps currently in `runOutbound`
 * across all open associations. Tracked here (not inside `attachPeerSession`)
 * so a snapshot caller can aggregate the whole Log without enumerating
 * sessions. Mutated by `sendBlockStream` on enqueue/completion.
 */
export class OutboundBlockStreamCounter {
  private count = 0;

  begin(): void {
    this.count += 1;
  }

  end(): void {
    if (this.count > 0) this.count -= 1;
  }

  size(): number {
    return this.count;
  }
}

const outbound = new WeakMap<Log, OutboundBlockStreamCounter>();

export function outboundBlockStreamCounter(log: Log): OutboundBlockStreamCounter {
  let c = outbound.get(log);
  if (c === undefined) {
    c = new OutboundBlockStreamCounter();
    outbound.set(log, c);
  }
  return c;
}

const registries = new WeakMap<Log, InflightBlockRegistry>();

export function inflightBlockRegistry(log: Log): InflightBlockRegistry {
  let r = registries.get(log);
  if (r === undefined) {
    r = new InflightBlockRegistry();
    registries.set(log, r);
  }
  return r;
}
