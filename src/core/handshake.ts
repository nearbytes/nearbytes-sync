import { randomBytes } from 'crypto';
import type { DuplexPeer } from './peerLoop.js';
import { createFrameDecoder, encodeFrame } from './codec.js';
import type { Subject, SyncMessage } from './types.js';
import { syncDebugLine, type TraceEmit } from '../syncDebugLog.js';

const PROTOCOL = 'nearbytes.sync.v1' as const;

/** Machine-readable reason for a failed pre-sync hello exchange. */
export type SyncHandshakeFailureCode =
  | 'timeout'
  | 'unsupported-protocol'
  | 'duplicate-nonce'
  | 'unauthorized-profile'
  | 'instance-loopback';

/**
 * Expected handshake failure (timeout, race, policy). Callers MUST NOT
 * log these with a stack trace — discovery will retry or the operator
 * sees a single-line `peer-connect-failed` event in the monitor.
 */
export class SyncHandshakeError extends Error {
  readonly name = 'SyncHandshakeError';

  constructor(
    readonly code: SyncHandshakeFailureCode,
    message: string,
    readonly retryable: boolean,
  ) {
    super(message);
  }
}

export interface FriendHandshakeOptions {
  readonly localProfilePublicKey: string;
  readonly localPeerId: string;
  readonly localInstancePublicKey: string;
  readonly subject: Subject;
  /**
   * Set of remote profile public keys (lower-case hex) we accept as the
   * remote peer's claimed identity. Per `sync-discovery-v1.md` DISC-24 this
   * is the union of served local profiles (sibling carriage, DISC-26) and
   * configured friends (friend carriage).
   */
  readonly allowedRemoteProfiles: ReadonlySet<string>;
  readonly timeoutMs?: number;
  /** Trace emitter threaded by reference from `StartOptions.trace` (TRACE-04). Defaults to the legacy global sink. */
  readonly trace?: TraceEmit;
}

export interface FriendHandshakeResult {
  readonly remoteProfile: string;
  readonly remotePeerId: string;
  readonly remoteInstancePublicKey: string;
  /**
   * Control frames that arrived in the same read window as the remote hello.
   * TCP may coalesce `hello` with the peer-loop's immediate `subscribe/delta`;
   * the handshake decoder must not drop those frames before the peer-loop
   * decoder is attached.
   */
  readonly earlyMessages: readonly SyncMessage[];
}

/**
 * Exchanges {@code hello} on a new duplex before anti-entropy.
 * Resolves with the verified remote profile public key and instance public key.
 */
export function exchangeFriendHandshake(
  peer: DuplexPeer,
  options: FriendHandshakeOptions,
): Promise<FriendHandshakeResult> {
  const trace = options.trace ?? syncDebugLine;
  const localProfile = options.localProfilePublicKey.toLowerCase();
  const localPeerId = options.localPeerId.toLowerCase();
  const localInstancePublicKey = options.localInstancePublicKey.toLowerCase();
  const sessionNonce = randomBytes(16).toString('hex');
  const seenNonces = new Set<string>();
  let remoteResult: FriendHandshakeResult | null = null;
  let localHelloSent = false;
  const earlyMessages: SyncMessage[] = [];

  return new Promise((resolve, reject) => {
    let stopHandshakeData: (() => void) | null = null;
    const timeout = setTimeout(() => {
      stopHandshakeData?.();
      peer.close();
      trace('wire', 'warn', 'hello ← timed out waiting for remote hello');
      reject(new SyncHandshakeError('timeout', 'sync handshake timed out', true));
    }, options.timeoutMs ?? 15_000);

    const clearTimer = (): void => clearTimeout(timeout);

    const detachHandshake = (): void => {
      stopHandshakeData?.();
      stopHandshakeData = null;
    };

    const tryComplete = (): void => {
      if (localHelloSent && remoteResult !== null) {
        peer.pauseInbound?.();
        detachHandshake();
        clearTimer();
        resolve(remoteResult);
      }
    };

    const sendHello = (): void => {
      const hello: SyncMessage = {
        type: 'hello',
        protocol: PROTOCOL,
        subject: options.subject,
        sessionNonce,
        senderProfile: localProfile,
        senderPeerId: localPeerId,
        senderInstancePublicKey: localInstancePublicKey,
      };
      peer.write(encodeFrame(hello));
      localHelloSent = true;
      trace('wire', 'info', `hello → subject=${JSON.stringify(options.subject)}`);
      tryComplete();
    };

    const onMessage = (msg: SyncMessage): void => {
      if (msg.type !== 'hello') {
        earlyMessages.push(msg);
        return;
      }
      if (msg.protocol !== PROTOCOL) {
        detachHandshake();
        clearTimer();
        peer.close();
        trace('wire', 'warn', `hello ← rejected: unsupported protocol ${msg.protocol}`);
        reject(
          new SyncHandshakeError(
            'unsupported-protocol',
            `sync handshake: unsupported protocol ${msg.protocol}`,
            false,
          ),
        );
        return;
      }
      if (seenNonces.has(msg.sessionNonce)) {
        detachHandshake();
        clearTimer();
        peer.close();
        trace('wire', 'warn', 'hello ← rejected: duplicate sessionNonce');
        reject(
          new SyncHandshakeError(
            'duplicate-nonce',
            'sync handshake: duplicate sessionNonce',
            true,
          ),
        );
        return;
      }
      seenNonces.add(msg.sessionNonce);

      const remote = msg.senderProfile?.toLowerCase();
      if (!remote || !options.allowedRemoteProfiles.has(remote)) {
        detachHandshake();
        clearTimer();
        peer.close();
        trace(
          'wire',
          'warn',
          `hello ← rejected: unauthorized profile ${remote ?? '(none)'}`,
        );
        reject(
          new SyncHandshakeError(
            'unauthorized-profile',
            'sync handshake: remote is not an authorized profile',
            false,
          ),
        );
        return;
      }
      const remotePeerId = msg.senderPeerId?.toLowerCase() ?? '';
      if (remotePeerId === '') {
        detachHandshake();
        clearTimer();
        peer.close();
        trace('wire', 'warn', 'hello ← rejected: no peer id advertised');
        reject(
          new SyncHandshakeError(
            'unauthorized-profile',
            'sync handshake: remote did not advertise peer id',
            false,
          ),
        );
        return;
      }
      const remoteInstancePublicKey = msg.senderInstancePublicKey?.toLowerCase() ?? '';
      if (remoteInstancePublicKey === '') {
        detachHandshake();
        clearTimer();
        peer.close();
        trace('wire', 'warn', 'hello ← rejected: no instance identity advertised');
        reject(
          new SyncHandshakeError(
            'unauthorized-profile',
            'sync handshake: remote did not advertise instance identity',
            false,
          ),
        );
        return;
      }
      if (remote === localProfile && remoteInstancePublicKey === localInstancePublicKey) {
        detachHandshake();
        clearTimer();
        peer.close();
        trace('wire', 'warn', 'hello ← rejected: instance-level loopback');
        reject(
          new SyncHandshakeError(
            'instance-loopback',
            'sync handshake: instance-level loopback',
            false,
          ),
        );
        return;
      }
      trace(
        'wire',
        'info',
        `hello ← accepted profile=${remote.slice(0, 12)} inst=${remoteInstancePublicKey.slice(0, 8)}`,
      );
      remoteResult = {
        remoteProfile: remote,
        remotePeerId,
        remoteInstancePublicKey,
        earlyMessages,
      };
      tryComplete();
    };

    stopHandshakeData = peer.onData(createFrameDecoder(onMessage));
    sendHello();
  });
}
