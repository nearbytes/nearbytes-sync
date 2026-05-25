import { randomBytes } from 'crypto';
import type { DuplexPeer } from './peerLoop.js';
import { createFrameDecoder, encodeFrame } from './codec.js';
import type { Subject, SyncMessage } from './types.js';

const PROTOCOL = 'nearbytes.sync.v1' as const;

export interface FriendHandshakeOptions {
  readonly localProfilePublicKey: string;
  readonly subject: Subject;
  readonly allowedRemoteProfiles: ReadonlySet<string>;
  readonly timeoutMs?: number;
}

/**
 * Exchanges {@code hello} on a new duplex before anti-entropy.
 * Resolves with the verified remote profile public key (lower-case hex).
 */
export function exchangeFriendHandshake(
  peer: DuplexPeer,
  options: FriendHandshakeOptions,
): Promise<string> {
  const localProfile = options.localProfilePublicKey.toLowerCase();
  const sessionNonce = randomBytes(16).toString('hex');
  const seenNonces = new Set<string>();
  let remoteProfile: string | null = null;
  let localHelloSent = false;

  return new Promise((resolve, reject) => {
    let stopHandshakeData: (() => void) | null = null;
    const timeout = setTimeout(() => {
      stopHandshakeData?.();
      peer.close();
      reject(new Error('sync handshake timed out'));
    }, options.timeoutMs ?? 15_000);

    const clearTimer = (): void => clearTimeout(timeout);

    const detachHandshake = (): void => {
      stopHandshakeData?.();
      stopHandshakeData = null;
    };

    const tryComplete = (): void => {
      if (localHelloSent && remoteProfile !== null) {
        detachHandshake();
        clearTimer();
        resolve(remoteProfile);
      }
    };

    const sendHello = (): void => {
      const hello: SyncMessage = {
        type: 'hello',
        protocol: PROTOCOL,
        subject: options.subject,
        sessionNonce,
        senderProfile: localProfile,
      };
      peer.write(encodeFrame(hello));
      localHelloSent = true;
      tryComplete();
    };

    const onMessage = (msg: SyncMessage): void => {
      if (msg.type !== 'hello') {
        return;
      }
      if (msg.protocol !== PROTOCOL) {
        detachHandshake();
        clearTimer();
        peer.close();
        reject(new Error(`sync handshake: unsupported protocol ${msg.protocol}`));
        return;
      }
      if (seenNonces.has(msg.sessionNonce)) {
        detachHandshake();
        clearTimer();
        peer.close();
        reject(new Error('sync handshake: duplicate sessionNonce'));
        return;
      }
      seenNonces.add(msg.sessionNonce);

      const remote = msg.senderProfile?.toLowerCase();
      if (!remote || !options.allowedRemoteProfiles.has(remote)) {
        detachHandshake();
        clearTimer();
        peer.close();
        reject(new Error('sync handshake: remote is not a configured friend'));
        return;
      }
      remoteProfile = remote;
      tryComplete();
    };

    stopHandshakeData = peer.onData(createFrameDecoder(onMessage));
    sendHello();
  });
}
