import { randomBytes } from 'crypto';
import type { DuplexPeer } from './peerLoop.js';
import { createFrameDecoder, encodeFrame } from './codec.js';
import type { Subject, SyncMessage } from './types.js';

const PROTOCOL = 'nearbytes.sync.v1' as const;

export interface FriendHandshakeOptions {
  readonly localProfilePublicKey: string;
  readonly localPeerId: string;
  readonly subject: Subject;
  /**
   * Set of remote profile public keys (lower-case hex) we accept as the
   * remote peer's claimed identity. Per `sync-discovery-v1.md` DISC-24 this
   * is the union of served local profiles (sibling carriage, DISC-26) and
   * configured friends (friend carriage).
   */
  readonly allowedRemoteProfiles: ReadonlySet<string>;
  readonly timeoutMs?: number;
}

export interface FriendHandshakeResult {
  readonly remoteProfile: string;
  /** Empty string when the remote did not advertise a peerId (pre-DISC-26 peer). */
  readonly remotePeerId: string;
}

/**
 * Exchanges {@code hello} on a new duplex before anti-entropy.
 * Resolves with the verified remote profile public key and per-process peerId.
 */
export function exchangeFriendHandshake(
  peer: DuplexPeer,
  options: FriendHandshakeOptions,
): Promise<FriendHandshakeResult> {
  const localProfile = options.localProfilePublicKey.toLowerCase();
  const localPeerId = options.localPeerId.toLowerCase();
  const sessionNonce = randomBytes(16).toString('hex');
  const seenNonces = new Set<string>();
  let remoteResult: FriendHandshakeResult | null = null;
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
      if (localHelloSent && remoteResult !== null) {
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
        reject(new Error('sync handshake: remote is not an authorized profile'));
        return;
      }
      const remotePeerId = msg.senderPeerId?.toLowerCase() ?? '';
      if (remote === localProfile && remotePeerId === localPeerId) {
        detachHandshake();
        clearTimer();
        peer.close();
        reject(new Error('sync handshake: process-level loopback'));
        return;
      }
      remoteResult = { remoteProfile: remote, remotePeerId };
      tryComplete();
    };

    stopHandshakeData = peer.onData(createFrameDecoder(onMessage));
    sendHello();
  });
}
