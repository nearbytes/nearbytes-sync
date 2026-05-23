declare module 'hyperswarm' {
  import type { EventEmitter } from 'events';
  import type { Duplex } from 'stream';

  interface PeerInfo {
    publicKey: Buffer;
  }

  export default class Hyperswarm extends EventEmitter {
    join(topic: Buffer, options?: { client?: boolean; server?: boolean }): Promise<void>;
    destroy(): Promise<void>;
    on(event: 'connection', listener: (socket: Duplex, peerInfo: PeerInfo) => void): this;
  }
}
