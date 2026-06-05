declare module 'hyperswarm' {
  import type { EventEmitter } from 'events';
  import type { Duplex } from 'stream';

  interface PeerInfo {
    publicKey: Buffer;
    topics?: readonly Buffer[];
    client?: boolean;
  }

  interface PeerDiscovery {
    flushed(): Promise<void>;
  }

  export default class Hyperswarm extends EventEmitter {
    join(topic: Buffer, options?: { client?: boolean; server?: boolean }): PeerDiscovery;
    destroy(): Promise<void>;
    on(event: 'connection', listener: (socket: Duplex, peerInfo: PeerInfo) => void): this;
  }
}
