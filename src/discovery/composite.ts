import type { DiscoveredPeer, PeerDiscovery } from './types.js';

export function createCompositeDiscovery(backends: PeerDiscovery[]): PeerDiscovery {
  const handlers = new Set<(peer: DiscoveredPeer) => void>();

  for (const backend of backends) {
    backend.onPeer((peer) => {
      for (const handler of handlers) {
        handler(peer);
      }
    });
  }

  return {
    async start(): Promise<void> {
      await Promise.all(backends.map((b) => b.start()));
    },
    onPeer(handler: (peer: DiscoveredPeer) => void): void {
      handlers.add(handler);
    },
    async stop(): Promise<void> {
      await Promise.all(backends.map((b) => b.stop()));
    },
  };
}
