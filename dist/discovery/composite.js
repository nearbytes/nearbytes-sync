export function createCompositeDiscovery(backends) {
    const handlers = new Set();
    for (const backend of backends) {
        backend.onPeer((peer) => {
            for (const handler of handlers) {
                handler(peer);
            }
        });
    }
    return {
        async start() {
            await Promise.all(backends.map((b) => b.start()));
        },
        onPeer(handler) {
            handlers.add(handler);
        },
        async stop() {
            await Promise.all(backends.map((b) => b.stop()));
        },
    };
}
//# sourceMappingURL=composite.js.map