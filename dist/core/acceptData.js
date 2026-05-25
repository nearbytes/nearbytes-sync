import { deserializeEvent, publicKeyFromHex, validateBlockBytes, validateEventBytes, } from 'nearbytes-log';
export async function acceptData(log, ref, bytes, options) {
    if (ref.kind === 'block') {
        const hash = ref.hash;
        if (await log.blocks.has(hash)) {
            return 'duplicate';
        }
        if (options?.verifyIntegrity !== false) {
            const validation = await validateBlockBytes(ref.hash, bytes);
            if (!validation.ok) {
                return 'invalid';
            }
        }
        // Streaming receiver: the digest is verified (either above via
        // `validateBlockBytes`, or incrementally inside the block stream sink
        // when `verifyIntegrity: false`). Either way the caller asserts that
        // `hash === SHA-256(bytes)`, so we take the log's fast path that skips
        // the second hash. See nearbytes-specs/storage/log-api-v1.md §2.3 and
        // requirements/sync-protocol-v1.md SYNC-36.
        await log.blocks.storeAlreadyVerified(hash, bytes, true);
        return 'stored';
    }
    const channelHex = ref.channel.toLowerCase();
    const publicKey = publicKeyFromHex(channelHex);
    if (!publicKey) {
        return 'invalid';
    }
    const validation = await validateEventBytes(channelHex, ref.hash, bytes);
    if (!validation.ok) {
        return 'invalid';
    }
    const serialized = JSON.parse(new TextDecoder().decode(bytes));
    const event = deserializeEvent(serialized);
    const eventHash = ref.hash;
    if (await log.events.listEvents(publicKey).then((h) => h.includes(eventHash))) {
        return 'duplicate';
    }
    await log.events.storeEvent(publicKey, event);
    return 'stored';
}
//# sourceMappingURL=acceptData.js.map