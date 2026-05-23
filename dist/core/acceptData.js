import { deserializeEvent, publicKeyFromHex, validateBlockBytes, validateEventBytes, } from 'nearbytes-log';
export async function acceptData(log, ref, bytes) {
    if (ref.kind === 'block') {
        const validation = await validateBlockBytes(ref.hash, bytes);
        if (!validation.ok) {
            return 'invalid';
        }
        const hash = ref.hash;
        if (await log.blocks.has(hash)) {
            return 'duplicate';
        }
        await log.blocks.store(hash, bytes, true);
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