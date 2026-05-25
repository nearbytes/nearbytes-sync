import { computeHash } from 'nearbytes-crypto';
const TOPIC_DOMAIN = new TextEncoder().encode('nearbytes:sync:v1');
function canonicalSubjectJson(subject) {
    const keys = Object.keys(subject).sort();
    const ordered = {};
    for (const key of keys) {
        ordered[key] = subject[key];
    }
    return JSON.stringify(ordered);
}
/**
 * Derives a 32-byte Hyperswarm topic for a subject.
 */
export async function syncTopic(subject) {
    const encoded = new TextEncoder().encode(JSON.stringify({ p: 'nearbytes.sync.topic.v1', subject: JSON.parse(canonicalSubjectJson(subject)) }));
    const input = new Uint8Array(TOPIC_DOMAIN.length + encoded.length);
    input.set(TOPIC_DOMAIN, 0);
    input.set(encoded, TOPIC_DOMAIN.length);
    const hex = await computeHash(input);
    const bytes = new Uint8Array(32);
    for (let i = 0; i < 32; i++) {
        bytes[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
    }
    return bytes;
}
export function profileSubject(publicKeyHex) {
    return { kind: 'profile', publicKey: publicKeyHex.toLowerCase() };
}
//# sourceMappingURL=topic.js.map