import type { Subject } from './types.js';
/**
 * Derives a 32-byte Hyperswarm topic for a subject.
 */
export declare function syncTopic(subject: Subject): Promise<Uint8Array>;
export declare function profileSubject(publicKeyHex: string): Subject;
//# sourceMappingURL=topic.d.ts.map