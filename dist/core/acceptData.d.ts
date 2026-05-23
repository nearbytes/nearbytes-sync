import type { Log } from 'nearbytes-log';
import type { ObjectRef } from './types.js';
export type AcceptResult = 'stored' | 'duplicate' | 'invalid';
export declare function acceptData(log: Log, ref: ObjectRef, bytes: Uint8Array): Promise<AcceptResult>;
//# sourceMappingURL=acceptData.d.ts.map