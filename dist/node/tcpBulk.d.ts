import type { Socket } from 'net';
/** Lower profile hex initiates outbound TCP (one session per friend pair). */
export declare function shouldInitiateSyncTcp(localProfileHex: string, remoteProfileHex: string): boolean;
export interface PumpResult {
    readonly bytes: number;
    readonly pumpBeginAt: number;
    readonly pumpEndAt: number;
}
/** stream-begin + readSync(16 MiB) + tryWrite/drain (nc-shape, fastest on localhost). */
export declare function pumpBlockFileOverSocket(socket: Socket, dataDir: string, hash: string): Promise<PumpResult>;
//# sourceMappingURL=tcpBulk.d.ts.map