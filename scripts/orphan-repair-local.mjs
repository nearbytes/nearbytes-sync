import { mkdir, rm, cp, readdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { createFilesystemLog } from 'nearbytes-log';
import { start } from '../dist/node/start.js';
import { createCryptoOperations, createSecret, bytesToHex } from 'nearbytes-crypto';
import { createSignedEvent } from 'nearbytes-log';
import { EventType } from 'nearbytes-crypto';

const base = path.join(os.tmpdir(), `nb-orphan-${Date.now()}`);
const dirA = path.join(base, 'a');
const dirB = path.join(base, 'b');
await rm(base, { recursive: true, force: true });
await mkdir(dirA, { recursive: true });
await mkdir(dirB, { recursive: true });

const PROFILE = 'orphan-test-profile:secret';
const crypto = createCryptoOperations();
const pk = bytesToHex((await crypto.deriveKeys(createSecret(PROFILE))).publicKey);
const volKp = await crypto.deriveKeys(createSecret('vol:pass'));
const volPk = bytesToHex(volKp.publicKey);

async function storeCreate(log, pathName, content) {
  const blockHash = await log.blocks.store(Buffer.from(content));
  const payload = {
    type: EventType.CREATE_FILE,
    path: pathName,
    content: { protocol: 'nb.content.single.v1', blockHash },
    wrappedKey: new Uint8Array(32),
    mimeType: 'application/octet-stream',
    createdAt: Date.now(),
  };
  const listed = await log.events.listEvents(volKp.publicKey);
  const head = listed.at(-1);
  const refs = head ? [head, blockHash] : [blockHash];
  const signed = await createSignedEvent(crypto, volKp, payload, refs);
  return log.events.storeEvent(volKp.publicKey, signed);
}

const logA = createFilesystemLog(dirA);
await storeCreate(logA, 'a.txt', 'AAA');
await storeCreate(logA, 'b.txt', 'BBB');

const logB = createFilesystemLog(dirB);
const channelB = path.join(dirB, 'channels', volPk);
await mkdir(channelB, { recursive: true });
const eventsA = await readdir(path.join(dirA, 'channels', volPk));
const orphan = eventsA.sort().at(-1);
await cp(path.join(dirA, 'channels', volPk, orphan), path.join(channelB, orphan));
const orphanJson = JSON.parse(await readFile(path.join(channelB, orphan), 'utf8'));
for (const h of orphanJson.envelope.blockRefs.slice(1)) {
  await mkdir(path.join(dirB, 'blocks'), { recursive: true });
  await cp(path.join(dirA, 'blocks', `${h}.bin`), path.join(dirB, 'blocks', `${h}.bin`));
}

const syncA = await start(logA, [], {
  serveProfilePublicKeys: [pk],
  activeProfilePublicKey: pk,
  blockStorageRoot: dirA,
});
const syncB = await start(logB, [], {
  serveProfilePublicKeys: [pk],
  activeProfilePublicKey: pk,
  blockStorageRoot: dirB,
});

let ok = false;
const t0 = Date.now();
for (let i = 0; i < 40; i++) {
  await new Promise((r) => setTimeout(r, 250));
  const listedB = await logB.events.listEvents(volKp.publicKey);
  if (listedB.length >= 2) {
    ok = true;
    break;
  }
}

console.log(
  JSON.stringify({
    ok,
    ms: Date.now() - t0,
    eventsB: (await logB.events.listEvents(volKp.publicKey)).length,
    peersA: syncA.snapshot().connectedPeers,
    peersB: syncB.snapshot().connectedPeers,
  }),
);

await syncA.stop();
await syncB.stop();
await rm(base, { recursive: true, force: true });
process.exit(ok ? 0 : 1);
