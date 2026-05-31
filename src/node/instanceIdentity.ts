import { createECDH } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

export interface InstanceIdentity {
  readonly publicKey: string;
  readonly privateKey: string;
}

const INSTANCE_DIR = 'sync';
const INSTANCE_FILENAME = 'instance.json';
const PUBLIC_KEY_RE = /^04[0-9a-f]{128}$/;
const PRIVATE_KEY_RE = /^[0-9a-f]{64}$/;

function derivePublicKey(privateKeyHex: string): string {
  const ecdh = createECDH('prime256v1');
  ecdh.setPrivateKey(Buffer.from(privateKeyHex, 'hex'));
  return ecdh.getPublicKey(undefined, 'uncompressed').toString('hex').toLowerCase();
}

function generateIdentity(): InstanceIdentity {
  const ecdh = createECDH('prime256v1');
  ecdh.generateKeys();
  return {
    publicKey: ecdh.getPublicKey(undefined, 'uncompressed').toString('hex').toLowerCase(),
    privateKey: ecdh.getPrivateKey().toString('hex').toLowerCase(),
  };
}

function parseIdentity(raw: string): InstanceIdentity | null {
  try {
    const parsed = JSON.parse(raw) as {
      version?: unknown;
      algorithm?: unknown;
      publicKey?: unknown;
      privateKey?: unknown;
    };
    if (
      parsed.version !== 1 ||
      parsed.algorithm !== 'P-256' ||
      typeof parsed.publicKey !== 'string' ||
      typeof parsed.privateKey !== 'string'
    ) {
      return null;
    }
    const publicKey = parsed.publicKey.toLowerCase();
    const privateKey = parsed.privateKey.toLowerCase();
    if (!PUBLIC_KEY_RE.test(publicKey) || !PRIVATE_KEY_RE.test(privateKey)) {
      return null;
    }
    if (derivePublicKey(privateKey) !== publicKey) {
      return null;
    }
    return { publicKey, privateKey };
  } catch {
    return null;
  }
}

function identityPath(dataDir: string): string {
  return join(dataDir, INSTANCE_DIR, INSTANCE_FILENAME);
}

function serializeIdentity(identity: InstanceIdentity): string {
  return `${JSON.stringify(
    {
      version: 1,
      algorithm: 'P-256',
      publicKey: identity.publicKey,
      privateKey: identity.privateKey,
    },
    null,
    2,
  )}\n`;
}

export function loadOrCreateInstanceIdentity(dataDir: string | undefined): InstanceIdentity {
  if (dataDir === undefined) {
    return generateIdentity();
  }
  mkdirSync(join(dataDir, INSTANCE_DIR), { recursive: true });
  const file = identityPath(dataDir);
  if (existsSync(file)) {
    const existing = parseIdentity(readFileSync(file, 'utf8'));
    if (existing !== null) {
      return existing;
    }
  }
  const fresh = generateIdentity();
  try {
    writeFileSync(file, serializeIdentity(fresh), {
      encoding: 'utf8',
      flag: 'wx',
      mode: 0o600,
    });
    return fresh;
  } catch {
    const raced = existsSync(file) ? parseIdentity(readFileSync(file, 'utf8')) : null;
    if (raced !== null) {
      return raced;
    }
    writeFileSync(file, serializeIdentity(fresh), {
      encoding: 'utf8',
      flag: 'w',
      mode: 0o600,
    });
    return fresh;
  }
}

export function peekInstancePublicKey(dataDir: string): string {
  try {
    const existing = parseIdentity(readFileSync(identityPath(dataDir), 'utf8'));
    return existing?.publicKey ?? '';
  } catch {
    return '';
  }
}
