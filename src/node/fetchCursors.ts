import { mkdir, readdir, readFile, rename, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

export interface FetchCursorStore {
  get(remoteProfilePublicKey: string, remoteInstancePublicKey: string): Promise<string | undefined>;
  put(
    remoteProfilePublicKey: string,
    remoteInstancePublicKey: string,
    cursor: string,
  ): Promise<void>;
}

interface FetchCursorRecord {
  readonly remoteProfilePublicKey: string;
  readonly remoteInstancePublicKey: string;
  readonly cursor: string;
  readonly updatedAt: string;
}

interface LegacyFetchCursorFile {
  readonly version: 1;
  readonly cursors: Record<string, FetchCursorRecord>;
}

interface PerPeerFetchCursorFile {
  readonly version: 1;
  readonly remoteProfilePublicKey: string;
  readonly remoteInstancePublicKey: string;
  readonly cursor: string;
  readonly updatedAt: string;
}

const FETCH_CURSORS_DIR = 'sync/fetch-cursors';
const LEGACY_FETCH_CURSORS_PATH = 'sync/fetch-cursors.json';

function key(remoteProfilePublicKey: string, remoteInstancePublicKey: string): string {
  return `${remoteProfilePublicKey.toLowerCase()}|${remoteInstancePublicKey.toLowerCase()}`;
}

function emptyLegacyFile(): LegacyFetchCursorFile {
  return { version: 1, cursors: {} };
}

function parseLegacyFile(raw: string): LegacyFetchCursorFile {
  try {
    const parsed = JSON.parse(raw) as { version?: unknown; cursors?: unknown };
    if (parsed.version !== 1 || typeof parsed.cursors !== 'object' || parsed.cursors === null) {
      return emptyLegacyFile();
    }
    const cursors: Record<string, FetchCursorRecord> = {};
    for (const [entryKey, value] of Object.entries(parsed.cursors)) {
      if (typeof value !== 'object' || value === null) {
        continue;
      }
      const record = value as Partial<FetchCursorRecord>;
      if (
        typeof record.remoteProfilePublicKey !== 'string' ||
        typeof record.remoteInstancePublicKey !== 'string' ||
        typeof record.cursor !== 'string' ||
        typeof record.updatedAt !== 'string'
      ) {
        continue;
      }
      cursors[entryKey] = {
        remoteProfilePublicKey: record.remoteProfilePublicKey.toLowerCase(),
        remoteInstancePublicKey: record.remoteInstancePublicKey.toLowerCase(),
        cursor: record.cursor,
        updatedAt: record.updatedAt,
      };
    }
    return { version: 1, cursors };
  } catch {
    return emptyLegacyFile();
  }
}

function parsePerPeerFile(
  raw: string,
): { remoteProfilePublicKey: string; remoteInstancePublicKey: string; cursor: string } | undefined {
  try {
    const parsed = JSON.parse(raw) as Partial<PerPeerFetchCursorFile>;
    if (
      parsed.version !== 1 ||
      typeof parsed.remoteProfilePublicKey !== 'string' ||
      typeof parsed.remoteInstancePublicKey !== 'string' ||
      typeof parsed.cursor !== 'string'
    ) {
      return undefined;
    }
    return {
      remoteProfilePublicKey: parsed.remoteProfilePublicKey.toLowerCase(),
      remoteInstancePublicKey: parsed.remoteInstancePublicKey.toLowerCase(),
      cursor: parsed.cursor,
    };
  } catch {
    return undefined;
  }
}

export function createFetchCursorStore(dataDir: string | undefined): FetchCursorStore {
  if (dataDir === undefined) {
    const cursors = new Map<string, string>();
    return {
      async get(remoteProfilePublicKey, remoteInstancePublicKey) {
        return cursors.get(key(remoteProfilePublicKey, remoteInstancePublicKey));
      },
      async put(remoteProfilePublicKey, remoteInstancePublicKey, cursor) {
        cursors.set(key(remoteProfilePublicKey, remoteInstancePublicKey), cursor);
      },
    };
  }

  const dirPath = join(dataDir, FETCH_CURSORS_DIR);
  const legacyPath = join(dataDir, LEGACY_FETCH_CURSORS_PATH);
  let chain: Promise<void> = Promise.resolve();

  const readLegacy = async (): Promise<LegacyFetchCursorFile> => {
    try {
      return parseLegacyFile(await readFile(legacyPath, 'utf8'));
    } catch {
      return emptyLegacyFile();
    }
  };

  const cursorPathForInstance = (remoteInstancePublicKey: string): string =>
    join(dirPath, `${remoteInstancePublicKey.toLowerCase()}.json`);

  const readPerPeer = async (
    remoteInstancePublicKey: string,
  ): Promise<{ remoteProfilePublicKey: string; remoteInstancePublicKey: string; cursor: string } | undefined> => {
    try {
      return parsePerPeerFile(await readFile(cursorPathForInstance(remoteInstancePublicKey), 'utf8'));
    } catch {
      return undefined;
    }
  };

  const writePerPeer = async (
    remoteProfilePublicKey: string,
    remoteInstancePublicKey: string,
    cursor: string,
  ): Promise<void> => {
    await mkdir(dirPath, { recursive: true });
    const path = cursorPathForInstance(remoteInstancePublicKey);
    const tmpPath = `${path}.tmp`;
    const payload: PerPeerFetchCursorFile = {
      version: 1,
      remoteProfilePublicKey: remoteProfilePublicKey.toLowerCase(),
      remoteInstancePublicKey: remoteInstancePublicKey.toLowerCase(),
      cursor,
      updatedAt: new Date().toISOString(),
    };
    await writeFile(tmpPath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
    await rename(tmpPath, path);
  };

  const migrateLegacyIfNeeded = async (): Promise<void> => {
    const legacy = await readLegacy();
    if (Object.keys(legacy.cursors).length === 0) {
      return;
    }
    const files = await readdir(dirPath).catch(() => []);
    if (files.length > 0) {
      return;
    }
    for (const record of Object.values(legacy.cursors)) {
      await writePerPeer(record.remoteProfilePublicKey, record.remoteInstancePublicKey, record.cursor);
    }
  };

  return {
    async get(remoteProfilePublicKey, remoteInstancePublicKey) {
      const instanceKey = remoteInstancePublicKey.toLowerCase();
      const profileKey = remoteProfilePublicKey.toLowerCase();

      const perPeer = await readPerPeer(instanceKey);
      if (
        perPeer !== undefined &&
        perPeer.remoteInstancePublicKey === instanceKey &&
        perPeer.remoteProfilePublicKey === profileKey
      ) {
        return perPeer.cursor;
      }

      await migrateLegacyIfNeeded();
      const migrated = await readPerPeer(instanceKey);
      if (
        migrated !== undefined &&
        migrated.remoteInstancePublicKey === instanceKey &&
        migrated.remoteProfilePublicKey === profileKey
      ) {
        return migrated.cursor;
      }

      const legacy = await readLegacy();
      return legacy.cursors[key(profileKey, instanceKey)]?.cursor;
    },
    async put(remoteProfilePublicKey, remoteInstancePublicKey, cursor) {
      const next = chain.then(async () => {
        await writePerPeer(remoteProfilePublicKey, remoteInstancePublicKey, cursor);
      });
      chain = next.catch(() => undefined);
      await next;
    },
  };
}
