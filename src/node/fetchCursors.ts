import { mkdir, readFile, rename, writeFile } from 'node:fs/promises';
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

interface FetchCursorFile {
  readonly version: 1;
  readonly cursors: Record<string, FetchCursorRecord>;
}

const FETCH_CURSORS_PATH = 'sync/fetch-cursors.json';

function key(remoteProfilePublicKey: string, remoteInstancePublicKey: string): string {
  return `${remoteProfilePublicKey.toLowerCase()}|${remoteInstancePublicKey.toLowerCase()}`;
}

function emptyFile(): FetchCursorFile {
  return { version: 1, cursors: {} };
}

function parseFile(raw: string): FetchCursorFile {
  try {
    const parsed = JSON.parse(raw) as { version?: unknown; cursors?: unknown };
    if (parsed.version !== 1 || typeof parsed.cursors !== 'object' || parsed.cursors === null) {
      return emptyFile();
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
    return emptyFile();
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

  const path = join(dataDir, FETCH_CURSORS_PATH);
  const tmpPath = `${path}.tmp`;
  let chain: Promise<void> = Promise.resolve();

  const read = async (): Promise<FetchCursorFile> => {
    try {
      return parseFile(await readFile(path, 'utf8'));
    } catch {
      return emptyFile();
    }
  };

  const write = async (file: FetchCursorFile): Promise<void> => {
    await mkdir(join(dataDir, 'sync'), { recursive: true });
    await writeFile(tmpPath, `${JSON.stringify(file, null, 2)}\n`, 'utf8');
    await rename(tmpPath, path);
  };

  return {
    async get(remoteProfilePublicKey, remoteInstancePublicKey) {
      const file = await read();
      return file.cursors[key(remoteProfilePublicKey, remoteInstancePublicKey)]?.cursor;
    },
    async put(remoteProfilePublicKey, remoteInstancePublicKey, cursor) {
      const next = chain.then(async () => {
        const file = await read();
        const entryKey = key(remoteProfilePublicKey, remoteInstancePublicKey);
        file.cursors[entryKey] = {
          remoteProfilePublicKey: remoteProfilePublicKey.toLowerCase(),
          remoteInstancePublicKey: remoteInstancePublicKey.toLowerCase(),
          cursor,
          updatedAt: new Date().toISOString(),
        };
        await write(file);
      });
      chain = next.catch(() => undefined);
      await next;
    },
  };
}
