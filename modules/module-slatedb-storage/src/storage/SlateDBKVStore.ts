import {
  Admin,
  AdminBuilder,
  Checkpoint,
  CheckpointCreateResult,
  Db,
  DbBuilder,
  DbReader,
  DbReaderBuilder,
  FlushType,
  KeyRange,
  ObjectStore,
  WriteBatch
} from '@slatedb/uniffi';
import fs from 'node:fs/promises';
import path from 'node:path';

export interface SlateDBKVStoreOptions {
  path?: string;
  dbPath?: string;
  objectStore?:
    | {
        type: 'memory';
      }
    | {
        type: 'file';
        path: string;
      }
    | {
        type: 's3';
        bucket: string;
        prefix?: string;
      }
    | {
        url: string;
      };
}

export type SlateDBKey = string | Uint8Array;

export interface SlateDBEntry<T = unknown> {
  key: Uint8Array;
  keyText: string;
  value: T;
}

export type SlateDBWriteOperation =
  | {
      type: 'put';
      key: SlateDBKey;
      value: unknown;
    }
  | {
      type: 'delete';
      key: SlateDBKey;
    };

type SlateDBReadable = Pick<Db | DbReader, 'get' | 'scan_prefix'>;

const DEFAULT_DB_PATH = 'powersync';
const LOCAL_FILE_OBJECT_STORE_URL = 'file:///';
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

/**
 * Absolute-minimal SlateDB K/V wrapper used by the storage POC.
 *
 * This intentionally does not implement the PowerSync storage interfaces yet.
 * It gives the module a real SlateDB-backed primitive for current-value keys
 * and checkpoint-pinned reads.
 */
export class SlateDBKVStore implements AsyncDisposable {
  private constructor(
    readonly objectStoreUrl: string,
    readonly dbPath: string,
    private readonly objectStore: ObjectStore,
    private readonly db: Db,
    private readonly admin: Admin
  ) {}

  static async open(options: SlateDBKVStoreOptions): Promise<SlateDBKVStore> {
    const { objectStoreUrl, dbPath } = await resolveObjectStoreOptions(options);
    const objectStore = ObjectStore.resolve(objectStoreUrl);

    const dbBuilder = new DbBuilder(dbPath, objectStore);
    const adminBuilder = new AdminBuilder(dbPath, objectStore);
    try {
      const db = await dbBuilder.build();
      const admin = adminBuilder.build();
      return new SlateDBKVStore(objectStoreUrl, dbPath, objectStore, db, admin);
    } finally {
      dbBuilder.dispose();
      adminBuilder.dispose();
    }
  }

  async get<T = unknown>(key: SlateDBKey): Promise<T | undefined> {
    return getFrom<T>(this.db, key);
  }

  async put(key: SlateDBKey, value: unknown): Promise<void> {
    await this.db.put(toKeyBytes(key), encodeValue(value));
  }

  async delete(key: SlateDBKey): Promise<void> {
    await this.db.delete(toKeyBytes(key));
  }

  async deletePrefix(prefix: SlateDBKey, options: { limit?: number } = {}): Promise<number> {
    const deletes: SlateDBWriteOperation[] = [];
    for await (const entry of this.scanPrefix(prefix, options)) {
      deletes.push({ type: 'delete', key: entry.key });
    }
    await this.write(deletes);
    return deletes.length;
  }

  async write(operations: SlateDBWriteOperation[]): Promise<void> {
    if (operations.length == 0) {
      return;
    }

    const batch = new WriteBatch();
    try {
      for (const operation of operations) {
        if (operation.type == 'put') {
          batch.put(toKeyBytes(operation.key), encodeValue(operation.value));
        } else {
          batch.delete(toKeyBytes(operation.key));
        }
      }
      await this.db.write(batch);
    } finally {
      batch.dispose();
    }
  }

  scanPrefix<T = unknown>(prefix: SlateDBKey, options?: { limit?: number }): AsyncIterable<SlateDBEntry<T>> {
    return scanPrefixFrom<T>(this.db, prefix, options);
  }

  async createCheckpoint(
    options: { name?: string; lifetimeMs?: number | bigint } = {}
  ): Promise<CheckpointCreateResult> {
    await this.db.flush_with_options({ flush_type: FlushType.MemTable });
    return this.admin.create_detached_checkpoint({
      name: options.name,
      lifetime_ms: options.lifetimeMs,
      source: undefined
    });
  }

  async listCheckpoints(name?: string): Promise<Checkpoint[]> {
    return this.admin.list_checkpoints(name);
  }

  async deleteCheckpoint(id: string): Promise<void> {
    await this.admin.delete_checkpoint(id);
  }

  async withCheckpoint<T>(checkpointId: string, callback: (reader: SlateDBCheckpointReader) => Promise<T>): Promise<T> {
    const readerBuilder = new DbReaderBuilder(this.dbPath, this.objectStore);
    let reader: DbReader | undefined;
    try {
      readerBuilder.with_checkpoint_id(checkpointId);
      reader = await readerBuilder.build();
      return await callback(new SlateDBCheckpointReader(reader));
    } finally {
      if (reader != null) {
        await reader.shutdown();
        reader.dispose();
      }
      readerBuilder.dispose();
    }
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.db.shutdown();
    this.admin.dispose();
    this.db.dispose();
    this.objectStore.dispose();
  }
}

async function resolveObjectStoreOptions(options: SlateDBKVStoreOptions): Promise<{
  objectStoreUrl: string;
  dbPath: string;
}> {
  const dbPath = options.dbPath ?? DEFAULT_DB_PATH;
  const objectStore = options.objectStore;

  if (objectStore == null) {
    if (options.path == null) {
      throw new Error(`SlateDB storage requires either path or object_store`);
    }
    return resolveFileObjectStore(options.path, dbPath);
  }

  if ('url' in objectStore) {
    return {
      objectStoreUrl: objectStore.url,
      dbPath
    };
  }

  if (objectStore.type == 'memory') {
    return {
      objectStoreUrl: 'memory:///',
      dbPath
    };
  }

  if (objectStore.type == 'file') {
    return resolveFileObjectStore(objectStore.path, dbPath);
  }

  return {
    objectStoreUrl: `s3://${objectStore.bucket}`,
    dbPath: joinObjectStorePath(objectStore.prefix, dbPath)
  };
}

async function resolveFileObjectStore(
  root: string,
  dbPath: string
): Promise<{ objectStoreUrl: string; dbPath: string }> {
  const rootPath = path.resolve(root);
  await fs.mkdir(rootPath, { recursive: true });
  return {
    objectStoreUrl: LOCAL_FILE_OBJECT_STORE_URL,
    dbPath: path.join(rootPath, dbPath).replace(/^\/+/, '')
  };
}

function joinObjectStorePath(...segments: Array<string | undefined>): string {
  return segments
    .filter((segment): segment is string => segment != null && segment.length > 0)
    .map((segment) => segment.replace(/^\/+|\/+$/g, ''))
    .filter((segment) => segment.length > 0)
    .join('/');
}

export class SlateDBCheckpointReader {
  constructor(private readonly reader: DbReader) {}

  async get<T = unknown>(key: SlateDBKey): Promise<T | undefined> {
    return getFrom<T>(this.reader, key);
  }

  scanPrefix<T = unknown>(prefix: SlateDBKey, options?: { limit?: number }): AsyncIterable<SlateDBEntry<T>> {
    return scanPrefixFrom<T>(this.reader, prefix, options);
  }
}

export function storageKey(...segments: Array<string | number | bigint>): string {
  return ['ps', 'v3', ...segments.map((segment) => encodeURIComponent(String(segment)))].join('/');
}

export function storagePrefix(...segments: Array<string | number | bigint>): string {
  return `${storageKey(...segments)}/`;
}

export function encodeOpId(opId: bigint | number): string {
  const value = BigInt(opId);
  if (value < 0n) {
    throw new RangeError(`op id must be non-negative: ${value}`);
  }
  return value.toString(16).padStart(32, '0');
}

export function encodeValue(value: unknown): Uint8Array {
  return textEncoder.encode(JSON.stringify(value, jsonReplacer));
}

export function decodeValue<T = unknown>(value: Uint8Array): T {
  return JSON.parse(textDecoder.decode(value), jsonReviver) as T;
}

async function getFrom<T>(reader: SlateDBReadable, key: SlateDBKey): Promise<T | undefined> {
  const value = await reader.get(toKeyBytes(key));
  return value == null ? undefined : decodeValue<T>(value);
}

async function* scanPrefixFrom<T>(
  reader: SlateDBReadable,
  prefix: SlateDBKey,
  options: { limit?: number } = {}
): AsyncIterable<SlateDBEntry<T>> {
  const iterator = await reader.scan_prefix(toKeyBytes(prefix), unboundedRange());
  try {
    let count = 0;
    while (options.limit == null || count < options.limit) {
      const next = await iterator.next();
      if (next == null) {
        return;
      }
      count++;
      yield {
        key: next.key,
        keyText: textDecoder.decode(next.key),
        value: decodeValue<T>(next.value)
      };
    }
  } finally {
    iterator.dispose();
  }
}

function toKeyBytes(key: SlateDBKey): Uint8Array {
  return typeof key == 'string' ? textEncoder.encode(key) : key;
}

function unboundedRange(): KeyRange {
  return {
    start: undefined,
    start_inclusive: false,
    end: undefined,
    end_inclusive: false
  };
}

function jsonReplacer(_key: string, value: unknown): unknown {
  if (typeof value == 'bigint') {
    return { __slatedb_bigint: value.toString() };
  }
  if (value instanceof Uint8Array) {
    return { __slatedb_bytes: Buffer.from(value).toString('base64url') };
  }
  return value;
}

function jsonReviver(_key: string, value: unknown): unknown {
  if (isTaggedValue(value, '__slatedb_bigint')) {
    return BigInt(value.__slatedb_bigint);
  }
  if (isTaggedValue(value, '__slatedb_bytes')) {
    return new Uint8Array(Buffer.from(value.__slatedb_bytes, 'base64url'));
  }
  return value;
}

function isTaggedValue(
  value: unknown,
  tag: '__slatedb_bigint' | '__slatedb_bytes'
): value is Record<typeof tag, string> {
  return (
    typeof value == 'object' &&
    value != null &&
    tag in value &&
    typeof (value as Record<typeof tag, unknown>)[tag] == 'string'
  );
}
