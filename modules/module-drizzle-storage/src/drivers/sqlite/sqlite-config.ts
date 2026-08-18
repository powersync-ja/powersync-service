import Database from 'better-sqlite3';
import type { ExtractTablesWithRelations } from 'drizzle-orm';
import type { BetterSQLiteTransaction } from 'drizzle-orm/better-sqlite3';
import { drizzle, type BetterSQLite3Database } from 'drizzle-orm/better-sqlite3';
import type { NormalizedDrizzleSqliteStorageConfig } from '../../types/types.js';
import { sqliteSchema } from './schema.js';

export type DrizzleStorageDatabase = BetterSQLite3Database<typeof sqliteSchema> & {
  $client: Database.Database;
};
export type DrizzleStorageTransaction = BetterSQLiteTransaction<
  typeof sqliteSchema,
  ExtractTablesWithRelations<typeof sqliteSchema>
>;

export interface SqliteDrizzleRuntime {
  readonly db: DrizzleStorageDatabase;
  readonly client: Database.Database;
  readonly readers: readonly DrizzleStorageDatabase[];
  read(): DrizzleStorageDatabase;
  transaction<T>(callback: (tx: DrizzleStorageTransaction) => T): T;
  close(): void;
}

export function createSqliteDrizzleRuntime(config: NormalizedDrizzleSqliteStorageConfig): SqliteDrizzleRuntime {
  const fileBacked = config.filename != ':memory:';
  const client = openConnection(config.filename, fileBacked);
  const db = drizzle(client, { schema: sqliteSchema });
  const readerCount = fileBacked ? Math.max(0, config.max_pool_size - 1) : 0;
  const readerClients = Array.from({ length: readerCount }, () => openConnection(config.filename, true, true));
  const readers = readerClients.map((reader) => drizzle(reader, { schema: sqliteSchema }));
  let nextReader = 0;

  return {
    db,
    client,
    readers,
    read() {
      if (readers.length == 0) {
        return db;
      }
      const reader = readers[nextReader % readers.length];
      nextReader++;
      return reader;
    },
    transaction(callback) {
      return db.transaction(callback);
    },
    close() {
      for (const reader of readerClients) {
        reader.close();
      }
      client.close();
    }
  };
}

function openConnection(filename: string, enableWal: boolean, readonly = false): Database.Database {
  const client = new Database(filename, { readonly, fileMustExist: readonly });
  client.defaultSafeIntegers(true);
  if (enableWal && !readonly) {
    client.pragma('journal_mode = WAL');
  }
  client.pragma('busy_timeout = 5000');
  return client;
}
