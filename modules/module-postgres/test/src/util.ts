import { PostgresRouteAPIAdapter } from '@module/api/PostgresRouteAPIAdapter.js';
import * as types from '@module/types/types.js';
import * as lib_postgres from '@powersync/lib-service-postgres';
import { logger } from '@powersync/lib-services-framework';
import {
  BucketStorageFactory,
  ReplicationCheckpoint,
  SUPPORTED_STORAGE_VERSIONS,
  TestStorageConfig,
  TestStorageFactory
} from '@powersync/service-core';
import * as pgwire from '@powersync/service-jpgwire';
import * as drizzle_storage from '@powersync/service-module-drizzle-storage';
import * as mikroorm_storage from '@powersync/service-module-mikroorm-storage';
import * as mongo_storage from '@powersync/service-module-mongodb-storage';
import * as postgres_storage from '@powersync/service-module-postgres-storage';
import { existsSync } from 'node:fs';
import { unlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, TestOptions } from 'vitest';
import { env } from './env.js';

export const TEST_URI = env.PG_TEST_URL;

export const INITIALIZED_MONGO_STORAGE_FACTORY = mongo_storage.test_utils.mongoTestStorageFactoryGenerator({
  url: env.MONGO_TEST_URL,
  isCI: env.CI
});

export const INITIALIZED_POSTGRES_STORAGE_FACTORY = postgres_storage.test_utils.postgresTestSetup({
  url: env.PG_STORAGE_TEST_URL
});

const DEFAULT_MIKROORM_SQLITE_STORAGE_FILENAME = join(
  tmpdir(),
  `powersync-postgres-replication-mikroorm-storage-${process.pid}-${process.env.VITEST_WORKER_ID ?? '0'}.sqlite`
);

const MIKROORM_SQLITE_STORAGE_FILENAME =
  env.MIKROORM_SQLITE_STORAGE_TEST_FILENAME || DEFAULT_MIKROORM_SQLITE_STORAGE_FILENAME;

const DEFAULT_DRIZZLE_SQLITE_STORAGE_FILENAME = join(
  tmpdir(),
  `powersync-postgres-replication-drizzle-storage-${process.pid}-${process.env.VITEST_WORKER_ID ?? '0'}.sqlite`
);
const DRIZZLE_SQLITE_STORAGE_FILENAME =
  env.DRIZZLE_SQLITE_STORAGE_TEST_FILENAME || DEFAULT_DRIZZLE_SQLITE_STORAGE_FILENAME;

export const INITIALIZED_MIKROORM_SQLITE_STORAGE_FACTORY: TestStorageConfig = {
  tableIdStrings: true,
  factory: async (options) => {
    if (!options?.doNotClear && existsSync(MIKROORM_SQLITE_STORAGE_FILENAME)) {
      await unlink(MIKROORM_SQLITE_STORAGE_FILENAME);
    }

    const config = mikroorm_storage.normalizeMikroOrmSqliteStorageConfig({
      type: mikroorm_storage.MIKRO_ORM_SQLITE_STORAGE_TYPE,
      filename: MIKROORM_SQLITE_STORAGE_FILENAME
    });
    const factory = await mikroorm_storage.createSqliteMikroOrmStorageFactory({
      config,
      slotNamePrefix: 'test_'
    });
    await factory.orm.schema.update();
    return factory;
  }
};

export const INITIALIZED_DRIZZLE_SQLITE_STORAGE_FACTORY: TestStorageConfig = {
  tableIdStrings: true,
  factory: async (options) => {
    if (!options?.doNotClear && existsSync(DRIZZLE_SQLITE_STORAGE_FILENAME)) {
      await unlink(DRIZZLE_SQLITE_STORAGE_FILENAME);
    }
    const factory = drizzle_storage.createSqliteDrizzleStorageFactory({
      config: drizzle_storage.normalizeDrizzleSqliteStorageConfig({
        type: drizzle_storage.DRIZZLE_SQLITE_STORAGE_TYPE,
        filename: DRIZZLE_SQLITE_STORAGE_FILENAME
      }),
      slotNamePrefix: 'test_'
    });
    drizzle_storage.runSqliteDrizzleMigrations(factory.runtime);
    return factory;
  }
};

const TEST_STORAGE_VERSIONS = SUPPORTED_STORAGE_VERSIONS;

export interface StorageVersionTestContext {
  factory: TestStorageFactory;
  storageVersion: number;
}

export function describeWithStorage(
  options: TestOptions & { storageVersions?: number[] },
  fn: (context: StorageVersionTestContext) => void
) {
  const storageVersions = options.storageVersions ?? TEST_STORAGE_VERSIONS;
  const describeFactory = (storageName: string, config: TestStorageConfig) => {
    describe(`${storageName} storage`, options, function () {
      for (const storageVersion of storageVersions) {
        describe(`storage v${storageVersion}`, function () {
          fn({
            factory: config.factory,
            storageVersion
          });
        });
      }
    });
  };

  if (env.TEST_MONGO_STORAGE) {
    describeFactory('mongodb', INITIALIZED_MONGO_STORAGE_FACTORY);
  }

  if (env.TEST_POSTGRES_STORAGE) {
    describeFactory('postgres', INITIALIZED_POSTGRES_STORAGE_FACTORY);
  }

  if (env.TEST_MIKROORM_SQLITE_STORAGE) {
    describeFactory('mikroorm sqlite', INITIALIZED_MIKROORM_SQLITE_STORAGE_FACTORY);
  }

  if (env.TEST_DRIZZLE_SQLITE_STORAGE) {
    describeFactory('drizzle sqlite', INITIALIZED_DRIZZLE_SQLITE_STORAGE_FACTORY);
  }
}

export const TEST_CONNECTION_OPTIONS = types.normalizeConnectionConfig({
  type: 'postgresql',
  uri: TEST_URI,
  sslmode: 'disable'
});

export async function clearTestDb(db: pgwire.PgClient) {
  await db.query(
    "select pg_drop_replication_slot(slot_name) from pg_replication_slots where active = false and slot_name like 'test_%'"
  );

  await db.query(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`);
  try {
    await db.query(`DROP PUBLICATION powersync`);
  } catch (e) {
    // Ignore
  }

  await db.query(`CREATE PUBLICATION powersync FOR ALL TABLES`);

  const tableRows = pgwire.pgwireRows(
    await db.query(`SELECT table_name FROM information_schema.tables where table_schema = 'public'`)
  );
  for (let row of tableRows) {
    const name = row.table_name;
    if (name.startsWith('test_')) {
      await db.query(`DROP TABLE public.${lib_postgres.escapeIdentifier(name)}`);
    }
  }

  const domainRows = pgwire.pgwireRows(
    await db.query(`
      SELECT typname,typtype
      FROM pg_type t
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
      WHERE n.nspname = 'public' AND typarray != 0
    `)
  );
  for (let row of domainRows) {
    if (row.typtype == 'd') {
      await db.query(`DROP DOMAIN public.${lib_postgres.escapeIdentifier(row.typname)} CASCADE`);
    } else {
      await db.query(`DROP TYPE public.${lib_postgres.escapeIdentifier(row.typname)} CASCADE`);
    }
  }
}

export async function connectPgWire(type?: 'replication' | 'standard') {
  const db = await pgwire.connectPgWire(TEST_CONNECTION_OPTIONS, { type, applicationName: 'powersync-tests' });
  return db;
}

export function connectPgPool() {
  const db = pgwire.connectPgWirePool(TEST_CONNECTION_OPTIONS);
  return db;
}

export async function getClientCheckpoint(
  db: pgwire.PgClient,
  storageFactory: BucketStorageFactory,
  options?: { timeout?: number }
): Promise<ReplicationCheckpoint> {
  const start = Date.now();

  const api = new PostgresRouteAPIAdapter(db);
  const lsn = await api.createReplicationHead(async (lsn) => lsn);

  // This old API needs a persisted checkpoint id.
  // Since we don't use LSNs anymore, the only way to get that is to wait.

  const timeout = options?.timeout ?? 50_000;

  logger.info(`Waiting for LSN checkpoint: ${lsn}`);
  while (Date.now() - start < timeout) {
    const storage = (await storageFactory.getActiveSyncConfig())?.storage;
    const cp = await storage?.getCheckpoint();

    if (cp?.lsn != null && cp.lsn >= lsn) {
      logger.info(`Got write checkpoint: ${lsn} : ${cp.checkpoint}`);
      return cp;
    }

    await new Promise((resolve) => setTimeout(resolve, 5));
  }

  throw new Error('Timeout while waiting for checkpoint');
}
