import { framework, PowerSyncMigrationManager, ServiceContext, storage } from '@powersync/service-core';
import path from 'path';
import { fileURLToPath } from 'url';
import { normalizePostgresStorageConfig } from '../../src//types/types.js';
import { PostgresMigrationAgent } from '../../src/migrations/PostgresMigrationAgent.js';
import { PostgresBucketStorageFactory } from '../../src/storage/PostgresBucketStorageFactory.js';
import { env } from './env.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export const TEST_URI = env.PG_STORAGE_TEST_URL;

const BASE_CONFIG = {
  type: 'postgresql' as const,
  uri: TEST_URI,
  sslmode: 'disable' as const
};

export const TEST_CONNECTION_OPTIONS = normalizePostgresStorageConfig(BASE_CONFIG);

/**
 * Vitest tries to load the migrations via .ts files which fails.
 * For tests this links to the relevant .js files correctly
 */
class TestPostgresMigrationAgent extends PostgresMigrationAgent {
  getInternalScriptsDir(): string {
    return path.resolve(__dirname, '../../dist/migrations/scripts');
  }
}

export const POSTGRES_STORAGE_FACTORY = async (options?: storage.TestStorageOptions) => {
  const migrationManager: PowerSyncMigrationManager = new framework.MigrationManager();
  await using migrationAgent = new TestPostgresMigrationAgent(BASE_CONFIG);
  migrationManager.registerMigrationAgent(migrationAgent);

  const mockServiceContext = { configuration: { storage: BASE_CONFIG } } as unknown as ServiceContext;

  if (!options?.doNotClear) {
    await migrationManager.migrate({
      direction: framework.migrations.Direction.Down,
      migrationContext: {
        service_context: mockServiceContext
      }
    });
  }

  // In order to run up migration after
  await migrationAgent.resetStore();

  await migrationManager.migrate({
    direction: framework.migrations.Direction.Up,
    migrationContext: {
      service_context: mockServiceContext
    }
  });

  return new PostgresBucketStorageFactory({
    config: TEST_CONNECTION_OPTIONS,
    slot_name_prefix: 'test_'
  });
};
