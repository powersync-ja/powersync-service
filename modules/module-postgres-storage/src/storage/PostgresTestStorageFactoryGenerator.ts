import { framework, PowerSyncMigrationManager, ServiceContext, TestStorageOptions } from '@powersync/service-core';
import { PostgresMigrationAgent } from '../migrations/PostgresMigrationAgent.js';
import { normalizePostgresStorageConfig } from '../types/types.js';
import { PostgresBucketStorageFactory } from './PostgresBucketStorageFactory.js';

export type PostgresTestStorageOptions = {
  url: string;
};

export const PostgresTestStorageFactoryGenerator = (factoryOptions: PostgresTestStorageOptions) => {
  return async (options?: TestStorageOptions) => {
    const migrationManager: PowerSyncMigrationManager = new framework.MigrationManager();

    const BASE_CONFIG = {
      type: 'postgresql' as const,
      uri: factoryOptions.url,
      sslmode: 'disable' as const
    };

    const TEST_CONNECTION_OPTIONS = normalizePostgresStorageConfig(BASE_CONFIG);

    await using migrationAgent = new PostgresMigrationAgent(BASE_CONFIG);
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
};
