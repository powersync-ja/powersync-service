import { framework, PowerSyncMigrationManager, ServiceContext, TestStorageOptions } from '@powersync/service-core';
import { PostgresMigrationAgent } from '../migrations/PostgresMigrationAgent.js';
import { normalizePostgresStorageConfig, PostgresStorageConfigDecoded } from '../types/types.js';
import { PostgresBucketStorageFactory } from './PostgresBucketStorageFactory.js';
import { PostgresReportStorageFactory } from './PostgresReportStorageFactory.js';

export type PostgresTestStorageOptions = {
  url: string;
  /**
   * Vitest can cause issues when loading .ts files for migrations.
   * This allows for providing a custom PostgresMigrationAgent.
   */
  migrationAgent?: (config: PostgresStorageConfigDecoded) => PostgresMigrationAgent;
};

export const postgresTestSetup = (factoryOptions: PostgresTestStorageOptions) => {
  const BASE_CONFIG = {
    type: 'postgresql' as const,
    uri: factoryOptions.url,
    sslmode: 'disable' as const
  };

  const TEST_CONNECTION_OPTIONS = normalizePostgresStorageConfig(BASE_CONFIG);

  const migrate = async (direction: framework.migrations.Direction) => {
    await using migrationManager: PowerSyncMigrationManager = new framework.MigrationManager();
    await using migrationAgent = factoryOptions.migrationAgent
      ? factoryOptions.migrationAgent(BASE_CONFIG)
      : new PostgresMigrationAgent(BASE_CONFIG);
    migrationManager.registerMigrationAgent(migrationAgent);

    const mockServiceContext = { configuration: { storage: BASE_CONFIG } } as unknown as ServiceContext;

    await migrationManager.migrate({
      direction: framework.migrations.Direction.Down,
      migrationContext: {
        service_context: mockServiceContext
      }
    });

    if (direction == framework.migrations.Direction.Up) {
      await migrationManager.migrate({
        direction: framework.migrations.Direction.Up,
        migrationContext: {
          service_context: mockServiceContext
        }
      });
    }
  };

  return {
    reportFactory: async (options?: TestStorageOptions) => {
      try {
        // if (!options?.doNotClear) {
        //   await migrate(framework.migrations.Direction.Up);
        // }

        return new PostgresReportStorageFactory({
          config: TEST_CONNECTION_OPTIONS
        });
      } catch (ex) {
        // Vitest does not display these errors nicely when using the `await using` syntx
        console.error(ex, ex.cause);
        throw ex;
      }
    },
    factory: async (options?: TestStorageOptions) => {
      try {
        if (!options?.doNotClear) {
          await migrate(framework.migrations.Direction.Up);
        }

        return new PostgresBucketStorageFactory({
          config: TEST_CONNECTION_OPTIONS,
          slot_name_prefix: 'test_'
        });
      } catch (ex) {
        // Vitest does not display these errors nicely when using the `await using` syntx
        console.error(ex, ex.cause);
        throw ex;
      }
    },
    migrate
  };
};

export const PostgresTestStorageFactoryGenerator = (factoryOptions: PostgresTestStorageOptions) => {
  return postgresTestSetup(factoryOptions).factory;
};
