import { framework, PowerSyncMigrationManager, ServiceContext, TestStorageOptions } from '@powersync/service-core';
import { PostgresMigrationAgent } from '../migrations/PostgresMigrationAgent.js';
import { normalizePostgresStorageConfig, PostgresStorageConfigDecoded } from '../types/types.js';
import { PostgresReportStorage } from '../storage/PostgresReportStorage.js';
import { PostgresBucketStorageFactory } from '../storage/PostgresBucketStorageFactory.js';
import { logger as defaultLogger, createLogger, transports } from '@powersync/lib-services-framework';

export type PostgresTestStorageOptions = {
  url: string;
  /**
   * Vitest can cause issues when loading .ts files for migrations.
   * This allows for providing a custom PostgresMigrationAgent.
   */
  migrationAgent?: (config: PostgresStorageConfigDecoded) => PostgresMigrationAgent;
};

export function postgresTestSetup(factoryOptions: PostgresTestStorageOptions) {
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

    // Migration logs can get really verbose in tests, so only log warnings and up.
    const logger = createLogger({
      level: 'warn',
      format: defaultLogger.format,
      transports: [new transports.Console()]
    });

    await migrationManager.migrate({
      direction: framework.migrations.Direction.Down,
      migrationContext: {
        service_context: mockServiceContext
      },
      logger
    });

    if (direction == framework.migrations.Direction.Up) {
      await migrationManager.migrate({
        direction: framework.migrations.Direction.Up,
        migrationContext: {
          service_context: mockServiceContext
        },
        logger
      });
    }
  };

  return {
    reportFactory: async (options?: TestStorageOptions) => {
      try {
        if (!options?.doNotClear) {
          await migrate(framework.migrations.Direction.Up);
        }

        return new PostgresReportStorage({
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
    migrate,
    tableIdStrings: true
  };
}

export function postgresTestStorageFactoryGenerator(factoryOptions: PostgresTestStorageOptions) {
  return postgresTestSetup(factoryOptions).factory;
}
