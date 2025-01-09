import { modules, system } from '@powersync/service-core';

import { PostgresMigrationAgent } from '../migrations/PostgresMigrationAgent.js';
import { PostgresStorageProvider } from '../storage/PostgresStorageProvider.js';
import { isPostgresStorageConfig, PostgresStorageConfig } from '../types/types.js';

export class PostgresStorageModule extends modules.AbstractModule {
  constructor() {
    super({
      name: 'Postgres Bucket Storage'
    });
  }

  async initialize(context: system.ServiceContextContainer): Promise<void> {
    const { storageEngine } = context;

    // Register the ability to use Postgres as a BucketStorage
    storageEngine.registerProvider(new PostgresStorageProvider());

    if (isPostgresStorageConfig(context.configuration.storage)) {
      context.migrations.registerMigrationAgent(
        new PostgresMigrationAgent(PostgresStorageConfig.decode(context.configuration.storage))
      );
    }
  }

  async teardown(): Promise<void> {
    // Teardown for this module is implemented in the storage engine
  }
}
