import { modules, system } from '@powersync/service-core';
import * as pg_types from '@powersync/service-module-postgres/types';
import { PostgresMigrationAgent } from '../migrations/PostgresMigrationAgent.js';
import { PostgresStorageProvider } from '../storage/PostgresStorageProvider.js';

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

    if (pg_types.isPostgresConfig(context.configuration.storage)) {
      context.migrations.registerMigrationAgent(new PostgresMigrationAgent(context.configuration.storage));
    }
  }

  async teardown(): Promise<void> {
    // Teardown for this module is implemented in the storage engine
  }
}
