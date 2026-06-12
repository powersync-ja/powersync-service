import { modules, system } from '@powersync/service-core';
import { MikroOrmMigrationAgent } from '../migrations/MikroOrmMigrationAgent.js';
import { MikroOrmStorageProvider } from '../storage/MikroOrmStorageProvider.js';
import { isMikroOrmSqliteStorageConfig, MikroOrmSqliteStorageConfig } from '../types/types.js';

export class MikroOrmStorageModule extends modules.AbstractModule {
  constructor() {
    super({
      name: 'MikroORM Bucket Storage'
    });
  }

  async initialize(context: system.ServiceContextContainer): Promise<void> {
    context.storageEngine.registerProvider(new MikroOrmStorageProvider());

    if (isMikroOrmSqliteStorageConfig(context.configuration.storage)) {
      context.migrations.registerMigrationAgent(
        new MikroOrmMigrationAgent(MikroOrmSqliteStorageConfig.decode(context.configuration.storage))
      );
    }
  }

  async teardown(): Promise<void> {
    // Teardown for this module is implemented by the storage engine.
  }
}
