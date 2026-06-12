import { modules, system } from '@powersync/service-core';
import { MikroOrmMigrationAgent } from '../migrations/MikroOrmMigrationAgent.js';
import { MikroOrmStorageProvider } from '../storage/MikroOrmStorageProvider.js';
import {
  isMikroOrmStorageConfig,
  MIKRO_ORM_MYSQL_STORAGE_TYPE,
  MIKRO_ORM_SQLITE_STORAGE_TYPE,
  MikroOrmStorageConfig
} from '../types/types.js';

export class MikroOrmStorageModule extends modules.AbstractModule {
  constructor() {
    super({
      name: 'MikroORM Bucket Storage'
    });
  }

  async initialize(context: system.ServiceContextContainer): Promise<void> {
    context.storageEngine.registerProvider(new MikroOrmStorageProvider(MIKRO_ORM_SQLITE_STORAGE_TYPE));
    context.storageEngine.registerProvider(new MikroOrmStorageProvider(MIKRO_ORM_MYSQL_STORAGE_TYPE));

    if (isMikroOrmStorageConfig(context.configuration.storage)) {
      context.migrations.registerMigrationAgent(
        new MikroOrmMigrationAgent(MikroOrmStorageConfig.decode(context.configuration.storage))
      );
    }
  }

  async teardown(): Promise<void> {
    // Teardown for this module is implemented by the storage engine.
  }
}
