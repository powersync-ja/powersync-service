import * as lib_mongo from '@powersync/lib-service-mongodb';
import * as core from '@powersync/service-core';
import { MongoMigrationAgent } from '../migrations/MongoMigrationAgent.js';
import { MongoStorageProvider } from '../storage/storage-index.js';
import * as types from '../types/types.js';

export class MongoStorageModule extends core.modules.AbstractModule {
  constructor() {
    super({
      name: 'MongoDB Storage'
    });
  }

  async initialize(context: core.system.ServiceContextContainer): Promise<void> {
    context.storageEngine.registerProvider(new MongoStorageProvider());

    if (types.isMongoStorageConfig(context.configuration.storage)) {
      context.migrations.registerMigrationAgent(
        new MongoMigrationAgent(this.resolveConfig(context.configuration.storage))
      );
    }
  }

  /**
   * Combines base config with normalized connection settings
   */
  private resolveConfig(config: types.MongoStorageConfig) {
    return {
      ...config,
      ...lib_mongo.normalizeMongoConfig(config)
    };
  }

  async teardown(options: core.modules.TearDownOptions): Promise<void> {
    // teardown is implemented in the storage engine
  }
}
