import { modules, system } from '@powersync/service-core';
import { DrizzleMigrationAgent } from '../migrations/DrizzleMigrationAgent.js';
import { DrizzleStorageProvider } from '../storage/DrizzleStorageProvider.js';
import { DrizzleStorageConfig, isDrizzleStorageConfig } from '../types/types.js';

export class DrizzleStorageModule extends modules.AbstractModule {
  constructor() {
    super({ name: 'Drizzle Bucket Storage' });
  }

  async initialize(context: system.ServiceContextContainer): Promise<void> {
    context.storageEngine.registerProvider(new DrizzleStorageProvider());
    if (isDrizzleStorageConfig(context.configuration.storage)) {
      context.migrations.registerMigrationAgent(
        new DrizzleMigrationAgent(DrizzleStorageConfig.decode(context.configuration.storage))
      );
    }
  }

  async teardown(): Promise<void> {}
}
