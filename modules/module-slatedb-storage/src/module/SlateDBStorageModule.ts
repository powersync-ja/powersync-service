import { modules, system } from '@powersync/service-core';

import { SlateDBMigrationAgent } from '../migrations/SlateDBMigrationAgent.js';
import { SlateDBStorageProvider } from '../storage/storage-index.js';
import { isSlateDBStorageConfig } from '../types/types.js';

export class SlateDBStorageModule extends modules.AbstractModule {
  constructor() {
    super({
      name: 'SlateDB Bucket Storage'
    });
  }

  async initialize(context: system.ServiceContextContainer): Promise<void> {
    context.storageEngine.registerProvider(new SlateDBStorageProvider());

    if (isSlateDBStorageConfig(context.configuration.storage)) {
      context.migrations.registerMigrationAgent(new SlateDBMigrationAgent());
    }
  }

  async teardown(): Promise<void> {
    // Teardown for this module is implemented in the storage engine.
  }
}
