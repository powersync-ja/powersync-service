import { modules, system } from '@powersync/service-core';

import { SlateDBStorageProvider } from '../storage/storage-index.js';

export class SlateDBStorageModule extends modules.AbstractModule {
  constructor() {
    super({
      name: 'SlateDB Bucket Storage'
    });
  }

  async initialize(context: system.ServiceContextContainer): Promise<void> {
    context.storageEngine.registerProvider(new SlateDBStorageProvider());
  }

  async teardown(): Promise<void> {
    // Teardown for this module is implemented in the storage engine.
  }
}
