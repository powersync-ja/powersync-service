import { LifeCycledSystem } from '@powersync/lib-services-framework';
import { ResolvedPowerSyncConfig } from '../util/util-index.js';
import { BucketStorageFactory } from './BucketStorage.js';
import { BucketStorageProvider, ActiveStorage } from './StorageProvider.js';

export type StorageEngineOptions = {
  configuration: ResolvedPowerSyncConfig;
  lifecycleEngine: LifeCycledSystem;
};

export class StorageEngine {
  // TODO: This will need to revisited when we actually support multiple storage providers.
  protected storageProviders: Map<string, BucketStorageProvider> = new Map();
  protected currentActiveStorage: ActiveStorage | null = null;

  constructor(options: StorageEngineOptions) {
    // This will create the relevant storage provider when the system is started.
    options.lifecycleEngine.withLifecycle(null, {
      start: async () => {
        const { configuration } = options;
        this.currentActiveStorage = await this.storageProviders.get(configuration.storage.type)!.getStorage({
          resolvedConfig: configuration
        });
      },
      stop: () => this.currentActiveStorage?.disposer()
    });
  }

  get activeBucketStorage(): BucketStorageFactory {
    if (!this.currentActiveStorage) {
      throw new Error(`No storage provider has been initialized yet.`);
    }
    return this.currentActiveStorage.storage;
  }

  /**
   * Register a provider which generates a {@link BucketStorageFactory}
   * given the matching config specified in the loaded {@link ResolvedPowerSyncConfig}
   */
  registerProvider(provider: BucketStorageProvider) {
    this.storageProviders.set(provider.type, provider);
  }
}
