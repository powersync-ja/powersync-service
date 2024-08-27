import { LifeCycledSystem } from '@powersync/lib-services-framework';
import { ResolvedPowerSyncConfig } from '../util/util-index.js';
import { BucketStorageFactory } from './BucketStorage.js';
import { BucketStorageProvider, GeneratedStorage } from './StorageProvider.js';

export type StorageFactoryProviderOptions = {
  configuration: ResolvedPowerSyncConfig;
  lifecycle_engine: LifeCycledSystem;
};

export class StorageFactoryProvider {
  // TODO: This will need to revisited when we actually support multiple storage providers.
  protected storageProviders: Map<string, BucketStorageProvider>;
  protected generatedStorage: GeneratedStorage | null;

  constructor(options: StorageFactoryProviderOptions) {
    this.storageProviders = new Map();
    this.generatedStorage = null;

    // This will create the relevant storage provider when the system is started.
    options.lifecycle_engine.withLifecycle(null, {
      start: async () => {
        const { configuration } = options;
        const generatedStorage = await this.storageProviders.get(configuration.storage.type)!.generate({
          resolved_config: configuration
        });

        this.generatedStorage = generatedStorage;
      },
      stop: () => this.generatedStorage?.disposer()
    });
  }

  get bucketStorage(): BucketStorageFactory {
    if (!this.generatedStorage) {
      throw new Error(`storage has not been initialized yet.`);
    }
    return this.generatedStorage.storage;
  }

  /**
   * Register a provider which generates a {@link BucketStorageFactory}
   * given the matching config specified in the loaded {@link ResolvedPowerSyncConfig}
   */
  registerProvider(provider: BucketStorageProvider) {
    this.storageProviders.set(provider.type, provider);
  }
}
