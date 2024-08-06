import { ResolvedPowerSyncConfig } from '../util/util-index.js';
import { GeneratedStorage, StorageProvider } from './StorageProvider.js';

export class StorageFactory {
  protected storageProviders: Map<string, StorageProvider>;

  constructor() {
    this.storageProviders = new Map();
  }

  /**
   * Register a provider which generates a {@link BucketStorageFactory}
   * given the matching config specified in the loaded {@link ResolvedPowerSyncConfig}
   */
  registerProvider(provider: StorageProvider) {
    this.storageProviders.set(provider.type, provider);
  }

  generateStorage(config: ResolvedPowerSyncConfig): Promise<GeneratedStorage> {
    return this.storageProviders.get(config.storage.type)!.generate({
      resolved_config: config
    });
  }
}
