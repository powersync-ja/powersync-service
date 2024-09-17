import { ResolvedPowerSyncConfig } from '../util/util-index.js';
import { BucketStorageFactory } from './BucketStorage.js';
import { BucketStorageProvider, ActiveStorage } from './StorageProvider.js';
import { logger } from '@powersync/lib-services-framework';

export type StorageEngineOptions = {
  configuration: ResolvedPowerSyncConfig;
};

export class StorageEngine {
  // TODO: This will need to revisited when we actually support multiple storage providers.
  private storageProviders: Map<string, BucketStorageProvider> = new Map();
  private currentActiveStorage: ActiveStorage | null = null;

  constructor(private options: StorageEngineOptions) {}

  get activeBucketStorage(): BucketStorageFactory {
    return this.activeStorage.storage;
  }

  get activeStorage(): ActiveStorage {
    if (!this.currentActiveStorage) {
      throw new Error(`No storage provider has been initialized yet.`);
    }

    return this.currentActiveStorage;
  }

  /**
   * Register a provider which generates a {@link BucketStorageFactory}
   * given the matching config specified in the loaded {@link ResolvedPowerSyncConfig}
   */
  registerProvider(provider: BucketStorageProvider) {
    this.storageProviders.set(provider.type, provider);
  }

  public async start(): Promise<void> {
    logger.info('Starting Storage Engine...');
    const { configuration } = this.options;
    this.currentActiveStorage = await this.storageProviders.get(configuration.storage.type)!.getStorage({
      resolvedConfig: configuration
    });
    logger.info(`Successfully activated storage: ${configuration.storage.type}.`);
    logger.info('Successfully started Storage Engine.');
  }

  /**
   *  Shutdown the storage engine, safely shutting down any activated storage providers.
   */
  public async shutDown(): Promise<void> {
    logger.info('Shutting down Storage Engine...');
    await this.currentActiveStorage?.shutDown();
    logger.info('Successfully shut down Storage Engine.');
  }
}
