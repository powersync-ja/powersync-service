import { logger } from '@powersync/lib-services-framework';
import { ResolvedPowerSyncConfig } from '../util/util-index.js';
import { BucketStorageFactory } from './BucketStorage.js';
import { ReplicationEventManager } from './ReplicationEventManager.js';
import { ActiveStorage, BucketStorageProvider, StorageSettings } from './StorageProvider.js';
import { DEFAULT_WRITE_CHECKPOINT_MODE } from './write-checkpoint.js';

export type StorageEngineOptions = {
  configuration: ResolvedPowerSyncConfig;
};

export const DEFAULT_STORAGE_SETTINGS: StorageSettings = {
  writeCheckpointMode: DEFAULT_WRITE_CHECKPOINT_MODE
};

export class StorageEngine {
  // TODO: This will need to revisited when we actually support multiple storage providers.
  private storageProviders: Map<string, BucketStorageProvider> = new Map();
  private currentActiveStorage: ActiveStorage | null = null;
  readonly events: ReplicationEventManager;

  private _activeSettings: StorageSettings;

  constructor(private options: StorageEngineOptions) {
    this.events = new ReplicationEventManager();
    this._activeSettings = DEFAULT_STORAGE_SETTINGS;
  }

  get activeBucketStorage(): BucketStorageFactory {
    return this.activeStorage.storage;
  }

  get activeStorage(): ActiveStorage {
    if (!this.currentActiveStorage) {
      throw new Error(`No storage provider has been initialized yet.`);
    }

    return this.currentActiveStorage;
  }

  get activeSettings(): StorageSettings {
    return { ...this._activeSettings };
  }

  updateSettings(settings: Partial<StorageSettings>) {
    if (this.currentActiveStorage) {
      throw new Error(`Storage is already active, settings cannot be modified.`);
    }
    this._activeSettings = {
      ...this._activeSettings,
      ...settings
    };
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
      resolvedConfig: configuration,
      eventManager: this.events,
      ...this.activeSettings
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
