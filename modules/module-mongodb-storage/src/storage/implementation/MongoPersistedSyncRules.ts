import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import {
  BucketDataScope,
  BucketDataSource,
  CompatibilityOption,
  DEFAULT_HYDRATION_STATE,
  HydratedSyncConfig,
  HydrationState,
  ParameterIndexLookupCreator,
  ParameterLookupScope,
  SyncConfigWithErrors,
  versionedHydrationState
} from '@powersync/service-sync-rules';
import { BucketDefinitionMapping } from './BucketDefinitionMapping.js';
import { StorageConfig } from './models.js';

export interface SyncConfigWithOptionalMapping {
  syncConfig: SyncConfigWithErrors;
  mapping: BucketDefinitionMapping | null;
}

export interface SyncConfigWithMapping {
  syncConfig: SyncConfigWithErrors;
  mapping: BucketDefinitionMapping;
}

export class MongoPersistedSyncRules implements storage.PersistedSyncRules {
  public readonly hydrationState: HydrationState;
  public readonly syncConfigWithErrors: SyncConfigWithErrors;
  public readonly slot_name: string;
  private readonly storageConfig: StorageConfig;

  constructor(
    public readonly id: number,
    storageConfig: StorageConfig,
    slotName: string,
    syncConfigs: SyncConfigWithOptionalMapping[]
  ) {
    this.slot_name = slotName;
    this.storageConfig = storageConfig;
    // FIXME: Updat the interface to support multiple sync configs
    this.syncConfigWithErrors = syncConfigs[0].syncConfig;
    // Compatibility must match between them all.
    const compatibility = this.syncConfigWithErrors.config.compatibility;

    if (storageConfig.incrementalReprocessing) {
      if (syncConfigs.some((c) => c.mapping == null)) {
        throw new ServiceAssertionError(`mapping is required for v3 storage`);
      }
      this.hydrationState = new MongoHydrationState(syncConfigs as SyncConfigWithMapping[], this.id);
    } else if (!compatibility.isEnabled(CompatibilityOption.versionedBucketIds) && !storageConfig.versionedBuckets) {
      this.hydrationState = DEFAULT_HYDRATION_STATE;
    } else {
      this.hydrationState = versionedHydrationState(this.id);
    }
  }

  hydratedSyncConfig(): HydratedSyncConfig {
    return this.syncConfigWithErrors.config.hydrate({ hydrationState: this.hydrationState });
  }
}

class MongoHydrationState implements HydrationState {
  private bucketDataSourceSyncConfig = new WeakMap<BucketDataSource, SyncConfigWithMapping>();
  private parameterIndexLookupSyncConfig = new WeakMap<ParameterIndexLookupCreator, SyncConfigWithMapping>();

  constructor(
    private readonly syncConfigs: SyncConfigWithMapping[],
    private readonly version: number
  ) {
    for (let syncConfig of syncConfigs) {
      for (let source of syncConfig.syncConfig.config.bucketDataSources) {
        this.bucketDataSourceSyncConfig.set(source, syncConfig);
      }
      for (let source of syncConfig.syncConfig.config.bucketParameterLookupSources) {
        this.parameterIndexLookupSyncConfig.set(source, syncConfig);
      }
    }
  }

  getBucketSourceScope(source: BucketDataSource): BucketDataScope {
    const syncConfig = this.bucketDataSourceSyncConfig.get(source);
    if (syncConfig == null) {
      throw new ServiceAssertionError(`No sync config found for bucket data source ${source.uniqueName}`);
    }
    const mapping = syncConfig.mapping;
    const defId = mapping.bucketSourceId(source);
    // Keep this aligned with versionedHydrationState() for now.
    //
    // Previous Mongo-specific behavior:
    // return {
    //   bucketPrefix: defId,
    //   source
    // };
    // FIXME: Should this use defId, or is uniqueName constant?
    return {
      bucketPrefix: `${this.version}#${source.uniqueName}`,
      source
    };
  }

  getParameterIndexLookupScope(source: ParameterIndexLookupCreator): ParameterLookupScope {
    const syncConfig = this.parameterIndexLookupSyncConfig.get(source);
    if (syncConfig == null) {
      throw new ServiceAssertionError(
        `No sync config found for parameter index lookup source ${source.sourceId.lookupName}#${source.sourceId.queryId}`
      );
    }
    const mapping = syncConfig.mapping;
    const defId = mapping.parameterLookupId(source);
    return {
      lookupName: defId,
      queryId: '',
      source
    };
  }
}
