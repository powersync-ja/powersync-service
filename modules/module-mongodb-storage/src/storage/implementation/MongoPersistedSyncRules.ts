import * as sqlite from 'node:sqlite';

import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import {
  BucketDataScope,
  BucketDataSource,
  CompatibilityOption,
  DEFAULT_HYDRATION_STATE,
  HydratedSyncConfig,
  HydrationState,
  nodeSqlite,
  ParameterIndexLookupCreator,
  ParameterLookupScope,
  versionedHydrationState
} from '@powersync/service-sync-rules';
import {
  BucketDefinitionMapping,
  MultiSyncConfigBucketDefinitionMapping,
  SyncConfigWithMapping,
  SyncConfigWithRequiredMapping
} from './BucketDefinitionMapping.js';
import { StorageConfig } from './models.js';

export class MongoPersistedSyncRules implements storage.PersistedSyncRules {
  public readonly hydrationState: HydrationState;
  public readonly syncConfigs: storage.PersistedSyncRules['syncConfigs'];
  public readonly slot_name: string;
  public readonly mapping: BucketDefinitionMapping;

  constructor(
    public readonly id: number,
    storageConfig: StorageConfig,
    slotName: string,
    syncConfigs: SyncConfigWithMapping[]
  ) {
    this.slot_name = slotName;
    this.syncConfigs = syncConfigs.map((config) => config.syncConfig);
    if (this.syncConfigs.length == 0) {
      throw new ServiceAssertionError(`At least one sync config is required`);
    }
    const [firstConfig] = this.syncConfigs;
    const compatibility = firstConfig.config.compatibility;
    for (const config of this.syncConfigs) {
      if (config.config.compatibility.equals(compatibility)) {
        continue;
      }
      throw new ServiceAssertionError(
        `All sync configs in a replication stream must use the same compatibility options`
      );
    }

    if (storageConfig.incrementalReprocessing) {
      if (syncConfigs.some((c) => c.mapping == null)) {
        throw new ServiceAssertionError(`mapping is required for v3 storage`);
      }
      const mappedConfigs = syncConfigs as SyncConfigWithRequiredMapping[];
      this.hydrationState = new MongoHydrationState(mappedConfigs, this.id);
      this.mapping = new MultiSyncConfigBucketDefinitionMapping(mappedConfigs);
    } else if (!compatibility.isEnabled(CompatibilityOption.versionedBucketIds) && !storageConfig.versionedBuckets) {
      const [syncConfig] = syncConfigs;
      if (syncConfigs.length != 1 || syncConfig == null) {
        throw new ServiceAssertionError(`Non-incremental storage requires exactly one sync config`);
      }
      this.hydrationState = DEFAULT_HYDRATION_STATE;
      this.mapping = syncConfig.mapping ?? new BucketDefinitionMapping();
    } else {
      const [syncConfig] = syncConfigs;
      if (syncConfigs.length != 1 || syncConfig == null) {
        throw new ServiceAssertionError(`Non-incremental storage requires exactly one sync config`);
      }
      this.hydrationState = versionedHydrationState(this.id);
      this.mapping = syncConfig.mapping ?? new BucketDefinitionMapping();
    }
  }

  hydratedSyncConfig(): HydratedSyncConfig {
    return new HydratedSyncConfig({
      definitions: this.syncConfigs.map((config) => config.config),
      createParams: {
        hydrationState: this.hydrationState,
        sqlite: nodeSqlite(sqlite)
      }
    });
  }
}

class MongoHydrationState implements HydrationState {
  private bucketDataSourceSyncConfig = new WeakMap<BucketDataSource, SyncConfigWithRequiredMapping>();
  private parameterIndexLookupSyncConfig = new WeakMap<ParameterIndexLookupCreator, SyncConfigWithRequiredMapping>();

  constructor(
    readonly syncConfigs: SyncConfigWithRequiredMapping[],
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
      // Keep this aligned with versionedHydrationState() for now.
      // May consider changing the format before stable release, e.g. bucketPrefix: defId
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
