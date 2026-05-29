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
  SyncConfigWithErrors,
  versionedHydrationState
} from '@powersync/service-sync-rules';
import { BucketDefinitionMapping } from './BucketDefinitionMapping.js';
import { StorageConfig } from './models.js';

export class MongoPersistedSyncRules implements storage.PersistedSyncRules {
  public readonly hydrationState: HydrationState;

  constructor(
    public readonly id: number,
    public readonly syncConfigWithErrors: SyncConfigWithErrors,
    public readonly slot_name: string,
    private readonly mapping: BucketDefinitionMapping | null,
    private readonly storageConfig: StorageConfig
  ) {
    if (this.storageConfig.incrementalReprocessing) {
      if (this.mapping == null) {
        throw new ServiceAssertionError(`mapping is required for v3 storage`);
      }
      this.hydrationState = new MongoHydrationState(this.mapping, this.id);
    } else if (
      !this.syncConfigWithErrors.config.compatibility.isEnabled(CompatibilityOption.versionedBucketIds) &&
      !this.storageConfig.versionedBuckets
    ) {
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
  constructor(
    private readonly mapping: BucketDefinitionMapping,
    private readonly version: number
  ) {}

  getBucketSourceScope(source: BucketDataSource): BucketDataScope {
    const defId = this.mapping.bucketSourceId(source);
    // Keep this aligned with versionedHydrationState() for now.
    //
    // Previous Mongo-specific behavior:
    // return {
    //   bucketPrefix: defId,
    //   source
    // };
    return {
      key: defId,
      bucketPrefix: `${this.version}#${source.uniqueName}`,
      source
    };
  }

  getParameterIndexLookupScope(source: ParameterIndexLookupCreator) {
    const defId = this.mapping.parameterLookupId(source);
    return {
      key: defId,
      lookupName: defId,
      queryId: '',
      source
    };
  }
}
