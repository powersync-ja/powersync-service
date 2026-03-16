import {
  BucketDataScope,
  BucketDataSource,
  CompatibilityOption,
  DEFAULT_HYDRATION_STATE,
  HydratedSyncRules,
  HydrationState,
  ParameterIndexLookupCreator,
  SyncConfigWithErrors,
  versionedHydrationState
} from '@powersync/service-sync-rules';
import { storage } from '@powersync/service-core';
import { BucketDefinitionMapping } from './BucketDefinitionMapping.js';
import { StorageConfig } from './models.js';

export class MongoPersistedSyncRules implements storage.PersistedSyncRules {
  public readonly hydrationState: HydrationState;

  constructor(
    public readonly id: number,
    public readonly sync_rules: SyncConfigWithErrors,
    public readonly slot_name: string,
    private readonly mapping: BucketDefinitionMapping | null,
    private readonly storageConfig: StorageConfig
  ) {
    if (this.mapping != null && this.storageConfig.incrementalReprocessing) {
      // FIXME: Recheck bucket name generation again when we get to merging sync config versions.
      // this.hydrationState = new MongoHydrationState(this.mapping);
      this.hydrationState = versionedHydrationState(this.id);
    } else if (
      !this.sync_rules.config.compatibility.isEnabled(CompatibilityOption.versionedBucketIds) &&
      !this.storageConfig.versionedBuckets
    ) {
      this.hydrationState = DEFAULT_HYDRATION_STATE;
    } else {
      this.hydrationState = versionedHydrationState(this.id);
    }
  }

  hydratedSyncRules(): HydratedSyncRules {
    return this.sync_rules.config.hydrate({ hydrationState: this.hydrationState });
  }
}

class MongoHydrationState implements HydrationState {
  constructor(private readonly mapping: BucketDefinitionMapping) {}

  getBucketSourceScope(source: BucketDataSource): BucketDataScope {
    const defId = this.mapping.bucketSourceId(source);
    return {
      bucketPrefix: defId,
      source
    };
  }

  getParameterIndexLookupScope(source: ParameterIndexLookupCreator) {
    const defId = this.mapping.parameterLookupId(source);
    return {
      lookupName: defId,
      queryId: '',
      source
    };
  }
}
