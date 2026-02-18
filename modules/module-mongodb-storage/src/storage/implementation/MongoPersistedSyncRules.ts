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

export class MongoPersistedSyncRules implements storage.PersistedSyncRules {
  public readonly slot_name: string;
  public readonly hydrationState: HydrationState;

  constructor(
    public readonly id: number,
    public readonly sync_rules: SyncConfigWithErrors,
    public readonly checkpoint_lsn: string | null,
    slot_name: string | null,
    public readonly mapping: BucketDefinitionMapping
  ) {
    this.slot_name = slot_name ?? `powersync_${id}`;
    if (!this.sync_rules.config.compatibility.isEnabled(CompatibilityOption.versionedBucketIds)) {
      this.hydrationState = DEFAULT_HYDRATION_STATE;
    } else if (this.mapping == null) {
      this.hydrationState = versionedHydrationState(this.id);
    } else {
      this.hydrationState = new MongoHydrationState(this.mapping);
    }
  }

  hydratedSyncRules(): HydratedSyncRules {
    return this.sync_rules.config.hydrate({ hydrationState: this.hydrationState });
  }
}

class MongoHydrationState implements HydrationState {
  constructor(private mapping: BucketDefinitionMapping) {}

  getBucketSourceScope(source: BucketDataSource): BucketDataScope {
    const defId = this.mapping.bucketSourceId(source);
    return {
      bucketPrefix: defId.toString(16),
      source: source
    };
  }
  getParameterIndexLookupScope(source: ParameterIndexLookupCreator) {
    const defId = this.mapping.parameterLookupId(source);
    return {
      lookupName: defId.toString(16),
      queryId: '',
      source
    };
  }
}
