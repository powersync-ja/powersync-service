import {
  CompatibilityOption,
  HydratedSyncRules,
  SyncConfigWithErrors,
  versionedHydrationState
} from '@powersync/service-sync-rules';

import { storage } from '@powersync/service-core';
import { DEFAULT_HYDRATION_STATE, HydrationState } from '@powersync/service-sync-rules/src/HydrationState.js';
import { StorageConfig } from './models.js';

export class MongoPersistedSyncRules implements storage.PersistedSyncRules {
  public readonly slot_name: string;
  public readonly hydrationState: HydrationState;

  constructor(
    public readonly id: number,
    public readonly sync_rules: SyncConfigWithErrors,
    public readonly checkpoint_lsn: string | null,
    slot_name: string | null,
    public readonly storageConfig: StorageConfig
  ) {
    this.slot_name = slot_name ?? `powersync_${id}`;

    if (
      storageConfig.versionedBuckets ||
      this.sync_rules.config.compatibility.isEnabled(CompatibilityOption.versionedBucketIds)
    ) {
      // For new sync config versions (using the new storage version), we always enable versioned bucket names.
      // For older versions, this depends on the compatibility option.
      this.hydrationState = versionedHydrationState(this.id);
    } else {
      this.hydrationState = DEFAULT_HYDRATION_STATE;
    }
  }

  hydratedSyncRules(): HydratedSyncRules {
    return this.sync_rules.config.hydrate({ hydrationState: this.hydrationState });
  }
}
