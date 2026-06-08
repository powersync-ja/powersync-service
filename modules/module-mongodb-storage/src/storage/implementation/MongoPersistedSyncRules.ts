import * as sqlite from 'node:sqlite';

import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import {
  CompatibilityOption,
  DEFAULT_HYDRATION_STATE,
  HydratedSyncConfig,
  HydrationState,
  nodeSqlite,
  versionedHydrationState
} from '@powersync/service-sync-rules';
import {
  BucketDefinitionMapping,
  MultiSyncConfigBucketDefinitionMapping,
  SyncConfigWithMapping,
  SyncConfigWithRequiredMapping
} from './BucketDefinitionMapping.js';
import { StorageConfig } from './models.js';
import { MongoHydrationState } from './MongoHydrationState.js';

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
