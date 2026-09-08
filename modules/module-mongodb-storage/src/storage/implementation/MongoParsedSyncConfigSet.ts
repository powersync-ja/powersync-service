import * as sqlite from 'node:sqlite';

import { ServiceAssertionError } from '@powersync/lib-services-framework';
import {
  BucketDefinitionMapping,
  MultiSyncConfigBucketDefinitionMapping,
  SingleSyncConfigBucketDefinitionMapping,
  storage,
  SyncConfigWithMapping,
  SyncConfigWithRequiredMapping
} from '@powersync/service-core';
import {
  CompatibilityOption,
  DEFAULT_HYDRATION_STATE,
  EventDefinitionId,
  HydratedEventDescriptor,
  HydratedSyncConfig,
  HydrationState,
  nodeSqlite,
  SyncConfigWithErrors,
  versionedHydrationState
} from '@powersync/service-sync-rules';
import { StorageConfig } from './models.js';
import { MongoHydrationState } from './MongoHydrationState.js';

export class MongoParsedSyncConfigSet implements storage.ParsedSyncConfigSet {
  public readonly hydrationState: HydrationState;
  public readonly syncConfigs: SyncConfigWithErrors[];
  public readonly replicationStreamName: string;
  public readonly mapping: BucketDefinitionMapping;

  readonly #configsWithMapping: readonly SyncConfigWithMapping[];

  constructor(
    public readonly replicationStreamId: number,
    storageConfig: StorageConfig,
    slotName: string,
    syncConfigs: SyncConfigWithMapping[]
  ) {
    this.replicationStreamName = slotName;
    this.#configsWithMapping = [...syncConfigs];
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
      this.hydrationState = new MongoHydrationState(mappedConfigs, this.replicationStreamId);
      this.mapping = new MultiSyncConfigBucketDefinitionMapping(mappedConfigs);
    } else if (!compatibility.isEnabled(CompatibilityOption.versionedBucketIds) && !storageConfig.versionedBuckets) {
      const [syncConfig] = syncConfigs;
      if (syncConfigs.length != 1 || syncConfig == null) {
        throw new ServiceAssertionError(`Non-incremental storage requires exactly one sync config`);
      }
      this.hydrationState = DEFAULT_HYDRATION_STATE;
      this.mapping = syncConfig.mapping ?? new SingleSyncConfigBucketDefinitionMapping();
    } else {
      const [syncConfig] = syncConfigs;
      if (syncConfigs.length != 1 || syncConfig == null) {
        throw new ServiceAssertionError(`Non-incremental storage requires exactly one sync config`);
      }
      this.hydrationState = versionedHydrationState(this.replicationStreamId);
      this.mapping = syncConfig.mapping ?? new SingleSyncConfigBucketDefinitionMapping();
    }
  }

  #hydratedSyncConfig: HydratedSyncConfig | undefined;

  get hydratedSyncConfig(): HydratedSyncConfig {
    this.#hydratedSyncConfig ??= new HydratedSyncConfig({
      definitions: this.syncConfigs.map((config) => config.config),
      createParams: {
        hydrationState: this.hydrationState,
        sqlite: nodeSqlite(sqlite)
      }
    });
    return this.#hydratedSyncConfig;
  }

  #eventById: ReadonlyMap<EventDefinitionId, HydratedEventDescriptor> | undefined;

  /**
   * Hydrated events for the replication stream, keyed by their assigned storage id.
   *
   * Each config's events are resolved against that config's own (single-config) mapping, so the resolution is
   * unambiguous, and the result is deduplicated by assigned id: unchanged events shared across configs collapse to
   * one entry, while a changed event keeps a separate entry under its new id.
   */
  get eventById(): ReadonlyMap<EventDefinitionId, HydratedEventDescriptor> {
    if (this.#eventById == null) {
      const map = new Map<EventDefinitionId, HydratedEventDescriptor>();
      const byDefinition = this.hydratedSyncConfig.eventDescriptorsByDefinition;
      for (const config of this.#configsWithMapping) {
        if (config.mapping == null) {
          continue;
        }
        for (const event of byDefinition.get(config.syncConfig.config) ?? []) {
          const id = config.mapping.eventId(event);
          if (!map.has(id)) {
            map.set(id, event);
          }
        }
      }
      this.#eventById = map;
    }
    return this.#eventById;
  }
}
