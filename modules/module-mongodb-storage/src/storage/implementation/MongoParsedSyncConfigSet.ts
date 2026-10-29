import * as sqlite from 'node:sqlite';

import {
  ReplicationAbortedError,
  ReplicationAssertionError,
  ServiceAssertionError
} from '@powersync/lib-services-framework';
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
  PrecompiledSyncConfig,
  sourceTableConfigsEqual,
  SyncConfigWithErrors,
  versionedHydrationState
} from '@powersync/service-sync-rules';
import { StorageConfig } from './models.js';
import { MongoHydrationState } from './MongoHydrationState.js';

export class MongoParsedSyncConfigSet implements storage.ParsedSyncConfigSet {
  /** Config definitions/mappings are immutable; only their embedded lifecycle state changes. */
  assertPublicationConfigs(states: readonly { _id: { toHexString(): string }; state: storage.SyncRuleState }[]): void {
    const configured = new Set(this.#configsWithMapping.map((config) => config.syncConfigId));
    const live = new Set(
      states
        .filter((state) =>
          [storage.SyncRuleState.ACTIVE, storage.SyncRuleState.PROCESSING, storage.SyncRuleState.ERRORED].includes(
            state.state
          )
        )
        .map((state) => state._id.toHexString())
    );
    // Snapshot/metadata writers may intentionally select only one processing
    // config while another active config serves clients. Full-stream checkpoint
    // writers separately require every replicating config in their checkpoint guard.
    if (![...live].some((id) => configured.has(id))) {
      throw new ReplicationAbortedError('Replication writer must reload the selected sync configs');
    }
    const mappings = this.#configsWithMapping
      .filter((config) => live.has(config.syncConfigId!))
      .map((config) => config.mapping!);
    // A stopped selected config is harmless only when all its persisted outputs
    // are still owned by live selected configs. Cleanup may remove every other id.
    const activeBuckets = new Set(mappings.flatMap((mapping) => mapping.allBucketDefinitionIds()));
    const activeParameters = new Set(mappings.flatMap((mapping) => mapping.allParameterIndexIds()));
    const activeEvents = new Set(mappings.flatMap((mapping) => mapping.allEventDefinitionIds()));
    if (
      this.mapping.allBucketDefinitionIds().some((id) => !activeBuckets.has(id)) ||
      this.mapping.allParameterIndexIds().some((id) => !activeParameters.has(id)) ||
      this.mapping.allEventDefinitionIds().some((id) => !activeEvents.has(id))
    ) {
      throw new ReplicationAbortedError('Replication writer mappings are no longer live; reload the sync configs');
    }
  }

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
      // Active and processing configs share one source reader and must use the same source-table options.
      if (
        this.syncConfigs.some(
          ({ config }) =>
            !sourceTableConfigsEqual(
              firstConfig.config instanceof PrecompiledSyncConfig
                ? (firstConfig.config.plan.sourceTableConfig ?? {})
                : {},
              config instanceof PrecompiledSyncConfig ? (config.plan.sourceTableConfig ?? {}) : {}
            )
        )
      ) {
        throw new ReplicationAssertionError(
          `The sync configs in replication stream ${this.replicationStreamId} have different source-table configuration.`
        );
      }
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
