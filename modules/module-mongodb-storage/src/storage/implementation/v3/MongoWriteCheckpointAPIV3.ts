import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { CustomWriteCheckpointFilters, GetCheckpointChangesOptions, storage } from '@powersync/service-core';
import { EventDefinitionId, HydratedSyncConfig } from '@powersync/service-sync-rules';
import { MongoCheckpointAPIOptions, MongoWriteCheckpointAPI } from '../MongoWriteCheckpointAPI.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

export type MongoCheckpointAPIV3Options = Omit<MongoCheckpointAPIOptions, 'db'> & {
  db: VersionedPowerSyncMongoV3;
};

export class MongoWriteCheckpointAPIV3 extends MongoWriteCheckpointAPI {
  declare db: VersionedPowerSyncMongoV3;

  // Supplied via the constructor or the setWriteCheckpointMode setter.
  private _resolveCustomWriteCheckpointEventId: storage.CustomWriteCheckpointEventIdResolver | undefined;

  constructor(options: MongoCheckpointAPIV3Options) {
    super(options);
    // Enforce the CUSTOM-requires-resolver invariant on construction too, not only setWriteCheckpointMode.
    this._resolveCustomWriteCheckpointEventId = this.requireCustomEventIdResolver(options.writeCheckpointMode);
  }

  protected resolveEventId(syncConfig: HydratedSyncConfig): EventDefinitionId {
    if (!this._resolveCustomWriteCheckpointEventId) {
      throw new ServiceAssertionError(`No resolveEventId resolver has been supplied via setWriteCheckpointMode.`);
    }
    const eventId = this._resolveCustomWriteCheckpointEventId(syncConfig);
    if (eventId == null) {
      throw new ServiceAssertionError('V3 custom checkpoints require an event definition id');
    }
    if (!syncConfig.eventDescriptors.some((event) => event.id == eventId)) {
      throw new ServiceAssertionError(`Unknown custom checkpoint event definition ${eventId}`);
    }
    return eventId;
  }

  protected override async lastCustomWriteCheckpoint(filters: CustomWriteCheckpointFilters): Promise<bigint | null> {
    const { user_id, syncConfig } = filters;
    const eventId = this.resolveEventId(syncConfig);

    const lastWriteCheckpoint = await this.db
      .customCheckpointRequests({
        eventId,
        replicationStreamId: this.replicationStreamId
      })
      .findOne({
        user_id
      });
    return lastWriteCheckpoint?.checkpoint ?? null;
  }

  override setWriteCheckpointMode(config: storage.WriteCheckpointModeConfig): void {
    this._resolveCustomWriteCheckpointEventId = this.requireCustomEventIdResolver(config);
    super.setWriteCheckpointMode(config);
  }

  /**
   * Validates and returns the event-id resolver for the given mode config. Shared by the constructor and
   * {@link setWriteCheckpointMode} so both paths enforce that CUSTOM mode always has a resolver.
   */
  private requireCustomEventIdResolver(
    config: storage.WriteCheckpointModeConfig
  ): storage.CustomWriteCheckpointEventIdResolver | undefined {
    if (config.mode != storage.WriteCheckpointMode.CUSTOM) {
      return undefined;
    }
    if (!config.resolveEventId) {
      throw new ServiceAssertionError(`V3 incremental reprocessing requires a resolveEventId resolver to be supplied.`);
    }
    return config.resolveEventId;
  }

  protected override async getCustomWriteCheckpointChanges(
    options: GetCheckpointChangesOptions
  ): Promise<{ invalidateWriteCheckpoints: boolean; updatedWriteCheckpoints: Map<string, bigint> }> {
    const eventId = this.resolveEventId(options.syncConfig);

    const limit = 1000;
    const changes = await this.db
      .customCheckpointRequests({
        replicationStreamId: this.replicationStreamId,
        eventId
      })
      .find(
        {
          op_id: { $gt: options.lastCheckpoint.checkpoint, $lte: options.nextCheckpoint.checkpoint }
        },
        {
          limit: limit + 1,
          // batchSize is 1 more than limit to auto-close the cursor.
          // See https://github.com/mongodb/node-mongodb-native/pull/4580
          batchSize: limit + 2,
          singleBatch: true
        }
      )
      .toArray();
    const invalidate = changes.length > limit;

    const updatedWriteCheckpoints = new Map<string, bigint>();
    if (!invalidate) {
      for (let c of changes) {
        updatedWriteCheckpoints.set(c.user_id, c.checkpoint);
      }
    }

    return {
      invalidateWriteCheckpoints: invalidate,
      updatedWriteCheckpoints
    };
  }
}
