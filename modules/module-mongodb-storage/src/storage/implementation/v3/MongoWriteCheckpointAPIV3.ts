import { ServiceAssertionError } from '@powersync/lib-services-framework';
import {
  CustomWriteCheckpointFilters,
  GetCheckpointChangesOptions,
  SingleSyncConfigBucketDefinitionMapping,
  storage
} from '@powersync/service-core';
import { EventDefinitionId } from '@powersync/service-sync-rules';
import { MongoCheckpointAPIOptions, MongoWriteCheckpointAPI } from '../MongoWriteCheckpointAPI.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

export type MongoCheckpointAPIV3Options = Omit<MongoCheckpointAPIOptions, 'db'> & {
  db: VersionedPowerSyncMongoV3;
  syncConfigMapping: () => SingleSyncConfigBucketDefinitionMapping;
};

export class MongoWriteCheckpointAPIV3 extends MongoWriteCheckpointAPI {
  declare db: VersionedPowerSyncMongoV3;

  // Supplied via the constructor or the setWriteCheckpointMode setter.
  private _customWriteCheckpointEventName: string | undefined;
  private readonly syncConfigMapping: () => SingleSyncConfigBucketDefinitionMapping;

  constructor(options: MongoCheckpointAPIV3Options) {
    super(options);
    this.syncConfigMapping = options.syncConfigMapping;
    // Enforce the CUSTOM-requires-event-name invariant on construction too, not only setWriteCheckpointMode.
    this._customWriteCheckpointEventName = this.requireCustomEventName(options.writeCheckpointMode);
  }

  protected resolveEventId(): EventDefinitionId {
    const eventName = this._customWriteCheckpointEventName;
    if (!eventName) {
      throw new ServiceAssertionError(`No eventName has been supplied via setWriteCheckpointMode.`);
    }
    return this.syncConfigMapping().eventDefinitionIdByName(eventName);
  }

  protected override async lastCustomWriteCheckpoint(filters: CustomWriteCheckpointFilters): Promise<bigint | null> {
    const { user_id } = filters;
    const eventId = this.resolveEventId();

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
    this._customWriteCheckpointEventName = this.requireCustomEventName(config);
    super.setWriteCheckpointMode(config);
  }

  /**
   * Validates and returns the event name for the given mode config. Shared by the constructor and
   * {@link setWriteCheckpointMode} so both paths enforce that CUSTOM mode always names its checkpoint event.
   */
  private requireCustomEventName(config: storage.WriteCheckpointModeConfig): string | undefined {
    if (config.mode != storage.WriteCheckpointMode.CUSTOM) {
      return undefined;
    }
    if (!config.eventName) {
      throw new ServiceAssertionError(`V3 incremental reprocessing requires an eventName to be supplied.`);
    }
    return config.eventName;
  }

  protected override async getCustomWriteCheckpointChanges(
    options: GetCheckpointChangesOptions
  ): Promise<{ invalidateWriteCheckpoints: boolean; updatedWriteCheckpoints: Map<string, bigint> }> {
    const eventId = this.resolveEventId();

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
