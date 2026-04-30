import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';
import { POWERSYNC_VERSION, storage } from '@powersync/service-core';

import { DO_NOT_LOG } from '@powersync/lib-services-framework';
import { MongoStorageConfig } from '../../types/types.js';
import { BaseVersionedPowerSyncMongo } from './common/VersionedPowerSyncMongoBase.js';
import {
  CheckpointEventDocument,
  ClientConnectionDocument,
  CommonSourceTableDocument,
  CustomWriteCheckpointDocument,
  IdSequenceDocument,
  InstanceDocument,
  SourceTableDocument,
  StorageConfig,
  SyncRuleDocumentBase,
  WriteCheckpointDocument
} from './models.js';
import {
  BucketDataDocumentV1,
  BucketParameterDocument,
  BucketStateDocumentV1,
  CurrentDataDocument
} from './v1/models.js';
import { VersionedPowerSyncMongoV1 } from './v1/VersionedPowerSyncMongoV1.js';
import { BucketDataDocumentV3 } from './v3/models.js';
import { VersionedPowerSyncMongoV3 } from './v3/VersionedPowerSyncMongoV3.js';

export interface PowerSyncMongoOptions {
  /**
   * Optional - uses the database from the MongoClient connection URI if not specified.
   */
  database?: string;
}

export class PowerSyncMongo {
  [DO_NOT_LOG] = true;

  readonly current_data: mongo.Collection<CurrentDataDocument>;
  readonly bucket_data: mongo.Collection<BucketDataDocumentV1>;
  readonly bucket_parameters: mongo.Collection<BucketParameterDocument>;
  readonly op_id_sequence: mongo.Collection<IdSequenceDocument>;
  readonly sync_rules: mongo.Collection<SyncRuleDocumentBase>;
  readonly source_tables: mongo.Collection<SourceTableDocument>;
  readonly custom_write_checkpoints: mongo.Collection<CustomWriteCheckpointDocument>;
  readonly write_checkpoints: mongo.Collection<WriteCheckpointDocument>;
  readonly instance: mongo.Collection<InstanceDocument>;
  readonly locks: mongo.Collection<lib_mongo.locks.Lock>;
  readonly bucket_state: mongo.Collection<BucketStateDocumentV1>;
  readonly checkpoint_events: mongo.Collection<CheckpointEventDocument>;
  readonly connection_report_events: mongo.Collection<ClientConnectionDocument>;

  readonly client: mongo.MongoClient;
  readonly db: mongo.Db;

  constructor(client: mongo.MongoClient, options?: PowerSyncMongoOptions) {
    this.client = client;

    const db = client.db(options?.database, {
      ...storage.BSON_DESERIALIZE_INTERNAL_OPTIONS
    });
    this.db = db;

    this.current_data = db.collection('current_data');
    this.bucket_data = db.collection('bucket_data');
    this.bucket_parameters = db.collection('bucket_parameters');
    this.op_id_sequence = db.collection('op_id_sequence');
    this.sync_rules = db.collection('sync_rules');
    this.source_tables = db.collection('source_tables');
    this.custom_write_checkpoints = db.collection('custom_write_checkpoints');
    this.write_checkpoints = db.collection('write_checkpoints');
    this.instance = db.collection('instance');
    this.locks = this.db.collection('locks');
    this.bucket_state = this.db.collection('bucket_state');
    this.checkpoint_events = this.db.collection('checkpoint_events');
    this.connection_report_events = this.db.collection('connection_report_events');
  }

  versioned(storageConfig: StorageConfig & { incrementalReprocessing: true }): VersionedPowerSyncMongoV3;
  versioned(storageConfig: StorageConfig & { incrementalReprocessing: false }): VersionedPowerSyncMongoV1;
  versioned(storageConfig: StorageConfig): VersionedPowerSyncMongo;
  versioned(storageConfig: StorageConfig): VersionedPowerSyncMongo {
    if (storageConfig.incrementalReprocessing) {
      return new VersionedPowerSyncMongoV3(this, storageConfig);
    }

    return new VersionedPowerSyncMongoV1(this, storageConfig);
  }

  /**
   * Not safe for user-provided prefix - only for hardcoded values.
   */
  async listBucketDataCollectionsV3(groupId?: number): Promise<mongo.Collection<BucketDataDocumentV3>[]> {
    const prefix = groupId == null ? 'bucket_data_' : `bucket_data_${groupId}_`;
    const collections = await this.db.listCollections({ name: new RegExp(`^${prefix}`) }, { nameOnly: true }).toArray();

    return collections
      .filter((collection) => collection.name.startsWith(prefix))
      .map((collection) => this.db.collection<BucketDataDocumentV3>(collection.name));
  }

  /**
   * Not safe for user-provided prefix - only for hardcoded values.
   */
  private async collectionsByPrefix(prefix: string): Promise<mongo.Collection<never>[]> {
    const collections = await this.db.listCollections({ name: new RegExp(`^${prefix}`) }, { nameOnly: true }).toArray();

    return collections
      .filter((collection) => collection.name.startsWith(prefix))
      .map((collection) => this.db.collection<never>(collection.name));
  }

  /**
   * List all parameter index collections across all replication streams.
   *
   * Primarily used to clear the db.
   */
  async listAllParameterIndexCollectionsV3(): Promise<mongo.Collection<never>[]> {
    return this.collectionsByPrefix(`parameter_index_`);
  }

  /**
   * List all parameter index collections across all replication streams.
   *
   * Primarily used to clear the db.
   */
  async listAllSourceRecordCollectionsV3(): Promise<mongo.Collection<never>[]> {
    return this.collectionsByPrefix(`source_records_`);
  }

  async listAllBucketStateCollectionsV3(): Promise<mongo.Collection<never>[]> {
    return this.collectionsByPrefix(`bucket_state_`);
  }

  sourceRecordsCollectionName(replicationStreamId: number, sourceTableId: mongo.ObjectId) {
    return `source_records_${replicationStreamId}_${sourceTableId.toHexString()}`;
  }

  sourceTableCollectionName(replicationStreamId: number) {
    return `source_table_${replicationStreamId}`;
  }

  async listSourceTableCollections(
    replicationStreamId?: number
  ): Promise<mongo.Collection<CommonSourceTableDocument>[]> {
    const filter =
      replicationStreamId == null
        ? { name: new RegExp('^source_table_') }
        : { name: this.sourceTableCollectionName(replicationStreamId) };
    const prefix = replicationStreamId == null ? 'source_table_' : this.sourceTableCollectionName(replicationStreamId);
    const collections = await this.db.listCollections(filter, { nameOnly: true }).toArray();

    return collections
      .filter((collection) => collection.name.startsWith(prefix))
      .map((collection) => this.db.collection<CommonSourceTableDocument>(collection.name));
  }

  /**
   * Clear all collections.
   */
  async clear() {
    await this.current_data.deleteMany({});
    for (const collection of await this.listAllSourceRecordCollectionsV3()) {
      await collection.drop();
    }
    await this.bucket_data.deleteMany({});
    for (const collection of await this.listBucketDataCollectionsV3()) {
      await collection.drop();
    }
    await this.bucket_parameters.deleteMany({});
    for (const collection of await this.listAllParameterIndexCollectionsV3()) {
      await collection.drop();
    }
    for (const collection of await this.listAllBucketStateCollectionsV3()) {
      await collection.drop();
    }
    await this.op_id_sequence.deleteMany({});
    await this.sync_rules.deleteMany({});
    for (const collection of await this.listSourceTableCollections()) {
      await collection.drop();
    }
    await this.source_tables.deleteMany({});
    await this.write_checkpoints.deleteMany({});
    await this.instance.deleteOne({});
    await this.locks.deleteMany({});
    await this.bucket_state.deleteMany({});
    await this.custom_write_checkpoints.deleteMany({});
  }

  /**
   * Drop the entire database.
   *
   * Primarily for tests.
   */
  async drop() {
    await this.db.dropDatabase();
  }

  /**
   * Call this after every checkpoint or sync rules status update. Rather call too often than too rarely.
   *
   * This is used in a similar way to the Postgres NOTIFY functionality.
   */
  async notifyCheckpoint() {
    await this.checkpoint_events.insertOne({} as any, { forceServerObjectId: true });
  }

  /**
   * Only use in migrations and tests.
   */
  async createCheckpointEventsCollection() {
    // We cover the case where the replication process was started before running this migration.
    const existingCollections = await this.db
      .listCollections({ name: 'checkpoint_events' }, { nameOnly: false })
      .toArray();
    const collection = existingCollections[0];
    if (collection != null) {
      if (!collection.options?.capped) {
        // Collection was auto-created but not capped, so we need to drop it
        await this.db.dropCollection('checkpoint_events');
      } else {
        // Collection previously created somehow - ignore
        return;
      }
    }

    await this.db.createCollection('checkpoint_events', {
      capped: true,
      // We want a small size, since opening a tailable cursor scans this entire collection.
      // On the other hand, if we fill this up faster than a process can read it, it will
      // invalidate the cursor. We do handle cursor invalidation events, but don't want
      // that to happen too often.
      size: 50 * 1024, // size in bytes
      max: 50 // max number of documents
    });
  }

  /**
   * Only use in migrations and tests.
   */
  async createConnectionReportingCollection() {
    const existingCollections = await this.db
      .listCollections({ name: 'connection_report_events' }, { nameOnly: false })
      .toArray();
    const collection = existingCollections[0];
    if (collection != null) {
      return;
    }
    await this.db.createCollection('connection_report_events');
  }

  /**
   * Only use in migrations and tests.
   */
  async createBucketStateIndex() {
    // TODO: Implement a better mechanism to use migrations in tests
    await this.bucket_state.createIndex(
      {
        '_id.g': 1,
        last_op: 1
      },
      { name: 'bucket_updates', unique: true }
    );
  }
  /**
   * Only use in migrations and tests.
   */
  async createBucketStateIndex2() {
    // TODO: Implement a better mechanism to use migrations in tests
    await this.bucket_state.createIndex(
      {
        '_id.g': 1,
        'estimate_since_compact.count': -1
      },
      { name: 'dirty_count' }
    );
  }
}

/**
 * This is similar to PowerSyncMongo, but blocks access to certain collections based on the storage version.
 */
export type VersionedPowerSyncMongo = BaseVersionedPowerSyncMongo;

export function createPowerSyncMongo(config: MongoStorageConfig, options?: lib_mongo.MongoConnectionOptions) {
  return new PowerSyncMongo(
    lib_mongo.createMongoClient(config, {
      powersyncVersion: POWERSYNC_VERSION,
      ...options
    }),
    { database: config.database }
  );
}
