import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';
import { POWERSYNC_VERSION, storage } from '@powersync/service-core';

import { MongoStorageConfig } from '../../types/types.js';
import {
  BucketDataDocumentV1,
  BucketDataDocumentV3,
  BucketParameterDocument,
  BucketParameterDocumentV3,
  BucketStateDocument,
  CheckpointEventDocument,
  ClientConnectionDocument,
  CommonCurrentDataDocument,
  CommonSourceTableDocument,
  CurrentDataDocument,
  CurrentDataDocumentV3,
  CustomWriteCheckpointDocument,
  IdSequenceDocument,
  InstanceDocument,
  SourceTableDocument,
  SourceTableDocumentV3,
  StorageConfig,
  SyncRuleDocument,
  WriteCheckpointDocument
} from './models.js';
import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { BucketDefinitionId, BucketDefinitionMapping, ParameterIndexId } from './BucketDefinitionMapping.js';

export interface PowerSyncMongoOptions {
  /**
   * Optional - uses the database from the MongoClient connection URI if not specified.
   */
  database?: string;
}

export class PowerSyncMongo {
  readonly current_data: mongo.Collection<CurrentDataDocument>;
  readonly bucket_data: mongo.Collection<BucketDataDocumentV1>;
  readonly bucket_parameters: mongo.Collection<BucketParameterDocument>;
  readonly op_id_sequence: mongo.Collection<IdSequenceDocument>;
  readonly sync_rules: mongo.Collection<SyncRuleDocument>;
  readonly source_tables: mongo.Collection<SourceTableDocument>;
  readonly custom_write_checkpoints: mongo.Collection<CustomWriteCheckpointDocument>;
  readonly write_checkpoints: mongo.Collection<WriteCheckpointDocument>;
  readonly instance: mongo.Collection<InstanceDocument>;
  readonly locks: mongo.Collection<lib_mongo.locks.Lock>;
  readonly bucket_state: mongo.Collection<BucketStateDocument>;
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

  versioned(storageConfig: StorageConfig) {
    return new VersionedPowerSyncMongo(this, storageConfig);
  }

  bucketDataCollectionNameV3(groupId: number, definitionId: BucketDefinitionId) {
    return `bucket_data_${groupId}_${definitionId}`;
  }

  bucketDataV3(groupId: number, definitionId: BucketDefinitionId): mongo.Collection<BucketDataDocumentV3> {
    return this.db.collection(this.bucketDataCollectionNameV3(groupId, definitionId));
  }

  async listBucketDataCollectionsV3(groupId?: number): Promise<mongo.Collection<BucketDataDocumentV3>[]> {
    const prefix = groupId == null ? 'bucket_data_' : `bucket_data_${groupId}_`;
    const collections = await this.db.listCollections({}, { nameOnly: true }).toArray();

    return collections
      .filter((collection) => collection.name.startsWith(prefix))
      .map((collection) => this.db.collection<BucketDataDocumentV3>(collection.name));
  }

  bucketParameterCollectionNameV3(replicationStreamId: number, indexId: ParameterIndexId) {
    return `parameter_index_${replicationStreamId}_${indexId}`;
  }

  parameterIndexV3(
    replicationStreamId: number,
    indexId: ParameterIndexId
  ): mongo.Collection<BucketParameterDocumentV3> {
    return this.db.collection(this.bucketParameterCollectionNameV3(replicationStreamId, indexId));
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

  sourceRecordsCollectionName(replicationStreamId: number, sourceTableId: mongo.ObjectId) {
    return `source_records_${replicationStreamId}_${sourceTableId.toHexString()}`;
  }

  sourceTableCollectionName(replicationStreamId: number) {
    return `source_table_${replicationStreamId}`;
  }

  sourceTables<T extends CommonSourceTableDocument>(replicationStreamId: number): mongo.Collection<T> {
    return this.db.collection<T>(this.sourceTableCollectionName(replicationStreamId));
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
export class VersionedPowerSyncMongo {
  readonly client: mongo.MongoClient;
  readonly db: mongo.Db;

  readonly storageConfig: StorageConfig;
  #upstream: PowerSyncMongo;

  constructor(upstream: PowerSyncMongo, storageConfig: StorageConfig) {
    this.#upstream = upstream;
    this.client = upstream.client;
    this.db = upstream.db;
    this.storageConfig = storageConfig;
  }

  get sourceRecordsV1() {
    if (this.storageConfig.incrementalReprocessing) {
      throw new ServiceAssertionError(
        'current_data collection should not be used when incrementalReprocessing is enabled'
      );
    }
    return this.#upstream.current_data;
  }

  sourceRecordsV3(replicationStreamId: number, sourceTableId: mongo.ObjectId) {
    if (!this.storageConfig.incrementalReprocessing) {
      throw new ServiceAssertionError(
        'v3_current_data collection should not be used when incrementalReprocessing is disabled'
      );
    }

    const collectionName = `source_records_${replicationStreamId}_${sourceTableId.toHexString()}`;
    return this.db.collection<CurrentDataDocumentV3>(collectionName);
  }

  async listSourceRecordCollectionsV3(
    replicationStreamId: number
  ): Promise<mongo.Collection<CommonCurrentDataDocument>[]> {
    const prefix = `source_records_${replicationStreamId}_`;
    const collections = await this.db.listCollections({ name: new RegExp(`^${prefix}`) }, { nameOnly: true }).toArray();

    return collections
      .filter((collection) => collection.name.startsWith(prefix))
      .map((collection) => this.db.collection<CommonCurrentDataDocument>(collection.name));
  }

  async initializeSourceRecordsCollection(replicationStreamId: number, sourceTableId: mongo.ObjectId) {
    if (!this.storageConfig.incrementalReprocessing) {
      throw new ServiceAssertionError(
        'source_records collection initialization should not be used when incrementalReprocessing is disabled'
      );
    }
    await this.sourceRecordsV3(replicationStreamId, sourceTableId).createIndex(
      {
        pending_delete: 1
      },
      {
        partialFilterExpression: { pending_delete: { $exists: true } },
        name: 'pending_delete'
      }
    );
  }

  source_tables(replicationStreamId: number): mongo.Collection<CommonSourceTableDocument> {
    return this.#upstream.sourceTables<CommonSourceTableDocument>(replicationStreamId);
  }

  async initializeStreamStorage(replicationStreamId: number) {
    await this.source_tables(replicationStreamId).createIndex(
      {
        connection_id: 1,
        schema_name: 1,
        table_name: 1,
        relation_id: 1
      },
      {
        name: 'source_lookup'
      }
    );
  }

  get bucket_data() {
    return this.#upstream.bucket_data;
  }

  get v1_bucket_data() {
    if (this.storageConfig.incrementalReprocessing) {
      throw new ServiceAssertionError(
        'bucket_data collection should not be used when incrementalReprocessing is enabled'
      );
    }
    return this.#upstream.bucket_data;
  }

  bucket_data_v3(groupId: number, definitionId: BucketDefinitionId) {
    if (!this.storageConfig.incrementalReprocessing) {
      throw new ServiceAssertionError(
        'v3 bucket_data collections should not be used when incrementalReprocessing is disabled'
      );
    }
    return this.#upstream.bucketDataV3(groupId, definitionId);
  }

  listBucketDataCollectionsV3(groupId?: number) {
    if (!this.storageConfig.incrementalReprocessing) {
      throw new ServiceAssertionError(
        'v3 bucket_data collections should not be used when incrementalReprocessing is disabled'
      );
    }
    return this.#upstream.listBucketDataCollectionsV3(groupId);
  }

  get parameterIndexV1() {
    if (this.storageConfig.incrementalReprocessing) {
      throw new ServiceAssertionError(
        'bucket_parameters collection should not be used when incrementalReprocessing is enabled'
      );
    }
    return this.#upstream.bucket_parameters;
  }

  parameterIndexV3(replicationStreamId: number, indexId: ParameterIndexId) {
    if (!this.storageConfig.incrementalReprocessing) {
      throw new ServiceAssertionError(
        'v3 bucket_parameters collections should not be used when incrementalReprocessing is disabled'
      );
    }
    return this.#upstream.parameterIndexV3(replicationStreamId, indexId);
  }

  /**
   * List parameter index collections for a specific replication stream.
   */
  async listParameterIndexCollectionsV3(
    replicationStreamId: number
  ): Promise<{ collection: mongo.Collection<BucketParameterDocumentV3>; indexId: ParameterIndexId }[]> {
    if (!this.storageConfig.incrementalReprocessing) {
      throw new ServiceAssertionError(
        'v3 bucket_parameters collections should not be used when incrementalReprocessing is disabled'
      );
    }

    const prefix = `parameter_index_${replicationStreamId}_`;
    const collections = await this.db.listCollections({ name: new RegExp(`^${prefix}`) }, { nameOnly: true }).toArray();

    return collections
      .filter((collection) => collection.name.startsWith(prefix))
      .map((collection) => ({
        collection: this.db.collection<BucketParameterDocumentV3>(collection.name),
        indexId: collection.name.slice(prefix.length)
      }));
  }

  get op_id_sequence() {
    return this.#upstream.op_id_sequence;
  }

  get sync_rules() {
    return this.#upstream.sync_rules;
  }

  get custom_write_checkpoints() {
    return this.#upstream.custom_write_checkpoints;
  }

  get write_checkpoints() {
    return this.#upstream.write_checkpoints;
  }

  get instance() {
    return this.#upstream.instance;
  }

  get locks() {
    return this.#upstream.locks;
  }

  get bucket_state() {
    return this.#upstream.bucket_state;
  }

  get checkpoint_events() {
    return this.#upstream.checkpoint_events;
  }

  get connection_report_events() {
    return this.#upstream.connection_report_events;
  }

  notifyCheckpoint() {
    return this.#upstream.notifyCheckpoint();
  }
}

export function createPowerSyncMongo(config: MongoStorageConfig, options?: lib_mongo.MongoConnectionOptions) {
  return new PowerSyncMongo(
    lib_mongo.createMongoClient(config, {
      powersyncVersion: POWERSYNC_VERSION,
      ...options
    }),
    { database: config.database }
  );
}
