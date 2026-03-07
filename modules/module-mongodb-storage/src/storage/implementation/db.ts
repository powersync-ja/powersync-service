import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';
import { POWERSYNC_VERSION, storage } from '@powersync/service-core';

import { MongoStorageConfig } from '../../types/types.js';
import {
  BucketDataDocument,
  BucketParameterDocument,
  BucketStateDocument,
  CheckpointEventDocument,
  ClientConnectionDocument,
  CurrentDataDocument,
  CurrentDataDocumentV3,
  CustomWriteCheckpointDocument,
  IdSequenceDocument,
  InstanceDocument,
  SourceTableDocument,
  StorageConfig,
  SyncRuleDocument,
  WriteCheckpointDocument
} from './models.js';
import { ServiceAssertionError } from '@powersync/lib-services-framework';

export interface PowerSyncMongoOptions {
  /**
   * Optional - uses the database from the MongoClient connection URI if not specified.
   */
  database?: string;
}

export class PowerSyncMongo {
  readonly current_data: mongo.Collection<CurrentDataDocument>;
  readonly v3_current_data: mongo.Collection<CurrentDataDocumentV3>;
  readonly bucket_data: mongo.Collection<BucketDataDocument>;
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
    this.v3_current_data = db.collection('v3_current_data');
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

  /**
   * Clear all collections.
   */
  async clear() {
    await this.current_data.deleteMany({});
    await this.v3_current_data.deleteMany({});
    await this.bucket_data.deleteMany({});
    await this.bucket_parameters.deleteMany({});
    await this.op_id_sequence.deleteMany({});
    await this.sync_rules.deleteMany({});
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

  async initializeStorageVersion(storageConfig: StorageConfig) {
    if (storageConfig.softDeleteCurrentData) {
      // Initialize the v3_current_data collection, which is used for the new storage version.
      // No-op if this already exists
      await this.v3_current_data.createIndex(
        {
          '_id.g': 1,
          pending_delete: 1
        },
        {
          partialFilterExpression: { pending_delete: { $exists: true } },
          name: 'pending_delete'
        }
      );
    }
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

  /**
   * Uses either `current_data` or `v3_current_data` collection based on the storage version.
   *
   * Use in places where it does not matter which version is used.
   */
  get common_current_data(): mongo.Collection<CurrentDataDocument> {
    if (this.storageConfig.softDeleteCurrentData) {
      return this.#upstream.v3_current_data;
    } else {
      return this.#upstream.current_data;
    }
  }

  get v1_current_data() {
    if (this.storageConfig.softDeleteCurrentData) {
      throw new ServiceAssertionError(
        'current_data collection should not be used when softDeleteCurrentData is enabled'
      );
    }
    return this.#upstream.current_data;
  }

  get v3_current_data() {
    if (!this.storageConfig.softDeleteCurrentData) {
      throw new ServiceAssertionError(
        'v3_current_data collection should not be used when softDeleteCurrentData is disabled'
      );
    }
    return this.#upstream.v3_current_data;
  }

  get bucket_data() {
    return this.#upstream.bucket_data;
  }

  get bucket_parameters() {
    return this.#upstream.bucket_parameters;
  }

  get op_id_sequence() {
    return this.#upstream.op_id_sequence;
  }

  get sync_rules() {
    return this.#upstream.sync_rules;
  }

  get source_tables() {
    return this.#upstream.source_tables;
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
