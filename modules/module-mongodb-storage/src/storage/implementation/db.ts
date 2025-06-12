import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';

import { MongoStorageConfig } from '../../types/types.js';
import {
  BucketDataDocument,
  BucketParameterDocument,
  BucketStateDocument,
  CheckpointEventDocument,
  CurrentDataDocument,
  CustomWriteCheckpointDocument,
  IdSequenceDocument,
  InstanceDocument,
  SourceTableDocument,
  SyncRuleDocument,
  WriteCheckpointDocument
} from './models.js';

export interface PowerSyncMongoOptions {
  /**
   * Optional - uses the database from the MongoClient connection URI if not specified.
   */
  database?: string;
}

export class PowerSyncMongo {
  readonly current_data: mongo.Collection<CurrentDataDocument>;
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

  readonly client: mongo.MongoClient;
  readonly db: mongo.Db;

  constructor(client: mongo.MongoClient, options?: PowerSyncMongoOptions) {
    this.client = client;

    const db = client.db(options?.database, {
      ...storage.BSON_DESERIALIZE_INTERNAL_OPTIONS
    });
    this.db = db;

    this.current_data = db.collection<CurrentDataDocument>('current_data');
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
  }

  /**
   * Clear all collections.
   */
  async clear() {
    await this.current_data.deleteMany({});
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
}

export function createPowerSyncMongo(config: MongoStorageConfig, options?: lib_mongo.MongoConnectionOptions) {
  return new PowerSyncMongo(lib_mongo.createMongoClient(config, options), { database: config.database });
}
