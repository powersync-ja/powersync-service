import * as mongo from 'mongodb';
import * as micro from '@journeyapps-platform/micro';

import * as db from '@/db/db-index.js';

import {
  BucketDataDocument,
  BucketParameterDocument,
  CurrentDataDocument,
  IdSequenceDocument,
  InstanceDocument,
  SourceTableDocument,
  SyncRuleDocument,
  WriteCheckpointDocument
} from './models.js';
import { BSON_DESERIALIZE_OPTIONS } from './util.js';
import { configFile } from '@powersync/service-types';

export interface PowerSyncMongoOptions {
  /**
   * Optional - uses the database from the MongoClient connection URI if not specified.
   */
  database?: string;
}

export function createPowerSyncMongo(config: configFile.PowerSyncConfig['storage']) {
  return new PowerSyncMongo(db.mongo.createMongoClient(config), { database: config.database });
}

export class PowerSyncMongo {
  readonly current_data: mongo.Collection<CurrentDataDocument>;
  readonly bucket_data: mongo.Collection<BucketDataDocument>;
  readonly bucket_parameters: mongo.Collection<BucketParameterDocument>;
  readonly op_id_sequence: mongo.Collection<IdSequenceDocument>;
  readonly sync_rules: mongo.Collection<SyncRuleDocument>;
  readonly source_tables: mongo.Collection<SourceTableDocument>;
  readonly write_checkpoints: mongo.Collection<WriteCheckpointDocument>;
  readonly instance: mongo.Collection<InstanceDocument>;
  readonly locks: mongo.Collection<micro.locks.Lock>;

  readonly client: mongo.MongoClient;
  readonly db: mongo.Db;

  constructor(client: mongo.MongoClient, options?: PowerSyncMongoOptions) {
    this.client = client;

    const db = client.db(options?.database, {
      // Note this issue with useBigInt64: https://jira.mongodb.org/browse/NODE-6165
      // Unfortunately there is no workaround if we want to continue using bigint.
      // We selectively disable this in individual queries where we don't need that.
      ...BSON_DESERIALIZE_OPTIONS
    });
    this.db = db;

    this.current_data = db.collection<CurrentDataDocument>('current_data');
    this.bucket_data = db.collection('bucket_data');
    this.bucket_parameters = db.collection('bucket_parameters');
    this.op_id_sequence = db.collection('op_id_sequence');
    this.sync_rules = db.collection('sync_rules');
    this.source_tables = db.collection('source_tables');
    this.write_checkpoints = db.collection('write_checkpoints');
    this.instance = db.collection('instance');
    this.locks = this.db.collection('locks');
  }

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
  }
}
