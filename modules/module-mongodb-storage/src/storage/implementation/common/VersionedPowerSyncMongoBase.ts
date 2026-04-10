import { mongo } from '@powersync/lib-service-mongodb';
import { DO_NOT_LOG } from '@powersync/lib-services-framework';
import { PowerSyncMongo } from '../db.js';
import { CommonSourceTableDocument, StorageConfig } from '../models.js';

export abstract class BaseVersionedPowerSyncMongo {
  readonly client: mongo.MongoClient;
  readonly db: mongo.Db;
  readonly storageConfig: StorageConfig;
  [DO_NOT_LOG] = true;

  constructor(
    protected readonly upstream: PowerSyncMongo,
    storageConfig: StorageConfig
  ) {
    this.client = upstream.client;
    this.db = upstream.db;
    this.storageConfig = storageConfig;
  }

  get bucket_data() {
    return this.upstream.bucket_data;
  }

  get op_id_sequence() {
    return this.upstream.op_id_sequence;
  }

  get sync_rules() {
    return this.upstream.sync_rules;
  }

  get custom_write_checkpoints() {
    return this.upstream.custom_write_checkpoints;
  }

  get write_checkpoints() {
    return this.upstream.write_checkpoints;
  }

  get instance() {
    return this.upstream.instance;
  }

  get locks() {
    return this.upstream.locks;
  }

  get checkpoint_events() {
    return this.upstream.checkpoint_events;
  }

  get connection_report_events() {
    return this.upstream.connection_report_events;
  }

  notifyCheckpoint() {
    return this.upstream.notifyCheckpoint();
  }

  protected sourceRecordsCollectionName(replicationStreamId: number, sourceTableId: mongo.ObjectId) {
    return this.upstream.sourceRecordsCollectionName(replicationStreamId, sourceTableId);
  }

  protected sourceTableCollectionName(replicationStreamId: number) {
    return this.upstream.sourceTableCollectionName(replicationStreamId);
  }

  protected async listCollectionsByPrefix<T extends mongo.Document>(prefix: string): Promise<mongo.Collection<T>[]> {
    const collections = await this.db.listCollections({ name: new RegExp(`^${prefix}`) }, { nameOnly: true }).toArray();

    return collections
      .filter((collection) => collection.name.startsWith(prefix))
      .map((collection) => this.db.collection<T>(collection.name));
  }

  abstract commonSourceTables(replicationStreamId: number): mongo.Collection<CommonSourceTableDocument>;

  abstract initializeStreamStorage(replicationStreamId: number): Promise<void>;
}
