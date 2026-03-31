import { mongo } from '@powersync/lib-service-mongodb';
import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { BucketDefinitionId, ParameterIndexId } from '../BucketDefinitionMapping.js';
import { PowerSyncMongo } from '../db.js';
import {
  BucketParameterDocumentV3,
  BucketStateDocumentV3,
  CommonSourceTableDocument,
  CurrentDataDocument,
  CurrentDataDocumentV3,
  SourceTableDocumentV3,
  StorageConfig
} from '../models.js';

export abstract class BaseVersionedPowerSyncMongo {
  readonly client: mongo.MongoClient;
  readonly db: mongo.Db;
  readonly storageConfig: StorageConfig;

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

  protected assertV1Enabled(message: string) {
    if (this.storageConfig.incrementalReprocessing) {
      throw new ServiceAssertionError(message);
    }
  }

  protected assertV3Enabled(message: string) {
    if (!this.storageConfig.incrementalReprocessing) {
      throw new ServiceAssertionError(message);
    }
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

  abstract get sourceRecordsV1(): mongo.Collection<CurrentDataDocument>;

  abstract get bucketStateV1(): mongo.Collection<any>;

  abstract sourceRecordsV3(
    replicationStreamId: number,
    sourceTableId: mongo.ObjectId
  ): mongo.Collection<CurrentDataDocumentV3>;

  abstract listSourceRecordCollectionsV3(
    replicationStreamId: number
  ): Promise<mongo.Collection<CurrentDataDocumentV3>[]>;

  abstract initializeSourceRecordsCollection(replicationStreamId: number, sourceTableId: mongo.ObjectId): Promise<void>;

  abstract commonSourceTables(replicationStreamId: number): mongo.Collection<CommonSourceTableDocument>;

  abstract bucketStateV3(replicationStreamId: number): mongo.Collection<BucketStateDocumentV3>;

  abstract sourceTablesV3(replicationStreamId: number): mongo.Collection<SourceTableDocumentV3>;

  abstract initializeStreamStorage(replicationStreamId: number): Promise<void>;

  abstract get v1_bucket_data(): mongo.Collection<any>;

  abstract bucket_data_v3(groupId: number, definitionId: BucketDefinitionId): mongo.Collection<any>;

  abstract listBucketDataCollectionsV3(groupId: number): Promise<mongo.Collection<any>[]>;

  abstract get parameterIndexV1(): mongo.Collection<any>;

  abstract parameterIndexV3(
    replicationStreamId: number,
    indexId: ParameterIndexId
  ): mongo.Collection<BucketParameterDocumentV3>;

  abstract listParameterIndexCollectionsV3(
    replicationStreamId: number
  ): Promise<{ collection: mongo.Collection<BucketParameterDocumentV3>; indexId: ParameterIndexId }[]>;
}
