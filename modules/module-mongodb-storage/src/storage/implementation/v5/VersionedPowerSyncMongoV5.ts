import { mongo } from '@powersync/lib-service-mongodb';
import { BucketDefinitionId, ParameterIndexId } from '../BucketDefinitionMapping.js';
import { VersionedPowerSyncMongo } from '../collection-access/versioned-collections.js';
import { PowerSyncMongo } from '../db.js';
import {
  BucketDataDocumentV5,
  BucketParameterDocument,
  BucketStateDocument,
  CurrentDataDocument,
  SourceTableDocument
} from './models.js';

/**
 * Typed wrapper around {@link VersionedPowerSyncMongo} for V5 (compressed bucket) storage.
 *
 * Each method narrows the return type to the V5-specific document shape so that
 * callers don't have to cast at every usage site.
 */
export class VersionedPowerSyncMongoV5 extends VersionedPowerSyncMongo {
  constructor(upstream: PowerSyncMongo, storageConfig: any) {
    super(upstream, storageConfig, 'V5');
  }

  sourceRecordsV5(replicationStreamId: number, sourceTableId: mongo.ObjectId): mongo.Collection<CurrentDataDocument> {
    return this.sourceRecords<CurrentDataDocument>(replicationStreamId, sourceTableId);
  }

  listSourceRecordCollectionsV5(replicationStreamId: number): Promise<mongo.Collection<CurrentDataDocument>[]> {
    return this.listSourceRecordCollections<CurrentDataDocument>(replicationStreamId);
  }

  bucketStateV5(replicationStreamId: number): mongo.Collection<BucketStateDocument> {
    return this.bucketState<BucketStateDocument>(replicationStreamId);
  }

  parameterIndexV5(replicationStreamId: number, indexId: ParameterIndexId): mongo.Collection<BucketParameterDocument> {
    return this.parameterIndex<BucketParameterDocument>(replicationStreamId, indexId);
  }

  sourceTablesV5(replicationStreamId: number): mongo.Collection<SourceTableDocument> {
    return this.sourceTables<SourceTableDocument>(replicationStreamId);
  }

  bucketDataV5(replicationStreamId: number, definitionId: BucketDefinitionId): mongo.Collection<BucketDataDocumentV5> {
    return this.bucketData<BucketDataDocumentV5>(replicationStreamId, definitionId);
  }

  listBucketDataCollectionsV5(replicationStreamId: number) {
    return this.listBucketDataCollections(replicationStreamId);
  }

  listParameterIndexCollectionsV5(
    replicationStreamId: number
  ): Promise<{ collection: mongo.Collection<BucketParameterDocument>; indexId: ParameterIndexId }[]> {
    // The base method returns a generic type; we know the concrete type because
    // this wrapper is only used for V5 storage.
    return this.listParameterIndexCollections(replicationStreamId) as unknown as Promise<
      { collection: mongo.Collection<BucketParameterDocument>; indexId: ParameterIndexId }[]
    >;
  }
}
