import { mongo } from '@powersync/lib-service-mongodb';
import { Logger } from '@powersync/lib-services-framework';
import { InternalOpId, storage } from '@powersync/service-core';
import { BucketDefinitionId, EvaluatedParameters, EvaluatedRow } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { MongoIdSequence } from '../MongoIdSequence.js';
import { PublicationStats } from './PreparedPublication.js';
import { SourceRecordBucketState, SourceRecordLookupState } from './SourceRecordStore.js';

export interface SaveBucketDataOptions {
  op_seq: MongoIdSequence;
  sourceKey: storage.ReplicaId;
  table: storage.SourceTable;
  evaluated: EvaluatedRow[];
  before_buckets: SourceRecordBucketState[];
}

export interface SaveParameterDataOptions {
  op_seq: MongoIdSequence;
  sourceKey: storage.ReplicaId;
  sourceTable: storage.SourceTable;
  evaluated: EvaluatedParameters[];
  existing_lookups: SourceRecordLookupState[];
}

export interface UpsertCurrentDataOptions {
  sourceTableId: bson.ObjectId;
  replicaId: storage.ReplicaId;
  data: bson.Binary | null;
  buckets: SourceRecordBucketState[];
  lookups: SourceRecordLookupState[];
}

export interface PersistedBatchOptions {
  logger?: Logger;
}

/** Operations used by the shared writer; each storage version owns its lifecycle. */
export interface PersistedBatch {
  readonly currentSize: number;
  saveBucketData(options: SaveBucketDataOptions): void;
  saveParameterData(options: SaveParameterDataOptions): void;
  hardDeleteCurrentData(sourceTableId: bson.ObjectId, replicaId: storage.ReplicaId): void;
  softDeleteCurrentData(
    sourceTableId: bson.ObjectId,
    replicaId: storage.ReplicaId,
    checkpointGreaterThan: bigint
  ): void;
  upsertCurrentData(values: UpsertCurrentDataOptions): void;
  shouldFlushTransaction(): boolean;
  flush(session: mongo.ClientSession, options?: storage.BucketBatchCommitOptions): Promise<PublicationStats>;
}

export interface BucketStateUpdate {
  definitionId: BucketDefinitionId | null;
  bucket: string;
  lastOp: InternalOpId;
  incrementCount: number;
  incrementBytes: number;
  incrementChunks: number;
}
