import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';
import { BucketDefinitionId, EvaluatedRow, ParameterIndexId } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import type { ParameterLookupOutput } from './PersistedBatch.js';

export interface SourceRecordLookupEntry {
  sourceTableId: bson.ObjectId;
  replicaId: storage.ReplicaId;
}

export interface SourceRecordBucketState {
  definitionId: BucketDefinitionId | null;
  bucket: string;
  table: string;
  id: string;
}

export interface SourceRecordLookupState {
  indexId: ParameterIndexId | null;
  lookup: bson.Binary;
}

export interface LoadedSourceRecord {
  sourceTableId: bson.ObjectId;
  replicaId: storage.ReplicaId;
  data: bson.Binary | null;
  buckets: SourceRecordBucketState[];
  lookups: SourceRecordLookupState[];
  cacheKey: string;
}

/**
 * The parts of an evaluated row identifying its bucket, see {@link SourceRecordStore.mapEvaluatedBuckets}.
 */
export type EvaluatedBucketRef = Pick<EvaluatedRow, 'source' | 'bucket' | 'table' | 'id'>;

export interface SourceRecordStore {
  mapEvaluatedBuckets(evaluated: EvaluatedBucketRef[]): SourceRecordBucketState[];
  mapParameterLookups(paramEvaluated: ParameterLookupOutput[]): SourceRecordLookupState[];
  loadSizes(session: mongo.ClientSession, entries: SourceRecordLookupEntry[]): Promise<Map<string, number>>;
  loadDocuments(
    session: mongo.ClientSession,
    entries: SourceRecordLookupEntry[],
    idsOnly: boolean
  ): Promise<Map<string, LoadedSourceRecord>>;
  loadTruncateBatch(
    session: mongo.ClientSession,
    sourceTableId: bson.ObjectId,
    limit: number
  ): Promise<LoadedSourceRecord[]>;
}
