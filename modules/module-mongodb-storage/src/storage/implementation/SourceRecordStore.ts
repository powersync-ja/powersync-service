import { mongo } from '@powersync/lib-service-mongodb';
import { Logger } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import { EvaluatedParameters, EvaluatedRow } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { BucketDefinitionId, ParameterIndexId } from './BucketDefinitionMapping.js';

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

export interface SourceRecordStore {
  mapEvaluatedBuckets(evaluated: EvaluatedRow[]): SourceRecordBucketState[];
  mapParameterLookups(paramEvaluated: EvaluatedParameters[]): SourceRecordLookupState[];
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
  cleanup(lastCheckpoint: bigint, logger: Logger): Promise<void>;
}
