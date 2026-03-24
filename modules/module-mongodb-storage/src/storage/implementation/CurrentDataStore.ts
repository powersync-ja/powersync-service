import { mongo } from '@powersync/lib-service-mongodb';
import { Logger } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import { EvaluatedParameters, EvaluatedRow } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { BucketDefinitionId, ParameterIndexId } from './BucketDefinitionMapping.js';

export interface CurrentDataLookupEntry {
  sourceTableId: bson.ObjectId;
  replicaId: storage.ReplicaId;
}

export interface CurrentDataBucketState {
  definitionId: BucketDefinitionId | null;
  bucket: string;
  table: string;
  id: string;
}

export interface CurrentDataLookupState {
  indexId: ParameterIndexId | null;
  lookup: bson.Binary;
}

export interface LoadedCurrentData {
  sourceTableId: bson.ObjectId;
  replicaId: storage.ReplicaId;
  data: bson.Binary | null;
  buckets: CurrentDataBucketState[];
  lookups: CurrentDataLookupState[];
  cacheKey: string;
}

export interface CurrentDataStore {
  mapEvaluatedBuckets(evaluated: EvaluatedRow[]): CurrentDataBucketState[];
  mapParameterLookups(paramEvaluated: EvaluatedParameters[]): CurrentDataLookupState[];
  loadSizes(session: mongo.ClientSession, entries: CurrentDataLookupEntry[]): Promise<Map<string, number>>;
  loadDocuments(
    session: mongo.ClientSession,
    entries: CurrentDataLookupEntry[],
    idsOnly: boolean
  ): Promise<Map<string, LoadedCurrentData>>;
  loadTruncateBatch(
    session: mongo.ClientSession,
    sourceTableId: bson.ObjectId,
    limit: number
  ): Promise<LoadedCurrentData[]>;
  cleanup(lastCheckpoint: bigint, logger: Logger): Promise<void>;
}
