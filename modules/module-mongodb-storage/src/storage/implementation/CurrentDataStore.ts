import { mongo } from '@powersync/lib-service-mongodb';
import { Logger } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import * as bson from 'bson';
import { CommonCurrentBucket, CommonCurrentLookup, CurrentDataDocumentId } from './models.js';

export interface CurrentDataLookupEntry {
  sourceTableId: bson.ObjectId;
  replicaId: storage.ReplicaId;
}

export interface LoadedCurrentData {
  sourceTableId: bson.ObjectId;
  id: CurrentDataDocumentId;
  replicaId: storage.ReplicaId;
  data: bson.Binary | null;
  buckets: CommonCurrentBucket[];
  lookups: CommonCurrentLookup[];
  cacheKey: string;
}

export interface CurrentDataStore {
  createId(sourceTableId: bson.ObjectId, replicaId: storage.ReplicaId): CurrentDataDocumentId;
  createLoadedDocument(
    sourceTableId: bson.ObjectId,
    id: CurrentDataDocumentId,
    data: bson.Binary | null,
    buckets: CommonCurrentBucket[],
    lookups: CommonCurrentLookup[]
  ): LoadedCurrentData;
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
