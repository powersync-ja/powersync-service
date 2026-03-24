import { mongo } from '@powersync/lib-service-mongodb';
import { Logger } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import * as bson from 'bson';
import { idPrefixFilter } from '../../utils/util.js';
import { VersionedPowerSyncMongo } from './db.js';
import { cacheKey } from './OperationBatch.js';
import { CurrentDataStore, CurrentDataLookupEntry, LoadedCurrentData } from './CurrentDataStore.js';
import { CurrentDataDocument, CurrentDataDocumentId, SourceKey } from './models.js';

export class CurrentDataStoreV1 implements CurrentDataStore {
  constructor(
    private readonly db: VersionedPowerSyncMongo,
    private readonly groupId: number
  ) {}

  createId(sourceTableId: bson.ObjectId, replicaId: storage.ReplicaId): CurrentDataDocumentId {
    return {
      g: this.groupId,
      t: sourceTableId,
      k: replicaId
    } satisfies SourceKey;
  }

  createLoadedDocument(
    sourceTableId: bson.ObjectId,
    id: CurrentDataDocumentId,
    data: bson.Binary | null,
    buckets: LoadedCurrentData['buckets'],
    lookups: LoadedCurrentData['lookups']
  ): LoadedCurrentData {
    const typedId = id as SourceKey;
    return {
      sourceTableId,
      id: typedId,
      replicaId: typedId.k,
      data,
      buckets,
      lookups,
      cacheKey: cacheKey(sourceTableId, typedId.k)
    };
  }

  async loadSizes(session: mongo.ClientSession, entries: CurrentDataLookupEntry[]): Promise<Map<string, number>> {
    const sizes = new Map<string, number>();
    for (const [sourceTableId, replicaIds] of this.groupEntries(entries)) {
      const sizeCursor: mongo.AggregationCursor<CurrentDataDocument & { size: number }> = this.db
        .v1_current_data(this.groupId, sourceTableId)
        .aggregate(
          [
            {
              $match: {
                _id: {
                  $in: replicaIds.map((replicaId) => this.createId(sourceTableId, replicaId) as SourceKey)
                }
              }
            },
            {
              $project: {
                _id: 1,
                size: { $bsonSize: '$$ROOT' }
              }
            }
          ],
          { session }
        );
      for await (const doc of sizeCursor.stream()) {
        sizes.set(cacheKey(sourceTableId, doc._id.k), doc.size);
      }
    }
    return sizes;
  }

  async loadDocuments(
    session: mongo.ClientSession,
    entries: CurrentDataLookupEntry[],
    idsOnly: boolean
  ): Promise<Map<string, LoadedCurrentData>> {
    const documents = new Map<string, LoadedCurrentData>();
    const projection = idsOnly ? { _id: 1 } : undefined;
    for (const [sourceTableId, replicaIds] of this.groupEntries(entries)) {
      const cursor = this.db.v1_current_data(this.groupId, sourceTableId).find(
        {
          _id: {
            $in: replicaIds.map((replicaId) => this.createId(sourceTableId, replicaId) as SourceKey)
          }
        },
        { session, projection }
      );
      for await (const doc of cursor.stream()) {
        const loaded = this.createLoadedDocument(
          sourceTableId,
          doc._id,
          idsOnly ? null : doc.data,
          idsOnly ? [] : doc.buckets,
          idsOnly ? [] : doc.lookups
        );
        documents.set(loaded.cacheKey, loaded);
      }
    }
    return documents;
  }

  async loadTruncateBatch(
    session: mongo.ClientSession,
    sourceTableId: bson.ObjectId,
    limit: number
  ): Promise<LoadedCurrentData[]> {
    const cursor = this.db.v1_current_data(this.groupId, sourceTableId).find(
      {
        _id: idPrefixFilter<SourceKey>({ g: this.groupId, t: sourceTableId }, ['k']),
        pending_delete: { $exists: false }
      },
      {
        projection: {
          _id: 1,
          buckets: 1,
          lookups: 1
        },
        limit,
        session
      }
    );
    return (await cursor.toArray()).map((doc) =>
      this.createLoadedDocument(sourceTableId, doc._id, null, doc.buckets, doc.lookups)
    );
  }

  async cleanup(_lastCheckpoint: bigint, _logger: Logger): Promise<void> {}

  private groupEntries(entries: CurrentDataLookupEntry[]): Map<bson.ObjectId, storage.ReplicaId[]> {
    const grouped = new Map<bson.ObjectId, storage.ReplicaId[]>();
    for (const entry of entries) {
      const existing = grouped.get(entry.sourceTableId) ?? [];
      existing.push(entry.replicaId);
      grouped.set(entry.sourceTableId, existing);
    }
    return grouped;
  }
}
