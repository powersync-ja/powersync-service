import { mongo } from '@powersync/lib-service-mongodb';
import { Logger } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import * as bson from 'bson';
import { VersionedPowerSyncMongo } from './db.js';
import { cacheKey } from './OperationBatch.js';
import { CurrentDataStore, CurrentDataLookupEntry, LoadedCurrentData } from './CurrentDataStore.js';
import { CurrentDataDocumentV3, CurrentDataDocumentId } from './models.js';

export class CurrentDataStoreV3 implements CurrentDataStore {
  constructor(
    private readonly db: VersionedPowerSyncMongo,
    private readonly groupId: number
  ) {}

  createId(_sourceTableId: bson.ObjectId, replicaId: storage.ReplicaId): CurrentDataDocumentId {
    return replicaId;
  }

  createLoadedDocument(
    sourceTableId: bson.ObjectId,
    id: CurrentDataDocumentId,
    data: bson.Binary | null,
    buckets: LoadedCurrentData['buckets'],
    lookups: LoadedCurrentData['lookups']
  ): LoadedCurrentData {
    return {
      sourceTableId,
      id,
      replicaId: id,
      data,
      buckets,
      lookups,
      cacheKey: cacheKey(sourceTableId, id)
    };
  }

  async loadSizes(session: mongo.ClientSession, entries: CurrentDataLookupEntry[]): Promise<Map<string, number>> {
    const sizes = new Map<string, number>();
    for (const [sourceTableId, replicaIds] of this.groupEntries(entries)) {
      const filter = {
        _id: { $in: replicaIds as any[] }
      } as unknown as mongo.Filter<CurrentDataDocumentV3>;
      const sizeCursor: mongo.AggregationCursor<CurrentDataDocumentV3 & { size: number }> = this.db
        .v3_current_data(this.groupId, sourceTableId)
        .aggregate(
          [
            {
              $match: filter
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
        sizes.set(cacheKey(sourceTableId, doc._id), doc.size);
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
      const filter = {
        _id: { $in: replicaIds as any[] }
      } as unknown as mongo.Filter<CurrentDataDocumentV3>;
      const cursor = this.db.v3_current_data(this.groupId, sourceTableId).find(filter, { session, projection });
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
    const cursor = this.db.v3_current_data(this.groupId, sourceTableId).find(
      {
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

  async cleanup(lastCheckpoint: bigint, logger: Logger): Promise<void> {
    let deletedCount = 0;
    for (const collection of await this.db.listCommonCurrentDataCollections(this.groupId)) {
      const result = await collection.deleteMany({
        pending_delete: { $exists: true, $lte: lastCheckpoint }
      });
      deletedCount += result.deletedCount;
    }
    if (deletedCount > 0) {
      logger.info(`Cleaned up ${deletedCount} pending delete current_data records for checkpoint ${lastCheckpoint}`);
    }
  }

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
