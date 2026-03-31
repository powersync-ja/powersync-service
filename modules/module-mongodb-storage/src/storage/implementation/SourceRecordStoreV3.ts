import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';
import { Logger } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import * as bson from 'bson';
import { EvaluatedParameters, EvaluatedRow } from '@powersync/service-sync-rules';
import { retryOnMongoMaxTimeMSExpired } from '../../utils/util.js';
import { VersionedPowerSyncMongo } from './db.js';
import { cacheKey } from './OperationBatch.js';
import { LoadedSourceRecord, SourceRecordLookupEntry, SourceRecordStore } from './SourceRecordStore.js';
import { CurrentDataDocumentV3, SourceTableDocumentV3 } from './models.js';
import { BucketDefinitionMapping } from './BucketDefinitionMapping.js';
import { serializeParameterLookupV3 } from './MongoParameterLookupV3.js';

export class SourceRecordStoreV3 implements SourceRecordStore {
  constructor(
    private readonly db: VersionedPowerSyncMongo,
    private readonly groupId: number,
    private readonly mapping: BucketDefinitionMapping
  ) {}

  mapEvaluatedBuckets(evaluated: EvaluatedRow[]): LoadedSourceRecord['buckets'] {
    return evaluated.map((entry) => ({
      definitionId: this.mapping.bucketSourceId(entry.source),
      bucket: entry.bucket,
      table: entry.table,
      id: entry.id
    }));
  }

  mapParameterLookups(paramEvaluated: EvaluatedParameters[]): LoadedSourceRecord['lookups'] {
    return paramEvaluated.map((entry) => ({
      indexId: this.mapping.parameterLookupId(entry.lookup.source),
      lookup: serializeParameterLookupV3(entry.lookup)
    }));
  }

  private createLoadedDocument(
    sourceTableId: bson.ObjectId,
    id: storage.ReplicaId,
    data: bson.Binary | null,
    buckets: CurrentDataDocumentV3['buckets'],
    lookups: CurrentDataDocumentV3['lookups']
  ): LoadedSourceRecord {
    return {
      sourceTableId,
      replicaId: id,
      data,
      buckets: buckets.map((bucket) => ({
        definitionId: bucket.def,
        bucket: bucket.bucket,
        table: bucket.table,
        id: bucket.id
      })),
      lookups: lookups.map((lookup) => ({
        indexId: lookup.i,
        lookup: lookup.l
      })),
      cacheKey: cacheKey(sourceTableId, id)
    };
  }

  async loadSizes(session: mongo.ClientSession, entries: SourceRecordLookupEntry[]): Promise<Map<string, number>> {
    const sizes = new Map<string, number>();
    for (const [sourceTableId, replicaIds] of this.groupEntries(entries)) {
      const filter = {
        _id: { $in: replicaIds as any[] }
      } as unknown as mongo.Filter<CurrentDataDocumentV3>;
      const sizeCursor: mongo.AggregationCursor<CurrentDataDocumentV3 & { size: number }> = this.db
        .sourceRecordsV3(this.groupId, sourceTableId)
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
    entries: SourceRecordLookupEntry[],
    idsOnly: boolean
  ): Promise<Map<string, LoadedSourceRecord>> {
    const documents = new Map<string, LoadedSourceRecord>();
    const projection = idsOnly ? { _id: 1 } : undefined;
    for (const [sourceTableId, replicaIds] of this.groupEntries(entries)) {
      const filter = {
        _id: { $in: replicaIds as any[] }
      } as unknown as mongo.Filter<CurrentDataDocumentV3>;
      const cursor = this.db.sourceRecordsV3(this.groupId, sourceTableId).find(filter, { session, projection });
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
  ): Promise<LoadedSourceRecord[]> {
    const cursor = this.db.sourceRecordsV3(this.groupId, sourceTableId).find(
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

  async postCommitCleanup(lastCheckpoint: bigint, logger: Logger): Promise<void> {
    // This cleans up soft deletes in source_records collections.
    // Since there may be a lot (100+) of these collections in some cases, we track which
    // ones have dirty deletes in source_tables.

    const dirtySourceTables = await this.db
      .sourceTablesV3(this.groupId)
      .find(
        {
          latest_pending_delete: { $exists: true }
        },
        {
          projection: { _id: 1, latest_pending_delete: 1 }
        }
      )
      .toArray();

    let deletedCount = 0;
    const sourceTableUpdates: mongo.AnyBulkWriteOperation<SourceTableDocumentV3>[] = [];
    for (const sourceTable of dirtySourceTables) {
      const collection = this.db.sourceRecordsV3(this.groupId, sourceTable._id);
      const result = await this.deletePendingDeletes(collection, sourceTable._id, lastCheckpoint, logger);
      deletedCount += result.deletedCount;

      if (sourceTable.latest_pending_delete != null && sourceTable.latest_pending_delete <= lastCheckpoint) {
        sourceTableUpdates.push({
          updateOne: {
            filter: {
              _id: sourceTable._id,
              // If the source table received more writes in the meantime, this will filter it out
              latest_pending_delete: sourceTable.latest_pending_delete
            },
            update: {
              $unset: {
                latest_pending_delete: 1
              }
            }
          }
        });
      }
    }

    if (sourceTableUpdates.length > 0) {
      await this.db.sourceTablesV3(this.groupId).bulkWrite(sourceTableUpdates, { ordered: false });
    }
    if (deletedCount > 0) {
      logger.info(`Cleaned up ${deletedCount} pending delete current_data records for checkpoint ${lastCheckpoint}`);
    }
  }

  private async deletePendingDeletes(
    collection: mongo.Collection<CurrentDataDocumentV3>,
    sourceTableId: bson.ObjectId,
    lastCheckpoint: bigint,
    logger: Logger
  ) {
    return retryOnMongoMaxTimeMSExpired(
      () =>
        collection.deleteMany(
          {
            pending_delete: { $exists: true, $lte: lastCheckpoint }
          },
          {
            maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS
          }
        ),
      {
        retryDelayMs: lib_mongo.db.MONGO_OPERATION_TIMEOUT_MS / 5,
        onRetry: (n: number) => {
          logger.warn(`Cleared batch ${n} of pending deletes for source table ${sourceTableId}, continuing...`);
        }
      }
    );
  }

  private groupEntries(entries: SourceRecordLookupEntry[]): Map<bson.ObjectId, storage.ReplicaId[]> {
    const grouped = new Map<bson.ObjectId, storage.ReplicaId[]>();
    for (const entry of entries) {
      const existing = grouped.get(entry.sourceTableId) ?? [];
      existing.push(entry.replicaId);
      grouped.set(entry.sourceTableId, existing);
    }
    return grouped;
  }
}
