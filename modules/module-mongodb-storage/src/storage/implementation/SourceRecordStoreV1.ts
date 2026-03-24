import { mongo } from '@powersync/lib-service-mongodb';
import { Logger } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import * as bson from 'bson';
import { idPrefixFilter } from '../../utils/util.js';
import { VersionedPowerSyncMongo } from './db.js';
import { cacheKey } from './OperationBatch.js';
import {
  SourceRecordLookupEntry,
  SourceRecordLookupState,
  LoadedSourceRecord,
  SourceRecordStore
} from './SourceRecordStore.js';
import { CurrentDataDocument, SourceKey } from './models.js';
import { EvaluatedParameters, EvaluatedRow } from '@powersync/service-sync-rules';

export class SourceRecordStoreV1 implements SourceRecordStore {
  constructor(
    private readonly db: VersionedPowerSyncMongo,
    private readonly groupId: number
  ) {}

  mapEvaluatedBuckets(evaluated: EvaluatedRow[]): LoadedSourceRecord['buckets'] {
    return evaluated.map((entry) => ({
      definitionId: null,
      bucket: entry.bucket,
      table: entry.table,
      id: entry.id
    }));
  }

  mapParameterLookups(paramEvaluated: EvaluatedParameters[]): SourceRecordLookupState[] {
    return paramEvaluated.map((entry) => ({
      indexId: null,
      lookup: storage.serializeLookup(entry.lookup)
    }));
  }

  private createId(sourceTableId: bson.ObjectId, replicaId: storage.ReplicaId): SourceKey {
    return {
      g: this.groupId,
      t: sourceTableId,
      k: replicaId
    } satisfies SourceKey;
  }

  private createLoadedDocument(
    sourceTableId: bson.ObjectId,
    id: SourceKey,
    data: bson.Binary | null,
    buckets: CurrentDataDocument['buckets'],
    lookups: CurrentDataDocument['lookups']
  ): LoadedSourceRecord {
    return {
      sourceTableId,
      replicaId: id.k,
      data,
      buckets: buckets.map((bucket) => ({
        definitionId: null,
        bucket: bucket.bucket,
        table: bucket.table,
        id: bucket.id
      })),
      lookups: lookups.map((lookup) => ({
        indexId: null,
        lookup
      })),
      cacheKey: cacheKey(sourceTableId, id.k)
    };
  }

  async loadSizes(session: mongo.ClientSession, entries: SourceRecordLookupEntry[]): Promise<Map<string, number>> {
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
    entries: SourceRecordLookupEntry[],
    idsOnly: boolean
  ): Promise<Map<string, LoadedSourceRecord>> {
    const documents = new Map<string, LoadedSourceRecord>();
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
  ): Promise<LoadedSourceRecord[]> {
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
