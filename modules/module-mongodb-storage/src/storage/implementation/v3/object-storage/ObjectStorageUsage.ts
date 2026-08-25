import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { BucketDefinitionId } from '@powersync/service-sync-rules';
import { randomUUID } from 'node:crypto';
import { BucketDataDocumentV3, ObjectStorageUsageDocument } from '../models.js';
import { VersionedPowerSyncMongoV3 } from '../VersionedPowerSyncMongoV3.js';

export const OBJECT_STORAGE_USAGE_BASE_WRITER_ID = '__base__';
export const DEFAULT_OBJECT_STORAGE_USAGE_STALE_WRITER_MS = 30 * 60 * 1000;
export const DEFAULT_OBJECT_STORAGE_USAGE_FOLD_LIMIT = 100;

export interface ReplicationStreamObjectStorageUsageResult {
  replication_stream_id: number;
  active_bytes: bigint;
}

export interface ReplicationStreamObjectStorageDefinitionUsageResult {
  replication_stream_id: number;
  definition_id: BucketDefinitionId;
  active_bytes: bigint;
}

export interface ObjectStorageUsageEntry {
  definition_id: BucketDefinitionId;
  active_bytes: bigint;
}

export function createObjectStorageUsageWriterId(): string {
  return randomUUID();
}

/**
 * Tracks active S3 references. Values are signed writer-local deltas, so reference changes and
 * accounting updates can be committed together without making replication and compaction share a
 * hot counter document.
 */
export class ObjectStorageUsage {
  constructor(
    private readonly db: VersionedPowerSyncMongoV3,
    private readonly replicationStreamId: number,
    readonly writerId?: string
  ) {
    if (writerId === OBJECT_STORAGE_USAGE_BASE_WRITER_ID) {
      throw new ReplicationAssertionError('The reserved object-storage usage writer id cannot be used by a writer');
    }
  }

  static bytes(document: Pick<BucketDataDocumentV3, 'storage_ref'>): bigint {
    return BigInt(document.storage_ref?.file_size ?? 0);
  }

  static async readAllDefinitionUsage(
    db: VersionedPowerSyncMongoV3
  ): Promise<ReplicationStreamObjectStorageDefinitionUsageResult[]> {
    return db.client.withSession({ snapshot: true }, async (session) => {
      // Deliberately read the whole usage collection. The number of replication streams is
      // expected to stay low, so a collection scan is cheaper than issuing one _id range query
      // per stream (and the usage collection is bounded by streams and writers, not buckets).
      const entries = await db.objectStorageUsage
        .aggregate<{
          _id: { replication_stream_id: number; definition_id: BucketDefinitionId };
          active_bytes: bigint;
        }>(
          [
            { $project: { replication_stream_id: '$_id.g', definitions: { $objectToArray: '$definitions' } } },
            { $unwind: '$definitions' },
            {
              $group: {
                _id: {
                  replication_stream_id: '$replication_stream_id',
                  definition_id: '$definitions.k'
                },
                active_bytes: { $sum: '$definitions.v' }
              }
            }
          ],
          { session, readConcern: 'snapshot' }
        )
        .toArray()
        .catch((error) => {
          if (lib_mongo.isMongoServerError(error) && error.codeName === 'NamespaceNotFound') {
            return [];
          }
          throw error;
        });

      return entries.map((entry) => {
        const activeBytes = BigInt(entry.active_bytes ?? 0);
        if (activeBytes < 0n) {
          return {
            replication_stream_id: entry._id.replication_stream_id,
            definition_id: entry._id.definition_id,
            active_bytes: 0n
          };
        }
        return {
          replication_stream_id: entry._id.replication_stream_id,
          definition_id: entry._id.definition_id,
          active_bytes: activeBytes
        };
      });
    });
  }

  static async readAllStreamUsage(db: VersionedPowerSyncMongoV3): Promise<ReplicationStreamObjectStorageUsageResult[]> {
    const definitionUsage = await this.readAllDefinitionUsage(db);
    const totals = new Map<number, bigint>();
    for (const entry of definitionUsage) {
      totals.set(entry.replication_stream_id, (totals.get(entry.replication_stream_id) ?? 0n) + entry.active_bytes);
    }

    return [...totals].map(([replicationStreamId, activeBytes]) => ({
      replication_stream_id: replicationStreamId,
      active_bytes: activeBytes
    }));
  }

  async applyDelta(definitionId: BucketDefinitionId, delta: bigint, session: mongo.ClientSession): Promise<void> {
    await this.applyDeltas(new Map([[definitionId, delta]]), session);
  }

  async applyDeltas(deltas: ReadonlyMap<BucketDefinitionId, bigint>, session: mongo.ClientSession): Promise<void> {
    if (this.writerId == null) {
      throw new ReplicationAssertionError('A writer id is required to apply object-storage usage');
    }

    const increments: Record<string, bigint> = {};
    for (const [definitionId, delta] of deltas) {
      if (delta === 0n) {
        continue;
      }
      increments[this.definitionPath(definitionId)] = delta;
    }
    if (Object.keys(increments).length === 0) {
      return;
    }

    await this.db.objectStorageUsage.updateOne(
      { _id: this.documentId() },
      {
        $inc: increments,
        $currentDate: { updated_at: true }
      },
      { upsert: true, session }
    );
  }

  async readEntries(): Promise<ObjectStorageUsageEntry[]> {
    return this.db.client.withSession({ snapshot: true }, (session) => this.readEntriesInSession(session));
  }

  async readStreamUsage(): Promise<ReplicationStreamObjectStorageUsageResult> {
    const entries = await this.readEntries();
    const activeBytes = entries.reduce((sum, entry) => sum + entry.active_bytes, 0n);
    this.assertNonNegative(activeBytes, 'stream');
    return {
      replication_stream_id: this.replicationStreamId,
      active_bytes: activeBytes
    };
  }

  async readEntriesInSession(session: mongo.ClientSession): Promise<ObjectStorageUsageEntry[]> {
    const entries = await this.db.objectStorageUsage
      .aggregate<{ _id: BucketDefinitionId; active_bytes: bigint }>(
        [
          { $match: { '_id.g': this.replicationStreamId } },
          { $project: { definitions: { $objectToArray: '$definitions' } } },
          { $unwind: '$definitions' },
          {
            $group: {
              _id: '$definitions.k',
              active_bytes: { $sum: '$definitions.v' }
            }
          }
        ],
        { session, readConcern: 'snapshot' }
      )
      .toArray()
      .catch((error) => {
        if (lib_mongo.isMongoServerError(error) && error.codeName === 'NamespaceNotFound') {
          return [];
        }
        throw error;
      });

    return entries.map((entry) => {
      const activeBytes = BigInt(entry.active_bytes ?? 0);
      this.assertNonNegative(activeBytes, `definition ${entry._id}`);
      return { definition_id: entry._id, active_bytes: activeBytes };
    });
  }

  async removeDefinition(definitionId: BucketDefinitionId, session: mongo.ClientSession): Promise<void> {
    await this.db.objectStorageUsage.updateMany(
      { '_id.g': this.replicationStreamId },
      { $unset: { [this.definitionPath(definitionId)]: 1 } },
      { session }
    );
  }

  async removeStream(session: mongo.ClientSession): Promise<void> {
    await this.db.objectStorageUsage.deleteMany({ '_id.g': this.replicationStreamId }, { session });
  }

  async foldStaleWriterDeltas(
    options: {
      staleWriterMs?: number;
      limit?: number;
    } = {}
  ): Promise<void> {
    const staleWriterMs = options.staleWriterMs ?? DEFAULT_OBJECT_STORAGE_USAGE_STALE_WRITER_MS;
    const limit = options.limit ?? DEFAULT_OBJECT_STORAGE_USAGE_FOLD_LIMIT;
    if (limit <= 0) {
      return;
    }

    await this.db.client.withSession((session) =>
      session.withTransaction(
        async () => {
          const staleDocuments = await this.db.objectStorageUsage
            .find(
              {
                '_id.g': this.replicationStreamId,
                '_id.w': { $ne: OBJECT_STORAGE_USAGE_BASE_WRITER_ID },
                $expr: {
                  $lt: [
                    '$updated_at',
                    { $dateSubtract: { startDate: '$$NOW', unit: 'millisecond', amount: staleWriterMs } }
                  ]
                }
              },
              { session, sort: { updated_at: 1 }, limit }
            )
            .toArray();
          if (staleDocuments.length === 0) {
            return;
          }

          const deltas = new Map<BucketDefinitionId, bigint>();
          for (const document of staleDocuments) {
            for (const [definitionId, delta] of Object.entries(document.definitions ?? {})) {
              this.validateDefinitionId(definitionId);
              deltas.set(definitionId, (deltas.get(definitionId) ?? 0n) + BigInt(delta));
            }
          }

          const increments: Record<string, bigint> = {};
          for (const [definitionId, delta] of deltas) {
            if (delta !== 0n) {
              increments[this.definitionPath(definitionId)] = delta;
            }
          }
          if (Object.keys(increments).length > 0) {
            await this.db.objectStorageUsage.updateOne(
              { _id: this.documentId(OBJECT_STORAGE_USAGE_BASE_WRITER_ID) },
              {
                $inc: increments,
                $currentDate: { updated_at: true }
              },
              { upsert: true, session }
            );
          }

          await this.db.objectStorageUsage.deleteMany(
            {
              '_id.g': this.replicationStreamId,
              _id: { $in: staleDocuments.map((document) => document._id) }
            },
            { session }
          );
        },
        { readConcern: { level: 'snapshot' }, writeConcern: { w: 'majority' } }
      )
    );
  }

  private definitionPath(definitionId: BucketDefinitionId): string {
    this.validateDefinitionId(definitionId);
    return `definitions.${definitionId}`;
  }

  private documentId(writerId = this.writerId): ObjectStorageUsageDocument['_id'] {
    if (writerId == null) {
      throw new ReplicationAssertionError('A writer id is required to identify object-storage usage');
    }
    return {
      g: this.replicationStreamId,
      w: writerId
    };
  }

  private validateDefinitionId(definitionId: BucketDefinitionId): void {
    if (definitionId.length === 0 || definitionId.includes('.') || definitionId.includes('$')) {
      throw new ReplicationAssertionError(`Invalid bucket definition id for object-storage usage: ${definitionId}`);
    }
  }

  private assertNonNegative(value: bigint, target: string): void {
    if (value < 0n) {
      throw new ReplicationAssertionError(`Negative active object-storage usage for ${target}: ${value}`);
    }
  }
}
