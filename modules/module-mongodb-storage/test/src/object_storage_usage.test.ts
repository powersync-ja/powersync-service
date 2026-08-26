import { MongoSyncBucketStorage } from '@module/storage/implementation/createMongoSyncBucketStorage.js';
import { BucketDataDocumentV3, ObjectStorageUsageDocument } from '@module/storage/implementation/v3/models.js';
import {
  OBJECT_STORAGE_USAGE_BASE_WRITER_ID,
  ObjectStorageUsage
} from '@module/storage/implementation/v3/object-storage/ObjectStorageUsage.js';
import { VersionedPowerSyncMongoV3 } from '@module/storage/implementation/v3/VersionedPowerSyncMongoV3.js';
import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';
import { updateSyncRulesFromYaml } from '@powersync/service-core';
import { BucketDefinitionId } from '@powersync/service-sync-rules';
import { describe, expect, test } from 'vitest';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';

const SYNC_RULES_YAML = `
bucket_definitions:
  global:
    data:
      - SELECT id FROM items
`;

type UsageContext = {
  db: VersionedPowerSyncMongoV3;
  bucketStorage: MongoSyncBucketStorage;
  definitionId: BucketDefinitionId;
};

type UsageEntry = {
  definition_id: BucketDefinitionId;
  active_bytes: bigint;
};

async function withUsageContext<T>(callback: (context: UsageContext) => Promise<T>): Promise<T> {
  await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
  const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
  const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;
  const db = bucketStorage.db as VersionedPowerSyncMongoV3;
  const definitionId = syncRules.syncConfigContent[0].mapping.allBucketDefinitionIds()[0];
  return callback({ db, bucketStorage, definitionId });
}

async function withTransaction<T>(
  db: VersionedPowerSyncMongoV3,
  callback: (session: mongo.ClientSession) => Promise<T>
): Promise<T> {
  const session = db.client.startSession();
  try {
    return await session.withTransaction(() => callback(session));
  } finally {
    await session.endSession();
  }
}

async function readUsageEntries(
  db: VersionedPowerSyncMongoV3,
  replicationStreamId: number,
  session?: mongo.ClientSession
): Promise<UsageEntry[]> {
  const entries = await db.objectStorageUsage
    .aggregate<{ _id: BucketDefinitionId; active_bytes: bigint }>(
      [
        { $match: { '_id.g': replicationStreamId } },
        { $project: { definitions: { $objectToArray: '$definitions' } } },
        { $unwind: '$definitions' },
        { $group: { _id: '$definitions.k', active_bytes: { $sum: '$definitions.v' } } }
      ],
      { session, readConcern: 'snapshot' }
    )
    .toArray()
    .catch((error) => {
      if (lib_mongo.isMongoNamespaceNotFoundError(error)) {
        return [];
      }
      throw error;
    });

  return entries.map((entry) => ({
    definition_id: entry._id,
    active_bytes: BigInt(entry.active_bytes ?? 0)
  }));
}

function usageDocument(
  replicationStreamId: number,
  writerId: string,
  definitions: Record<string, bigint>,
  updatedAt = new Date(0)
): ObjectStorageUsageDocument {
  return {
    _id: { g: replicationStreamId, w: writerId },
    updated_at: updatedAt,
    definitions
  };
}

function bucketDataDocument(
  replicationStreamId: number,
  definitionId: BucketDefinitionId,
  fileSize: number
): BucketDataDocumentV3 {
  return {
    _id: { b: `${replicationStreamId}-${definitionId}`, o: 1n },
    min_op: 1n,
    checksum: 1n,
    count: 1,
    size: fileSize,
    storage_ref: {
      path: `bucket-data/${replicationStreamId}/${definitionId}/1.bson`,
      file_size: fileSize
    }
  };
}

describe('ObjectStorageUsage', () => {
  test('counts a committed reference exactly once after an aborted transaction is retried', async () => {
    await withUsageContext(async ({ db, bucketStorage, definitionId }) => {
      const replicationStreamId = bucketStorage.replicationStreamId;
      const writerId = 'replication-writer';
      const usage = new ObjectStorageUsage(db, replicationStreamId, writerId);
      const bucketData = db.bucketData(replicationStreamId, definitionId);
      const document = bucketDataDocument(replicationStreamId, definitionId, 123);

      await expect(
        withTransaction(db, async (session) => {
          await bucketData.insertOne(document, { session });
          await usage.applyDelta(definitionId, 123n, session);
          throw new Error('abort this attempt');
        })
      ).rejects.toThrow('abort this attempt');

      await withTransaction(db, async (session) => {
        await bucketData.insertOne(document, { session });
        await usage.applyDelta(definitionId, 123n, session);
      });

      expect(await bucketData.countDocuments({ storage_ref: { $exists: true } })).toBe(1);
      await expect(readUsageEntries(db, replicationStreamId)).resolves.toEqual([
        { definition_id: definitionId, active_bytes: 123n }
      ]);
    });
  });

  test('snapshot usage reads observe a complete pre-fold view', async () => {
    await withUsageContext(async ({ db, bucketStorage, definitionId }) => {
      const replicationStreamId = bucketStorage.replicationStreamId;
      const usage = new ObjectStorageUsage(db, replicationStreamId);
      await db.objectStorageUsage.insertMany([
        usageDocument(replicationStreamId, OBJECT_STORAGE_USAGE_BASE_WRITER_ID, { [definitionId]: 100n }),
        usageDocument(replicationStreamId, 'replication', { [definitionId]: 20n }),
        usageDocument(replicationStreamId, 'compactor', { [definitionId]: -5n })
      ]);

      const readSession = db.client.startSession();
      try {
        readSession.startTransaction({ readConcern: { level: 'snapshot' } });
        await db.objectStorageUsage.findOne(
          { _id: { g: replicationStreamId, w: OBJECT_STORAGE_USAGE_BASE_WRITER_ID } },
          { session: readSession }
        );

        await usage.foldStaleWriterDeltas({ staleWriterMs: 0 });
        const entries = await readUsageEntries(db, replicationStreamId, readSession);

        await readSession.commitTransaction();
        expect(entries).toEqual([{ definition_id: definitionId, active_bytes: 115n }]);
      } catch (error) {
        await readSession.abortTransaction().catch(() => undefined);
        throw error;
      } finally {
        await readSession.endSession();
      }

      await expect(readUsageEntries(db, replicationStreamId)).resolves.toEqual([
        { definition_id: definitionId, active_bytes: 115n }
      ]);
    });
  });

  test('folding preserves totals across both writer/folder commit orders', async () => {
    await withUsageContext(async ({ db, bucketStorage, definitionId }) => {
      const replicationStreamId = bucketStorage.replicationStreamId;
      const writerId = 'reused-writer';
      const usage = new ObjectStorageUsage(db, replicationStreamId, writerId);

      await withTransaction(db, (session) => usage.applyDelta(definitionId, 10n, session));
      await db.objectStorageUsage.updateOne(
        { _id: { g: replicationStreamId, w: writerId } },
        { $set: { updated_at: new Date(0) } }
      );
      await usage.foldStaleWriterDeltas({ staleWriterMs: 0 });

      // The folder committed first and deleted the writer shard. The writer's
      // next transaction must recreate it through the upsert.
      await withTransaction(db, (session) => usage.applyDelta(definitionId, 4n, session));
      expect((await readUsageEntries(db, replicationStreamId))[0].active_bytes).toBe(14n);

      await db.objectStorageUsage.updateOne(
        { _id: { g: replicationStreamId, w: writerId } },
        { $set: { updated_at: new Date(0) } }
      );
      await usage.foldStaleWriterDeltas({ staleWriterMs: 0 });

      // The writer committed before the folder. Folding the recreated shard
      // must preserve the same visible total.
      expect((await readUsageEntries(db, replicationStreamId))[0].active_bytes).toBe(14n);
    });
  });

  test('definition cleanup racing a fold removes only that definition, and stream clear is isolated', async () => {
    await withUsageContext(async ({ db, bucketStorage, definitionId }) => {
      const replicationStreamId = bucketStorage.replicationStreamId;
      const otherStreamId = replicationStreamId + 1;
      const otherDefinitionId = 'other_definition';
      const usage = new ObjectStorageUsage(db, replicationStreamId);

      await db.objectStorageUsage.insertMany([
        usageDocument(replicationStreamId, OBJECT_STORAGE_USAGE_BASE_WRITER_ID, {
          [definitionId]: 3n,
          [otherDefinitionId]: 7n
        }),
        usageDocument(replicationStreamId, 'writer', { [definitionId]: 20n, [otherDefinitionId]: 11n }),
        usageDocument(otherStreamId, 'other-writer', { [definitionId]: 50n })
      ]);

      await Promise.all([
        usage.foldStaleWriterDeltas({ staleWriterMs: 0 }),
        withTransaction(db, (session) => usage.removeDefinition(definitionId, session))
      ]);

      await expect(readUsageEntries(db, replicationStreamId)).resolves.toEqual([
        { definition_id: otherDefinitionId, active_bytes: 18n }
      ]);

      await withTransaction(db, async (session) => {
        await new ObjectStorageUsage(db, otherStreamId).removeStream(session);
      });
      await db.objectStorageUsage.insertOne(usageDocument(otherStreamId, 'other-writer', { [definitionId]: 50n }));

      await withTransaction(db, async (session) => {
        await usage.removeStream(session);
      });

      await expect(ObjectStorageUsage.readAllDefinitionUsage(db)).resolves.toEqual([
        { replication_stream_id: otherStreamId, definition_id: definitionId, active_bytes: 50n }
      ]);
    });
  });

  test('returns no entries for an absent usage collection', async () => {
    await withUsageContext(async ({ db, bucketStorage }) => {
      await db.objectStorageUsage.drop().catch(() => undefined);
      await expect(readUsageEntries(db, bucketStorage.replicationStreamId)).resolves.toEqual([]);
    });
  });
});
