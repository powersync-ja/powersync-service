import { mongo } from '@powersync/lib-service-mongodb';
import { register } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import * as uniqueWriteCheckpointUserId from '../../src/migrations/db/migrations/1782950400001-unique-write-checkpoint-user-id.js';
import { MongoMigrationAgent } from '../../src/migrations/MongoMigrationAgent.js';
import { createPowerSyncMongo } from '../../src/storage/storage-index.js';
import { MongoStorageConfig } from '../../src/types/types.js';
import { env } from './env.js';

const MIGRATION_AGENT_FACTORY = () => {
  return new MongoMigrationAgent({ type: 'mongodb', uri: env.MONGO_TEST_URL });
};

describe('Mongo Migrations Store', () => register.registerMigrationTests(MIGRATION_AGENT_FACTORY));

describe('Mongo migrations', () => {
  test('deduplicates write checkpoints before creating unique user_id index', async () => {
    const storageConfig: MongoStorageConfig = {
      type: 'mongodb',
      uri: env.MONGO_TEST_URL,
      database: 'powersync_test_unique_write_checkpoint_user_id'
    };
    const db = createPowerSyncMongo(storageConfig);

    try {
      await db.drop();
      await db.write_checkpoints.createIndex({ user_id: 1 }, { name: 'user_id' });

      await db.write_checkpoints.insertMany([
        {
          _id: new mongo.ObjectId('000000000000000000000001'),
          user_id: 'user1',
          client_id: 1n,
          lsns: { '1': '1/0' },
          processed_at_lsn: null
        },
        {
          _id: new mongo.ObjectId('000000000000000000000003'),
          user_id: 'user1',
          client_id: 3n,
          lsns: { '1': '3/0' },
          processed_at_lsn: '3/0',
          checkpoint_requested_at: new Date('2024-01-01T00:00:00.000Z')
        },
        {
          _id: new mongo.ObjectId('000000000000000000000002'),
          user_id: 'user1',
          client_id: 2n,
          lsns: { '1': '2/0' },
          processed_at_lsn: null
        },
        {
          _id: new mongo.ObjectId('000000000000000000000004'),
          user_id: 'user2',
          client_id: 5n,
          lsns: { '1': '5/0' },
          processed_at_lsn: '5/0',
          checkpoint_requested_at: new Date('2024-01-01T00:00:00.000Z')
        },
        {
          _id: new mongo.ObjectId('000000000000000000000005'),
          user_id: 'user2',
          client_id: 5n,
          lsns: { '1': '6/0' },
          processed_at_lsn: null,
          checkpoint_requested_at: new Date('2024-02-01T00:00:00.000Z')
        },
        {
          _id: new mongo.ObjectId('000000000000000000000006'),
          user_id: 'user3',
          client_id: 7n,
          lsns: { '1': '7/0' },
          processed_at_lsn: null
        }
      ]);

      await uniqueWriteCheckpointUserId.up({
        service_context: {
          configuration: {
            storage: storageConfig
          }
        }
      } as any);

      const docs = await db.write_checkpoints.find({}, { sort: { user_id: 1 } }).toArray();
      expect(docs.map((doc) => ({ user_id: doc.user_id, client_id: doc.client_id, lsn: doc.lsns['1'] }))).toEqual([
        { user_id: 'user1', client_id: 3n, lsn: '3/0' },
        { user_id: 'user2', client_id: 5n, lsn: '6/0' },
        { user_id: 'user3', client_id: 7n, lsn: '7/0' }
      ]);

      const index = (await db.write_checkpoints.indexes()).find((index) => index.name == 'user_id');
      expect(index?.unique).toBe(true);

      // The write checkpoint API relies on upserts by user_id. This direct
      // duplicate insert proves MongoDB now enforces the single-row invariant
      // those upserts depend on, instead of allowing another matching document.
      await expect(
        db.write_checkpoints.insertOne({
          _id: new mongo.ObjectId('000000000000000000000007'),
          user_id: 'user1',
          client_id: 8n,
          lsns: { '1': '8/0' },
          processed_at_lsn: null
        })
      ).rejects.toThrow();
    } finally {
      await db.drop();
      await db.client.close();
    }
  });
});
