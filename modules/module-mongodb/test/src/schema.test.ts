import { mongo } from '@powersync/lib-service-mongodb';
import { afterEach, beforeEach, describe, expect, test } from 'vitest';

import { inferCollectionSchema } from '@module/api/infer-collection-schema.js';
import { MongoRouteAPIAdapter } from '@module/api/MongoRouteAPIAdapter.js';
import { DATABASE_TYPE, DatabaseType } from './DatabaseType.js';
import { testTimeout } from './test-timeouts.js';
import { connectMongoData, requireFailCommand, TEST_CONNECTION_OPTIONS } from './util.js';

const isDocumentDb = DATABASE_TYPE == DatabaseType.DOCUMENTDB;

describe('collection schema inference', { timeout: testTimeout(20_000) }, () => {
  let client: mongo.MongoClient;
  let db: mongo.Db;

  beforeEach(async () => {
    ({ client } = await connectMongoData({ monitorCommands: true }));
    db = client.db(`${TEST_CONNECTION_OPTIONS.database}_schema`);
    await db.dropDatabase();
  });

  afterEach(async () => {
    await db.dropDatabase();
    await client.close();
  });

  test('merges types across documents and only infers top-level fields', async () => {
    const collection = db.collection('mixed');
    await collection.insertMany([
      {
        _id: 1 as any,
        value: null,
        whole: new mongo.Double(42),
        huge: new mongo.Double(1e100),
        fractional: new mongo.Double(-1.25),
        nan: NaN,
        positiveInfinity: Infinity,
        negativeInfinity: -Infinity,
        long: mongo.Long.fromString('9007199254740993'),
        array: [{ nested: 'value' }],
        object: { nested: true }
      },
      { _id: 2 as any, value: 'text', whole: new mongo.Int32(-42), array: [] },
      { _id: 3 as any, value: 42, whole: new mongo.Double(-0) },
      { _id: 4 as any, value: 1.25 },
      { _id: 5 as any, value: new mongo.Double(42) }
    ]);

    expect(await inferCollectionSchema(collection, isDocumentDb)).toMatchObject([
      { name: '_id', sqlite_type: 4, internal_type: 'Integer' },
      { name: 'array', sqlite_type: 2, internal_type: 'Array' },
      { name: 'fractional', sqlite_type: 8, internal_type: 'Double' },
      { name: 'huge', sqlite_type: 4, internal_type: 'Integer' },
      { name: 'long', sqlite_type: 4, internal_type: 'Long' },
      { name: 'nan', sqlite_type: 8, internal_type: 'Double' },
      { name: 'negativeInfinity', sqlite_type: 8, internal_type: 'Double' },
      { name: 'object', sqlite_type: 2, internal_type: 'Object' },
      { name: 'positiveInfinity', sqlite_type: 8, internal_type: 'Double' },
      { name: 'value', sqlite_type: 14, internal_type: 'Double | Integer | Null | String' },
      { name: 'whole', sqlite_type: 4, internal_type: 'Integer' }
    ]);
  });

  test('distinguishes UUIDs from binary values by subtype and length', async () => {
    const collection = db.collection('binary');
    await collection.insertMany([
      {
        _id: 1 as any,
        uuid: new mongo.UUID('00000000-0000-0000-0000-000000000000'),
        binary: new mongo.Binary(Buffer.alloc(16)),
        mixed: new mongo.UUID()
      },
      {
        _id: 2 as any,
        uuid: new mongo.UUID('ffffffff-ffff-ffff-ffff-ffffffffffff'),
        binary: new mongo.Binary(Buffer.alloc(16, 255)),
        mixed: new mongo.Binary(Buffer.alloc(16), mongo.Binary.SUBTYPE_UUID_OLD)
      },
      {
        _id: 3 as any,
        uuid: new mongo.UUID(),
        binary: new mongo.Binary(Buffer.alloc(16), mongo.Binary.SUBTYPE_MD5)
      },
      { _id: 4 as any, binary: new mongo.Binary(Buffer.alloc(15, 255), mongo.Binary.SUBTYPE_USER_DEFINED) },
      { _id: 5 as any, binary: new mongo.Binary(Buffer.alloc(17)) },
      { _id: 6 as any, binary: new mongo.Binary(Buffer.alloc(0)) }
    ]);

    expect(await inferCollectionSchema(collection, isDocumentDb)).toMatchObject([
      { name: '_id', sqlite_type: 4, internal_type: 'Integer' },
      { name: 'binary', sqlite_type: 1, internal_type: 'Binary' },
      { name: 'mixed', sqlite_type: 3, internal_type: 'Binary | UUID' },
      { name: 'uuid', sqlite_type: 2, internal_type: 'UUID' }
    ]);
  });

  test('handles empty collections and documents with only an id', async () => {
    const collection = await db.createCollection('empty');
    expect(await inferCollectionSchema(collection, isDocumentDb)).toEqual([]);

    await collection.insertOne({});
    expect(await inferCollectionSchema(collection, isDocumentDb)).toEqual([
      { name: '_id', sqlite_type: 2, type: 'ObjectId', internal_type: 'ObjectId', pg_type: 'ObjectId' }
    ]);
  });

  test.skipIf(DATABASE_TYPE == DatabaseType.DOCUMENTDB)(
    'preserves field names with a non-simple collation',
    async () => {
      const collection = await db.createCollection('collation', { collation: { locale: 'en', strength: 1 } });
      await collection.insertOne({ Name: 'text', name: 1, 'with.dot': true, $field: [] });

      const columns = await inferCollectionSchema(collection, isDocumentDb);
      expect(columns).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ name: 'Name', sqlite_type: 2 }),
          expect.objectContaining({ name: 'name', sqlite_type: 4 }),
          expect.objectContaining({ name: 'with.dot', sqlite_type: 4 }),
          expect.objectContaining({ name: '$field', internal_type: 'Array' })
        ])
      );
      expect(columns).toHaveLength(5);
    }
  );

  test('returns only small schema metadata for multi-megabyte documents', async () => {
    const collection = db.collection('large');
    const largeString = 'x'.repeat(2 * 1024 * 1024);
    for (let i = 0; i < 4; i++) {
      await collection.insertOne({ text: largeString, binary: Buffer.alloc(2 * 1024 * 1024), array: [largeString] });
    }

    const responseSizes: number[] = [];
    const executionLimits: number[] = [];
    client.on('commandStarted', (event: mongo.CommandStartedEvent) => {
      if (event.commandName == 'aggregate') {
        executionLimits.push(event.command.maxTimeMS);
      }
    });
    client.on('commandSucceeded', (event: mongo.CommandSucceededEvent) => {
      if (event.commandName == 'aggregate' || event.commandName == 'getMore') {
        responseSizes.push(mongo.BSON.calculateObjectSize(event.reply as mongo.Document));
      }
    });

    expect(await inferCollectionSchema(collection, isDocumentDb)).toMatchObject([
      { name: '_id', sqlite_type: 2, internal_type: 'ObjectId' },
      { name: 'array', sqlite_type: 2, internal_type: 'Array' },
      { name: 'binary', sqlite_type: 1, internal_type: 'Binary' },
      { name: 'text', sqlite_type: 2, internal_type: 'String' }
    ]);
    expect(responseSizes.length).toBeGreaterThan(0);
    expect(Math.max(...responseSizes)).toBeLessThan(4096);
    expect(executionLimits).toEqual([30_000]);
  });

  test.skipIf(DATABASE_TYPE == DatabaseType.DOCUMENTDB)('fails schema inference on a query timeout', async (ctx) => {
    await db.collection('timeout').insertOne({ value: 'text' });
    await using adapter = new MongoRouteAPIAdapter({
      type: 'mongodb',
      ...TEST_CONNECTION_OPTIONS,
      database: db.databaseName
    });
    await using failCommand = await requireFailCommand(client, ctx);
    await failCommand.configure({
      mode: { times: 1 },
      data: {
        failCommands: ['aggregate'],
        errorCode: 50 // MaxTimeMSExpired
      }
    });

    // Exercise the real server/driver error path without waiting for the full timeout.
    await expect(adapter.getConnectionSchema()).rejects.toMatchObject({
      name: 'MongoServerError',
      code: 50,
      codeName: 'MaxTimeMSExpired'
    });
  });
});
