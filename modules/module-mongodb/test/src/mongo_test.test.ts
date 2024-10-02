import { ChangeStream } from '@module/replication/ChangeStream.js';
import * as mongo from 'mongodb';
import { describe, expect, test } from 'vitest';
import { clearTestDb, connectMongoData } from './util.js';
import { SqliteRow } from '@powersync/service-sync-rules';
import { constructAfterRecord } from '@module/replication/MongoRelation.js';

describe('mongo data types', () => {
  async function setupTable(db: mongo.Db) {
    await clearTestDb(db);
  }

  async function insert(collection: mongo.Collection) {
    await collection.insertMany([
      {
        _id: 1 as any,
        null: null,
        text: 'text',
        uuid: new mongo.UUID('baeb2514-4c57-436d-b3cc-c1256211656d'),
        bool: true,
        bytea: Buffer.from('test'),
        int2: 1000,
        int4: 1000000,
        int8: 9007199254740993n,
        float: 3.14
      },
      { _id: 2 as any, nested: { test: 'thing' } },
      { _id: 3 as any, date: new Date('2023-03-06 15:47+02') },
      {
        _id: 4 as any,
        timestamp: mongo.Timestamp.fromBits(123, 456),
        objectId: mongo.ObjectId.createFromHexString('66e834cc91d805df11fa0ecb')
      }
    ]);
  }

  async function insertNested(collection: mongo.Collection) {
    await collection.insertMany([
      {
        _id: 1 as any,
        null: [null],
        text: ['text'],
        uuid: [new mongo.UUID('baeb2514-4c57-436d-b3cc-c1256211656d')],
        bool: [true],
        bytea: [Buffer.from('test')],
        int2: [1000],
        int4: [1000000],
        int8: [9007199254740993n],
        float: [3.14]
      },
      { _id: 2 as any, nested: [{ test: 'thing' }] },
      { _id: 3 as any, date: [new Date('2023-03-06 15:47+02')] },
      {
        _id: 10 as any,
        timestamp: [mongo.Timestamp.fromBits(123, 456)],
        objectId: [mongo.ObjectId.createFromHexString('66e834cc91d805df11fa0ecb')]
      }
    ]);
  }

  function checkResults(transformed: Record<string, any>[]) {
    expect(transformed[0]).toMatchObject({
      _id: 1n,
      text: 'text',
      uuid: 'baeb2514-4c57-436d-b3cc-c1256211656d',
      bool: 1n,
      bytea: new Uint8Array([116, 101, 115, 116]),
      int2: 1000n,
      int4: 1000000n,
      int8: 9007199254740993n,
      float: 3.14,
      null: null
    });
    expect(transformed[1]).toMatchObject({
      _id: 2n,
      nested: '{"test":"thing"}'
    });

    expect(transformed[2]).toMatchObject({
      _id: 3n,
      date: '2023-03-06 13:47:00.000Z'
    });

    expect(transformed[3]).toMatchObject({
      _id: 4n,
      objectId: '66e834cc91d805df11fa0ecb',
      timestamp: 1958505087099n
    });
  }

  function checkResultsNested(transformed: Record<string, any>[]) {
    expect(transformed[0]).toMatchObject({
      _id: 1n,
      text: `["text"]`,
      uuid: '["baeb2514-4c57-436d-b3cc-c1256211656d"]',
      bool: '[1]',
      bytea: '[null]',
      int2: '[1000]',
      int4: '[1000000]',
      int8: `[9007199254740993]`,
      float: '[3.14]',
      null: '[null]'
    });

    // Note: Depending on to what extent we use the original postgres value, the whitespace may change, and order may change.
    // We do expect that decimals and big numbers are preserved.
    expect(transformed[1]).toMatchObject({
      _id: 2n,
      nested: '[{"test":"thing"}]'
    });

    expect(transformed[2]).toMatchObject({
      _id: 3n,
      date: '["2023-03-06 13:47:00.000Z"]'
    });

    expect(transformed[3]).toMatchObject({
      _id: 10n,
      objectId: '["66e834cc91d805df11fa0ecb"]',
      timestamp: '[1958505087099]'
    });
  }

  test('test direct queries', async () => {
    const { db, client } = await connectMongoData();
    const collection = db.collection('test_data');
    try {
      await setupTable(db);

      await insert(collection);

      const transformed = [...ChangeStream.getQueryData(await db.collection('test_data').find().toArray())];

      checkResults(transformed);
    } finally {
      await client.close();
    }
  });

  test('test direct queries - arrays', async () => {
    const { db, client } = await connectMongoData();
    const collection = db.collection('test_data_arrays');
    try {
      await setupTable(db);

      await insertNested(collection);

      const transformed = [...ChangeStream.getQueryData(await db.collection('test_data_arrays').find().toArray())];

      checkResultsNested(transformed);
    } finally {
      await client.close();
    }
  });

  test('test replication', async () => {
    // With MongoDB, replication uses the exact same document format
    // as normal queries. We test it anyway.
    const { db, client } = await connectMongoData();
    const collection = db.collection('test_data');
    try {
      await setupTable(db);

      const stream = db.watch([], {
        useBigInt64: true,
        maxAwaitTimeMS: 50,
        fullDocument: 'updateLookup'
      });

      await stream.tryNext();

      await insert(collection);

      const transformed = await getReplicationTx(stream, 4);

      checkResults(transformed);
    } finally {
      await client.close();
    }
  });

  test('test replication - arrays', async () => {
    const { db, client } = await connectMongoData();
    const collection = db.collection('test_data');
    try {
      await setupTable(db);

      const stream = db.watch([], {
        useBigInt64: true,
        maxAwaitTimeMS: 50,
        fullDocument: 'updateLookup'
      });

      await stream.tryNext();

      await insertNested(collection);

      const transformed = await getReplicationTx(stream, 4);

      checkResultsNested(transformed);
    } finally {
      await client.close();
    }
  });
});

/**
 * Return all the inserts from the first transaction in the replication stream.
 */
async function getReplicationTx(replicationStream: mongo.ChangeStream, count: number) {
  let transformed: SqliteRow[] = [];
  for await (const doc of replicationStream) {
    transformed.push(constructAfterRecord((doc as any).fullDocument));
    if (transformed.length == count) {
      break;
    }
  }
  return transformed;
}