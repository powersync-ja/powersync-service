import { mongo } from '@powersync/lib-service-mongodb';
import {
  applyRowContext,
  CompatibilityContext,
  CompatibilityEdition,
  SqliteInputRow,
  SqlSyncRules,
  TimeValue,
  CustomArray
} from '@powersync/service-sync-rules';
import { describe, expect, test } from 'vitest';

import { MongoRouteAPIAdapter } from '@module/api/MongoRouteAPIAdapter.js';
import { ChangeStream } from '@module/replication/ChangeStream.js';
import { constructAfterRecord } from '@module/replication/MongoRelation.js';
import { PostImagesOption } from '@module/types/types.js';
import { clearTestDb, connectMongoData, TEST_CONNECTION_OPTIONS } from './util.js';

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
        float: 3.14,
        decimal: new mongo.Decimal128('3.14')
      },
      { _id: 2 as any, nested: { test: 'thing' } },
      { _id: 3 as any, date: new Date('2023-03-06 15:47+02') },
      {
        _id: 4 as any,
        timestamp: mongo.Timestamp.fromBits(123, 456),
        objectId: mongo.ObjectId.createFromHexString('66e834cc91d805df11fa0ecb'),
        regexp: new mongo.BSONRegExp('test', 'i'),
        minKey: new mongo.MinKey(),
        maxKey: new mongo.MaxKey(),
        symbol: new mongo.BSONSymbol('test'),
        js: new mongo.Code('testcode'),
        js2: new mongo.Code('testcode', { foo: 'bar' }),
        pointer: new mongo.DBRef('mycollection', mongo.ObjectId.createFromHexString('66e834cc91d805df11fa0ecb')),
        pointer2: new mongo.DBRef(
          'mycollection',
          mongo.ObjectId.createFromHexString('66e834cc91d805df11fa0ecb'),
          'mydb',
          { foo: 'bar' }
        )
      },
      {
        _id: 6 as any,
        int4: -1,
        int8: -9007199254740993n,
        float: -3.14,
        decimal: new mongo.Decimal128('-3.14')
      }
    ]);
  }

  async function insertUndefined(db: mongo.Db, collection: string, array?: boolean) {
    // MongoDB has deprecated the `undefined` value, making it really
    // difficult to insert one into the database.
    // mapReduce is also deprecated, but it's one way to still generate
    // the value.
    const mapInput = db.collection('map_input');
    await mapInput.insertOne({ test: 'test' });
    const fin = array ? `return { result: [undefined] }` : `return { result: undefined }`;
    await db.command({
      mapReduce: 'map_input',
      map: new mongo.Code(`function () {
          // We only need to emit once for a single result:
          emit(5, {});
        }`),
      reduce: new mongo.Code(`function (key, values) {
          // Return an object whose property is explicitly set to undefined
          return undefined;
        }`),
      finalize: new mongo.Code(`function (key, reducedVal) {
          ${fin};
        }`),
      out: { merge: 'map_output' }
    });

    await db
      .collection('map_output')
      .aggregate([
        { $set: { undefined: '$value.result' } },
        { $project: { undefined: 1 } },
        {
          $merge: {
            into: collection
          }
        }
      ])
      .toArray();

    await mapInput.drop();
    await db.collection('map_output').drop();
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
        float: [3.14],
        decimal: [new mongo.Decimal128('3.14')]
      },
      { _id: 2 as any, nested: [{ test: 'thing' }] },
      { _id: 3 as any, date: [new Date('2023-03-06 15:47+02')] },
      {
        _id: 6 as any,
        int4: [-1],
        int8: [-9007199254740993n],
        float: [-3.14],
        decimal: [new mongo.Decimal128('-3.14')]
      },
      {
        _id: 10 as any,
        timestamp: [mongo.Timestamp.fromBits(123, 456)],
        objectId: [mongo.ObjectId.createFromHexString('66e834cc91d805df11fa0ecb')],
        regexp: [new mongo.BSONRegExp('test', 'i')],
        minKey: [new mongo.MinKey()],
        maxKey: [new mongo.MaxKey()],
        symbol: [new mongo.BSONSymbol('test')],
        js: [new mongo.Code('testcode')],
        pointer: [new mongo.DBRef('mycollection', mongo.ObjectId.createFromHexString('66e834cc91d805df11fa0ecb'))],
        undefined: [undefined]
      }
    ]);
  }

  function checkResults(transformed: SqliteInputRow[]) {
    const sqliteValue = transformed.map((e) => applyRowContext(e, CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY));

    expect(sqliteValue[0]).toMatchObject({
      _id: 1n,
      text: 'text',
      uuid: 'baeb2514-4c57-436d-b3cc-c1256211656d',
      bool: 1n,
      bytea: new Uint8Array([116, 101, 115, 116]),
      int2: 1000n,
      int4: 1000000n,
      int8: 9007199254740993n,
      float: 3.14,
      null: null,
      decimal: '3.14'
    });
    expect(sqliteValue[1]).toMatchObject({
      _id: 2n,
      nested: '{"test":"thing"}'
    });

    expect(sqliteValue[2]).toMatchObject({
      _id: 3n,
      date: new TimeValue('2023-03-06 13:47:00.000Z', '2023-03-06T13:47:00.000Z')
    });

    expect(sqliteValue[3]).toMatchObject({
      _id: 4n,
      objectId: '66e834cc91d805df11fa0ecb',
      timestamp: 1958505087099n,
      regexp: '{"pattern":"test","options":"i"}',
      minKey: null,
      maxKey: null,
      symbol: 'test',
      js: '{"code":"testcode","scope":null}',
      js2: '{"code":"testcode","scope":{"foo":"bar"}}',
      pointer: '{"collection":"mycollection","oid":"66e834cc91d805df11fa0ecb","fields":{}}',
      pointer2: '{"collection":"mycollection","oid":"66e834cc91d805df11fa0ecb","db":"mydb","fields":{"foo":"bar"}}'
    });

    // This must specifically be null, and not undefined.
    expect(sqliteValue[4].undefined).toBeNull();

    expect(sqliteValue[5]).toMatchObject({
      _id: 6n,
      int4: -1n,
      int8: -9007199254740993n,
      float: -3.14,
      decimal: '-3.14'
    });
  }

  function checkResultsNested(transformed: SqliteInputRow[]) {
    const sqliteValue = transformed.map((e) => applyRowContext(e, CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY));

    expect(sqliteValue[0]).toMatchObject({
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
    expect(sqliteValue[1]).toMatchObject({
      _id: 2n,
      nested: '[{"test":"thing"}]'
    });

    expect(sqliteValue[2]).toMatchObject({
      _id: 3n,
      date: ['2023-03-06 13:47:00.000Z']
    });

    expect(sqliteValue[3]).toMatchObject({
      _id: 5n,
      undefined: '[null]'
    });

    expect(sqliteValue[4]).toMatchObject({
      _id: 6n,
      int4: '[-1]',
      int8: '[-9007199254740993]',
      float: '[-3.14]',
      decimal: '["-3.14"]'
    });

    expect(sqliteValue[5]).toMatchObject({
      _id: 10n,
      objectId: '["66e834cc91d805df11fa0ecb"]',
      timestamp: '[1958505087099]',
      regexp: '[{"pattern":"test","options":"i"}]',
      symbol: '["test"]',
      js: '[{"code":"testcode","scope":null}]',
      pointer: '[{"collection":"mycollection","oid":"66e834cc91d805df11fa0ecb","fields":{}}]',
      minKey: '[null]',
      maxKey: '[null]'
    });
  }

  test('test direct queries', async () => {
    const { db, client } = await connectMongoData();

    const collection = db.collection('test_data');
    try {
      await setupTable(db);
      await insert(collection);
      await insertUndefined(db, 'test_data');

      const rawResults = await db
        .collection('test_data')
        .find({}, { sort: { _id: 1 } })
        .toArray();
      // It is tricky to save "undefined" with mongo, so we check that it succeeded.
      expect(rawResults[4].undefined).toBeUndefined();
      const transformed = [...ChangeStream.getQueryData(rawResults)];
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
      await insertUndefined(db, 'test_data_arrays', true);

      const rawResults = await db
        .collection('test_data_arrays')
        .find({}, { sort: { _id: 1 } })
        .toArray();
      expect(rawResults[3].undefined).toEqual([undefined]);
      const transformed = [...ChangeStream.getQueryData(rawResults)];

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
        maxAwaitTimeMS: 50,
        fullDocument: 'updateLookup'
      });

      await stream.tryNext();

      await insert(collection);
      await insertUndefined(db, 'test_data');

      const transformed = await getReplicationTx(stream, 6);

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
        maxAwaitTimeMS: 50,
        fullDocument: 'updateLookup'
      });

      await stream.tryNext();

      await insertNested(collection);
      await insertUndefined(db, 'test_data_arrays', true);

      const transformed = await getReplicationTx(stream, 6);

      checkResultsNested(transformed);
    } finally {
      await client.close();
    }
  });

  test('connection schema', async () => {
    await using adapter = new MongoRouteAPIAdapter({
      type: 'mongodb',
      ...TEST_CONNECTION_OPTIONS
    });
    const db = adapter.db;
    await clearTestDb(db);

    const collection = db.collection('test_data');
    await setupTable(db);
    await insert(collection);
    await insertUndefined(db, 'test_data');

    const schema = await adapter.getConnectionSchema();
    const dbSchema = schema.filter((s) => s.name == TEST_CONNECTION_OPTIONS.database)[0];
    expect(dbSchema).not.toBeNull();
    expect(dbSchema.tables).toMatchObject([
      {
        name: 'test_data',
        columns: [
          { name: '_id', sqlite_type: 4, internal_type: 'Integer' },
          { name: 'bool', sqlite_type: 4, internal_type: 'Boolean' },
          { name: 'bytea', sqlite_type: 1, internal_type: 'Binary' },
          { name: 'date', sqlite_type: 2, internal_type: 'Date' },
          { name: 'decimal', sqlite_type: 2, internal_type: 'Decimal' },
          { name: 'float', sqlite_type: 8, internal_type: 'Double' },
          { name: 'int2', sqlite_type: 4, internal_type: 'Integer' },
          { name: 'int4', sqlite_type: 4, internal_type: 'Integer' },
          { name: 'int8', sqlite_type: 4, internal_type: 'Long' },
          // We can fix these later
          { name: 'js', sqlite_type: 2, internal_type: 'Object' },
          { name: 'js2', sqlite_type: 2, internal_type: 'Object' },
          { name: 'maxKey', sqlite_type: 0, internal_type: 'MaxKey' },
          { name: 'minKey', sqlite_type: 0, internal_type: 'MinKey' },
          { name: 'nested', sqlite_type: 2, internal_type: 'Object' },
          { name: 'null', sqlite_type: 0, internal_type: 'Null' },
          { name: 'objectId', sqlite_type: 2, internal_type: 'ObjectId' },
          // We can fix these later
          { name: 'pointer', sqlite_type: 2, internal_type: 'Object' },
          { name: 'pointer2', sqlite_type: 2, internal_type: 'Object' },
          { name: 'regexp', sqlite_type: 2, internal_type: 'RegExp' },
          // Can fix this later
          { name: 'symbol', sqlite_type: 2, internal_type: 'String' },
          { name: 'text', sqlite_type: 2, internal_type: 'String' },
          { name: 'timestamp', sqlite_type: 4, internal_type: 'Timestamp' },
          { name: 'undefined', sqlite_type: 0, internal_type: 'Null' },
          { name: 'uuid', sqlite_type: 2, internal_type: 'UUID' }
        ]
      }
    ]);
  });

  test('validate postImages', async () => {
    await using adapter = new MongoRouteAPIAdapter({
      type: 'mongodb',
      ...TEST_CONNECTION_OPTIONS,
      postImages: PostImagesOption.READ_ONLY
    });
    const db = adapter.db;
    await clearTestDb(db);

    const collection = db.collection('test_data');
    await setupTable(db);
    await insert(collection);

    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  global:
    data:
      - select _id as id, * from test_data

      `,
      {
        ...adapter.getParseSyncRulesOptions(),
        // No schema-based validation at this point
        schema: undefined
      }
    );
    const source_table_patterns = rules.getSourceTables();
    const results = await adapter.getDebugTablesInfo(source_table_patterns, rules);

    const result = results[0];
    expect(result).not.toBeNull();
    expect(result.table).toMatchObject({
      schema: 'powersync_test_data',
      name: 'test_data',
      replication_id: ['_id'],
      data_queries: true,
      parameter_queries: false,
      errors: [
        {
          level: 'fatal',
          message: 'changeStreamPreAndPostImages not enabled on powersync_test_data.test_data'
        }
      ]
    });
  });

  test('validate postImages - auto-configure', async () => {
    await using adapter = new MongoRouteAPIAdapter({
      type: 'mongodb',
      ...TEST_CONNECTION_OPTIONS,
      postImages: PostImagesOption.AUTO_CONFIGURE
    });
    const db = adapter.db;
    await clearTestDb(db);

    const collection = db.collection('test_data');
    await setupTable(db);
    await insert(collection);

    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  global:
    data:
      - select _id as id, * from test_data

      `,
      {
        ...adapter.getParseSyncRulesOptions(),
        // No schema-based validation at this point
        schema: undefined
      }
    );
    const source_table_patterns = rules.getSourceTables();
    const results = await adapter.getDebugTablesInfo(source_table_patterns, rules);

    const result = results[0];
    expect(result).not.toBeNull();
    expect(result.table).toMatchObject({
      schema: 'powersync_test_data',
      name: 'test_data',
      replication_id: ['_id'],
      data_queries: true,
      parameter_queries: false,
      errors: [
        {
          level: 'warning',
          message:
            'changeStreamPreAndPostImages not enabled on powersync_test_data.test_data, will be enabled automatically'
        }
      ]
    });
  });

  test('validate postImages - off', async () => {
    await using adapter = new MongoRouteAPIAdapter({
      type: 'mongodb',
      ...TEST_CONNECTION_OPTIONS,
      postImages: PostImagesOption.OFF
    });
    const db = adapter.db;
    await clearTestDb(db);

    const collection = db.collection('test_data');
    await setupTable(db);
    await insert(collection);

    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  global:
    data:
      - select _id as id, * from test_data

      `,
      {
        ...adapter.getParseSyncRulesOptions(),
        // No schema-based validation at this point
        schema: undefined
      }
    );
    const source_table_patterns = rules.getSourceTables();
    const results = await adapter.getDebugTablesInfo(source_table_patterns, rules);

    const result = results[0];
    expect(result).not.toBeNull();
    expect(result.table).toMatchObject({
      schema: 'powersync_test_data',
      name: 'test_data',
      replication_id: ['_id'],
      data_queries: true,
      parameter_queries: false,
      errors: []
    });
  });

  test('date format', async () => {
    const { db, client } = await connectMongoData();
    const collection = db.collection('test_data');
    try {
      await setupTable(db);
      await collection.insertOne({
        fraction: new Date('2023-03-06 15:47:01.123+02'),
        noFraction: new Date('2023-03-06 15:47:01+02')
      });

      const rawResults = await db
        .collection('test_data')
        .find({}, { sort: { _id: 1 } })
        .toArray();
      const [row] = [...ChangeStream.getQueryData(rawResults)];

      const oldFormat = applyRowContext(row, CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY);
      expect(oldFormat).toMatchObject({
        fraction: '2023-03-06 13:47:01.123Z',
        noFraction: '2023-03-06 13:47:01.000Z'
      });

      const newFormat = applyRowContext(row, new CompatibilityContext(CompatibilityEdition.SYNC_STREAMS));
      expect(newFormat).toMatchObject({
        fraction: '2023-03-06T13:47:01.123Z',
        noFraction: '2023-03-06T13:47:01.000Z'
      });
    } finally {
      await client.close();
    }
  });
});

/**
 * Return all the inserts from the first transaction in the replication stream.
 */
async function getReplicationTx(replicationStream: mongo.ChangeStream, count: number) {
  let transformed: SqliteInputRow[] = [];
  for await (const doc of replicationStream) {
    // Specifically filter out map_input / map_output collections
    if (!(doc as any)?.ns?.coll?.startsWith('test_data')) {
      continue;
    }
    transformed.push(constructAfterRecord((doc as any).fullDocument));
    if (transformed.length == count) {
      break;
    }
  }
  transformed.sort((a, b) => Number(a._id) - Number(b._id));
  return transformed;
}
