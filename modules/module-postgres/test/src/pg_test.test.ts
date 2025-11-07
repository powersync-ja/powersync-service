import * as pgwire from '@powersync/service-jpgwire';
import {
  applyRowContext,
  CompatibilityContext,
  SqliteInputRow,
  DateTimeValue,
  TimeValue,
  CompatibilityEdition
} from '@powersync/service-sync-rules';
import { describe, expect, test } from 'vitest';
import { clearTestDb, connectPgPool, connectPgWire, TEST_URI } from './util.js';
import { WalStream } from '@module/replication/WalStream.js';
import { PostgresTypeResolver } from '@module/types/resolver.js';
import { CustomTypeRegistry } from '@module/types/registry.js';

describe('pg data types', () => {
  async function setupTable(db: pgwire.PgClient) {
    await clearTestDb(db);
    await db.query(`CREATE TABLE test_data(
        id serial primary key,
        text text,
        uuid uuid,
        varchar varchar(255),
        bool bool,
        bytea bytea,
        int2 int2,
        int4 int4,
        int8 int8,
        float4 float4,
        float8 float8,
        numeric numeric, -- same as decimal
        json json,
        jsonb jsonb,
        pg_lsn pg_lsn,
        date date,
        time time,
        timestamp timestamp,
        timestamptz timestamptz,
        interval interval,
        macaddr macaddr,
        inet inet,
        oid oid
    )`);

    await db.query(`DROP TABLE IF EXISTS test_data_arrays`);
    await db.query(`CREATE TABLE test_data_arrays(
        id serial primary key,
        text text[],
        uuid uuid[],
        varchar varchar(255)[],
        bool bool[],
        bytea bytea[],
        int2 int2[],
        int4 int4[],
        int8 int8[],
        float4 float4[],
        float8 float8[],
        numeric numeric[], -- same as decimal
        json json[],
        jsonb jsonb[],
        pg_lsn pg_lsn[],
        date date[],
        time time[],
        timestamp timestamp[],
        timestamptz timestamptz[],
        interval interval[],
        macaddr macaddr[],
        inet inet[],
        oid oid[],
        multidimensional text[][] -- same as text[]
    )`);
  }

  async function insert(db: pgwire.PgClient) {
    await db.query(`
INSERT INTO test_data(id, text, uuid, varchar, bool, bytea, int2, int4, int8, numeric, float4, float8)
VALUES(1, 'text', 'baeb2514-4c57-436d-b3cc-c1256211656d', 'varchar', true, 'test', 1000, 1000000, 9007199254740993, 18014398509481982123, 3.14, 314);
    
INSERT INTO test_data(id, json, jsonb)
VALUES(2, '{"test": "thing" }', '{"test": "thing" }');

INSERT INTO test_data(id, date, time, timestamp, timestamptz)
VALUES(3, '2023-03-06', '15:47', '2023-03-06 15:47', '2023-03-06 15:47+02');

INSERT INTO test_data(id, pg_lsn, interval, macaddr, inet, oid)
VALUES(4, '016/B374D848', '1 hour', '00:00:5e:00:53:af', '127.0.0.1', 1007);

INSERT INTO test_data(id, date, time, timestamp, timestamptz)
VALUES(5, '-infinity'::date, 'allballs'::time, '-infinity'::timestamp, '-infinity'::timestamptz);

INSERT INTO test_data(id, timestamp, timestamptz)
VALUES(6, 'epoch'::timestamp, 'epoch'::timestamptz);

INSERT INTO test_data(id, timestamp, timestamptz)
VALUES(7, 'infinity'::timestamp, 'infinity'::timestamptz);

INSERT INTO test_data(id, timestamptz)
VALUES(8, '0022-02-03 12:13:14+03'::timestamptz);

INSERT INTO test_data(id, timestamptz)
VALUES(9, '10022-02-03 12:13:14+03'::timestamptz);
    `);
  }

  async function insertArrays(db: pgwire.PgClient) {
    await db.query(`
INSERT INTO test_data_arrays(id, text, uuid, varchar, bool, bytea, int2, int4, int8, numeric)
VALUES(1, ARRAY['text', '}te][xt{"'], '{"baeb2514-4c57-436d-b3cc-c1256211656d"}', '{"varchar"}', '{true}', '{"test"}', '{1000}', '{1000000}', '{9007199254740993}', '{18014398509481982123}');

INSERT INTO test_data_arrays(id, json, jsonb)
VALUES(2, ARRAY['{"test": "thing"}' :: json, '{"test": "}te][xt{"}' :: json], ARRAY['{"test": "thing", "foo": 5.0, "bignum": 18014398509481982123, "bool":true}' :: jsonb]);

INSERT INTO test_data_arrays(id, date, time, timestamp, timestamptz)
VALUES(3, ARRAY['2023-03-06'::date], ARRAY['15:47'::time], ARRAY['2023-03-06 15:47'::timestamp], ARRAY['2023-03-06 15:47+02'::timestamptz, '2023-03-06 15:47:00.123450+02'::timestamptz]);

INSERT INTO test_data_arrays(id, pg_lsn, interval, macaddr, inet, oid)
VALUES(4, ARRAY['016/B374D848'::pg_lsn], ARRAY['1 hour'::interval], ARRAY['00:00:5e:00:53:af'::macaddr], ARRAY['127.0.0.1'::inet], ARRAY[1007::oid]);

-- Empty arrays
INSERT INTO test_data_arrays(id, text, uuid, varchar, bool, bytea, int2, int4, int8, numeric)
VALUES(5, ARRAY[]::text[], ARRAY[]::uuid[], ARRAY[]::varchar[], ARRAY[]::bool[], ARRAY[]::bytea[], ARRAY[]::int2[], ARRAY[]::int4[], ARRAY[]::int8[], ARRAY[]::numeric[]);

-- Two-dimentional array
INSERT INTO test_data_arrays(id, multidimensional)
VALUES(6, ARRAY[['one', 1], ['two', 2], ['three', Null]]::TEXT[]);

-- Empty array
INSERT INTO test_data_arrays(id, multidimensional)
VALUES(7, ARRAY[[], [], []]::TEXT[]);

-- Empty array
INSERT INTO test_data_arrays(id, multidimensional)
VALUES(8, ARRAY[]::TEXT[]);

-- Array with only null
INSERT INTO test_data_arrays(id, multidimensional)
VALUES(9, ARRAY[NULL]::TEXT[]);

-- Array with 'null'
INSERT INTO test_data_arrays(id, multidimensional)
VALUES(10, ARRAY['null']::TEXT[]);
    `);
  }

  function checkResults(transformed: Record<string, any>[]) {
    expect(transformed[0]).toMatchObject({
      id: 1n,
      text: 'text',
      uuid: 'baeb2514-4c57-436d-b3cc-c1256211656d',
      varchar: 'varchar',
      bool: 1n,
      bytea: new Uint8Array([116, 101, 115, 116]),
      int2: 1000n,
      int4: 1000000n,
      int8: 9007199254740993n,
      float4: 3.14,
      float8: 314,
      numeric: '18014398509481982123'
    });
    expect(transformed[1]).toMatchObject({
      id: 2n,
      json: '{"test": "thing" }', // Whitespace preserved
      jsonb: '{"test": "thing"}' // Whitespace according to pg JSON conventions
    });

    expect(transformed[2]).toMatchObject({
      id: 3n,
      date: '2023-03-06',
      time: TimeValue.parse('15:47:00', pgwire.postgresTimeOptions),
      timestamp: new DateTimeValue('2023-03-06T15:47:00.000000', '2023-03-06 15:47:00', pgwire.postgresTimeOptions),
      timestamptz: new DateTimeValue('2023-03-06T13:47:00.000000Z', '2023-03-06 13:47:00Z', pgwire.postgresTimeOptions)
    });

    expect(transformed[3]).toMatchObject({
      id: 4n,
      pg_lsn: '00000016/B374D848',
      interval: '01:00:00',
      macaddr: '00:00:5e:00:53:af',
      inet: '127.0.0.1',
      oid: 1007n
    });

    expect(transformed[4]).toMatchObject({
      id: 5n,
      date: '0000-01-01',
      time: TimeValue.parse('00:00:00', pgwire.postgresTimeOptions),
      timestamp: new DateTimeValue('0000-01-01T00:00:00', undefined, pgwire.postgresTimeOptions),
      timestamptz: new DateTimeValue('0000-01-01T00:00:00Z', undefined, pgwire.postgresTimeOptions)
    });

    expect(transformed[5]).toMatchObject({
      id: 6n,
      timestamp: new DateTimeValue('1970-01-01T00:00:00.000000', '1970-01-01 00:00:00', pgwire.postgresTimeOptions),
      timestamptz: new DateTimeValue('1970-01-01T00:00:00.000000Z', '1970-01-01 00:00:00Z', pgwire.postgresTimeOptions)
    });

    expect(transformed[6]).toMatchObject({
      id: 7n,
      timestamp: new DateTimeValue('9999-12-31T23:59:59', undefined, pgwire.postgresTimeOptions),
      timestamptz: new DateTimeValue('9999-12-31T23:59:59Z', undefined, pgwire.postgresTimeOptions)
    });

    expect(transformed[7]).toMatchObject({
      id: 8n,
      timestamptz: new DateTimeValue('0022-02-03T09:13:14.000000Z', '0022-02-03 09:13:14Z', pgwire.postgresTimeOptions)
    });

    expect(transformed[8]).toMatchObject({
      id: 9n,
      // 10022-02-03 12:13:14+03 - out of range of both our date parsing logic, and sqlite's date functions
      // We can consider just preserving the source string as an alternative if this causes issues.
      timestamptz: null
    });
  }

  function checkResultArrays(transformed: Record<string, any>[]) {
    expect(transformed[0]).toMatchObject({
      id: 1n,
      text: `["text","}te][xt{\\""]`,
      uuid: '["baeb2514-4c57-436d-b3cc-c1256211656d"]',
      varchar: '["varchar"]',
      bool: '[1]',
      bytea: '[null]',
      int2: '[1000]',
      int4: '[1000000]',
      int8: `[9007199254740993]`,
      numeric: `["18014398509481982123"]`
    });

    // Note: Depending on to what extent we use the original postgres value, the whitespace may change, and order may change.
    // We do expect that decimals and big numbers are preserved.
    expect(transformed[1]).toMatchObject({
      id: 2n,
      // Expected output after a serialize + parse cycle:
      // json: `[{"test":"thing"},{"test":"}te][xt{"}]`,
      // jsonb: `[{"foo":5.0,"bool":true,"test":"thing","bignum":18014398509481982123}]`
      // Expected using direct PG values:
      json: `[{"test": "thing"},{"test": "}te][xt{"}]`,
      jsonb: `[{"foo": 5.0, "bool": true, "test": "thing", "bignum": 18014398509481982123}]`
    });

    expect(transformed[2]).toMatchObject({
      id: 3n,
      date: `["2023-03-06"]`,
      time: `["15:47:00"]`,
      timestamp: '["2023-03-06 15:47:00"]',
      timestamptz: '["2023-03-06 13:47:00Z","2023-03-06 13:47:00.12345Z"]'
    });

    expect(transformed[3]).toMatchObject({
      id: 4n,
      pg_lsn: `["00000016/B374D848"]`,
      interval: `["01:00:00"]`,
      macaddr: `["00:00:5e:00:53:af"]`,
      inet: `["127.0.0.1"]`,
      oid: `[1007]`
    });

    expect(transformed[4]).toMatchObject({
      id: 5n,
      text: '[]',
      uuid: '[]',
      varchar: '[]',
      bool: '[]',
      bytea: '[]',
      int2: '[]',
      int4: '[]',
      int8: '[]',
      numeric: '[]'
    });

    expect(transformed[5]).toMatchObject({
      id: 6n,
      multidimensional: '[["one","1"],["two","2"],["three",null]]'
    });

    expect(transformed[6]).toMatchObject({
      id: 7n,
      multidimensional: '[]'
    });

    expect(transformed[7]).toMatchObject({
      id: 8n,
      multidimensional: '[]'
    });

    expect(transformed[8]).toMatchObject({
      id: 9n,
      multidimensional: '[null]'
    });

    expect(transformed[9]).toMatchObject({
      id: 10n,
      multidimensional: '["null"]'
    });
  }

  test('test direct queries', async () => {
    const db = await connectPgPool();
    try {
      await setupTable(db);

      await insert(db);

      const transformed = [
        ...WalStream.getQueryData(pgwire.pgwireRows(await db.query(`SELECT * FROM test_data ORDER BY id`)))
      ];

      checkResults(transformed);
    } finally {
      await db.end();
    }
  });

  test('test direct queries - parameterized', async () => {
    // Parameterized queries may use a different underlying protocol,
    // so we make sure it also gets the same results.
    const db = await connectPgPool();
    try {
      await setupTable(db);

      await insert(db);

      const transformed = [
        ...WalStream.getQueryData(
          pgwire.pgwireRows(
            await db.query({
              statement: `SELECT * FROM test_data WHERE $1 ORDER BY id`,
              params: [{ type: 'bool', value: true }]
            })
          )
        )
      ];

      checkResults(transformed);
    } finally {
      await db.end();
    }
  });

  test('test direct queries - arrays', async () => {
    const db = await connectPgPool();
    try {
      await setupTable(db);

      await insertArrays(db);

      const transformed = [
        ...WalStream.getQueryData(pgwire.pgwireRows(await db.query(`SELECT * FROM test_data_arrays ORDER BY id`)))
      ].map((e) => applyRowContext(e, CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY));

      checkResultArrays(transformed);
    } finally {
      await db.end();
    }
  });

  test('test replication', async () => {
    const db = await connectPgPool();
    try {
      await setupTable(db);

      const slotName = 'test_slot';

      await db.query({
        statement: 'SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1',
        params: [{ type: 'varchar', value: slotName }]
      });

      await db.query({
        statement: `SELECT slot_name, lsn FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')`,
        params: [{ type: 'varchar', value: slotName }]
      });

      await insert(db);

      const pg: pgwire.PgConnection = await pgwire.pgconnect({ replication: 'database' }, TEST_URI);
      const replicationStream = await pg.logicalReplication({
        slot: slotName,
        options: {
          proto_version: '1',
          publication_names: 'powersync'
        }
      });

      const transformed = await getReplicationTx(db, replicationStream);
      await pg.end();

      checkResults(transformed);
    } finally {
      await db.end();
    }
  });

  test('test replication - arrays', async () => {
    const db = await connectPgPool();
    try {
      await setupTable(db);

      const slotName = 'test_slot';

      await db.query({
        statement: 'SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1',
        params: [{ type: 'varchar', value: slotName }]
      });

      await db.query({
        statement: `SELECT slot_name, lsn FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')`,
        params: [{ type: 'varchar', value: slotName }]
      });

      await insertArrays(db);

      const pg: pgwire.PgConnection = await pgwire.pgconnect({ replication: 'database' }, TEST_URI);
      const replicationStream = await pg.logicalReplication({
        slot: slotName,
        options: {
          proto_version: '1',
          publication_names: 'powersync'
        }
      });

      const transformed = await getReplicationTx(db, replicationStream);
      await pg.end();

      checkResultArrays(transformed.map((e) => applyRowContext(e, CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY)));
    } finally {
      await db.end();
    }
  });

  test('schema', async function () {
    const db = await connectPgWire();

    await setupTable(db);

    // TODO need a test for adapter
    // const schema = await api.getConnectionsSchema(db);
    // expect(schema).toMatchSnapshot();
  });

  test('date formats', async () => {
    const db = await connectPgWire();
    try {
      await setupTable(db);

      await db.query(`
INSERT INTO test_data(id, time, timestamp, timestamptz) VALUES (1, '17:42:01.12', '2023-03-06 15:47:12.4', '2023-03-06 15:47+02');
`);

      const [row] = [
        ...WalStream.getQueryData(
          pgwire.pgwireRows(await db.query(`SELECT time, timestamp, timestamptz FROM test_data`))
        )
      ];

      const oldFormat = applyRowContext(row, CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY);
      expect(oldFormat).toMatchObject({
        time: '17:42:01.12',
        timestamp: '2023-03-06 15:47:12.4',
        timestamptz: '2023-03-06 13:47:00Z'
      });

      const newFormat = applyRowContext(row, new CompatibilityContext({ edition: CompatibilityEdition.SYNC_STREAMS }));
      expect(newFormat).toMatchObject({
        time: '17:42:01.120000',
        timestamp: '2023-03-06T15:47:12.400000',
        timestamptz: '2023-03-06T13:47:00.000000Z'
      });
    } finally {
      await db.end();
    }
  });

  test('test replication - custom types', async () => {
    const db = await connectPgPool();
    try {
      await clearTestDb(db);
      await db.query(`CREATE DOMAIN rating_value AS FLOAT CHECK (VALUE BETWEEN 0 AND 5);`);
      await db.query(`CREATE TYPE composite AS (foo rating_value[], bar TEXT);`);
      await db.query(`CREATE TYPE nested_composite AS (a BOOLEAN, b composite);`);
      await db.query(`CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')`);

      await db.query(`CREATE TABLE test_custom(
        id serial primary key,
        rating rating_value,
        composite composite,
        nested_composite nested_composite,
        boxes box[],
        mood mood
      );`);

      const slotName = 'test_slot';

      await db.query({
        statement: 'SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1',
        params: [{ type: 'varchar', value: slotName }]
      });

      await db.query({
        statement: `SELECT slot_name, lsn FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')`,
        params: [{ type: 'varchar', value: slotName }]
      });

      await db.query(`
        INSERT INTO test_custom
          (rating, composite, nested_composite, boxes, mood)
        VALUES (
          1,
          (ARRAY[2,3], 'bar'),
          (TRUE, (ARRAY[2,3], 'bar')),
          ARRAY[box(point '(1,2)', point '(3,4)'), box(point '(5, 6)', point '(7,8)')],
          'happy'
        );
      `);

      const pg: pgwire.PgConnection = await pgwire.pgconnect({ replication: 'database' }, TEST_URI);
      const replicationStream = await pg.logicalReplication({
        slot: slotName,
        options: {
          proto_version: '1',
          publication_names: 'powersync'
        }
      });

      const [transformed] = await getReplicationTx(db, replicationStream);
      await pg.end();

      const oldFormat = applyRowContext(transformed, CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY);
      expect(oldFormat).toMatchObject({
        rating: '1',
        composite: '("{2,3}",bar)',
        nested_composite: '(t,"(""{2,3}"",bar)")',
        boxes: '["(3","4)","(1","2);(7","8)","(5","6)"]',
        mood: 'happy'
      });

      const newFormat = applyRowContext(
        transformed,
        new CompatibilityContext({ edition: CompatibilityEdition.SYNC_STREAMS })
      );
      expect(newFormat).toMatchObject({
        rating: 1,
        composite: '{"foo":[2.0,3.0],"bar":"bar"}',
        nested_composite: '{"a":1,"b":{"foo":[2.0,3.0],"bar":"bar"}}',
        boxes: JSON.stringify(['(3,4),(1,2)', '(7,8),(5,6)']),
        mood: 'happy'
      });
    } finally {
      await db.end();
    }
  });

  test('test replication - multiranges', async () => {
    const db = await connectPgPool();

    if (!(await new PostgresTypeResolver(db).supportsMultiRanges())) {
      // This test requires Postgres 14 or later.
      return;
    }

    try {
      await clearTestDb(db);

      await db.query(`CREATE TABLE test_custom(
        id serial primary key,
        ranges int4multirange[]
      );`);

      const slotName = 'test_slot';

      await db.query({
        statement: 'SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1',
        params: [{ type: 'varchar', value: slotName }]
      });

      await db.query({
        statement: `SELECT slot_name, lsn FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')`,
        params: [{ type: 'varchar', value: slotName }]
      });

      await db.query(`
        INSERT INTO test_custom
          (ranges)
        VALUES (
          ARRAY[int4multirange(int4range(2, 4), int4range(5, 7, '(]'))]::int4multirange[]
        );
      `);

      const pg: pgwire.PgConnection = await pgwire.pgconnect({ replication: 'database' }, TEST_URI);
      const replicationStream = await pg.logicalReplication({
        slot: slotName,
        options: {
          proto_version: '1',
          publication_names: 'powersync'
        }
      });

      const [transformed] = await getReplicationTx(db, replicationStream);
      await pg.end();

      const oldFormat = applyRowContext(transformed, CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY);
      expect(oldFormat).toMatchObject({
        ranges: '{"{[2,4),[6,8)}"}'
      });

      const newFormat = applyRowContext(
        transformed,
        new CompatibilityContext({ edition: CompatibilityEdition.SYNC_STREAMS })
      );
      expect(newFormat).toMatchObject({
        ranges: JSON.stringify([
          [
            { lower: 2, upper: 4, lower_exclusive: 0, upper_exclusive: 1 },
            { lower: 6, upper: 8, lower_exclusive: 0, upper_exclusive: 1 }
          ]
        ])
      });
    } finally {
      await db.end();
    }
  });
});

/**
 * Return all the inserts from the first transaction in the replication stream.
 */
async function getReplicationTx(db: pgwire.PgClient, replicationStream: pgwire.ReplicationStream) {
  const typeCache = new PostgresTypeResolver(db);
  await typeCache.fetchTypesForSchema();

  let transformed: SqliteInputRow[] = [];
  for await (const batch of replicationStream.pgoutputDecode()) {
    for (const msg of batch.messages) {
      if (msg.tag == 'insert') {
        transformed.push(typeCache.constructAfterRecord(msg));
      } else if (msg.tag == 'commit') {
        return transformed;
      }
    }
  }
  return transformed;
}
