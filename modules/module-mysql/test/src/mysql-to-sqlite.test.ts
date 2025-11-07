import { applyRowContext, CompatibilityContext, SqliteInputRow, SqliteRow } from '@powersync/service-sync-rules';
import { afterAll, describe, expect, test } from 'vitest';
import { clearTestDb, TEST_CONNECTION_OPTIONS } from './util.js';
import { eventIsWriteMutation, eventIsXid } from '@module/replication/zongji/zongji-utils.js';
import * as common from '@module/common/common-index.js';
import { BinLogEvent, ZongJi } from '@powersync/mysql-zongji';
import { MySQLConnectionManager } from '@module/replication/MySQLConnectionManager.js';
import { toColumnDescriptors } from '@module/common/common-index.js';

describe('MySQL Data Types', () => {
  const connectionManager = new MySQLConnectionManager(TEST_CONNECTION_OPTIONS, {});

  afterAll(async () => {
    await connectionManager.end();
  });

  async function setupTable() {
    const connection = await connectionManager.getConnection();
    await clearTestDb(connection);
    await connection.query(`CREATE TABLE test_data (
    tinyint_col TINYINT,
    smallint_col SMALLINT,
    mediumint_col MEDIUMINT,
    int_col INT,
    integer_col INTEGER,
    bigint_col BIGINT,
    float_col FLOAT,
    double_col DOUBLE,
    decimal_col DECIMAL(10,2),
    numeric_col NUMERIC(10,2),
    bit_col BIT(8),
    boolean_col BOOLEAN,
    serial_col SERIAL,
    
    date_col DATE,
    datetime_col DATETIME(3),
    timestamp_col TIMESTAMP(3),
    time_col TIME,
    year_col YEAR,

    char_col CHAR(10),
    varchar_col VARCHAR(255),
    varchar_binary_encoding_col VARCHAR(255) CHARACTER SET binary,
    varchar_with_bin_collation_col VARCHAR(255) COLLATE utf8mb4_bin,
    
    binary_col BINARY(16),
    varbinary_col VARBINARY(256),
    tinyblob_col TINYBLOB,
    blob_col BLOB,
    mediumblob_col MEDIUMBLOB,
    longblob_col LONGBLOB,
    tinytext_col TINYTEXT,
    text_col TEXT,
    mediumtext_col MEDIUMTEXT,
    longtext_col LONGTEXT,
    enum_col ENUM('value1', 'value2', 'value3'),
    set_col SET('value1', 'value2', 'value3'),

    json_col JSON,

    geometry_col GEOMETRY,
    point_col POINT,
    linestring_col LINESTRING,
    polygon_col POLYGON,
    multipoint_col MULTIPOINT,
    multilinestring_col MULTILINESTRING,
    multipolygon_col MULTIPOLYGON,
    geometrycollection_col GEOMETRYCOLLECTION
    )`);

    connection.release();
  }

  test('Number types mappings', async () => {
    await setupTable();
    await connectionManager.query(`
INSERT INTO test_data (
    tinyint_col,
    smallint_col,
    mediumint_col,
    int_col,
    integer_col,
    bigint_col,
    double_col,
    decimal_col,
    numeric_col,
    bit_col,
    boolean_col
    -- serial_col is auto-incremented and can be left out
) VALUES (
    127,                 -- TINYINT maximum value
    32767,               -- SMALLINT maximum value
    8388607,             -- MEDIUMINT maximum value
    2147483647,          -- INT maximum value
    2147483647,          -- INTEGER maximum value
    9223372036854775807, -- BIGINT maximum value
    3.1415926535,        -- DOUBLE example
    12345.67,            -- DECIMAL(10,2) example
    12345.67,            -- NUMERIC(10,2) example
    b'10101010',         -- BIT(8) example in binary notation
    TRUE                 -- BOOLEAN value (alias for TINYINT(1))
    -- serial_col is auto-incremented
)`);

    const databaseRows = await getDatabaseRows(connectionManager, 'test_data');
    const replicatedRows = await getReplicatedRows();

    const expectedResult = {
      tinyint_col: 127n,
      smallint_col: 32767n,
      mediumint_col: 8388607n,
      int_col: 2147483647n,
      integer_col: 2147483647n,
      bigint_col: 9223372036854775807n,
      double_col: 3.1415926535,
      decimal_col: 12345.67,
      numeric_col: 12345.67,
      bit_col: new Uint8Array([0b10101010]).valueOf(),
      boolean_col: 1n,
      serial_col: 1n
    };
    expect(databaseRows[0]).toMatchObject(expectedResult);
    expect(replicatedRows[0]).toMatchObject(expectedResult);
  });

  test('Float type mapping', async () => {
    await setupTable();
    const expectedFloatValue = 3.14;
    await connectionManager.query(`INSERT INTO test_data (float_col) VALUES (${expectedFloatValue})`);

    const databaseRows = await getDatabaseRows(connectionManager, 'test_data');
    const replicatedRows = await getReplicatedRows();

    const allowedPrecision = 0.0001;

    const actualFloatValueDB = databaseRows[0].float_col;
    let difference = Math.abs((actualFloatValueDB as number) - expectedFloatValue);
    expect(difference).toBeLessThan(allowedPrecision);

    const actualFloatValueReplicated = replicatedRows[0].float_col;
    difference = Math.abs((actualFloatValueReplicated as number) - expectedFloatValue);
    expect(difference).toBeLessThan(allowedPrecision);
  });

  test('Character types mappings', async () => {
    await setupTable();
    await connectionManager.query(`
INSERT INTO test_data (
    char_col,
    varchar_col,
    varchar_binary_encoding_col,
    varchar_with_bin_collation_col,
    binary_col,
    varbinary_col,
    tinyblob_col,
    blob_col,
    mediumblob_col,
    longblob_col,
    tinytext_col,
    text_col,
    mediumtext_col,
    longtext_col,
    enum_col
) VALUES (
    'CharData',               -- CHAR(10) with padding spaces
    'Variable character data',-- VARCHAR(255)
    'Varchar with binary encoding', -- VARCHAR(255) with binary encoding
    'Variable character data with bin collation', -- VARCHAR(255) with bin collation
    'ShortBin',               -- BINARY(16)
    'VariableBinaryData',     -- VARBINARY(256)
    'TinyBlobData',           -- TINYBLOB
    'BlobData',               -- BLOB
    'MediumBlobData',         -- MEDIUMBLOB
    'LongBlobData',           -- LONGBLOB
    'TinyTextData',           -- TINYTEXT
    'TextData',               -- TEXT
    'MediumTextData',         -- MEDIUMTEXT
    'LongTextData',           -- LONGTEXT
    'value1'                 -- ENUM('value1', 'value2', 'value3')
);`);

    const databaseRows = await getDatabaseRows(connectionManager, 'test_data');
    const replicatedRows = await getReplicatedRows();
    const expectedResult = {
      char_col: 'CharData',
      varchar_col: 'Variable character data',
      varchar_binary_encoding_col: new Uint8Array([
        86, 97, 114, 99, 104, 97, 114, 32, 119, 105, 116, 104, 32, 98, 105, 110, 97, 114, 121, 32, 101, 110, 99, 111,
        100, 105, 110, 103
      ]),
      varchar_with_bin_collation_col: 'Variable character data with bin collation',
      binary_col: new Uint8Array([83, 104, 111, 114, 116, 66, 105, 110, 0, 0, 0, 0, 0, 0, 0, 0]), // Pad with 0
      varbinary_col: new Uint8Array([
        0x56, 0x61, 0x72, 0x69, 0x61, 0x62, 0x6c, 0x65, 0x42, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x44, 0x61, 0x74, 0x61
      ]),
      tinyblob_col: new Uint8Array([0x54, 0x69, 0x6e, 0x79, 0x42, 0x6c, 0x6f, 0x62, 0x44, 0x61, 0x74, 0x61]),
      blob_col: new Uint8Array([0x42, 0x6c, 0x6f, 0x62, 0x44, 0x61, 0x74, 0x61]),
      mediumblob_col: new Uint8Array([
        0x4d, 0x65, 0x64, 0x69, 0x75, 0x6d, 0x42, 0x6c, 0x6f, 0x62, 0x44, 0x61, 0x74, 0x61
      ]),
      longblob_col: new Uint8Array([0x4c, 0x6f, 0x6e, 0x67, 0x42, 0x6c, 0x6f, 0x62, 0x44, 0x61, 0x74, 0x61]),
      tinytext_col: 'TinyTextData',
      text_col: 'TextData',
      mediumtext_col: 'MediumTextData',
      longtext_col: 'LongTextData',
      enum_col: 'value1'
    };

    expect(databaseRows[0]).toMatchObject(expectedResult);
    expect(replicatedRows[0]).toMatchObject(expectedResult);
  });

  test('Date types mappings', async () => {
    await setupTable();
    // Timezone offset is set on the pool to +00:00
    await connectionManager.query(`
        INSERT INTO test_data(date_col, datetime_col, timestamp_col, time_col, year_col)
        VALUES('2023-03-06', '2023-03-06 15:47', '2023-03-06 15:47', '15:47:00', '2023');
      `);

    const databaseRows = await getDatabaseRows(connectionManager, 'test_data');
    const replicatedRows = await getReplicatedRows();
    const expectedResult = {
      date_col: '2023-03-06',
      datetime_col: '2023-03-06T15:47:00.000Z',
      timestamp_col: '2023-03-06T15:47:00.000Z',
      time_col: '15:47:00',
      year_col: 2023
    };

    expect(applyRowContext(databaseRows[0], CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY)).toMatchObject(
      expectedResult
    );
    expect(applyRowContext(replicatedRows[0], CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY)).toMatchObject(
      expectedResult
    );
    expect(applyRowContext(databaseRows[0], new CompatibilityContext({ edition: 2 }))).toMatchObject(expectedResult);
    expect(applyRowContext(replicatedRows[0], new CompatibilityContext({ edition: 2 }))).toMatchObject(expectedResult);
  });

  test('Date types edge cases mappings', async () => {
    await setupTable();

    const connection = await connectionManager.getConnection();
    try {
      // Disable strict mode, to allow dates such as '2024-00-00'.
      await connection.query(`SET SESSION sql_mode=''`);
      await connection.query(`SET SESSION time_zone='+00:00'`);

      await connection.query(`INSERT INTO test_data(timestamp_col) VALUES('1970-01-01 00:00:01')`);
      await connection.query(`INSERT INTO test_data(timestamp_col) VALUES('2038-01-19 03:14:07.499')`);
      await connection.query(`INSERT INTO test_data(datetime_col) VALUES('1000-01-01 00:00:00')`);
      await connection.query(`INSERT INTO test_data(datetime_col) VALUES('9999-12-31 23:59:59.499')`);
      await connection.query(`INSERT INTO test_data(datetime_col) VALUES('0000-00-00 00:00:00')`);
      await connection.query(`INSERT INTO test_data(datetime_col) VALUES('2024-00-00 00:00:00')`);
      // TODO: This has a mismatch between querying directly and with Zongji.
      // await connection.query(`INSERT INTO test_data(date_col) VALUES('2024-00-00')`);

      const expectedResults = [
        { timestamp_col: '1970-01-01T00:00:01.000Z' },
        { timestamp_col: '2038-01-19T03:14:07.499Z' },
        { datetime_col: '1000-01-01T00:00:00.000Z' },
        { datetime_col: '9999-12-31T23:59:59.499Z' },
        { datetime_col: null },
        { datetime_col: null }
        // { date_col: '2023-11-30' } or { date_col: null }?
      ];

      const databaseRows = await getDatabaseRows(connectionManager, 'test_data');
      const replicatedRows = await getReplicatedRows(expectedResults.length);

      for (let i = 0; i < expectedResults.length; i++) {
        expect(applyRowContext(databaseRows[i], CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY)).toMatchObject(
          expectedResults[i]
        );
        expect(applyRowContext(replicatedRows[i], CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY)).toMatchObject(
          expectedResults[i]
        );
      }
    } finally {
      connection.release;
    }
  });

  test('Json types mappings', async () => {
    await setupTable();

    const expectedJSON = { name: 'John Doe', age: 30, married: true };
    const expectedSet = ['value1', 'value3'];

    // For convenience, we map the SET data type to a JSON Array
    await connectionManager.query(
      `INSERT INTO test_data (json_col, set_col) VALUES ('${JSON.stringify(expectedJSON)}', '${expectedSet.join(',')}')`
    );

    const databaseRows = await getDatabaseRows(connectionManager, 'test_data');
    const replicatedRows = await getReplicatedRows();

    const actualDBJSONValue = JSON.parse(databaseRows[0].json_col as string);
    const actualReplicatedJSONValue = JSON.parse(replicatedRows[0].json_col as string);
    expect(actualDBJSONValue).toEqual(expectedJSON);
    expect(actualReplicatedJSONValue).toEqual(expectedJSON);

    const actualDBSetValue = JSON.parse(databaseRows[0].set_col as string);
    const actualReplicatedSetValue = JSON.parse(replicatedRows[0].set_col as string);
    expect(actualDBSetValue).toEqual(expectedSet);
    expect(actualReplicatedSetValue).toEqual(expectedSet);
  });
});

async function getDatabaseRows(connection: MySQLConnectionManager, tableName: string): Promise<SqliteInputRow[]> {
  const [results, fields] = await connection.query(`SELECT * FROM ${tableName}`);
  const columns = toColumnDescriptors(fields);
  return results.map((row) => common.toSQLiteRow(row, columns));
}

/**
 * Return all the inserts from the first transaction in the binlog stream.
 */
async function getReplicatedRows(expectedTransactionsCount?: number): Promise<SqliteInputRow[]> {
  let transformed: SqliteInputRow[] = [];
  const zongji = new ZongJi({
    host: TEST_CONNECTION_OPTIONS.hostname,
    user: TEST_CONNECTION_OPTIONS.username,
    password: TEST_CONNECTION_OPTIONS.password
  });

  const completionPromise = new Promise<SqliteInputRow[]>((resolve, reject) => {
    zongji.on('binlog', (evt: BinLogEvent) => {
      try {
        if (eventIsWriteMutation(evt)) {
          const tableMapEntry = evt.tableMap[evt.tableId];
          const columns = toColumnDescriptors(tableMapEntry);
          const records = evt.rows.map((row: Record<string, any>) => common.toSQLiteRow(row, columns));
          transformed.push(...records);
        } else if (eventIsXid(evt)) {
          if (expectedTransactionsCount !== undefined) {
            expectedTransactionsCount--;
            if (expectedTransactionsCount == 0) {
              zongji.stop();
              resolve(transformed);
            }
          } else {
            zongji.stop();
            resolve(transformed);
          }
        }
      } catch (e) {
        reject(e);
      }
    });
  });

  zongji.start({
    includeEvents: ['tablemap', 'writerows', 'xid'],
    filename: 'mysql-bin.000001',
    position: 0
  });

  return completionPromise;
}
