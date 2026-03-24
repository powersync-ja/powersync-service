import {
  applyRowContext,
  CompatibilityContext,
  SQLITE_TRUE,
  SqliteInputRow,
  TimeValuePrecision
} from '@powersync/service-sync-rules';
import { afterAll, beforeEach, describe, expect, test } from 'vitest';
import {
  clearTestDb,
  createUpperCaseUUID,
  enableCDCForTable,
  TEST_CONNECTION_OPTIONS,
  waitForPendingCDCChanges
} from './util.js';
import { CDCToSqliteRow, toSqliteInputRow } from '@module/common/mssqls-to-sqlite.js';
import { MSSQLConnectionManager } from '@module/replication/MSSQLConnectionManager.js';
import {
  escapeIdentifier,
  getCaptureInstances,
  getLatestLSN,
  getLatestReplicatedLSN,
  toQualifiedTableName
} from '@module/utils/mssql.js';
import sql from 'mssql';
import { CDC_SCHEMA } from '@module/common/MSSQLSourceTable.js';

describe('MSSQL Data Types Tests', () => {
  const connectionManager = new MSSQLConnectionManager(TEST_CONNECTION_OPTIONS, {});

  beforeEach(async () => {
    await clearTestDb(connectionManager);
    await setupTestTable();
  });
  afterAll(async () => {
    await connectionManager.end();
  });

  async function setupTestTable() {
    await connectionManager.query(`
      CREATE TABLE ${escapeIdentifier(connectionManager.schema)}.test_data (
        id INT IDENTITY(1,1) PRIMARY KEY,
        tinyint_col TINYINT,
        smallint_col SMALLINT,
        int_col INT,
        bigint_col BIGINT,
        float_col FLOAT,
        real_col REAL,
        decimal_col DECIMAL(10,2),
        numeric_col NUMERIC(10,2),
        money_col MONEY,
        smallmoney_col SMALLMONEY,
        bit_col BIT,
        
        date_col DATE,
        datetime_col DATETIME,
        datetime2_col DATETIME2(7),
        smalldatetime_col SMALLDATETIME,
        datetimeoffset_col DATETIMEOFFSET(3),
        time_col TIME(7),
        
        char_col CHAR(10),
        varchar_col VARCHAR(255),
        varchar_max_col VARCHAR(MAX),
        nchar_col NCHAR(15),
        nvarchar_col NVARCHAR(255),
        nvarchar_max_col NVARCHAR(MAX),
        text_col TEXT,
        ntext_col NTEXT,
        
        binary_col BINARY(16),
        varbinary_col VARBINARY(256),
        varbinary_max_col VARBINARY(MAX),
        image_col IMAGE,
        
        uniqueidentifier_col UNIQUEIDENTIFIER,
        xml_col XML,
        json_col NVARCHAR(MAX),
        
        hierarchyid_col HIERARCHYID,
        geometry_col GEOMETRY,
        geography_col GEOGRAPHY
      )
    `);

    await enableCDCForTable({ connectionManager, table: 'test_data' });
  }

  test('Number types mappings', async () => {
    const beforeLSN = await getLatestLSN(connectionManager);
    await connectionManager.query(`
      INSERT INTO ${escapeIdentifier(connectionManager.schema)}.test_data(
        tinyint_col,
        smallint_col,
        int_col,
        bigint_col,
        float_col,
        real_col,
        decimal_col,
        numeric_col,
        money_col,
        smallmoney_col,
        bit_col
      ) VALUES (
        255,                 -- TINYINT maximum value
        32767,               -- SMALLINT maximum value
        2147483647,          -- INT maximum value
        9223372036854775807, -- BIGINT maximum value
        3.1415926535,        -- FLOAT example
        3.14,                -- REAL example
        12345.67,            -- DECIMAL(10,2) example
        12345.67,            -- NUMERIC(10,2) example
        12345.67,            -- MONEY example
        123.45,              -- SMALLMONEY example
        1                    -- BIT value
      )
    `);
    await waitForPendingCDCChanges(beforeLSN, connectionManager);

    const databaseRows = await getDatabaseRows(connectionManager, 'test_data');
    const replicatedRows = await getReplicatedRows(connectionManager, 'test_data');

    const expectedResult: SqliteInputRow = {
      tinyint_col: 255,
      smallint_col: 32767,
      int_col: 2147483647,
      bigint_col: 9223372036854775807n,
      float_col: 3.1415926535,
      real_col: expect.closeTo(3.14, 2),
      decimal_col: 12345.67,
      numeric_col: 12345.67,
      money_col: 12345.67,
      smallmoney_col: 123.45,
      bit_col: SQLITE_TRUE
    };
    expect(databaseRows[0]).toMatchObject(expectedResult);
    expect(replicatedRows[0]).toMatchObject(expectedResult);
  });

  test('Character types mappings', async () => {
    const beforeLSN = await getLatestLSN(connectionManager);
    await connectionManager.query(`
      INSERT INTO ${escapeIdentifier(connectionManager.schema)}.test_data (
        char_col,
        varchar_col,
        varchar_max_col,
        nchar_col,
        nvarchar_col,
        nvarchar_max_col,
        text_col,
        ntext_col
      ) VALUES (
        'CharData',               -- CHAR(10) with padding spaces
        'Variable character data',-- VARCHAR(255)
        'Variable character data MAX', -- VARCHAR(MAX)
        N'UnicodeChar',           -- NCHAR(15)
        N'Variable Unicode data', -- NVARCHAR(255)
        N'Variable Unicode data MAX', -- NVARCHAR(MAX)
        'TextData',               -- TEXT
        N'UnicodeTextData'        -- NTEXT
      )
    `);
    await waitForPendingCDCChanges(beforeLSN, connectionManager);

    const databaseRows = await getDatabaseRows(connectionManager, 'test_data');
    const replicatedRows = await getReplicatedRows(connectionManager, 'test_data');
    const expectedResult = {
      char_col: 'CharData  ', // CHAR pads with spaces up to the defined length (10)
      varchar_col: 'Variable character data',
      varchar_max_col: 'Variable character data MAX',
      nchar_col: 'UnicodeChar    ', // NCHAR pads with spaces up to the defined length (15)
      nvarchar_col: 'Variable Unicode data',
      nvarchar_max_col: 'Variable Unicode data MAX',
      text_col: 'TextData',
      ntext_col: 'UnicodeTextData'
    };

    expect(databaseRows[0]).toMatchObject(expectedResult);
    expect(replicatedRows[0]).toMatchObject(expectedResult);
  });

  test('Binary types mappings', async () => {
    const beforeLSN = await getLatestLSN(connectionManager);
    const binaryData = Buffer.from('BinaryData');
    await connectionManager.query(
      `
      INSERT INTO ${escapeIdentifier(connectionManager.schema)}.test_data (
        binary_col,
        varbinary_col,
        varbinary_max_col,
        image_col
      ) VALUES (
        @binary_col,
        @varbinary_col,
        @varbinary_max_col,
        @image_col
      )
    `,
      [
        { name: 'binary_col', type: sql.Binary, value: binaryData },
        { name: 'varbinary_col', type: sql.VarBinary, value: binaryData },
        { name: 'varbinary_max_col', type: sql.VarBinary(sql.MAX), value: binaryData },
        { name: 'image_col', type: sql.Image, value: binaryData }
      ]
    );
    await waitForPendingCDCChanges(beforeLSN, connectionManager);

    const databaseRows = await getDatabaseRows(connectionManager, 'test_data');
    const replicatedRows = await getReplicatedRows(connectionManager, 'test_data');

    const expectedBinary = new Uint8Array(binaryData);
    const expectedBinaryPadded = new Uint8Array(16);
    expectedBinaryPadded.set(expectedBinary.slice(0, 16), 0);

    const expectedResult: SqliteInputRow = {
      binary_col: expectedBinaryPadded,
      varbinary_col: expectedBinary,
      varbinary_max_col: expectedBinary,
      image_col: expectedBinary
    };

    expect(databaseRows[0]).toMatchObject(expectedResult);
    expect(replicatedRows[0]).toMatchObject(expectedResult);
  });

  test('Date types mappings', async () => {
    const beforeLSN = await getLatestLSN(connectionManager);
    const testDate = new Date('2023-03-06T15:47:00.123Z');
    // This adds 0.4567 milliseconds to the JS date, see https://github.com/tediousjs/tedious/blob/0c256f186600d7230aec05553ebad209bed81acc/src/data-types/datetime2.ts#L74.
    // Note that there's a typo in tedious there. When reading dates, the property is actually called nanosecondsDelta.
    // This is only relevant when binding datetime values, so only in this test.
    Object.defineProperty(testDate, 'nanosecondDelta', {
      enumerable: false,
      value: 0.0004567
    });
    await connectionManager.query(
      `
      INSERT INTO ${escapeIdentifier(connectionManager.schema)}.test_data(
        date_col, 
        datetime_col, 
        datetime2_col, 
        smalldatetime_col,
        time_col
      )
      VALUES (
        @date_col,
        @datetime_col,
        @datetime2_col,
        @smalldatetime_col,
        @time_col
      )
    `,
      [
        { name: 'date_col', type: sql.Date, value: testDate },
        { name: 'datetime_col', type: sql.DateTime, value: testDate },
        { name: 'datetime2_col', type: sql.DateTime2(7), value: testDate },
        { name: 'smalldatetime_col', type: sql.SmallDateTime, value: testDate },
        { name: 'time_col', type: sql.Time(7), value: testDate }
      ]
    );
    await waitForPendingCDCChanges(beforeLSN, connectionManager);

    const databaseRows = await getDatabaseRows(connectionManager, 'test_data');
    const replicatedRows = await getReplicatedRows(connectionManager, 'test_data');
    const expectedResult = {
      date_col: '2023-03-06',
      datetime_col: '2023-03-06T15:47:00.123000000Z',
      datetime2_col: '2023-03-06T15:47:00.123456700Z',
      smalldatetime_col: '2023-03-06T15:47:00.000000000Z',
      time_col: '15:47:00.123456700'
    };

    expect(applyRowContext(databaseRows[0], CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY)).toMatchObject(
      expectedResult
    );
    expect(applyRowContext(replicatedRows[0], CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY)).toMatchObject(
      expectedResult
    );

    const restrictedPrecisionResult = {
      date_col: '2023-03-06',
      datetime_col: '2023-03-06T15:47:00.123000Z',
      datetime2_col: '2023-03-06T15:47:00.123456Z',
      smalldatetime_col: '2023-03-06T15:47:00.000000Z',
      time_col: '15:47:00.123456'
    };
    const restrictedPrecision = new CompatibilityContext({
      edition: 2,
      maxTimeValuePrecision: TimeValuePrecision.microseconds
    });
    expect(applyRowContext(databaseRows[0], restrictedPrecision)).toMatchObject(restrictedPrecisionResult);
    expect(applyRowContext(replicatedRows[0], restrictedPrecision)).toMatchObject(restrictedPrecisionResult);
  });

  test('Date types edge cases mappings', async () => {
    await connectionManager.query(`
      INSERT INTO ${escapeIdentifier(connectionManager.schema)}.test_data(datetime2_col) 
      VALUES ('0001-01-01 00:00:00.000')
    `);
    await connectionManager.query(`
      INSERT INTO ${escapeIdentifier(connectionManager.schema)}.test_data(datetime2_col) 
      VALUES ('9999-12-31 23:59:59.999')
    `);
    await connectionManager.query(`
      INSERT INTO ${escapeIdentifier(connectionManager.schema)}.test_data(datetime_col) 
      VALUES ('1753-01-01 00:00:00')
    `);
    const beforeLSN = await getLatestLSN(connectionManager);
    await connectionManager.query(`
      INSERT INTO ${escapeIdentifier(connectionManager.schema)}.test_data(datetime_col) 
      VALUES ('9999-12-31 23:59:59.997')
    `);
    await waitForPendingCDCChanges(beforeLSN, connectionManager);

    const expectedResults = [
      { datetime2_col: '0001-01-01T00:00:00.000000000Z' },
      { datetime2_col: '9999-12-31T23:59:59.999000000Z' },
      { datetime_col: '1753-01-01T00:00:00.000000000Z' },
      { datetime_col: '9999-12-31T23:59:59.997000000Z' }
    ];

    const databaseRows = await getDatabaseRows(connectionManager, 'test_data');
    const replicatedRows = await getReplicatedRows(connectionManager, 'test_data');

    for (let i = 0; i < expectedResults.length; i++) {
      expect(applyRowContext(databaseRows[i], CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY)).toMatchObject(
        expectedResults[i]
      );
      expect(applyRowContext(replicatedRows[i], CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY)).toMatchObject(
        expectedResults[i]
      );
    }
  });

  test('DateTimeOffset type mapping', async () => {
    const beforeLSN = await getLatestLSN(connectionManager);
    // DateTimeOffset preserves timezone information
    await connectionManager.query(`
      INSERT INTO ${escapeIdentifier(connectionManager.schema)}.test_data(datetimeoffset_col) 
      VALUES ('2023-03-06 15:47:00.000 +05:00')
    `);
    await waitForPendingCDCChanges(beforeLSN, connectionManager);

    const expectedResult = {
      datetimeoffset_col: '2023-03-06T10:47:00.000000000Z' // Converted to UTC
    };

    const databaseRows = await getDatabaseRows(connectionManager, 'test_data');
    const replicatedRows = await getReplicatedRows(connectionManager, 'test_data');

    // Note: The driver converts DateTimeOffset to Date, which incorporates the timezone offset which is then represented in UTC.
    expect(applyRowContext(databaseRows[0], CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY)).toMatchObject(
      expectedResult
    );
    expect(applyRowContext(replicatedRows[0], CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY)).toMatchObject(
      expectedResult
    );
  });

  test('UniqueIdentifier type mapping', async () => {
    const beforeLSN = await getLatestLSN(connectionManager);

    const testGuid = createUpperCaseUUID();
    await connectionManager.query(
      `
      INSERT INTO ${escapeIdentifier(connectionManager.schema)}.test_data(uniqueidentifier_col) 
      VALUES (@guid)
    `,
      [{ name: 'guid', type: sql.UniqueIdentifier, value: testGuid }]
    );
    await waitForPendingCDCChanges(beforeLSN, connectionManager);

    const databaseRows = await getDatabaseRows(connectionManager, 'test_data');
    const replicatedRows = await getReplicatedRows(connectionManager, 'test_data');

    // GUIDs are returned as strings
    expect(databaseRows[0].uniqueidentifier_col).toBe(testGuid);
    expect(replicatedRows[0].uniqueidentifier_col).toBe(testGuid);
  });

  test('JSON type mapping', async () => {
    const beforeLSN = await getLatestLSN(connectionManager);
    const expectedJSON = { name: 'John Doe', age: 30, married: true };
    await connectionManager.query(
      `
      INSERT INTO ${escapeIdentifier(connectionManager.schema)}.test_data(json_col) 
      VALUES (@json)
    `,
      [{ name: 'json', type: sql.NVarChar(sql.MAX), value: JSON.stringify(expectedJSON) }]
    );
    await waitForPendingCDCChanges(beforeLSN, connectionManager);

    const databaseRows = await getDatabaseRows(connectionManager, 'test_data');
    const replicatedRows = await getReplicatedRows(connectionManager, 'test_data');

    const actualDBJSONValue = JSON.parse(databaseRows[0].json_col as string);
    const actualReplicatedJSONValue = JSON.parse(replicatedRows[0].json_col as string);
    expect(actualDBJSONValue).toEqual(expectedJSON);
    expect(actualReplicatedJSONValue).toEqual(expectedJSON);
  });

  test('XML type mapping', async () => {
    const beforeLSN = await getLatestLSN(connectionManager);
    const xmlData = '<root><item>value</item></root>';
    await connectionManager.query(
      `
      INSERT INTO ${escapeIdentifier(connectionManager.schema)}.test_data(xml_col) 
      VALUES (@xml)
    `,
      [{ name: 'xml', type: sql.Xml, value: xmlData }]
    );
    await waitForPendingCDCChanges(beforeLSN, connectionManager);

    const databaseRows = await getDatabaseRows(connectionManager, 'test_data');
    const replicatedRows = await getReplicatedRows(connectionManager, 'test_data');

    expect(databaseRows[0].xml_col).toBe(xmlData);
    expect(replicatedRows[0].xml_col).toBe(xmlData);
  });

  // TODO: Update test when properly converting spatial types
  // test('Spatial types mappings', async () => {
  //   const beforeLSN = await getLatestLSN(connectionManager);
  //   // Geometry and Geography types are stored as binary/WKT strings
  //   await connectionManager.query(`
  //     INSERT INTO [${connectionManager.schema}].test_data(geometry_col, geography_col)
  //     VALUES (
  //       geometry::STGeomFromText('POINT(1 2)', 0),
  //       geography::STGeomFromText('POINT(1 2)', 4326)
  //     )
  //   `);
  //   await waitForPendingCDCChanges(beforeLSN, connectionManager);
  //
  //   const databaseRows = await getDatabaseRows(connectionManager, 'test_data');
  //   const replicatedRows = await getReplicatedRows(connectionManager, 'test_data');
  //
  //   // The driver currently returns spatial types as non standard objects. We just convert them to JSON strings for now
  //   expect(databaseRows[0].geometry_col).toBeDefined();
  //   expect(databaseRows[0].geography_col).toBeDefined();
  //   expect(replicatedRows[0].geometry_col).toBeDefined();
  //   expect(replicatedRows[0].geography_col).toBeDefined();
  // });

  // TODO: Enable when HierarchyID type is properly supported
  // test('HierarchyID type mapping', async () => {
  //   const hierarchyid = '/1/';
  //   const beforeLSN = await getLatestLSN(connectionManager);
  //   await connectionManager.query(`
  //     INSERT INTO [${connectionManager.schema}].test_data(hierarchyid_col)
  //     VALUES (@hierarchyid)
  //   `,
  //     [{ name: 'hierarchyid', type: sql.VarChar, value: hierarchyid }]
  //   );
  //   await waitForPendingCDCChanges(beforeLSN, connectionManager);
  //
  //   const databaseRows = await getDatabaseRows(connectionManager, 'test_data');
  //   const replicatedRows = await getReplicatedRows(connectionManager, 'test_data');
  //
  //   const expectedBinary = new Uint8Array(Buffer.from(hierarchyid));
  //
  //   expect(databaseRows[0].hierarchyid_col).toEqual(expectedBinary);
  //   expect(replicatedRows[0].hierarchyid_col).toEqual(expectedBinary);
  // });
});

async function getDatabaseRows(
  connectionManager: MSSQLConnectionManager,
  tableName: string
): Promise<SqliteInputRow[]> {
  const { recordset: rows } = await connectionManager.query(
    `SELECT * FROM ${toQualifiedTableName(connectionManager.schema, tableName)}`
  );
  return rows.map((row) => {
    const converted = toSqliteInputRow(row, rows.columns);
    // Exclude id column from results
    const { id, ...rest } = converted;
    return rest;
  });
}

/**
 * Return all the updates from the CDC stream for the table.
 */
async function getReplicatedRows(
  connectionManager: MSSQLConnectionManager,
  tableName: string
): Promise<SqliteInputRow[]> {
  const endLSN = await getLatestReplicatedLSN(connectionManager);

  const captureInstances = await getCaptureInstances({
    connectionManager,
    table: {
      schema: connectionManager.schema,
      name: tableName
    }
  });
  if (captureInstances.size === 0) {
    throw new Error(`No CDC capture instance found for table ${tableName}`);
  }

  const captureInstance = Array.from(captureInstances.values())[0].instances[0];
  const startLSN = captureInstance.minLSN;
  // Query CDC changes
  const { recordset: results } = await connectionManager.query(
    `
      SELECT * FROM ${CDC_SCHEMA}.fn_cdc_get_all_changes_${captureInstance.name}(@from_lsn, @to_lsn, 'all update old') ORDER BY __$start_lsn, __$seqval
  `,
    [
      { name: 'from_lsn', type: sql.VarBinary, value: startLSN.toBinary() },
      { name: 'to_lsn', type: sql.VarBinary, value: endLSN.toBinary() }
    ]
  );

  return results
    .filter((row) => row.__$operation === 2) // Only INSERT operations
    .map((row) => {
      const converted = CDCToSqliteRow({ row, columns: results.columns });
      // Exclude id column from results
      const { id, ...rest } = converted;
      return rest;
    });
}
