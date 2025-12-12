import { describe, expect, test } from 'vitest';
import { CompatibilityContext, ExpressionType, SqlDataQuery } from '../../src/index.js';
import { ASSETS, BASIC_SCHEMA, PARSE_OPTIONS } from './util.js';

describe('data queries', () => {
  test('bucket parameters = query', function () {
    const sql = 'SELECT * FROM assets WHERE assets.org_id = bucket.org_id';
    const query = SqlDataQuery.fromSql(['org_id'], sql, PARSE_OPTIONS, compatibility);
    expect(query.errors).toEqual([]);

    expect(query.evaluateRow(ASSETS, { id: 'asset1', org_id: 'org1' })).toEqual([
      {
        serializedBucketParameters: '["org1"]',
        table: 'assets',
        id: 'asset1',
        data: { id: 'asset1', org_id: 'org1' }
      }
    ]);

    expect(query.evaluateRow(ASSETS, { id: 'asset1', org_id: null })).toEqual([]);
  });

  test('bucket parameters IN query', function () {
    const sql = 'SELECT * FROM assets WHERE bucket.category IN assets.categories';
    const query = SqlDataQuery.fromSql(['category'], sql, PARSE_OPTIONS, compatibility);
    expect(query.errors).toEqual([]);

    expect(query.evaluateRow(ASSETS, { id: 'asset1', categories: JSON.stringify(['red', 'green']) })).toMatchObject([
      {
        serializedBucketParameters: '["red"]',
        table: 'assets',
        id: 'asset1'
      },
      {
        serializedBucketParameters: '["green"]',
        table: 'assets',
        id: 'asset1'
      }
    ]);

    expect(query.evaluateRow(ASSETS, { id: 'asset1', org_id: null })).toEqual([]);
  });

  test('static IN data query', function () {
    const sql = `SELECT * FROM assets WHERE 'green' IN assets.categories`;
    const query = SqlDataQuery.fromSql([], sql, PARSE_OPTIONS, compatibility);
    expect(query.errors).toEqual([]);

    expect(query.evaluateRow(ASSETS, { id: 'asset1', categories: JSON.stringify(['red', 'green']) })).toMatchObject([
      {
        serializedBucketParameters: '[]',
        table: 'assets',
        id: 'asset1'
      }
    ]);

    expect(query.evaluateRow(ASSETS, { id: 'asset1', categories: JSON.stringify(['red', 'blue']) })).toEqual([]);
  });

  test('data IN static query', function () {
    const sql = `SELECT * FROM assets WHERE assets.condition IN '["good","great"]'`;
    const query = SqlDataQuery.fromSql([], sql, PARSE_OPTIONS, compatibility);
    expect(query.errors).toEqual([]);

    expect(query.evaluateRow(ASSETS, { id: 'asset1', condition: 'good' })).toMatchObject([
      {
        serializedBucketParameters: '[]',
        table: 'assets',
        id: 'asset1'
      }
    ]);

    expect(query.evaluateRow(ASSETS, { id: 'asset1', condition: 'bad' })).toEqual([]);
  });

  test('table alias', function () {
    const sql = 'SELECT * FROM assets as others WHERE others.org_id = bucket.org_id';
    const query = SqlDataQuery.fromSql(['org_id'], sql, PARSE_OPTIONS, compatibility);
    expect(query.errors).toEqual([]);

    expect(query.evaluateRow(ASSETS, { id: 'asset1', org_id: 'org1' })).toEqual([
      {
        serializedBucketParameters: '["org1"]',
        table: 'others',
        id: 'asset1',
        data: { id: 'asset1', org_id: 'org1' }
      }
    ]);
  });

  test('types', () => {
    const schema = BASIC_SCHEMA;

    const q1 = SqlDataQuery.fromSql(
      ['user_id'],
      `SELECT * FROM assets WHERE owner_id = bucket.user_id`,
      PARSE_OPTIONS,
      compatibility
    );
    expect(q1.getColumnOutputs(schema)).toEqual([
      {
        name: 'assets',
        columns: [
          { name: 'id', type: ExpressionType.TEXT },
          { name: 'name', type: ExpressionType.TEXT },
          { name: 'count', type: ExpressionType.INTEGER },
          { name: 'owner_id', type: ExpressionType.TEXT }
        ]
      }
    ]);

    const q2 = SqlDataQuery.fromSql(
      ['user_id'],
      `
  SELECT id :: integer as id,
   upper(name) as name_upper,
   hex('test') as hex,
   count + 2 as count2,
   count * 3.0 as count3,
   count * '4' as count4,
   name ->> '$.attr' as json_value,
   ifnull(name, 2.0) as maybe_name
  FROM assets WHERE owner_id = bucket.user_id`,
      PARSE_OPTIONS,
      compatibility
    );
    expect(q2.getColumnOutputs(schema)).toEqual([
      {
        name: 'assets',
        columns: [
          { name: 'id', type: ExpressionType.INTEGER },
          { name: 'name_upper', type: ExpressionType.TEXT },
          { name: 'hex', type: ExpressionType.TEXT },
          { name: 'count2', type: ExpressionType.INTEGER },
          { name: 'count3', type: ExpressionType.REAL },
          { name: 'count4', type: ExpressionType.NUMERIC },
          { name: 'json_value', type: ExpressionType.ANY_JSON },
          { name: 'maybe_name', type: ExpressionType.TEXT.or(ExpressionType.REAL) }
        ]
      }
    ]);
  });

  test('validate columns', () => {
    const schema = BASIC_SCHEMA;
    const q1 = SqlDataQuery.fromSql(
      ['user_id'],
      'SELECT id, name, count FROM assets WHERE owner_id = bucket.user_id',
      { ...PARSE_OPTIONS, schema },
      compatibility
    );
    expect(q1.errors).toEqual([]);

    const q2 = SqlDataQuery.fromSql(
      ['user_id'],
      'SELECT id, upper(description) as d FROM assets WHERE other_id = bucket.user_id',
      { ...PARSE_OPTIONS, schema },
      compatibility
    );
    expect(q2.errors).toMatchObject([
      {
        message: `Column not found: other_id`,
        type: 'warning'
      },
      {
        message: `Column not found: description`,
        type: 'warning'
      }
    ]);

    const q3 = SqlDataQuery.fromSql(
      ['user_id'],
      'SELECT id, description, * FROM nope WHERE other_id = bucket.user_id',
      { ...PARSE_OPTIONS, schema },
      compatibility
    );
    expect(q3.errors).toMatchObject([
      {
        message: `Table test_schema.nope not found`,
        type: 'warning'
      }
    ]);

    const q4 = SqlDataQuery.fromSql([], 'SELECT * FROM other', { ...PARSE_OPTIONS, schema }, compatibility);
    expect(q4.errors).toMatchObject([
      {
        message: `Query must return an "id" column`,
        type: 'warning'
      }
    ]);

    const q5 = SqlDataQuery.fromSql(
      [],
      'SELECT other_id as id, * FROM other',
      { ...PARSE_OPTIONS, schema },
      compatibility
    );
    expect(q5.errors).toMatchObject([]);
  });

  test('invalid query - invalid IN', function () {
    const sql = 'SELECT * FROM assets WHERE assets.category IN bucket.categories';
    const query = SqlDataQuery.fromSql(['categories'], sql, PARSE_OPTIONS, compatibility);
    expect(query.errors).toMatchObject([
      { type: 'fatal', message: 'Cannot use bucket parameters on the right side of IN operators' }
    ]);
  });

  test('invalid query - not all parameters used', function () {
    const sql = 'SELECT * FROM assets WHERE 1';
    const query = SqlDataQuery.fromSql(['org_id'], sql, PARSE_OPTIONS, compatibility);
    expect(query.errors).toMatchObject([
      { type: 'fatal', message: 'Query must cover all bucket parameters. Expected: ["bucket.org_id"] Got: []' }
    ]);
  });

  test('invalid query - parameter not defined', function () {
    const sql = 'SELECT * FROM assets WHERE assets.org_id = bucket.org_id';
    const query = SqlDataQuery.fromSql([], sql, PARSE_OPTIONS, compatibility);
    expect(query.errors).toMatchObject([
      { type: 'fatal', message: 'Query must cover all bucket parameters. Expected: [] Got: ["bucket.org_id"]' }
    ]);
  });

  test('invalid query - function on parameter (1)', function () {
    const sql = 'SELECT * FROM assets WHERE assets.org_id = upper(bucket.org_id)';
    const query = SqlDataQuery.fromSql(['org_id'], sql, PARSE_OPTIONS, compatibility);
    expect(query.errors).toMatchObject([{ type: 'fatal', message: 'Cannot use bucket parameters in expressions' }]);
  });

  test('invalid query - function on parameter (2)', function () {
    const sql = 'SELECT * FROM assets WHERE assets.org_id = upper(bucket.org_id)';
    const query = SqlDataQuery.fromSql([], sql, PARSE_OPTIONS, compatibility);
    expect(query.errors).toMatchObject([{ type: 'fatal', message: 'Cannot use bucket parameters in expressions' }]);
  });

  test('invalid query - match clause in select', () => {
    const sql = 'SELECT id, (bucket.org_id = assets.org_id) as org_matches FROM assets where org_id = bucket.org_id';
    const query = SqlDataQuery.fromSql(['org_id'], sql, PARSE_OPTIONS, compatibility);
    expect(query.errors[0].message).toMatch(/Parameter match expression is not allowed here/);
  });

  test('case-sensitive queries (1)', () => {
    const sql = 'SELECT * FROM Assets';
    const query = SqlDataQuery.fromSql([], sql, PARSE_OPTIONS, compatibility);
    expect(query.errors).toMatchObject([
      { message: `Unquoted identifiers are converted to lower-case. Use "Assets" instead.` }
    ]);
  });

  test('case-sensitive queries (2)', () => {
    const sql = 'SELECT *, Name FROM assets';
    const query = SqlDataQuery.fromSql([], sql, PARSE_OPTIONS, compatibility);
    expect(query.errors).toMatchObject([
      { message: `Unquoted identifiers are converted to lower-case. Use "Name" instead.` }
    ]);
  });

  test('case-sensitive queries (3)', () => {
    const sql = 'SELECT * FROM assets WHERE Archived = False';
    const query = SqlDataQuery.fromSql([], sql, PARSE_OPTIONS, compatibility);
    expect(query.errors).toMatchObject([
      { message: `Unquoted identifiers are converted to lower-case. Use "Archived" instead.` }
    ]);
  });

  test.skip('case-sensitive queries (4)', () => {
    // Cannot validate table alias yet
    const sql = 'SELECT * FROM assets as myAssets';
    const query = SqlDataQuery.fromSql([], sql, PARSE_OPTIONS, compatibility);
    expect(query.errors).toMatchObject([
      { message: `Unquoted identifiers are converted to lower-case. Use "myAssets" instead.` }
    ]);
  });

  test.skip('case-sensitive queries (5)', () => {
    // Cannot validate table alias yet
    const sql = 'SELECT * FROM assets myAssets';
    const query = SqlDataQuery.fromSql([], sql, PARSE_OPTIONS, compatibility);
    expect(query.errors).toMatchObject([
      { message: `Unquoted identifiers are converted to lower-case. Use "myAssets" instead.` }
    ]);
  });

  test.skip('case-sensitive queries (6)', () => {
    // Cannot validate anything with a schema yet
    const sql = 'SELECT * FROM public.ASSETS';
    const query = SqlDataQuery.fromSql([], sql, PARSE_OPTIONS, compatibility);
    expect(query.errors).toMatchObject([
      { message: `Unquoted identifiers are converted to lower-case. Use "ASSETS" instead.` }
    ]);
  });

  test.skip('case-sensitive queries (7)', () => {
    // Cannot validate schema yet
    const sql = 'SELECT * FROM PUBLIC.assets';
    const query = SqlDataQuery.fromSql([], sql, PARSE_OPTIONS, compatibility);
    expect(query.errors).toMatchObject([
      { message: `Unquoted identifiers are converted to lower-case. Use "PUBLIC" instead.` }
    ]);
  });
});

const compatibility = CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY;
