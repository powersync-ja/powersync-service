import {
  DEFAULT_SCHEMA,
  DEFAULT_TAG,
  DartSchemaGenerator,
  ExpressionType,
  JsSchemaGenerator,
  SourceTableInterface,
  SqlDataQuery,
  SqlParameterQuery,
  SqlSyncRules,
  StaticSchema,
  normalizeTokenParameters
} from '@powersync/service-sync-rules';
import { describe, expect, test } from 'vitest';
import { SourceTable } from '../../src/storage/SourceTable.js';

class TestSourceTable implements SourceTableInterface {
  readonly connectionTag = DEFAULT_TAG;
  readonly schema = DEFAULT_SCHEMA;

  constructor(public readonly table: string) {}
}

const ASSETS = new TestSourceTable('assets');
const USERS = new TestSourceTable('users');

describe('sync rules', () => {
  test('parse empty sync rules', () => {
    const rules = SqlSyncRules.fromYaml('bucket_definitions: {}');
    expect(rules.bucket_descriptors).toEqual([]);
  });

  test('parse global sync rules', () => {
    const rules = SqlSyncRules.fromYaml(`
bucket_definitions:
  mybucket:
    data:
      - SELECT id, description FROM assets
    `);
    const bucket = rules.bucket_descriptors[0];
    expect(bucket.name).toEqual('mybucket');
    expect(bucket.bucket_parameters).toEqual([]);
    const dataQuery = bucket.data_queries[0];
    expect(dataQuery.bucket_parameters).toEqual([]);
    expect(dataQuery.columnOutputNames()).toEqual(['id', 'description']);
    expect(rules.evaluateRow({ sourceTable: ASSETS, record: { id: 'asset1', description: 'test' } })).toEqual([
      {
        ruleId: '1',
        table: 'assets',
        id: 'asset1',
        data: {
          id: 'asset1',
          description: 'test'
        },
        bucket: 'mybucket[]'
      }
    ]);
    expect(rules.getStaticBucketIds({ token_parameters: {}, user_parameters: {} })).toEqual(['mybucket[]']);
  });

  test('parse global sync rules with filter', () => {
    const rules = SqlSyncRules.fromYaml(`
bucket_definitions:
  mybucket:
    parameters: SELECT WHERE token_parameters.is_admin
    data: []
    `);
    const bucket = rules.bucket_descriptors[0];
    expect(bucket.bucket_parameters).toEqual([]);
    const param_query = bucket.global_parameter_queries[0];

    expect(param_query.filter!.filter({ token_parameters: { is_admin: 1n } })).toEqual([{}]);
    expect(param_query.filter!.filter({ token_parameters: { is_admin: 0n } })).toEqual([]);
    expect(rules.getStaticBucketIds(normalizeTokenParameters({ is_admin: true }))).toEqual(['mybucket[]']);
    expect(rules.getStaticBucketIds(normalizeTokenParameters({ is_admin: false }))).toEqual([]);
    expect(rules.getStaticBucketIds(normalizeTokenParameters({}))).toEqual([]);
  });

  test('parse global sync rules with table filter', () => {
    const rules = SqlSyncRules.fromYaml(`
bucket_definitions:
  mybucket:
    parameters: SELECT FROM users WHERE users.id = token_parameters.user_id AND users.is_admin
    data: []
    `);
    const bucket = rules.bucket_descriptors[0];
    expect(bucket.bucket_parameters).toEqual([]);
    const param_query = bucket.parameter_queries[0];
    expect(param_query.bucket_parameters).toEqual([]);
    expect(rules.evaluateParameterRow(USERS, { id: 'user1', is_admin: 1 })).toEqual([
      {
        bucket_parameters: [{}],
        lookup: ['mybucket', '1', 'user1']
      }
    ]);
    expect(rules.evaluateParameterRow(USERS, { id: 'user1', is_admin: 0 })).toEqual([]);
  });

  test('parse bucket with parameters', () => {
    const rules = SqlSyncRules.fromYaml(`
bucket_definitions:
  mybucket:
    parameters: SELECT token_parameters.user_id
    data:
      - SELECT id, description FROM assets WHERE assets.user_id = bucket.user_id AND NOT assets.archived
    `);
    const bucket = rules.bucket_descriptors[0];
    expect(bucket.bucket_parameters).toEqual(['user_id']);
    const param_query = bucket.global_parameter_queries[0];
    expect(param_query.bucket_parameters).toEqual(['user_id']);
    expect(rules.getStaticBucketIds(normalizeTokenParameters({ user_id: 'user1' }))).toEqual(['mybucket["user1"]']);

    const data_query = bucket.data_queries[0];
    expect(data_query.bucket_parameters).toEqual(['user_id']);
    expect(
      rules.evaluateRow({
        sourceTable: ASSETS,
        record: { id: 'asset1', description: 'test', user_id: 'user1' }
      })
    ).toEqual([
      {
        ruleId: '1',
        bucket: 'mybucket["user1"]',
        id: 'asset1',
        data: {
          id: 'asset1',
          description: 'test'
        },
        table: 'assets'
      }
    ]);
    expect(
      rules.evaluateRow({
        sourceTable: ASSETS,
        record: { id: 'asset1', description: 'test', user_id: 'user1', archived: 1 }
      })
    ).toEqual([]);
  });

  test('parse bucket with parameters and OR condition', () => {
    const rules = SqlSyncRules.fromYaml(`
bucket_definitions:
  mybucket:
    parameters: SELECT token_parameters.user_id
    data:
      - SELECT id, description FROM assets WHERE assets.user_id = bucket.user_id OR assets.owner_id = bucket.user_id
    `);
    const bucket = rules.bucket_descriptors[0];
    expect(bucket.bucket_parameters).toEqual(['user_id']);
    const param_query = bucket.global_parameter_queries[0];
    expect(param_query.bucket_parameters).toEqual(['user_id']);
    expect(rules.getStaticBucketIds(normalizeTokenParameters({ user_id: 'user1' }))).toEqual(['mybucket["user1"]']);

    const data_query = bucket.data_queries[0];
    expect(data_query.bucket_parameters).toEqual(['user_id']);
    expect(
      rules.evaluateRow({
        sourceTable: ASSETS,
        record: { id: 'asset1', description: 'test', user_id: 'user1' }
      })
    ).toEqual([
      {
        ruleId: '1',
        bucket: 'mybucket["user1"]',
        id: 'asset1',
        data: {
          id: 'asset1',
          description: 'test'
        },
        table: 'assets'
      }
    ]);
    expect(
      rules.evaluateRow({
        sourceTable: ASSETS,
        record: { id: 'asset1', description: 'test', owner_id: 'user1' }
      })
    ).toEqual([
      {
        ruleId: '1',
        bucket: 'mybucket["user1"]',
        id: 'asset1',
        data: {
          id: 'asset1',
          description: 'test'
        },
        table: 'assets'
      }
    ]);
  });

  test('parse bucket with parameters and invalid OR condition', () => {
    expect(() => {
      const rules = SqlSyncRules.fromYaml(`
bucket_definitions:
  mybucket:
    parameters: SELECT token_parameters.user_id
    data:
      - SELECT id, description FROM assets WHERE assets.user_id = bucket.user_id AND (assets.user_id = bucket.foo OR assets.other_id = bucket.bar)
    `);
    }).toThrowError(/must use the same parameters/);
  });

  test('reject unsupported queries', () => {
    expect(
      SqlSyncRules.validate(`
bucket_definitions:
  mybucket:
    parameters: SELECT token_parameters.user_id LIMIT 1
    data: []
    `)
    ).toMatchObject([{ message: 'LIMIT is not supported' }]);

    expect(
      SqlSyncRules.validate(`
bucket_definitions:
  mybucket:
    data:
      - SELECT DISTINCT id, description FROM assets
    `)
    ).toMatchObject([{ message: 'DISTINCT is not supported' }]);

    expect(
      SqlSyncRules.validate(`
bucket_definitions:
  mybucket:
    parameters: SELECT token_parameters.user_id OFFSET 10
    data: []
    `)
    ).toMatchObject([{ message: 'LIMIT is not supported' }]);

    expect(() => {
      const rules = SqlSyncRules.fromYaml(`
bucket_definitions:
  mybucket:
    parameters: SELECT token_parameters.user_id FOR UPDATE SKIP LOCKED
    data: []
    `);
    }).toThrowError(/SKIP is not supported/);

    expect(() => {
      const rules = SqlSyncRules.fromYaml(`
bucket_definitions:
  mybucket:
    parameters: SELECT token_parameters.user_id FOR UPDATE
    data: []
    `);
    }).toThrowError(/FOR is not supported/);

    expect(() => {
      const rules = SqlSyncRules.fromYaml(`
bucket_definitions:
  mybucket:
    data:
      - SELECT id, description FROM assets ORDER BY id
    `);
    }).toThrowError(/ORDER BY is not supported/);
  });

  test('transforming things', () => {
    const rules = SqlSyncRules.fromYaml(`
bucket_definitions:
  mybucket:
    parameters: SELECT upper(token_parameters.user_id) AS user_id
    data:
      - SELECT id, upper(description) AS description_upper FROM assets WHERE upper(assets.user_id) = bucket.user_id AND NOT assets.archived
    `);
    const bucket = rules.bucket_descriptors[0];
    expect(bucket.bucket_parameters).toEqual(['user_id']);
    expect(rules.getStaticBucketIds(normalizeTokenParameters({ user_id: 'user1' }))).toEqual(['mybucket["USER1"]']);

    expect(
      rules.evaluateRow({
        sourceTable: ASSETS,
        record: { id: 'asset1', description: 'test', user_id: 'user1' }
      })
    ).toEqual([
      {
        ruleId: '1',
        bucket: 'mybucket["USER1"]',
        id: 'asset1',
        data: {
          id: 'asset1',
          description_upper: 'TEST'
        },
        table: 'assets'
      }
    ]);
  });

  test('transforming things with upper-case functions', () => {
    // Testing that we can use different case for the function names
    const rules = SqlSyncRules.fromYaml(`
bucket_definitions:
  mybucket:
    parameters: SELECT UPPER(token_parameters.user_id) AS user_id
    data:
      - SELECT id, UPPER(description) AS description_upper FROM assets WHERE UPPER(assets.user_id) = bucket.user_id AND NOT assets.archived
    `);
    const bucket = rules.bucket_descriptors[0];
    expect(bucket.bucket_parameters).toEqual(['user_id']);
    expect(rules.getStaticBucketIds(normalizeTokenParameters({ user_id: 'user1' }))).toEqual(['mybucket["USER1"]']);

    expect(
      rules.evaluateRow({
        sourceTable: ASSETS,
        record: { id: 'asset1', description: 'test', user_id: 'user1' }
      })
    ).toEqual([
      {
        ruleId: '1',
        bucket: 'mybucket["USER1"]',
        id: 'asset1',
        data: {
          id: 'asset1',
          description_upper: 'TEST'
        },
        table: 'assets'
      }
    ]);
  });

  test('transforming json', () => {
    const rules = SqlSyncRules.fromYaml(`
bucket_definitions:
  mybucket:
    data:
      - SELECT id, data ->> 'count' AS count, data -> 'bool' AS bool1, data ->> 'bool' AS bool2, 'true' ->> '$' as bool3, json_extract(data, '$.bool') AS bool4 FROM assets
    `);
    expect(
      rules.evaluateRow({
        sourceTable: ASSETS,
        record: { id: 'asset1', data: JSON.stringify({ count: 5, bool: true }) }
      })
    ).toEqual([
      {
        ruleId: '1',
        bucket: 'mybucket[]',
        id: 'asset1',
        data: {
          id: 'asset1',
          count: 5n,
          bool1: 'true',
          bool2: 1n,
          bool3: 1n,
          bool4: 1n
        },
        table: 'assets'
      }
    ]);
  });

  test('IN json', () => {
    const rules = SqlSyncRules.fromYaml(`
bucket_definitions:
  mybucket:
    parameters: SELECT token_parameters.region_id
    data:
      - SELECT id, description FROM assets WHERE bucket.region_id IN assets.region_ids
    `);

    expect(
      rules.evaluateRow({
        sourceTable: ASSETS,
        record: {
          id: 'asset1',
          description: 'test',
          region_ids: JSON.stringify(['region1', 'region2'])
        }
      })
    ).toEqual([
      {
        ruleId: '1',
        bucket: 'mybucket["region1"]',
        id: 'asset1',
        data: {
          id: 'asset1',
          description: 'test'
        },
        table: 'assets'
      },
      {
        ruleId: '1',
        bucket: 'mybucket["region2"]',
        id: 'asset1',
        data: {
          id: 'asset1',
          description: 'test'
        },
        table: 'assets'
      }
    ]);
  });

  test('direct boolean param', () => {
    const rules = SqlSyncRules.fromYaml(`
bucket_definitions:
  mybucket:
    parameters: SELECT token_parameters.is_admin
    data:
      - SELECT id, description, role, 'admin' as rule FROM assets WHERE bucket.is_admin
      - SELECT id, description, role, 'normal' as rule FROM assets WHERE (bucket.is_admin OR bucket.is_admin = false) AND assets.role != 'admin'
    `);

    expect(
      rules.evaluateRow({ sourceTable: ASSETS, record: { id: 'asset1', description: 'test', role: 'admin' } })
    ).toEqual([
      {
        ruleId: '1',
        bucket: 'mybucket[1]',
        id: 'asset1',
        data: {
          id: 'asset1',
          description: 'test',
          role: 'admin',
          rule: 'admin'
        },
        table: 'assets'
      }
    ]);

    // TODO: Deduplicate somewhere
    expect(
      rules.evaluateRow({ sourceTable: ASSETS, record: { id: 'asset2', description: 'test', role: 'normal' } })
    ).toEqual([
      {
        ruleId: '1',
        bucket: 'mybucket[1]',
        id: 'asset2',
        data: {
          id: 'asset2',
          description: 'test',
          role: 'normal',
          rule: 'admin'
        },
        table: 'assets'
      },
      {
        ruleId: '2',
        bucket: 'mybucket[1]',
        id: 'asset2',
        data: {
          id: 'asset2',
          description: 'test',
          role: 'normal',
          rule: 'normal'
        },
        table: 'assets'
      },
      {
        ruleId: '2',
        bucket: 'mybucket[0]',
        id: 'asset2',
        data: {
          id: 'asset2',
          description: 'test',
          role: 'normal',
          rule: 'normal'
        },
        table: 'assets'
      }
    ]);

    expect(rules.getStaticBucketIds(normalizeTokenParameters({ is_admin: true }))).toEqual(['mybucket[1]']);
  });

  test('token_parameters IN query', function () {
    const sql = 'SELECT id as group_id FROM groups WHERE token_parameters.user_id IN groups.user_ids';
    const query = SqlParameterQuery.fromSql('mybucket', sql) as SqlParameterQuery;
    query.id = '1';
    expect(query.evaluateParameterRow({ id: 'group1', user_ids: JSON.stringify(['user1', 'user2']) })).toEqual([
      {
        lookup: ['mybucket', '1', 'user1'],
        bucket_parameters: [
          {
            group_id: 'group1'
          }
        ]
      },
      {
        lookup: ['mybucket', '1', 'user2'],
        bucket_parameters: [
          {
            group_id: 'group1'
          }
        ]
      }
    ]);
    expect(
      query.getLookups(
        normalizeTokenParameters({
          user_id: 'user1'
        })
      )
    ).toEqual([['mybucket', '1', 'user1']]);
  });

  test('IN token_parameters query', function () {
    const sql = 'SELECT id as region_id FROM regions WHERE name IN token_parameters.region_names';
    const query = SqlParameterQuery.fromSql('mybucket', sql) as SqlParameterQuery;
    query.id = '1';
    expect(query.evaluateParameterRow({ id: 'region1', name: 'colorado' })).toEqual([
      {
        lookup: ['mybucket', '1', 'colorado'],
        bucket_parameters: [
          {
            region_id: 'region1'
          }
        ]
      }
    ]);
    expect(
      query.getLookups(
        normalizeTokenParameters({
          region_names: JSON.stringify(['colorado', 'texas'])
        })
      )
    ).toEqual([
      ['mybucket', '1', 'colorado'],
      ['mybucket', '1', 'texas']
    ]);
  });

  test('some math', () => {
    const rules = SqlSyncRules.fromYaml(`
bucket_definitions:
  mybucket:
    data:
      - SELECT id, (5 / 2) AS int, (5 / 2.0) AS float, (CAST(5 AS real) / 2) AS float2 FROM assets
    `);

    expect(rules.evaluateRow({ sourceTable: ASSETS, record: { id: 'asset1' } })).toEqual([
      {
        ruleId: '1',
        bucket: 'mybucket[]',
        id: 'asset1',
        data: {
          id: 'asset1',
          int: 2n,
          float: 2.5,
          float2: 2.5
        },
        table: 'assets'
      }
    ]);
  });

  test('bucket with static numeric parameters', () => {
    const rules = SqlSyncRules.fromYaml(`
bucket_definitions:
  mybucket:
    parameters: SELECT token_parameters.int1, token_parameters.float1, token_parameters.float2
    data:
      - SELECT id FROM assets WHERE assets.int1 = bucket.int1 AND assets.float1 = bucket.float1 AND assets.float2 = bucket.float2
    `);
    expect(rules.getStaticBucketIds(normalizeTokenParameters({ int1: 314, float1: 3.14, float2: 314 }))).toEqual([
      'mybucket[314,3.14,314]'
    ]);

    expect(
      rules.evaluateRow({ sourceTable: ASSETS, record: { id: 'asset1', int1: 314n, float1: 3.14, float2: 314 } })
    ).toEqual([
      {
        ruleId: '1',
        bucket: 'mybucket[314,3.14,314]',
        id: 'asset1',
        data: {
          id: 'asset1'
        },
        table: 'assets'
      }
    ]);
  });

  test('bucket with queried numeric parameters', () => {
    const sql =
      'SELECT users.int1, users.float1, users.float2 FROM users WHERE users.int1 = token_parameters.int1 AND users.float1 = token_parameters.float1 AND users.float2 = token_parameters.float2';
    const query = SqlParameterQuery.fromSql('mybucket', sql) as SqlParameterQuery;
    query.id = '1';
    // Note: We don't need to worry about numeric vs decimal types in the lookup - JSONB handles normalization for us.
    expect(query.evaluateParameterRow({ int1: 314n, float1: 3.14, float2: 314 })).toEqual([
      {
        lookup: ['mybucket', '1', 314n, 3.14, 314],

        bucket_parameters: [{ int1: 314n, float1: 3.14, float2: 314 }]
      }
    ]);

    // Similarly, we don't need to worry about the types here.
    // This test just checks the current behavior.
    expect(query.getLookups(normalizeTokenParameters({ int1: 314n, float1: 3.14, float2: 314 }))).toEqual([
      ['mybucket', '1', 314n, 3.14, 314n]
    ]);

    // We _do_ need to care about the bucket string representation.
    expect(query.resolveBucketIds([{ int1: 314, float1: 3.14, float2: 314 }], normalizeTokenParameters({}))).toEqual([
      'mybucket[314,3.14,314]'
    ]);

    expect(query.resolveBucketIds([{ int1: 314n, float1: 3.14, float2: 314 }], normalizeTokenParameters({}))).toEqual([
      'mybucket[314,3.14,314]'
    ]);
  });

  test('parameter query with token filter (1)', () => {
    // Also supported: token_parameters.is_admin = true
    // Not supported: token_parameters.is_admin != false
    // Support could be added later.
    const sql = 'SELECT FROM users WHERE users.id = token_parameters.user_id AND token_parameters.is_admin';
    const query = SqlParameterQuery.fromSql('mybucket', sql) as SqlParameterQuery;
    query.id = '1';

    expect(query.evaluateParameterRow({ id: 'user1' })).toEqual([
      {
        lookup: ['mybucket', '1', 'user1', 1n],
        bucket_parameters: [{}]
      }
    ]);

    expect(query.getLookups(normalizeTokenParameters({ user_id: 'user1', is_admin: true }))).toEqual([
      ['mybucket', '1', 'user1', 1n]
    ]);
    // Would not match any actual lookups
    expect(query.getLookups(normalizeTokenParameters({ user_id: 'user1', is_admin: false }))).toEqual([
      ['mybucket', '1', 'user1', 0n]
    ]);
  });

  test('parameter query with token filter (2)', () => {
    const sql =
      'SELECT users.id AS user_id, token_parameters.is_admin as is_admin FROM users WHERE users.id = token_parameters.user_id AND token_parameters.is_admin';
    const query = SqlParameterQuery.fromSql('mybucket', sql) as SqlParameterQuery;
    query.id = '1';

    expect(query.evaluateParameterRow({ id: 'user1' })).toEqual([
      {
        lookup: ['mybucket', '1', 'user1', 1n],

        bucket_parameters: [{ user_id: 'user1' }]
      }
    ]);

    expect(query.getLookups(normalizeTokenParameters({ user_id: 'user1', is_admin: true }))).toEqual([
      ['mybucket', '1', 'user1', 1n]
    ]);

    expect(
      query.resolveBucketIds([{ user_id: 'user1' }], normalizeTokenParameters({ user_id: 'user1', is_admin: true }))
    ).toEqual(['mybucket["user1",1]']);
  });

  test('custom table and id', () => {
    const rules = SqlSyncRules.fromYaml(`
bucket_definitions:
  mybucket:
    data:
      - SELECT client_id AS id, description FROM assets_123 as assets WHERE assets.archived = false
      - SELECT other_id AS id, description FROM assets_123 as assets
    `);

    expect(
      rules.evaluateRow({
        sourceTable: new TestSourceTable('assets_123'),
        record: { client_id: 'asset1', description: 'test', archived: 0n, other_id: 'other1' }
      })
    ).toEqual([
      {
        ruleId: '1',
        bucket: 'mybucket[]',
        id: 'asset1',
        data: {
          id: 'asset1',
          description: 'test'
        },
        table: 'assets'
      },
      {
        ruleId: '2',
        bucket: 'mybucket[]',
        id: 'other1',
        data: {
          id: 'other1',
          description: 'test'
        },
        table: 'assets'
      }
    ]);
  });

  test('wildcard table', () => {
    const rules = SqlSyncRules.fromYaml(`
bucket_definitions:
  mybucket:
    data:
      - SELECT client_id AS id, description, _table_suffix as suffix, * FROM "assets_%" as assets WHERE assets.archived = false AND _table_suffix > '100'
    `);

    expect(
      rules.evaluateRow({
        sourceTable: new TestSourceTable('assets_123'),
        record: { client_id: 'asset1', description: 'test', archived: 0n, other_id: 'other1' }
      })
    ).toEqual([
      {
        ruleId: '1',
        bucket: 'mybucket[]',
        id: 'asset1',
        data: {
          id: 'asset1',
          description: 'test',
          suffix: '123',
          archived: 0n,
          client_id: 'asset1',
          other_id: 'other1'
        },
        table: 'assets'
      }
    ]);
  });

  test('wildcard without alias', () => {
    const rules = SqlSyncRules.fromYaml(`
bucket_definitions:
  mybucket:
    data:
      - SELECT *, _table_suffix as suffix, * FROM "%" WHERE archived = false
    `);

    expect(
      rules.evaluateRow({
        sourceTable: ASSETS,
        record: { id: 'asset1', description: 'test', archived: 0n }
      })
    ).toEqual([
      {
        ruleId: '1',
        bucket: 'mybucket[]',
        id: 'asset1',
        data: {
          id: 'asset1',
          description: 'test',
          suffix: 'assets',
          archived: 0n
        },
        table: 'assets'
      }
    ]);
  });

  test('should filter schemas', () => {
    const rules = SqlSyncRules.fromYaml(`
bucket_definitions:
  mybucket:
    data:
      - SELECT id FROM "assets" # Yes
      - SELECT id FROM "public"."assets" # yes
      - SELECT id FROM "default.public"."assets" # yes
      - SELECT id FROM "other"."assets" # no
      - SELECT id FROM "other.public"."assets" # no
    `);

    expect(
      rules.evaluateRow({
        sourceTable: ASSETS,
        record: { id: 'asset1' }
      })
    ).toEqual([
      {
        ruleId: '1',
        bucket: 'mybucket[]',
        id: 'asset1',
        data: {
          id: 'asset1'
        },
        table: 'assets'
      },
      {
        ruleId: '2',
        bucket: 'mybucket[]',
        id: 'asset1',
        data: {
          id: 'asset1'
        },
        table: 'assets'
      },
      {
        ruleId: '3',
        bucket: 'mybucket[]',
        id: 'asset1',
        data: {
          id: 'asset1'
        },
        table: 'assets'
      }
    ]);
  });

  test('case-sensitive parameter queries (1)', () => {
    const sql = 'SELECT users."userId" AS user_id FROM users WHERE users."userId" = token_parameters.user_id';
    const query = SqlParameterQuery.fromSql('mybucket', sql) as SqlParameterQuery;
    query.id = '1';

    expect(query.evaluateParameterRow({ userId: 'user1' })).toEqual([
      {
        lookup: ['mybucket', '1', 'user1'],

        bucket_parameters: [{ user_id: 'user1' }]
      }
    ]);
  });

  test('case-sensitive parameter queries (2)', () => {
    // Note: This documents current behavior.
    // This may change in the future - we should check against expected behavior for
    // Postgres and/or SQLite.
    const sql = 'SELECT users.userId AS user_id FROM users WHERE users.userId = token_parameters.user_id';
    const query = SqlParameterQuery.fromSql('mybucket', sql) as SqlParameterQuery;
    query.id = '1';

    expect(query.evaluateParameterRow({ userId: 'user1' })).toEqual([]);
    expect(query.evaluateParameterRow({ userid: 'user1' })).toEqual([
      {
        lookup: ['mybucket', '1', 'user1'],

        bucket_parameters: [{ user_id: 'user1' }]
      }
    ]);
  });

  test('dynamic global parameter query', () => {
    const sql = "SELECT workspaces.id AS workspace_id FROM workspaces WHERE visibility = 'public'";
    const query = SqlParameterQuery.fromSql('mybucket', sql) as SqlParameterQuery;
    query.id = '1';

    expect(query.evaluateParameterRow({ id: 'workspace1', visibility: 'public' })).toEqual([
      {
        lookup: ['mybucket', '1'],

        bucket_parameters: [{ workspace_id: 'workspace1' }]
      }
    ]);

    expect(query.evaluateParameterRow({ id: 'workspace1', visibility: 'private' })).toEqual([]);
  });

  test('invalid OR in parameter queries', () => {
    // Supporting this case is more tricky. We can do this by effectively denormalizing the OR clause
    // into separate queries, but it's a significant change. For now, developers should do that manually.
    const sql =
      "SELECT workspaces.id AS workspace_id FROM workspaces WHERE workspaces.user_id = token_parameters.user_id OR visibility = 'public'";
    const query = SqlParameterQuery.fromSql('mybucket', sql) as SqlParameterQuery;
    expect(query.errors[0].message).toMatch(/must use the same parameters/);
  });

  test('types', () => {
    const schema = new StaticSchema([
      {
        tag: SourceTable.DEFAULT_TAG,
        schemas: [
          {
            name: SourceTable.DEFAULT_SCHEMA,
            tables: [
              {
                name: 'assets',
                columns: [
                  { name: 'id', pg_type: 'uuid' },
                  { name: 'name', pg_type: 'text' },
                  { name: 'count', pg_type: 'int4' },
                  { name: 'owner_id', pg_type: 'uuid' }
                ]
              }
            ]
          }
        ]
      }
    ]);

    const q1 = SqlDataQuery.fromSql('q1', ['user_id'], `SELECT * FROM assets WHERE owner_id = bucket.user_id`);
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
      'q1',
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
  FROM assets WHERE owner_id = bucket.user_id`
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
    const schema = new StaticSchema([
      {
        tag: SourceTable.DEFAULT_TAG,
        schemas: [
          {
            name: SourceTable.DEFAULT_SCHEMA,
            tables: [
              {
                name: 'assets',
                columns: [
                  { name: 'id', pg_type: 'uuid' },
                  { name: 'name', pg_type: 'text' },
                  { name: 'count', pg_type: 'int4' },
                  { name: 'owner_id', pg_type: 'uuid' }
                ]
              }
            ]
          }
        ]
      }
    ]);
    const q1 = SqlDataQuery.fromSql(
      'q1',
      ['user_id'],
      'SELECT id, name, count FROM assets WHERE owner_id = bucket.user_id',
      schema
    );
    expect(q1.errors).toEqual([]);

    const q2 = SqlDataQuery.fromSql(
      'q2',
      ['user_id'],
      'SELECT id, upper(description) as d FROM assets WHERE other_id = bucket.user_id',
      schema
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
      'q3',
      ['user_id'],
      'SELECT id, description, * FROM nope WHERE other_id = bucket.user_id',
      schema
    );
    expect(q3.errors).toMatchObject([
      {
        message: `Table public.nope not found`,
        type: 'warning'
      }
    ]);
  });

  test('schema generation', () => {
    const schema = new StaticSchema([
      {
        tag: SourceTable.DEFAULT_TAG,
        schemas: [
          {
            name: SourceTable.DEFAULT_SCHEMA,
            tables: [
              {
                name: 'assets',
                columns: [
                  { name: 'id', pg_type: 'uuid' },
                  { name: 'name', pg_type: 'text' },
                  { name: 'count', pg_type: 'int4' },
                  { name: 'owner_id', pg_type: 'uuid' }
                ]
              }
            ]
          }
        ]
      }
    ]);

    const rules = SqlSyncRules.fromYaml(`
bucket_definitions:
  mybucket:
    data:
      - SELECT * FROM assets as assets1
      - SELECT id, name, count FROM assets as assets2
      - SELECT id, owner_id as other_id, foo FROM assets as ASSETS2
    `);

    expect(new DartSchemaGenerator().generate(rules, schema)).toEqual(`Schema([
  Table('assets1', [
    Column.text('name'),
    Column.integer('count'),
    Column.text('owner_id')
  ]),
  Table('assets2', [
    Column.text('name'),
    Column.integer('count'),
    Column.text('other_id'),
    Column.text('foo')
  ])
]);
`);

    expect(new JsSchemaGenerator().generate(rules, schema)).toEqual(`new Schema([
  new Table({
    name: 'assets1',
    columns: [
      new Column({ name: 'name', type: ColumnType.TEXT }),
      new Column({ name: 'count', type: ColumnType.INTEGER }),
      new Column({ name: 'owner_id', type: ColumnType.TEXT })
    ]
  }),
  new Table({
    name: 'assets2',
    columns: [
      new Column({ name: 'name', type: ColumnType.TEXT }),
      new Column({ name: 'count', type: ColumnType.INTEGER }),
      new Column({ name: 'other_id', type: ColumnType.TEXT }),
      new Column({ name: 'foo', type: ColumnType.TEXT })
    ]
  })
])
`);
  });
});
