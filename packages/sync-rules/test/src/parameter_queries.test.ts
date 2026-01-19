import { beforeEach, describe, expect, test } from 'vitest';
import {
  UnscopedParameterLookup,
  SqlParameterQuery,
  SourceTableInterface,
  debugHydratedMergedSource,
  BucketParameterQuerier,
  QuerierError,
  GetQuerierOptions,
  RequestParameters,
  ScopedParameterLookup,
  mergeParameterIndexLookupCreators,
  SqliteJsonRow
} from '../../src/index.js';
import { StaticSqlParameterQuery } from '../../src/StaticSqlParameterQuery.js';
import {
  BASIC_SCHEMA,
  EMPTY_DATA_SOURCE,
  findQuerierLookups,
  normalizeTokenParameters,
  PARSE_OPTIONS
} from './util.js';
import { HydrationState } from '../../src/HydrationState.js';

describe('parameter queries', () => {
  const table = (name: string): SourceTableInterface => ({
    connectionTag: 'default',
    name,
    schema: PARSE_OPTIONS.defaultSchema
  });

  const TABLE_USERS = table('users');
  const TABLE_REGIONS = table('regions');
  const TABLE_WORKSPACES = table('workspaces');
  const TABLE_POSTS = table('posts');
  const TABLE_GROUPS = table('groups');

  test('token_parameters IN query', function () {
    const sql = 'SELECT id as group_id FROM groups WHERE token_parameters.user_id IN groups.user_ids';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(
      query.evaluateParameterRow(TABLE_GROUPS, { id: 'group1', user_ids: JSON.stringify(['user1', 'user2']) })
    ).toEqual([
      {
        lookup: UnscopedParameterLookup.normalized(['user1']),
        bucketParameters: [
          {
            group_id: 'group1'
          }
        ]
      },
      {
        lookup: UnscopedParameterLookup.normalized(['user2']),
        bucketParameters: [
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
    ).toEqual([UnscopedParameterLookup.normalized(['user1'])]);
  });

  test('IN token_parameters query', function () {
    const sql = 'SELECT id as region_id FROM regions WHERE name IN token_parameters.region_names';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.evaluateParameterRow(TABLE_REGIONS, { id: 'region1', name: 'colorado' })).toEqual([
      {
        lookup: UnscopedParameterLookup.normalized(['colorado']),
        bucketParameters: [
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
    ).toEqual([UnscopedParameterLookup.normalized(['colorado']), UnscopedParameterLookup.normalized(['texas'])]);
  });

  test('queried numeric parameters', () => {
    const sql =
      'SELECT users.int1, users.float1, users.float2 FROM users WHERE users.int1 = token_parameters.int1 AND users.float1 = token_parameters.float1 AND users.float2 = token_parameters.float2';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);
    // Note: We don't need to worry about numeric vs decimal types in the lookup - JSONB handles normalization for us.
    expect(query.evaluateParameterRow(TABLE_USERS, { int1: 314n, float1: 3.14, float2: 314 })).toEqual([
      {
        lookup: UnscopedParameterLookup.normalized([314n, 3.14, 314]),

        bucketParameters: [{ int1: 314n, float1: 3.14, float2: 314 }]
      }
    ]);

    // Similarly, we don't need to worry about the types here.
    // This test just checks the current behavior.
    expect(query.getLookups(normalizeTokenParameters({ int1: 314n, float1: 3.14, float2: 314 }))).toEqual([
      UnscopedParameterLookup.normalized([314n, 3.14, 314n])
    ]);

    // We _do_ need to care about the bucket string representation.
    expect(
      query.resolveBucketDescriptions([{ int1: 314, float1: 3.14, float2: 314 }], normalizeTokenParameters({}), {
        bucketPrefix: 'mybucket'
      })
    ).toEqual([{ bucket: 'mybucket[314,3.14,314]', priority: 3 }]);

    expect(
      query.resolveBucketDescriptions([{ int1: 314n, float1: 3.14, float2: 314 }], normalizeTokenParameters({}), {
        bucketPrefix: 'mybucket'
      })
    ).toEqual([{ bucket: 'mybucket[314,3.14,314]', priority: 3 }]);
  });

  test('plain token_parameter (baseline)', () => {
    const sql = 'SELECT id from users WHERE filter_param = token_parameters.user_id';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);

    expect(query.evaluateParameterRow(TABLE_USERS, { id: 'test_id', filter_param: 'test_param' })).toEqual([
      {
        lookup: UnscopedParameterLookup.normalized(['test_param']),

        bucketParameters: [{ id: 'test_id' }]
      }
    ]);

    expect(query.getLookups(normalizeTokenParameters({ user_id: 'test' }))).toEqual([
      UnscopedParameterLookup.normalized(['test'])
    ]);
  });

  test('function on token_parameter', () => {
    const sql = 'SELECT id from users WHERE filter_param = upper(token_parameters.user_id)';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);

    expect(query.evaluateParameterRow(TABLE_USERS, { id: 'test_id', filter_param: 'test_param' })).toEqual([
      {
        lookup: UnscopedParameterLookup.normalized(['test_param']),

        bucketParameters: [{ id: 'test_id' }]
      }
    ]);

    expect(query.getLookups(normalizeTokenParameters({ user_id: 'test' }))).toEqual([
      UnscopedParameterLookup.normalized(['TEST'])
    ]);
  });

  test('token parameter member operator', () => {
    const sql = "SELECT id from users WHERE filter_param = token_parameters.some_param ->> 'description'";
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);

    expect(query.evaluateParameterRow(TABLE_USERS, { id: 'test_id', filter_param: 'test_param' })).toEqual([
      {
        lookup: UnscopedParameterLookup.normalized(['test_param']),

        bucketParameters: [{ id: 'test_id' }]
      }
    ]);

    expect(query.getLookups(normalizeTokenParameters({ some_param: { description: 'test_description' } }))).toEqual([
      UnscopedParameterLookup.normalized(['test_description'])
    ]);
  });

  test('token parameter and binary operator', () => {
    const sql = 'SELECT id from users WHERE filter_param = token_parameters.some_param + 2';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);

    expect(query.getLookups(normalizeTokenParameters({ some_param: 3 }))).toEqual([
      UnscopedParameterLookup.normalized([5n])
    ]);
  });

  test('token parameter IS NULL as filter', () => {
    const sql = 'SELECT id from users WHERE filter_param = (token_parameters.some_param IS NULL)';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);

    expect(query.getLookups(normalizeTokenParameters({ some_param: null }))).toEqual([
      UnscopedParameterLookup.normalized([1n])
    ]);
    expect(query.getLookups(normalizeTokenParameters({ some_param: 'test' }))).toEqual([
      UnscopedParameterLookup.normalized([0n])
    ]);
  });

  test('direct token parameter', () => {
    const sql = 'SELECT FROM users WHERE token_parameters.some_param';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);

    expect(query.evaluateParameterRow(TABLE_USERS, { id: 'user1' })).toEqual([
      {
        lookup: UnscopedParameterLookup.normalized([1n]),
        bucketParameters: [{}]
      }
    ]);

    expect(query.getLookups(normalizeTokenParameters({ user_id: 'user1', some_param: null }))).toEqual([
      UnscopedParameterLookup.normalized([0n])
    ]);
    expect(query.getLookups(normalizeTokenParameters({ user_id: 'user1', some_param: 123 }))).toEqual([
      UnscopedParameterLookup.normalized([1n])
    ]);
  });

  test('token parameter IS NULL', () => {
    const sql = 'SELECT FROM users WHERE token_parameters.some_param IS NULL';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);

    expect(query.evaluateParameterRow(TABLE_USERS, { id: 'user1' })).toEqual([
      {
        lookup: UnscopedParameterLookup.normalized([1n]),
        bucketParameters: [{}]
      }
    ]);

    expect(query.getLookups(normalizeTokenParameters({ user_id: 'user1', some_param: null }))).toEqual([
      UnscopedParameterLookup.normalized([1n])
    ]);
    expect(query.getLookups(normalizeTokenParameters({ user_id: 'user1', some_param: 123 }))).toEqual([
      UnscopedParameterLookup.normalized([0n])
    ]);
  });

  test('token parameter IS NOT NULL', () => {
    const sql = 'SELECT FROM users WHERE token_parameters.some_param IS NOT NULL';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);

    expect(query.evaluateParameterRow(TABLE_USERS, { id: 'user1' })).toEqual([
      {
        lookup: UnscopedParameterLookup.normalized([1n]),
        bucketParameters: [{}]
      }
    ]);

    expect(query.getLookups(normalizeTokenParameters({ user_id: 'user1', some_param: null }))).toEqual([
      UnscopedParameterLookup.normalized([0n])
    ]);
    expect(query.getLookups(normalizeTokenParameters({ user_id: 'user1', some_param: 123 }))).toEqual([
      UnscopedParameterLookup.normalized([1n])
    ]);
  });

  test('token parameter NOT', () => {
    const sql = 'SELECT FROM users WHERE NOT token_parameters.is_admin';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);

    expect(query.evaluateParameterRow(TABLE_USERS, { id: 'user1' })).toEqual([
      {
        lookup: UnscopedParameterLookup.normalized([1n]),
        bucketParameters: [{}]
      }
    ]);

    expect(query.getLookups(normalizeTokenParameters({ user_id: 'user1', is_admin: false }))).toEqual([
      UnscopedParameterLookup.normalized([1n])
    ]);
    expect(query.getLookups(normalizeTokenParameters({ user_id: 'user1', is_admin: 123 }))).toEqual([
      UnscopedParameterLookup.normalized([0n])
    ]);
  });

  test('row filter and token parameter IS NULL', () => {
    const sql = 'SELECT FROM users WHERE users.id = token_parameters.user_id AND token_parameters.some_param IS NULL';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);

    expect(query.evaluateParameterRow(TABLE_USERS, { id: 'user1' })).toEqual([
      {
        lookup: UnscopedParameterLookup.normalized(['user1', 1n]),
        bucketParameters: [{}]
      }
    ]);

    expect(query.getLookups(normalizeTokenParameters({ user_id: 'user1', some_param: null }))).toEqual([
      UnscopedParameterLookup.normalized(['user1', 1n])
    ]);
    expect(query.getLookups(normalizeTokenParameters({ user_id: 'user1', some_param: 123 }))).toEqual([
      UnscopedParameterLookup.normalized(['user1', 0n])
    ]);
  });

  test('row filter and direct token parameter', () => {
    const sql = 'SELECT FROM users WHERE users.id = token_parameters.user_id AND token_parameters.some_param';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);

    expect(query.evaluateParameterRow(TABLE_USERS, { id: 'user1' })).toEqual([
      {
        lookup: UnscopedParameterLookup.normalized(['user1', 1n]),
        bucketParameters: [{}]
      }
    ]);

    expect(query.getLookups(normalizeTokenParameters({ user_id: 'user1', some_param: 123 }))).toEqual([
      UnscopedParameterLookup.normalized(['user1', 1n])
    ]);
    expect(query.getLookups(normalizeTokenParameters({ user_id: 'user1', some_param: null }))).toEqual([
      UnscopedParameterLookup.normalized(['user1', 0n])
    ]);
  });

  test('cast', () => {
    const sql = 'SELECT FROM users WHERE users.id = cast(token_parameters.user_id as text)';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);

    expect(query.getLookups(normalizeTokenParameters({ user_id: 'user1' }))).toEqual([
      UnscopedParameterLookup.normalized(['user1'])
    ]);
    expect(query.getLookups(normalizeTokenParameters({ user_id: 123 }))).toEqual([
      UnscopedParameterLookup.normalized(['123'])
    ]);
  });

  test('IS NULL row filter', () => {
    const sql = 'SELECT id FROM users WHERE role IS NULL';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);

    expect(query.evaluateParameterRow(TABLE_USERS, { id: 'user1', role: null })).toEqual([
      {
        lookup: UnscopedParameterLookup.normalized([]),
        bucketParameters: [{ id: 'user1' }]
      }
    ]);

    expect(query.getLookups(normalizeTokenParameters({ user_id: 'user1' }))).toEqual([
      UnscopedParameterLookup.normalized([])
    ]);
  });

  test('token filter (1)', () => {
    // Also supported: token_parameters.is_admin = true
    // Not supported: token_parameters.is_admin != false
    // Support could be added later.
    const sql = 'SELECT FROM users WHERE users.id = token_parameters.user_id AND token_parameters.is_admin';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);

    expect(query.evaluateParameterRow(TABLE_USERS, { id: 'user1' })).toEqual([
      {
        lookup: UnscopedParameterLookup.normalized(['user1', 1n]),
        bucketParameters: [{}]
      }
    ]);

    expect(query.getLookups(normalizeTokenParameters({ user_id: 'user1', is_admin: true }))).toEqual([
      UnscopedParameterLookup.normalized(['user1', 1n])
    ]);
    // Would not match any actual lookups
    expect(query.getLookups(normalizeTokenParameters({ user_id: 'user1', is_admin: false }))).toEqual([
      UnscopedParameterLookup.normalized(['user1', 0n])
    ]);
  });

  test('token filter (2)', () => {
    const sql =
      'SELECT users.id AS user_id, token_parameters.is_admin as is_admin FROM users WHERE users.id = token_parameters.user_id AND token_parameters.is_admin';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);

    expect(query.evaluateParameterRow(TABLE_USERS, { id: 'user1' })).toEqual([
      {
        lookup: UnscopedParameterLookup.normalized(['user1', 1n]),

        bucketParameters: [{ user_id: 'user1' }]
      }
    ]);

    expect(query.getLookups(normalizeTokenParameters({ user_id: 'user1', is_admin: true }))).toEqual([
      UnscopedParameterLookup.normalized(['user1', 1n])
    ]);

    expect(
      query.resolveBucketDescriptions(
        [{ user_id: 'user1' }],
        normalizeTokenParameters({ user_id: 'user1', is_admin: true }),
        { bucketPrefix: 'mybucket' }
      )
    ).toEqual([{ bucket: 'mybucket["user1",1]', priority: 3 }]);
  });

  test('case-sensitive parameter queries (1)', () => {
    const sql = 'SELECT users."userId" AS user_id FROM users WHERE users."userId" = token_parameters.user_id';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);

    expect(query.evaluateParameterRow(TABLE_USERS, { userId: 'user1' })).toEqual([
      {
        lookup: UnscopedParameterLookup.normalized(['user1']),

        bucketParameters: [{ user_id: 'user1' }]
      }
    ]);
  });

  test('case-sensitive parameter queries (2)', () => {
    // Note: This documents current behavior.
    // This may change in the future - we should check against expected behavior for
    // Postgres and/or SQLite.
    const sql = 'SELECT users.userId AS user_id FROM users WHERE users.userId = token_parameters.user_id';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toMatchObject([
      { message: `Unquoted identifiers are converted to lower-case. Use "userId" instead.` },
      { message: `Unquoted identifiers are converted to lower-case. Use "userId" instead.` }
    ]);

    expect(query.evaluateParameterRow(TABLE_USERS, { userId: 'user1' })).toEqual([]);
    expect(query.evaluateParameterRow(TABLE_USERS, { userid: 'user1' })).toEqual([
      {
        lookup: UnscopedParameterLookup.normalized(['user1']),

        bucketParameters: [{ user_id: 'user1' }]
      }
    ]);
  });

  test('case-sensitive parameter queries (3)', () => {
    const sql = 'SELECT user_id FROM users WHERE Users.user_id = token_parameters.user_id';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toMatchObject([
      { message: `Unquoted identifiers are converted to lower-case. Use "Users" instead.` }
    ]);
  });

  test('case-sensitive parameter queries (4)', () => {
    const sql = 'SELECT Users.user_id FROM users WHERE user_id = token_parameters.user_id';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toMatchObject([
      { message: `Unquoted identifiers are converted to lower-case. Use "Users" instead.` }
    ]);
  });

  test('case-sensitive parameter queries (5)', () => {
    const sql = 'SELECT user_id FROM Users WHERE user_id = token_parameters.user_id';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toMatchObject([
      { message: `Unquoted identifiers are converted to lower-case. Use "Users" instead.` }
    ]);
  });

  test('case-sensitive parameter queries (6)', () => {
    const sql = 'SELECT userId FROM users';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toMatchObject([
      { message: `Unquoted identifiers are converted to lower-case. Use "userId" instead.` }
    ]);
  });

  test('case-sensitive parameter queries (7)', () => {
    const sql = 'SELECT user_id as userId FROM users';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toMatchObject([
      { message: `Unquoted identifiers are converted to lower-case. Use "userId" instead.` }
    ]);
  });

  test('dynamic global parameter query', () => {
    const sql = "SELECT workspaces.id AS workspace_id FROM workspaces WHERE visibility = 'public'";
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);

    expect(query.evaluateParameterRow(TABLE_WORKSPACES, { id: 'workspace1', visibility: 'public' })).toEqual([
      {
        lookup: UnscopedParameterLookup.normalized([]),

        bucketParameters: [{ workspace_id: 'workspace1' }]
      }
    ]);

    expect(query.evaluateParameterRow(TABLE_WORKSPACES, { id: 'workspace1', visibility: 'private' })).toEqual([]);
  });

  test('multiple different functions on token_parameter with AND', () => {
    // This is treated as two separate lookup index values
    const sql =
      'SELECT id from users WHERE filter_param = upper(token_parameters.user_id) AND filter_param = lower(token_parameters.user_id)';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);

    expect(query.evaluateParameterRow(TABLE_USERS, { id: 'test_id', filter_param: 'test_param' })).toEqual([
      {
        lookup: UnscopedParameterLookup.normalized(['test_param', 'test_param']),

        bucketParameters: [{ id: 'test_id' }]
      }
    ]);

    expect(query.getLookups(normalizeTokenParameters({ user_id: 'test' }))).toEqual([
      UnscopedParameterLookup.normalized(['TEST', 'test'])
    ]);
  });

  test('multiple same functions on token_parameter with OR', () => {
    // This is treated as the same index lookup value, can use OR with the two clauses
    const sql =
      'SELECT id from users WHERE filter_param1 = upper(token_parameters.user_id) OR filter_param2 = upper(token_parameters.user_id)';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);

    expect(
      query.evaluateParameterRow(TABLE_USERS, { id: 'test_id', filter_param1: 'test1', filter_param2: 'test2' })
    ).toEqual([
      {
        lookup: UnscopedParameterLookup.normalized(['test1']),
        bucketParameters: [{ id: 'test_id' }]
      },
      {
        lookup: UnscopedParameterLookup.normalized(['test2']),
        bucketParameters: [{ id: 'test_id' }]
      }
    ]);

    expect(query.getLookups(normalizeTokenParameters({ user_id: 'test' }))).toEqual([
      UnscopedParameterLookup.normalized(['TEST'])
    ]);
  });

  test('request.parameters()', function () {
    const sql = "SELECT FROM posts WHERE category = request.parameters() ->> 'category_id'";
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      {
        accept_potentially_dangerous_queries: true,
        ...PARSE_OPTIONS
      },
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.evaluateParameterRow(TABLE_POSTS, { id: 'group1', category: 'red' })).toEqual([
      {
        lookup: UnscopedParameterLookup.normalized(['red']),
        bucketParameters: [{}]
      }
    ]);
    expect(query.getLookups(normalizeTokenParameters({}, { category_id: 'red' }))).toEqual([
      UnscopedParameterLookup.normalized(['red'])
    ]);
  });

  test('nested request.parameters() (1)', function () {
    const sql = "SELECT FROM posts WHERE category = request.parameters() -> 'details' ->> 'category'";
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      {
        accept_potentially_dangerous_queries: true,
        ...PARSE_OPTIONS
      },
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.getLookups(normalizeTokenParameters({}, { details: { category: 'red' } }))).toEqual([
      UnscopedParameterLookup.normalized(['red'])
    ]);
  });

  test('nested request.parameters() (2)', function () {
    const sql = "SELECT FROM posts WHERE category = request.parameters() ->> 'details.category'";
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      {
        accept_potentially_dangerous_queries: true,
        ...PARSE_OPTIONS
      },
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.getLookups(normalizeTokenParameters({}, { details: { category: 'red' } }))).toEqual([
      UnscopedParameterLookup.normalized(['red'])
    ]);
  });

  test('IN request.parameters()', function () {
    // Can use -> or ->> here
    const sql = "SELECT id as region_id FROM regions WHERE name IN request.parameters() -> 'region_names'";
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      {
        accept_potentially_dangerous_queries: true,
        ...PARSE_OPTIONS
      },
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.evaluateParameterRow(TABLE_REGIONS, { id: 'region1', name: 'colorado' })).toEqual([
      {
        lookup: UnscopedParameterLookup.normalized(['colorado']),
        bucketParameters: [
          {
            region_id: 'region1'
          }
        ]
      }
    ]);
    expect(
      query.getLookups(
        normalizeTokenParameters(
          {},
          {
            region_names: ['colorado', 'texas']
          }
        )
      )
    ).toEqual([UnscopedParameterLookup.normalized(['colorado']), UnscopedParameterLookup.normalized(['texas'])]);
  });

  test('user_parameters in SELECT', function () {
    const sql = 'SELECT id, user_parameters.other_id as other_id FROM users WHERE id = token_parameters.user_id';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.evaluateParameterRow(TABLE_USERS, { id: 'user1' })).toEqual([
      {
        lookup: UnscopedParameterLookup.normalized(['user1']),
        bucketParameters: [{ id: 'user1' }]
      }
    ]);
    const requestParams = normalizeTokenParameters({ user_id: 'user1' }, { other_id: 'red' });
    expect(query.getLookups(requestParams)).toEqual([UnscopedParameterLookup.normalized(['user1'])]);
  });

  test('request.parameters() in SELECT', function () {
    const sql =
      "SELECT id, request.parameters() ->> 'other_id' as other_id FROM users WHERE id = token_parameters.user_id";
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.evaluateParameterRow(TABLE_USERS, { id: 'user1' })).toEqual([
      {
        lookup: UnscopedParameterLookup.normalized(['user1']),
        bucketParameters: [{ id: 'user1' }]
      }
    ]);
    const requestParams = normalizeTokenParameters({ user_id: 'user1' }, { other_id: 'red' });
    expect(query.getLookups(requestParams)).toEqual([UnscopedParameterLookup.normalized(['user1'])]);
  });

  test('request.jwt()', function () {
    const sql = "SELECT FROM users WHERE id = request.jwt() ->> 'sub'";
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);

    const requestParams = normalizeTokenParameters({ user_id: 'user1' });
    expect(query.getLookups(requestParams)).toEqual([UnscopedParameterLookup.normalized(['user1'])]);
  });

  test('request.user_id()', function () {
    const sql = 'SELECT FROM users WHERE id = request.user_id()';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toEqual([]);

    const requestParams = normalizeTokenParameters({ user_id: 'user1' });
    expect(query.getLookups(requestParams)).toEqual([UnscopedParameterLookup.normalized(['user1'])]);
  });

  describe('custom hydrationState', function () {
    const hydrationState: HydrationState = {
      getBucketSourceScope(source) {
        return { bucketPrefix: `${source.uniqueName}-test` };
      },
      getParameterIndexLookupScope(source) {
        return {
          lookupName: `${source.defaultLookupScope.lookupName}.test`,
          queryId: `${source.defaultLookupScope.queryId}.test`
        };
      }
    };

    let query: SqlParameterQuery;

    beforeEach(() => {
      const sql = 'SELECT id as group_id FROM groups WHERE token_parameters.user_id IN groups.user_ids';
      query = SqlParameterQuery.fromSql(
        'mybucket',
        sql,
        PARSE_OPTIONS,
        'myquery',
        EMPTY_DATA_SOURCE
      ) as SqlParameterQuery;

      expect(query.errors).toEqual([]);
    });

    test('for lookups', function () {
      const merged = mergeParameterIndexLookupCreators(hydrationState, [query]);
      const result = merged.evaluateParameterRow(TABLE_GROUPS, {
        id: 'group1',
        user_ids: JSON.stringify(['test-user', 'other-user'])
      });
      expect(result).toEqual([
        {
          lookup: ScopedParameterLookup.direct({ lookupName: 'mybucket.test', queryId: 'myquery.test' }, ['test-user']),
          bucketParameters: [{ group_id: 'group1' }]
        },
        {
          lookup: ScopedParameterLookup.direct({ lookupName: 'mybucket.test', queryId: 'myquery.test' }, [
            'other-user'
          ]),
          bucketParameters: [{ group_id: 'group1' }]
        }
      ]);
    });

    test('for queries', async function () {
      const hydrated = query.createParameterQuerierSource({ hydrationState });

      const queriers: BucketParameterQuerier[] = [];
      const errors: QuerierError[] = [];
      const pending = { queriers, errors };

      const querierOptions: GetQuerierOptions = {
        hasDefaultStreams: true,
        globalParameters: new RequestParameters(
          {
            sub: 'test-user'
          },
          {}
        ),
        streams: {}
      };

      hydrated.pushBucketParameterQueriers(pending, querierOptions);

      expect(errors).toEqual([]);
      expect(queriers.length).toBe(1);

      const querier = queriers[0];
      expect(querier.hasDynamicBuckets).toBeTruthy();
      expect(await findQuerierLookups(querier)).toEqual([
        ScopedParameterLookup.direct({ lookupName: 'mybucket.test', queryId: 'myquery.test' }, ['test-user'])
      ]);
    });
  });

  test('invalid OR in parameter queries', () => {
    // Supporting this case is more tricky. We can do this by effectively denormalizing the OR clause
    // into separate queries, but it's a significant change. For now, developers should do that manually.
    const sql =
      "SELECT workspaces.id AS workspace_id FROM workspaces WHERE workspaces.user_id = token_parameters.user_id OR visibility = 'public'";
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors[0].message).toMatch(/must use the same parameters/);
  });

  test('invalid OR in parameter queries (2)', () => {
    const sql =
      'SELECT id from users WHERE filter_param = upper(token_parameters.user_id) OR filter_param = lower(token_parameters.user_id)';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors[0].message).toMatch(/must use the same parameters/);
  });

  test('invalid parameter match clause (1)', () => {
    const sql = 'SELECT FROM users WHERE (id = token_parameters.user_id) = false';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors[0].message).toMatch(/Parameter match clauses cannot be used here/);
  });

  test('invalid parameter match clause (2)', () => {
    const sql = 'SELECT FROM users WHERE NOT (id = token_parameters.user_id)';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors[0].message).toMatch(/Parameter match clauses cannot be used here/);
  });

  test('invalid parameter match clause (3)', () => {
    // May be supported in the future
    const sql = 'SELECT FROM users WHERE token_parameters.start_at < users.created_at';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors[0].message).toMatch(/Cannot use table values and parameters in the same clauses/);
  });

  test('invalid parameter match clause (4)', () => {
    const sql = 'SELECT FROM users WHERE json_extract(users.description, token_parameters.path)';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors[0].message).toMatch(/Cannot use table values and parameters in the same clauses/);
  });

  test('invalid parameter match clause (5)', () => {
    const sql = 'SELECT (user_parameters.role = posts.roles) as r FROM posts';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors[0].message).toMatch(/Parameter match expression is not allowed here/);
  });

  test('invalid function schema', () => {
    const sql = 'SELECT FROM users WHERE something.length(users.id) = 0';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors[0].message).toMatch(/Function 'something.length' is not defined/);
  });

  test('validate columns', () => {
    const schema = BASIC_SCHEMA;

    const q1 = SqlParameterQuery.fromSql(
      'q4',
      'SELECT id FROM assets WHERE owner_id = token_parameters.user_id',
      {
        ...PARSE_OPTIONS,
        schema
      },
      '1',
      EMPTY_DATA_SOURCE
    );
    expect(q1.errors).toMatchObject([]);

    const q2 = SqlParameterQuery.fromSql(
      'q5',
      'SELECT id as asset_id FROM assets WHERE other_id = token_parameters.user_id',
      { ...PARSE_OPTIONS, schema },
      '1',
      EMPTY_DATA_SOURCE
    );

    expect(q2.errors).toMatchObject([
      {
        message: 'Column not found: other_id',
        type: 'warning'
      }
    ]);
  });

  describe('dangerous queries', function () {
    function testDangerousQuery(sql: string) {
      test(sql, function () {
        const query = SqlParameterQuery.fromSql(
          'mybucket',
          sql,
          PARSE_OPTIONS,
          '1',
          EMPTY_DATA_SOURCE
        ) as SqlParameterQuery;
        expect(query.errors).toMatchObject([
          {
            message:
              "Potentially dangerous query based on parameters set by the client. The client can send any value for these parameters so it's not a good place to do authorization."
          }
        ]);
        expect(query.usesDangerousRequestParameters).toEqual(true);
      });
    }
    function testSafeQuery(sql: string) {
      test(sql, function () {
        const query = SqlParameterQuery.fromSql(
          'mybucket',
          sql,
          PARSE_OPTIONS,
          '1',
          EMPTY_DATA_SOURCE
        ) as SqlParameterQuery;
        expect(query.errors).toEqual([]);
        expect(query.usesDangerousRequestParameters).toEqual(false);
      });
    }

    testSafeQuery('SELECT id as user_id FROM users WHERE users.user_id = request.user_id()');
    testSafeQuery(
      "SELECT request.jwt() ->> 'org_id' as org_id, id as project_id FROM projects WHERE id = request.parameters() ->> 'project_id'"
    );
    testSafeQuery(
      "SELECT id as project_id FROM projects WHERE org_id = request.jwt() ->> 'org_id' AND id = request.parameters() ->> 'project_id'"
    );
    testSafeQuery('SELECT id as category_id FROM categories');
    // Can be considered dangerous, but tricky to implement with the current parsing structure
    testSafeQuery(
      "SELECT id as project_id FROM projects WHERE id = request.parameters() ->> 'project_id' AND request.jwt() ->> 'role' = 'authenticated'"
    );

    testDangerousQuery("SELECT id as project_id FROM projects WHERE id = request.parameters() ->> 'project_id'");

    testDangerousQuery("SELECT id as category_id, request.parameters() ->> 'project_id' as project_id FROM categories");
    // Can be safe, but better to opt in
    testDangerousQuery("SELECT id as category_id FROM categories WHERE request.parameters() ->> 'include_categories'");
  });

  describe('bucket priorities', () => {
    test('valid definition', function () {
      const sql = 'SELECT id as group_id, 1 AS _priority FROM groups WHERE token_parameters.user_id IN groups.user_ids';
      const query = SqlParameterQuery.fromSql(
        'mybucket',
        sql,
        PARSE_OPTIONS,
        '1',
        EMPTY_DATA_SOURCE
      ) as SqlParameterQuery;
      expect(query.errors).toEqual([]);
      expect(Object.entries(query.lookupExtractors)).toHaveLength(1);
      expect(Object.entries(query.parameterExtractors)).toHaveLength(0);
      expect(query.bucketParameters).toEqual(['group_id']);
      expect(query.priority).toBe(1);
    });

    test('valid definition, static query', function () {
      const sql = 'SELECT token_parameters.user_id, 0 AS _priority';
      const query = SqlParameterQuery.fromSql(
        'mybucket',
        sql,
        PARSE_OPTIONS,
        '1',
        EMPTY_DATA_SOURCE
      ) as StaticSqlParameterQuery;
      expect(query.errors).toEqual([]);
      expect(Object.entries(query.parameterExtractors)).toHaveLength(1);
      expect(query.bucketParameters).toEqual(['user_id']);
      expect(query.priority).toBe(0);
    });

    test('invalid dynamic query', function () {
      const sql = 'SELECT LENGTH(assets.name) AS _priority FROM assets';
      const query = SqlParameterQuery.fromSql(
        'mybucket',
        sql,
        PARSE_OPTIONS,
        '1',
        EMPTY_DATA_SOURCE
      ) as SqlParameterQuery;

      expect(query.errors).toMatchObject([{ message: 'Priority must be a simple integer literal' }]);
    });

    test('invalid literal type', function () {
      const sql = "SELECT 'very fast please' AS _priority";
      const query = SqlParameterQuery.fromSql(
        'mybucket',
        sql,
        PARSE_OPTIONS,
        '1',
        EMPTY_DATA_SOURCE
      ) as StaticSqlParameterQuery;

      expect(query.errors).toMatchObject([{ message: 'Priority must be a simple integer literal' }]);
    });

    test('invalid literal value', function () {
      const sql = 'SELECT 15 AS _priority';
      const query = SqlParameterQuery.fromSql(
        'mybucket',
        sql,
        PARSE_OPTIONS,
        '1',
        EMPTY_DATA_SOURCE
      ) as StaticSqlParameterQuery;

      expect(query.errors).toMatchObject([
        { message: 'Invalid value for priority, must be between 0 and 3 (inclusive).' }
      ]);
    });
  });
});
