/// <reference path="../matchers.d.ts" />
import { describe, expect, test } from 'vitest';
import {
  BucketParameterQuerier,
  DEFAULT_TAG,
  ParameterLookup,
  SourceTableInterface,
  SqliteJsonRow,
  SqliteRow,
  StaticSchema,
  SyncStream,
  syncStreamFromSql
} from '../../src/index.js';
import { normalizeQuerierOptions, PARSE_OPTIONS, TestSourceTable } from './util.js';

describe('streams', () => {
  test('without filter', () => {
    const desc = parseStream('SELECT * FROM comments');

    expect(desc.variants).toHaveLength(1);
    expect(evaluateBucketIds(desc, COMMENTS, { id: 'foo' })).toStrictEqual(['stream|0[]']);
    expect(desc.evaluateRow({ sourceTable: USERS, record: { id: 'foo' } })).toHaveLength(0);
  });

  test('row condition', () => {
    const desc = parseStream('SELECT * FROM comments WHERE length(content) > 5');
    expect(evaluateBucketIds(desc, COMMENTS, { id: 'foo', content: 'a' })).toStrictEqual([]);
    expect(evaluateBucketIds(desc, COMMENTS, { id: 'foo', content: 'aaaaaa' })).toStrictEqual(['stream|0[]']);
  });

  test('stream parameter', () => {
    const desc = parseStream('SELECT * FROM comments WHERE issue_id = subscription_parameters.id');
    expect(evaluateBucketIds(desc, COMMENTS, { id: 'foo', issue_id: 'a' })).toStrictEqual(['stream|0["a"]']);
  });

  test('row filter and stream parameter', async () => {
    const desc = parseStream(
      'SELECT * FROM comments WHERE length(content) > 5 AND issue_id = subscription_parameters.id'
    );
    expect(evaluateBucketIds(desc, COMMENTS, { id: 'foo', content: 'a', issue_id: 'a' })).toStrictEqual([]);
    expect(evaluateBucketIds(desc, COMMENTS, { id: 'foo', content: 'aaaaaa', issue_id: 'i' })).toStrictEqual([
      'stream|0["i"]'
    ]);

    expect(await queryBucketIds(desc, { parameters: { id: 'subscribed_issue' } })).toStrictEqual([
      'stream|0["subscribed_issue"]'
    ]);
  });

  describe('or', () => {
    test('parameter match or request condition', async () => {
      const desc = parseStream('SELECT * FROM issues WHERE owner_id = request.user_id() OR token_parameters.is_admin');

      expect(evaluateBucketIds(desc, ISSUES, { id: 'foo', owner_id: 'u1' })).toStrictEqual([
        'stream|0["u1"]',
        'stream|1[]'
      ]);

      expect(
        await queryBucketIds(desc, {
          token_parameters: {
            user_id: 'u1',
            is_admin: false
          }
        })
      ).toStrictEqual(['stream|0["u1"]']);

      expect(
        await queryBucketIds(desc, {
          token_parameters: {
            user_id: 'u1',
            is_admin: true
          }
        })
      ).toStrictEqual(['stream|0["u1"]', 'stream|1[]']);
    });

    test('parameter match or row condition', async () => {
      const desc = parseStream('SELECT * FROM issues WHERE owner_id = request.user_id() OR LENGTH(name) = 3');
      expect(evaluateBucketIds(desc, ISSUES, { id: 'foo', owner_id: 'a', name: '' })).toStrictEqual(['stream|0["a"]']);
      expect(evaluateBucketIds(desc, ISSUES, { id: 'foo', owner_id: 'a', name: 'aaa' })).toStrictEqual([
        'stream|0["a"]',
        'stream|1[]'
      ]);

      expect(
        await queryBucketIds(desc, {
          token_parameters: {
            user_id: 'u1'
          }
        })
      ).toStrictEqual(['stream|0["u1"]', 'stream|1[]']);
    });

    test('row condition or parameter condition', async () => {
      const desc = parseStream('SELECT * FROM comments WHERE LENGTH(content) > 5 OR token_parameters.is_admin');

      expect(evaluateBucketIds(desc, COMMENTS, { id: 'foo', content: 'short' })).toStrictEqual(['stream|1[]']);
      expect(evaluateBucketIds(desc, COMMENTS, { id: 'foo', content: 'longer' })).toStrictEqual([
        'stream|0[]',
        'stream|1[]'
      ]);

      expect(
        await queryBucketIds(desc, {
          token_parameters: {
            is_admin: false
          }
        })
      ).toStrictEqual(['stream|0[]']);
      expect(
        await queryBucketIds(desc, {
          token_parameters: {
            is_admin: true
          }
        })
      ).toStrictEqual(['stream|0[]', 'stream|1[]']);
    });

    test('row condition or row condition', () => {
      const desc = parseStream(
        'SELECT * FROM comments WHERE LENGTH(content) > 5 OR json_array_length(tagged_users) > 1'
      );
      // Complex conditions that only operate on row data don't need variants.
      expect(desc.variants).toHaveLength(1);

      expect(evaluateBucketIds(desc, COMMENTS, { id: 'foo', content: 'short', tagged_users: '[]' })).toStrictEqual([]);
      expect(evaluateBucketIds(desc, COMMENTS, { id: 'foo', content: 'longer', tagged_users: '[]' })).toStrictEqual([
        'stream|0[]'
      ]);
      expect(
        evaluateBucketIds(desc, COMMENTS, { id: 'foo', content: 'longer', tagged_users: '["a","b"]' })
      ).toStrictEqual(['stream|0[]']);
      expect(
        evaluateBucketIds(desc, COMMENTS, { id: 'foo', content: 'short', tagged_users: '["a","b"]' })
      ).toStrictEqual(['stream|0[]']);
    });

    test('request condition or request condition', async () => {
      const desc = parseStream('SELECT * FROM comments WHERE token_parameters.a OR token_parameters.b');
      // Complex conditions that only operate on request data don't need variants.
      expect(desc.variants).toHaveLength(1);

      expect(evaluateBucketIds(desc, COMMENTS, { id: 'foo', content: 'whatever]' })).toStrictEqual(['stream|0[]']);
      expect(
        await queryBucketIds(desc, {
          token_parameters: {
            a: false,
            b: false
          }
        })
      ).toStrictEqual([]);
      expect(
        await queryBucketIds(desc, {
          token_parameters: {
            a: true,
            b: false
          }
        })
      ).toStrictEqual(['stream|0[]']);
    });

    test('subquery or token parameter', async () => {
      const desc = parseStream(
        'SELECT * FROM comments WHERE issue_id IN (SELECT id FROM issues WHERE owner_id = request.user_id()) OR token_parameters.is_admin'
      );

      expect(evaluateBucketIds(desc, COMMENTS, { id: 'c', issue_id: 'i1' })).toStrictEqual([
        'stream|0["i1"]',
        'stream|1[]'
      ]);

      expect(desc.evaluateParameterRow(ISSUES, { id: 'i1', owner_id: 'u1' })).toStrictEqual([
        {
          lookup: ParameterLookup.normalized('stream', '0', ['u1']),
          bucketParameters: [
            {
              result: 'i1'
            }
          ]
        }
      ]);

      function getParameterSets(lookups: ParameterLookup[]) {
        expect(lookups).toStrictEqual([ParameterLookup.normalized('stream', '0', ['u1'])]);

        return [{ result: 'i1' }];
      }
      expect(
        await queryBucketIds(desc, { token_parameters: { user_id: 'u1', is_admin: false }, getParameterSets })
      ).toStrictEqual(['stream|0["i1"]']);
      expect(
        await queryBucketIds(desc, { token_parameters: { user_id: 'u1', is_admin: true }, getParameterSets })
      ).toStrictEqual(['stream|0["i1"]', 'stream|1[]']);
    });

    // TODO: Two subqueries
  });

  describe('in', () => {
    test('row value in subquery', async () => {
      const desc = parseStream(
        'SELECT * FROM comments WHERE issue_id IN (SELECT id FROM issues WHERE owner_id = request.user_id())'
      );

      expect(desc.tableSyncsParameters(ISSUES)).toBe(true);
      expect(desc.evaluateParameterRow(ISSUES, { id: 'issue_id', owner_id: 'user1', name: 'name' })).toStrictEqual([
        {
          lookup: ParameterLookup.normalized('stream', '0', ['user1']),
          bucketParameters: [
            {
              result: 'issue_id'
            }
          ]
        }
      ]);
      expect(evaluateBucketIds(desc, COMMENTS, { id: 'c', issue_id: 'issue_id' })).toStrictEqual([
        'stream|0["issue_id"]'
      ]);

      expect(
        await queryBucketIds(desc, {
          token_parameters: { user_id: 'user1' },
          getParameterSets(lookups) {
            expect(lookups).toStrictEqual([ParameterLookup.normalized('stream', '0', ['user1'])]);

            return [{ result: 'issue_id' }];
          }
        })
      ).toStrictEqual(['stream|0["issue_id"]']);
    });

    test('parameter value in subquery', async () => {
      const desc = parseStream('SELECT * FROM issues WHERE request.user_id() IN (SELECT id FROM users WHERE is_admin)');

      expect(desc.tableSyncsParameters(ISSUES)).toBe(false);
      expect(desc.tableSyncsParameters(USERS)).toBe(true);

      expect(desc.evaluateParameterRow(USERS, { id: 'u', is_admin: 1n })).toStrictEqual([
        {
          lookup: ParameterLookup.normalized('stream', '0', ['u']),
          bucketParameters: [
            {
              result: 'u'
            }
          ]
        }
      ]);
      expect(desc.evaluateParameterRow(USERS, { id: 'u', is_admin: 0n })).toStrictEqual([]);

      // Should return bucket id for admin users
      expect(
        await queryBucketIds(desc, {
          token_parameters: { user_id: 'u' },
          getParameterSets: (lookups: ParameterLookup[]) => {
            expect(lookups).toStrictEqual([ParameterLookup.normalized('stream', '0', ['u'])]);
            return [{ result: 'u' }];
          }
        })
      ).toStrictEqual(['stream|0[]']);

      // And not for others
      expect(
        await queryBucketIds(desc, {
          token_parameters: { user_id: 'u2' },
          getParameterSets: (lookups: ParameterLookup[]) => {
            expect(lookups).toStrictEqual([ParameterLookup.normalized('stream', '0', ['u2'])]);
            return [];
          }
        })
      ).toStrictEqual([]);
    });

    test('two subqueries', async () => {
      const desc = parseStream(`SELECT * FROM users WHERE
            id IN (SELECT user_a FROM friends WHERE user_b = request.user_id()) OR
            id IN (SELECT user_b FROM friends WHERE user_a = request.user_id())
        `);

      expect(evaluateBucketIds(desc, USERS, { id: 'a', name: 'a' })).toStrictEqual(['stream|0["a"]', 'stream|1["a"]']);

      expect(desc.evaluateParameterRow(FRIENDS, { user_a: 'a', user_b: 'b' })).toStrictEqual([
        {
          lookup: ParameterLookup.normalized('stream', '0', ['b']),
          bucketParameters: [
            {
              result: 'a'
            }
          ]
        },
        {
          lookup: ParameterLookup.normalized('stream', '1', ['a']),
          bucketParameters: [
            {
              result: 'b'
            }
          ]
        }
      ]);

      function getParameterSets(lookups: ParameterLookup[]) {
        expect(lookups).toHaveLength(1);
        const [lookup] = lookups;
        if (lookup.values[1] == '0') {
          expect(lookup).toStrictEqual(ParameterLookup.normalized('stream', '0', ['a']));
          return [];
        } else {
          expect(lookup).toStrictEqual(ParameterLookup.normalized('stream', '1', ['a']));
          return [{ result: 'b' }];
        }
      }

      expect(
        await queryBucketIds(desc, {
          token_parameters: { user_id: 'a' },
          getParameterSets
        })
      ).toStrictEqual(['stream|1["b"]']);
    });
  });

  describe('overlap', () => {
    test('row value in subquery', async () => {
      const desc = parseStream(
        'SELECT * FROM comments WHERE tagged_users && (SELECT user_a FROM friends WHERE user_b = request.user_id())'
      );

      expect(desc.tableSyncsParameters(FRIENDS)).toBe(true);
      expect(desc.evaluateParameterRow(FRIENDS, { user_a: 'a', user_b: 'b' })).toStrictEqual([
        {
          lookup: ParameterLookup.normalized('stream', '0', ['b']),
          bucketParameters: [
            {
              result: 'a'
            }
          ]
        }
      ]);
      expect(evaluateBucketIds(desc, COMMENTS, { id: 'c', tagged_users: '["a", "b"]' })).toStrictEqual([
        'stream|0["a"]',
        'stream|0["b"]'
      ]);

      expect(
        await queryBucketIds(desc, {
          token_parameters: { user_id: 'user1' },
          getParameterSets(lookups) {
            expect(lookups).toStrictEqual([ParameterLookup.normalized('stream', '0', ['user1'])]);

            return [{ result: 'issue_id' }];
          }
        })
      ).toStrictEqual(['stream|0["issue_id"]']);
    });
  });

  describe('errors', () => {
    test('IN operator with static left clause', () => {
      const [_, errors] = syncStreamFromSql(
        's',
        "SELECT * FROM issues WHERE 'static' IN (SELECT id FROM users WHERE is_admin)",
        options
      );

      expect(errors).toMatchObject([
        expect.toBeSqlRuleError(
          'For IN subqueries, the left operand must either depend on the row to sync or stream parameters.',
          "'static'"
        )
      ]);
    });
  });

  /*
  test('static filter', () => {
    const desc = parseSingleBucketDescription(`
streams:
  lists:
    query: SELECT * FROM assets WHERE count > 5
`);
    expect(desc.bucketParameters).toBeFalsy();
    assert.isEmpty(desc.parameterQueries);

    assert.isEmpty(
      desc.evaluateRow({
        sourceTable: ASSETS,
        record: {
          count: 4
        }
      })
    );

    expect(
      desc.evaluateRow({
        sourceTable: ASSETS,
        record: {
          count: 6
        }
      })
    ).toHaveLength(1);
  });

  test('stream param', () => {
    const desc = parseSingleBucketDescription(`
streams:
  lists:
    query: SELECT * FROM assets WHERE id = stream.params() ->> 'id';
`);

    // This should be desuraged to
    //  params: SELECT request.params() ->> 'id' AS p0
    //  data: SELECT * FROM assets WHERE id = bucket.p0

    expect(desc.globalParameterQueries).toHaveLength(1);
    const [parameter] = desc.globalParameterQueries;
    expect(parameter.bucketParameters).toEqual(['p0']);

    const [data] = desc.dataQueries;
    expect(data.bucketParameters).toEqual(['bucket.p0']);
  });

  test('user filter', () => {
    const desc = parseSingleBucketDescription(`
streams:
  lists:
    query: SELECT * FROM assets WHERE request.jwt() ->> 'isAdmin'
`);

    // This should be desuraged to
    //  params: SELECT request.params() ->> 'id' AS p0
    //  data: SELECT * FROM assets WHERE id = bucket.p0

    const [data] = desc.dataQueries;
    expect(data.bucketParameters).toEqual(['bucket.p0']);
  });
  */
});

const USERS = new TestSourceTable('users');
const ISSUES = new TestSourceTable('issues');
const COMMENTS = new TestSourceTable('comments');
const FRIENDS = new TestSourceTable('friends');

const schema = new StaticSchema([
  {
    tag: DEFAULT_TAG,
    schemas: [
      {
        name: 'test_schema',
        tables: [
          {
            name: 'users',
            columns: [
              { name: 'id', pg_type: 'uuid' },
              { name: 'name', pg_type: 'text' },
              { name: 'is_admin', pg_type: 'bool' }
            ]
          },
          {
            name: 'issues',
            columns: [
              { name: 'id', pg_type: 'uuid' },
              { name: 'owner_id', pg_type: 'uuid' },
              { name: 'name', pg_type: 'text' }
            ]
          },
          {
            name: 'comments',
            columns: [
              { name: 'id', pg_type: 'uuid' },
              { name: 'issue_id', pg_type: 'uuid' },
              { name: 'content', pg_type: 'text' },
              { name: 'tagged_users', pg_type: 'text' }
            ]
          },
          {
            name: 'friends',
            columns: [
              { name: 'id', pg_type: 'uuid' },
              { name: 'user_a', pg_type: 'uuid' },
              { name: 'user_b', pg_type: 'uuid' }
            ]
          }
        ]
      }
    ]
  }
]);

const options = { schema: schema, ...PARSE_OPTIONS };

function evaluateBucketIds(stream: SyncStream, sourceTable: SourceTableInterface, record: SqliteRow) {
  return stream.evaluateRow({ sourceTable, record }).map((r) => {
    if ('error' in r) {
      throw new Error(`Unexpected error evaluating row: ${r.error}`);
    }

    return r.bucket;
  });
}

async function queryBucketIds(
  stream: SyncStream,
  options?: {
    token_parameters?: Record<string, any>;
    parameters?: Record<string, any>;
    getParameterSets?: (lookups: ParameterLookup[]) => SqliteJsonRow[];
  }
) {
  const queriers: BucketParameterQuerier[] = [];
  stream.pushBucketParameterQueriers(
    queriers,
    normalizeQuerierOptions(
      options?.token_parameters ?? {},
      {},
      { stream: [{ opaque_id: 'test_subscription', parameters: options?.parameters ?? null }] }
    )
  );

  async function getParameterSets(lookups: ParameterLookup[]): Promise<SqliteJsonRow[]> {
    const provided = options?.getParameterSets;
    if (provided) {
      return provided(lookups);
    } else {
      throw 'unexpected dynamic lookup';
    }
  }

  const buckets: string[] = [];
  for (const querier of queriers) {
    buckets.push(...querier.staticBuckets.map((b) => b.bucket));

    if (querier.hasDynamicBuckets) {
      buckets.push(
        ...(
          await querier.queryDynamicBucketDescriptions({
            getParameterSets
          })
        ).map((e) => e.bucket)
      );
    }
  }
  return buckets;
}

function parseStream(sql: string, name = 'stream') {
  const [stream, errors] = syncStreamFromSql(name, sql, options);
  if (errors.length) {
    throw new Error(`Unexpected errors when parsing stream ${sql}: ${errors}`);
  }

  return stream;
}
