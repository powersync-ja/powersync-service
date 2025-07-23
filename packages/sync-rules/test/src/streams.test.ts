/// <reference path="../matchers.d.ts" />
import { describe, expect, test } from 'vitest';
import {
  BucketParameterQuerier,
  DEFAULT_TAG,
  GetBucketParameterQuerierResult,
  mergeBucketParameterQueriers,
  ParameterLookup,
  QuerierError,
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
      ).toStrictEqual(['stream|1[]', 'stream|0["i1"]']);
    });
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

    test('negated subquery', () => {
      const [_, errors] = syncStreamFromSql(
        's',
        'select * from comments where issue_id not in (select id from issues where owner_id = request.user_id())',
        options
      );

      expect(errors).toMatchObject([
        expect.toBeSqlRuleError(
          'Negations are not allowed here',
          'issue_id not in (select id from issues where owner_id = request.user_id()'
        )
      ]);
    });

    test('negated subquery from outer not operator', () => {
      const [_, errors] = syncStreamFromSql(
        's',
        'select * from comments where not (issue_id in (select id from issues where owner_id = request.user_id()))',
        options
      );

      expect(errors).toMatchObject([
        expect.toBeSqlRuleError(
          'Negations are not allowed here',
          'not (issue_id in (select id from issues where owner_id = request.user_id()'
        )
      ]);
    });
  });

  describe('normalization', () => {
    test('double negation', async () => {
      const desc = parseStream(
        'select * from comments where NOT (issue_id not in (select id from issues where owner_id = request.user_id()))'
      );

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

    test('negated or', () => {
      const desc = parseStream(
        'select * from comments where not (length(content) = 5 OR issue_id not in (select id from issues where owner_id = request.user_id()))'
      );

      expect(evaluateBucketIds(desc, COMMENTS, { id: 'c', issue_id: 'issue_id', content: 'foo' })).toStrictEqual([
        'stream|0["issue_id"]'
      ]);
      expect(evaluateBucketIds(desc, COMMENTS, { id: 'c', issue_id: 'issue_id', content: 'foooo' })).toStrictEqual([]);
    });

    test('negated and', () => {
      const desc = parseStream(
        'select * from comments where not (length(content) = 5 AND issue_id not in (select id from issues where owner_id = request.user_id()))'
      );
      expect(desc.variants).toHaveLength(2);

      expect(evaluateBucketIds(desc, COMMENTS, { id: 'c', issue_id: 'issue_id', content: 'foo' })).toStrictEqual([
        'stream|0[]',
        'stream|1["issue_id"]'
      ]);
      expect(evaluateBucketIds(desc, COMMENTS, { id: 'c', issue_id: 'issue_id', content: 'foooo' })).toStrictEqual([
        'stream|1["issue_id"]'
      ]);
    });

    test('distribute and', async () => {
      const desc = parseStream(
        `select * from comments where 
          (issue_id in (select id from issues where owner_id = request.user_id())
            OR token_parameters.is_admin)
          AND
          LENGTH(content) > 2
        `
      );
      expect(desc.variants).toHaveLength(2);

      expect(evaluateBucketIds(desc, COMMENTS, { id: 'c', issue_id: 'issue_id', content: 'a' })).toStrictEqual([]);
      expect(evaluateBucketIds(desc, COMMENTS, { id: 'c', issue_id: 'issue_id', content: 'aaa' })).toStrictEqual([
        'stream|0["issue_id"]',
        'stream|1[]'
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
      expect(
        await queryBucketIds(desc, {
          token_parameters: { user_id: 'user1', is_admin: true },
          getParameterSets(lookups) {
            expect(lookups).toStrictEqual([ParameterLookup.normalized('stream', '0', ['user1'])]);

            return [{ result: 'issue_id' }];
          }
        })
      ).toStrictEqual(['stream|1[]', 'stream|0["issue_id"]']);
    });
  });
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

async function createQueriers(
  stream: SyncStream,
  options?: {
    token_parameters?: Record<string, any>;
    parameters?: Record<string, any>;
    getParameterSets?: (lookups: ParameterLookup[]) => SqliteJsonRow[];
  }
): Promise<GetBucketParameterQuerierResult> {
  const queriers: BucketParameterQuerier[] = [];
  const errors: QuerierError[] = [];
  const pending = { queriers, errors };

  stream.pushBucketParameterQueriers(
    pending,
    normalizeQuerierOptions(
      options?.token_parameters ?? {},
      {},
      { stream: [{ opaque_id: 0, parameters: options?.parameters ?? null }] }
    )
  );

  return { querier: mergeBucketParameterQueriers(queriers), errors };
}

async function queryBucketIds(
  stream: SyncStream,
  options?: {
    token_parameters?: Record<string, any>;
    parameters?: Record<string, any>;
    getParameterSets?: (lookups: ParameterLookup[]) => SqliteJsonRow[];
  }
) {
  const { querier, errors } = await createQueriers(stream, options);
  expect(errors).toHaveLength(0);

  async function getParameterSets(lookups: ParameterLookup[]): Promise<SqliteJsonRow[]> {
    const provided = options?.getParameterSets;
    if (provided) {
      return provided(lookups);
    } else {
      throw 'unexpected dynamic lookup';
    }
  }

  const buckets: string[] = [];

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

  return buckets;
}

function parseStream(sql: string, name = 'stream') {
  const [stream, errors] = syncStreamFromSql(name, sql, options);
  if (errors.length) {
    throw new Error(`Unexpected errors when parsing stream ${sql}: ${errors}`);
  }

  return stream;
}
