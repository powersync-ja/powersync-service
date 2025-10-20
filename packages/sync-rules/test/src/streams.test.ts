/// <reference path="../matchers.d.ts" />
import { describe, expect, test } from 'vitest';
import {
  BucketParameterQuerier,
  CompatibilityContext,
  CompatibilityEdition,
  DEFAULT_TAG,
  GetBucketParameterQuerierResult,
  GetQuerierOptions,
  mergeBucketParameterQueriers,
  ParameterLookup,
  QuerierError,
  RequestParameters,
  SourceTableInterface,
  SqliteJsonRow,
  SqliteRow,
  SqlSyncRules,
  StaticSchema,
  StreamParseOptions,
  SyncStream,
  syncStreamFromSql
} from '../../src/index.js';
import { normalizeQuerierOptions, PARSE_OPTIONS, TestSourceTable } from './util.js';

describe('streams', () => {
  test('refuses edition: 1', () => {
    expect(() =>
      syncStreamFromSql('stream', 'SELECT * FROM comments', {
        compatibility: CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY,
        defaultSchema: 'public'
      })
    ).throws('Sync streams require edition 2 or later');
  });

  test('without filter', () => {
    const desc = parseStream('SELECT * FROM comments');

    expect(desc.variants).toHaveLength(1);
    expect(evaluateBucketIds(desc, COMMENTS, { id: 'foo' })).toStrictEqual(['1#stream|0[]']);
    expect(desc.evaluateRow({ sourceTable: USERS, bucketIdTransformer, record: { id: 'foo' } })).toHaveLength(0);
  });

  test('row condition', () => {
    const desc = parseStream('SELECT * FROM comments WHERE length(content) > 5');
    expect(evaluateBucketIds(desc, COMMENTS, { id: 'foo', content: 'a' })).toStrictEqual([]);
    expect(evaluateBucketIds(desc, COMMENTS, { id: 'foo', content: 'aaaaaa' })).toStrictEqual(['1#stream|0[]']);
  });

  test('stream parameter', () => {
    const desc = parseStream("SELECT * FROM comments WHERE issue_id = subscription.parameter('id')");
    expect(evaluateBucketIds(desc, COMMENTS, { id: 'foo', issue_id: 'a' })).toStrictEqual(['1#stream|0["a"]']);
  });

  test('row filter and stream parameter', async () => {
    const desc = parseStream(
      "SELECT * FROM comments WHERE length(content) > 5 AND issue_id = subscription.parameter('id')"
    );
    expect(evaluateBucketIds(desc, COMMENTS, { id: 'foo', content: 'a', issue_id: 'a' })).toStrictEqual([]);
    expect(evaluateBucketIds(desc, COMMENTS, { id: 'foo', content: 'aaaaaa', issue_id: 'i' })).toStrictEqual([
      '1#stream|0["i"]'
    ]);

    expect(await queryBucketIds(desc, { parameters: { id: 'subscribed_issue' } })).toStrictEqual([
      '1#stream|0["subscribed_issue"]'
    ]);
  });

  test('legacy token parameter', async () => {
    const desc = parseStream(`SELECT * FROM issues WHERE owner_id = auth.parameter('$.parameters.test')`);

    const queriers: BucketParameterQuerier[] = [];
    const errors: QuerierError[] = [];
    const pending = { queriers, errors };
    desc.pushBucketParameterQueriers(
      pending,
      normalizeQuerierOptions(
        { test: 'foo' },
        {},
        { stream: [{ opaque_id: 0, parameters: null }] },
        bucketIdTransformer
      )
    );

    expect(mergeBucketParameterQueriers(queriers).staticBuckets).toEqual([
      {
        bucket: '1#stream|0["foo"]',
        definition: 'stream',
        inclusion_reasons: [
          {
            subscription: 0
          }
        ],
        priority: 3
      }
    ]);
  });

  describe('or', () => {
    test('parameter match or request condition', async () => {
      const desc = parseStream("SELECT * FROM issues WHERE owner_id = auth.user_id() OR auth.parameter('is_admin')");

      expect(evaluateBucketIds(desc, ISSUES, { id: 'foo', owner_id: 'u1' })).toStrictEqual([
        '1#stream|0["u1"]',
        '1#stream|1[]'
      ]);

      expect(
        await queryBucketIds(desc, {
          token: {
            sub: 'u1',
            is_admin: false
          }
        })
      ).toStrictEqual(['1#stream|0["u1"]']);

      expect(
        await queryBucketIds(desc, {
          token: {
            sub: 'u1',
            is_admin: true
          }
        })
      ).toStrictEqual(['1#stream|0["u1"]', '1#stream|1[]']);
    });

    test('parameter match or row condition', async () => {
      const desc = parseStream('SELECT * FROM issues WHERE owner_id = auth.user_id() OR LENGTH(name) = 3');
      expect(evaluateBucketIds(desc, ISSUES, { id: 'foo', owner_id: 'a', name: '' })).toStrictEqual([
        '1#stream|0["a"]'
      ]);
      expect(evaluateBucketIds(desc, ISSUES, { id: 'foo', owner_id: 'a', name: 'aaa' })).toStrictEqual([
        '1#stream|0["a"]',
        '1#stream|1[]'
      ]);

      expect(
        await queryBucketIds(desc, {
          token: {
            sub: 'u1'
          }
        })
      ).toStrictEqual(['1#stream|0["u1"]', '1#stream|1[]']);
    });

    test('row condition or parameter condition', async () => {
      const desc = parseStream("SELECT * FROM comments WHERE LENGTH(content) > 5 OR auth.parameter('is_admin')");

      expect(evaluateBucketIds(desc, COMMENTS, { id: 'foo', content: 'short' })).toStrictEqual(['1#stream|1[]']);
      expect(evaluateBucketIds(desc, COMMENTS, { id: 'foo', content: 'longer' })).toStrictEqual([
        '1#stream|0[]',
        '1#stream|1[]'
      ]);

      expect(
        await queryBucketIds(desc, {
          token: {
            is_admin: false
          }
        })
      ).toStrictEqual(['1#stream|0[]']);
      expect(
        await queryBucketIds(desc, {
          token: {
            is_admin: true
          }
        })
      ).toStrictEqual(['1#stream|0[]', '1#stream|1[]']);
    });

    test('row condition or row condition', () => {
      const desc = parseStream(
        'SELECT * FROM comments WHERE LENGTH(content) > 5 OR json_array_length(tagged_users) > 1'
      );
      // Complex conditions that only operate on row data don't need variants.
      expect(desc.variants).toHaveLength(1);

      expect(evaluateBucketIds(desc, COMMENTS, { id: 'foo', content: 'short', tagged_users: '[]' })).toStrictEqual([]);
      expect(evaluateBucketIds(desc, COMMENTS, { id: 'foo', content: 'longer', tagged_users: '[]' })).toStrictEqual([
        '1#stream|0[]'
      ]);
      expect(
        evaluateBucketIds(desc, COMMENTS, { id: 'foo', content: 'longer', tagged_users: '["a","b"]' })
      ).toStrictEqual(['1#stream|0[]']);
      expect(
        evaluateBucketIds(desc, COMMENTS, { id: 'foo', content: 'short', tagged_users: '["a","b"]' })
      ).toStrictEqual(['1#stream|0[]']);
    });

    test('request condition or request condition', async () => {
      const desc = parseStream("SELECT * FROM comments WHERE auth.parameter('a') OR auth.parameters() ->> 'b'");
      // Complex conditions that only operate on request data don't need variants.
      expect(desc.variants).toHaveLength(1);

      expect(evaluateBucketIds(desc, COMMENTS, { id: 'foo', content: 'whatever]' })).toStrictEqual(['1#stream|0[]']);
      expect(
        await queryBucketIds(desc, {
          token: {
            a: false,
            b: false
          }
        })
      ).toStrictEqual([]);
      expect(
        await queryBucketIds(desc, {
          token: {
            a: true,
            b: false
          }
        })
      ).toStrictEqual(['1#stream|0[]']);
    });

    test('subquery or token parameter', async () => {
      const desc = parseStream(
        "SELECT * FROM comments WHERE issue_id IN (SELECT id FROM issues WHERE owner_id = auth.user_id()) OR auth.parameter('is_admin')"
      );

      expect(evaluateBucketIds(desc, COMMENTS, { id: 'c', issue_id: 'i1' })).toStrictEqual([
        '1#stream|0["i1"]',
        '1#stream|1[]'
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
      expect(await queryBucketIds(desc, { token: { sub: 'u1', is_admin: false }, getParameterSets })).toStrictEqual([
        '1#stream|0["i1"]'
      ]);
      expect(await queryBucketIds(desc, { token: { sub: 'u1', is_admin: true }, getParameterSets })).toStrictEqual([
        '1#stream|1[]',
        '1#stream|0["i1"]'
      ]);
    });
  });

  describe('in', () => {
    test('row value in subquery', async () => {
      const desc = parseStream(
        'SELECT * FROM comments WHERE issue_id IN (SELECT id FROM issues WHERE owner_id = auth.user_id())'
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
        '1#stream|0["issue_id"]'
      ]);

      expect(
        await queryBucketIds(desc, {
          token: { sub: 'user1' },
          getParameterSets(lookups) {
            expect(lookups).toStrictEqual([ParameterLookup.normalized('stream', '0', ['user1'])]);

            return [{ result: 'issue_id' }];
          }
        })
      ).toStrictEqual(['1#stream|0["issue_id"]']);
    });

    test('parameter value in subquery', async () => {
      const desc = parseStream('SELECT * FROM issues WHERE auth.user_id() IN (SELECT id FROM users WHERE is_admin)');

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
          token: { sub: 'u' },
          getParameterSets: (lookups: ParameterLookup[]) => {
            expect(lookups).toStrictEqual([ParameterLookup.normalized('stream', '0', ['u'])]);
            return [{ result: 'u' }];
          }
        })
      ).toStrictEqual(['1#stream|0[]']);

      // And not for others
      expect(
        await queryBucketIds(desc, {
          token: { sub: 'u2' },
          getParameterSets: (lookups: ParameterLookup[]) => {
            expect(lookups).toStrictEqual([ParameterLookup.normalized('stream', '0', ['u2'])]);
            return [];
          }
        })
      ).toStrictEqual([]);
    });

    test('two subqueries', async () => {
      const desc = parseStream(`SELECT * FROM users WHERE
            id IN (SELECT user_a FROM friends WHERE user_b = auth.user_id()) OR
            id IN (SELECT user_b FROM friends WHERE user_a = auth.user_id())
        `);

      expect(evaluateBucketIds(desc, USERS, { id: 'a', name: 'a' })).toStrictEqual([
        '1#stream|0["a"]',
        '1#stream|1["a"]'
      ]);

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
          token: { sub: 'a' },
          getParameterSets
        })
      ).toStrictEqual(['1#stream|1["b"]']);
    });

    test('on parameter data', async () => {
      const desc = parseStream("SELECT * FROM comments WHERE issue_id IN (subscription.parameters() -> 'issue_id')");

      expect(evaluateBucketIds(desc, COMMENTS, { id: 'a', issue_id: 'i' })).toStrictEqual(['1#stream|0["i"]']);
      expect(
        await queryBucketIds(desc, {
          token: { sub: 'a' },
          parameters: { issue_id: ['i1', 'i2'] }
        })
      ).toStrictEqual(['1#stream|0["i1"]', '1#stream|0["i2"]']);
    });

    test('on parameter data and table', async () => {
      const desc = parseStream(
        "SELECT * FROM comments WHERE issue_id IN (SELECT id FROM issues WHERE owner_id = auth.user_id()) AND label IN (subscription.parameters() -> 'labels')"
      );

      expect(evaluateBucketIds(desc, COMMENTS, { id: 'a', issue_id: 'i', label: 'l' })).toStrictEqual([
        '1#stream|0["i","l"]'
      ]);
      expect(
        await queryBucketIds(desc, {
          token: { sub: 'a' },
          parameters: { labels: ['l1', 'l2'] },
          getParameterSets(lookups) {
            expect(lookups).toHaveLength(1);
            const [lookup] = lookups;
            expect(lookup).toStrictEqual(ParameterLookup.normalized('stream', '0', ['a']));
            return [{ result: 'i1' }, { result: 'i2' }];
          }
        })
      ).toStrictEqual([
        '1#stream|0["i1","l1"]',
        '1#stream|0["i1","l2"]',
        '1#stream|0["i2","l1"]',
        '1#stream|0["i2","l2"]'
      ]);
    });
  });

  describe('overlap', () => {
    test('row value in subquery', async () => {
      const desc = parseStream(
        'SELECT * FROM comments WHERE tagged_users && (SELECT user_a FROM friends WHERE user_b = auth.user_id())'
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
        '1#stream|0["a"]',
        '1#stream|0["b"]'
      ]);

      expect(
        await queryBucketIds(desc, {
          token: { sub: 'user1' },
          getParameterSets(lookups) {
            expect(lookups).toStrictEqual([ParameterLookup.normalized('stream', '0', ['user1'])]);

            return [{ result: 'issue_id' }];
          }
        })
      ).toStrictEqual(['1#stream|0["issue_id"]']);
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
        'select * from comments where issue_id not in (select id from issues where owner_id = auth.user_id())',
        options
      );

      expect(errors).toMatchObject([
        expect.toBeSqlRuleError(
          'Negations are not allowed here',
          'issue_id not in (select id from issues where owner_id = auth.user_id()'
        )
      ]);
    });

    test('negated subquery from outer not operator', () => {
      const [_, errors] = syncStreamFromSql(
        's',
        'select * from comments where not (issue_id in (select id from issues where owner_id = auth.user_id()))',
        options
      );

      expect(errors).toMatchObject([
        expect.toBeSqlRuleError(
          'Negations are not allowed here',
          'not (issue_id in (select id from issues where owner_id = auth.user_id()'
        )
      ]);
    });

    test('subquery with two columns', () => {
      const [_, errors] = syncStreamFromSql(
        's',
        'select * from comments where issue_id in (select id, owner_id from issues where owner_id = auth.user_id())',
        options
      );

      expect(errors).toMatchObject([
        expect.toBeSqlRuleError(
          'This subquery must return exactly one column',
          'select id, owner_id from issues where owner_id = auth.user_id()'
        )
      ]);
    });

    test('legacy request function', () => {
      const [_, errors] = syncStreamFromSql('s', 'select * from issues where owner_id = request.user_id()', options);

      expect(errors).toMatchObject([
        expect.toBeSqlRuleError("Function 'request.user_id' is not defined", 'request.user_id()')
      ]);
    });
  });

  describe('normalization', () => {
    test('double negation', async () => {
      const desc = parseStream(
        'select * from comments where NOT (issue_id not in (select id from issues where owner_id = auth.user_id()))'
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
        '1#stream|0["issue_id"]'
      ]);

      expect(
        await queryBucketIds(desc, {
          token: { sub: 'user1' },
          getParameterSets(lookups) {
            expect(lookups).toStrictEqual([ParameterLookup.normalized('stream', '0', ['user1'])]);

            return [{ result: 'issue_id' }];
          }
        })
      ).toStrictEqual(['1#stream|0["issue_id"]']);
    });

    test('negated or', () => {
      const desc = parseStream(
        'select * from comments where not (length(content) = 5 OR issue_id not in (select id from issues where owner_id = auth.user_id()))'
      );

      expect(evaluateBucketIds(desc, COMMENTS, { id: 'c', issue_id: 'issue_id', content: 'foo' })).toStrictEqual([
        '1#stream|0["issue_id"]'
      ]);
      expect(evaluateBucketIds(desc, COMMENTS, { id: 'c', issue_id: 'issue_id', content: 'foooo' })).toStrictEqual([]);
    });

    test('negated and', () => {
      const desc = parseStream(
        'select * from comments where not (length(content) = 5 AND issue_id not in (select id from issues where owner_id = auth.user_id()))'
      );
      expect(desc.variants).toHaveLength(2);

      expect(evaluateBucketIds(desc, COMMENTS, { id: 'c', issue_id: 'issue_id', content: 'foo' })).toStrictEqual([
        '1#stream|0[]',
        '1#stream|1["issue_id"]'
      ]);
      expect(evaluateBucketIds(desc, COMMENTS, { id: 'c', issue_id: 'issue_id', content: 'foooo' })).toStrictEqual([
        '1#stream|1["issue_id"]'
      ]);
    });

    test('distribute and', async () => {
      const desc = parseStream(
        `select * from comments where 
          (issue_id in (select id from issues where owner_id = auth.user_id())
            OR auth.parameter('is_admin'))
          AND
          LENGTH(content) > 2
        `
      );
      expect(desc.variants).toHaveLength(2);

      expect(evaluateBucketIds(desc, COMMENTS, { id: 'c', issue_id: 'issue_id', content: 'a' })).toStrictEqual([]);
      expect(evaluateBucketIds(desc, COMMENTS, { id: 'c', issue_id: 'issue_id', content: 'aaa' })).toStrictEqual([
        '1#stream|0["issue_id"]',
        '1#stream|1[]'
      ]);

      expect(
        await queryBucketIds(desc, {
          token: { sub: 'user1' },
          getParameterSets(lookups) {
            expect(lookups).toStrictEqual([ParameterLookup.normalized('stream', '0', ['user1'])]);

            return [{ result: 'issue_id' }];
          }
        })
      ).toStrictEqual(['1#stream|0["issue_id"]']);
      expect(
        await queryBucketIds(desc, {
          token: { sub: 'user1', is_admin: true },
          getParameterSets(lookups) {
            expect(lookups).toStrictEqual([ParameterLookup.normalized('stream', '0', ['user1'])]);

            return [{ result: 'issue_id' }];
          }
        })
      ).toStrictEqual(['1#stream|1[]', '1#stream|0["issue_id"]']);
    });
  });

  describe('regression tests', () => {
    test('table alias', async () => {
      // Regression test for https://discord.com/channels/1138230179878154300/1422138173907144724/1427962895425208382
      const accountMember = new TestSourceTable('account_member');
      const schema = new StaticSchema([
        {
          tag: DEFAULT_TAG,
          schemas: [
            {
              name: 'test_schema',
              tables: [
                {
                  name: 'account_member',
                  columns: [
                    { name: 'id', pg_type: 'uuid' },
                    { name: 'account_id', pg_type: 'uuid' }
                  ]
                }
              ]
            }
          ]
        }
      ]);

      const stream = parseStream(
        'select * from account_member as "outer" where account_id in (select "inner".account_id from account_member as "inner" where "inner".id = auth.user_id())',
        'account_member',
        { ...options, schema }
      );
      const row = { id: 'id', account_id: 'account_id' };

      expect(stream.tableSyncsData(accountMember)).toBeTruthy();
      expect(stream.tableSyncsParameters(accountMember)).toBeTruthy();

      // Ensure lookup steps work.
      expect(stream.evaluateParameterRow(accountMember, row)).toStrictEqual([
        {
          lookup: ParameterLookup.normalized('account_member', '0', ['id']),
          bucketParameters: [
            {
              result: 'account_id'
            }
          ]
        }
      ]);
      expect(evaluateBucketIds(stream, accountMember, row)).toStrictEqual(['1#account_member|0["account_id"]']);
      expect(
        await queryBucketIds(stream, {
          token: { sub: 'id' },
          parameters: {},
          getParameterSets(lookups) {
            expect(lookups).toStrictEqual([ParameterLookup.normalized('account_member', '0', ['id'])]);
            return [{ result: 'account_id' }];
          }
        })
      ).toStrictEqual(['1#account_member|0["account_id"]']);

      // And that the data alias is respected for generated schemas.
      const outputSchema = {};
      stream.resolveResultSets(schema, outputSchema);
      expect(Object.keys(outputSchema)).toStrictEqual(['outer']);
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
              { name: 'tagged_users', pg_type: 'text' },
              { name: 'label', pg_type: 'text' }
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

const options: StreamParseOptions = {
  schema: schema,
  ...PARSE_OPTIONS,
  compatibility: new CompatibilityContext(CompatibilityEdition.SYNC_STREAMS)
};

const bucketIdTransformer = SqlSyncRules.versionedBucketIdTransformer('1');

function evaluateBucketIds(stream: SyncStream, sourceTable: SourceTableInterface, record: SqliteRow) {
  return stream.evaluateRow({ sourceTable, record, bucketIdTransformer }).map((r) => {
    if ('error' in r) {
      throw new Error(`Unexpected error evaluating row: ${r.error}`);
    }

    return r.bucket;
  });
}

async function createQueriers(
  stream: SyncStream,
  options?: {
    token?: Record<string, any>;
    parameters?: Record<string, any>;
    getParameterSets?: (lookups: ParameterLookup[]) => SqliteJsonRow[];
  }
): Promise<GetBucketParameterQuerierResult> {
  const queriers: BucketParameterQuerier[] = [];
  const errors: QuerierError[] = [];
  const pending = { queriers, errors };

  const querierOptions: GetQuerierOptions = {
    hasDefaultStreams: true,
    globalParameters: new RequestParameters(
      {
        sub: 'test-user',
        ...options?.token
      },
      {}
    ),
    streams: { [stream.name]: [{ opaque_id: 0, parameters: options?.parameters ?? null }] },
    bucketIdTransformer
  };

  stream.pushBucketParameterQueriers(pending, querierOptions);

  return { querier: mergeBucketParameterQueriers(queriers), errors };
}

async function queryBucketIds(
  stream: SyncStream,
  options?: {
    token?: Record<string, any>;
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

function parseStream(sql: string, name = 'stream', parseOptions: StreamParseOptions = options) {
  const [stream, errors] = syncStreamFromSql(name, sql, parseOptions);
  if (errors.length) {
    throw new Error(`Unexpected errors when parsing stream ${sql}: ${errors}`);
  }

  return stream;
}
