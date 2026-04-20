import { describe, expect } from 'vitest';
import {
  HydratedSyncRules,
  ScopedParameterLookup,
  SourceTableInterface,
  SqliteJsonRow,
  SqliteRow,
  SqliteValue
} from '../../../../src/index.js';
import { lookupScope, requestParameters, TestSourceTable } from '../../util.js';
import { syncTest } from './utils.js';

describe('evaluating rows', () => {
  syncTest('emits rows', ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 3
  
streams:
  stream:
      query: SELECT * FROM users
`);

    expect(
      desc.evaluateRow({
        sourceTable: USERS,
        record: {
          id: 'foo',
          _double: 1,
          _int: 1n,
          _null: null,
          _text: 'text',
          _blob: new Uint8Array(10) // non-JSON columns should be removed
        }
      })
    ).toStrictEqual([
      {
        bucket: 'stream|0[]',
        id: 'foo',
        data: { id: 'foo', _double: 1, _int: 1n, _null: null, _text: 'text' },
        table: 'users'
      }
    ]);
  });

  syncTest('NOT IN', ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 3

streams:
  stream:
    auto_subscribe: true
    query: SELECT * FROM notes WHERE state NOT IN '["deleted", "archived"]'
`);

    const notes = new TestSourceTable('notes');
    expect(desc.evaluateRow({ sourceTable: notes, record: { id: 'id', state: 'public' } })).toHaveLength(1);
    expect(desc.evaluateRow({ sourceTable: notes, record: { id: 'id', state: 'deleted' } })).toHaveLength(0);
  });

  syncTest('debugWriteOutputTables', ({ sync }) => {
    const desc = sync.prepareWithoutHydration(`
config:
  edition: 3
  
streams:
  stream:
      queries:
        - SELECT * FROM users
        - SELECT * FROM notes WHERE owner = auth.user_id() AND length(content) > 10
`);

    // This output is arguably not particularly helpful, but it's only used for debugging purposes and it provides some
    // insights into how the stream has been turned into a scalar query.
    expect(desc.debugGetOutputTables()).toStrictEqual({
      users: [{ query: 'SELECT 1' }],
      notes: [{ query: 'SELECT ?2 WHERE "length"(?1) > 10' }]
    });
  });

  syncTest('forwards parameters', ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 3
  
streams:
  stream:
      accept_potentially_dangerous_queries: true
      query: SELECT * FROM users WHERE value = subscription.parameter('p')
`);

    function evaluate(value: SqliteValue) {
      const rows = desc.evaluateRow({ sourceTable: USERS, record: { id: 'foo', value } });
      if (rows.length == 0) {
        return undefined;
      }

      return rows[0].bucket;
    }

    expect(evaluate(1)).toStrictEqual('stream|0[1]');
    expect(evaluate(1n)).toStrictEqual('stream|0[1]');
    expect(evaluate(1.1)).toStrictEqual('stream|0[1.1]');
    expect(evaluate('1')).toStrictEqual('stream|0["1"]');

    // null is not equal to itself, so WHERE null = subscription.paraeter('p') should not match any rows.
    expect(evaluate(null)).toStrictEqual(undefined);

    // We can't store binary values in bucket parameters
    expect(evaluate(new Uint8Array(10))).toStrictEqual(undefined);
  });

  syncTest('output table name', ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 3
  
streams:
  stream:
      query: SELECT * FROM users u
`);
    expect(
      desc.evaluateRow({
        sourceTable: USERS,
        record: {
          id: 'foo'
        }
      })
    ).toStrictEqual([
      {
        bucket: 'stream|0[]',
        id: 'foo',
        data: { id: 'foo' },
        table: 'u'
      }
    ]);
  });

  syncTest('wildcard with alias', ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 3
  
streams:
  stream:
      query: SELECT * FROM "%" output
`);
    expect(
      desc.evaluateRow({
        sourceTable: USERS,
        record: {
          id: 'foo'
        }
      })
    ).toStrictEqual([
      {
        bucket: 'stream|0[]',
        id: 'foo',
        data: { id: 'foo' },
        table: 'output'
      }
    ]);
  });

  syncTest('wildcard without alias', ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 3
  
streams:
  stream:
      query: SELECT * FROM "%"
`);
    expect(
      desc.evaluateRow({
        sourceTable: USERS,
        record: {
          id: 'foo'
        }
      })
    ).toStrictEqual([
      {
        bucket: 'stream|0[]',
        id: 'foo',
        data: { id: 'foo' },
        table: 'users'
      }
    ]);
  });

  syncTest('multiple tables in bucket', ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 3
  
streams:
  stream:
      queries:
        - SELECT * FROM users
        - SELECT * FROM comments
`);
    expect(evaluateBucketIds(desc, USERS, { id: 'foo' })).toStrictEqual(['stream|0[]']);
    expect(evaluateBucketIds(desc, COMMENTS, { id: 'foo2' })).toStrictEqual(['stream|0[]']);
  });
});

describe('evaluating parameters', () => {
  syncTest('emits parameters', ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 3
  
streams:
  stream:
      query: SELECT * FROM comments WHERE issue_id IN (SELECT id FROM issues WHERE owner_id = auth.user_id())
`);

    expect(desc.tableSyncsData(COMMENTS)).toBeTruthy();
    expect(desc.tableSyncsData(ISSUES)).toBeFalsy();
    expect(desc.tableSyncsParameters(ISSUES)).toBeTruthy();

    expect(desc.evaluateParameterRow(ISSUES, { id: 'issue_id', owner_id: 'user1', name: 'name' })).toStrictEqual([
      {
        lookup: ScopedParameterLookup.direct(lookupScope('lookup', '0'), ['user1']),
        bucketParameters: [
          {
            '0': 'issue_id'
          }
        ]
      }
    ]);
  });

  syncTest('skips null and binary values', ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 3
  
streams:
  stream:
      auto_subscribe: true
      query: SELECT * FROM comments WHERE issue_id IN (SELECT id FROM issues WHERE owner_id = auth.user_id())
`);
    const blob = new Uint8Array(10);

    expect(desc.evaluateParameterRow(ISSUES, { id: 'issue_id', owner_id: 'user1' })).toHaveLength(1);
    expect(desc.evaluateParameterRow(ISSUES, { id: 'issue_id', owner_id: null })).toHaveLength(0);
    expect(desc.evaluateParameterRow(ISSUES, { id: null, owner_id: 'user1' })).toHaveLength(0);

    expect(desc.evaluateParameterRow(ISSUES, { id: 'issue_id', owner_id: blob })).toHaveLength(0);
    expect(desc.evaluateParameterRow(ISSUES, { id: blob, owner_id: 'user1' })).toHaveLength(0);
  });

  syncTest('respects filters', ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 3
  
streams:
  stream:
      auto_subscribe: true
      accept_potentially_dangerous_queries: true
      query: SELECT users.* FROM users, orgs WHERE users.org_id = orgs.id AND orgs.name = subscription.parameter('org') AND orgs.is_active = 1
`);
    const orgs = new TestSourceTable('orgs');

    const active = desc.evaluateParameterRow(orgs, { id: 'a', name: 'org-a', is_active: 1 });
    const inactive = desc.evaluateParameterRow(orgs, { id: 'b', name: 'org-b', is_active: 0 });

    expect(active.length).toBe(1);
    expect(inactive.length).toBe(0);
  });
});

describe('querier', () => {
  syncTest('tracks source metadata on stream APIs', async ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 3

streams:
  stream:
      accept_potentially_dangerous_queries: true
      queries:
        - SELECT * FROM comments WHERE issue_id = subscription.parameter('issue')
        - SELECT * FROM comments WHERE issue_id IN (SELECT id FROM issues WHERE owner_id = auth.user_id())
`);
    const streamSource = desc.definition.bucketSources[0];
    expect(streamSource.dataSources).toHaveLength(2);

    const rowResults = desc.evaluateRow({ sourceTable: COMMENTS, record: { id: 'c1', issue_id: 'i1' } });
    expect(rowResults).toHaveLength(1);
    expect(rowResults[0].bucket).toBe('stream|0["i1"]');
    expect(rowResults[0].source).toBe(streamSource.dataSources[0]);

    expect(desc.definition.bucketParameterLookupSources).toHaveLength(1);
    const parameterResults = desc.evaluateParameterRow(ISSUES, { id: 'i1', owner_id: 'u1' });
    expect(parameterResults).toHaveLength(1);
    expect(parameterResults[0].lookup.source).toBe(desc.definition.bucketParameterLookupSources[0]);

    const { querier, errors } = desc.getBucketParameterQuerier({
      globalParameters: requestParameters({ sub: 'u1' }),
      hasDefaultStreams: false,
      streams: {
        stream: [{ priorityOverride: null, opaque_id: 0, parameters: { issue: 'i1' } }]
      }
    });
    expect(errors).toHaveLength(0);
    expect(querier.staticBuckets).toHaveLength(1);
    expect(querier.staticBuckets[0].source).toBe(streamSource.dataSources[0]);

    const dynamicBuckets = await querier.queryDynamicBucketDescriptions({
      async getParameterSets() {
        return [{ '0': 'i1' }];
      }
    });
    expect(dynamicBuckets).toHaveLength(1);
    expect(dynamicBuckets[0].source).toBe(streamSource.dataSources[1]);
  });

  syncTest('static', ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 3
  
streams:
  stream:
      auto_subscribe: true
      query: SELECT * FROM issues WHERE is_public
`);

    const { querier } = desc.getBucketParameterQuerier({
      globalParameters: requestParameters(
        {
          sub: 'user'
        },
        {}
      ),
      hasDefaultStreams: true,
      streams: {}
    });

    expect(querier.staticBuckets.map((e) => e.bucket)).toStrictEqual(['stream|0[]']);
  });

  syncTest('static request filter', ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 3
  
streams:
  stream:
      auto_subscribe: true
      query: SELECT * FROM issues WHERE auth.parameter('is_admin')
`);

    {
      const { querier, errors } = desc.getBucketParameterQuerier({
        globalParameters: requestParameters({ sub: 'user' }),
        hasDefaultStreams: true,
        streams: {}
      });
      expect(errors).toStrictEqual([]);
      expect(querier.staticBuckets).toStrictEqual([]);
    }
    {
      const { querier, errors } = desc.getBucketParameterQuerier({
        globalParameters: requestParameters({ sub: 'user', is_admin: true }),
        hasDefaultStreams: true,
        streams: {}
      });
      expect(errors).toStrictEqual([]);
      expect(querier.staticBuckets).toHaveLength(1);
    }
  });

  syncTest('request data', ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 3
  
streams:
  stream:
      auto_subscribe: true
      query: SELECT * FROM issues WHERE owner = auth.user_id()
`);

    const { querier, errors } = desc.getBucketParameterQuerier({
      globalParameters: requestParameters({ sub: 'user' }),
      hasDefaultStreams: true,
      streams: {}
    });
    expect(errors).toStrictEqual([]);

    expect(querier.staticBuckets.map((e) => e.bucket)).toStrictEqual(['stream|0["user"]']);
  });

  syncTest('parameter lookups', async ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 3
  
streams:
  stream:
      auto_subscribe: true
      query: |
        SELECT c.* FROM comments c
          INNER JOIN issues i ON c.issue = i.id
          INNER JOIN users owner ON owner.name = i.owned_by
        WHERE owner.id = auth.user_id()
`);

    const { querier, errors } = desc.getBucketParameterQuerier({
      globalParameters: requestParameters({ sub: 'user' }),
      hasDefaultStreams: true,
      streams: {}
    });
    expect(errors).toStrictEqual([]);

    expect(querier.staticBuckets.map((e) => e.bucket)).toStrictEqual([]);
    let call = 0;
    const buckets = await querier.queryDynamicBucketDescriptions({
      getParameterSets: async function (lookups: ScopedParameterLookup[]): Promise<SqliteJsonRow[]> {
        if (call == 0) {
          // First call. Lookup from users.id => users.name
          call++;
          expect(lookups).toStrictEqual([ScopedParameterLookup.direct(lookupScope('lookup', '0'), ['user'])]);
          return [{ '0': 'name' }];
        } else if (call == 1) {
          // Second call. Lookup from issues.owned_by => issues.id
          call++;
          expect(lookups).toStrictEqual([ScopedParameterLookup.direct(lookupScope('lookup', '1'), ['name'])]);
          return [{ '0': 'issue' }];
        }

        throw new Error('Function not implemented.');
      }
    });
    expect(buckets.map((b) => b.bucket)).toStrictEqual(['stream|0["issue"]']);
  });

  syncTest('multiple IN operators', ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 3

streams:
  stream:
    auto_subscribe: true
    with:
      a: SELECT value FROM json_each(auth.parameter('a'))
      b: SELECT value FROM json_each(auth.parameter('b'))
    query: SELECT notes.* FROM notes, a, b WHERE notes.state = a.value AND notes.other = b.value
`);

    const { querier, errors } = desc.getBucketParameterQuerier({
      globalParameters: requestParameters({ sub: 'user', a: ['a1', 'a2'], b: ['b1', 'b2'] }, {}),
      hasDefaultStreams: true,
      streams: {}
    });
    expect(errors).toStrictEqual([]);

    expect(querier.staticBuckets.map((e) => e.bucket)).toStrictEqual([
      'stream|0["a1","b1"]',
      'stream|0["a1","b2"]',
      'stream|0["a2","b1"]',
      'stream|0["a2","b2"]'
    ]);
  });

  describe('expanding request conditions', () => {
    syncTest('based on parameter', async ({ sync }) => {
      const desc = sync.prepareSyncStreams(`
config:
  edition: 3

streams:
  stream:
    auto_subscribe: true
    query: SELECT * FROM posts WHERE 'posts' IN (SELECT table_name FROM synced_table WHERE "user" = auth.user_id())
`);

      const { querier, errors } = desc.getBucketParameterQuerier({
        globalParameters: requestParameters({ sub: 'user' }, {}),
        hasDefaultStreams: true,
        streams: {}
      });
      expect(errors).toStrictEqual([]);
      expect(querier.staticBuckets).toStrictEqual([]);

      // Should not return any streams if the synced_table lookup is empty.
      expect(
        await querier.queryDynamicBucketDescriptions({
          getParameterSets: async function (lookups: ScopedParameterLookup[]): Promise<SqliteJsonRow[]> {
            expect(lookups).toStrictEqual([ScopedParameterLookup.direct(lookupScope('lookup', '0'), ['user'])]);
            return [];
          }
        })
      ).toStrictEqual([]);

      expect(
        await querier.queryDynamicBucketDescriptions({
          getParameterSets: async function (lookups: ScopedParameterLookup[]): Promise<SqliteJsonRow[]> {
            expect(lookups).toStrictEqual([ScopedParameterLookup.direct(lookupScope('lookup', '0'), ['user'])]);
            return [{}];
          }
        })
      ).toStrictEqual([
        {
          bucket: 'stream|0[]',
          definition: 'stream',
          inclusion_reasons: ['default'],
          priority: 3
        }
      ]);
    });

    syncTest('based on static filter', async ({ sync }) => {
      const desc = sync.prepareSyncStreams(`
config:
  edition: 3

streams:
  stream:
    auto_subscribe: true
    query: SELECT * FROM posts WHERE 'posts' IN auth.parameter('synced_objects')
`);

      {
        const { querier, errors } = desc.getBucketParameterQuerier({
          globalParameters: requestParameters({ sub: 'user', synced_objects: ['another_table'] }, {}),
          hasDefaultStreams: true,
          streams: {}
        });
        expect(errors).toStrictEqual([]);
        expect(querier.staticBuckets).toStrictEqual([]);
      }
      {
        const { querier, errors } = desc.getBucketParameterQuerier({
          globalParameters: requestParameters({ sub: 'user', synced_objects: ['another_table', 'posts'] }, {}),
          hasDefaultStreams: true,
          streams: {}
        });
        expect(errors).toStrictEqual([]);
        expect(querier.staticBuckets).toHaveLength(1);
      }
    });

    syncTest('skips dynamic lookup if static lookup makes graph uninstantiable', async ({ sync }) => {
      const desc = sync.prepareSyncStreams(`
config:
  edition: 3

streams:
  stream:
    auto_subscribe: true
    query: SELECT * FROM posts WHERE id IN (SELECT post FROM owned_posts WHERE owner = auth.user_id()) AND 'posts' IN auth.parameter('synced_objects')
`);

      {
        // second AND is known to be false from token parameter, so skip any lookups.
        const { querier, errors } = desc.getBucketParameterQuerier({
          globalParameters: requestParameters({ sub: 'user', synced_objects: ['another_table'] }, {}),
          hasDefaultStreams: true,
          streams: {}
        });
        expect(errors).toStrictEqual([]);
        expect(querier.staticBuckets).toStrictEqual([]);

        expect(querier.hasDynamicBuckets).toStrictEqual(false);
      }

      {
        const { querier, errors } = desc.getBucketParameterQuerier({
          globalParameters: requestParameters({ sub: 'user', synced_objects: ['another_table', 'posts'] }, {}),
          hasDefaultStreams: true,
          streams: {}
        });
        expect(errors).toStrictEqual([]);
        expect(querier.staticBuckets).toHaveLength(0);

        // Should request dynamic lookups to query left side of AND
        expect(querier.hasDynamicBuckets).toStrictEqual(true);

        for (const hasLookupResult of [false, true]) {
          expect(
            await querier.queryDynamicBucketDescriptions({
              getParameterSets: async function (lookups: ScopedParameterLookup[]): Promise<SqliteJsonRow[]> {
                expect(lookups).toStrictEqual([ScopedParameterLookup.direct(lookupScope('lookup', '0'), ['user'])]);
                return hasLookupResult ? [{}] : [];
              }
            })
          ).toHaveLength(hasLookupResult ? 1 : 0);
        }
      }
    });

    syncTest('multiple references', async ({ sync }) => {
      const desc = sync.prepareSyncStreams(`
config:
  edition: 3

streams:
  stream:
    auto_subscribe: true
    query: SELECT a.* FROM a, b, c, d WHERE a.c1 = b.c1 AND b.c2 = c.c2 AND c.c3 = d.c3 AND d.c4 = a.c4
`);

      const data = { id: 'foo', c1: 'c1', c2: 'c2', c3: 'c3', c4: 'c4' };

      expect(
        desc
          .evaluateRow({
            sourceTable: new TestSourceTable('a'),
            record: data
          })
          .map((r) => r.bucket)
        // Note that bucket parameters have an arbitrary order, but they must match querier outputs.
      ).toStrictEqual(['stream|0["c4","c1"]']);

      // Outputs of d are used directly (d.c4 = a.c4) and as an input to c (c.c3 = d.c3)
      expect(desc.evaluateParameterRow(new TestSourceTable('d'), data)[0]).toStrictEqual({
        bucketParameters: [{ '0': 'c3', '1': 'c4' }],
        lookup: ScopedParameterLookup.direct({ lookupName: 'lookup', queryId: '0', source: null as any }, [])
      });
      // Table c: Index from c3 to c2 for lookup in b
      expect(desc.evaluateParameterRow(new TestSourceTable('c'), data)[0]).toStrictEqual({
        bucketParameters: [{ '0': 'c2' }],
        lookup: ScopedParameterLookup.direct({ lookupName: 'lookup', queryId: '1', source: null as any }, ['c3'])
      });
      // Table b: Index from c2 to c1 for bucket parameter
      expect(desc.evaluateParameterRow(new TestSourceTable('b'), data)[0]).toStrictEqual({
        bucketParameters: [{ '0': 'c1' }],
        lookup: ScopedParameterLookup.direct({ lookupName: 'lookup', queryId: '2', source: null as any }, ['c2'])
      });

      const { querier, errors } = desc.getBucketParameterQuerier({
        globalParameters: requestParameters({}, {}),
        hasDefaultStreams: true,
        streams: {}
      });
      expect(errors).toStrictEqual([]);
      expect(querier.staticBuckets).toStrictEqual([]);

      expect(
        await querier.queryDynamicBucketDescriptions({
          getParameterSets: async function (lookups: ScopedParameterLookup[]): Promise<SqliteJsonRow[]> {
            for (const lookup of lookups) {
              expect(lookup.values[0]).toStrictEqual('lookup');
              switch (lookup.values[1]) {
                case '0':
                  return [{ '0': 'c3', '1': 'c4' }];
                case '1':
                  return [{ '0': 'c2' }];
                case '2':
                  return [{ '0': 'c1' }];
              }
            }

            return [];
          }
        })
      ).toStrictEqual([
        {
          bucket: 'stream|0["c4","c1"]',
          definition: 'stream',
          inclusion_reasons: ['default'],
          priority: 3
        }
      ]);
    });
  });
});

function evaluateBucketIds(source: HydratedSyncRules, sourceTable: SourceTableInterface, record: SqliteRow) {
  return source.evaluateRow({ sourceTable, record }).map((r) => r.bucket);
}

const USERS = new TestSourceTable('users');
const COMMENTS = new TestSourceTable('comments');
const ISSUES = new TestSourceTable('issues');
