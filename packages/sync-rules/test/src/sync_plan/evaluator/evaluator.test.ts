import { describe, expect } from 'vitest';
import { syncTest } from './utils.js';
import {
  HydratedSyncRules,
  RequestParameters,
  ScopedParameterLookup,
  SourceTableInterface,
  SqliteJsonRow,
  SqliteRow,
  SqliteValue
} from '../../../../src/index.js';
import { requestParameters, TestSourceTable } from '../../util.js';

describe('evaluating rows', () => {
  syncTest('emits rows', ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 2
  sync_config_compiler: true
  
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

  syncTest('debugWriteOutputTables', ({ sync }) => {
    const desc = sync.prepareWithoutHydration(`
config:
  edition: 2
  sync_config_compiler: true
  
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
  edition: 2
  sync_config_compiler: true
  
streams:
  stream:
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
  edition: 2
  sync_config_compiler: true
  
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
  edition: 2
  sync_config_compiler: true
  
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
  edition: 2
  sync_config_compiler: true
  
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
  edition: 2
  sync_config_compiler: true
  
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
  edition: 2
  sync_config_compiler: true
  
streams:
  stream:
      query: SELECT * FROM comments WHERE issue_id IN (SELECT id FROM issues WHERE owner_id = auth.user_id())
`);

    expect(desc.tableSyncsData(COMMENTS)).toBeTruthy();
    expect(desc.tableSyncsData(ISSUES)).toBeFalsy();
    expect(desc.tableSyncsParameters(ISSUES)).toBeTruthy();

    expect(desc.evaluateParameterRow(ISSUES, { id: 'issue_id', owner_id: 'user1', name: 'name' })).toStrictEqual([
      {
        lookup: ScopedParameterLookup.direct({ lookupName: 'lookup', queryId: '0' }, ['user1']),
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
  edition: 2
  sync_config_compiler: true
  
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
});

describe('querier', () => {
  syncTest('static', ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 2
  sync_config_compiler: true
  
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

  syncTest('request data', ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 2
  sync_config_compiler: true
  
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
  edition: 2
  sync_config_compiler: true
  
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
          expect(lookups).toStrictEqual([
            ScopedParameterLookup.direct(
              {
                lookupName: 'lookup',
                queryId: '0'
              },
              ['user']
            )
          ]);
          return [{ '0': 'name' }];
        } else if (call == 1) {
          // Second call. Lookup from issues.owned_by => issues.id
          call++;
          expect(lookups).toStrictEqual([
            ScopedParameterLookup.direct(
              {
                lookupName: 'lookup',
                queryId: '1'
              },
              ['name']
            )
          ]);
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
  edition: 2
  sync_config_compiler: true

streams:
  stream:
    auto_subscribe: true
    with:
      a: SELECT value FROM json_each(auth.parameter('a'))
      b: SELECT value FROM json_each(auth.parameter('b'))
    query: SELECT notes.* FROM notes, a, b WHERE notes.state = a.value AND notes.other = b.value
`);

    const { querier, errors } = desc.getBucketParameterQuerier({
      globalParameters: new RequestParameters({ sub: 'user', a: ['a1', 'a2'], b: ['b1', 'b2'] }, {}),
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
});

function evaluateBucketIds(source: HydratedSyncRules, sourceTable: SourceTableInterface, record: SqliteRow) {
  return source.evaluateRow({ sourceTable, record }).map((r) => r.bucket);
}

const USERS = new TestSourceTable('users');
const COMMENTS = new TestSourceTable('comments');
const ISSUES = new TestSourceTable('issues');
