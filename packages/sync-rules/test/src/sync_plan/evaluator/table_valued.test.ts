import { describe, expect } from 'vitest';
import { syncTest } from './utils.js';
import { TestSourceTable } from '../../util.js';
import { RequestParameters, ScopedParameterLookup, SqliteJsonRow } from '../../../../src/index.js';

describe('table-valued functions', () => {
  syncTest('as partition key', ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 2
  sync_config_compiler: true

streams:
  stream:
      query: SELECT s.id AS id FROM stores s INNER JOIN json_each(s.tags) as tags WHERE tags.value = subscription.parameter('tag')
`);

    const sourceTable = new TestSourceTable('stores');
    expect(desc.evaluateRow({ sourceTable, record: { id: 'id', tags: '[1,2,3]' } })).toStrictEqual(
      [1, 2, 3].map((e) => ({ bucket: `stream|0[${e}]`, data: { id: 'id' }, table: 's', id: 'id' }))
    );
  });

  syncTest('as parameter output', async ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 2
  sync_config_compiler: true

streams:
  stream:
      query: |
        SELECT users.* FROM users
          INNER JOIN conversations
          INNER JOIN json_each(conversations.members) AS members
        WHERE users.id = members.value AND conversations.id = subscription.parameter('chat')
`);

    const users = new TestSourceTable('users');
    const conversations = new TestSourceTable('conversations');

    expect(desc.evaluateRow({ sourceTable: users, record: { id: 'user' } }).map((e) => e.bucket)).toStrictEqual([
      'stream|0["user"]'
    ]);
    expect(
      desc.evaluateParameterRow(conversations, { id: 'chat', members: JSON.stringify(['user', 'another']) })
    ).toStrictEqual([
      {
        lookup: ScopedParameterLookup.direct({ lookupName: 'lookup', queryId: '0' }, ['chat']),
        bucketParameters: [
          {
            '0': 'user'
          }
        ]
      },
      {
        lookup: ScopedParameterLookup.direct({ lookupName: 'lookup', queryId: '0' }, ['chat']),
        bucketParameters: [
          {
            '0': 'another'
          }
        ]
      }
    ]);

    const { querier } = desc.getBucketParameterQuerier({
      globalParameters: new RequestParameters({ sub: 'user' }, {}),
      hasDefaultStreams: false,
      streams: {
        stream: [
          {
            parameters: { chat: 'chat' },
            opaque_id: 0
          }
        ]
      }
    });

    const buckets = await querier.queryDynamicBucketDescriptions({
      getParameterSets: async function (lookups: ScopedParameterLookup[]): Promise<SqliteJsonRow[]> {
        expect(lookups).toStrictEqual([
          ScopedParameterLookup.direct(
            {
              lookupName: 'lookup',
              queryId: '0'
            },
            ['chat']
          )
        ]);

        return [{ '0': 'user' }, { '0': 'another' }];
      }
    });

    expect(buckets.map((b) => b.bucket)).toStrictEqual(['stream|0["user"]', 'stream|0["another"]']);
  });
});
