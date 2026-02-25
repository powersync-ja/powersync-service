import { describe, expect } from 'vitest';
import { syncTest } from './utils.js';
import { requestParameters, TestSourceTable } from '../../util.js';
import {
  deserializeSyncPlan,
  ImplicitSchemaTablePattern,
  ScopedParameterLookup,
  serializeSyncPlan,
  SqliteJsonRow,
  StreamDataSource,
  TableProcessorTableValuedFunction
} from '../../../../src/index.js';
import { PreparedStreamBucketDataSource } from '../../../../src/sync_plan/evaluator/bucket_data_source.js';

describe('table-valued functions', () => {
  syncTest('as partition key', ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 3

streams:
  stream:
      accept_potentially_dangerous_queries: true
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
  edition: 3

streams:
  stream:
      accept_potentially_dangerous_queries: true
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
      globalParameters: requestParameters({ sub: 'user' }, {}),
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

  syncTest('filter on function output', ({ sync }) => {
    const desc = sync.prepareSyncStreams(`
config:
  edition: 3

streams:
  stream:
      query: |
        SELECT customers.id as id FROM customers, json_each(customers.active_regions) AS region WHERE region.value < 'm'
`);

    const customers = new TestSourceTable('customers');
    const rows = desc.evaluateRow({
      sourceTable: customers,
      record: { id: 'c1', active_regions: JSON.stringify(['us-east', 'eu-west']) }
    });

    expect(rows).toMatchObject([{ bucket: 'stream|0[]', data: { id: 'c1' }, id: 'c1', table: 'customers' }]);
  });

  syncTest('table-valued output in oplog data', ({ sync }) => {
    // The compiler currently doesn't support this, but the sync plan format can represent queries like
    // `SELECT products.id, expanded.value as region FROM products, json_each(region.regions) AS expanded`.
    const jsonEach: TableProcessorTableValuedFunction = {
      functionName: 'json_each',
      functionInputs: [{ type: 'data', source: { column: 'regions' } }],
      filters: []
    };
    const source: StreamDataSource = {
      outputTableName: 'products',
      hashCode: 0,
      sourceTable: new ImplicitSchemaTablePattern(null, 'products'),
      columns: [
        { alias: 'id', expr: { type: 'data', source: { column: 'id' } } },
        { alias: 'region', expr: { type: 'data', source: { function: jsonEach, outputName: 'value' } } }
      ],
      filters: [],
      parameters: [],
      tableValuedFunctions: [jsonEach]
    };
    const plan = deserializeSyncPlan(
      serializeSyncPlan({
        dataSources: [source],
        buckets: [{ hashCode: 0, uniqueName: 'a', sources: [source] }],
        parameterIndexes: [],
        streams: []
      })
    );

    const evaluator = new PreparedStreamBucketDataSource(plan.buckets[0], {
      defaultSchema: 'test_schema',
      engine: sync.engine,
      sourceText: ''
    });
    const products = new TestSourceTable('products');

    expect(
      evaluator.evaluateRow({
        sourceTable: products,
        record: {
          id: 'id',
          regions: '[]'
        }
      })
    ).toHaveLength(0);
    expect(
      evaluator.evaluateRow({
        sourceTable: products,
        record: {
          id: 'id',
          regions: '["foo", "bar"]'
        }
      })
    ).toEqual([
      expect.objectContaining({ data: { id: 'id', region: 'foo' } }),
      expect.objectContaining({ data: { id: 'id', region: 'bar' } })
    ]);
  });

  syncTest('filter on function output and source row', ({ sync }) => {
    // The compiler currently doesn't support it, but it should. `SELECT * FROM tasks WHERE status IN '["active", "pending"]'`.
    const jsonEach: TableProcessorTableValuedFunction = {
      functionName: 'json_each',
      functionInputs: [{ type: 'lit_string', value: `["active", "pending"]` }],
      filters: []
    };
    const source: StreamDataSource = {
      outputTableName: 'tasks',
      hashCode: 0,
      sourceTable: new ImplicitSchemaTablePattern(null, 'tasks'),
      columns: ['star'],
      filters: [
        {
          type: 'binary',
          operator: '=',
          left: { type: 'data', source: { column: 'status' } },
          right: { type: 'data', source: { function: jsonEach, outputName: 'value' } }
        }
      ],
      parameters: [],
      tableValuedFunctions: [jsonEach]
    };
    const plan = deserializeSyncPlan(
      serializeSyncPlan({
        dataSources: [source],
        buckets: [{ hashCode: 0, uniqueName: 'a', sources: [source] }],
        parameterIndexes: [],
        streams: []
      })
    );
    const evaluator = new PreparedStreamBucketDataSource(plan.buckets[0], {
      defaultSchema: 'test_schema',
      engine: sync.engine,
      sourceText: ''
    });
    const tasks = new TestSourceTable('tasks');
    expect(
      evaluator.evaluateRow({
        sourceTable: tasks,
        record: {
          id: 'id',
          status: 'archived'
        }
      })
    ).toHaveLength(0);
    expect(
      evaluator.evaluateRow({
        sourceTable: tasks,
        record: {
          id: 'id',
          status: 'active'
        }
      })
    ).toHaveLength(1);
    expect(
      evaluator.evaluateRow({
        sourceTable: tasks,
        record: {
          id: 'id',
          status: 'pending'
        }
      })
    ).toHaveLength(1);
  });
});
