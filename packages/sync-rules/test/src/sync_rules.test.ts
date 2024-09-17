import { describe, expect, test } from 'vitest';
import {
  DEFAULT_TAG,
  DartSchemaGenerator,
  JsLegacySchemaGenerator,
  SqlSyncRules,
  StaticSchema,
  TsSchemaGenerator
} from '../../src/index.js';

import { ASSETS, BASIC_SCHEMA, PARSE_OPTIONS, TestSourceTable, USERS, normalizeTokenParameters } from './util.js';

describe('sync rules', () => {
  test('parse empty sync rules', () => {
    const rules = SqlSyncRules.fromYaml('bucket_definitions: {}', PARSE_OPTIONS);
    expect(rules.bucket_descriptors).toEqual([]);
  });

  test('parse global sync rules', () => {
    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  mybucket:
    data:
      - SELECT id, description FROM assets
    `,
      PARSE_OPTIONS
    );
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
    expect(rules.getStaticBucketIds(normalizeTokenParameters({}))).toEqual(['mybucket[]']);
  });

  test('parse global sync rules with filter', () => {
    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  mybucket:
    parameters: SELECT WHERE token_parameters.is_admin
    data: []
    `,
      PARSE_OPTIONS
    );
    const bucket = rules.bucket_descriptors[0];
    expect(bucket.bucket_parameters).toEqual([]);
    const param_query = bucket.global_parameter_queries[0];

    // Internal API, subject to change
    expect(param_query.filter!.lookupParameterValue(normalizeTokenParameters({ is_admin: 1n }))).toEqual(1n);
    expect(param_query.filter!.lookupParameterValue(normalizeTokenParameters({ is_admin: 0n }))).toEqual(0n);

    expect(rules.getStaticBucketIds(normalizeTokenParameters({ is_admin: true }))).toEqual(['mybucket[]']);
    expect(rules.getStaticBucketIds(normalizeTokenParameters({ is_admin: false }))).toEqual([]);
    expect(rules.getStaticBucketIds(normalizeTokenParameters({}))).toEqual([]);
  });

  test('parse global sync rules with table filter', () => {
    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  mybucket:
    parameters: SELECT FROM users WHERE users.id = token_parameters.user_id AND users.is_admin
    data: []
    `,
      PARSE_OPTIONS
    );
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
    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  mybucket:
    parameters: SELECT token_parameters.user_id, user_parameters.device_id
    data:
      - SELECT id, description FROM assets WHERE assets.user_id = bucket.user_id AND assets.device_id = bucket.device_id AND NOT assets.archived
    `,
      PARSE_OPTIONS
    );
    const bucket = rules.bucket_descriptors[0];
    expect(bucket.bucket_parameters).toEqual(['user_id', 'device_id']);
    const param_query = bucket.global_parameter_queries[0];
    expect(param_query.bucket_parameters).toEqual(['user_id', 'device_id']);
    expect(rules.getStaticBucketIds(normalizeTokenParameters({ user_id: 'user1' }, { device_id: 'device1' }))).toEqual([
      'mybucket["user1","device1"]'
    ]);

    const data_query = bucket.data_queries[0];
    expect(data_query.bucket_parameters).toEqual(['user_id', 'device_id']);
    expect(
      rules.evaluateRow({
        sourceTable: ASSETS,
        record: { id: 'asset1', description: 'test', user_id: 'user1', device_id: 'device1' }
      })
    ).toEqual([
      {
        ruleId: '1',
        bucket: 'mybucket["user1","device1"]',
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
        record: { id: 'asset1', description: 'test', user_id: 'user1', archived: 1, device_id: 'device1' }
      })
    ).toEqual([]);
  });

  test('parse bucket with parameters and OR condition', () => {
    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  mybucket:
    parameters: SELECT token_parameters.user_id
    data:
      - SELECT id, description FROM assets WHERE assets.user_id = bucket.user_id OR assets.owner_id = bucket.user_id
    `,
      PARSE_OPTIONS
    );
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
      const rules = SqlSyncRules.fromYaml(
        `
bucket_definitions:
  mybucket:
    parameters: SELECT token_parameters.user_id
    data:
      - SELECT id, description FROM assets WHERE assets.user_id = bucket.user_id AND (assets.user_id = bucket.foo OR assets.other_id = bucket.bar)
    `,
        PARSE_OPTIONS
      );
    }).toThrowError(/must use the same parameters/);
  });

  test('reject unsupported queries', () => {
    expect(
      SqlSyncRules.validate(
        `
bucket_definitions:
  mybucket:
    parameters: SELECT token_parameters.user_id LIMIT 1
    data: []
    `,
        PARSE_OPTIONS
      )
    ).toMatchObject([{ message: 'LIMIT is not supported' }]);

    expect(
      SqlSyncRules.validate(
        `
bucket_definitions:
  mybucket:
    data:
      - SELECT DISTINCT id, description FROM assets
    `,
        PARSE_OPTIONS
      )
    ).toMatchObject([{ message: 'DISTINCT is not supported' }]);

    expect(
      SqlSyncRules.validate(
        `
bucket_definitions:
  mybucket:
    parameters: SELECT token_parameters.user_id OFFSET 10
    data: []
    `,
        PARSE_OPTIONS
      )
    ).toMatchObject([{ message: 'LIMIT is not supported' }]);

    expect(() => {
      const rules = SqlSyncRules.fromYaml(
        `
bucket_definitions:
  mybucket:
    parameters: SELECT token_parameters.user_id FOR UPDATE SKIP LOCKED
    data: []
    `,
        PARSE_OPTIONS
      );
    }).toThrowError(/SKIP is not supported/);

    expect(() => {
      const rules = SqlSyncRules.fromYaml(
        `
bucket_definitions:
  mybucket:
    parameters: SELECT token_parameters.user_id FOR UPDATE
    data: []
    `,
        PARSE_OPTIONS
      );
    }).toThrowError(/FOR is not supported/);

    expect(() => {
      const rules = SqlSyncRules.fromYaml(
        `
bucket_definitions:
  mybucket:
    data:
      - SELECT id, description FROM assets ORDER BY id
    `,
        PARSE_OPTIONS
      );
    }).toThrowError(/ORDER BY is not supported/);
  });

  test('transforming things', () => {
    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  mybucket:
    parameters: SELECT upper(token_parameters.user_id) AS user_id
    data:
      - SELECT id, upper(description) AS description_upper FROM assets WHERE upper(assets.user_id) = bucket.user_id AND NOT assets.archived
    `,
      PARSE_OPTIONS
    );
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
    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  mybucket:
    parameters: SELECT UPPER(token_parameters.user_id) AS user_id
    data:
      - SELECT id, UPPER(description) AS description_upper FROM assets WHERE UPPER(assets.user_id) = bucket.user_id AND NOT assets.archived
    `,
      PARSE_OPTIONS
    );
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
    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  mybucket:
    data:
      - SELECT id, data ->> 'count' AS count, data -> 'bool' AS bool1, data ->> 'bool' AS bool2, 'true' ->> '$' as bool3, json_extract(data, '$.bool') AS bool4 FROM assets
    `,
      PARSE_OPTIONS
    );
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
    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  mybucket:
    parameters: SELECT token_parameters.region_id
    data:
      - SELECT id, description FROM assets WHERE bucket.region_id IN assets.region_ids
    `,
      PARSE_OPTIONS
    );

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
    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  mybucket:
    parameters: SELECT token_parameters.is_admin
    data:
      - SELECT id, description, role, 'admin' as rule FROM assets WHERE bucket.is_admin
      - SELECT id, description, role, 'normal' as rule FROM assets WHERE (bucket.is_admin OR bucket.is_admin = false) AND assets.role != 'admin'
    `,
      PARSE_OPTIONS
    );

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

  test('some math', () => {
    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  mybucket:
    data:
      - SELECT id, (5 / 2) AS int, (5 / 2.0) AS float, (CAST(5 AS real) / 2) AS float2 FROM assets
    `,
      PARSE_OPTIONS
    );

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
    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  mybucket:
    parameters: SELECT token_parameters.int1, token_parameters.float1, token_parameters.float2
    data:
      - SELECT id FROM assets WHERE assets.int1 = bucket.int1 AND assets.float1 = bucket.float1 AND assets.float2 = bucket.float2
    `,
      PARSE_OPTIONS
    );
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

  test('static parameter query with function on token_parameter', () => {
    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  mybucket:
    parameters: SELECT upper(token_parameters.user_id) as upper
    data: []
    `,
      PARSE_OPTIONS
    );
    expect(rules.errors).toEqual([]);
    expect(rules.getStaticBucketIds(normalizeTokenParameters({ user_id: 'test' }))).toEqual(['mybucket["TEST"]']);
  });

  test('custom table and id', () => {
    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  mybucket:
    data:
      - SELECT client_id AS id, description FROM assets_123 as assets WHERE assets.archived = false
      - SELECT other_id AS id, description FROM assets_123 as assets
    `,
      PARSE_OPTIONS
    );

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
    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  mybucket:
    data:
      - SELECT client_id AS id, description, _table_suffix as suffix, * FROM "assets_%" as assets WHERE assets.archived = false AND _table_suffix > '100'
    `,
      PARSE_OPTIONS
    );

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
    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  mybucket:
    data:
      - SELECT *, _table_suffix as suffix, * FROM "%" WHERE archived = false
    `,
      PARSE_OPTIONS
    );

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
    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  mybucket:
    data:
      - SELECT id FROM "assets" # Yes
      - SELECT id FROM "test_schema"."assets" # yes
      - SELECT id FROM "default.test_schema"."assets" # yes
      - SELECT id FROM "other"."assets" # no
      - SELECT id FROM "other.test_schema"."assets" # no
    `,
      PARSE_OPTIONS
    );

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

  test('propagate parameter schema errors', () => {
    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  mybucket:
    parameters: SELECT id FROM assets WHERE other_id = token_parameters.user_id
    data: []
    `,
      { schema: BASIC_SCHEMA, ...PARSE_OPTIONS }
    );

    expect(rules.errors).toMatchObject([
      {
        message: 'Column not found: other_id',
        type: 'warning'
      }
    ]);
  });

  test('dangerous query errors', () => {
    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  mybucket:
    parameters: SELECT request.parameters() ->> 'project_id' as project_id
    data: []
    `,
      { schema: BASIC_SCHEMA, ...PARSE_OPTIONS }
    );

    expect(rules.errors).toMatchObject([
      {
        message:
          "Potentially dangerous query based on parameters set by the client. The client can send any value for these parameters so it's not a good place to do authorization.",
        type: 'warning'
      }
    ]);
  });

  test('dangerous query errors - ignored', () => {
    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  mybucket:
    accept_potentially_dangerous_queries: true
    parameters: SELECT request.parameters() ->> 'project_id' as project_id
    data: []
    `,
      { schema: BASIC_SCHEMA, ...PARSE_OPTIONS }
    );

    expect(rules.errors).toEqual([]);
  });

  test('schema generation', () => {
    const schema = new StaticSchema([
      {
        tag: DEFAULT_TAG,
        schemas: [
          {
            name: 'test_schema',
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

    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  mybucket:
    data:
      - SELECT * FROM assets as assets1
      - SELECT id, name, count FROM assets as assets2
      - SELECT id, owner_id as other_id, foo FROM assets as ASSETS2
    `,
      PARSE_OPTIONS
    );

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

    expect(new JsLegacySchemaGenerator().generate(rules, schema)).toEqual(`new Schema([
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

    expect(new TsSchemaGenerator().generate(rules, schema)).toEqual(
      `import { column, Schema, Table } from '@powersync/web';
// OR: import { column, Schema, Table } from '@powersync/react-native';

const assets1 = new Table(
  {
    // id column (text) is automatically included
    name: column.text,
    count: column.integer,
    owner_id: column.text
  },
  { indexes: {} }
);

const assets2 = new Table(
  {
    // id column (text) is automatically included
    name: column.text,
    count: column.integer,
    other_id: column.text,
    foo: column.text
  },
  { indexes: {} }
);

export const AppSchema = new Schema({
  assets1,
  assets2
});

export type Database = (typeof AppSchema)['types'];
`
    );
  });
});
