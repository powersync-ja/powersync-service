import { describe, expect, test } from 'vitest';
import { ParameterLookup, SqlParameterQuery, SqlSyncRules } from '../../src/index.js';

import {
  ASSETS,
  BASIC_SCHEMA,
  PARSE_OPTIONS,
  TestSourceTable,
  USERS,
  normalizeQuerierOptions,
  normalizeTokenParameters
} from './util.js';
import { SqlBucketDescriptor } from '../../src/SqlBucketDescriptor.js';
import { StaticSqlParameterQuery } from '../../src/StaticSqlParameterQuery.js';

describe('sync rules', () => {
  const bucketIdTransformer = SqlSyncRules.versionedBucketIdTransformer('');

  test('parse empty sync rules', () => {
    const rules = SqlSyncRules.fromYaml('bucket_definitions: {}', PARSE_OPTIONS);
    expect(rules.bucketParameterLookupSources).toEqual([]);
    expect(rules.bucketParameterQuerierSources).toEqual([]);
    expect(rules.bucketDataSources).toEqual([]);
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
    const hydrated = rules.hydrate({ bucketIdTransformer });
    const bucket = rules.bucketSources[0] as SqlBucketDescriptor;
    expect(bucket.name).toEqual('mybucket');
    expect(bucket.bucketParameters).toEqual([]);
    const dataQuery = bucket.dataQueries[0];
    expect(dataQuery.bucketParameters).toEqual([]);
    expect(dataQuery.columnOutputNames()).toEqual(['id', 'description']);
    expect(
      hydrated.evaluateRow({
        sourceTable: ASSETS,
        record: { id: 'asset1', description: 'test' }
      })
    ).toEqual([
      {
        table: 'assets',
        id: 'asset1',
        data: {
          id: 'asset1',
          description: 'test'
        },
        bucket: 'mybucket[]'
      }
    ]);
    expect(hydrated.getBucketParameterQuerier(normalizeQuerierOptions({})).querier).toMatchObject({
      staticBuckets: [{ bucket: 'mybucket[]', priority: 3 }],
      hasDynamicBuckets: false
    });
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
    const hydrated = rules.hydrate({ bucketIdTransformer });
    expect(rules.bucketParameterLookupSources).toEqual([]);
    const parameterSource = rules.bucketParameterQuerierSources[0];
    expect(parameterSource.bucketParameters).toEqual([]);

    // Internal API, subject to change
    const parameterQuery = parameterSource as StaticSqlParameterQuery;
    expect(parameterQuery.filter!.lookupParameterValue(normalizeTokenParameters({ is_admin: 1n }))).toEqual(1n);
    expect(parameterQuery.filter!.lookupParameterValue(normalizeTokenParameters({ is_admin: 0n }))).toEqual(0n);

    expect(hydrated.getBucketParameterQuerier(normalizeQuerierOptions({ is_admin: true })).querier).toMatchObject({
      staticBuckets: [{ bucket: 'mybucket[]', priority: 3 }],
      hasDynamicBuckets: false
    });
    expect(hydrated.getBucketParameterQuerier(normalizeQuerierOptions({ is_admin: false })).querier).toMatchObject({
      staticBuckets: [],
      hasDynamicBuckets: false
    });
    expect(hydrated.getBucketParameterQuerier(normalizeQuerierOptions({})).querier).toMatchObject({
      staticBuckets: [],
      hasDynamicBuckets: false
    });
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
    const hydrated = rules.hydrate({ bucketIdTransformer });
    expect(hydrated.evaluateParameterRow(USERS, { id: 'user1', is_admin: 1 })).toEqual([
      {
        bucketParameters: [{}],
        lookup: ParameterLookup.normalized('mybucket', '1', ['user1'])
      }
    ]);
    expect(hydrated.evaluateParameterRow(USERS, { id: 'user1', is_admin: 0 })).toEqual([]);
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
    const hydrated = rules.hydrate({ bucketIdTransformer });
    const parameterSource = rules.bucketParameterQuerierSources[0];
    const bucketData = rules.bucketDataSources[0];
    expect(parameterSource.bucketParameters).toEqual(['user_id', 'device_id']);
    expect(bucketData.bucketParameters).toEqual(['user_id', 'device_id']);
    expect(
      hydrated.getBucketParameterQuerier(normalizeQuerierOptions({ user_id: 'user1' }, { device_id: 'device1' }))
        .querier.staticBuckets
    ).toEqual([
      { bucket: 'mybucket["user1","device1"]', definition: 'mybucket', inclusion_reasons: ['default'], priority: 3 }
    ]);

    expect(
      hydrated.evaluateRow({
        sourceTable: ASSETS,
        record: { id: 'asset1', description: 'test', user_id: 'user1', device_id: 'device1' }
      })
    ).toEqual([
      {
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
      hydrated.evaluateRow({
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
    const hydrated = rules.hydrate({ bucketIdTransformer });
    const bucketParameters = rules.bucketParameterQuerierSources[0];
    const bucketData = rules.bucketDataSources[0];
    expect(bucketParameters.bucketParameters).toEqual(['user_id']);
    expect(bucketData.bucketParameters).toEqual(['user_id']);
    expect(
      hydrated.getBucketParameterQuerier(normalizeQuerierOptions({ user_id: 'user1' })).querier.staticBuckets
    ).toEqual([{ bucket: 'mybucket["user1"]', definition: 'mybucket', inclusion_reasons: ['default'], priority: 3 }]);

    expect(
      hydrated.evaluateRow({
        sourceTable: ASSETS,
        record: { id: 'asset1', description: 'test', user_id: 'user1' }
      })
    ).toEqual([
      {
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
      hydrated.evaluateRow({
        sourceTable: ASSETS,
        record: { id: 'asset1', description: 'test', owner_id: 'user1' }
      })
    ).toEqual([
      {
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
    const hydrated = rules.hydrate({ bucketIdTransformer });
    const bucketParameters = rules.bucketParameterQuerierSources[0];
    expect(bucketParameters.bucketParameters).toEqual(['user_id']);
    expect(hydrated.getBucketParameterQuerier(normalizeQuerierOptions({ user_id: 'user1' })).querier).toMatchObject({
      staticBuckets: [{ bucket: 'mybucket["USER1"]', priority: 3 }],
      hasDynamicBuckets: false
    });

    expect(
      hydrated.evaluateRow({
        sourceTable: ASSETS,
        record: { id: 'asset1', description: 'test', user_id: 'user1' }
      })
    ).toEqual([
      {
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
    const hydrated = rules.hydrate({ bucketIdTransformer });
    const bucketParameters = rules.bucketParameterQuerierSources[0];
    expect(bucketParameters.bucketParameters).toEqual(['user_id']);
    expect(hydrated.getBucketParameterQuerier(normalizeQuerierOptions({ user_id: 'user1' })).querier).toMatchObject({
      staticBuckets: [{ bucket: 'mybucket["USER1"]', priority: 3 }],
      hasDynamicBuckets: false
    });

    expect(
      hydrated.evaluateRow({
        sourceTable: ASSETS,
        record: { id: 'asset1', description: 'test', user_id: 'user1' }
      })
    ).toEqual([
      {
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
    const hydrated = rules.hydrate({ bucketIdTransformer });
    expect(
      hydrated.evaluateRow({
        sourceTable: ASSETS,
        record: { id: 'asset1', data: JSON.stringify({ count: 5, bool: true }) }
      })
    ).toEqual([
      {
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
    const hydrated = rules.hydrate({ bucketIdTransformer });

    expect(
      hydrated.evaluateRow({
        sourceTable: ASSETS,
        record: {
          id: 'asset1',
          description: 'test',
          region_ids: JSON.stringify(['region1', 'region2'])
        }
      })
    ).toEqual([
      {
        bucket: 'mybucket["region1"]',
        id: 'asset1',
        data: {
          id: 'asset1',
          description: 'test'
        },
        table: 'assets'
      },
      {
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
    const hydrated = rules.hydrate({ bucketIdTransformer });

    expect(
      hydrated.evaluateRow({
        sourceTable: ASSETS,
        record: { id: 'asset1', description: 'test', role: 'admin' }
      })
    ).toEqual([
      {
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
      hydrated.evaluateRow({
        sourceTable: ASSETS,
        record: { id: 'asset2', description: 'test', role: 'normal' }
      })
    ).toEqual([
      {
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

    expect(
      hydrated.getBucketParameterQuerier(normalizeQuerierOptions({ is_admin: true })).querier.staticBuckets
    ).toEqual([{ bucket: 'mybucket[1]', definition: 'mybucket', inclusion_reasons: ['default'], priority: 3 }]);
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
    const hydrated = rules.hydrate({ bucketIdTransformer });

    expect(hydrated.evaluateRow({ sourceTable: ASSETS, record: { id: 'asset1' } })).toEqual([
      {
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
    const hydrated = rules.hydrate({ bucketIdTransformer });
    expect(
      hydrated.getBucketParameterQuerier(normalizeQuerierOptions({ int1: 314, float1: 3.14, float2: 314 })).querier
    ).toMatchObject({ staticBuckets: [{ bucket: 'mybucket[314,3.14,314]', priority: 3 }] });

    expect(
      hydrated.evaluateRow({
        sourceTable: ASSETS,
        record: { id: 'asset1', int1: 314n, float1: 3.14, float2: 314 }
      })
    ).toEqual([
      {
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
    const hydrated = rules.hydrate({ bucketIdTransformer });
    expect(hydrated.getBucketParameterQuerier(normalizeQuerierOptions({ user_id: 'test' })).querier).toMatchObject({
      staticBuckets: [{ bucket: 'mybucket["TEST"]', priority: 3 }],
      hasDynamicBuckets: false
    });
  });

  test('custom table and id', () => {
    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  mybucket:
    data:
      - SELECT client_id AS id, description, '1' as rule FROM assets_123 as assets WHERE assets.archived = false
      - SELECT other_id AS id, description, '2' as rule FROM assets_123 as assets
    `,
      PARSE_OPTIONS
    );
    const hydrated = rules.hydrate({ bucketIdTransformer });

    expect(
      hydrated.evaluateRow({
        sourceTable: new TestSourceTable('assets_123'),
        record: { client_id: 'asset1', description: 'test', archived: 0n, other_id: 'other1' }
      })
    ).toEqual([
      {
        bucket: 'mybucket[]',
        id: 'asset1',
        data: {
          id: 'asset1',
          description: 'test',
          rule: '1'
        },
        table: 'assets'
      },
      {
        bucket: 'mybucket[]',
        id: 'other1',
        data: {
          id: 'other1',
          description: 'test',
          rule: '2'
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
    const hydrated = rules.hydrate({ bucketIdTransformer });

    expect(
      hydrated.evaluateRow({
        sourceTable: new TestSourceTable('assets_123'),
        record: { client_id: 'asset1', description: 'test', archived: 0n, other_id: 'other1' }
      })
    ).toEqual([
      {
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
    const hydrated = rules.hydrate({ bucketIdTransformer });

    expect(
      hydrated.evaluateRow({
        sourceTable: ASSETS,
        record: { id: 'asset1', description: 'test', archived: 0n }
      })
    ).toEqual([
      {
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
      - SELECT id FROM "assets" as assets1 # Yes
      - SELECT id FROM "test_schema"."assets" as assets2 # yes
      - SELECT id FROM "default.test_schema"."assets" as assets3 # yes
      - SELECT id FROM "other"."assets" as assets4 # no
      - SELECT id FROM "other.test_schema"."assets" as assets5 # no
    `,
      PARSE_OPTIONS
    );
    const hydrated = rules.hydrate({ bucketIdTransformer });

    expect(
      hydrated.evaluateRow({
        sourceTable: ASSETS,
        record: { id: 'asset1' }
      })
    ).toEqual([
      {
        bucket: 'mybucket[]',
        id: 'asset1',
        data: {
          id: 'asset1'
        },
        table: 'assets1'
      },
      {
        bucket: 'mybucket[]',
        id: 'asset1',
        data: {
          id: 'asset1'
        },
        table: 'assets2'
      },
      {
        bucket: 'mybucket[]',
        id: 'asset1',
        data: {
          id: 'asset1'
        },
        table: 'assets3'
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

  test('null bucket definition', () => {
    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  mybucket:
    `,
      { schema: BASIC_SCHEMA, ...PARSE_OPTIONS, throwOnError: false }
    );

    expect(rules.errors).toMatchObject([
      {
        message: "'mybucket' bucket definition must be an object",
        type: 'fatal'
      },
      // Ideally this should not be displayed - it's an additional JSON schema validation error
      // for the same issue. For now we just include both.
      {
        message: 'must be object',
        type: 'fatal'
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

  test('priorities on queries', () => {
    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  highprio:
    parameters: SELECT 0 as _priority;
    data:
      - SELECT * FROM assets WHERE count <= 10;
  defaultprio:
    data:
      - SELECT * FROM assets WHERE count > 10;
    `,
      { schema: BASIC_SCHEMA, ...PARSE_OPTIONS }
    );

    expect(rules.errors).toEqual([]);

    const hydrated = rules.hydrate({ bucketIdTransformer });
    expect(hydrated.getBucketParameterQuerier(normalizeQuerierOptions({})).querier).toMatchObject({
      staticBuckets: [
        { bucket: 'highprio[]', priority: 0 },
        { bucket: 'defaultprio[]', priority: 3 }
      ]
    });
  });

  test('priorities on bucket', () => {
    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  highprio:
    priority: 0
    data:
      - SELECT * FROM assets WHERE count <= 10;
  defaultprio:
    data:
      - SELECT * FROM assets WHERE count > 10;
    `,
      { schema: BASIC_SCHEMA, ...PARSE_OPTIONS }
    );

    expect(rules.errors).toEqual([]);

    const hydrated = rules.hydrate({ bucketIdTransformer });
    expect(hydrated.getBucketParameterQuerier(normalizeQuerierOptions({})).querier).toMatchObject({
      staticBuckets: [
        { bucket: 'highprio[]', priority: 0 },
        { bucket: 'defaultprio[]', priority: 3 }
      ]
    });
  });

  test(`invalid priority on bucket`, () => {
    expect(() =>
      SqlSyncRules.fromYaml(
        `
bucket_definitions:
  highprio:
    priority: instant
    data:
      - SELECT * FROM assets WHERE count <= 10;
    `,
        { schema: BASIC_SCHEMA, ...PARSE_OPTIONS }
      )
    ).toThrowError(/Invalid priority/);
  });

  test(`can't duplicate priority`, () => {
    expect(() =>
      SqlSyncRules.fromYaml(
        `
bucket_definitions:
  highprio:
    priority: 1
    parameters: SELECT 0 as _priority;
    data:
      - SELECT * FROM assets WHERE count <= 10;
    `,
        { schema: BASIC_SCHEMA, ...PARSE_OPTIONS }
      )
    ).toThrowError(/Cannot set priority multiple times/);
  });

  test('dynamic bucket definitions list', () => {
    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  mybucket:
    parameters:
      - SELECT request.user_id() as user_id
      - SELECT id as user_id FROM users WHERE id = request.user_id()
    data: []

  by_list:
    parameters:
      - SELECT id as list_id FROM lists WHERE owner_id = request.user_id()
    data: []

  admin_only:
    parameters:
      - SELECT id as list_id FROM lists WHERE (request.jwt() ->> 'is_admin' IS NULL)
    data: []
    `,
      PARSE_OPTIONS
    );
    const bucketParameters = rules.bucketParameterQuerierSources[0];
    expect(bucketParameters.bucketParameters).toEqual(['user_id']);

    const hydrated = rules.hydrate({ bucketIdTransformer });

    expect(hydrated.getBucketParameterQuerier(normalizeQuerierOptions({ user_id: 'user1' })).querier).toMatchObject({
      hasDynamicBuckets: true,
      parameterQueryLookups: [
        ParameterLookup.normalized('mybucket', '2', ['user1']),
        ParameterLookup.normalized('by_list', '1', ['user1']),
        // These are not filtered out yet, due to how the lookups are structured internally
        ParameterLookup.normalized('admin_only', '1', [1])
      ],
      staticBuckets: [
        {
          bucket: 'mybucket["user1"]',
          priority: 3
        }
      ]
    });
  });
});
