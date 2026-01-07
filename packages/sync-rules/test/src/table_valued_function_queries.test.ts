import { describe, expect, test } from 'vitest';
import {
  CompatibilityContext,
  CompatibilityEdition,
  CompatibilityOption,
  RequestParameters,
  SqlParameterQuery
} from '../../src/index.js';
import { StaticSqlParameterQuery } from '../../src/StaticSqlParameterQuery.js';
import { EMPTY_DATA_SOURCE, PARSE_OPTIONS } from './util.js';

describe('table-valued function queries', () => {
  test('json_each(array param)', function () {
    const sql = "SELECT json_each.value as v FROM json_each(request.parameters() -> 'array')";
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      {
        ...PARSE_OPTIONS,
        accept_potentially_dangerous_queries: true
      },
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucketParameters).toEqual(['v']);

    expect(
      query.getStaticBucketDescriptions(new RequestParameters({ sub: '' }, { array: [1, 2, 3, null] }), {
        bucketPrefix: 'mybucket'
      })
    ).toEqual([
      { bucket: 'mybucket[1]', priority: 3 },
      { bucket: 'mybucket[2]', priority: 3 },
      { bucket: 'mybucket[3]', priority: 3 },
      { bucket: 'mybucket["null"]', priority: 3 }
    ]);
  });

  test('json_each(array param), fixed json', function () {
    const sql = "SELECT json_each.value as v FROM json_each(request.parameters() -> 'array')";
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      {
        ...PARSE_OPTIONS,
        accept_potentially_dangerous_queries: true,
        compatibility: new CompatibilityContext({
          edition: CompatibilityEdition.LEGACY,
          overrides: new Map([[CompatibilityOption.fixedJsonExtract, true]])
        })
      },
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucketParameters).toEqual(['v']);

    expect(
      query.getStaticBucketDescriptions(new RequestParameters({ sub: '' }, { array: [1, 2, 3, null] }), {
        bucketPrefix: 'mybucket'
      })
    ).toEqual([
      { bucket: 'mybucket[1]', priority: 3 },
      { bucket: 'mybucket[2]', priority: 3 },
      { bucket: 'mybucket[3]', priority: 3 },
      { bucket: 'mybucket[null]', priority: 3 }
    ]);
  });

  test('json_each(static string)', function () {
    const sql = `SELECT json_each.value as v FROM json_each('[1,2,3]')`;
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucketParameters).toEqual(['v']);

    expect(
      query.getStaticBucketDescriptions(new RequestParameters({ sub: '' }, {}), {
        bucketPrefix: 'mybucket'
      })
    ).toEqual([
      { bucket: 'mybucket[1]', priority: 3 },
      { bucket: 'mybucket[2]', priority: 3 },
      { bucket: 'mybucket[3]', priority: 3 }
    ]);
  });

  test('json_each(null)', function () {
    const sql = `SELECT json_each.value as v FROM json_each(null)`;
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucketParameters).toEqual(['v']);

    expect(
      query.getStaticBucketDescriptions(new RequestParameters({ sub: '' }, {}), {
        bucketPrefix: 'mybucket'
      })
    ).toEqual([]);
  });

  test('json_each(array param not present)', function () {
    const sql = "SELECT json_each.value as v FROM json_each(request.parameters() -> 'array_not_present')";
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      {
        ...PARSE_OPTIONS,
        accept_potentially_dangerous_queries: true
      },
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucketParameters).toEqual(['v']);

    expect(
      query.getStaticBucketDescriptions(new RequestParameters({ sub: '' }, {}), {
        bucketPrefix: 'mybucket'
      })
    ).toEqual([]);
  });

  test('json_each(array param not present, ifnull)', function () {
    const sql = "SELECT json_each.value as v FROM json_each(ifnull(request.parameters() -> 'array_not_present', '[]'))";
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      {
        ...PARSE_OPTIONS,
        accept_potentially_dangerous_queries: true
      },
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucketParameters).toEqual(['v']);

    expect(
      query.getStaticBucketDescriptions(new RequestParameters({ sub: '' }, {}), {
        bucketPrefix: 'mybucket'
      })
    ).toEqual([]);
  });

  test('json_each on json_keys', function () {
    const sql = `SELECT value FROM json_each(json_keys('{"a": [], "b": 2, "c": null}'))`;
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucketParameters).toEqual(['value']);

    expect(
      query.getStaticBucketDescriptions(new RequestParameters({ sub: '' }, {}), {
        bucketPrefix: 'mybucket'
      })
    ).toEqual([
      { bucket: 'mybucket["a"]', priority: 3 },
      { bucket: 'mybucket["b"]', priority: 3 },
      { bucket: 'mybucket["c"]', priority: 3 }
    ]);
  });

  test('json_each with fn alias', function () {
    const sql = "SELECT e.value FROM json_each(request.parameters() -> 'array') e";
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      {
        ...PARSE_OPTIONS,
        accept_potentially_dangerous_queries: true
      },
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucketParameters).toEqual(['value']);

    expect(
      query.getStaticBucketDescriptions(new RequestParameters({ sub: '' }, { array: [1, 2, 3] }), {
        bucketPrefix: 'mybucket'
      })
    ).toEqual([
      { bucket: 'mybucket[1]', priority: 3 },
      { bucket: 'mybucket[2]', priority: 3 },
      { bucket: 'mybucket[3]', priority: 3 }
    ]);
  });

  test('json_each with direct value', function () {
    const sql = "SELECT value FROM json_each(request.parameters() -> 'array')";
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      {
        ...PARSE_OPTIONS,
        accept_potentially_dangerous_queries: true
      },
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucketParameters).toEqual(['value']);

    expect(
      query.getStaticBucketDescriptions(new RequestParameters({ sub: '' }, { array: [1, 2, 3] }), {
        bucketPrefix: 'mybucket'
      })
    ).toEqual([
      { bucket: 'mybucket[1]', priority: 3 },
      { bucket: 'mybucket[2]', priority: 3 },
      { bucket: 'mybucket[3]', priority: 3 }
    ]);
  });

  test('json_each in filters (1)', function () {
    const sql = "SELECT value as v FROM json_each(request.parameters() -> 'array') e WHERE e.value >= 2";
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      {
        ...PARSE_OPTIONS,
        accept_potentially_dangerous_queries: true
      },
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucketParameters).toEqual(['v']);

    expect(
      query.getStaticBucketDescriptions(new RequestParameters({ sub: '' }, { array: [1, 2, 3] }), {
        bucketPrefix: 'mybucket'
      })
    ).toEqual([
      { bucket: 'mybucket[2]', priority: 3 },
      { bucket: 'mybucket[3]', priority: 3 }
    ]);
  });

  test('json_each with nested json', function () {
    const sql =
      "SELECT value ->> 'id' as project_id FROM json_each(request.jwt() -> 'projects') WHERE (value ->> 'role') = 'admin'";
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      {
        ...PARSE_OPTIONS,
        accept_potentially_dangerous_queries: true
      },
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucketParameters).toEqual(['project_id']);

    expect(
      query.getStaticBucketDescriptions(
        new RequestParameters(
          {
            sub: '',
            projects: [
              { id: 1, role: 'admin' },
              { id: 2, role: 'user' }
            ]
          },
          {}
        ),
        {
          bucketPrefix: 'mybucket'
        }
      )
    ).toEqual([{ bucket: 'mybucket[1]', priority: 3 }]);
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

    testSafeQuery('select value from json_each(request.user_id())');
    testDangerousQuery("select value from json_each(request.parameters() ->> 'project_ids')");
    testSafeQuery("select request.user_id() as user_id, value FROM json_each(request.parameters() ->> 'project_ids')");
    testSafeQuery(
      "select request.parameters() ->> 'something' as something, value as project_id FROM json_each(request.jwt() ->> 'project_ids')"
    );
  });
});
