import { describe, expect, test } from 'vitest';
import { RequestParameters, SqlParameterQuery } from '../../src/index.js';
import { StaticSqlParameterQuery } from '../../src/StaticSqlParameterQuery.js';
import { PARSE_OPTIONS } from './util.js';

describe('table-valued function queries', () => {
  test('json_each(array param)', function () {
    const sql = "SELECT json_each.value as v FROM json_each(request.parameters() -> 'array')";
    const query = SqlParameterQuery.fromSql('mybucket', sql, {
      ...PARSE_OPTIONS,
      accept_potentially_dangerous_queries: true
    }) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucket_parameters).toEqual(['v']);

    expect(query.getStaticBucketIds(new RequestParameters({ sub: '' }, { array: [1, 2, 3] }))).toEqual([
      'mybucket[1]',
      'mybucket[2]',
      'mybucket[3]'
    ]);
  });

  test('json_each(static string)', function () {
    const sql = `SELECT json_each.value as v FROM json_each('[1,2,3]')`;
    const query = SqlParameterQuery.fromSql('mybucket', sql, PARSE_OPTIONS) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucket_parameters).toEqual(['v']);

    expect(query.getStaticBucketIds(new RequestParameters({ sub: '' }, {}))).toEqual([
      'mybucket[1]',
      'mybucket[2]',
      'mybucket[3]'
    ]);
  });

  test('json_each(null)', function () {
    const sql = `SELECT json_each.value as v FROM json_each(null)`;
    const query = SqlParameterQuery.fromSql('mybucket', sql, PARSE_OPTIONS) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucket_parameters).toEqual(['v']);

    expect(query.getStaticBucketIds(new RequestParameters({ sub: '' }, {}))).toEqual([]);
  });

  test('json_each with fn alias', function () {
    const sql = "SELECT e.value FROM json_each(request.parameters() -> 'array') e";
    const query = SqlParameterQuery.fromSql('mybucket', sql, {
      ...PARSE_OPTIONS,
      accept_potentially_dangerous_queries: true
    }) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucket_parameters).toEqual(['value']);

    expect(query.getStaticBucketIds(new RequestParameters({ sub: '' }, { array: [1, 2, 3] }))).toEqual([
      'mybucket[1]',
      'mybucket[2]',
      'mybucket[3]'
    ]);
  });

  test('json_each with direct value', function () {
    const sql = "SELECT value FROM json_each(request.parameters() -> 'array')";
    const query = SqlParameterQuery.fromSql('mybucket', sql, {
      ...PARSE_OPTIONS,
      accept_potentially_dangerous_queries: true
    }) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucket_parameters).toEqual(['value']);

    expect(query.getStaticBucketIds(new RequestParameters({ sub: '' }, { array: [1, 2, 3] }))).toEqual([
      'mybucket[1]',
      'mybucket[2]',
      'mybucket[3]'
    ]);
  });

  test('json_each in filters', function () {
    const sql = "SELECT value as v FROM json_each(request.parameters() -> 'array') e WHERE e.value >= 2";
    const query = SqlParameterQuery.fromSql('mybucket', sql, {
      ...PARSE_OPTIONS,
      accept_potentially_dangerous_queries: true
    }) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucket_parameters).toEqual(['v']);

    expect(query.getStaticBucketIds(new RequestParameters({ sub: '' }, { array: [1, 2, 3] }))).toEqual([
      'mybucket[2]',
      'mybucket[3]'
    ]);
  });

  describe('dangerous queries', function () {
    function testDangerousQuery(sql: string) {
      test(sql, function () {
        const query = SqlParameterQuery.fromSql('mybucket', sql, PARSE_OPTIONS) as SqlParameterQuery;
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
        const query = SqlParameterQuery.fromSql('mybucket', sql, PARSE_OPTIONS) as SqlParameterQuery;
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
