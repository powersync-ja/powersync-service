import { describe, expect, test } from 'vitest';
import { SqlParameterQuery } from '../../src/index.js';
import { StaticSqlParameterQuery } from '../../src/StaticSqlParameterQuery.js';
import { normalizeTokenParameters } from './util.js';

describe('static parameter queries', () => {
  test('basic query', function () {
    const sql = 'SELECT token_parameters.user_id';
    const query = SqlParameterQuery.fromSql('mybucket', sql) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucket_parameters!).toEqual(['user_id']);
    expect(query.getStaticBucketIds(normalizeTokenParameters({ user_id: 'user1' }))).toEqual(['mybucket["user1"]']);
  });

  test('global query', function () {
    const sql = 'SELECT';
    const query = SqlParameterQuery.fromSql('mybucket', sql) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucket_parameters!).toEqual([]);
    expect(query.getStaticBucketIds(normalizeTokenParameters({ user_id: 'user1' }))).toEqual(['mybucket[]']);
  });

  test('query with filter', function () {
    const sql = 'SELECT token_parameters.user_id WHERE token_parameters.is_admin';
    const query = SqlParameterQuery.fromSql('mybucket', sql) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.getStaticBucketIds(normalizeTokenParameters({ user_id: 'user1', is_admin: true }))).toEqual([
      'mybucket["user1"]'
    ]);
    expect(query.getStaticBucketIds(normalizeTokenParameters({ user_id: 'user1', is_admin: false }))).toEqual([]);
  });

  test('function in select clause', function () {
    const sql = 'SELECT upper(token_parameters.user_id) as upper_id';
    const query = SqlParameterQuery.fromSql('mybucket', sql) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.getStaticBucketIds(normalizeTokenParameters({ user_id: 'user1' }))).toEqual(['mybucket["USER1"]']);
    expect(query.bucket_parameters!).toEqual(['upper_id']);
  });

  test('function in filter clause', function () {
    const sql = "SELECT WHERE upper(token_parameters.role) = 'ADMIN'";
    const query = SqlParameterQuery.fromSql('mybucket', sql) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.getStaticBucketIds(normalizeTokenParameters({ role: 'admin' }))).toEqual(['mybucket[]']);
    expect(query.getStaticBucketIds(normalizeTokenParameters({ role: 'user' }))).toEqual([]);
  });

  test('comparison in filter clause', function () {
    const sql = 'SELECT WHERE token_parameters.id1 = token_parameters.id2';
    const query = SqlParameterQuery.fromSql('mybucket', sql) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.getStaticBucketIds(normalizeTokenParameters({ id1: 't1', id2: 't1' }))).toEqual(['mybucket[]']);
    expect(query.getStaticBucketIds(normalizeTokenParameters({ id1: 't1', id2: 't2' }))).toEqual([]);
  });

  test('request.parameters()', function () {
    const sql = "SELECT request.parameters() ->> 'org_id' as org_id";
    const query = SqlParameterQuery.fromSql('mybucket', sql, undefined, {
      accept_potentially_dangerous_queries: true
    }) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);

    expect(query.getStaticBucketIds(normalizeTokenParameters({}, { org_id: 'test' }))).toEqual(['mybucket["test"]']);
  });

  test('request.jwt()', function () {
    const sql = "SELECT request.jwt() ->> 'sub' as user_id";
    const query = SqlParameterQuery.fromSql('mybucket', sql) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucket_parameters).toEqual(['user_id']);

    expect(query.getStaticBucketIds(normalizeTokenParameters({ user_id: 'user1' }))).toEqual(['mybucket["user1"]']);
  });

  test('request.user_id()', function () {
    const sql = 'SELECT request.user_id() as user_id';
    const query = SqlParameterQuery.fromSql('mybucket', sql) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucket_parameters).toEqual(['user_id']);

    expect(query.getStaticBucketIds(normalizeTokenParameters({ user_id: 'user1' }))).toEqual(['mybucket["user1"]']);
  });

  test('case-sensitive queries (1)', () => {
    const sql = 'SELECT request.user_id() as USER_ID';
    const query = SqlParameterQuery.fromSql('mybucket', sql) as SqlParameterQuery;
    expect(query.errors).toMatchObject([
      { message: `Unquoted identifiers are converted to lower-case. Use "USER_ID" instead.` }
    ]);
  });

  describe('dangerous queries', function () {
    function testDangerousQuery(sql: string) {
      test(sql, function () {
        const query = SqlParameterQuery.fromSql('mybucket', sql) as SqlParameterQuery;
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
        const query = SqlParameterQuery.fromSql('mybucket', sql) as SqlParameterQuery;
        expect(query.errors).toEqual([]);
        expect(query.usesDangerousRequestParameters).toEqual(false);
      });
    }

    testSafeQuery('select request.user_id() as user_id');
    testDangerousQuery("select request.parameters() ->> 'project_id' as project_id");
    testSafeQuery("select request.user_id() as user_id, request.parameters() ->> 'project_id' as project_id");
    testDangerousQuery("select where request.parameters() ->> 'include_comments'");
    testSafeQuery("select where request.jwt() ->> 'role' = 'authenticated'");
    testSafeQuery("select request.user_id() as user_id where request.jwt() ->> 'role' = 'authenticated'");
    // Does use token parameters, but is still considered dangerous
    // Any authenticated user can select an arbitrary project_id
    testDangerousQuery(
      "select request.parameters() ->> 'project_id' as project_id where request.jwt() ->> 'role' = 'authenticated'"
    );
  });
});
