import { describe, expect, test } from 'vitest';
import { RequestParameters, SqlParameterQuery } from '../../src/index.js';
import { StaticSqlParameterQuery } from '../../src/StaticSqlParameterQuery.js';
import { normalizeTokenParameters, PARSE_OPTIONS } from './util.js';

describe('static parameter queries', () => {
  test('basic query', function () {
    const sql = 'SELECT token_parameters.user_id';
    const query = SqlParameterQuery.fromSql('mybucket', sql, PARSE_OPTIONS) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucket_parameters!).toEqual(['user_id']);
    expect(query.getStaticBucketDescriptions(normalizeTokenParameters({ user_id: 'user1' }))).toEqual([{bucket: 'mybucket["user1"]', priority: 1}]);
  });

  test('global query', function () {
    const sql = 'SELECT';
    const query = SqlParameterQuery.fromSql('mybucket', sql, PARSE_OPTIONS) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucket_parameters!).toEqual([]);
    expect(query.getStaticBucketDescriptions(normalizeTokenParameters({ user_id: 'user1' }))).toEqual([{bucket: 'mybucket[]', priority: 1}]);
  });

  test('query with filter', function () {
    const sql = 'SELECT token_parameters.user_id WHERE token_parameters.is_admin';
    const query = SqlParameterQuery.fromSql('mybucket', sql, PARSE_OPTIONS) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.getStaticBucketDescriptions(normalizeTokenParameters({ user_id: 'user1', is_admin: true }))).toEqual([
      {bucket: 'mybucket["user1"]', priority: 1}
    ]);
    expect(query.getStaticBucketDescriptions(normalizeTokenParameters({ user_id: 'user1', is_admin: false }))).toEqual([]);
  });

  test('function in select clause', function () {
    const sql = 'SELECT upper(token_parameters.user_id) as upper_id';
    const query = SqlParameterQuery.fromSql('mybucket', sql, PARSE_OPTIONS) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.getStaticBucketDescriptions(normalizeTokenParameters({ user_id: 'user1' }))).toEqual([{bucket: 'mybucket["USER1"]', priority: 1}]);
    expect(query.bucket_parameters!).toEqual(['upper_id']);
  });

  test('function in filter clause', function () {
    const sql = "SELECT WHERE upper(token_parameters.role) = 'ADMIN'";
    const query = SqlParameterQuery.fromSql('mybucket', sql, PARSE_OPTIONS) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.getStaticBucketDescriptions(normalizeTokenParameters({ role: 'admin' }))).toEqual([{bucket: 'mybucket[]', priority: 1}]);
    expect(query.getStaticBucketDescriptions(normalizeTokenParameters({ role: 'user' }))).toEqual([]);
  });

  test('comparison in filter clause', function () {
    const sql = 'SELECT WHERE token_parameters.id1 = token_parameters.id2';
    const query = SqlParameterQuery.fromSql('mybucket', sql, PARSE_OPTIONS) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.getStaticBucketDescriptions(normalizeTokenParameters({ id1: 't1', id2: 't1' }))).toEqual([{bucket: 'mybucket[]', priority: 1}]);
    expect(query.getStaticBucketDescriptions(normalizeTokenParameters({ id1: 't1', id2: 't2' }))).toEqual([]);
  });

  test('request.parameters()', function () {
    const sql = "SELECT request.parameters() ->> 'org_id' as org_id";
    const query = SqlParameterQuery.fromSql('mybucket', sql, {
      ...PARSE_OPTIONS,
      accept_potentially_dangerous_queries: true
    }) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);

    expect(query.getStaticBucketDescriptions(normalizeTokenParameters({}, { org_id: 'test' }))).toEqual([{bucket: 'mybucket["test"]', priority: 1}]);
  });

  test('request.jwt()', function () {
    const sql = "SELECT request.jwt() ->> 'sub' as user_id";
    const query = SqlParameterQuery.fromSql('mybucket', sql, PARSE_OPTIONS) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucket_parameters).toEqual(['user_id']);

    expect(query.getStaticBucketDescriptions(normalizeTokenParameters({ user_id: 'user1' }))).toEqual([{bucket: 'mybucket["user1"]', priority: 1}]);
  });

  test('request.user_id()', function () {
    const sql = 'SELECT request.user_id() as user_id';
    const query = SqlParameterQuery.fromSql('mybucket', sql, PARSE_OPTIONS) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucket_parameters).toEqual(['user_id']);

    expect(query.getStaticBucketDescriptions(normalizeTokenParameters({ user_id: 'user1' }))).toEqual([{bucket: 'mybucket["user1"]', priority: 1}]);
  });

  test('static value', function () {
    const sql = `SELECT WHERE 1`;
    const query = SqlParameterQuery.fromSql('mybucket', sql, PARSE_OPTIONS) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.getStaticBucketDescriptions(new RequestParameters({ sub: '' }, {}))).toEqual([{bucket: 'mybucket[]', priority: 1}]);
  });

  test('static expression (1)', function () {
    const sql = `SELECT WHERE 1 = 1`;
    const query = SqlParameterQuery.fromSql('mybucket', sql, PARSE_OPTIONS) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.getStaticBucketDescriptions(new RequestParameters({ sub: '' }, {}))).toEqual([{bucket: 'mybucket[]', priority: 1}]);
  });

  test('static expression (2)', function () {
    const sql = `SELECT WHERE 1 != 1`;
    const query = SqlParameterQuery.fromSql('mybucket', sql, PARSE_OPTIONS) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.getStaticBucketDescriptions(new RequestParameters({ sub: '' }, {}))).toEqual([]);
  });

  test('static IN expression', function () {
    const sql = `SELECT WHERE 'admin' IN '["admin", "superuser"]'`;
    const query = SqlParameterQuery.fromSql('mybucket', sql, PARSE_OPTIONS) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.getStaticBucketDescriptions(new RequestParameters({ sub: '' }, {}))).toEqual([{bucket: 'mybucket[]', priority: 1}]);
  });

  test('IN for permissions in request.jwt() (1)', function () {
    // Can use -> or ->> here
    const sql = `SELECT 'read:users' IN (request.jwt() ->> 'permissions') as access_granted`;
    const query = SqlParameterQuery.fromSql('mybucket', sql, PARSE_OPTIONS) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(
      query.getStaticBucketDescriptions(new RequestParameters({ sub: '', permissions: ['write', 'read:users'] }, {}))
    ).toEqual([{bucket: 'mybucket[1]', priority: 1}]);
    expect(
      query.getStaticBucketDescriptions(new RequestParameters({ sub: '', permissions: ['write', 'write:users'] }, {}))
    ).toEqual([{bucket: 'mybucket[0]', priority: 1}]);
  });

  test('IN for permissions in request.jwt() (2)', function () {
    // Can use -> or ->> here
    const sql = `SELECT WHERE 'read:users' IN (request.jwt() ->> 'permissions')`;
    const query = SqlParameterQuery.fromSql('mybucket', sql, PARSE_OPTIONS) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(
      query.getStaticBucketDescriptions(new RequestParameters({ sub: '', permissions: ['write', 'read:users'] }, {}))
    ).toEqual([{bucket: 'mybucket[]', priority: 1}]);
    expect(
      query.getStaticBucketDescriptions(new RequestParameters({ sub: '', permissions: ['write', 'write:users'] }, {}))
    ).toEqual([]);
  });

  test('IN for permissions in request.jwt() (3)', function () {
    const sql = `SELECT WHERE request.jwt() ->> 'role' IN '["admin", "superuser"]'`;
    const query = SqlParameterQuery.fromSql('mybucket', sql, PARSE_OPTIONS) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.getStaticBucketDescriptions(new RequestParameters({ sub: '', role: 'superuser' }, {}))).toEqual([{bucket: 'mybucket[]', priority: 1}]);
    expect(query.getStaticBucketDescriptions(new RequestParameters({ sub: '', role: 'superadmin' }, {}))).toEqual([]);
  });

  test('case-sensitive queries (1)', () => {
    const sql = 'SELECT request.user_id() as USER_ID';
    const query = SqlParameterQuery.fromSql('mybucket', sql, PARSE_OPTIONS) as SqlParameterQuery;
    expect(query.errors).toMatchObject([
      { message: `Unquoted identifiers are converted to lower-case. Use "USER_ID" instead.` }
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
