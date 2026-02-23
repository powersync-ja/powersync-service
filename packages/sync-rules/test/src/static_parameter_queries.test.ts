import { describe, expect, test } from 'vitest';
import { BucketDataScope, HydrationState } from '../../src/HydrationState.js';
import { BucketParameterQuerier, GetQuerierOptions, QuerierError, SqlParameterQuery } from '../../src/index.js';
import { StaticSqlParameterQuery } from '../../src/StaticSqlParameterQuery.js';
import { EMPTY_DATA_SOURCE, PARSE_OPTIONS, removeSourceSymbol, requestParameters } from './util.js';

describe('static parameter queries', () => {
  const MYBUCKET_SCOPE: BucketDataScope = {
    bucketPrefix: 'mybucket',
    source: EMPTY_DATA_SOURCE
  };

  function getStaticBucketDescriptions(
    query: StaticSqlParameterQuery,
    parameters: ReturnType<typeof requestParameters>,
    scope: BucketDataScope
  ) {
    return query.getStaticBucketDescriptions(parameters, scope).map(removeSourceSymbol);
  }

  test('basic query', function () {
    const sql = 'SELECT token_parameters.user_id';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucketParameters!).toEqual(['user_id']);
    expect(getStaticBucketDescriptions(query, requestParameters({ sub: 'user1' }), MYBUCKET_SCOPE)).toEqual([
      { bucket: 'mybucket["user1"]', priority: 3 }
    ]);
  });

  test('uses bucketPrefix', function () {
    const sql = 'SELECT token_parameters.user_id';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucketParameters!).toEqual(['user_id']);
    expect(
      getStaticBucketDescriptions(query, requestParameters({ sub: 'user1' }), {
        bucketPrefix: '1#mybucket',
        source: EMPTY_DATA_SOURCE
      })
    ).toEqual([{ bucket: '1#mybucket["user1"]', priority: 3 }]);
  });

  test('global query', function () {
    const sql = 'SELECT';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucketParameters!).toEqual([]);
    expect(getStaticBucketDescriptions(query, requestParameters({ sub: 'user1' }), MYBUCKET_SCOPE)).toEqual([
      { bucket: 'mybucket[]', priority: 3 }
    ]);
  });

  test('query with filter', function () {
    const sql = 'SELECT token_parameters.user_id WHERE token_parameters.is_admin';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(
      getStaticBucketDescriptions(
        query,
        requestParameters({ sub: 'user1', parameters: { is_admin: true } }),
        MYBUCKET_SCOPE
      )
    ).toEqual([{ bucket: 'mybucket["user1"]', priority: 3 }]);
    expect(
      getStaticBucketDescriptions(
        query,
        requestParameters({ sub: 'user1', parameters: { is_admin: false } }),
        MYBUCKET_SCOPE
      )
    ).toEqual([]);
  });

  test('function in select clause', function () {
    const sql = 'SELECT upper(token_parameters.user_id) as upper_id';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(getStaticBucketDescriptions(query, requestParameters({ sub: 'user1' }), MYBUCKET_SCOPE)).toEqual([
      { bucket: 'mybucket["USER1"]', priority: 3 }
    ]);
    expect(query.bucketParameters!).toEqual(['upper_id']);
  });

  test('function in filter clause', function () {
    const sql = "SELECT WHERE upper(token_parameters.role) = 'ADMIN'";
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(
      getStaticBucketDescriptions(
        query,
        requestParameters({ sub: 'admin', parameters: { role: 'admin' } }),
        MYBUCKET_SCOPE
      )
    ).toEqual([{ bucket: 'mybucket[]', priority: 3 }]);
    expect(
      getStaticBucketDescriptions(
        query,
        requestParameters({ sub: 'user', parameters: { role: 'user' } }),
        MYBUCKET_SCOPE
      )
    ).toEqual([]);
  });

  test('comparison in filter clause', function () {
    const sql = 'SELECT WHERE token_parameters.id1 = token_parameters.id2';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(
      getStaticBucketDescriptions(query, requestParameters({ parameters: { id1: 't1', id2: 't1' } }), MYBUCKET_SCOPE)
    ).toEqual([{ bucket: 'mybucket[]', priority: 3 }]);
    expect(
      getStaticBucketDescriptions(query, requestParameters({ parameters: { id1: 't1', id2: 't2' } }), MYBUCKET_SCOPE)
    ).toEqual([]);
  });

  test('request.parameters()', function () {
    const sql = "SELECT request.parameters() ->> 'org_id' as org_id";
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

    expect(getStaticBucketDescriptions(query, requestParameters({}, { org_id: 'test' }), MYBUCKET_SCOPE)).toEqual([
      { bucket: 'mybucket["test"]', priority: 3 }
    ]);
  });

  test("request.jwt() ->> 'sub'", function () {
    const sql = "SELECT request.jwt() ->> 'sub' as user_id";
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucketParameters).toEqual(['user_id']);

    expect(getStaticBucketDescriptions(query, requestParameters({ sub: 'user1' }), MYBUCKET_SCOPE)).toEqual([
      { bucket: 'mybucket["user1"]', priority: 3 }
    ]);
    expect(getStaticBucketDescriptions(query, requestParameters({ sub: 123 }), MYBUCKET_SCOPE)).toEqual([
      { bucket: 'mybucket[123]', priority: 3 }
    ]);
    expect(getStaticBucketDescriptions(query, requestParameters({ sub: true }), MYBUCKET_SCOPE)).toEqual([
      { bucket: 'mybucket[1]', priority: 3 }
    ]);
    expect(getStaticBucketDescriptions(query, requestParameters({ sub: { a: 123 } }), MYBUCKET_SCOPE)).toEqual([
      { bucket: 'mybucket[\"{\\\"a\\\":123.0}\"]', priority: 3 }
    ]);
  });

  test("request.jwt() ->> 'other'", function () {
    // This is the same as the `request.jwt() ->> 'sub'` test, but with a different claim name to verify that it's not treated as a special case.
    const sql = "SELECT request.jwt() ->> 'other' as user_id";
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucketParameters).toEqual(['user_id']);

    expect(getStaticBucketDescriptions(query, requestParameters({ other: 'user1' }), MYBUCKET_SCOPE)).toEqual([
      { bucket: 'mybucket["user1"]', priority: 3 }
    ]);
    expect(getStaticBucketDescriptions(query, requestParameters({ other: 123 }), MYBUCKET_SCOPE)).toEqual([
      { bucket: 'mybucket[123]', priority: 3 }
    ]);
    expect(getStaticBucketDescriptions(query, requestParameters({ other: true }), MYBUCKET_SCOPE)).toEqual([
      { bucket: 'mybucket[1]', priority: 3 }
    ]);
    expect(getStaticBucketDescriptions(query, requestParameters({ other: { a: 123 } }), MYBUCKET_SCOPE)).toEqual([
      { bucket: 'mybucket[\"{\\\"a\\\":123.0}\"]', priority: 3 }
    ]);
  });

  test("request.jwt() ->> '$.sub.email'", function () {
    const sql = "SELECT request.jwt() ->> 'sub.email' as email";
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucketParameters).toEqual(['email']);

    expect(
      getStaticBucketDescriptions(query, requestParameters({ sub: { email: 'a@example.org' } }), MYBUCKET_SCOPE)
    ).toEqual([{ bucket: 'mybucket["a@example.org"]', priority: 3 }]);
  });

  test('request.user_id()', function () {
    const sql = 'SELECT request.user_id() as user_id';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucketParameters).toEqual(['user_id']);

    expect(getStaticBucketDescriptions(query, requestParameters({ sub: 'user1' }), MYBUCKET_SCOPE)).toEqual([
      { bucket: 'mybucket["user1"]', priority: 3 }
    ]);

    expect(getStaticBucketDescriptions(query, requestParameters({ sub: 123 }), MYBUCKET_SCOPE)).toEqual([
      { bucket: 'mybucket[123]', priority: 3 }
    ]);
    expect(getStaticBucketDescriptions(query, requestParameters({ sub: true }), MYBUCKET_SCOPE)).toEqual([
      { bucket: 'mybucket[1]', priority: 3 }
    ]);
    // This is not expected to be used - we just document the current behavior
    expect(getStaticBucketDescriptions(query, requestParameters({ sub: { a: 123 } }), MYBUCKET_SCOPE)).toEqual([
      { bucket: 'mybucket[\"{\\\"a\\\":123.0}\"]', priority: 3 }
    ]);
  });

  test('typeof request.user_id()', function () {
    const sql = 'SELECT typeof(request.user_id()) as user_id';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(query.bucketParameters).toEqual(['user_id']);

    expect(getStaticBucketDescriptions(query, requestParameters({ sub: '123' }), MYBUCKET_SCOPE)).toEqual([
      { bucket: 'mybucket["text"]', priority: 3 }
    ]);
    expect(getStaticBucketDescriptions(query, requestParameters({ sub: 123 }), MYBUCKET_SCOPE)).toEqual([
      { bucket: 'mybucket["real"]', priority: 3 }
    ]);
    expect(getStaticBucketDescriptions(query, requestParameters({ sub: true }), MYBUCKET_SCOPE)).toEqual([
      { bucket: 'mybucket["integer"]', priority: 3 }
    ]);
  });

  test('static value', function () {
    const sql = `SELECT WHERE 1`;
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(getStaticBucketDescriptions(query, requestParameters({}), MYBUCKET_SCOPE)).toEqual([
      { bucket: 'mybucket[]', priority: 3 }
    ]);
  });

  test('static expression (1)', function () {
    const sql = `SELECT WHERE 1 = 1`;
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(getStaticBucketDescriptions(query, requestParameters({}), MYBUCKET_SCOPE)).toEqual([
      { bucket: 'mybucket[]', priority: 3 }
    ]);
  });

  test('static expression (2)', function () {
    const sql = `SELECT WHERE 1 != 1`;
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(getStaticBucketDescriptions(query, requestParameters({}), MYBUCKET_SCOPE)).toEqual([]);
  });

  test('static IN expression', function () {
    const sql = `SELECT WHERE 'admin' IN '["admin", "superuser"]'`;
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(getStaticBucketDescriptions(query, requestParameters({}), MYBUCKET_SCOPE)).toEqual([
      { bucket: 'mybucket[]', priority: 3 }
    ]);
  });

  test('IN for permissions in request.jwt() (1)', function () {
    // Can use -> or ->> here
    const sql = `SELECT 'read:users' IN (request.jwt() ->> 'permissions') as access_granted`;
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(
      getStaticBucketDescriptions(
        query,
        requestParameters({ sub: '', permissions: ['write', 'read:users'] }),
        MYBUCKET_SCOPE
      )
    ).toEqual([{ bucket: 'mybucket[1]', priority: 3 }]);
    expect(
      getStaticBucketDescriptions(
        query,
        requestParameters({ sub: '', permissions: ['write', 'write:users'] }),
        MYBUCKET_SCOPE
      )
    ).toEqual([{ bucket: 'mybucket[0]', priority: 3 }]);
  });

  test('IN for permissions in request.jwt() (2)', function () {
    // Can use -> or ->> here
    const sql = `SELECT WHERE 'read:users' IN (request.jwt() ->> 'permissions')`;
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(
      getStaticBucketDescriptions(
        query,
        requestParameters({ sub: '', permissions: ['write', 'read:users'] }),
        MYBUCKET_SCOPE
      )
    ).toEqual([{ bucket: 'mybucket[]', priority: 3 }]);
    expect(
      getStaticBucketDescriptions(
        query,
        requestParameters({ sub: '', permissions: ['write', 'write:users'] }),
        MYBUCKET_SCOPE
      )
    ).toEqual([]);
  });

  test('IN for permissions in request.jwt() (3)', function () {
    const sql = `SELECT WHERE request.jwt() ->> 'role' IN '["admin", "superuser"]'`;
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;
    expect(query.errors).toEqual([]);
    expect(getStaticBucketDescriptions(query, requestParameters({ role: 'superuser' }), MYBUCKET_SCOPE)).toEqual([
      { bucket: 'mybucket[]', priority: 3 }
    ]);
    expect(getStaticBucketDescriptions(query, requestParameters({ role: 'superadmin' }), MYBUCKET_SCOPE)).toEqual([]);
  });

  test('case-sensitive queries (1)', () => {
    const sql = 'SELECT request.user_id() as USER_ID';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as SqlParameterQuery;
    expect(query.errors).toMatchObject([
      { message: `Unquoted identifiers are converted to lower-case. Use "USER_ID" instead.` }
    ]);
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

  test('custom hydrationState for buckets', function () {
    const sql = 'SELECT token_parameters.user_id';
    const query = SqlParameterQuery.fromSql(
      'mybucket',
      sql,
      PARSE_OPTIONS,
      '1',
      EMPTY_DATA_SOURCE
    ) as StaticSqlParameterQuery;

    expect(query.errors).toEqual([]);

    const hydrationState: HydrationState = {
      getBucketSourceScope(source) {
        return { bucketPrefix: `${source.uniqueName}-test`, source };
      },
      getParameterIndexLookupScope(source) {
        return {
          lookupName: `${source.defaultLookupScope.lookupName}.test`,
          queryId: `${source.defaultLookupScope.queryId}.test`,
          source
        };
      }
    };

    // Internal API
    const hydrated = query.createParameterQuerierSource({ hydrationState });

    const queriers: BucketParameterQuerier[] = [];
    const errors: QuerierError[] = [];
    const pending = { queriers, errors };

    const querierOptions: GetQuerierOptions = {
      hasDefaultStreams: true,
      globalParameters: requestParameters({ sub: 'test-user' }),
      streams: {}
    };

    hydrated.pushBucketParameterQueriers(pending, querierOptions);

    expect(errors).toEqual([]);
    expect(queriers).toMatchObject([
      {
        staticBuckets: [
          {
            bucket: 'mybucket-test["test-user"]',
            definition: 'mybucket',
            inclusion_reasons: ['default'],
            priority: 3
          }
        ]
      }
    ]);
  });
});
